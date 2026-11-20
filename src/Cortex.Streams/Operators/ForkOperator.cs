using Cortex.Streams.Abstractions;
using Cortex.Streams.ErrorHandling;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// Operator that splits the stream into multiple branches for fan-out processing.
    /// Forwards telemetry and error handling configuration to all branches.
    /// </summary>
    /// <remarks>
    /// Branches can be added and removed while items are flowing. Changes are copy-on-write:
    /// each item reads the branch set once, so a change takes effect for items that reach the
    /// fork after it, and an item already being processed finishes on the set it started with.
    /// </remarks>
    internal class ForkOperator<T> : IOperator, IHasNextOperators, ITelemetryEnabled, IErrorHandlingEnabled, IForkOperator
    {
        private readonly object _branchesLock = new object();

        // Immutable snapshot, replaced as a whole under _branchesLock; read without a lock
        private BranchOperator<T>[] _branches = Array.Empty<BranchOperator<T>>();
        private IOperator _continuationOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Error handling
        private StreamExecutionOptions _executionOptions;

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"fork_operator_processed_{typeof(T).Name}", "Number of items processed by ForkOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"fork_operator_processing_time_{typeof(T).Name}", "Processing time for ForkOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"ForkOperator_{typeof(T).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _recordProcessingTime = null;
            }

            // Propagate telemetry to all branches
            foreach (var branch in Volatile.Read(ref _branches))
            {
                if (branch is ITelemetryEnabled telemetryEnabled)
                {
                    telemetryEnabled.SetTelemetryProvider(telemetryProvider);
                }
            }

            // Propagate telemetry to the continuation operator
            if (_continuationOperator is ITelemetryEnabled continuationTelemetryEnabled)
            {
                continuationTelemetryEnabled.SetTelemetryProvider(telemetryProvider);
            }
        }

        /// <summary>
        /// Forwards error handling configuration to all branches and the continuation operator.
        /// </summary>
        public void SetErrorHandling(StreamExecutionOptions options)
        {
            _executionOptions = options;

            // Forward error handling to all branches
            foreach (var branch in Volatile.Read(ref _branches))
            {
                if (branch is IErrorHandlingEnabled errorHandlingEnabled)
                {
                    errorHandlingEnabled.SetErrorHandling(options);
                }
            }

            // Forward error handling to the continuation operator
            if (_continuationOperator is IErrorHandlingEnabled continuationErrorHandlingEnabled)
            {
                continuationErrorHandlingEnabled.SetErrorHandling(options);
            }
        }

        /// <summary>
        /// Adds a branch, replacing a branch with the same name in its position.
        /// </summary>
        public void AddBranch(string name, BranchOperator<T> branchOperator)
        {
            ValidateBranch(name, branchOperator);
            ConfigureNewBranch(branchOperator);

            lock (_branchesLock)
            {
                var current = _branches;
                var index = IndexOf(current, name);
                BranchOperator<T>[] updated;

                if (index >= 0)
                {
                    updated = (BranchOperator<T>[])current.Clone();
                    updated[index] = branchOperator;
                }
                else
                {
                    updated = Append(current, branchOperator);
                }

                Volatile.Write(ref _branches, updated);
            }
        }

        /// <summary>
        /// Adds a branch unless a branch with the same name already exists.
        /// </summary>
        /// <returns>True if the branch was added; false if the name is already taken.</returns>
        public bool TryAddBranch(string name, BranchOperator<T> branchOperator)
        {
            ValidateBranch(name, branchOperator);
            ConfigureNewBranch(branchOperator);

            lock (_branchesLock)
            {
                var current = _branches;
                if (IndexOf(current, name) >= 0)
                    return false;

                Volatile.Write(ref _branches, Append(current, branchOperator));
                return true;
            }
        }

        /// <summary>
        /// Removes the branch with the given name.
        /// </summary>
        /// <returns>True if a branch was removed; otherwise false.</returns>
        public bool RemoveBranch(string name)
        {
            if (name == null)
                return false;

            lock (_branchesLock)
            {
                var current = _branches;
                var index = IndexOf(current, name);
                if (index < 0)
                    return false;

                var updated = new BranchOperator<T>[current.Length - 1];
                Array.Copy(current, 0, updated, 0, index);
                Array.Copy(current, index + 1, updated, index, current.Length - index - 1);
                Volatile.Write(ref _branches, updated);
                return true;
            }
        }

        /// <summary>
        /// Gets a snapshot of the branches currently attached to the fork, in insertion order.
        /// </summary>
        public IReadOnlyList<IBranchInfo> GetBranchInfos()
        {
            return (IBranchInfo[])Volatile.Read(ref _branches).Clone();
        }

        public void Process(object input)
        {
            // Read the snapshot once so concurrent branch changes cannot affect this item
            var branches = Volatile.Read(ref _branches);

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();

                using (var span = _tracer.StartSpan("ForkOperator.Process"))
                {
                    try
                    {
                        span.SetAttribute("branch_count", branches.Length.ToString());
                        foreach (var branch in branches)
                        {
                            branch.Process(input);
                        }

                        // Process continuation operator after branches
                        _continuationOperator?.Process(input);

                        span.SetAttribute("status", "success");
                    }
                    catch (Exception ex)
                    {
                        span.SetAttribute("status", "error");
                        span.SetAttribute("exception", ex.ToString());
                        throw;
                    }
                    finally
                    {
                        stopwatch.Stop();
                        _recordProcessingTime(stopwatch.Elapsed.TotalMilliseconds);
                        _incrementProcessedCounter();
                    }
                }
            }
            else
            {
                foreach (var branch in branches)
                {
                    branch.Process(input);
                }

                // Process continuation operator after branches
                _continuationOperator?.Process(input);
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            _continuationOperator = nextOperator;

            // Propagate telemetry to the new continuation operator if already configured
            if (_telemetryProvider != null && nextOperator is ITelemetryEnabled telemetryEnabled)
            {
                telemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            foreach (var branch in Volatile.Read(ref _branches))
            {
                yield return branch;
            }

            if (_continuationOperator != null)
            {
                yield return _continuationOperator;
            }
        }

        /// <summary>
        /// Gets a snapshot of the branches, keyed by name.
        /// </summary>
        public IReadOnlyDictionary<string, BranchOperator<T>> Branches
        {
            get
            {
                var snapshot = new Dictionary<string, BranchOperator<T>>();
                foreach (var branch in Volatile.Read(ref _branches))
                {
                    snapshot[branch.BranchName] = branch;
                }
                return snapshot;
            }
        }

        private static void ValidateBranch(string name, BranchOperator<T> branchOperator)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Branch name cannot be null or empty.", nameof(name));
            if (branchOperator == null)
                throw new ArgumentNullException(nameof(branchOperator));
        }

        private void ConfigureNewBranch(BranchOperator<T> branchOperator)
        {
            // Propagate telemetry to the new branch if already configured
            if (_telemetryProvider != null)
            {
                branchOperator.SetTelemetryProvider(_telemetryProvider);
            }

            // Propagate error handling to the new branch if already configured
            if (_executionOptions != null)
            {
                branchOperator.SetErrorHandling(_executionOptions);
            }
        }

        private static int IndexOf(BranchOperator<T>[] branches, string name)
        {
            for (var i = 0; i < branches.Length; i++)
            {
                if (string.Equals(branches[i].BranchName, name, StringComparison.Ordinal))
                    return i;
            }
            return -1;
        }

        private static BranchOperator<T>[] Append(BranchOperator<T>[] branches, BranchOperator<T> branchOperator)
        {
            var updated = new BranchOperator<T>[branches.Length + 1];
            Array.Copy(branches, updated, branches.Length);
            updated[branches.Length] = branchOperator;
            return updated;
        }
    }
}
