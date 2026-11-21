using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Abstractions;
using Cortex.Streams.ErrorHandling;
using Cortex.Streams.Operators;
using Cortex.Streams.Performance;
using Cortex.Streams.Watermarks;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams
{
    /// <summary>
    /// Represents a built stream that can be started and stopped.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream (internal use only).</typeparam>
    public class Stream<TIn, TCurrent> : IStream<TIn>, IStatefulOperator, IDisposable
    {
        private readonly string _name;
        private readonly IOperator _operatorChain;
        private bool _isStarted;
        private bool _isDisposed;

        private readonly ITelemetryProvider _telemetryProvider;
        private readonly StreamExecutionOptions _executionOptions;
        private readonly StreamPerformanceOptions _performanceOptions;

        // Buffered processor for async processing; replaced by Start() after a stop
        private volatile BufferedProcessor<TIn> _bufferedProcessor;
        private readonly object _processorLock = new object();

        // Serialises runtime branch changes so a name check and its add happen together
        private readonly object _branchMutationLock = new object();

        // Watermark operators in the graph and the operators they have switched to event time
        private readonly object _eventTimeLock = new object();
        private readonly HashSet<IWatermarkAware> _eventTimeOperators = new HashSet<IWatermarkAware>();
        private volatile IWatermarkGenerator[] _watermarkGenerators = new IWatermarkGenerator[0];

        /// <param name="branchOperators">
        /// Branches declared by the builder. Not stored: branches are read from the operator graph,
        /// which also holds branches declared before a type-changing operator and branches added at runtime.
        /// </param>
        internal Stream(
            string name,
            IOperator operatorChain,
            List<BranchOperator<TCurrent>> branchOperators,
            ITelemetryProvider telemetryProvider,
            StreamExecutionOptions executionOptions,
            StreamPerformanceOptions performanceOptions = null)
        {
            _name = name;
            _operatorChain = operatorChain;
            _telemetryProvider = telemetryProvider;
            _executionOptions = executionOptions;
            _performanceOptions = performanceOptions ?? StreamPerformanceOptions.Default;

            // Initialize telemetry in operators
            InitializeOperators(_operatorChain);

            // Switch operators downstream of WithWatermarks to event time
            WireWatermarks();
        }

        /// <summary>
        /// Applies the stream's telemetry provider and error handling options to every operator reachable from <paramref name="root"/>.
        /// </summary>
        private void InitializeOperators(IOperator root)
        {
            var operators = CollectOperators(root);

            foreach (var op in operators)
            {
                if (op is ITelemetryEnabled telemetryEnabled)
                {
                    telemetryEnabled.SetTelemetryProvider(_telemetryProvider);
                }
            }

            foreach (var op in operators)
            {
                if (op is IErrorHandlingEnabled errorHandlingEnabled)
                {
                    errorHandlingEnabled.SetErrorHandling(_executionOptions);
                }

                if (op is IStreamStopAware stopAware)
                {
                    stopAware.SetStopAction(Stop);
                }
            }
        }

        /// <summary>
        /// Returns every operator reachable from <paramref name="root"/>, each once, in depth-first order.
        /// </summary>
        private static List<IOperator> CollectOperators(IOperator root)
        {
            var result = new List<IOperator>();
            CollectOperators(root, result, new HashSet<IOperator>());
            return result;
        }

        private static void CollectOperators(IOperator op, List<IOperator> result, HashSet<IOperator> visited, bool stopAtMerge = false)
        {
            if (op == null || !visited.Add(op))
                return;

            // A merge and everything after it are shared with the merge's other branches
            if (stopAtMerge && op is IMergeOperator)
                return;

            result.Add(op);

            foreach (var nextOp in GetSuccessors(op))
            {
                CollectOperators(nextOp, result, visited, stopAtMerge);
            }
        }

        private static IEnumerable<IOperator> GetSuccessors(IOperator op)
        {
            if (op is IHasNextOperators hasNextOperators)
                return hasNextOperators.GetNextOperators();

            var field = op.GetType().GetField("_nextOperator",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var next = field?.GetValue(op) as IOperator;
            return next == null ? Enumerable.Empty<IOperator>() : new[] { next };
        }

        /// <summary>
        /// Returns the operators reachable from <paramref name="root"/> in topological order (reverse post-order),
        /// so every operator comes after all operators that feed it. <paramref name="root"/> itself is excluded.
        /// </summary>
        private static List<IOperator> TopologicalOrder(IOperator root)
        {
            var postOrder = new List<IOperator>();
            var visited = new HashSet<IOperator>();

            void Visit(IOperator op)
            {
                if (op == null || !visited.Add(op))
                    return;

                foreach (var next in GetSuccessors(op))
                {
                    Visit(next);
                }
                postOrder.Add(op);
            }

            Visit(root);
            postOrder.Reverse();
            postOrder.Remove(root);
            return postOrder;
        }

        /// <summary>
        /// Gives every watermark operator the event-time operators downstream of it, in pipeline order, and switches
        /// operators it has not seen before to event time. Runs at construction and after every runtime branch change.
        /// </summary>
        /// <exception cref="InvalidOperationException">A path assigns watermarks twice, or an operator is downstream of two watermark operators.</exception>
        private void WireWatermarks()
        {
            lock (_eventTimeLock)
            {
                var generators = CollectOperators(_operatorChain).OfType<IWatermarkGenerator>().ToArray();
                var owners = new Dictionary<IWatermarkAware, IWatermarkGenerator>();

                foreach (var generator in generators)
                {
                    var downstream = TopologicalOrder((IOperator)generator);
                    if (downstream.Any(op => op is IWatermarkGenerator))
                        throw new InvalidOperationException(
                            $"Stream '{_name}' assigns watermarks more than once on the same path. " +
                            "Call WithWatermarks once, before the first window or join.");

                    var subscribers = downstream.OfType<IWatermarkAware>().ToArray();
                    foreach (var subscriber in subscribers)
                    {
                        if (owners.TryGetValue(subscriber, out var owner) && owner != generator)
                            throw new InvalidOperationException(
                                $"Stream '{_name}' has an operator downstream of two watermark operators; " +
                                "an operator can follow only one WithWatermarks.");

                        owners[subscriber] = generator;
                        EnableEventTime(subscriber, generator);
                    }

                    generator.SetSubscribers(subscribers);
                }

                // Forget operators no longer in the graph, such as removed branches
                _eventTimeOperators.IntersectWith(owners.Keys);
                _watermarkGenerators = generators;
            }
        }

        /// <summary>
        /// Switches the event-time operators of a branch that is about to be added at <paramref name="attachPoint"/>,
        /// so its first item is already handled in event time. The branch is wired to the watermark after it is added.
        /// </summary>
        private void PrepareBranchForEventTime(IOperator attachPoint, IOperator branchRoot)
        {
            lock (_eventTimeLock)
            {
                var branchOperators = CollectOperators(branchRoot);
                if (branchOperators.Any(op => op is IWatermarkGenerator))
                    throw new InvalidOperationException(
                        $"A branch added at runtime to stream '{_name}' cannot assign watermarks.");

                var generator = _watermarkGenerators
                    .FirstOrDefault(g => CollectOperators((IOperator)g).Contains(attachPoint));
                if (generator == null)
                    return;

                foreach (var aware in branchOperators.OfType<IWatermarkAware>())
                {
                    EnableEventTime(aware, generator);
                }
            }
        }

        // Called under _eventTimeLock
        private void EnableEventTime(IWatermarkAware operatorToEnable, IWatermarkGenerator generator)
        {
            if (!_eventTimeOperators.Add(operatorToEnable))
                return;

            operatorToEnable.EnableEventTime(generator.Context);

            var current = generator.CurrentWatermark;
            if (current > DateTime.MinValue)
            {
                operatorToEnable.OnWatermark(current);
            }
        }

        /// <param name="replaceCompleted">
        /// True only from <see cref="Start"/>: a processor completed by a previous stop is replaced by a new one
        /// (new buffer, consumers and statistics). Emit methods never revive a stopped processor.
        /// </param>
        private void EnsureBufferedProcessorInitialized(bool replaceCompleted = false)
        {
            if (!_performanceOptions.EnableBufferedProcessing)
                return;

            var current = _bufferedProcessor;
            if (current != null && !(replaceCompleted && current.IsCompleted))
                return;

            lock (_processorLock)
            {
                current = _bufferedProcessor;
                if (current != null && !(replaceCompleted && current.IsCompleted))
                    return;

                BufferedProcessor<TIn> processor = null;
                processor = new BufferedProcessor<TIn>(
                    _performanceOptions,
                    value => ProcessItem(value, processor),
                    () =>
                    {
                        // A processor of an earlier run must not stop the current one
                        if (ReferenceEquals(_bufferedProcessor, processor))
                            _isStarted = false;
                    });
                _bufferedProcessor = processor;
            }
        }

        private void ProcessItem(TIn value, BufferedProcessor<TIn> processor)
        {
            try
            {
                _operatorChain.Process(value);
            }
            catch (StreamStoppedException)
            {
                // An item still in flight from an earlier run must not stop a restarted stream
                if (ReferenceEquals(_bufferedProcessor, processor))
                    Stop();
            }
        }


        /// <summary>
        /// Starts the stream processing.
        /// </summary>
        public void Start()
        {
            _isStarted = true;

            foreach (var generator in _watermarkGenerators)
            {
                generator.Resume();
            }

            // Initialize buffered processor if enabled, or replace the one a previous stop completed
            if (_performanceOptions.EnableBufferedProcessing)
            {
                EnsureBufferedProcessorInitialized(replaceCompleted: true);
            }
        }

        /// <summary>
        /// Stops the stream processing.
        /// </summary>
        public void Stop()
        {
            _isStarted = false;
            PauseWatermarks();

            if (_operatorChain is SourceOperatorAdapter<TCurrent> sourceAdapter)
            {
                sourceAdapter.Stop();
            }

            // Stop the buffered processor if it exists
            _bufferedProcessor?.Stop();
        }

        /// <summary>
        /// Stops the stream processing asynchronously, waiting for any buffered items to be processed.
        /// The wait ends early when <paramref name="cancellationToken"/> is cancelled or after 30 seconds;
        /// the consumers then stop after their current item and the items still buffered are reported
        /// to <see cref="StreamPerformanceOptions.OnItemDropped"/> with <see cref="DropReason.StreamStopped"/>.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the graceful shutdown.</param>
        /// <returns>A task that represents the asynchronous stop operation.</returns>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled before the buffer drained.</exception>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            _isStarted = false;
            PauseWatermarks();

            if (_operatorChain is SourceOperatorAdapter<TCurrent> sourceAdapter)
            {
                sourceAdapter.Stop();
            }

            // Wait for buffered items to be processed
            if (_bufferedProcessor != null)
            {
                await _bufferedProcessor.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private void PauseWatermarks()
        {
            foreach (var generator in _watermarkGenerators)
            {
                generator.Pause();
            }
        }

        /// <summary>
        /// Gets the current status of the stream.
        /// </summary>
        /// <returns>A string indicating whether the stream is running or stopped.</returns>
        public StreamStatuses GetStatus()
        {
            return _isStarted ? StreamStatuses.RUNNING : StreamStatuses.NOT_RUNNING;
        }

        /// <summary>
        /// Emits data into the stream when no source operator is used.
        /// This method blocks until the entire pipeline has finished processing the item.
        /// </summary>
        /// <param name="value">The data to emit.</param>
        public void Emit(TIn value)
        {
            if (!_isStarted)
                throw new InvalidOperationException("Stream has not been started.");

            if (_operatorChain is SourceOperatorAdapter<TIn>)
                throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");

            try
            {
                _operatorChain.Process(value);
            }
            catch (StreamStoppedException)
            {
                // Global error strategy requested a graceful stop
                Stop();
                // Swallow for graceful shutdown
            }
        }

        /// <summary>
        /// Asynchronously emits data into the stream when no source operator is used.
        /// When buffered processing is enabled, this method adds the item to an internal buffer 
        /// and returns quickly (subject to backpressure settings).
        /// When buffered processing is disabled (default), this runs the pipeline asynchronously using Task.Run.
        /// </summary>
        /// <param name="value">The value to emit.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the emit operation.</param>
        /// <returns>A task that represents the asynchronous emit operation.</returns>
        public async Task EmitAsync(TIn value, CancellationToken cancellationToken = default)
        {
            if (!_isStarted)
                throw new InvalidOperationException("Stream has not been started.");

            if (_operatorChain is SourceOperatorAdapter<TIn>)
                throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");

            cancellationToken.ThrowIfCancellationRequested();

            if (_performanceOptions.EnableBufferedProcessing)
            {
                EnsureBufferedProcessorInitialized();
                await _bufferedProcessor.TryEnqueueAsync(value, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Backward compatible behavior: run synchronously on thread pool
                await Task.Run(() =>
                {
                    try
                    {
                        _operatorChain.Process(value);
                    }
                    catch (StreamStoppedException)
                    {
                        Stop();
                    }
                }, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Emits multiple values to the stream asynchronously.
        /// When buffered processing is enabled, items are added to the buffer in bulk for better throughput.
        /// </summary>
        /// <param name="values">The values to emit.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the emit operation.</param>
        /// <returns>A task that represents the asynchronous batch emit operation.</returns>
        public async Task EmitBatchAsync(IEnumerable<TIn> values, CancellationToken cancellationToken = default)
        {
            if (!_isStarted)
                throw new InvalidOperationException("Stream has not been started.");

            if (_operatorChain is SourceOperatorAdapter<TIn>)
                throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            cancellationToken.ThrowIfCancellationRequested();

            if (_performanceOptions.EnableBufferedProcessing)
            {
                EnsureBufferedProcessorInitialized();
                foreach (var value in values)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _bufferedProcessor.TryEnqueueAsync(value, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                // Process each item asynchronously
                foreach (var value in values)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Run(() =>
                    {
                        try
                        {
                            _operatorChain.Process(value);
                        }
                        catch (StreamStoppedException)
                        {
                            Stop();
                        }
                    }, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Emits a value to the stream without waiting for processing to complete (fire-and-forget).
        /// Requires buffered processing to be enabled via <see cref="StreamPerformanceOptions"/>.
        /// If the buffer is full, behavior is determined by the <see cref="BackpressureStrategy"/>.
        /// </summary>
        /// <param name="value">The value to emit.</param>
        /// <returns>True if the value was accepted into the buffer; false if it was dropped.</returns>
        /// <exception cref="InvalidOperationException">Thrown when buffered processing is not enabled or stream is not started.</exception>
        /// <exception cref="BufferFullException">Thrown when the buffer is full and strategy is ThrowException.</exception>
        public bool EmitAndForget(TIn value)
        {
            if (!_isStarted)
                throw new InvalidOperationException("Stream has not been started.");

            if (_operatorChain is SourceOperatorAdapter<TIn>)
                throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");

            if (!_performanceOptions.EnableBufferedProcessing)
                throw new InvalidOperationException(
                    "EmitAndForget requires buffered processing to be enabled. " +
                    "Configure the stream with WithPerformanceOptions() and set EnableBufferedProcessing = true.");

            EnsureBufferedProcessorInitialized();
            return _bufferedProcessor.TryEnqueue(value);
        }

        /// <summary>
        /// Gets the current buffer statistics when buffered processing is enabled.
        /// Returns null if buffered processing is not enabled.
        /// </summary>
        public BufferStatistics GetBufferStatistics()
        {
            if (!_performanceOptions.EnableBufferedProcessing || _bufferedProcessor == null)
                return null;

            return new BufferStatistics
            {
                CurrentCount = _bufferedProcessor.CurrentBufferCount,
                Capacity = _performanceOptions.BufferCapacity,
                TotalEnqueued = _bufferedProcessor.ItemsEnqueued,
                TotalProcessed = _bufferedProcessor.ItemsProcessed,
                TotalDropped = _bufferedProcessor.ItemsDropped,
                TotalFailed = _bufferedProcessor.ItemsFailed
            };
        }

        public IReadOnlyDictionary<string, IBranchInfo> GetBranches()
        {
            var branchDict = new Dictionary<string, IBranchInfo>();
            foreach (var fork in CollectOperators(_operatorChain).OfType<IForkOperator>())
            {
                foreach (var branch in fork.GetBranchInfos())
                {
                    branchDict[branch.BranchName] = branch;
                }
            }
            return branchDict;
        }

        /// <inheritdoc />
        public void AddBranch<TBranch>(string name, Action<IBranchStreamBuilder<TIn, TBranch>> config)
        {
            AddBranchCore(name, null, config);
        }

        /// <inheritdoc />
        public void AddBranch<TBranch>(string name, Func<TBranch, bool> condition, Action<IBranchStreamBuilder<TIn, TBranch>> config)
        {
            if (condition == null)
                throw new ArgumentNullException(nameof(condition));

            AddBranchCore(name, condition, config);
        }

        private void AddBranchCore<TBranch>(string name, Func<TBranch, bool> condition, Action<IBranchStreamBuilder<TIn, TBranch>> config)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Branch name cannot be null or empty.", nameof(name));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            var fork = FindRuntimeBranchPoint<TBranch>();
            var branchOperator = BranchStreamBuilder<TIn, TBranch>.BuildBranch(_name, name, condition, config);

            // Configure the whole branch before any item can reach it
            InitializeOperators(branchOperator);
            PrepareBranchForEventTime(fork, branchOperator);

            lock (_branchMutationLock)
            {
                if (GetBranches().ContainsKey(name) || !fork.TryAddBranch(name, branchOperator))
                    throw new ArgumentException($"A branch named '{name}' already exists in stream '{_name}'.", nameof(name));
            }

            WireWatermarks();
        }

        /// <inheritdoc />
        public bool RemoveBranch(string name)
        {
            if (string.IsNullOrEmpty(name))
                return false;

            var removedBranches = new List<IOperator>();
            lock (_branchMutationLock)
            {
                foreach (var fork in CollectOperators(_operatorChain).OfType<IForkOperator>())
                {
                    var removed = fork.RemoveBranch(name);
                    if (removed != null)
                        removedBranches.Add(removed);
                }
            }

            if (removedBranches.Count > 0)
            {
                WireWatermarks();
            }

            // Release the removed branches' own resources, such as window timers
            foreach (var removed in removedBranches)
            {
                var branchOperators = new List<IOperator>();
                CollectOperators(removed, branchOperators, new HashSet<IOperator>(), stopAtMerge: true);
                foreach (var disposable in branchOperators.OfType<IDisposable>())
                {
                    disposable.Dispose();
                }
            }

            return removedBranches.Count > 0;
        }

        private ForkOperator<TBranch> FindRuntimeBranchPoint<TBranch>()
        {
            var forks = CollectOperators(_operatorChain)
                .OfType<ForkOperator<TBranch>>()
                .Where(fork => fork.AcceptsRuntimeBranches)
                .ToList();

            if (forks.Count == 0)
                throw new InvalidOperationException(
                    $"Stream '{_name}' has no branch point for items of type '{typeof(TBranch).Name}'. " +
                    "Declare a branch with AddBranch or FanOut at that point when building the stream.");

            if (forks.Count > 1)
                throw new InvalidOperationException(
                    $"Stream '{_name}' has {forks.Count} branch points for items of type '{typeof(TBranch).Name}'; " +
                    "a branch can be added at runtime only when exactly one exists.");

            return forks[0];
        }

        public IEnumerable<IDataStore> GetStateStores()
        {
            var visitedOperators = new HashSet<IOperator>();
            var stateStores = new List<IDataStore>();
            CollectStateStores(_operatorChain, stateStores, visitedOperators);
            return stateStores;
        }

        private void CollectStateStores(IOperator op, List<IDataStore> stateStores, HashSet<IOperator> visitedOperators)
        {
            if (op == null || visitedOperators.Contains(op))
                return;

            visitedOperators.Add(op);

            if (op is IStatefulOperator statefulOperator)
            {
                stateStores.AddRange(statefulOperator.GetStateStores());
            }

            if (op is IHasNextOperators hasNextOperators)
            {
                foreach (var nextOp in hasNextOperators.GetNextOperators())
                {
                    CollectStateStores(nextOp, stateStores, visitedOperators);
                }
            }
            else if (op is IOperator nextOperator)
            {
                var field = op.GetType().GetField("_nextOperator", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                if (field != null)
                {
                    var nextOp = field.GetValue(op) as IOperator;
                    CollectStateStores(nextOp, stateStores, visitedOperators);
                }
            }
        }

        public TStateStore GetStateStoreByName<TStateStore>(string name) where TStateStore : IDataStore
        {
            return GetStateStores()
                .OfType<TStateStore>()
                .FirstOrDefault(store => store.Name == name);
        }

        public IEnumerable<TStateStore> GetStateStoresByType<TStateStore>() where TStateStore : IDataStore
        {
            return GetStateStores()
                .OfType<TStateStore>();
        }

        /// <summary>
        /// Disposes the stream and releases all resources.
        /// </summary>
        public void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            Stop();
            _bufferedProcessor?.Dispose();

            foreach (var generator in _watermarkGenerators.OfType<IDisposable>())
            {
                generator.Dispose();
            }
        }
    }
}
