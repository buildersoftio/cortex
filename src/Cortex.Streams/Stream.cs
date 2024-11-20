using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.Streams
{
    /// <summary>
    /// Represents a built stream that can be started and stopped.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
    public class Stream<TIn, TCurrent> : IStream<TIn, TCurrent>, IStatefulOperator
    {
        private readonly string _name;
        private readonly IOperator _operatorChain;
        private readonly List<BranchOperator<TCurrent>> _branchOperators;
        private bool _isStarted;

        private readonly ITelemetryProvider _telemetryProvider;

        internal Stream(string name, IOperator operatorChain, List<BranchOperator<TCurrent>> branchOperators, ITelemetryProvider telemetryProvider)
        {
            _name = name;
            _operatorChain = operatorChain;
            _branchOperators = branchOperators;
            _telemetryProvider = telemetryProvider;

            // Initialize telemetry in operators
            InitializeTelemetry(_operatorChain);
        }

        private void InitializeTelemetry(IOperator op)
        {
            if (op == null)
                return;

            if (op is ITelemetryEnabled telemetryEnabled)
            {
                telemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }

            if (op is IHasNextOperators hasNextOperators)
            {
                foreach (var nextOp in hasNextOperators.GetNextOperators())
                {
                    InitializeTelemetry(nextOp);
                }
            }
            else
            {
                var field = op.GetType().GetField("_nextOperator", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                if (field != null)
                {
                    var nextOp = field.GetValue(op) as IOperator;
                    InitializeTelemetry(nextOp);
                }
            }
        }


        /// <summary>
        /// Starts the stream processing.
        /// </summary>
        public void Start()
        {
            _isStarted = true;
        }

        /// <summary>
        /// Stops the stream processing.
        /// </summary>
        public void Stop()
        {
            _isStarted = false;

            if (_operatorChain is SourceOperatorAdapter<TCurrent> sourceAdapter)
            {
                sourceAdapter.Stop();
            }
        }

        /// <summary>
        /// Gets the current status of the stream.
        /// </summary>
        /// <returns>A string indicating whether the stream is running or stopped.</returns>
        public string GetStatus()
        {
            return _isStarted ? "Running" : "Stopped";
        }

        /// <summary>
        /// Emits data into the stream when no source operator is used.
        /// </summary>
        /// <param name="value">The data to emit.</param>
        public void Emit(TIn value)
        {
            if (_isStarted)
            {
                if (_operatorChain is SourceOperatorAdapter<TIn>)
                {
                    throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");
                }

                _operatorChain.Process(value);
            }
            else
            {
                throw new InvalidOperationException("Stream has not been started.");
            }
        }

        public IReadOnlyDictionary<string, BranchOperator<TCurrent>> GetBranches()
        {
            var branchDict = new Dictionary<string, BranchOperator<TCurrent>>();
            foreach (var branchOperator in _branchOperators)
            {
                branchDict[branchOperator.BranchName] = branchOperator;
            }
            return branchDict;
        }

        public IEnumerable<IStateStore> GetStateStores()
        {
            var visitedOperators = new HashSet<IOperator>();
            var stateStores = new List<IStateStore>();
            CollectStateStores(_operatorChain, stateStores, visitedOperators);
            return stateStores;
        }

        private void CollectStateStores(IOperator op, List<IStateStore> stateStores, HashSet<IOperator> visitedOperators)
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

        public TStateStore GetStateStoreByName<TStateStore>(string name) where TStateStore : IStateStore
        {
            return GetStateStores()
                .OfType<TStateStore>()
                .FirstOrDefault(store => store.Name == name);
        }

        public IEnumerable<TStateStore> GetStateStoresByType<TStateStore>() where TStateStore : IStateStore
        {
            return GetStateStores()
                .OfType<TStateStore>();
        }
    }
}
