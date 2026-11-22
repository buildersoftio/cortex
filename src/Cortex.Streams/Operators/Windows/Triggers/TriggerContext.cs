using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators.Windows.Triggers
{
    /// <summary>
    /// Default implementation of the trigger context.
    /// </summary>
    /// <typeparam name="TInput">The type of items in the window.</typeparam>
    public class TriggerContext<TInput> : ITriggerContext<TInput>
    {
        private readonly Dictionary<string, object> _state = new Dictionary<string, object>();
        private readonly Func<int> _elementCountProvider;
        private readonly Func<DateTime> _currentTimeProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="TriggerContext{TInput}"/> class.
        /// </summary>
        /// <param name="windowKey">The window key.</param>
        /// <param name="elementCountProvider">A function that provides the current element count.</param>
        public TriggerContext(string windowKey, Func<int> elementCountProvider)
            : this(windowKey, elementCountProvider, () => DateTime.UtcNow)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TriggerContext{TInput}"/> class with its own clock.
        /// </summary>
        /// <param name="windowKey">The window key.</param>
        /// <param name="elementCountProvider">A function that provides the current element count.</param>
        /// <param name="currentTimeProvider">
        /// The time reported as <see cref="CurrentProcessingTime"/>: the wall clock, or the watermark when the window works in event time.
        /// </param>
        public TriggerContext(string windowKey, Func<int> elementCountProvider, Func<DateTime> currentTimeProvider)
        {
            WindowKey = windowKey ?? throw new ArgumentNullException(nameof(windowKey));
            _elementCountProvider = elementCountProvider ?? throw new ArgumentNullException(nameof(elementCountProvider));
            _currentTimeProvider = currentTimeProvider ?? throw new ArgumentNullException(nameof(currentTimeProvider));
        }

        /// <inheritdoc/>
        public int ElementCount => _elementCountProvider();

        /// <inheritdoc/>
        public string WindowKey { get; }

        /// <inheritdoc/>
        public DateTime CurrentProcessingTime => _currentTimeProvider();

        /// <inheritdoc/>
        public TState GetState<TState>(string key)
        {
            if (_state.TryGetValue(key, out var value))
            {
                return (TState)value;
            }
            return default;
        }

        /// <inheritdoc/>
        public void SetState<TState>(string key, TState value)
        {
            _state[key] = value;
        }

        /// <inheritdoc/>
        public void ClearState()
        {
            _state.Clear();
        }
    }
}
