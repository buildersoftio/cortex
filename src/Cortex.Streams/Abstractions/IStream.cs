using Cortex.States;
using Cortex.Streams.Abstractions;
using Cortex.Streams.Performance;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams
{
    public interface IStream<TIn>
    {
        /// <summary>
        /// Start the stream processing.
        /// </summary>
        void Start();

        /// <summary>
        /// Stops the stream processing.
        /// </summary>
        void Stop();

        /// <summary>
        /// Stops the stream processing asynchronously, waiting for any buffered items to be processed.
        /// The wait ends early when <paramref name="cancellationToken"/> is cancelled or after 30 seconds;
        /// the consumers then stop after their current item and the items still buffered are reported as dropped
        /// with <see cref="DropReason.StreamStopped"/>.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the graceful shutdown.</param>
        /// <returns>A task that represents the asynchronous stop operation.</returns>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled before the buffer drained.</exception>
        Task StopAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Processes the specified input value and emits it to the underlying stream.
        /// This method blocks until the entire pipeline has finished processing the item.
        /// </summary>
        /// <param name="value">The input value to be emitted. The meaning and requirements of this value depend on the implementation.</param>
        void Emit(TIn value);

        /// <summary>
        /// Asynchronously emits the specified value to the underlying stream.
        /// When buffered processing is enabled via <see cref="StreamPerformanceOptions"/>, 
        /// this method adds the item to an internal buffer and returns immediately (subject to backpressure settings).
        /// When buffered processing is disabled (default), this runs the pipeline asynchronously using Task.Run.
        /// </summary>
        /// <param name="value">The value to emit. The meaning and requirements of this value depend on the implementation.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the emit operation.</param>
        /// <returns>A task that represents the asynchronous emit operation.</returns>
        Task EmitAsync(TIn value, CancellationToken cancellationToken = default);

        /// <summary>
        /// Emits multiple values to the stream asynchronously.
        /// When buffered processing is enabled, items are added to the buffer in bulk for better throughput.
        /// </summary>
        /// <param name="values">The values to emit.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the emit operation.</param>
        /// <returns>A task that represents the asynchronous batch emit operation.</returns>
        Task EmitBatchAsync(IEnumerable<TIn> values, CancellationToken cancellationToken = default);

        /// <summary>
        /// Emits a value to the stream without waiting for processing to complete (fire-and-forget).
        /// Requires buffered processing to be enabled via <see cref="StreamPerformanceOptions"/>.
        /// If the buffer is full, behavior is determined by the <see cref="BackpressureStrategy"/>.
        /// </summary>
        /// <param name="value">The value to emit.</param>
        /// <returns>True if the value was accepted into the buffer; false if it was dropped.</returns>
        /// <exception cref="System.InvalidOperationException">Thrown when buffered processing is not enabled.</exception>
        /// <exception cref="BufferFullException">Thrown when the buffer is full and strategy is ThrowException.</exception>
        bool EmitAndForget(TIn value);

        StreamStatuses GetStatus();

        IReadOnlyDictionary<string, IBranchInfo> GetBranches();

        /// <summary>
        /// Adds a branch to the running stream. The branch is attached to the stream's only branch point
        /// (created by <c>AddBranch</c> or <c>FanOut</c> when the stream was built) whose items are of type
        /// <typeparamref name="TBranch"/>, and receives the items that reach that point after this call returns.
        /// </summary>
        /// <typeparam name="TBranch">The type of the items at the branch point.</typeparam>
        /// <param name="name">The unique name of the branch.</param>
        /// <param name="config">An action that configures the branch's operators.</param>
        /// <exception cref="System.ArgumentException">The name is null or empty, or a branch with this name already exists.</exception>
        /// <exception cref="System.ArgumentNullException"><paramref name="config"/> is null.</exception>
        /// <exception cref="System.InvalidOperationException">
        /// The stream has no branch point, or more than one, for <typeparamref name="TBranch"/>; or the branch has no operators.
        /// </exception>
        void AddBranch<TBranch>(string name, Action<IBranchStreamBuilder<TIn, TBranch>> config);

        /// <summary>
        /// Adds a conditional branch to the running stream: only items matching <paramref name="condition"/> enter it.
        /// The branch point is chosen as for <see cref="AddBranch{TBranch}(string, Action{IBranchStreamBuilder{TIn, TBranch}})"/>.
        /// </summary>
        /// <typeparam name="TBranch">The type of the items at the branch point.</typeparam>
        /// <param name="name">The unique name of the branch.</param>
        /// <param name="condition">A predicate deciding which items enter the branch. It follows the stream's error handling.</param>
        /// <param name="config">An action that configures the branch's operators.</param>
        /// <exception cref="System.ArgumentException">The name is null or empty, or a branch with this name already exists.</exception>
        /// <exception cref="System.ArgumentNullException"><paramref name="condition"/> or <paramref name="config"/> is null.</exception>
        /// <exception cref="System.InvalidOperationException">
        /// The stream has no branch point, or more than one, for <typeparamref name="TBranch"/>; or the branch has no operators.
        /// </exception>
        void AddBranch<TBranch>(string name, Func<TBranch, bool> condition, Action<IBranchStreamBuilder<TIn, TBranch>> config);

        /// <summary>
        /// Removes a branch from the running stream. Items that enter the branch point after this call
        /// returns no longer reach the branch; an item already being processed may still complete in it.
        /// Disposable operators of the branch, such as window operators, are disposed, so windows still
        /// open in the branch are dropped. Operators after a merge are shared and stay untouched.
        /// </summary>
        /// <param name="name">The name of the branch to remove.</param>
        /// <returns>True if a branch with this name was removed; otherwise false.</returns>
        bool RemoveBranch(string name);

        TStateStore GetStateStoreByName<TStateStore>(string name) where TStateStore : IDataStore;
        IEnumerable<TStateStore> GetStateStoresByType<TStateStore>() where TStateStore : IDataStore;

        /// <summary>
        /// Gets the current buffer statistics when buffered processing is enabled.
        /// Returns null if buffered processing is not enabled.
        /// </summary>
        BufferStatistics GetBufferStatistics();
    }

    /// <summary>
    /// Statistics about the stream's internal buffer.
    /// </summary>
    public sealed class BufferStatistics
    {
        /// <summary>
        /// Current number of items in the buffer.
        /// </summary>
        public int CurrentCount { get; set; }

        /// <summary>
        /// Maximum capacity of the buffer.
        /// </summary>
        public int Capacity { get; set; }

        /// <summary>
        /// Total number of items enqueued since the stream started.
        /// </summary>
        public long TotalEnqueued { get; set; }

        /// <summary>
        /// Total number of items successfully processed since the stream started.
        /// </summary>
        public long TotalProcessed { get; set; }

        /// <summary>
        /// Total number of items dropped since the stream started: items discarded by
        /// <see cref="BackpressureStrategy.DropOldest"/> or <see cref="BackpressureStrategy.DropNewest"/>,
        /// and items rejected because the stream was stopping. Each one is also reported to
        /// <see cref="StreamPerformanceOptions.OnItemDropped"/>.
        /// </summary>
        public long TotalDropped { get; set; }

        /// <summary>
        /// Total number of buffered items whose processing threw an exception that the stream's
        /// error handling did not handle, since the stream started. Each one is also reported to
        /// <see cref="StreamPerformanceOptions.OnProcessingError"/>.
        /// </summary>
        public long TotalFailed { get; set; }

        /// <summary>
        /// Current buffer utilization as a percentage (0-100).
        /// </summary>
        public double UtilizationPercent
        {
            get { return Capacity > 0 ? (CurrentCount * 100.0) / Capacity : 0; }
        }
    }
}
