using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Cortex.Streams.Performance
{
    /// <summary>
    /// Internal processor that handles buffered async processing of stream items.
    /// Manages the bounded buffer, backpressure, and parallel consumption using System.Threading.Channels.
    /// </summary>
    /// <typeparam name="T">The type of items being processed.</typeparam>
    internal sealed class BufferedProcessor<T> : IDisposable
    {
        private readonly Channel<T> _channel;
        private readonly StreamPerformanceOptions _options;
        private readonly Action<T> _processAction;
        private readonly Action _onStop;
        private readonly CancellationTokenSource _cts;
        private readonly Task[] _consumerTasks;
        
        private long _itemsEnqueued;
        private long _itemsProcessed;
        private long _itemsDropped;
        private bool _isDisposed;
        private volatile bool _writerCompleted;

        /// <summary>
        /// Gets the current number of items in the buffer.
        /// </summary>
        public int CurrentBufferCount => _channel.Reader.Count;

        /// <summary>
        /// Gets the total number of items enqueued since the processor started.
        /// </summary>
        public long ItemsEnqueued => Interlocked.Read(ref _itemsEnqueued);

        /// <summary>
        /// Gets the total number of items successfully processed since the processor started.
        /// </summary>
        public long ItemsProcessed => Interlocked.Read(ref _itemsProcessed);

        /// <summary>
        /// Gets the total number of items dropped (by backpressure or because the processor was stopping) since the processor started.
        /// </summary>
        public long ItemsDropped => Interlocked.Read(ref _itemsDropped);

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferedProcessor{T}"/> class.
        /// </summary>
        /// <param name="options">Performance options for buffer configuration.</param>
        /// <param name="processAction">Action to process each item.</param>
        /// <param name="onStop">Optional action to call when the processor stops.</param>
        public BufferedProcessor(
            StreamPerformanceOptions options,
            Action<T> processAction,
            Action onStop = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _processAction = processAction ?? throw new ArgumentNullException(nameof(processAction));
            _onStop = onStop;
            _cts = new CancellationTokenSource();

            // Configure channel based on backpressure strategy.
            // DropNewest uses Wait mode: EnqueueWithDrop rejects the incoming item itself when TryWrite fails,
            // because the channel's own DropNewest mode would evict the newest buffered item instead.
            var channelOptions = new BoundedChannelOptions(_options.BufferCapacity)
            {
                FullMode = _options.BackpressureStrategy == BackpressureStrategy.DropOldest
                    ? BoundedChannelFullMode.DropOldest
                    : BoundedChannelFullMode.Wait,
                SingleReader = _options.ConcurrencyLevel == 1,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            };

            _channel = _options.BackpressureStrategy == BackpressureStrategy.DropOldest
                ? Channel.CreateBounded<T>(channelOptions, evicted => ReportDrop(evicted, DropReason.BufferFullDropOldest))
                : Channel.CreateBounded<T>(channelOptions);

            // Start consumer tasks
            _consumerTasks = new Task[_options.ConcurrencyLevel];
            for (int i = 0; i < _options.ConcurrencyLevel; i++)
            {
                _consumerTasks[i] = _options.BatchSize > 1
                    ? RunBatchConsumerAsync(_cts.Token)
                    : RunSingleItemConsumerAsync(_cts.Token);
            }
        }

        /// <summary>
        /// Attempts to enqueue an item into the buffer synchronously.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>True if the item was enqueued; false if it was dropped.</returns>
        /// <exception cref="BufferFullException">Thrown when the buffer is full and strategy is ThrowException.</exception>
        /// <exception cref="OperationCanceledException">Thrown when blocking times out or is cancelled.</exception>
        public bool TryEnqueue(T item, CancellationToken cancellationToken = default)
        {
            if (_isDisposed)
            {
                ReportDrop(item, DropReason.StreamStopped);
                return false;
            }

            cancellationToken.ThrowIfCancellationRequested();

            switch (_options.BackpressureStrategy)
            {
                case BackpressureStrategy.ThrowException:
                    return EnqueueOrThrow(item);

                case BackpressureStrategy.DropOldest:
                case BackpressureStrategy.DropNewest:
                    return EnqueueWithDrop(item);

                case BackpressureStrategy.Block:
                default:
                    return EnqueueBlockingSync(item, cancellationToken);
            }
        }

        /// <summary>
        /// Asynchronously enqueues an item into the buffer.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>True if the item was enqueued; false if it was dropped.</returns>
        public async ValueTask<bool> TryEnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            if (_isDisposed)
            {
                ReportDrop(item, DropReason.StreamStopped);
                return false;
            }

            cancellationToken.ThrowIfCancellationRequested();

            switch (_options.BackpressureStrategy)
            {
                case BackpressureStrategy.ThrowException:
                    return EnqueueOrThrow(item);

                case BackpressureStrategy.DropOldest:
                case BackpressureStrategy.DropNewest:
                    return EnqueueWithDrop(item);

                case BackpressureStrategy.Block:
                default:
                    return await EnqueueBlockingAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }

        private bool EnqueueBlockingSync(T item, CancellationToken cancellationToken)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_options.BlockingTimeout != Timeout.InfiniteTimeSpan)
            {
                timeoutCts.CancelAfter(_options.BlockingTimeout);
            }

            try
            {
                // Spin briefly then block
                var spinWait = new SpinWait();
                while (!_channel.Writer.TryWrite(item))
                {
                    if (timeoutCts.Token.IsCancellationRequested)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            throw new OperationCanceledException(cancellationToken);
                        throw new OperationCanceledException("Buffer blocking timeout exceeded.");
                    }

                    if (spinWait.NextSpinWillYield)
                    {
                        // Block asynchronously but wait synchronously
                        var writeTask = _channel.Writer.WriteAsync(item, timeoutCts.Token);
                        writeTask.AsTask().GetAwaiter().GetResult();
                        Interlocked.Increment(ref _itemsEnqueued);
                        return true;
                    }
                    
                    spinWait.SpinOnce();
                }

                Interlocked.Increment(ref _itemsEnqueued);
                return true;
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw;
                throw new OperationCanceledException("Buffer blocking timeout exceeded.");
            }
            catch (ChannelClosedException)
            {
                ReportDrop(item, DropReason.StreamStopped);
                return false;
            }
        }

        private async ValueTask<bool> EnqueueBlockingAsync(T item, CancellationToken cancellationToken)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_options.BlockingTimeout != Timeout.InfiniteTimeSpan)
            {
                timeoutCts.CancelAfter(_options.BlockingTimeout);
            }

            try
            {
                await _channel.Writer.WriteAsync(item, timeoutCts.Token).ConfigureAwait(false);
                Interlocked.Increment(ref _itemsEnqueued);
                return true;
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw;
                throw new OperationCanceledException("Buffer blocking timeout exceeded.");
            }
            catch (ChannelClosedException)
            {
                ReportDrop(item, DropReason.StreamStopped);
                return false;
            }
        }

        private bool EnqueueOrThrow(T item)
        {
            if (_channel.Writer.TryWrite(item))
            {
                Interlocked.Increment(ref _itemsEnqueued);
                return true;
            }

            if (_writerCompleted)
            {
                ReportDrop(item, DropReason.StreamStopped);
                return false;
            }

            throw new BufferFullException(_options.BufferCapacity, item);
        }

        private bool EnqueueWithDrop(T item)
        {
            // DropOldest: the channel evicts the oldest item itself and reports it through ReportDrop.
            // DropNewest: the channel is in Wait mode, so TryWrite fails when the buffer is full.
            if (_channel.Writer.TryWrite(item))
            {
                Interlocked.Increment(ref _itemsEnqueued);
                return true;
            }

            var reason = _writerCompleted || _options.BackpressureStrategy == BackpressureStrategy.DropOldest
                ? DropReason.StreamStopped
                : DropReason.BufferFullDropNewest;
            ReportDrop(item, reason);
            return false;
        }

        private void ReportDrop(T item, DropReason reason)
        {
            Interlocked.Increment(ref _itemsDropped);
            _options.OnItemDropped?.Invoke(item, reason);
        }

        private async Task RunSingleItemConsumerAsync(CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var item in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    try
                    {
                        _processAction(item);
                        Interlocked.Increment(ref _itemsProcessed);
                    }
                    catch
                    {
                        // Error handling is done within the process action (via StreamExecutionOptions)
                        // We continue processing to maintain throughput
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Normal shutdown
            }
            catch (ChannelClosedException)
            {
                // Channel was completed
            }
        }

        private async Task RunBatchConsumerAsync(CancellationToken cancellationToken)
        {
            var batch = new List<T>(_options.BatchSize);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    batch.Clear();

                    // Try to read up to BatchSize items or until timeout
                    using var batchTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    batchTimeoutCts.CancelAfter(_options.BatchTimeout);

                    try
                    {
                        while (batch.Count < _options.BatchSize)
                        {
                            if (_channel.Reader.TryRead(out var item))
                            {
                                batch.Add(item);
                            }
                            else
                            {
                                // Wait for more items
                                if (!await _channel.Reader.WaitToReadAsync(batchTimeoutCts.Token).ConfigureAwait(false))
                                {
                                    break; // Channel completed
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException) when (batchTimeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    {
                        // Batch timeout - process what we have
                    }

                    // Process the batch
                    foreach (var item in batch)
                    {
                        try
                        {
                            _processAction(item);
                            Interlocked.Increment(ref _itemsProcessed);
                        }
                        catch
                        {
                            // Error handling is done within the process action
                        }
                    }

                    // If channel is completed and we processed the last batch, exit
                    if (_channel.Reader.Completion.IsCompleted && batch.Count == 0)
                    {
                        break;
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Normal shutdown
            }
            catch (ChannelClosedException)
            {
                // Channel was completed
            }
        }

        /// <summary>
        /// Signals that no more items will be added and waits for processing to complete.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            _writerCompleted = true;
            _channel.Writer.Complete();
            
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(30)); // Default graceful shutdown timeout
            
            try
            {
                await Task.WhenAll(_consumerTasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                // Timeout during graceful shutdown
            }
        }

        /// <summary>
        /// Stops processing immediately without waiting for the buffer to drain.
        /// </summary>
        public void Stop()
        {
            _writerCompleted = true;
            _cts.Cancel();
            _channel.Writer.TryComplete();
            _onStop?.Invoke();
        }

        /// <summary>
        /// Disposes the processor and releases resources.
        /// </summary>
        public void Dispose()
        {
            if (_isDisposed) return;
            
            _isDisposed = true;
            Stop();
            
            try
            {
                // Give consumer tasks a chance to complete
                Task.WhenAll(_consumerTasks).Wait(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Ignore exceptions during disposal
            }
            
            _cts.Dispose();
        }
    }
}
