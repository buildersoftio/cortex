using System;

namespace Cortex.Streams.Performance
{
    /// <summary>
    /// Configuration options for stream performance, memory management, and async processing.
    /// These options enable high-throughput scenarios with backpressure handling.
    /// </summary>
    public sealed class StreamPerformanceOptions
    {
        /// <summary>
        /// Enables asynchronous buffered processing for <see cref="IStream{TIn, TCurrent}.EmitAsync"/> 
        /// and <see cref="IStream{TIn, TCurrent}.EmitAndForget"/>.
        /// When disabled (default), <see cref="IStream{TIn, TCurrent}.EmitAsync"/> still runs asynchronously 
        /// but without internal buffering.
        /// </summary>
        public bool EnableBufferedProcessing { get; set; } = false;

        /// <summary>
        /// Maximum number of items that can be buffered when <see cref="EnableBufferedProcessing"/> is true.
        /// When the buffer reaches this capacity, the <see cref="BackpressureStrategy"/> determines behavior.
        /// Default is 10,000 items.
        /// </summary>
        public int BufferCapacity { get; set; } = 10_000;

        /// <summary>
        /// The strategy to use when the buffer is full.
        /// Default is <see cref="BackpressureStrategy.Block"/>.
        /// </summary>
        public BackpressureStrategy BackpressureStrategy { get; set; } = BackpressureStrategy.Block;

        /// <summary>
        /// Number of items to process in a single batch when buffered processing is enabled.
        /// Higher values can improve throughput but increase latency. Set to 1 for immediate processing.
        /// Default is 1 (process items immediately as they arrive).
        /// </summary>
        public int BatchSize { get; set; } = 1;

        /// <summary>
        /// Maximum time to wait for a batch to fill before processing available items.
        /// Only applies when <see cref="BatchSize"/> is greater than 1.
        /// Default is 100 milliseconds.
        /// </summary>
        public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Number of concurrent processing tasks when buffered processing is enabled.
        /// Higher values allow parallel processing but require thread-safe operators.
        /// Default is 1 (single consumer for ordered processing).
        /// </summary>
        public int ConcurrencyLevel { get; set; } = 1;

        /// <summary>
        /// Timeout for blocking operations when <see cref="BackpressureStrategy.Block"/> is used.
        /// After this timeout, an <see cref="OperationCanceledException"/> is thrown.
        /// Set to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to wait indefinitely.
        /// Default is 30 seconds.
        /// </summary>
        public TimeSpan BlockingTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Callback invoked when an item is dropped due to backpressure.
        /// This allows monitoring and logging of dropped items.
        /// </summary>
        public Action<object, DropReason> OnItemDropped { get; set; }

        /// <summary>
        /// Callback invoked when an exception escapes the pipeline while a buffered item is processed,
        /// that is, when the stream's error handling did not handle it.
        /// The item is skipped and counted in <see cref="BufferStatistics.TotalFailed"/>, and the consumer
        /// continues with the next item. Exceptions thrown by this callback are ignored.
        /// </summary>
        public Action<object, Exception> OnProcessingError { get; set; }

        /// <summary>
        /// Default performance options with buffered processing disabled.
        /// This maintains backward compatibility with existing code.
        /// </summary>
        public static readonly StreamPerformanceOptions Default = new StreamPerformanceOptions();

        /// <summary>
        /// Creates performance options optimized for high-throughput scenarios.
        /// Enables buffered processing with a large buffer and parallel consumers.
        /// </summary>
        /// <param name="bufferCapacity">Buffer capacity (default 100,000)</param>
        /// <param name="concurrencyLevel">Number of parallel consumers (default Environment.ProcessorCount)</param>
        /// <returns>High-throughput performance options</returns>
        public static StreamPerformanceOptions HighThroughput(
            int bufferCapacity = 100_000,
            int? concurrencyLevel = null)
        {
            return new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = bufferCapacity,
                BackpressureStrategy = BackpressureStrategy.Block,
                BatchSize = 100,
                BatchTimeout = TimeSpan.FromMilliseconds(50),
                ConcurrencyLevel = concurrencyLevel ?? Environment.ProcessorCount,
                BlockingTimeout = TimeSpan.FromSeconds(60)
            };
        }

        /// <summary>
        /// Creates performance options optimized for low-latency scenarios.
        /// Enables buffered processing with immediate single-item processing.
        /// </summary>
        /// <param name="bufferCapacity">Buffer capacity (default 10,000)</param>
        /// <returns>Low-latency performance options</returns>
        public static StreamPerformanceOptions LowLatency(int bufferCapacity = 10_000)
        {
            return new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = bufferCapacity,
                BackpressureStrategy = BackpressureStrategy.Block,
                BatchSize = 1,
                ConcurrencyLevel = 1,
                BlockingTimeout = TimeSpan.FromSeconds(30)
            };
        }

        /// <summary>
        /// Creates performance options that drop old items when under pressure.
        /// Useful for scenarios where latest data is more important than historical data.
        /// </summary>
        /// <param name="bufferCapacity">Buffer capacity (default 10,000)</param>
        /// <param name="onItemDropped">Callback for dropped items</param>
        /// <returns>Drop-oldest performance options</returns>
        public static StreamPerformanceOptions DropOldest(
            int bufferCapacity = 10_000,
            Action<object, DropReason> onItemDropped = null)
        {
            return new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = bufferCapacity,
                BackpressureStrategy = BackpressureStrategy.DropOldest,
                BatchSize = 1,
                ConcurrencyLevel = 1,
                OnItemDropped = onItemDropped
            };
        }
    }

    /// <summary>
    /// Reason why an item was dropped from the processing buffer.
    /// </summary>
    public enum DropReason
    {
        /// <summary>
        /// Item was dropped because the buffer was full (DropNewest strategy).
        /// </summary>
        BufferFullDropNewest,

        /// <summary>
        /// Item was evicted to make room for a newer item (DropOldest strategy).
        /// </summary>
        BufferFullDropOldest,

        /// <summary>
        /// Item was dropped due to stream shutdown.
        /// </summary>
        StreamStopped
    }
}
