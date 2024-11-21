using Cortex.Streams.Files.Deserializers;
using Cortex.Streams.Operators;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams.Files
{
    /// <summary>
    /// Source operator that reads data from a file (CSV, XML, or Jsonl) and emits each record into the stream.
    /// </summary>
    /// <typeparam name="TOutput">The type of data to emit.</typeparam>
    public class FileSourceOperator<TOutput> : ISourceOperator<TOutput>, IDisposable where TOutput : new()
    {
        private readonly string _filePath;
        private readonly FileFormat _fileFormat;
        private readonly IDeserializer<TOutput> _deserializer;
        private CancellationTokenSource _cts;
        private Task _readingTask;
        private readonly object _lock = new object();
        private bool _headersInitialized = false;
        private DefaultCsvDeserializer<TOutput> _csvDeserializer;

        /// <summary>
        /// Initializes a new instance of FileSourceOperator.
        /// </summary>
        /// <param name="filePath">Path to the input file.</param>
        /// <param name="fileFormat">Format of the input file.</param>
        /// <param name="deserializer">Custom deserializer. If null, default deserializers are used.</param>
        public FileSourceOperator(string filePath, FileFormat fileFormat, IDeserializer<TOutput> deserializer = null)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            _fileFormat = fileFormat;
            _deserializer = deserializer;

            if (_fileFormat == FileFormat.CSV && _deserializer == null)
            {
                _csvDeserializer = new DefaultCsvDeserializer<TOutput>();
            }
        }

        /// <summary>
        /// Starts reading the file and emitting data into the stream.
        /// </summary>
        /// <param name="emit">Action to emit data into the stream.</param>
        public void Start(Action<TOutput> emit)
        {
            if (emit == null)
                throw new ArgumentNullException(nameof(emit));

            lock (_lock)
            {
                if (_cts != null)
                    throw new InvalidOperationException("FileSourceOperator is already running.");

                _cts = new CancellationTokenSource();
                _readingTask = Task.Run(() => ReadFileAsync(emit, _cts.Token), _cts.Token);
            }
        }

        /// <summary>
        /// Stops reading the file.
        /// </summary>
        public void Stop()
        {
            lock (_lock)
            {
                if (_cts == null)
                    return;

                _cts.Cancel();
                try
                {
                    _readingTask.Wait();
                }
                catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is TaskCanceledException))
                {
                    // Expected when task is canceled.
                }
                finally
                {
                    _cts.Dispose();
                    _cts = null;
                }
            }
        }

        private async Task ReadFileAsync(Action<TOutput> emit, CancellationToken cancellationToken)
        {
            try
            {
                using var reader = new StreamReader(_filePath);
                string line;
                bool isFirstLine = true;

                while ((line = await reader.ReadLineAsync()) != null)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (_fileFormat == FileFormat.CSV)
                    {
                        if (isFirstLine)
                        {
                            _csvDeserializer?.InitializeHeaders(line);
                            isFirstLine = false;
                            continue; // Skip header line
                        }

                        var record = _deserializer != null ? _deserializer.Deserialize(line) : _csvDeserializer!.Deserialize(line);
                        emit(record);
                    }
                    else if (_fileFormat == FileFormat.Jsonl)
                    {
                        var record = _deserializer != null ? _deserializer.Deserialize(line) : new DefaultJsonDeserializer<TOutput>().Deserialize(line);
                        emit(record);
                    }
                    else if (_fileFormat == FileFormat.XML)
                    {
                        // For XML, assuming each line is a separate XML element representing TOutput
                        var record = _deserializer != null ? _deserializer.Deserialize(line) : new DefaultXmlDeserializer<TOutput>().Deserialize(line);
                        emit(record);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Gracefully handle cancellation
            }
            catch (Exception ex)
            {
                // Log or handle exceptions as needed
                Console.WriteLine($"Error in FileSourceOperator: {ex.Message}");
                throw;
            }
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
