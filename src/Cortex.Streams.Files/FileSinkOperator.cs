using Cortex.Streams.Files.Serializers;
using Cortex.Streams.Operators;
using System;
using System.IO;

namespace Cortex.Streams.Files
{
    public class FileSinkOperator<TInput> : ISinkOperator<TInput>, IDisposable where TInput : new()
    {
        private readonly string _outputDirectory;
        private readonly FileSinkMode _sinkMode;
        private readonly ISerializer<TInput> _serializer;
        private readonly string _singleFilePath;
        private StreamWriter _singleFileWriter;
        private readonly object _lock = new object();
        private bool _isRunning = false;

        /// <summary>
        /// Initializes a new instance of FileSinkOperator.
        /// </summary>
        /// <param name="outputDirectory">Directory where output files will be written.</param>
        /// <param name="sinkMode">Mode of sinking: SingleFile or MultiFile.</param>
        /// <param name="serializer">Custom serializer. If null, default serializers are used based on file format.</param>
        /// <param name="singleFileName">Name of the single file (required if sinkMode is SingleFile).</param>
        public FileSinkOperator(
            string outputDirectory,
            FileSinkMode sinkMode,
            ISerializer<TInput> serializer = null,
            string singleFileName = "output.txt")
        {
            _outputDirectory = outputDirectory ?? throw new ArgumentNullException(nameof(outputDirectory));
            _sinkMode = sinkMode;
            _serializer = serializer;
            Directory.CreateDirectory(_outputDirectory);

            if (_sinkMode == FileSinkMode.SingleFile)
            {
                _singleFilePath = Path.Combine(_outputDirectory, singleFileName);

                lock (_lock)
                {
                    if (_isRunning)
                        throw new InvalidOperationException("FileSinkOperator is already running.");

                    if (_sinkMode == FileSinkMode.SingleFile)
                    {
                        // check if file exists; delete the file
                        if (File.Exists(_singleFilePath))
                            File.Delete(_singleFilePath);

                        _singleFileWriter = new StreamWriter(new FileStream(_singleFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
                        {
                            AutoFlush = true
                        };
                    }
                }
                _isRunning = true;
            }
            _isRunning = true;
        }

        /// <summary>
        /// Starts the sink operator.
        /// </summary>
        public void Start()
        {
            // this sink runs immediately
        }

        /// <summary>
        /// Processes the input data by writing it to the appropriate file(s).
        /// </summary>
        /// <param name="input">The data to write.</param>
        public void Process(TInput input)
        {
            if (!_isRunning)
                throw new InvalidOperationException("FileSinkOperator is not running. Call Start() before processing data.");

            string serializedData = _serializer != null ? _serializer.Serialize(input) : DefaultSerializer(input);

            if (_sinkMode == FileSinkMode.SingleFile)
            {
                lock (_lock)
                {
                    _singleFileWriter.WriteLine(serializedData);
                }
            }
            else if (_sinkMode == FileSinkMode.MultiFile)
            {
                string fileName = $"{Guid.NewGuid()}.txt";
                string filePath = Path.Combine(_outputDirectory, fileName);
                try
                {
                    File.WriteAllText(filePath, serializedData);
                }
                catch (Exception ex)
                {
                    // Log or handle exceptions as needed
                    Console.WriteLine($"Error writing to file {filePath}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Stops the sink operator and closes any open files.
        /// </summary>
        public void Stop()
        {
            lock (_lock)
            {
                if (!_isRunning)
                    return;

                if (_sinkMode == FileSinkMode.SingleFile && _singleFileWriter != null)
                {
                    _singleFileWriter.Flush();
                    _singleFileWriter.Close();
                    _singleFileWriter.Dispose();
                    _singleFileWriter = null;
                }

                _isRunning = false;
            }
        }

        /// <summary>
        /// Default serializer that converts the input to its string representation.
        /// </summary>
        /// <param name="input">The data to serialize.</param>
        /// <returns>String representation of the input.</returns>
        private string DefaultSerializer(TInput input)
        {
            return input?.ToString() ?? string.Empty;
        }

        public void Dispose()
        {
            Stop();
        }

    }
}
