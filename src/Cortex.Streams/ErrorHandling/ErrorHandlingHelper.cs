using System;
using System.Threading;

namespace Cortex.Streams.ErrorHandling
{
    /// <summary>
    /// Helper class for executing operations with error handling support.
    /// Provides retry, skip, and stop functionality based on stream execution options.
    /// </summary>
    public static class ErrorHandlingHelper
    {
        public static bool TryExecute<TInput>(
            StreamExecutionOptions options,
            string operatorName,
            object rawInput,
            Func<TInput> action)
        {
            if (options == null ||
                (options.ErrorHandlingStrategy == ErrorHandlingStrategy.None && options.OnError == null))
            {
                // Fast-path: no global error handling configured
                action();
                return true;
            }

            var attempt = 0;

            while (true)
            {
                try
                {
                    action();
                    return true;
                }
                catch (StreamStoppedException)
                {
                    // A downstream operator already decided to stop the stream; never retry or re-decide it.
                    throw;
                }
                catch (Exception ex)
                {
                    attempt++;
                    var decision = ResolveDecision(options, operatorName, rawInput, ex, attempt);

                    switch (decision)
                    {
                        case ErrorHandlingDecision.Skip:
                            return false;

                        case ErrorHandlingDecision.Retry:
                            if (attempt >= options.MaxRetries)
                                throw new StreamStoppedException(
                                    $"Maximum retry attempts ({options.MaxRetries}) exceeded in operator '{operatorName}'.", ex);

                            if (options.RetryDelay > TimeSpan.Zero)
                                Thread.Sleep(options.RetryDelay);
                            break; // retry

                        case ErrorHandlingDecision.Stop:
                            throw new StreamStoppedException(
                                $"Stream '{options.StreamName}' stopped by error handling strategy in operator '{operatorName}'.", ex);

                        case ErrorHandlingDecision.Rethrow:
                        default:
                            throw;
                    }
                }
            }
        }

        public static bool TryExecute<TInput>(
            StreamExecutionOptions options,
            string operatorName,
            object rawInput,
            Action<TInput> action)
        {
            if (options == null ||
                (options.ErrorHandlingStrategy == ErrorHandlingStrategy.None && options.OnError == null))
            {
                // Fast-path: no global error handling configured
                action((TInput)rawInput);
                return true;
            }

            var attempt = 0;

            while (true)
            {
                try
                {
                    action((TInput)rawInput);
                    return true;
                }
                catch (StreamStoppedException)
                {
                    // A downstream operator already decided to stop the stream; never retry or re-decide it.
                    throw;
                }
                catch (Exception ex)
                {
                    attempt++;
                    var decision = ResolveDecision(options, operatorName, rawInput, ex, attempt);

                    switch (decision)
                    {
                        case ErrorHandlingDecision.Skip:
                            return false;

                        case ErrorHandlingDecision.Retry:
                            if (attempt >= options.MaxRetries)
                                throw new StreamStoppedException(
                                    $"Maximum retry attempts ({options.MaxRetries}) exceeded in operator '{operatorName}'.", ex);

                            if (options.RetryDelay > TimeSpan.Zero)
                                Thread.Sleep(options.RetryDelay);
                            break; // retry

                        case ErrorHandlingDecision.Stop:
                            throw new StreamStoppedException(
                                $"Stream '{options.StreamName}' stopped by error handling strategy in operator '{operatorName}'.", ex);

                        case ErrorHandlingDecision.Rethrow:
                        default:
                            throw;
                    }
                }
            }
        }

        public static bool TryExecute<TInput, TOutput>(
            StreamExecutionOptions options,
            string operatorName,
            object rawInput,
            Func<TInput, TOutput> action,
            TInput typedInput,
            out TOutput output)
        {
            output = default;

            if (options == null ||
                (options.ErrorHandlingStrategy == ErrorHandlingStrategy.None && options.OnError == null))
            {
                output = action(typedInput);
                return true;
            }

            var attempt = 0;

            while (true)
            {
                try
                {
                    output = action(typedInput);
                    return true;
                }
                catch (StreamStoppedException)
                {
                    // A downstream operator already decided to stop the stream; never retry or re-decide it.
                    throw;
                }
                catch (Exception ex)
                {
                    attempt++;
                    var decision = ResolveDecision(options, operatorName, rawInput, ex, attempt);

                    switch (decision)
                    {
                        case ErrorHandlingDecision.Skip:
                            return false;

                        case ErrorHandlingDecision.Retry:
                            if (attempt >= options.MaxRetries)
                                throw new StreamStoppedException(
                                    $"Maximum retry attempts ({options.MaxRetries}) exceeded in operator '{operatorName}'.", ex);

                            if (options.RetryDelay > TimeSpan.Zero)
                                Thread.Sleep(options.RetryDelay);
                            break; // retry

                        case ErrorHandlingDecision.Stop:
                            throw new StreamStoppedException(
                                $"Stream '{options.StreamName}' stopped by error handling strategy in operator '{operatorName}'.", ex);

                        case ErrorHandlingDecision.Rethrow:
                        default:
                            throw;
                    }
                }
            }
        }

        private static ErrorHandlingDecision ResolveDecision(
            StreamExecutionOptions options,
            string operatorName,
            object rawInput,
            Exception ex,
            int attempt)
        {
            var ctx = new StreamErrorContext(
                options.StreamName,
                operatorName,
                rawInput,
                ex,
                attempt);

            if (options.OnError != null)
                return options.OnError(ctx);

            // fall back to global strategy
            switch (options.ErrorHandlingStrategy)
            {
                case ErrorHandlingStrategy.Skip:
                    return ErrorHandlingDecision.Skip;
                case ErrorHandlingStrategy.Retry:
                    return ErrorHandlingDecision.Retry;
                case ErrorHandlingStrategy.Stop:
                    return ErrorHandlingDecision.Stop;
                default:
                    return ErrorHandlingDecision.Rethrow;
            }
        }
    }
}
