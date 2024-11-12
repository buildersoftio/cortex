using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams
{
    public class SourceStreamBuilder<TOut> : ISourceStreamBuilder<TOut>
    {
        private readonly string _name;
        private IOperator _firstOperator;
        private IOperator _lastOperator;

        public SourceStreamBuilder(string name, ISourceOperator<TOut> sourceOperator)
        {
            _name = name;
            _firstOperator = new SourceOperatorAdapter<TOut>(sourceOperator);
            _lastOperator = _firstOperator;
        }

        public ISinkBuilder<TOut> Sink(Action<TOut> sinkFunction)
        {
            var sinkOperator = new SinkOperator<TOut>(sinkFunction);
            _lastOperator.SetNext(sinkOperator);
            _lastOperator = sinkOperator;
            return new SinkBuilder<TOut>(_name, _firstOperator);
        }

        public ISinkBuilder<TOut> ToSink(ISinkOperator<TOut> sinkOperator)
        {
            var sinkAdapter = new SinkOperatorAdapter<TOut>(sinkOperator);
            _lastOperator.SetNext(sinkAdapter);
            _lastOperator = sinkAdapter;
            return new SinkBuilder<TOut>(_name, _firstOperator);
        }

        public Stream<TOut> Build()
        {
            return new Stream<TOut>(_name, _firstOperator);
        }

        IStreamBuilder<TOut, TOut> ISourceStreamBuilder<TOut>.Map<TNext>(Func<TOut, TNext> mapFunction)
        {
            var mapOperator = new MapOperator<TOut, TNext>(mapFunction);
            _lastOperator.SetNext(mapOperator);
            _lastOperator = mapOperator;

            return StreamBuilder<TOut, TOut>.CreateNewStream(_name, _firstOperator, _lastOperator);
        }
    }
}