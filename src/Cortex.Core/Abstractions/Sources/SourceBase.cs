namespace Cortex.Core.Sources
{
    public class SourceBase<TIn> : ISourceDesigner<TIn>
    {
        // HERE is the error, research how to store MAPPING
        List<Func<TIn, object>> test;

        internal SourceBase()
        {

        }

        public Designers.IMapperDesigner<TOut> Map<TOut>(Func<TIn, TOut> func)
        {
            throw new NotImplementedException();
        }
    }
}

