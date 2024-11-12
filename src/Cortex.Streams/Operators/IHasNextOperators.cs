using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Streams.Operators
{
    public interface IHasNextOperators
    {
        IEnumerable<IOperator> GetNextOperators();
    }
}
