using Cortex.States;
using Cortex.Streams.Operators;
using System.Collections.Generic;

namespace Cortex.Streams
{
    public interface IStream<TIn, TCurrent>
    {
        void Start();
        void Stop();
        void Emit(TIn value);
        string GetStatus();
        IReadOnlyDictionary<string, BranchOperator<TCurrent>> GetBranches();

        TStateStore GetStateStoreByName<TStateStore>(string name) where TStateStore : IDataStore;
        IEnumerable<TStateStore> GetStateStoresByType<TStateStore>() where TStateStore : IDataStore;
    }
}
