using System.Collections.Generic;
using System;

namespace Cortex.Telemetry
{
    public interface ITracingProvider
    {
        ITracer GetTracer(string name);
    }

    public interface ITracer
    {
        ISpan StartSpan(string name);
    }

    public interface ISpan : IDisposable
    {
        void SetAttribute(string key, string value);
        void AddEvent(string name, IDictionary<string, object> attributes = null);
    }
}
