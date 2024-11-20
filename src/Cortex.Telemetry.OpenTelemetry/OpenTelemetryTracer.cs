using OpenTelemetry.Trace;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Telemetry.OpenTelemetry
{
    public class OpenTelemetryTracer : ITracer
    {
        private readonly Tracer _tracer;

        public OpenTelemetryTracer(Tracer tracer)
        {
            _tracer = tracer;
        }

        public ISpan StartSpan(string name)
        {
            var span = _tracer.StartSpan(name);
            return new OpenTelemetrySpan(span);
        }
    }

    public class OpenTelemetrySpan : ISpan
    {
        private readonly TelemetrySpan _span;

        public OpenTelemetrySpan(TelemetrySpan span)
        {
            _span = span;
        }

        public void SetAttribute(string key, string value)
        {
            _span.SetAttribute(key, value);
        }

        public void AddEvent(string name, IDictionary<string, object> attributes = null)
        {
            if (attributes != null)
            {
                // Convert IDictionary<string, object> to IEnumerable<KeyValuePair<string, object?>>
                var attributesWithNullableValues = attributes.Select(
                    kvp => new KeyValuePair<string, object?>(kvp.Key, kvp.Value));

                // Add the event with the converted attributes
                _span.AddEvent(name, new SpanAttributes(attributesWithNullableValues));
            }
            else
            {
                // Add the event without attributes
                _span.AddEvent(name);
            }
        }

        public void Dispose()
        {
            _span.End();
        }
    }
}
