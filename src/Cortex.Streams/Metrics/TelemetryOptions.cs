using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using System;
using System.Collections.Generic;

namespace Cortex.Streams.Metrics
{
    public class TelemetryOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether telemetry is enabled.
        /// </summary>
        public bool Enabled { get; set; } = false;

        /// <summary>
        /// Gets or sets the resource builder for telemetry data.
        /// </summary>
        public ResourceBuilder ResourceBuilder { get; set; }

        /// <summary>
        /// Gets or sets a list of configurations for the MeterProviderBuilder.
        /// </summary>
        public List<Action<MeterProviderBuilder>> MeterProviderConfigurations { get; set; } = new List<Action<MeterProviderBuilder>>();
    }
}
