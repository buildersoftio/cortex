using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.States.ClickHouse
{
    internal class ClickHousePropertyConverter
    {
        public string ConvertToString(object value)
        {
            if (value == null) return null;
            return value.ToString();
        }

        public object ConvertFromString(Type type, string str)
        {
            if (str == null) return null;

            if (type == typeof(string)) return str;

            // Basic numeric and other conversions
            if (type == typeof(int) || type == typeof(int?))
            {
                if (int.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var i)) return i;
                return type == typeof(int) ? 0 : (int?)null;
            }
            if (type == typeof(long) || type == typeof(long?))
            {
                if (long.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var l)) return l;
                return type == typeof(long) ? 0L : (long?)null;
            }
            if (type == typeof(double) || type == typeof(double?))
            {
                if (double.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return d;
                return type == typeof(double) ? 0.0 : (double?)null;
            }
            if (type == typeof(float) || type == typeof(float?))
            {
                if (float.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var f)) return f;
                return type == typeof(float) ? 0f : (float?)null;
            }
            if (type == typeof(bool) || type == typeof(bool?))
            {
                if (bool.TryParse(str, out var b)) return b;
                return type == typeof(bool) ? false : (bool?)null;
            }
            if (type == typeof(DateTime) || type == typeof(DateTime?))
            {
                if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt)) return dt;
                return type == typeof(DateTime) ? DateTime.MinValue : (DateTime?)null;
            }
            if (type == typeof(Guid) || type == typeof(Guid?))
            {
                if (Guid.TryParse(str, out var g)) return g;
                return type == typeof(Guid) ? Guid.Empty : (Guid?)null;
            }
            if (type == typeof(decimal) || type == typeof(decimal?))
            {
                if (decimal.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var dec)) return dec;
                return type == typeof(decimal) ? 0m : (decimal?)null;
            }
            if (type == typeof(TimeSpan) || type == typeof(TimeSpan?))
            {
                if (TimeSpan.TryParse(str, CultureInfo.InvariantCulture, out var ts)) return ts;
                return type == typeof(TimeSpan) ? TimeSpan.Zero : (TimeSpan?)null;
            }

            // If unknown type, just return the string
            return str;
        }
    }
}
