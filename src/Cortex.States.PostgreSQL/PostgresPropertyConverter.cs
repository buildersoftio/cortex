using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.States.PostgreSQL
{
    internal class PostgresPropertyConverter
    {
        public string ConvertToString(object value)
        {
            if (value == null) return null;
            return value.ToString();
        }

        public object ConvertFromString(Type type, string str)
        {
            if (str == null) return null;

            var underlying = Nullable.GetUnderlyingType(type) ?? type;

            if (underlying == typeof(string)) return str;
            if (underlying == typeof(int))
            {
                if (int.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var i)) return i;
                return 0;
            }
            if (underlying == typeof(long))
            {
                if (long.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var l)) return l;
                return 0L;
            }
            if (underlying == typeof(bool))
            {
                if (bool.TryParse(str, out var b)) return b;
                return false;
            }
            if (underlying == typeof(DateTime))
            {
                if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt)) return dt;
                return DateTime.MinValue;
            }
            if (underlying == typeof(decimal))
            {
                if (decimal.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var dec)) return dec;
                return 0m;
            }
            if (underlying == typeof(double))
            {
                if (double.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return d;
                return 0.0;
            }
            if (underlying == typeof(float))
            {
                if (float.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var f)) return f;
                return 0f;
            }
            if (underlying == typeof(Guid))
            {
                if (Guid.TryParse(str, out var g)) return g;
                return Guid.Empty;
            }
            if (underlying == typeof(TimeSpan))
            {
                // Attempt parse
                if (TimeSpan.TryParse(str, CultureInfo.InvariantCulture, out var ts)) return ts;
                return TimeSpan.Zero;
            }

            // default fallback
            return str;
        }
    }
}
