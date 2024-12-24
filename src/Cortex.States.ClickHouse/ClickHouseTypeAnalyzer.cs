using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.States.ClickHouse
{
    internal class ClickHouseTypeAnalyzer
    {
        public PropertyInfo[] ScalarProperties { get; private set; }
        public PropertyInfo[] ListProperties { get; private set; }

        private readonly Type _valueType;

        public ClickHouseTypeAnalyzer(Type valueType)
        {
            _valueType = valueType;
            AnalyzeType();
        }

        private void AnalyzeType()
        {
            // Retrieve all top-level properties
            var props = _valueType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0)
                .ToArray();

            var scalarProps = new List<PropertyInfo>();
            var listProps = new List<PropertyInfo>();

            foreach (var prop in props)
            {
                if (IsGenericList(prop.PropertyType))
                    listProps.Add(prop);
                else
                    scalarProps.Add(prop);
            }

            ScalarProperties = scalarProps.ToArray();
            ListProperties = listProps.ToArray();
        }

        private bool IsGenericList(Type type)
        {
            if (!type.IsGenericType) return false;
            return type.GetGenericTypeDefinition() == typeof(List<>);
        }
    }
}
