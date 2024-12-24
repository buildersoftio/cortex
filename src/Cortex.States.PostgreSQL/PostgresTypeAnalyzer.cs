using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.States.PostgreSQL
{
    internal class PostgresTypeAnalyzer
    {
        public PropertyInfo[] ScalarProperties { get; private set; }
        public List<ListPropertyMetadata> ListProperties { get; private set; }

        // For List<T> scenario
        public bool IsListType { get; private set; }
        public Type ChildItemType { get; private set; }
        public PropertyInfo[] ChildScalarProperties { get; private set; }

        private readonly Type _valueType;
        private readonly string _baseTableName;

        public PostgresTypeAnalyzer(Type valueType, string baseTableName)
        {
            _valueType = valueType;
            _baseTableName = baseTableName;
            AnalyzeType();
        }

        private void AnalyzeType()
        {
            if (IsGenericList(_valueType, out Type itemType))
            {
                // Value is a List<TChild>
                IsListType = true;
                ChildItemType = itemType;
                ChildScalarProperties = GetScalarProperties(itemType);
                ScalarProperties = new PropertyInfo[0];
                ListProperties = new List<ListPropertyMetadata>();
            }
            else
            {
                // Normal object scenario
                IsListType = false;
                var props = _valueType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0)
                    .ToArray();

                var scalarProps = new List<PropertyInfo>();
                var listProps = new List<ListPropertyMetadata>();

                foreach (var prop in props)
                {
                    if (IsListProperty(prop, out Type childItemType))
                    {
                        var childScalars = GetScalarProperties(childItemType);
                        listProps.Add(new ListPropertyMetadata
                        {
                            Property = prop,
                            ChildScalarProperties = childScalars,
                            ChildItemType = childItemType,
                            TableName = $"{_baseTableName}_{prop.Name}"
                        });
                    }
                    else
                    {
                        scalarProps.Add(prop);
                    }
                }

                ScalarProperties = scalarProps.ToArray();
                ListProperties = listProps;
            }
        }

        private bool IsListProperty(PropertyInfo prop, out Type itemType)
        {
            itemType = null;
            if (!prop.PropertyType.IsGenericType) return false;
            var genType = prop.PropertyType.GetGenericTypeDefinition();
            if (genType == typeof(List<>))
            {
                itemType = prop.PropertyType.GetGenericArguments()[0];
                return true;
            }
            return false;
        }

        private bool IsGenericList(Type type, out Type itemType)
        {
            itemType = null;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
            {
                itemType = type.GetGenericArguments()[0];
                return true;
            }
            return false;
        }

        private PropertyInfo[] GetScalarProperties(Type t)
        {
            return t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0 && !IsListProperty(p, out _))
                    .ToArray();
        }
    }
}
