using System.Reflection;

namespace Cortex.States.PostgreSQL
{
    internal class ListPropertyMetadata
    {
        public PropertyInfo Property { get; set; }
        public PropertyInfo[] ChildScalarProperties { get; set; }
        public Type ChildItemType { get; set; }
        public string TableName { get; set; }
    }
}
