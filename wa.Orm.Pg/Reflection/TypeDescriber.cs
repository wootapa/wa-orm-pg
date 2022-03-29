using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace wa.Orm.Pg.Reflection
{
    public class TypeDescriber : List<PropertyDescriber>
    {
        public Type Type { get; private set; }
        public string Table { get; private set; }
        private Hashtable _properties = new Hashtable();

        public TypeDescriber(Type type)
        {
            Type = type;
            Table = Type.GetTypeInfo().GetCustomAttribute<TableAttribute>()?.Name ?? Type.Name.ToLower();

            var properties = Type.GetProperties();

            foreach (var property in properties.Where(x => (x.CanRead || x.CanWrite)))
            {
                Add(new PropertyDescriber(property));
            }

            foreach (var property in properties)
            {
                var prop = new PropertyDescriber(property);
                _properties[prop.Property.Name] = prop.Property;
                _properties[prop.DbName] = prop.Property;
            }
        }

        public IEnumerable<PropertyDescriber> Keys
        {
            get { return this.Where(x => x.IsKey); }
        }

        public IEnumerable<PropertyDescriber> NonKeys
        {
            get { return this.Where(x => !x.IsKey); }
        }

        public IEnumerable<PropertyDescriber> WriteableProperties
        {
            get { return this.Where(x => x.IsWriteable); }
        }

        public IEnumerable<PropertyDescriber> WriteableColumns
        {
            get { return this.Where(x => x.IsReadable && !x.IsGenerated); }
        }

        public IEnumerable<PropertyDescriber> Arguments
        {
            get { return this.Where(x => x.IsArgument); }
        }

        public IEnumerable<PropertyDescriber> Generated
        {
            get { return this.Where(x => x.IsGenerated); }
        }

        private Hashtable setters = new Hashtable();
        private Hashtable getters = new Hashtable();

        public void SetValue(string propertyName, object obj, object value)
        {
            PropertyInfo prop = (PropertyInfo)_properties[propertyName];

            if (prop != null)
            {
                if (prop.PropertyType.IsEnum)
                    prop.SetValue(obj, Enum.Parse(prop.PropertyType, value.ToString()), null);
                else
                    prop.SetValue(obj, value is DBNull ? null : value, null);
            }
        }

        public object GetValue(string propertyName, object obj)
        {
            PropertyInfo prop = (PropertyInfo)_properties[propertyName];

            if (prop != null)
                return prop.GetValue(obj, null);

            return null;
        }
    }
}
