using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace wa.Orm.Pg.Reflection;

public class TypeDescriber : List<PropertyDescriber>
{
    public Type Type { get; }
    public string Table { get; }
    private readonly Hashtable properties = new();

    public TypeDescriber(Type type)
    {
        Type = type;
        Table = Type.GetTypeInfo().GetCustomAttribute<TableAttribute>()?.Name ?? Util.ToUnderscore(Type.Name);

        var props = Type.GetProperties();

        foreach (var property in props.Where(x => x.CanRead || x.CanWrite))
        {
            Add(new PropertyDescriber(property));
        }

        foreach (var property in props)
        {
            var prop = new PropertyDescriber(property);
            this.properties[prop.Property.Name] = prop.Property;
            this.properties[prop.Column] = prop.Property;
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

    public IEnumerable<PropertyDescriber> Writable
    {
        get { return this.Where(x => x.IsWriteable); }
    }

    public IEnumerable<PropertyDescriber> Arguments
    {
        get { return this.Where(x => x.IsArgument); }
    }

    public IEnumerable<PropertyDescriber> Generated
    {
        get { return this.Where(x => x.IsGenerated); }
    }

    public void SetValue(string propertyName, object obj, object value)
    {
        var prop = (PropertyInfo)properties[propertyName];

        if (prop == null) return;
        if (prop.PropertyType.IsEnum)
            prop.SetValue(obj, Enum.Parse(prop.PropertyType, value.ToString()), null);
        else
            prop.SetValue(obj, value is DBNull ? null : value, null);
    }

    public object GetValue(string propertyName, object obj)
    {
        var prop = (PropertyInfo)properties[propertyName];
        return prop != null ? prop.GetValue(obj, null) : null;
    }
}