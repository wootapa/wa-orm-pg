using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace wa.Orm.Pg.Reflection;

public class PropertyDescriber
{
    public PropertyInfo Property { get; set; }
    public string Column { get; set; }
    public bool IsKey { get; private set; }
    public bool IsWriteable { get; private set; }
    public bool IsReadable { get; private set; }
    public bool IsArgument { get; private set; }
    public bool IsGenerated { get; private set; }
    public bool IsStringEnum { get; private set; }

    public PropertyDescriber(PropertyInfo info)
    {
        Property = info;
        Column = info.GetCustomAttribute<ColumnAttribute>()?.Name ?? Util.ToUnderscore(info.Name);
        IsKey = info.GetCustomAttribute<KeyAttribute>() != null;
        IsWriteable = Property.CanWrite && info.GetCustomAttribute<GeneratedAttribute>() == null;
        IsReadable = Property.CanRead;
        IsArgument = Property.CanRead;
        IsGenerated = info.GetCustomAttribute<GeneratedAttribute>() != null;
        IsStringEnum = info.GetCustomAttribute<StringEnumAttribute>() != null;
    }
}