using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using wa.Orm.Pg.Reflection;

namespace wa.Orm.Pg;

/// <summary>
/// Internal extension for commands
/// </summary>
internal static class CommandExtension
{
    /// <summary>
    /// Add parameter to a command
    /// </summary>
    /// <param name="this">Command to add parameter to</param>
    /// <param name="key">Key of the parameter</param>
    /// <param name="value">Value of the parameter</param>
    /// <param name="stringEnum">Handle enum as a string parameter</param>
    public static void ApplyParameter(this DbCommand @this, string key, object value, bool stringEnum = false)
    {
        IDbDataParameter parameter = @this.CreateParameter();
        parameter.ParameterName = key;
        parameter.Value = value is Enum && stringEnum ? value.ToString() : value;
        @this.Parameters.Add(parameter);
    }

    /// <summary>
    /// Add parameters to a command
    /// </summary>
    /// <param name="this">Command to add parameters to</param>
    /// <param name="args">Object that holds the parameters</param>
    public static void ApplyParameters(this DbCommand @this, object args = null)
    {
        if (args == null)
        {
            return;
        }

        var td = TypeHandler.Get(args);

        foreach (var property in td.Arguments)
        {
            var value = td.GetValue(property.Property.Name, args);
            @this.ApplyParameter(property.Property.Name, value ?? DBNull.Value, property.IsStringEnum);
        }
    }

    /// <summary>
    /// Add parameters to an indexed bulk command
    /// </summary>
    /// <param name="this">Command to add parameters to</param>
    /// <param name="argsList">List of objects that holds the parameters</param>
    public static void ApplyParameters(this DbCommand @this, IEnumerable<object> argsList = null)
    {
        if (argsList == null)
        {
            return;
        }

        var argsListArray = argsList as object[] ?? argsList.ToArray();
        var td = TypeHandler.Get(argsListArray.First());

        var i = 0;
        foreach (var args in argsListArray)
        {
            foreach (var property in td.Arguments)
            {
                var value = td.GetValue(property.Property.Name, args);
                @this.ApplyParameter(property.Property.Name + i, value ?? DBNull.Value, property.IsStringEnum);
            }

            i++;
        }
    }
}