using System;
using System.Data.Common;

namespace wa.Orm.Pg;

/// <summary>
/// Internal extension for DbDataReader
/// </summary>
internal static class DbDataReaderExtension
{
    /// <summary>
    /// Returns value at column index. 
    /// If value is DBNull then null is returned.
    /// </summary>
    /// <param name="this">The reader</param>
    /// <param name="ordinal">Column index</param>
    /// <returns></returns>
    public static object GetValueWithNull(this DbDataReader @this, int ordinal)
    {
        var value = @this.GetValue(ordinal);
        return value is DBNull ? null : value;
    }
}