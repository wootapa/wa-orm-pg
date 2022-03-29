using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using wa.Orm.Pg.Reflection;

namespace wa.Orm.Pg
{
    /// <summary>
    /// Internal extension for commands
    /// </summary>
    internal static class CommandExtension
    {
        /// <summary>
        /// Add parameter to a command
        /// </summary>
        /// <param name="cmd">Command to add parameter to</param>
        /// <param name="key">Key of the parameter</param>
        /// <param name="value">Value of the parameter</param>
        public static void ApplyParameter(this DbCommand cmd, string key, object value)
        {
            IDbDataParameter parameter = cmd.CreateParameter();
            parameter.ParameterName = key;
            parameter.Value = value;
            cmd.Parameters.Add(parameter);
        }

        /// <summary>
        /// Add parameters to a command
        /// </summary>
        /// <param name="cmd">Command to add parameters to</param>
        /// <param name="args">Object that holds the parameters</param>
        public static void ApplyParameters(this DbCommand cmd, object args = null)
        {
            if (args == null) return;

            var td = TypeHandler.Get(args);

            foreach (var property in td.Arguments)
            {
                var value = td.GetValue(property.Property.Name, args);
                cmd.ApplyParameter(property.Property.Name, value ?? DBNull.Value);
            }
        }

        /// <summary>
        /// Add parameters to an indexed bulk command
        /// </summary>
        /// <param name="cmd">Command to add parameters to</param>
        /// <param name="argsList">List of objects that holds the parameters</param>
        public static void ApplyParameters(this DbCommand cmd, IEnumerable<object> argsList = null)
        {
            if (argsList == null) return;

            var td = TypeHandler.Get(argsList.First());

            int i = 0;
            foreach (var args in argsList)
            {
                foreach (var property in td.Arguments)
                {
                    var value = td.GetValue(property.Property.Name, args);
                    cmd.ApplyParameter(property.Property.Name + i, value ?? DBNull.Value);
                }
                i++;
            }
        }
    }
}
