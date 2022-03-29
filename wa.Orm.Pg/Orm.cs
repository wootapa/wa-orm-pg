using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using wa.Orm.Pg.Reflection;

namespace wa.Orm.Pg
{
    /// <summary>
    /// Extensions for objects to insert, update, delete etc.
    /// </summary>
    public static class OrmExtension
    {
        /// <summary>
        /// Get a object of T by its ID
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="ids">Id(s) in same order as marked with KeyAttribute</param>
        /// <returns>T</returns>
        public static T Get<T>(this DbConnection conn, params object[] ids)
            where T : class, new()
        {
            TypeDescriber td = TypeHandler.Get<T>();
            PropertyDescriber[] keys = td.Keys.ToArray();

            if (keys.Length == 0)
            {
                throw new ArgumentException("T must be a type with properties decorated with KeyAttribute");
            }
            else if (keys.Length != ids.Length)
            {
                throw new ArgumentException(string.Format("KeyAttribute-count ({0}) and argument-count ({1}) must match", keys.Length, ids.Length));
            }
            else
            {
                string where = string.Join(" AND ", keys.Select(x => x.DbName + "=@" + x.Property.Name));
                string sql = $"SELECT * FROM {td.Table} WHERE {where}";

                DbCommand cmd = conn.Prepare(sql);
                for (var i = 0; i < ids.Length; i++)
                {
                    cmd.ApplyParameter(keys[i].Property.Name, ids[i]);
                }

                return conn.Query<T>(cmd).FirstOrDefault();
            }
        }


        /// <summary>
        /// Get a object of T by its ID
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="ids">Id(s) in same order as marked with KeyAttribute</param>
        /// <returns>T</returns>
        public static Task<T> GetAsync<T>(this DbConnection conn, params object[] ids)
            where T : class, new()
        {
            return GetAsync<T>(conn, CancellationToken.None, ids);
        }

        /// <summary>
        /// Get a object of T by its ID
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <param name="ids">Id(s) in same order as marked with KeyAttribute</param>
        /// <returns>T</returns>
        public static async Task<T> GetAsync<T>(this DbConnection conn, CancellationToken cancellationToken, params object[] ids)
            where T : class, new()
        {
            TypeDescriber td = TypeHandler.Get<T>();
            PropertyDescriber[] keys = td.Keys.ToArray();

            if (keys.Length == 0)
            {
                throw new ArgumentException("T must be a type with properties decorated with KeyAttribute");
            }
            else if (keys.Length != ids.Length)
            {
                throw new ArgumentException(string.Format("KeyAttribute-count ({0}) and argument-count ({1}) must match", keys.Length, ids.Length));
            }
            else
            {
                string where = string.Join(",", keys.Select(x => x.DbName + "=@" + x.Property.Name));
                string sql = $"SELECT * FROM {td.Table} WHERE {where}";

                DbCommand cmd = await conn.PrepareAsync(sql, cancellationToken: cancellationToken).ConfigureAwait(false);
                for (var i = 0; i < ids.Length; i++)
                {
                    cmd.ApplyParameter(keys[i].Property.Name, ids[i]);
                }

                return (await conn.QueryAsync<T>(cmd, cancellationToken).FirstOrDefaultAsync().ConfigureAwait(false));
            }
        }

        /// <summary>
        /// Insert object in table. Will populate generated fields with values from database.
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Insert<T>(this DbConnection conn, T data, DbTransaction transaction = null)
            where T : class
        {
            TypeDescriber td = TypeHandler.Get<T>();
            IEnumerable<PropertyDescriber> generated = td.Generated;

            if (generated.Count() > 0)
            {
                string columns = td.WriteableColumns.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
                string values = td.WriteableColumns.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);
                string returns = string.Join(",", generated.Select(x => x.DbName));

                string sql = $"INSERT INTO {td.Table} ({columns}) VALUES ({values}) RETURNING {returns}";

                var result = conn.QueryAssoc(sql, data, transaction).FirstOrDefault();

                foreach (var prop in generated)
                {
                    td.SetValue(prop.Property.Name, data, result[prop.DbName]);
                }

                return 1;
            }
            else
            {
                return conn.Insert(td.Table, data, transaction);
            }
        }

        /// <summary>
        /// Insert object in table. Will populate generated fields with values from database.
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> InsertAsync<T>(this DbConnection conn, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where T : class
        {
            TypeDescriber td = TypeHandler.Get<T>();
            IEnumerable<PropertyDescriber> generated = td.Generated;

            if (generated.Count() > 0)
            {
                string columns = td.WriteableColumns.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
                string values = td.WriteableColumns.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);
                string returns = string.Join(",", generated.Select(x => x.DbName));

                string sql = $"INSERT INTO {td.Table} ({columns}) VALUES ({values}) RETURNING {returns}";

                var result = (await conn.QueryAssocAsync(sql, data, transaction, cancellationToken).FirstOrDefaultAsync().ConfigureAwait(false));

                foreach (var prop in generated)
                {
                    td.SetValue(prop.Property.Name, data, result[prop.DbName]);
                }

                return 1;
            }
            else
            {
                return await conn.InsertAsync(td.Table, data, transaction, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Insert objects in table. Will not populate generated fields.
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertMany(this DbConnection conn, IEnumerable<object> dataList, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            return conn.InsertMany(td.Table, dataList, transaction);
        }

        /// <summary>
        /// Insert objects in table. Will not populate generated fields.
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertManyAsync(this DbConnection conn, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            return conn.InsertManyAsync(td.Table, dataList, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert object in table or ignores if exists
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertIfMissing<T>(this DbConnection conn, T data, DbTransaction transaction = null)
            where T : class
        {
            return conn.InsertManyIfMissing(new List<T> { data }, transaction);
        }

        /// <summary>
        /// Insert object in table or ignores if exists
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertIfMissingAsync<T>(this DbConnection conn, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where T : class
        {
            return conn.InsertManyIfMissingAsync(new List<T> { data }, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert objects in table or ignores if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertManyIfMissing(this DbConnection conn, IEnumerable<object> dataList, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
            {
                return conn.InsertManyIfMissing(td.Table, dataList, string.Join(",", keys.Select(x => x.DbName)), transaction);
            }
            else
            {
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataList.First().GetType().Name);
            }
        }

        /// <summary>
        /// Insert objects in table or ignores if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertManyIfMissingAsync(this DbConnection conn, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
            {
                return conn.InsertManyIfMissingAsync(td.Table, dataList, string.Join(",", keys.Select(x => x.DbName)), transaction, cancellationToken);
            }
            else
            {
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataList.First().GetType().Name);
            }
        }

        /// <summary>
        /// Insert object in table or update if exists
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>true when inserted, false when updated</returns>
        public static bool Upsert<T>(this DbConnection conn, T data, DbTransaction transaction = null)
            where T : class
        {
            return conn.UpsertMany(new List<T> { data }, transaction).First();
        }

        /// <summary>
        /// Insert object in table or update if exists
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>true when inserted, false when updated</returns>
        public static async Task<bool> UpsertAsync<T>(this DbConnection conn, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where T : class
        {
            return (await conn.UpsertManyAsync(new List<T> { data }, transaction, cancellationToken).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <summary>
        /// Insert objects in table or updates if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of true when inserted, false when updated</returns>
        public static IEnumerable<bool> UpsertMany(this DbConnection conn, IEnumerable<object> dataList, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
            {
                return conn.UpsertMany(td.Table, dataList, string.Join(",", keys.Select(x => x.DbName)), transaction);
            }
            else
            {
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataList.First().GetType().Name);
            }
        }

        /// <summary>
        /// Insert objects in table or updates if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IEnumerable of true when inserted, false when updated</returns>
        public static Task<IEnumerable<bool>> UpsertManyAsync(this DbConnection conn, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Any())
            {
                return conn.UpsertManyAsync(td.Table, dataList, string.Join(",", keys.Select(x => x.DbName)), transaction, cancellationToken);
            }
            else
            {
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataList.First().GetType().Name);
            }
        }

        /// <summary>
        /// Update object in table
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Update<T>(this DbConnection conn, T data, DbTransaction transaction = null)
            where T : class
        {
            TypeDescriber td = TypeHandler.Get<T>();
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
                return conn.Update(td.Table, data, string.Join(",", keys.Select(x => x.DbName + "=@" + x.Property.Name)), null, transaction);
            else
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
        }

        /// <summary>
        /// Update object in table
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> UpdateAsync<T>(this DbConnection conn, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where T : class
        {
            TypeDescriber td = TypeHandler.Get<T>();
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
                return conn.UpdateAsync(td.Table, data, string.Join(",", keys.Select(x => x.DbName + "=@" + x.Property.Name)), null, transaction, cancellationToken);
            else
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
        }

        /// <summary>
        /// Delete object from table
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Delete<T>(this DbConnection conn, T data, DbTransaction transaction = null)
            where T : class
        {
            TypeDescriber td = TypeHandler.Get<T>();
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
                return conn.Delete(td.Table, string.Join(" AND ", keys.Select(x => x.DbName + "=@" + x.Property.Name)), data, transaction);
            else
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
        }

        /// <summary>
        /// Delete object from table
        /// </summary>
        /// <typeparam name="T">type of object</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> DeleteAsync<T>(this DbConnection conn, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where T : class
        {
            TypeDescriber td = TypeHandler.Get<T>();
            IEnumerable<PropertyDescriber> keys = td.Keys;

            if (keys.Count() > 0)
                return conn.DeleteAsync(td.Table, string.Join(" AND ", keys.Select(x => x.DbName + "=@" + x.Property.Name)), data, transaction, cancellationToken);
            else
                throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
        }
    }
}
