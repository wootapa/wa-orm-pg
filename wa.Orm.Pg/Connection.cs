using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using wa.Orm.Pg.Reflection;

namespace wa.Orm.Pg
{
    /// <summary>
    /// Extensions for DbConnection that simplifies database communication.
    /// </summary>
    public static class ConnectionExtension
    {
        /// <summary>
        /// Prepares a command and makes sure connection is open
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-command to be executed</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>The created command</returns>
        public static DbCommand Prepare(this DbConnection conn, string sql, DbTransaction transaction = null)
        {
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;

            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }

            return cmd;
        }

        /// <summary>
        /// Prepares a command and makes sure connection is open
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-command to be executed</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>The created command</returns>
        public static async Task<DbCommand> PrepareAsync(this DbConnection conn, string sql, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;

            if (conn.State == ConnectionState.Closed)
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            return cmd;
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of T</returns>
        public static IEnumerable<T> Query<T>(this DbConnection conn, string sql, object args = null, DbTransaction transaction = null) where T : new()
        {
            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(args);

            return conn.Query<T>(cmd);
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of T</returns>
        public static async IAsyncEnumerable<T> QueryAsync<T>(this DbConnection conn, string sql, object args = null, DbTransaction transaction = null, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : new()
        {
            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(args);

            await foreach (var result in conn.QueryAsync<T>(cmd, cancellationToken).ConfigureAwait(false))
            {
                yield return result;
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <returns>IEnumerable of T</returns>
        public static IEnumerable<T> Query<T>(this DbConnection conn, DbCommand cmd) where T : new()
        {
            using (var reader = cmd.ExecuteReader())
            {
                var td = TypeHandler.Get<T>();

                while (reader.Read())
                {
                    var result = new T();

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        td.SetValue(reader.GetName(i), result, reader.GetValueWithNull(i));
                    }

                    yield return result;
                }
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of T</returns>
        public static async IAsyncEnumerable<T> QueryAsync<T>(this DbConnection conn, DbCommand cmd, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : new()
        {
            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                var td = TypeHandler.Get<T>();

                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var result = new T();

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        td.SetValue(reader.GetName(i), result, reader.GetValueWithNull(i));
                    }

                    yield return result;
                }
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of array of object</returns>
        public static IEnumerable<object[]> QueryArray(this DbConnection conn, string sql, object args = null, DbTransaction transaction = null)
        {
            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(args);

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    object[] result = new object[reader.FieldCount];

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        result[i] = reader.GetValueWithNull(i);
                    }

                    yield return result;
                }
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of array of object</returns>
        public static async IAsyncEnumerable<object[]> QueryArrayAsync(this DbConnection conn, string sql, object args = null, DbTransaction transaction = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(args);

            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.Read())
                {
                    object[] result = new object[reader.FieldCount];

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        result[i] = reader.GetValueWithNull(i);
                    }

                    yield return result;
                }
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of dictionary, where key is column name</returns>
        public static IEnumerable<IDictionary<string, object>> QueryAssoc(this DbConnection conn, string sql, object args = null, DbTransaction transaction = null)
        {
            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(args);

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Dictionary<string, object> result = new Dictionary<string, object>();

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        result[reader.GetName(i)] = reader.GetValueWithNull(i);
                    }

                    yield return result;
                }
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of dictionary, where key is column name</returns>
        public static async IAsyncEnumerable<IDictionary<string, object>> QueryAssocAsync(this DbConnection conn, string sql, object args = null, DbTransaction transaction = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(args);

            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    Dictionary<string, object> result = new Dictionary<string, object>();

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        result[reader.GetName(i)] = reader.GetValueWithNull(i);
                    }

                    yield return result;
                }
            }
        }

        /// <summary>
        /// Insert row in a table
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Insert(this DbConnection conn, string table, object data, DbTransaction transaction = null)
        {
            return conn.InsertMany(table, new List<Object> { data }, transaction);
        }

        /// <summary>
        /// Insert row in a table
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertAsync(this DbConnection conn, string table, object data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return conn.InsertManyAsync(table, new List<Object> { data }, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert rows in table
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertMany(this DbConnection conn, string table, IEnumerable<object> dataList, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            string columns = string.Join(",", td.WriteableColumns.Select(x => x.DbName));
            string values = string.Join(",", dataList.Select((data, i) => $"({string.Join(",", td.WriteableColumns.Select(x => "@" + x.Property.Name + i))})"));

            string sql = $"INSERT INTO {table} ({columns}) VALUES {values}";

            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(dataList);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Insert rows in table
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> InsertManyAsync(this DbConnection conn, string table, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            string columns = string.Join(",", td.WriteableColumns.Select(x => x.DbName));
            string values = string.Join(",", dataList.Select((data, i) => $"({string.Join(",", td.WriteableColumns.Select(x => "@" + x.Property.Name + i))})"));

            string sql = $"INSERT INTO {table} ({columns}) VALUES {values}";

            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(dataList);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert row in a table and returns generated id
        /// </summary>
        /// <typeparam name="Tid">type of primary key field</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>value of generated id</returns>
        public static Tid Insert<Tid>(this DbConnection conn, string table, object data, string pk, DbTransaction transaction = null)
            where Tid : struct
        {
            TypeDescriber td = TypeHandler.Get(data);

            string columns = td.WriteableColumns.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
            string values = td.WriteableColumns.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            string sql = $"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {pk}";

            return conn.Scalar<Tid>(sql, data, transaction);
        }

        /// <summary>
        /// Insert row in a table and returns generated id
        /// </summary>
        /// <typeparam name="Tid">type of primary key field</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>value of generated id</returns>
        public static Task<Tid> InsertAsync<Tid>(this DbConnection conn, string table, object data, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where Tid : struct
        {
            TypeDescriber td = TypeHandler.Get(data);

            string columns = td.WriteableColumns.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
            string values = td.WriteableColumns.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            string sql = $"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {pk}";

            return conn.ScalarAsync<Tid>(sql, data, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert row in table or ignores if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertIfMissing(this DbConnection conn, string table, object data, string pk, DbTransaction transaction = null)
        {
            return conn.InsertManyIfMissing(table, new List<Object> { data }, pk, transaction);
        }

        /// <summary>
        /// Insert row in table or ignores if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertIfMissingAsync(this DbConnection conn, string table, object data, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return conn.InsertManyIfMissingAsync(table, new List<Object> { data }, pk, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert rows in table or ignores if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertManyIfMissing(this DbConnection conn, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            string columns = string.Join(",", td.WriteableColumns.Select(x => x.DbName));
            string values = string.Join(",", dataList.Select((data, i) => $"({string.Join(",", td.WriteableColumns.Select(x => "@" + x.Property.Name + i))})"));

            string sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO NOTHING";

            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(dataList);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Insert rows in table or ignores if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> InsertManyIfMissingAsync(this DbConnection conn, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            string columns = string.Join(",", td.WriteableColumns.Select(x => x.DbName));
            string values = string.Join(",", dataList.Select((data, i) => $"({string.Join(",", td.WriteableColumns.Select(x => "@" + x.Property.Name + i))})"));

            string sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO NOTHING";

            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(dataList);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert row in table or update if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>true when inserted, false when updated</returns>
        public static bool Upsert(this DbConnection conn, string table, object data, string pk, DbTransaction transaction = null)
        {
            return conn.UpsertMany(table, new List<Object> { data }, pk, transaction).First();
        }

        /// <summary>
        /// Insert row in table or update if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>true when inserted, false when updated</returns>
        public static async Task<bool> UpsertAsync(this DbConnection conn, string table, object data, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return (await conn.UpsertManyAsync(table, new List<Object> { data }, pk, transaction, cancellationToken).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <summary>
        /// Insert rows in table or updates if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of true when inserted, false when updated</returns>
        public static IEnumerable<bool> UpsertMany(this DbConnection conn, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            string columns = string.Join(",", td.WriteableColumns.Select(x => x.DbName));
            string values = string.Join(",", dataList.Select((data, i) => $"({string.Join(",", td.WriteableColumns.Select(x => "@" + x.Property.Name + i))})"));
            string set = string.Join(",", td.WriteableColumns.Select(x => x.DbName + "=excluded." + x.DbName));

            string sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO UPDATE SET {set} RETURNING (xmax = 0) as inserted";

            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(dataList);

            return conn.ScalarList<bool>(cmd);
        }

        /// <summary>
        /// Insert rows in table or updates if exists
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IEnumerable of true when inserted, false when updated</returns>
        public static async Task<IEnumerable<bool>> UpsertManyAsync(this DbConnection conn, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(dataList.First());

            string columns = string.Join(",", td.WriteableColumns.Select(x => x.DbName));
            string values = string.Join(",", dataList.Select((data, i) => $"({string.Join(",", td.WriteableColumns.Select(x => "@" + x.Property.Name + i))})"));
            string set = string.Join(",", td.WriteableColumns.Select(x => x.DbName + "=excluded." + x.DbName));

            string sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO UPDATE SET {set} RETURNING (xmax = 0) as inserted";

            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(dataList);

            return await conn.ScalarListAsync<bool>(cmd, cancellationToken).ToListAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Update row(s) in a table matching the where clause
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Additional arguments to apply other then data e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Update(this DbConnection conn, string table, object data, string where, object args = null, DbTransaction transaction = null)
        {
            TypeDescriber td = TypeHandler.Get(data);

            string set = td.WriteableColumns.Select(x => x.DbName + "=@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            string sql = $"UPDATE {table} SET {set} WHERE {where}";

            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(data);
            cmd.ApplyParameters(args);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Update row(s) in a table matching the where clause
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Additional arguments to apply other then data e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> UpdateAsync(this DbConnection conn, string table, object data, string where, object args = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(data);

            string set = td.WriteableColumns.Select(x => x.DbName + "=@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            string sql = $"UPDATE {table} SET {set} WHERE {where}";

            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(data);
            cmd.ApplyParameters(args);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete row(s) in a table matching the where clause
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Arguments to apply e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Delete(this DbConnection conn, string table, string where, object args = null, DbTransaction transaction = null)
        {
            return conn.Execute($"DELETE FROM {table} WHERE {where}", args, transaction);
        }

        /// <summary>
        /// Delete row(s) in a table matching the where clause
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Arguments to apply e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> DeleteAsync(this DbConnection conn, string table, string where, object args = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return conn.ExecuteAsync($"DELETE FROM {table} WHERE {where}", args, transaction, cancellationToken);
        }

        /// <summary>
        /// Read first value of first row
        /// </summary>
        /// <typeparam name="T">type to read</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>First value of first row as T</returns>
        public static T Scalar<T>(this DbConnection conn, string sql, object data = null, DbTransaction transaction = null)
        {
            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(data);

            return (T)cmd.ExecuteScalar();
        }

        /// <summary>
        /// Read first value of first row
        /// </summary>
        /// <typeparam name="T">type to read</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>First value of first row as T</returns>
        public static async Task<T> ScalarAsync<T>(this DbConnection conn, string sql, object data = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(data);

            return (T)(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Read first value of all rows
        /// </summary>
        /// <typeparam name="T">type to read</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <returns>IEnumerable of T</returns>
        public static IEnumerable<T> ScalarList<T>(this DbConnection conn, DbCommand cmd)
        {
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return (T)reader.GetValueWithNull(0);
                }
            }
        }
        /// <summary>
        /// Read first value of all rows
        /// </summary>
        /// <typeparam name="T">type to read</typeparam>
        /// <param name="conn">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of T</returns>
        public static async IAsyncEnumerable<T> ScalarListAsync<T>(this DbConnection conn, DbCommand cmd, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    yield return (T)reader.GetValueWithNull(0);
                }
            }
        }

        /// <summary>
        /// Executes SQL-statement
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Execute(this DbConnection conn, string sql, object data = null, DbTransaction transaction = null)
        {
            DbCommand cmd = conn.Prepare(sql, transaction);
            cmd.ApplyParameters(data);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Executes SQL-statement
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> ExecuteAsync(this DbConnection conn, string sql, object data = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            DbCommand cmd = await conn.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(data);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
