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
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-command to be executed</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>The created command</returns>
        public static DbCommand Prepare(this DbConnection @this, string sql, DbTransaction transaction = null)
        {
            var cmd = @this.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;

            if (@this.State == ConnectionState.Closed)
            {
                @this.Open();
            }

            return cmd;
        }

        /// <summary>
        /// Prepares a command and makes sure connection is open
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-command to be executed</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>The created command</returns>
        public static async Task<DbCommand> PrepareAsync(this DbConnection @this, string sql, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var cmd = @this.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;

            if (@this.State == ConnectionState.Closed)
            {
                await @this.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            return cmd;
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of T</returns>
        public static IEnumerable<T> Query<T>(this DbConnection @this, string sql, object args = null, DbTransaction transaction = null) where T : new()
        {
            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(args);

            return @this.Query<T>(cmd);
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of T</returns>
        public static async IAsyncEnumerable<T> QueryAsync<T>(this DbConnection @this, string sql, object args = null, DbTransaction transaction = null, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : new()
        {
            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(args);

            await foreach (var result in @this.QueryAsync<T>(cmd, cancellationToken).ConfigureAwait(false))
            {
                yield return result;
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <returns>IEnumerable of T</returns>
        public static IEnumerable<T> Query<T>(this DbConnection @this, DbCommand cmd) where T : new()
        {
            using var reader = cmd.ExecuteReader();
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

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <typeparam name="T">class to map data to</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of T</returns>
        public static async IAsyncEnumerable<T> QueryAsync<T>(this DbConnection @this, DbCommand cmd, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : new()
        {
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
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

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of array of object</returns>
        public static IEnumerable<object[]> QueryArray(this DbConnection @this, string sql, object args = null, DbTransaction transaction = null)
        {
            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(args);

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var result = new object[reader.FieldCount];

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    result[i] = reader.GetValueWithNull(i);
                }

                yield return result;
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of array of object</returns>
        public static async IAsyncEnumerable<object[]> QueryArrayAsync(this DbConnection @this, string sql, object args = null, DbTransaction transaction = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(args);

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken))
            {
                var result = new object[reader.FieldCount];

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    result[i] = reader.GetValueWithNull(i);
                }

                yield return result;
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of dictionary, where key is column name</returns>
        public static IEnumerable<IDictionary<string, object>> QueryAssoc(this DbConnection @this, string sql, object args = null, DbTransaction transaction = null)
        {
            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(args);

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var result = new Dictionary<string, object>();

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    result[reader.GetName(i)] = reader.GetValueWithNull(i);
                }

                yield return result;
            }
        }

        /// <summary>
        /// Read data from database
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="args">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of dictionary, where key is column name</returns>
        public static async IAsyncEnumerable<IDictionary<string, object>> QueryAssocAsync(this DbConnection @this, string sql, object args = null, DbTransaction transaction = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(args);

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var result = new Dictionary<string, object>();

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    result[reader.GetName(i)] = reader.GetValueWithNull(i);
                }

                yield return result;
            }
        }

        /// <summary>
        /// Insert row in a table
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Insert(this DbConnection @this, string table, object data, DbTransaction transaction = null)
        {
            return @this.InsertMany(table, new List<Object> { data }, transaction);
        }

        /// <summary>
        /// Insert row in a table
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertAsync(this DbConnection @this, string table, object data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return @this.InsertManyAsync(table, new List<object> { data }, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert rows in table
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertMany(this DbConnection @this, string table, IEnumerable<object> dataList, DbTransaction transaction = null)
        {
            var dataListArray = dataList as object[] ?? dataList.ToArray();
            var td = TypeHandler.Get(dataListArray.First());

            var columns = string.Join(",", td.Writable.Select(x => x.DbName));
            var values = string.Join(",", dataListArray.Select((data, i) => $"({string.Join(",", td.Writable.Select(x => "@" + x.Property.Name + i))})"));

            var sql = $"INSERT INTO {table} ({columns}) VALUES {values}";

            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(dataListArray);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Insert rows in table
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> InsertManyAsync(this DbConnection @this, string table, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var dataListArray = dataList as object[] ?? dataList.ToArray();
            var td = TypeHandler.Get(dataListArray.First());

            var columns = string.Join(",", td.Writable.Select(x => x.DbName));
            var values = string.Join(",", dataListArray.Select((data, i) => $"({string.Join(",", td.Writable.Select(x => "@" + x.Property.Name + i))})"));

            var sql = $"INSERT INTO {table} ({columns}) VALUES {values}";

            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(dataListArray);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert row in a table and returns generated id
        /// </summary>
        /// <typeparam name="TId">type of primary key field</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>value of generated id</returns>
        public static TId Insert<TId>(this DbConnection @this, string table, object data, string pk, DbTransaction transaction = null)
            where TId : struct
        {
            var td = TypeHandler.Get(data);

            var columns = td.Writable.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
            var values = td.Writable.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            var sql = $"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {pk}";

            return @this.Scalar<TId>(sql, data, transaction);
        }

        /// <summary>
        /// Insert row in a table and returns generated id
        /// </summary>
        /// <typeparam name="Tid">type of primary key field</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>value of generated id</returns>
        public static Task<Tid> InsertAsync<Tid>(this DbConnection @this, string table, object data, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
            where Tid : struct
        {
            var td = TypeHandler.Get(data);

            var columns = td.Writable.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
            var values = td.Writable.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            var sql = $"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {pk}";

            return @this.ScalarAsync<Tid>(sql, data, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert row in table or ignores if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertIfMissing(this DbConnection @this, string table, object data, string pk, DbTransaction transaction = null)
        {
            return @this.InsertManyIfMissing(table, new List<Object> { data }, pk, transaction);
        }

        /// <summary>
        /// Insert row in table or ignores if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> InsertIfMissingAsync(this DbConnection @this, string table, object data, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return @this.InsertManyIfMissingAsync(table, new List<object> { data }, pk, transaction, cancellationToken);
        }

        /// <summary>
        /// Insert rows in table or ignores if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int InsertManyIfMissing(this DbConnection @this, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null)
        {
            var dataListArray = dataList as object[] ?? dataList.ToArray();
            var td = TypeHandler.Get(dataListArray.First());

            var columns = string.Join(",", td.Writable.Select(x => x.DbName));
            var values = string.Join(",", dataListArray.Select((data, i) => $"({string.Join(",", td.Writable.Select(x => "@" + x.Property.Name + i))})"));

            var sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO NOTHING";

            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(dataListArray);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Insert rows in table or ignores if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to insert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> InsertManyIfMissingAsync(this DbConnection @this, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var dataListArray = dataList as object[] ?? dataList.ToArray();
            var td = TypeHandler.Get(dataListArray.First());

            var columns = string.Join(",", td.Writable.Select(x => x.DbName));
            var values = string.Join(",", dataListArray.Select((data, i) => $"({string.Join(",", td.Writable.Select(x => "@" + x.Property.Name + i))})"));

            var sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO NOTHING";

            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(dataListArray);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert row in table or update if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>true when inserted, false when updated</returns>
        public static bool Upsert(this DbConnection @this, string table, object data, string pk, DbTransaction transaction = null)
        {
            return @this.UpsertMany(table, new List<Object> { data }, pk, transaction).First();
        }

        /// <summary>
        /// Insert row in table or update if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>true when inserted, false when updated</returns>
        public static async Task<bool> UpsertAsync(this DbConnection @this, string table, object data, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return (await @this.UpsertManyAsync(table, new List<Object> { data }, pk, transaction, cancellationToken).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <summary>
        /// Insert rows in table or updates if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>IEnumerable of true when inserted, false when updated</returns>
        public static IEnumerable<bool> UpsertMany(this DbConnection @this, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null)
        {
            var dataListArray = dataList as object[] ?? dataList.ToArray();
            var td = TypeHandler.Get(dataListArray.First());

            var columns = string.Join(",", td.Writable.Select(x => x.DbName));
            var values = string.Join(",", dataListArray.Select((data, i) => $"({string.Join(",", td.Writable.Select(x => "@" + x.Property.Name + i))})"));
            var set = string.Join(",", td.Writable.Select(x => x.DbName + "=excluded." + x.DbName));

            string sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO UPDATE SET {set} RETURNING (xmax = 0) as inserted";

            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(dataListArray);

            return @this.ScalarList<bool>(cmd);
        }

        /// <summary>
        /// Insert rows in table or updates if exists
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to upsert into</param>
        /// <param name="dataList">List of objects containing the data</param>
        /// <param name="pk">Name of primary key field</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IEnumerable of true when inserted, false when updated</returns>
        public static async Task<IEnumerable<bool>> UpsertManyAsync(this DbConnection @this, string table, IEnumerable<object> dataList, string pk, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var dataListArray = dataList as object[] ?? dataList.ToArray();
            var td = TypeHandler.Get(dataListArray.First());

            var columns = string.Join(",", td.Writable.Select(x => x.DbName));
            var values = string.Join(",", dataListArray.Select((data, i) => $"({string.Join(",", td.Writable.Select(x => "@" + x.Property.Name + i))})"));
            var set = string.Join(",", td.Writable.Select(x => x.DbName + "=excluded." + x.DbName));

            var sql = $@"
                INSERT INTO {table} ({columns}) VALUES {values}
                ON CONFLICT ({pk}) DO UPDATE SET {set} RETURNING (xmax = 0) as inserted";

            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(dataListArray);

            return await @this.ScalarListAsync<bool>(cmd, cancellationToken).ToListAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Update row(s) in a table matching the where clause
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Additional arguments to apply other then data e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Update(this DbConnection @this, string table, object data, string where, object args = null, DbTransaction transaction = null)
        {
            var td = TypeHandler.Get(data);

            var set = td.Writable.Select(x => x.DbName + "=@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            var sql = $"UPDATE {table} SET {set} WHERE {where}";

            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(data);
            cmd.ApplyParameters(args);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Update row(s) in a table matching the where clause
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="data">Object containing the data</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Additional arguments to apply other then data e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> UpdateAsync(this DbConnection @this, string table, object data, string where, object args = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            TypeDescriber td = TypeHandler.Get(data);

            var set = td.Writable.Select(x => x.DbName + "=@" + x.Property.Name).Aggregate((a, b) => a + "," + b);

            var sql = $"UPDATE {table} SET {set} WHERE {where}";

            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(data);
            cmd.ApplyParameters(args);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete row(s) in a table matching the where clause
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Arguments to apply e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Delete(this DbConnection @this, string table, string where, object args = null, DbTransaction transaction = null)
        {
            return @this.Execute($"DELETE FROM {table} WHERE {where}", args, transaction);
        }

        /// <summary>
        /// Delete row(s) in a table matching the where clause
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="table">Name of table to update</param>
        /// <param name="where">Where clause e.g. "id=@id"</param>
        /// <param name="args">Arguments to apply e.g. new { id = 1 }</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static Task<int> DeleteAsync(this DbConnection @this, string table, string where, object args = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            return @this.ExecuteAsync($"DELETE FROM {table} WHERE {where}", args, transaction, cancellationToken);
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
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>First value of first row as T</returns>
        public static async Task<T> ScalarAsync<T>(this DbConnection @this, string sql, object data = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(data);

            return (T)(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Read first value of all rows
        /// </summary>
        /// <typeparam name="T">type to read</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <returns>IEnumerable of T</returns>
        public static IEnumerable<T> ScalarList<T>(this DbConnection @this, DbCommand cmd)
        {
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                yield return (T)reader.GetValueWithNull(0);
            }
        }
        /// <summary>
        /// Read first value of all rows
        /// </summary>
        /// <typeparam name="T">type to read</typeparam>
        /// <param name="this">A connection</param>
        /// <param name="cmd">Command to be executed</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>IAsyncEnumerable of T</returns>
        public static async IAsyncEnumerable<T> ScalarListAsync<T>(this DbConnection @this, DbCommand cmd, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return (T)reader.GetValueWithNull(0);
            }
        }

        /// <summary>
        /// Executes SQL-statement
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>Rows affected</returns>
        public static int Execute(this DbConnection @this, string sql, object data = null, DbTransaction transaction = null)
        {
            var cmd = @this.Prepare(sql, transaction);
            cmd.ApplyParameters(data);

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Executes SQL-statement
        /// </summary>
        /// <param name="this">A connection</param>
        /// <param name="sql">SQL-statement to be executed</param>
        /// <param name="data">Arguments to apply on SQL-statement</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <param name="cancellationToken">Cancellationtoken</param>
        /// <returns>Rows affected</returns>
        public static async Task<int> ExecuteAsync(this DbConnection @this, string sql, object data = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            var cmd = await @this.PrepareAsync(sql, transaction, cancellationToken).ConfigureAwait(false);
            cmd.ApplyParameters(data);

            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
