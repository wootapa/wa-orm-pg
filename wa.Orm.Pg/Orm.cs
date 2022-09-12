using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using wa.Orm.Pg.Reflection;

namespace wa.Orm.Pg;

/// <summary>
/// Extensions for objects to insert, update, delete etc.
/// </summary>
public static class OrmExtension
{
    /// <summary>
    /// Get a object of T by its ID
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="ids">Id(s) in same order as marked with KeyAttribute</param>
    /// <returns>T</returns>
    public static T Get<T>(this DbConnection @this, params object[] ids)
        where T : class, new()
    {
        var td = TypeHandler.Get<T>();
        var keys = td.Keys.ToArray();

        if (keys.Length == 0)
        {
            throw new ArgumentException("T must be a type with properties decorated with KeyAttribute");
        }

        if (keys.Length != ids.Length)
        {
            throw new ArgumentException($"KeyAttribute-count ({keys.Length}) and argument-count ({ids.Length}) must match");
        }

        var where = string.Join(" AND ", keys.Select(x => x.DbName + "=@" + x.Property.Name));
        var sql = $"SELECT * FROM {td.Table} WHERE {where}";

        var cmd = @this.Prepare(sql);
        for (var i = 0; i < ids.Length; i++)
        {
            cmd.ApplyParameter(keys[i].Property.Name, ids[i]);
        }

        return @this.Query<T>(cmd).FirstOrDefault();
    }


    /// <summary>
    /// Get a object of T by its ID
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="ids">Id(s) in same order as marked with KeyAttribute</param>
    /// <returns>T</returns>
    public static Task<T> GetAsync<T>(this DbConnection @this, params object[] ids)
        where T : class, new()
    {
        return GetAsync<T>(@this, CancellationToken.None, ids);
    }

    /// <summary>
    /// Get a object of T by its ID
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <param name="ids">Id(s) in same order as marked with KeyAttribute</param>
    /// <returns>T</returns>
    public static async Task<T> GetAsync<T>(this DbConnection @this, CancellationToken cancellationToken, params object[] ids)
        where T : class, new()
    {
        var td = TypeHandler.Get<T>();
        var keys = td.Keys.ToArray();

        if (keys.Length == 0)
        {
            throw new ArgumentException("T must be a type with properties decorated with KeyAttribute");
        }

        if (keys.Length != ids.Length)
        {
            throw new ArgumentException($"KeyAttribute-count ({keys.Length}) and argument-count ({ids.Length}) must match");
        }

        var where = string.Join(",", keys.Select(x => x.DbName + "=@" + x.Property.Name));
        var sql = $"SELECT * FROM {td.Table} WHERE {where}";

        DbCommand cmd = await @this.PrepareAsync(sql, cancellationToken: cancellationToken).ConfigureAwait(false);
        for (var i = 0; i < ids.Length; i++)
        {
            cmd.ApplyParameter(keys[i].Property.Name, ids[i]);
        }

        return (await @this.QueryAsync<T>(cmd, cancellationToken).FirstOrDefaultAsync(cancellationToken: cancellationToken).ConfigureAwait(false));
    }

    /// <summary>
    /// Insert object in table. Will populate generated fields with values from database.
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>Rows affected</returns>
    public static int Insert<T>(this DbConnection @this, T data, DbTransaction transaction = null)
        where T : class
    {
        var td = TypeHandler.Get<T>();
        var generated = td.Generated.ToArray();

        if (generated.Any())
        {
            var columns = td.Writable.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
            var values = td.Writable.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);
            var returns = string.Join(",", generated.Select(x => x.DbName));

            var sql = $"INSERT INTO {td.Table} ({columns}) VALUES ({values}) RETURNING {returns}";

            var result = @this.QueryAssoc(sql, data, transaction).FirstOrDefault();

            foreach (var prop in generated)
            {
                td.SetValue(prop.Property.Name, data, result?[prop.DbName]);
            }

            return 1;
        }

        return @this.Insert(td.Table, data, transaction);
    }

    /// <summary>
    /// Insert object in table. Will populate generated fields with values from database.
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>Rows affected</returns>
    public static async Task<int> InsertAsync<T>(this DbConnection @this, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        where T : class
    {
        var td = TypeHandler.Get<T>();
        var generated = td.Generated.ToArray();

        if (generated.Any())
        {
            var columns = td.Writable.Select(x => x.DbName).Aggregate((a, b) => a + "," + b);
            var values = td.Writable.Select(x => "@" + x.Property.Name).Aggregate((a, b) => a + "," + b);
            var returns = string.Join(",", generated.Select(x => x.DbName));

            var sql = $"INSERT INTO {td.Table} ({columns}) VALUES ({values}) RETURNING {returns}";

            var result = (await @this.QueryAssocAsync(sql, data, transaction, cancellationToken).FirstOrDefaultAsync().ConfigureAwait(false));

            foreach (var prop in generated)
            {
                td.SetValue(prop.Property.Name, data, result?[prop.DbName]);
            }

            return 1;
        }

        return await @this.InsertAsync(td.Table, data, transaction, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Insert objects in table. Will not populate generated fields.
    /// </summary>
    /// <param name="this">A connection</param>
    /// <param name="dataList">List of objects containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>Rows affected</returns>
    public static int InsertMany(this DbConnection @this, IEnumerable<object> dataList, DbTransaction transaction = null)
    {
        var dataListArray = dataList as object[] ?? dataList.ToArray();
        var td = TypeHandler.Get(dataListArray.First());

        return @this.InsertMany(td.Table, dataListArray, transaction);
    }

    /// <summary>
    /// Insert objects in table. Will not populate generated fields.
    /// </summary>
    /// <param name="this">A connection</param>
    /// <param name="dataList">List of objects containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>Rows affected</returns>
    public static Task<int> InsertManyAsync(this DbConnection @this, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
    {
        var dataListArray = dataList as object[] ?? dataList.ToArray();
        var td = TypeHandler.Get(dataListArray.First());

        return @this.InsertManyAsync(td.Table, dataListArray, transaction, cancellationToken);
    }

    /// <summary>
    /// Insert object in table or ignores if exists
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>Rows affected</returns>
    public static int InsertIfMissing<T>(this DbConnection @this, T data, DbTransaction transaction = null)
        where T : class
    {
        return @this.InsertManyIfMissing(new List<T> { data }, transaction);
    }

    /// <summary>
    /// Insert object in table or ignores if exists
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>Rows affected</returns>
    public static Task<int> InsertIfMissingAsync<T>(this DbConnection @this, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        where T : class
    {
        return @this.InsertManyIfMissingAsync(new List<T> { data }, transaction, cancellationToken);
    }

    /// <summary>
    /// Insert objects in table or ignores if exists
    /// </summary>
    /// <param name="this">A connection</param>
    /// <param name="dataList">List of objects containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>Rows affected</returns>
    public static int InsertManyIfMissing(this DbConnection @this, IEnumerable<object> dataList, DbTransaction transaction = null)
    {
        var dataListArray = dataList as object[] ?? dataList.ToArray();
        var td = TypeHandler.Get(dataListArray.First());
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.InsertManyIfMissing(td.Table, dataListArray, string.Join(",", keys.Select(x => x.DbName)), transaction);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataListArray.First().GetType().Name);
    }

    /// <summary>
    /// Insert objects in table or ignores if exists
    /// </summary>
    /// <param name="this">A connection</param>
    /// <param name="dataList">List of objects containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>Rows affected</returns>
    public static Task<int> InsertManyIfMissingAsync(this DbConnection @this, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
    {
        var dataListArray = dataList as object[] ?? dataList.ToArray();
        var td = TypeHandler.Get(dataListArray.First());
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.InsertManyIfMissingAsync(td.Table, dataListArray, string.Join(",", keys.Select(x => x.DbName)), transaction, cancellationToken);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataListArray.First().GetType().Name);
    }

    /// <summary>
    /// Insert object in table or update if exists
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>true when inserted, false when updated</returns>
    public static bool Upsert<T>(this DbConnection @this, T data, DbTransaction transaction = null)
        where T : class
    {
        return @this.UpsertMany(new List<T> { data }, transaction).First();
    }

    /// <summary>
    /// Insert object in table or update if exists
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>true when inserted, false when updated</returns>
    public static async Task<bool> UpsertAsync<T>(this DbConnection @this, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        where T : class
    {
        return (await @this.UpsertManyAsync(new List<T> { data }, transaction, cancellationToken).ConfigureAwait(false)).FirstOrDefault();
    }

    /// <summary>
    /// Insert objects in table or updates if exists
    /// </summary>
    /// <param name="this">A connection</param>
    /// <param name="dataList">List of objects containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>IEnumerable of true when inserted, false when updated</returns>
    public static IEnumerable<bool> UpsertMany(this DbConnection @this, IEnumerable<object> dataList, DbTransaction transaction = null)
    {
        var dataListArray = dataList as object[] ?? dataList.ToArray();
        var td = TypeHandler.Get(dataListArray.First());
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.UpsertMany(td.Table, dataListArray, string.Join(",", keys.Select(x => x.DbName)), transaction);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataListArray.First().GetType().Name);
    }

    /// <summary>
    /// Insert objects in table or updates if exists
    /// </summary>
    /// <param name="this">A connection</param>
    /// <param name="dataList">List of objects containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>IEnumerable of true when inserted, false when updated</returns>
    public static Task<IEnumerable<bool>> UpsertManyAsync(this DbConnection @this, IEnumerable<object> dataList, DbTransaction transaction = null, CancellationToken cancellationToken = default)
    {
        var dataListArray = dataList as object[] ?? dataList.ToArray();
        var td = TypeHandler.Get(dataListArray.First());
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.UpsertManyAsync(td.Table, dataListArray, string.Join(",", keys.Select(x => x.DbName)), transaction, cancellationToken);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + dataListArray.First().GetType().Name);
    }

    /// <summary>
    /// Update object in table
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>Rows affected</returns>
    public static int Update<T>(this DbConnection @this, T data, DbTransaction transaction = null)
        where T : class
    {
        var td = TypeHandler.Get<T>();
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.Update(td.Table, data, string.Join(",", keys.Select(x => x.DbName + "=@" + x.Property.Name)), null, transaction);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
    }

    /// <summary>
    /// Update object in table
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>Rows affected</returns>
    public static Task<int> UpdateAsync<T>(this DbConnection @this, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        where T : class
    {
        var td = TypeHandler.Get<T>();
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.UpdateAsync(td.Table, data, string.Join(",", keys.Select(x => x.DbName + "=@" + x.Property.Name)), null, transaction, cancellationToken);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
    }

    /// <summary>
    /// Delete object from table
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <returns>Rows affected</returns>
    public static int Delete<T>(this DbConnection @this, T data, DbTransaction transaction = null)
        where T : class
    {
        var td = TypeHandler.Get<T>();
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.Delete(td.Table, string.Join(" AND ", keys.Select(x => x.DbName + "=@" + x.Property.Name)), data, transaction);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
    }

    /// <summary>
    /// Delete object from table
    /// </summary>
    /// <typeparam name="T">type of object</typeparam>
    /// <param name="this">A connection</param>
    /// <param name="data">Object containing the data</param>
    /// <param name="transaction">Transaction to associate with the command</param>
    /// <param name="cancellationToken">Cancellationtoken</param>
    /// <returns>Rows affected</returns>
    public static Task<int> DeleteAsync<T>(this DbConnection @this, T data, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        where T : class
    {
        var td = TypeHandler.Get<T>();
        var keys = td.Keys.ToArray();

        if (keys.Any())
        {
            return @this.DeleteAsync(td.Table, string.Join(" AND ", keys.Select(x => x.DbName + "=@" + x.Property.Name)), data, transaction, cancellationToken);
        }

        throw new ArgumentException("Invalid object. Atleast one property must be marked with KeyAttribute on type " + data.GetType().Name);
    }
}