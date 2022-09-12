using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test;

[Collection("DBTest")]
public class BulkTest : IDisposable
{
    private readonly DbConnection db;

    public BulkTest()
    {
        db = DatabaseFactory.Connect();
    }

    [Fact]
    public void InsertMany()
    {
        var personList = Enumerable.Range(0, 100).Select(i => new Person
        {
            FirstName = "FirstName" + i,
            LastName = "LastName " + i,
            Gender = Gender.Female,
            DateCreated = DateTime.Now
        }).ToArray();

        var rowsAffected = db.InsertMany("persons", personList);

        Assert.Equal(100, rowsAffected);
    }

    [Fact]
    public async Task InsertManyAsync()
    {
        var personList = Enumerable.Range(0, 100).Select(i => new Person
        {
            FirstName = "FirstName" + i,
            LastName = "LastName " + i,
            Gender = Gender.Female,
            DateCreated = DateTime.Now
        }).ToArray();

        var rowsAffected = await db.InsertManyAsync("persons", personList).ConfigureAwait(false);

        Assert.Equal(100, rowsAffected);
    }

    [Fact]
    public void InsertManyIfMissing()
    {
        var carList = Enumerable.Range(0, 100).Select(i => new Car
        {
            Id = "CCC" + i,
            Make = "Car " + i
        }).ToArray();

        var insertCount1 = db.InsertManyIfMissing("cars", carList.Take(25), "id");
        var insertCount2 = db.InsertManyIfMissing("cars", carList, "id");

        Assert.Equal(25, insertCount1);
        Assert.Equal(75, insertCount2);
    }

    [Fact]
    public async Task InsertManyIfMissingAsync()
    {
        var carList = Enumerable.Range(0, 100).Select(i => new Car
        {
            Id = "CCCC" + i,
            Make = "Car " + i
        }).ToArray();

        var insertCount1 = await db.InsertManyIfMissingAsync("cars", carList.Take(25), "id").ConfigureAwait(false);
        var insertCount2 = await db.InsertManyIfMissingAsync("cars", carList, "id").ConfigureAwait(false);

        Assert.Equal(25, insertCount1);
        Assert.Equal(75, insertCount2);
    }

    [Fact]
    public void UpsertMany()
    {
        var carList = Enumerable.Range(0, 100).Select(i => new Car
        {
            Id = "DDD" + i,
            Make = "Car " + i
        }).ToArray();

        var result1 = db.UpsertMany("cars", carList, "id").ToList();
        var insertCount = result1.Count(inserted => inserted);

        var result2 = db.UpsertMany("cars", carList, "id").ToList();
        var updateCount = result2.Count(inserted => !inserted);

        Assert.Equal(100, insertCount);
        Assert.Equal(100, updateCount);
    }

    [Fact]
    public async Task UpsertManyAsync()
    {
        var carList = Enumerable.Range(0, 100).Select(i => new Car
        {
            Id = "DDDD" + i,
            Make = "Car " + i
        }).ToArray();

        var result1 = (await db.UpsertManyAsync("cars", carList, "id").ConfigureAwait(false)).ToList();
        var insertCount = result1.Count(inserted => inserted);

        var result2 = (await db.UpsertManyAsync("cars", carList, "id").ConfigureAwait(false)).ToList();
        var updateCount = result2.Count(inserted => !inserted);

        Assert.Equal(100, insertCount);
        Assert.Equal(100, updateCount);
    }

    public void Dispose()
    {
        db.Dispose();
    }
}