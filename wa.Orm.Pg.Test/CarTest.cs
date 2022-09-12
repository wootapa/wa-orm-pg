using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test;

[Collection("DBTest")]
public class CarTest : IDisposable
{
    private readonly DbConnection db;

    public CarTest()
    {
        db = DatabaseFactory.Connect();
    }

    [Fact]
    public void InsertAndUpsert()
    {
        var data = new Car
        {
            Id = "AAA001",
            Make = "Audi S4"
        };

        db.Insert("cars", data);

        data.Make = "BMW M5";
        var isInsert = db.Upsert("cars", data, "id");

        var car = db.Query<Car>("SELECT * FROM cars WHERE id=@Id", data).FirstOrDefault();

        Assert.False(isInsert);
        Assert.Equal(data.Make, car!.Make);
    }

    [Fact]
    public async Task InsertAndUpsertAsync()
    {
        var data = new Car
        {
            Id = "AAAA001",
            Make = "Audi S4"
        };

        await db.InsertAsync("cars", data).ConfigureAwait(false);

        data.Make = "BMW M5";
        var isInsert = await db.UpsertAsync("cars", data, "id").ConfigureAwait(false);

        var car = (await db.QueryAsync<Car>("SELECT * FROM cars WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.False(isInsert);
        Assert.Equal(data.Make, car!.Make);
    }

    [Fact]
    public void UpsertAndUpsert()
    {
        var data = new Car
        {
            Id = "AAA002",
            Make = "VW Passat"
        };

        var isInsert = db.Upsert("cars", data, "id");

        data.Make = "Volvo V70";
        var isUpdate = !db.Upsert("cars", data, "id");

        var car = db.Query<Car>("SELECT * FROM cars WHERE id=@Id", data).FirstOrDefault();

        Assert.True(isInsert);
        Assert.True(isUpdate);
        Assert.Equal(data.Make, car!.Make);
    }

    [Fact]
    public async Task UpsertAndUpsertAsync()
    {
        var data = new Car
        {
            Id = "AAAA002",
            Make = "VW Passat"
        };

        var isInsert = await db.UpsertAsync("cars", data, "id").ConfigureAwait(false);

        data.Make = "Volvo V70";
        var isUpdate = !(await db.UpsertAsync("cars", data, "id").ConfigureAwait(false));

        var car = db.Query<Car>("SELECT * FROM cars WHERE id=@Id", data).FirstOrDefault();

        Assert.True(isInsert);
        Assert.True(isUpdate);
        Assert.Equal(data.Make, car!.Make);
    }

    [Fact]
    public void InsertIfMissing()
    {
        var data = new Car
        {
            Id = "AAA003",
            Make = "Ferrari F40"
        };

        var affected = db.InsertIfMissing("cars", data, "id");

        data.Make = "Lamborghini Aventador";
        var affected2 = db.InsertIfMissing("cars", data, "id");

        var car = db.Query<Car>("SELECT * FROM cars WHERE id=@Id", data).FirstOrDefault();

        Assert.Equal(1, affected);
        Assert.Equal(0, affected2);
        Assert.Equal("Ferrari F40", car!.Make);
    }

    [Fact]
    public async Task InsertIfMissingAsync()
    {
        var data = new Car
        {
            Id = "AAAA003",
            Make = "Ferrari F40"
        };

        var affected = await db.InsertIfMissingAsync("cars", data, "id").ConfigureAwait(false);

        data.Make = "Lamborghini Aventador";
        var affected2 = await db.InsertIfMissingAsync("cars", data, "id").ConfigureAwait(false);

        var car = (await db.QueryAsync<Car>("SELECT * FROM cars WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.Equal(1, affected);
        Assert.Equal(0, affected2);
        Assert.Equal("Ferrari F40", car!.Make);
    }

    public void Dispose()
    {
        db.Dispose();
    }
}