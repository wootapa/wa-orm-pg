using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test;

[Collection("DBTest")]
public class PersonTest : IDisposable
{
    private readonly DbConnection db;
    private readonly Person data;

    public PersonTest()
    {
        db = DatabaseFactory.Connect();

        data = new Person
        {
            FirstName = "Foo",
            LastName = "Bar",
            Gender = Gender.Male,
            DateCreated = DateTime.Now
        };
    }

    [Fact]
    public void Insert()
    {
        var id = db.Insert<int>("persons", data, "id");

        Assert.True(id > 0);
    }

    [Fact]
    public async Task InsertAsync()
    {
        var id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);

        Assert.True(id > 0);
    }

    [Fact]
    public void Upsert()
    {
        data.FirstName = "Gandalf";
        data.Age = 1000;
        data.Id = db.Insert<int>("persons", data, "id");

        // Will create new as pk is generated
        data.FirstName = "Saruman";
        data.Age = 2000;
        db.Upsert("persons", data, "id");

        var person = db.Query<Person>("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(person);
        Assert.Equal(data.Id, person.Id);
        Assert.NotEqual(person.FirstName, data.FirstName);
        Assert.NotEqual(person.Age, data.Age);
    }

    [Fact]
    public async Task UpsertAsync()
    {
        data.FirstName = "Gandalf";
        data.Age = 1000;
        data.Id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);

        // Will create new as pk is generated
        data.FirstName = "Saruman";
        data.Age = 2000;
        await db.UpsertAsync("persons", data, "id");

        var person = (await db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(person);
        Assert.Equal(data.Id, person.Id);
        Assert.NotEqual(person.FirstName, data.FirstName);
        Assert.NotEqual(person.Age, data.Age);
    }

    [Fact]
    public void Update()
    {
        data.Id = db.Insert<int>("persons", data, "id");
        data.FirstName = "Baz";
        data.Age = 20;

        db.Update("persons", data, "id=@Id");
        var updated = db.Query<Person>("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(updated);
        Assert.Equal("Baz", updated.FirstName);
        Assert.Equal(20, updated.Age);
    }

    [Fact]
    public async Task UpdateAsync()
    {
        data.Id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);
        data.FirstName = "Baz";
        data.Age = 20;

        await db.UpdateAsync("persons", data, "id=@Id").ConfigureAwait(false);
        var updated = (await db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(updated);
        Assert.Equal("Baz", updated.FirstName);
        Assert.Equal(20, updated.Age);
    }

    [Fact]
    public void Delete()
    {
        data.Id = db.Insert<int>("persons", data, "id");

        var affected = db.Delete("persons", "id=@id", data);

        Assert.Equal(1, affected);
    }

    [Fact]
    public async Task DeleteAsync()
    {
        data.Id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);

        var affected = await db.DeleteAsync("persons", "id=@id", data).ConfigureAwait(false);

        Assert.Equal(1, affected);
    }

    [Fact]
    public void Query()
    {
        data.Id = db.Insert<int>("persons", data, "id");

        var person = db.Query<Person>("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(person);
        Assert.Equal(data.Id, person.Id);
        Assert.Equal(data.FirstName, person.FirstName);
        Assert.Equal(data.Age, person.Age);
        Assert.Equal(data.Gender, person.Gender);
    }

    [Fact]
    public async Task QueryAsync()
    {
        data.Id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);

        var person = (await db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(person);
        Assert.Equal(data.Id, person.Id);
        Assert.Equal(data.FirstName, person.FirstName);
        Assert.Equal(data.Age, person.Age);
        Assert.Equal(data.Gender, person.Gender);
    }

    [Fact]
    public void QueryAssoc()
    {
        data.Id = db.Insert<int>("persons", data, "id");

        var person = db.QueryAssoc("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(person);
        Assert.Equal(data.Id, int.Parse(person["id"].ToString() ?? string.Empty));
        Assert.Equal(data.FirstName, person["first_name"]);
        Assert.Equal(data.Age, person["age"]);
        Assert.Equal(data.Gender, person["gender"]);
    }

    [Fact]
    public async Task QueryAssocAsync()
    {
        data.Id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);

        var person = (await db.QueryAssocAsync("SELECT * FROM persons WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(person);
        Assert.Equal(data.Id, int.Parse(person["id"].ToString() ?? string.Empty));
        Assert.Equal(data.FirstName, person["first_name"]);
        Assert.Equal(data.Age, person["age"]);
        Assert.Equal(data.Gender, person["gender"]);
    }

    [Fact]
    public void QueryArray()
    {
        data.Id = db.Insert<int>("persons", data, "id");

        var person = db.QueryArray("SELECT id,first_name,age,gender,date_created FROM persons WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(person);
        Assert.Equal(data.Id, int.Parse(person[0].ToString() ?? string.Empty));
        Assert.Equal(data.FirstName, person[1]);
        Assert.Equal(data.Age, person[2]);
        Assert.Equal(data.Gender, person[3]);
    }

    [Fact]
    public async Task QueryArrayAsync()
    {
        data.Id = await db.InsertAsync<int>("persons", data, "id").ConfigureAwait(false);

        var person = (await db.QueryArrayAsync("SELECT id,first_name,age,gender,date_created FROM persons WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(person);
        Assert.Equal(data.Id, int.Parse(person[0].ToString() ?? string.Empty));
        Assert.Equal(data.FirstName, person[1]);
        Assert.Equal(data.Age, person[2]);
        Assert.Equal(data.Gender, person[3]);
    }

    public void Dispose()
    {
        db.Dispose();
    }
}