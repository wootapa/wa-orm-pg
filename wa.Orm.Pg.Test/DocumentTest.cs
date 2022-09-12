using System;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test;

[Collection("DBTest")]
public class DocumentTest : IDisposable
{
    private readonly DbConnection db;
    private readonly Document data;

    public DocumentTest()
    {
        db = DatabaseFactory.Connect();

        data = new Document
        {
            Id = Guid.NewGuid(),
            Name = "file.txt",
            Data = Encoding.UTF8.GetBytes("Hello world!"),
            DateCreated = DateTime.Now
        };
    }

    [Fact]
    public void Insert()
    {
        db.Insert("document", data);

        var id = db.Scalar<Guid>("SELECT id FROM document WHERE id=@Id", data);

        Assert.Equal(data.Id, id);
    }

    [Fact]
    public async Task InsertAsync()
    {
        await db.InsertAsync("document", data).ConfigureAwait(false);

        var id = await db.ScalarAsync<Guid>("SELECT id FROM document WHERE id=@Id", data).ConfigureAwait(false);

        Assert.Equal(data.Id, id);
    }

    [Fact]
    public void Update()
    {
        db.Insert("document", data);
        data.Name = "changed.txt";

        var affected = db.Update("document", data, "id=@Id");

        var newName = db.Scalar<string>("SELECT name FROM document WHERE id=@Id", data);

        Assert.Equal(1, affected);
        Assert.Equal(data.Name, newName);
    }

    [Fact]
    public async Task UpdateAsync()
    {
        await db.InsertAsync("document", data).ConfigureAwait(false);
        data.Name = "changed.txt";

        var affected = await db.UpdateAsync("document", data, "id=@Id").ConfigureAwait(false);

        var newName = await db.ScalarAsync<string>("SELECT name FROM document WHERE id=@Id", data).ConfigureAwait(false);

        Assert.Equal(1, affected);
        Assert.Equal(data.Name, newName);
    }

    [Fact]
    public void Delete()
    {
        db.Insert("document", data);

        var affected = db.Delete("document", "id=@Id", data);

        Assert.Equal(1, affected);
    }

    [Fact]
    public async Task DeleteAsync()
    {
        await db.InsertAsync("document", data).ConfigureAwait(false);

        var affected = await db.DeleteAsync("document", "id=@Id", data).ConfigureAwait(false);

        Assert.Equal(1, affected);
    }

    [Fact]
    public void Query()
    {
        db.Insert("document", data);

        var doc = db.Query<Document>("SELECT * FROM document WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(doc);
        Assert.Equal(data.Id, doc.Id);
        Assert.Equal(data.Name, doc.Name);
        Assert.Equal(Encoding.UTF8.GetString(data.Data), Encoding.UTF8.GetString(doc.Data));
    }

    [Fact]
    public async Task QueryAsync()
    {
        await db.InsertAsync("document", data).ConfigureAwait(false);

        var doc = await db.QueryAsync<Document>("SELECT * FROM document WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false);

        Assert.NotNull(doc);
        Assert.Equal(data.Id, doc.Id);
        Assert.Equal(data.Name, doc.Name);
        Assert.Equal(Encoding.UTF8.GetString(data.Data), Encoding.UTF8.GetString(doc.Data));
    }

    [Fact]
    public void QueryAssoc()
    {
        db.Insert("document", data);

        var doc = db.QueryAssoc("SELECT * FROM document WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(doc);
        Assert.Equal(data.Id, doc["id"]);
        Assert.Equal(data.Name, doc["name"]);
        Assert.Equal(Encoding.UTF8.GetString(data.Data), Encoding.UTF8.GetString((byte[])doc["data"]));
    }

    [Fact]
    public async Task QueryAssocAsync()
    {
        await db.InsertAsync("document", data);

        var doc = (await db.QueryAssocAsync("SELECT * FROM document WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(doc);
        Assert.Equal(data.Id, doc["id"]);
        Assert.Equal(data.Name, doc["name"]);
        Assert.Equal(Encoding.UTF8.GetString(data.Data), Encoding.UTF8.GetString((byte[])doc["data"]));
    }

    [Fact]
    public void QueryArray()
    {
        db.Insert("document", data);

        var doc = db.QueryArray("SELECT id,name,data,date_created FROM document WHERE id=@Id", data).FirstOrDefault();

        Assert.NotNull(doc);
        Assert.Equal(data.Id, doc[0]);
        Assert.Equal(data.Name, doc[1]);
        Assert.Equal(Encoding.UTF8.GetString(data.Data), Encoding.UTF8.GetString((byte[])doc[2]));
    }

    [Fact]
    public async Task QueryArrayAsync()
    {
        await db.InsertAsync("document", data).ConfigureAwait(false);

        var doc = (await db.QueryArrayAsync("SELECT id,name,data,date_created FROM document WHERE id=@Id", data).FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.NotNull(doc);
        Assert.Equal(data.Id, doc[0]);
        Assert.Equal(data.Name, doc[1]);
        Assert.Equal(Encoding.UTF8.GetString(data.Data), Encoding.UTF8.GetString((byte[])doc[2]));
    }

    public void Dispose()
    {
        db.Dispose();
    }
}