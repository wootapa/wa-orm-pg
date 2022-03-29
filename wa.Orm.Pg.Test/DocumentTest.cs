using System;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test
{
    [Collection("DBTest")]
    public class DocumentTest : IDisposable
    {
        private DbConnection _db;
        private Document _data;

        public DocumentTest()
        {
            _db = DatabaseFactory.Connect();

            _data = new Document
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
            _db.Insert("document", _data);

            Guid id = _db.Scalar<Guid>("SELECT id FROM document WHERE id=@Id", _data);

            Assert.Equal(_data.Id, id);
        }

        [Fact]
        public async Task InsertAsync()
        {
            await _db.InsertAsync("document", _data).ConfigureAwait(false);

            Guid id = await _db.ScalarAsync<Guid>("SELECT id FROM document WHERE id=@Id", _data).ConfigureAwait(false);

            Assert.Equal(_data.Id, id);
        }

        [Fact]
        public void Update()
        {
            _db.Insert("document", _data);
            _data.Name = "changed.txt";

            int affected = _db.Update("document", _data, "id=@Id");

            string newName = _db.Scalar<string>("SELECT name FROM document WHERE id=@Id", _data);

            Assert.Equal(1, affected);
            Assert.Equal(_data.Name, newName);
        }

        [Fact]
        public async Task UpdateAsync()
        {
            await _db.InsertAsync("document", _data).ConfigureAwait(false);
            _data.Name = "changed.txt";

            int affected = await _db.UpdateAsync("document", _data, "id=@Id").ConfigureAwait(false);

            string newName = await _db.ScalarAsync<string>("SELECT name FROM document WHERE id=@Id", _data).ConfigureAwait(false);

            Assert.Equal(1, affected);
            Assert.Equal(_data.Name, newName);
        }

        [Fact]
        public void Delete()
        {
            _db.Insert("document", _data);

            int affected = _db.Delete("document", "id=@Id", _data);

            Assert.Equal(1, affected);
        }

        [Fact]
        public async Task DeleteAsync()
        {
            await _db.InsertAsync("document", _data).ConfigureAwait(false);

            int affected = await _db.DeleteAsync("document", "id=@Id", _data).ConfigureAwait(false);

            Assert.Equal(1, affected);
        }

        [Fact]
        public void Query()
        {
            _db.Insert("document", _data);

            Document doc = _db.Query<Document>("SELECT * FROM document WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(doc);
            Assert.Equal(_data.Id, doc.Id);
            Assert.Equal(_data.Name, doc.Name);
            Assert.Equal(Encoding.UTF8.GetString(_data.Data), Encoding.UTF8.GetString(doc.Data));
        }

        [Fact]
        public async Task QueryAsync()
        {
            await _db.InsertAsync("document", _data).ConfigureAwait(false);

            Document doc = (await _db.QueryAsync<Document>("SELECT * FROM document WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(doc);
            Assert.Equal(_data.Id, doc.Id);
            Assert.Equal(_data.Name, doc.Name);
            Assert.Equal(Encoding.UTF8.GetString(_data.Data), Encoding.UTF8.GetString(doc.Data));
        }

        [Fact]
        public void QueryAssoc()
        {
            _db.Insert("document", _data);

            var doc = _db.QueryAssoc("SELECT * FROM document WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(doc);
            Assert.Equal(_data.Id, doc["id"]);
            Assert.Equal(_data.Name, doc["name"]);
            Assert.Equal(Encoding.UTF8.GetString(_data.Data), Encoding.UTF8.GetString((byte[])doc["data"]));
        }

        [Fact]
        public async Task QueryAssocAsync()
        {
            _db.Insert("document", _data);

            var doc = (await _db.QueryAssocAsync("SELECT * FROM document WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(doc);
            Assert.Equal(_data.Id, doc["id"]);
            Assert.Equal(_data.Name, doc["name"]);
            Assert.Equal(Encoding.UTF8.GetString(_data.Data), Encoding.UTF8.GetString((byte[])doc["data"]));
        }

        [Fact]
        public void QueryArray()
        {
            _db.Insert("document", _data);

            var doc = _db.QueryArray("SELECT id,name,data,date_created FROM document WHERE id=@Id", _data).FirstOrDefault();

            Assert.NotNull(doc);
            Assert.Equal(_data.Id, doc[0]);
            Assert.Equal(_data.Name, doc[1]);
            Assert.Equal(Encoding.UTF8.GetString(_data.Data), Encoding.UTF8.GetString((byte[])doc[2]));
        }

        [Fact]
        public async Task QueryArrayAsync()
        {
            await _db.InsertAsync("document", _data).ConfigureAwait(false);

            var doc = (await _db.QueryArrayAsync("SELECT id,name,data,date_created FROM document WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.NotNull(doc);
            Assert.Equal(_data.Id, doc[0]);
            Assert.Equal(_data.Name, doc[1]);
            Assert.Equal(Encoding.UTF8.GetString(_data.Data), Encoding.UTF8.GetString((byte[])doc[2]));
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}