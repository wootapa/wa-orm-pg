using System;
using System.Linq;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test
{
    [Collection("DBTest")]
    public class ObjectTest
    {
        [Fact]
        public void Get()
        {
            Person person = null;
            int id = 0;

            using (var db = DatabaseFactory.Connect())
            {
                id = db.Insert<int>("persons", new Person { FirstName = "Foo" }, "id");
                person = db.Get<Person>(id);
            }

            Assert.NotNull(person);
            Assert.Equal(id, person.Id);
            Assert.Equal("Foo", person.FirstName);
        }

        [Fact]
        public void GetObjectWithoutKey()
        {
            using (var db = DatabaseFactory.Connect())
                Assert.Throws<ArgumentException>(() => db.Get<Document>(Guid.NewGuid()));
        }

        [Fact]
        public async Task GetObjectWithoutKeyAsync()
        {
            using (var db = await DatabaseFactory.ConnectAsync())
                await Assert.ThrowsAsync<ArgumentException>(() => db.GetAsync<Document>(Guid.NewGuid())).ConfigureAwait(false);
        }

        [Fact]
        public void GetObjectWhereKeyCountDoesntMatch()
        {
            using (var db = DatabaseFactory.Connect())
                Assert.Throws<ArgumentException>(() => db.Get<Person>(1, 2));
        }

        [Fact]
        public async Task GetObjectWhereKeyCountDoesntMatchAsync()
        {
            using (var db = await DatabaseFactory.ConnectAsync())
                await Assert.ThrowsAsync<ArgumentException>(() => db.GetAsync<Person>(1, 2)).ConfigureAwait(false);
        }

        [Fact]
        public void InsertObject()
        {
            Person person = new Person
            {
                FirstName = "Foo",
                LastName = "Bar"
            };

            using (var db = DatabaseFactory.Connect())
                db.Insert(person);

            Assert.True(person.Id > 0);
        }

        [Fact]
        public async Task InsertObjectAsync()
        {
            Person person = new Person
            {
                FirstName = "Foo",
                LastName = "Bar"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
                await db.InsertAsync(person).ConfigureAwait(false);

            Assert.True(person.Id > 0);
        }

        [Fact]
        public void InsertObjectWithoutKey()
        {
            Document document = new Document
            {
                Name = "foo.txt"
            };

            int affected;

            using (var db = DatabaseFactory.Connect())
                affected = db.Insert(document);

            Assert.True(affected == 1);
        }

        [Fact]
        public async Task InsertObjectWithoutKeyAsync()
        {
            Document document = new Document
            {
                Id = Guid.NewGuid(),
                Name = "foo2.txt"
            };

            int affected;

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
                affected = await db.InsertAsync(document).ConfigureAwait(false);

            Assert.True(affected == 1);
        }

        [Fact]
        public void UpdateObject()
        {
            int affected;

            Person person = new Person
            {
                FirstName = "Foo",
                LastName = "Bar"
            };

            using (var db = DatabaseFactory.Connect())
            {
                db.Insert(person);
                person.FirstName = "Baz";
                person.Age = 20;
                person.Gender = Gender.Male;

                affected = db.Update(person);

                person = db.Query<Person>("SELECT * FROM persons WHERE id=@Id", person).FirstOrDefault();
            }

            Assert.True(affected == 1);
            Assert.NotNull(person);
            Assert.Equal("Baz", person.FirstName);
            Assert.Equal("Bar", person.LastName);
            Assert.Equal(Gender.Male, person.Gender);
            Assert.Equal(20, person.Age);
        }

        [Fact]
        public async Task UpdateObjectAsync()
        {
            int affected;

            Person person = new Person
            {
                FirstName = "Foo",
                LastName = "Bar"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
            {
                await db.InsertAsync(person).ConfigureAwait(false);
                person.FirstName = "Baz";
                person.Age = 20;
                person.Gender = Gender.Male;

                affected = await db.UpdateAsync(person).ConfigureAwait(false);

                person = (await db.QueryAsync<Person>("SELECT * FROM persons WHERE id=@Id", person).FirstOrDefaultAsync().ConfigureAwait(false));
            }

            Assert.True(affected == 1);
            Assert.NotNull(person);
            Assert.Equal("Baz", person.FirstName);
            Assert.Equal("Bar", person.LastName);
            Assert.Equal(Gender.Male, person.Gender);
            Assert.Equal(20, person.Age);
        }

        [Fact]
        public void UpsertObject()
        {
            bool isInsert;
            bool isUpdate;

            Car car = new Car
            {
                Id = "BBB001",
                Make = "VW Golf"
            };

            using (var db = DatabaseFactory.Connect())
            {
                db.Delete(car);

                isInsert = db.Upsert(car);

                car.Make = "Ford Focus";
                isUpdate = !db.Upsert(car);

                car = db.Get<Car>(car.Id);
            }

            Assert.True(isInsert);
            Assert.True(isUpdate);
            Assert.Equal("BBB001", car.Id);
            Assert.Equal("Ford Focus", car.Make);
        }

        [Fact]
        public async Task UpsertObjectAsync()
        {
            bool isInsert;
            bool isUpdate;

            Car car = new Car
            {
                Id = "BBB001",
                Make = "VW Golf"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
            {
                db.Delete(car);

                isInsert = await db.UpsertAsync(car).ConfigureAwait(false);

                car.Make = "Ford Focus";
                isUpdate = !(await db.UpsertAsync(car).ConfigureAwait(false));

                car = db.Get<Car>(car.Id);
            }

            Assert.True(isInsert);
            Assert.True(isUpdate);
            Assert.Equal("BBB001", car.Id);
            Assert.Equal("Ford Focus", car.Make);
        }

        [Fact]
        public void UpsertObjectWithoutKey()
        {
            Document document = new Document
            {
                Name = "somedocument.txt"
            };

            using (var db = DatabaseFactory.Connect())
                Assert.Throws<ArgumentException>(() => db.Upsert(document));
        }

        [Fact]
        public async Task UpsertObjectWithoutKeyAsync()
        {
            Document document = new Document
            {
                Name = "somedocument.txt"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
                await Assert.ThrowsAsync<ArgumentException>(() => db.UpsertAsync(document)).ConfigureAwait(false);
        }

        [Fact]
        public void InsertObjectIfMissing()
        {
            int affected;
            int affected2;

            Car car = new Car
            {
                Id = "BBB002",
                Make = "Saab 9000"
            };

            using (var db = DatabaseFactory.Connect())
            {
                db.Delete(car);

                affected = db.InsertIfMissing(car);

                car.Make = "Seat Leon";
                affected2 = db.InsertIfMissing(car);

                car = db.Get<Car>(car.Id);
            }

            Assert.Equal(1, affected);
            Assert.Equal(0, affected2);
            Assert.Equal("BBB002", car.Id);
            Assert.Equal("Saab 9000", car.Make);
        }

        [Fact]
        public async Task InsertObjectIfMissingAsync()
        {
            int affected;
            int affected2;

            Car car = new Car
            {
                Id = "BBB002",
                Make = "Saab 9000"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
            {
                await db.DeleteAsync(car).ConfigureAwait(false);

                affected = await db.InsertIfMissingAsync(car).ConfigureAwait(false);

                car.Make = "Seat Leon";
                affected2 = await db.InsertIfMissingAsync(car).ConfigureAwait(false);

                car = await db.GetAsync<Car>(car.Id).ConfigureAwait(false);
            }

            Assert.Equal(1, affected);
            Assert.Equal(0, affected2);
            Assert.Equal("BBB002", car.Id);
            Assert.Equal("Saab 9000", car.Make);
        }

        [Fact]
        public void InsertObjectIfMissingWithoutKey()
        {
            Document document = new Document
            {
                Name = "somedocument.txt"
            };

            using (var db = DatabaseFactory.Connect())
                Assert.Throws<ArgumentException>(() => db.InsertIfMissing(document));
        }

        [Fact]
        public async Task InsertObjectIfMissingWithoutKeyAsync()
        {
            Document document = new Document
            {
                Name = "somedocument.txt"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
                await Assert.ThrowsAsync<ArgumentException>(() => db.InsertIfMissingAsync(document)).ConfigureAwait(false);
        }

        [Fact]
        public void UpdateObjectWithoutKey()
        {
            Document document = new Document
            {
                Name = "foo.txt"
            };

            using (var db = DatabaseFactory.Connect())
                Assert.Throws<ArgumentException>(() => db.Update(document));
        }

        [Fact]
        public async Task UpdateObjectWithoutKeyAsync()
        {
            Document document = new Document
            {
                Name = "foo.txt"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
                await Assert.ThrowsAsync<ArgumentException>(() => db.UpdateAsync(document)).ConfigureAwait(false);
        }

        [Fact]
        public void DeleteObject()
        {
            int affected;

            Person person = new Person
            {
                FirstName = "Foo",
                LastName = "Bar"
            };

            using (var db = DatabaseFactory.Connect())
            {
                db.Insert(person);

                affected = db.Delete(person);
            }

            Assert.True(affected == 1);
        }

        [Fact]
        public async Task DeleteObjectAsync()
        {
            int affected;

            Person person = new Person
            {
                FirstName = "Foo",
                LastName = "Bar"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
            {
                await db.InsertAsync(person).ConfigureAwait(false);

                affected = await db.DeleteAsync(person).ConfigureAwait(false);
            }

            Assert.True(affected == 1);
        }

        [Fact]
        public void DeleteObjectWithoutKey()
        {
            Document document = new Document
            {
                Name = "foo.txt"
            };

            using (var db = DatabaseFactory.Connect())
                Assert.Throws<ArgumentException>(() => db.Delete(document));
        }

        [Fact]
        public async Task DeleteObjectWithoutKeyAsync()
        {
            Document document = new Document
            {
                Name = "foo.txt"
            };

            using (var db = await DatabaseFactory.ConnectAsync().ConfigureAwait(false))
                await Assert.ThrowsAsync<ArgumentException>(() => db.DeleteAsync(document)).ConfigureAwait(false);
        }
    }
}
