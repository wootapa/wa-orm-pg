using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test
{
    [Collection("DBTest")]
    public class CarTest : IDisposable
    {
        private DbConnection _db;

        public CarTest()
        {
            _db = DatabaseFactory.Connect();
        }

        [Fact]
        public void InsertAndUpsert()
        {
            Car _data = new Car
            {
                Id = "AAA001",
                Make = "Audi S4"
            };

            _db.Insert("cars", _data);

            _data.Make = "BMW M5";
            bool isInsert = _db.Upsert("cars", _data, "id");

            Car car = _db.Query<Car>("SELECT * FROM cars WHERE id=@Id", _data).FirstOrDefault();

            Assert.False(isInsert);
            Assert.Equal(_data.Make, car.Make);
        }

        [Fact]
        public async Task InsertAndUpsertAsync()
        {
            Car _data = new Car
            {
                Id = "AAAA001",
                Make = "Audi S4"
            };

            await _db.InsertAsync("cars", _data).ConfigureAwait(false);

            _data.Make = "BMW M5";
            bool isInsert = await _db.UpsertAsync("cars", _data, "id").ConfigureAwait(false);

            Car car = (await _db.QueryAsync<Car>("SELECT * FROM cars WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.False(isInsert);
            Assert.Equal(_data.Make, car.Make);
        }

        [Fact]
        public void UpsertAndUpsert()
        {
            Car _data = new Car
            {
                Id = "AAA002",
                Make = "VW Passat"
            };

            bool isInsert = _db.Upsert("cars", _data, "id");

            _data.Make = "Volvo V70";
            bool isUpdate = !_db.Upsert("cars", _data, "id");

            Car car = _db.Query<Car>("SELECT * FROM cars WHERE id=@Id", _data).FirstOrDefault();

            Assert.True(isInsert);
            Assert.True(isUpdate);
            Assert.Equal(_data.Make, car.Make);
        }

        [Fact]
        public async Task UpsertAndUpsertAsync()
        {
            Car _data = new Car
            {
                Id = "AAAA002",
                Make = "VW Passat"
            };

            bool isInsert = await _db.UpsertAsync("cars", _data, "id").ConfigureAwait(false);

            _data.Make = "Volvo V70";
            bool isUpdate = !(await _db.UpsertAsync("cars", _data, "id").ConfigureAwait(false));

            Car car = _db.Query<Car>("SELECT * FROM cars WHERE id=@Id", _data).FirstOrDefault();

            Assert.True(isInsert);
            Assert.True(isUpdate);
            Assert.Equal(_data.Make, car.Make);
        }

        [Fact]
        public void InsertIfMissing()
        {
            Car _data = new Car
            {
                Id = "AAA003",
                Make = "Ferrari F40"
            };

            int affected = _db.InsertIfMissing("cars", _data, "id");

            _data.Make = "Lamborghini Aventador";
            int affected2 = _db.InsertIfMissing("cars", _data, "id");

            Car car = _db.Query<Car>("SELECT * FROM cars WHERE id=@Id", _data).FirstOrDefault();

            Assert.Equal(1, affected);
            Assert.Equal(0, affected2);
            Assert.Equal("Ferrari F40", car.Make);
        }

        [Fact]
        public async Task InsertIfMissingAsync()
        {
            Car _data = new Car
            {
                Id = "AAAA003",
                Make = "Ferrari F40"
            };

            int affected = await _db.InsertIfMissingAsync("cars", _data, "id").ConfigureAwait(false);

            _data.Make = "Lamborghini Aventador";
            int affected2 = await _db.InsertIfMissingAsync("cars", _data, "id").ConfigureAwait(false);

            Car car = (await _db.QueryAsync<Car>("SELECT * FROM cars WHERE id=@Id", _data).FirstOrDefaultAsync().ConfigureAwait(false));

            Assert.Equal(1, affected);
            Assert.Equal(0, affected2);
            Assert.Equal("Ferrari F40", car.Make);
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}