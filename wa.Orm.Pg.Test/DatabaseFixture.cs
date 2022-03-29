using Xunit;

namespace wa.Orm.Pg.Test
{
    public class DatabaseFixture
    {
        public DatabaseFixture()
        {
            DatabaseFactory.CreatePostgres();
        }
    }

    [CollectionDefinition("DBTest")]
    public class DBTest : ICollectionFixture<DatabaseFixture> { }
}