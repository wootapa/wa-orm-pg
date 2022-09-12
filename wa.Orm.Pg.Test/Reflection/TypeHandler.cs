using wa.Orm.Pg.Reflection;
using wa.Orm.Pg.Test.Models;
using Xunit;

namespace wa.Orm.Pg.Test.Reflection
{
    public class TypeHandlerTest
    {
        [Fact]
        public void ReadByClass()
        {
            Assert.NotNull(TypeHandler.Get<Person>());
        }

        [Fact]
        public void ReadByObject()
        {
            Assert.NotNull(TypeHandler.Get(new Person()));
        }

        [Fact]
        public void ReadFromCache()
        {
            Assert.Same(TypeHandler.Get<Person>(), TypeHandler.Get(new Person()));
        }

        [Fact]
        public void Different()
        {
            Assert.NotSame(TypeHandler.Get<Person>(), TypeHandler.Get<Document>());
        }
    }
}
