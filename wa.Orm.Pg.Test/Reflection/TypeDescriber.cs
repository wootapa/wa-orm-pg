using wa.Orm.Pg.Reflection;
using wa.Orm.Pg.Test.Models;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace wa.Orm.Pg.Test.Reflection
{
    public class TypeDescriberTest
    {
        private readonly ITestOutputHelper testOutputHelper;

        public TypeDescriberTest(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void HasType()
        {
            TypeDescriber type = TypeHandler.Get<Person>();

            Assert.Equal(typeof(Person), type.Type);
        }

        [Fact]
        public void HasTable()
        {
            TypeDescriber person = TypeHandler.Get<Person>();
            TypeDescriber document = TypeHandler.Get<Document>();

            Assert.Equal("persons", person.Table);
            Assert.Equal("document", document.Table);
        }

        [Fact]
        public void PropertiesRegistered()
        {
            TypeDescriber person = TypeHandler.Get<Person>();
            TypeDescriber document = TypeHandler.Get<Document>();

            Assert.Equal(7, person.Count);
            Assert.Equal(4, document.Count);
        }

        [Fact]
        public void WriteableProperties()
        {
            TypeDescriber person = TypeHandler.Get<Person>();
            TypeDescriber document = TypeHandler.Get<Document>();
            
            Assert.Equal(4, person.Writable.Count());
            Assert.Equal(4, document.Writable.Count());
        }

        [Fact]
        public void ReadableProperties()
        {
            TypeDescriber person = TypeHandler.Get<Person>();
            TypeDescriber document = TypeHandler.Get<Document>();

            Assert.Equal(4, person.Writable.Count());
            Assert.Equal(4, document.Writable.Count());
        }

        [Fact]
        public void KeysAssigned()
        {
            TypeDescriber person = TypeHandler.Get<Person>();
            TypeDescriber document = TypeHandler.Get<Document>();

            Assert.Single(person.Keys);
            Assert.Empty(document.Keys);

            Assert.Equal(6, person.NonKeys.Count());
            Assert.Equal(4, document.NonKeys.Count());
        }
    }
}
