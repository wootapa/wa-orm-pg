using wa.Orm.Pg.Reflection;
using wa.Orm.Pg.Test.Models;
using System.Linq;
using Xunit;

namespace wa.Orm.Pg.Test.Reflection
{
    public class TypeDescriberTest
    {
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

            Assert.Equal(5, person.WriteableProperties.Count());
            Assert.Equal(4, document.WriteableProperties.Count());
        }

        [Fact]
        public void ReadableProperties()
        {
            TypeDescriber person = TypeHandler.Get<Person>();
            TypeDescriber document = TypeHandler.Get<Document>();

            Assert.Equal(4, person.WriteableColumns.Count());
            Assert.Equal(4, document.WriteableColumns.Count());
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
