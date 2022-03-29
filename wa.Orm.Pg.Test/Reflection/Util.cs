using wa.Orm.Pg.Reflection;
using wa.Orm.Pg.Test.Models;
using System;
using System.Linq;
using Xunit;

namespace wa.Orm.Pg.Test.Reflection
{
    public class UtilTest
    {
        [Theory]
        [InlineData("visualStudio")]
        [InlineData("VisualStudio")]
        [InlineData(" VisualStudio ")]
        public void ToUnderscore(string input)
        {
            Assert.Equal("visual_studio", Util.ToUnderscore(input));
        }
    }
}
