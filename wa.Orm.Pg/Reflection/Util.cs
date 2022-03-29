using System.Text.RegularExpressions;

namespace wa.Orm.Pg.Reflection
{
    public class Util
    {
        public static string ToUnderscore(string input)
        {
            return Regex.Replace(input.Trim(), @"(?<=.)([A-Z])", "_$1").ToLower();
        }
    }
}
