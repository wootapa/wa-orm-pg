using System;
using System.Collections.Concurrent;

namespace wa.Orm.Pg.Reflection
{
    public static class TypeHandler
    {
        private static readonly ConcurrentDictionary<Type, TypeDescriber> types = new ConcurrentDictionary<Type, TypeDescriber>();

        public static TypeDescriber Get<T>()
        {
            if (!types.TryGetValue(typeof(T), out TypeDescriber result))
            {
                types.TryAdd(typeof(T), result = new TypeDescriber(typeof(T)));
            }

            return result;
        }

        public static TypeDescriber Get(object obj)
        {
            Type type = obj.GetType();

            if (!types.TryGetValue(type, out TypeDescriber result))
            {
                types.TryAdd(type, result = new TypeDescriber(type));
            }

            return result;
        }
    }
}
