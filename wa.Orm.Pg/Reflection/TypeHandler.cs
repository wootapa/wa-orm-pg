using System;
using System.Collections.Concurrent;

namespace wa.Orm.Pg.Reflection;

public static class TypeHandler
{
    private static readonly ConcurrentDictionary<Type, TypeDescriber> Types = new();

    public static TypeDescriber Get<T>()
    {
        if (!Types.TryGetValue(typeof(T), out TypeDescriber result))
        {
            Types.TryAdd(typeof(T), result = new TypeDescriber(typeof(T)));
        }

        return result;
    }

    public static TypeDescriber Get(object obj)
    {
        var type = obj.GetType();

        if (!Types.TryGetValue(type, out var result))
        {
            Types.TryAdd(type, result = new TypeDescriber(type));
        }

        return result;
    }
}