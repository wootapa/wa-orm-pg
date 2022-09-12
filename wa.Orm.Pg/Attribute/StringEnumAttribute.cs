using System;

namespace wa.Orm.Pg;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class StringEnumAttribute : Attribute
{
}
