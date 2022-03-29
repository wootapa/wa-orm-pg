using System;

namespace wa.Orm.Pg
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class GeneratedAttribute : Attribute
    {
        public GeneratedAttribute() { }
    }
}
