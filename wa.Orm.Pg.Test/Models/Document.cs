using System;

namespace wa.Orm.Pg.Test.Models;

public class Document
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public byte[] Data { get; set; }
    public DateTime DateCreated { get; set; }
}