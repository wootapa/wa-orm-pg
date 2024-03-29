﻿using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace wa.Orm.Pg.Test.Models;

[Table("persons")]
public class Person
{
    [Key]
    [Generated]
    public long Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string FullName => FirstName + " " + LastName;
    public int? Age { get; set; }
    public Gender Gender { get; set; }
    [Generated]
    public DateTime? DateCreated { get; set; }
}