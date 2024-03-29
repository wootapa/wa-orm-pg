# wa.Orm.Pg
.NET object mapper for PostgreSQL.

## Simple querying database
Model used in example:
```
public class Person
{
    [Generated]
    public int Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public int Age { get; set; }
    public Gender Gender { get; set; }
    public DateTime DateCreated { get; set; }
}

public enum Gender
{
    Unknown,
    Male,
    Female
}

public class Document
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public byte[] Data { get; set; }
    public DateTime DateCreated { get; set; }
}

public class Car
{
    public string Id { get; set; }
    public string Make { get; set; }
}
```

Querying database and map to model, assuming ```c``` is of type ```DbConnection```:
```
Person[] persons = c.Query<Person>("SELECT * FROM persons").ToArray();
```

```persons``` now contains an array of ```Person``` with data pulled from the database.

Quite simple, right? Check out more examples below. Don't forget to read the "Important!" notes further down.



## Examples
Examples below will assume that ```c``` is of type ```DbConnection```.

### Basic query
a list of the given type

```c.Query<Person>("SELECT * FROM persons");```

### Assoc query
a list of dictionary where key matches columns name

```c.QueryAssoc("SELECT id,first_name FROM persons");```

### Array query
a list of array where index matches columns index

```c.QueryArray("SELECT id,first_name FROM persons");```

### Insert

inserts ```document``` into table documents
```
var document = new Document
{
    Id = Guid.NewGuid(),
    Name = "foo.txt",
    Data = Gender.Male,
    DateCreated = DateTime.Now
};

c.Insert("documents", document);
```


Or if you have a table containing an auto generated id
```
var person = new Person
{
    FirstName = "Batman",
    LastName = "Petersson",
    Gender = Gender.Male,
    DateCreated = DateTime.Now
};

person.Id = c.Insert<int>("persons", person, "id");
```
### InsertIfMissing
inserts ```car``` into table cars if id="ABC123" is missing, otherwise ignores. Returns rows affected.
```
Car car = new Car
{
    Id = "ABC123",
    Make = "Saab 9000"
};

// Will insert
c.InsertIfMissing("cars", car, "id");

// No effect and wont throw PK error
car.Make = "Volvo V70";
c.InsertIfMissing("cars", car);
```

### Update
updates table persons and set ```person``` where id=1

```c.Update("persons", person, "id=@Id", new { Id = 1 });```

### Upsert
Inserts ```car``` into table cars if id="ABC123" is missing, otherwise updates. Returns true for inserted, false for updated.
```
Car car = new Car
{
    Id = "ABC123",
    Make = "Saab 9000"
};

//Will insert
c.Upsert("cars", car, "id");

// Will update
car.Make = "Volvo V70";
c.Upsert("cars", car, "id");
```

### Delete
deletes from table persons where first_name="Batman"

```c.Delete("persons", "first_name=@FirstName", new { FirstName = "Batman" });```

### Execute
execute anything, returning rows affected

```c.Execute("TRUNCATE persons");```


## Bulk
Insert, InsertIfMissing and Upsert all support bulk loading. Just pass a list.
```
int rowsAffected = c.Insert("cars", carList);
```
```
int rowsAffected = c.InsertIfMissing("cars", carList, "id")
```
```
var result = c.Upsert("cars", carList, "id").ToList();
int insertCount = result.Count(inserted => inserted);
int updateCount = result.Count(inserted => !inserted);
```


## Decorated classes

You can do even simpler Inserts, Updates and Deletes by decorating your classes with ```Table```, ```Key```,```Generated``` and ```StringEnum``` attributes.

```
[Table("persons")]
public class Person
{
    [Key]
    [Generated]
    public long Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public int? Age { get; set; }
    //[StringEnum] // Would treat as an ordinary string
    public Gender Gender { get; set; }
    [Generated]
    public DateTime? DateCreated { get; set; }
}

[Table("cars")]
public class Car
{
    [Key]
    public string Id { get; set; }
    public string Make { get; set; }
}
```

### Get
```
c.Get<Person>(1);
``` 
will read table "persons" and return an object of ```Person``` from the database where id is 1.

### Insert
```
c.Insert(person);
``` 
will insert into table "persons" and map properties marked with ```Generated``` to the object.

### InsertIfMissing
```
c.InsertIfMissing(car);
```
inserts ```car``` into table cars if ```Key``` property is missing, otherwise ignores.

### Update
```
c.Update(person);
``` 
updates table "persons" matching the key field(s).

### Upsert
```
c.Upsert(car);
```
Inserts ```car``` into table cars if ```Key``` property is missing, otherwise updates.

### Delete
```
c.Delete(person);
``` 
deletes from table "persons" matching the key field(s).


## Important!
```[Generated]``` properties are ignored on ```Insert``` and ```Update```, but will be mapped with ```Query```. Useful when there is an autogenerated id or you have a property in your model that doesn't match a column in the table.

```yield``` is used in ```Query```-methods. Make sure to read data before disposing the connection by calling ```.ToList()``` or ```.ToArray()```.

All methods will open the connection, if not opened yet.

Uppercase in properties names will assume that the column name has an underscore before e.g. ```FirstName``` matches ```first_name``` column in database.

No more ```DBNull```! Seamless translates ```DBNull``` to ```null``` and ```null``` to ```DBNull```.