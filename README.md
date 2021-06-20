# SimpleJdbc

Simple wrapper library for dealing with SQL-in-Java and JDBC. No dependencies, no reflection.

## Features
* Convenient, fluent syntax
* Named Parameters (`:paramName` instead of `?`)
* Always uses `PreparedStatement` under the hood
* `QueryResult` wraps and fixes issues with `ResultSet`:
  * Boxed types (`Integer` instead of `int`) to make dealing with `null`-valued columns easier
  * Optional use of `Optional` for `null`-valued columns if that's your preferred style
  * Handles `java.time` classes out-of-the-box
  * "Escape hatch" lets you use underlying `ResultSet` for advanced use-cases  
* Basic transaction support    
* Avoids "magic" - no reflection, no code generation, no dynamic proxies

## Examples

### Query

```java
// TODO: convenience functions for common result extraction patterns
List<SomeObject> results =
    SimpleJdbc.using(dataSource)
        .query("select foo, bar, baz from some_table where id = :id and thing = :thing")
        .bind("id", 123)
        .bind("thing", 456)
        .select(
            queryResult -> {
              var list = new ArrayList<SomeObject>();
              while (queryResult.next()) {
                  list.add(
                      SomeObject.builder()
                          .foo(rowResult.getInteger("foo"))
                          .bar(rowResult.getString("bar"))
                          .baz(rowResult.getBigDecimal("baz")));
              }
              return list;
            });
```

### Statement

```java
int rowsAffected = 
    SimpleJdbc.using(dataSource)
        .statement("update some_table set foo = :foo, bar = :bar where id = :id")
        .bind("foo", "someValue")
        .bind("bar", 56.25)
        .bind("id", 12345L)
        .execute();
```

### Batch Statement
```java
int[] rowsAffected =
    SimpleJdbc.using(dataSource)
        .batchStatement("update some_table set foo = :foo, bar = :bar where id = :id")
        .bind("foo", "someValue")
        .bind("bar", 56.25)
        .bind("id", 12345L)
        .addBatch()
        .bind("foo", "someOtherValue")
        .bind("bar", 25.56)
        .bind("id", 54321L)
        .executeBatch();
        
```

### Insert
For simple use-cases only.

```java
// TODO: Return boolean instead of int, since a SIMPLE insert can only affect one row
// TODO: Consider auto-increment columns
int rowsAffected =
    SimpleJdbc.using(dataSource)
        .insert()
        .into("some_table")
        .set("column1", 123)
        .set("column2", 456)
        .execute();
```

### Update
For simple use-cases only.

```java
int rowsAffected =
    SimpleJdbc.using(dataSource)
        .update()
        .table("some_table")
        .set("foo", 123)
        .set("bar", 456)
        .where("id = :id and some_other_field = :someOtherField")
        .bind("id", 12345L)
        .bind("someOtherField", "someValue")
        .execute();
```

It's possible to perform the update without specifying any conditions, but since this is relatively
rare, and because it's dangerous to do this by mistake (updating every row in a table when you meant
only to update one would be... _unpleasant_), you have to call a special method so it's more difficult
to do it accidentally:

```java
int rowsAffected =
    SimpleJdbc.using(dataSource)
        .update()
        .table("some_table")
        .set("foo", 123)
        .set("bar", 456)
        .executeUnconditionally();
```

### Transactions
```java
SimpleJdbc.using(dataSource)
    .transactionally(simpleJdbc -> {
      // use lambda parameter (a SimpleJdbc instance) to do stuff to the database 
      // automatically calls rollback() on underlying Connection if an exception is raised
      // or commit() at end of function if no exceptions raised
    });
```

## TODO

### Definitely
1. Transactions
    * Prevent creation of nested transactions (because they wouldn't work correctly anyway)
    * Support control over transaction isolation level
    * Integration test (with real DB(s))
2. Enhanced DSL for queries - exists(), single(), first(), list()
3. Add more integration tests with more DB types (at minimum: MySQL & Oracle)
4. Clob and Blob support
5. Javadoc
6. SimpleJdbcException subtypes (different errors, different exception types)

### Maybe
1. QueryRowResult stream()
3. SQL _script_ support (likely vendor-specific... if so, a no-go)