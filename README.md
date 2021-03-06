# SimpleJdbc

Simple wrapper library for dealing with SQL-in-Java and JDBC. No dependencies, no reflection.

## Features

* Convenient, fluent syntax
* Named Parameters (`:foo`, `:bar` instead of `?`, `?`)
* Always uses `PreparedStatement` under the hood, helping to avoid SQL injection attacks
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
List<SomeObject> results =
    SimpleJdbc.using(dataSource)
        .query("select foo, bar, baz from some_table where id = :id and thing = :thing")
        .bind("id",123)
        .bind("thing",456)
        .select(queryResult-> /* your mapping logic here */);
```

#### Query Result Extraction

Zooming in a little bit on the query result extraction, there are several helper methods available
for the most common use-cases.

The most common scenarios are those where each _row_ returned by a query is a single logical
_result_, meaning _multiple rows_ imply _multiple results_.

The following methods are more convenient to use in these situations:

##### Existence Check

The code below simply returns `true` if the query returned any results, `false` otherwise.

```java
boolean isResultExists = simpleJdbc.query(...).selectExists();
```

##### Returning a List of Results

Most often, our queries return multiple rows, and we want to treat each row as an individual result.
The below method makes this more convenient by invoking the provided callback once per result row, and collecting the
results as a list.

```java
List<Foo> results =
    simpleJdbc
        .query(/* query */ )
        .bind(/* bindings */)
        .selectList(queryResult -> /* your mapping logic here */);
```

##### Getting the First Result (if it exists)

The code below invokes the provided callback for the first result row only, and returns the result
in an `Optional`. If the query yields no results, the returned `Optional` is empty. If the query
yields multiple results, everything beyond the first result is simply ignored.

```java
Optional<Foo> result =
    simpleJdbc
        .query(/* query */ )
        .bind(/* bindings */)
        .selectFirst(queryResult -> /* your mapping logic here */);
```

##### Expecting _At Most One_ Result

The code below behaves exactly like the previous method, with the exception (pun intended) that if
the query yields more than one result, it is considered an error, and an exception will be thrown
rather than returning a result.

```java
Optional<Foo> result =
    simpleJdbc
        .query(/* query */ )
        .bind(/* bindings */)
        .selectMaybeOne(queryResult -> /* your mapping logic here */);
```

##### Expecting _Exactly One_ Result

The code below behaves very similarly to the previous method, but tightens the contract even
further. If the query yields one row, the callback is invoked and result returned directly (not
wrapped in an `Optional`). If the query yields anything other than a _single_ result, an exception
is thrown.

```java
Foo result =
    simpleJdbc
        .query(/* query */ )
        .bind(/* bindings */)
        .selectExactlyOne(queryResult -> /* your mapping logic here */);
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

It's possible to perform the update without specifying any `where()` conditions. However, this is
relatively rare, and mistakes can be dangerous (updating **every** row in a table when you meant to
update **only one** would be... _unpleasant_). For that reason, a special method must be called in
order to perform an unconditional update, making it more difficult to do it accidentally:

```java
int rowsAffected =
    SimpleJdbc.using(dataSource)
        .update()
        .table("some_table")
        .set("foo",123)
        .set("bar",456)
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

It is possible to control the transaction isolation level by passing an additional parameter:
```java
SimpleJdbc.using(dataSource)
    .transactionally(
        TransactionIsolationLevel.REPEATABLE_READ,
        simpleJdbc -> {
            // use lambda parameter (a SimpleJdbc instance) to do stuff to the database 
            // automatically calls rollback() on underlying Connection if an exception is raised
            // or commit() at end of function if no exceptions raised
        });
```

## TODO

### Definitely

1. Transactions
    * Prevent creation of nested transactions (because they wouldn't work correctly anyway)
    * Integration test (with real DB(s))
3. Add more integration tests with more DB types (at minimum: MySQL & Oracle)
4. Clob and Blob support
5. Javadoc
6. SimpleJdbcException subtypes (different errors, different exception types)

### Maybe

1. SQL _script_ support (likely vendor-specific... if so, a no-go)