# SimpleJdbc

Simple wrapper library for dealing with SQL-in-Java and JDBC. No dependencies, no reflection.

## Features

* Convenient, fluent syntax
* Named Parameters (`:foo`, `:bar` instead of `?`, `?`)
* Always uses `PreparedStatement` under the hood, helping to avoid SQL injection attacks
* `QueryResult` wraps and fixes issues with `ResultSet`:
    * Boxed types (`Integer` instead of `int`) to make dealing with `null`-valued columns easier
    * Optional use of `Optional` for `null`-valued columns if that's your preferred style
    * Favors `java.time` classes instead of `java.sql` classes
    * "Escape hatch" lets you use underlying `ResultSet` for advanced use-cases
* Basic transaction support
* Avoids "magic" - no reflection, no code generation, no dynamic proxies

## Statement

```java
int rowsAffected =
    SimpleJdbc.using(dataSource)
        .statement("update some_table set foo = :foo, bar = :bar where id = :id")
        .bind("foo", "someValue")
        .bind("bar", 56.25)
        .bind("id", 12345L)
        .execute();
```

## Batch Statement

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

## Insert

```java
int rowsAffected =
    SimpleJdbc.using(dataSource)
        .insert()
        .into("some_table")
        .set("column1", 123)
        .set("column2", 456)
        .execute();
```

## Batch Insert

```java
int[] rowsAffected =
    SimpleJdbc.using(dataSource)
        .batchInsert()
        .into("some_table")
        .set("column1", 123)
        .set("column2", "abc")
        .addBatch()
        .set("column1", 456)
        .set("column2", "def")
        .addBatch()
        .executeBatch();
```

## Update

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
        .set("foo", 123)
        .set("bar", 456)
        .executeUnconditionally();
```

## Query

```java
var result =
    SimpleJdbc.using(dataSource)
        .query("select foo, bar, baz from some_table where id = :id and thing = :thing")
        .bind("id",123)
        .bind("thing",456)
        .select(queryResult -> /* do interesting things with QueryResult here */);
```

### Query Result Extraction

In the above example, the `select()` method accepts a `QueryResultExtractor`, which is a functional
interface (lambda-compatible) having the following signature:

```java
public interface QueryResultExtractor<T> {
  T extract(QueryResult queryResult) throws SQLException;
}
``` 

What this means is you can pass the `select()` method a lambda which accepts a single parameter
(`QueryResult`) and returns anything you want (`T`). The result computed by your `QueryResultExtractor`
lambda is what ends up being returned from the `select()` call.

> If you are familiar with Spring's `JdbcTemplate` class, you will notice that `QueryResultExtractor`
> fills the same role as Spring JDBC's `ResultSetExtractor`. As the names imply, the main difference
> between these two are that while a `ResultSetExtrator` operates on a `ResultSet`, a
> `QueryResultExtractor` operates on a `QueryResult`.

It is important to note that when using the `select()` method, you need to manually advance the 
cursor through the query results, just like with a `ResultSet`. There are a number of convenience
methods that save you this trouble (described later).

### Differences between `QueryResult` and `ResultSet`

A `QueryResult` is just a thin wrapper around a `ResultSet`, exposing a smaller, opinionated interface
with some useful differences in behavior.

#### Only column labels allowed

Unlike `ResultSet`, which exposes both `getX(int columnIndex)` and `getX(String columnLabel)`, the
only methods exposed by `QueryResult` use column labels. The author of this library is of the opinion
that referencing query results (or parameters) by index is a smell that makes code more prone to bugs
in the future. 

> NOTE to readers of this documentation: if you disagree or otherwise think it would be useful to
> elaborate on this point, feel free to open an issue to enhance the docs :-)

To illustrate the difference:

```java
String sql = "select foo from some_table";

ResultSet resultSet = getResultSetFrom(sql);
resultSet.getString("foo"); // works
resultSet.getString(1);     // also works
        
QueryResult queryResult = getQueryResultFrom(sql);
queryResult.getString("foo"); // works
// queryResult.getString(1);  // would not compile
```

#### Boxed types over primitives

Because `ResultSet` returns primitive types whenever possible, dealing with `null`-valued columns
can be problematic (if you care about accurately representing the column value):

```java
// Given a ResultSet retrieved from the following query:
// select 5 as foo, null as bar from some_table

Integer foo = resultSet.getInt("foo"); // returns 5
Integer bar = resultSet.getInt("bar"); // returns 0   <--- this is bad! null is not zero!
```

Of course, you _can_ determine if a column was `null`, but you have to explicitly check for it after
every `getX()` call, like this:

```java
Integer baz = resultSet.getInt("baz"); // returns 0
if (resultSet.wasNull()) {             // condition is true, because the last value was *actually* null
  baz = null;                          // there I fixed it
}
```

On the other hand, `QueryResult` returns boxed types, so when a column is `null`, you get back `null`:

```java
// Same query & ResultSet, but accessed via a QueryResult

Integer foo = queryResult.getInteger("foo"); // returns 5
Integer bar = queryResult.getInteger("bar"); // returns null
```

#### Optionally, you can use `Optional`

If you prefer it, `QueryResult` can return `Optional<T>` instead of plain boxed types. Just call the
`opt()` method to switch to the "Optional View", where the return types of all the `getX()` methods
get wrapped in an `Optional`:

```java
// Normal "boxed" mode
QueryResult queryResult = ...;
Integer foo = queryResult.getInteger("foo");
        
// Optional mode
QueryResult.OptionalView optQueryResult = queryResult.opt();
Optional<Integer> foo2 = optQueryResult.getInteger("foo");
```

If you prefer boxed mode in general, but want an Optional occasionally, it is canonical to reach for
`opt()` inline whenever you need it:

```java
QueryResult queryResult = ...;

MyThing thing = 
    MyThing.builder()
        .setFoo(queryResult.getInteger("foo"))
        .setBar(queryResult.getLong("bar"))
        .setBaz(queryResult.opt().getString("baz").orElse("some default value"))
        .setQux(queryResult.getInstant("qux"))
        .setQuux(queryResult.opt().getInteger("quux").orElseThrow(() -> new MissingQuuxException()))
        .build();
```

#### Favors `java.time` over `java.sql` types

`QueryResult` automatically converts date/time-related `java.sql` types to `java.time` types so that 
you don't have to. Nothing too special here - we're just calling a method on the `java.sql` types
themselves to achieve this, but we handle `null` as you'd expect it to be handled.

Available methods:

```java
QueryResult queryResult = ...;

queryResult.getInstant("foo");       // from java.sql.Timestamp
queryResult.getLocalDateTime("bar"); // from java.sql.Timestamp
queryResult.getLocalDate("baz");     // from java.sql.Date
queryResult.getLocalTime("qux");     // from java.sql.Time
```

#### Escape Hatch

While `QueryResult` is more convenient to use, it is not as feature-packed as `ResultSet`. Some
glaring omissions include support for `CLOB` or `BLOB` types, and many "advanced" use-cases.
Thankfully, when adopting `SimpleJdbc` and using `QueryResult`, you don't have to throw the baby out
with the bathwater.

It is possible to easily access the underlying `ResultSet` instance from any `QueryResult`:

```java
ResultSet resultSet = queryResult.toResultSet();
```

## Query - convenience methods

Using `query()...select()` as described above is great and all, but one annoying chore that `QueryResult`
does not solve for you is the need to call `next()` to advance the cursor through the underlying `ResultSet`.

Thankfully, there are a number of additional convenience methods that _do_ save you this trouble:
* `selectList(queryRowResult -> ...)`
* `selectFirst(queryRowResult -> ...)`
* `selectMaybeOne(queryRowResult -> ...)`
* `selectExactlyOne(queryRowResult -> ...)`
* `selectExists()`

Each of these are described in more detail below, but first it should be noted that the convenience
methods that accept a lambda are provided with a `QueryRowResult` and are called _once per result row_.
This, as opposed to `select(queryResult -> ...)`, which is provided a `QueryResult` and called only one time.

The only material difference between a `QueryResult` and a `QueryRowResult` is that the latter _lacks_
a `next()` method. This is to help programmers avoid shooting themselves in the foot, in case they
forgot they were using a convenience method.

Below you will find additional details about each convenience method:

### Returning a List of Results

Most often, our queries return multiple rows, and we want to treat each row as an individual result.
The `selectList()` method makes this more convenient by invoking the provided callback once per result row, and collecting the
results as a list.

```java
List<Foo> results = simpleJdbc.query(...).selectList(queryRowResult -> /* return Foo */);
```

Just this once, we'll compare the difference between the `select()` method and the convenience method:

```java
// Inconvenient
List<String> foos =
    simpleJdbc.query("select foo from some_table where filter_column = :filterValue")
        .bind("filterValue", "SOME_VALUE")
        .select(
            queryResult -> {
                List<String> returnVal = new ArrayList<>();
                while (queryResult.next()) {
                  returnVal.add(queryResult.getString("foo"));
                }
                return returnVal;
            });

// More Convenient
List<String> foos =
    simpleJdbc.query("select foo from some_table where filter_column = :filterValue")
        .bind("filterValue", "SOME_VALUE")
        .selectList(queryRowResult -> queryRowResult.getString("foo"));
```

### Getting the First Result (if it exists)

The `selectFirst()` method invokes the provided callback for the first result row only, and returns the result
in an `Optional`. If the query yields no results, the returned `Optional` is empty. If the query
yields multiple results, everything beyond the first result is simply ignored.

```java
Optional<Foo> result = simpleJdbc.query(...).selectFirst(queryRowResult -> ...);
```

### Expecting _At Most One_ Result

The `selectMaybeOne()` method behaves exactly like the previous method, with the exception (pun intended) that if
the query yields more than one result, an exception will be thrown rather than returning a result.

```java
Optional<Foo> result = simpleJdbc.query(...).selectMaybeOne(queryRowResult -> ...);
```

### Expecting _Exactly One_ Result

The `selectExactlyOne()` method behaves very similarly to the previous method, but tightens the contract even
further. If the query yields one row, the callback is invoked and result returned directly (not wrapped in `Optional<>`).
If the query yields anything other than a _single_ row, an exception is thrown.

```java
Foo result = simpleJdbc.query(...).selectExactlyOne(queryRowResult -> ...);
```

### Existence Check

Sometimes, you just want to know if there where _any_ results, and you don't even need to extract
anything. The `selectExists()` method simply returns `true` if the query returned any results, `false` otherwise.

```java
boolean isResultExists = simpleJdbc.query(...).selectExists();
```

## Transactions

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

1. More integration tests with more DB vendors (at minimum: MySQL & Oracle)
2. Clob and Blob support
3. More Javadoc
4. SimpleJdbcException subtypes (different errors, different exception types)

### Maybe

1. SQL _script_ support
2. Actively prevent creation of nested transactions (because they wouldn't work correctly anyway)
3. Plugins / hooks? Something that could conveniently implement an in-clause builder
