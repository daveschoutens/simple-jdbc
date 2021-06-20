# SimpleJdbc

Simple wrapper library for dealing with SQL-in-Java and JDBC. No dependencies, no reflection.

# TODO

1. Transactions
    * Prevent creation of nested transactions
    * Support control over transaction isolation level
    * Integration test (with real DB(s))
    * Maybe - Attempt to prevent use of 'outer' SimpleJdbc instance within 'transactionally' block,
      as this is an easy thing to do, but is almost always wrong
2. Enhanced DSL for queries - single(), first(), list()
3. Add more integration tests with more DB types (at minimum: MySQL & Oracle)
4. Clob and Blob support
5. Javadoc
6. SimpleJdbcException subtypes (different errors, different exception types)

# Maybe...

1. QueryRowResult stream()
3. SQL _script_ support (likely vendor-specific... if so, a no-go)