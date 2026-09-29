# Test data

- `dvdrental/` (a `pg_restore` directory dump) and `dvdrental_mysql.sql`: the DVD Rental
  sample database, a port of MySQL's [Sakila sample database](https://dev.mysql.com/doc/sakila/en/).
  The Sakila schema and data are licensed under the New BSD license
  ([license](https://dev.mysql.com/doc/sakila/en/sakila-license.html)). The data is fictional
  and is used here only for the demo and the integration tests.
- `hostile.sql`: this project's own fixture covering every decode edge case the Postgres
  integration tests check (see the header comment in the file).
