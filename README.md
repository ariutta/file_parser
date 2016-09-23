# fixed2sql
Watch directory for new fixed-width input files and load into SQL database on drop.

## Install

Install [Node.js](https://nodejs.org/en/download/) and [PostgreSQL](https://www.postgresql.org/download/).

Clone and install this repo:

```
git clone https://github.com/ariutta/fixed2sql.git
cd ./fixed2sql
npm install
```

Create production and test PostgreSQL databases:

```
CREATE DATABASE fixed2sql;
CREATE DATABASE fixed2sqltest;
```

Create a top-level `.env` file (same level as this `README`) and add connection strings for production and test PostgreSQL databases:

```
DATABASE_URL=postgres://127.0.0.1:5432/fixed2sql
TEST_DATABASE_URL=postgres://127.0.0.1:5432/fixed2sqltest
```

Test:
```
npm test
```

Run:

```
fixed2sql
```

or if that doesn't work:

```
node ./bin/fixed2sql.js
```

If the appropriate specification file(s) are in the `spec/` directory, you can now drop data files into the `data/` directory.
