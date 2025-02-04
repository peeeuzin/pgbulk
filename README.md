# pgbulk
![GitHub License](https://img.shields.io/github/license/peeeuzin/populate)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-pr/peeeuzin/populate)

## What's pgbulk?
A library for bulk inserts into PostgreSQL with TypeScript, you just need to parse it.

Populate can be used for bulk insert CSV files into PostgreSQL with performance.

## Installation
You can install the package using npm:
```bash
npm install pgbulk # npm 
yarn add pgbulk # yarn
pnpm add pgbulk # pnpm 
```

## Example
```ts
import { PGBulk } from 'pgbulk';

class Users extends PGBulk {
  constructor(connectionURL: string) {
    super({
      connection: {
        connectionString: connectionURL,
      },
      strategy: "csv",
      tables: {
        users: [
          {
            databaseColumn: "id",
            type: "TEXT",
          },

          {
            databaseColumn: "name",
            type: "TEXT",
          },

          {
            databaseColumn: "nickname",
            csvColumn: "aka",
            type: "TEXT",
          },
        ]
      },
      allowDisableIndexes: true,
      allowDisableForeignKeys: true,
    })

    this.register("pathto/data", "users-*.csv");
  },

  await parse(row: Row) {
    // do some parsing thing
    // ...
    return row
  }
}

const usersPopulate = new Users("<postgres_url>");

await usersPopulate.start();
await usersPopulate.finish();
```