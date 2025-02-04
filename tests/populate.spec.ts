import path from "path";
import { Populate } from "../src";
import { createCSVFile } from "../utils/faker";
import { Client } from "pg";

describe("Populate", () => {
  const connectionURL =
    "postgresql://postgres:postgres@localhost:5432/populate-test";

  jest.setTimeout(120000);

  beforeAll(async () => {
    const client = new Client({
      connectionString: connectionURL,
    });

    await client.connect();

    await client.query(`
    CREATE TABLE IF NOT EXISTS addresses (
      id TEXT PRIMARY KEY,
      street TEXT NOT NULL,
      state TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      age INT NOT NULL,
      name TEXT NOT NULL,
      nickname TEXT,
      address TEXT,

      FOREIGN KEY ("address") REFERENCES "addresses" ("id") ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS "name_index" ON users USING btree (name);

    TRUNCATE TABLE users CASCADE;
    TRUNCATE TABLE addresses CASCADE;
    `);

    await client.end();
  });

  it("should start populate files", async () => {
    const populate = new Populate({
      connection: {
        connectionString: connectionURL,
      },
      strategy: "csv",
      quiet: true,
      csvConfig: {
        headers: [
          "id",
          "age",
          "name",
          "nickname",
          "address_id",
          "street",
          "state",
        ],
      },
      tables: {
        addresses: [
          {
            databaseColumn: "id",
            csvColumn: "address_id",
            type: "TEXT",
          },
          {
            databaseColumn: "street",
            type: "TEXT",
          },
          {
            databaseColumn: "state",
            type: "TEXT",
          },
        ],

        users: [
          {
            databaseColumn: "id",
            type: "TEXT",
          },
          {
            databaseColumn: "age",
            type: "INT",
          },
          {
            databaseColumn: "name",
            type: "TEXT",
          },
          {
            databaseColumn: "nickname",
            type: "TEXT",
          },
          {
            databaseColumn: "address",
            type: "TEXT",
            ref: "address_id",
          },
        ],
      },
      allowDisableIndexes: true,
      allowDisableForeignKeys: true,
    });

    await Promise.all([
      await createCSVFile(
        path.join(__dirname, "data", "test-populate-0.csv"),
        10_000
      ),

      await createCSVFile(
        path.join(__dirname, "data", "test-populate-1.csv"),
        10_000
      ),
    ]);

    await populate.register(
      path.join(__dirname, "data"),
      "test-populate-*.csv"
    );

    await populate.start();
    await populate.finish();
  });
});
