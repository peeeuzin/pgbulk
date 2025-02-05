import path from "path";
import { PGBulk } from "../src";
import { createCSVFile } from "../utils/faker";
import { Client } from "pg";

describe("Populate", () => {
  const connectionURL =
    "postgresql://postgres:postgres@localhost:5432/pgbulk-test";
  const client = new Client({
    connectionString: connectionURL,
  });

  jest.setTimeout(120000);

  beforeAll(async () => {
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
  });

  beforeEach(async () => {
    await client.query(`
    TRUNCATE TABLE users CASCADE;
    TRUNCATE TABLE addresses CASCADE;`);
  });

  afterAll(async () => {
    await client.end();
  });

  it("should start populate files", async () => {
    class PGBulkTest extends PGBulk {
      constructor(connectionURL: string) {
        super({
          connection: {
            connectionString: connectionURL,
          },
          strategy: "csv",
          quiet: true,
          csvConfig: {
            columns: [
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

        this.register(path.join(__dirname, "data"), "test-populate-*.csv");
      }
    }

    const populate = new PGBulkTest(connectionURL);

    expect(populate.usingTemporaryTableStrategy).toBe(true);

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

    await populate.start();

    const usersRowCount = (await client.query("SELECT * FROM users")).rowCount;
    const addressesRowCount = (await client.query("SELECT * FROM addresses"))
      .rowCount;

    expect(usersRowCount).toBe(20_000);
    expect(addressesRowCount).toBe(20_000);
  });

  it("should use not temporary strategy", async () => {
    class PGBulkTest extends PGBulk {
      constructor(connectionURL: string) {
        super({
          connection: {
            connectionString: connectionURL,
          },
          strategy: "csv",
          quiet: true,
          csvConfig: {
            columns: [
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
          },
          allowDisableIndexes: true,
          allowDisableForeignKeys: true,
        });

        this.register(path.join(__dirname, "data"), "test-populate-*.csv");
      }
    }

    const populate = new PGBulkTest(connectionURL);

    expect(populate.usingTemporaryTableStrategy).toBe(false);

    await Promise.all([
      await createCSVFile(
        path.join(__dirname, "data", "test-populate-0.csv"),
        1_000
      ),

      await createCSVFile(
        path.join(__dirname, "data", "test-populate-1.csv"),
        1_000
      ),
    ]);

    await populate.start();

    const usersRowCount = (await client.query("SELECT * FROM users")).rowCount;
    const addressesRowCount = (await client.query("SELECT * FROM addresses"))
      .rowCount;

    expect(usersRowCount).toBe(0);
    expect(addressesRowCount).toBe(2_000);
  });
});
