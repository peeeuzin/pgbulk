import { faker } from "@faker-js/faker";
import { createWriteStream } from "fs";
import { Readable, pipeline } from "stream";
import { promisify } from "util";
import path from "path";
import { stringify } from "csv";

// Promisify pipeline for easier async/await usage.
const pipelineAsync = promisify(pipeline);

/**
 * Generator function that yields fake data objects.
 * Each object has a `name` and an `email` property.
 *
 * @param count - The number of fake records to generate.
 */
function* generateFakeData(count: number) {
  for (let i = 0; i < count; i++) {
    const address_id = faker.string.uuid();

    yield {
      users_id: faker.string.uuid(),
      users_age: faker.number.int(),
      users_name: faker.person.fullName(),
      users_nickname: faker.internet.displayName(),
      users_address: address_id,

      addresses_id: address_id,
      addresses_street: faker.location.street(),
      addresses_state: faker.location.state(),
    };
  }
}

/**
 * Main function that generates the CSV file and then parses it.
 */
async function main() {
  // Retrieve the count from the command line.
  // Usage: ts-node generateCsv.ts 1000
  const arg = process.argv[2] || 1000;
  if (!arg) {
    console.error(
      "Please provide the number of records to generate as a command line argument."
    );
    process.exit(1);
  }

  const count = Number(arg);
  if (isNaN(count) || count < 1) {
    console.error("The provided argument must be a positive number.");
    process.exit(1);
  }

  const outputFile = path.resolve(__dirname, "data", "faker.csv");

  // Create a readable stream from the generated fake data.
  const fakeDataStream = Readable.from(generateFakeData(count));

  // Create a writable stream to the output CSV file.
  const writableStream = createWriteStream(outputFile);

  try {
    await pipelineAsync(
      fakeDataStream,
      stringify({
        quoted: true,
        header: true,
      }),
      writableStream
    );
    console.log(`CSV file '${outputFile}' created with ${count} records.`);
  } catch (err) {
    console.error("Pipeline failed:", err);
    process.exit(1);
  }
}

// Execute the main function.
main().catch((err) => {
  console.error("Error in main:", err);
});
