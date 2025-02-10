import { faker } from "@faker-js/faker";
import { createWriteStream } from "fs";
import { Readable, pipeline } from "stream";
import { promisify } from "util";
import { stringify } from "csv-stringify";

const pipelineAsync = promisify(pipeline);

function* generateFakeData(count: number) {
  for (let i = 0; i < count; i++) {
    yield {
      id: faker.string.uuid(),
      age: faker.number.int({ max: 100, min: 1 }),
      name: faker.person.fullName(),
      nickname: faker.internet.displayName(),

      address_id: faker.string.uuid(),
      street: faker.location.street(),
      state: faker.location.state(),
    };
  }
}

export async function createCSVFile(path: string, count: number) {
  // const outputFile = path.resolve(__dirname, "data", "faker.csv");
  const fakeDataStream = Readable.from(generateFakeData(count));

  const writableStream = createWriteStream(path);

  await pipelineAsync(
    fakeDataStream,
    stringify({
      quoted: true,
    }),
    writableStream
  );
}
