import { argv, env } from "node:process";
import fs from "fs/promises";
import {
  ChangeStreamInsertDocument,
  Collection,
  MongoBulkWriteError,
  MongoClient,
} from "mongodb";
import { faker } from "@faker-js/faker";

const CUSTOMERS_COLLECTION_NAME = "customers";

const ANON_CUSTOMERS_COLLECTION_NAME = "customers_anonymised";

const BATCH_SIZE = 1000;

const BATCH_INTERVAL = 1000;

const RESUME_TOKEN_PATH = "./resume_token.txt";

type Customer = {
  firstName: string;
  lastName: string;
  email: string;
  address: {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
  };
  createdAt: Date;
};

function anonymizeCustomer(customer: Customer): Customer {
  const firstName = faker.string.alphanumeric(8);
  const lastName = faker.string.alphanumeric(8);
  const emailProvider = customer.email.substring(customer.email.indexOf("@"));
  const email = `${faker.string.alphanumeric(8)}${emailProvider}`;
  const line1 = faker.string.alphanumeric(8);
  const line2 = faker.string.alphanumeric(8);
  const postcode = faker.string.alphanumeric(8);

  return {
    ...customer,
    firstName,
    lastName,
    email,
    address: {
      ...customer.address,
      line1,
      line2,
      postcode,
    },
  };
}

async function syncFull(
  customers: Collection<Customer>,
  customersAnonymized: Collection<Customer>
) {
  console.log(`[${new Date().toISOString()}] Full synchronization mode`);

  await customersAnonymized.deleteMany();

  const cursor = customers.find().sort({ createdAt: "asc" });
  let i = 0;
  let bulkOp = customersAnonymized.initializeUnorderedBulkOp();

  try {
    for await (const customer of cursor) {
      bulkOp.insert(anonymizeCustomer(customer));
      ++i;
      if (i === BATCH_SIZE) {
        await bulkOp.execute();
        i = 0;
        bulkOp = customersAnonymized.initializeUnorderedBulkOp();
        console.log(
          `[${new Date().toISOString()}] Inserted ${i} anonymized customers (full-reindex)`
        );
      }
    }

    if (i > 0) {
      await bulkOp.execute();
      console.log(
        `[${new Date().toISOString()}] Inserted ${i} anonymized customers (full-reindex)`
      );
    }
  } catch (error) {
    if (error instanceof MongoBulkWriteError && error.code === 11000) {
      console.warn(
        `[${new Date().toISOString()}] Encountered duplicate key error collection, aborting (full-reindex)`
      );
    } else {
      throw error;
    }
  }
  console.log(`[${new Date().toISOString()}] Full synchronization complete`);
}

async function syncRealTime(
  customers: Collection<Customer>,
  customersAnonymized: Collection<Customer>
) {
  console.log(`[${new Date().toISOString()}] Real-time synchronization mode`);

  let initResumeToken: string | undefined;
  try {
    initResumeToken = await fs.readFile(RESUME_TOKEN_PATH, {
      encoding: "ascii",
    });
    console.log(
      `[${new Date().toISOString()}] Continuing with resume token: ${initResumeToken}`
    );
  } catch (err) {
    console.error(
      `[${new Date().toISOString()}] Continuing without resume token`
    );
  }

  let batch: { customer: Customer; resumeToken: string }[] = [];

  const changeStream = customers
    .watch<Customer, ChangeStreamInsertDocument<Customer>>(
      [{ $match: { operationType: "insert" } }],
      {
        fullDocument: "required",
        resumeAfter: initResumeToken ? { _data: initResumeToken } : undefined,
      }
    )
    .on("change", ({ fullDocument }) => {
      batch.push({
        customer: anonymizeCustomer(fullDocument),
        resumeToken: (<{ _data: string }>changeStream.resumeToken)._data,
      });
    });

  const timer = setInterval(() => {
    if (!batch.length) {
      return;
    }

    const batchToInsert = batch.slice(0, BATCH_SIZE);
    batch = batch.slice(BATCH_SIZE);

    const resumeToken = batchToInsert.at(-1)!.resumeToken;

    customersAnonymized
      .insertMany(
        batchToInsert.map(({ customer }) => customer),
        { ordered: false }
      )
      .then(() =>
        fs.writeFile(RESUME_TOKEN_PATH, resumeToken, {
          encoding: "ascii",
        })
      )
      .then(() => {
        console.log(
          `[${new Date().toISOString()}] Inserted ${
            batchToInsert.length
          } anonymized customers (real-time)`
        );
      })
      .catch((error) => {
        if (error instanceof MongoBulkWriteError && error.code === 11000) {
          console.warn(
            `[${new Date().toISOString()}] Encountered duplicate key error collection, continuing (real-time)`
          );
          return;
        }
        throw error;
      });
  }, BATCH_INTERVAL);

  return { changeStream, timer };
}

async function main() {
  const client = await MongoClient.connect(<string>env.DB_URI);
  const db = client.db();
  const customers = db.collection<Customer>(CUSTOMERS_COLLECTION_NAME);
  const customersAnonymized = db.collection<Customer>(
    ANON_CUSTOMERS_COLLECTION_NAME
  );

  if (argv.includes("--full-reindex")) {
    process.on("SIGINT", () => {
      console.log(
        `[${new Date().toISOString()}] Got SIGINT. Graceful shutdown start`
      );
      client.close();
    });

    await syncFull(customers, customersAnonymized);
    await client.close();
    process.exit(0);
  } else {
    const { changeStream, timer } = await syncRealTime(
      customers,
      customersAnonymized
    );

    process.on("SIGINT", () => {
      console.log(
        `[${new Date().toISOString()}] Got SIGINT. Graceful shutdown start`
      );
      clearInterval(timer);
      changeStream.close().then(() => client.close());
    });
  }
}

main().catch(console.dir);
