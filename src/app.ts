import { env } from "node:process";
import { faker } from "@faker-js/faker";
import { MongoClient } from "mongodb";

const CUSTOMERS_COLLECTION = "customers";

const CUSTOMERS_PER_BATCH = {
  min: 1,
  max: 10,
};

const BATCH_INTERVAL = 200;

function generateCustomers() {
  const batchSize = faker.number.int(CUSTOMERS_PER_BATCH);

  const customers = [];

  for (let i = 0; i < batchSize; ++i) {
    const customer = {
      firstName: faker.person.firstName(),
      lastName: faker.person.lastName(),
      email: faker.internet.email(),
      address: {
        line1: faker.location.streetAddress(),
        line2: faker.location.secondaryAddress(),
        postcode: faker.location.zipCode(),
        city: faker.location.city(),
        state: faker.location.state({ abbreviated: true }),
        country: faker.location.countryCode(),
      },
      createdAt: new Date(),
    };

    customers.push(customer);
  }

  return customers;
}

async function main() {
  const client = await MongoClient.connect(<string>env.DB_URI);

  const db = client.db();
  const collection = db.collection(CUSTOMERS_COLLECTION);

  const timer = setInterval(async () => {
    const customers = generateCustomers();
    await collection.insertMany(customers);
    console.log(
      `[${new Date().toISOString()}] Inserted ${customers.length} customers`
    );
  }, BATCH_INTERVAL);

  process.on("SIGINT", () => {
    console.log(
      `[${new Date().toISOString()}] Got SIGINT. Graceful shutdown start`
    );
    clearInterval(timer);
    client.close();
  });
}

main().catch(console.dir);
