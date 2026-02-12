import { Client } from "pg";
import { faker } from "@faker-js/faker";

export async function setupOltp() {
  await manageDatabase();
  await seed();
}

async function manageDatabase() {
  const client = new Client({
    user: "samlewis",
    host: "localhost",
    database: "postgres",
    port: 5432,
  });

  await client.connect();

  try {
    await client.query("DROP DATABASE IF EXISTS oltp WITH (FORCE)");
    console.log("Database dropped.");

    await client.query("CREATE DATABASE oltp");
    console.log("Database created.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.end();
  }
}

async function seed() {
  const client = new Client({
    database: "oltp",
    host: "localhost",
  });

  await client.connect();

  console.log("Cleaning and Creating Tables...");

  await client.query(`
    DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS users;

    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      email TEXT UNIQUE,
      name TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE products (
      id SERIAL PRIMARY KEY,
      name TEXT,
      price DECIMAL(10,2),
      stock INT
    );

    CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      user_id INT REFERENCES users(id),
      total_price DECIMAL(10,2),
      status TEXT
    );

    CREATE TABLE order_items (
      id SERIAL PRIMARY KEY,
      order_id INT REFERENCES orders(id),
      product_id INT REFERENCES products(id),
      quantity INT
    );
  `);

  const BATCH_SIZE = 1000;
  const statuses = ["pending", "shipped", "delivered", "cancelled"];

  console.log("Seeding Users...");

  for (let i = 0; i < 1000000; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < 1000000; j++) {
      const idx = j * 2;
      values.push(`($${idx + 1}, $${idx + 2})`);
      params.push(`user${i + j}_${faker.internet.email()}`, faker.person.fullName());
    }
    await client.query(
      `INSERT INTO users (email, name) VALUES ${values.join(",")}`,
      params,
    );
  }

  console.log("Seeding Products...");

  {
    const values: string[] = [];
    const params: any[] = [];
    for (let i = 0; i < 200; i++) {
      const idx = i * 3;
      values.push(`($${idx + 1}, $${idx + 2}, $${idx + 3})`);
      params.push(
        faker.commerce.productName(),
        faker.commerce.price(),
        faker.number.int({ min: 1, max: 100 }),
      );
    }
    await client.query(
      `INSERT INTO products (name, price, stock) VALUES ${values.join(",")}`,
      params,
    );
  }

  console.log("Seeding Orders...");

  for (let i = 0; i < 2000000; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < 2000000; j++) {
      const idx = j * 3;
      values.push(`($${idx + 1}, $${idx + 2}, $${idx + 3})`);
      params.push(
        faker.number.int({ min: 1, max: 1000000 }),
        faker.commerce.price({ min: 5, max: 500 }),
        statuses[faker.number.int({ min: 0, max: 3 })],
      );
    }
    await client.query(
      `INSERT INTO orders (user_id, total_price, status) VALUES ${values.join(",")}`,
      params,
    );
  }

  console.log("Seeding Order Items...");

  for (let i = 0; i < 5000000; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < 5000000; j++) {
      const idx = j * 3;
      values.push(`($${idx + 1}, $${idx + 2}, $${idx + 3})`);
      params.push(
        faker.number.int({ min: 1, max: 2000000 }),
        faker.number.int({ min: 1, max: 200 }),
        faker.number.int({ min: 1, max: 10 }),
      );
    }
    await client.query(
      `INSERT INTO order_items (order_id, product_id, quantity) VALUES ${values.join(",")}`,
      params,
    );
  }

  console.log("Done! Database populated.");
  await client.end();
}

import { fileURLToPath } from "url";
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  setupOltp().catch(console.error);
}
