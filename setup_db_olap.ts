import { Client } from "pg";
import { faker } from "@faker-js/faker";

export async function setupOlap() {
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
    await client.query("DROP DATABASE IF EXISTS olap WITH (FORCE)");
    console.log("Database dropped.");

    await client.query("CREATE DATABASE olap");
    console.log("Database created.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.end();
  }
}

async function seed() {
  const client = new Client({
    database: "olap",
    host: "localhost",
  });

  await client.connect();

  console.log("Creating Tables...");

  await client.query(`
    DROP TABLE IF EXISTS fact_sales;
    DROP TABLE IF EXISTS dim_date;
    DROP TABLE IF EXISTS dim_product;
    DROP TABLE IF EXISTS dim_customer;
    DROP TABLE IF EXISTS dim_region;

    CREATE TABLE dim_date (
      id SERIAL PRIMARY KEY,
      full_date DATE,
      year INT,
      quarter INT,
      month INT,
      day INT,
      day_of_week INT,
      is_weekend BOOLEAN
    );

    CREATE TABLE dim_region (
      id SERIAL PRIMARY KEY,
      country TEXT,
      state TEXT,
      city TEXT
    );

    CREATE TABLE dim_customer (
      id SERIAL PRIMARY KEY,
      name TEXT,
      email TEXT,
      segment TEXT,
      region_id INT REFERENCES dim_region(id)
    );

    CREATE TABLE dim_product (
      id SERIAL PRIMARY KEY,
      name TEXT,
      category TEXT,
      subcategory TEXT,
      brand TEXT,
      unit_cost DECIMAL(10,2)
    );

    CREATE TABLE fact_sales (
      id SERIAL PRIMARY KEY,
      date_id INT REFERENCES dim_date(id),
      customer_id INT REFERENCES dim_customer(id),
      product_id INT REFERENCES dim_product(id),
      region_id INT REFERENCES dim_region(id),
      quantity INT,
      unit_price DECIMAL(10,2),
      discount DECIMAL(5,2),
      total_amount DECIMAL(12,2)
    );
  `);

  const BATCH_SIZE = 1000;
  const segments = ["Consumer", "Corporate", "Enterprise", "Government"];
  const categories = ["Electronics", "Clothing", "Home", "Sports", "Food"];
  const subcategories: Record<string, string[]> = {
    Electronics: ["Phones", "Laptops", "Tablets", "Accessories"],
    Clothing: ["Shirts", "Pants", "Shoes", "Outerwear"],
    Home: ["Furniture", "Kitchen", "Decor", "Lighting"],
    Sports: ["Equipment", "Apparel", "Footwear", "Accessories"],
    Food: ["Snacks", "Beverages", "Dairy", "Produce"],
  };
  const brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"];

  // dim_date: one row per day for 3 years (2022-2024)
  console.log("Seeding dim_date...");

  const startDate = new Date("2022-01-01");
  const endDate = new Date("2024-12-31");
  const dates: Date[] = [];
  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    dates.push(new Date(d));
  }

  for (let i = 0; i < dates.length; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    const batch = dates.slice(i, i + BATCH_SIZE);
    for (let j = 0; j < batch.length; j++) {
      const d = batch[j];
      const idx = j * 7;
      values.push(
        `($${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4}, $${idx + 5}, $${idx + 6}, $${idx + 7})`,
      );
      const dow = d.getDay();
      params.push(
        d.toISOString().split("T")[0],
        d.getFullYear(),
        Math.floor(d.getMonth() / 3) + 1,
        d.getMonth() + 1,
        d.getDate(),
        dow,
        dow === 0 || dow === 6,
      );
    }
    await client.query(
      `INSERT INTO dim_date (full_date, year, quarter, month, day, day_of_week, is_weekend) VALUES ${values.join(",")}`,
      params,
    );
  }

  const totalDates = dates.length;
  console.log(`  ${totalDates} dates inserted.`);

  // dim_region: 500 regions
  console.log("Seeding dim_region...");

  const TOTAL_REGIONS = 500;
  for (let i = 0; i < TOTAL_REGIONS; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < TOTAL_REGIONS; j++) {
      const idx = j * 3;
      values.push(`($${idx + 1}, $${idx + 2}, $${idx + 3})`);
      params.push(
        faker.location.country(),
        faker.location.state(),
        faker.location.city(),
      );
    }
    await client.query(
      `INSERT INTO dim_region (country, state, city) VALUES ${values.join(",")}`,
      params,
    );
  }

  // dim_customer: 500,000 customers
  console.log("Seeding dim_customer...");

  const TOTAL_CUSTOMERS = 500000;
  for (let i = 0; i < TOTAL_CUSTOMERS; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < TOTAL_CUSTOMERS; j++) {
      const idx = j * 4;
      values.push(`($${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4})`);
      params.push(
        faker.person.fullName(),
        `cust${i + j}_${faker.internet.email()}`,
        segments[faker.number.int({ min: 0, max: 3 })],
        faker.number.int({ min: 1, max: TOTAL_REGIONS }),
      );
    }
    await client.query(
      `INSERT INTO dim_customer (name, email, segment, region_id) VALUES ${values.join(",")}`,
      params,
    );
  }

  // dim_product: 1,000 products
  console.log("Seeding dim_product...");

  const TOTAL_PRODUCTS = 1000;
  for (let i = 0; i < TOTAL_PRODUCTS; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < TOTAL_PRODUCTS; j++) {
      const idx = j * 5;
      values.push(
        `($${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4}, $${idx + 5})`,
      );
      const cat = categories[faker.number.int({ min: 0, max: 4 })];
      const subs = subcategories[cat];
      params.push(
        faker.commerce.productName(),
        cat,
        subs[faker.number.int({ min: 0, max: subs.length - 1 })],
        brands[faker.number.int({ min: 0, max: 4 })],
        faker.commerce.price({ min: 1, max: 200 }),
      );
    }
    await client.query(
      `INSERT INTO dim_product (name, category, subcategory, brand, unit_cost) VALUES ${values.join(",")}`,
      params,
    );
  }

  // fact_sales: 10,000,000 rows
  console.log("Seeding fact_sales...");

  const TOTAL_SALES = 10000000;
  for (let i = 0; i < TOTAL_SALES; i += BATCH_SIZE) {
    const values: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE && i + j < TOTAL_SALES; j++) {
      const idx = j * 8;
      values.push(
        `($${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4}, $${idx + 5}, $${idx + 6}, $${idx + 7}, $${idx + 8})`,
      );
      const qty = faker.number.int({ min: 1, max: 20 });
      const unitPrice = parseFloat(faker.commerce.price({ min: 5, max: 500 }));
      const discount = parseFloat((Math.random() * 0.3).toFixed(2));
      const total = parseFloat((qty * unitPrice * (1 - discount)).toFixed(2));
      params.push(
        faker.number.int({ min: 1, max: totalDates }),
        faker.number.int({ min: 1, max: TOTAL_CUSTOMERS }),
        faker.number.int({ min: 1, max: TOTAL_PRODUCTS }),
        faker.number.int({ min: 1, max: TOTAL_REGIONS }),
        qty,
        unitPrice,
        discount,
        total,
      );
    }
    await client.query(
      `INSERT INTO fact_sales (date_id, customer_id, product_id, region_id, quantity, unit_price, discount, total_amount) VALUES ${values.join(",")}`,
      params,
    );
  }

  console.log("Done! OLAP database populated.");
  await client.end();
}

import { fileURLToPath } from "url";
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  setupOlap().catch(console.error);
}
