import { Client } from "pg";
import { PrismaClient } from "@prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { drizzle } from "drizzle-orm/node-postgres";
import * as sqlQueries from "./query/sql";
import * as prismaQueries from "./query/prisma";
import * as drizzleQueries from "./query/drizzle";
import { setupOltp } from "./setup_db_oltp";
import { setupOlap } from "./setup_db_olap";

// Benchmark a raw SQL query
async function benchmarkSql(
  client: Client,
  name: string,
  query: string,
  params?: any[],
) {
  const start = performance.now();
  const result = await client.query(query, params);
  const ms = performance.now() - start;
  console.log(`  ${name}: ${ms.toFixed(2)}ms (${result.rowCount} rows)`);
}

// Benchmark an ORM query (async function)
async function benchmarkFn(name: string, fn: () => Promise<any>) {
  const start = performance.now();
  const result = await fn();
  const ms = performance.now() - start;
  const count = Array.isArray(result) ? result.length : result?.count ?? "?";
  console.log(`  ${name}: ${ms.toFixed(2)}ms (${count} rows)`);
}

// ============ RAW SQL ============

async function runOltpSql() {
  const client = new Client({ database: "oltp", host: "localhost" });
  await client.connect();
  console.log("\n=== OLTP: Raw SQL ===\n");

  await benchmarkSql(client, "getUserById", sqlQueries.getUserById, [1]);
  await benchmarkSql(client, "getUserByEmail", sqlQueries.getUserByEmail, [
    "user0_test@example.com",
  ]);
  await benchmarkSql(client, "getProductById", sqlQueries.getProductById, [1]);
  await benchmarkSql(client, "getOrderById", sqlQueries.getOrderById, [1]);
  await benchmarkSql(client, "getOrdersByUser", sqlQueries.getOrdersByUser, [1]);
  await benchmarkSql(client, "getProductStock", sqlQueries.getProductStock, [1]);
  await benchmarkSql(client, "getRecentOrders", sqlQueries.getRecentOrders, [20, 0]);
  await benchmarkSql(client, "searchUsersByName", sqlQueries.searchUsersByName, ["John"]);

  await client.end();
}

async function runOlapSql() {
  const client = new Client({ database: "olap", host: "localhost" });
  await client.connect();
  console.log("\n=== OLAP: Raw SQL ===\n");

  await benchmarkSql(client, "revenueByQuarter", sqlQueries.revenueByQuarter);
  await benchmarkSql(client, "monthlyRevenueTrend", sqlQueries.monthlyRevenueTrend);
  await benchmarkSql(client, "topProductsByRevenue", sqlQueries.topProductsByRevenue);
  await benchmarkSql(client, "revenueByCategoryAndYear", sqlQueries.revenueByCategoryAndYear);
  await benchmarkSql(client, "revenueByCountry", sqlQueries.revenueByCountry);
  await benchmarkSql(client, "revenueBySegment", sqlQueries.revenueBySegment);
  await benchmarkSql(client, "topCustomersBySpend", sqlQueries.topCustomersBySpend);
  await benchmarkSql(client, "weekendVsWeekdaySales", sqlQueries.weekendVsWeekdaySales);
  await benchmarkSql(client, "avgDiscountByBrand", sqlQueries.avgDiscountByBrand);
  await benchmarkSql(client, "yoyGrowthByCategory", sqlQueries.yoyGrowthByCategory);

  await client.end();
}

// ============ PRISMA ============

async function runOltpPrisma() {
  const adapter = new PrismaPg({ connectionString: "postgresql://samlewis@localhost:5432/oltp" });
  const prisma = new PrismaClient({ adapter });
  console.log("\n=== OLTP: Prisma ===\n");

  await benchmarkFn("getUserById", () => prismaQueries.getUserById(prisma, 1));
  await benchmarkFn("getUserByEmail", () =>
    prismaQueries.getUserByEmail(prisma, "user0_test@example.com"),
  );
  await benchmarkFn("getProductById", () => prismaQueries.getProductById(prisma, 1));
  await benchmarkFn("getOrderById", () => prismaQueries.getOrderById(prisma, 1));
  await benchmarkFn("getOrdersByUser", () => prismaQueries.getOrdersByUser(prisma, 1));
  await benchmarkFn("getProductStock", () => prismaQueries.getProductStock(prisma, 1));
  await benchmarkFn("getRecentOrders", () => prismaQueries.getRecentOrders(prisma, 20, 0));
  await benchmarkFn("searchUsersByName", () => prismaQueries.searchUsersByName(prisma, "John"));

  await prisma.$disconnect();
}

async function runOlapPrisma() {
  const adapter = new PrismaPg({ connectionString: "postgresql://samlewis@localhost:5432/olap" });
  const prisma = new PrismaClient({ adapter });
  console.log("\n=== OLAP: Prisma ===\n");

  await benchmarkFn("revenueByQuarter", () => prismaQueries.revenueByQuarter(prisma));
  await benchmarkFn("monthlyRevenueTrend", () => prismaQueries.monthlyRevenueTrend(prisma));
  await benchmarkFn("topProductsByRevenue", () => prismaQueries.topProductsByRevenue(prisma));
  await benchmarkFn("revenueByCategoryAndYear", () => prismaQueries.revenueByCategoryAndYear(prisma));
  await benchmarkFn("revenueByCountry", () => prismaQueries.revenueByCountry(prisma));
  await benchmarkFn("revenueBySegment", () => prismaQueries.revenueBySegment(prisma));
  await benchmarkFn("topCustomersBySpend", () => prismaQueries.topCustomersBySpend(prisma));
  await benchmarkFn("weekendVsWeekdaySales", () => prismaQueries.weekendVsWeekdaySales(prisma));
  await benchmarkFn("avgDiscountByBrand", () => prismaQueries.avgDiscountByBrand(prisma));
  await benchmarkFn("yoyGrowthByCategory", () => prismaQueries.yoyGrowthByCategory(prisma));

  await prisma.$disconnect();
}

// ============ DRIZZLE ============

async function runOltpDrizzle() {
  const client = new Client({ database: "oltp", host: "localhost" });
  await client.connect();
  const db = drizzle(client);
  console.log("\n=== OLTP: Drizzle ===\n");

  await benchmarkFn("getUserById", () => drizzleQueries.getUserById(db, 1));
  await benchmarkFn("getUserByEmail", () =>
    drizzleQueries.getUserByEmail(db, "user0_test@example.com"),
  );
  await benchmarkFn("getProductById", () => drizzleQueries.getProductById(db, 1));
  await benchmarkFn("getOrderById", () => drizzleQueries.getOrderById(db, 1));
  await benchmarkFn("getOrdersByUser", () => drizzleQueries.getOrdersByUser(db, 1));
  await benchmarkFn("getProductStock", () => drizzleQueries.getProductStock(db, 1));
  await benchmarkFn("getRecentOrders", () => drizzleQueries.getRecentOrders(db, 20, 0));
  await benchmarkFn("searchUsersByName", () => drizzleQueries.searchUsersByName(db, "John"));

  await client.end();
}

async function runOlapDrizzle() {
  const client = new Client({ database: "olap", host: "localhost" });
  await client.connect();
  const db = drizzle(client);
  console.log("\n=== OLAP: Drizzle ===\n");

  await benchmarkFn("revenueByQuarter", () => drizzleQueries.revenueByQuarter(db));
  await benchmarkFn("monthlyRevenueTrend", () => drizzleQueries.monthlyRevenueTrend(db));
  await benchmarkFn("topProductsByRevenue", () => drizzleQueries.topProductsByRevenue(db));
  await benchmarkFn("revenueByCategoryAndYear", () => drizzleQueries.revenueByCategoryAndYear(db));
  await benchmarkFn("revenueByCountry", () => drizzleQueries.revenueByCountry(db));
  await benchmarkFn("revenueBySegment", () => drizzleQueries.revenueBySegment(db));
  await benchmarkFn("topCustomersBySpend", () => drizzleQueries.topCustomersBySpend(db));
  await benchmarkFn("weekendVsWeekdaySales", () => drizzleQueries.weekendVsWeekdaySales(db));
  await benchmarkFn("avgDiscountByBrand", () => drizzleQueries.avgDiscountByBrand(db));
  await benchmarkFn("yoyGrowthByCategory", () => drizzleQueries.yoyGrowthByCategory(db));

  await client.end();
}

// ============ MAIN ============

async function warmup() {
  console.log("=== Warmup pass (priming Postgres cache) ===\n");

  // OLTP warmup
  const oltpClient = new Client({ database: "oltp", host: "localhost" });
  await oltpClient.connect();
  await oltpClient.query(sqlQueries.getUserById, [1]);
  await oltpClient.query(sqlQueries.getUserByEmail, ["user0_test@example.com"]);
  await oltpClient.query(sqlQueries.getProductById, [1]);
  await oltpClient.query(sqlQueries.getOrderById, [1]);
  await oltpClient.query(sqlQueries.getOrdersByUser, [1]);
  await oltpClient.query(sqlQueries.getProductStock, [1]);
  await oltpClient.query(sqlQueries.getRecentOrders, [20, 0]);
  await oltpClient.query(sqlQueries.searchUsersByName, ["John"]);
  await oltpClient.end();
  console.log("  OLTP warmed up.");

  // OLAP warmup
  const olapClient = new Client({ database: "olap", host: "localhost" });
  await olapClient.connect();
  await olapClient.query(sqlQueries.revenueByQuarter);
  await olapClient.query(sqlQueries.monthlyRevenueTrend);
  await olapClient.query(sqlQueries.topProductsByRevenue);
  await olapClient.query(sqlQueries.revenueByCategoryAndYear);
  await olapClient.query(sqlQueries.revenueByCountry);
  await olapClient.query(sqlQueries.revenueBySegment);
  await olapClient.query(sqlQueries.topCustomersBySpend);
  await olapClient.query(sqlQueries.weekendVsWeekdaySales);
  await olapClient.query(sqlQueries.avgDiscountByBrand);
  await olapClient.query(sqlQueries.yoyGrowthByCategory);
  await olapClient.end();
  console.log("  OLAP warmed up.\n");
}

async function main() {
  console.log("=== Setting up databases ===\n");
  await setupOltp();
  await setupOlap();

  await warmup();

  const totalStart = performance.now();

  // OLTP benchmarks
  await runOltpSql();
  await runOltpPrisma();
  await runOltpDrizzle();

  // OLAP benchmarks
  await runOlapSql();
  await runOlapPrisma();
  await runOlapDrizzle();

  const totalMs = performance.now() - totalStart;
  console.log(`\nTotal benchmark time: ${totalMs.toFixed(2)}ms`);
}

main().catch(console.error);
