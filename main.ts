import { Client } from "pg";
import * as oltp from "./query/sql";
import * as olap from "./query/sql";
import { setupOltp } from "./setup_db_oltp";
import { setupOlap } from "./setup_db_olap";

async function benchmark(
  client: Client,
  name: string,
  query: string,
  params?: any[],
) {
  const start = performance.now();
  const result = await client.query(query, params);
  const ms = performance.now() - start;
  console.log(`  ${name}: ${ms.toFixed(2)}ms (${result.rowCount} rows)`);
  return { name, ms, rowCount: result.rowCount };
}

async function runOltpsql() {
  const client = new Client({ database: "oltp", host: "localhost" });
  await client.connect();

  console.log("\n=== OLTP Benchmarks: Raw SQL ===\n");

  await benchmark(client, "getUserById", oltp.getUserById, [1]);
  await benchmark(client, "getUserByEmail", oltp.getUserByEmail, [
    "user0_test@example.com",
  ]);
  await benchmark(client, "getProductById", oltp.getProductById, [1]);
  await benchmark(client, "getOrderById", oltp.getOrderById, [1]);
  await benchmark(client, "getOrdersByUser", oltp.getOrdersByUser, [1]);
  await benchmark(client, "getProductStock", oltp.getProductStock, [1]);
  await benchmark(client, "getRecentOrders", oltp.getRecentOrders, [20, 0]);
  await benchmark(client, "searchUsersByName", oltp.searchUsersByName, [
    "John",
  ]);

  await client.end();
}

async function runOlapsql() {
  const client = new Client({ database: "olap", host: "localhost" });
  await client.connect();

  console.log("\n=== OLAP Benchmarks: Raw SQL ===\n");

  await benchmark(client, "revenueByQuarter", olap.revenueByQuarter);
  await benchmark(client, "monthlyRevenueTrend", olap.monthlyRevenueTrend);
  await benchmark(client, "topProductsByRevenue", olap.topProductsByRevenue);
  await benchmark(
    client,
    "revenueByCategoryAndYear",
    olap.revenueByCategoryAndYear,
  );
  await benchmark(client, "revenueByCountry", olap.revenueByCountry);
  await benchmark(client, "revenueBySegment", olap.revenueBySegment);
  await benchmark(client, "topCustomersBySpend", olap.topCustomersBySpend);
  await benchmark(client, "weekendVsWeekdaySales", olap.weekendVsWeekdaySales);
  await benchmark(client, "avgDiscountByBrand", olap.avgDiscountByBrand);
  await benchmark(client, "yoyGrowthByCategory", olap.yoyGrowthByCategory);

  await client.end();
}

async function main() {
  console.log("=== Setting up databases ===\n");
  await setupOltp();
  await setupOlap();

  const totalStart = performance.now();

  await runOltpsql();
  await runOlapsql();

  const totalMs = performance.now() - totalStart;
  console.log(`\nTotal: ${totalMs.toFixed(2)}ms`);
}

main().catch(console.error);
