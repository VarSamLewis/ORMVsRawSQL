import { drizzle } from "drizzle-orm/node-postgres";
import {
  pgTable,
  serial,
  text,
  integer,
  decimal,
  timestamp,
  date,
  boolean,
} from "drizzle-orm/pg-core";
import { eq, ilike, desc, sql, asc, gte, and } from "drizzle-orm";
import { Client } from "pg";

// ============ OLTP SCHEMA ============

export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").unique(),
  name: text("name"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const products = pgTable("products", {
  id: serial("id").primaryKey(),
  name: text("name"),
  price: decimal("price", { precision: 10, scale: 2 }),
  stock: integer("stock"),
});

export const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").references(() => users.id),
  totalPrice: decimal("total_price", { precision: 10, scale: 2 }),
  status: text("status"),
});

export const orderItems = pgTable("order_items", {
  id: serial("id").primaryKey(),
  orderId: integer("order_id").references(() => orders.id),
  productId: integer("product_id").references(() => products.id),
  quantity: integer("quantity"),
});

// ============ OLAP SCHEMA ============

export const dimDate = pgTable("dim_date", {
  id: serial("id").primaryKey(),
  fullDate: date("full_date"),
  year: integer("year"),
  quarter: integer("quarter"),
  month: integer("month"),
  day: integer("day"),
  dayOfWeek: integer("day_of_week"),
  isWeekend: boolean("is_weekend"),
});

export const dimRegion = pgTable("dim_region", {
  id: serial("id").primaryKey(),
  country: text("country"),
  state: text("state"),
  city: text("city"),
});

export const dimCustomer = pgTable("dim_customer", {
  id: serial("id").primaryKey(),
  name: text("name"),
  email: text("email"),
  segment: text("segment"),
  regionId: integer("region_id").references(() => dimRegion.id),
});

export const dimProduct = pgTable("dim_product", {
  id: serial("id").primaryKey(),
  name: text("name"),
  category: text("category"),
  subcategory: text("subcategory"),
  brand: text("brand"),
  unitCost: decimal("unit_cost", { precision: 10, scale: 2 }),
});

export const factSales = pgTable("fact_sales", {
  id: serial("id").primaryKey(),
  dateId: integer("date_id").references(() => dimDate.id),
  customerId: integer("customer_id").references(() => dimCustomer.id),
  productId: integer("product_id").references(() => dimProduct.id),
  regionId: integer("region_id").references(() => dimRegion.id),
  quantity: integer("quantity"),
  unitPrice: decimal("unit_price", { precision: 10, scale: 2 }),
  discount: decimal("discount", { precision: 5, scale: 2 }),
  totalAmount: decimal("total_amount", { precision: 12, scale: 2 }),
});

// ============ OLTP QUERIES ============

type DrizzleDB = ReturnType<typeof drizzle>;

// Single row lookups
export const getUserById = (db: DrizzleDB, id: number) =>
  db.select().from(users).where(eq(users.id, id));

export const getUserByEmail = (db: DrizzleDB, email: string) =>
  db.select().from(users).where(eq(users.email, email));

export const getProductById = (db: DrizzleDB, id: number) =>
  db.select().from(products).where(eq(products.id, id));

export const getOrderById = (db: DrizzleDB, id: number) =>
  db.select().from(orders).where(eq(orders.id, id));

// User's orders with item details
export const getOrdersByUser = (db: DrizzleDB, userId: number) =>
  db
    .select({
      orderId: orders.id,
      status: orders.status,
      totalPrice: orders.totalPrice,
      product: products.name,
      quantity: orderItems.quantity,
    })
    .from(orders)
    .innerJoin(orderItems, eq(orderItems.orderId, orders.id))
    .innerJoin(products, eq(products.id, orderItems.productId))
    .where(eq(orders.userId, userId))
    .orderBy(desc(orders.id));

// Insert a new order
export const insertOrder = (
  db: DrizzleDB,
  userId: number,
  totalPrice: string,
) =>
  db
    .insert(orders)
    .values({ userId, totalPrice, status: "pending" })
    .returning({ id: orders.id });

// Insert an order item
export const insertOrderItem = (
  db: DrizzleDB,
  orderId: number,
  productId: number,
  quantity: number,
) => db.insert(orderItems).values({ orderId, productId, quantity });

// Update order status
export const updateOrderStatus = (
  db: DrizzleDB,
  id: number,
  status: string,
) => db.update(orders).set({ status }).where(eq(orders.id, id));

// Decrement product stock
export const decrementStock = (
  db: DrizzleDB,
  id: number,
  amount: number,
) =>
  db
    .update(products)
    .set({ stock: sql`${products.stock} - ${amount}` })
    .where(and(eq(products.id, id), gte(products.stock, amount)));

// Check product availability
export const getProductStock = (db: DrizzleDB, id: number) =>
  db
    .select({ id: products.id, name: products.name, stock: products.stock })
    .from(products)
    .where(eq(products.id, id));

// Recent orders (paginated)
export const getRecentOrders = (
  db: DrizzleDB,
  limit: number,
  offset: number,
) =>
  db
    .select({
      id: orders.id,
      customer: users.name,
      totalPrice: orders.totalPrice,
      status: orders.status,
    })
    .from(orders)
    .innerJoin(users, eq(users.id, orders.userId))
    .orderBy(desc(orders.id))
    .limit(limit)
    .offset(offset);

// Search users by name
export const searchUsersByName = (db: DrizzleDB, name: string) =>
  db
    .select({ id: users.id, name: users.name, email: users.email })
    .from(users)
    .where(ilike(users.name, `%${name}%`))
    .limit(20);

// ============ OLAP QUERIES ============

// Total revenue by year and quarter
export const revenueByQuarter = (db: DrizzleDB) =>
  db
    .select({
      year: dimDate.year,
      quarter: dimDate.quarter,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
    })
    .from(factSales)
    .innerJoin(dimDate, eq(dimDate.id, factSales.dateId))
    .groupBy(dimDate.year, dimDate.quarter)
    .orderBy(asc(dimDate.year), asc(dimDate.quarter));

// Monthly revenue trend
export const monthlyRevenueTrend = (db: DrizzleDB) =>
  db
    .select({
      year: dimDate.year,
      month: dimDate.month,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
      numSales: sql<number>`count(*)`,
    })
    .from(factSales)
    .innerJoin(dimDate, eq(dimDate.id, factSales.dateId))
    .groupBy(dimDate.year, dimDate.month)
    .orderBy(asc(dimDate.year), asc(dimDate.month));

// Top 10 products by revenue
export const topProductsByRevenue = (db: DrizzleDB) =>
  db
    .select({
      name: dimProduct.name,
      category: dimProduct.category,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
      unitsSold: sql<number>`sum(${factSales.quantity})`,
    })
    .from(factSales)
    .innerJoin(dimProduct, eq(dimProduct.id, factSales.productId))
    .groupBy(dimProduct.id, dimProduct.name, dimProduct.category)
    .orderBy(sql`sum(${factSales.totalAmount}) desc`)
    .limit(10);

// Revenue by product category and year
export const revenueByCategoryAndYear = (db: DrizzleDB) =>
  db
    .select({
      category: dimProduct.category,
      year: dimDate.year,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
    })
    .from(factSales)
    .innerJoin(dimProduct, eq(dimProduct.id, factSales.productId))
    .innerJoin(dimDate, eq(dimDate.id, factSales.dateId))
    .groupBy(dimProduct.category, dimDate.year)
    .orderBy(asc(dimProduct.category), asc(dimDate.year));

// Revenue by region (top 20 countries)
export const revenueByCountry = (db: DrizzleDB) =>
  db
    .select({
      country: dimRegion.country,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
      numSales: sql<number>`count(*)`,
    })
    .from(factSales)
    .innerJoin(dimRegion, eq(dimRegion.id, factSales.regionId))
    .groupBy(dimRegion.country)
    .orderBy(sql`sum(${factSales.totalAmount}) desc`)
    .limit(20);

// Revenue by customer segment
export const revenueBySegment = (db: DrizzleDB) =>
  db
    .select({
      segment: dimCustomer.segment,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
      customers: sql<number>`count(distinct ${dimCustomer.id})`,
      transactions: sql<number>`count(*)`,
    })
    .from(factSales)
    .innerJoin(dimCustomer, eq(dimCustomer.id, factSales.customerId))
    .groupBy(dimCustomer.segment)
    .orderBy(sql`sum(${factSales.totalAmount}) desc`);

// Top 10 customers by spend
export const topCustomersBySpend = (db: DrizzleDB) =>
  db
    .select({
      name: dimCustomer.name,
      segment: dimCustomer.segment,
      totalSpend: sql<string>`sum(${factSales.totalAmount})`,
      numOrders: sql<number>`count(*)`,
    })
    .from(factSales)
    .innerJoin(dimCustomer, eq(dimCustomer.id, factSales.customerId))
    .groupBy(dimCustomer.id, dimCustomer.name, dimCustomer.segment)
    .orderBy(sql`sum(${factSales.totalAmount}) desc`)
    .limit(10);

// Weekend vs weekday sales
export const weekendVsWeekdaySales = (db: DrizzleDB) =>
  db
    .select({
      isWeekend: dimDate.isWeekend,
      numSales: sql<number>`count(*)`,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
      avgSale: sql<string>`avg(${factSales.totalAmount})`,
    })
    .from(factSales)
    .innerJoin(dimDate, eq(dimDate.id, factSales.dateId))
    .groupBy(dimDate.isWeekend);

// Average discount by brand
export const avgDiscountByBrand = (db: DrizzleDB) =>
  db
    .select({
      brand: dimProduct.brand,
      avgDiscount: sql<string>`avg(${factSales.discount})`,
      revenue: sql<string>`sum(${factSales.totalAmount})`,
    })
    .from(factSales)
    .innerJoin(dimProduct, eq(dimProduct.id, factSales.productId))
    .groupBy(dimProduct.brand)
    .orderBy(sql`avg(${factSales.discount}) desc`);

// Year-over-year growth by category
export const yoyGrowthByCategory = (db: DrizzleDB) =>
  db.execute(sql`
    WITH yearly AS (
      SELECT p.category, d.year, SUM(f.total_amount) AS revenue
      FROM fact_sales f
      JOIN dim_product p ON p.id = f.product_id
      JOIN dim_date d ON d.id = f.date_id
      GROUP BY p.category, d.year
    )
    SELECT curr.category, curr.year, curr.revenue,
           prev.revenue AS prev_year_revenue,
           ROUND(((curr.revenue - prev.revenue) / prev.revenue) * 100, 2) AS growth_pct
    FROM yearly curr
    LEFT JOIN yearly prev ON prev.category = curr.category AND prev.year = curr.year - 1
    ORDER BY curr.category, curr.year
  `);
