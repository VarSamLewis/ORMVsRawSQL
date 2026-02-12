import { PrismaClient } from "@prisma/client";

// ============ OLTP QUERIES ============

// Single row lookups
export const getUserById = (prisma: PrismaClient, id: number) =>
  prisma.user.findUnique({ where: { id } });

export const getUserByEmail = (prisma: PrismaClient, email: string) =>
  prisma.user.findUnique({ where: { email } });

export const getProductById = (prisma: PrismaClient, id: number) =>
  prisma.product.findUnique({ where: { id } });

export const getOrderById = (prisma: PrismaClient, id: number) =>
  prisma.order.findUnique({ where: { id } });

// User's orders with item details
export const getOrdersByUser = (prisma: PrismaClient, userId: number) =>
  prisma.order.findMany({
    where: { userId },
    orderBy: { id: "desc" },
    include: {
      items: {
        include: { product: { select: { name: true } } },
      },
    },
  });

// Insert a new order
export const insertOrder = (
  prisma: PrismaClient,
  userId: number,
  totalPrice: number,
) =>
  prisma.order.create({
    data: { userId, totalPrice, status: "pending" },
  });

// Insert an order item
export const insertOrderItem = (
  prisma: PrismaClient,
  orderId: number,
  productId: number,
  quantity: number,
) =>
  prisma.orderItem.create({
    data: { orderId, productId, quantity },
  });

// Update order status
export const updateOrderStatus = (
  prisma: PrismaClient,
  id: number,
  status: string,
) => prisma.order.update({ where: { id }, data: { status } });

// Decrement product stock
export const decrementStock = (
  prisma: PrismaClient,
  id: number,
  amount: number,
) =>
  prisma.product.updateMany({
    where: { id, stock: { gte: amount } },
    data: { stock: { decrement: amount } },
  });

// Check product availability
export const getProductStock = (prisma: PrismaClient, id: number) =>
  prisma.product.findUnique({
    where: { id },
    select: { id: true, name: true, stock: true },
  });

// Recent orders (paginated)
export const getRecentOrders = (
  prisma: PrismaClient,
  limit: number,
  offset: number,
) =>
  prisma.order.findMany({
    orderBy: { id: "desc" },
    take: limit,
    skip: offset,
    include: { user: { select: { name: true } } },
  });

// Search users by name
export const searchUsersByName = (prisma: PrismaClient, name: string) =>
  prisma.user.findMany({
    where: { name: { contains: name, mode: "insensitive" } },
    select: { id: true, name: true, email: true },
    take: 20,
  });

// ============ OLAP QUERIES ============
// Prisma lacks native support for multi-table GROUP BY / aggregations,
// so complex analytics queries use $queryRawUnsafe — this is realistic.

// Total revenue by year and quarter
export const revenueByQuarter = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT d.year, d.quarter, SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_date d ON d.id = f.date_id
    GROUP BY d.year, d.quarter
    ORDER BY d.year, d.quarter
  `);

// Monthly revenue trend
export const monthlyRevenueTrend = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT d.year, d.month, SUM(f.total_amount) AS revenue, COUNT(*) AS num_sales
    FROM fact_sales f
    JOIN dim_date d ON d.id = f.date_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
  `);

// Top 10 products by revenue
export const topProductsByRevenue = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT p.name, p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units_sold
    FROM fact_sales f
    JOIN dim_product p ON p.id = f.product_id
    GROUP BY p.id, p.name, p.category
    ORDER BY revenue DESC
    LIMIT 10
  `);

// Revenue by product category
export const revenueByCategoryAndYear = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT p.category, d.year, SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_product p ON p.id = f.product_id
    JOIN dim_date d ON d.id = f.date_id
    GROUP BY p.category, d.year
    ORDER BY p.category, d.year
  `);

// Revenue by region (top 20 countries)
export const revenueByCountry = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT r.country, SUM(f.total_amount) AS revenue, COUNT(*) AS num_sales
    FROM fact_sales f
    JOIN dim_region r ON r.id = f.region_id
    GROUP BY r.country
    ORDER BY revenue DESC
    LIMIT 20
  `);

// Revenue by customer segment
export const revenueBySegment = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT c.segment, SUM(f.total_amount) AS revenue, COUNT(DISTINCT c.id) AS customers, COUNT(*) AS transactions
    FROM fact_sales f
    JOIN dim_customer c ON c.id = f.customer_id
    GROUP BY c.segment
    ORDER BY revenue DESC
  `);

// Top 10 customers by spend
export const topCustomersBySpend = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT c.name, c.segment, SUM(f.total_amount) AS total_spend, COUNT(*) AS num_orders
    FROM fact_sales f
    JOIN dim_customer c ON c.id = f.customer_id
    GROUP BY c.id, c.name, c.segment
    ORDER BY total_spend DESC
    LIMIT 10
  `);

// Weekend vs weekday sales
export const weekendVsWeekdaySales = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT d.is_weekend, COUNT(*) AS num_sales, SUM(f.total_amount) AS revenue,
           AVG(f.total_amount) AS avg_sale
    FROM fact_sales f
    JOIN dim_date d ON d.id = f.date_id
    GROUP BY d.is_weekend
  `);

// Average discount by brand
export const avgDiscountByBrand = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
    SELECT p.brand, AVG(f.discount) AS avg_discount, SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_product p ON p.id = f.product_id
    GROUP BY p.brand
    ORDER BY avg_discount DESC
  `);

// Year-over-year growth by category
export const yoyGrowthByCategory = (prisma: PrismaClient) =>
  prisma.$queryRawUnsafe(`
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
