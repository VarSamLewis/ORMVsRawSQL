// ============ OLTP QUERIES ============

// Single row lookups
export const getUserById = "SELECT * FROM users WHERE id = $1";
export const getUserByEmail = "SELECT * FROM users WHERE email = $1";
export const getProductById = "SELECT * FROM products WHERE id = $1";
export const getOrderById = "SELECT * FROM orders WHERE id = $1";

// User's orders with item details
export const getOrdersByUser = `
  SELECT o.id AS order_id, o.status, o.total_price,
         p.name AS product, oi.quantity
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.id
  JOIN products p ON p.id = oi.product_id
  WHERE o.user_id = $1
  ORDER BY o.id DESC
`;

// Insert a new order with items (single order row)
export const insertOrder = `
  INSERT INTO orders (user_id, total_price, status)
  VALUES ($1, $2, 'pending')
  RETURNING id
`;

// Insert an order item
export const insertOrderItem = `
  INSERT INTO order_items (order_id, product_id, quantity)
  VALUES ($1, $2, $3)
`;

// Update order status
export const updateOrderStatus = `
  UPDATE orders SET status = $2 WHERE id = $1
`;

// Decrement product stock
export const decrementStock = `
  UPDATE products SET stock = stock - $2 WHERE id = $1 AND stock >= $2
`;

// Check product availability
export const getProductStock = "SELECT id, name, stock FROM products WHERE id = $1";

// Recent orders (paginated)
export const getRecentOrders = `
  SELECT o.id, u.name AS customer, o.total_price, o.status
  FROM orders o
  JOIN users u ON u.id = o.user_id
  ORDER BY o.id DESC
  LIMIT $1 OFFSET $2
`;

// Search users by name
export const searchUsersByName = `
  SELECT id, name, email FROM users WHERE name ILIKE '%' || $1 || '%' LIMIT 20
`;

// ============ OLAP QUERIES ============

// Total revenue by year and quarter
export const revenueByQuarter = `
  SELECT d.year, d.quarter, SUM(f.total_amount) AS revenue
  FROM fact_sales f
  JOIN dim_date d ON d.id = f.date_id
  GROUP BY d.year, d.quarter
  ORDER BY d.year, d.quarter
`;

// Monthly revenue trend
export const monthlyRevenueTrend = `
  SELECT d.year, d.month, SUM(f.total_amount) AS revenue, COUNT(*) AS num_sales
  FROM fact_sales f
  JOIN dim_date d ON d.id = f.date_id
  GROUP BY d.year, d.month
  ORDER BY d.year, d.month
`;

// Top 10 products by revenue
export const topProductsByRevenue = `
  SELECT p.name, p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units_sold
  FROM fact_sales f
  JOIN dim_product p ON p.id = f.product_id
  GROUP BY p.id, p.name, p.category
  ORDER BY revenue DESC
  LIMIT 10
`;

// Revenue by product category
export const revenueByCategoryAndYear = `
  SELECT p.category, d.year, SUM(f.total_amount) AS revenue
  FROM fact_sales f
  JOIN dim_product p ON p.id = f.product_id
  JOIN dim_date d ON d.id = f.date_id
  GROUP BY p.category, d.year
  ORDER BY p.category, d.year
`;

// Revenue by region (top 20 countries)
export const revenueByCountry = `
  SELECT r.country, SUM(f.total_amount) AS revenue, COUNT(*) AS num_sales
  FROM fact_sales f
  JOIN dim_region r ON r.id = f.region_id
  GROUP BY r.country
  ORDER BY revenue DESC
  LIMIT 20
`;

// Revenue by customer segment
export const revenueBySegment = `
  SELECT c.segment, SUM(f.total_amount) AS revenue, COUNT(DISTINCT c.id) AS customers, COUNT(*) AS transactions
  FROM fact_sales f
  JOIN dim_customer c ON c.id = f.customer_id
  GROUP BY c.segment
  ORDER BY revenue DESC
`;

// Top 10 customers by spend
export const topCustomersBySpend = `
  SELECT c.name, c.segment, SUM(f.total_amount) AS total_spend, COUNT(*) AS num_orders
  FROM fact_sales f
  JOIN dim_customer c ON c.id = f.customer_id
  GROUP BY c.id, c.name, c.segment
  ORDER BY total_spend DESC
  LIMIT 10
`;

// Weekend vs weekday sales
export const weekendVsWeekdaySales = `
  SELECT d.is_weekend, COUNT(*) AS num_sales, SUM(f.total_amount) AS revenue,
         AVG(f.total_amount) AS avg_sale
  FROM fact_sales f
  JOIN dim_date d ON d.id = f.date_id
  GROUP BY d.is_weekend
`;

// Average discount by brand
export const avgDiscountByBrand = `
  SELECT p.brand, AVG(f.discount) AS avg_discount, SUM(f.total_amount) AS revenue
  FROM fact_sales f
  JOIN dim_product p ON p.id = f.product_id
  GROUP BY p.brand
  ORDER BY avg_discount DESC
`;

// Year-over-year growth by category
export const yoyGrowthByCategory = `
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
`;
