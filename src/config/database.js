const { Pool } = require("pg")

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
})

// Test database connection
async function testConnection() {
  try {
    const client = await pool.connect()
    console.log("Database connected successfully")
    client.release()
  } catch (error) {
    console.error("Database connection failed:", error)
    throw error
  }
}

// Initialize database tables
async function initializeDatabase() {
  try {
    await testConnection()

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS public.users (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        age INTEGER NOT NULL,
        address JSONB NULL,
        additional_info JSONB NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      
      CREATE INDEX IF NOT EXISTS idx_users_age ON public.users(age);
    `

    await pool.query(createTableQuery)
    console.log("Database tables initialized")
  } catch (error) {
    console.error("Database initialization failed:", error)
    throw error
  }
}

module.exports = {
  pool,
  initializeDatabase,
}
