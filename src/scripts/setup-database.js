const { Pool } = require("pg")
require("dotenv").config()

async function setupDatabase() {
  // First connect to postgres database to create our target database
  const adminPool = new Pool({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    database: "postgres", // Connect to default postgres database
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
  })

  try {
    // Create database if it doesn't exist
    const createDbQuery = `
      SELECT 1 FROM pg_database WHERE datname = $1
    `

    const dbExists = await adminPool.query(createDbQuery, [process.env.DB_NAME])

    if (dbExists.rows.length === 0) {
      await adminPool.query(`CREATE DATABASE ${process.env.DB_NAME}`)
      console.log(`Database '${process.env.DB_NAME}' created successfully`)
    } else {
      console.log(`Database '${process.env.DB_NAME}' already exists`)
    }

    await adminPool.end()

    // Now connect to our target database and create tables
    const appPool = new Pool({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
    })

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

    await appPool.query(createTableQuery)
    console.log("Tables created successfully")

    await appPool.end()
    console.log("Database setup completed!")
  } catch (error) {
    console.error("Database setup failed:", error)
    process.exit(1)
  }
}

setupDatabase()
