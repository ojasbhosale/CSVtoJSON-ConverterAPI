// src/controllers/csvController.js

const fs = require("fs")
const path = require("path")
const { pool } = require("../config/database")
const csvParser = require("../utils/csvParser")
const { calculateAgeDistribution, printAgeDistributionReport } = require("../utils/ageDistribution")

class CSVController {
  async uploadAndProcessCSV(req, res) {
    try {
      if (!req.file) {
        return res.status(400).json({ error: "No CSV file uploaded" })
      }

      const filePath = req.file.path
      const fileSize = fs.statSync(filePath).size
      console.log(`Processing CSV file: ${filePath} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`)

      // Parse CSV file
      const startTime = Date.now()
      const jsonData = await csvParser.parseCSVFile(filePath)
      const parseTime = Date.now() - startTime
      console.log(`Parsed ${jsonData.length} records from CSV in ${parseTime}ms`)

      // Insert data into database with batch processing for large files
      const insertStartTime = Date.now()
      const insertedCount = await this.insertUsersToDatabase(jsonData)
      const insertTime = Date.now() - insertStartTime
      console.log(`Inserted ${insertedCount} users into database in ${insertTime}ms`)

      // Calculate and print age distribution
      await this.generateAgeDistributionReport()

      // Clean up uploaded file
      fs.unlinkSync(filePath)

      res.json({
        success: true,
        message: `Successfully processed ${insertedCount} records`,
        recordsProcessed: insertedCount,
        processingTime: {
          parsing: `${parseTime}ms`,
          insertion: `${insertTime}ms`,
          total: `${parseTime + insertTime}ms`,
        },
      })
    } catch (error) {
      console.error("Error processing CSV:", error)

      // Clean up file if it exists
      if (req.file && fs.existsSync(req.file.path)) {
        fs.unlinkSync(req.file.path)
      }

      res.status(500).json({ error: error.message })
    }
  }

  /**
   * Insert users to database with batch processing for large datasets
   * Optimized for 50k+ records
   * @param {Array} jsonData - Array of user objects
   * @returns {number} Number of inserted records
   */
  async insertUsersToDatabase(jsonData) {
    const client = await pool.connect()
    let insertedCount = 0
    const batchSize = 1000 // Process in batches of 1000 records

    try {
      await client.query("BEGIN")

      // Process in batches for better performance with large datasets
      for (let i = 0; i < jsonData.length; i += batchSize) {
        const batch = jsonData.slice(i, i + batchSize)

        // Prepare batch insert query
        const values = []
        const placeholders = []

        for (let j = 0; j < batch.length; j++) {
          const record = batch[j]

          // Extract mandatory fields
          const firstName = record.name?.firstName || ""
          const lastName = record.name?.lastName || ""
          const fullName = `${firstName} ${lastName}`.trim()
          const age = Number.parseInt(record.age) || 0

          // Validate age
          if (age < 0 || age > 150) {
            console.warn(`Invalid age ${age} for user ${fullName}. Setting to 0.`)
          }

          // Extract address (all address.* properties)
          const address = record.address || null

          // Extract additional info (everything except name, age, address)
          const additionalInfo = { ...record }
          delete additionalInfo.name
          delete additionalInfo.age
          delete additionalInfo.address

          // Add to batch values
          const baseIndex = j * 4
          placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4})`)

          values.push(
            fullName,
            age,
            address ? JSON.stringify(address) : null,
            Object.keys(additionalInfo).length > 0 ? JSON.stringify(additionalInfo) : null,
          )
        }

        // Execute batch insert
        if (placeholders.length > 0) {
          const insertQuery = `
            INSERT INTO public.users (name, age, address, additional_info)
            VALUES ${placeholders.join(", ")}
          `

          await client.query(insertQuery, values)
          insertedCount += batch.length

          // Progress logging for large batches
          if (insertedCount % 10000 === 0) {
            console.log(`Inserted ${insertedCount} records...`)
          }
        }
      }

      await client.query("COMMIT")
      console.log(`✓ Successfully inserted ${insertedCount} records using batch processing`)
      return insertedCount
    } catch (error) {
      await client.query("ROLLBACK")
      console.error("Database insertion failed:", error)
      throw error
    } finally {
      client.release()
    }
  }

  async getAllUsers(req, res) {
    try {
      const limit = req.query.limit ? Number.parseInt(req.query.limit) : 100
      const offset = req.query.offset ? Number.parseInt(req.query.offset) : 0

      // Get total count
      const countResult = await pool.query("SELECT COUNT(*) as total FROM public.users")
      const totalUsers = Number.parseInt(countResult.rows[0].total)

      // Get paginated results
      const result = await pool.query("SELECT * FROM public.users ORDER BY id LIMIT $1 OFFSET $2", [limit, offset])

      res.json({
        success: true,
        users: result.rows,
        pagination: {
          total: totalUsers,
          limit: limit,
          offset: offset,
          hasMore: offset + limit < totalUsers,
        },
      })
    } catch (error) {
      console.error("Error fetching users:", error)
      res.status(500).json({ error: error.message })
    }
  }

  async getAgeDistribution(req, res) {
    try {
      const distribution = await calculateAgeDistribution()
      res.json({
        success: true,
        ageDistribution: distribution,
      })
    } catch (error) {
      console.error("Error calculating age distribution:", error)
      res.status(500).json({ error: error.message })
    }
  }

  async generateAgeDistributionReport() {
    try {
      const distribution = await calculateAgeDistribution()
      printAgeDistributionReport(distribution)
    } catch (error) {
      console.error("Error generating age distribution report:", error)
    }
  }
}

module.exports = new CSVController()
