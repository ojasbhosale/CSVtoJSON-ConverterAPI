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
      console.log(`Processing CSV file: ${filePath}`)

      // Parse CSV file
      const jsonData = await csvParser.parseCSVFile(filePath)
      console.log(`Parsed ${jsonData.length} records from CSV`)

      // Insert data into database
      const insertedCount = await this.insertUsersToDatabase(jsonData)
      console.log(`Inserted ${insertedCount} users into database`)

      // Calculate and print age distribution
      await this.generateAgeDistributionReport()

      // Clean up uploaded file
      fs.unlinkSync(filePath)

      res.json({
        success: true,
        message: `Successfully processed ${insertedCount} records`,
        recordsProcessed: insertedCount,
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

  async insertUsersToDatabase(jsonData) {
    const client = await pool.connect()
    let insertedCount = 0

    try {
      await client.query("BEGIN")

      for (const record of jsonData) {
        // Extract mandatory fields
        const firstName = record.name?.firstName || ""
        const lastName = record.name?.lastName || ""
        const fullName = `${firstName} ${lastName}`.trim()
        const age = Number.parseInt(record.age) || 0

        // Extract address
        const address = record.address || null

        // Extract additional info (everything except name, age, address)
        const additionalInfo = { ...record }
        delete additionalInfo.name
        delete additionalInfo.age
        delete additionalInfo.address

        // Insert into database
        const insertQuery = `
          INSERT INTO public.users (name, age, address, additional_info)
          VALUES ($1, $2, $3, $4)
        `

        await client.query(insertQuery, [
          fullName,
          age,
          address ? JSON.stringify(address) : null,
          Object.keys(additionalInfo).length > 0 ? JSON.stringify(additionalInfo) : null,
        ])

        insertedCount++
      }

      await client.query("COMMIT")
      return insertedCount
    } catch (error) {
      await client.query("ROLLBACK")
      throw error
    } finally {
      client.release()
    }
  }

  async getAllUsers(req, res) {
    try {
      const result = await pool.query("SELECT * FROM public.users ORDER BY id")
      res.json({
        success: true,
        users: result.rows,
        count: result.rows.length,
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
