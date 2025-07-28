// src/controllers/csvController.js

const express = require("express")
const cors = require("cors")
const multer = require("multer")
const path = require("path")
const fs = require("fs")
require("dotenv").config()

const csvController = require("./controllers/csvController")
const { initializeDatabase } = require("./config/database")

const app = express()
const PORT = process.env.PORT || 3000

// Middleware
app.use(cors())
app.use(express.json())
app.use(express.urlencoded({ extended: true }))

// Create uploads directory if it doesn't exist
const uploadDir = process.env.CSV_UPLOAD_PATH || "./uploads"
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true })
}

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir)
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9)
    cb(null, "csv-" + uniqueSuffix + path.extname(file.originalname))
  },
})

const upload = multer({
  storage: storage,
  fileFilter: (req, file, cb) => {
    if (file.mimetype === "text/csv" || path.extname(file.originalname).toLowerCase() === ".csv") {
      cb(null, true)
    } else {
      cb(new Error("Only CSV files are allowed!"), false)
    }
  },
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit
  },
})

// Routes
app.get("/", (req, res) => {
  res.json({
    message: "CSV to JSON Converter API",
    endpoints: {
      "POST /api/upload-csv": "Upload and process CSV file",
      "GET /api/users": "Get all users from database",
      "GET /api/age-distribution": "Get age distribution report",
    },
  })
})

app.post("/api/upload-csv", upload.single("csvFile"), csvController.uploadAndProcessCSV.bind(csvController))

app.get("/api/users", csvController.getAllUsers)
app.get("/api/age-distribution", csvController.getAgeDistribution)

// Error handling middleware
app.use((error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    if (error.code === "LIMIT_FILE_SIZE") {
      return res.status(400).json({ error: "File too large. Maximum size is 50MB." })
    }
  }

  console.error("Error:", error)
  res.status(500).json({ error: error.message || "Internal server error" })
})

// Initialize database and start server
async function startServer() {
  try {
    await initializeDatabase()
    console.log("Database initialized successfully")

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`)
      console.log(`Upload directory: ${uploadDir}`)
    })
  } catch (error) {
    console.error("Failed to start server:", error)
    process.exit(1)
  }
}

startServer()
