const fs = require("fs")

class CSVParser {
  /**
   * Parse CSV file and convert to JSON with nested object support
   * Optimized for large files (50k+ records)
   * @param {string} filePath - Path to CSV file
   * @returns {Array} Array of JSON objects
   */
  async parseCSVFile(filePath) {
    return new Promise((resolve, reject) => {
      try {
        console.log("Starting CSV file parsing...")
        const csvContent = fs.readFileSync(filePath, "utf8")
        const jsonData = this.parseCSVContent(csvContent)
        console.log(`CSV parsing completed. Processed ${jsonData.length} records.`)
        resolve(jsonData)
      } catch (error) {
        reject(error)
      }
    })
  }

  /**
   * Parse CSV content string to JSON
   * Handles first line as labels (headers) requirement
   * @param {string} csvContent - CSV content as string
   * @returns {Array} Array of JSON objects
   */
  parseCSVContent(csvContent) {
    const lines = this.splitCSVLines(csvContent)

    if (lines.length < 1) {
      throw new Error("CSV file is empty")
    }

    if (lines.length < 2) {
      throw new Error("CSV file must have at least a header row and one data row")
    }

    // First line is always labels for properties (as per requirement)
    const headers = this.parseCSVRow(lines[0])
    console.log(`Found ${headers.length} columns in header row`)

    // Validate mandatory fields
    this.validateMandatoryFields(headers)

    // Validate that sub-properties are grouped together
    this.validatePropertyGrouping(headers)

    const jsonData = []
    let processedRows = 0

    // Parse data rows (starting from line 1, since line 0 is headers)
    for (let i = 1; i < lines.length; i++) {
      if (lines[i].trim() === "") continue // Skip empty lines

      const values = this.parseCSVRow(lines[i])

      if (values.length !== headers.length) {
        console.warn(
          `Row ${i + 1}: Column count mismatch. Expected ${headers.length}, got ${values.length}. Skipping row.`,
        )
        continue
      }

      try {
        const jsonObject = this.createNestedObject(headers, values)
        jsonData.push(jsonObject)
        processedRows++

        // Progress logging for large files
        if (processedRows % 10000 === 0) {
          console.log(`Processed ${processedRows} rows...`)
        }
      } catch (error) {
        console.warn(`Row ${i + 1}: Error creating object - ${error.message}. Skipping row.`)
        continue
      }
    }

    console.log(`Successfully processed ${processedRows} data rows`)
    return jsonData
  }

  /**
   * Validate that sub-properties of complex properties are grouped together
   * This helps ensure data integrity for nested objects
   * @param {Array} headers - Array of header names
   */
  validatePropertyGrouping(headers) {
    const propertyGroups = new Map()

    // Group headers by their root property
    headers.forEach((header, index) => {
      const rootProperty = header.split(".")[0]
      if (!propertyGroups.has(rootProperty)) {
        propertyGroups.set(rootProperty, [])
      }
      propertyGroups.get(rootProperty).push({ header, index })
    })

    // Check if sub-properties are grouped together
    for (const [rootProperty, properties] of propertyGroups) {
      if (properties.length > 1) {
        const indices = properties.map((p) => p.index)
        const minIndex = Math.min(...indices)
        const maxIndex = Math.max(...indices)

        // Check if all indices are consecutive
        const expectedIndices = Array.from({ length: maxIndex - minIndex + 1 }, (_, i) => minIndex + i)
        const actualIndices = indices.sort((a, b) => a - b)

        const areConsecutive = expectedIndices.every((expected, i) => expected === actualIndices[i])

        if (!areConsecutive) {
          console.warn(
            `Warning: Sub-properties of '${rootProperty}' are not grouped together. This may affect data integrity.`,
          )
        }
      }
    }
  }

  /**
   * Split CSV content into lines, handling quoted fields with newlines
   * Optimized for large files
   * @param {string} csvContent - CSV content
   * @returns {Array} Array of lines
   */
  splitCSVLines(csvContent) {
    const lines = []
    let currentLine = ""
    let insideQuotes = false
    const contentLength = csvContent.length

    for (let i = 0; i < contentLength; i++) {
      const char = csvContent[i]
      const nextChar = csvContent[i + 1]

      if (char === '"') {
        if (insideQuotes && nextChar === '"') {
          // Escaped quote
          currentLine += '""'
          i++ // Skip next quote
        } else {
          // Toggle quote state
          insideQuotes = !insideQuotes
          currentLine += char
        }
      } else if (char === "\n" && !insideQuotes) {
        // End of line
        if (currentLine.trim() !== "") {
          lines.push(currentLine)
        }
        currentLine = ""
      } else if (char === "\r" && nextChar === "\n" && !insideQuotes) {
        // Windows line ending
        if (currentLine.trim() !== "") {
          lines.push(currentLine)
        }
        currentLine = ""
        i++ // Skip \n
      } else {
        currentLine += char
      }
    }

    // Add last line if exists
    if (currentLine.trim() !== "") {
      lines.push(currentLine)
    }

    return lines
  }

  /**
   * Parse a single CSV row into array of values
   * Handles quoted fields and escaped quotes
   * @param {string} row - CSV row string
   * @returns {Array} Array of values
   */
  parseCSVRow(row) {
    const values = []
    let currentValue = ""
    let insideQuotes = false
    const rowLength = row.length

    for (let i = 0; i < rowLength; i++) {
      const char = row[i]
      const nextChar = row[i + 1]

      if (char === '"') {
        if (insideQuotes && nextChar === '"') {
          // Escaped quote
          currentValue += '"'
          i++ // Skip next quote
        } else {
          // Toggle quote state
          insideQuotes = !insideQuotes
        }
      } else if (char === "," && !insideQuotes) {
        // Field separator
        values.push(currentValue.trim())
        currentValue = ""
      } else {
        currentValue += char
      }
    }

    // Add last value
    values.push(currentValue.trim())

    return values
  }

  /**
   * Validate that mandatory fields are present in headers
   * @param {Array} headers - Array of header names
   */
  validateMandatoryFields(headers) {
    const mandatoryFields = ["name.firstName", "name.lastName", "age"]
    const missingFields = mandatoryFields.filter((field) => !headers.includes(field))

    if (missingFields.length > 0) {
      throw new Error(
        `Missing mandatory fields: ${missingFields.join(", ")}. These fields must be present in the first line (header row).`,
      )
    }

    console.log("✓ All mandatory fields found in header row")
  }

  /**
   * Create nested object from dot-notation headers and values
   * Supports infinite depth nesting (a.b.c.d.e.f.g...)
   * @param {Array} headers - Array of header names (with dot notation)
   * @param {Array} values - Array of corresponding values
   * @returns {Object} Nested JSON object
   */
  createNestedObject(headers, values) {
    const result = {}

    for (let i = 0; i < headers.length; i++) {
      const header = headers[i]
      const value = values[i]

      if (header && value !== undefined && value !== "") {
        this.setNestedProperty(result, header, value)
      }
    }

    return result
  }

  /**
   * Set nested property using dot notation
   * Supports infinite depth: a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z.a1.b1.c1...
   * @param {Object} obj - Target object
   * @param {string} path - Dot-notation path (e.g., 'name.firstName' or 'a.b.c.d.e.f.g')
   * @param {*} value - Value to set
   */
  setNestedProperty(obj, path, value) {
    const keys = path.split(".")
    let current = obj

    // Navigate/create nested structure for all keys except the last one
    for (let i = 0; i < keys.length - 1; i++) {
      const key = keys[i]

      // Validate key name
      if (!key || key.trim() === "") {
        throw new Error(`Invalid property path: '${path}'. Empty key found.`)
      }

      // Create nested object if it doesn't exist or if it's not an object
      if (!(key in current) || typeof current[key] !== "object" || current[key] === null) {
        current[key] = {}
      }
      current = current[key]
    }

    // Set the final value
    const lastKey = keys[keys.length - 1]
    if (!lastKey || lastKey.trim() === "") {
      throw new Error(`Invalid property path: '${path}'. Empty final key.`)
    }

    current[lastKey] = this.convertValue(value)
  }

  /**
   * Convert string value to appropriate type
   * @param {string} value - String value
   * @returns {*} Converted value
   */
  convertValue(value) {
    if (value === "") return ""

    // Try to convert to number
    if (!isNaN(value) && !isNaN(Number.parseFloat(value))) {
      const num = Number.parseFloat(value)
      return Number.isInteger(num) ? Number.parseInt(value) : num
    }

    // Try to convert to boolean
    if (value.toLowerCase() === "true") return true
    if (value.toLowerCase() === "false") return false

    // Return as string
    return value
  }

  /**
   * Get statistics about the CSV structure
   * @param {Array} headers - Array of header names
   * @returns {Object} Statistics object
   */
  getCSVStatistics(headers) {
    const stats = {
      totalColumns: headers.length,
      nestedProperties: 0,
      maxDepth: 0,
      propertyGroups: new Map(),
    }

    headers.forEach((header) => {
      const depth = header.split(".").length
      if (depth > 1) {
        stats.nestedProperties++
      }
      stats.maxDepth = Math.max(stats.maxDepth, depth)

      const rootProperty = header.split(".")[0]
      if (!stats.propertyGroups.has(rootProperty)) {
        stats.propertyGroups.set(rootProperty, 0)
      }
      stats.propertyGroups.set(rootProperty, stats.propertyGroups.get(rootProperty) + 1)
    })

    return stats
  }
}

module.exports = new CSVParser()
