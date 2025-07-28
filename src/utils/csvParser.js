const fs = require("fs")

class CSVParser {
  /**
   * Parse CSV file and convert to JSON with nested object support
   * @param {string} filePath - Path to CSV file
   * @returns {Array} Array of JSON objects
   */
  async parseCSVFile(filePath) {
    return new Promise((resolve, reject) => {
      try {
        const csvContent = fs.readFileSync(filePath, "utf8")
        const jsonData = this.parseCSVContent(csvContent)
        resolve(jsonData)
      } catch (error) {
        reject(error)
      }
    })
  }

  /**
   * Parse CSV content string to JSON
   * @param {string} csvContent - CSV content as string
   * @returns {Array} Array of JSON objects
   */
  parseCSVContent(csvContent) {
    const lines = this.splitCSVLines(csvContent)

    if (lines.length < 2) {
      throw new Error("CSV file must have at least a header row and one data row")
    }

    // Parse header row
    const headers = this.parseCSVRow(lines[0])

    // Validate mandatory fields
    this.validateMandatoryFields(headers)

    const jsonData = []

    // Parse data rows
    for (let i = 1; i < lines.length; i++) {
      if (lines[i].trim() === "") continue // Skip empty lines

      const values = this.parseCSVRow(lines[i])

      if (values.length !== headers.length) {
        console.warn(`Row ${i + 1}: Column count mismatch. Expected ${headers.length}, got ${values.length}`)
        continue
      }

      const jsonObject = this.createNestedObject(headers, values)
      jsonData.push(jsonObject)
    }

    return jsonData
  }

  /**
   * Split CSV content into lines, handling quoted fields with newlines
   * @param {string} csvContent - CSV content
   * @returns {Array} Array of lines
   */
  splitCSVLines(csvContent) {
    const lines = []
    let currentLine = ""
    let insideQuotes = false

    for (let i = 0; i < csvContent.length; i++) {
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
   * @param {string} row - CSV row string
   * @returns {Array} Array of values
   */
  parseCSVRow(row) {
    const values = []
    let currentValue = ""
    let insideQuotes = false

    for (let i = 0; i < row.length; i++) {
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
      throw new Error(`Missing mandatory fields: ${missingFields.join(", ")}`)
    }
  }

  /**
   * Create nested object from dot-notation headers and values
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
   * @param {Object} obj - Target object
   * @param {string} path - Dot-notation path (e.g., 'name.firstName')
   * @param {*} value - Value to set
   */
  setNestedProperty(obj, path, value) {
    const keys = path.split(".")
    let current = obj

    for (let i = 0; i < keys.length - 1; i++) {
      const key = keys[i]
      if (!(key in current) || typeof current[key] !== "object" || current[key] === null) {
        current[key] = {}
      }
      current = current[key]
    }

    const lastKey = keys[keys.length - 1]
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
}

module.exports = new CSVParser()
