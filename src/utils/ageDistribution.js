const { pool } = require("../config/database")

/**
 * Calculate age distribution from database
 * @returns {Object} Age distribution object
 */
async function calculateAgeDistribution() {
  try {
    const query = `
      SELECT 
        COUNT(*) as total_users,
        COUNT(CASE WHEN age < 20 THEN 1 END) as under_20,
        COUNT(CASE WHEN age >= 20 AND age <= 40 THEN 1 END) as age_20_to_40,
        COUNT(CASE WHEN age > 40 AND age <= 60 THEN 1 END) as age_40_to_60,
        COUNT(CASE WHEN age > 60 THEN 1 END) as over_60
      FROM public.users
    `

    const result = await pool.query(query)
    const row = result.rows[0]

    const totalUsers = Number.parseInt(row.total_users)

    if (totalUsers === 0) {
      return {
        totalUsers: 0,
        distribution: {
          under_20: { count: 0, percentage: 0 },
          "20_to_40": { count: 0, percentage: 0 },
          "40_to_60": { count: 0, percentage: 0 },
          over_60: { count: 0, percentage: 0 },
        },
      }
    }

    const distribution = {
      totalUsers,
      distribution: {
        under_20: {
          count: Number.parseInt(row.under_20),
          percentage: Math.round((Number.parseInt(row.under_20) / totalUsers) * 100),
        },
        "20_to_40": {
          count: Number.parseInt(row.age_20_to_40),
          percentage: Math.round((Number.parseInt(row.age_20_to_40) / totalUsers) * 100),
        },
        "40_to_60": {
          count: Number.parseInt(row.age_40_to_60),
          percentage: Math.round((Number.parseInt(row.age_40_to_60) / totalUsers) * 100),
        },
        over_60: {
          count: Number.parseInt(row.over_60),
          percentage: Math.round((Number.parseInt(row.over_60) / totalUsers) * 100),
        },
      },
    }

    return distribution
  } catch (error) {
    console.error("Error calculating age distribution:", error)
    throw error
  }
}

/**
 * Print age distribution report to console
 * @param {Object} distribution - Age distribution data
 */
function printAgeDistributionReport(distribution) {
  console.log("\n" + "=".repeat(50))
  console.log("AGE DISTRIBUTION REPORT")
  console.log("=".repeat(50))
  console.log(`Total Users: ${distribution.totalUsers}`)
  console.log("-".repeat(30))
  console.log("Age-Group".padEnd(15) + "% Distribution")
  console.log("-".repeat(30))
  console.log(`< 20`.padEnd(15) + `${distribution.distribution.under_20.percentage}`)
  console.log(`20 to 40`.padEnd(15) + `${distribution.distribution["20_to_40"].percentage}`)
  console.log(`40 to 60`.padEnd(15) + `${distribution.distribution["40_to_60"].percentage}`)
  console.log(`> 60`.padEnd(15) + `${distribution.distribution.over_60.percentage}`)
  console.log("=".repeat(50) + "\n")
}

module.exports = {
  calculateAgeDistribution,
  printAgeDistributionReport,
}
