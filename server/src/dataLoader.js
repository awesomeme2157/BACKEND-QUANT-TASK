const fs = require('fs');
const csvParser = require('csv-parser');

/**
 * Load a CSV file into an array of objects.
 * @param {string} filePath - Path to the CSV file.
 * @returns {Promise<Array>} - Parsed CSV data.
 */
function loadCSV(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', (err) => reject(err));
    });
}

module.exports = { loadCSV };
