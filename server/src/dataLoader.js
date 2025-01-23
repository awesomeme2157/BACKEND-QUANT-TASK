// const fs = require('fs');
// const path = require('path');
// const csv = require('csv-parser');

// /**
//  * Load CSV data from a file and combine Date and Time into a single timestamp if necessary.
//  * @param {string} filePath - Path to the CSV file.
//  * @returns {Promise<Array>} - Parsed CSV data.
//  */
// function loadCSV(filePath) {
//     return new Promise((resolve, reject) => {
//         const results = [];
//         fs.createReadStream(filePath)
//             .pipe(csv())
//             .on('data', (data) => {
//                 if (data.Date && data.Time) {
//                     // Determine the date format and combine Date and Time into a single timestamp
//                     let dateParts;
//                     if (data.Date.includes('-')) {
//                         // Format: DD-MM-YYYY
//                         dateParts = data.Date.split('-');
//                         data.Date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
//                     } else if (data.Date.includes('/')) {
//                         // Format: DD/MM/YY
//                         dateParts = data.Date.split('/');
//                         data.Date = `20${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
//                     }
//                     const timestamp = `${data.Date}T${data.Time}Z`;
//                     results.push({
//                         ...data,
//                         timestamp,
//                         last_traded_quantity: data['Last Traded Quantity'],
//                         stock_name: data['Stock Name']
//                     });
//                 } else {
//                     // For SampleDayData.csv which does not have Time column
//                     results.push(data);
//                 }
//             })
//             .on('end', () => {
//                 resolve(results);
//             })
//             .on('error', (err) => {
//                 reject(err);
//             });
//     });
// }

// /**
//  * Normalize intraday data by ensuring the timestamp field is valid and in ISO format.
//  * @param {Array} data - Raw intraday data.
//  * @returns {Array} - Normalized intraday data.
//  */
// function normalizeIntradayData(data) {
//     return data.map(entry => {
//         if (!entry.timestamp) {
//             console.error('Missing timestamp field:', entry);
//             return null; // or handle appropriately
//         }
//         const date = new Date(entry.timestamp);
//         if (isNaN(date)) {
//             console.error('Invalid date value:', entry.timestamp);
//             return null; // or handle appropriately
//         }
//         return {
//             ...entry,
//             timestamp: date.toISOString()
//         };
//     }).filter(entry => entry !== null); // Remove invalid entries if necessary
// }

// module.exports = { loadCSV, normalizeIntradayData };


const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

/**
 * Load CSV data from a file and combine Date and Time into a single timestamp if necessary.
 * @param {string} filePath - Path to the CSV file.
 * @returns {Promise<Array>} - Parsed CSV data.
 */
function loadCSV(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => {
                if (data.Date && data.Time) {
                    // Determine the date format and combine Date and Time into a single timestamp
                    let dateParts;
                    if (data.Date.includes('-')) {
                        // Format: DD-MM-YYYY
                        dateParts = data.Date.split('-');
                        data.Date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
                    } else if (data.Date.includes('/')) {
                        // Format: DD/MM/YY
                        dateParts = data.Date.split('/');
                        data.Date = `20${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
                    }
                    const timestamp = `${data.Date}T${data.Time}Z`;
                    results.push({
                        ...data,
                        timestamp,
                        last_traded_quantity: data['Last Traded Quantity'],
                        stock_name: data['Stock Name']
                    });
                } else {
                    // For SampleDayData.csv which does not have Time column
                    results.push(data);
                }
            })
            .on('end', () => {
                resolve(results);
            })
            .on('error', (err) => {
                reject(err);
            });
    });
}

/**
 * Normalize intraday data by ensuring the timestamp field is valid and in ISO format.
 * @param {Array} data - Raw intraday data.
 * @returns {Array} - Normalized intraday data.
 */
function normalizeIntradayData(data) {
    return data.map(entry => {
        if (!entry.timestamp) {
            console.error('Missing timestamp field:', entry);
            return null; // or handle appropriately
        }
        const date = new Date(entry.timestamp);
        if (isNaN(date)) {
            console.error('Invalid date value:', entry.timestamp);
            return null; // or handle appropriately
        }
        return {
            ...entry,
            timestamp: date.toISOString()
        };
    }).filter(entry => entry !== null); // Remove invalid entries if necessary
}

module.exports = { loadCSV, normalizeIntradayData };