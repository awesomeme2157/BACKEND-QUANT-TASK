/**
 * Preprocess day data to calculate 30-day average volume for each stock.
 * @param {Array} dayData - Daily volume data.
 * @returns {Object} - Average volumes by stock name.
 */
function preprocessDayData(dayData) {
    const volumeSums = {};
    const volumeCounts = {};

    dayData.forEach(row => {
        const stock = row['Stock Name'];
        const volume = parseInt(row.Volume, 10);

        if (!volumeSums[stock]) {
            volumeSums[stock] = 0;
            volumeCounts[stock] = 0;
        }

        volumeSums[stock] += volume;
        volumeCounts[stock] += 1;
    });

    const avgVolumes = {};
    for (const stock in volumeSums) {
        avgVolumes[stock] = volumeSums[stock] / volumeCounts[stock];
    }

    return avgVolumes;
}

/**
 * Calculate rolling 60-minute cumulative volume and compare to average volume.
 * @param {Array} intradayData - Second-by-second intraday data.
 * @param {Object} avgVolumes - Average volumes by stock name.
 * @returns {Array} - Results with stock name and timestamp.
 */
function calculateRollingVolume(intradayData, avgVolumes) {
    const results = [];
    const grouped = intradayData.reduce((acc, row) => {
        acc[row.stock_name] = acc[row.stock_name] || [];
        acc[row.stock_name].push(row);
        return acc;
    }, {});

    for (const stock in grouped) {
        const stockData = grouped[stock];

        // Filter for trading hours (9:15 AM to 3:30 PM)
        const marketOpen = new Date('1970-01-01T09:15:00Z').getTime();
        const marketClose = new Date('1970-01-01T15:30:00Z').getTime();
        const filteredData = stockData.filter(row => {
            const date = new Date(row.timestamp);
            const time = date.getTime() % (24 * 60 * 60 * 1000);
            console.log(`Stock: ${stock}, Timestamp: ${row.timestamp}, Date: ${date}, Time: ${time}, Market Open: ${marketOpen}, Market Close: ${marketClose}`);
            return time >= marketOpen && time <= marketClose;
        });

        if (filteredData.length === 0) {
            console.log(`No data for stock ${stock} within trading hours.`);
            continue;
        }

        // Calculate prefix sums for cumulative volume
        const prefixSums = [];
        let cumulativeSum = 0;

        filteredData.forEach((row, i) => {
            const volume = parseInt(row.last_traded_quantity, 10);
            cumulativeSum += volume;
            prefixSums[i] = cumulativeSum;
        });

        // Find the first timestamp where the rolling 60-minute volume exceeds the 30-day average
        let found = false;
        for (let i = 0; i < filteredData.length; i++) {
            const windowStartTime = new Date(filteredData[i].timestamp).getTime() - 3600000; // 60 minutes in ms
            const startIndex = filteredData.findIndex(row => new Date(row.timestamp).getTime() >= windowStartTime);
            const rollingVolume = prefixSums[i] - (startIndex > 0 ? prefixSums[startIndex - 1] : 0);

            console.log(`Stock: ${stock}, Timestamp: ${filteredData[i].timestamp}, Rolling Volume: ${rollingVolume}, Average Volume: ${avgVolumes[stock]}`);

            if (rollingVolume > avgVolumes[stock]) {
                results.push({ stock_name: stock, timestamp: filteredData[i].timestamp });
                found = true;
                break;
            }
        }

        // If no crossover found, add null
        if (!found) {
            results.push({ stock_name: stock, timestamp: null });
        }
    }

    return results;
}

module.exports = { preprocessDayData, calculateRollingVolume };
