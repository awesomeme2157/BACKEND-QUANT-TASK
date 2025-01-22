/**
 * Preprocess daily data to calculate the 30-day average volume for each stock.
 * @param {Array} dayData - Daily trading data.
 * @returns {Object} - Average volumes by stock name.
 */
function preprocessDayData(dayData) {
    const averages = {};

    dayData.forEach(row => {
        row.Date = new Date(row.Date);
        row.Volume = parseInt(row.Volume, 10);
    });

    const stocks = [...new Set(dayData.map(row => row['Stock Name']))];

    stocks.forEach(stock => {
        const stockData = dayData.filter(row => row['Stock Name'] === stock);
        stockData.sort((a, b) => a.Date - b.Date);

        for (let i = 29; i < stockData.length; i++) {
            const last30Days = stockData.slice(i - 29, i + 1);
            const avgVolume = last30Days.reduce((sum, r) => sum + r.Volume, 0) / 30;
            averages[stock] = avgVolume;
        }
    });

    return averages;
}

/**
 * Calculate the timestamp when the rolling cumulative volume exceeds the 30-day average.
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
        let cumulativeSum = 0;

        for (let i = 0; i < stockData.length; i++) {
            cumulativeSum += parseInt(stockData[i].last_traded_quantity, 10);

            // Calculate the rolling 60-minute volume
            const windowStart = stockData.findIndex(
                r => new Date(stockData[i].timestamp) - new Date(r.timestamp) <= 3600000
            );
            const rollingVolume = cumulativeSum - (windowStart > 0 ? cumulativeSum - stockData[windowStart].last_traded_quantity : 0);

            if (rollingVolume > avgVolumes[stock]) {
                results.push({ stock_name: stock, timestamp: stockData[i].timestamp });
                break;
            }
        }

        if (!results.find(r => r.stock_name === stock)) {
            results.push({ stock_name: stock, timestamp: null });
        }
    }

    return results;
}

module.exports = { preprocessDayData, calculateRollingVolume };
