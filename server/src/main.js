const path = require('path');
const fs = require('fs');
const { loadCSV, normalizeIntradayData } = require('./dataLoader');
const { preprocessDayData, calculateRollingVolume } = require('./processor');

(async () => {
    try {
        console.log('--- Starting Script ---');

        // File paths
        const sampleDayFile = path.join(__dirname, '../data/SampleDayData.csv');
        const intradayFile19 = path.join(__dirname, '../data/19thAprilSampleData.csv');
        const intradayFile22 = path.join(__dirname, '../data/22ndAprilSampleData.csv');
        const outputFile = path.join(__dirname, '../output/results.json');

        // Load and normalize data
        console.time('Load CSV Files');
        const dayData = await loadCSV(sampleDayFile);
        const intradayData19Raw = await loadCSV(intradayFile19);
        const intradayData22Raw = await loadCSV(intradayFile22);
        console.timeEnd('Load CSV Files');

        console.time('Normalize Intraday Data');
        const intradayData19 = normalizeIntradayData(intradayData19Raw);
        const intradayData22 = normalizeIntradayData(intradayData22Raw);
        console.timeEnd('Normalize Intraday Data');

        console.time('Process Day Data');
        const avgVolumes = preprocessDayData(dayData);
        console.timeEnd('Process Day Data');

        console.time('Process Intraday Data for 19th April');
        const results19 = calculateRollingVolume(intradayData19, avgVolumes);
        console.timeEnd('Process Intraday Data for 19th April');

        console.time('Process Intraday Data for 22nd April');
        const results22 = calculateRollingVolume(intradayData22, avgVolumes);
        console.timeEnd('Process Intraday Data for 22nd April');

        // Combine results
        const results = {
            "19th April 2024": results19.map(result => ({
                stock_name: result.stock_name,
                timestamp: result.timestamp ? `${result.timestamp.split('T')[0]} ${result.timestamp.split('T')[1].replace('Z', '')}` : null
            })),
            "22nd April 2024": results22.map(result => ({
                stock_name: result.stock_name,
                timestamp: result.timestamp ? `${result.timestamp.split('T')[0]} ${result.timestamp.split('T')[1].replace('Z', '')}` : null
            }))
        };

        // Save results to output file
        fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));
        console.log(`Results saved to ${outputFile}`);
    } catch (err) {
        console.error('Error:', err);
    }
})();
