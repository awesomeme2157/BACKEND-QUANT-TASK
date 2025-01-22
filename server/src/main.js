const path = require('path');
const fs = require('fs');
const { loadCSV } = require('./dataLoader');
const { preprocessDayData, calculateRollingVolume } = require('./processor');

(async () => {
    try {
        // File paths
        const sampleDayFile = path.join(__dirname, '../data/SampleDayData.csv');
        const intradayFile19 = path.join(__dirname, '../data/19thAprilSampleData.csv');
        const intradayFile22 = path.join(__dirname, '../data/22ndAprilSampleData.csv');
        const outputFile = path.join(__dirname, '../output/results.json');

        // Measure time to load data
        console.time('Load CSV Files');
        const dayData = await loadCSV(sampleDayFile);
        const intradayData19 = await loadCSV(intradayFile19);
        const intradayData22 = await loadCSV(intradayFile22);
        console.timeEnd('Load CSV Files'); // Log how long it took to load files

        // Measure time to preprocess day data
        console.time('Process Day Data');
        const avgVolumes = preprocessDayData(dayData);
        console.timeEnd('Process Day Data'); // Log how long it took to calculate 30-day averages

        // Measure time to process intraday data
        console.time('Process Intraday Data for 19th April');
        const result19 = calculateRollingVolume(intradayData19, avgVolumes);
        console.timeEnd('Process Intraday Data for 19th April');

        console.time('Process Intraday Data for 22nd April');
        const result22 = calculateRollingVolume(intradayData22, avgVolumes);
        console.timeEnd('Process Intraday Data for 22nd April');

        // Combine and save results
        console.time('Save Results');
        const finalResults = { '19th April 2024': result19, '22nd April 2024': result22 };
        fs.writeFileSync(outputFile, JSON.stringify(finalResults, null, 2));
        console.timeEnd('Save Results'); // Log how long it took to save the results

        console.log(`Results saved to ${outputFile}`);
    } catch (err) {
        console.error('Error:', err);
    }
})();
