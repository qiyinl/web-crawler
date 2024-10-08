const axios = require('axios');
const fs = require('fs');

// Function to fetch and save JSON data from multiple URLs
async function fetchAndSaveJson(urls, outputFilePath) {
    const allData = {};

    for (const url of urls) {
        try {
            // Fetch data from the current URL
            const response = await axios.get(url);
            const jsonData = response.data;

            // Label each JSON object with a sanitized version of the URL
            let label = url.replace(/https?:\/\/|www\.|\/|[^a-zA-Z0-9]/g, '_');
            
            // Remove any leading underscores (in case the label starts with '_')
            label = label.replace(/^_+/, '');
            
            allData[label] = jsonData;
            console.log(`Fetched data from: ${url}`);
        } catch (error) {
            console.error(`Error fetching JSON from ${url}:`, error);
        }
    }

    // Write all collected data into a single JSON file
    return new Promise((resolve, reject) => {
        fs.writeFile(outputFilePath, JSON.stringify(allData, null, 2), (err) => {
            if (err) {
                console.error('Error writing to file:', err);
                reject(err);
            } else {
                console.log('Data saved successfully to', outputFilePath);
                resolve();
            }
        });
    });
}

// Function to read combined JSON and process domain data, removing duplicates
function processJsonAndExtractDomains(inputFilePath, outputFilePath) {
    return new Promise((resolve, reject) => {
        // Read the combined output JSON file
        fs.readFile(inputFilePath, 'utf8', (err, data) => {
            if (err) {
                console.error('Error reading file:', err);
                reject(err);
                return;
            }

            try {
                const combinedData = JSON.parse(data);
                const domainData = {};

                // Loop over each entry in the combined JSON
                for (const label in combinedData) {
                    if (combinedData.hasOwnProperty(label)) {
                        const entry = combinedData[label];
                        
                        // Extract the sellers array if it exists
                        const sellers = entry.sellers || [];
                        const domains = sellers.map(seller => seller.domain);

                        // Remove duplicate domains using Set
                        const uniqueDomains = Array.from(new Set(domains));

                        // Store the count and list of unique domains for this URL
                        domainData[label] = {
                            count: uniqueDomains.length,
                            'Unique domains': uniqueDomains
                        };
                    }
                }

                // Write the new domain data to a JSON file
                fs.writeFile(outputFilePath, JSON.stringify(domainData, null, 2), (err) => {
                    if (err) {
                        console.error('Error writing to file:', err);
                        reject(err);
                    } else {
                        console.log('Domain data saved successfully to', outputFilePath);
                        resolve();
                    }
                });
            } catch (parseError) {
                console.error('Error parsing JSON:', parseError);
                reject(parseError);
            }
        });
    });
}

// Function to consolidate all unique domains and count their occurrences
function consolidateUniqueDomains(inputFilePath, outputFilePath) {
    return new Promise((resolve, reject) => {
        fs.readFile(inputFilePath, 'utf8', (err, data) => {
            if (err) {
                console.error('Error reading file:', err);
                reject(err);
                return;
            }

            try {
                const domainData = JSON.parse(data);
                const consolidatedDomains = {};
                let totalUniqueUrlCount = 0; // To keep track of the sum of all counts

                // Loop through each entry to count domain occurrences and sum unique URLs
                for (const entry of Object.entries(domainData)) {
                    const label = entry[0];
                    const uniqueDomains = entry[1]['Unique domains'];
                    const count = entry[1]['count'];

                    // Add the count of unique domains from this label to the total
                    totalUniqueUrlCount += count;

                    uniqueDomains.forEach(domain => {
                        // Increment the count for each domain
                        if (consolidatedDomains[domain]) {
                            consolidatedDomains[domain]++;
                        } else {
                            consolidatedDomains[domain] = 1;
                        }
                    });
                }

                // Sort the domains by their counts in descending order
                const sortedDomains = Object.entries(consolidatedDomains)
                    .sort(([, countA], [, countB]) => countB - countA)
                    .reduce((obj, [key, value]) => {
                        obj[key] = value;
                        return obj;
                    }, {});

                // Get the current date in dd-mm-yyyy format
                const crawlDate = new Date();
                const formattedDate = `${String(crawlDate.getDate()).padStart(2, '0')}-${String(crawlDate.getMonth() + 1).padStart(2, '0')}-${crawlDate.getFullYear()}`;

                // Add the count of unique URLs to the output
                const outputData = {
                    crawlDate: formattedDate,
                    uniqueUrlCount: totalUniqueUrlCount, // Total sum of counts of all unique domains
                    domains: sortedDomains
                };

                // Write consolidated domains and their counts to a new JSON file
                fs.writeFile(outputFilePath, JSON.stringify(outputData, null, 2), (err) => {
                    if (err) {
                        console.error('Error writing to file:', err);
                        reject(err);
                    } else {
                        console.log('Consolidated domain data saved successfully to', outputFilePath);
                        resolve();
                    }
                });
            } catch (parseError) {
                console.error('Error parsing JSON:', parseError);
                reject(parseError);
            }
        });
    });
}

// Main function to execute the tasks sequentially
async function main() {
    const urls = [
        'https://awg.la/sellers.json',
        'https://revry.tv/sellers.json',
        'https://www.philo.com/sellers.json',
        'https://www.freewheel.com/sellers.json',
        'https://pubmatic.com/sellers.json'
        // Add more URLs as needed
    ];

    const combinedOutputFilePath = './combinedOutput.json';
    const domainOutputFilePath = './domainData.json';
    const consolidatedDomainOutputFilePath = './consolidatedDomainData.json';

    try {
        // Step 1: Fetch and save JSON data
        await fetchAndSaveJson(urls, combinedOutputFilePath);
        
        // Step 2: Process the combined JSON and extract domains, removing duplicates
        await processJsonAndExtractDomains(combinedOutputFilePath, domainOutputFilePath);
        
        // Step 3: Consolidate unique domains and count occurrences
        await consolidateUniqueDomains(domainOutputFilePath, consolidatedDomainOutputFilePath);
        
        console.log('All tasks completed successfully.');
    } catch (error) {
        console.error('Error during execution:', error);
    }
}

// Execute the main function
main();
