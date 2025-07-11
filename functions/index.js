const functions = require('firebase-functions');
const admin = require('firebase-admin');
const axios = require('axios');
const cheerio = require('cheerio');
const { S3 } = require('aws-sdk');

// Initialize Firebase Admin SDK
admin.initializeApp();
const db = admin.firestore();

// --- Configuration ---
// IMPORTANT: This APP_ID is specific to your Firebase project.
const APP_ID = '1:954907716498:web:deb01f747073610f07ef4b';

// Base URL where you host your ROMs and thumbnails.
// This is crucial. The 'url' and 'thumbnail' fields in Firestore will point here.
const COLLECTION_PATH = `artifacts/${APP_ID}/public/data/games`;
const BASE_URLS = {
    archive: 'https://archive.org/download',
    myrient: 'https://myrient.erista.me/files',
    libretro: 'https://thumbnails.libretro.com',
    mega65: 'https://files.mega65.org',
    custom: 'https://my-emulation-site-cdn.com' // <--- REPLACE THIS WITH YOUR ACTUAL CDN URL
};
const SOURCES = {
    archive: {
        baseUrl: BASE_URLS.archive,
        searchUrl: 'https://archive.org/advancedsearch.php',
        systems: ['mame', 'atari2600', 'nes', 'snes']
    },
    myrient: {
        baseUrl: BASE_URLS.myrient,
        systems: ['nes', 'snes', 'genesis', 'ps1']
    },
    libretro: {
        baseUrl: BASE_URLS.libretro,
        systems: ['nes', 'snes', 'gba', 'n64']
    },
    mega65: {
        baseUrl: BASE_URLS.mega65,
        systems: ['c64']
    },
    nesninja: { // <--- NEW NESNINJA SOURCE ADDED
        baseUrl: 'https://nesninja.com',
        systems: ['nes'] // NESNinja primarily focuses on NES
    }
};
const BATCH_SIZE = 499; // Firestore batch limit
const REQUEST_DELAY = 2000; // 2-second delay for rate limiting

// Genre mapping
const GENRE_MAP = {
    'mario': 'Platformer',
    'zelda': 'Adventure',
    'street fighter': 'Fighting',
    'sonic': 'Platformer',
    'final fantasy': 'RPG & Strategy',
    'pac-man': 'Puzzle',
    'metroid': 'Action', // Adjusted from Adventure based on common usage
    'doom': 'Shooter',
    'tetris': 'Puzzle',
    'kernal': 'System',
    'basic': 'System',
    'castlevania': 'Platformer',
    'duck hunt': 'Shmups',
    'chrono trigger': 'RPG & Strategy',
    'mega man': 'Platformer',
    'pokemon': 'RPG & Strategy',
    'columns': 'Puzzle',
    'phantasy star': 'RPG & Strategy',
    'thunderbirds': 'Shmups',
    'slugfest': 'Sports'
};

// S3 client (optional, for hosting on your CDN)
const s3 = new S3({
    accessKeyId: functions.config().s3?.access_key, // Use optional chaining for safety
    secretAccessKey: functions.config().s3?.secret_key,
    endpoint: functions.config().s3?.endpoint // E.g., 's3.amazonaws.com' or custom endpoint
});

// Helper: Infer category
function inferCategory(name) {
    name = name.toLowerCase();
    for (const [key, category] of Object.entries(GENRE_MAP)) {
        if (name.includes(key)) return category;
    }
    return randomChoice(['Platformer', 'RPG & Strategy', 'Fighting', 'Shooter', 'Puzzle', 'Adventure', 'Sports', 'Racing', 'Simulation', 'System']);
}

// Helper: Random choice
function randomChoice(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}

// Helper: Sleep
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper: Upload to S3 (uncomment and configure if hosting files)
// This function would typically be used if you download ROMs/thumbnails
// from external sources and then upload them to your own S3 bucket.
// Remember to configure S3 credentials via Firebase environment variables.
/*
async function uploadToS3(fileUrl, key, bucketName) {
    try {
        const response = await axios.get(fileUrl, { responseType: 'arraybuffer' }); // Get binary data
        const contentType = response.headers['content-type'] || 'application/octet-stream';

        await s3.upload({
            Bucket: bucketName, // Ensure this is correctly set from functions.config().s3.bucket or hardcoded
            Key: key,
            Body: response.data,
            ContentType: contentType,
            ACL: 'public-read' // Make the uploaded file publicly accessible
        }).promise();
        console.log(`Uploaded ${key} to S3`);
        // Return the full public URL of the uploaded file
        // Adjust endpoint if using custom S3 endpoint, e.g., `https://${bucketName}.s3.amazonaws.com/${key}`
        return `${functions.config().s3.endpoint}/${bucketName}/${key}`;
    } catch (error) {
        console.error(`Error uploading ${key} to S3:`, error.message);
        return null; // Indicate failure
    }
}
*/

// --- External Data Fetching Functions ---
// IMPORTANT: These are conceptual. You MUST implement the actual logic, respecting ToS and rate limits.

// Internet Archive: Use API
async function fetchInternetArchiveData() {
    console.log('Fetching data from Internet Archive...');
    const fetchedGames = [];
    for (const system of SOURCES.archive.systems) {
        try {
            const query = `q=collection:software AND mediatype:software AND subject:${system}&fl[]=identifier,title&rows=50&output=json`;
            const response = await axios.get(`${SOURCES.archive.searchUrl}?${query}`, {
                headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }, // Identify your scraper
                timeout: 10000
            });
            const items = response.data.response.docs;
            for (const item of items) {
                const name = item.title || 'Unknown';
                const identifier = item.identifier;
                const romUrl = `${BASE_URLS.archive}/${identifier}/${identifier}.zip`; // Common archive format
                const thumbnailUrl = `${BASE_URLS.archive}/${identifier}/__ia_thumb.jpg`; // Common thumbnail for IA items

                fetchedGames.push({
                    name,
                    url: romUrl,
                    thumbnail: thumbnailUrl,
                    system,
                    category: inferCategory(name),
                    source: 'Internet Archive'
                });
                await sleep(REQUEST_DELAY);
            }
            console.log(`Fetched ${items.length} games from Internet Archive (${system}).`);
        } catch (error) {
            console.error(`Error fetching from Internet Archive (${system}):`, error.message);
        }
        await sleep(REQUEST_DELAY * 2); // Longer delay between system fetches
    }
    return fetchedGames;
}

// Myrient: Scrape ROMs (Conceptual - Myrient is direct file hosting, not browsable HTML directories)
// Note: Myrient typically hosts large, organized No-Intro/Redump sets.
// Scraping HTML directories is generally unreliable for Myrient.
// For Myrient, you'd usually have a local copy of their DAT files and match hashes,
// or construct direct download links if you know the exact file paths.
async function fetchMyrientData() {
    console.log('Fetching data from Myrient (conceptual scraping)...');
    const fetchedGames = [];
    for (const system of SOURCES.myrient.systems) {
        try {
            // Myrient structure example: https://myrient.erista.me/files/No-Intro/Nintendo%20-%20Nintendo%20Entertainment%20System/
            // This is a simplified example. Real Myrient data fetching would involve
            // parsing directory listings (if accessible) or knowing exact file paths.
            let systemPathSegment = '';
            switch (system) {
                case 'nes': systemPathSegment = 'No-Intro/Nintendo - Nintendo Entertainment System'; break;
                case 'snes': systemPathSegment = 'No-Intro/Nintendo - Super Nintendo Entertainment System'; break;
                case 'genesis': systemPathSegment = 'No-Intro/Sega - Mega Drive - Genesis'; break;
                case 'ps1': systemPathSegment = 'Redump/Sony - PlayStation'; break; // PS1 might be Redump
                default: continue;
            }

            const directoryUrl = `${BASE_URLS.myrient}/${systemPathSegment}/`;
            const response = await axios.get(directoryUrl, {
                headers: { 'User-Agent': 'Mozilla/5.0' },
                timeout: 10000
            });
            const $ = cheerio.load(response.data);
            const romLinks = $('a[href$=".zip"], a[href$=".7z"], a[href$=".iso"], a[href$=".chd"]').slice(0, 50); // Limit to 50

            for (const element of romLinks) {
                const fileName = $(element).attr('href');
                const name = fileName.replace(/\.(zip|7z|iso|chd)$/i, '').replace(/ \([^)]+\)/g, '').trim();
                if (name) {
                    const romUrl = `${directoryUrl}${fileName}`; // Myrient's direct download URL
                    const thumbnail = `${BASE_URLS.custom}/thumbs/${system}/${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}_thumb.png`; // Points to your CDN

                    fetchedGames.push({
                        name,
                        url: romUrl,
                        thumbnail,
                        system,
                        category: inferCategory(name),
                        source: 'Myrient'
                    });
                }
                await sleep(REQUEST_DELAY);
            }
            console.log(`Fetched ${romLinks.length} games from Myrient (${system}).`);
        } catch (error) {
            console.error(`Error fetching from Myrient (${system}):`, error.message);
        }
        await sleep(REQUEST_DELAY * 2);
    }
    return fetchedGames;
}


// Libretro: Fetch thumbnails and metadata
async function fetchLibretroData() {
    console.log('Fetching data from Libretro...');
    const fetchedGames = [];
    for (const system of SOURCES.libretro.systems) {
        try {
            let systemDisplayName = '';
            switch (system) {
                case 'nes': systemDisplayName = 'Nintendo - Nintendo Entertainment System'; break;
                case 'snes': systemDisplayName = 'Nintendo - Super Nintendo Entertainment System'; break;
                case 'gba': systemDisplayName = 'Nintendo - Game Boy Advance'; break;
                case 'n64': systemDisplayName = 'Nintendo - Nintendo 64'; break;
                default: continue;
            }

            const thumbnailListUrl = `${BASE_URLS.libretro}/${systemDisplayName}/Named_Boxarts/`;
            const response = await axios.get(thumbnailListUrl, {
                headers: { 'User-Agent': 'Mozilla/5.0' },
                timeout: 10000
            });
            const $ = cheerio.load(response.data);
            const thumbnails = $('a[href$=".png"]').slice(0, 50); // Limit to 50 per system

            for (const element of thumbnails) {
                const thumbnailFileName = $(element).attr('href');
                const name = thumbnailFileName.replace('.png', '').replace(/ \([^)]+\)/g, '').trim();
                if (name) {
                    const romUrl = `${BASE_URLS.custom}/roms/${system}/${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}.zip`; // Assume you host ROMs
                    const thumbnail = `${thumbnailListUrl}${thumbnailFileName}`; // Points to Libretro's hosted thumbnail

                    fetchedGames.push({
                        name: name,
                        url: romUrl,
                        thumbnail: thumbnail,
                        system: system,
                        category: inferCategory(name),
                        source: 'Libretro'
                    });
                }
                await sleep(REQUEST_DELAY);
            }
            console.log(`Fetched ${thumbnails.length} thumbnails for ${system} from Libretro.`);
        } catch (error) {
            console.error(`Error fetching from Libretro (${system}):`, error.message);
        }
        await sleep(REQUEST_DELAY * 2);
    }
    return fetchedGames;
}

// MEGA65: Fetch open-source ROMs (Conceptual - based on known files)
async function fetchMega65Data() {
    console.log('Fetching data from MEGA65...');
    const fetchedGames = [];
    try {
        // These are known open-source ROMs from files.mega65.org
        const roms = [
            { name: 'C64 KERNAL', file: 'c64_kernal.rom', system: 'c64', category: 'System' },
            { name: 'C64 BASIC', file: 'c64_basic.rom', system: 'c64', category: 'System' },
            { name: 'C64 Character ROM', file: 'c64_char.rom', system: 'c64', category: 'System' },
            // Add more known open-source files from MEGA65 if available
        ];
        for (const rom of roms) {
            const name = rom.name;
            const romUrl = `${BASE_URLS.mega65}/roms/${rom.file}`; // Direct link to ROM
            const category = rom.category;
            const thumbnail = `${BASE_URLS.custom}/thumbs/${rom.system}/${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}_thumb.png`; // Points to your CDN

            fetchedGames.push({
                name,
                url: romUrl,
                thumbnail,
                system: rom.system,
                category,
                source: 'MEGA65'
            });
            await sleep(REQUEST_DELAY);
        }
        console.log(`Fetched ${roms.length} ROMs from MEGA65.`);
    } catch (error) {
        console.error('Error fetching from MEGA65:', error.message);
    }
    return fetchedGames;
}

// NESNinja: Fetch game data from NESNinja.com by scraping.
// This is a conceptual implementation. Real scraping requires robust error handling,
// pagination, and adherence to robots.txt and terms of service.
// You would typically scrape their game listings (e.g., by category or alphabetical).
async function fetchNESNinjaData() {
    console.log('Fetching data from NESNinja.com...');
    const fetchedGames = [];
    const system = 'nes'; // NESNinja is primarily NES

    try {
        // Example: Scrape their main game list or a category page
        // You'll need to inspect NESNinja.com's HTML to find the correct selectors for game names and links.
        // This is a simplified example, a real implementation would need to handle pagination and specific categories.
        const url = `${SOURCES.nesninja.baseUrl}/games`; // Example URL for a game list
        const response = await axios.get(url, {
            headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Google-Cloud-Function/1.0)' },
            timeout: 15000
        });
        const $ = cheerio.load(response.data);

        // This selector is an example. You need to find the actual HTML elements
        // that contain game titles and links on nesninja.com
        $('a[href*="/roms/"]').each((i, element) => { // Replace '.game-link' with the actual CSS selector
            const name = $(element).text().trim();
            const relativeUrl = $(element).attr('href');

            if (name && relativeUrl) {
                // Construct the full URL to the ROM.
                // IMPORTANT: NESNinja.com links to ROMs hosted elsewhere.
                // You must ensure you have legal permission to link directly to those ROMs,
                // or preferably, download legally obtained ROMs and host them on your CDN.
                const romUrl = `${BASE_URLS.custom}/roms/${system}/${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}.nes`; // Points to YOUR hosted ROM
                const thumbnailUrl = `${BASE_URLS.custom}/thumbs/${system}/${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}_thumb.png`; // Points to YOUR hosted thumbnail

                fetchedGames.push({
                    name,
                    url: romUrl,
                    thumbnail: thumbnailUrl,
                    system: system,
                    category: inferCategory(name), // Infer category based on game name
                    source: 'NESNinja.com'
                });
            }
        });
        console.log(`Fetched ${fetchedGames.length} games from NESNinja.com.`);

    } catch (error) {
        console.error(`Error fetching from NESNinja.com:`, error.message);
    }
    await sleep(REQUEST_DELAY * 2); // Longer delay after fetching from a source
    return fetchedGames;
}


/**
 * Aggregates data from all defined external sources.
 * @returns {Promise<Array<Object>>} Combined array of games from all sources.
 */
async function getAllExternalGames() {
    const externalSources = [
        fetchInternetArchiveData(),
        fetchMyrientData(),
        fetchLibretroData(),
        fetchMega65Data(),
        fetchNESNinjaData() // <--- ADDED NESNINJA HERE
        // Add more source fetching functions here (e.g., Vimm's Lair, etc.)
    ];

    const results = await Promise.allSettled(externalSources);
    let allExternalGames = [];

    results.forEach(result => {
        if (result.status === 'fulfilled') {
            allExternalGames = allExternalGames.concat(result.value);
        } else {
            console.error('Failed to fetch data from one source:', result.reason);
        }
    });

    // Deduplicate games by a unique key (sanitized name + system)
    const uniqueGames = new Map(); // Using Map to preserve insertion order and for efficient lookup
    allExternalGames.forEach(game => {
        const uniqueKey = `${game.name.toLowerCase().replace(/[^a-z0-9]/g, '_')}-${game.system}`;
        if (!uniqueGames.has(uniqueKey)) {
            uniqueGames.set(uniqueKey, {
                ...game,
                category: inferCategory(game.name) // Re-infer category for consistency
            });
        }
        // If a game already exists, you could add logic here to prioritize sources
        // For now, first one wins.
    });

    return Array.from(uniqueGames.values());
}

// --- Firestore Sync Logic ---

/**
 * Syncs external game data with Firestore.
 * This function is designed to be idempotent (can be run multiple times without issues).
 */
async function syncFirestoreWithExternalData() {
    console.log('Starting Firestore sync process...');

    // 1. Get existing games from Firestore
    let existingGamesMap = new Map(); // Map<uniqueKey, {firestoreDocId, gameData}>
    try {
        const snapshot = await db.collection(FIRESTORE_COLLECTION_PATH).get();
        snapshot.forEach(doc => {
            const game = doc.data();
            // Use a robust unique key for comparison (e.g., sanitized name + system)
            const uniqueKey = `${game.name.toLowerCase().replace(/[^a-z0-9]/g, '_')}-${game.system}`;
            existingGamesMap.set(uniqueKey, { id: doc.id, data: game });
        });
        console.log(`Found ${existingGamesMap.size} existing games in Firestore.`);
    } catch (error) {
        console.error('Error fetching existing games from Firestore:', error);
        throw new Error('Failed to retrieve existing games for sync.');
    }

    // 2. Get games from all external sources
    const externalGames = await getAllExternalGames();
    console.log(`Fetched ${externalGames.length} unique games from external sources.`);

    // 3. Prepare batch writes for Firestore updates
    let currentBatch = db.batch();
    let updatesCount = 0;
    let additionsCount = 0;
    let batchOperations = 0;

    for (const game of externalGames) { // Renamed 'game' from 'externalGame' for consistency with outer loop
        const uniqueKey = `${game.name.toLowerCase().replace(/[^a-z0-9]/g, '_')}-${game.system}`;
        const existingGame = existingGamesMap.get(uniqueKey);

        if (existingGame) {
            // Game exists, check for updates
            const currentData = existingGame.data;
            let needsUpdate = false;

            // Prioritize external data if it's more "real" or different from placeholders
            // Check if the current URL is a placeholder or if the new URL is different and not a placeholder
            if (game.url && (currentData.url.startsWith('https://example.com') || currentData.url.startsWith('https://placehold.co') || currentData.url !== game.url)) {
                currentData.url = game.url;
                needsUpdate = true;
            }
            // Check if the current thumbnail is a placeholder or if the new thumbnail is different and not a placeholder
            if (game.thumbnail && (currentData.thumbnail.startsWith('https://placehold.co') || currentData.thumbnail !== game.thumbnail)) {
                currentData.thumbnail = game.thumbnail;
                needsUpdate = true;
            }
            // Update category if it's 'Other' or more specific from external source
            if (currentData.category === 'Other' && game.category !== 'Other') {
                currentData.category = game.category;
                needsUpdate = true;
            }
            // Add or update source if it's new or more specific
            if (game.source && currentData.source !== game.source) {
                currentData.source = game.source;
                needsUpdate = true;
            }

            if (needsUpdate) {
                const docRef = db.collection(FIRESTORE_COLLECTION_PATH).doc(existingGame.id);
                currentBatch.update(docRef, currentData);
                updatesCount++;
                batchOperations++;
            }
        } else {
            // Game is new, add it to Firestore
            const docRef = db.collection(FIRESTORE_COLLECTION_PATH).doc(); // Let Firestore generate ID
            currentBatch.set(docRef, game);
            additionsCount++;
            batchOperations++;
        }

        // Commit batch if it's full
        if (batchOperations >= BATCH_SIZE) {
            console.log(`Committing batch with ${batchOperations} operations...`);
            await currentBatch.commit();
            batchOperations = 0; // Reset counter
            currentBatch = db.batch(); // Start a new batch
        }
    }

    // Commit any remaining operations in the last batch
    if (batchOperations > 0) {
        console.log(`Committing final batch with ${batchOperations} operations...`);
        await currentBatch.commit();
    }

    console(`Firestore sync complete. Added: ${additionsCount}, Updated: ${updatesCount}`);
    return null;
}

// --- Firebase Cloud Function Definition ---

// This function will run periodically (e.g., daily)
// It's configured to be triggered by a Pub/Sub topic, which Cloud Scheduler will publish to.
exports.syncGames = functions
    .runWith({ memory: '1GB', timeoutSeconds: 540 }) // Increased memory and timeout for large syncs
    .pubsub.topic('firebase-schedule-syncGames') // This topic is automatically created by Firebase
    .onPublish(async (message, context) => {
        try {
            await syncFirestoreWithExternalData();
            console.log('Game sync function executed successfully.');
            return null; // Indicate success
        } catch (error) {
            console.error('Error during game sync function execution:', error);
            // You might want to send an alert or log to a dedicated error tracking system
            throw new Error('Game sync failed.'); // Indicate failure
        }
    });

/*
// Helper: Upload to S3 (uncomment and configure if hosting files)
// This function would typically be used if you download ROMs/thumbnails
// from external sources and then upload them to your own S3 bucket.
// Remember to configure S3 credentials via Firebase environment variables.
async function uploadToS3(fileUrl, key, bucketName) {
    try {
        const response = await axios.get(fileUrl, { responseType: 'arraybuffer' }); // Get binary data
        const contentType = response.headers['content-type'] || 'application/octet-stream';

        await s3.upload({
            Bucket: bucketName, // Ensure this is correctly set from functions.config().s3.bucket or hardcoded
            Key: key,
            Body: response.data,
            ContentType: contentType,
            ACL: 'public-read' // Make the uploaded file publicly accessible
        }).promise();
        console.log(`Uploaded ${key} to S3`);
        // Return the full public URL of the uploaded file
        // Adjust endpoint if using custom S3 endpoint, e.g., `https://${bucketName}.s3.amazonaws.com/${key}`
        return `${functions.config().s3.endpoint}/${bucketName}/${key}`;
    } catch (error) {
        console.error(`Error uploading ${key} to S3:`, error.message);
        return null; // Indicate failure
    }
}
*/