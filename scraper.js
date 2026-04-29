require('dotenv').config();
const { chromium } = require('playwright');
const axios = require('axios');
const fs = require('fs/promises'); // Added for local file caching
const path = require('path');

/**
 * LinkedInJobScraper
 * An automated data extraction pipeline for LinkedIn Job search results.
 * Includes local file-system caching for idempotency (prevents duplicate extraction).
 */
class LinkedInJobScraper {
    constructor() {
        // Core Configuration & Environment Variables
        this.config = {
            startUrl: 'https://www.linkedin.com/jobs/search?f_F=it%2Ceng&f_JT=F%2CC&f_TPR=r86400&f_WT=1%2C3&location=' + process.env.LOCATION,
            cookie: process.env.LI_AT_COOKIE,   
            webhookUrl: process.env.N8N_WEBHOOK_URL,
            batchSize: 25,
            delays: {
                minNav: 2000,
                maxNav: 4000,
                pageLoadBuffer: 2000
            },
            timeouts: {
                domElement: 15000
            },
            userDataDir: './user_session',
            cacheFile: path.join(__dirname, 'scraped_jobs_cache.json') // Cache file location
        };

        // State Management
        this.currentBatch = [];
        this.allJobUrls = new Set(); // URLs to process in the current run
        this.scrapedJobIds = new Set(); // Global registry of historically scraped Job IDs
        this.browser = null;
        this.context = null;
        this.page = null;
    }

    /* ========================================================================
       PIPELINE ORCHESTRATION
       ======================================================================== */

    async run() {
        try {
            await this.loadCache(); // Load historical data first
            await this.initBrowser();
            await this.gatherJobUrls();
            await this.processJobs();
            console.log('🎉 Scraping pipeline completed successfully!');
        } catch (error) {
            console.error('💥 Fatal Pipeline Error:', error);
        } finally {
            await this.saveCache(); // Ensure cache is saved even if it crashes
            if (this.browser) {
                console.log('🧹 Cleaning up browser resources...');
                await this.browser.close();
            }
        }
    }

    /* ========================================================================
       CACHE MANAGEMENT
       ======================================================================== */

    /**
     * Loads previously scraped Job IDs from the local JSON file into memory.
     */
    async loadCache() {
        try {
            const data = await fs.readFile(this.config.cacheFile, 'utf8');
            const ids = JSON.parse(data);
            this.scrapedJobIds = new Set(ids);
            console.log(`📦 Cache Loaded: ${this.scrapedJobIds.size} previously scraped jobs skipped.`);
        } catch (error) {
            if (error.code === 'ENOENT') {
                console.log('📦 No existing cache found. Creating a new local registry.');
                this.scrapedJobIds = new Set();
            } else {
                console.error('❌ Failed to parse cache file. Starting fresh.', error.message);
                this.scrapedJobIds = new Set();
            }
        }
    }

    /**
     * Persists the in-memory Set of scraped Job IDs back to the local JSON file.
     */
    async saveCache() {
        try {
            const data = JSON.stringify(Array.from(this.scrapedJobIds), null, 2);
            await fs.writeFile(this.config.cacheFile, data, 'utf8');
            console.log(`💾 Cache Saved: Registry now contains ${this.scrapedJobIds.size} jobs.`);
        } catch (error) {
            console.error('❌ Failed to save cache file:', error.message);
        }
    }

    /**
     * Extracts the unique 10-digit Job ID from a LinkedIn Job URL.
     * @param {string} url - The LinkedIn job URL
     * @returns {string|null} - The Job ID
     */
    extractJobId(url) {
        const match = url.match(/\/view\/(\d+)/);
        return match ? match[1] : null;
    }

    /* ========================================================================
       SETUP & INITIALIZATION
       ======================================================================== */

    async initBrowser() {
        console.log('🚀 Launching Persistent Browser Context...');
        
        this.context = await chromium.launchPersistentContext(this.config.userDataDir, {
            headless: true, // Switch to false if manual login is required
            viewport: { width: 1280, height: 800 },
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            args: [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox'
            ]
        });

        this.page = this.context.pages()[0] || await this.context.newPage();

        await this.page.goto('https://www.linkedin.com/feed/');
        
        const loginButton = await this.page.$('.nav__button-secondary');
        if (loginButton || this.page.url().includes('login')) {
            console.log('⚠️ Manual Login Required. Please log in in the browser window...');
            await this.page.waitForURL('**/feed/**', { timeout: 0 }); 
            console.log('✅ Login detected! Session saved to folder.');
        }
    }

    /* ========================================================================
       PHASE 1: JOB DISCOVERY
       ======================================================================== */

    async gatherJobUrls() {
        console.log('🔍 Phase 1: Starting job URL discovery...');
        await this.page.goto(this.config.startUrl, { waitUntil: 'domcontentloaded' });
        await this.randomDelay(this.config.delays.minNav, this.config.delays.maxNav);

        let hasNextPage = true;
        let pageNum = 1;

        while (hasNextPage) {
            console.log(`📄 Scanning search page ${pageNum}...`);
            await this.scrollLeftPanel();

            const rawLinks = await this.page.$$eval('.job-card-container__link', anchors => 
                anchors.map(a => a.href.split('?')[0]) 
            );

            let newLinksFound = 0;
            rawLinks.forEach(link => {
                const jobId = this.extractJobId(link);
                // Only add to queue if it's NOT in cache AND NOT already queued
                if (jobId && !this.scrapedJobIds.has(jobId) && !this.allJobUrls.has(link)) {
                    this.allJobUrls.add(link);
                    newLinksFound++;
                }
            });

            console.log(`   ↳ Found ${newLinksFound} NEW links on this page. Queue size: ${this.allJobUrls.size}`);

            const nextPageNum = pageNum + 1;
            const nextButton = await this.page.$(`button[aria-label="Page ${nextPageNum}"]`);

            if (nextButton) {
                await nextButton.scrollIntoViewIfNeeded();
                await this.page.waitForTimeout(1000); 
                
                await nextButton.click();
                console.log(`   ↳ Navigating to page ${nextPageNum}...`);
                await this.page.waitForTimeout(4000); 
                pageNum++;
            } else {
                hasNextPage = false;
                console.log('✅ Reached the end of available search pagination.');
            }
        }
    }

    async scrollLeftPanel() {
        try {
            await this.page.waitForSelector('.job-card-container', { timeout: this.config.timeouts.domElement });
        } catch (error) {
            console.log('   ⚠️ No job cards found on this page. Aborting scroll.');
            return; 
        }

        await this.page.evaluate(async () => {
            function getScrollableParent(node) {
                if (node == null || node === document.body) return null;
                const overflowY = window.getComputedStyle(node).overflowY;
                const isScrollable = overflowY === 'auto' || overflowY === 'scroll';
                if (isScrollable && node.scrollHeight > node.clientHeight) return node;
                return getScrollableParent(node.parentNode);
            }

            const firstJobCard = document.querySelector('.job-card-container');
            if (!firstJobCard) return;

            const scrollableDiv = getScrollableParent(firstJobCard);
            if (!scrollableDiv) return;

            await new Promise((resolve) => {
                const scrollDistance = 400; 
                let lastScrollHeight = scrollableDiv.scrollHeight;
                let unchangedTicks = 0; 

                const timer = setInterval(() => {
                    scrollableDiv.scrollBy(0, scrollDistance);
                    const isAtBottom = scrollableDiv.scrollTop + scrollableDiv.clientHeight >= scrollableDiv.scrollHeight - 50;

                    if (isAtBottom) {
                        if (scrollableDiv.scrollHeight === lastScrollHeight) {
                            unchangedTicks++;
                            if (unchangedTicks >= 4) {
                                clearInterval(timer);
                                resolve();
                            }
                        } else {
                            unchangedTicks = 0;
                            lastScrollHeight = scrollableDiv.scrollHeight;
                        }
                    } else {
                        unchangedTicks = 0;
                        lastScrollHeight = scrollableDiv.scrollHeight;
                    }
                }, 400); 
            });
        });

        await this.page.waitForTimeout(this.config.delays.pageLoadBuffer);
    }

    /* ========================================================================
       PHASE 2: DATA EXTRACTION
       ======================================================================== */

    async processJobs() {
        console.log('⚙️ Phase 2: Starting data extraction...');
        const urls = Array.from(this.allJobUrls);

        if (urls.length === 0) {
            console.log('✅ No new jobs to process. Exiting.');
            return;
        }

        for (let i = 0; i < urls.length; i++) {
            const url = urls[i];
            console.log(`[${i + 1}/${urls.length}] Scraping: ${url}`);
            
            try {
                await this.page.goto(url, { waitUntil: 'domcontentloaded' });
                await this.randomDelay(this.config.delays.minNav, this.config.delays.maxNav);

                const seeMoreBtn = await this.page.$('.jobs-description__footer-button');
                if (seeMoreBtn) await seeMoreBtn.click().catch(() => {});

                const jobData = await this.extractCurrentJobData(url);
                
                if (jobData && jobData.Title && jobData.company) {
                    this.currentBatch.push(jobData);
                    
                    // Mark this specific Job ID as scraped in our memory cache
                    const jobId = this.extractJobId(url);
                    if (jobId) this.scrapedJobIds.add(jobId);
                }

                if (this.currentBatch.length >= this.config.batchSize) {
                    await this.sendBatchToN8n();
                    await this.saveCache(); // Save progress to disk after every batch
                }

            } catch (error) {
                console.error(`❌ Data extraction failed for ${url}:`, error.message);
            }
        }

        if (this.currentBatch.length > 0) {
            await this.sendBatchToN8n();
        }
    }

    async extractCurrentJobData(url) {
        await this.page.waitForTimeout(this.config.delays.pageLoadBuffer); 

        return await this.page.evaluate((jobUrl) => {
            const allText = document.body.innerText || '';

            const calculateAbsoluteDate = (relativeStr) => {
                const now = new Date();
                if (!relativeStr || relativeStr === 'Unknown') return now.toISOString();

                const match = relativeStr.match(/(\d+)\s+(minute|hour|day|week|month)s?\s+ago/i);
                if (!match) return now.toISOString(); 

                const amount = parseInt(match[1], 10);
                const unit = match[2].toLowerCase();

                if (unit === 'minute') now.setMinutes(now.getMinutes() - amount);
                else if (unit === 'hour') now.setHours(now.getHours() - amount);
                else if (unit === 'day') now.setDate(now.getDate() - amount);
                else if (unit === 'week') now.setDate(now.getDate() - (amount * 7));
                else if (unit === 'month') now.setMonth(now.getMonth() - amount);

                return now.toISOString();
            };

            const docTitleParts = document.title.split('|').map(p => p.trim());
            let title = docTitleParts[0] || null;
            let companyName = docTitleParts[1] || null;

            if (!companyName || companyName === 'LinkedIn') {
                const companyLink = document.querySelector('a[href*="/company/"]');
                if (companyLink) companyName = companyLink.innerText.trim();
            }

            let location = 'Singapore'; 
            const locationMatch = allText.match(/([A-Z][a-zA-Z\s]+,\s*[A-Z][a-zA-Z\s]+)\s+\(/); 
            if (locationMatch) location = locationMatch[1].trim();

            let postedDate = 'Unknown';
            const dateMatch = allText.match(/(?:Reposted\s+)?(\d+\s+(?:minutes?|hours?|days?|weeks?|months?)\s+ago)/i);
            
            if (dateMatch) {
                postedDate = calculateAbsoluteDate(dateMatch[1].trim());
            } else {
                postedDate = new Date().toISOString(); 
            }

            let description = null;
            const descContainer = document.querySelector('.jobs-description-content__text') || 
                                  document.querySelector('#job-details');
            
            if (descContainer && descContainer.innerText.trim().length > 20) {
                description = descContainer.innerText;
            } else {
                const startMatch = allText.match(/About\s+(?!the\s+company|us|linkedin)[^\n]+/i);
                
                if (startMatch) {
                    let rawDescription = allText.substring(startMatch.index + startMatch[0].length);
                    
                    const bottomBoundaries = [
                        'About the company',
                        'About Us',
                        'Set alert for similar jobs',
                        'Job search faster with Premium',
                        'Show more',
                        'Report this job',
                        'Trending employee content',
                        'Application status',
                        'Use AI to assess how you fit',
                        'Take the next step in your job search'
                    ];

                    let earliestBoundaryIndex = rawDescription.length;
                    for (const boundary of bottomBoundaries) {
                        const boundaryIndex = rawDescription.indexOf(boundary);
                        if (boundaryIndex !== -1 && boundaryIndex < earliestBoundaryIndex) {
                            earliestBoundaryIndex = boundaryIndex;
                        }
                    }
                    
                    description = rawDescription.substring(0, earliestBoundaryIndex);
                }
            }

            if (description) {
                description = description.replace(/\n{3,}/g, '\n\n').trim(); 
            }

            return {
                postedDate: postedDate, 
                Title: title,
                company: companyName,
                location: location,
                description: description,
                canonicalUrl: jobUrl,
                scrapedAt: new Date().toISOString()
            };
        }, url);
    }

    /* ========================================================================
       PHASE 3: DELIVERY
       ======================================================================== */

    async sendBatchToN8n() {
        console.log(`📤 Dispatching batch of ${this.currentBatch.length} jobs to n8n webhook...`);
        try {
            await axios.post(this.config.webhookUrl, {
                jobs: this.currentBatch,
                timestamp: new Date().toISOString()
            });
            console.log('✅ Batch delivered successfully.');
            this.currentBatch = []; 
        } catch (error) {
            console.error('❌ Webhook delivery failed:', error.message);
        }
    }

    /* ========================================================================
       UTILITIES
       ======================================================================== */

    async randomDelay(min, max) {
        const delay = Math.floor(Math.random() * (max - min + 1)) + min;
        return new Promise(resolve => setTimeout(resolve, delay));
    }
}



// ----------------------------------------------------------------------------
// Execution Entry Point
// ----------------------------------------------------------------------------
const scraper = new LinkedInJobScraper();
scraper.run();