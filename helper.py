POSTS_SCRIPT = """
                () => {
                    // Gather all possible post containers (li, div, and data-urn)
                    const post_cards = [
                        ...document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li'),
                        ...document.querySelectorAll('li.artdeco-list__item'),
                        ...document.querySelectorAll('div.fie-impression-container'),
                        ...document.querySelectorAll('div[data-urn^="urn:li:activity:"]')
                    ];
                    const seen = new Set();
                    const posts = [];

                    // Helper functions
                    const getText = (element, selectors) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return (el.innerText || el.textContent).trim();
                        }
                        return '';
                    };
                    const getAttr = (element, selectors, attr) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return el.getAttribute(attr);
                        }
                        return '';
                    };

                    for (const card of post_cards) {
                        // Deduplicate by data-urn if present
                        const postUrn = getAttr(card, ['.feed-shared-update-v2', '[data-urn^="urn:li:activity:"]'], 'data-urn');
                        if (postUrn && seen.has(postUrn)) continue;
                        if (postUrn) seen.add(postUrn);

                        // Repost detection
                        const repostHeaderText = getText(card, [
                            '.update-components-header__text-view',
                            '.feed-shared-actor__meta--repost',
                            '.update-components-actor__sub-description'
                        ]);
                        const isRepost = repostHeaderText.toLowerCase().includes('reposted this') ||
                                        repostHeaderText.toLowerCase().includes('reshared this') ? 1 : 0;

                        // Author
                        const authorNameSelectors = [
                            '.update-components-actor__title span[dir="ltr"] span[aria-hidden="true"]',
                            '.update-components-actor__name',
                            '.feed-shared-actor__name'
                        ];
                        const authorUrlSelectors = [
                            '.update-components-actor__meta-link',
                            '.update-components-actor__image',
                            '.feed-shared-actor__container-link'
                        ];
                        const postTextSelectors = [
                            '.update-components-update-v2__commentary .update-components-text',
                            '.feed-shared-update-v2__description',
                            '.feed-shared-text',
                            '.update-components-text',
                            '.feed-shared-article__description',
                            '.feed-shared-external-video__description',
                            '.feed-shared-linkedin-video__description'
                        ];
                        const timestampSelectors = [
                            '.update-components-actor__sub-description span[aria-hidden="true"]',
                            '.feed-shared-actor__sub-description span[aria-hidden="true"]'
                        ];

                        const authorName = getText(card, authorNameSelectors);
                        let authorUrl = getAttr(card, authorUrlSelectors, 'href');
                        if (authorUrl && authorUrl.startsWith('/')) {
                            authorUrl = 'https://www.linkedin.com' + authorUrl;
                        }
                        const postText = getText(card, postTextSelectors);
                        const timestampText = getText(card, timestampSelectors);

                        // Post URL from data-urn
                        const postUrl = postUrn ? `https://www.linkedin.com/feed/update/${postUrn}` : '';

                        const timestamp = timestampText.split('•')[0].trim();

                        // --- Engagement Metrics from both sides ---
                        // LEFT: reactions (likes)
                        let leftReactions = '';
                        const leftReactionsElem = card.querySelector('.social-details-social-counts__reactions--left-aligned .social-details-social-counts__reactions-count');
                        if (leftReactionsElem) {
                            leftReactions = leftReactionsElem.innerText.trim();
                        }

                        // RIGHT: reposts/shares/comments
                        let rightReposts = '';
                        let rightComments = '';
                        const rightItems = card.querySelectorAll('.social-details-social-counts__item--right-aligned');
                        rightItems.forEach(item => {
                            const text = item.innerText.trim();
                            if (/repost|share/i.test(text)) {
                                rightReposts = text.replace(/[^0-9]/g, '');
                            }
                            if (/comment/i.test(text)) {
                                rightComments = text.replace(/[^0-9]/g, '');
                            }
                        });

                        // Fallback: also parse the general engagement text as before
                        const engagementText = getText(card, [
                            '.social-details-social-counts',
                            '.feed-shared-social-counts'
                        ]);
                        const likesMatch = engagementText.match(/([\\d,.]+\\w*)\\s*(like|reaction)/i);
                        const commentsMatch = engagementText.match(/([\\d,.]+\\w*)\\s*comment/i);
                        const sharesMatch = engagementText.match(/([\\d,.]+\\w*)\\s*(repost|share)/i);

                        // Combine all sources, prefer left/right if available, else fallback to regex
                        const engagement = {
                            likes: leftReactions || (likesMatch ? likesMatch[1] : '0'),
                            comments: rightComments || (commentsMatch ? commentsMatch[1] : '0'),
                            shares: rightReposts || (sharesMatch ? sharesMatch[1] : '0')
                        };

                        // Media selectors
                        const media = [];
                        const imageElements = card.querySelectorAll(
                            '.update-components-image__image, .feed-shared-image__image, .feed-shared-image img'
                        );
                        imageElements.forEach(img => {
                            if (img.src) media.push({ type: 'image', url: img.src });
                        });
                        if (card.querySelector('.update-components-linkedin-video, .feed-shared-video, .feed-shared-external-video')) {
                            media.push({ type: 'video', present: true });
                        }

                        // Only add if we have a post URL (guarantees uniqueness and validity)
                        if (postUrl) {
                            posts.push({
                                reposted: isRepost,
                                author_name: authorName,
                                author_url: authorUrl,
                                url: postUrl,
                                text: postText,
                                timestamp: timestamp,
                                engagement: engagement,
                                media: media
                            });
                        }
                    }
                    return posts;
                }
            """


COMMENTS_SCRIPT  = """
                () => {
                    // Gather all possible comment containers (li, div, and data-urn)
                    const cards = [
                        ...document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li'),
                        ...document.querySelectorAll('li.artdeco-list__item'),
                        ...document.querySelectorAll('div.fie-impression-container'),
                        ...document.querySelectorAll('div[data-urn^="urn:li:activity:"]')
                    ];
                    const seen = new Set();
                    const comments = [];

                    // Helper functions with fallback logic
                    const getText = (element, selectors) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return (el.innerText || el.textContent).trim();
                        }
                        return '';
                    };
                    const getAttr = (element, selectors, attr) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return el.getAttribute(attr);
                        }
                        return '';
                    };

                    for (const card of cards) {
                        // Deduplicate by data-urn if present
                        const postUrn = getAttr(card, ['.feed-shared-update-v2', '[data-urn^="urn:li:activity:"]'], 'data-urn');
                        if (postUrn && seen.has(postUrn)) continue;
                        if (postUrn) seen.add(postUrn);

                        // --- Post Owner Details (with fallbacks) ---
                        const postOwnerName = getText(card, [
                            '.update-components-actor__title span[aria-hidden="true"]',
                            '.feed-shared-actor__name',
                            '.update-components-actor__name'
                        ]);
                        const postOwnerUrl = getAttr(card, [
                            '.update-components-actor__meta-link',
                            '.feed-shared-actor__container-link'
                        ], 'href');

                        // --- Post URL (most reliable method) ---
                        const postUrl = postUrn ? `https://www.linkedin.com/feed/update/${postUrn}` : '';

                        // --- Parent Post Text (fallbacks for various layouts) ---
                        const parentPostText = getText(card, [
                            '.feed-shared-update-v2__description .update-components-text',
                            '.feed-shared-update-v2__description',
                            '.feed-shared-text',
                            '.update-components-text'
                        ]);

                        // --- Your Comment Details (with fallbacks) ---
                        const yourCommentText = getText(card, [
                            'article.comments-comment-entity .comments-comment-item__main-content span[dir="ltr"]',
                            'article.comments-comment-entity .update-components-text',
                            '.comments-comment-entity .update-components-text',
                            'article.comments-comment-entity .comments-comment-item__main-content',
                            '.comments-comment-item__main-content span[aria-hidden="true"]'
                        ]);
                        const yourCommentTimestamp = getText(card, [
                            'article.comments-comment-entity time.comments-comment-meta__data',
                            'time.comments-comment-meta__data',
                            '.comments-comment-entity time',
                            'time'
                        ]);

                        // Filter out cards that are not valid comment activities
                        // Both your comment text and the post owner's name should exist
                        if (yourCommentText && postOwnerName) {
                            comments.push({
                                "post_owner_name": postOwnerName,
                                "post_owner_url": postOwnerUrl,
                                "post_url": postUrl,
                                "parent_post_text": parentPostText,
                                "text": yourCommentText,
                                "timestamp": yourCommentTimestamp,
                            })
                        }
                    }
                    return comments;
                }
            """


REACTIONS_SCRIPT = """
                () => {
                    // Gather all possible reaction containers (li, div, and data-urn)
                    const cards = [
                        ...document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li'),
                        ...document.querySelectorAll('li.artdeco-list__item'),
                        ...document.querySelectorAll('div.fie-impression-container'),
                        ...document.querySelectorAll('div[data-urn^="urn:li:activity:"]')
                    ];
                    const seen = new Set();
                    const reactions = [];

                    // Helper functions with fallback logic
                    const getText = (element, selectors) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return (el.innerText || el.textContent).trim();
                        }
                        return '';
                    };
                    const getAttr = (element, selectors, attr) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return el.getAttribute(attr);
                        }
                        return '';
                    };

                    for (const card of cards) {
                        // Deduplicate by data-urn if present
                        const postUrn = getAttr(card, ['.feed-shared-update-v2', '[data-urn^="urn:li:activity:"]'], 'data-urn');
                        if (postUrn && seen.has(postUrn)) continue;
                        if (postUrn) seen.add(postUrn);

                        // --- Post Owner Details (with fallbacks) ---
                        const postOwnerName = getText(card, [
                            '.update-components-actor__title span[aria-hidden="true"]',
                            '.feed-shared-actor__name',
                            '.update-components-actor__name'
                        ]);
                        const postOwnerUrl = getAttr(card, [
                            '.update-components-actor__meta-link',
                            '.feed-shared-actor__container-link'
                        ], 'href');

                        // --- Post URL (most reliable method) ---
                        const postUrl = postUrn ? `https://www.linkedin.com/feed/update/${postUrn}` : '';

                        // --- Post Text (fallbacks for various layouts) ---
                        const postText = getText(card, [
                            '.feed-shared-update-v2__description .update-components-text',
                            '.feed-shared-update-v2__description',
                            '.feed-shared-text',
                            '.update-components-text'
                        ]);

                        // --- Timestamp (with fallbacks) ---
                        const timestampText = getText(card, [
                            '.update-components-actor__sub-description span[aria-hidden="true"]',
                            '.feed-shared-actor__sub-description span[aria-hidden="true"]'
                        ]);
                        const timestamp = timestampText.split('•')[0].trim();

                        // Only add if we have a post owner and a post URL
                        if (postOwnerName && postUrl) {
                            reactions.push({
                                post_owner_name: postOwnerName,
                                post_owner_url: postOwnerUrl,
                                post_url: postUrl,
                                post_text: postText,
                                timestamp: timestamp
                            });
                        }
                    }
                    return reactions;
                }
            """


stealth_mode_script = """
        () => {
            // Function to override property
            const overrideProperty = (obj, propName, value) => {
                Object.defineProperty(obj, propName, {
                    value,
                    writable: false,
                    configurable: false,
                    enumerable: true
                });
            };
            
            // WebDriver
            overrideProperty(navigator, 'webdriver', false);
            
            // Plugins
            overrideProperty(navigator, 'plugins', {
                length: Math.floor(Math.random() * 5) + 3,
                refresh: () => {},
                item: () => {},
                namedItem: () => {},
                // Add some fake plugins
                0: { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
                1: { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                2: { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
            });
            
            // User agent components
            const brands = [
                { brand: 'Chromium', version: '115' },
                { brand: 'Not:A-Brand', version: '8' },
                { brand: 'Google Chrome', version: '115' }
            ];
            
            // userAgentData
            if (!navigator.userAgentData) {
                overrideProperty(navigator, 'userAgentData', {
                    brands,
                    mobile: false,
                    platform: 'Windows',
                    toJSON: () => ({}),
                    getHighEntropyValues: () => Promise.resolve({
                        architecture: 'x86',
                        bitness: '64',
                        brands,
                        mobile: false,
                        model: '',
                        platform: 'Windows',
                        platformVersion: '10.0',
                        uaFullVersion: '120.0.6099.109'
                    })
                });
            }
            
            // Hardware concurrency
            overrideProperty(navigator, 'hardwareConcurrency', Math.floor(Math.random() * 8) + 4);
            
            // Device memory
            overrideProperty(navigator, 'deviceMemory', Math.pow(2, Math.floor(Math.random() * 4) + 4));
            
            // Languages
            overrideProperty(navigator, 'languages', ['en-US', 'en']);
            
            // Add Chrome object
            if (!window.chrome) {
                window.chrome = {
                    runtime: {},
                    loadTimes: () => {},
                    csi: () => {},
                    app: {}
                };
            }
        }
        """