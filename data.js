// ============================================================
// PACIFIC NORTHWEST FTTH COMPETITIVE INTELLIGENCE — DATA LAYER
// All data publicly sourced. Every entry has source + sourceUrl.
// Last updated: March 2026
// ============================================================

// ---- PASSWORD HASH (SHA-256 of "pnw2026") ----
const PASSWORD_HASH = "829f36c0d7e51ec12a4a68e4b0cecdd868ae44965c69fe9b92dde54df08707b9";

// ---- PROVIDER DEFINITIONS ----
const PROVIDERS = {
    ziply:      { name: "Ziply Fiber",              sponsor: "WaveDivision/BCE (acq. Aug 2025)", color: "#16A34A", short: "ZIP" },
    hunter:     { name: "Hunter Communications",    sponsor: "Oak Hill Capital (acq. Dec 2025)",   color: "#DC2626", short: "HNT" },
    quantum:    { name: "Quantum Fiber (now AT&T)",  sponsor: "AT&T (acq. Feb 2026)",            color: "#0EA5E9", short: "QF"  },
    ezee:       { name: "Ezee Fiber",               sponsor: "Private (TX-based)",               color: "#EC4899", short: "EZE" },
    lightcurve: { name: "Lightcurve",               sponsor: "Palisade Infrastructure",          color: "#14B8A6", short: "LC"  },
    astound:    { name: "Astound Broadband",         sponsor: "Stonepeak (fmr. Wave)",           color: "#F97316", short: "AST" },
    fatbeam:    { name: "Fat Beam",                  sponsor: "Columbia Capital",                 color: "#F59E0B", short: "FB"  },
    emerald:    { name: "Emerald Broadband",         sponsor: "Public Benefit Corp",              color: "#8B5CF6", short: "EMR" },
    tds:        { name: "TDS Telecom",               sponsor: "Telephone & Data Systems (TDS)",   color: "#1D4ED8", short: "TDS" },
    sparklight: { name: "Sparklight (Cable One)",    sponsor: "Cable One Inc.",                   color: "#7C3AED", short: "SPK" },
    lumen:      { name: "AT&T (formerly Lumen)",       sponsor: "AT&T Inc. (acq. Feb 2026)",       color: "#6B7280", short: "ATT" },
    comcast:    { name: "Comcast Xfinity",           sponsor: "Comcast Corp.",                    color: "#E11D48", short: "CMC" }
};

// ---- OPERATOR PROFILES (public data only) ----
const OPERATOR_PROFILES = [
    {
        id: "ziply",
        name: "Ziply Fiber",
        shortName: "Ziply",
        color: "#16A34A",
        metrics: [
            { label: "Total Customers", value: "~1.3M", source: "BroadbandNow — Ziply Fiber profile page", sourceUrl: "https://broadbandnow.com/Ziply-Fiber", archiveUrl: "https://web.archive.org/web/2026/https://broadbandnow.com/Ziply-Fiber" },
            { label: "States Served", value: "WA, OR, ID, MT", source: "Ziply Fiber — service area page", sourceUrl: "https://ziplyfiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://ziplyfiber.com/" },
            { label: "Technology", value: "FTTP (XGS-PON)", source: "Ziply Fiber — internet plans page", sourceUrl: "https://ziplyfiber.com/fiber-internet", archiveUrl: "https://web.archive.org/web/2026/https://ziplyfiber.com/fiber-internet" },
            { label: "Key Markets", value: "Seattle, Portland, Boise, Spokane, Everett", source: "FCC BDC — broadband availability map", sourceUrl: "https://broadbandmap.fcc.gov/data-download/nationwide-data" },
            { label: "Ownership", value: "BCE Inc. (acquired Aug 2025)", source: "Light Reading (Aug 2025) — 'BCE closes $5B deal for Ziply Fiber'", sourceUrl: "https://www.lightreading.com/fiber/bce-closes-5-billion-deal-for-ziply-fiber", archiveUrl: "https://web.archive.org/web/2025/https://www.lightreading.com/fiber/bce-closes-5-billion-deal-for-ziply-fiber" },
            { label: "Speed Tiers", value: "Up to 50 Gbps (business)", source: "Ziply Fiber — internet plans page", sourceUrl: "https://ziplyfiber.com/fiber-internet", archiveUrl: "https://web.archive.org/web/2026/https://ziplyfiber.com/fiber-internet" }
        ]
    },
    {
        id: "hunter",
        name: "Hunter Communications",
        shortName: "Hunter",
        color: "#DC2626",
        metrics: [
            { label: "Total Passings", value: "~100K residential + commercial", source: "Oak Hill Capital PR (Dec 2025) — ¶2: 'passing approximately 100,000 locations'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" },
            { label: "Subscribers", value: "~25,000+", source: "Oak Hill Capital PR — ¶2: 'more than 25,000 customers'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" },
            { label: "Route Miles", value: "~3,000 route miles", source: "Oak Hill Capital PR — ¶2: 'more than 3,000 route miles'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" },
            { label: "States Served", value: "OR (Southern + Willamette Valley)", source: "Oak Hill Capital PR — ¶1: 'Oregon and Northern California'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" },
            { label: "Technology", value: "FTTP", source: "Oak Hill Capital PR — ¶1: 'fiber-to-the-premises (\"FTTP\")'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" },
            { label: "Key Markets", value: "Medford (HQ), Eugene, Salem, Grants Pass, Klamath Falls", source: "PRNewswire (Mar 2025) — Salem expansion announcement", sourceUrl: "https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html", archiveUrl: "https://web.archive.org/web/2025/https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html" },
            { label: "Ownership", value: "Oak Hill Capital (acquired Dec 2025)", source: "Oak Hill Capital PR — ¶1: 'definitive agreement to acquire'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" },
            { label: "Oak Hill Deal", value: "22nd broadband investment; 8 FTTP platforms, 35 states, ~5M passings", source: "Oak Hill Capital PR — ¶6: 'eight FTTP platforms…five million locations'", sourceUrl: "https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/", archiveUrl: "https://web.archive.org/web/2026/https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/" }
        ]
    },
    {
        id: "quantum",
        name: "Quantum Fiber / AT&T",
        shortName: "Quantum",
        color: "#0EA5E9",
        metrics: [
            { label: "PNW Passings", value: "~500K+ (WA/OR legacy CenturyLink)", source: "FCC BDC — broadband availability map", sourceUrl: "https://broadbandmap.fcc.gov/data-download/nationwide-data" },
            { label: "Acquisition", value: "AT&T closed $5.75B deal Feb 2, 2026", source: "AT&T Newsroom (Feb 2026) — official close announcement", sourceUrl: "https://about.att.com/story/2026/att-lumen-deal-close.html", archiveUrl: "https://web.archive.org/web/2026/https://about.att.com/story/2026/att-lumen-deal-close.html" },
            { label: "Technology", value: "FTTP (XGS-PON, up to 8 Gbps)", source: "Quantum Fiber — service page", sourceUrl: "https://www.quantumfiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.quantumfiber.com/" },
            { label: "Key Markets", value: "Portland, Seattle, Boise metro areas", source: "BroadbandNow — Portland fiber providers", sourceUrl: "https://broadbandnow.com/Oregon/Portland", archiveUrl: "https://web.archive.org/web/2026/https://broadbandnow.com/Oregon/Portland" },
            { label: "Deal Value", value: "$5.75B for Lumen consumer fiber (1M+ subs, 4M+ locations)", source: "Denver Post (May 2025) — 'AT&T to purchase Lumen fiber'", sourceUrl: "https://www.denverpost.com/2025/05/21/att-lumen-fiber-purchase-quantum-centurylink/", archiveUrl: "https://web.archive.org/web/2025/https://www.denverpost.com/2025/05/21/att-lumen-fiber-purchase-quantum-centurylink/" },
            { label: "Idaho Plans", value: "Nearly doubling Idaho fiber build rate in 2026", source: "BoiseDev (Feb 2026) — 'Major company acquires fiber internet networks in Boise'", sourceUrl: "https://boisedev.com/news/2026/02/03/major-company-acquires-fiber-internet-networks-in-boise/", archiveUrl: "https://web.archive.org/web/2026/https://boisedev.com/news/2026/02/03/major-company-acquires-fiber-internet-networks-in-boise/" }
        ]
    },
    {
        id: "ezee",
        name: "Ezee Fiber",
        shortName: "Ezee",
        color: "#EC4899",
        metrics: [
            { label: "WA Investment", value: "$400M announced", source: "PRNewswire (Oct 2025) — ¶1: 'investing more than $400 million'", sourceUrl: "https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html", archiveUrl: "https://web.archive.org/web/2025/https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html" },
            { label: "First WA Customer", value: "Oct 22, 2025 (Kent)", source: "PRNewswire (Oct 2025) — headline: 'First Customer in Kent, WA'", sourceUrl: "https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html", archiveUrl: "https://web.archive.org/web/2025/https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html" },
            { label: "Technology", value: "FTTP (up to 8 Gbps residential)", source: "Ezee Fiber — plans page", sourceUrl: "https://ezeefiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://ezeefiber.com/" },
            { label: "States Served", value: "TX (HQ), WA (new)", source: "Ezee Fiber — service areas", sourceUrl: "https://ezeefiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://ezeefiber.com/" },
            { label: "Key Markets", value: "Kent, Des Moines, Puyallup, Milton, Lakewood, Fife, Algona, Pacific, Vancouver WA", source: "Ezee Fiber — availability checker", sourceUrl: "https://ezeefiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://ezeefiber.com/" },
            { label: "Ownership", value: "Private (TX-based)", source: "Ezee Fiber — about page", sourceUrl: "https://ezeefiber.com/about/", archiveUrl: "https://web.archive.org/web/2026/https://ezeefiber.com/about/" }
        ]
    },
    {
        id: "lightcurve",
        name: "Lightcurve",
        shortName: "Lightcurve",
        color: "#14B8A6",
        metrics: [
            { label: "Total Passings", value: "~160K", source: "Lightcurve — homepage hero stats", sourceUrl: "https://getlightcurve.com/", archiveUrl: "https://web.archive.org/web/2026/https://getlightcurve.com/" },
            { label: "HQ", value: "Tacoma, WA", source: "Lightcurve — contact page", sourceUrl: "https://getlightcurve.com/", archiveUrl: "https://web.archive.org/web/2026/https://getlightcurve.com/" },
            { label: "History", value: "Palisade acquired Rainier Connect, rebranded Lightcurve", source: "Lightcurve blog — 'Palisade acquires Rainier Connect' post", sourceUrl: "https://getlightcurve.com/palisade-infrastructure-acquires-rainier-connect-and-announces-name-change-to-lightcurve/", archiveUrl: "https://web.archive.org/web/2025/https://getlightcurve.com/palisade-infrastructure-acquires-rainier-connect-and-announces-name-change-to-lightcurve/" },
            { label: "Technology", value: "FTTP + HFC upgrade", source: "Lightcurve — internet service page", sourceUrl: "https://getlightcurve.com/", archiveUrl: "https://web.archive.org/web/2026/https://getlightcurve.com/" },
            { label: "Key Markets", value: "Tacoma, Eatonville, Centralia, Yelm, Ellensburg, Rochester, Lewis/Thurston/Kittitas Cos.", source: "Lightcurve — service area page", sourceUrl: "https://getlightcurve.com/", archiveUrl: "https://web.archive.org/web/2026/https://getlightcurve.com/" },
            { label: "Build Plan", value: "2,750 mi fiber planned, 163K premises across Pierce/Thurston/Lewis/Kittitas", source: "Lightcurve — fiber expansion page", sourceUrl: "https://getlightcurve.com/", archiveUrl: "https://web.archive.org/web/2026/https://getlightcurve.com/" },
            { label: "Ownership", value: "Palisade Infrastructure", source: "Lightcurve blog — acquisition announcement", sourceUrl: "https://getlightcurve.com/palisade-infrastructure-acquires-rainier-connect-and-announces-name-change-to-lightcurve/", archiveUrl: "https://web.archive.org/web/2025/https://getlightcurve.com/palisade-infrastructure-acquires-rainier-connect-and-announces-name-change-to-lightcurve/" }
        ]
    },
    {
        id: "astound",
        name: "Astound Broadband",
        shortName: "Astound",
        color: "#F97316",
        metrics: [
            { label: "Portland Metro Passings", value: "~51K", source: "BroadbandNow — Astound Broadband profile", sourceUrl: "https://broadbandnow.com/Astound-Broadband", archiveUrl: "https://web.archive.org/web/2026/https://broadbandnow.com/Astound-Broadband" },
            { label: "Oregon Coast Build", value: "108-mi Hwy 6 fiber (completed Apr 2025)", source: "GeekWire (Apr 2025) — 'Astound powers up new fiber near Oregon Coast'", sourceUrl: "https://www.geekwire.com/2025/astound-powers-up-new-fiber-internet-near-oregon-coast-with-help-from-aws/", archiveUrl: "https://web.archive.org/web/2025/https://www.geekwire.com/2025/astound-powers-up-new-fiber-internet-near-oregon-coast-with-help-from-aws/" },
            { label: "Technology", value: "HFC + FTTP", source: "Astound Broadband — Oregon service page", sourceUrl: "https://www.astound.com/oregon/", archiveUrl: "https://web.archive.org/web/2026/https://www.astound.com/oregon/" },
            { label: "Key Markets", value: "Hillsboro, N Seattle, Tillamook, Oregon Coast", source: "BroadbandNow — Seattle fiber providers", sourceUrl: "https://broadbandnow.com/Washington/Seattle", archiveUrl: "https://web.archive.org/web/2026/https://broadbandnow.com/Washington/Seattle" },
            { label: "Ownership", value: "Stonepeak (fmr. Wave Broadband)", source: "Astound Broadband — about page", sourceUrl: "https://www.astound.com/about/", archiveUrl: "https://web.archive.org/web/2026/https://www.astound.com/about/" }
        ]
    },
    {
        id: "fatbeam",
        name: "Fat Beam",
        shortName: "Fat Beam",
        color: "#F59E0B",
        metrics: [
            { label: "Focus", value: "Enterprise / Government fiber → now residential", source: "Fat Beam — homepage banner", sourceUrl: "https://www.fatbeamfiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/" },
            { label: "Spokane Valley Expansion", value: "24.5 mi fiber (EMAN acquisition)", source: "Fierce Network — 'Fatbeam acquires EMAN Networks' article", sourceUrl: "https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber", archiveUrl: "https://web.archive.org/web/2025/https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber" },
            { label: "Technology", value: "FTTP (enterprise + residential)", source: "Fat Beam — residential plans page", sourceUrl: "https://www.fatbeamfiber.com/residential", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/residential" },
            { label: "Key Markets", value: "Spokane, Spokane Valley, Liberty Lake, CDA, Boise, Meridian", source: "Fat Beam — Spokane location page", sourceUrl: "https://www.fatbeamfiber.com/locations/spokane-wa", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/locations/spokane-wa" },
            { label: "Ownership", value: "Columbia Capital", source: "Fat Beam — about page", sourceUrl: "https://www.fatbeamfiber.com/about/", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/about/" },
            { label: "Residential FTTH", value: "Now offering residential fiber (250 Mbps–2 Gbps). No contracts.", source: "Fat Beam news — 'Expands fiber to new Boise areas and Meridian'", sourceUrl: "https://www.fatbeamfiber.com/media-news/fatbeam-fiber-expands-fiber-network-to-new-boise-areas-and-meridian", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/media-news/fatbeam-fiber-expands-fiber-network-to-new-boise-areas-and-meridian" },
            { label: "Boise Expansion", value: "Phase 1 Boise complete; expanding to Meridian + Twin Falls", source: "Fat Beam news — Boise/Meridian expansion announcement", sourceUrl: "https://www.fatbeamfiber.com/media-news/fatbeam-fiber-expands-fiber-network-to-new-boise-areas-and-meridian", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/media-news/fatbeam-fiber-expands-fiber-network-to-new-boise-areas-and-meridian" },
            { label: "Network Scale", value: "150K+ fiber miles across 8 states", source: "Fat Beam — homepage 'who we are' section", sourceUrl: "https://www.fatbeamfiber.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.fatbeamfiber.com/" }
        ]
    },
    {
        id: "emerald",
        name: "Emerald Broadband",
        shortName: "Emerald",
        color: "#8B5CF6",
        metrics: [
            { label: "Structure", value: "Public Benefit Corporation", source: "Emerald Broadband — about section", sourceUrl: "https://www.emeraldbroadband.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.emeraldbroadband.com/" },
            { label: "Service Area", value: "Eugene/Springfield, OR", source: "Emerald Broadband — coverage map", sourceUrl: "https://www.emeraldbroadband.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.emeraldbroadband.com/" },
            { label: "Technology", value: "FTTP", source: "Emerald Broadband — service page", sourceUrl: "https://www.emeraldbroadband.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.emeraldbroadband.com/" },
            { label: "Key Markets", value: "River Road, Whiteaker, South Eugene, Oakridge", source: "Emerald Broadband — neighborhoods page", sourceUrl: "https://www.emeraldbroadband.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.emeraldbroadband.com/" },
            { label: "Mission", value: "Bridge digital divide; lowest-cost fiber in Eugene", source: "Emerald Broadband — mission statement", sourceUrl: "https://www.emeraldbroadband.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.emeraldbroadband.com/" },
            { label: "Model", value: "Community-driven: residents mark interest for future builds", source: "Lookout Eugene (May 2025) — 'Emerald Broadband brings the future of internet to Oregon'", sourceUrl: "https://lookouteugene-springfield.com/story/latest-news/2025/05/13/emerald-broadband-brings-the-future-of-internet-to-oregon-why-its-time-to-ditch-cable-for-local-fiber/", archiveUrl: "https://web.archive.org/web/2025/https://lookouteugene-springfield.com/story/latest-news/2025/05/13/emerald-broadband-brings-the-future-of-internet-to-oregon-why-its-time-to-ditch-cable-for-local-fiber/" }
        ]
    },
    {
        id: "tds",
        name: "TDS Telecom",
        shortName: "TDS",
        color: "#1D4ED8",
        metrics: [
            { label: "Total Passings (national)", value: "~1.1M fiber + 300K copper", source: "TDS Telecom — investor presentations", sourceUrl: "https://tdstelecom.com/", archiveUrl: "https://web.archive.org/web/2026/https://tdstelecom.com/" },
            { label: "PNW Markets", value: "Bend, OR; Spokane, WA; Billings/Great Falls/Missoula, MT", source: "FCC BDC — broadband availability map", sourceUrl: "https://broadbandmap.fcc.gov/data-download/nationwide-data" },
            { label: "Technology", value: "FTTP (new) + HFC/Copper (legacy)", source: "TDS Telecom — internet plans page", sourceUrl: "https://tdstelecom.com/shop/internet.html", archiveUrl: "https://web.archive.org/web/2026/https://tdstelecom.com/shop/internet.html" },
            { label: "Ownership", value: "Telephone & Data Systems (NYSE: TDS)", source: "TDS Inc. — investor relations page", sourceUrl: "https://www.tdsinc.com/investors", archiveUrl: "https://web.archive.org/web/2026/https://www.tdsinc.com/investors" },
            { label: "Montana Build", value: "150K+ fiber locations; Billings, Great Falls, Helena, Missoula, Butte, Laurel", source: "Fierce Network (Mar 2023) — 'TDS Telecom targets fiber to 150K in Big Sky Country'", sourceUrl: "https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond", archiveUrl: "https://web.archive.org/web/2025/https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond" },
            { label: "Spokane Build", value: "88K premises, 1,100 mi fiber in Spokane metro", source: "Fierce Network (Mar 2023) — ¶ 'Spokane metro' section of same article", sourceUrl: "https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond", archiveUrl: "https://web.archive.org/web/2025/https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond" },
            { label: "MT Customers", value: "10,000+ MT customers (Feb 2026)", source: "FiberBroadband.org (Feb 2026) — 'TDS celebrates 10,000 Montana customers'", sourceUrl: "https://fiberbroadband.org/2026/02/03/tds-celebrates-10000-montana-customers/", archiveUrl: "https://web.archive.org/web/2026/https://fiberbroadband.org/2026/02/03/tds-celebrates-10000-montana-customers/" }
        ]
    },
    {
        id: "lumen",
        name: "AT&T (formerly Lumen/CenturyLink)",
        shortName: "AT&T",
        color: "#6B7280",
        metrics: [
            { label: "PNW Legacy Footprint", value: "WA, OR, ID, MT (ILEC territory)", source: "FCC BDC — broadband availability map", sourceUrl: "https://broadbandmap.fcc.gov/data-download/nationwide-data" },
            { label: "Acquisition", value: "AT&T closed $5.75B deal for Lumen consumer fiber Feb 2, 2026", source: "AT&T Newsroom (Feb 2026) — official close announcement", sourceUrl: "https://about.att.com/story/2026/att-lumen-deal-close.html", archiveUrl: "https://web.archive.org/web/2026/https://about.att.com/story/2026/att-lumen-deal-close.html" },
            { label: "Brand Transition", value: "Quantum Fiber from AT&T → AT&T Fiber (2-year TSA)", source: "Light Reading — 'AT&T closes $5.75B Lumen mass markets deal'", sourceUrl: "https://www.lightreading.com/broadband/att-closes-5-75b-lumen-mass-markets-deal", archiveUrl: "https://web.archive.org/web/2026/https://www.lightreading.com/broadband/att-closes-5-75b-lumen-mass-markets-deal" },
            { label: "AT&T Fiber Target", value: "60M+ fiber locations by 2030 (national)", source: "AT&T Investor Day — strategic plan", sourceUrl: "https://investors.att.com/", archiveUrl: "https://web.archive.org/web/2026/https://investors.att.com/" },
            { label: "FCC Filing ID", value: "130228 (CenturyLink legacy — AT&T won't refile until D25/J26)", source: "FCC BDC — entity search", sourceUrl: "https://broadbandmap.fcc.gov/" },
            { label: "Remaining Lumen", value: "Enterprise/wholesale only post-consumer sale", source: "Lumen Technologies — corporate overview", sourceUrl: "https://www.lumen.com/", archiveUrl: "https://web.archive.org/web/2026/https://www.lumen.com/" }
        ]
    },
    {
        id: "comcast",
        name: "Comcast Xfinity",
        shortName: "Comcast",
        color: "#E11D48",
        hiddenFromProfiles: true,
        metrics: [
            { label: "PNW Footprint", value: "WA, OR (major cable incumbent)", source: "FCC BDC", sourceUrl: "https://broadbandmap.fcc.gov/data-download/nationwide-data" },
            { label: "Technology", value: "HFC (DOCSIS 4.0 upgrade) + FTTP edge-outs", source: "Comcast", sourceUrl: "https://www.xfinity.com/" },
            { label: "Key Markets", value: "Seattle, Portland, Tacoma, Bellevue, Everett", source: "BroadbandNow", sourceUrl: "https://broadbandnow.com/Comcast" },
            { label: "Ownership", value: "Comcast Corp. (NASDAQ: CMCSA)", source: "Comcast", sourceUrl: "https://corporate.comcast.com/" }
        ]
    }
];

// ============================================================
// CENSUS TRACT DATA — Metro-level polygons with build economics
// Coords are rectangular approximations (replace with TIGER polygons
// from BDC processing). Housing units from ACS 2020 5-Year.
// ============================================================
const CENSUS_TRACTS = {
    // ==========================================
    // WASHINGTON — Seattle Metro
    // ==========================================
    seattle_wa: {
        county: 'King',
        state: 'Washington',
        fips: '53033',
        permitPortal: 'https://cosaccela.seattle.gov/portal/',
        tracts: [
            { id: '007100', name: 'Downtown Seattle', coords: [[47.620,-122.355],[47.620,-122.325],[47.595,-122.325],[47.595,-122.355]], housingUnits: 15200, density: 'urban', costPerPassing: 700, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Dense fiber coverage per FCC data', notes: 'Core Ziply urban market. High-rise residential & commercial.', overlap: false, isNew: false, announcedPassings: null },
            { id: '007200', name: 'Capitol Hill / First Hill', coords: [[47.635,-122.325],[47.635,-122.285],[47.605,-122.285],[47.605,-122.325]], housingUnits: 18500, density: 'urban', costPerPassing: 725, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Dense fiber coverage per FCC data', notes: 'Core Ziply urban market. High-density residential.', overlap: false, isNew: false, announcedPassings: null },
            { id: '007400', name: 'University District', coords: [[47.680,-122.335],[47.680,-122.285],[47.645,-122.285],[47.645,-122.335]], housingUnits: 14200, density: 'urban', costPerPassing: 750, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Quantum both show coverage', notes: 'UW student housing dense. Ziply vs Quantum/AT&T overlap.', overlap: true, isNew: false, announcedPassings: null },
            { id: '010800', name: 'North Seattle / Northgate', coords: [[47.735,-122.365],[47.735,-122.305],[47.695,-122.305],[47.695,-122.365]], housingUnits: 16450, density: 'suburban', costPerPassing: 825, provider: 'Astound', permitStatus: 'Partial Coverage', sourceType: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Washington/Seattle', sourceText: 'Astound (fmr. Wave) N Seattle presence', notes: 'Astound legacy Wave footprint. Some Ziply overlap.', overlap: true, isNew: false, announcedPassings: null },
            { id: '011200', name: 'South Seattle / Rainier Valley', coords: [[47.575,-122.315],[47.575,-122.255],[47.525,-122.255],[47.525,-122.315]], housingUnits: 12850, density: 'suburban', costPerPassing: 850, provider: 'Quantum', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Quantum (CenturyLink) fiber per FCC', notes: 'Legacy CenturyLink fiber. AT&T closed acquisition Feb 2026.', overlap: false, isNew: false, announcedPassings: null },
            { id: '007600', name: 'Ballard / Fremont', coords: [[47.685,-122.405],[47.685,-122.355],[47.655,-122.355],[47.655,-122.405]], housingUnits: 11200, density: 'urban', costPerPassing: 775, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber coverage per FCC', notes: 'Strong Ziply market. Growing tech worker population.', overlap: false, isNew: false, announcedPassings: null },
            { id: '007800', name: 'Queen Anne / Magnolia', coords: [[47.655,-122.405],[47.655,-122.355],[47.620,-122.355],[47.620,-122.405]], housingUnits: 9800, density: 'urban', costPerPassing: 800, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'Established Ziply residential market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '011500', name: 'West Seattle', coords: [[47.585,-122.405],[47.585,-122.355],[47.545,-122.355],[47.545,-122.405]], housingUnits: 13500, density: 'suburban', costPerPassing: 850, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Quantum overlap', notes: 'Contested market with bridge access issues.', overlap: true, isNew: false, announcedPassings: null },
            { id: '011700', name: 'Columbia City / Beacon Hill', coords: [[47.560,-122.305],[47.560,-122.275],[47.530,-122.275],[47.530,-122.305]], housingUnits: 10200, density: 'suburban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Light rail corridor growth area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '007900', name: 'SoDo / Georgetown', coords: [[47.565,-122.345],[47.565,-122.315],[47.540,-122.315],[47.540,-122.345]], housingUnits: 5100, density: 'urban', costPerPassing: 750, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Industrial/commercial mix. Lower residential density.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Tacoma / Pierce County
    // ==========================================
    tacoma_wa: {
        county: 'Pierce',
        state: 'Washington',
        fips: '53053',
        permitPortal: 'https://tacomaatlas.com/',
        tracts: [
            { id: '061300', name: 'Downtown Tacoma', coords: [[47.275,-122.475],[47.275,-122.425],[47.240,-122.425],[47.240,-122.475]], housingUnits: 9850, density: 'urban', costPerPassing: 775, provider: 'Lightcurve', permitStatus: 'Active - HQ Market', sourceType: 'Company Website', sourceUrl: 'https://getlightcurve.com/', sourceText: 'Lightcurve HQ - Tacoma core market', notes: 'LIGHTCURVE HQ MARKET. 160K total passings company-wide.', overlap: false, isNew: false, announcedPassings: null },
            { id: '061500', name: 'Tacoma North End / Proctor', coords: [[47.305,-122.495],[47.305,-122.445],[47.270,-122.445],[47.270,-122.495]], housingUnits: 11250, density: 'suburban', costPerPassing: 850, provider: 'Lightcurve', permitStatus: 'Active Service', sourceType: 'Company Website', sourceUrl: 'https://getlightcurve.com/', sourceText: 'Lightcurve residential fiber', notes: 'Established Lightcurve (fmr. Rainier Connect) market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '061600', name: 'Tacoma East / Fircrest', coords: [[47.265,-122.425],[47.265,-122.375],[47.230,-122.375],[47.230,-122.425]], housingUnits: 8450, density: 'suburban', costPerPassing: 850, provider: 'Lightcurve', permitStatus: 'Active Service', sourceType: 'Company Website', sourceUrl: 'https://getlightcurve.com/', sourceText: 'Lightcurve fiber per company site', notes: 'Core Lightcurve residential area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '062000', name: 'Lakewood', coords: [[47.195,-122.535],[47.195,-122.475],[47.155,-122.475],[47.155,-122.535]], housingUnits: 14680, density: 'suburban', costPerPassing: 875, provider: 'Quantum', permitStatus: 'Legacy CenturyLink', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'CenturyLink fiber per FCC data', notes: 'Quantum (CenturyLink) legacy. Now AT&T (Feb 2026).', overlap: false, isNew: false, announcedPassings: null },
            { id: '061700', name: 'Puyallup', coords: [[47.205,-122.325],[47.205,-122.265],[47.165,-122.265],[47.165,-122.325]], housingUnits: 12920, density: 'suburban', costPerPassing: 1148, provider: 'Ezee', permitStatus: 'ACTIVE BUILD', sourceType: 'Company Website', sourceUrl: 'https://ezeefiber.com/', sourceText: 'Ezee Fiber expansion target', notes: 'EZEE EXPANSION. Part of $400M WA investment.', overlap: false, isNew: true, announcedPassings: null },
            { id: '061800', name: 'University Place', coords: [[47.235,-122.575],[47.235,-122.515],[47.195,-122.515],[47.195,-122.575]], housingUnits: 10500, density: 'suburban', costPerPassing: 875, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Lightcurve + Quantum overlap', notes: 'Contested market. Lightcurve expanding vs Quantum incumbent.', overlap: true, isNew: false, announcedPassings: null },
            { id: '062100', name: 'South Tacoma / Parkland', coords: [[47.155,-122.475],[47.155,-122.415],[47.115,-122.415],[47.115,-122.475]], housingUnits: 11200, density: 'suburban', costPerPassing: 900, provider: 'Quantum', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'CenturyLink/Quantum per FCC', notes: 'PLU area. Quantum legacy, potential Ezee expansion target.', overlap: false, isNew: false, announcedPassings: null },
            { id: '061900', name: 'Spanaway / Graham', coords: [[47.115,-122.445],[47.115,-122.365],[47.065,-122.365],[47.065,-122.445]], housingUnits: 13200, density: 'suburban', costPerPassing: 925, provider: 'None', permitStatus: 'Underserved', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC data', notes: 'Fast-growing area south of Tacoma. Expansion opportunity.', overlap: false, isNew: false, announcedPassings: null },
            { id: '062200', name: 'Bonney Lake / Sumner', coords: [[47.195,-122.205],[47.195,-122.145],[47.155,-122.145],[47.155,-122.205]], housingUnits: 9800, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'Growing east Pierce County. Partial Ziply footprint.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Kent / Auburn (Ezee territory)
    // ==========================================
    kent_wa: {
        county: 'King',
        state: 'Washington',
        fips: '53033',
        permitPortal: 'https://permitcenter.kentwa.gov/',
        tracts: [
            { id: '029200', name: 'Downtown Kent', coords: [[47.395,-122.265],[47.395,-122.215],[47.365,-122.215],[47.365,-122.265]], housingUnits: 8250, density: 'suburban', costPerPassing: 1114, provider: 'Ezee', permitStatus: 'ACTIVE BUILD - First Customer Oct 2025', sourceType: 'Press Release', sourceUrl: 'https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html', sourceText: 'PRNewswire 10/22/25: First customer connected in Kent', notes: 'EZEE FIBER HQ MARKET. $400M WA investment. Up to 8 Gbps. Cost includes 1.35× underground (HDD) premium.', overlap: false, isNew: true, announcedPassings: null },
            { id: '029300', name: 'Kent East Hill', coords: [[47.415,-122.215],[47.415,-122.165],[47.385,-122.165],[47.385,-122.215]], housingUnits: 11820, density: 'suburban', costPerPassing: 1148, provider: 'Ezee', permitStatus: 'Construction Underway', sourceType: 'Company Website', sourceUrl: 'https://ezeefiber.com/cities/wa/kent/', sourceText: 'Ezee Fiber Kent service page', notes: 'Active Ezee buildout. High-density residential target.', overlap: false, isNew: true, announcedPassings: null },
            { id: '029400', name: 'Kent West Valley', coords: [[47.405,-122.305],[47.405,-122.265],[47.365,-122.265],[47.365,-122.305]], housingUnits: 7650, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Existing Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply legacy footprint per FCC data', notes: 'Ziply incumbent. Ezee entering market with overbuild.', overlap: true, isNew: false, announcedPassings: null },
            { id: '029500', name: 'Des Moines', coords: [[47.415,-122.335],[47.415,-122.295],[47.385,-122.295],[47.385,-122.335]], housingUnits: 6450, density: 'suburban', costPerPassing: 1148, provider: 'Ezee', permitStatus: 'Expansion Target', sourceType: 'Company Website', sourceUrl: 'https://ezeefiber.com/', sourceText: 'Ezee Fiber expansion markets', notes: 'EZEE EXPANSION. Adjacent to Kent launch market.', overlap: false, isNew: true, announcedPassings: null },
            { id: '029600', name: 'Auburn North', coords: [[47.335,-122.255],[47.335,-122.205],[47.305,-122.205],[47.305,-122.255]], housingUnits: 8920, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber coverage per FCC', notes: 'Ziply incumbent market south of Kent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '029700', name: 'Federal Way', coords: [[47.335,-122.355],[47.335,-122.295],[47.295,-122.295],[47.295,-122.355]], housingUnits: 12500, density: 'suburban', costPerPassing: 850, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Quantum coverage overlap', notes: 'Contested market. Ziply and Quantum both present.', overlap: true, isNew: false, announcedPassings: null },
            { id: '029800', name: 'Algona / Pacific', coords: [[47.290,-122.255],[47.290,-122.215],[47.265,-122.215],[47.265,-122.255]], housingUnits: 4850, density: 'suburban', costPerPassing: 1181, provider: 'Ezee', permitStatus: 'Construction Started', sourceType: 'Company Website', sourceUrl: 'https://ezeefiber.com/', sourceText: 'Ezee Fiber blog - Algona/Pacific expansion', notes: 'New Ezee build area. Small market adjacent to Auburn.', overlap: false, isNew: true, announcedPassings: null },
            { id: '029900', name: 'Covington / Maple Valley', coords: [[47.385,-122.135],[47.385,-122.065],[47.345,-122.065],[47.345,-122.135]], housingUnits: 11400, density: 'suburban', costPerPassing: 925, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial coverage per FCC', notes: 'Growing suburban area. Partial Ziply footprint.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Bellevue / Eastside
    // ==========================================
    bellevue_wa: {
        county: 'King',
        state: 'Washington',
        fips: '53033',
        permitPortal: 'https://permits.bellevuewa.gov/',
        tracts: [
            { id: '023100', name: 'Downtown Bellevue', coords: [[47.625,-122.215],[47.625,-122.185],[47.600,-122.185],[47.600,-122.215]], housingUnits: 12500, density: 'urban', costPerPassing: 725, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply strong coverage per FCC', notes: 'Amazon HQ2 area. Dense tech employment.', overlap: false, isNew: false, announcedPassings: null },
            { id: '023200', name: 'Redmond / Overlake', coords: [[47.685,-122.145],[47.685,-122.095],[47.655,-122.095],[47.655,-122.145]], housingUnits: 14200, density: 'suburban', costPerPassing: 800, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Microsoft campus area. Strong Ziply presence.', overlap: false, isNew: false, announcedPassings: null },
            { id: '023300', name: 'Kirkland', coords: [[47.695,-122.215],[47.695,-122.175],[47.665,-122.175],[47.665,-122.215]], housingUnits: 11800, density: 'suburban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Google campus area. Affluent residential.', overlap: false, isNew: false, announcedPassings: null },
            { id: '023400', name: 'Issaquah / Sammamish', coords: [[47.555,-122.065],[47.555,-121.995],[47.520,-121.995],[47.520,-122.065]], housingUnits: 15600, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC', notes: 'Affluent east King County. Growing.', overlap: false, isNew: false, announcedPassings: null },
            { id: '023500', name: 'Woodinville / Bothell', coords: [[47.765,-122.175],[47.765,-122.115],[47.735,-122.115],[47.735,-122.175]], housingUnits: 13400, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'North Eastside corridor.', overlap: false, isNew: false, announcedPassings: null },
            { id: '023600', name: 'Mercer Island', coords: [[47.585,-122.235],[47.585,-122.205],[47.555,-122.205],[47.555,-122.235]], housingUnits: 6200, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Island community. Affluent market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '023700', name: 'Renton', coords: [[47.495,-122.225],[47.495,-122.175],[47.465,-122.175],[47.465,-122.225]], housingUnits: 14500, density: 'suburban', costPerPassing: 825, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Quantum overlap per FCC', notes: 'Boeing HQ area. Growing residential.', overlap: true, isNew: false, announcedPassings: null },
            { id: '023800', name: 'Newcastle / Factoria', coords: [[47.545,-122.185],[47.545,-122.155],[47.520,-122.155],[47.520,-122.185]], housingUnits: 7800, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC', notes: 'Suburban Eastside residential.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Spokane
    // ==========================================
    spokane_wa: {
        county: 'Spokane',
        state: 'Washington',
        fips: '53063',
        permitPortal: 'https://my.spokanecity.org/permits/',
        tracts: [
            { id: '000100', name: 'Downtown Spokane', coords: [[47.680,-117.455],[47.680,-117.385],[47.640,-117.385],[47.640,-117.455]], housingUnits: 8850, density: 'urban', costPerPassing: 725, provider: 'Fat Beam + Ziply', permitStatus: 'Active - Both Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Both providers show coverage in FCC data', notes: 'TDS 88K premise build + Fat Beam residential FTTH + Ziply incumbent. Major 4-way competition.', overlap: true, isNew: false, announcedPassings: null },
            { id: '000200', name: 'South Hill', coords: [[47.645,-117.435],[47.645,-117.365],[47.605,-117.365],[47.605,-117.435]], housingUnits: 12250, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber coverage confirmed', notes: 'Strong Ziply residential market per FCC data.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'North Spokane', coords: [[47.720,-117.455],[47.720,-117.385],[47.680,-117.385],[47.680,-117.455]], housingUnits: 9450, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Established Ziply residential market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Spokane Valley', coords: [[47.695,-117.385],[47.695,-117.285],[47.650,-117.285],[47.650,-117.385]], housingUnits: 15620, density: 'suburban', costPerPassing: 825, provider: 'Fat Beam', permitStatus: 'Enterprise Expansion', sourceType: 'News Article', sourceUrl: 'https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber', sourceText: 'Fierce Network: EMAN acquisition added 24.5 miles fiber', notes: 'Fat Beam acquired EMAN Networks fiber assets in Spokane Valley.', overlap: false, isNew: true, announcedPassings: null },
            { id: '000500', name: 'Liberty Lake', coords: [[47.695,-117.195],[47.695,-117.105],[47.650,-117.105],[47.650,-117.195]], housingUnits: 8450, density: 'suburban', costPerPassing: 900, provider: 'Fat Beam', permitStatus: 'Active - Expanding', sourceType: 'Company Website', sourceUrl: 'https://www.fatbeamfiber.com/locations/spokane-wa', sourceText: 'Fat Beam Spokane/CDA service page', notes: 'Fat Beam expansion market. Tech hub growth.', overlap: false, isNew: true, announcedPassings: null },
            { id: '000600', name: 'Airway Heights', coords: [[47.680,-117.565],[47.680,-117.505],[47.640,-117.505],[47.640,-117.565]], housingUnits: 5250, density: 'suburban', costPerPassing: 900, provider: 'None', permitStatus: 'Underserved', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC data', notes: 'Near Fairchild AFB. Expansion opportunity.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000700', name: 'Cheney', coords: [[47.505,-117.595],[47.505,-117.545],[47.475,-117.545],[47.475,-117.595]], housingUnits: 5800, density: 'suburban', costPerPassing: 950, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC', notes: 'EWU campus area. Student housing.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000800', name: 'Mead / North County', coords: [[47.765,-117.405],[47.765,-117.345],[47.735,-117.345],[47.735,-117.405]], housingUnits: 7200, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'Growing North Spokane suburban area.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Everett / Snohomish
    // ==========================================
    everett_wa: {
        county: 'Snohomish',
        state: 'Washington',
        fips: '53061',
        permitPortal: 'https://snohomishcountywa.gov/5685/Online-Permits',
        tracts: [
            { id: '041200', name: 'Downtown Everett', coords: [[47.985,-122.215],[47.985,-122.185],[47.960,-122.185],[47.960,-122.215]], housingUnits: 8500, density: 'urban', costPerPassing: 775, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Boeing Everett plant area. Core Ziply market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '041300', name: 'Lynnwood / Mountlake Terrace', coords: [[47.835,-122.325],[47.835,-122.275],[47.800,-122.275],[47.800,-122.325]], housingUnits: 14200, density: 'suburban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Light rail extension area. Growing density.', overlap: false, isNew: false, announcedPassings: null },
            { id: '041400', name: 'Edmonds / Shoreline', coords: [[47.815,-122.375],[47.815,-122.325],[47.780,-122.325],[47.780,-122.375]], housingUnits: 11600, density: 'suburban', costPerPassing: 825, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Astound overlap per FCC', notes: 'Astound legacy Wave territory overlaps with Ziply.', overlap: true, isNew: false, announcedPassings: null },
            { id: '041500', name: 'Marysville / Lake Stevens', coords: [[48.065,-122.185],[48.065,-122.095],[48.025,-122.095],[48.025,-122.185]], housingUnits: 16800, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Expanding', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply expansion per FCC data', notes: 'Fast-growing north Snohomish County.', overlap: false, isNew: false, announcedPassings: null },
            { id: '041600', name: 'Mukilteo / South Everett', coords: [[47.955,-122.315],[47.955,-122.275],[47.920,-122.275],[47.920,-122.315]], housingUnits: 9400, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC', notes: 'Boeing Paine Field area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '041700', name: 'Snohomish / Monroe', coords: [[47.925,-121.855],[47.925,-121.775],[47.885,-121.775],[47.885,-121.855]], housingUnits: 8900, density: 'rural', costPerPassing: 1100, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC', notes: 'Rural east Snohomish. BEAD opportunity.', overlap: false, isNew: false, announcedPassings: null },
            { id: '041800', name: 'Arlington / Stanwood', coords: [[48.215,-122.185],[48.215,-122.115],[48.175,-122.115],[48.175,-122.185]], housingUnits: 10200, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'I-5 corridor north of Everett.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Olympia
    // ==========================================
    olympia_wa: {
        county: 'Thurston',
        state: 'Washington',
        fips: '53067',
        permitPortal: 'https://www.thurstoncountywa.gov/planning',
        tracts: [
            { id: '010100', name: 'Downtown Olympia', coords: [[47.050,-122.915],[47.050,-122.885],[47.030,-122.885],[47.030,-122.915]], housingUnits: 7200, density: 'urban', costPerPassing: 800, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'State capital. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '010200', name: 'Lacey', coords: [[47.045,-122.835],[47.045,-122.775],[47.015,-122.775],[47.015,-122.835]], housingUnits: 11500, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Joint Base Lewis-McChord adjacent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '010300', name: 'Tumwater', coords: [[46.995,-122.925],[46.995,-122.885],[46.965,-122.885],[46.965,-122.925]], housingUnits: 8600, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'Olympia Brewery area. Growing residential.', overlap: false, isNew: false, announcedPassings: null },
            { id: '010400', name: 'Yelm', coords: [[46.945,-122.615],[46.945,-122.585],[46.925,-122.585],[46.925,-122.615]], housingUnits: 4500, density: 'rural', costPerPassing: 1000, provider: 'Lightcurve', permitStatus: 'Upgrade Planned 2026', sourceType: 'Company Website', sourceUrl: 'https://getlightcurve.com/', sourceText: 'Lightcurve Yelm upgrade announcement', notes: 'Lightcurve fiber upgrade planned for 2026.', overlap: false, isNew: true, announcedPassings: null },
            { id: '010500', name: 'East Olympia / SE Thurston', coords: [[46.985,-122.825],[46.985,-122.755],[46.945,-122.755],[46.945,-122.825]], housingUnits: 7800, density: 'rural', costPerPassing: 1050, provider: 'None', permitStatus: 'Underserved', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC', notes: 'Rural area. BEAD eligible.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // WASHINGTON — Vancouver WA
    // ==========================================
    vancouver_wa: {
        county: 'Clark',
        state: 'Washington',
        fips: '53011',
        permitPortal: 'https://www.cityofvancouver.us/ced/page/permits',
        tracts: [
            { id: '040100', name: 'Downtown Vancouver', coords: [[45.640,-122.685],[45.640,-122.655],[45.620,-122.655],[45.620,-122.685]], housingUnits: 8200, density: 'urban', costPerPassing: 775, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Portland suburb across Columbia River. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '040200', name: 'Hazel Dell / Salmon Creek', coords: [[45.695,-122.685],[45.695,-122.635],[45.660,-122.635],[45.660,-122.685]], housingUnits: 12500, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Growing north Vancouver suburbs.', overlap: false, isNew: false, announcedPassings: null },
            { id: '040300', name: 'Camas / Washougal', coords: [[45.605,-122.435],[45.605,-122.345],[45.570,-122.345],[45.570,-122.435]], housingUnits: 10200, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'Tech corridor. Growing east Clark County.', overlap: false, isNew: false, announcedPassings: null },
            { id: '040400', name: 'Battle Ground / Ridgefield', coords: [[45.785,-122.555],[45.785,-122.495],[45.745,-122.495],[45.745,-122.555]], housingUnits: 9800, density: 'suburban', costPerPassing: 925, provider: 'None', permitStatus: 'Limited Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC', notes: 'Fast-growing north Clark County. Expansion opportunity.', overlap: false, isNew: false, announcedPassings: null },
            { id: '040500', name: 'East Vancouver / Orchards', coords: [[45.655,-122.605],[45.655,-122.545],[45.620,-122.545],[45.620,-122.605]], housingUnits: 11200, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Suburban residential. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // OREGON — Portland Metro
    // ==========================================
    portland_or: {
        county: 'Multnomah',
        state: 'Oregon',
        fips: '41051',
        permitPortal: 'https://www.portlandoregon.gov/bds/',
        tracts: [
            { id: '004100', name: 'Downtown Portland / Pearl', coords: [[45.540,-122.710],[45.540,-122.660],[45.505,-122.660],[45.505,-122.710]], housingUnits: 18500, density: 'urban', costPerPassing: 700, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Full fiber coverage per FCC data', notes: 'Core Ziply market. Full fiber coverage per FCC BDC.', overlap: false, isNew: false, announcedPassings: null },
            { id: '004200', name: 'NE Portland / Rose City', coords: [[45.565,-122.660],[45.565,-122.595],[45.530,-122.595],[45.530,-122.660]], housingUnits: 14200, density: 'urban', costPerPassing: 775, provider: 'Quantum', permitStatus: 'Partial Coverage', sourceType: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Oregon/Portland', sourceText: 'Quantum/CenturyLink fiber in NE Portland', notes: 'Quantum/AT&T market. AT&T acquisition closed Feb 2026.', overlap: true, isNew: false, announcedPassings: null },
            { id: '004300', name: 'Beaverton', coords: [[45.510,-122.835],[45.510,-122.765],[45.470,-122.765],[45.470,-122.835]], housingUnits: 16850, density: 'suburban', costPerPassing: 800, provider: 'Ziply', permitStatus: 'Expansion Underway', sourceType: 'FCC BDC + Company', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply expansion visible in recent FCC filings', notes: 'Active Ziply expansion. Nike HQ area.', overlap: false, isNew: true, announcedPassings: null },
            { id: '004400', name: 'Hillsboro / Intel', coords: [[45.555,-123.015],[45.555,-122.935],[45.510,-122.935],[45.510,-123.015]], housingUnits: 22200, density: 'suburban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Expansion Underway', sourceType: 'FCC BDC + Company', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply expansion in Intel corridor', notes: 'Intel corridor. Major Ziply investment area per FCC data.', overlap: false, isNew: true, announcedPassings: null },
            { id: '004500', name: 'SE Portland / Hawthorne', coords: [[45.520,-122.660],[45.520,-122.595],[45.480,-122.595],[45.480,-122.660]], housingUnits: 15800, density: 'urban', costPerPassing: 750, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Quantum both present', notes: 'Contested market. Dense residential.', overlap: true, isNew: false, announcedPassings: null },
            { id: '004600', name: 'Tigard / Lake Oswego', coords: [[45.475,-122.795],[45.475,-122.725],[45.420,-122.725],[45.420,-122.795]], housingUnits: 19500, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Affluent suburban market. Strong Ziply presence.', overlap: false, isNew: false, announcedPassings: null },
            { id: '004700', name: 'Gresham', coords: [[45.525,-122.485],[45.525,-122.415],[45.485,-122.415],[45.485,-122.485]], housingUnits: 14200, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'East Portland suburb. Growing market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '004800', name: 'N Portland / St Johns', coords: [[45.605,-122.755],[45.605,-122.695],[45.565,-122.695],[45.565,-122.755]], housingUnits: 11500, density: 'suburban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC', notes: 'North Portland growth area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '004900', name: 'Oregon City / Milwaukie', coords: [[45.365,-122.625],[45.365,-122.565],[45.325,-122.565],[45.325,-122.625]], housingUnits: 12800, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'Clackamas County. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '005000', name: 'Tualatin / Sherwood', coords: [[45.395,-122.785],[45.395,-122.725],[45.355,-122.725],[45.355,-122.785]], housingUnits: 10800, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'South metro suburban. Growing.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // OREGON — Eugene / Springfield
    // ==========================================
    eugene_or: {
        county: 'Lane',
        state: 'Oregon',
        fips: '41039',
        permitPortal: 'https://www.lanecounty.org/government/county_departments/public_works/right-of-way_permits',
        tracts: [
            { id: '001000', name: 'Downtown Eugene', coords: [[44.070,-123.115],[44.070,-123.065],[44.040,-123.065],[44.040,-123.115]], housingUnits: 8420, density: 'urban', costPerPassing: 750, provider: 'Hunter', permitStatus: 'Commercial Active', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/downtown-eugene-fiber-project/', sourceText: 'Downtown Eugene Fiber Project (EUGNet partnership)', notes: 'PERMIT SEARCH: Try "44 Broadway", "940 Willamette", or "942 Olive St" in Eugene permits. EUGNet commercial fiber active since 2017.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001100', name: 'University District', coords: [[44.060,-123.065],[44.060,-123.015],[44.030,-123.015],[44.030,-123.065]], housingUnits: 11850, density: 'urban', costPerPassing: 750, provider: 'Contested', permitStatus: 'Multiple Interest', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'FCC Broadband Map - check specific addresses', notes: 'High student housing density. Both Hunter and Ziply have partial coverage per FCC BDC.', overlap: true, isNew: false, announcedPassings: null },
            { id: '001200', name: 'South Eugene', coords: [[44.040,-123.115],[44.040,-123.055],[44.005,-123.055],[44.005,-123.115]], housingUnits: 9180, density: 'suburban', costPerPassing: 875, provider: 'Hunter', permitStatus: 'Expansion Target', sourceType: 'Press Release', sourceUrl: 'https://hunterfiber.com/eugene/', sourceText: 'Hunter Eugene service page - residential expansion', notes: 'Hunter targeting residential after commercial success downtown.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001300', name: 'West Eugene / Bethel', coords: [[44.085,-123.175],[44.085,-123.115],[44.050,-123.115],[44.050,-123.175]], housingUnits: 10650, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'FCC National Broadband Map - Ziply coverage', notes: 'Existing Ziply/Bell legacy footprint per FCC data.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001400', name: 'Santa Clara / River Road', coords: [[44.115,-123.145],[44.115,-123.085],[44.080,-123.085],[44.080,-123.145]], housingUnits: 8890, density: 'suburban', costPerPassing: 875, provider: 'Emerald', permitStatus: 'PBC Build', sourceType: 'Company Website', sourceUrl: 'https://www.emeraldbroadband.com/', sourceText: 'Emerald Broadband - River Road expansion', notes: 'Emerald Broadband PBC local expansion. Community-focused fiber.', overlap: false, isNew: true, announcedPassings: null },
            { id: '001600', name: 'Springfield West', coords: [[44.065,-123.015],[44.065,-122.965],[44.035,-122.965],[44.035,-123.015]], housingUnits: 9120, density: 'suburban', costPerPassing: 850, provider: 'Hunter', permitStatus: 'Expansion Target', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/springfield/', sourceText: 'Hunter Springfield service page', notes: 'Hunter residential expansion target.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001700', name: 'Springfield Central / Gateway', coords: [[44.065,-122.965],[44.065,-122.905],[44.035,-122.905],[44.035,-122.965]], housingUnits: 8580, density: 'suburban', costPerPassing: 850, provider: 'Hunter', permitStatus: 'Expansion Target', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/springfield/', sourceText: 'Hunter Springfield service page', notes: 'Part of Hunter Springfield expansion per company website.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001800', name: 'North Eugene / Cal Young', coords: [[44.085,-123.085],[44.085,-123.025],[44.055,-123.025],[44.055,-123.085]], housingUnits: 7250, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Established Ziply market. Higher-income residential.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // OREGON — Salem
    // ==========================================
    salem_or: {
        county: 'Marion',
        state: 'Oregon',
        fips: '41047',
        permitPortal: 'https://egov.cityofsalem.net/PACPortal',
        tracts: [
            { id: '000100', name: 'Downtown Salem', coords: [[44.955,-123.055],[44.955,-123.005],[44.925,-123.005],[44.925,-123.055]], housingUnits: 7150, density: 'urban', costPerPassing: 775, provider: 'None', permitStatus: 'No Fiber Provider', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'FCC Map shows no fiber coverage downtown', notes: 'State capital. No fiber provider active per FCC BDC. Ripe for entry.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'NE Salem / Morningside', coords: [[44.985,-123.005],[44.985,-122.945],[44.950,-122.945],[44.950,-123.005]], housingUnits: 8850, density: 'suburban', costPerPassing: 850, provider: 'Hunter', permitStatus: 'ACTIVE BUILD - Phase 1', sourceType: 'Press Release', sourceUrl: 'https://www.salemreporter.com/2025/08/15/hunter-communications-expands-its-fiber-internet-into-salem/', sourceText: 'Salem Reporter 8/15/25: "began working at 1168 15th Street N.E."', notes: 'PERMIT SEARCH: Use address "1168 15th St NE" in Salem PAC Portal. Hunter Phase 1 construction began Aug 2025.', overlap: false, isNew: true, announcedPassings: 2500 },
            { id: '000300', name: 'Highland / East Salem', coords: [[44.955,-122.945],[44.955,-122.885],[44.920,-122.885],[44.920,-122.945]], housingUnits: 7620, density: 'suburban', costPerPassing: 875, provider: 'Hunter', permitStatus: 'ANNOUNCED - Phase 2', sourceType: 'Press Release', sourceUrl: 'https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html', sourceText: 'PRNewswire 5/20/25: "Highland" listed as target neighborhood', notes: 'Hunter Phase 2 per PRNewswire announcement.', overlap: false, isNew: true, announcedPassings: null },
            { id: '000400', name: 'Grant / Four Corners', coords: [[45.015,-123.015],[45.015,-122.955],[44.980,-122.955],[44.980,-123.015]], housingUnits: 6480, density: 'suburban', costPerPassing: 875, provider: 'Hunter', permitStatus: 'ANNOUNCED - Phase 2', sourceType: 'Press Release', sourceUrl: 'https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html', sourceText: 'PRNewswire 5/20/25: "Grant" listed as target neighborhood', notes: 'Hunter Phase 2 per PRNewswire announcement.', overlap: false, isNew: true, announcedPassings: null },
            { id: '000500', name: 'South Salem', coords: [[44.925,-123.055],[44.925,-122.995],[44.890,-122.995],[44.890,-123.055]], housingUnits: 8050, density: 'suburban', costPerPassing: 850, provider: 'None', permitStatus: 'Potential Phase 3', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'No fiber per FCC - natural expansion area', notes: 'Not yet announced. Higher-income area - potential Phase 3.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000600', name: 'West Salem', coords: [[44.970,-123.105],[44.970,-123.055],[44.930,-123.055],[44.930,-123.105]], housingUnits: 9250, density: 'suburban', costPerPassing: 900, provider: 'None', permitStatus: 'No Activity', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'No fiber coverage per FCC BDC', notes: 'Across Willamette River. Not in Hunter announced neighborhoods.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000800', name: 'Keizer', coords: [[45.025,-123.045],[45.025,-122.985],[44.985,-122.985],[44.985,-123.045]], housingUnits: 11120, density: 'suburban', costPerPassing: 825, provider: 'None', permitStatus: 'No Activity', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Adjacent city - no fiber per FCC', notes: 'Natural expansion market for Hunter. Not yet announced.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // OREGON — Bend / Central Oregon
    // ==========================================
    bend_or: {
        county: 'Deschutes',
        state: 'Oregon',
        fips: '41017',
        permitPortal: 'https://www.deschutes.org/cd/page/apply-permit',
        tracts: [
            { id: '000200', name: 'Downtown Bend', coords: [[44.065,-121.325],[44.065,-121.295],[44.045,-121.295],[44.045,-121.325]], housingUnits: 6800, density: 'urban', costPerPassing: 800, provider: 'TDS', permitStatus: 'Active FTTP Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS fiber per FCC data', notes: 'TDS Telecom FTTP market. Growing tech hub.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'NW Bend / Mt Bachelor Village', coords: [[44.085,-121.345],[44.085,-121.305],[44.060,-121.305],[44.060,-121.345]], housingUnits: 5200, density: 'suburban', costPerPassing: 900, provider: 'TDS', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS coverage per FCC', notes: 'Affluent residential area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'East Bend / Pilot Butte', coords: [[44.065,-121.285],[44.065,-121.245],[44.040,-121.245],[44.040,-121.285]], housingUnits: 7400, density: 'suburban', costPerPassing: 875, provider: 'TDS', permitStatus: 'Expanding', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS expansion per FCC data', notes: 'Growing east Bend neighborhoods.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000500', name: 'Redmond', coords: [[44.285,-121.195],[44.285,-121.155],[44.260,-121.155],[44.260,-121.195]], housingUnits: 8500, density: 'suburban', costPerPassing: 875, provider: 'TDS', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS coverage in Redmond per FCC', notes: 'Redmond Municipal Airport hub. Growing.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000600', name: 'South Bend / Sunriver', coords: [[44.020,-121.355],[44.020,-121.295],[43.985,-121.295],[43.985,-121.355]], housingUnits: 6200, density: 'rural', costPerPassing: 1050, provider: 'TDS', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS partial per FCC', notes: 'Resort community. Seasonal population.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // OREGON — Medford / Southern Oregon
    // ==========================================
    medford_or: {
        county: 'Jackson',
        state: 'Oregon',
        fips: '41029',
        permitPortal: 'https://www.medfordoregon.gov/Government/Departments/Planning/Permits',
        tracts: [
            { id: '000100', name: 'Downtown Medford', coords: [[42.340,-122.895],[42.340,-122.855],[42.310,-122.855],[42.310,-122.895]], housingUnits: 7500, density: 'urban', costPerPassing: 775, provider: 'Hunter', permitStatus: 'HQ Market - Active', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/', sourceText: 'Hunter Communications HQ in Medford', notes: 'HUNTER HQ MARKET. Core service area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'East Medford / Bear Creek', coords: [[42.345,-122.845],[42.345,-122.795],[42.315,-122.795],[42.315,-122.845]], housingUnits: 9200, density: 'suburban', costPerPassing: 850, provider: 'Hunter', permitStatus: 'Active Service', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/', sourceText: 'Hunter fiber service area', notes: 'Established Hunter residential market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Central Point', coords: [[42.385,-122.925],[42.385,-122.885],[42.360,-122.885],[42.360,-122.925]], housingUnits: 6800, density: 'suburban', costPerPassing: 875, provider: 'Hunter', permitStatus: 'Active Service', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/', sourceText: 'Hunter service area', notes: 'North Medford suburb. Hunter active.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Grants Pass', coords: [[42.445,-123.345],[42.445,-123.305],[42.415,-123.305],[42.415,-123.345]], housingUnits: 8900, density: 'suburban', costPerPassing: 875, provider: 'Hunter', permitStatus: 'Active Service', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/', sourceText: 'Hunter service area', notes: 'Josephine County. Hunter market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000500', name: 'Ashland', coords: [[42.205,-122.725],[42.205,-122.685],[42.180,-122.685],[42.180,-122.725]], housingUnits: 5600, density: 'suburban', costPerPassing: 925, provider: 'Hunter', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Hunter coverage per FCC', notes: 'Southern Oregon University area. Hunter active.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000600', name: 'White City / Eagle Point', coords: [[42.405,-122.765],[42.405,-122.715],[42.370,-122.715],[42.370,-122.765]], housingUnits: 7200, density: 'rural', costPerPassing: 1000, provider: 'Hunter', permitStatus: 'Expanding', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/', sourceText: 'Hunter expansion area', notes: 'Rural Jackson County. Hunter expanding.', overlap: false, isNew: true, announcedPassings: null },
            { id: '000700', name: 'Klamath Falls', coords: [[42.230,-121.785],[42.230,-121.745],[42.200,-121.745],[42.200,-121.785]], housingUnits: 7800, density: 'suburban', costPerPassing: 900, provider: 'Hunter', permitStatus: 'Active Service', sourceType: 'Company Website', sourceUrl: 'https://hunterfiber.com/', sourceText: 'Hunter Klamath Falls service', notes: 'Klamath County. Hunter active.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // IDAHO — Boise / Treasure Valley
    // ==========================================
    boise_id: {
        county: 'Ada',
        state: 'Idaho',
        fips: '16001',
        permitPortal: 'https://www.cityofboise.org/departments/planning-and-development-services/permits/',
        tracts: [
            { id: '001000', name: 'Downtown Boise', coords: [[43.625,-116.215],[43.625,-116.185],[43.605,-116.185],[43.605,-116.215]], housingUnits: 7800, density: 'urban', costPerPassing: 750, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Core Ziply Idaho market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001100', name: 'Boise North End', coords: [[43.645,-116.215],[43.645,-116.185],[43.625,-116.185],[43.625,-116.215]], housingUnits: 6500, density: 'urban', costPerPassing: 800, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Historic neighborhood. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001200', name: 'Boise Bench / Vista', coords: [[43.595,-116.235],[43.595,-116.195],[43.575,-116.195],[43.575,-116.235]], housingUnits: 9200, density: 'suburban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC', notes: 'Established Ziply residential area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001300', name: 'Meridian', coords: [[43.625,-116.415],[43.625,-116.345],[43.590,-116.345],[43.590,-116.415]], housingUnits: 18500, density: 'suburban', costPerPassing: 850, provider: 'Contested', permitStatus: 'Multiple Providers', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply + Sparklight overlap per FCC', notes: 'Fastest-growing city in ID. Ziply and Sparklight compete.', overlap: true, isNew: false, announcedPassings: null },
            { id: '001400', name: 'Eagle', coords: [[43.705,-116.375],[43.705,-116.335],[43.685,-116.335],[43.685,-116.375]], housingUnits: 7200, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC data', notes: 'Affluent suburb. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001500', name: 'SE Boise / Micron', coords: [[43.585,-116.175],[43.585,-116.115],[43.555,-116.115],[43.555,-116.175]], housingUnits: 11200, density: 'suburban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Micron Technology HQ area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001600', name: 'Star / Middleton', coords: [[43.705,-116.525],[43.705,-116.465],[43.680,-116.465],[43.680,-116.525]], housingUnits: 5800, density: 'rural', costPerPassing: 1050, provider: 'None', permitStatus: 'Limited Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC data', notes: 'Rapidly growing. BEAD opportunity.', overlap: false, isNew: false, announcedPassings: null },
            { id: '001700', name: 'Kuna', coords: [[43.505,-116.435],[43.505,-116.395],[43.480,-116.395],[43.480,-116.435]], housingUnits: 7400, density: 'suburban', costPerPassing: 925, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC', notes: 'South Boise suburb. Growing rapidly.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // IDAHO — Nampa / Caldwell (Canyon County)
    // ==========================================
    nampa_id: {
        county: 'Canyon',
        state: 'Idaho',
        fips: '16027',
        permitPortal: 'https://www.cityofnampa.us/190/Building-Division',
        tracts: [
            { id: '000100', name: 'Downtown Nampa', coords: [[43.585,-116.575],[43.585,-116.545],[43.565,-116.545],[43.565,-116.575]], housingUnits: 6800, density: 'urban', costPerPassing: 800, provider: 'Sparklight', permitStatus: 'Active HFC Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Sparklight coverage per FCC', notes: 'Sparklight cable incumbent. Some Ziply overlap.', overlap: true, isNew: false, announcedPassings: null },
            { id: '000200', name: 'East Nampa', coords: [[43.585,-116.535],[43.585,-116.495],[43.560,-116.495],[43.560,-116.535]], housingUnits: 8200, density: 'suburban', costPerPassing: 850, provider: 'Sparklight', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Sparklight per FCC data', notes: 'Sparklight cable market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Caldwell', coords: [[43.675,-116.695],[43.675,-116.645],[43.650,-116.645],[43.650,-116.695]], housingUnits: 9500, density: 'suburban', costPerPassing: 875, provider: 'Sparklight', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Sparklight coverage per FCC', notes: 'Canyon County seat. Sparklight cable incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Middleton / Star West', coords: [[43.715,-116.635],[43.715,-116.585],[43.690,-116.585],[43.690,-116.635]], housingUnits: 5200, density: 'rural', costPerPassing: 1050, provider: 'None', permitStatus: 'Underserved', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited coverage per FCC', notes: 'Rapidly growing. Fiber opportunity.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // IDAHO — Coeur d'Alene
    // ==========================================
    cda_id: {
        county: 'Kootenai',
        state: 'Idaho',
        fips: '16055',
        permitPortal: 'https://www.cdaid.org/1237/departments/building-services',
        tracts: [
            { id: '000100', name: 'Downtown CDA', coords: [[47.680,-116.795],[47.680,-116.765],[47.660,-116.765],[47.660,-116.795]], housingUnits: 5800, density: 'urban', costPerPassing: 800, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Ziply incumbent. Tourism hub.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'Post Falls', coords: [[47.725,-116.965],[47.725,-116.915],[47.700,-116.915],[47.700,-116.965]], housingUnits: 8200, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Fast-growing suburb west of CDA.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Hayden / Rathdrum', coords: [[47.775,-116.795],[47.775,-116.745],[47.750,-116.745],[47.750,-116.795]], housingUnits: 7400, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'North Kootenai County growth area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Dalton Gardens / East CDA', coords: [[47.705,-116.745],[47.705,-116.705],[47.680,-116.705],[47.680,-116.745]], housingUnits: 4800, density: 'suburban', costPerPassing: 925, provider: 'Fat Beam', permitStatus: 'Enterprise Service', sourceType: 'Company Website', sourceUrl: 'https://www.fatbeamfiber.com/locations/spokane-wa', sourceText: 'Fat Beam CDA market per company site', notes: 'Fat Beam enterprise. Near Spokane metro.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // IDAHO — Idaho Falls
    // ==========================================
    idaho_falls_id: {
        county: 'Bonneville',
        state: 'Idaho',
        fips: '16019',
        permitPortal: 'https://www.idahofallsidaho.gov/187/Building-Division',
        tracts: [
            { id: '000100', name: 'Downtown Idaho Falls', coords: [[43.500,-112.055],[43.500,-112.025],[43.480,-112.025],[43.480,-112.055]], housingUnits: 6200, density: 'urban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'INL corridor. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'Ammon', coords: [[43.480,-111.975],[43.480,-111.935],[43.455,-111.935],[43.455,-111.975]], housingUnits: 7500, density: 'suburban', costPerPassing: 850, provider: 'Ammon Fiber', permitStatus: 'Municipal Fiber Active', sourceType: 'Company Website', sourceUrl: 'https://www.ammonfiber.com/', sourceText: 'Ammon municipal fiber network', notes: 'Nationally recognized municipal open-access fiber network.', overlap: true, isNew: false, announcedPassings: null },
            { id: '000300', name: 'West Idaho Falls', coords: [[43.505,-112.095],[43.505,-112.055],[43.480,-112.055],[43.480,-112.095]], housingUnits: 5800, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'West side residential. Ziply market.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Rexburg', coords: [[43.835,-111.795],[43.835,-111.765],[43.815,-111.765],[43.815,-111.795]], housingUnits: 5200, density: 'suburban', costPerPassing: 950, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'BYU-Idaho campus area. Student housing.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // MONTANA — Missoula
    // ==========================================
    missoula_mt: {
        county: 'Missoula',
        state: 'Montana',
        fips: '30063',
        permitPortal: 'https://www.ci.missoula.mt.us/462/Building-Division',
        tracts: [
            { id: '000100', name: 'Downtown Missoula', coords: [[46.880,-114.005],[46.880,-113.975],[46.860,-113.975],[46.860,-114.005]], housingUnits: 5800, density: 'urban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'University of Montana area. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'South Missoula / Lolo', coords: [[46.855,-114.025],[46.855,-113.975],[46.825,-113.975],[46.825,-114.025]], housingUnits: 7200, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'South Missoula residential corridor.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Reserve / Grant Creek', coords: [[46.905,-114.045],[46.905,-113.995],[46.880,-113.995],[46.880,-114.045]], housingUnits: 6400, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'Commercial corridor. Growing.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'East Missoula / Bonner', coords: [[46.880,-113.935],[46.880,-113.885],[46.860,-113.885],[46.860,-113.935]], housingUnits: 4200, density: 'rural', costPerPassing: 1100, provider: 'None', permitStatus: 'Limited Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC', notes: 'Rural east Missoula. BEAD eligible.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // MONTANA — Billings
    // ==========================================
    billings_mt: {
        county: 'Yellowstone',
        state: 'Montana',
        fips: '30111',
        permitPortal: 'https://ci.billings.mt.us/156/Building-Division',
        tracts: [
            { id: '000100', name: 'Downtown Billings', coords: [[45.790,-108.555],[45.790,-108.525],[45.770,-108.525],[45.770,-108.555]], housingUnits: 6500, density: 'urban', costPerPassing: 825, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Largest MT city. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'West Billings / Shiloh', coords: [[45.795,-108.615],[45.795,-108.565],[45.770,-108.565],[45.770,-108.615]], housingUnits: 8200, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Growing west side commercial corridor.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Billings Heights', coords: [[45.815,-108.505],[45.815,-108.455],[45.790,-108.455],[45.790,-108.505]], housingUnits: 9500, density: 'suburban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC', notes: 'North Billings growth area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Laurel', coords: [[45.680,-108.785],[45.680,-108.745],[45.660,-108.745],[45.660,-108.785]], housingUnits: 4200, density: 'suburban', costPerPassing: 950, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'Refinery town southwest of Billings.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // MONTANA — Great Falls
    // ==========================================
    great_falls_mt: {
        county: 'Cascade',
        state: 'Montana',
        fips: '30013',
        permitPortal: 'https://www.greatfallsmt.net/planning-community-development/building-permits',
        tracts: [
            { id: '000100', name: 'Downtown Great Falls', coords: [[47.510,-111.305],[47.510,-111.275],[47.490,-111.275],[47.490,-111.305]], housingUnits: 5500, density: 'urban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Malmstrom AFB adjacent. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'SW Great Falls', coords: [[47.505,-111.345],[47.505,-111.305],[47.480,-111.305],[47.480,-111.345]], housingUnits: 6200, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Residential growth area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'East Great Falls', coords: [[47.515,-111.255],[47.515,-111.215],[47.495,-111.215],[47.495,-111.255]], housingUnits: 5800, density: 'suburban', costPerPassing: 900, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC', notes: 'East side residential.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // MONTANA — Helena
    // ==========================================
    helena_mt: {
        county: 'Lewis and Clark',
        state: 'Montana',
        fips: '30049',
        permitPortal: 'https://www.helenamt.gov/government/departments/community-development',
        tracts: [
            { id: '000100', name: 'Downtown Helena', coords: [[46.600,-112.045],[46.600,-112.015],[46.580,-112.015],[46.580,-112.045]], housingUnits: 5200, density: 'urban', costPerPassing: 850, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'State capital. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'East Helena / Montana City', coords: [[46.600,-111.965],[46.600,-111.905],[46.575,-111.905],[46.575,-111.965]], housingUnits: 4800, density: 'suburban', costPerPassing: 950, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'Growing east Helena corridor.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'West Helena / Birdseye', coords: [[46.615,-112.085],[46.615,-112.045],[46.590,-112.045],[46.590,-112.085]], housingUnits: 4500, density: 'suburban', costPerPassing: 925, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'West Helena residential.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // MONTANA — Kalispell / Flathead
    // ==========================================
    kalispell_mt: {
        county: 'Flathead',
        state: 'Montana',
        fips: '30029',
        permitPortal: 'https://flathead.mt.gov/planning_zoning/',
        tracts: [
            { id: '000100', name: 'Downtown Kalispell', coords: [[48.205,-114.325],[48.205,-114.295],[48.185,-114.295],[48.185,-114.325]], housingUnits: 5200, density: 'urban', costPerPassing: 875, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply fiber per FCC data', notes: 'Glacier Park gateway. Ziply incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'Whitefish', coords: [[48.415,-114.365],[48.415,-114.335],[48.395,-114.335],[48.395,-114.365]], housingUnits: 4800, density: 'suburban', costPerPassing: 950, provider: 'Ziply', permitStatus: 'Active Service', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply coverage per FCC', notes: 'Resort town. Affluent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Columbia Falls', coords: [[48.375,-114.195],[48.375,-114.165],[48.355,-114.165],[48.355,-114.195]], housingUnits: 3800, density: 'suburban', costPerPassing: 975, provider: 'Ziply', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Ziply partial per FCC data', notes: 'Aluminum smelter area. Growing residential.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Bigfork / Lakeside', coords: [[48.075,-114.085],[48.075,-114.035],[48.045,-114.035],[48.045,-114.085]], housingUnits: 3200, density: 'rural', costPerPassing: 1100, provider: 'None', permitStatus: 'Limited Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited coverage per FCC', notes: 'Flathead Lake area. Rural. BEAD eligible.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // IDAHO — Twin Falls
    // ==========================================
    twin_falls_id: {
        county: 'Twin Falls',
        state: 'Idaho',
        fips: '16083',
        permitPortal: 'https://www.tfid.org/149/Building-Services',
        tracts: [
            { id: '000100', name: 'Downtown Twin Falls', coords: [[42.570,-114.475],[42.570,-114.445],[42.550,-114.445],[42.550,-114.475]], housingUnits: 5400, density: 'urban', costPerPassing: 850, provider: 'Sparklight', permitStatus: 'Cable Active / Fiber Limited', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Sparklight cable coverage per FCC data', notes: 'Sparklight cable incumbent. Limited fiber options.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'East Twin Falls', coords: [[42.575,-114.435],[42.575,-114.395],[42.555,-114.395],[42.555,-114.435]], housingUnits: 6200, density: 'suburban', costPerPassing: 900, provider: 'Sparklight', permitStatus: 'Cable Active', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Sparklight coverage per FCC', notes: 'CSI campus area. Growing suburban.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Kimberly', coords: [[42.545,-114.375],[42.545,-114.345],[42.525,-114.345],[42.525,-114.375]], housingUnits: 3800, density: 'suburban', costPerPassing: 975, provider: 'Sparklight', permitStatus: 'Partial Coverage', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Sparklight partial per FCC data', notes: 'Small town east of Twin Falls.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Jerome', coords: [[42.730,-114.525],[42.730,-114.495],[42.710,-114.495],[42.710,-114.525]], housingUnits: 4500, density: 'suburban', costPerPassing: 950, provider: 'None', permitStatus: 'Limited Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Limited fiber per FCC data', notes: 'Dairy industry hub. BEAD eligible area.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000500', name: 'Filer / West County', coords: [[42.575,-114.615],[42.575,-114.565],[42.550,-114.565],[42.550,-114.615]], housingUnits: 3200, density: 'rural', costPerPassing: 1100, provider: 'None', permitStatus: 'Underserved', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Underserved per FCC data', notes: 'Rural agricultural. BEAD target area.', overlap: false, isNew: false, announcedPassings: null }
        ]
    },
    // ==========================================
    // MONTANA — Bozeman
    // ==========================================
    bozeman_mt: {
        county: 'Gallatin',
        state: 'Montana',
        fips: '30031',
        permitPortal: 'https://www.bozeman.net/departments/community-development/building-division',
        tracts: [
            { id: '000100', name: 'Downtown Bozeman', coords: [[45.690,-111.055],[45.690,-111.025],[45.670,-111.025],[45.670,-111.055]], housingUnits: 5800, density: 'urban', costPerPassing: 825, provider: 'TDS', permitStatus: 'Active Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS fiber per FCC data', notes: 'MSU campus area. TDS fiber incumbent.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000200', name: 'West Bozeman', coords: [[45.695,-111.095],[45.695,-111.055],[45.675,-111.055],[45.675,-111.095]], housingUnits: 7200, density: 'suburban', costPerPassing: 875, provider: 'TDS', permitStatus: 'Active Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS coverage per FCC', notes: 'Rapid growth corridor. New housing.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000300', name: 'Four Corners', coords: [[45.645,-111.105],[45.645,-111.055],[45.620,-111.055],[45.620,-111.105]], housingUnits: 4800, density: 'suburban', costPerPassing: 950, provider: 'TDS', permitStatus: 'Expanding', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS expanding per FCC data', notes: 'Growing commercial area south of Bozeman.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000400', name: 'Belgrade', coords: [[45.785,-111.185],[45.785,-111.155],[45.765,-111.155],[45.765,-111.185]], housingUnits: 5500, density: 'suburban', costPerPassing: 925, provider: 'TDS', permitStatus: 'Active Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'TDS fiber per FCC', notes: 'Airport community. Fast growth.', overlap: false, isNew: false, announcedPassings: null },
            { id: '000500', name: 'Livingston', coords: [[45.665,-110.575],[45.665,-110.545],[45.650,-110.545],[45.650,-110.575]], housingUnits: 3800, density: 'suburban', costPerPassing: 1000, provider: 'Lumen', permitStatus: 'DSL/Limited Fiber', sourceType: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', sourceText: 'Lumen limited fiber per FCC data', notes: 'Paradise Valley gateway. Lumen DSL legacy.', overlap: false, isNew: false, announcedPassings: null }
        ]
    }
};

// ============================================================
// MARKET MARKERS — Map marker points for all tracked providers
// ============================================================
const MARKET_MARKERS = [
    // === ZIPLY FIBER (Green) ===
    { lat: 47.6062, lng: -122.3321, name: 'Seattle Metro', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: '~500K passings', notes: 'Core metro market', source: 'FCC BDC Jun 2025', sourceUrl: 'https://broadbandmap.fcc.gov/data-download/nationwide-data', isNew: false, overlap: false, size: 'large' },
    { lat: 45.5152, lng: -122.6784, name: 'Portland Metro', state: 'OR', provider: 'Ziply', status: 'ACTIVE', passings: '~400K passings', notes: 'Core metro market', source: 'FCC BDC Jun 2025', sourceUrl: 'https://broadbandmap.fcc.gov/data-download/nationwide-data', isNew: false, overlap: false, size: 'large' },
    { lat: 43.6150, lng: -116.2023, name: 'Boise / Treasure Valley', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: '~120K passings', notes: 'Core Idaho market', source: 'FCC BDC Jun 2025', sourceUrl: 'https://broadbandmap.fcc.gov/data-download/nationwide-data', isNew: false, overlap: false, size: 'large' },
    { lat: 47.6101, lng: -122.2015, name: 'Bellevue / Eastside', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Strong coverage', notes: 'Tech corridor', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.9790, lng: -122.2021, name: 'Everett / Snohomish', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Boeing corridor', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.0379, lng: -122.9007, name: 'Olympia', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'State capital', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 45.6387, lng: -122.6615, name: 'Vancouver WA', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Clark County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.6588, lng: -117.4260, name: 'Spokane', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active residential', notes: 'Eastern WA hub', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.6777, lng: -116.7805, name: 'Coeur d\'Alene', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Kootenai County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.4917, lng: -112.0339, name: 'Idaho Falls', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'INL corridor', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.8721, lng: -113.9940, name: 'Missoula', state: 'MT', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'University of Montana', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 45.7833, lng: -108.5007, name: 'Billings', state: 'MT', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Largest MT city', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.5002, lng: -111.3008, name: 'Great Falls', state: 'MT', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Malmstrom AFB', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.5927, lng: -112.0361, name: 'Helena', state: 'MT', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'State capital', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 48.1920, lng: -114.3168, name: 'Kalispell / Flathead', state: 'MT', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Glacier gateway', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.6135, lng: -116.3917, name: 'Meridian', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: 'Growing', notes: 'Fastest-growing ID city', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 43.8014, lng: -111.7900, name: 'Rexburg', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: 'Partial', notes: 'BYU-Idaho area', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.8713, lng: -112.4455, name: 'Pocatello', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'ISU campus area', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.5558, lng: -114.4700, name: 'Twin Falls', state: 'ID', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Magic Valley', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },

    // === HUNTER COMMUNICATIONS (Red) ===
    { lat: 44.9429, lng: -123.0351, name: 'Salem (NEW BUILD)', state: 'OR', provider: 'Hunter', status: 'NEW BUILD', passings: '7,000 target by 2026', notes: 'Phase 1 active, Phase 2 announced', source: 'PRNewswire 5/20/25', sourceUrl: 'https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html', isNew: true, overlap: false, size: 'large' },
    { lat: 44.0521, lng: -123.0868, name: 'Eugene', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: '~104K people (49.7%)', notes: 'Battleground: Hunter vs Ziply vs Emerald', source: 'BroadbandNow FCC data', sourceUrl: 'https://broadbandnow.com/Hunter-Communications', isNew: false, overlap: true, size: 'large' },
    { lat: 42.3265, lng: -122.8756, name: 'Medford (HQ)', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: '~30K+ passings', notes: 'Hunter headquarters', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'large' },
    { lat: 42.2249, lng: -121.7817, name: 'Klamath Falls', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'Southern Oregon', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.4390, lng: -123.3284, name: 'Grants Pass', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'Josephine County', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.2165, lng: -123.3417, name: 'Roseburg', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'Douglas County', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.1874, lng: -122.7095, name: 'Ashland', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'SOU campus area', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 44.0462, lng: -123.0220, name: 'Springfield', state: 'OR', provider: 'Hunter', status: 'EXPANDING', passings: 'Expanding', notes: 'East of Eugene', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/springfield/', isNew: false, overlap: false, size: 'normal' },

    // === QUANTUM FIBER / AT&T (Sky Blue) ===
    { lat: 45.5450, lng: -122.6200, name: 'NE Portland', state: 'OR', provider: 'Quantum', status: 'ACTIVE', passings: 'Up to 8 Gbps', notes: 'Now AT&T (Feb 2026)', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Oregon/Portland', isNew: false, overlap: true, size: 'normal' },
    { lat: 45.4950, lng: -122.6100, name: 'SE Portland', state: 'OR', provider: 'Quantum', status: 'ACTIVE', passings: 'Rose City Park area', notes: 'Now AT&T (Feb 2026)', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Oregon/Portland', isNew: false, overlap: true, size: 'normal' },
    { lat: 47.6250, lng: -122.3100, name: 'Seattle (Central)', state: 'WA', provider: 'Quantum', status: 'ACTIVE', passings: 'Partial coverage', notes: 'Now AT&T (Feb 2026)', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 47.2529, lng: -122.4443, name: 'Tacoma', state: 'WA', provider: 'Quantum', status: 'ACTIVE', passings: 'Legacy CenturyLink', notes: 'Now AT&T (Feb 2026)', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.5840, lng: -116.2135, name: 'Boise (Quantum)', state: 'ID', provider: 'Quantum', status: 'ACTIVE', passings: 'Partial coverage', notes: 'Now AT&T (Feb 2026)', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 47.1700, lng: -122.5150, name: 'Lakewood', state: 'WA', provider: 'Quantum', status: 'ACTIVE', passings: 'Legacy CenturyLink', notes: 'Now AT&T (Feb 2026)', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.3200, lng: -122.3200, name: 'Federal Way', state: 'WA', provider: 'Quantum', status: 'ACTIVE', passings: 'Partial coverage', notes: 'Now AT&T', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },

    // === EZEE FIBER (Pink) ===
    { lat: 47.3809, lng: -122.2348, name: 'Kent (HQ)', state: 'WA', provider: 'Ezee', status: 'NEW BUILD', passings: 'First customer Oct 2025', notes: '$400M WA investment', source: 'PRNewswire 10/22/25', sourceUrl: 'https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html', isNew: true, overlap: false, size: 'large' },
    { lat: 47.4018, lng: -122.3245, name: 'Des Moines', state: 'WA', provider: 'Ezee', status: 'NEW BUILD', passings: 'Expanding', notes: 'Adjacent to Kent', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.1854, lng: -122.2929, name: 'Puyallup', state: 'WA', provider: 'Ezee', status: 'NEW BUILD', passings: 'Active build', notes: 'Pierce County expansion', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.2787, lng: -122.2518, name: 'Algona / Pacific', state: 'WA', provider: 'Ezee', status: 'NEW BUILD', passings: 'Construction started', notes: 'South King County', source: 'Ezee Fiber blog', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.3073, lng: -122.2285, name: 'Auburn', state: 'WA', provider: 'Ezee', status: 'EXPANDING', passings: 'Expansion target', notes: 'South of Kent', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: false, size: 'normal' },

    // === LIGHTCURVE (Teal) ===
    { lat: 47.2529, lng: -122.4643, name: 'Tacoma (HQ)', state: 'WA', provider: 'Lightcurve', status: 'ACTIVE', passings: '~120K passings', notes: 'HQ market (fmr. Rainier Connect)', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/', isNew: false, overlap: false, size: 'large' },
    { lat: 46.8676, lng: -122.2687, name: 'Eatonville', state: 'WA', provider: 'Lightcurve', status: 'ACTIVE', passings: 'Active', notes: 'Legacy market', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.7162, lng: -122.9543, name: 'Centralia / Chehalis', state: 'WA', provider: 'Lightcurve', status: 'EXPANDING', passings: 'Expanding', notes: 'Lewis County', source: 'SouthSoundBiz', sourceUrl: 'https://getlightcurve.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.9418, lng: -122.6059, name: 'Yelm', state: 'WA', provider: 'Lightcurve', status: 'EXPANDING', passings: 'Upgrade planned 2026', notes: 'Fiber upgrade', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/', isNew: true, overlap: false, size: 'normal' },
    { lat: 46.9965, lng: -120.5478, name: 'Ellensburg / Kittitas', state: 'WA', provider: 'Lightcurve', status: 'NEW BUILD', passings: 'New fiber launch Jan 2026', notes: 'CWU campus area', source: 'Lightcurve blog', sourceUrl: 'https://getlightcurve.com/', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.0573, lng: -122.7712, name: 'Rochester / Grand Mound', state: 'WA', provider: 'Lightcurve', status: 'ACTIVE', passings: 'Active', notes: 'South Thurston County', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/', isNew: false, overlap: false, size: 'normal' },

    // === ASTOUND / Wave (Orange) ===
    { lat: 45.5229, lng: -122.9898, name: 'Hillsboro', state: 'OR', provider: 'Astound', status: 'ACTIVE', passings: 'Partial coverage', notes: 'Intel corridor', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Astound-Broadband', isNew: false, overlap: false, size: 'normal' },
    { lat: 45.4562, lng: -123.8428, name: 'Tillamook', state: 'OR', provider: 'Astound', status: 'NEW BUILD', passings: '108-mi fiber build', notes: 'AWS-funded coast build', source: 'GeekWire', sourceUrl: 'https://www.geekwire.com/2025/astound-powers-up-new-fiber-internet-near-oregon-coast-with-help-from-aws/', isNew: true, overlap: false, size: 'normal' },
    { lat: 44.8082, lng: -124.0631, name: 'Depoe Bay / Lincoln City', state: 'OR', provider: 'Astound', status: 'ACTIVE', passings: 'Oregon Coast', notes: 'Coast service area', source: 'Astound Broadband', sourceUrl: 'https://www.astound.com/oregon/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.6862, lng: -122.3521, name: 'N Seattle / Shoreline', state: 'WA', provider: 'Astound', status: 'ACTIVE', passings: '~51K Portland area', notes: 'Legacy Wave footprint', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Washington/Seattle', isNew: false, overlap: true, size: 'normal' },
    { lat: 45.4873, lng: -122.8044, name: 'Tigard / Tualatin', state: 'OR', provider: 'Astound', status: 'ACTIVE', passings: 'Partial coverage', notes: 'South Portland suburbs', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Astound-Broadband', isNew: false, overlap: false, size: 'normal' },

    // === FAT BEAM (Amber) ===
    { lat: 47.6588, lng: -117.4260, name: 'Spokane', state: 'WA', provider: 'Fat Beam', status: 'ACTIVE', passings: '~50K enterprise', notes: 'Enterprise focus', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.6732, lng: -117.2394, name: 'Spokane Valley', state: 'WA', provider: 'Fat Beam', status: 'EXPANDING', passings: '24.5 mi fiber (EMAN acq.)', notes: 'EMAN Networks acquisition', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.6743, lng: -117.1181, name: 'Liberty Lake', state: 'WA', provider: 'Fat Beam', status: 'EXPANDING', passings: 'Enterprise expanding', notes: 'Tech hub', source: 'Fat Beam', sourceUrl: 'https://www.fatbeamfiber.com/locations/spokane-wa', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.6777, lng: -116.7805, name: 'Coeur d\'Alene (enterprise)', state: 'ID', provider: 'Fat Beam', status: 'ACTIVE', passings: 'Enterprise', notes: 'Near Spokane metro', source: 'Fat Beam', sourceUrl: 'https://www.fatbeamfiber.com/locations/spokane-wa', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.6095, lng: -116.2140, name: 'Boise (enterprise)', state: 'ID', provider: 'Fat Beam', status: 'ACTIVE', passings: 'Enterprise', notes: 'Enterprise fiber', source: 'Fat Beam', sourceUrl: 'https://www.fatbeamfiber.com/', isNew: false, overlap: false, size: 'normal' },

    // === EMERALD BROADBAND (Violet) ===
    { lat: 44.0505, lng: -123.0950, name: 'Eugene (Local PBC)', state: 'OR', provider: 'Emerald', status: 'ACTIVE', passings: 'River Road, Whiteaker, S. Eugene', notes: 'Public Benefit Corp', source: 'Emerald Broadband', sourceUrl: 'https://www.emeraldbroadband.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 44.0462, lng: -123.0220, name: 'Springfield', state: 'OR', provider: 'Emerald', status: 'EXPANDING', passings: 'Expanding', notes: 'Community fiber', source: 'Emerald Broadband', sourceUrl: 'https://www.emeraldbroadband.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.7465, lng: -122.4615, name: 'Oakridge', state: 'OR', provider: 'Emerald', status: 'ACTIVE', passings: 'Active', notes: 'Rural Lane County', source: 'Emerald Broadband', sourceUrl: 'https://www.emeraldbroadband.com/', isNew: false, overlap: false, size: 'normal' },

    // === TDS TELECOM (Blue) ===
    { lat: 44.0582, lng: -121.3153, name: 'Bend', state: 'OR', provider: 'TDS', status: 'ACTIVE', passings: 'Active FTTP', notes: 'Central Oregon hub', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 44.2726, lng: -121.1739, name: 'Redmond OR', state: 'OR', provider: 'TDS', status: 'ACTIVE', passings: 'Active', notes: 'Airport hub', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },

    // === SPARKLIGHT / Cable One (Purple) ===
    { lat: 43.5846, lng: -116.5635, name: 'Nampa', state: 'ID', provider: 'Sparklight', status: 'ACTIVE', passings: 'Active HFC', notes: 'Cable incumbent', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 43.6629, lng: -116.6874, name: 'Caldwell', state: 'ID', provider: 'Sparklight', status: 'ACTIVE', passings: 'Active HFC', notes: 'Canyon County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.5558, lng: -114.4700, name: 'Twin Falls', state: 'ID', provider: 'Sparklight', status: 'ACTIVE', passings: 'Active HFC', notes: 'Magic Valley', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 42.8713, lng: -112.4455, name: 'Pocatello', state: 'ID', provider: 'Sparklight', status: 'ACTIVE', passings: 'Active HFC', notes: 'SE Idaho', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 43.4917, lng: -112.0339, name: 'Idaho Falls', state: 'ID', provider: 'Sparklight', status: 'ACTIVE', passings: 'Active HFC', notes: 'E Idaho', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },

    // === LUMEN/CENTURYLINK (Gray) ===
    { lat: 46.7324, lng: -117.0002, name: 'Pullman / Moscow', state: 'WA/ID', provider: 'Lumen', status: 'ACTIVE', passings: 'Legacy copper + partial fiber', notes: 'WSU/UI area. Enterprise only post-AT&T sale.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.2396, lng: -119.0987, name: 'Kennewick / Tri-Cities', state: 'WA', provider: 'Lumen', status: 'ACTIVE', passings: 'Legacy service', notes: 'Hanford area. Consumer fiber sold to AT&T.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.6021, lng: -120.5059, name: 'Yakima', state: 'WA', provider: 'Lumen', status: 'ACTIVE', passings: 'Legacy service', notes: 'Central WA. Consumer fiber sold to AT&T.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },

    // === COMCAST (Rose) ===
    { lat: 47.6500, lng: -122.3500, name: 'Seattle (cable)', state: 'WA', provider: 'Comcast', status: 'ACTIVE', passings: 'Major cable incumbent', notes: 'DOCSIS 4.0 upgrade path', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 45.5200, lng: -122.6800, name: 'Portland (cable)', state: 'OR', provider: 'Comcast', status: 'ACTIVE', passings: 'Major cable incumbent', notes: 'DOCSIS 4.0 upgrade path', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' }
];

// ============================================================
// BUILD PIPELINE — Announced builds with sources
// ============================================================
const BUILD_PIPELINE = [
    { provider: 'Hunter', market: 'Salem, OR', state: 'OR', status: 'ACTIVE BUILD', targetHHP: 7000, buildCost: 6.1, timeline: 'Phase 1 Aug 2025, 7K by 2026', overlapRisk: 'Low - no fiber incumbent', notes: 'Phase 1 active NE Salem. Phase 2 announced for Highland, Grant neighborhoods.', source: 'PRNewswire 5/20/25', sourceUrl: 'https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html' },
    { provider: 'Hunter', market: 'Salem Phase 1', state: 'OR', status: 'ACTIVE BUILD', targetHHP: 2500, buildCost: 2.2, timeline: '2,500 HH by EOY 2025', overlapRisk: 'Low', notes: 'Construction began Aug 2025 at 1168 15th St NE. Citywide timeline uncertain per Salem Reporter.', source: 'Salem Reporter 8/15/25', sourceUrl: 'https://www.salemreporter.com/2025/08/15/hunter-communications-expands-its-fiber-internet-into-salem/' },
    { provider: 'Hunter', market: 'Eugene/Springfield', state: 'OR', status: 'EXPANDING', targetHHP: 5000, buildCost: 4.4, timeline: 'Continuous expansion', overlapRisk: 'High - Ziply + Emerald overlap', notes: 'Expanding from commercial to residential. Service page lists Springfield, Oakridge, Coburg.', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/eugene/' },
    { provider: 'Ezee', market: 'Kent, WA (+ S King Co)', state: 'WA', status: 'ACTIVE BUILD', targetHHP: null, buildCost: 400, timeline: 'First customer Oct 2025, expanding to Des Moines/Puyallup', overlapRisk: 'Medium - Ziply incumbent in some areas', notes: '$400M WA investment. Up to 8 Gbps residential. Kent launch market.', source: 'PRNewswire 10/22/25', sourceUrl: 'https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html' },
    { provider: 'Fat Beam', market: 'Spokane Valley', state: 'WA', status: 'EXPANDING', targetHHP: null, buildCost: null, timeline: 'EMAN acquisition completed', overlapRisk: 'Low - enterprise focus', notes: '24.5 miles of metro fiber acquired from EMAN Networks.', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber' },
    { provider: 'Lightcurve', market: 'Tacoma/Pierce Co expansion', state: 'WA', status: 'EXPANDING', targetHHP: 160000, buildCost: null, timeline: 'Yelm upgrade 2026, Kittitas Co. build Jan 2026', overlapRisk: 'Medium - Quantum/AT&T overlap in some areas', notes: '160K total passings company-wide. Expanding Yelm, Ellensburg, Rochester.', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/' },
    { provider: 'Quantum/AT&T', market: 'Portland/Seattle legacy', state: 'OR/WA', status: 'TRANSITION', targetHHP: null, buildCost: 5750, timeline: 'AT&T deal closed Feb 2, 2026', overlapRisk: 'High - overlaps Ziply in multiple markets', notes: 'AT&T paid $5.75B for Lumen consumer fiber. Includes PNW CenturyLink footprint.', source: 'AT&T Newsroom 2/2/26', sourceUrl: 'https://about.att.com/story/2026/att-lumen-deal-close.html' },
    { provider: 'Astound', market: 'Oregon Coast (Hwy 6)', state: 'OR', status: 'COMPLETED', targetHHP: null, buildCost: null, timeline: 'Completed Apr 2025', overlapRisk: 'Low - rural new build', notes: '108-mile fiber build along Hwy 6. AWS-funded. Rural Oregon coast.', source: 'GeekWire 4/8/25', sourceUrl: 'https://www.geekwire.com/2025/astound-powers-up-new-fiber-internet-near-oregon-coast-with-help-from-aws/' },
    { provider: 'Ziply', market: 'Beaverton/Hillsboro expansion', state: 'OR', status: 'EXPANDING', targetHHP: null, buildCost: null, timeline: 'Active expansion 2025-2026', overlapRisk: 'Low - incumbent territory', notes: 'Expanding fiber in Intel corridor per FCC BDC filing updates.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { provider: 'Ziply', market: 'BCE integration / network upgrade', state: 'WA/OR/ID/MT', status: 'EXPANDING', targetHHP: null, buildCost: null, timeline: 'BCE closed acquisition Aug 2025', overlapRisk: 'N/A - incumbent upgrade', notes: 'BCE Inc. acquired Ziply Fiber for ~$5B. Expect accelerated fiber-to-the-home upgrades.', source: 'Light Reading', sourceUrl: 'https://www.lightreading.com/fiber/bce-closes-5-billion-deal-for-ziply-fiber' },
    { provider: 'TDS', market: 'Bend, OR fiber expansion', state: 'OR', status: 'EXPANDING', targetHHP: null, buildCost: null, timeline: 'Ongoing FTTP upgrades', overlapRisk: 'Medium - Vero Fiber now overbuilding', notes: 'TDS upgrading legacy copper to FTTP in Bend/Redmond market. Now faces Vero Fiber competition.', source: 'TDS Telecom', sourceUrl: 'https://tdstelecom.com/' },
    { provider: 'Vero Fiber', market: 'Bend, OR (acquired BendTel)', state: 'OR', status: 'ACTIVE BUILD', targetHHP: null, buildCost: null, timeline: 'Acquired BendTel Dec 1, 2025. 2026: Old Bend, Orchard District expansion.', overlapRisk: 'High - overbuilding TDS/BendBroadband territory', notes: 'Vero Fiber acquired BendTel Dec 1, 2025. Deploying FTTH in Portland Ave/River West. 2026 targets: Old Bend, Orchard District.', source: 'Vero Fiber', sourceUrl: 'https://www.verofiber.com/' },
    { provider: 'DFN', market: 'Eugene/Lane County', state: 'OR', status: 'ACTIVE', targetHHP: null, buildCost: null, timeline: 'Active fiber ISP', overlapRisk: 'High - Eugene is 6+ provider market', notes: 'DFN Networks — 9,596 BSLs in Lane County per BDC J25. Significant local fiber footprint.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { provider: 'Hyak', market: 'Eugene/Lane County', state: 'OR', status: 'ACTIVE', targetHHP: null, buildCost: null, timeline: 'Active fiber ISP', overlapRisk: 'Medium - overlap with Hunter/DFN', notes: 'Hyak Networks — 3,865 BSLs in Lane County per BDC J25.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { provider: 'Emerald', market: 'Eugene/River Road expansion', state: 'OR', status: 'EXPANDING', targetHHP: null, buildCost: null, timeline: 'Continuous community fiber build', overlapRisk: 'Medium - Eugene is contested', notes: 'Public Benefit Corp. Expanding in Eugene neighborhoods: River Road, Whiteaker, South Eugene. Consolidation target.', source: 'Emerald Broadband', sourceUrl: 'https://www.emeraldbroadband.com/' }
,
    // ---- NEW BUILDS FROM RESEARCH (Mar 2026) ----
    { provider: 'TDS', market: 'Spokane, WA metro', state: 'WA', status: 'ACTIVE BUILD', targetHHP: 88000, buildCost: null, timeline: '88K premises, 1,100 mi fiber. Active construction.', overlapRisk: 'High - Ziply incumbent + AT&T + FatBeam residential', notes: 'TDS major FTTP overbuild in Spokane. 88K premises target. Competing against Ziply and AT&T.', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond' },
    { provider: 'TDS', market: 'Billings, MT', state: 'MT', status: 'ACTIVE BUILD', targetHHP: 50000, buildCost: null, timeline: 'First MT city (Aug 2022). 10K+ customers. Expanding to Laurel/Lockwood.', overlapRisk: 'High - Ziply also building 18-20K addresses', notes: 'TDS first MT market. Now 10K+ customers. Adding Laurel + Lockwood 2026. Up to 8 Gbps.', source: 'TDS Telecom', sourceUrl: 'https://tdsfiber.com/build/billings-mt/' },
    { provider: 'TDS', market: 'Great Falls / Helena / Missoula / Butte, MT', state: 'MT', status: 'ACTIVE BUILD', targetHHP: 100000, buildCost: null, timeline: '150K+ total MT fiber locations. Multi-city rollout.', overlapRisk: 'High - Ziply + KDS (Great Falls), Blackfoot (Missoula)', notes: 'TDS targeting 150K+ fiber locations across Montana. 100+ local employees, 1,000+ mi fiber.', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond' },
    { provider: 'TDS', market: 'Sunriver, OR', state: 'OR', status: 'ACTIVE BUILD', targetHHP: null, buildCost: null, timeline: 'All homes by Dec 31, 2026', overlapRisk: 'Low - resort community greenfield', notes: 'FTTH to all Sunriver homes by EOY 2026.', source: 'Sunriver Owners', sourceUrl: 'https://www.sunriverowners.org/community/sunriver-fiber' },
    { provider: 'Ziply', market: 'Billings / Great Falls, MT', state: 'MT', status: 'ACTIVE BUILD', targetHHP: 30000, buildCost: null, timeline: '18-20K Billings + ~10K Great Falls by EOY 2026', overlapRisk: 'High - TDS already established in both markets', notes: 'Brand-new Ziply fiber builds. Up to 50 Gbps. Greenfield in Montana.', source: 'Yahoo Finance', sourceUrl: 'https://finance.yahoo.com/news/ziply-fiber-aggressively-expanding-montana-183500923.html' },
    { provider: 'Ziply', market: 'Vancouver WA', state: 'WA', status: 'ACTIVE BUILD', targetHHP: 11000, buildCost: null, timeline: 'Launched Aug 2025. 11K+ under construction.', overlapRisk: 'Medium - Ezee also entering market', notes: 'Ziply launched fiber in Vancouver WA Aug 2025. Active construction.', source: 'Ziply Fiber', sourceUrl: 'https://ziplyfiber.com/new-fiber-locations/washington/vancouver' },
    { provider: 'Ziply', market: 'Cottage Grove, OR', state: 'OR', status: 'ACTIVE BUILD', targetHHP: 2100, buildCost: null, timeline: 'Launched Jan 2026, 2,100+ addresses', overlapRisk: 'Low - greenfield', notes: 'Part of Ziply\'s 150+ new fiber markets since 2020.', source: 'Ziply Fiber', sourceUrl: 'https://ziplyfiber.com/new-fiber-locations/oregon' },
    { provider: 'Ezee', market: 'Vancouver WA', state: 'WA', status: 'ACTIVE BUILD', targetHHP: null, buildCost: null, timeline: 'Construction started Oct 2025', overlapRisk: 'Medium - Ziply + AT&T incumbent', notes: 'Ezee expanding from Kent to Vancouver WA. Part of $400M WA investment.', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/' },
    { provider: 'Fat Beam', market: 'Boise / Meridian / Twin Falls residential', state: 'ID', status: 'ACTIVE BUILD', targetHHP: null, buildCost: null, timeline: 'Phase 1 Boise complete. Meridian construction 2025. Twin Falls next.', overlapRisk: 'Medium - Ziply + AT&T incumbent in Boise', notes: 'Enterprise-to-residential expansion. 250 Mbps–2 Gbps symmetrical. No contracts.', source: 'Fat Beam', sourceUrl: 'https://www.fatbeamfiber.com/media-news/fatbeam-fiber-expands-fiber-network-to-new-boise-areas-and-meridian' },
    { provider: 'Astound', market: 'Pierce County rural FTTH', state: 'WA', status: 'ACTIVE BUILD', targetHHP: 3200, buildCost: 12.1, timeline: '3,200 premises by EOY 2026', overlapRisk: 'Low - rural areas without fiber', notes: '$12.1M rural Pierce County FTTH build. Targeting unserved/underserved areas.', source: 'Astound Broadband', sourceUrl: 'https://www.astound.com/' },
    { provider: 'Vero Fiber', market: 'Kalispell / Flathead Valley, MT', state: 'MT', status: 'EXPANDING', targetHHP: 1500, buildCost: null, timeline: 'Acquired Montana Digital (MtnMax) Mar 2025', overlapRisk: 'Low - filling in acquired footprint', notes: 'PE-backed consolidator. Acquired MtnMax (261 route mi, 1,500+ customers). Expanding adjacent markets.', source: 'PR Newswire', sourceUrl: 'https://www.prnewswire.com/news-releases/vero-fiber-acquires-montana-digital-expanding-fiber-to-the-premise-business-in-montana-302391884.html' },
    { provider: 'Hunter', market: 'Willamette Valley expansion (Oak Hill Capital)', state: 'OR', status: 'EXPANDING', targetHHP: null, buildCost: null, timeline: 'Oak Hill acquired Dec 2025. Accelerated FTTP densification.', overlapRisk: 'Medium - entering Ziply/AT&T territory', notes: 'Oak Hill Capital (22nd broadband deal, 8 FTTP platforms, 35 states, ~5M passings) to fund expansion beyond Southern OR.', source: 'Oak Hill Capital', sourceUrl: 'https://oakhill.com/2025/12/10/oak-hill-capital-to-acquire-hunter-communications/' }];

// ============================================================
// OVERLAP ALERTS — Key competitive hotspot warnings
// ============================================================
const OVERLAP_ALERTS = [
    { market: 'Eugene/Springfield, OR', severity: 'HIGH', providers: ['Hunter', 'AT&T/Quantum', 'DFN', 'Ziply', 'Hyak', 'Emerald'], description: '6+ fiber providers in Lane County. Hunter dominant (27,523 BSLs), AT&T/Quantum #2 (13,736), DFN (9,596), Ziply (8,559), Hyak (3,865), Emerald PBC growing. Key consolidation market.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Bend, OR', severity: 'MEDIUM', providers: ['TDS/BendBroadband', 'Vero Fiber', 'AT&T/Quantum'], description: 'TDS incumbent (14,675 BSLs) now faces Vero Fiber overbuild (acquired BendTel Dec 2025). AT&T (4,256 BSLs) also present. Fastest-growing OR city.', source: 'FCC BDC J25 / Vero Fiber', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Kent / South King County, WA', severity: 'HIGH', providers: ['Ezee', 'Ziply', 'Comcast'], description: 'Ezee $400M greenfield overbuild entering Ziply incumbent territory. Comcast cable also present.', source: 'PRNewswire', sourceUrl: 'https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html' },
    { market: 'Portland Metro, OR', severity: 'HIGH', providers: ['Ziply', 'Quantum/AT&T', 'Astound', 'Comcast'], description: '4-way competition in select neighborhoods. AT&T now controls former Quantum/CenturyLink fiber assets.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Seattle Metro, WA', severity: 'HIGH', providers: ['Ziply', 'Quantum/AT&T', 'Astound', 'Comcast'], description: 'Dense multi-provider overlap. AT&T entry via Quantum acquisition adds new competitor.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Tacoma / Pierce County, WA', severity: 'MEDIUM', providers: ['Lightcurve', 'Quantum/AT&T', 'Ezee'], description: 'Lightcurve incumbent expanding, AT&T legacy present, Ezee entering from south (Puyallup).', source: 'Lightcurve / Ezee Fiber', sourceUrl: 'https://getlightcurve.com/' },
    { market: 'Spokane / Spokane Valley, WA', severity: 'MEDIUM', providers: ['Ziply', 'Fat Beam'], description: 'Ziply residential vs Fat Beam enterprise. Minimal direct residential overlap.', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/telecom/fatbeam-acquires-eman-networks-spokane-valley-network-gains-24-5-miles-fiber' },
    { market: 'Boise / Meridian, ID', severity: 'MEDIUM', providers: ['Ziply', 'Sparklight', 'Quantum/AT&T'], description: 'Ziply fiber vs Sparklight HFC vs legacy Quantum. Fast-growing market attracting investment.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Nampa / Caldwell, ID', severity: 'LOW', providers: ['Sparklight', 'Ziply'], description: 'Sparklight cable incumbent with some Ziply fiber overlap. Growing Canyon County.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Federal Way, WA', severity: 'MEDIUM', providers: ['Ziply', 'Quantum/AT&T'], description: 'Ziply and former Quantum/CenturyLink overlap. Now Ziply vs AT&T.', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/' },
    { market: 'Salem, OR', severity: 'LOW', providers: ['Hunter'], description: 'Greenfield build - no fiber incumbent. Hunter entering without overbuild risk.', source: 'PRNewswire', sourceUrl: 'https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html' }
,
    // ---- NEW OVERLAP ALERTS FROM RESEARCH (Mar 2026) ----
    { market: 'Spokane, WA', severity: 'HIGH', providers: ['Ziply', 'TDS', 'AT&T/Quantum', 'FatBeam'], description: '4-way fiber competition. TDS building 88K premises / 1,100 mi. FatBeam now offering residential FTTH. Ziply incumbent. AT&T legacy.', source: 'Fierce Network / FatBeam', sourceUrl: 'https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond' },
    { market: 'Vancouver WA', severity: 'MEDIUM', providers: ['Ziply', 'Ezee', 'AT&T/Quantum'], description: 'Ziply launched Aug 2025 (11K+ under construction). Ezee started construction Oct 2025. AT&T legacy present.', source: 'Ziply Fiber / Ezee Fiber', sourceUrl: 'https://ziplyfiber.com/' },
    { market: 'Billings, MT', severity: 'HIGH', providers: ['TDS', 'Ziply'], description: 'Both TDS and Ziply making major capital investments. TDS: 50K+ addresses, 10K+ customers. Ziply: 18-20K new addresses.', source: 'TDS / Yahoo Finance', sourceUrl: 'https://tdsfiber.com/build/billings-mt/' },
    { market: 'Great Falls, MT', severity: 'HIGH', providers: ['Ziply', 'TDS', 'KDS Fiber'], description: 'Rare 3-way fiber overbuild. All three building simultaneously in a city of ~60K.', source: 'Ziply / TDS / KDS', sourceUrl: 'https://ziplyfiber.com/news/great-falls-mt' },
    { market: 'Missoula, MT', severity: 'MEDIUM', providers: ['Blackfoot', 'TDS', 'Ziply'], description: '3-way fiber battleground. Blackfoot ILEC (~25%), TDS overbuilder, Ziply new entrant.', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Montana/Missoula' },
    { market: 'Bozeman, MT', severity: 'MEDIUM', providers: ['Montana Opticom', 'SkyNet', 'Spectrum'], description: 'Montana Opticom (~48%) vs SkyNet (~77%) vs Spectrum cable. Two strong local providers.', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Montana/Bozeman' },
    { market: 'Boise / Meridian, ID', severity: 'HIGH', providers: ['Ziply', 'AT&T', 'FatBeam', 'Sparklight'], description: 'AT&T doubling Idaho fiber builds. FatBeam entering residential. Ziply incumbent. Sparklight cable. Fastest-growing ID metro.', source: 'BoiseDev / FatBeam', sourceUrl: 'https://boisedev.com/news/2026/02/03/major-company-acquires-fiber-internet-networks-in-boise/' },
    { market: 'Kalispell / Flathead Valley, MT', severity: 'LOW', providers: ['Vero Fiber', 'MontanaSky', 'Blackfoot'], description: 'Vero Fiber (PE-backed) acquired MtnMax Mar 2025. Consolidation play — PE acquirer targeting small fiber operators.', source: 'PR Newswire', sourceUrl: 'https://www.prnewswire.com/news-releases/vero-fiber-acquires-montana-digital-expanding-fiber-to-the-premise-business-in-montana-302391884.html' }];

// ============================================================
// FCC BDC INCREMENTAL ANALYSIS
// Shows what FCC BDC currently reports vs. announced builds
// BDC data as of Jun 2025 filing; press releases through Mar 2026
// ============================================================
const FCC_INCREMENTAL = [
    {
        operator: 'Ziply Fiber',
        operatorId: 'ziply',
        fccBDCPassings: '~1,600,000 (4-state footprint)',
        fccBDCDate: 'Jun 2025',
        fccBDCSource: 'FCC BDC',
        fccBDCUrl: 'https://broadbandmap.fcc.gov/data-download/fixed',
        announcedAdditional: [
            { market: 'Beaverton/Hillsboro expansion', passings: 'TBD', source: 'FCC BDC filings', sourceUrl: 'https://broadbandmap.fcc.gov/', status: 'Active build' },
            { market: 'BCE integration upgrades', passings: 'Copper-to-fiber upgrades across footprint', source: 'Light Reading', sourceUrl: 'https://www.lightreading.com/fiber/bce-closes-5-billion-deal-for-ziply-fiber', status: 'Planned' }
        ],
        incrementalEstimate: 'BCE capital to accelerate copper-to-fiber conversion',
        incrementalNote: 'Ziply is the largest fiber provider in the PNW with ~1.3M subscribers across WA/OR/ID/MT. BCE acquisition (Aug 2025) brings additional capital for fiber upgrades of remaining copper territory.',
        methodology: 'FCC BDC data + BroadbandNow subscriber counts + BCE acquisition filings.'
    },
    {
        operator: 'Hunter Communications',
        operatorId: 'hunter',
        fccBDCPassings: '~100,000 (Oregon)',
        fccBDCDate: 'Jun 2025',
        fccBDCSource: 'FCC BDC',
        fccBDCUrl: 'https://broadbandmap.fcc.gov/data-download/fixed',
        announcedAdditional: [
            { market: 'Salem (7,000 HH)', passings: '7,000', source: 'PRNewswire 5/20/25', sourceUrl: 'https://www.prnewswire.com/news-releases/oregon-based-internet-provider-announces-expansion-into-salem-metro-region-302460727.html', status: 'Phase 1 active, Phase 2 announced' },
            { market: 'Eugene/Springfield residential', passings: '5,000+', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/eugene/', status: 'Expanding from commercial base' }
        ],
        incrementalEstimate: '12,000+ new passings in pipeline',
        incrementalNote: 'Hunter has ~100K passings and ~25K subscribers primarily in Southern Oregon. Salem entry is first Willamette Valley build. Eugene residential expansion underway.',
        methodology: 'BroadbandNow subscriber data + PRNewswire Salem announcement + Hunter service area pages.'
    },
    {
        operator: 'Quantum Fiber (now AT&T)',
        operatorId: 'quantum',
        fccBDCPassings: '~500,000+ (PNW CenturyLink legacy)',
        fccBDCDate: 'Jun 2025',
        fccBDCSource: 'FCC BDC',
        fccBDCUrl: 'https://broadbandmap.fcc.gov/data-download/fixed',
        announcedAdditional: [
            { market: 'AT&T integration', passings: 'Existing footprint transferred', source: 'AT&T Newsroom 2/2/26', sourceUrl: 'https://about.att.com/story/2026/att-lumen-deal-close.html', status: 'Closed Feb 2026' }
        ],
        incrementalEstimate: 'Primarily technology upgrade of existing footprint under AT&T ownership',
        incrementalNote: 'AT&T closed $5.75B acquisition of Lumen consumer fiber (Quantum Fiber) on Feb 2, 2026. PNW assets include Portland, Seattle, Boise metro fiber. Expect AT&T to invest in expansion and XGS-PON upgrades.',
        methodology: 'AT&T Newsroom press release + Denver Post reporting on deal terms.'
    },
    {
        operator: 'Ezee Fiber',
        operatorId: 'ezee',
        fccBDCPassings: '0 (new WA entrant)',
        fccBDCDate: 'Jun 2025',
        fccBDCSource: 'FCC BDC',
        fccBDCUrl: 'https://broadbandmap.fcc.gov/data-download/fixed',
        announcedAdditional: [
            { market: 'Kent + S King County', passings: 'TBD ($400M total WA investment)', source: 'PRNewswire 10/22/25', sourceUrl: 'https://www.prnewswire.com/news-releases/ezee-fiber-connects-first-customer-in-kent-wa-marking-launch-in-washington-state-302591803.html', status: 'Active build, first customer Oct 2025' },
            { market: 'Des Moines/Puyallup/Auburn', passings: 'Expanding', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', status: 'Construction started' }
        ],
        incrementalEstimate: '100% incremental — new entrant not in BDC',
        incrementalNote: 'Ezee Fiber is a TX-based overbuilder entering WA with $400M investment. Not yet in FCC BDC. First WA customer connected Oct 2025 in Kent. Expanding to Des Moines, Puyallup, Auburn.',
        methodology: 'PRNewswire launch announcement + Ezee Fiber website/blog.'
    },
    {
        operator: 'Lightcurve',
        operatorId: 'lightcurve',
        fccBDCPassings: '~160,000 (Pierce/Thurston/Lewis Counties)',
        fccBDCDate: 'Jun 2025',
        fccBDCSource: 'FCC BDC',
        fccBDCUrl: 'https://broadbandmap.fcc.gov/data-download/fixed',
        announcedAdditional: [
            { market: 'Yelm fiber upgrade', passings: 'Upgrade existing HFC', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/', status: 'Planned 2026' },
            { market: 'Kittitas County / Ellensburg', passings: 'New fiber build', source: 'Lightcurve', sourceUrl: 'https://getlightcurve.com/', status: 'Construction Jan 2026' }
        ],
        incrementalEstimate: 'Yelm upgrade + Ellensburg new build',
        incrementalNote: 'Palisade Infrastructure acquired Rainier Connect and rebranded to Lightcurve. 160K total passings. Expanding with Yelm fiber upgrade and new Kittitas County build.',
        methodology: 'Lightcurve website/newsroom + company announcements.'
    },
    {
        operator: 'Astound Broadband',
        operatorId: 'astound',
        fccBDCPassings: '~150,000 (OR/WA combined)',
        fccBDCDate: 'Jun 2025',
        fccBDCSource: 'FCC BDC',
        fccBDCUrl: 'https://broadbandmap.fcc.gov/data-download/fixed',
        announcedAdditional: [
            { market: 'Oregon Coast (108-mi Hwy 6)', passings: 'Rural fiber build', source: 'GeekWire 4/8/25', sourceUrl: 'https://www.geekwire.com/2025/astound-powers-up-new-fiber-internet-near-oregon-coast-with-help-from-aws/', status: 'Completed Apr 2025' }
        ],
        incrementalEstimate: '108-mi rural fiber build completed, incremental passings TBD',
        incrementalNote: 'Astound (fmr. Wave Broadband) completed 108-mile Hwy 6 fiber build to Oregon Coast with AWS funding. Portland/Seattle HFC territory upgrading to DOCSIS 3.1.',
        methodology: 'GeekWire reporting + BroadbandNow market data.'
    }
];

// ============================================================
// QUICK NAVIGATION — Jump points for metro areas
// ============================================================
const QUICK_NAV = [
    { label: 'Kent, WA', key: 'kent_wa', lat: 47.3700, lng: -122.2600, zoom: 11, tag: 'EZEE NEW', tagColor: 'pink' },
    { label: 'Seattle', key: 'seattle_wa', lat: 47.6300, lng: -122.3400, zoom: 11, tag: 'MULTI', tagColor: 'green' },
    { label: 'Bellevue / Eastside', key: 'bellevue_wa', lat: 47.6300, lng: -122.1700, zoom: 11, tag: 'ZIPLY', tagColor: 'green' },
    { label: 'Tacoma', key: 'tacoma_wa', lat: 47.2200, lng: -122.4500, zoom: 11, tag: 'LIGHTCURVE', tagColor: 'teal' },
    { label: 'Everett', key: 'everett_wa', lat: 47.9800, lng: -122.2000, zoom: 11, tag: 'ZIPLY', tagColor: 'green' },
    { label: 'Olympia', key: 'olympia_wa', lat: 47.0400, lng: -122.8700, zoom: 12, tag: 'UNDERSERVED', tagColor: 'amber' },
    { label: 'Spokane', key: 'spokane_wa', lat: 47.6588, lng: -117.3260, zoom: 11, tag: 'TDS BUILD', tagColor: 'blue' },
    { label: 'Vancouver WA', key: 'vancouver_wa', lat: 45.6500, lng: -122.6500, zoom: 11, tag: 'NEW BUILD', tagColor: 'pink' },
    { label: 'Portland', key: 'portland_or', lat: 45.5152, lng: -122.8000, zoom: 11, tag: 'QUANTUM', tagColor: 'sky' },
    { label: 'Eugene', key: 'eugene_or', lat: 44.0521, lng: -123.0550, zoom: 12, tag: 'BATTLE', tagColor: 'red' },
    { label: 'Salem', key: 'salem_or', lat: 44.9600, lng: -123.0000, zoom: 11, tag: 'HUNTER NEW', tagColor: 'red' },
    { label: 'Bend', key: 'bend_or', lat: 44.0600, lng: -121.3000, zoom: 12, tag: 'TDS', tagColor: 'blue' },
    { label: 'Medford', key: 'medford_or', lat: 42.3265, lng: -122.8756, zoom: 11, tag: 'HUNTER HQ', tagColor: 'red' },
    { label: 'Boise', key: 'boise_id', lat: 43.6150, lng: -116.2500, zoom: 11, tag: 'ATT+ZIP', tagColor: 'sky' },
    { label: 'Nampa / Caldwell', key: 'nampa_id', lat: 43.6200, lng: -116.6000, zoom: 11, tag: 'SPARKLIGHT', tagColor: 'purple' },
    { label: 'Coeur d\'Alene', key: 'cda_id', lat: 47.6777, lng: -116.7805, zoom: 12, tag: 'ZIPLY', tagColor: 'green' },
    { label: 'Idaho Falls', key: 'idaho_falls_id', lat: 43.4917, lng: -112.0339, zoom: 12, tag: 'ZIPLY', tagColor: 'green' },
    { label: 'Twin Falls', key: 'twin_falls_id', lat: 42.5558, lng: -114.4700, zoom: 12, tag: 'SPARKLIGHT', tagColor: 'purple' },
    { label: 'Missoula', key: 'missoula_mt', lat: 46.8721, lng: -113.9940, zoom: 12, tag: 'BATTLE', tagColor: 'red' },
    { label: 'Billings', key: 'billings_mt', lat: 45.7833, lng: -108.5007, zoom: 12, tag: 'TDS+ZIP', tagColor: 'blue' },
    { label: 'Great Falls', key: 'great_falls_mt', lat: 47.5002, lng: -111.3008, zoom: 12, tag: '3-WAY', tagColor: 'red' },
    { label: 'Helena', key: 'helena_mt', lat: 46.5927, lng: -112.0361, zoom: 12, tag: 'ZIPLY', tagColor: 'green' },
    { label: 'Kalispell', key: 'kalispell_mt', lat: 48.1920, lng: -114.3168, zoom: 11, tag: 'VERO ACQ', tagColor: 'amber' },
    { label: 'Bozeman', key: 'bozeman_mt', lat: 45.6770, lng: -111.0429, zoom: 12, tag: 'OPTICOM', tagColor: 'amber' },
    { label: 'Full Region', key: 'overview', lat: 46.0, lng: -118.0, zoom: 5, tag: 'ALL', tagColor: 'slate' }
];

// ============================================================
// TRACT REGIONS — Group tracts by geographic area for lazy loading
// ============================================================
const TRACT_REGIONS = {
    puget_sound: ['seattle_wa', 'tacoma_wa', 'kent_wa', 'bellevue_wa', 'everett_wa', 'olympia_wa'],
    portland_metro: ['portland_or', 'vancouver_wa'],
    willamette_valley: ['eugene_or', 'salem_or'],
    central_oregon: ['bend_or'],
    southern_oregon: ['medford_or'],
    eastern_wa: ['spokane_wa'],
    idaho_west: ['boise_id', 'nampa_id'],
    idaho_north: ['cda_id'],
    idaho_east: ['idaho_falls_id', 'twin_falls_id'],
    montana_west: ['missoula_mt', 'kalispell_mt'],
    montana_central: ['helena_mt', 'great_falls_mt', 'bozeman_mt'],
    montana_east: ['billings_mt']
};

// ============================================================
// Additional market markers to reach 80+ coverage
// ============================================================
MARKET_MARKERS.push(
    // Additional Ziply markets
    { lat: 47.7511, lng: -122.2054, name: 'Kenmore / Bothell', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'North King County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.4443, lng: -122.3047, name: 'Tukwila / SeaTac', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Airport area', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 48.7519, lng: -122.4787, name: 'Bellingham', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'WWU campus area', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.2396, lng: -119.0987, name: 'Kennewick / Tri-Cities', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Hanford area', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.6021, lng: -120.5059, name: 'Yakima', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Partial', notes: 'Central WA', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 48.0520, lng: -122.1771, name: 'Marysville', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'North Snohomish', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 45.3573, lng: -122.6068, name: 'Oregon City', state: 'OR', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Clackamas County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 45.4057, lng: -122.7704, name: 'Tualatin / Sherwood', state: 'OR', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'South Portland suburbs', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 46.1382, lng: -122.9382, name: 'Longview / Kelso', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'Cowlitz County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: false, size: 'normal' },
    { lat: 47.4831, lng: -122.2171, name: 'Renton', state: 'WA', provider: 'Ziply', status: 'ACTIVE', passings: 'Active', notes: 'South King County', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    // Additional Hunter markets
    { lat: 42.3849, lng: -122.9116, name: 'Central Point', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'North of Medford', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.2109, lng: -123.3450, name: 'Roseburg', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'Douglas County seat', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 43.7465, lng: -122.4615, name: 'Oakridge', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: 'Active', notes: 'Upper Willamette', source: 'Hunter Communications', sourceUrl: 'https://hunterfiber.com/', isNew: false, overlap: false, size: 'normal' },
    // Additional Ezee
    { lat: 47.3210, lng: -122.3128, name: 'Federal Way (Ezee target)', state: 'WA', provider: 'Ezee', status: 'EXPANDING', passings: 'Expansion target', notes: 'South King County', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: true, size: 'normal' },
    // Additional Comcast
    { lat: 47.6101, lng: -122.2015, name: 'Bellevue (cable)', state: 'WA', provider: 'Comcast', status: 'ACTIVE', passings: 'Cable incumbent', notes: 'Eastside cable', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 47.9790, lng: -122.2021, name: 'Everett (cable)', state: 'WA', provider: 'Comcast', status: 'ACTIVE', passings: 'Cable incumbent', notes: 'Snohomish cable', source: 'FCC BDC', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    // ---- EUGENE/LANE COUNTY FOCUS MARKET ----
    { lat: 44.0521, lng: -123.0868, name: 'Eugene (Hunter)', state: 'OR', provider: 'Hunter', status: 'ACTIVE', passings: '27,523 BSLs (BDC)', notes: 'Dominant fiber in Lane Co. 27,523 BSLs. SDCF Capital PE-backed.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'large' },
    { lat: 44.0565, lng: -123.0750, name: 'Eugene (DFN)', state: 'OR', provider: 'DFN', status: 'ACTIVE', passings: '9,596 BSLs (BDC)', notes: 'DFN Networks — 9,596 BSLs in Lane Co. Local fiber ISP.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 44.0480, lng: -123.0950, name: 'Eugene (Hyak)', state: 'OR', provider: 'Hyak', status: 'ACTIVE', passings: '3,865 BSLs (BDC)', notes: 'Hyak Networks — 3,865 BSLs in Lane Co.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    { lat: 44.0550, lng: -123.1000, name: 'Eugene (Emerald Broadband)', state: 'OR', provider: 'Emerald', status: 'ACTIVE', passings: '~2,500 BSLs (est.)', notes: 'Public Benefit Corp fiber ISP. Founded 2016. 859 Willamette St. Consolidation target.', source: 'Emerald Broadband', sourceUrl: 'https://www.emeraldbroadband.com/', isNew: false, overlap: true, size: 'normal' },
    // ---- KENT, WA — ACTIVE BUILDERS ----
    { lat: 47.3809, lng: -122.2348, name: 'Kent (Ezee Fiber — BUILDING)', state: 'WA', provider: 'Ezee', status: 'EXPANDING', passings: 'First customer Oct 2025', notes: '$400M+ WA investment. Expanding to Des Moines, Puyallup. Active overbuilder.', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: true, size: 'large' },
    { lat: 47.3850, lng: -122.2250, name: 'Kent (AT&T/Quantum)', state: 'WA', provider: 'Quantum', status: 'ACTIVE', passings: '~9.5% of Kent (BDC)', notes: 'Legacy CenturyLink fiber. Now AT&T (Feb 2026). Limited Kent coverage.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'normal' },
    // ---- BEND, OR — ACTIVE BUILDERS ----
    { lat: 44.0582, lng: -121.3153, name: 'Bend (TDS/BendBroadband)', state: 'OR', provider: 'TDS', status: 'ACTIVE', passings: '14,675 BSLs (BDC)', notes: 'Incumbent. BendBroadband subsidiary. Sunriver FTTH project.', source: 'FCC BDC J25', sourceUrl: 'https://broadbandmap.fcc.gov/', isNew: false, overlap: true, size: 'large' },
    { lat: 44.0620, lng: -121.3050, name: 'Bend (Vero Fiber — BUILDING)', state: 'OR', provider: 'Vero', status: 'EXPANDING', passings: 'Active FTTH deploy', notes: 'Acquired BendTel Dec 1, 2025. Deploying FTTH: Portland Ave, River West. 2026: Old Bend, Orchard District.', source: 'Vero Fiber', sourceUrl: 'https://www.verofiber.com/', isNew: true, overlap: true, size: 'normal' }
,
    // ---- WA GROWTH AREAS (from research Mar 2026) ----
    { lat: 47.6580, lng: -117.4100, name: 'Spokane (TDS — MAJOR BUILD)', state: 'WA', provider: 'TDS', status: 'NEW BUILD', passings: '88K premises / 1,100 mi fiber', notes: 'Major TDS FTTP overbuild in Spokane metro', source: 'Fierce Network', sourceUrl: 'https://www.fierce-network.com/broadband/tds-telecom-targets-fiber-110k-big-sky-country-and-beyond', isNew: true, overlap: true, size: 'large' },
    { lat: 47.6600, lng: -117.4000, name: 'Spokane (FatBeam — Residential FTTH)', state: 'WA', provider: 'Fat Beam', status: 'NEW BUILD', passings: 'Now residential FTTH (250M-2G)', notes: 'Enterprise provider now offering residential. No contracts, 2 Gbps.', source: 'Fat Beam', sourceUrl: 'https://www.fatbeamfiber.com/', isNew: true, overlap: true, size: 'normal' },
    { lat: 47.3750, lng: -122.2400, name: 'Kent (Google Fiber — EXPLORING)', state: 'WA', provider: 'Google Fiber', status: 'EXPLORING', passings: 'Advanced conversations w/ city (May 2024)', notes: 'Google Fiber exploring Kent single-family FTTH. Not yet committed.', source: 'GeekWire', sourceUrl: 'https://www.geekwire.com/', isNew: true, overlap: true, size: 'normal' },
    { lat: 45.6350, lng: -122.6700, name: 'Vancouver WA (Ziply — NEW BUILD)', state: 'WA', provider: 'Ziply', status: 'NEW BUILD', passings: '11K+ addresses under construction', notes: 'Ziply launched Aug 2025 in Vancouver WA. Active fiber construction.', source: 'Ziply Fiber', sourceUrl: 'https://ziplyfiber.com/new-fiber-locations/washington/vancouver', isNew: true, overlap: true, size: 'normal' },
    { lat: 45.6300, lng: -122.6800, name: 'Vancouver WA (Ezee — NEW BUILD)', state: 'WA', provider: 'Ezee', status: 'NEW BUILD', passings: 'Construction started Oct 2025', notes: 'Ezee expanding beyond Kent to Vancouver WA. Part of $400M WA investment.', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: true, size: 'normal' },
    { lat: 47.0400, lng: -122.8900, name: 'Olympia (underserved — ~2.9% fiber)', state: 'WA', provider: 'Lightcurve', status: 'EXPANDING', passings: 'Thurston Co expansion', notes: 'Only ~2.9% fiber availability. Lightcurve expanding into Thurston Co.', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Washington/Olympia', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.1900, lng: -122.2800, name: 'Puyallup/Milton/Lakewood (Ezee)', state: 'WA', provider: 'Ezee', status: 'EXPANDING', passings: 'Active construction', notes: 'Ezee expanding from Kent into Pierce Co suburbs.', source: 'Ezee Fiber', sourceUrl: 'https://ezeefiber.com/', isNew: true, overlap: false, size: 'normal' },
    { lat: 47.2400, lng: -122.4800, name: 'Tacoma (Astound Rural FTTH)', state: 'WA', provider: 'Astound', status: 'NEW BUILD', passings: '3,200 premises by EOY 2026', notes: '$12.1M Pierce County rural FTTH build. 3,200 premises target.', source: 'Astound Broadband', sourceUrl: 'https://www.astound.com/', isNew: true, overlap: false, size: 'normal' },
    // ---- OR GROWTH AREAS (from research Mar 2026) ----
    { lat: 44.9450, lng: -123.0250, name: 'Salem (Hunter Phase 1 — ACTIVE)', state: 'OR', provider: 'Hunter', status: 'ACTIVE BUILD', passings: '2,500 by EOY 2025, 7K by 2026', notes: 'Phase 1 active: Morningside, Faye Wright, Grant, Highland. Up to 2.5 Gbps res.', source: 'Salem Reporter', sourceUrl: 'https://www.salemreporter.com/2025/08/15/hunter-communications-expands-its-fiber-internet-into-salem/', isNew: true, overlap: false, size: 'large' },
    { lat: 44.0600, lng: -121.3100, name: 'Bend (Vero Fiber — Portland Ave/River West)', state: 'OR', provider: 'Vero', status: 'ACTIVE BUILD', passings: 'BendTel acq. Dec 2025', notes: 'PE-backed (Delta-v Capital, Hamilton Lane $80M). Old Bend + Orchard District next.', source: 'PR Newswire', sourceUrl: 'https://www.prnewswire.com/news-releases/vero-fiber-completes-acquisition-of-bendtel-inc-302630739.html', isNew: true, overlap: true, size: 'normal' },
    { lat: 44.0550, lng: -121.3200, name: 'Sunriver (TDS — FTTH all homes)', state: 'OR', provider: 'TDS', status: 'ACTIVE BUILD', passings: 'All homes by Dec 31, 2026', notes: 'TDS building FTTH to all Sunriver homes by EOY 2026.', source: 'Sunriver Owners', sourceUrl: 'https://www.sunriverowners.org/community/sunriver-fiber', isNew: true, overlap: false, size: 'normal' },
    { lat: 44.0521, lng: -123.1050, name: 'Eugene (DFN — Douglas Electric Coop)', state: 'OR', provider: 'DFN', status: 'ACTIVE', passings: '2,500+ mi fiber', notes: 'Douglas Fast Net. Wholly-owned subsidiary of Douglas Electric Cooperative. Founded 2001. $25M invested.', source: 'DFN', sourceUrl: 'https://dfn.net/about-us/', isNew: false, overlap: true, size: 'normal' },
    { lat: 43.9600, lng: -123.2900, name: 'Cottage Grove (Ziply — NEW)', state: 'OR', provider: 'Ziply', status: 'NEW BUILD', passings: '2,100+ addresses (Jan 2026)', notes: 'Ziply launched fiber in Cottage Grove Jan 2026. 150+ new fiber markets since 2020.', source: 'Ziply Fiber', sourceUrl: 'https://ziplyfiber.com/new-fiber-locations/oregon', isNew: true, overlap: false, size: 'normal' },
    { lat: 44.5646, lng: -123.2620, name: 'Corvallis (PEAK Internet — limited FTTP)', state: 'OR', provider: 'PEAK', status: 'ACTIVE', passings: '~4.3% of Corvallis homes', notes: 'PEAK Internet: fiber to ~4.3% homes, up to 5 Gbps. Mostly fixed wireless.', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Oregon/Corvallis', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.1950, lng: -122.7100, name: 'Ashland (AFN — Municipal Open-Access)', state: 'OR', provider: 'AFN', status: 'ACTIVE', passings: '40 mi fiber + 119 mi coax', notes: 'City-owned since late 1990s. Open-access model. Aging infrastructure, needs fiber upgrade.', source: 'City of Ashland', sourceUrl: 'https://ashlandoregon.gov/171/Ashland-Fiber-Network', isNew: false, overlap: false, size: 'normal' },
    // ---- ID GROWTH AREAS (from research Mar 2026) ----
    { lat: 43.6200, lng: -116.2300, name: 'Boise (FatBeam — Residential FTTH)', state: 'ID', provider: 'Fat Beam', status: 'NEW BUILD', passings: 'Phase 1 complete, Meridian next', notes: 'FatBeam launched residential FTTH in Boise. Expanding to Meridian + Twin Falls.', source: 'Fat Beam', sourceUrl: 'https://www.fatbeamfiber.com/media-news/fatbeam-fiber-expands-fiber-network-to-new-boise-areas-and-meridian', isNew: true, overlap: true, size: 'normal' },
    { lat: 43.6200, lng: -116.2000, name: 'Boise (AT&T — doubling build rate)', state: 'ID', provider: 'Quantum', status: 'EXPANDING', passings: 'Nearly doubling ID fiber builds', notes: 'AT&T pledged to nearly double Idaho fiber build rate in 2026 post-Lumen acquisition.', source: 'BoiseDev', sourceUrl: 'https://boisedev.com/news/2026/02/03/major-company-acquires-fiber-internet-networks-in-boise/', isNew: true, overlap: true, size: 'normal' },
    { lat: 43.4917, lng: -112.0339, name: 'Idaho Falls (Municipal Fiber — citywide)', state: 'ID', provider: 'Municipal', status: 'ACTIVE', passings: 'Citywide open-access fiber', notes: 'Idaho Falls Fiber: citywide open-access. 6 ISPs compete on same network. Municipal electric utility.', source: 'Idaho Falls Fiber', sourceUrl: 'https://www.idahofallsfiber.com/', isNew: false, overlap: false, size: 'normal' },
    { lat: 42.5558, lng: -114.4700, name: 'Twin Falls (73.9% fiber availability)', state: 'ID', provider: 'Quantum', status: 'ACTIVE', passings: 'AT&T + TDS + Project Mutual', notes: '73.9% fiber availability. AT&T/Quantum + TDS + Project Mutual Telephone Coop.', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Idaho/Twin-Falls', isNew: false, overlap: true, size: 'normal' },
    { lat: 47.6777, lng: -116.7805, name: 'Coeur d\'Alene (TDS — most widely available)', state: 'ID', provider: 'TDS', status: 'ACTIVE', passings: 'Up to 8 Gbps', notes: 'TDS most widely available FTTP in CdA. + Ziply + Intermax Networks (local fiber/WISP).', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/', isNew: false, overlap: true, size: 'normal' },
    // ---- MT GROWTH AREAS (from research Mar 2026) ----
    { lat: 45.7833, lng: -108.5007, name: 'Billings (TDS — 50K+ addresses)', state: 'MT', provider: 'TDS', status: 'ACTIVE BUILD', passings: '50K+ addresses, 10K+ customers', notes: 'TDS first MT city (Aug 2022). 10K+ customers Feb 2026. Expanding to Laurel/Lockwood.', source: 'TDS Telecom', sourceUrl: 'https://tdsfiber.com/build/billings-mt/', isNew: true, overlap: true, size: 'large' },
    { lat: 45.7900, lng: -108.4900, name: 'Billings (Ziply — 18-20K addresses)', state: 'MT', provider: 'Ziply', status: 'ACTIVE BUILD', passings: '18K-20K addresses by EOY 2026', notes: 'Brand-new Ziply fiber build in Billings. Up to 50 Gbps.', source: 'Yahoo Finance', sourceUrl: 'https://finance.yahoo.com/news/ziply-fiber-aggressively-expanding-montana-183500923.html', isNew: true, overlap: true, size: 'normal' },
    { lat: 47.5002, lng: -111.3008, name: 'Great Falls (3-way fiber overbuild)', state: 'MT', provider: 'Multiple', status: 'ACTIVE BUILD', passings: 'Ziply ~10K + TDS + KDS', notes: 'Rare 3-way overbuild: Ziply, TDS, and KDS Fiber all building simultaneously.', source: 'Ziply Fiber', sourceUrl: 'https://ziplyfiber.com/news/great-falls-mt', isNew: true, overlap: true, size: 'normal' },
    { lat: 47.5050, lng: -111.2900, name: 'Great Falls (KDS Fiber — local)', state: 'MT', provider: 'KDS', status: 'ACTIVE', passings: 'South/SW Great Falls + expanding', notes: 'KDS Fiber (Konceptio Data Services). Local since 2007. 1 Gbps. 100% underground conduit.', source: 'KDS Fiber', sourceUrl: 'https://www.kdsmt.com/', isNew: false, overlap: true, size: 'normal' },
    { lat: 46.8721, lng: -113.9940, name: 'Missoula (3-way fiber battleground)', state: 'MT', provider: 'Multiple', status: 'ACTIVE BUILD', passings: 'Blackfoot + TDS + Ziply', notes: 'Blackfoot (ILEC, ~25%), TDS (overbuilder), Ziply (new entrant). 3-way fiber competition.', source: 'BroadbandNow', sourceUrl: 'https://broadbandnow.com/Montana/Missoula', isNew: true, overlap: true, size: 'normal' },
    { lat: 45.6770, lng: -111.0429, name: 'Bozeman (Montana Opticom — 48% coverage)', state: 'MT', provider: 'MT Opticom', status: 'ACTIVE', passings: '~48% of Bozeman homes', notes: 'Family-owned since 1999. Only locally-owned all-fiber telco in Gallatin Valley. $64.99-$99.99/mo.', source: 'Montana Opticom', sourceUrl: 'https://broadbandnow.com/Montana-Opticom', isNew: false, overlap: true, size: 'normal' },
    { lat: 45.6800, lng: -111.0350, name: 'Bozeman (SkyNet — 77% coverage)', state: 'MT', provider: 'SkyNet', status: 'ACTIVE', passings: '~77% of Bozeman homes', notes: 'SkyNet Communications. 30+ years. Multi-100Gb backbone. Local fiber/wireless ISP.', source: 'SkyNet Communications', sourceUrl: 'http://getskynet.com/', isNew: false, overlap: true, size: 'normal' },
    { lat: 48.1920, lng: -114.3168, name: 'Kalispell (Vero Fiber — acq. MtnMax)', state: 'MT', provider: 'Vero', status: 'EXPANDING', passings: '1,500+ customers acquired', notes: 'Vero Fiber acquired Montana Digital (MtnMax) Mar 2025. 261 route mi. PE-backed consolidator.', source: 'PR Newswire', sourceUrl: 'https://www.prnewswire.com/news-releases/vero-fiber-acquires-montana-digital-expanding-fiber-to-the-premise-business-in-montana-302391884.html', isNew: true, overlap: true, size: 'normal' });

// ---- CONSOLIDATION CANDIDATES (from FCC BDC J25 filing, <50K BSLs in PNW states) ----
// fiberBSLs = unique location_ids with technology=50 (FTTP) from BDC CSV data
// totalBSLs = unique location_ids across all technologies from BDC CSV data
// techMix = actual technologies found in BDC filings (50=Fiber, 70=FWA, 10=DSL, 40=Cable)
// Data source: FCC BDC J25 filing (as-of 2025-06-30), downloaded Mar 2026
const CONSOLIDATION_CANDIDATES = [
    // === IDAHO ===
    { provider: "Silver Star Communications", providerId: "131190", states: ["ID"], totalBSLs: 45965, fiberBSLs: 42143, keyMarkets: "Idaho (Upper Snake River Valley)", techMix: "Fiber, DSL", ownership: "Private (limited public info)", strategicNotes: "Largest Idaho ILEC in our screen. 91.7% fiber — well-upgraded copper plant. Rural FTTP footprint across eastern ID." },
    { provider: "Project Mutual Telephone Cooperative", providerId: "131075", states: ["ID"], totalBSLs: 26059, fiberBSLs: 24003, keyMarkets: "Idaho (Rupert/Burley area)", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Member-owned cooperative", strategicNotes: "Member-owned coop. 92.1% fiber — highest fiber % among coops. Not a traditional acquisition target." },
    { provider: "AlwaysOn Internet", providerId: "300187", states: ["ID"], totalBSLs: 29083, fiberBSLs: 0, keyMarkets: "Idaho", techMix: "Fixed Wireless", ownership: "Private (limited public info)", strategicNotes: "BDC brand: AlwaysOn Internet. 100% fixed wireless, zero fiber. Limited acquisition value for FTTH strategy." },
    { provider: "MountainLink", providerId: "460052", states: ["ID"], totalBSLs: 15025, fiberBSLs: 0, keyMarkets: "Idaho", techMix: "Fixed Wireless", ownership: "Private LLC", strategicNotes: "BDC brand: MountainLink. 100% fixed wireless, zero fiber. No FTTP infrastructure." },
    // === MONTANA ===
    { provider: "CIS", providerId: "520061", states: ["MT"], totalBSLs: 46648, fiberBSLs: 0, keyMarkets: "Montana", techMix: "Fixed Wireless", ownership: "Private", strategicNotes: "Centric Integrated Systems. Despite large BSL count, 100% fixed wireless — zero fiber. Limited M&A value for FTTH." },
    { provider: "Montana Internet Corporation", providerId: "240109", states: ["MT"], totalBSLs: 41114, fiberBSLs: 1176, keyMarkets: "Montana", techMix: "Fiber, Fixed Wireless", ownership: "Private", strategicNotes: "Statewide MT ISP. Only 2.9% fiber (1,176 BSLs) — predominantly fixed wireless. Minimal fiber infrastructure." },
    { provider: "SkyNet Communications", providerId: "330070", states: ["MT"], totalBSLs: 39091, fiberBSLs: 0, keyMarkets: "Bozeman / Belgrade, MT", techMix: "Fixed Wireless", ownership: "Chris Johnson (founder-owned). 30+ years in business. Multi-100Gb backbone.", strategicNotes: "~77% Bozeman coverage BUT 100% fixed wireless in BDC — zero fiber BSLs. Gig speeds via FWA. Founder-owned 30+ yr business. Customer base valuable but no FTTP plant." },
    { provider: "Triangle Communications", providerId: "131367", states: ["MT"], totalBSLs: 25451, fiberBSLs: 20236, keyMarkets: "Central Montana (Havre HQ)", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Member-owned cooperative", strategicNotes: "Central MT coop. 79.5% fiber — strong FTTP upgrade. Not a traditional acquisition target but indicates healthy rural fiber build." },
    { provider: "Little Apple Technologies", providerId: "140055", states: ["MT"], totalBSLs: 36408, fiberBSLs: 0, keyMarkets: "Montana", techMix: "Fixed Wireless", ownership: "Private", strategicNotes: "BDC brand: Little Apple Technologies. 100% fixed wireless, zero fiber. No FTTP infrastructure." },
    { provider: "Nemont", providerId: "130890", states: ["MT"], totalBSLs: 22583, fiberBSLs: 11365, keyMarkets: "NE Montana (Scobey HQ)", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Member-owned coop (subs: Sagebrush Cellular, Project Telephone, Missouri Valley Comms; acq. Parsec Data 2018)", strategicNotes: "Large NE Montana coop with multiple subsidiaries. 50.3% fiber. Active acquirer (Parsec Data 2018). Not a traditional target." },
    { provider: "Grizzly Broadband", providerId: "380051", states: ["MT"], totalBSLs: 25706, fiberBSLs: 4872, keyMarkets: "Bitterroot Valley, MT", techMix: "Fiber, Fixed Wireless", ownership: "Electric cooperative (Ravalli Electric; acquired Grizzly Broadband 2021)", strategicNotes: "BDC brand: Grizzly Broadband. Only 19.0% fiber so far — FTTH build in progress in Bitterroot Valley. Remainder is FWA." },
    { provider: "KDS Fiber", providerId: "230025", states: ["MT"], totalBSLs: 28228, fiberBSLs: 1255, keyMarkets: "Great Falls / Cascade County, MT", techMix: "Fiber, Fixed Wireless", ownership: "Private LLC. Operating since 2007. 100% underground conduit construction.", strategicNotes: "Only 4.4% fiber (1,255 BSLs) despite 'KDS Fiber' branding — majority is FWA. Part of rare 3-way overbuild with Ziply + TDS in Great Falls." },
    { provider: "Mid-Rivers Communications", providerId: "130826", states: ["MT"], totalBSLs: 24066, fiberBSLs: 11585, keyMarkets: "Eastern Montana (Circle HQ)", techMix: "Fiber, Cable, DSL, Fixed Wireless", ownership: "Member-owned cooperative", strategicNotes: "Eastern MT coop. 48.1% fiber — upgrading legacy copper/cable. Multi-tech mix includes cable, DSL, FWA alongside fiber." },
    { provider: "3 Rivers Communications", providerId: "130002", states: ["MT"], totalBSLs: 21193, fiberBSLs: 17752, keyMarkets: "Fairfield, MT (north-central)", techMix: "Fiber, DSL", ownership: "Member-owned coop (16,000 members, 8,000 mi fiber, $350M invested, 99% FTTH)", strategicNotes: "Highly capitalized MT coop. 83.8% fiber in BDC — near complete FTTH upgrade. $350M invested. Largest rural FTTH in MT." },
    { provider: "Montana Opticom", providerId: "240061", states: ["MT"], totalBSLs: 20452, fiberBSLs: 20452, keyMarkets: "Bozeman / Gallatin Valley, MT", techMix: "Fiber", ownership: "Family-owned (founded 1999). Only locally-owned all-fiber telco in Gallatin Valley.", strategicNotes: "100% fiber — pure FTTH play. ~48% Bozeman coverage. Family business since 1999. $64.99-$99.99/mo. Ideal tuck-in for Bozeman market entry." },
    { provider: "Visionary Broadband", providerId: "300171", states: ["MT"], totalBSLs: 19486, fiberBSLs: 1764, keyMarkets: "Gillette, WY / MT", techMix: "Fiber, Fixed Wireless", ownership: "PE-backed: GTCR Fund XIII (67%); parent of Mammoth Networks + Skywerx. $100M+ committed to fiber builds.", strategicNotes: "Only 9.1% fiber — mostly FWA despite PE investment. GTCR (67%). Active fiber builder in WY/MT/CO. Potential acquirer or target." },
    { provider: "Access Montana", providerId: "131129", states: ["MT"], totalBSLs: 15840, fiberBSLs: 5204, keyMarkets: "Montana", techMix: "Fiber, Cable, DSL, Fixed Wireless", ownership: "Community cooperative", strategicNotes: "BDC brand: Access Montana. 32.9% fiber — multi-tech coop. Transitioning from legacy to fiber." },
    { provider: "Rocky Mountain Internet", providerId: "420110", states: ["MT"], totalBSLs: 19537, fiberBSLs: 0, keyMarkets: "Montana", techMix: "Fixed Wireless", ownership: "Private", strategicNotes: "100% fixed wireless in BDC — zero fiber. Limited acquisition value for FTTH strategy." },
    { provider: "Bullitt Communications", providerId: "300125", states: ["MT"], totalBSLs: 19263, fiberBSLs: 0, keyMarkets: "Montana", techMix: "Fixed Wireless", ownership: "Private (limited public info)", strategicNotes: "BDC brand: Bullitt Communications. 100% fixed wireless, zero fiber." },
    { provider: "MontanaSky Networks", providerId: "270063", states: ["MT"], totalBSLs: 16428, fiberBSLs: 3250, keyMarkets: "NW Montana (Kalispell, Whitefish, Bigfork, Columbia Falls, Libby, Troy)", techMix: "Fiber, Fixed Wireless", ownership: "Frederick Weber (founder-majority owner). Acquired MtnMax wireless customer base from Vero deal.", strategicNotes: "Largest WISP in NW Montana. Only 19.8% fiber. Acquired MtnMax wireless customers when Vero bought MtnMax fiber. Community-focused." },
    // === IDAHO + MONTANA ===
    { provider: "Blackfoot Communications", providerId: "130132", states: ["ID", "MT"], totalBSLs: 38729, fiberBSLs: 19919, keyMarkets: "Missoula, MT / SE Idaho", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Member-owned coop since 1954 (acquired Cutthroat 2012, Modwest; HQ Missoula)", strategicNotes: "Largest ID/MT coop. 51.4% fiber. DEFAULTED on 688 RDOF locations (May 2025) in Flathead/Gallatin/Missoula counties. Under pressure from TDS + Ziply overbuild." },
    // === OREGON ===
    { provider: "PrineTIME Internet Solutions", providerId: "330059", states: ["OR"], totalBSLs: 39401, fiberBSLs: 2957, keyMarkets: "Oregon", techMix: "Fiber, Fixed Wireless", ownership: "Private", strategicNotes: "Only 7.5% fiber — predominantly fixed wireless despite large BSL footprint. Private OR ISP." },
    { provider: "Webformix", providerId: "290118", states: ["OR"], totalBSLs: 36947, fiberBSLs: 0, keyMarkets: "Bend / Deschutes County, OR", techMix: "Fixed Wireless", ownership: "Eric Ozrelic (founder). Transmits from Awbrey Butte.", strategicNotes: "100% fixed wireless — confirmed zero fiber in BDC. Bend-based WISP (up to ~200 Mbps). Limited acquisition value for FTTH." },
    { provider: "Umpqua Broadband", providerId: "430089", states: ["OR"], totalBSLs: 30550, fiberBSLs: 0, keyMarkets: "Roseburg / Douglas County, OR", techMix: "Fixed Wireless", ownership: "Private", strategicNotes: "BDC brand: Umpqua Broadband. 100% fixed wireless, zero fiber. Formerly listed as Project A, Inc." },
    { provider: "Wtechlink", providerId: "400130", states: ["OR"], totalBSLs: 11802, fiberBSLs: 10524, keyMarkets: "Oregon", techMix: "Fiber, Fixed Wireless", ownership: "Private", strategicNotes: "89.2% fiber — strong FTTP footprint. Smaller than originally estimated but high-quality fiber network." },
    { provider: "Cal-Ore Communications", providerId: "160127", states: ["OR"], totalBSLs: 18160, fiberBSLs: 2760, keyMarkets: "Southern Oregon / N California (Cal-Ore subsidiary, founded 1950)", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Publicly traded (OTC Pink: LICT); Mario Gabelli/GAMCO ~24% ownership. 400+ mi fiber backbone.", strategicNotes: "ONLY publicly traded entity. Only 15.2% fiber. Limited FTTP speeds (65 Mbps) vs Hunter 2.5 Gbps — competitively vulnerable. 65+ year legacy telco." },
    { provider: "Beaver Creek Cooperative Telephone", providerId: "130104", states: ["OR"], totalBSLs: 8721, fiberBSLs: 2042, keyMarkets: "Oregon", techMix: "Fiber, Cable, DSL", ownership: "Member-owned cooperative", strategicNotes: "Oregon rural telephone cooperative. 23.4% fiber. Multi-tech legacy coop (cable + DSL + fiber)." },
    { provider: "Rally Networks", providerId: "130986", states: ["WA", "OR", "ID"], totalBSLs: 14936, fiberBSLs: 13201, keyMarkets: "WA, OR, ID (Rally Networks brand)", techMix: "Fiber, Cable, DSL", ownership: "Family of companies; includes NorthState, Home Telephone, Pine Telephone, Skyline Telecom", strategicNotes: "88.4% fiber — strong FTTP upgrade across multi-brand family. Spans 3 PNW states. Attractive consolidation play if all brands acquired together." },
    { provider: "Pioneer Telephone Cooperative", providerId: "131046", states: ["OR"], totalBSLs: 15405, fiberBSLs: 3540, keyMarkets: "Oregon", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Member-owned cooperative", strategicNotes: "Oregon rural telephone coop. 23.0% fiber — still largely copper/FWA." },
    // === WASHINGTON ===
    { provider: "Grant County PUD", providerId: "240110", states: ["WA"], totalBSLs: 38626, fiberBSLs: 38626, keyMarkets: "Grant County, WA", techMix: "Fiber", ownership: "Public utility district (wholesale-only by WA state law)", strategicNotes: "100% fiber. WA PUD — wholesale only by state law. NOT an acquisition target. Provides infrastructure for retail ISPs." },
    { provider: "Washington Broadband", providerId: "330082", states: ["WA"], totalBSLs: 32940, fiberBSLs: 2130, keyMarkets: "Washington", techMix: "Fiber, Cable, Fixed Wireless", ownership: "Private (sold fiber/cable assets to Spectrum; now wireless-only)", strategicNotes: "Only 6.5% fiber. Sold wireline to Spectrum. Now primarily wireless. Limited acquisition value." },
    { provider: "Chelan County PUD", providerId: "140020", states: ["WA"], totalBSLs: 29944, fiberBSLs: 29944, keyMarkets: "Chelan County (Wenatchee area)", techMix: "Fiber", ownership: "Public utility district (wholesale-only by WA state law)", strategicNotes: "100% fiber. WA PUD — wholesale only by state law. NOT an acquisition target." },
    { provider: "Lightcurve", providerId: "130335", states: ["WA"], totalBSLs: 26484, fiberBSLs: 3200, keyMarkets: "Kittitas, Thurston, Yakima Cos.", techMix: "Fiber, DSL", ownership: "Palisade Infrastructure (PE)", strategicNotes: "Only 12.1% fiber — legacy Consolidated Comms WA copper plant. Palisade PE-backed since May 2024 sale. FTTP upgrade opportunity. See also 130786 (Mashell/Rainier Connect) in Pierce Co." },
    { provider: "Swift-Stream Internet", providerId: "470055", states: ["WA"], totalBSLs: 24525, fiberBSLs: 164, keyMarkets: "Washington", techMix: "Fiber, Fixed Wireless", ownership: "Private", strategicNotes: "BDC brand: Swift-Stream Internet. Only 0.7% fiber (164 BSLs). Almost entirely fixed wireless." },
    { provider: "Douglas County PUD", providerId: "190303", states: ["WA"], totalBSLs: 15347, fiberBSLs: 0, keyMarkets: "Douglas County, WA", techMix: "DSL, Fixed Wireless", ownership: "Public utility district (wholesale-only by WA state law)", strategicNotes: "Zero fiber in BDC — DSL + FWA only. WA PUD — NOT an acquisition target." },
    // === MULTI-STATE ===
    { provider: "Native Network", providerId: "370071", states: ["WA", "OR"], totalBSLs: 45680, fiberBSLs: 29942, keyMarkets: "WA, OR (tribal broadband focus)", techMix: "Fiber, DSL, Fixed Wireless", ownership: "Andrew Metcalfe & Jenny Rickel (founders; sold NWT to Zayo 2008)", strategicNotes: "65.5% fiber. Tribal broadband focus. Founders previously sold NWT to Zayo (2008). Unique niche — social mission." },
    { provider: "Columbia iConnect", providerId: "270027", states: ["WA", "OR"], totalBSLs: 24152, fiberBSLs: 1246, keyMarkets: "WA, OR (Columbia REA territory)", techMix: "Fiber, Fixed Wireless", ownership: "Subsidiary of Columbia REA (electric cooperative)", strategicNotes: "Only 5.2% fiber. Fiber over electric utility infrastructure but mostly FWA." },
    { provider: "WIFIBER", providerId: "310017", states: ["WA", "ID"], totalBSLs: 25879, fiberBSLs: 4946, keyMarkets: "WA, ID", techMix: "Fiber, Fixed Wireless", ownership: "Private LLC", strategicNotes: "BDC brand: WIFIBER. 19.1% fiber. WA/ID cross-border provider. Mix of fiber + FWA." },
    { provider: "Inland Cellular", providerId: "410192", states: ["WA", "ID"], totalBSLs: 28183, fiberBSLs: 6171, keyMarkets: "Eastern WA, Northern ID", techMix: "Fiber, Cable, Fixed Wireless", ownership: "Private", strategicNotes: "21.9% fiber. Eastern WA/N Idaho wireless + fiber provider. Growing fiber footprint." },
    { provider: "360 Communications", providerId: "250003", states: ["OR", "WA"], totalBSLs: 15780, fiberBSLs: 0, keyMarkets: "OR, WA", techMix: "Fixed Wireless", ownership: "Private LLC", strategicNotes: "100% fixed wireless, zero fiber. Cross-border OR/WA provider. No FTTP infrastructure." },
    // === EMERALD BROADBAND (Eugene, OR — added per research, not in BDC) ===
    { provider: "Emerald Broadband", providerId: "TBD", states: ["OR"], totalBSLs: 2500, fiberBSLs: 2500, keyMarkets: "Eugene/Lane County, OR", techMix: "Fiber", ownership: "Public Benefit Corporation (founded 2016, 859 Willamette St Suite 310, Eugene)", strategicNotes: "Eugene-based PBC fiber ISP. Small but growing in Lane County. Mission-driven. Not in BDC — BSLs estimated." }
]

// ============================================================
// OREGON TOWNS — City-level mapping for OR BDC aggregation
// Maps cities to county FIPS codes and census tract prefixes
// County FIPS: state (41) + county (3 digits) = 5-digit prefix
// ============================================================
var OREGON_TOWNS = [
    // Portland Metro
    { name: "Portland", county: "Multnomah", countyFips: "41051", pop2020: 652503, estHH: 280000, areaSqMi: 133.5, lat: 45.5152, lng: -122.6784, metro: "Portland Metro", notes: "Largest city in OR; Ziply/AT&T (fka Lumen)/Astound all active" },
    { name: "Gresham", county: "Multnomah", countyFips: "41051", pop2020: 114247, estHH: 42000, areaSqMi: 23.5, lat: 45.4983, lng: -122.4310, metro: "Portland Metro", notes: "East Portland suburb; Ziply primary fiber" },
    { name: "Hillsboro", county: "Washington", countyFips: "41067", pop2020: 106447, estHH: 40000, areaSqMi: 25.8, lat: 45.5229, lng: -122.9898, metro: "Portland Metro", notes: "Intel HQ; dense tech corridor; Ziply + AT&T (fka Lumen)" },
    { name: "Beaverton", county: "Washington", countyFips: "41067", pop2020: 97494, estHH: 42000, areaSqMi: 19.6, lat: 45.4871, lng: -122.8037, metro: "Portland Metro", notes: "Nike HQ area; heavy Ziply fiber presence" },
    { name: "Tigard", county: "Washington", countyFips: "41067", pop2020: 54961, estHH: 23000, areaSqMi: 11.7, lat: 45.4312, lng: -122.7715, metro: "Portland Metro", notes: "SW Portland suburb" },
    { name: "Tualatin", county: "Washington", countyFips: "41067", pop2020: 28475, estHH: 11500, areaSqMi: 8.2, lat: 45.3840, lng: -122.7637, metro: "Portland Metro", notes: "Industrial/residential mix" },
    { name: "Sherwood", county: "Washington", countyFips: "41067", pop2020: 20450, estHH: 7600, areaSqMi: 4.3, lat: 45.3565, lng: -122.8404, metro: "Portland Metro", notes: "Growing suburb" },
    { name: "Lake Oswego", county: "Clackamas", countyFips: "41005", pop2020: 40162, estHH: 17500, areaSqMi: 10.7, lat: 45.4207, lng: -122.6706, metro: "Portland Metro", notes: "Affluent suburb; AT&T (fka Lumen) fiber active" },
    { name: "Oregon City", county: "Clackamas", countyFips: "41005", pop2020: 37339, estHH: 15000, areaSqMi: 10.0, lat: 45.3573, lng: -122.6068, metro: "Portland Metro", notes: "Clackamas county seat" },
    { name: "West Linn", county: "Clackamas", countyFips: "41005", pop2020: 26764, estHH: 10500, areaSqMi: 7.4, lat: 45.3651, lng: -122.6120, metro: "Portland Metro", notes: "Upscale residential" },
    { name: "Milwaukie", county: "Clackamas", countyFips: "41005", pop2020: 21119, estHH: 9200, areaSqMi: 4.8, lat: 45.4462, lng: -122.6393, metro: "Portland Metro", notes: "Inner SE suburb" },
    { name: "St. Helens", county: "Columbia", countyFips: "41009", pop2020: 14108, estHH: 5800, areaSqMi: 5.5, lat: 45.8640, lng: -122.8065, metro: "Portland Metro", notes: "Columbia county seat; Astound present" },
    // Willamette Valley
    { name: "Salem", county: "Marion", countyFips: "41047", pop2020: 175535, estHH: 68000, areaSqMi: 48.9, lat: 44.9429, lng: -123.0351, metro: "Willamette Valley", notes: "State capital; Hunter/Astound/AT&T (fka Lumen)/Ziply" },
    { name: "Keizer", county: "Marion", countyFips: "41047", pop2020: 39824, estHH: 15000, areaSqMi: 7.2, lat: 44.9901, lng: -123.0262, metro: "Willamette Valley", notes: "N Salem suburb; Astound cable" },
    { name: "Woodburn", county: "Marion", countyFips: "41047", pop2020: 27340, estHH: 9000, areaSqMi: 6.2, lat: 45.1437, lng: -122.8554, metro: "Willamette Valley", notes: "Ag town between Salem/Portland" },
    { name: "Eugene", county: "Lane", countyFips: "41039", pop2020: 176654, estHH: 75000, areaSqMi: 44.4, lat: 44.0521, lng: -123.0868, metro: "Willamette Valley", notes: "UO home; Hunter dominant + AT&T/Quantum + DFN + Ziply + Hyak + Emerald Broadband. Key consolidation market." },
    { name: "Springfield", county: "Lane", countyFips: "41039", pop2020: 62256, estHH: 25000, areaSqMi: 15.7, lat: 44.0462, lng: -122.9746, metro: "Willamette Valley", notes: "Eugene twin city; Hunter dominant + AT&T/Quantum + DFN active" },
    { name: "Corvallis", county: "Benton", countyFips: "41003", pop2020: 59922, estHH: 24000, areaSqMi: 14.1, lat: 44.5646, lng: -123.2620, metro: "Willamette Valley", notes: "OSU home; Ziply + AT&T (fka Lumen)" },
    { name: "Albany", county: "Linn", countyFips: "41043", pop2020: 56828, estHH: 22000, areaSqMi: 17.5, lat: 44.6365, lng: -123.1059, metro: "Willamette Valley", notes: "I-5 corridor; Ziply + Linn" },
    { name: "McMinnville", county: "Yamhill", countyFips: "41071", pop2020: 34690, estHH: 13500, areaSqMi: 10.6, lat: 45.2101, lng: -123.1968, metro: "Willamette Valley", notes: "Wine country; Ziply fiber + Hunter" },
    { name: "Newberg", county: "Yamhill", countyFips: "41071", pop2020: 24132, estHH: 9000, areaSqMi: 6.0, lat: 45.3001, lng: -122.9731, metro: "Willamette Valley", notes: "George Fox U; Ziply area" },
    { name: "Dallas", county: "Polk", countyFips: "41053", pop2020: 17100, estHH: 6800, areaSqMi: 5.2, lat: 44.9193, lng: -123.3172, metro: "Willamette Valley", notes: "Polk county seat" },
    { name: "Monmouth", county: "Polk", countyFips: "41053", pop2020: 11490, estHH: 4800, areaSqMi: 3.0, lat: 44.8484, lng: -123.2337, metro: "Willamette Valley", notes: "WOU campus town" },
    // Southern Oregon
    { name: "Medford", county: "Jackson", countyFips: "41029", pop2020: 85824, estHH: 35000, areaSqMi: 25.7, lat: 42.3265, lng: -122.8756, metro: "Southern Oregon", notes: "Rogue Valley hub; Hunter dominant fiber" },
    { name: "Ashland", county: "Jackson", countyFips: "41029", pop2020: 21360, estHH: 10000, areaSqMi: 6.6, lat: 42.1946, lng: -122.7095, metro: "Southern Oregon", notes: "SOU/Shakespeare; Hunter fiber" },
    { name: "Central Point", county: "Jackson", countyFips: "41029", pop2020: 19442, estHH: 7800, areaSqMi: 3.5, lat: 42.3779, lng: -122.9023, metro: "Southern Oregon", notes: "N Medford suburb; Hunter" },
    { name: "Grants Pass", county: "Josephine", countyFips: "41033", pop2020: 39087, estHH: 17000, areaSqMi: 10.9, lat: 42.4390, lng: -123.3284, metro: "Southern Oregon", notes: "Josephine county seat; Hunter + Ziply" },
    { name: "Klamath Falls", county: "Klamath", countyFips: "41035", pop2020: 22698, estHH: 9500, areaSqMi: 19.8, lat: 42.2249, lng: -121.7817, metro: "Southern Oregon", notes: "Hunter fiber buildout area" },
    { name: "Roseburg", county: "Douglas", countyFips: "41019", pop2020: 24820, estHH: 11000, areaSqMi: 10.0, lat: 43.2165, lng: -123.3417, metro: "Southern Oregon", notes: "I-5 corridor; Douglas county seat" },
    // Central Oregon
    { name: "Bend", county: "Deschutes", countyFips: "41017", pop2020: 99178, estHH: 42000, areaSqMi: 34.8, lat: 44.0582, lng: -121.3153, metro: "Central Oregon", notes: "Fastest-growing OR city; TDS/BendBroadband incumbent + Vero Fiber (acquired BendTel Dec 2025) actively building FTTH + AT&T (fka Lumen)" },
    { name: "Redmond", county: "Deschutes", countyFips: "41017", pop2020: 33274, estHH: 13000, areaSqMi: 18.6, lat: 44.2726, lng: -121.1740, metro: "Central Oregon", notes: "N of Bend; TDS area" },
    { name: "Prineville", county: "Crook", countyFips: "41013", pop2020: 11195, estHH: 4500, areaSqMi: 10.9, lat: 44.2999, lng: -120.7343, metro: "Central Oregon", notes: "Facebook/Apple data centers; TDS" },
    { name: "Madras", county: "Jefferson", countyFips: "41031", pop2020: 7409, estHH: 2800, areaSqMi: 8.2, lat: 44.6332, lng: -121.1295, metro: "Central Oregon", notes: "Jefferson county seat; TDS" },
    // Coast
    { name: "Newport", county: "Lincoln", countyFips: "41041", pop2020: 10532, estHH: 5200, areaSqMi: 10.0, lat: 44.6368, lng: -124.0535, metro: "Oregon Coast", notes: "Lincoln county seat; Astound cable" },
    { name: "Lincoln City", county: "Lincoln", countyFips: "41041", pop2020: 9919, estHH: 5500, areaSqMi: 5.7, lat: 44.9582, lng: -124.0179, metro: "Oregon Coast", notes: "Resort town; Astound" },
    { name: "Coos Bay", county: "Coos", countyFips: "41011", pop2020: 16630, estHH: 7500, areaSqMi: 10.6, lat: 43.3666, lng: -124.2179, metro: "Oregon Coast", notes: "Largest coastal city; Ziply" },
    { name: "North Bend", county: "Coos", countyFips: "41011", pop2020: 7739, estHH: 3600, areaSqMi: 3.9, lat: 43.4065, lng: -124.2240, metro: "Oregon Coast", notes: "Coos Bay twin city; Ziply" },
    { name: "Brookings", county: "Curry", countyFips: "41015", pop2020: 6904, estHH: 3500, areaSqMi: 3.4, lat: 42.0526, lng: -124.2840, metro: "Oregon Coast", notes: "S coast; Ziply fiber" },
    { name: "Astoria", county: "Clatsop", countyFips: "41007", pop2020: 10181, estHH: 4800, areaSqMi: 6.2, lat: 46.1879, lng: -123.8313, metro: "Oregon Coast", notes: "N coast; Hunter + Astound" },
    { name: "Tillamook", county: "Tillamook", countyFips: "41057", pop2020: 5290, estHH: 2400, areaSqMi: 1.7, lat: 45.4562, lng: -123.8426, metro: "Oregon Coast", notes: "Dairy country; Astound cable" },
    // Eastern Oregon
    { name: "La Grande", county: "Union", countyFips: "41061", pop2020: 13404, estHH: 5800, areaSqMi: 4.6, lat: 45.3246, lng: -118.0877, metro: "Eastern Oregon", notes: "EOU campus; Ziply fiber territory" },
    { name: "Baker City", county: "Baker", countyFips: "41001", pop2020: 10149, estHH: 4500, areaSqMi: 7.2, lat: 44.7749, lng: -117.8344, metro: "Eastern Oregon", notes: "Baker county seat; Ziply" },
    { name: "Pendleton", county: "Umatilla", countyFips: "41059", pop2020: 16644, estHH: 7000, areaSqMi: 10.5, lat: 45.6721, lng: -118.7886, metro: "Eastern Oregon", notes: "Round-Up city; AT&T (fka Lumen) territory" },
    { name: "Hermiston", county: "Umatilla", countyFips: "41059", pop2020: 19354, estHH: 7200, areaSqMi: 7.8, lat: 45.8404, lng: -119.2895, metro: "Eastern Oregon", notes: "Fastest-growing E OR city; AT&T (fka Lumen)" },
    { name: "The Dalles", county: "Wasco", countyFips: "41065", pop2020: 15340, estHH: 6200, areaSqMi: 6.7, lat: 45.5946, lng: -121.1787, metro: "Eastern Oregon", notes: "Google data center; AT&T (fka Lumen)" },
    { name: "Hood River", county: "Hood River", countyFips: "41027", pop2020: 8313, estHH: 3400, areaSqMi: 2.6, lat: 45.7054, lng: -121.5215, metro: "Eastern Oregon", notes: "Columbia Gorge; AT&T (fka Lumen)" },
];

// ---- ACQUISITION TARGETS — Deep profiles for Ziply/BCE tuck-in strategy ----
// BCE (Bell Canada) completed acquisition of Ziply Aug 1, 2025 for C$5.0B + ~C$2.6B net debt.
// Ziply now at ~1.4M fiber locations across WA, OR, ID, MT. BCE targeting 8M US fiber locations long-term.
// Sources: ziplyfiber.com/news/press-release/ziply-bce, searchlightcap.com
const ACQUISITION_TARGETS = [
    {
        name: "Lightcurve",
        fccId: "130786",
        legalEntity: "Mashell Telecom, Inc.",
        hq: "Eatonville, WA",
        owner: "Palisade Infrastructure (Australian infra fund, A$10B+ AUM)",
        ceo: "Anand Vadapalli (fmr CEO Alaska Communications)",
        totalPassings: 160000,
        fiberPct: 12.1,
        fiberBSLs: 3200,
        totalBDCBSLs: 26484,
        employees: "100-150",
        states: ["WA"],
        keyMarkets: "Pierce Co (Tacoma, Puyallup, Eatonville), Lewis Co (Centralia, Chehalis), Kittitas Co (Ellensburg), Thurston Co (Yelm), Yakima Co (Selah)",
        techMix: "HFC (cable) + FTTP + DSL. 20.7% of customers on DSL.",
        acquisitionHistory: "Rainier Connect (Nov 2023, undisclosed) + Consolidated Comms WA ($73M, May 2024)",
        impliedValuation: "$200-400M+ (full platform, 160K passings)",
        revenue: "~$20M+ (CCI WA assets alone had ~$20M revenue FY2023)",
        fiberUpgrades: "Ellensburg & Selah: copper-to-fiber completed 2025. Yelm: fiber 2026. Centralia/Chehalis: 1,000+ additional locations 2026. Kittitas Co: $3.3M ARPA-funded rural build.",
        acquisitionLikelihood: "LOW — Palisade just bought (2023-24), long-duration open-ended fund, CEO signaling growth",
        strategicFit: "HIGH — 160K WA passings, direct overlap in Pierce Co, copper network = Ziply fiber overbuild playbook",
        color: "#14B8A6",
        sources: [
            { name: "Consolidated WA sale closure", url: "https://www.businesswire.com/news/home/20240430603125/en/Consolidated-Communications-Closes-on-Sale-of-its-Washington-Assets" },
            { name: "Palisade acquires Rainier Connect", url: "https://www.prweb.com/releases/palisade-infrastructure-acquires-rainier-connect-and-announces-name-change-to-lightcurve-301981546.html" },
            { name: "Lightcurve fiber Ellensburg/Selah", url: "https://getlightcurve.com/lightcurve-brings-fast-and-reliable-fiber-internet-to-residents-in-ellensburg-and-selah-wa/" },
            { name: "South Sound Biz profile", url: "https://www.southsoundbiz.com/profiles/palisade-infrastructure-rainier-connect-lightcurve/article_5c7acd00-754a-11ef-9882-b749c69fb017.html" },
            { name: "Lightcurve Lewis Co expansion", url: "https://www.southsoundbiz.com/news/lightcurve-extends-high-speed-fiber-internet-in-lewis-county-elsewhere-plans-more-upgrades/article_da5adef8-6b4e-47e1-bc0a-f467d4b4bf77.html" }
        ]
    },
    {
        name: "Montana Opticom",
        fccId: "130569",
        legalEntity: "Montana Opticom LLC",
        hq: "Bozeman, MT",
        owner: "Ascent Data (Pittsburgh, PA — Jim Dolan family)",
        ceo: "Not publicly disclosed",
        totalPassings: 20452,
        fiberPct: 100,
        fiberBSLs: 20452,
        totalBDCBSLs: 20452,
        employees: "11-50",
        states: ["MT"],
        keyMarkets: "Bozeman, Belgrade, Gallatin Valley (~48% of Bozeman addresses covered)",
        techMix: "100% FTTP — clean fiber network",
        acquisitionHistory: "Founded 1999, fiber in Big Sky since 2005. Sold Big Sky facilities to 3 Rivers Communications. Received $64M federal broadband stimulus grant (2010).",
        impliedValuation: "~$30-50M est. (based on ~$3M revenue, 20K+ BSLs in high-growth market)",
        revenue: "~$3M (2025 estimate)",
        fiberUpgrades: "Already 100% fiber. Up to 1 Gbps symmetric. 5.0/5.0 customer satisfaction.",
        acquisitionLikelihood: "MEDIUM-HIGH — PE-owned (Ascent Data would sell at right price), clean 100% fiber asset",
        strategicFit: "MEDIUM-HIGH — Bozeman is fastest-growing MT metro, Ziply already in MT, 100% fiber = no upgrade needed",
        color: "#F59E0B",
        sources: [
            { name: "BroadbandNow profile", url: "https://broadbandnow.com/Montana-Opticom" },
            { name: "Calix partnership", url: "https://www.calix.com/press-release/2015/05/montana-opticom-and-calix-bring-gigabit-services-to-belgrade--mo.html" },
            { name: "Bozeman Daily Chronicle", url: "https://www.bozemandailychronicle.com/news/economy/montana-opticom-buyer-rejects-need-for-optic-cable-project/article_2308a622-5d47-11e3-bedf-0019bb2963f4.html" }
        ]
    },
    {
        name: "Rally Networks",
        fccId: "130986",
        legalEntity: "Oregon Telephone Company et al.",
        hq: "Baker City, OR",
        owner: "Family/Private (100+ year heritage, multi-company roll-up)",
        ceo: "Not publicly disclosed",
        totalPassings: null,
        fiberPct: 88.4,
        fiberBSLs: null,
        totalBDCBSLs: null,
        employees: "~26",
        states: ["OR", "WA", "ID"],
        keyMarkets: "Baker City, Mount Vernon, John Day (OR); 62 cities across 9 states (incl. MO, OK, AR)",
        techMix: "88.4% Fiber, balance DSL/cable. High fiber penetration for rural ILEC.",
        acquisitionHistory: "Roll-up of Oregon Telephone Co (est. 1914), NorthState Telephone, Home Telephone, Pine Telephone, Skyline Telecom",
        impliedValuation: "Unknown — small, multi-state, family-owned",
        revenue: "Not publicly disclosed",
        fiberUpgrades: "Already 88.4% fiber — mostly upgraded.",
        acquisitionLikelihood: "MEDIUM — family succession could trigger sale, but deep community roots",
        strategicFit: "MEDIUM — PNW assets valuable but company spans 6 states; would need PNW-only carve-out",
        color: "#8B5CF6",
        sources: [
            { name: "Rally Networks About", url: "https://rallynet.us/about-us/" },
            { name: "Rally Networks regions", url: "https://rallynet.us/regions-we-serve/oregon-washington-and-idaho/" },
            { name: "BroadbandNow profile", url: "https://broadbandnow.com/rally-networks" }
        ]
    },
    {
        name: "Wtechlink",
        fccId: "400130",
        legalEntity: "Wtechlink LLC",
        hq: "Pendleton, OR",
        owner: "Founder-owned (Byron Wysocki + Jordan McDonald)",
        ceo: "Byron Wysocki (founder, est. 2001)",
        totalPassings: null,
        fiberPct: 89.2,
        fiberBSLs: null,
        totalBDCBSLs: null,
        employees: "2-10",
        states: ["OR"],
        keyMarkets: "Pendleton, Milton-Freewater, Cayuse, Pilot Rock, Adams, Athena, Helix, Umatilla Indian Reservation",
        techMix: "89.2% Fiber, balance fixed wireless. Up to 10 Gbps symmetric fiber.",
        acquisitionHistory: "Founded 2001 as wireless ISP, expanded to fiber. No prior acquisitions.",
        impliedValuation: "Micro — very small operation",
        revenue: "Not publicly disclosed",
        fiberUpgrades: "Already 89.2% fiber. 5.0/5.0 customer satisfaction.",
        acquisitionLikelihood: "MEDIUM — small founder exit possible, but micro-scale",
        strategicFit: "LOW — micro-acquisition, rural eastern OR, but adjacent to Ziply WA footprint",
        color: "#06B6D4",
        sources: [
            { name: "Wtechlink website", url: "https://www.wtechlink.com/" },
            { name: "East Oregonian profile", url: "https://www.eastoregonian.com/news/local/connecting-pendleton/article_550a4ac7-658b-5caf-9a98-45b95f2b0972.html" },
            { name: "ISP Reports", url: "https://ispreports.org/internet-service-providers/wtechlink-availability/" }
        ]
    },
    {
        name: "Emerald Broadband",
        fccId: null,
        legalEntity: "Emerald Broadband LLC (Public Benefit Corporation)",
        hq: "Eugene, OR",
        owner: "Founder-owned PBC (started in a garage, River Road area, 2016)",
        ceo: "Not publicly disclosed",
        totalPassings: null,
        fiberPct: 100,
        fiberBSLs: null,
        totalBDCBSLs: null,
        employees: "<20 est.",
        states: ["OR"],
        keyMarkets: "Eugene, Springfield, Westfir, Oakridge (Lane County)",
        techMix: "Active Ethernet — dedicated fiber line per premises. Acquired coax company in 2019, overbuilding with fiber.",
        acquisitionHistory: "Founded 2016, acquired coax company 2019. Mission: bridge digital divide in Lane County.",
        impliedValuation: "Very small — mission-driven PBC",
        revenue: "Not publicly disclosed",
        fiberUpgrades: "All new builds are 100% fiber (Active Ethernet).",
        acquisitionLikelihood: "LOW — Public Benefit Corp structure, mission-driven, likely ideologically opposed to selling to large corp",
        strategicFit: "LOW — very small, but Eugene/Springfield market is strategically interesting if Lane Co matters",
        color: "#8B5CF6",
        sources: [
            { name: "Emerald Broadband website", url: "https://www.emeraldbroadband.com/" },
            { name: "3-GIS customer spotlight", url: "https://info.3-gis.com/emerald-broadband-customer-spotlight" },
            { name: "Lookout Eugene-Springfield", url: "https://lookouteugene-springfield.com/story/latest-news/2025/04/09/emerald-broadband-connecting-lane-county-one-fiber-line-at-a-time/" }
        ]
    }
];

// ---- TOP EXPANSION OPPORTUNITIES — Best towns/clusters for Ziply greenfield or overbuild ----
// Ranked by housing density (HU/mi²), competitive landscape, and strategic adjacency.
// Density tiers: Urban (>2K HU/mi²), Suburban (500-2K), Rural (<500).
// Build cost: Urban $700-775, Suburban $825-900, Rural $1,100-1,500+
// Sources: Census ACS 2020 5-Year Estimates, FCC BDC J25, company announcements.
const EXPANSION_OPPORTUNITIES = [
    // === TIER 1: HIGH-DENSITY, WEAK INCUMBENT (best ROI) ===
    { town: "Central Point", state: "OR", county: "Jackson", estHH: 7800, areaSqMi: 3.5, density: 2229, tier: "Urban", incumbent: "AT&T/Quantum (weak)", ziplyPresent: false, buildCost: 750, rationale: "Highest density in S OR. AT&T/Quantum is weak incumbent with limited fiber. Adjacent to Medford — cluster opportunity.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Keizer", state: "OR", county: "Marion", estHH: 15200, areaSqMi: 7.3, density: 2083, tier: "Urban", incumbent: "Comcast cable only", ziplyPresent: false, buildCost: 740, rationale: "Dense Salem suburb. No fiber incumbent. Pure cable market = greenfield fiber opportunity.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Tigard", state: "OR", county: "Washington", estHH: 22800, areaSqMi: 11.6, density: 1966, tier: "Suburban", incumbent: "Ziply (partial) + Comcast", ziplyPresent: true, buildCost: 850, rationale: "Large Portland suburb. Ziply has partial coverage — fill-in build to complete footprint. High-value HH.", source: "Census ACS 2020 / FCC BDC", sourceUrl: "https://broadbandmap.fcc.gov/" },
    { town: "Springfield", state: "OR", county: "Lane", estHH: 24600, areaSqMi: 15.4, density: 1597, tier: "Suburban", incumbent: "AT&T/Quantum + Emerald (small)", ziplyPresent: false, buildCost: 850, rationale: "Large Lane Co city. AT&T legacy copper. Emerald too small to defend market. 24K+ HH greenfield.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Meridian", state: "ID", county: "Ada", estHH: 42000, areaSqMi: 32.8, density: 1280, tier: "Suburban", incumbent: "Lumen/Brightspeed (weak copper)", ziplyPresent: false, buildCost: 860, rationale: "Fastest-growing Idaho city. 42K HH. Lumen/Brightspeed weak. Huge greenfield opportunity adjacent to Boise.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Nampa", state: "ID", county: "Canyon", estHH: 35000, areaSqMi: 31.2, density: 1122, tier: "Suburban", incumbent: "Lumen/Brightspeed (weak copper)", ziplyPresent: false, buildCost: 870, rationale: "2nd largest Boise metro city. 35K HH with weak incumbent. Cable One/Sparklight cable but no fiber.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Medford", state: "OR", county: "Jackson", estHH: 34500, areaSqMi: 25.3, density: 1363, tier: "Suburban", incumbent: "AT&T/Quantum (limited fiber) + Hunter", ziplyPresent: false, buildCost: 850, rationale: "Largest S OR city. Hunter expanding but small. AT&T legacy. Cluster with Central Point + Ashland.", source: "Census ACS 2020 / FCC BDC", sourceUrl: "https://broadbandmap.fcc.gov/" },
    { town: "Bend", state: "OR", county: "Deschutes", estHH: 43000, areaSqMi: 35.6, density: 1208, tier: "Suburban", incumbent: "TDS/BendBroadband + Vero Fiber", ziplyPresent: false, buildCost: 870, rationale: "Fastest-growing OR city. 43K HH. TDS incumbent but Vero entering. Competitive but high-value.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },

    // === TIER 2: MEDIUM-DENSITY, STRATEGIC ADJACENCY ===
    { town: "Coeur d'Alene", state: "ID", county: "Kootenai", estHH: 22000, areaSqMi: 17.1, density: 1287, tier: "Suburban", incumbent: "TDS (active fiber build)", ziplyPresent: false, buildCost: 860, rationale: "Fastest-growing N Idaho metro. Adjacent to Spokane (Ziply stronghold). TDS building 22K+, but Ziply could leverage Spokane infrastructure.", source: "Census ACS 2020 / TDS", sourceUrl: "https://tdsfiber.com/build/coeur-d-alene-id/" },
    { town: "Bozeman", state: "MT", county: "Gallatin", estHH: 24000, areaSqMi: 20.6, density: 1165, tier: "Suburban", incumbent: "Montana Opticom (48%) + TDS", ziplyPresent: true, buildCost: 870, rationale: "Fastest-growing MT metro. Montana Opticom (acq target) covers 48%. Ziply already building. Complete the market.", source: "Census ACS 2020 / FCC BDC", sourceUrl: "https://broadbandmap.fcc.gov/" },
    { town: "Salem", state: "OR", county: "Marion", estHH: 68000, areaSqMi: 48.5, density: 1402, tier: "Suburban", incumbent: "Comcast + AT&T/Quantum (partial)", ziplyPresent: false, buildCost: 855, rationale: "Oregon state capital. 68K HH — massive greenfield. Combine with Keizer for 83K HH cluster.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Missoula", state: "MT", county: "Missoula", estHH: 32000, areaSqMi: 28.3, density: 1131, tier: "Suburban", incumbent: "Lumen/Brightspeed (copper) + TDS (partial)", ziplyPresent: true, buildCost: 870, rationale: "2nd largest MT city. University town = high broadband demand. Ziply already active. Expand footprint.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Olympia", state: "WA", county: "Thurston", estHH: 24000, areaSqMi: 19.7, density: 1218, tier: "Suburban", incumbent: "Lightcurve (expanding) + Comcast", ziplyPresent: false, buildCost: 860, rationale: "WA state capital. Only ~2.9% fiber availability. Lightcurve expanding but slow. Massive underserved market.", source: "BroadbandNow", sourceUrl: "https://broadbandnow.com/Washington/Olympia" },
    { town: "Eugene", state: "OR", county: "Lane", estHH: 72000, areaSqMi: 44.4, density: 1622, tier: "Suburban", incumbent: "Comcast + Emerald (tiny) + AT&T legacy", ziplyPresent: false, buildCost: 850, rationale: "3rd largest OR city. 72K HH. No serious fiber competitor. Combine with Springfield for 96K HH cluster.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },

    // === TIER 3: CLUSTER PLAYS — Adjacent towns that combine into large markets ===
    { town: "Billings", state: "MT", county: "Yellowstone", estHH: 48000, areaSqMi: 46.3, density: 1037, tier: "Suburban", incumbent: "TDS (50K+ fiber build) + Ziply", ziplyPresent: true, buildCost: 880, rationale: "Largest MT city. TDS and Ziply both building actively. High competition but largest market in MT. Ziply targeting 18-20K addresses.", source: "Yahoo Finance / TDS", sourceUrl: "https://finance.yahoo.com/news/ziply-fiber-aggressively-expanding-montana-183500923.html" },
    { town: "Idaho Falls", state: "ID", county: "Bonneville", estHH: 26000, areaSqMi: 23.5, density: 1106, tier: "Suburban", incumbent: "Lumen/Brightspeed (weak)", ziplyPresent: false, buildCost: 880, rationale: "Eastern ID's largest city. INL (Idaho National Lab) nearby = high-income HH. Weak copper incumbent.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Corvallis", state: "OR", county: "Benton", estHH: 24000, areaSqMi: 14.3, density: 1678, tier: "Suburban", incumbent: "Comcast + AT&T legacy", ziplyPresent: false, buildCost: 850, rationale: "Oregon State University. High-density college town. No fiber competitor. 24K HH.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Twin Falls", state: "ID", county: "Twin Falls", estHH: 19000, areaSqMi: 20.1, density: 945, tier: "Suburban", incumbent: "Sparklight (cable) + Lumen (copper)", ziplyPresent: false, buildCost: 890, rationale: "Magic Valley regional center. Sparklight cable only. No fiber incumbent = greenfield.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Pocatello", state: "ID", county: "Bannock", estHH: 22000, areaSqMi: 25.6, density: 859, tier: "Suburban", incumbent: "Lumen/Brightspeed (copper)", ziplyPresent: false, buildCost: 885, rationale: "Idaho State University town. Weak copper incumbent. Cluster with Idaho Falls for 48K HH E Idaho market.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Great Falls", state: "MT", county: "Cascade", estHH: 28000, areaSqMi: 22.6, density: 1239, tier: "Suburban", incumbent: "Lumen/Brightspeed (copper)", ziplyPresent: true, buildCost: 865, rationale: "3rd largest MT city. Malmstrom AFB = stable population. Ziply targeting ~10K addresses. Fill in remaining.", source: "Yahoo Finance", sourceUrl: "https://finance.yahoo.com/news/ziply-fiber-aggressively-expanding-montana-183500923.html" },
    { town: "Helena", state: "MT", county: "Lewis and Clark", estHH: 16000, areaSqMi: 15.8, density: 1013, tier: "Suburban", incumbent: "Lumen/Brightspeed (copper)", ziplyPresent: false, buildCost: 880, rationale: "MT state capital. 16K HH. Weak incumbent. Government/institutional anchor.", source: "Census ACS 2020", sourceUrl: "https://data.census.gov/" },
    { town: "Kalispell", state: "MT", county: "Flathead", estHH: 12000, areaSqMi: 11.5, density: 1043, tier: "Suburban", incumbent: "Vero Fiber (acquired MtnMax)", ziplyPresent: false, buildCost: 890, rationale: "Flathead Valley gateway. Vero Fiber entering via MtnMax acquisition. Growing tourism/retirement market.", source: "Census ACS 2020 / PR Newswire", sourceUrl: "https://www.prnewswire.com/news-releases/vero-fiber-acquires-montana-digital-expanding-fiber-to-the-premise-business-in-montana-302391884.html" }
];
