// ============================================================
// CALCULATION & METHODOLOGY ENGINE — Pacific Northwest
// Fiber competitive map calculation helpers, BDC overlap
// analysis, popup builders, and consolidation metrics.
//
// Adapted from the Northeast calculations.js pattern for the
// PNW four-state footprint (WA, OR, ID, MT).
// ============================================================

// ============================================================
// 1. computeBDCOverlaps(bdcLayers, options)
//    Compute fiber-on-fiber overlap from BDC GeoJSON data.
//    Ziply is treated as the primary entity (analogous to
//    Verizon/Frontier merged entity in Northeast).
// ============================================================

/**
 * Compute BDC fiber overlap across all providers in the PNW footprint.
 *
 * @param {Object} bdcLayers - Keyed object of provider BDC GeoJSON data, e.g.
 *   { ziply: ZIPLY_BDC_COVERAGE, lumen: LUMEN_BDC_COVERAGE, hunter: ..., ... }
 *   Each value is a GeoJSON FeatureCollection with features containing
 *   properties: { id, bsls, state, county, hu100, pop100, areaLandSqKm, blocks, ... }
 *
 * @param {Object} [options]
 * @param {number} [options.minBSLThreshold=10] - Minimum BSL count to filter noise
 *
 * @returns {Object} Aggregated overlap results stored on window.BDC_OVERLAPS
 */
function computeBDCOverlaps(bdcLayers, options) {
    var opts = options || {};
    var minBSL = (opts.minBSLThreshold !== undefined) ? opts.minBSLThreshold : 10;

    // If called with null/undefined bdcLayers, attempt to use previously stashed data
    if (!bdcLayers && window._pnwBDCRawLayers) {
        bdcLayers = window._pnwBDCRawLayers;
    }
    if (bdcLayers) {
        window._pnwBDCRawLayers = bdcLayers;
    }
    if (!bdcLayers) {
        console.warn('[Overlap] No BDC data provided and none stashed.');
        return null;
    }

    // ----------------------------------------------------------
    // Build dataset list — each provider maps to a competitive entity.
    // Ziply is the primary entity. If a "ziply_legacy" or similar
    // key appears, merge it under the ziply entity (like VZ+Frontier
    // in the NE map).
    // ----------------------------------------------------------
    var allDatasets = [];
    var providerKeys = Object.keys(bdcLayers);

    for (var k = 0; k < providerKeys.length; k++) {
        var key = providerKeys[k];
        var raw = bdcLayers[key];
        if (!raw || !raw.features) continue;

        // Determine competitive entity — merge Ziply variants under one entity
        var entity = key;
        var label = key;

        switch (key) {
            case 'ziply':
                entity = 'ziply';
                label = 'Ziply Fiber';
                break;
            case 'ziply_legacy':
            case 'ziply_frontier':
                entity = 'ziply';
                label = 'Ziply (legacy)';
                break;
            case 'lumen':
            case 'centurylink':
                entity = 'lumen';
                label = 'Lumen/CenturyLink';
                break;
            case 'hunter':
                entity = 'hunter';
                label = 'Hunter Communications';
                break;
            case 'quantum':
            case 'att':
                entity = 'quantum';
                label = 'Quantum Fiber/AT&T';
                break;
            case 'ezee':
                entity = 'ezee';
                label = 'Ezee Fiber';
                break;
            case 'lightcurve':
                entity = 'lightcurve';
                label = 'Lightcurve';
                break;
            case 'astound':
            case 'wave':
                entity = 'astound';
                label = 'Astound/Wave';
                break;
            case 'fatbeam':
                entity = 'fatbeam';
                label = 'Fat Beam';
                break;
            case 'emerald':
                entity = 'emerald';
                label = 'Emerald Broadband';
                break;
            case 'charter':
            case 'spectrum':
                entity = 'charter';
                label = 'Charter/Spectrum';
                break;
            case 'comcast':
            case 'xfinity':
                entity = 'comcast';
                label = 'Comcast/Xfinity';
                break;
            case 'fidium':
            case 'consolidated':
                entity = 'fidium';
                label = 'Fidium/Consolidated';
                break;
            default:
                entity = key;
                label = key;
        }

        allDatasets.push({ entity: entity, label: label, sourceKey: key, features: raw.features });
    }

    console.log('[Overlap] Computing PNW overlaps with minBSL=' + minBSL +
        ', datasets: ' + allDatasets.map(function(d) { return d.label + '(' + d.features.length + ')'; }).join(', '));

    // ----------------------------------------------------------
    // Build block group index: GEOID -> { providers, geometry, state, county, ... }
    // ----------------------------------------------------------
    var bgIndex = {};
    var totalFeatures = 0;
    var skippedBelowMin = 0;

    for (var d = 0; d < allDatasets.length; d++) {
        var ds = allDatasets[d];
        totalFeatures += ds.features.length;

        for (var i = 0; i < ds.features.length; i++) {
            var f = ds.features[i];
            var bsls = f.properties.bsls || 0;

            // Skip features below the minimum BSL threshold
            if (bsls < minBSL) {
                skippedBelowMin++;
                continue;
            }

            var id = f.properties.id;
            if (!bgIndex[id]) {
                bgIndex[id] = {
                    providers: {},
                    geometry: f.geometry,
                    state: f.properties.state,
                    county: f.properties.county,
                    hu100: f.properties.hu100 || 0,
                    pop100: f.properties.pop100 || 0,
                    areaLandSqKm: f.properties.areaLandSqKm || 0,
                };
            }

            var bg = bgIndex[id];
            if (!bg.providers[ds.entity]) {
                bg.providers[ds.entity] = { bsls: bsls, label: ds.label };
            } else {
                // Same entity (e.g. ziply + ziply_legacy both map to 'ziply')
                if (bsls > bg.providers[ds.entity].bsls) {
                    bg.providers[ds.entity].bsls = bsls;
                }
                // Update label to indicate merged data
                if (ds.entity === 'ziply') {
                    bg.providers[ds.entity].label = 'Ziply Fiber';
                }
            }
        }
    }

    console.log('[Overlap] Indexed ' + Object.keys(bgIndex).length + ' block groups (' +
        totalFeatures + ' total features, ' + skippedBelowMin + ' skipped below ' + minBSL + ' BSL threshold)');

    // ----------------------------------------------------------
    // Filter to block groups with 2+ distinct competitive entities
    // ----------------------------------------------------------
    var overlapBGs = {};
    var bgIds = Object.keys(bgIndex);
    for (var b = 0; b < bgIds.length; b++) {
        var bid = bgIds[b];
        var provKeys2 = Object.keys(bgIndex[bid].providers);
        if (provKeys2.length >= 2) {
            overlapBGs[bid] = bgIndex[bid];
        }
    }

    console.log('[Overlap] Found ' + Object.keys(overlapBGs).length +
        ' overlap block groups (2+ entities, each >=' + minBSL + ' BSLs)');

    // ----------------------------------------------------------
    // Aggregation: byState
    // ----------------------------------------------------------
    var byState = {};
    var overlapIds = Object.keys(overlapBGs);
    for (var s = 0; s < overlapIds.length; s++) {
        var oBg = overlapBGs[overlapIds[s]];
        var st = oBg.state;
        if (!byState[st]) byState[st] = { bgs: 0, bsls: 0, hu: 0, providers: {}, counties: {} };
        byState[st].bgs++;
        byState[st].hu += oBg.hu100;
        var pks = Object.keys(oBg.providers);
        for (var p = 0; p < pks.length; p++) {
            byState[st].bsls += oBg.providers[pks[p]].bsls;
            byState[st].providers[pks[p]] = true;
        }
        byState[st].counties[oBg.county] = true;
    }

    // ----------------------------------------------------------
    // Aggregation: byCounty
    // ----------------------------------------------------------
    var byCounty = {};
    for (var c = 0; c < overlapIds.length; c++) {
        var cBg = overlapBGs[overlapIds[c]];
        var cKey = cBg.state + '-' + cBg.county;
        if (!byCounty[cKey]) byCounty[cKey] = { state: cBg.state, county: cBg.county, bgs: 0, bsls: 0, hu: 0, providers: {} };
        byCounty[cKey].bgs++;
        byCounty[cKey].hu += cBg.hu100;
        var cpks = Object.keys(cBg.providers);
        for (var cp = 0; cp < cpks.length; cp++) {
            byCounty[cKey].bsls += cBg.providers[cpks[cp]].bsls;
            byCounty[cKey].providers[cpks[cp]] = true;
        }
    }

    // ----------------------------------------------------------
    // Aggregation: byPair (every pairwise combination of entities)
    // ----------------------------------------------------------
    var byPair = {};
    for (var pp = 0; pp < overlapIds.length; pp++) {
        var pBg = overlapBGs[overlapIds[pp]];
        var entities = Object.keys(pBg.providers).sort();
        for (var ei = 0; ei < entities.length; ei++) {
            for (var ej = ei + 1; ej < entities.length; ej++) {
                var pairKey = entities[ei] + '+' + entities[ej];
                if (!byPair[pairKey]) byPair[pairKey] = { bgs: 0, bsls: 0, states: {} };
                byPair[pairKey].bgs++;
                var bslsA = pBg.providers[entities[ei]].bsls;
                var bslsB = pBg.providers[entities[ej]].bsls;
                byPair[pairKey].bsls += Math.max(bslsA, bslsB);
                byPair[pairKey].states[pBg.state] = true;
            }
        }
    }

    // ----------------------------------------------------------
    // Aggregation: byProviderExposure (total vs overlap footprint per entity)
    // ----------------------------------------------------------
    var byProviderExposure = {};
    // Initialize known PNW entities
    var knownEntities = ['ziply', 'lumen', 'hunter', 'quantum', 'ezee', 'lightcurve', 'astound', 'fatbeam', 'emerald', 'charter', 'comcast', 'fidium'];
    for (var ke = 0; ke < knownEntities.length; ke++) {
        byProviderExposure[knownEntities[ke]] = { overlapBGs: 0, overlapBSLs: 0, totalBGs: 0, totalBSLs: 0 };
    }

    var allBgIds = Object.keys(bgIndex);
    for (var x = 0; x < allBgIds.length; x++) {
        var xBg = bgIndex[allBgIds[x]];
        var xProv = Object.keys(xBg.providers);
        var isOverlap = xProv.length >= 2;
        for (var xp = 0; xp < xProv.length; xp++) {
            var ent = xProv[xp];
            if (!byProviderExposure[ent]) {
                byProviderExposure[ent] = { overlapBGs: 0, overlapBSLs: 0, totalBGs: 0, totalBSLs: 0 };
            }
            byProviderExposure[ent].totalBGs++;
            byProviderExposure[ent].totalBSLs += xBg.providers[ent].bsls;
            if (isOverlap) {
                byProviderExposure[ent].overlapBGs++;
                byProviderExposure[ent].overlapBSLs += xBg.providers[ent].bsls;
            }
        }
    }

    // ----------------------------------------------------------
    // Store results on window for global access
    // ----------------------------------------------------------
    window.BDC_OVERLAPS = {
        bgIndex: bgIndex,
        overlapBGs: overlapBGs,
        byState: byState,
        byCounty: byCounty,
        byPair: byPair,
        byProviderExposure: byProviderExposure,
        totalOverlapBGs: Object.keys(overlapBGs).length,
        minBSLThreshold: minBSL,
        computed: true,
    };

    console.log('[Overlap] DONE: ' + window.BDC_OVERLAPS.totalOverlapBGs +
        ' overlap block groups, ' + Object.keys(byPair).length + ' pairs, ' +
        Object.keys(byState).length + ' states');

    return window.BDC_OVERLAPS;
}


// ============================================================
// 2. buildTractPopupContent(tract)
//    Build HTML popup content for a census tract polygon click.
// ============================================================

/**
 * Build an HTML popup for a census tract on the PNW competitive map.
 *
 * @param {Object} tract - Tract object with properties:
 *   { id, name, housingUnits, density, costPerPassing, provider,
 *     permitStatus, sourceType, sourceUrl, sourceText, notes,
 *     overlap, isNew, fccProviders, announcedPassings, county, state }
 * @returns {string} HTML string for Leaflet popup
 */
function buildTractPopupContent(tract) {
    var county = tract.county || '';
    var state = tract.state || '';
    var locationLabel = (county && state) ? county + ', ' + state : (state || '');

    var huDisplay = formatNumber(tract.housingUnits || 0);
    var densityLabel = tract.density || getDensityClass(tract.huPerSqMi || 0);
    var costPerPassing = tract.costPerPassing || getCostPerPassing(densityLabel);
    var estBuildCost = estimateBuildCost(tract.housingUnits || 0, densityLabel);

    // Provider color mapping for PNW providers
    var providerColors = {
        'Ziply': '#16A34A',
        'Ziply Fiber': '#16A34A',
        'Hunter': '#DC2626',
        'Hunter Communications': '#DC2626',
        'Quantum': '#0EA5E9',
        'Quantum/AT&T': '#0EA5E9',
        'Ezee': '#EC4899',
        'Ezee Fiber': '#EC4899',
        'Lightcurve': '#14B8A6',
        'Astound': '#F97316',
        'Astound/Wave': '#F97316',
        'Fat Beam': '#F59E0B',
        'Fatbeam': '#F59E0B',
        'Emerald': '#8B5CF6',
        'Emerald Broadband': '#8B5CF6',
        'Lumen': '#6366F1',
        'CenturyLink': '#6366F1',
        'Contested': '#7C3AED',
    };
    var provColor = providerColors[tract.provider] || '#6B7280';

    // Permit status color mapping
    var statusColors = {
        'Active Service': { bg: '#DCFCE7', text: '#15803D' },
        'Commercial Active': { bg: '#DBEAFE', text: '#1D4ED8' },
        'Active Build': { bg: '#FEF3C7', text: '#92400E' },
        'Expansion Target': { bg: '#FEF3C7', text: '#92400E' },
        'PBC Build': { bg: '#F3E8FF', text: '#6B21A8' },
        'Permit Filed': { bg: '#DBEAFE', text: '#1D4ED8' },
        'Grant Build': { bg: '#CCFBF1', text: '#0F766E' },
        'Multiple Interest': { bg: '#FEE2E2', text: '#991B1B' },
        'Planned': { bg: '#F3E8FF', text: '#6B21A8' },
        'Construction': { bg: '#FEF3C7', text: '#92400E' },
        'Announced': { bg: '#FEF3C7', text: '#92400E' },
    };
    var sc = statusColors[tract.permitStatus] || { bg: '#F3F4F6', text: '#374151' };

    var html = '<div style="font-family:Inter,-apple-system,sans-serif;min-width:300px;">';

    // Header: Tract name and ID
    html += '<div style="font-size:11px;color:#64748B;margin-bottom:4px;">Census Tract ' + tract.id + (locationLabel ? ' | ' + locationLabel : '') + '</div>';
    html += '<div style="font-weight:700;font-size:15px;color:#0F172A;margin-bottom:6px;">' + (tract.name || 'Tract ' + tract.id) + '</div>';

    // Provider and status badges
    html += '<div style="margin-bottom:8px;">';
    html += '<span style="font-weight:600;font-size:12px;color:' + provColor + ';">' + (tract.provider || 'Unknown') + '</span>';
    html += ' <span style="display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;margin-left:6px;background:' + sc.bg + ';color:' + sc.text + ';">' + (tract.permitStatus || 'Unknown') + '</span>';
    html += '</div>';

    // Housing units, density, and build cost stats
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">';
    html += '<div style="background:#F1F5F9;padding:8px;border-radius:6px;">';
    html += '<div style="font-size:10px;color:#64748B;">Housing Units</div>';
    html += '<div style="font-weight:700;font-size:18px;color:#0F172A;">' + huDisplay + '</div>';
    html += '<div style="font-size:9px;color:#94A3B8;">' + densityLabel + '</div>';
    html += '</div>';
    html += '<div style="background:#F1F5F9;padding:8px;border-radius:6px;">';
    html += '<div style="font-size:10px;color:#64748B;">Est. Build Cost</div>';
    html += '<div style="font-weight:700;font-size:18px;color:#0F172A;">' + formatCurrency(estBuildCost) + '</div>';
    html += '<div style="font-size:9px;color:#94A3B8;">' + formatCurrency(costPerPassing) + '/passing</div>';
    html += '</div>';
    html += '</div>';

    // Announced passings (if any)
    if (tract.announcedPassings) {
        html += '<div style="background:#ECFDF5;padding:6px 8px;border-radius:6px;border-left:3px solid #10B981;margin-bottom:8px;">';
        html += '<div style="font-size:10px;color:#065F46;font-weight:600;">Announced Passings: ' + formatNumber(tract.announcedPassings) + '</div>';
        html += '</div>';
    }

    // Overlap indicator
    if (tract.overlap) {
        html += '<div style="background:#FEF2F2;padding:6px 10px;border-radius:6px;border:1px solid #FECACA;display:flex;align-items:center;gap:6px;margin-bottom:8px;">';
        html += '<span style="width:8px;height:8px;border-radius:50%;background:#DC2626;display:inline-block;"></span>';
        html += '<span style="font-size:11px;font-weight:700;color:#DC2626;">Fiber Overlap Zone</span>';
        html += '</div>';
    }

    // Source
    html += '<div style="border-top:1px solid #E2E8F0;padding-top:6px;margin-top:4px;">';
    html += '<div style="font-size:10px;color:#64748B;margin-bottom:4px;"><strong>Source:</strong> ' + (tract.sourceType || 'N/A') + '</div>';
    if (tract.sourceUrl) {
        html += '<a href="' + tract.sourceUrl + '" target="_blank" rel="noopener" style="font-size:10px;color:#0EA5E9;text-decoration:underline;">' + (tract.sourceText || 'View source') + ' &rarr;</a>';
    }
    html += '</div>';

    // Notes
    if (tract.notes) {
        html += '<div style="margin-top:8px;font-size:11px;color:#475569;font-style:italic;">' + tract.notes + '</div>';
    }

    // FCC Providers table (if tract has fccProviders array)
    if (tract.fccProviders && tract.fccProviders.length) {
        // Fiber-on-fiber competition badge
        var fiberCount = tract.fccProviders.filter(function(p) { return p.tech === 'Fiber'; }).length;
        if (fiberCount >= 2) {
            var competitionLabel = fiberCount >= 3 ? 'Highly Contested' : 'Fiber-on-Fiber Competition';
            var badgeColor = fiberCount >= 3 ? '#DC2626' : '#7C3AED';
            html += '<div style="margin-top:8px;padding:6px 10px;border-radius:6px;background:' + badgeColor + '11;border:1px solid ' + badgeColor + '33;display:flex;align-items:center;gap:6px;">';
            html += '<span style="width:8px;height:8px;border-radius:50%;background:' + badgeColor + ';display:inline-block;"></span>';
            html += '<span style="font-size:11px;font-weight:700;color:' + badgeColor + ';">' + competitionLabel + ': ' + fiberCount + ' fiber providers in this tract</span>';
            html += '</div>';
        }

        html += '<div style="margin-top:8px;background:#F8FAFC;padding:8px;border-radius:6px;border:1px solid #E2E8F0;">';
        html += '<div style="font-size:11px;font-weight:700;color:#1E40AF;margin-bottom:6px;">FCC BDC Provider Filings</div>';
        html += '<table style="width:100%;border-collapse:collapse;font-size:10px;">';
        html += '<tr style="background:#EFF6FF;">';
        html += '<th style="text-align:left;padding:4px 6px;color:#1E40AF;font-weight:600;">Provider</th>';
        html += '<th style="text-align:left;padding:4px 6px;color:#1E40AF;font-weight:600;">Tech</th>';
        html += '<th style="text-align:right;padding:4px 6px;color:#1E40AF;font-weight:600;">Down/Up</th>';
        html += '<th style="text-align:right;padding:4px 6px;color:#1E40AF;font-weight:600;">BSLs</th>';
        html += '</tr>';

        for (var fi = 0; fi < tract.fccProviders.length; fi++) {
            var fp = tract.fccProviders[fi];
            var rowBg = (fi % 2 === 0) ? '#FFFFFF' : '#F8FAFC';
            var techIcon = fp.tech === 'Fiber' ? '&#x1F7E2;' : fp.tech === 'Cable (DOCSIS)' ? '&#x1F7E1;' : fp.tech === 'Fixed Wireless' ? '&#x1F7E3;' : '&#x26AA;';
            var techColor = fp.tech === 'Fiber' ? '#059669' : fp.tech === 'Cable (DOCSIS)' ? '#D97706' : fp.tech === 'Fixed Wireless' ? '#7C3AED' : '#94A3B8';
            var downLabel = fp.maxDown >= 1000 ? (fp.maxDown / 1000) + 'G' : fp.maxDown + 'M';
            var upLabel = fp.maxUp >= 1000 ? (fp.maxUp / 1000) + 'G' : fp.maxUp + 'M';

            html += '<tr style="background:' + rowBg + ';border-bottom:1px solid #E2E8F0;">';
            html += '<td style="padding:3px 6px;font-weight:500;color:#334155;white-space:nowrap;">' + fp.name + '</td>';
            html += '<td style="padding:3px 6px;color:' + techColor + ';">' + techIcon + ' ' + fp.tech + '</td>';
            html += '<td style="padding:3px 6px;text-align:right;font-weight:500;">' + downLabel + '/' + upLabel + '</td>';
            html += '<td style="padding:3px 6px;text-align:right;font-weight:600;color:#0F172A;">' + formatNumber(fp.bslCount) + '</td>';
            html += '</tr>';
        }

        html += '</table>';
        html += '</div>';
    }

    html += '</div>';
    return html;
}


// ============================================================
// 3. buildBDCPopupContent(properties)
//    Build HTML popup for a BDC block group polygon click.
// ============================================================

/**
 * Build an HTML popup for a BDC block group layer click.
 *
 * @param {Object} properties - Feature properties from BDC GeoJSON:
 *   { id (GEOID), bsls, bslsResidential, bslsBusiness, hu100, pop100,
 *     areaLandSqKm, county, state, blocks, coveragePct, ... }
 * @param {Object} [providerConfig] - Optional provider display config:
 *   { name, detail, fccId, color, colorLight, colorMid, colorDark, bgLight, speeds }
 * @returns {string} HTML string for Leaflet popup
 */
function buildBDCPopupContent(properties, providerConfig) {
    var props = properties || {};
    var cfg = providerConfig || {
        name: 'Provider',
        detail: '',
        fccId: '',
        color: '#6B7280',
        colorLight: '#F3F4F6',
        colorMid: '#9CA3AF',
        colorDark: '#374151',
        bgLight: '#F9FAFB',
        speeds: 'N/A',
    };

    var geoid = props.id || 'N/A';
    var totalBSLs = props.bsls || 0;
    var resBSLs = props.bslsResidential || 0;
    var bizBSLs = props.bslsBusiness || 0;
    var hu100 = props.hu100 || 0;
    var areaKm = props.areaLandSqKm || 0;
    var coveragePct = props.coveragePct || (hu100 > 0 ? Math.round((totalBSLs / hu100) * 100) : 0);
    var countyName = props.county || 'N/A';
    var stateName = props.state || 'N/A';

    var fccVerifyUrl = cfg.fccId
        ? 'https://broadbandmap.fcc.gov/provider-detail/fixed?version=jun2025&providers=' + cfg.fccId + '_50_on&zoom=13&speed=0_0'
        : 'https://broadbandmap.fcc.gov/data-download/fixed';

    var html = '<div style="font-family:Inter,-apple-system,sans-serif;min-width:260px;">';

    // Colored header bar
    html += '<div style="background:' + cfg.color + ';color:white;padding:8px 12px;margin:-12px -16px 10px;border-radius:8px 8px 0 0;">';
    html += '<div style="font-size:11px;opacity:0.8;">FCC BDC Coverage &middot; Block Group</div>';
    html += '<div style="font-weight:700;font-size:14px;">' + countyName + ', ' + stateName + '</div>';
    html += '<div style="font-size:10px;opacity:0.7;">GEOID: ' + geoid + '</div>';
    html += '</div>';

    // BSL count + Census HU stats
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">';
    html += '<div style="background:' + cfg.bgLight + ';padding:6px 8px;border-radius:6px;border-left:3px solid ' + cfg.color + ';">';
    html += '<div style="font-size:10px;color:' + cfg.colorDark + ';">BSLs Filed</div>';
    html += '<div style="font-weight:700;font-size:16px;color:' + cfg.colorDark + ';">' + formatNumber(totalBSLs) + '</div>';
    if (resBSLs > 0 || bizBSLs > 0) {
        html += '<div style="font-size:9px;color:#94A3B8;">' + formatNumber(resBSLs) + ' res / ' + formatNumber(bizBSLs) + ' biz</div>';
    }
    html += '</div>';
    html += '<div style="background:' + cfg.bgLight + ';padding:6px 8px;border-radius:6px;border-left:3px solid ' + cfg.colorMid + ';">';
    html += '<div style="font-size:10px;color:' + cfg.colorDark + ';">Census HUs</div>';
    html += '<div style="font-weight:700;font-size:16px;color:' + cfg.colorDark + ';">' + (hu100 ? formatNumber(hu100) : 'N/A') + '</div>';
    html += '</div>';
    html += '</div>';

    // Coverage percentage bar
    if (coveragePct > 0) {
        html += '<div style="margin-bottom:8px;">';
        html += '<div style="display:flex;justify-content:space-between;font-size:10px;color:#64748B;margin-bottom:2px;">';
        html += '<span>BSL / Housing Unit Coverage</span>';
        html += '<span style="font-weight:600;color:' + cfg.color + ';">' + coveragePct + '%</span>';
        html += '</div>';
        html += '<div style="background:#E2E8F0;border-radius:4px;height:6px;overflow:hidden;">';
        html += '<div style="background:linear-gradient(90deg,' + cfg.colorMid + ',' + cfg.color + ');height:100%;width:' + Math.min(coveragePct, 100) + '%;border-radius:4px;"></div>';
        html += '</div>';
        html += '</div>';
    }

    // Area and secondary stats
    html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;font-size:10px;margin-bottom:8px;">';
    html += '<div style="text-align:center;background:#F8FAFC;padding:4px;border-radius:4px;">';
    html += '<div style="color:#64748B;">Blocks</div>';
    html += '<div style="font-weight:600;">' + (props.blocks || 'N/A') + '</div>';
    html += '</div>';
    html += '<div style="text-align:center;background:#F8FAFC;padding:4px;border-radius:4px;">';
    html += '<div style="color:#64748B;">Population</div>';
    html += '<div style="font-weight:600;">' + (props.pop100 ? formatNumber(props.pop100) : 'N/A') + '</div>';
    html += '</div>';
    html += '<div style="text-align:center;background:#F8FAFC;padding:4px;border-radius:4px;">';
    html += '<div style="color:#64748B;">Area (km&sup2;)</div>';
    html += '<div style="font-weight:600;">' + (areaKm || 'N/A') + '</div>';
    html += '</div>';
    html += '</div>';

    // Provider info footer
    html += '<div style="font-size:10px;color:#64748B;border-top:1px dashed #E2E8F0;padding-top:6px;">';
    html += '<div><b>Provider:</b> ' + cfg.name + (cfg.detail ? ' (' + cfg.detail + ')' : '') + '</div>';
    html += '<div><b>Technology:</b> FTTP (' + cfg.speeds + ' Mbps)</div>';
    html += '<div><b>Filing:</b> Jun 2025 (pub. Feb 2026)</div>';
    html += '<div><b>County:</b> ' + countyName + '</div>';
    html += '<div><b>State:</b> ' + stateName + '</div>';
    html += '</div>';

    // Verify link
    html += '<div style="margin-top:6px;padding-top:6px;border-top:1px dashed #E2E8F0;">';
    html += '<a href="' + fccVerifyUrl + '" target="_blank" rel="noopener" style="font-size:10px;color:#2563EB;text-decoration:underline;font-weight:600;">Verify on FCC Broadband Map</a>';
    html += ' <span style="font-size:9px;color:#94A3B8;">| <a href="https://broadbandmap.fcc.gov/data-download/fixed" target="_blank" rel="noopener" style="color:#94A3B8;text-decoration:underline;">Data Download</a></span>';
    html += '</div>';

    html += '</div>';
    return html;
}


// ============================================================
// 4. Build cost calculation helpers
// ============================================================

/**
 * Classify density based on housing units per square mile.
 * @param {number} huPerSqMi - Housing units per square mile
 * @returns {string} 'urban', 'suburban', or 'rural'
 */
function getDensityClass(huPerSqMi) {
    if (huPerSqMi >= 3000) return 'urban';
    if (huPerSqMi >= 1000) return 'suburban';
    return 'rural';
}

/**
 * Get estimated fiber cost per passing based on density classification.
 * Based on industry benchmarks for PNW construction costs:
 *   - Urban: $750/passing (dense, existing conduit/utility corridors)
 *   - Suburban: $825-850/passing (typical subdivision builds)
 *   - Rural: $1,100+/passing (long runs, sparse housing)
 *
 * @param {string} density - Density class: 'urban', 'suburban', or 'rural'
 * @returns {number} Estimated cost per passing in dollars
 */
function getCostPerPassing(density) {
    switch (density) {
        case 'urban':
            return 750;
        case 'suburban':
            return 850;
        case 'rural':
            return 1100;
        default:
            return 850; // default to suburban
    }
}

/**
 * Estimate total build cost for a given number of housing units and density.
 *
 * @param {number} housingUnits - Number of housing units (passings)
 * @param {string} density - Density class: 'urban', 'suburban', or 'rural'
 * @returns {number} Estimated total build cost in dollars
 */
function estimateBuildCost(housingUnits, density) {
    var costPerPassing = getCostPerPassing(density);
    return housingUnits * costPerPassing;
}


// ============================================================
// 5. Utility formatters
// ============================================================

/**
 * Format a number with locale-appropriate comma separators.
 * @param {number} n - Number to format
 * @returns {string} Formatted number string
 */
function formatNumber(n) {
    if (n === null || n === undefined) return 'N/A';
    return Number(n).toLocaleString();
}

/**
 * Format a number as a compact currency string.
 * e.g. 1500000 -> "$1.5M", 75000 -> "$75K", 500 -> "$500"
 * @param {number} n - Dollar amount
 * @returns {string} Formatted currency string
 */
function formatCurrency(n) {
    if (n === null || n === undefined) return '$0';
    if (n >= 1000000000) return '$' + (n / 1000000000).toFixed(1) + 'B';
    if (n >= 1000000) return '$' + (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return '$' + (n / 1000).toFixed(0) + 'K';
    return '$' + Number(n).toLocaleString();
}

/**
 * Format a number as a percentage string.
 * @param {number} n - Value between 0 and 1, or 0-100
 * @param {number} [decimals=1] - Decimal places
 * @returns {string} Formatted percentage string
 */
function formatPercent(n, decimals) {
    if (n === null || n === undefined) return '0%';
    var d = (decimals !== undefined) ? decimals : 1;
    // If value is between 0 and 1 (exclusive), treat as ratio and multiply by 100
    var val = (n > 0 && n < 1) ? n * 100 : n;
    return val.toFixed(d) + '%';
}


// ============================================================
// 6. computeConsolidationMetrics(discoveryData)
//    Compute consolidation candidate metrics for small
//    fiber providers in the PNW footprint.
// ============================================================

/**
 * Analyze BDC discovery/coverage data to identify consolidation
 * candidates — smaller fiber providers that could be acquisition
 * targets for Ziply Fiber in the PNW.
 *
 * A candidate qualifies if they have fewer than 50K fiber BSLs
 * across the four-state footprint.
 *
 * @param {Object} discoveryData - BDC discovery data, structured as:
 *   { providers: [ { id, name, fiberBSLs, states, counties, ... }, ... ] }
 *   OR the window.BDC_OVERLAPS object from computeBDCOverlaps
 *
 * @returns {Array} Sorted array of candidate objects:
 *   [ { name, entity, fiberBSLs, estimatedValue, strategicFit,
 *       ziplyOverlapBGs, ziplyOverlapBSLs, overlapPct, states, recommendation }, ... ]
 */
function computeConsolidationMetrics(discoveryData) {
    if (!discoveryData) {
        console.warn('[Consolidation] No discovery data provided.');
        return [];
    }

    var candidates = [];

    // PNW target states for reference
    var pnwStates = { 'WA': true, 'OR': true, 'ID': true, 'MT': true,
                      'Washington': true, 'Oregon': true, 'Idaho': true, 'Montana': true };

    // ----------------------------------------------------------
    // Mode 1: If discoveryData has a providers array (from BDC discovery script)
    // ----------------------------------------------------------
    if (discoveryData.providers && Array.isArray(discoveryData.providers)) {
        var provList = discoveryData.providers;
        // Get Ziply's overlap data if available
        var overlaps = window.BDC_OVERLAPS || null;

        for (var i = 0; i < provList.length; i++) {
            var prov = provList[i];
            var fiberBSLs = prov.fiberBSLs || 0;

            // Only consider providers with <50K fiber BSLs
            if (fiberBSLs >= 50000) continue;
            // Skip Ziply itself
            if (prov.name && prov.name.toLowerCase().indexOf('ziply') !== -1) continue;

            var candidate = {
                name: prov.name || 'Unknown',
                entity: prov.id || prov.entity || 'unknown',
                fiberBSLs: fiberBSLs,
                estimatedValue: estimateProviderValue(fiberBSLs),
                strategicFit: assessStrategicFit(prov, pnwStates),
                ziplyOverlapBGs: 0,
                ziplyOverlapBSLs: 0,
                overlapPct: 0,
                states: prov.states || [],
                recommendation: '',
            };

            // Compute Ziply overlap if overlap data is available
            if (overlaps && overlaps.byPair) {
                var entityKey = (prov.entity || prov.id || prov.name || '').toLowerCase().replace(/[\s\/]+/g, '_');
                // Check for overlap pairs involving this entity and ziply
                var pairKeys = Object.keys(overlaps.byPair);
                for (var pk = 0; pk < pairKeys.length; pk++) {
                    var pair = pairKeys[pk];
                    if (pair.indexOf('ziply') !== -1 && pair.indexOf(entityKey) !== -1) {
                        candidate.ziplyOverlapBGs += overlaps.byPair[pair].bgs;
                        candidate.ziplyOverlapBSLs += overlaps.byPair[pair].bsls;
                    }
                }
                if (fiberBSLs > 0) {
                    candidate.overlapPct = Math.round((candidate.ziplyOverlapBSLs / fiberBSLs) * 100);
                }
            }

            // Generate recommendation
            candidate.recommendation = generateRecommendation(candidate);
            candidates.push(candidate);
        }
    }

    // ----------------------------------------------------------
    // Mode 2: If discoveryData is the BDC_OVERLAPS object itself
    // ----------------------------------------------------------
    else if (discoveryData.byProviderExposure) {
        var exposure = discoveryData.byProviderExposure;
        var entities = Object.keys(exposure);

        for (var e = 0; e < entities.length; e++) {
            var entKey = entities[e];
            // Skip Ziply itself
            if (entKey === 'ziply') continue;

            var exp = exposure[entKey];
            if (exp.totalBSLs >= 50000) continue;
            if (exp.totalBSLs === 0) continue;

            var entityLabel = formatEntityLabel(entKey);

            var cand = {
                name: entityLabel,
                entity: entKey,
                fiberBSLs: exp.totalBSLs,
                estimatedValue: estimateProviderValue(exp.totalBSLs),
                strategicFit: 'medium',  // Default without fuller data
                ziplyOverlapBGs: 0,
                ziplyOverlapBSLs: 0,
                overlapPct: 0,
                states: [],
                recommendation: '',
            };

            // Compute Ziply overlap from byPair
            if (discoveryData.byPair) {
                var allPairs = Object.keys(discoveryData.byPair);
                for (var ap = 0; ap < allPairs.length; ap++) {
                    var pairName = allPairs[ap];
                    if (pairName.indexOf('ziply') !== -1 && pairName.indexOf(entKey) !== -1) {
                        cand.ziplyOverlapBGs += discoveryData.byPair[pairName].bgs;
                        cand.ziplyOverlapBSLs += discoveryData.byPair[pairName].bsls;
                    }
                }
                if (exp.totalBSLs > 0) {
                    cand.overlapPct = Math.round((cand.ziplyOverlapBSLs / exp.totalBSLs) * 100);
                }
            }

            // Determine states from byState data
            if (discoveryData.byState) {
                var stateKeys = Object.keys(discoveryData.byState);
                for (var sk = 0; sk < stateKeys.length; sk++) {
                    if (discoveryData.byState[stateKeys[sk]].providers &&
                        discoveryData.byState[stateKeys[sk]].providers[entKey]) {
                        cand.states.push(stateKeys[sk]);
                    }
                }
            }

            cand.recommendation = generateRecommendation(cand);
            candidates.push(cand);
        }
    }

    // Sort: highest strategic value first (estimated value descending)
    candidates.sort(function(a, b) {
        // Primary: strategic fit rank
        var fitRank = { 'high': 3, 'medium': 2, 'low': 1 };
        var fitDiff = (fitRank[b.strategicFit] || 0) - (fitRank[a.strategicFit] || 0);
        if (fitDiff !== 0) return fitDiff;
        // Secondary: estimated value descending
        return b.estimatedValue - a.estimatedValue;
    });

    console.log('[Consolidation] Identified ' + candidates.length + ' candidates with <50K fiber BSLs');
    return candidates;
}


// ============================================================
// Internal helpers for consolidation metrics
// ============================================================

/**
 * Estimate the acquisition value of a provider based on fiber BSL count.
 * Rough heuristic: ~$2,000-3,500 per fiber BSL depending on scale.
 * @param {number} fiberBSLs
 * @returns {number} Estimated value in dollars
 */
function estimateProviderValue(fiberBSLs) {
    if (fiberBSLs <= 0) return 0;
    // Smaller providers command premium per-BSL (less competition, niche markets)
    if (fiberBSLs < 5000) return fiberBSLs * 3500;
    if (fiberBSLs < 15000) return fiberBSLs * 3000;
    if (fiberBSLs < 30000) return fiberBSLs * 2500;
    return fiberBSLs * 2000;
}

/**
 * Assess strategic fit based on provider data and PNW state footprint.
 * @param {Object} prov - Provider data
 * @param {Object} pnwStates - Set of target PNW state names/abbreviations
 * @returns {string} 'high', 'medium', or 'low'
 */
function assessStrategicFit(prov, pnwStates) {
    var states = prov.states || [];
    var inFootprint = 0;
    for (var i = 0; i < states.length; i++) {
        if (pnwStates[states[i]]) inFootprint++;
    }

    // High fit: operates primarily in Ziply's core states (WA, OR)
    if (inFootprint >= 2) return 'high';
    if (inFootprint >= 1) return 'medium';
    return 'low';
}

/**
 * Format an entity key into a human-readable label.
 * @param {string} entityKey
 * @returns {string}
 */
function formatEntityLabel(entityKey) {
    var labels = {
        'ziply': 'Ziply Fiber',
        'lumen': 'Lumen/CenturyLink',
        'hunter': 'Hunter Communications',
        'quantum': 'Quantum Fiber/AT&T',
        'ezee': 'Ezee Fiber',
        'lightcurve': 'Lightcurve',
        'astound': 'Astound/Wave',
        'fatbeam': 'Fat Beam',
        'emerald': 'Emerald Broadband',
        'charter': 'Charter/Spectrum',
        'comcast': 'Comcast/Xfinity',
        'fidium': 'Fidium/Consolidated',
    };
    return labels[entityKey] || entityKey;
}

/**
 * Generate a recommendation string for a consolidation candidate.
 * @param {Object} candidate
 * @returns {string}
 */
function generateRecommendation(candidate) {
    var parts = [];

    if (candidate.overlapPct > 50) {
        parts.push('High overlap with Ziply (' + candidate.overlapPct + '%) — consolidation reduces competition');
    } else if (candidate.overlapPct > 20) {
        parts.push('Moderate overlap with Ziply (' + candidate.overlapPct + '%) — partial footprint synergy');
    } else if (candidate.overlapPct > 0) {
        parts.push('Low overlap (' + candidate.overlapPct + '%) — incremental footprint expansion');
    }

    if (candidate.fiberBSLs < 5000) {
        parts.push('Small tuck-in opportunity');
    } else if (candidate.fiberBSLs < 20000) {
        parts.push('Mid-size acquisition target');
    } else {
        parts.push('Significant regional player');
    }

    if (candidate.strategicFit === 'high') {
        parts.push('Strong strategic fit in PNW core footprint');
    } else if (candidate.strategicFit === 'medium') {
        parts.push('Moderate strategic fit');
    } else {
        parts.push('Limited footprint overlap — evaluate adjacency');
    }

    return parts.join('. ') + '.';
}
