// Vercel Serverless Function — /api/packed
// Packed On Time analysis: reads Base!A1:L from the packed sheet

const crypto         = require('crypto');
const SPREADSHEET_ID = '19SX6rQM12Rgz52hbGKC9BmopGMXT4aq87K0w2WMZ4sA';
const RANGE          = 'Base!A1:L';

// ── Service Account JWT ───────────────────────────────────────────────
let saToken = null, saTokenExp = 0;

function b64url(buf) {
  return buf.toString('base64').replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
}

async function getServiceAccountToken(sa) {
  if (saToken && Date.now() < saTokenExp) return saToken;
  const { client_email, private_key } = sa;
  const now = Math.floor(Date.now() / 1000);
  const hdr = b64url(Buffer.from(JSON.stringify({ alg:'RS256', typ:'JWT' })));
  const pay = b64url(Buffer.from(JSON.stringify({
    iss: client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600, iat: now,
  })));
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(`${hdr}.${pay}`);
  const jwt = `${hdr}.${pay}.${b64url(sign.sign(private_key))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  if (!resp.ok) throw new Error(`Token error: ${resp.status}`);
  const data = await resp.json();
  saToken    = data.access_token;
  saTokenExp = Date.now() + (data.expires_in - 60) * 1000;
  return saToken;
}

// ── Data Processing ───────────────────────────────────────────────────
// Columns (index): 0=to_id, 1=station_name, 2=destino, 3=received_datetime,
// 4=packed_datetime, 5=transporting_datetime, 6=estimated_cpt_datetime,
// 7=qtd_pacotes, 8=target_process, 9=packed_day, 10=cpt_status, 11=Diff

function parseDiffMinutes(diff) {
  if (!diff || !diff.trim()) return null;
  const parts = diff.trim().split(':');
  if (parts.length === 3) {
    return parseInt(parts[0]) * 60 + parseInt(parts[1]) + parseInt(parts[2]) / 60;
  }
  return null;
}

function diffBucket(minutes) {
  if (minutes === null) return null;
  if (minutes < 30)  return 'menos30';
  if (minutes <= 60) return 'd30a60';
  return 'mais60';
}

function emptyBucket() { return { total: 0, sent: 0, notSent: 0 }; }

function processData(raw) {
  const rows = Array.isArray(raw.values) ? raw.values.slice(1) : [];

  const byDay     = {};
  const buckets   = { menos30: emptyBucket(), d30a60: emptyBucket(), mais60: emptyBucket() };
  let totalSent = 0, totalNotSent = 0;
  let totalOnTime = 0, totalOutTime = 0, totalCptNA = 0;

  rows.forEach(r => {
    const packedDay = (r[9] || '').substring(0, 10);
    if (!packedDay || packedDay.length < 10) return;

    const sent    = !!(r[5] && r[5].trim() !== '');
    const status  = r[10] || '';
    const diffMin = parseDiffMinutes(r[11]);
    const bucket  = diffBucket(diffMin);

    if (!byDay[packedDay]) byDay[packedDay] = {
      total: 0, sent: 0, notSent: 0,
      onTime: 0, outTime: 0, cptNA: 0,
      menos30: emptyBucket(), d30a60: emptyBucket(), mais60: emptyBucket(),
    };

    const d = byDay[packedDay];
    d.total++;
    if (sent) { d.sent++; totalSent++; } else { d.notSent++; totalNotSent++; }

    if (status === 'ON_TIME')           { d.onTime++;  totalOnTime++;  }
    else if (status === 'OUT_TIME')     { d.outTime++; totalOutTime++; }
    else if (status === 'CPT_NOT_AVAILABLE') { d.cptNA++;  totalCptNA++;  }

    if (bucket) {
      d[bucket].total++;
      if (sent) d[bucket].sent++; else d[bucket].notSent++;
      buckets[bucket].total++;
      if (sent) buckets[bucket].sent++; else buckets[bucket].notSent++;
    }
  });

  const total = totalSent + totalNotSent;
  return {
    total, totalSent, totalNotSent,
    totalOnTime, totalOutTime, totalCptNA,
    buckets, byDay,
    days: Object.keys(byDay).sort(),
    generatedAt: Date.now(),
  };
}

// ── Handler ──────────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=120, stale-while-revalidate=300');

  try {
    let sheetsUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(RANGE)}`;
    let headers   = {};

    if (process.env.GOOGLE_SERVICE_ACCOUNT) {
      const sa    = JSON.parse(Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT, 'base64').toString('utf8'));
      const token = await getServiceAccountToken(sa);
      headers     = { Authorization: `Bearer ${token}` };
    } else if (process.env.SHEETS_API_KEY) {
      sheetsUrl += `?key=${process.env.SHEETS_API_KEY}`;
    } else {
      return res.status(500).json({ error: 'Configure GOOGLE_SERVICE_ACCOUNT ou SHEETS_API_KEY.' });
    }

    const response = await fetch(sheetsUrl, { headers });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Sheets API ${response.status}: ${body.substring(0, 300)}`);
    }
    const raw  = await response.json();
    const data = processData(raw);
    res.json(data);
  } catch (err) {
    console.error('[api/packed] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
};
