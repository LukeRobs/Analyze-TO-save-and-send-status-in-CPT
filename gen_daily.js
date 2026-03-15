const fs = require('fs');
const raw = JSON.parse(fs.readFileSync(process.env.USERPROFILE + '/daily_3k.json'));
const headers = raw.values[0];
const rows = raw.values.slice(1);

// Columns (from Daily sheet):
// 0:date_cpt  1:LT  2:vehicle_type  3:eta_plan  4:cpt_plan  5:cpt_realized
// 6:Status_trip  7:Date_SoC  8:Turno_cpt_plan  9:cpt_real_robô  10:Status_Real
// 11:Destino  12:Shipments  13:Turno_Real  14:Doca  15:Pacotes_Real

function parseDate(s) {
  if (!s || s.trim() === '' || s === '.0') return null;
  // Handle "3/13/2026 23:52:00" format
  const parts = s.trim().split(' ');
  if (parts[0] && parts[0].includes('/')) {
    const [m, d, y] = parts[0].split('/');
    const time = parts[1] || '00:00:00';
    return new Date(`${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}T${time}`);
  }
  // Handle "2026-03-14 19:00:00" format
  return new Date(s.replace(' ', 'T'));
}

// Normalize any date string to "YYYY-MM-DDTHH:MM:SS" (no timezone shift)
function normalizeStr(s) {
  if (!s || s.trim() === '' || s === '.0') return null;
  const str = s.trim();
  if (str.includes('/')) {
    // "3/13/2026 23:52:00"
    const [datePart, timePart = '00:00:00'] = str.split(' ');
    const [m, d, y] = datePart.split('/');
    const [hh, mm, ss = '00'] = timePart.split(':');
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}T${hh.padStart(2,'0')}:${mm}:${ss}`;
  }
  // "2026-03-14 19:00:00"
  const [datePart, timePart = '00:00:00'] = str.split(' ');
  const [hh, mm, ss = '00'] = timePart.split(':');
  return `${datePart}T${hh.padStart(2,'0')}:${mm}:${ss}`;
}

function extractTime(s) {
  const n = normalizeStr(s);
  if (!n) return '';
  return n.substring(11, 16); // HH:MM always 2 digits
}

// perdeu CPT = cpt_real_robô filled AND cpt_real_robô > cpt_plan (string compare is safe now)
function perdeuCPT(row) {
  const robo = normalizeStr(row[9]);
  const plan = normalizeStr(row[4]);
  if (!robo || !plan) return false;
  return robo > plan;
}

// Parse Shipments: Brazilian format "7.900" = 7,900 packages (period = thousands sep)
function parseShipments(s) {
  if (!s || s === '.0' || s === '0.0' || s === '0') return 0;
  // Remove ALL dots (thousands separators), replace comma with dot for decimals
  const cleaned = s.trim().replace(/\./g, '').replace(',', '.');
  return Math.round(parseFloat(cleaned) || 0);
}

// Pacotes_Real (col P, index 15): use if filled; fallback to Shipments (col M, index 12)
function getShipments(r) {
  const real = r[15];
  if (real && real !== '.0' && real !== '0' && real !== '0.0') return parseShipments(real);
  return parseShipments(r[12]);
}

// Status considered as "Carregadas" for SPR/Liberado card
const CARREGADAS = new Set(['Carregado', 'Carregado/Liberado', 'Finalizado']);

const byDate = {};
const allRows = [];
let withDoca = 0, withRobo = 0, perdeuCount = 0;

rows.forEach(r => {
  // Use Date_SoC (col H, index 7) as operational date — groups T3 overnight shifts correctly.
  // Fallback to date_cpt (col A, index 0) when Date_SoC is empty.
  const dateSoc = (r[7] || r[0] || '').substring(0, 10);
  if (!dateSoc || dateSoc.length < 10) return;

  const turno = r[13] || '';
  if (!turno) return;

  const destino  = r[11] || '';
  const doca     = r[14] || '';
  const statusR  = r[10] || '';
  const pct      = perdeuCPT(r);
  const ship     = getShipments(r);  // Pacotes_Real (col P) se preenchido, senão Shipments (col M)
  const isCarr   = CARREGADAS.has(statusR);

  if (doca) withDoca++;
  if (r[9]) withRobo++;
  if (pct) perdeuCount++;

  const row = {
    d:   dateSoc,           // Date_SoC — operational date (col H)
    lt:  r[1]  || '',
    vt:  r[2]  || '',
    ep:  extractTime(r[3]),
    cp:  extractTime(r[4]),
    cr:  extractTime(r[9]),
    sr:  statusR,
    dest:destino,
    doca:doca,
    tr:  turno,
    ship:ship,              // Shipments (integer)
    pct: pct ? 1 : 0,
  };
  allRows.push(row);

  if (!byDate[dateSoc]) byDate[dateSoc] = {};
  if (!byDate[dateSoc][turno]) byDate[dateSoc][turno] = {
    total:0, statusReal:{}, destinos:{}, docas:{}, perdeuCPT:0,
    totalShip:0, carregadas:0, shipCarregadas:0
  };
  const tg = byDate[dateSoc][turno];
  tg.total++;
  tg.totalShip    += ship;
  tg.statusReal[statusR] = (tg.statusReal[statusR]||0) + 1;
  if (destino) tg.destinos[destino] = (tg.destinos[destino]||0) + 1;
  if (doca)    tg.docas[doca]       = (tg.docas[doca]||0) + 1;
  if (pct)     tg.perdeuCPT++;
  if (isCarr)  { tg.carregadas++; tg.shipCarregadas += ship; }
});

const dates = Object.keys(byDate).sort();
console.log('Dates:', dates[0], '->', dates[dates.length-1], '| Total:', dates.length);
console.log('Total rows:', allRows.length);
console.log('With Doca:', withDoca, '| With cpt_real_robô:', withRobo, '| Perdeu CPT:', perdeuCount);

// Print today stats
const today = '2026-03-14';
['T1','T2','T3'].forEach(t => {
  const tg = byDate[today] && byDate[today][t];
  if (!tg) return;
  console.log(`\n${today} ${t}: total=${tg.total} perdeu=${tg.perdeuCPT}`);
  console.log('  Status:', JSON.stringify(tg.statusReal));
  console.log('  Top5 Destinos:', Object.entries(tg.destinos).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([k,v])=>k+':'+v).join(', '));
  console.log('  Top5 Docas:', Object.entries(tg.docas).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([k,v])=>k+':'+v).join(', '));
  console.log(`  totalShip=${tg.totalShip} carregadas=${tg.carregadas} shipCarr=${tg.shipCarregadas} SPR=${tg.carregadas>0?(tg.shipCarregadas/tg.carregadas).toFixed(0):0}`);
});

// Sample row LT0Q3E020GJY1
const sample = allRows.find(r => r.lt === 'LT0Q3E020GJY1');
console.log('\nSample LT0Q3E020GJY1:', JSON.stringify(sample));

// Sample perdeu CPT rows
const perdeu = allRows.filter(r => r.pct);
console.log('\nPerdeu CPT sample:');
perdeu.slice(0,5).forEach(r => console.log(' ', r.d, r.lt, r.tr, '| cp:', r.cp, '| cr:', r.cr));

// Write data.js
const DATES = dates;
const BY_DATE = byDate;
const ALL_ROWS = allRows;

const output = `const DATES=${JSON.stringify(DATES)};
const BY_DATE=${JSON.stringify(BY_DATE)};
const ALL_ROWS=${JSON.stringify(ALL_ROWS)};`;

fs.writeFileSync(process.env.USERPROFILE + '/data_daily.js', output);
const size = fs.statSync(process.env.USERPROFILE + '/data_daily.js').size;
console.log(`\ndata_daily.js: ${(size/1024).toFixed(1)} KB`);
