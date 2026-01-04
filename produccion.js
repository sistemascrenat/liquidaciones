// produccion.js — COMPLETO (nuevo esquema final)
// ✅ CSV import (staging + confirmar + anular)
// ✅ Staging: produccion_imports/{importId} + items
// ✅ Resolver pendientes (Clínicas / Cirugías) con mappings persistentes
// ✅ Cirugías pendientes separadas por: Clínica + Tipo Paciente + Cirugía CSV (no mezcla clínicas)
// ✅ Confirmar bloqueado si hay pendientes
// ✅ Confirmar escribe en NUEVO esquema:
//    produccion/{YYYY-MM}/items/{PACIENTE_ID}
// ✅ Preview muestra MUCHAS columnas + modal detalle por fila
// ✅ Modal con scroll interno OK

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { loadSidebar } from './layout.js';

import {
  collection, doc, setDoc, getDoc, getDocs, writeBatch,
  serverTimestamp, query, where, orderBy, limit, startAfter
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   DOM
========================= */
const $ = (id)=> document.getElementById(id);

/* =========================
   CSV columnas esperadas
========================= */
const EXPECTED_COLS = [
  '#','Suspendida','Confirmado','Fecha','Hora','Clínica',
  'Cirujano','Anestesista','Ayudante 1','Ayudante 2','Arsenalera',
  'Cirugía','Tipo de Paciente','Previsión','Nombre Paciente','RUT','Teléfono',
  'Dirección','e-mail','Sexo','Fecha nac. (dd/mm/aaaa)','Edad','Fecha consulta',
  'Peso','Altura (talla) m','IMC','Diabetes','Hipertensión','Sahos',
  'Trastorno músculo esquelético','Síndrome metabólico','Insuficiencia renal',
  'Transtorno metabólico carbohidratos','Dislipidemia','Hígado graso','Hiperuricemia',
  'Hipotiroidismo','Reflujo','Otras','Ex. Laboratorio','Espirometría','Endoscopía',
  'Eco Abdominal','Test de Esfuerzo','Grupo Sangre RH','Valor','Pagado','Fecha de Pago',
  'Derechos de Pabellón','HMQ','Insumos'
];

/* =========================
   Utils
========================= */
function clean(s){ return (s ?? '').toString().trim(); }

function normalizeKey(s){
  return clean(s)
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .replace(/\s+/g,' ')
    .trim();
}

function nonEmptyOrUndef(v){
  const x = clean(v);
  return x === '' ? undefined : x;
}

function toBool(v){
  const x = normalizeKey(v);
  if(x === 'si' || x === 'sí' || x === 'true' || x === '1') return true;
  if(x === 'no' || x === 'false' || x === '0') return false;
  return null;
}

function parseCLPNumber(v){
  const s = (v ?? '').toString().replace(/[^\d]/g,'');
  return Number(s || 0) || 0;
}

function clp(n){
  const x = Number(n || 0) || 0;
  const s = Math.round(x).toString();
  return '$' + s.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
}

function pad(n, w=4){
  const s = String(n);
  return s.length >= w ? s : ('0'.repeat(w - s.length) + s);
}

function nowId(){
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = pad(d.getMonth()+1,2);
  const dd = pad(d.getDate(),2);
  const hh = pad(d.getHours(),2);
  const mi = pad(d.getMinutes(),2);
  const ss = pad(d.getSeconds(),2);
  return `${yyyy}${mm}${dd}_${hh}${mi}${ss}`;
}

function monthIndex(name){
  const m = normalizeKey(name);
  const map = {
    'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
    'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12
  };
  return map[m] || 0;
}

function monthKey(ano, mesNum){
  return `${ano}-${pad(mesNum,2)}`; // YYYY-MM
}

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

/* Fecha CSV -> ISO (intenta dd-mm-aaaa o dd/mm/aaaa) */
function parseFechaISO(fechaTxt){
  const s = clean(fechaTxt);
  const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if(!m) return null;
  const dd = pad(m[1],2);
  const mm = pad(m[2],2);
  const yyyy = m[3];
  return `${yyyy}-${mm}-${dd}`;
}

/* Hora -> HH:MM (si viene "8:00 a.m." etc, intentamos) */
function parseHoraHHMM(horaTxt){
  const s0 = clean(horaTxt).toLowerCase();
  if(!s0) return null;

  // 08:00 / 8:00
  const m1 = s0.match(/^(\d{1,2})\s*:\s*(\d{2})/);
  if(!m1) return null;

  let hh = Number(m1[1]);
  const mm = Number(m1[2]);

  const isPM = s0.includes('p.m') || s0.includes('pm');
  const isAM = s0.includes('a.m') || s0.includes('am');

  if(isPM && hh < 12) hh += 12;
  if(isAM && hh === 12) hh = 0;

  return `${pad(hh,2)}:${pad(mm,2)}`;
}

/* =========================
   CSV parsing
========================= */
function detectDelimiter(text){
  const head = text.split(/\r?\n/).slice(0,5).join('\n');
  const semis = (head.match(/;/g) || []).length;
  const commas = (head.match(/,/g) || []).length;
  return semis > commas ? ';' : ',';
}

function parseCSV(text){
  const delim = detectDelimiter(text);
  const rows = [];
  let row = [];
  let cur = '';
  let inQuotes = false;

  for(let i=0;i<text.length;i++){
    const ch = text[i];
    const next = text[i+1];

    if(ch === '"'){
      if(inQuotes && next === '"'){ cur += '"'; i++; }
      else inQuotes = !inQuotes;
      continue;
    }

    if(!inQuotes && ch === delim){
      row.push(cur); cur = ''; continue;
    }

    if(!inQuotes && (ch === '\n' || ch === '\r')){
      if(ch === '\r' && next === '\n') i++;
      row.push(cur);
      rows.push(row);
      row = [];
      cur = '';
      continue;
    }

    cur += ch;
  }

  if(cur.length || row.length){
    row.push(cur);
    rows.push(row);
  }

  return rows
    .map(r=> r.map(c=> (c ?? '').toString()))
    .filter(r=> r.some(c=> clean(c) !== ''));
}

/* =========================
   Firestore refs
========================= */
const colClinicas = collection(db, 'clinicas');
const colProcedimientos = collection(db, 'procedimientos'); // cirugías: tipo="cirugia"
const colAmbulatorios = collection(db, 'ambulatorios');

const docMapClinicas = doc(db, 'produccion_mappings', 'clinicas');
const docMapCirugias = doc(db, 'produccion_mappings', 'cirugias'); // map por key compuesta
const docMapAmb      = doc(db, 'produccion_mappings', 'ambulatorios');

/* staging */
const colImports = collection(db, 'produccion_imports');

/* =========================
   State
========================= */
const state = {
  user: null,

  importId: '',
  status: 'idle', // idle | staged | confirmada | anulada
  monthName: '',
  monthNum: 0,
  year: 0,
  ym: '',          // YYYY-MM
  filename: '',

  stagedItems: [], // [{ idx, raw, normalizado, resolved }]

  catalogs: {
    clinicas: [],
    clinicasByNorm: new Map(),
    clinicasById: new Map(),

    cirugias: [],
    cirugiasByNorm: new Map(),
    cirugiasById: new Map(),

    amb: [],
    ambByNorm: new Map(),
    ambById: new Map()
  },

  maps: {
    clinicas: new Map(), // normClinTxt -> {id}
    cirugias: new Map(), // keyCompuesta -> {id}
    amb: new Map()
  },

  pending: {
    clinicas: [], // [{csvName, norm}]
    cirugias: [], // [{key, csvName, normCir, tipoPaciente, tipoPacienteNorm, clinTxt, clinNorm, clinicaId, suggestions}]
    amb: []
  }
};

/* =========================
   UI
========================= */
function setStatus(text){ $('statusInfo').textContent = text || '—'; }

function setPills(){
  const pc = state.pending.clinicas.length;
  const ps = state.pending.cirugias.length;
  const pa = state.pending.amb.length;
  const total = pc + ps + pa;

  const pillPend = $('pillPendientes');
  pillPend.textContent = `Pendientes: ${total}`;
  pillPend.className = 'pill ' + (total === 0 ? 'ok' : 'warn');

  const pillC = $('pillClinicas');
  pillC.textContent = `Clínicas: ${pc}`;
  pillC.className = 'pill ' + (pc === 0 ? 'ok' : 'warn');

  const pillS = $('pillCirugias');
  pillS.textContent = `Cirugías: ${ps}`;
  pillS.className = 'pill ' + (ps === 0 ? 'ok' : 'warn');

  const pillA = $('pillAmb');
  pillA.textContent = `Ambulatorios: ${pa}`;
  pillA.className = 'pill ' + (pa === 0 ? 'ok' : 'warn');

  const hint = $('hintResolver');
  hint.textContent = (state.status === 'staged')
    ? (total === 0 ? '✅ Todo resuelto. Puedes confirmar.' : '⚠️ Resuelve pendientes para confirmar.')
    : 'Cargar CSV → resolver faltantes → confirmar.';
}

function setButtons(){
  const staged = state.status === 'staged';
  const confirmed = state.status === 'confirmada';
  const totalPend = state.pending.clinicas.length + state.pending.cirugias.length + state.pending.amb.length;

  $('btnResolver').disabled = !(staged && totalPend > 0);
  $('btnConfirmar').disabled = !(staged && totalPend === 0);
  $('btnAnular').disabled = !(staged || confirmed);
}

/* modal resolver */
function openResolverModal(){
  $('modalResolverBackdrop').classList.add('show');
  paintResolverModal();
}
function closeResolverModal(){
  $('modalResolverBackdrop').classList.remove('show');
}

/* modal detalle */
function openDetalleModal(html, sub=''){
  $('detalleSub').textContent = sub || '—';
  $('detalleBody').innerHTML = html;
  $('modalDetalleBackdrop').classList.add('show');
}
function closeDetalleModal(){
  $('modalDetalleBackdrop').classList.remove('show');
}

/* =========================
   Preview
========================= */
function boolMark(v){
  if(v === true) return '<span class="ok">Sí</span>';
  if(v === false) return '<span class="bad">No</span>';
  return '<span class="muted">—</span>';
}

function paintPreview(){
  const tb = $('tbody');
  tb.innerHTML = '';

  const rows = state.stagedItems || [];
  $('countPill').textContent = `${rows.length} fila${rows.length===1?'':'s'}`;

  const max = Math.min(rows.length, 80);

  for(let i=0;i<max;i++){
    const it = rows[i];
    const n = it.normalizado || {};
    const r = it.resolved || {};
    const raw = it.raw || {};
    const prof = n.profesionalesResumen || '';

    const flags = [];
    if(r.clinicaOk === false) flags.push('Clínica');
    if(r.cirugiaOk === false) flags.push('Cirugía');

    const st = (flags.length === 0)
      ? `<span class="ok">OK</span>`
      : `<span class="warn">Pendiente: ${flags.join(', ')}</span>`;

    const tr = document.createElement('tr');

    const idxTxt = raw['#'] ?? pad(i+1,2);
    const clinTxt = n.clinica || '—';
    const cirTxt  = n.cirugia || '—';

    tr.innerHTML = `
      <td class="mono">${escapeHtml(idxTxt)}</td>
      <td>${boolMark(n.suspendida)}</td>
      <td>${boolMark(n.confirmado)}</td>
      <td>${escapeHtml(n.fecha || '—')}</td>
      <td>${escapeHtml(n.hora || '—')}</td>

      <td>
        <div><b>${escapeHtml(clinTxt)}</b></div>
        <div class="tiny muted">ID: ${escapeHtml(r.clinicaId || '—')}</div>
      </td>

      <td>
        <div><b>${escapeHtml(cirTxt)}</b></div>
        <div class="tiny muted">ID: ${escapeHtml(r.cirugiaId || '—')}</div>
        <div class="tiny muted">Key: ${escapeHtml(r.cirugiaKey || '—')}</div>
      </td>

      <td>${escapeHtml(n.tipoPaciente || '—')}</td>
      <td>${escapeHtml(raw['Previsión'] || '—')}</td>
      <td>${escapeHtml(raw['Nombre Paciente'] || '—')}</td>
      <td class="mono">${escapeHtml(raw['RUT'] || '—')}</td>
      <td>${escapeHtml(raw['Teléfono'] || '—')}</td>
      <td>${escapeHtml(raw['Dirección'] || '—')}</td>
      <td>${escapeHtml(raw['e-mail'] || '—')}</td>
      <td>${escapeHtml(raw['Sexo'] || '—')}</td>
      <td>${escapeHtml(raw['Fecha nac. (dd/mm/aaaa)'] || '—')}</td>
      <td>${escapeHtml(raw['Edad'] || '—')}</td>

      <td><b>${clp(n.valor || 0)}</b></td>
      <td>${clp(n.dp || 0)}</td>
      <td>${clp(n.hmq || 0)}</td>
      <td>${clp(n.ins || 0)}</td>

      <td>${boolMark(n.pagado)}</td>
      <td>${escapeHtml(n.fechaPago || '—')}</td>

      <td class="tiny">${prof ? escapeHtml(prof) : '<span class="muted">—</span>'}</td>
      <td>${st}</td>
      <td>
        <button class="btn sm" data-ver="${i}">Ver</button>
      </td>
    `;

    tb.appendChild(tr);
  }

  // bind botones "Ver"
  tb.querySelectorAll('[data-ver]').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const i = Number(btn.getAttribute('data-ver'));
      const it = state.stagedItems[i];
      if(!it) return;

      const raw = it.raw || {};
      const n = it.normalizado || {};
      const r = it.resolved || {};

      const pairs = EXPECTED_COLS
        .filter(k => raw[k] !== undefined)
        .map(k => `
          <div style="display:grid;grid-template-columns:220px 1fr;gap:10px;padding:8px;border-bottom:1px solid rgba(0,0,0,.06);">
            <div class="muted tiny"><b>${escapeHtml(k)}</b></div>
            <div>${escapeHtml(String(raw[k]))}</div>
          </div>
        `).join('');

      const header = `
        <div class="card" style="padding:12px;">
          <div style="font-weight:900;">${escapeHtml(raw['Nombre Paciente'] || '(Sin nombre)')}</div>
          <div class="muted tiny" style="margin-top:4px;">
            ${escapeHtml(n.fecha || '—')} · ${escapeHtml(n.hora || '—')} · ${escapeHtml(n.clinica || '—')} · ${escapeHtml(n.tipoPaciente || '—')}
          </div>
          <div class="help" style="margin-top:8px;">
            <b>IDs resueltos:</b> Clínica=${escapeHtml(r.clinicaId || '—')} · Cirugía=${escapeHtml(r.cirugiaId || '—')}
          </div>
        </div>
        <div style="height:10px;"></div>
      `;

      openDetalleModal(header + `<div class="card" style="padding:0;">${pairs}</div>`,
        `Fila ${raw['#'] || (i+1)} · ImportID ${state.importId || '—'}`
      );
    });
  });

  if(rows.length > max){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td colspan="26" class="muted tiny">
        Mostrando ${max} de ${rows.length}. (El resto igual quedó en staging)
      </td>
    `;
    tb.appendChild(tr);
  }
}

/* =========================
   Header index / raw / normalizado
========================= */
function buildHeaderIndex(headerRow){
  const idx = new Map();
  const headerNorm = headerRow.map(h=> normalizeKey(h));
  for(const col of EXPECTED_COLS){
    const want = normalizeKey(col);
    const j = headerNorm.indexOf(want);
    if(j >= 0) idx.set(col, j);
  }
  return idx;
}

function readCell(row, headerIdx, colName){
  const j = headerIdx.get(colName);
  if(j === undefined) return '';
  return row[j] ?? '';
}

function compactRaw(row, headerIdx){
  const raw = {};
  for(const col of EXPECTED_COLS){
    const val = nonEmptyOrUndef(readCell(row, headerIdx, col));
    if(val !== undefined) raw[col] = val;
  }
  return raw;
}

function buildNormalizado(raw){
  const fecha = raw['Fecha'] ?? null;
  const hora = raw['Hora'] ?? null;
  const clinica = raw['Clínica'] ?? null;
  const cirugia = raw['Cirugía'] ?? null;
  const tipoPaciente = raw['Tipo de Paciente'] ?? null;

  const valor = parseCLPNumber(raw['Valor'] ?? 0);
  const dp = parseCLPNumber(raw['Derechos de Pabellón'] ?? 0);
  const hmq = parseCLPNumber(raw['HMQ'] ?? 0);
  const ins = parseCLPNumber(raw['Insumos'] ?? 0);

  const suspendida = toBool(raw['Suspendida']);
  const confirmado = toBool(raw['Confirmado']);
  const pagado = toBool(raw['Pagado']);
  const fechaPago = raw['Fecha de Pago'] ?? null;

  const prof = {
    cirujano: raw['Cirujano'] ?? null,
    anestesista: raw['Anestesista'] ?? null,
    ayudante1: raw['Ayudante 1'] ?? null,
    ayudante2: raw['Ayudante 2'] ?? null,
    arsenalera: raw['Arsenalera'] ?? null
  };

  const parts = [];
  if(prof.cirujano) parts.push(`Cirujano: ${prof.cirujano}`);
  if(prof.anestesista) parts.push(`Anest: ${prof.anestesista}`);
  if(prof.ayudante1) parts.push(`Ay1: ${prof.ayudante1}`);
  if(prof.ayudante2) parts.push(`Ay2: ${prof.ayudante2}`);
  if(prof.arsenalera) parts.push(`Ars: ${prof.arsenalera}`);

  return {
    fecha: fecha || null,
    hora: hora || null,
    clinica: clinica || null,
    cirugia: cirugia || null,
    tipoPaciente: tipoPaciente || null,

    valor, dp, hmq, ins,

    suspendida,
    confirmado,
    pagado,
    fechaPago: fechaPago || null,

    profesionales: prof,
    profesionalesResumen: parts.join(' · ') || null
  };
}

function validateMinimum(headerIdx){
  const needed = ['Fecha','Clínica','Cirugía','Tipo de Paciente','Valor'];
  const missing = needed.filter(k => headerIdx.get(k) === undefined);
  return missing;
}

/* =========================
   Load catalogs + mappings
========================= */
async function loadMappings(){
  async function loadOne(docRef){
    const snap = await getDoc(docRef);
    if(!snap.exists()) return new Map();
    const data = snap.data() || {};
    const m = (data.map && typeof data.map === 'object') ? data.map : {};
    const out = new Map();
    for(const k of Object.keys(m)){
      const v = m[k] || {};
      if(v.id) out.set(k, { id: v.id });
    }
    return out;
  }
  state.maps.clinicas = await loadOne(docMapClinicas);
  state.maps.cirugias  = await loadOne(docMapCirugias);
  state.maps.amb       = await loadOne(docMapAmb);
}

async function loadCatalogs(){
  // CLINICAS
  {
    const snap = await getDocs(colClinicas);
    const out = [];
    snap.forEach(d=>{
      const x = d.data() || {};
      const id = clean(x.id) || d.id;
      const nombre = clean(x.nombre) || id;
      if(id) out.push({ id, nombre });
    });
    out.sort((a,b)=> normalizeKey(a.nombre).localeCompare(normalizeKey(b.nombre)));
    state.catalogs.clinicas = out;
    state.catalogs.clinicasByNorm = new Map(out.map(c=> [normalizeKey(c.nombre), c]));
    state.catalogs.clinicasById   = new Map(out.map(c=> [c.id, c]));
  }

  // AMBULATORIOS (no bloquea hoy)
  {
    const snap = await getDocs(colAmbulatorios);
    const out = [];
    snap.forEach(d=>{
      const x = d.data() || {};
      const id = clean(x.id) || d.id;
      const nombre = clean(x.nombre) || id;
      if(id) out.push({ id, nombre });
    });
    out.sort((a,b)=> normalizeKey(a.nombre).localeCompare(normalizeKey(b.nombre)));
    state.catalogs.amb = out;
    state.catalogs.ambByNorm = new Map(out.map(a=> [normalizeKey(a.nombre), a]));
    state.catalogs.ambById   = new Map(out.map(a=> [a.id, a]));
  }

  // CIRUGIAS (procedimientos tipo cirugia)
  {
    const qy = query(colProcedimientos, where('tipo','==','cirugia'));
    const snap = await getDocs(qy);
    const out = [];
    snap.forEach(d=>{
      const x = d.data() || {};
      const id = d.id;
      const nombre = clean(x.nombre) || '';
      const codigo = clean(x.codigo) || id;
      if(id && nombre) out.push({ id, nombre, codigo });
    });
    out.sort((a,b)=> normalizeKey(a.nombre).localeCompare(normalizeKey(b.nombre)));
    state.catalogs.cirugias = out;
    state.catalogs.cirugiasByNorm = new Map(out.map(s=> [normalizeKey(s.nombre), s]));
    state.catalogs.cirugiasById   = new Map(out.map(s=> [s.id, s]));
  }
}

/* =========================
   Resolution model (clínica + tipoPaciente + cirugía)
========================= */

/* Key compuesta para cirugías: clinicaId|tipoPacienteNorm|cirugiaNorm */
function cirugiaKey(clinicaId, tipoPacienteNorm, cirugiaNorm){
  return `cl:${clinicaId || 'NA'}|tp:${tipoPacienteNorm || 'na'}|cx:${cirugiaNorm || 'na'}`;
}

function suggestCirugias(normName){
  const all = state.catalogs.cirugias || [];
  const out = [];
  for(const s of all){
    const nn = normalizeKey(s.nombre);
    if(nn.includes(normName) || normName.includes(nn)){
      out.push({ id: s.id, nombre: s.nombre });
      if(out.length >= 6) break;
    }
  }
  return out;
}

function resolveOneItem(normalizado){
  const resolved = {
    clinicaId: null,
    cirugiaId: null,
    ambulatorioId: null,

    clinicaOk: true,
    cirugiaOk: true,
    ambOk: true,

    cirugiaKey: null
  };

  // --- Clínica ---
  const clinTxt = clean(normalizado.clinica || '');
  if(clinTxt){
    const clinNorm = normalizeKey(clinTxt);

    const mapped = state.maps.clinicas.get(clinNorm);
    if(mapped?.id){
      resolved.clinicaId = mapped.id;
      resolved.clinicaOk = true;
    } else {
      const found = state.catalogs.clinicasByNorm.get(clinNorm);
      if(found?.id){
        resolved.clinicaId = found.id;
        resolved.clinicaOk = true;
      } else {
        resolved.clinicaOk = false;
      }
    }
  } else {
    resolved.clinicaOk = false;
  }

  // --- Cirugía (depende del contexto) ---
  const cirTxt = clean(normalizado.cirugia || '');
  const tpTxt  = clean(normalizado.tipoPaciente || '');
  const tpNorm = normalizeKey(tpTxt || '');

  if(!cirTxt){
    resolved.cirugiaOk = false;
    return resolved;
  }

  const cirNorm = normalizeKey(cirTxt);

  // si no hay clinicaId resuelta, no intentamos cirugia contextual
  if(!resolved.clinicaId){
    resolved.cirugiaOk = false;
    resolved.cirugiaKey = cirugiaKey(null, tpNorm, cirNorm);
    return resolved;
  }

  const key = cirugiaKey(resolved.clinicaId, tpNorm, cirNorm);
  resolved.cirugiaKey = key;

  // 1) mapping explícito por key compuesta
  const mapped = state.maps.cirugias.get(key);
  if(mapped?.id){
    resolved.cirugiaId = mapped.id;
    resolved.cirugiaOk = true;
    return resolved;
  }

  // 2) fallback: match exacto por nombre (sin contexto)
  const found = state.catalogs.cirugiasByNorm.get(cirNorm);
  if(found?.id){
    resolved.cirugiaId = found.id;
    // ojo: esto puede ser OK, pero queda “sin mapping contextual”
    resolved.cirugiaOk = true;
    return resolved;
  }

  resolved.cirugiaOk = false;
  return resolved;
}

function recomputePending(){
  state.pending.clinicas = [];
  state.pending.cirugias = [];
  state.pending.amb = [];

  for(const it of state.stagedItems){
    it.resolved = resolveOneItem(it.normalizado || {});
  }

  const seenClin = new Set();
  const seenCir  = new Set();

  for(const it of state.stagedItems){
    const n = it.normalizado || {};
    const r = it.resolved || {};

    // clínicas pendientes (por nombre)
    const clinTxt = clean(n.clinica || '');
    if(clinTxt && r.clinicaOk === false){
      const norm = normalizeKey(clinTxt);
      if(!seenClin.has(norm)){
        seenClin.add(norm);
        state.pending.clinicas.push({ csvName: clinTxt, norm });
      }
    }

    // cirugías pendientes (por clave contextual)
    const cirTxt = clean(n.cirugia || '');
    if(cirTxt && r.cirugiaOk === false){
      const clinTxt2 = clean(n.clinica || '');
      const clinNorm = normalizeKey(clinTxt2 || '');
      const tpTxt = clean(n.tipoPaciente || '');
      const tpNorm = normalizeKey(tpTxt || '');
      const cirNorm = normalizeKey(cirTxt || '');

      const key = r.cirugiaKey || cirugiaKey(r.clinicaId || null, tpNorm, cirNorm);

      if(!seenCir.has(key)){
        seenCir.add(key);
        state.pending.cirugias.push({
          key,
          csvName: cirTxt,
          normCir: cirNorm,
          tipoPaciente: tpTxt || '(Sin tipo)',
          tipoPacienteNorm: tpNorm,
          clinTxt: clinTxt2 || '(Sin clínica)',
          clinNorm,
          clinicaId: r.clinicaId || null,
          suggestions: suggestCirugias(cirNorm)
        });
      }
    }
  }

  state.pending.clinicas.sort((a,b)=> a.norm.localeCompare(b.norm));
  state.pending.cirugias.sort((a,b)=> a.key.localeCompare(b.key));

  setPills();
  setButtons();
  paintPreview();
}

/* =========================
   Persist mappings
========================= */
async function persistMapping(docRef, key, id){
  if(!key || !id) return;

  await setDoc(docRef, {
    map: { [key]: { id, actualizadoEl: serverTimestamp(), actualizadoPor: state.user?.email || '' } },
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  if(docRef === docMapClinicas) state.maps.clinicas.set(key, { id });
  if(docRef === docMapCirugias) state.maps.cirugias.set(key, { id });
  if(docRef === docMapAmb) state.maps.amb.set(key, { id });
}

function suggestClinicaId(){
  const tail = (Date.now() % 1000).toString().padStart(3,'0');
  return `C${tail}`;
}

/* =========================
   Resolver modal paint
========================= */
function paintResolverModal(){
  const pc = state.pending.clinicas.length;
  const ps = state.pending.cirugias.length;
  const pa = state.pending.amb.length;
  const total = pc + ps + pa;

  $('resolverResumen').innerHTML = `
    Pendientes totales: <b>${total}</b><br/>
    Clínicas: <b>${pc}</b> · Cirugías: <b>${ps}</b> · Ambulatorios: <b>${pa}</b>
  `;

  // CLINICAS
  const wrapC = $('resolverClinicasList');
  wrapC.innerHTML = '';
  if(pc === 0){
    wrapC.innerHTML = `<div class="muted tiny">✅ Sin pendientes de clínicas.</div>`;
  } else {
    for(const item of state.pending.clinicas){
      const row = document.createElement('div');
      row.className = 'miniRow';

      const options = state.catalogs.clinicas
        .map(c=> `<option value="${escapeHtml(c.id)}">${escapeHtml(`${c.nombre} (${c.id})`)}</option>`)
        .join('');

      row.innerHTML = `
        <div>
          <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
          <div class="muted tiny mono">key: ${escapeHtml(item.norm)}</div>
        </div>

        <div>
          <label>Asociar a</label>
          <select data-assoc-clin="${escapeHtml(item.norm)}">
            <option value="">(Seleccionar clínica)</option>
            ${options}
          </select>
        </div>

        <div class="rowBtns">
          <button class="btn soft sm" data-create-clin="${escapeHtml(item.norm)}" type="button">+ Crear</button>
          <button class="btn primary sm" data-save-clin="${escapeHtml(item.norm)}" type="button">Guardar</button>
        </div>
      `;

      row.querySelector(`[data-save-clin="${CSS.escape(item.norm)}"]`).addEventListener('click', async ()=>{
        const sel = row.querySelector(`select[data-assoc-clin="${CSS.escape(item.norm)}"]`);
        const id = sel.value || '';
        if(!id){ toast('Selecciona una clínica'); return; }
        await persistMapping(docMapClinicas, item.norm, id);
        toast('Clínica asociada');
        await refreshAfterMapping();
        paintResolverModal();
      });

      row.querySelector(`[data-create-clin="${CSS.escape(item.norm)}"]`).addEventListener('click', async ()=>{
        const nombre = item.csvName;
        const suggested = suggestClinicaId();
        const id = prompt('ID de clínica (ej: C001). Puedes editar:', suggested) || '';
        const finalId = clean(id);
        if(!finalId){ toast('Cancelado'); return; }

        await setDoc(doc(db,'clinicas', finalId), {
          id: finalId,
          nombre,
          creadoEl: serverTimestamp(),
          creadoPor: state.user?.email || '',
          actualizadoEl: serverTimestamp(),
          actualizadoPor: state.user?.email || ''
        }, { merge:true });

        await persistMapping(docMapClinicas, item.norm, finalId);

        toast('Clínica creada y asociada');
        await refreshAfterMapping();
        paintResolverModal();
      });

      wrapC.appendChild(row);
    }
  }

  // AMBULATORIOS placeholder
  $('resolverAmbList').innerHTML =
    `<div class="muted tiny">— (Tu CSV actual no trae ambulatorios. Cuando lo agregues, aquí aparecerán.)</div>`;

  // CIRUGIAS (por key contextual)
  const wrapS = $('resolverCirugiasList');
  wrapS.innerHTML = '';
  if(ps === 0){
    wrapS.innerHTML = `<div class="muted tiny">✅ Sin pendientes de cirugías.</div>`;
  } else {
    for(const item of state.pending.cirugias){
      const row = document.createElement('div');
      row.className = 'miniRow';

      const options = state.catalogs.cirugias
        .map(s=> `<option value="${escapeHtml(s.id)}">${escapeHtml(`${s.nombre} (${s.codigo})`)}</option>`)
        .join('');

      const sug = (item.suggestions || [])
        .map(x=> `<button class="pill" style="cursor:pointer;" data-sug-key="${escapeHtml(item.key)}" data-sug-id="${escapeHtml(x.id)}" type="button">${escapeHtml(x.nombre)}</button>`)
        .join(' ');

      row.innerHTML = `
        <div>
          <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
          <div class="muted tiny mono">key: ${escapeHtml(item.key)}</div>

          <div class="ctxPills">
            <span class="ctxPill tp">Tipo: ${escapeHtml(item.tipoPaciente)}</span>
            <span class="ctxPill cl">Clínica: ${escapeHtml(item.clinTxt)}${item.clinicaId ? ` (${escapeHtml(item.clinicaId)})` : ''}</span>
            <span class="ctxPill cx">Cirugía CSV: ${escapeHtml(item.csvName)}</span>
          </div>

          <div class="help" style="margin-top:8px;">
            Sugerencias rápidas:
            <div style="margin-top:6px; display:flex; gap:8px; flex-wrap:wrap;">
              ${sug || '<span class="muted tiny">Sin sugerencias.</span>'}
            </div>
          </div>
        </div>

        <div>
          <label>Asociar a</label>
          <select data-assoc-cir="${escapeHtml(item.key)}">
            <option value="">(Seleccionar cirugía)</option>
            ${options}
          </select>
          <div class="help">
            o <button class="linkBtn" data-go-cir="${escapeHtml(item.key)}" type="button">crear en Cirugías</button>
          </div>
        </div>

        <div class="rowBtns">
          <button class="btn primary sm" data-save-cir="${escapeHtml(item.key)}" type="button">Guardar</button>
        </div>
      `;

      // sugerencias -> set select
      row.querySelectorAll(`[data-sug-key="${CSS.escape(item.key)}"]`).forEach(btn=>{
        btn.addEventListener('click', ()=>{
          const id = btn.getAttribute('data-sug-id') || '';
          const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
          sel.value = id;
        });
      });

      // guardar mapping contextual
      row.querySelector(`[data-save-cir="${CSS.escape(item.key)}"]`).addEventListener('click', async ()=>{
        const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
        const id = sel.value || '';
        if(!id){ toast('Selecciona una cirugía'); return; }
        await persistMapping(docMapCirugias, item.key, id);
        toast('Cirugía asociada (contextual)');
        await refreshAfterMapping();
        paintResolverModal();
      });

      // ir a cirugías para crear (prefill)
      row.querySelector(`[data-go-cir="${CSS.escape(item.key)}"]`).addEventListener('click', ()=>{
        try{
          localStorage.setItem('CR_PREFILL_CIRUGIA_NOMBRE', item.csvName);
          localStorage.setItem('CR_RETURN_TO', 'produccion.html');
          localStorage.setItem('CR_RETURN_IMPORTID', state.importId || '');
          // guardamos también contexto por si quieres usarlo en cirugias.html luego
          localStorage.setItem('CR_PREFILL_CTX_CLINICA', item.clinicaId || item.clinTxt || '');
          localStorage.setItem('CR_PREFILL_CTX_TIPO', item.tipoPaciente || '');
        }catch(e){ /* ignore */ }
        window.location.href = 'cirugias.html';
      });

      wrapS.appendChild(row);
    }
  }
}

/* =========================
   Staging save
========================= */
async function saveStagingToFirestore(){
  const importId = state.importId;
  const refImport = doc(db, 'produccion_imports', importId);

  const meta = {
    id: importId,
    ym: state.ym,
    mes: state.monthName,
    mesNum: state.monthNum,
    ano: state.year,
    filename: state.filename,
    estado: 'staged',
    filas: state.stagedItems.length,
    creadoEl: serverTimestamp(),
    creadoPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  await setDoc(refImport, meta, { merge:true });

  const itemsCol = collection(db, 'produccion_imports', importId, 'items');
  const chunkSize = 400;
  let idx = 0;

  while(idx < state.stagedItems.length){
    const batch = writeBatch(db);
    const slice = state.stagedItems.slice(idx, idx + chunkSize);

    slice.forEach((it, k)=>{
      const itemId = `ITEM_${pad(idx + k + 1, 4)}`;
      const refItem = doc(itemsCol, itemId);

      batch.set(refItem, {
        importId,
        itemId,
        idx: idx + k + 1,
        estado: 'staged',
        raw: it.raw,
        normalizado: it.normalizado,
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge:true });
    });

    await batch.commit();
    idx += chunkSize;
  }
}

/* =========================
   Confirmar / Anular
========================= */
async function confirmarImportacion(){
  if(state.status !== 'staged'){
    toast('No hay importación en staging para confirmar.');
    return;
  }

  const totalPend = state.pending.clinicas.length + state.pending.cirugias.length + state.pending.amb.length;
  if(totalPend > 0){
    toast('Aún hay pendientes. Resuélvelos antes de confirmar.');
    return;
  }

  const importId = state.importId;
  const refImport = doc(db, 'produccion_imports', importId);

  // leer items staging
  const itemsCol = collection(db, 'produccion_imports', importId, 'items');
  const itemsSnap = await getDocs(itemsCol);
  if(itemsSnap.empty){
    toast('No hay items en la importación.');
    return;
  }

  // nuevo esquema: produccion/{YYYY-MM}/items/{PACIENTE_ID}
  const ym = state.ym;
  const colMesItems = collection(db, 'produccion', ym, 'items');

  const docs = [];
  itemsSnap.forEach(d => docs.push({ id: d.id, data: d.data() || {} }));

  const batchSize = 350;
  let i = 0;

  while(i < docs.length){
    const batch = writeBatch(db);
    const slice = docs.slice(i, i + batchSize);

    slice.forEach(({id, data})=>{
      const n = data.normalizado || {};
      const raw = data.raw || {};

      const resolved = resolveOneItem(n);

      // ID paciente (determinístico)
      const pacienteId = `PAC_${ym}_${importId}_${id}`; // único

      const fechaISO = parseFechaISO(n.fecha);
      const horaHHMM = parseHoraHHMM(n.hora);
      const fechaHoraISO = (fechaISO && horaHHMM) ? `${fechaISO}T${horaHHMM}:00` : null;

      const refPaciente = doc(colMesItems, pacienteId);

      batch.set(refPaciente, {
        id: pacienteId,

        ym,
        importId,
        importItemId: id,

        // fecha/hora
        fecha: n.fecha ?? null,
        hora: n.hora ?? null,
        fechaISO,
        horaHHMM,
        fechaHoraISO,

        // textos
        clinica: n.clinica ?? null,
        cirugia: n.cirugia ?? null,
        tipoPaciente: n.tipoPaciente ?? null,
        prevision: raw['Previsión'] ?? null,

        nombrePaciente: raw['Nombre Paciente'] ?? null,
        rut: raw['RUT'] ?? null,
        telefono: raw['Teléfono'] ?? null,
        direccion: raw['Dirección'] ?? null,
        email: raw['e-mail'] ?? null,
        sexo: raw['Sexo'] ?? null,
        fechaNac: raw['Fecha nac. (dd/mm/aaaa)'] ?? null,
        edad: raw['Edad'] ?? null,

        // IDs resueltos
        clinicaId: resolved.clinicaId ?? null,
        cirugiaId: resolved.cirugiaId ?? null,
        cirugiaKey: resolved.cirugiaKey ?? null,
        ambulatorioId: null,

        // valores
        valor: Number(n.valor || 0) || 0,
        derechosPabellon: Number(n.dp || 0) || 0,
        hmq: Number(n.hmq || 0) || 0,
        insumos: Number(n.ins || 0) || 0,

        // flags
        suspendida: n.suspendida,
        confirmado: n.confirmado,
        pagado: n.pagado,
        fechaPago: n.fechaPago ?? null,

        // profesionales
        profesionales: n.profesionales || {},

        // raw completo compacto
        raw,

        estado: 'activa',
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge:true });
    });

    await batch.commit();
    i += batchSize;
  }

  // marcar import como confirmada
  await setDoc(refImport, {
    estado: 'confirmada',
    ym,
    confirmadoEl: serverTimestamp(),
    confirmadoPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  state.status = 'confirmada';
  setStatus(`✅ Importación confirmada: ${importId} → produccion/${ym}/items`);
  setButtons();
  toast('Importación confirmada (nuevo esquema)');
}

async function anularImportacion(){
  if(!state.importId){
    toast('No hay importación para anular.');
    return;
  }

  const ok = confirm(`¿Anular importación?\n\n${state.importId}\n\n(No se borra, solo se marca como anulada)`);
  if(!ok) return;

  const importId = state.importId;
  const ym = state.ym;

  // cabecera import
  await setDoc(doc(db,'produccion_imports', importId), {
    estado: 'anulada',
    anuladaEl: serverTimestamp(),
    anuladaPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  // desactivar pacientes en produccion/{ym}/items por importId (paginado)
  const colMesItems = collection(db, 'produccion', ym, 'items');
  let last = null;
  let total = 0;

  while(true){
    const qy = last
      ? query(colMesItems, where('importId','==', importId), orderBy('__name__'), startAfter(last), limit(350))
      : query(colMesItems, where('importId','==', importId), orderBy('__name__'), limit(350));

    const snap = await getDocs(qy);
    if(snap.empty) break;

    const batch = writeBatch(db);
    snap.forEach(d=>{
      batch.set(d.ref, {
        estado: 'anulada',
        anuladaEl: serverTimestamp(),
        anuladaPor: state.user?.email || ''
      }, { merge:true });
      total++;
    });

    await batch.commit();
    last = snap.docs[snap.docs.length - 1];
  }

  state.status = 'anulada';
  setStatus(`⛔ Importación anulada: ${importId} (${total} pacientes desactivados en ${ym})`);
  setButtons();
  toast('Importación anulada');
}

/* =========================
   Refresh pipeline
========================= */
async function refreshAfterMapping(){
  await loadMappings();
  await loadCatalogs();
  recomputePending();

  setStatus(state.status === 'staged'
    ? `🟡 Staging: ${state.stagedItems.length} filas · ImportID: ${state.importId} · YM: ${state.ym}`
    : (state.status === 'confirmada'
      ? `✅ Confirmada: ${state.importId} → produccion/${state.ym}/items`
      : (state.status === 'anulada'
        ? `⛔ Anulada: ${state.importId}`
        : '—'
      )
    )
  );
}

/* =========================
   Load CSV flow
========================= */
async function handleLoadCSV(file){
  if(!file){ toast('Selecciona un archivo CSV'); return; }

  const mes = clean($('mes').value);
  const ano = Number($('ano').value || 0) || 0;
  if(!ano || ano < 2020){ toast('Año inválido'); return; }

  const text = await file.text();
  const rows = parseCSV(text);

  if(rows.length < 2){ toast('CSV vacío o inválido'); return; }

  const header = rows[0].map(h=> clean(h));
  const headerIdx = buildHeaderIndex(header);

  const missing = validateMinimum(headerIdx);
  if(missing.length){
    toast(`CSV no trae columnas mínimas: ${missing.join(', ')}`);
    console.warn('Header detectado:', header);
    return;
  }

  const staged = [];
  for(let i=1;i<rows.length;i++){
    const raw = compactRaw(rows[i], headerIdx);
    if(Object.keys(raw).length === 0) continue;

    const normalizado = buildNormalizado(raw);
    staged.push({ idx: i, raw, normalizado, resolved: null });
  }

  if(!staged.length){ toast('No se encontraron filas válidas.'); return; }

  const mesNum = monthIndex(mes);
  const ym = monthKey(ano, mesNum);
  const importId = `PROD_${ym}_${nowId()}`;

  state.importId = importId;
  state.status = 'staged';
  state.monthName = mes;
  state.monthNum = mesNum;
  state.year = ano;
  state.ym = ym;
  state.filename = file.name;
  state.stagedItems = staged;

  $('importId').value = importId;

  setStatus(`🟡 Staging listo: ${staged.length} pacientes (YM ${ym})`);
  await saveStagingToFirestore();
  toast('Staging guardado en Firestore');

  await refreshAfterMapping();
}

/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    await loadSidebar({ active:'produccion' });
    setActiveNav('produccion');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    $('mes').value = 'Octubre';
    setStatus('—');

    // carga inicial
    await loadMappings();
    await loadCatalogs();
    recomputePending();
    setButtons();
    paintPreview();

    // eventos
    $('btnCargar').addEventListener('click', async ()=>{
      const f = $('fileCSV').files?.[0];
      try{ await handleLoadCSV(f); }
      catch(err){ console.error(err); toast('Error cargando CSV (ver consola)'); }
    });

    $('btnResolver').addEventListener('click', openResolverModal);

    $('btnConfirmar').addEventListener('click', async ()=>{
      try{ await confirmarImportacion(); }
      catch(err){ console.error(err); toast('Error confirmando (ver consola)'); }
    });

    $('btnAnular').addEventListener('click', async ()=>{
      try{ await anularImportacion(); }
      catch(err){ console.error(err); toast('Error anulando (ver consola)'); }
    });

    // modal resolver
    $('btnResolverClose').addEventListener('click', closeResolverModal);
    $('btnResolverCancelar').addEventListener('click', closeResolverModal);
    $('btnResolverRevisar').addEventListener('click', async ()=>{
      await refreshAfterMapping();
      toast('Pendientes recalculados');
      paintResolverModal();
    });
    $('modalResolverBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalResolverBackdrop')) closeResolverModal();
    });

    // modal detalle
    $('btnDetalleClose').addEventListener('click', closeDetalleModal);
    $('btnDetalleCerrar').addEventListener('click', closeDetalleModal);
    $('modalDetalleBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalDetalleBackdrop')) closeDetalleModal();
    });
  }
});
