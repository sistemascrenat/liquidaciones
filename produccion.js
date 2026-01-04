// produccion.js — COMPLETO (mejorado, sin romper lo que ya funciona)
// ✅ Importación CSV (staging + confirmar + anular)
// ✅ Guarda TODAS las columnas del CSV en raw, pero NUNCA guarda strings vacíos
// ✅ Resolución persistente (Clínicas / Cirugías / Ambulatorios placeholder)
// ✅ Cirugías: pendientes se resuelven por CONTEXTO real: Clínica + Tipo Paciente + Cirugía (CSV)
// ✅ Confirmar bloqueado si hay pendientes
// ✅ Confirmar guarda en Firestore: produccion/{YYYY-MM}/items/{...}  (como pediste)
// ✅ Preview: muestra TODAS las columnas del CSV + Estado
// ✅ Preview paginado: 60 por página + tabs + buscador global
// ✅ Modal resolver scrolleable (HTML ya lo corrige)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { loadSidebar } from './layout.js';

import {
  collection, doc, setDoc, getDoc, getDocs, writeBatch,
  serverTimestamp, query, where, limit, orderBy, startAfter
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   DOM helpers
========================= */
const $ = (id)=> document.getElementById(id);

/* =========================
   Columnas EXACTAS del CSV (como tú las listaste)
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
  return undefined;
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

function monthId(year, monthNum){
  return `${year}-${pad(monthNum,2)}`; // YYYY-MM
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
      row.push(cur);
      cur = '';
      continue;
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
const colImports = collection(db, 'produccion_imports');

// Catálogos (compatibles con tu cirugias.js)
const colClinicas = collection(db, 'clinicas');
const colProcedimientos = collection(db, 'procedimientos');
const colAmbulatorios = collection(db, 'ambulatorios');

// Mappings persistentes
const docMapClinicas = doc(db, 'produccion_mappings', 'clinicas');
const docMapCirugias  = doc(db, 'produccion_mappings', 'cirugias');      // key compuesto
const docMapAmb       = doc(db, 'produccion_mappings', 'ambulatorios');  // placeholder

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
  filename: '',

  stagedItems: [], // [{ idx, raw, normalizado, resolved, _search }]

  ui: {
    pageSize: 60,
    page: 1,
    query: ''
  },
  view: {
    filtered: []
  },

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
    clinicas: new Map(),  // normClinicaCsv -> {id}
    cirugias: new Map(),  // keyCompuesto -> {id}
    amb: new Map()
  },

  pending: {
    clinicas: [], // [{csvName, norm}]
    cirugias: [], // [{key, csvName, normCir, clinicaCsv, normClin, tipoCsv, normTipo, suggestions:[]}]
    amb: []
  }
};

/* =========================
   UI helpers
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

/* =========================
   Preview table: header + pagination + search
========================= */
function buildThead(){
  const ths = [
    `<th>#</th>`,
    ...EXPECTED_COLS.map(c => `<th>${escapeHtml(c)}</th>`),
    `<th>Estado</th>`
  ].join('');

  $('thead').innerHTML = `<tr>${ths}</tr>`;
}

function buildSearchText(it){
  if(it._search) return it._search;
  const raw = it.raw || {};
  const n = it.normalizado || {};
  const rawText = Object.entries(raw).map(([k,v])=>`${k}:${v}`).join(' | ');
  const normText = [
    n.fecha,n.hora,n.clinica,n.cirugia,n.tipoPaciente,
    n.profesionalesResumen
  ].filter(Boolean).join(' | ');

  it._search = normalizeKey(`${rawText} | ${normText}`);
  return it._search;
}

function applyFilter(){
  const q = normalizeKey(state.ui.query || '');
  if(!q){
    state.view.filtered = [...state.stagedItems];
  } else {
    state.view.filtered = state.stagedItems.filter(it => buildSearchText(it).includes(q));
  }

  const total = state.view.filtered.length;
  const pages = Math.max(1, Math.ceil(total / state.ui.pageSize));
  if(state.ui.page > pages) state.ui.page = pages;
  if(state.ui.page < 1) state.ui.page = 1;
}

function paintPager(){
  const total = state.view.filtered.length;
  const pageSize = state.ui.pageSize;
  const pages = Math.max(1, Math.ceil(total / pageSize));
  const page = state.ui.page;

  const from = total === 0 ? 0 : ((page - 1) * pageSize + 1);
  const to = Math.min(total, page * pageSize);

  $('pagerInfo').textContent = total === 0
    ? `0 resultados`
    : `Mostrando ${from}-${to} de ${total} · Página ${page}/${pages}`;

  $('btnPrev').disabled = (page <= 1);
  $('btnNext').disabled = (page >= pages);

  const tabs = $('pagerTabs');
  tabs.innerHTML = '';

  const maxTabs = 9;
  const half = Math.floor(maxTabs / 2);
  let start = Math.max(1, page - half);
  let end = Math.min(pages, start + maxTabs - 1);
  start = Math.max(1, end - maxTabs + 1);

  function addTab(label, target, active=false){
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'btn small' + (active ? ' primary' : '');
    b.textContent = label;
    b.addEventListener('click', ()=>{
      state.ui.page = target;
      paintPreview();
    });
    tabs.appendChild(b);
  }

  if(start > 1){
    addTab('1', 1, page === 1);
    if(start > 2){
      const dot = document.createElement('span');
      dot.className = 'muted tiny';
      dot.textContent = '…';
      dot.style.alignSelf = 'center';
      tabs.appendChild(dot);
    }
  }

  for(let p=start;p<=end;p++){
    addTab(String(p), p, p===page);
  }

  if(end < pages){
    if(end < pages-1){
      const dot = document.createElement('span');
      dot.className = 'muted tiny';
      dot.textContent = '…';
      dot.style.alignSelf = 'center';
      tabs.appendChild(dot);
    }
    addTab(String(pages), pages, page===pages);
  }
}

function estadoCell(it){
  const r = it.resolved || {};
  const flags = [];
  if(r.clinicaOk === false) flags.push('Clínica');
  if(r.cirugiaOk === false) flags.push('Cirugía');
  if(r.ambOk === false) flags.push('Amb');

  if(flags.length === 0) return `<span class="ok">OK</span>`;
  return `<span class="warn">Pendiente: ${escapeHtml(flags.join(', '))}</span>`;
}

function formatCell(colName, rawVal){
  // formateos útiles sin alterar el raw
  if(rawVal === undefined || rawVal === null || clean(rawVal) === '') return `<span class="muted">—</span>`;

  const v = rawVal;

  if(colName === 'Valor' || colName === 'Derechos de Pabellón' || colName === 'HMQ' || colName === 'Insumos'){
    const num = parseCLPNumber(v);
    return `<b>${clp(num)}</b>`;
  }

  if(colName === 'Suspendida' || colName === 'Confirmado' || colName === 'Pagado'){
    const b = toBool(v);
    if(b === true) return `<span class="ok">Sí</span>`;
    if(b === false) return `<span class="muted">No</span>`;
    return escapeHtml(v);
  }

  // texto
  return escapeHtml(v);
}

function paintPreview(){
  applyFilter();

  const tb = $('tbody');
  tb.innerHTML = '';

  // pill cuenta total cargada (no filtrada)
  $('countPill').textContent = `${state.stagedItems.length} fila${state.stagedItems.length===1?'':'s'}`;

  const rows = state.view.filtered || [];
  const total = rows.length;

  const pageSize = state.ui.pageSize;
  const page = state.ui.page;
  const start = (page - 1) * pageSize;
  const slice = rows.slice(start, start + pageSize);

  for(let i=0;i<slice.length;i++){
    const it = slice[i];
    const raw = it.raw || {};

    const tr = document.createElement('tr');

    const tds = [];
    tds.push(`<td class="mono">${pad(start + i + 1,2)}</td>`);

    // TODAS las columnas del CSV, en orden esperado
    for(const col of EXPECTED_COLS){
      const val = raw[col];
      // algunos campos largos mejor wrap
      const wrapClass = (col === 'Dirección' || col === 'Otras' || col === 'Ex. Laboratorio') ? 'wrap' : '';
      tds.push(`<td class="${wrapClass}">${formatCell(col, val)}</td>`);
    }

    tds.push(`<td>${estadoCell(it)}</td>`);

    tr.innerHTML = tds.join('');
    tb.appendChild(tr);
  }

  if(total === 0){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan="${EXPECTED_COLS.length + 2}" class="muted tiny">Sin resultados para el filtro.</td>`;
    tb.appendChild(tr);
  }

  paintPager();
}

/* =========================
   Core: header mapping
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

    suspendida: suspendida ?? null,
    confirmado: confirmado ?? null,
    pagado: pagado ?? null,
    fechaPago: fechaPago || null,

    profesionales: prof,
    profesionalesResumen: parts.join(' · ') || null
  };
}

function validateMinimum(headerIdx){
  const needed = ['Fecha','Clínica','Cirugía','Tipo de Paciente','Valor'];
  return needed.filter(k => headerIdx.get(k) === undefined);
}

/* =========================
   Load mappings + catalogs
========================= */
async function loadMappings(){
  async function loadOne(docRef){
    const snap = await getDoc(docRef);
    if(!snap.exists()) return new Map();
    const data = snap.data() || {};
    const m = data.map && typeof data.map === 'object' ? data.map : {};
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
      if(!id) return;
      out.push({ id, nombre });
    });
    out.sort((a,b)=> normalizeKey(a.nombre).localeCompare(normalizeKey(b.nombre)));
    state.catalogs.clinicas = out;
    state.catalogs.clinicasByNorm = new Map(out.map(c=> [normalizeKey(c.nombre), c]));
    state.catalogs.clinicasById   = new Map(out.map(c=> [c.id, c]));
  }

  // AMBULATORIOS (placeholder)
  {
    const snap = await getDocs(colAmbulatorios);
    const out = [];
    snap.forEach(d=>{
      const x = d.data() || {};
      const id = clean(x.id) || d.id;
      const nombre = clean(x.nombre) || id;
      if(!id) return;
      out.push({ id, nombre });
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
      if(!id || !nombre) return;
      out.push({ id, nombre, codigo });
    });
    out.sort((a,b)=> normalizeKey(a.nombre).localeCompare(normalizeKey(b.nombre)));
    state.catalogs.cirugias = out;
    state.catalogs.cirugiasByNorm = new Map(out.map(s=> [normalizeKey(s.nombre), s]));
    state.catalogs.cirugiasById   = new Map(out.map(s=> [s.id, s]));
  }
}

/* =========================
   Resolución: clave compuesta para cirugías
========================= */
function cirKey(normClin, normTipo, normCir){
  return `${normClin}||${normTipo}||${normCir}`;
}

function suggestCirugias(normCir){
  const all = state.catalogs.cirugias || [];
  const out = [];
  for(const s of all){
    const nn = normalizeKey(s.nombre);
    if(nn.includes(normCir) || normCir.includes(nn)){
      out.push({ id: s.id, nombre: s.nombre });
      if(out.length >= 8) break;
    }
  }
  return out;
}

function resolveOneItem(n){
  const resolved = {
    clinicaId: null,
    cirugiaId: null,
    ambulatorioId: null,

    clinicaOk: true,
    cirugiaOk: true,
    ambOk: true,

    // context norm (útil para debug)
    _normClin: '',
    _normTipo: '',
    _normCir: '',
    _cirKey: ''
  };

  // --- Clínica ---
  const clinTxt = clean(n.clinica || '');
  if(clinTxt){
    const normClin = normalizeKey(clinTxt);
    resolved._normClin = normClin;

    const mapped = state.maps.clinicas.get(normClin);
    if(mapped?.id){
      resolved.clinicaId = mapped.id;
      resolved.clinicaOk = true;
    } else {
      const found = state.catalogs.clinicasByNorm.get(normClin);
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

  // --- Cirugía (por Clínica + Tipo Paciente + Cirugía CSV) ---
  const cirTxt = clean(n.cirugia || '');
  const tipoTxt = clean(n.tipoPaciente || '');
  if(cirTxt){
    const normCir = normalizeKey(cirTxt);
    const normTipo = normalizeKey(tipoTxt || ''); // si viene vacío, igual forma parte de la key
    const normClin = resolved._normClin || normalizeKey(clinTxt || '');

    resolved._normCir = normCir;
    resolved._normTipo = normTipo;

    const key = cirKey(normClin, normTipo, normCir);
    resolved._cirKey = key;

    // 1) mapping por key compuesta
    const mapped = state.maps.cirugias.get(key);
    if(mapped?.id){
      resolved.cirugiaId = mapped.id;
      resolved.cirugiaOk = true;
    } else {
      // 2) fallback: match exacto por nombre (solo si existe un procedimiento con ese nombre exacto)
      const found = state.catalogs.cirugiasByNorm.get(normCir);
      if(found?.id){
        // OJO: esto resuelve solo si coincide exacto.
        resolved.cirugiaId = found.id;
        resolved.cirugiaOk = true;
      } else {
        resolved.cirugiaOk = false;
      }
    }
  } else {
    resolved.cirugiaOk = false;
  }

  // --- Ambulatorio placeholder ---
  resolved.ambOk = true;
  resolved.ambulatorioId = null;

  return resolved;
}

function recomputePending(){
  state.pending.clinicas = [];
  state.pending.cirugias = [];
  state.pending.amb = [];

  // recalcula resolved por cada fila
  for(const it of state.stagedItems){
    it.resolved = resolveOneItem(it.normalizado || {});
  }

  const seenClin = new Set();
  const seenCir  = new Set();

  for(const it of state.stagedItems){
    const n = it.normalizado || {};
    const r = it.resolved || {};

    // clínicas
    const clinTxt = clean(n.clinica || '');
    if(clinTxt && r.clinicaOk === false){
      const norm = normalizeKey(clinTxt);
      if(!seenClin.has(norm)){
        seenClin.add(norm);
        state.pending.clinicas.push({ csvName: clinTxt, norm });
      }
    }

    // cirugías por contexto
    const cirTxt = clean(n.cirugia || '');
    const tipoTxt = clean(n.tipoPaciente || '');
    const normClin = normalizeKey(clinTxt);
    const normTipo = normalizeKey(tipoTxt);
    const normCir = normalizeKey(cirTxt);

    if(cirTxt && r.cirugiaOk === false){
      const key = cirKey(normClin, normTipo, normCir);
      if(!seenCir.has(key)){
        seenCir.add(key);
        state.pending.cirugias.push({
          key,
          csvName: cirTxt,
          normCir,
          clinicaCsv: clinTxt || '(sin clínica)',
          normClin,
          tipoCsv: tipoTxt || '(sin tipo)',
          normTipo,
          suggestions: suggestCirugias(normCir)
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
   Mappings: persist
========================= */
async function persistMapping(docRef, key, id){
  if(!key || !id) return;

  await setDoc(docRef, {
    map: {
      [key]: { id, actualizadoEl: serverTimestamp(), actualizadoPor: state.user?.email || '' }
    },
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge: true });

  if(docRef === docMapClinicas) state.maps.clinicas.set(key, { id });
  if(docRef === docMapCirugias)  state.maps.cirugias.set(key, { id });
  if(docRef === docMapAmb)       state.maps.amb.set(key, { id });
}

/* =========================
   Modal Resolver
========================= */
function openResolverModal(){
  $('modalResolverBackdrop').style.display = 'block';
  paintResolverModal();
}

function closeResolverModal(){
  $('modalResolverBackdrop').style.display = 'none';
}

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

        <div class="field" style="margin:0;">
          <label>Asociar a</label>
          <select data-assoc-clin="${escapeHtml(item.norm)}">
            <option value="">(Seleccionar clínica)</option>
            ${options}
          </select>
        </div>

        <div style="display:flex; gap:8px; justify-content:flex-end;">
          <button class="btn soft" data-create-clin="${escapeHtml(item.norm)}" type="button">+ Crear</button>
          <button class="btn primary" data-save-clin="${escapeHtml(item.norm)}" type="button">Guardar</button>
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

  // AMBULATORIOS (placeholder)
  const wrapA = $('resolverAmbList');
  wrapA.innerHTML = `<div class="muted tiny">— (Tu CSV actual no trae ambulatorios. Cuando lo agregues, aquí aparecerán.)</div>`;

  // CIRUGIAS (por contexto)
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
        .map(x=> `<span class="pill warn" style="cursor:pointer;" data-sug-key="${escapeHtml(item.key)}" data-sug-id="${escapeHtml(x.id)}">${escapeHtml(x.nombre)}</span>`)
        .join(' ');

      row.innerHTML = `
        <div>
          <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
          <div class="muted tiny">
            <b>Clínica:</b> ${escapeHtml(item.clinicaCsv)} · <b>Tipo:</b> ${escapeHtml(item.tipoCsv)}
          </div>
          <div class="muted tiny mono">key: ${escapeHtml(item.key)}</div>
          ${sug ? `<div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">${sug}</div>` : `<div class="muted tiny" style="margin-top:8px;">Sin sugerencias.</div>`}
        </div>

        <div class="field" style="margin:0;">
          <label>Asociar a</label>
          <select data-assoc-cir="${escapeHtml(item.key)}">
            <option value="">(Seleccionar cirugía)</option>
            ${options}
          </select>
          <div class="help">
            o <button class="linkBtn" data-go-cir="${escapeHtml(item.key)}" type="button">crear en Cirugías</button>
          </div>
        </div>

        <div style="display:flex; gap:8px; justify-content:flex-end;">
          <button class="btn primary" data-save-cir="${escapeHtml(item.key)}" type="button">Guardar</button>
        </div>
      `;

      row.querySelectorAll('[data-sug-key]').forEach(pill=>{
        pill.addEventListener('click', ()=>{
          const id = pill.getAttribute('data-sug-id') || '';
          const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
          sel.value = id;
        });
      });

      row.querySelector(`[data-save-cir="${CSS.escape(item.key)}"]`).addEventListener('click', async ()=>{
        const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
        const id = sel.value || '';
        if(!id){ toast('Selecciona una cirugía'); return; }
        await persistMapping(docMapCirugias, item.key, id);
        toast('Cirugía asociada');
        await refreshAfterMapping();
        paintResolverModal();
      });

      row.querySelector(`[data-go-cir="${CSS.escape(item.key)}"]`).addEventListener('click', ()=>{
        // prefill + contexto
        try{
          localStorage.setItem('CR_PREFILL_CIRUGIA_NOMBRE', item.csvName);
          localStorage.setItem('CR_PREFILL_TIPO_PACIENTE', item.tipoCsv);
          localStorage.setItem('CR_PREFILL_CLINICA', item.clinicaCsv);
          localStorage.setItem('CR_RETURN_TO', 'produccion.html');
          localStorage.setItem('CR_RETURN_IMPORTID', state.importId || '');
        }catch(e){ /* ignore */ }
        window.location.href = 'cirugias.html';
      });

      wrapS.appendChild(row);
    }
  }
}

function suggestClinicaId(){
  const tail = (Date.now() % 1000).toString().padStart(3,'0');
  return `C${tail}`;
}

/* =========================
   Escape HTML
========================= */
function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

/* =========================
   Firestore: staging save
========================= */
async function saveStagingToFirestore(){
  const importId = state.importId;
  const refImport = doc(db, 'produccion_imports', importId);

  const meta = {
    id: importId,
    mes: state.monthName,
    mesNum: state.monthNum,
    ano: state.year,
    monthId: monthId(state.year, state.monthNum),
    filename: state.filename,
    estado: 'staged',
    filas: state.stagedItems.length,
    creadoEl: serverTimestamp(),
    creadoPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  await setDoc(refImport, meta, { merge: true });

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
      }, { merge: true });
    });

    await batch.commit();
    idx += chunkSize;
  }
}

/* =========================
   Confirmar / Anular
   ✅ NUEVO: Confirmar guarda en produccion/{YYYY-MM}/items/{...}
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

  const itemsCol = collection(db, 'produccion_imports', importId, 'items');
  const itemsSnap = await getDocs(itemsCol);
  if(itemsSnap.empty){
    toast('No hay items en la importación.');
    return;
  }

  const mid = monthId(state.year, state.monthNum);
  const refMes = doc(db, 'produccion', mid);
  const colMesItems = collection(db, 'produccion', mid, 'items');

  // aseguramos doc mes (metadata)
  await setDoc(refMes, {
    id: mid,
    ano: state.year,
    mes: state.monthName,
    mesNum: state.monthNum,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

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

      // ID del item en el mes: mezcla import + item para que sea único
      const mesItemId = `${importId}_${id}`;
      const refItem = doc(colMesItems, mesItemId);

      batch.set(refItem, {
        id: mesItemId,
        importId,
        importItemId: id,

        // claves
        fecha: n.fecha ?? null,
        hora: n.hora ?? null,
        clinica: n.clinica ?? null,
        cirugia: n.cirugia ?? null,
        tipoPaciente: n.tipoPaciente ?? null,

        // ids resueltos
        clinicaId: resolved.clinicaId ?? null,
        cirugiaId: resolved.cirugiaId ?? null,
        ambulatorioId: resolved.ambulatorioId ?? null,

        // valores numéricos
        valor: Number(n.valor || 0) || 0,
        derechosPabellon: Number(n.dp || 0) || 0,
        hmq: Number(n.hmq || 0) || 0,
        insumos: Number(n.ins || 0) || 0,

        // flags
        suspendida: n.suspendida ?? null,
        confirmado: n.confirmado ?? null,
        pagado: n.pagado ?? null,
        fechaPago: n.fechaPago ?? null,

        // profesionales
        profesionales: n.profesionales || {},

        // raw completo
        raw,

        estado: 'activa',
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge:true });
    });

    await batch.commit();
    i += batchSize;
  }

  await setDoc(refImport, {
    estado: 'confirmada',
    confirmadoEl: serverTimestamp(),
    confirmadoPor: state.user?.email || '',
    monthId: mid,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge: true });

  state.status = 'confirmada';
  setStatus(`✅ Importación confirmada: ${importId} → produccion/${mid}/items`);
  setButtons();
  toast('Importación confirmada');
}

async function anularImportacion(){
  if(!state.importId){
    toast('No hay importación para anular.');
    return;
  }

  const ok = confirm(`¿Anular importación?\n\n${state.importId}\n\n(No se borra, solo se marca como anulada)`);
  if(!ok) return;

  const importId = state.importId;
  const refImport = doc(db, 'produccion_imports', importId);

  await setDoc(refImport, {
    estado: 'anulada',
    anuladaEl: serverTimestamp(),
    anuladaPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge: true });

  // ✅ Anular también en produccion/{YYYY-MM}/items donde importId == ...
  // Asumimos que anulas el import actual (mismo mes seleccionado)
  const mid = monthId(state.year, state.monthNum);
  const colMesItems = collection(db, 'produccion', mid, 'items');

  let last = null;
  let total = 0;

  while(true){
    const qy = last
      ? query(colMesItems, where('importId','==', importId), orderBy('__name__'), startAfter(last), limit(400))
      : query(colMesItems, where('importId','==', importId), orderBy('__name__'), limit(400));

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
  setStatus(`⛔ Importación anulada: ${importId} (${total} filas desactivadas en produccion/${mid}/items)`);
  setButtons();
  toast('Importación anulada');
}

/* =========================
   Load CSV flow
========================= */
async function handleLoadCSV(file){
  if(!file){
    toast('Selecciona un archivo CSV');
    return;
  }

  const mes = clean($('mes').value);
  const ano = Number($('ano').value || 0) || 0;

  if(!ano || ano < 2020){
    toast('Año inválido');
    return;
  }

  const text = await file.text();
  const rows = parseCSV(text);

  if(rows.length < 2){
    toast('CSV vacío o inválido');
    return;
  }

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
    const row = rows[i];
    const raw = compactRaw(row, headerIdx);
    if(Object.keys(raw).length === 0) continue;

    const normalizado = buildNormalizado(raw);

    staged.push({ idx: i, raw, normalizado, resolved: null, _search: null });
  }

  if(!staged.length){
    toast('No se encontraron filas válidas.');
    return;
  }

  const mm = pad(monthIndex(mes), 2);
  const importId = `PROD_${ano}_${mm}_${nowId()}`;

  state.importId = importId;
  state.status = 'staged';
  state.monthName = mes;
  state.monthNum = monthIndex(mes);
  state.year = ano;
  state.filename = file.name;
  state.stagedItems = staged;

  // reset buscador/página
  state.ui.page = 1;
  state.ui.query = '';
  if($('q')) $('q').value = '';

  $('importId').value = importId;

  setStatus(`🟡 Staging listo: ${staged.length} filas (sin afectar liquidaciones)`);
  buildThead();

  await saveStagingToFirestore();
  toast('Staging guardado en Firestore');

  await refreshAfterMapping();
}

/* =========================
   Refresh pipeline after changes
========================= */
async function refreshAfterMapping(){
  await loadMappings();
  await loadCatalogs();

  recomputePending();

  setStatus(
    state.status === 'staged'
      ? `🟡 Staging: ${state.stagedItems.length} filas · ImportID: ${state.importId}`
      : (state.status === 'confirmada'
          ? `✅ Confirmada: ${state.importId}`
          : (state.status === 'anulada'
              ? `⛔ Anulada: ${state.importId}`
              : '—'
            )
        )
  );
}

/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    await loadSidebar({ active: 'produccion' });
    setActiveNav('produccion');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    // defaults
    $('mes').value = 'Octubre';
    buildThead();

    setStatus('—');

    await loadMappings();
    await loadCatalogs();

    recomputePending();
    setButtons();
    paintPreview();

    // Eventos
    $('btnCargar').addEventListener('click', async ()=>{
      const f = $('fileCSV').files?.[0];
      try{
        await handleLoadCSV(f);
      }catch(err){
        console.error(err);
        toast('Error cargando CSV (ver consola)');
      }
    });

    $('btnResolver').addEventListener('click', ()=>{
      openResolverModal();
    });

    $('btnConfirmar').addEventListener('click', async ()=>{
      try{
        await confirmarImportacion();
      }catch(err){
        console.error(err);
        toast('Error confirmando (ver consola)');
      }
    });

    $('btnAnular').addEventListener('click', async ()=>{
      try{
        await anularImportacion();
      }catch(err){
        console.error(err);
        toast('Error anulando (ver consola)');
      }
    });

    // Buscador global
    $('q').addEventListener('input', ()=>{
      state.ui.query = $('q').value || '';
      state.ui.page = 1;
      paintPreview();
    });

    // Pager prev/next
    $('btnPrev').addEventListener('click', ()=>{
      if(state.ui.page > 1){
        state.ui.page--;
        paintPreview();
      }
    });

    $('btnNext').addEventListener('click', ()=>{
      applyFilter();
      const pages = Math.max(1, Math.ceil(state.view.filtered.length / state.ui.pageSize));
      if(state.ui.page < pages){
        state.ui.page++;
        paintPreview();
      }
    });

    // Modal resolver
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
  }
});
