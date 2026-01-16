// produccion.js — COMPLETO (renovado)
// ✅ Sidebar mantiene ancho (HTML)
// ✅ Ignora filas "basura": si Nombre Paciente y RUT vienen vacíos => NO se importa esa línea
// ✅ Resolución cirugías por contexto: Clínica + Tipo Paciente + Cirugía
// ✅ Preview paginado (60) + buscador global
// ✅ Confirmar guarda en Firestore como: produccion/{YYYY}/meses/{MM}/pacientes/{RUT}/items/{FECHAISO}_{HHMM}
// ✅ Anular: marca import como anulada + marca items por importId vía collectionGroup('items') (filtrado por ano/mesNum)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { loadSidebar } from './layout.js';

import {
  collection, doc, setDoc, getDoc, getDocs, writeBatch, updateDoc,
  serverTimestamp, query, where, limit, orderBy, startAfter,
  collectionGroup
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   DOM
========================= */
const $ = (id)=> document.getElementById(id);

/* =========================
   Columnas esperadas (como pediste)
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

function normalizeProName(s){
  // Normaliza nombres tipo "Dr. Cortés", "Dra Paula Contreras", etc.
  let x = normalizeKey(s);

  // quitar prefijos comunes
  x = x
    .replace(/\bdr\b\.?/g,'')
    .replace(/\bdra\b\.?/g,'')
    .replace(/\bdoctor\b/g,'')
    .replace(/\bdoctora\b/g,'')
    .replace(/\bprof\b\.?/g,'')
    .replace(/\bprofesor\b/g,'')
    .replace(/\bprofa\b\.?/g,'')
    .replace(/\bprofa\b/g,'')
    .replace(/\s+/g,' ')
    .trim();

  return x;
}

function profKey(roleId, profNameCsv){
  return `${clean(roleId)}||${normalizeProName(profNameCsv)}`;
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
  return `${year}-${pad(monthNum,2)}`;
}

/* ---- Fecha/Hora para docId FECHAISO_HHMM ---- */
function parseDateToISO(s){
  const t = clean(s);
  if(!t) return '';
  // yyyy-mm-dd
  let m = t.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
  if(m){
    const yyyy = m[1], mm = pad(m[2],2), dd = pad(m[3],2);
    return `${yyyy}-${mm}-${dd}`;
  }
  // dd-mm-yyyy o dd/mm/yyyy
  m = t.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/);
  if(m){
    const dd = pad(m[1],2), mm = pad(m[2],2), yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }
  return '';
}

function parseHora24(s){
  const t = normalizeKey(s);
  if(!t) return '';
  // ej: "8:00 a.m." / "8:00 am" / "08:00" / "20:15"
  let m = t.match(/^(\d{1,2})[:.](\d{2})\s*(a\.?m\.?|p\.?m\.?|am|pm)?$/);
  if(!m) return '';
  let hh = Number(m[1]);
  const mm = Number(m[2]);
  const ap = (m[3] || '').replace(/\./g,'');
  if(ap === 'am'){
    if(hh === 12) hh = 0;
  } else if(ap === 'pm'){
    if(hh < 12) hh += 12;
  }
  return `${pad(hh,2)}${pad(mm,2)}`; // HHMM (para docId)
}

function normalizeRutKey(rut){
  // DocId seguro: sin puntos/espacios, mayúscula, conserva dígitos y K
  const t = clean(rut).toUpperCase();
  if(!t) return '';
  // quita todo excepto 0-9 y K
  const base = t.replace(/[^0-9K]/g,'');
  return base;
}

const TIPOS_PACIENTE = [
  'Particular / Isapre',
  'Fonasa',
  'MLE'
];


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
      row.push(cur); cur=''; continue;
    }
    if(!inQuotes && (ch === '\n' || ch === '\r')){
      if(ch === '\r' && next === '\n') i++;
      row.push(cur); rows.push(row);
      row=[]; cur=''; continue;
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

const colClinicas = collection(db, 'clinicas');
const colProcedimientos = collection(db, 'procedimientos');
const colAmbulatorios = collection(db, 'ambulatorios');

const docMapClinicas = doc(db, 'produccion_mappings', 'clinicas');
const docMapCirugias  = doc(db, 'produccion_mappings', 'cirugias');
const docMapAmb       = doc(db, 'produccion_mappings', 'ambulatorios');
const docMapProf      = doc(db, 'produccion_mappings', 'profesionales');

/* =========================
   State
========================= */
const state = {
  user: null,

  importId: '',
  status: 'idle',
  monthName: '',
  monthNum: 0,
  year: 0,
  filename: '',

  stagedItems: [],

  ui: { pageSize: 60, page: 1, query: '' },
  view: { filtered: [] },

  catalogs: {
    clinicas: [], clinicasByNorm: new Map(), clinicasById: new Map(),
    cirugias: [], cirugiasByNorm: new Map(), cirugiasById: new Map(),
    amb: [], ambByNorm: new Map(), ambById: new Map(),

    profesionales: [],
    profByNorm: new Map(),
    profById: new Map()
  },

  maps: {
    clinicas: new Map(),
    cirugias: new Map(),
    amb: new Map(),
    prof: new Map()
  },

  pending: { clinicas: [], cirugias: [], amb: [], prof: [] },

  // ✅ NUEVO: pendientes permitidos (por key)
  allowPend: {
    clinicas: new Set(),  // key = normClin
    cirugias: new Set(),  // key = cirKey(...)
    prof: new Set()       // key = role||normName
  },

  // ✅ Cola de cambios (modo “Guardar todo”):
  // key = `${pacienteId}||${timeId}` o `STAGING||${idx}`
  // value = { it, patch, queuedAt }
  dirtyEdits: new Map()
};

/* =========================
   UI helpers
========================= */
function setStatus(text){ $('statusInfo').textContent = text || '—'; }

function setPills(){
  const pc = state.pending.clinicas.length;
  const ps = state.pending.cirugias.length;
  const pa = state.pending.amb.length;
  const pp = state.pending.prof.length;

  const total = pc + ps + pa + pp;

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

  const pillP = $('pillProf'); // si existe en HTML
  if(pillP){
    pillP.textContent = `Profesionales: ${pp}`;
    pillP.className = 'pill ' + (pp === 0 ? 'ok' : 'warn');
  }

  $('hintResolver').textContent = (state.status === 'staged')
    ? (total === 0
        ? '✅ Todo resuelto. Puedes confirmar.'
        : '⚠️ Hay pendientes. Puedes confirmar igual (quedarán marcados como PENDIENTE) o resolver ahora.')
    : 'Cargar CSV → resolver faltantes (opcional) → confirmar.';
}

function setButtons(){
  const staged = state.status === 'staged';
  const confirmed = state.status === 'confirmada';

  const totalPend =
    state.pending.clinicas.length +
    state.pending.cirugias.length +
    state.pending.amb.length +
    state.pending.prof.length; // ✅


  $('btnResolver').disabled = !(staged && totalPend > 0);
  
  // ✅ Confirmar SIEMPRE habilitado mientras haya staging.
  // (Se confirmará con pendientes marcadas en producción)
  $('btnConfirmar').disabled = !staged;
  
  $('btnAnular').disabled = !(staged || confirmed);
}

function dirtyCount(){
  return state.dirtyEdits?.size || 0;
}

function refreshDirtyUI(){
  // 1) Muestra en status
  const n = dirtyCount();
  const base = $('statusInfo')?.textContent || '—';
  // Evita duplicar (lo mantenemos simple: reescribimos desde setStatus en otro patch si quieres)
  if($('dirtyPill')){
    $('dirtyPill').textContent = `Cambios en cola: ${n}`;
    $('dirtyPill').className = 'pill ' + (n === 0 ? 'ok' : 'warn');
  }
}

function enqueueDirtyEdit(key, it, patch){
  state.dirtyEdits.set(key, { it, patch, queuedAt: Date.now() });
  refreshDirtyUI();
}

async function flushDirtyEdits(){
  if(dirtyCount() === 0){
    toast('No hay cambios en cola.');
    return;
  }
  const ok = confirm(`¿Guardar ${dirtyCount()} cambio(s) en cola?`);
  if(!ok) return;

  // Guardado secuencial (seguro). Si luego quieres batch, lo optimizamos.
  for(const [k, v] of state.dirtyEdits.entries()){
    await saveOneItemPatch(v.it, v.patch);
    state.dirtyEdits.delete(k);
  }

  refreshDirtyUI();
  toast('✅ Cola guardada');
}


/* =========================
   Preview table
========================= */
function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

function buildThead(){
  const ths = [
    `<th>#</th>`,
    ...EXPECTED_COLS.map(c => `<th>${escapeHtml(c)}</th>`),
    `<th>Estado</th>`,
    `<th>Acciones</th>`
  ].join('');
  $('thead').innerHTML = `<tr>${ths}</tr>`;
}

function buildSearchText(it){
  if(it._search) return it._search;
  const raw = it.raw || {};
  const n = it.normalizado || {};
  const rawText = Object.entries(raw).map(([k,v])=>`${k}:${v}`).join(' | ');
  const normText = [
    n.fechaISO,n.horaHM,n.clinica,n.cirugia,n.tipoPaciente,
    n.nombrePaciente,n.rut
  ].filter(Boolean).join(' | ');

  it._search = normalizeKey(`${rawText} | ${normText}`);
  return it._search;
}

function applyFilter(){
  const q = normalizeKey(state.ui.query || '');
  state.view.filtered = !q
    ? [...state.stagedItems]
    : state.stagedItems.filter(it => buildSearchText(it).includes(q));

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

  for(let p=start;p<=end;p++) addTab(String(p), p, p===page);

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
  if(r.clinicaOk === false || r._pendClin) flags.push('Clínica');
  if(r.cirugiaOk === false || r._pendCir) flags.push('Cirugía');
  
  if(r.cirujanoOk === false || r._pend_cirujano) flags.push('Cirujano');
  if(r.anestesistaOk === false || r._pend_anestesista) flags.push('Anestesista');
  if(r.ayudante1Ok === false || r._pend_ayudante1) flags.push('Ay1');
  if(r.ayudante2Ok === false || r._pend_ayudante2) flags.push('Ay2');
  if(r.arsenaleraOk === false || r._pend_arsenalera) flags.push('Arsenalera');

  if(flags.length === 0) return `<span class="ok">OK</span>`;
  return `<span class="warn">Pendiente: ${escapeHtml(flags.join(', '))}</span>`;
}

function formatCell(colName, rawVal){
  if(rawVal === undefined || rawVal === null || clean(rawVal) === '') return `<span class="muted">—</span>`;

  if(colName === 'Valor' || colName === 'Derechos de Pabellón' || colName === 'HMQ' || colName === 'Insumos'){
    return `<b>${clp(parseCLPNumber(rawVal))}</b>`;
  }
  if(colName === 'Suspendida' || colName === 'Confirmado' || colName === 'Pagado'){
    const b = toBool(rawVal);
    if(b === true) return `<span class="ok">Sí</span>`;
    if(b === false) return `<span class="muted">No</span>`;
  }
  return escapeHtml(rawVal);
}

function paintPreview(){
  applyFilter();

  $('countPill').textContent = `${state.stagedItems.length} fila${state.stagedItems.length===1?'':'s'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

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

    for(const col of EXPECTED_COLS){
      const val = raw[col];
      const wrapClass = (col === 'Dirección' || col === 'Otras' || col === 'Ex. Laboratorio') ? 'wrap' : '';
      tds.push(`<td class="${wrapClass}">${formatCell(col, val)}</td>`);
    }

    tds.push(`<td>${estadoCell(it)}</td>`);

    // ✅ Detalle (funciona sobre staging también, pero guarda solo si está confirmada)
    tds.push(`<td>
      <button class="btn small" type="button" data-open-item="${escapeHtml(String(it.idx || ''))}">Detalle</button>
    </td>`);

    tr.innerHTML = tds.join('');
    tb.appendChild(tr);

    const btn = tr.querySelector('[data-open-item]');
    if(btn){
      btn.addEventListener('click', ()=> openItemModal(it));
    }
  }

  if(total === 0){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan="${EXPECTED_COLS.length + 2}" class="muted tiny">Sin resultados para el filtro.</td>`;
    tb.appendChild(tr);
  }

  paintPager();
}

/* =========================
   Header mapping
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
  const fechaTxt = raw['Fecha'] ?? '';
  const horaTxt  = raw['Hora'] ?? '';

  const fechaISO = parseDateToISO(fechaTxt);
  const horaHM   = parseHora24(horaTxt); // HHMM

  const clinica = raw['Clínica'] ?? null;
  const cirugia = raw['Cirugía'] ?? null;
  const tipoPaciente = raw['Tipo de Paciente'] ?? null;

  const nombrePaciente = raw['Nombre Paciente'] ?? null;
  const rut = raw['RUT'] ?? null;

  const valor = parseCLPNumber(raw['Valor'] ?? 0);
  const dp = parseCLPNumber(raw['Derechos de Pabellón'] ?? 0);
  const hmq = parseCLPNumber(raw['HMQ'] ?? 0);
  const ins = parseCLPNumber(raw['Insumos'] ?? 0);

  const prof = {
    cirujano: raw['Cirujano'] ?? null,
    anestesista: raw['Anestesista'] ?? null,
    ayudante1: raw['Ayudante 1'] ?? null,
    ayudante2: raw['Ayudante 2'] ?? null,
    arsenalera: raw['Arsenalera'] ?? null
  };

  return {
    fechaISO,
    horaHM,
    clinica: clinica || null,
    cirugia: cirugia || null,
    tipoPaciente: tipoPaciente || null,
    nombrePaciente: nombrePaciente || null,
    rut: rut || null,

    valor, dp, hmq, ins,
    suspendida: toBool(raw['Suspendida']) ?? null,
    confirmado: toBool(raw['Confirmado']) ?? null,
    pagado: toBool(raw['Pagado']) ?? null,
    fechaPago: raw['Fecha de Pago'] ?? null,

    profesionales: prof
  };
}

function validateMinimum(headerIdx){
  const needed = ['Fecha','Clínica','Cirugía','Tipo de Paciente','Valor'];
  return needed.filter(k => headerIdx.get(k) === undefined);
}

/* =========================
   Catalogs + mappings
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
  state.maps.prof      = await loadOne(docMapProf);
}

async function loadCatalogs(){
  // Clínicas
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
    state.catalogs.clinicasById = new Map(out.map(c=> [c.id, c]));
  }

  // Ambulatorios (placeholder)
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
    state.catalogs.ambById = new Map(out.map(a=> [a.id, a]));
  }

  // Cirugías (procedimientos tipo cirugia)
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
    state.catalogs.cirugiasById = new Map(out.map(s=> [s.id, s]));
  }

  // ✅ Profesionales
  {
    const snap = await getDocs(collection(db, 'profesionales'));
    const out = [];
    snap.forEach(d=>{
      const x = d.data() || {};
      const id = clean(d.id); // RUT sin puntos/guion
      const estado = clean(x.estado) || 'activo';

      const nombre =
        clean(x.nombreProfesional) ||      // ✅ tu campo real en Firestore
        clean(x.nombre) ||
        clean(x.nombreCompleto) ||
        clean(x.displayName) ||
        clean(x.apellidos ? `${x.nombres||''} ${x.apellidos||''}` : '') ||
        id;


      if(!id) return;
      out.push({ id, nombre, estado, raw: x });
    });

    const activos = out.filter(p => normalizeKey(p.estado) !== 'inactivo');

    state.catalogs.profesionales = activos;
    state.catalogs.profById = new Map(activos.map(p=> [p.id, p]));

    const idx = new Map();
    for(const p of activos){
      const k = normalizeProName(p.nombre);
      if(!k) continue;
      if(!idx.has(k)) idx.set(k, []);
      idx.get(k).push({ id: p.id, nombre: p.nombre });
    }
    state.catalogs.profByNorm = idx;
  }
}


/* =========================
   Resolución cirugías por contexto
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

    // ✅ Profesionales IDs (RUT)
    cirujanoId: null,
    anestesistaId: null,
    ayudante1Id: null,
    ayudante2Id: null,
    arsenaleraId: null,

    clinicaOk: true,
    cirugiaOk: true,
    ambOk: true,

    // ✅ flags profesionales
    cirujanoOk: true,
    anestesistaOk: true,
    ayudante1Ok: true,
    ayudante2Ok: true,
    arsenaleraOk: true,

    _cirKey: ''
  };

  // Clínica
  const clinTxt = clean(n.clinica || '');
  if(clinTxt){
    const normClin = normalizeKey(clinTxt);
    const mapped = state.maps.clinicas.get(normClin);
    if(mapped?.id){
      resolved.clinicaId = mapped.id;
    } else {
      const found = state.catalogs.clinicasByNorm.get(normClin);
      if(found?.id) resolved.clinicaId = found.id;
      else {
        // ✅ si el usuario decidió “dejar pendiente” esta clínica, no bloquea
        if(state.allowPend.clinicas.has(normClin)){
          resolved.clinicaOk = true; // no bloquea
          resolved.clinicaId = null; // sigue sin id
          resolved._pendClin = true; // marca interna
        } else {
          resolved.clinicaOk = false;
        }
      }

    }
  } else {
    resolved.clinicaOk = false;
  }

  // Cirugía por contexto: clínica + tipo + cirugía
  const cirTxt = clean(n.cirugia || '');
  const tipoTxt = clean(n.tipoPaciente || '');
  if(cirTxt){
    const normClin = normalizeKey(clinTxt || '');
    const normTipo = normalizeKey(tipoTxt || '');
    const normCir = normalizeKey(cirTxt);

    const key = cirKey(normClin, normTipo, normCir);
    resolved._cirKey = key;

    const mapped = state.maps.cirugias.get(key);
    if(mapped?.id){
      resolved.cirugiaId = mapped.id;
    } else {
      const found = state.catalogs.cirugiasByNorm.get(normCir);
      if(found?.id) resolved.cirugiaId = found.id; // solo match exacto por nombre
      else {
        if(state.allowPend.cirugias.has(key)){
          resolved.cirugiaOk = true;
          resolved.cirugiaId = null;
          resolved._pendCir = true;
        } else {
          resolved.cirugiaOk = false;
        }
      }
    }
  } else {
    resolved.cirugiaOk = false;
  }

  // Ambulatorios placeholder
  resolved.ambOk = true;
  resolved.ambulatorioId = null;

    // ✅ Profesionales: resolver por rol + nombre CSV
  const prof = (n.profesionales || {});
  const roles = [
    { role:'r_cirujano',     field:'cirujano',    ok:'cirujanoOk',    out:'cirujanoId' },
    { role:'r_anestesista',  field:'anestesista', ok:'anestesistaOk', out:'anestesistaId' },
    { role:'r_ayudante_1',   field:'ayudante1',   ok:'ayudante1Ok',   out:'ayudante1Id' },
    { role:'r_ayudante_2',   field:'ayudante2',   ok:'ayudante2Ok',   out:'ayudante2Id' },
    { role:'r_arsenalera',   field:'arsenalera',  ok:'arsenaleraOk',  out:'arsenaleraId' }
  ];

  for(const r of roles){
    const nameCsv = clean(prof[r.field] || '');
    if(!nameCsv){
      // si viene vacío, no lo consideramos pendiente (no bloquea)
      resolved[r.ok] = true;
      resolved[r.out] = null;
      continue;
    }

    const key = profKey(r.role, nameCsv);

    // 1) mapping manual guardado
    const mapped = state.maps.prof.get(key);
    if(mapped?.id){
      resolved[r.out] = mapped.id;
      resolved[r.ok] = true;
      continue;
    }

    // 2) match exacto por nombre normalizado (si hay único)
    const norm = normalizeProName(nameCsv);
    const candidates = state.catalogs.profByNorm.get(norm) || [];
    if(candidates.length === 1){
      resolved[r.out] = candidates[0].id;
      resolved[r.ok] = true;
    } else {
      const pendKey = profKey(r.role, nameCsv);
      if(state.allowPend.prof.has(pendKey)){
        resolved[r.ok] = true;
        resolved[r.out] = null;
        resolved[`_pend_${r.field}`] = true;
      } else {
        resolved[r.ok] = false;
      }
    }
  }


  return resolved;
}

function recomputePending(){
  state.pending.clinicas = [];
  state.pending.cirugias = [];
  state.pending.amb = [];
  state.pending.prof = [];

  // 1) recalcular resolved por cada item
  for(const it of state.stagedItems){
    it.resolved = resolveOneItem(it.normalizado || {});
  }

  const seenClin = new Set();
  const seenCir = new Set();
  const seenProf = new Set(); // ✅ NUEVO: key rol||nombre

  // 2) armar listas de pendientes únicas
  for(const it of state.stagedItems){
    const n = it.normalizado || {};
    const r = it.resolved || {};

    // ---------- Clínicas ----------
    const clinTxt = clean(n.clinica || '');
    if(clinTxt && r.clinicaOk === false){
      const norm = normalizeKey(clinTxt);
      if(!seenClin.has(norm)){
        seenClin.add(norm);
        state.pending.clinicas.push({ csvName: clinTxt, norm });
      }
    }

    // ---------- Cirugías ----------
    const cirTxt = clean(n.cirugia || '');
    const tipoTxt = clean(n.tipoPaciente || '');
    if(cirTxt && r.cirugiaOk === false){
      const key = cirKey(normalizeKey(clinTxt), normalizeKey(tipoTxt), normalizeKey(cirTxt));
      if(!seenCir.has(key)){
        seenCir.add(key);
        state.pending.cirugias.push({
          key,
          csvName: cirTxt,
          clinicaCsv: clinTxt || '(sin clínica)',
          tipoCsv: tipoTxt || '(sin tipo)',
          suggestions: suggestCirugias(normalizeKey(cirTxt))
        });
      }
    }

    // ---------- ✅ Profesionales (ESTO ES EL 7.2 QUE TE FALTABA) ----------
    const prof = (n.profesionales || {});
    const defs = [
      { label:'Cirujano',    role:'r_cirujano',    field:'cirujano',    ok: r.cirujanoOk },
      { label:'Anestesista', role:'r_anestesista', field:'anestesista', ok: r.anestesistaOk },
      { label:'Ayudante 1',  role:'r_ayudante_1',  field:'ayudante1',   ok: r.ayudante1Ok },
      { label:'Ayudante 2',  role:'r_ayudante_2',  field:'ayudante2',   ok: r.ayudante2Ok },
      { label:'Arsenalera',  role:'r_arsenalera',  field:'arsenalera',  ok: r.arsenaleraOk }
    ];

    for(const d of defs){
      const nameCsv = clean(prof[d.field] || '');
      if(!nameCsv) continue;       // vacío no bloquea
      if(d.ok !== false) continue; // solo pendientes

      const key = profKey(d.role, nameCsv);
      if(seenProf.has(key)) continue;
      seenProf.add(key);

      // sugerencias por nombre normalizado
      const norm = normalizeProName(nameCsv);
      const candidates = state.catalogs.profByNorm.get(norm) || [];

      state.pending.prof.push({
        key,
        roleId: d.role,
        roleLabel: d.label,
        csvName: nameCsv,
        suggestions: candidates.slice(0, 8) // [{id,nombre}]
      });
    }
  }

  // 3) ordenar
  state.pending.clinicas.sort((a,b)=> a.norm.localeCompare(b.norm));
  state.pending.cirugias.sort((a,b)=> a.key.localeCompare(b.key));
  state.pending.prof.sort((a,b)=> a.key.localeCompare(b.key)); // ✅ NUEVO

  // 4) refrescar UI
  setPills();
  setButtons();
  paintPreview();
}

async function persistMapping(docRef, key, id){
  await setDoc(docRef, {
    map: { [key]: { id, actualizadoEl: serverTimestamp(), actualizadoPor: state.user?.email || '' } },
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  if(docRef === docMapClinicas) state.maps.clinicas.set(key, { id });
  if(docRef === docMapCirugias) state.maps.cirugias.set(key, { id });
  if(docRef === docMapAmb) state.maps.amb.set(key, { id });
  if(docRef === docMapProf) state.maps.prof.set(key, { id });
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

function suggestClinicaId(){
  const tail = (Date.now() % 1000).toString().padStart(3,'0');
  return `C${tail}`;
}

function paintResolverModal(){
  const pc = state.pending.clinicas.length;
  const ps = state.pending.cirugias.length;
  const pa = state.pending.amb.length;
  const pp = state.pending.prof.length; // ✅
  const total = pc + ps + pa + pp;

  $('resolverResumen').innerHTML = `
    Pendientes totales: <b>${total}</b><br/>
    Clínicas: <b>${pc}</b> · Cirugías: <b>${ps}</b> · Ambulatorios: <b>${pa}</b> · Profesionales: <b>${pp}</b>
  `;

  // Clínicas
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
          <button class="btn" data-pend-clin="${escapeHtml(item.norm)}" type="button">Dejar pendiente</button>
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

      row.querySelector(`[data-pend-clin="${CSS.escape(item.norm)}"]`).addEventListener('click', async ()=>{
        state.allowPend.clinicas.add(item.norm);
        toast('Clínica marcada como pendiente (se podrá confirmar).');
        recomputePending();
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

  // Ambulatorios (placeholder)
  $('resolverAmbList').innerHTML = `<div class="muted tiny">— (Tu CSV actual no trae ambulatorios.)</div>`;

  // Cirugías por contexto
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

      const tipoOptions = TIPOS_PACIENTE
        .map(t => {
          const sel = normalizeKey(t) === normalizeKey(item.tipoCsv) ? 'selected' : '';
          return `<option value="${escapeHtml(t)}" ${sel}>${escapeHtml(t)}</option>`;
        })
        .join('');
      
      row.innerHTML = `
        <div>
          <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
          <div class="muted tiny"><b>Clínica:</b> ${escapeHtml(item.clinicaCsv)}</div>
      
          <!-- ✅ Tipo editable -->
          <div class="field" style="margin:8px 0 0 0;">
            <label>Tipo de Paciente (editable)</label>
            <select data-tipo-cir="${escapeHtml(item.key)}">
              ${tipoOptions}
            </select>
            <div class="help">Esto corrige el “contexto” usado para resolver la cirugía.</div>
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
          <button class="btn" data-pend-cir="${escapeHtml(item.key)}" type="button">Dejar pendiente</button>
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

      row.querySelector(`[data-pend-cir="${CSS.escape(item.key)}"]`).addEventListener('click', ()=>{
      // registra “permitido”
      state.allowPend.cirugias.add(item.key);
    
      // ✅ importante: si el usuario cambió Tipo Paciente, lo aplicamos al staging
      const selTipo = row.querySelector(`select[data-tipo-cir="${CSS.escape(item.key)}"]`);
      const tipoElegido = clean(selTipo?.value || item.tipoCsv || '');
    
      for(const it of state.stagedItems){
        const r = it.resolved || {};
        if(r._cirKey === item.key){
          if(it.normalizado) it.normalizado.tipoPaciente = tipoElegido || null;
          if(it.raw) it.raw['Tipo de Paciente'] = tipoElegido || it.raw['Tipo de Paciente'];
          it._search = null;
        }
      }
    
      toast('Cirugía marcada como pendiente (se podrá confirmar).');
      recomputePending();
      paintResolverModal();
    });

      row.querySelector(`[data-save-cir="${CSS.escape(item.key)}"]`).addEventListener('click', async ()=>{
        const selCir = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
        const id = selCir.value || '';
        if(!id){ toast('Selecciona una cirugía'); return; }
      
        const selTipo = row.querySelector(`select[data-tipo-cir="${CSS.escape(item.key)}"]`);
        const tipoElegido = clean(selTipo?.value || item.tipoCsv || '');
      
        // ✅ 1) Actualiza staging en memoria: tipoPaciente + raw['Tipo de Paciente']
        for(const it of state.stagedItems){
          const r = it.resolved || {};
          if(r._cirKey === item.key){
            if(it.normalizado){
              it.normalizado.tipoPaciente = tipoElegido || null;
            }
            if(it.raw){
              it.raw['Tipo de Paciente'] = tipoElegido || it.raw['Tipo de Paciente'];
            }
            // invalida búsqueda cacheada
            it._search = null;
          }
        }
      
        // ✅ 2) Persiste mapping con KEY recalculada usando el tipo elegido
        // item.key era la key antigua; recalculamos con el tipo corregido
        const normClin = normalizeKey(item.clinicaCsv || '');
        const normTipo = normalizeKey(tipoElegido || '');
        const normCir  = normalizeKey(item.csvName || '');
      
        const newKey = cirKey(normClin, normTipo, normCir);
        await persistMapping(docMapCirugias, newKey, id);
      
        toast('Cirugía asociada (y tipo corregido en staging)');
        await refreshAfterMapping();
        paintResolverModal();
      });


      row.querySelector(`[data-go-cir="${CSS.escape(item.key)}"]`).addEventListener('click', ()=>{
        try{
          localStorage.setItem('CR_PREFILL_CIRUGIA_NOMBRE', item.csvName);
          localStorage.setItem('CR_PREFILL_TIPO_PACIENTE', item.tipoCsv);
          localStorage.setItem('CR_PREFILL_CLINICA', item.clinicaCsv);
          localStorage.setItem('CR_RETURN_TO', 'produccion.html');
          localStorage.setItem('CR_RETURN_IMPORTID', state.importId || '');
        }catch(e){ }
        window.location.href = 'cirugias.html';
      });

      wrapS.appendChild(row);
    }
  }

  // ✅ Profesionales
  const wrapP = $('resolverProfesionalesList');
  if(wrapP){
    wrapP.innerHTML = '';
    const pp = state.pending.prof.length;

    if(pp === 0){
      wrapP.innerHTML = `<div class="muted tiny">✅ Sin pendientes de profesionales.</div>`;
    } else {
      for(const item of state.pending.prof){
        const row = document.createElement('div');
        row.className = 'miniRow';

        // options: todos los profesionales activos
        const options = state.catalogs.profesionales
          .map(p=> {
            const label = `${p.nombre || '(sin nombre)'} (${p.id})`; // ✅ nombreProfesional + RUT
            return `<option value="${escapeHtml(p.id)}">${escapeHtml(label)}</option>`;
          })
          .join('');


        const sug = (item.suggestions || [])
          .map(x=> `<span class="pill warn" style="cursor:pointer;" data-sugp-key="${escapeHtml(item.key)}" data-sugp-id="${escapeHtml(x.id)}">${escapeHtml(`${x.nombre}`)}</span>`)
          .join(' ');

        row.innerHTML = `
          <div>
            <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
            <div class="muted tiny"><b>Rol:</b> ${escapeHtml(item.roleLabel)}</div>
            <div class="muted tiny mono">key: ${escapeHtml(item.key)}</div>
            ${sug ? `<div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">${sug}</div>` : `<div class="muted tiny" style="margin-top:8px;">Sin sugerencias.</div>`}
          </div>

          <div class="field" style="margin:0;">
            <label>Asociar a</label>
            <select data-assoc-prof="${escapeHtml(item.key)}">
              <option value="">(Seleccionar profesional)</option>
              ${options}
            </select>
          </div>

          <div style="display:flex; gap:8px; justify-content:flex-end;">
            <button class="btn" data-pend-prof="${escapeHtml(item.key)}" type="button">Dejar pendiente</button>
            <button class="btn primary" data-save-prof="${escapeHtml(item.key)}" type="button">Guardar</button>
          </div>
        `;

        row.querySelectorAll('[data-sugp-key]').forEach(pill=>{
          pill.addEventListener('click', ()=>{
            const id = pill.getAttribute('data-sugp-id') || '';
            const sel = row.querySelector(`select[data-assoc-prof="${CSS.escape(item.key)}"]`);
            sel.value = id;
          });
        });

        row.querySelector(`[data-pend-prof="${CSS.escape(item.key)}"]`).addEventListener('click', ()=>{
          state.allowPend.prof.add(item.key);
          toast('Profesional marcado como pendiente (se podrá confirmar).');
          recomputePending();
          paintResolverModal();
        });

        row.querySelector(`[data-save-prof="${CSS.escape(item.key)}"]`).addEventListener('click', async ()=>{
          const sel = row.querySelector(`select[data-assoc-prof="${CSS.escape(item.key)}"]`);
          const id = sel.value || '';
          if(!id){ toast('Selecciona un profesional'); return; }
          await persistMapping(docMapProf, item.key, id);
          toast('Profesional asociado');
          await refreshAfterMapping();
          paintResolverModal();
        });

        wrapP.appendChild(row);
      }
    }
  }

}

/* =========================
   Staging save
========================= */
async function saveStagingToFirestore(){
  const importId = state.importId;
  const refImport = doc(db, 'produccion_imports', importId);

  await setDoc(refImport, {
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
  }, { merge:true });

  const itemsCol = collection(db, 'produccion_imports', importId, 'items');

  const chunkSize = 400;
  let idx = 0;
  while(idx < state.stagedItems.length){
    const batch = writeBatch(db);
    const slice = state.stagedItems.slice(idx, idx + chunkSize);

    slice.forEach((it, k)=>{
      const itemId = `ITEM_${pad(idx + k + 1, 4)}`;
      batch.set(doc(itemsCol, itemId), {
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

async function reemplazarMesAntesDeConfirmar(YYYY, MM, newImportId){
  // 1) Marcar items activos del mes como “reemplazado”
  const cg = collectionGroup(db, 'items');
  let last = null;
  let total = 0;

  while(true){
    const qy = last
      ? query(
          cg,
          where('ano','==', state.year),
          where('mesNum','==', state.monthNum),
          where('estado','==','activa'),
          orderBy('__name__'),
          startAfter(last),
          limit(300)
        )
      : query(
          cg,
          where('ano','==', state.year),
          where('mesNum','==', state.monthNum),
          where('estado','==','activa'),
          orderBy('__name__'),
          limit(300)
        );

    const snap = await getDocs(qy);
    if(snap.empty) break;

    const batch = writeBatch(db);
    snap.forEach(d=>{
      batch.set(d.ref, {
        estado: 'reemplazado',
        reemplazadoEl: serverTimestamp(),
        reemplazadoPor: state.user?.email || '',
        reemplazadoPorImportId: newImportId
      }, { merge:true });
      total++;
    });
    await batch.commit();
    last = snap.docs[snap.docs.length - 1];
  }

  // 2) Marcar imports confirmadas previas del mismo mes como “reemplazada”
  const qImp = query(
    colImports,
    where('ano','==', state.year),
    where('mesNum','==', state.monthNum),
    where('estado','==','confirmada')
  );
  const impSnap = await getDocs(qImp);
  for(const d of impSnap.docs){
    await setDoc(d.ref, {
      estado: 'reemplazada',
      reemplazadaEl: serverTimestamp(),
      reemplazadaPor: state.user?.email || '',
      reemplazadaPorImportId: newImportId
    }, { merge:true });
  }

  return total;
}


/* =========================
   Confirmar / Anular
========================= */
async function confirmarImportacion(){
  if(state.status !== 'staged'){ toast('No hay staging para confirmar.'); return; }

  const totalPend =
    state.pending.clinicas.length +
    state.pending.cirugias.length +
    state.pending.amb.length +
    state.pending.prof.length;
  
  if(totalPend > 0){
    // ✅ ahora se permite confirmar
    toast(`Confirmando con ${totalPend} pendientes (quedarán marcados).`);
  }

  const importId = state.importId;
  const itemsCol = collection(db, 'produccion_imports', importId, 'items');
  const itemsSnap = await getDocs(itemsCol);
  if(itemsSnap.empty){ toast('No hay items en staging.'); return; }

  const YYYY = String(state.year);
  const MM = pad(state.monthNum,2);

  // ✅ Reemplazo del mes (soft-delete): marca lo anterior como reemplazado
  const replacedCount = await reemplazarMesAntesDeConfirmar(YYYY, MM, importId);
  if(replacedCount > 0){
    toast(`Mes ${YYYY}-${MM}: ${replacedCount} items previos marcados como reemplazados.`);
  }


  // Asegura doc año y doc mes
  await setDoc(doc(db, 'produccion', YYYY), {
    ano: state.year,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  await setDoc(doc(db, 'produccion', YYYY, 'meses', MM), {
    ano: state.year,
    mesNum: state.monthNum,
    mes: state.monthName,
    monthId: monthId(state.year, state.monthNum),
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  const docs = [];
  itemsSnap.forEach(d => docs.push(d.data() || {}));

  const batchSize = 320;
  let i = 0;

  while(i < docs.length){
    const batch = writeBatch(db);
    const slice = docs.slice(i, i + batchSize);

    for(const data of slice){
      const n = data.normalizado || {};
      const raw = data.raw || {};

      const resolved = resolveOneItem(n);

      const fechaISO = n.fechaISO || parseDateToISO(raw['Fecha'] || '');
      const horaHM = n.horaHM || parseHora24(raw['Hora'] || '');
      const rutKey = normalizeRutKey(n.rut || raw['RUT'] || '');

      // Si no hay fecha/hora, igual no rompemos: generamos un id estable por item
      const timeId = (fechaISO && horaHM) ? `${fechaISO}_${horaHM}` : `SIN_FECHA_${data.itemId || nowId()}`;

      // Doc paciente: por RUT (como pediste). Si viniera sin RUT, lo mandamos a un bucket SINRUT
      const pacienteId = rutKey || `SINRUT_${importId}_${data.itemId || nowId()}`;

      const refPaciente = doc(db, 'produccion', YYYY, 'meses', MM, 'pacientes', pacienteId);
      batch.set(refPaciente, {
        rut: rutKey ? (n.rut || raw['RUT'] || null) : null,
        nombrePaciente: n.nombrePaciente || raw['Nombre Paciente'] || null,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: state.user?.email || ''
      }, { merge:true });

      const refItem = doc(db, 'produccion', YYYY, 'meses', MM, 'pacientes', pacienteId, 'items', timeId);

      batch.set(refItem, {
        // filtros / trazabilidad
        importId,
        ano: state.year,
        mesNum: state.monthNum,
        monthId: monthId(state.year, state.monthNum),

        // claves
        fechaISO: fechaISO || null,
        horaHM: horaHM || null, // HHMM
        clinica: n.clinica ?? null,
        cirugia: n.cirugia ?? null,
        tipoPaciente: n.tipoPaciente ?? null,

        // ids resueltos
        clinicaId: resolved.clinicaId ?? null,
        cirugiaId: resolved.cirugiaId ?? null,
        ambulatorioId: resolved.ambulatorioId ?? null,

        // valores
        valor: Number(n.valor || 0) || 0,
        derechosPabellon: Number(n.dp || 0) || 0,
        hmq: Number(n.hmq || 0) || 0,
        insumos: Number(n.ins || 0) || 0,

        // flags
        suspendida: n.suspendida ?? null,
        confirmado: n.confirmado ?? null,
        pagado: n.pagado ?? null,
        fechaPago: n.fechaPago ?? null,

        profesionales: n.profesionales || {},

        // ✅ IDs por rol (RUT sin puntos/guion)
        profesionalesId: {
          cirujanoId: resolved.cirujanoId ?? null,
          anestesistaId: resolved.anestesistaId ?? null,
          ayudante1Id: resolved.ayudante1Id ?? null,
          ayudante2Id: resolved.ayudante2Id ?? null,
          arsenaleraId: resolved.arsenaleraId ?? null
        },


        // raw completo
        raw,

        estado: 'activa',
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge:true });
    }

    await batch.commit();
    i += batchSize;
  }

  await setDoc(doc(db, 'produccion_imports', importId), {
    estado: 'confirmada',
    confirmadoEl: serverTimestamp(),
    confirmadoPor: state.user?.email || '',
    confirmadoEn: `produccion/${YYYY}/meses/${MM}/pacientes/{RUT}/items/{fecha_hora}`,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  state.status = 'confirmada';
  setStatus(`✅ Confirmada: ${importId} → produccion/${YYYY}/meses/${MM}/pacientes/{RUT}/items`);
  setButtons();
  toast('Importación confirmada');
}

async function anularImportacion(){
  if(!state.importId){ toast('No hay importación para anular.'); return; }

  const ok = confirm(`¿Anular importación?\n\n${state.importId}\n\n(No se borra; se marca como anulada)`);
  if(!ok) return;

  const importId = state.importId;
  await setDoc(doc(db, 'produccion_imports', importId), {
    estado: 'anulada',
    anuladaEl: serverTimestamp(),
    anuladaPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  // ✅ Marcar items de producción asociados al importId (dispersos en subcolecciones items)
  // Usamos collectionGroup('items') + filtros por importId + ano + mesNum (para acotar)
  const cg = collectionGroup(db, 'items');
  let last = null;
  let total = 0;

  while(true){
    const qy = last
      ? query(
          cg,
          where('importId','==', importId),
          where('ano','==', state.year),
          where('mesNum','==', state.monthNum),
          orderBy('__name__'),
          startAfter(last),
          limit(300)
        )
      : query(
          cg,
          where('importId','==', importId),
          where('ano','==', state.year),
          where('mesNum','==', state.monthNum),
          orderBy('__name__'),
          limit(300)
        );

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
  setStatus(`⛔ Importación anulada: ${importId} (${total} items marcados anulados en producción)`);
  setButtons();
  toast('Importación anulada');
}

/* =========================
   Carga CSV (con filtro de filas basura)
========================= */
async function handleLoadCSV(file){
  if(!file){ toast('Selecciona un CSV'); return; }

  const mes = clean($('mes').value);
  const ano = Number($('ano').value || 0) || 0;
  if(!ano || ano < 2020){ toast('Año inválido'); return; }

  const text = await file.text();
  const rows = parseCSV(text);
  if(rows.length < 2){ toast('CSV vacío o inválido'); return; }

  const header = rows[0].map(h=> clean(h));
  const headerIdx = buildHeaderIndex(header);

  const missing = validateMinimum(headerIdx);
  if(missing.length){ toast(`CSV sin columnas mínimas: ${missing.join(', ')}`); return; }

  const staged = [];
  for(let i=1;i<rows.length;i++){
    const row = rows[i];

    const raw = compactRaw(row, headerIdx);
    if(Object.keys(raw).length === 0) continue;

    // ✅ FILTRO: si NO hay Nombre Paciente y NO hay RUT => línea basura (reuso del documento)
    const nombre = clean(raw['Nombre Paciente'] || '');
    const rut = clean(raw['RUT'] || '');
    if(!nombre && !rut) continue;

    const normalizado = buildNormalizado(raw);
    staged.push({ idx:i, raw, normalizado, resolved:null, _search:null });
  }

  if(!staged.length){ toast('No se encontraron filas válidas (las vacías se descartaron).'); return; }

  const mm = pad(monthIndex(mes), 2);
  const importId = `PROD_${ano}_${mm}_${nowId()}`;

  state.importId = importId;
  state.status = 'staged';
  state.monthName = mes;
  state.monthNum = monthIndex(mes);
  state.year = ano;
  state.filename = file.name;
  state.stagedItems = staged;

  state.ui.page = 1;
  state.ui.query = '';
  $('q').value = '';

  $('importId').value = importId;

  setStatus(`🟡 Staging listo: ${staged.length} filas (líneas vacías descartadas)`);
  buildThead();

  await saveStagingToFirestore();
  toast('Staging guardado en Firestore');

  await refreshAfterMapping();
}

/* =========================
   Refresh pipeline
========================= */
async function refreshAfterMapping(){
  await loadMappings();
  await loadCatalogs();
  recomputePending();
  setStatus(
    state.status === 'staged'
      ? `🟡 Staging: ${state.stagedItems.length} filas · ImportID: ${state.importId}`
      : state.status === 'confirmada'
        ? `✅ Confirmada: ${state.importId}`
        : state.status === 'anulada'
          ? `⛔ Anulada: ${state.importId}`
          : '—'
  );
}

function openItemModal(it){
  $('modalItemBackdrop').style.display = 'block';

  const n = it.normalizado || {};
  const raw = it.raw || {};
  const fechaISO = n.fechaISO || parseDateToISO(raw['Fecha'] || '');
  const horaHM = n.horaHM || parseHora24(raw['Hora'] || '');
  const rutKey = normalizeRutKey(n.rut || raw['RUT'] || '');
  const YYYY = String(state.year);
  const MM = pad(state.monthNum,2);

  const timeId = (fechaISO && horaHM) ? `${fechaISO}_${horaHM}` : null;
  const pacienteId = rutKey || null;

  $('itemSub').textContent = pacienteId && timeId
    ? `Paciente: ${pacienteId} · Item: ${timeId}`
    : `Item staging (sin ID estable)`;

  // Form simple: editar campos críticos
  const form = `
    <div class="grid2">
      <div class="field">
        <label>Clínica (texto)</label>
        <input id="edClinica" value="${escapeHtml(n.clinica || raw['Clínica'] || '')}"/>
      </div>
      <div class="field">
        <label>Tipo Paciente</label>
        <input id="edTipo" value="${escapeHtml(n.tipoPaciente || raw['Tipo de Paciente'] || '')}"/>
      </div>
    </div>

    <div class="field" style="margin-top:10px;">
      <label>Cirugía (texto)</label>
      <input id="edCirugia" value="${escapeHtml(n.cirugia || raw['Cirugía'] || '')}"/>
    </div>

    <div class="grid2" style="margin-top:10px;">
      <div class="field"><label>Cirujano</label><input id="edCirujano" value="${escapeHtml((n.profesionales||{}).cirujano || raw['Cirujano'] || '')}"/></div>
      <div class="field"><label>Anestesista</label><input id="edAnestesista" value="${escapeHtml((n.profesionales||{}).anestesista || raw['Anestesista'] || '')}"/></div>
    </div>

    <div class="grid3" style="margin-top:10px;">
      <div class="field"><label>Ayudante 1</label><input id="edAy1" value="${escapeHtml((n.profesionales||{}).ayudante1 || raw['Ayudante 1'] || '')}"/></div>
      <div class="field"><label>Ayudante 2</label><input id="edAy2" value="${escapeHtml((n.profesionales||{}).ayudante2 || raw['Ayudante 2'] || '')}"/></div>
      <div class="field"><label>Arsenalera</label><input id="edArs" value="${escapeHtml((n.profesionales||{}).arsenalera || raw['Arsenalera'] || '')}"/></div>
    </div>

    <div class="help" style="margin-top:10px;">
      Tip: aquí ajustas el texto; luego el sistema recalcula resolución (IDs / pendientes).
    </div>
  `;
  $('itemForm').innerHTML = form;

  // key estable para acumular cambios
  const key = (pacienteId && timeId) ? `${pacienteId}||${timeId}` : `STAGING||${it.idx}`;

  // 1) Guardar 1 ítem (inmediato)
  $('btnGuardarItem').onclick = async ()=>{
    const patch = collectItemPatchFromModal();
    await saveOneItemPatch(it, patch);
    toast('✅ Item guardado');
    closeItemModal();
    refreshDirtyUI();
  };

  // 2) Guardar todo = ENCOLAR (no guardar aún)
  $('btnGuardarTodo').onclick = ()=>{
    const patch = collectItemPatchFromModal();
    enqueueDirtyEdit(key, it, patch);
    toast(`🟡 Agregado a cola (total: ${dirtyCount()})`);
    closeItemModal();
  };

}

function closeItemModal(){
  $('modalItemBackdrop').style.display = 'none';
  $('itemForm').innerHTML = '';
}

function collectItemPatchFromModal(){
  return {
    clinica: clean($('edClinica').value),
    tipoPaciente: clean($('edTipo').value),
    cirugia: clean($('edCirugia').value),

    profesionales: {
      cirujano: clean($('edCirujano').value),
      anestesista: clean($('edAnestesista').value),
      ayudante1: clean($('edAy1').value),
      ayudante2: clean($('edAy2').value),
      arsenalera: clean($('edArs').value)
    }
  };
}

async function saveOneItemPatch(it, patch){
  // 1) aplica al staging en memoria
  it.normalizado = it.normalizado || {};
  it.normalizado.clinica = patch.clinica || null;
  it.normalizado.tipoPaciente = patch.tipoPaciente || null;
  it.normalizado.cirugia = patch.cirugia || null;
  it.normalizado.profesionales = patch.profesionales || {};

  it.raw = it.raw || {};
  it.raw['Clínica'] = patch.clinica;
  it.raw['Tipo de Paciente'] = patch.tipoPaciente;
  it.raw['Cirugía'] = patch.cirugia;
  it.raw['Cirujano'] = patch.profesionales.cirujano;
  it.raw['Anestesista'] = patch.profesionales.anestesista;
  it.raw['Ayudante 1'] = patch.profesionales.ayudante1;
  it.raw['Ayudante 2'] = patch.profesionales.ayudante2;
  it.raw['Arsenalera'] = patch.profesionales.arsenalera;

  it._search = null;

  // 2) recalcula resolución local
  it.resolved = resolveOneItem(it.normalizado);

  // 3) si ya está confirmada, persiste en producción
  if(state.status === 'confirmada'){
    const n = it.normalizado || {};
    const fechaISO = n.fechaISO || parseDateToISO(it.raw['Fecha'] || '');
    const horaHM = n.horaHM || parseHora24(it.raw['Hora'] || '');
    const rutKey = normalizeRutKey(n.rut || it.raw['RUT'] || '');
    if(!fechaISO || !horaHM || !rutKey) return;

    const YYYY = String(state.year);
    const MM = pad(state.monthNum,2);
    const pacienteId = rutKey;
    const timeId = `${fechaISO}_${horaHM}`;

    const refItem = doc(db, 'produccion', YYYY, 'meses', MM, 'pacientes', pacienteId, 'items', timeId);

    // guarda “texto” + ids recalculados + pendientes
    await updateDoc(refItem, {
      clinica: n.clinica ?? null,
      cirugia: n.cirugia ?? null,
      tipoPaciente: n.tipoPaciente ?? null,
      profesionales: n.profesionales || {},

      clinicaId: it.resolved?.clinicaId ?? null,
      cirugiaId: it.resolved?.cirugiaId ?? null,

      profesionalesId: {
        cirujanoId: it.resolved?.cirujanoId ?? null,
        anestesistaId: it.resolved?.anestesistaId ?? null,
        ayudante1Id: it.resolved?.ayudante1Id ?? null,
        ayudante2Id: it.resolved?.ayudante2Id ?? null,
        arsenaleraId: it.resolved?.arsenaleraId ?? null
      },

      pendientes: {
        clinica: !!it.resolved?._pendClin || (it.resolved?.clinicaOk === false),
        cirugia: !!it.resolved?._pendCir || (it.resolved?.cirugiaOk === false),
        profesionales: {
          cirujano: !!it.resolved?._pend_cirujano || (it.resolved?.cirujanoOk === false),
          anestesista: !!it.resolved?._pend_anestesista || (it.resolved?.anestesistaOk === false),
          ayudante1: !!it.resolved?._pend_ayudante1 || (it.resolved?.ayudante1Ok === false),
          ayudante2: !!it.resolved?._pend_ayudante2 || (it.resolved?.ayudante2Ok === false),
          arsenalera: !!it.resolved?._pend_arsenalera || (it.resolved?.arsenaleraOk === false)
        },
        tipoPaciente: !clean(n.tipoPaciente || '')
      },

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    });
  }

  recomputePending();
  paintPreview();
}

// ✅ Guarda la cola completa (cuando tú decidas)
async function saveAllDirtyEdits(){
  await flushDirtyEdits();
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

    $('mes').value = 'Octubre';
    buildThead();

    await loadMappings();
    await loadCatalogs();

    recomputePending();
    setButtons();
    paintPreview();

    $('btnCargar').addEventListener('click', async ()=>{
      try{
        await handleLoadCSV($('fileCSV').files?.[0]);
      }catch(err){
        console.error(err);
        toast('Error cargando CSV (ver consola)');
      }
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

    $('q').addEventListener('input', ()=>{
      state.ui.query = $('q').value || '';
      state.ui.page = 1;
      paintPreview();
    });

    $('btnPrev').addEventListener('click', ()=>{
      if(state.ui.page > 1){ state.ui.page--; paintPreview(); }
    });

    $('btnNext').addEventListener('click', ()=>{
      applyFilter();
      const pages = Math.max(1, Math.ceil(state.view.filtered.length / state.ui.pageSize));
      if(state.ui.page < pages){ state.ui.page++; paintPreview(); }
    });

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

    $('btnItemClose')?.addEventListener('click', closeItemModal);
    $('btnItemCancelar')?.addEventListener('click', closeItemModal);
    
    $('modalItemBackdrop')?.addEventListener('click', (e)=>{
      if(e.target === $('modalItemBackdrop')) closeItemModal();
    });

    // ✅ Si existe un botón global para guardar cola, lo conectamos
    $('btnGuardarCola')?.addEventListener('click', async ()=>{
      try{ await flushDirtyEdits(); }
      catch(err){ console.error(err); toast('Error guardando cola (ver consola)'); }
    });


  }
});
