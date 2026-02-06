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
  collectionGroup, deleteField
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   DOM
========================= */
const $ = (id)=> document.getElementById(id);

/* =========================
   Columnas esperadas (como pediste)
========================= */
const EXPECTED_COLS = [
  'Suspendida','Confirmado','Fecha','Hora','Clínica',
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

/* =========================
   ETIQUETA "AGENDA"
   - SIEMPRE: DR(A) APELLIDOS, NOMBRES (RUT)
   - TODO EN MAYÚSCULAS (como hoy)
   - Reglas por cantidad de palabras:
     2 -> N | A
     3 -> N | A A
     4 -> N N | A A
     5+ -> N N N | A A (resto queda en apellidos)
   ========================= */
function proEtiquetaAgenda(nombreRaw, rut){
  const cleanName = normalizeProName(nombreRaw || ''); // ya quita dr/dra, etc.
  const parts = cleanName.split(/\s+/).filter(Boolean);

  let nombres = '';
  let apellidos = '';

  if(parts.length <= 1){
    apellidos = parts.join(' ');
  } else if(parts.length === 2){
    nombres = parts[0];
    apellidos = parts[1];
  } else if(parts.length === 3){
    nombres = parts[0];
    apellidos = parts.slice(1).join(' ');
  } else if(parts.length === 4){
    nombres = parts.slice(0,2).join(' ');
    apellidos = parts.slice(2).join(' ');
  } else {
    nombres = parts.slice(0,3).join(' ');
    apellidos = parts.slice(3).join(' ');
  }

  const etiqueta = `DR(A) ${apellidos}, ${nombres}`.trim();
  const rutTxt = (rut || '').trim();
  return rutTxt ? `${etiqueta} (${rutTxt})`.toUpperCase() : etiqueta.toUpperCase();
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

function setDefaultToPreviousMonth(){
  const meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];

  const d = new Date();
  // nos vamos al día 1 para evitar efectos raros (meses cortos)
  d.setDate(1);
  // retrocede 1 mes
  d.setMonth(d.getMonth() - 1);

  $('mes').value = meses[d.getMonth()];
  $('ano').value = String(d.getFullYear());
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

function normalizeTipoPaciente(v){
  // Normaliza lo que venga del CSV a tu set fijo:
  // - "isapre" o "particular" => "Particular / Isapre"
  // - "fonasa" => "Fonasa"
  // - "mle" => "MLE"
  // - otros / vacío => '' (para que quede pendiente)
  const x = normalizeKey(v);
  if(!x) return '';
  if(x.includes('fonasa')) return 'Fonasa';
  if(x.includes('mle')) return 'MLE';
  if(x.includes('isapre') || x.includes('particular')) return 'Particular / Isapre';
  return ''; // desconocido => pendiente
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
    profByNorm: new Map(),      // nombre completo normalizado -> [{id,nombre}]
    profById: new Map(),        // id -> prof
    profByLastToken: new Map(), // apellido (último token) -> [{id,nombre}]
    profTokens: new Map()       // id -> Set(tokens) (para ranking parcial)
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

  // ✅ persistir la cola en Firestore (debounced)
  schedulePersistDirtyQueue();
}


async function flushDirtyEdits(options = {}){
  const total = dirtyCount();
  if(total === 0){
    toast('No hay cambios en cola.');
    return;
  }

  const ok = confirm(`¿Guardar ${total} cambio(s) en cola?`);
  if(!ok) return;

  let okCount = 0;
  let failCount = 0;

  const entries = Array.from(state.dirtyEdits.entries());

  for(const [k, v] of entries){
    try{
      if(!v?.it){
        failCount++;
        console.warn('⚠️ Cola: item huérfano (no existe en staging actual)', { key:k, v });
        continue;
      }
  
      await saveOneItemPatch(v.it, v.patch, options);
      state.dirtyEdits.delete(k);
      okCount++;
      schedulePersistDirtyQueue();
  
    }catch(err){
      failCount++;
      console.warn('❌ flushDirtyEdits: no se pudo guardar', { key:k, err });
    }
  }


  refreshDirtyUI();

  if(failCount === 0) toast(`✅ Cola guardada (${okCount}/${total})`);
  else toast(`⚠️ Cola parcial: ${okCount} guardado(s), ${failCount} falló/fallaron. Revisa consola.`);
}


/* =========================
   ✅ Persistencia COLA en Firestore
   Ruta: produccion_imports/{importId}/queue/dirtyEdits
========================= */

function dirtyQueueDocRef(importId){
  if(!importId) return null;
  return doc(db, 'produccion_imports', importId, 'queue', 'dirtyEdits');
}

// debounce simple para no escribir a cada click en Firestore
let _dirtyQueueTimer = null;

function schedulePersistDirtyQueue(){
  if(_dirtyQueueTimer) clearTimeout(_dirtyQueueTimer);
  _dirtyQueueTimer = setTimeout(async ()=>{
    try{
      await persistDirtyQueueNow();
    }catch(err){
      console.warn('❌ persistDirtyQueueNow() falló', err);
    }
  }, 250);
}

async function persistDirtyQueueNow(){
  const importId = state.importId;
  const ref = dirtyQueueDocRef(importId);
  if(!ref) return;

  // serializar Map -> objeto plano
  const entriesObj = {};
  for(const [k, v] of state.dirtyEdits.entries()){
    entriesObj[k] = {
      // guardamos lo mínimo necesario para reintentar
      itemId: clean(v?.it?.itemId || ''),
      idx: Number(v?.it?.idx || 0) || 0,
      patch: v?.patch || {},
      queuedAt: Number(v?.queuedAt || Date.now()) || Date.now()
    };
  }

  await setDoc(ref, {
    importId,
    updatedAt: serverTimestamp(),
    updatedBy: state.user?.email || '',
    entries: entriesObj
  }, { merge:true });
}

async function loadDirtyQueueFromFirestore(importId){
  state.dirtyEdits = new Map();

  const ref = dirtyQueueDocRef(importId);
  if(!ref) { refreshDirtyUI(); return; }

  const snap = await getDoc(ref);
  if(!snap.exists()){
    refreshDirtyUI();
    return;
  }

  const data = snap.data() || {};
  const entries = data.entries || {};

  // reconstruir it desde state.stagedItems por itemId (o idx si no existe)
  for(const k of Object.keys(entries)){
    const e = entries[k] || {};
    const itemId = clean(e.itemId || '');
    const idx = Number(e.idx || 0) || 0;

    let it = null;
    if(itemId){
      it = (state.stagedItems || []).find(x => clean(x.itemId) === itemId) || null;
    }
    if(!it && idx){
      it = (state.stagedItems || []).find(x => Number(x.idx||0) === idx) || null;
    }

    // si no encontramos el item, lo dejamos “huérfano” pero NO rompemos
    const patch = e.patch || {};
    const queuedAt = Number(e.queuedAt || Date.now()) || Date.now();

    state.dirtyEdits.set(k, { it, patch, queuedAt });
  }

  refreshDirtyUI();
}

async function clearDirtyQueueDoc(importId){
  const ref = dirtyQueueDocRef(importId);
  if(!ref) return;
  await setDoc(ref, {
    entries: {},
    clearedAt: serverTimestamp(),
    clearedBy: state.user?.email || ''
  }, { merge:true });
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
      <button class="btn small" type="button" data-open-item="${escapeHtml(String(it.idx || ''))}">Editar</button>
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
    tr.innerHTML = `<td colspan="${EXPECTED_COLS.length + 3}" class="muted tiny">Sin resultados para el filtro.</td>`;
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
  const tipoPaciente = raw['Tipo de Paciente'] ?? '';
  const tipoPacienteNorm = normalizeTipoPaciente(tipoPaciente);

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
    tipoPaciente: tipoPacienteNorm ? tipoPacienteNorm : (clean(tipoPaciente) || null),
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

    const idxFull = new Map();       // nombre completo normalizado
    const idxLast = new Map();       // apellido (último token)
    const idxTokens = new Map();     // id -> Set(tokens)
    
    for(const p of activos){
      const full = normalizeProName(p.nombre); // ya quita dr/dra/etc
      if(!full) continue;
    
      // 1) Índice por nombre completo
      if(!idxFull.has(full)) idxFull.set(full, []);
      idxFull.get(full).push({ id: p.id, nombre: p.nombre });
    
      // 2) Índice por último token (apellido probable)
      const toks = full.split(' ').filter(Boolean);
      const last = toks[toks.length - 1] || '';
      if(last){
        if(!idxLast.has(last)) idxLast.set(last, []);
        idxLast.get(last).push({ id: p.id, nombre: p.nombre });
      }
    
      // 3) Tokens por id (para ranking parcial)
      idxTokens.set(p.id, new Set(toks));
    }
    
    state.catalogs.profByNorm = idxFull;
    state.catalogs.profByLastToken = idxLast;
    state.catalogs.profTokens = idxTokens;

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

function findProfesionalesCandidates(nameCsv){
  // Devuelve candidatos ordenados por score (mejor primero)
  const norm = normalizeProName(nameCsv);
  if(!norm) return [];

  // 1) Match exacto por nombre completo normalizado
  const exact = state.catalogs.profByNorm.get(norm) || [];
  if(exact.length) return exact.slice(0, 8);

  const parts = norm.split(' ').filter(Boolean);

  // 2) Si viene 1 token (probable apellido): buscar por último token
  if(parts.length === 1){
    const last = parts[0];
    const byLast = state.catalogs.profByLastToken.get(last) || [];
    return byLast.slice(0, 8);
  }

  // 3) Ranking parcial: puntuar por tokens compartidos
  // Regla simple y buena:
  // - score = cantidad de tokens del CSV que aparecen en tokens del profesional
  // - preferimos score más alto
  const wanted = new Set(parts);
  const scored = [];

  for(const p of (state.catalogs.profesionales || [])){
    const tokSet = state.catalogs.profTokens.get(p.id);
    if(!tokSet) continue;

    let score = 0;
    for(const t of wanted){
      if(tokSet.has(t)) score++;
    }
    if(score > 0){
      scored.push({ id: p.id, nombre: p.nombre, score });
    }
  }

  scored.sort((a,b)=> (b.score - a.score) || a.nombre.localeCompare(b.nombre,'es'));
  return scored.slice(0, 8).map(x=> ({ id:x.id, nombre:x.nombre }));
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

    // 2) match inteligente (exacto / apellido / parcial con ranking)
    const candidates = findProfesionalesCandidates(nameCsv);
    
    /*
      ✅ REGLA NUEVA (ANTI-CRUCE):
      - Auto-asignar SOLO si el match es EXACTO por nombre completo normalizado
        y además es ÚNICO.
      - Cualquier match parcial (apellido / tokens / ranking) => queda pendiente.
    */
    const norm = normalizeProName(nameCsv);
    const exactList = state.catalogs.profByNorm.get(norm) || [];
    
    if(exactList.length === 1){
      resolved[r.out] = exactList[0].id;
      resolved[r.ok] = true;
    } else {
      const pendKey = profKey(r.role, nameCsv);
      if(state.allowPend.prof.has(pendKey)){
        resolved[r.ok] = true;
        resolved[r.out] = null;
        resolved[`_pend_${r.field}`] = true;
      } else {
        // ✅ si NO hay match exacto único => pendiente para que el usuario elija
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
      const candidates = findProfesionalesCandidates(nameCsv);
      
      state.pending.prof.push({
        key,
        roleId: d.role,
        roleLabel: d.label,
        csvName: nameCsv,
        suggestions: candidates // [{id,nombre}] ya viene top 8
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

function formatImportDate(ts){
  try{
    const d = ts?.toDate ? ts.toDate() : (ts instanceof Date ? ts : null);
    if(!d) return 'Sin fecha';
    // Ej: "miércoles, 21 de enero de 2026, 06:48"
    return new Intl.DateTimeFormat('es-CL', {
      weekday:'long', year:'numeric', month:'long', day:'numeric',
      hour:'2-digit', minute:'2-digit'
    }).format(d);
  }catch(e){
    return 'Sin fecha';
  }
}

async function fillImportSuggestions(){
  const sel = $('importSelect');
  if(!sel) return;

  // reset select SIEMPRE
  sel.innerHTML = `<option value="">(Selecciona una importación del mes)</option>`;
  if($('importId')) $('importId').value = '';

  const ano = Number($('ano')?.value || 0) || 0;
  const mesName = clean($('mes')?.value || '');
  const mesNum = monthIndex(mesName);

  if(!ano || !mesNum) return;

  try{
    // ✅ SIN orderBy => no requiere índice compuesto
    // Traemos hasta 50 y ordenamos localmente por creadoEl desc.
    const qy = query(
      colImports,
      where('ano','==', ano),
      where('mesNum','==', mesNum),
      limit(50)
    );

    const snap = await getDocs(qy);

    const docs = [];
    snap.forEach(d=>{
      const x = d.data() || {};
      const id = clean(x.id || d.id);
      if(!id) return;

      const ts = x.creadoEl;
      const ms = ts?.toMillis ? ts.toMillis() : (ts instanceof Date ? ts.getTime() : 0);

      docs.push({ id, x, ms });
    });

    // ordenar desc por fecha (más reciente primero)
    docs.sort((a,b)=> (b.ms || 0) - (a.ms || 0));

    for(const it of docs){
      const x = it.x || {};
      const id = it.id;

      const estado = clean(x.estado || '');
      const filas = Number(x.filas || 0) || 0;
      const when = formatImportDate(x.creadoEl);

      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = `${when} — ${estado || '—'} — ${filas} filas`;
      sel.appendChild(opt);
    }

    // auto-selecciona el primero real si existe
    if(sel.options.length > 1){
      sel.selectedIndex = 1;
      if($('importId')) $('importId').value = sel.value;
    }

  }catch(err){
    console.warn('fillImportSuggestions()', err);
    toast('No se pudieron cargar importaciones (ver consola).');
  }
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

  /* =========================
     CLÍNICAS
  ========================= */
  const wrapC = $('resolverClinicasList');
  wrapC.innerHTML = '';

  if(pc === 0){
    wrapC.innerHTML = `<div class="muted tiny">✅ Sin pendientes de clínicas.</div>`;
  } else {
    for(const item of state.pending.clinicas){
      const row = document.createElement('div');
      row.className = 'miniRow';

      const options = (state.catalogs.clinicas || [])
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

      // Guardar
      row.querySelector(`[data-save-clin="${CSS.escape(item.norm)}"]`)?.addEventListener('click', async ()=>{
        const sel = row.querySelector(`select[data-assoc-clin="${CSS.escape(item.norm)}"]`);
        const id = sel?.value || '';
        if(!id){ toast('Selecciona una clínica'); return; }
        await persistMapping(docMapClinicas, item.norm, id);
        toast('Clínica asociada');
        await refreshAfterMapping();
        paintResolverModal();
      });

      // Pendiente
      row.querySelector(`[data-pend-clin="${CSS.escape(item.norm)}"]`)?.addEventListener('click', ()=>{
        state.allowPend.clinicas.add(item.norm);
        toast('Clínica marcada como pendiente (se podrá confirmar).');
        recomputePending();
        paintResolverModal();
      });

      // Crear
      row.querySelector(`[data-create-clin="${CSS.escape(item.norm)}"]`)?.addEventListener('click', async ()=>{
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

  /* =========================
     AMBULATORIOS (placeholder)
  ========================= */
  $('resolverAmbList').innerHTML = `<div class="muted tiny">— (Tu CSV actual no trae ambulatorios.)</div>`;

  /* =========================
     CIRUGÍAS POR CONTEXTO
     ✅ ARREGLADO: data-pend-cir / data-create-cir / data-save-cir
  ========================= */
  const wrapS = $('resolverCirugiasList');
  wrapS.innerHTML = '';

  if(ps === 0){
    wrapS.innerHTML = `<div class="muted tiny">✅ Sin pendientes de cirugías.</div>`;
  } else {
    for(const item of state.pending.cirugias){
      const row = document.createElement('div');
      row.className = 'miniRow';

      const options = (state.catalogs.cirugias || [])
        .map(s=> `<option value="${escapeHtml(s.id)}">${escapeHtml(`${s.nombre} (${s.codigo})`)}</option>`)
        .join('');

      const sug = (item.suggestions || [])
        .map(x=> `<span class="pill warn" style="cursor:pointer;" data-sug-key="${escapeHtml(item.key)}" data-sug-id="${escapeHtml(x.id)}">${escapeHtml(x.nombre)}</span>`)
        .join(' ');

      const tipoOptions = (TIPOS_PACIENTE || [])
        .map(t => {
          const sel = normalizeKey(t) === normalizeKey(item.tipoCsv) ? 'selected' : '';
          return `<option value="${escapeHtml(t)}" ${sel}>${escapeHtml(t)}</option>`;
        })
        .join('');

      row.innerHTML = `
        <div>
          <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
          <div class="muted tiny"><b>Clínica:</b> ${escapeHtml(item.clinicaCsv)}</div>

          <div class="field" style="margin:8px 0 0 0;">
            <label>Tipo de Paciente (editable)</label>
            <select data-tipo-cir="${escapeHtml(item.key)}">
              ${tipoOptions}
            </select>
            <div class="help">Esto corrige el “contexto” usado para resolver la cirugía.</div>
          </div>

          <div class="muted tiny mono">key: ${escapeHtml(item.key)}</div>
          ${sug
            ? `<div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">${sug}</div>`
            : `<div class="muted tiny" style="margin-top:8px;">Sin sugerencias.</div>`
          }
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

      // Click sugerencias -> setear select
      row.querySelectorAll('[data-sug-key]')?.forEach(pill=>{
        pill.addEventListener('click', ()=>{
          const id = pill.getAttribute('data-sug-id') || '';
          const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
          if(sel) sel.value = id;
        });
      });

      // Pendiente (cirugía)
      row.querySelector(`[data-pend-cir="${CSS.escape(item.key)}"]`)?.addEventListener('click', ()=>{
        state.allowPend.cirugias.add(item.key);

        // si cambió tipo, reflejar en staging
        const selTipo = row.querySelector(`select[data-tipo-cir="${CSS.escape(item.key)}"]`);
        const tipoElegido = clean(selTipo?.value || item.tipoCsv || '');

        for(const it of (state.stagedItems || [])){
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

      // Guardar (cirugía)
      row.querySelector(`[data-save-cir="${CSS.escape(item.key)}"]`)?.addEventListener('click', async ()=>{
        const selCir = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.key)}"]`);
        const id = selCir?.value || '';
        if(!id){ toast('Selecciona una cirugía'); return; }

        const selTipo = row.querySelector(`select[data-tipo-cir="${CSS.escape(item.key)}"]`);
        const tipoElegido = clean(selTipo?.value || item.tipoCsv || '');

        // 1) staging
        for(const it of (state.stagedItems || [])){
          const r = it.resolved || {};
          if(r._cirKey === item.key){
            if(it.normalizado) it.normalizado.tipoPaciente = tipoElegido || null;
            if(it.raw) it.raw['Tipo de Paciente'] = tipoElegido || it.raw['Tipo de Paciente'];
            it._search = null;
          }
        }

        // 2) persist mapping con key recalculada
        const normClin = normalizeKey(item.clinicaCsv || '');
        const normTipo = normalizeKey(tipoElegido || '');
        const normCir  = normalizeKey(item.csvName || '');

        const newKey = cirKey(normClin, normTipo, normCir);
        await persistMapping(docMapCirugias, newKey, id);

        toast('Cirugía asociada (y tipo corregido en staging)');
        await refreshAfterMapping();
        paintResolverModal();
      });

      // Ir a crear cirugia
      row.querySelector(`[data-go-cir="${CSS.escape(item.key)}"]`)?.addEventListener('click', ()=>{
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

  /* =========================
     PROFESIONALES
     ✅ ARREGLADO: botón +Crear + listener async
  ========================= */
  const wrapP = $('resolverProfesionalesList');
  if(wrapP){
    wrapP.innerHTML = '';

    if(pp === 0){
      wrapP.innerHTML = `<div class="muted tiny">✅ Sin pendientes de profesionales.</div>`;
    } else {
      for(const item of state.pending.prof){
        const row = document.createElement('div');
        row.className = 'miniRow';

        const profSorted = [...(state.catalogs.profesionales || [])]
          .map(p => ({
            ...p,
            _agendaLabel: proEtiquetaAgenda(p.nombre || '', p.id || '')
          }))
          .sort((a, b) => (a._agendaLabel || '').localeCompare((b._agendaLabel || ''), 'es'));

        const options = profSorted
          .map(p => `<option value="${escapeHtml(p.id)}">${escapeHtml(p._agendaLabel)}</option>`)
          .join('');

        const sug = (item.suggestions || [])
          .map(x=> `<span class="pill warn" style="cursor:pointer;" data-sugp-key="${escapeHtml(item.key)}" data-sugp-id="${escapeHtml(x.id)}">${escapeHtml(`${x.nombre}`)}</span>`)
          .join(' ');

        row.innerHTML = `
          <div>
            <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
            <div class="muted tiny"><b>Rol:</b> ${escapeHtml(item.roleLabel)}</div>
            <div class="muted tiny mono">key: ${escapeHtml(item.key)}</div>
            ${sug
              ? `<div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">${sug}</div>`
              : `<div class="muted tiny" style="margin-top:8px;">Sin sugerencias.</div>`
            }
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
            <button class="btn soft" data-create-prof="${escapeHtml(item.key)}" type="button">+ Crear</button>
            <button class="btn primary" data-save-prof="${escapeHtml(item.key)}" type="button">Guardar</button>
          </div>
        `;

        // click sugerencia -> setear select
        row.querySelectorAll('[data-sugp-key]')?.forEach(pill=>{
          pill.addEventListener('click', ()=>{
            const id = pill.getAttribute('data-sugp-id') || '';
            const sel = row.querySelector(`select[data-assoc-prof="${CSS.escape(item.key)}"]`);
            if(sel) sel.value = id;
          });
        });

        // pendiente
        row.querySelector(`[data-pend-prof="${CSS.escape(item.key)}"]`)?.addEventListener('click', ()=>{
          state.allowPend.prof.add(item.key);
          toast('Profesional marcado como pendiente (se podrá confirmar).');
          recomputePending();
          paintResolverModal();
        });

        // ✅ crear profesional (ANTES lo tenías SUELTO con await)
        row.querySelector(`[data-create-prof="${CSS.escape(item.key)}"]`)?.addEventListener('click', async ()=>{
          const rut = clean(prompt('RUT del profesional (sin puntos ni guion, ej: 12345678K):', '') || '');
          if(!rut){ toast('Cancelado'); return; }

          const nombre = clean(prompt('Nombre del profesional (ej: Paula Paoletto):', item.csvName || '') || '');
          if(!nombre){ toast('Falta nombre'); return; }

          await setDoc(doc(db, 'profesionales', rut), {
            nombreProfesional: nombre,
            estado: 'activo',
            creadoEl: serverTimestamp(),
            creadoPor: state.user?.email || '',
            actualizadoEl: serverTimestamp(),
            actualizadoPor: state.user?.email || ''
          }, { merge:true });

          await persistMapping(docMapProf, item.key, rut);

          toast('✅ Profesional creado y asociado');
          await refreshAfterMapping();
          paintResolverModal();
        });

        // guardar asociación
        row.querySelector(`[data-save-prof="${CSS.escape(item.key)}"]`)?.addEventListener('click', async ()=>{
          const sel = row.querySelector(`select[data-assoc-prof="${CSS.escape(item.key)}"]`);
          const id = sel?.value || '';
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
   ✅ Aprender mapping desde el ítem (cuando el usuario elige en el modal)
   - Guarda decisión en produccion_mappings/*
========================= */
async function learnMappingsFromItemDecision(patch){
  if(!patch || !patch._selectedIds || !patch._originalCsv) return;

  const sel = patch._selectedIds;
  const orig = patch._originalCsv;

  const jobs = [];

  // ---- Clínica ----
  if(sel.clinicaId && clean(orig.clinica)){
    const keyClin = normalizeKey(orig.clinica);
    jobs.push(persistMapping(docMapClinicas, keyClin, sel.clinicaId));
  }

  // ---- Cirugía por contexto (Clínica + Tipo + Cirugía) ----
  if(sel.cirugiaId && clean(orig.cirugia)){
    const keyCir = cirKey(
      normalizeKey(orig.clinica || ''),
      normalizeKey(orig.tipoPaciente || ''),
      normalizeKey(orig.cirugia || '')
    );
    jobs.push(persistMapping(docMapCirugias, keyCir, sel.cirugiaId));
  }

  // ---- Profesionales por rol ----
  const roles = [
    { role:'r_cirujano',    name: orig.profesionales?.cirujano,    id: sel.profIds?.cirujanoId },
    { role:'r_anestesista', name: orig.profesionales?.anestesista, id: sel.profIds?.anestesistaId },
    { role:'r_ayudante_1',  name: orig.profesionales?.ayudante1,   id: sel.profIds?.ayudante1Id },
    { role:'r_ayudante_2',  name: orig.profesionales?.ayudante2,   id: sel.profIds?.ayudante2Id },
    { role:'r_arsenalera',  name: orig.profesionales?.arsenalera,  id: sel.profIds?.arsenaleraId }
  ];

  for(const r of roles){
    if(r.id && clean(r.name)){
      const keyProf = profKey(r.role, r.name);
      jobs.push(persistMapping(docMapProf, keyProf, r.id));
    }
  }

  if(jobs.length){
    await Promise.all(jobs);
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
    
      // ✅ clave: guardamos el itemId dentro del objeto en memoria
      it.itemId = itemId;
    
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

// ✅ Loader de import/staging (recupera stagedItems + estado) + recupera COLA
async function loadStagingFromFirestore(importId){
  if(!importId) throw new Error('loadStagingFromFirestore: importId vacío');

  const ref = doc(db, 'produccion_imports', importId);
  const snap = await getDoc(ref);

  if(!snap.exists()){
    // si no existe, dejamos estado limpio
    state.importId = importId;
    state.status = 'idle';
    state.filename = '';
    state.stagedItems = [];
    state.pending = { clinicas: [], cirugias: [], amb: [], prof: [] };
    refreshDirtyUI();
    paintPreview();
    setPills();
    setButtons();
    setStatus('Import no encontrado (staging vacío).');
    return;
  }

  const data = snap.data() || {};

  // 1) estado base
  state.importId  = importId;
  state.filename  = data.filename || '';
  state.year      = Number(data.year || state.year || 0) || state.year;
  state.monthNum  = Number(data.monthNum || state.monthNum || 0) || state.monthNum;
  state.monthName = data.monthName || state.monthName || '';
  state.status    = (data.status === 'confirmada') ? 'confirmada' : 'staged';

  // 2) stagedItems
  // OJO: ajusta el nombre del campo si en tu doc se llama distinto (ej: items, stagedItems, rows, etc.)
  const items = Array.isArray(data.stagedItems) ? data.stagedItems : [];
  state.stagedItems = items;

  // 3) Recalcular “pending” en base a resolved (esto depende de tus helpers existentes)
  // Si tú ya tienes una función tipo recomputePending(), úsala acá.
  try{
    state.pending = { clinicas: [], cirugias: [], amb: [], prof: [] };
    for(const it of state.stagedItems){
      const r = it.resolved || {};
      if(r._pendClin) state.pending.clinicas.push(it);
      if(r._pendCir)  state.pending.cirugias.push(it);

      if(r._pend_cirujano)     state.pending.prof.push({ role:'cirujano', it });
      if(r._pend_anestesista)  state.pending.prof.push({ role:'anestesista', it });
      if(r._pend_ayudante1)    state.pending.prof.push({ role:'ayudante1', it });
      if(r._pend_ayudante2)    state.pending.prof.push({ role:'ayudante2', it });
      if(r._pend_arsenalera)   state.pending.prof.push({ role:'arsenalera', it });
    }
  }catch(err){
    console.warn('⚠️ No se pudo recalcular pendientes desde staging', err);
  }

  // 4) ✅ IMPORTANTE: cargar cola guardada (si existe)
  await loadDirtyQueueFromFirestore(importId);

  // 5) repintar UI
  state.ui.page = 1;
  paintPreview();
  setPills();
  setButtons();
  setStatus(`Staging cargado: ${state.stagedItems.length} filas · Import ${importId}`);
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

async function guardarColaEnProduccionConfirmada(queueItems){
  // queueItems = items ya “resueltos” o editados desde la cola (aunque antes estaban PENDIENTE)
  // OBJETIVO: escribir SI O SI en la producción confirmada (path final)

  const YYYY = String(state.year);
  const MM = pad(state.monthNum, 2);

  const batch = writeBatch(db);

  let escritos = 0;

  for (const it of queueItems) {
    const n = it.normalizado || {};
    const raw = it.raw || {};

    const fechaISO = n.fechaISO || parseDateToISO(raw['Fecha'] || '');
    const horaHM   = n.horaHM   || parseHora24(raw['Hora'] || '');
    const rutKey   = normalizeRutKey(n.rut || raw['RUT'] || '');

    // Si no hay ID estable, no podemos escribir confirmada
    if (!fechaISO || !horaHM || !rutKey) continue;

    const itemId = `${fechaISO}_${horaHM}`;

    // ✅ ESTE ES EL PATH CONFIRMADO (el definitivo)
    const ref = doc(db, 'produccion', YYYY, 'meses', MM, 'pacientes', rutKey, 'items', itemId);

    // Importante: MERGE para “pisar” los campos resueltos SIN romper lo ya confirmado
    // y mantener trazabilidad si usas importId.
    const payload = {
      raw: it.raw || {},
      normalizado: {
        ...(it.normalizado || {}),
        fechaISO,
        horaHM,
        rut: rutKey
      },
    
      // si quieres empujar también los campos “resueltos”:
      clinica: it.normalizado?.clinica ?? null,
      cirugia: it.normalizado?.cirugia ?? null,
      tipoPaciente: it.normalizado?.tipoPaciente ?? null,
      profesionales: it.normalizado?.profesionales || {},
    
      // marca post-fix
      pendiente: false,
    
      actualizadoEl: serverTimestamp(),
      actualizadoPor: (state.user?.email || '')
    };
    
    batch.set(ref, payload, { merge: true });

    escritos++;
  }

  if (!escritos) {
    toast('No hay ítems válidos en la cola para guardar en producción confirmada.', 'warn');
    return { escritos: 0 };
  }

  await batch.commit();
  toast(`Cola guardada en Producción Confirmada (${escritos}).`, 'ok');
  return { escritos };
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

  // ✅ 1) Guardar staging en Firestore (NECESARIO para confirmar y para sobrevivir recargas)
  await saveStagingToFirestore();

  // ✅ 2) Limpiar cola en Firestore (import nuevo)
  await clearDirtyQueueDoc(state.importId);

  // ✅ 3) Refrescar selector del mes y seleccionar el nuevo import
  await fillImportSuggestions();
  if ($('importSelect')) $('importSelect').value = state.importId;
  if ($('importId')) $('importId').value = state.importId;

  // ✅ 4) Re-cargar desde Firestore para:
  // - tener idx consistente (1..N)
  // - asegurar itemId estable
  // - asegurar que la cola se rehidrate bien
  await loadStagingFromFirestore(state.importId);
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

/* =========================================================
   EDIT ITEM — SOLO SELECTS (Opción A)
   ========================================================= */

// value especial para “pendiente”
const PEND_VALUE = '__PEND__';

function optionHtml(value, label, selected=false){
  const sel = selected ? 'selected' : '';
  return `<option value="${escapeHtml(value)}" ${sel}>${escapeHtml(label)}</option>`;
}

function buildSelectClinicaHTML(it){
  const n = it.normalizado || {};
  const raw = it.raw || {};
  const r = it.resolved || {};

  const clinCsv = clean(n.clinica || raw['Clínica'] || '');
  const clinicaId = clean(r.clinicaId || '');

  const opts = [];

  // ✅ Opción A: si hay ID, NO mostramos pendiente, preseleccionamos ID
  if(clinicaId){
    // opción “(seleccionada)” por ID
    for(const c of (state.catalogs.clinicas || [])){
      opts.push(optionHtml(c.id, `${c.nombre} (${c.id})`, c.id === clinicaId));
    }
    // fallback: si por alguna razón el ID no está en el catálogo cargado
    if(!opts.some(o => o.includes(`value="${escapeHtml(clinicaId)}"`))){
      opts.unshift(optionHtml(clinicaId, `(ID no encontrado en catálogo) ${clinicaId}`, true));
      for(const c of (state.catalogs.clinicas || [])){
        opts.push(optionHtml(c.id, `${c.nombre} (${c.id})`, false));
      }
    }
    return opts.join('');
  }

  // ✅ Si NO hay ID => aparece pendiente
  opts.push(optionHtml(PEND_VALUE, `⚠️ PENDIENTE: ${clinCsv || '(sin clínica)'}`, true));
  opts.push(optionHtml('', '(Seleccionar clínica)', false));
  for(const c of (state.catalogs.clinicas || [])){
    opts.push(optionHtml(c.id, `${c.nombre} (${c.id})`, false));
  }
  return opts.join('');
}

function buildSelectTipoPacienteHTML(it){
  const n = it.normalizado || {};
  const raw = it.raw || {};
  const tipoCsv = clean(n.tipoPaciente || raw['Tipo de Paciente'] || '');

  const opts = [];
  // Para tipo paciente NO hay “ID”; dejamos la lógica simple:
  // si viene vacío => pendiente, si viene con valor => seleccionado.
  if(!tipoCsv){
    opts.push(optionHtml(PEND_VALUE, '⚠️ PENDIENTE: (sin Tipo de Paciente)', true));
  } else {
    // Si viene con algo que no calza con catálogo, lo dejamos como primera opción “actual”
    if(!TIPOS_PACIENTE.some(t => normalizeKey(t) === normalizeKey(tipoCsv))){
      opts.push(optionHtml(tipoCsv, `Actual: ${tipoCsv}`, true));
    }
  }

  // listado oficial
  opts.push(optionHtml('', '(Seleccionar tipo)', !tipoCsv));
  for(const t of TIPOS_PACIENTE){
    const selected = normalizeKey(t) === normalizeKey(tipoCsv);
    opts.push(optionHtml(t, t, selected));
  }
  return opts.join('');
}

function buildSelectCirugiaHTML(it){
  const n = it.normalizado || {};
  const raw = it.raw || {};
  const r = it.resolved || {};

  const cirCsv = clean(n.cirugia || raw['Cirugía'] || '');
  const cirugiaId = clean(r.cirugiaId || '');

  const opts = [];

  // ✅ Opción A: si hay ID, NO mostramos pendiente, preseleccionamos ID
  if(cirugiaId){
    for(const s of (state.catalogs.cirugias || [])){
      opts.push(optionHtml(s.id, `${s.nombre} (${s.codigo || s.id})`, s.id === cirugiaId));
    }
    if(!opts.some(o => o.includes(`value="${escapeHtml(cirugiaId)}"`))){
      opts.unshift(optionHtml(cirugiaId, `(ID no encontrado en catálogo) ${cirugiaId}`, true));
      for(const s of (state.catalogs.cirugias || [])){
        opts.push(optionHtml(s.id, `${s.nombre} (${s.codigo || s.id})`, false));
      }
    }
    return opts.join('');
  }

  // ✅ Si NO hay ID => aparece pendiente
  opts.push(optionHtml(PEND_VALUE, `⚠️ PENDIENTE: ${cirCsv || '(sin cirugía)'}`, true));
  opts.push(optionHtml('', '(Seleccionar cirugía)', false));
  for(const s of (state.catalogs.cirugias || [])){
    opts.push(optionHtml(s.id, `${s.nombre} (${s.codigo || s.id})`, false));
  }
  return opts.join('');
}

function buildSelectProfesionalHTML(it, roleField, resolvedIdField, label){
  const n = it.normalizado || {};
  const raw = it.raw || {};
  const r = it.resolved || {};

  const profCsv = clean((n.profesionales||{})[roleField] || raw[label] || '');
  const profId = clean(r[resolvedIdField] || '');

  const opts = [];

  // ✅ Opción A: si hay ID, NO mostramos pendiente
  if(profId){
    // ✅ Orden por etiqueta agenda (APELLIDOS, NOMBRES)
    const profSorted = [...(state.catalogs.profesionales || [])]
      .map(p => ({ ...p, _agendaLabel: proEtiquetaAgenda(p.nombre || '', p.id || '') }))
      .sort((a,b)=> (a._agendaLabel||'').localeCompare((b._agendaLabel||''),'es'));

    for(const p of profSorted){
      opts.push(optionHtml(p.id, p._agendaLabel, p.id === profId));
    }

    // fallback: si por alguna razón el ID no está en el catálogo cargado
    if(!opts.some(o => o.includes(`value="${escapeHtml(profId)}"`))){
      opts.unshift(optionHtml(profId, `(ID no encontrado en catálogo) ${profId}`, true));
      for(const p of profSorted){
        opts.push(optionHtml(p.id, p._agendaLabel, false));
      }
    }

    return opts.join('');
  } // ✅ ESTA LLAVE ES LA CLAVE (cierra el IF)

  // Si no hay nombre en CSV, permitimos “(vacío)” sin bloquear
  if(!profCsv){
    opts.push(optionHtml('', '(Sin profesional)', true));
  } else {
    opts.push(optionHtml(PEND_VALUE, `⚠️ PENDIENTE: ${profCsv}`, true));
    opts.push(optionHtml('', '(Seleccionar profesional)', false));
  }

  const profSorted = [...(state.catalogs.profesionales || [])]
    .map(p => ({ ...p, _agendaLabel: proEtiquetaAgenda(p.nombre || '', p.id || '') }))
    .sort((a,b)=> (a._agendaLabel||'').localeCompare((b._agendaLabel||''),'es'));

  for(const p of profSorted){
    opts.push(optionHtml(p.id, p._agendaLabel, false));
  }

  return opts.join('');
}

function getTextFromSelect(selectEl){
  // devuelve label “limpio” del option seleccionado (sin el “(ID)”)
  if(!selectEl) return '';
  const opt = selectEl.options?.[selectEl.selectedIndex];
  return clean(opt?.textContent || '');
}

/* =========================================================
   EDITAR MÁS INFORMACIÓN (Modal avanzado - edición libre RAW)
   - Permite editar TODAS las columnas fuera del modal simple
   - Permite agregar campos nuevos (keys nuevas) => raw[key]=value
   - En STAGED: persiste en produccion_imports/{importId}/items/{itemId}
   - En CONFIRMADA: persiste en producción, pero NO permite cambiar Fecha/Hora/RUT
   ========================================================= */

const MORE_BLOCKED_KEYS_CONFIRMED = new Set(['Fecha','Hora','RUT']); // mover doc sería otro flujo

function ensureMoreModalDOM(){
  if(document.getElementById('modalMoreBackdrop')) return;

  const wrap = document.createElement('div');
  wrap.innerHTML = `
    <div id="modalMoreBackdrop" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,.35); z-index:9999;">
      <div style="max-width:980px; margin:5vh auto; background:#fff; border-radius:16px; box-shadow:0 18px 60px rgba(0,0,0,.25); overflow:hidden;">
        <div style="display:flex; align-items:center; justify-content:space-between; padding:14px 16px; border-bottom:1px solid #e5e7eb;">
          <div>
            <div style="font-weight:900; font-size:16px;">Editar más información</div>
            <div id="moreSub" style="font-size:12px; color:#6b7280; margin-top:2px;">—</div>
          </div>
          <button id="btnMoreCloseX" type="button" class="btn" style="border-radius:10px;">✕</button>
        </div>

        <div style="padding:14px 16px; border-bottom:1px solid #e5e7eb; display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
          <input id="moreSearch" type="text" placeholder="Buscar campo…" style="flex:1; min-width:220px; padding:10px 12px; border:1px solid #e5e7eb; border-radius:12px;">
          <button id="btnMoreAddField" type="button" class="btn soft">+ Agregar campo</button>
          <div id="moreHint" style="font-size:12px; color:#6b7280;"></div>
        </div>

        <div id="moreList" style="padding:14px 16px; max-height:62vh; overflow:auto;"></div>

        <div style="padding:14px 16px; border-top:1px solid #e5e7eb; display:flex; justify-content:space-between; gap:10px; flex-wrap:wrap;">
          <button id="btnMoreCancel" type="button" class="btn">Cerrar</button>
          <div style="display:flex; gap:10px; flex-wrap:wrap;">
            <button id="btnMoreApply" type="button" class="btn soft">Aplicar al ítem</button>
            <button id="btnMoreSave" type="button" class="btn primary">Guardar cambios</button>
          </div>
        </div>
      </div>
    </div>
  `;
  document.body.appendChild(wrap);

  // Cerrar por X / Cancel / backdrop
  document.getElementById('btnMoreCloseX').addEventListener('click', closeMoreModal);
  document.getElementById('btnMoreCancel').addEventListener('click', closeMoreModal);
  document.getElementById('modalMoreBackdrop').addEventListener('click', (e)=>{
    if(e.target === document.getElementById('modalMoreBackdrop')) closeMoreModal();
  });

  // Buscar
  document.getElementById('moreSearch').addEventListener('input', ()=>{
    const it = state._moreEditingItemRef;
    if(it) paintMoreModal(it);
  });

  // Agregar campo nuevo
  document.getElementById('btnMoreAddField').addEventListener('click', ()=>{
    addMoreRow('', '');
  });

  // Aplicar / Guardar
  document.getElementById('btnMoreApply').addEventListener('click', ()=>{
    const it = state._moreEditingItemRef;
    if(!it) return;
    const patch = collectMorePatch();
    applyMorePatchToItemInMemory(it, patch);
    toast('✅ Aplicado al ítem (en memoria).');
    // no cerramos, para seguir editando
    paintMoreModal(it);
    paintPreview();
  });

  document.getElementById('btnMoreSave').addEventListener('click', async ()=>{
    const it = state._moreEditingItemRef;
    if(!it) return;

    const patch = collectMorePatch();

    // En confirmada: NO permitir Fecha/Hora/RUT (mover doc sería otro flujo)
    if(state.status === 'confirmada'){
      for(const k of Object.keys(patch)){
        if(MORE_BLOCKED_KEYS_CONFIRMED.has(k)){
          toast('En confirmada no se puede cambiar Fecha/Hora/RUT desde este modal (movería el documento).');
          return;
        }
      }
    }

    await saveMorePatch(it, patch);
    toast('✅ Cambios guardados');
    paintMoreModal(it);
  });
}

function closeMoreModal(){
  const back = document.getElementById('modalMoreBackdrop');
  if(back) back.style.display = 'none';
  state._moreEditingItemRef = null;
}

function addMoreRow(key, value){
  const list = document.getElementById('moreList');
  if(!list) return;

  const row = document.createElement('div');
  row.className = 'moreRow';
  row.style.display = 'grid';
  row.style.gridTemplateColumns = '260px 1fr auto';
  row.style.gap = '10px';
  row.style.alignItems = 'start';
  row.style.padding = '10px 0';
  row.style.borderBottom = '1px dashed #e5e7eb';

  row.innerHTML = `
    <div>
      <div style="font-size:12px; color:#6b7280; margin-bottom:6px;">Campo</div>
      <input class="moreKey" type="text" value="${escapeHtml(key)}"
        style="width:100%; padding:10px 12px; border:1px solid #e5e7eb; border-radius:12px;">
    </div>

    <div>
      <div style="font-size:12px; color:#6b7280; margin-bottom:6px;">Valor</div>
      <textarea class="moreVal"
        style="width:100%; min-height:44px; padding:10px 12px; border:1px solid #e5e7eb; border-radius:12px; resize:vertical;">${escapeHtml(value)}</textarea>
    </div>

    <div style="display:flex; justify-content:flex-end; padding-top:22px;">
      <button type="button" class="btn moreDel" style="border-radius:10px;">Eliminar</button>
    </div>
  `;

  row.querySelector('.moreDel').addEventListener('click', ()=> row.remove());

  list.appendChild(row);
}

function keysInSimpleModal(){
  // Campos que ya están en el modal simple (no los repetimos aquí por defecto)
  return new Set([
    'Clínica','Tipo de Paciente','Cirugía',
    'Cirujano','Anestesista','Ayudante 1','Ayudante 2','Arsenalera'
  ]);
}

function paintMoreModal(it){
  ensureMoreModalDOM();

  const n = it.normalizado || {};
  const raw = it.raw || {};

  // subtitle
  const fechaISO = n.fechaISO || parseDateToISO(raw['Fecha'] || '');
  const horaHM = n.horaHM || parseHora24(raw['Hora'] || '');
  const rutKey = normalizeRutKey(n.rut || raw['RUT'] || '');
  const timeId = (fechaISO && horaHM) ? `${fechaISO}_${horaHM}` : '(sin fecha/hora)';
  const pacienteId = rutKey || '(sin RUT)';

  document.getElementById('moreSub').textContent = `Paciente: ${pacienteId} · Item: ${timeId} · Estado: ${state.status}`;

  // hint
  const hint = (state.status === 'confirmada')
    ? 'En confirmada: NO se permite cambiar Fecha/Hora/RUT (movería el documento).'
    : 'En staged: puedes cambiar cualquier campo (se guarda en staging).';
  document.getElementById('moreHint').textContent = hint;

  const q = normalizeKey(document.getElementById('moreSearch').value || '');

  const list = document.getElementById('moreList');
  list.innerHTML = '';

  const skip = keysInSimpleModal();

  // 1) Base: todas las EXPECTED_COLS excepto las del simple modal y '#'
  const baseKeys = EXPECTED_COLS.filter(k => k !== '#' && !skip.has(k));

  // 2) Extras: keys que existan en raw pero no estén en baseKeys ni en skip
  const extraKeys = Object.keys(raw || {}).filter(k => !baseKeys.includes(k) && !skip.has(k));

  // orden final: baseKeys + extras (extras ordenadas)
  extraKeys.sort((a,b)=> normalizeKey(a).localeCompare(normalizeKey(b)));
  const allKeys = [...baseKeys, ...extraKeys];

  // pintar filas
  for(const k of allKeys){
    const v = raw[k] ?? '';
    const show = !q || normalizeKey(k).includes(q) || normalizeKey(String(v)).includes(q);
    if(!show) continue;
    addMoreRow(k, String(v ?? ''));
  }

  // si con búsqueda no quedó nada, mostrar hint
  if(!list.children.length){
    const msg = document.createElement('div');
    msg.className = 'muted tiny';
    msg.textContent = 'Sin resultados para esa búsqueda. Usa “+ Agregar campo” para crear uno nuevo.';
    list.appendChild(msg);
  }

  // mostrar modal
  document.getElementById('modalMoreBackdrop').style.display = 'block';
}

function openMoreModalFromItemModal(){
  const it = state._editingItemRef;
  if(!it) return;
  state._moreEditingItemRef = it;
  paintMoreModal(it);
}

function collectMorePatch(){
  const list = document.getElementById('moreList');
  const patch = {};
  if(!list) return patch;

  const rows = [...list.querySelectorAll('.moreRow')];
  for(const row of rows){
    const k = clean(row.querySelector('.moreKey')?.value || '');
    const v = (row.querySelector('.moreVal')?.value ?? '').toString();

    if(!k) continue;
    patch[k] = v; // puede ser string vacío; eso significa "vaciar"
  }
  return patch;
}

function applyMorePatchToItemInMemory(it, patch){
  it.raw = it.raw || {};

  // aplicar patch a raw (vaciar => deja string vacío)
  for(const [k,v] of Object.entries(patch)){
    it.raw[k] = v;
  }

  // si tocó campos “estructurales”, recalculamos normalizado+resolved
  const structuralKeys = new Set([
    'Fecha','Hora','Clínica','Cirugía','Tipo de Paciente',
    'Nombre Paciente','RUT',
    'Valor','Derechos de Pabellón','HMQ','Insumos',
    'Suspendida','Confirmado','Pagado','Fecha de Pago',
    'Cirujano','Anestesista','Ayudante 1','Ayudante 2','Arsenalera'
  ]);

  let needsRebuild = false;
  for(const k of Object.keys(patch)){
    if(structuralKeys.has(k)){ needsRebuild = true; break; }
  }

  if(needsRebuild){
    it.normalizado = buildNormalizado(it.raw);
    it.resolved = resolveOneItem(it.normalizado || {});
    it._search = null;
  } else {
    // igual invalida búsqueda por seguridad
    it._search = null;
  }
}

async function saveMorePatch(it, patch){
  // 1) aplica en memoria
  applyMorePatchToItemInMemory(it, patch);

  // 2) persistencia según estado
  if(state.status === 'staged'){
    const importId = state.importId;
    const itemId = it.itemId;
    if(!importId || !itemId){
      console.warn('saveMorePatch: falta importId/itemId', { importId, itemId });
      toast('No se pudo guardar: falta itemId/importId (ver consola).');
      return;
    }

    const refStagingItem = doc(db, 'produccion_imports', importId, 'items', itemId);
    await setDoc(refStagingItem, {
      raw: it.raw,
      normalizado: it.normalizado,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    }, { merge:true });

    recomputePending();
    paintPreview();
    return;
  }

  if(state.status === 'confirmada'){
    const n = it.normalizado || {};
    const fechaISO = n.fechaISO || parseDateToISO(it.raw['Fecha'] || '');
    const horaHM = n.horaHM || parseHora24(it.raw['Hora'] || '');
    const rutKey = normalizeRutKey(n.rut || it.raw['RUT'] || '');
    if(!fechaISO || !horaHM || !rutKey){
      toast('No se pudo guardar en confirmada: falta Fecha/Hora/RUT.');
      return;
    }

    const YYYY = String(state.year);
    const MM = pad(state.monthNum,2);
    const pacienteId = rutKey;
    const timeId = `${fechaISO}_${horaHM}`;

    const refItem = doc(db, 'produccion', YYYY, 'meses', MM, 'pacientes', pacienteId, 'items', timeId);

    // guardamos raw + normalizado recalculado (si aplica) + resolved ids recalculados
    await updateDoc(refItem, {
      raw: it.raw,
      // opcional pero útil para consistencia:
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

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    });

    recomputePending();
    paintPreview();
    return;
  }

  // otros estados: no hacemos nada
  toast('Estado no permite guardar desde este modal.');
}

function openItemModal(it){

  state._editingItemRef = it; // ✅ fallback para textos “PENDIENTE” en los selects
  ensureMoreModalDOM();       // ✅ crea el modal avanzado si aún no existe
  
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
        <label>Clínica</label>
        <select id="edClinicaSel">
          ${buildSelectClinicaHTML(it)}
        </select>
        <div class="help">Si ya había ID, queda seleccionado sin “pendiente”.</div>
      </div>

      <div class="field">
        <label>Tipo de Paciente</label>
        <select id="edTipoSel">
          ${buildSelectTipoPacienteHTML(it)}
        </select>
      </div>
    </div>

    <div class="field" style="margin-top:10px;">
      <label>Cirugía</label>
      <select id="edCirugiaSel">
        ${buildSelectCirugiaHTML(it)}
      </select>
      <div class="help">La cirugía se resuelve por Clínica + Tipo Paciente + Cirugía.</div>
    </div>

    <div class="grid2" style="margin-top:10px;">
      <div class="field">
        <label>Cirujano</label>
        <select id="edCirujanoSel">
          ${buildSelectProfesionalHTML(it, 'cirujano', 'cirujanoId', 'Cirujano')}
        </select>
      </div>

      <div class="field">
        <label>Anestesista</label>
        <select id="edAnestesistaSel">
          ${buildSelectProfesionalHTML(it, 'anestesista', 'anestesistaId', 'Anestesista')}
        </select>
      </div>
    </div>

    <div class="grid3" style="margin-top:10px;">
      <div class="field">
        <label>Ayudante 1</label>
        <select id="edAy1Sel">
          ${buildSelectProfesionalHTML(it, 'ayudante1', 'ayudante1Id', 'Ayudante 1')}
        </select>
      </div>

      <div class="field">
        <label>Ayudante 2</label>
        <select id="edAy2Sel">
          ${buildSelectProfesionalHTML(it, 'ayudante2', 'ayudante2Id', 'Ayudante 2')}
        </select>
      </div>

      <div class="field">
        <label>Arsenalera</label>
        <select id="edArsSel">
          ${buildSelectProfesionalHTML(it, 'arsenalera', 'arsenaleraId', 'Arsenalera')}
        </select>
      </div>
    </div>

    <!-- ✅ NUEVO: botón para abrir modal avanzado -->
    <div style="display:flex; justify-content:flex-end; margin-top:12px;">
      <button id="btnMoreInfo" type="button" class="btn soft">Editar más información</button>
    </div>
  `;
  $('itemForm').innerHTML = form;

  // ✅ click botón "Editar más información"
  $('btnMoreInfo').onclick = ()=>{
    openMoreModalFromItemModal();
  };

  // key estable para acumular cambios
  const key = (pacienteId && timeId) ? `${pacienteId}||${timeId}` : `STAGING||${it.idx}`;

  // 1) Guardar 1 ítem (inmediato)
  $('btnGuardarItem').onclick = async ()=>{
    const patch = collectItemPatchFromModal();

    try{
      await saveOneItemPatch(it, patch);
      toast('✅ Item guardado');
      closeItemModal();
      refreshDirtyUI();

    }catch(err){
      console.error('❌ btnGuardarItem: no se pudo guardar', err);
      toast(`❌ No se pudo guardar: ${err?.message || 'ver consola'}`);
      // NO cerramos el modal para que puedas reintentar
    }
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
   state._editingItemRef = null; 
}

function collectItemPatchFromModal(){
  // Clínica
  const clinSel = $('edClinicaSel');
  const clinVal = clean(clinSel?.value || '');
  let clinicaTxt = '';

  if(clinVal && clinVal !== PEND_VALUE){
    // Si eligió una clínica del catálogo, guardamos su NOMBRE (para que el resolver por texto funcione)
    const c = state.catalogs.clinicasById.get(clinVal);
    clinicaTxt = clean(c?.nombre || '');
  } else {
    // pendiente: mantenemos el texto original (no lo inventamos)
    const it = state._editingItemRef || null;
    clinicaTxt = clean(it?.normalizado?.clinica || it?.raw?.['Clínica'] || '');
  }

  // Tipo Paciente
  const tipoSel = $('edTipoSel');
  const tipoVal = clean(tipoSel?.value || '');
  let tipoTxt = '';
  if(tipoVal && tipoVal !== PEND_VALUE) tipoTxt = tipoVal;
  else {
    const it = state._editingItemRef || null;
    tipoTxt = clean(it?.normalizado?.tipoPaciente || it?.raw?.['Tipo de Paciente'] || '');
  }

  // Cirugía
  const cirSel = $('edCirugiaSel');
  const cirVal = clean(cirSel?.value || '');
  let cirugiaTxt = '';
  if(cirVal && cirVal !== PEND_VALUE){
    const s = state.catalogs.cirugiasById.get(cirVal);
    cirugiaTxt = clean(s?.nombre || '');
  } else {
    const it = state._editingItemRef || null;
    cirugiaTxt = clean(it?.normalizado?.cirugia || it?.raw?.['Cirugía'] || '');
  }

  // Profesionales (guardamos el NOMBRE del catálogo si eligió uno)
  function pickProfText(selId, roleField, rawLabel){
    const sel = $(selId);
    const v = clean(sel?.value || '');
  
    if(!v) return ''; // “Sin profesional”
  
    if(v !== PEND_VALUE){
      const p = state.catalogs.profById.get(v);
      return clean(p?.nombre || '');
    }
  
    // pendiente => mantiene el texto original
    const it = state._editingItemRef || null;
    return clean(
      it?.normalizado?.profesionales?.[roleField] ||
      it?.raw?.[rawLabel] ||
      ''
    );
  }


  // ✅ Guardamos también: (1) IDs elegidos y (2) textos originales del CSV
  const it = state._editingItemRef || null;

  const origClin = clean(it?.normalizado?.clinica || it?.raw?.['Clínica'] || '');
  const origTipo = clean(it?.normalizado?.tipoPaciente || it?.raw?.['Tipo de Paciente'] || '');
  const origCir  = clean(it?.normalizado?.cirugia || it?.raw?.['Cirugía'] || '');

  const origProf = {
    cirujano:    clean(it?.normalizado?.profesionales?.cirujano    || it?.raw?.['Cirujano']     || ''),
    anestesista: clean(it?.normalizado?.profesionales?.anestesista || it?.raw?.['Anestesista']  || ''),
    ayudante1:   clean(it?.normalizado?.profesionales?.ayudante1   || it?.raw?.['Ayudante 1']   || ''),
    ayudante2:   clean(it?.normalizado?.profesionales?.ayudante2   || it?.raw?.['Ayudante 2']   || ''),
    arsenalera:  clean(it?.normalizado?.profesionales?.arsenalera  || it?.raw?.['Arsenalera']   || '')
  };

  // IDs elegidos (si el usuario eligió algo real)
  const clinIdSel = clean($('edClinicaSel')?.value || '');
  const cirIdSel  = clean($('edCirugiaSel')?.value || '');

  function pickProfId(selId){
    const v = clean($(selId)?.value || '');
    if(!v) return '';                  // “Sin profesional”
    if(v === PEND_VALUE) return '';    // pendiente
    return v;                          // id (rut)
  }

  const profIdsSel = {
    cirujanoId:    pickProfId('edCirujanoSel'),
    anestesistaId: pickProfId('edAnestesistaSel'),
    ayudante1Id:   pickProfId('edAy1Sel'),
    ayudante2Id:   pickProfId('edAy2Sel'),
    arsenaleraId:  pickProfId('edArsSel')
  };

  return {
    clinica: clinicaTxt,
    tipoPaciente: tipoTxt,
    cirugia: cirugiaTxt,

    profesionales: {
      cirujano:     pickProfText('edCirujanoSel',    'cirujano',    'Cirujano'),
      anestesista:  pickProfText('edAnestesistaSel', 'anestesista', 'Anestesista'),
      ayudante1:    pickProfText('edAy1Sel',         'ayudante1',   'Ayudante 1'),
      ayudante2:    pickProfText('edAy2Sel',         'ayudante2',   'Ayudante 2'),
      arsenalera:   pickProfText('edArsSel',         'arsenalera',  'Arsenalera')
    },

    // ✅ EXTRA (para “aprender”)
    _selectedIds: {
      clinicaId: (clinIdSel && clinIdSel !== PEND_VALUE) ? clinIdSel : '',
      cirugiaId: (cirIdSel  && cirIdSel  !== PEND_VALUE) ? cirIdSel  : '',
      profIds: profIdsSel
    },
    _originalCsv: {
      clinica: origClin,
      tipoPaciente: origTipo,
      cirugia: origCir,
      profesionales: origProf
    }
  };
}


async function saveOneItemPatch(it, patch, options = {}){
  // ✅ 0) “Aprender” mapping desde este ítem (para futuras importaciones)
  await learnMappingsFromItemDecision(patch);

  // 1) aplica al staging en memoria
  it.normalizado = it.normalizado || {};
  it.normalizado.clinica = patch.clinica || null;
  it.normalizado.tipoPaciente = patch.tipoPaciente || null;
  it.normalizado.cirugia = patch.cirugia || null;
  it.normalizado.profesionales = patch.profesionales || {};

  // ✅ IMPORTANTE: si el CSV trae Confirmado=Sí, mantenlo como "confirmado" en normalizado
  // (esto hace que un item "ya confirmado" se persista en Producción aunque el estado global sea staged)
  const confirmadoBool = (it.normalizado?.confirmado === true) || (toBool(it.raw?.['Confirmado']) === true);

  it.raw = it.raw || {};
  it.raw['Clínica'] = patch.clinica;
  it.raw['Tipo de Paciente'] = patch.tipoPaciente;
  it.raw['Cirugía'] = patch.cirugia;
  it.raw['Cirujano'] = patch.profesionales.cirujano;
  it.raw['Anestesista'] = patch.profesionales.anestesista;
  it.raw['Ayudante 1'] = patch.profesionales.ayudante1;
  it.raw['Ayudante 2'] = patch.profesionales.ayudante2;
  it.raw['Arsenalera'] = patch.profesionales.arsenalera;

  // ✅ si ya venía confirmado, asegúrate que siga marcado como Sí en la tabla
  if(confirmadoBool) it.raw['Confirmado'] = 'Sí';

  it._search = null;


  // 2) recalcula resolución local
  it.resolved = resolveOneItem(it.normalizado);
  
  console.log('🧠 resolveOneItem RESULT:', JSON.parse(JSON.stringify(it.resolved)));
  console.log('📦 PATCH recibido:', JSON.parse(JSON.stringify(patch)));
  
  // ✅ FIX: si el usuario eligió un ID explícito en el dropdown, úsalo como fuente de verdad
  // (evita depender del mapping por texto/contexto)
  const sel = patch?._selectedIds || {};
  console.log('👤 _selectedIds detectado:', JSON.parse(JSON.stringify(sel)));
  
  if(sel.clinicaId){
    it.resolved = it.resolved || {};
    it.resolved.clinicaId = sel.clinicaId;
    it.resolved.clinicaOk = true;
    it.resolved._pendClin = false;
    console.log('✅ Override manual clínica aplicado:', sel.clinicaId);
  }
  
  if(sel.cirugiaId){
    it.resolved = it.resolved || {};
    it.resolved.cirugiaId = sel.cirugiaId;
    it.resolved.cirugiaOk = true;
    it.resolved._pendCir = false;
    console.log('✅ Override manual cirugía aplicado:', sel.cirugiaId);
  }
  
  console.log('💾 RESOLVED FINAL QUE SE VA A GUARDAR:', JSON.parse(JSON.stringify(it.resolved)));


  /* =========================
     3) Persistencia
     - options.forceFinal === true  -> SIEMPRE a PRODUCCIÓN
     - Si NO forceFinal:
         - Si state.status === 'confirmada' -> PRODUCCIÓN
         - Si item está Confirmado=Sí       -> PRODUCCIÓN (aunque state.status sea staged)
         - Si no -> STAGING (produccion_imports)
  ========================= */

  const forceFinal = !!options.forceFinal;

  // --- helper: guardar en PRODUCCIÓN (ruta final) ---
  const saveToFinal = async () => {
    const n = it.normalizado || {};
    const fechaISO = n.fechaISO || parseDateToISO(it.raw['Fecha'] || '');
    const horaHM  = n.horaHM  || parseHora24(it.raw['Hora'] || '');
    const rutKey  = normalizeRutKey(n.rut || it.raw['RUT'] || '');

    if(!fechaISO || !horaHM || !rutKey){
      throw new Error(`FINAL: falta RUT/Fecha/Hora para guardar. rut=${rutKey} fecha=${fechaISO} hora=${horaHM}`);
    }

    const YYYY = String(state.year);
    const MM = pad(state.monthNum,2);
    const pacienteId = rutKey;
    const timeId = `${fechaISO}_${horaHM}`;

    const refItem = doc(db, 'produccion', YYYY, 'meses', MM, 'pacientes', pacienteId, 'items', timeId);

    await setDoc(refItem, {
      raw: it.raw,
      normalizado: it.normalizado,

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
        clinica: !!(it.resolved?._pendClin) || (it.resolved?.clinicaOk === false),
        cirugia: !!(it.resolved?._pendCir)  || (it.resolved?.cirugiaOk === false),
        profesionales: {
          cirujano:    !!(it.resolved?._pend_cirujano)    || (it.resolved?.cirujanoOk === false),
          anestesista: !!(it.resolved?._pend_anestesista) || (it.resolved?.anestesistaOk === false),
          ayudante1:   !!(it.resolved?._pend_ayudante1)   || (it.resolved?.ayudante1Ok === false),
          ayudante2:   !!(it.resolved?._pend_ayudante2)   || (it.resolved?.ayudante2Ok === false),
          arsenalera:  !!(it.resolved?._pend_arsenalera)  || (it.resolved?.arsenaleraOk === false)
        },
        tipoPaciente: !clean(n.tipoPaciente || '')
      },

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || '',
      importId: state.importId || null
    }, { merge:true });
  };

  // --- helper: guardar en IMPORT (staging) ---
  const saveToStaging = async () => {
    const importId = state.importId;
    const itemId = it.itemId;

    if(!importId || !itemId){
      console.error('❌ STAGED: falta importId/itemId', { importId, itemId, it });
      throw new Error(`STAGED: falta importId/itemId para guardar. importId=${importId} itemId=${itemId}`);
    }

    const refStagingItem = doc(db, 'produccion_imports', importId, 'items', itemId);

    await setDoc(refStagingItem, {
      raw: it.raw,
      normalizado: it.normalizado,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    }, { merge:true });
  };

  // ✅ Decisión final (AQUÍ está la corrección clave)
  // ✅ Decisión final
  if (forceFinal) {
    // Si forzamos final: guardamos en producción…
    await saveToFinal();
  
    // …y si existe staging, lo mantenemos sincronizado (para que al F5 no “reviva” el CSV viejo)
    if (state.importId && it.itemId) {
      await saveToStaging();
    }
  
  } else {
    if (state.status === 'staged') {
      // staging normal
      await saveToStaging();
    }
  
    if (state.status === 'confirmada') {
      // ✅ confirmada: guardamos en producción…
      await saveToFinal();
  
      // ✅ …y también en staging para que loadStagingFromFirestore() refleje el cambio tras F5
      if (state.importId && it.itemId) {
        await saveToStaging();
      }
    }
  }

  // ✅ Siempre refrescar UI después de guardar
  recomputePending();
  paintPreview();
}

// ✅ Guarda la cola completa
// - staged     -> guarda en STAGING (produccion_imports)
// - confirmada -> guarda en PRODUCCIÓN
async function saveAllDirtyEdits(){
  if(state.status === 'confirmada'){
    await flushDirtyEdits({ forceFinal: true });   // PRODUCCIÓN
  } else {
    await flushDirtyEdits({ forceFinal: false });  // STAGING
  }
}



/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user) => {
    state.user = user;

    // Sidebar / nav
    await loadSidebar({ active: 'produccion' });
    setActiveNav('produccion');

    // UI usuario
    const whoEl = $('who');
    if (whoEl) whoEl.textContent = `Conectado: ${user.email}`;
    wireLogout();

    // ✅ Al entrar: por defecto muestra el MES PASADO (ej: enero 2026 → diciembre 2025)
    setDefaultToPreviousMonth();

    // Header tabla
    buildThead();

    // Data base
    await loadMappings();
    await loadCatalogs();

    // Render inicial
    recomputePending();
    setButtons();
    paintPreview();

    // ✅ Import selector: cargar sugerencias al entrar
    await fillImportSuggestions();

    // ✅ (OPCIONAL PERO RECOMENDADO)
    // Si fillImportSuggestions() auto-seleccionó el 1° ítem, cargamos staging automáticamente.
    if ($('importSelect')?.value) {
      $('importId').value = clean($('importSelect').value || '');
      await loadStagingFromFirestore($('importSelect').value);
    }

    /* -------------------------
       Import selector + ImportID
       (UNO SOLO, sin duplicar)
    ------------------------- */
    const syncImportIdFromSelect = () => {
      const sel = $('importSelect');
      const hid = $('importId');
      if (!sel || !hid) return '';
      const v = clean(sel.value || '');
      hid.value = v;
      return v;
    };

    $('importSelect')?.addEventListener('change', async () => {
      const v = syncImportIdFromSelect();
      if (v) await loadStagingFromFirestore(v);
    });

    /* -------------------------
       Si cambia mes/año: refrescar sugerencias
       (y limpiar selection/importId)
    ------------------------- */
    $('mes')?.addEventListener('change', async () => {
      await fillImportSuggestions();
      if ($('importSelect')) $('importSelect').value = '';
      if ($('importId')) $('importId').value = '';
    });

    $('ano')?.addEventListener('change', async () => {
      await fillImportSuggestions();
      if ($('importSelect')) $('importSelect').value = '';
      if ($('importId')) $('importId').value = '';
    });

    /* -------------------------
       Buscar
    ------------------------- */
    $('q')?.addEventListener('input', (e) => {
      state.ui.query = e.target.value || '';
      state.ui.page = 1;
      paintPreview();
    });

    /* -------------------------
       Pager
    ------------------------- */
    $('btnPrev')?.addEventListener('click', () => {
      state.ui.page = Math.max(1, (state.ui.page || 1) - 1);
      paintPreview();
    });

    $('btnNext')?.addEventListener('click', () => {
      state.ui.page = (state.ui.page || 1) + 1;
      paintPreview();
    });

    /* -------------------------
       Cargar CSV
    ------------------------- */
    $('csvFile')?.addEventListener('change', async (e) => {
      const f = e.target.files?.[0];
      if (!f) return;
      await handleLoadCSV(f);
      e.target.value = ''; // permitir recargar el mismo archivo sin renombrar
    });
    
    $('btnLimpiarCola')?.addEventListener('click', async () => {
      const n = dirtyCount();
      if(n === 0){
        toast('No hay cola para limpiar.');
        return;
      }
    
      const ok = confirm(`¿Limpiar la cola de cambios? (${n} pendiente/s)\n\nEsto NO guarda cambios; solo los descarta.`);
      if(!ok) return;
    
      // limpia en memoria
      state.dirtyEdits.clear();
      refreshDirtyUI();
    
      // limpia en Firestore (solo si hay import actual)
      if(state.importId){
        await clearDirtyQueueDoc(state.importId);
      }
    
      toast('✅ Cola limpiada');
    });


    /* -------------------------
       Resolver modal
    ------------------------- */
    $('btnResolver')?.addEventListener('click', () => openResolverModal());
    $('btnResolverClose')?.addEventListener('click', () => closeResolverModal());
    $('modalResolverBackdrop')?.addEventListener('click', (e) => {
      if (e.target === $('modalResolverBackdrop')) closeResolverModal();
    });

    /* -------------------------
       Confirmar / Anular
    ------------------------- */
    $('btnConfirmar')?.addEventListener('click', confirmarImportacion);
    $('btnAnular')?.addEventListener('click', anularImportacion);

    /* -------------------------
       Cargar staging manual por ImportID
    ------------------------- */
    $('btnCargarImport')?.addEventListener('click', async () => {
      const importId = clean($('importId')?.value || '');
      if(!importId){
        toast('Ingresa o selecciona un ImportID.');
        return;
      }
      await loadStagingFromFirestore(importId);
    });

  /* --------------------------
     Guardar cola (“Guardar todo”) real
  -------------------------- */
  $('btnGuardarCola')?.addEventListener('click', () => saveAllDirtyEdits());

  /* --------------------------
     UI cola (si existe)
  -------------------------- */
  refreshDirtyUI();

}  // ✅ cierre correcto del callback onUser()

});


