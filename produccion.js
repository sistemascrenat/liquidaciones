// produccion.js — COMPLETO (mejorado + cirugías con contexto Tipo Paciente + Clínica)
// ✅ Importación CSV (staging + confirmar + anular)
// ✅ Guarda TODAS las columnas del CSV en raw, pero NUNCA guarda strings vacíos
// ✅ Incluye "Resolución" (Clínicas / Cirugías / Ambulatorios) con mappings persistentes
// ✅ Confirmar queda bloqueado si hay pendientes
// ✅ Confirmar escribe IDs resueltos: clinicaId / cirugiaId / ambulatorioId
// ✅ Cirugías pendientes muestran CONTEXTO: Tipo Paciente + Clínica (sin romper mappings por nombre exacto)
// ✅ Anular paginado (sin límite fijo)

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

// Firestore NO permite campos vacíos "" dentro de maps.
// Regla: devolvemos undefined si está vacío.
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

/* =========================
   Tipo Paciente (normalización)
   - NO rompe nada: es solo un "helper" para contexto + futura tarifación.
   - Se guarda como tipoPacienteKey en produccion al confirmar.
========================= */
function normalizePacienteKey(tipoPaciente, cirugiaName=''){
  const t = normalizeKey(tipoPaciente);
  const s = normalizeKey(cirugiaName);

  // 1) por columna Tipo de Paciente
  const byCol = (() => {
    if(!t) return '';
    if(t.includes('fonasa')) return 'fonasa';
    if(t.includes('isapre')) return 'isapre';
    if(t.includes('part')) return 'particular';
    if(t.includes('priv')) return 'particular';
    if(t.includes('particular')) return 'particular';
    // si viene otra cosa, lo dejamos "t" (compacto)
    return t;
  })();

  if(byCol) return byCol;

  // 2) fallback por texto de cirugía (casos tipo "part bypass")
  // (solo para CONTEXTO visual; NO alteramos el mapeo por nombre exacto)
  if(s.startsWith('part ') || s.startsWith('particular ')) return 'particular';
  if(s.startsWith('fona ') || s.startsWith('fonasa ')) return 'fonasa';
  if(s.startsWith('isa ') || s.startsWith('isapre ')) return 'isapre';

  return '';
}

function pacienteLabel(key){
  const k = normalizeKey(key);
  if(k === 'particular' || k === 'part') return 'Particular';
  if(k === 'fonasa') return 'Fonasa';
  if(k === 'isapre') return 'Isapre';
  if(!k) return '—';
  // capitaliza simple
  return k.charAt(0).toUpperCase() + k.slice(1);
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
const colProduccion = collection(db, 'produccion');

// Catálogos (mismos nombres que ya vienes usando)
const colClinicas = collection(db, 'clinicas');
const colProcedimientos = collection(db, 'procedimientos');
const colAmbulatorios = collection(db, 'ambulatorios');

// Mappings persistentes
const docMapClinicas = doc(db, 'produccion_mappings', 'clinicas');
const docMapCirugias  = doc(db, 'produccion_mappings', 'cirugias');
const docMapAmb       = doc(db, 'produccion_mappings', 'ambulatorios');

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

  stagedItems: [], // [{ idx, raw, normalizado, resolved }]

  // catálogos cargados
  catalogs: {
    clinicas: [],             // [{id, nombre}]
    clinicasByNorm: new Map(),// norm(nombre)-> {id,nombre}
    clinicasById: new Map(),  // id -> {id,nombre}

    cirugias: [],             // [{id, nombre, codigo}]
    cirugiasByNorm: new Map(),// norm(nombre)-> {id,nombre,codigo}
    cirugiasById: new Map(),

    amb: [],                  // [{id, nombre}]
    ambByNorm: new Map(),
    ambById: new Map()
  },

  // mappings
  maps: {
    clinicas: new Map(),   // normCsv -> {id}
    cirugias: new Map(),   // normCsv -> {id}   (por NOMBRE EXACTO normalizado)
    amb: new Map()
  },

  // pendientes detectados (UI)
  pending: {
    clinicas: [], // [{csvName, norm}]
    cirugias: [], // [{csvName, norm, suggestions, contexts:[{clinicaText, clinicaId, pacienteKey}] }]
    amb: []       // [{csvName, norm}]
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

function paintPreview(){
  const tb = $('tbody');
  tb.innerHTML = '';

  const rows = state.stagedItems || [];
  $('countPill').textContent = `${rows.length} fila${rows.length===1?'':'s'}`;

  const max = Math.min(rows.length, 60);

  for(let i=0;i<max;i++){
    const it = rows[i];
    const n = it.normalizado || {};
    const r = it.resolved || {};
    const prof = n.profesionalesResumen || '';

    const flags = [];
    if(r.clinicaOk === false) flags.push('Clínica');
    if(r.cirugiaOk === false) flags.push('Cirugía');
    if(r.ambOk === false) flags.push('Amb');

    const st =
      flags.length === 0
        ? `<span class="ok">OK</span>`
        : `<span class="warn">Pendiente: ${flags.join(', ')}</span>`;

    const clinicaNice = (() => {
      if(r.clinicaId){
        const c = state.catalogs.clinicasById.get(r.clinicaId);
        return c?.nombre ? `${escapeHtml(c.nombre)} <span class="muted tiny mono">(${escapeHtml(r.clinicaId)})</span>` : escapeHtml(r.clinicaId);
      }
      return escapeHtml(n.clinica || '—');
    })();

    const cirugiaNice = (() => {
      if(r.cirugiaId){
        const s = state.catalogs.cirugiasById.get(r.cirugiaId);
        return s?.nombre ? `${escapeHtml(s.nombre)} <span class="muted tiny mono">(${escapeHtml(s.codigo || s.id)})</span>` : escapeHtml(r.cirugiaId);
      }
      return escapeHtml(n.cirugia || '—');
    })();

    const pacKey = n.tipoPacienteKey || '';
    const pacNice = pacienteLabel(pacKey);

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono">${pad(i+1,2)}</td>
      <td>${escapeHtml(n.fecha || '—')}</td>
      <td>${escapeHtml(n.hora || '—')}</td>
      <td>
        <div><b>${clinicaNice}</b></div>
        <div class="tiny muted">${r.clinicaId ? `ID: ${escapeHtml(r.clinicaId)}` : 'ID: —'}</div>
      </td>
      <td>
        <div><b>${cirugiaNice}</b></div>
        <div class="tiny muted">${r.cirugiaId ? `ID: ${escapeHtml(r.cirugiaId)}` : 'ID: —'}</div>
      </td>
      <td>
        <div>${escapeHtml(n.tipoPaciente || '—')}</div>
        <div class="tiny muted">Key: ${escapeHtml(pacKey || '—')} · ${escapeHtml(pacNice)}</div>
      </td>
      <td><b>${clp(n.valor || 0)}</b></td>
      <td>${clp(n.dp || 0)}</td>
      <td>${clp(n.hmq || 0)}</td>
      <td>${clp(n.ins || 0)}</td>
      <td class="tiny">${prof ? escapeHtml(prof) : '<span class="muted">—</span>'}</td>
      <td>${st}</td>
    `;
    tb.appendChild(tr);
  }

  if(rows.length > max){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td colspan="12" class="muted tiny">
        Mostrando ${max} de ${rows.length}. (El resto igual quedó en staging)
      </td>
    `;
    tb.appendChild(tr);
  }
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

  const tipoPacienteKey = normalizePacienteKey(tipoPaciente, cirugia);

  return {
    fecha: fecha || null,
    hora: hora || null,
    clinica: clinica || null,
    cirugia: cirugia || null,
    tipoPaciente: tipoPaciente || null,

    // nuevo (no rompe)
    tipoPacienteKey: tipoPacienteKey || null,

    valor,
    dp,
    hmq,
    ins,

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
   Load catalogs + mappings
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

  // AMBULATORIOS (si no existe colección, quedará vacío)
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
   Resolution engine
========================= */
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
    ambOk: true
  };

  // --- Clínica ---
  const clinTxt = clean(normalizado.clinica || '');
  if(clinTxt){
    const norm = normalizeKey(clinTxt);

    // 1) mapping explícito
    const mapped = state.maps.clinicas.get(norm);
    if(mapped?.id){
      resolved.clinicaId = mapped.id;
      resolved.clinicaOk = true;
    } else {
      // 2) match por catálogo (nombre exacto normalizado)
      const found = state.catalogs.clinicasByNorm.get(norm);
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

  // --- Cirugía (mapping por NOMBRE EXACTO normalizado, pero con CONTEXTO visual) ---
  const cirTxt = clean(normalizado.cirugia || '');
  if(cirTxt){
    const norm = normalizeKey(cirTxt);

    const mapped = state.maps.cirugias.get(norm);
    if(mapped?.id){
      resolved.cirugiaId = mapped.id;
      resolved.cirugiaOk = true;
    } else {
      const found = state.catalogs.cirugiasByNorm.get(norm);
      if(found?.id){
        resolved.cirugiaId = found.id;
        resolved.cirugiaOk = true;
      } else {
        resolved.cirugiaOk = false;
      }
    }
  } else {
    resolved.cirugiaOk = false;
  }

  // --- Ambulatorio (placeholder) ---
  resolved.ambOk = true;
  resolved.ambulatorioId = null;

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

  // Cirugías: agrupamos por nombre exacto (norm), PERO agregamos contextos (tipo paciente + clínica)
  const seenCir = new Map(); // norm -> {csvName,norm,suggestions,contextsMap}
  // contextsMap key: `${clinicaId||clinicaText}||${pacienteKey||''}`

  for(const it of state.stagedItems){
    const n = it.normalizado || {};
    const r = it.resolved || {};

    // Clínicas pendientes
    const clinTxt = clean(n.clinica || '');
    if(clinTxt && r.clinicaOk === false){
      const norm = normalizeKey(clinTxt);
      if(!seenClin.has(norm)){
        seenClin.add(norm);
        state.pending.clinicas.push({ csvName: clinTxt, norm });
      }
    }

    // Cirugías pendientes (con contexto)
    const cirTxt = clean(n.cirugia || '');
    if(cirTxt && r.cirugiaOk === false){
      const norm = normalizeKey(cirTxt);

      let bucket = seenCir.get(norm);
      if(!bucket){
        bucket = {
          csvName: cirTxt,
          norm,
          suggestions: suggestCirugias(norm),
          contextsMap: new Map()
        };
        seenCir.set(norm, bucket);
      }

      const clinicaId = r.clinicaId || '';
      const clinicaText = clean(n.clinica || '') || '—';
      const pacienteKey = clean(n.tipoPacienteKey || '') || clean(normalizePacienteKey(n.tipoPaciente, n.cirugia)) || '';

      const ck = `${clinicaId || clinicaText}||${pacienteKey || ''}`;
      if(!bucket.contextsMap.has(ck)){
        bucket.contextsMap.set(ck, {
          clinicaId: clinicaId || null,
          clinicaText,
          pacienteKey: pacienteKey || null
        });
      }
    }

    // ambulatorios: hoy no bloquea
  }

  // Ordena
  state.pending.clinicas.sort((a,b)=> a.norm.localeCompare(b.norm));

  // Construye array final de cirugías pendientes
  const cirPend = [];
  for(const bucket of seenCir.values()){
    const contexts = Array.from(bucket.contextsMap.values())
      .slice(0, 12); // límite visual razonable
    cirPend.push({
      csvName: bucket.csvName,
      norm: bucket.norm,
      suggestions: bucket.suggestions,
      contexts
    });
  }
  cirPend.sort((a,b)=> a.norm.localeCompare(b.norm));
  state.pending.cirugias = cirPend;

  state.pending.amb.sort((a,b)=> a.norm.localeCompare(b.norm));

  setPills();
  setButtons();
  paintPreview();
}

/* =========================
   Mappings: persist
========================= */
async function persistMapping(docRef, normKey, id){
  if(!normKey || !id) return;

  await setDoc(docRef, {
    map: {
      [normKey]: { id, actualizadoEl: serverTimestamp(), actualizadoPor: state.user?.email || '' }
    },
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge: true });

  if(docRef === docMapClinicas) state.maps.clinicas.set(normKey, { id });
  if(docRef === docMapCirugias)  state.maps.cirugias.set(normKey, { id });
  if(docRef === docMapAmb)       state.maps.amb.set(normKey, { id });
}

/* =========================
   Modal Resolver
========================= */
function openResolverModal(){
  $('modalResolverBackdrop').style.display = 'grid';
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

  /* -------- CLINICAS -------- */
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
        const id = prompt('ID de clínica (ej: CLINICA_4 o C004). Puedes editar:', suggested) || '';
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

  /* -------- AMBULATORIOS (placeholder) -------- */
  const wrapA = $('resolverAmbList');
  wrapA.innerHTML = `<div class="muted tiny">— (Tu CSV actual no trae ambulatorios. Cuando lo agregues, aquí aparecerán.)</div>`;

  /* -------- CIRUGIAS -------- */
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
        .map(x=> `<span class="pill warn" style="cursor:pointer;" data-sug-cir="${escapeHtml(item.norm)}" data-sug-id="${escapeHtml(x.id)}">${escapeHtml(x.nombre)}</span>`)
        .join(' ');

      // Contextos: clínica + tipo paciente (esto era tu requerimiento)
      const contexts = (item.contexts || []).map(ctx=>{
        const clin = ctx.clinicaId
          ? (state.catalogs.clinicasById.get(ctx.clinicaId)?.nombre || ctx.clinicaId)
          : (ctx.clinicaText || '—');
        const clinShow = ctx.clinicaId ? `${clin} (${ctx.clinicaId})` : clin;
        const pacShow = pacienteLabel(ctx.pacienteKey || '');
        return `<span class="pill" title="Contexto"><b>${escapeHtml(pacShow)}</b> · ${escapeHtml(clinShow)}</span>`;
      }).join(' ');

      row.innerHTML = `
        <div>
          <div style="font-weight:900;">${escapeHtml(item.csvName)}</div>
          <div class="muted tiny mono">key: ${escapeHtml(item.norm)}</div>

          <div class="help" style="margin-top:8px;">
            Contexto detectado (Tipo Paciente + Clínica):
          </div>
          <div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">
            ${contexts || `<span class="muted tiny">—</span>`}
          </div>

          ${sug ? `
            <div class="help" style="margin-top:10px;">Sugerencias rápidas:</div>
            <div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">${sug}</div>
          ` : `<div class="muted tiny" style="margin-top:10px;">Sin sugerencias.</div>`}
        </div>

        <div class="field" style="margin:0;">
          <label>Asociar a</label>
          <select data-assoc-cir="${escapeHtml(item.norm)}">
            <option value="">(Seleccionar cirugía)</option>
            ${options}
          </select>
          <div class="help">
            o <button class="linkBtn" data-go-cir="${escapeHtml(item.norm)}" type="button">crear en Cirugías</button>
          </div>
        </div>

        <div style="display:flex; gap:8px; justify-content:flex-end;">
          <button class="btn primary" data-save-cir="${escapeHtml(item.norm)}" type="button">Guardar</button>
        </div>
      `;

      // click sugerencias => setea select
      row.querySelectorAll('[data-sug-cir]').forEach(pill=>{
        pill.addEventListener('click', ()=>{
          const id = pill.getAttribute('data-sug-id') || '';
          const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.norm)}"]`);
          sel.value = id;
        });
      });

      // guardar asociación (por nombre exacto normalizado)
      row.querySelector(`[data-save-cir="${CSS.escape(item.norm)}"]`).addEventListener('click', async ()=>{
        const sel = row.querySelector(`select[data-assoc-cir="${CSS.escape(item.norm)}"]`);
        const id = sel.value || '';
        if(!id){ toast('Selecciona una cirugía'); return; }
        await persistMapping(docMapCirugias, item.norm, id);
        toast('Cirugía asociada');
        await refreshAfterMapping();
        paintResolverModal();
      });

      // ir a cirugías para crear (prefill)
      row.querySelector(`[data-go-cir="${CSS.escape(item.norm)}"]`).addEventListener('click', ()=>{
        try{
          localStorage.setItem('CR_PREFILL_CIRUGIA_NOMBRE', item.csvName);
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
  return `CLINICA_${tail}`;
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

  const y = state.year;
  const m = pad(state.monthNum, 2);

  const batchSize = 350;
  const docs = [];
  itemsSnap.forEach(d => docs.push({ id: d.id, data: d.data() || {} }));

  let i = 0;
  while(i < docs.length){
    const batch = writeBatch(db);
    const slice = docs.slice(i, i + batchSize);

    slice.forEach(({id, data})=>{
      const n = data.normalizado || {};
      const raw = data.raw || {};

      const resolved = resolveOneItem(n);

      const prodId = `PROD_${y}_${m}_${importId}_${id}`;
      const refProd = doc(db, 'produccion', prodId);

      batch.set(refProd, {
        id: prodId,
        importId,
        importItemId: id,
        mes: state.monthName,
        mesNum: state.monthNum,
        ano: state.year,

        // human
        fecha: n.fecha ?? null,
        hora: n.hora ?? null,
        clinica: n.clinica ?? null,
        cirugia: n.cirugia ?? null,
        tipoPaciente: n.tipoPaciente ?? null,

        // NUEVO (no rompe): key normalizada del tipo paciente
        tipoPacienteKey: n.tipoPacienteKey ?? null,

        // IDs resueltos
        clinicaId: resolved.clinicaId ?? null,
        cirugiaId: resolved.cirugiaId ?? null,
        ambulatorioId: resolved.ambulatorioId ?? null,

        // números
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

        raw,
        estado: 'activa',
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge: true });
    });

    await batch.commit();
    i += batchSize;
  }

  await setDoc(refImport, {
    estado: 'confirmada',
    confirmadoEl: serverTimestamp(),
    confirmadoPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge: true });

  state.status = 'confirmada';
  setStatus(`✅ Importación confirmada: ${importId}`);
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

  let last = null;
  let total = 0;

  while(true){
    const qy = last
      ? query(colProduccion, where('importId','==', importId), orderBy('__name__'), startAfter(last), limit(400))
      : query(colProduccion, where('importId','==', importId), orderBy('__name__'), limit(400));

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
  setStatus(`⛔ Importación anulada: ${importId} (${total} filas desactivadas)`);
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
    staged.push({ idx: i, raw, normalizado, resolved: null });
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

  $('importId').value = importId;

  setStatus(`🟡 Staging listo: ${staged.length} filas (sin afectar liquidaciones)`);

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

  setStatus(state.status === 'staged'
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

    $('mes').value = 'Octubre';

    setStatus('—');

    // carga inicial catálogos + mappings (por si entras sin cargar CSV)
    await loadMappings();
    await loadCatalogs();

    recomputePending();
    setButtons();
    paintPreview();

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
  }
});
