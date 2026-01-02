// produccion.js — COMPLETO
// ✅ Importación CSV (staging + confirmar + anular)
// ✅ Guarda TODAS las columnas del CSV en raw, pero NUNCA guarda strings vacíos
// ✅ Estructura:
//   - produccion_imports/{importId}  (estado: staged/confirmada/anulada)
//   - produccion_imports/{importId}/items/{ITEM_0001...} (raw + normalizado)
//   - produccion/{prodId} (solo cuando confirmas) -> fuente única para Liquidaciones

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { loadSidebar } from './layout.js';

import {
  collection, doc, setDoc, getDoc, getDocs, writeBatch,
  serverTimestamp, query, where, orderBy, limit
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   DOM helpers
========================= */
const $ = (id)=> document.getElementById(id);

/* =========================
   Columnas EXACTAS del CSV (como tú las listaste)
   (OJO: el CSV puede venir con "Clínica" con tilde, etc.)
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
function clean(s){
  return (s ?? '').toString().trim();
}

function normalizeKey(s){
  return clean(s)
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase();
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
  // acepta "$ 1.234.567" / "1.234.567" / "1234567"
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
  // 2026-01-02T03:04:05.123Z -> 20260102_030405
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

// Detecta delimitador por conteo
function detectDelimiter(text){
  const head = text.split(/\r?\n/).slice(0,5).join('\n');
  const semis = (head.match(/;/g) || []).length;
  const commas = (head.match(/,/g) || []).length;
  return semis > commas ? ';' : ',';
}

// Parser CSV básico con soporte de comillas
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

  // último campo
  if(cur.length || row.length){
    row.push(cur);
    rows.push(row);
  }

  // limpiar filas vacías
  return rows
    .map(r=> r.map(c=> (c ?? '').toString()))
    .filter(r=> r.some(c=> clean(c) !== ''));
}

/* =========================
   Firestore refs
========================= */
const colImports = collection(db, 'produccion_imports');
const colProduccion = collection(db, 'produccion');

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
  stagedItems: [] // [{ idx, normalizado, rawCompact }]
};

/* =========================
   UI render
========================= */
function setStatus(text){
  $('statusInfo').textContent = text || '—';
}

function setButtons(){
  const staged = state.status === 'staged';
  const confirmed = state.status === 'confirmada';
  const anulada = state.status === 'anulada';

  $('btnConfirmar').disabled = !staged;
  $('btnAnular').disabled = !(staged || confirmed); // puedes anular también confirmada (tú dijiste que sí)
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
    const prof = n.profesionalesResumen || '';

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono">${pad(i+1,2)}</td>
      <td>${n.fecha || '<span class="muted">—</span>'}</td>
      <td>${n.hora || '<span class="muted">—</span>'}</td>
      <td>${n.clinica || '<span class="muted">—</span>'}</td>
      <td><b>${n.cirugia || '<span class="muted">—</span>'}</b></td>
      <td>${n.tipoPaciente || '<span class="muted">—</span>'}</td>
      <td><b>${clp(n.valor || 0)}</b></td>
      <td>${clp(n.dp || 0)}</td>
      <td>${clp(n.hmq || 0)}</td>
      <td>${clp(n.ins || 0)}</td>
      <td class="tiny">${prof ? prof : '<span class="muted">—</span>'}</td>
    `;
    tb.appendChild(tr);
  }

  if(rows.length > max){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td colspan="11" class="muted tiny">
        Mostrando ${max} de ${rows.length}. (El resto igual quedó en staging)
      </td>
    `;
    tb.appendChild(tr);
  }
}

/* =========================
   Core: convertir CSV -> staging items
========================= */
function buildHeaderIndex(headerRow){
  // Normalizamos headers del archivo para mapearlos a EXPECTED_COLS.
  const idx = new Map(); // expectedCol -> index
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
  // Guarda TODAS las columnas, pero SOLO si tienen valor (no string vacío)
  const raw = {};
  for(const col of EXPECTED_COLS){
    const val = nonEmptyOrUndef(readCell(row, headerIdx, col));
    if(val !== undefined) raw[col] = val;
  }
  return raw;
}

function buildNormalizado(raw){
  // Campos clave para UI / Liquidaciones:
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

  // Profesionales por rol (texto como venga en CSV)
  const prof = {
    cirujano: raw['Cirujano'] ?? null,
    anestesista: raw['Anestesista'] ?? null,
    ayudante1: raw['Ayudante 1'] ?? null,
    ayudante2: raw['Ayudante 2'] ?? null,
    arsenalera: raw['Arsenalera'] ?? null
  };

  // Resumen legible para tabla
  const parts = [];
  if(prof.cirujano) parts.push(`Cirujano: ${prof.cirujano}`);
  if(prof.anestesista) parts.push(`Anest: ${prof.anestesista}`);
  if(prof.ayudante1) parts.push(`Ay1: ${prof.ayudante1}`);
  if(prof.ayudante2) parts.push(`Ay2: ${prof.ayudante2}`);
  if(prof.arsenalera) parts.push(`Ars: ${prof.arsenalera}`);

  // Normalizado sin strings vacíos:
  const n = {
    fecha: fecha || null,
    hora: hora || null,
    clinica: clinica || null,
    cirugia: cirugia || null,
    tipoPaciente: tipoPaciente || null,

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

  // Limpieza final: removemos keys con null si quieres “ultra compacto”
  // (Firestore sí acepta null, así que lo dejamos; sirve para consistencia)
  return n;
}

function validateMinimum(headerIdx){
  // No forzamos “todas”, pero sí que existan las claves base
  const needed = ['Fecha','Clínica','Cirugía','Tipo de Paciente','Valor'];
  const missing = needed.filter(k => headerIdx.get(k) === undefined);
  return missing;
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

  // 1) guarda cabecera
  await setDoc(refImport, meta, { merge: true });

  // 2) guarda items en subcolección (batch en chunks)
  const itemsCol = collection(db, 'produccion_imports', importId, 'items');
  const chunkSize = 400;
  let idx = 0;

  while(idx < state.stagedItems.length){
    const batch = writeBatch(db);
    const slice = state.stagedItems.slice(idx, idx + chunkSize);

    slice.forEach((it, k)=>{
      const n = it.normalizado;
      const raw = it.raw;

      // docId determinístico
      const itemId = `ITEM_${pad(idx + k + 1, 4)}`;
      const refItem = doc(itemsCol, itemId);

      // IMPORTANTE: raw NO tiene campos vacíos.
      batch.set(refItem, {
        importId,
        itemId,
        idx: idx + k + 1,
        estado: 'staged',          // por si después quieres anular a nivel item
        raw,
        normalizado: n,
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

  const importId = state.importId;
  const refImport = doc(db, 'produccion_imports', importId);

  // Leer items staging
  const itemsCol = collection(db, 'produccion_imports', importId, 'items');
  const itemsSnap = await getDocs(itemsCol);

  if(itemsSnap.empty){
    toast('No hay items en la importación.');
    return;
  }

  // Crear registros en "produccion" (fuente única para liquidaciones)
  // prodId: PROD_{YYYY}_{MM}_{importId}_{ITEM_0001}
  const y = state.year;
  const m = pad(state.monthNum, 2);

  const batchSize = 400;
  const docs = [];
  itemsSnap.forEach(d => docs.push({ id: d.id, data: d.data() || {} }));

  let i = 0;
  while(i < docs.length){
    const batch = writeBatch(db);
    const slice = docs.slice(i, i + batchSize);

    slice.forEach(({id, data})=>{
      const n = data.normalizado || {};
      const raw = data.raw || {};

      const prodId = `PROD_${y}_${m}_${importId}_${id}`;
      const refProd = doc(db, 'produccion', prodId);

      batch.set(refProd, {
        id: prodId,
        importId,
        importItemId: id,
        mes: state.monthName,
        mesNum: state.monthNum,
        ano: state.year,

        // claves para queries (liquidaciones)
        fecha: n.fecha ?? null,
        hora: n.hora ?? null,
        clinica: n.clinica ?? null,
        cirugia: n.cirugia ?? null,
        tipoPaciente: n.tipoPaciente ?? null,

        // valores numéricos (CLP)
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

        // RAW completo (pero compacto, sin vacíos)
        raw,

        estado: 'activa', // para anulación futura
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge: true });
    });

    await batch.commit();
    i += batchSize;
  }

  // Marcar import confirmada
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

  // Marca cabecera anulada
  await setDoc(refImport, {
    estado: 'anulada',
    anuladaEl: serverTimestamp(),
    anuladaPor: state.user?.email || '',
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge: true });

  // Además, desactiva filas en "produccion" asociadas a importId (sin borrar)
  // (en produccion guardamos importId)
  // Firestore no permite update masivo sin leer, así que lo hacemos por query + batches.
  const qy = query(colProduccion, where('importId','==', importId), limit(2000));
  const snap = await getDocs(qy);

  if(!snap.empty){
    const all = [];
    snap.forEach(d=> all.push(d));
    let i = 0;
    while(i < all.length){
      const batch = writeBatch(db);
      all.slice(i, i+400).forEach(d=>{
        batch.set(d.ref, {
          estado: 'anulada',
          anuladaEl: serverTimestamp(),
          anuladaPor: state.user?.email || ''
        }, { merge: true });
      });
      await batch.commit();
      i += 400;
    }
  }

  state.status = 'anulada';
  setStatus(`⛔ Importación anulada: ${importId}`);
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

  // construir staging items
  const staged = [];
  for(let i=1;i<rows.length;i++){
    const row = rows[i];

    // raw compacto SIN campos vacíos
    const raw = compactRaw(row, headerIdx);

    // si una fila viene totalmente vacía (salvo #), la ignoramos
    const rawKeys = Object.keys(raw);
    if(rawKeys.length === 0) continue;

    // normalizado (con números)
    const normalizado = buildNormalizado(raw);

    staged.push({
      idx: i,
      raw,
      normalizado
    });
  }

  if(!staged.length){
    toast('No se encontraron filas válidas.');
    return;
  }

  // generar importId
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
  setButtons();
  paintPreview();

  // guardar staging en Firestore
  await saveStagingToFirestore();
  toast('Staging guardado en Firestore');
}

/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    // Sidebar común
    await loadSidebar({ active: 'produccion' });
    setActiveNav('produccion');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    // default mes/año (si quieres)
    $('mes').value = 'Octubre';

    setStatus('—');
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
  }
});
