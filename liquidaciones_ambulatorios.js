// liquidaciones_ambulatorios.js — COMPLETO

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe } from './utils.js';
import { loadSidebar } from './layout.js';
await loadSidebar({ active: 'liquidaciones_ambulatorios' });

import {
  collection,
  collectionGroup,
  getDocs,
  query,
  where,
  doc,
  getDoc,
  setDoc,
  serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

import {
  PDFDocument,
  StandardFonts,
  rgb
} from 'https://cdn.skypack.dev/pdf-lib@1.17.1';

/* =========================
   AJUSTE ÚNICO
========================= */
const PROD_ITEMS_GROUP = 'items';

/* =========================
   Helpers
========================= */
const $ = (id)=> document.getElementById(id);

function normalize(s=''){
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .trim();
}

function canonRutAny(v=''){
  const s = (v ?? '').toString().toUpperCase().trim();
  if(!s) return '';
  return s.replace(/[^0-9K]/g,'');
}

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

function hasRealValue(v){
  return !(v === undefined || v === null || String(v).trim() === '');
}

function asNumberLoose(v){
  const s = (v ?? '').toString().replace(/[^\d]/g,'');
  return Number(s || 0) || 0;
}

function parseDecimalFlexible(v){
  let s = (v ?? '').toString().trim();
  if(!s) return 0;

  s = s.replace(/\s+/g, '');

  const lastComma = s.lastIndexOf(',');
  const lastDot = s.lastIndexOf('.');

  if(lastComma >= 0 && lastDot >= 0){
    if(lastComma > lastDot){
      s = s.replace(/\./g, '').replace(',', '.');
    }else{
      s = s.replace(/,/g, '');
    }
  }else if(lastComma >= 0){
    s = s.replace(',', '.');
  }

  s = s.replace(/[^0-9.-]/g, '');

  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function clp(n){
  const x = Number(n || 0) || 0;
  const s = Math.round(x).toString();
  const withDots = s.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return `$${withDots}`;
}

function fmtDateISOorDMY(v){
  const s = cleanReminder(v);
  if(!s) return '';
  if(/^\d{2}\/\d{2}\/\d{4}$/.test(s)) return s;
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.split('-').reverse().join('/');
  return s;
}

function pillHtml(kind, text){
  const cls = kind === 'ok' ? 'ok' : (kind === 'warn' ? 'warn' : (kind === 'bad' ? 'bad' : ''));
  return `<span class="pill ${cls}">${escapeHtml(text)}</span>`;
}

function download(filename, text, mime='text/plain'){
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(()=> URL.revokeObjectURL(url), 1500);
}

function downloadBytes(filename, bytes, mime='application/pdf'){
  const blob = new Blob([bytes], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(()=> URL.revokeObjectURL(url), 1500);
}

function ensureXLSX(){
  if(!window.XLSX){
    throw new Error('XLSX no está cargado en la página');
  }
}

function autoWidthFromRows(rows = [], headers = []){
  const widths = headers.map(h => ({ wch: String(h || '').length + 2 }));

  for(const row of rows){
    headers.forEach((h, i) => {
      const val = row?.[h];
      const len = String(val ?? '').length + 2;
      if(!widths[i] || len > widths[i].wch){
        widths[i] = { wch: Math.min(len, 60) };
      }
    });
  }

  return widths;
}

function exportRowsToXLSX(filename, sheetName, headers, rows){
  ensureXLSX();

  const data = rows.map(r => {
    const out = {};
    headers.forEach(h => { out[h] = r?.[h] ?? ''; });
    return out;
  });

  const ws = XLSX.utils.json_to_sheet(data, { header: headers });
  ws['!cols'] = autoWidthFromRows(data, headers);

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, String(sheetName || 'Datos').slice(0, 31));

  XLSX.writeFile(wb, filename);
}

function safeFileName(s){
  return normalize(s || 'profesional')
    .replace(/[^a-z0-9]+/g,'-')
    .replace(/^-+|-+$/g,'')
    .slice(0,60) || 'profesional';
}

function normKeyLoose(s=''){
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g,'');
}

function pickRaw(raw, key){
  if(!raw || typeof raw !== 'object') return '';
  if(raw[key] !== undefined) return raw[key];

  const nk = normKeyLoose(key);
  for(const k of Object.keys(raw)){
    if(normKeyLoose(k) === nk) return raw[k];
  }
  return '';
}

function extractPA(v=''){
  const s = (v ?? '').toString().toUpperCase();
  const m = s.match(/PA\d{3,6}/);
  return m ? m[0] : '';
}

function monthNameEs(m){
  return ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'][m-1] || '';
}

function yyyymm(ano, mesNum){
  return `${String(ano)}${String(mesNum).padStart(2,'0')}`;
}

function pickDisplayEstadoLinea(l){
  if(l.isAlerta) return 'ALERTA';
  if(l.isPendiente) return 'PENDIENTE';
  return 'OK';
}

function roleLabel(roleId=''){
  const r = normalize(roleId);
  if(r === 'r_nutricionista') return 'NUTRICIONISTA';
  if(r === 'r_psicologo') return 'PSICÓLOGO';
  if(r === 'r_nutrilogo') return 'NUTRIÓLOGO';
  if(r === 'r_cirujano') return 'CIRUJANO';
  return String(roleId || '').toUpperCase();
}

function professionFromRole(roleId=''){
  const r = normalize(roleId);
  if(r === 'r_nutricionista') return 'NUTRICIONISTA';
  if(r === 'r_psicologo') return 'PSICÓLOGO';
  if(r === 'r_nutrilogo') return 'NUTRIÓLOGO';
  if(r === 'r_cirujano') return 'CIRUJANO';
  return 'PROFESIONAL';
}

function normalizeOrigin(v=''){
  const x = normalize(v);
  if(x.includes('mk')) return 'MK';
  if(x.includes('reservo')) return 'RESERVO';
  return (v || '').toString().toUpperCase().trim();
}

function containsAny(text='', arr=[]){
  const t = normalize(text);
  return arr.some(x => t.includes(normalize(x)));
}

/* =========================
   State
========================= */
const state = {
  user: null,

  mesNum: null,
  ano: null,
  q: '',

  profesionalesByName: new Map(),
  profesionalesById: new Map(),

  procedimientosByName: new Map(),
  procedimientosById: new Map(),
  procedimientosByCodigo: new Map(),

  prodRows: [],
  liquidResumen: [],
  lastDetailExportLines: [],

  ajustesCache: new Map(),
  ajustesActualKey: '',
  ajustesActualAgg: null,
  
  fechaPagoManual: ''
};

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colProcedimientos = collection(db, 'procedimientos');

function fechaPagoDocRef(){
  return doc(db, 'liquidaciones_ambulatorias_config', ajustesMonthId(), 'config', 'fechaPago');
}

async function loadFechaPagoMes(){
  try{
    const snap = await getDoc(fechaPagoDocRef());
    const x = snap.exists() ? (snap.data() || {}) : {};

    state.fechaPagoManual = cleanReminder(x.fechaPago || '');
  }catch(e){
    console.warn('No se pudo leer fecha de pago del mes', e);
    state.fechaPagoManual = '';
  }
}

async function saveFechaPagoMes(fechaPago){
  await setDoc(fechaPagoDocRef(), {
    monthId: ajustesMonthId(),
    ano: Number(state.ano),
    mesNum: Number(state.mesNum),
    fechaPago: cleanReminder(fechaPago || ''),
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  state.fechaPagoManual = cleanReminder(fechaPago || '');
}

/* =========================
   Catálogos
========================= */
async function loadProfesionales(){
  const snap = await getDocs(colProfesionales);
  const byName = new Map();
  const byId = new Map();

  snap.forEach(d=>{
    const x = d.data() || {};

    const rutId = cleanReminder(x.rutId) || d.id;
    const nombreProfesional = cleanReminder(x.nombreProfesional) || cleanReminder(x.nombre) || '';
    const razonSocial = cleanReminder(x.razonSocial) || '';
    const rutEmpresa = cleanReminder(x.rutEmpresa) || '';
    const estado = (cleanReminder(x.estado) || 'activo').toLowerCase();

    const rutPersonal = cleanReminder(x.rut) || String(rutId || '');
    const tipoPersona = (cleanReminder(x.tipoPersona) || (razonSocial ? 'juridica' : 'natural')).toLowerCase();

    const rolePrincipalRaw = cleanReminder(x.rolPrincipalId || x.rolPrincipal || '');
    const rolePrincipal = rolePrincipalRaw || '';

    const docProf = {
      id: String(rutId || d.id),
      rutId: String(rutId || d.id),

      nombreProfesional: toUpperSafe(nombreProfesional || ''),
      rut: rutPersonal,

      razonSocial: toUpperSafe(razonSocial || ''),
      rutEmpresa,

      tipoPersona,
      estado,
      rolePrincipal,
      profesionDisplay: cleanReminder(x.profesionDisplay || professionFromRole(rolePrincipal))
    };

    const keys = new Set();
    keys.add(String(docProf.id || '').trim());
    keys.add(String(docProf.rutId || '').trim());
    keys.add(String(docProf.rut || '').trim());
    keys.add(canonRutAny(docProf.id));
    keys.add(canonRutAny(docProf.rutId));
    keys.add(canonRutAny(docProf.rut));

    for(const k of keys){
      const kk = (k ?? '').toString().trim();
      if(kk) byId.set(kk, docProf);
    }

    if(nombreProfesional) byName.set(normalize(nombreProfesional), docProf);
  });

  state.profesionalesByName = byName;
  state.profesionalesById = byId;
}

function normalizeModoValor(v=''){
  return normalize(v) === 'archivo' ? 'archivo' : 'fijo';
}

function normalizeProcDocAmb(id, x){
  const tarifaRaw = (x?.tarifa && typeof x.tarifa === 'object') ? x.tarifa : {};

  return {
    id,
    codigo: cleanReminder(x.codigo) || id,
    tipo: cleanReminder(x.tipo) || 'ambulatorio',
    archivo: cleanReminder(x.archivo) || '',
    categoria: cleanReminder(x.categoria) || '',
    tratamiento: cleanReminder(x.tratamiento || x.nombre) || '',
    nombre: cleanReminder(x.tratamiento || x.nombre) || cleanReminder(x.nombre) || id,
    estado: (cleanReminder(x.estado) || 'activa').toLowerCase(),

    tarifa: {
      modoValor: normalizeModoValor(tarifaRaw.modoValor),
      valor: hasRealValue(tarifaRaw.valor) ? Number(tarifaRaw.valor) : null,
      columnaOrigen: cleanReminder(tarifaRaw.columnaOrigen || ''),
      comisionPct: hasRealValue(tarifaRaw.comisionPct) ? Number(tarifaRaw.comisionPct) : null,
      valorProfesional: hasRealValue(tarifaRaw.valorProfesional) ? Number(tarifaRaw.valorProfesional) : null
    }
  };
}

async function loadProcedimientosAmbulatorios(){
  const snap = await getDocs(colProcedimientos);
  const byName = new Map();
  const byId = new Map();
  const byCodigo = new Map();

  snap.forEach(d=>{
    const x = d.data() || {};
    const docProc = normalizeProcDocAmb(d.id, x);

    const tipoN = normalize(docProc.tipo);
    const codigoN = String(docProc.codigo || '').toUpperCase().trim();

    const esAmb =
      tipoN === 'ambulatorio' ||
      /^PA\d+$/.test(codigoN);

    if(!esAmb) return;

    byId.set(String(docProc.id), docProc);

    if(docProc.codigo){
      byCodigo.set(String(docProc.codigo).trim().toUpperCase(), docProc);
    }

    if(docProc.nombre) byName.set(normalize(docProc.nombre), docProc);
    if(docProc.tratamiento) byName.set(normalize(docProc.tratamiento), docProc);
  });

  state.procedimientosByName = byName;
  state.procedimientosById = byId;
  state.procedimientosByCodigo = byCodigo;
}

/* =========================
   Producción ambulatoria
========================= */
function getRawContainer(x){
  if(x?.raw && typeof x.raw === 'object') return x.raw;
  if(x?.dataReservo && typeof x.dataReservo === 'object') return x.dataReservo;
  if(x?.dataMK && typeof x.dataMK === 'object') return x.dataMK;
  return {};
}

function resolveProcedimientoCandidate(x, raw){
  const resolved = (x?.resolved && typeof x.resolved === 'object') ? x.resolved : {};
  const sel = (x?._selectedIds && typeof x._selectedIds === 'object') ? x._selectedIds : {};
  const norm = (x?.normalizado && typeof x.normalizado === 'object') ? x.normalizado : {};

  const fromIds = [
    resolved.procedimientoId,
    resolved.ambulatorioId,
    sel.procedimientoId,
    sel.ambulatorioId,
    x.procedimientoId,
    x.ambulatorioId,
    x.procedimientoCodigo,
    x.codigoProcedimiento,
    norm.procedimientoId,
    norm.ambulatorioId,
    norm.codigoProcedimiento
  ].map(v=> cleanReminder(v)).filter(Boolean);

  const fromNames = [
    resolved.procedimientoNombre,
    x.procedimientoNombre,
    x.prestacion,
    x.tratamiento,
    x.procedimiento,
    x.nombreProcedimiento,
    norm.procedimientoNombre,
    norm.prestacion,
    pickRaw(raw, 'Tratamiento'),
    pickRaw(raw, 'Prestación'),
    pickRaw(raw, 'Prestacion'),
    pickRaw(raw, 'D Artículo'),
    pickRaw(raw, 'D Articulo'),
    pickRaw(raw, 'Procedimiento')
  ].map(v=> cleanReminder(v)).filter(Boolean);

  const paCode =
    fromIds.map(extractPA).find(Boolean) ||
    fromNames.map(extractPA).find(Boolean) ||
    '';

  const rawId = fromIds.find(Boolean) || '';
  const rawName = fromNames.find(Boolean) || '';

  return { paCode, rawId, rawName };
}

function resolveProfesionalCandidate(x, raw){
  const resolved = (x?.resolved && typeof x.resolved === 'object') ? x.resolved : {};
  const sel = (x?._selectedIds && typeof x._selectedIds === 'object') ? x._selectedIds : {};
  const profIds = (resolved.profIds && typeof resolved.profIds === 'object') ? resolved.profIds : {};

  const idAny = cleanReminder(
    resolved.profesionalId ||
    profIds.profesionalId ||
    sel.profesionalId ||
    x.profesionalId ||
    x.rutProfesional ||
    x.rutDoctor ||
    ''
  );

  const nameAny = cleanReminder(
    resolved.profesionalNombre ||
    x.profesionalNombre ||
    x.profesional ||
    x.medico ||
    pickRaw(raw, 'Profesional') ||
    pickRaw(raw, 'D Médico') ||
    pickRaw(raw, 'D Medico')
  );

  return {
    idAny,
    idCanon: canonRutAny(idAny),
    nameAny
  };
}

function resolveFechaHoraPaciente(x, raw){
  const fecha = fmtDateISOorDMY(
    x.fechaISO ||
    x.fechaNorm ||
    x.fecha ||
    pickRaw(raw, 'Fecha')
  );

  const hora = cleanReminder(
    x.horaHM ||
    x.hora ||
    pickRaw(raw, 'Hora')
  );

  const pacienteNombre = toUpperSafe(cleanReminder(
    x.nombrePaciente ||
    x.paciente ||
    pickRaw(raw, 'Paciente')
  ));

  const pacienteRut = cleanReminder(
    x.rutPaciente ||
    x.rut ||
    pickRaw(raw, 'Rut')
  );

  return { fecha, hora, pacienteNombre, pacienteRut };
}

function getTarifaAmbulatoria(procDoc){
  const t = (procDoc?.tarifa && typeof procDoc.tarifa === 'object') ? procDoc.tarifa : {};

  return {
    modoValor: normalizeModoValor(t.modoValor),
    valor: hasRealValue(t.valor) ? Number(t.valor) : null,
    columnaOrigen: cleanReminder(t.columnaOrigen || ''),
    comisionPct: hasRealValue(t.comisionPct) ? Number(t.comisionPct) : null,
    valorProfesional: hasRealValue(t.valorProfesional) ? Number(t.valorProfesional) : null
  };
}

function getValorDesdeRaw(raw, columna){
  if(!raw || !columna) return 0;

  const buscada = normalize(columna);
  for(const k of Object.keys(raw || {})){
    if(normalize(k) === buscada){
      return parseDecimalFlexible(raw[k]);
    }
  }
  return 0;
}

function getValorMKFallback(raw){
  const posibles = [
    'Total',
    'Valor',
    'Monto',
    'Subtotal',
    'Total Neto',
    'Total Pago'
  ];

  for(const p of posibles){
    const n = parseDecimalFlexible(pickRaw(raw, p));
    if(n > 0) return n;
  }

  return 0;
}

function calcularPagoSegunOrigen(x, procDoc){
  const raw = getRawContainer(x);
  const tarifa = getTarifaAmbulatoria(procDoc);

  let valorBase = 0;
  let pagoProfesional = 0;
  let utilidad = 0;
  const origen = normalizeOrigin(x.origen || x.archivo || pickRaw(raw,'Origen') || pickRaw(raw,'Archivo') || '');

  if(origen === 'MK'){
    // ✅ MK: paga porcentaje del valor indicado por el archivo
    valorBase =
      tarifa.modoValor === 'archivo'
        ? getValorDesdeRaw(raw, tarifa.columnaOrigen) || getValorMKFallback(raw)
        : getValorMKFallback(raw) || Number(tarifa.valor ?? 0) || 0;

    if(hasRealValue(tarifa.comisionPct) && Number(tarifa.comisionPct) > 0 && valorBase > 0){
      pagoProfesional = Math.round(valorBase * (Number(tarifa.comisionPct) / 100));
    }else if(hasRealValue(tarifa.valorProfesional)){
      // fallback de seguridad
      pagoProfesional = Number(tarifa.valorProfesional) || 0;
    }

    utilidad = valorBase - pagoProfesional;
  } else {
    // ✅ Reservo: paga valor a pagar al profesional
    valorBase =
      tarifa.modoValor === 'archivo'
        ? getValorDesdeRaw(raw, tarifa.columnaOrigen)
        : Number(tarifa.valor ?? 0) || 0;

    if(hasRealValue(tarifa.valorProfesional)){
      pagoProfesional = Number(tarifa.valorProfesional) || 0;
    }else if(hasRealValue(tarifa.comisionPct) && Number(tarifa.comisionPct) > 0 && valorBase > 0){
      // fallback por seguridad
      pagoProfesional = Math.round(valorBase * (Number(tarifa.comisionPct) / 100));
    }

    utilidad = valorBase - pagoProfesional;
  }

  return {
    origen,
    tarifa,
    valorBase,
    pagoProfesional,
    utilidad
  };
}

function isLikelyAmbulatorioItem(x, procDoc){
  const resolved = (x?.resolved && typeof x.resolved === 'object') ? x.resolved : {};
  const procId = cleanReminder(
    resolved.procedimientoId ||
    x.procedimientoId ||
    x.ambulatorioId ||
    x.procedimientoCodigo ||
    ''
  );

  const procNombre = cleanReminder(
    resolved.procedimientoNombre ||
    x.procedimientoNombre ||
    x.prestacion ||
    x.tratamiento ||
    x.procedimiento ||
    ''
  );

  const paCode = extractPA(procId) || extractPA(procNombre);

  if(paCode) return true;
  if(procDoc) return true;

  return false;
}

async function loadProduccionMes(){
  if(!state.mesNum || !state.ano) return;

  const colItemsGroup = collectionGroup(db, PROD_ITEMS_GROUP);

  const qy = query(
    colItemsGroup,
    where('ano','==', Number(state.ano)),
    where('mesNum','==', Number(state.mesNum)),
    where('confirmadoEnProduccion','==', true),
    where('estadoRegistro','==', 'activo')
  );

  const snap = await getDocs(qy);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};

    if(normalize(x.estadoRegistro) !== 'activo') return;
    if(x.confirmadoEnProduccion !== true) return;

    const raw = getRawContainer(x);
    const procCandidate = resolveProcedimientoCandidate(x, raw);

    const procDoc =
      (procCandidate.paCode && state.procedimientosByCodigo.get(procCandidate.paCode)) ||
      (procCandidate.rawId && state.procedimientosById.get(String(procCandidate.rawId).trim())) ||
      (procCandidate.rawName && state.procedimientosByName.get(normalize(procCandidate.rawName))) ||
      null;

    if(!isLikelyAmbulatorioItem(x, procDoc)) return;

    const appEstado = normalize(
      x?.aplicacion?.estado ||
      x?.estadoAplicacion ||
      ''
    );

    if(appEstado === 'no_aplica') return;

    out.push({
      id: d.id,
      data: x,
      procDoc
    });
  });

  state.prodRows = out;
}

/* =========================
   Ajustes mensuales
========================= */
function ajustesMonthId(){
  return yyyymm(state.ano, state.mesNum);
}

function ajustesDocRef(profKey){
  return doc(db, 'liquidaciones_ambulatorias_config', ajustesMonthId(), 'profesionales', profKey);
}

async function loadAjustesProfesional(profKey){
  const cacheKey = `${ajustesMonthId()}__${profKey}`;
  if(state.ajustesCache.has(cacheKey)) return state.ajustesCache.get(cacheKey);

  try{
    const snap = await getDoc(ajustesDocRef(profKey));
    const x = snap.exists() ? (snap.data() || {}) : {};

    const data = {
      descuentoAplica: x.descuentoAplica === true,
      descuentoCLP: Number(x.descuentoCLP || 0) || 0,
      descuentoAsunto: cleanReminder(x.descuentoAsunto || ''),
      balonAplica: x.balonAplica === true,
      balonCantidad: Number(x.balonCantidad || 0) || 0,
      balonValorUnitario: Number(x.balonValorUnitario || 0) || 0,
      balonAsunto: cleanReminder(x.balonAsunto || 'Instalación de balón')
    };

    state.ajustesCache.set(cacheKey, data);
    return data;
  }catch(e){
    console.warn('No se pudo leer ajustes ambulatorios', profKey, e);
    const data = {
      descuentoAplica: false,
      descuentoCLP: 0,
      descuentoAsunto: '',
      balonAplica: false,
      balonCantidad: 0,
      balonValorUnitario: 0,
      balonAsunto: 'Instalación de balón'
    };
    state.ajustesCache.set(cacheKey, data);
    return data;
  }
}

async function saveAjustesProfesional(profKey, payload){
  await setDoc(ajustesDocRef(profKey), {
    ...payload,
    monthId: ajustesMonthId(),
    ano: Number(state.ano),
    mesNum: Number(state.mesNum),
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  const cacheKey = `${ajustesMonthId()}__${profKey}`;
  state.ajustesCache.set(cacheKey, payload);
}

/* =========================
   Reglas de modalidad por rol
========================= */
function procedimientoTextoUnificado(line){
  return [
    line.procedimientoNombre || '',
    line.procedimientoId || '',
    line.categoria || '',
    line.origen || ''
  ].join(' | ');
}

function classifyModalidadByRole(roleId, line){
  const role = normalize(roleId);
  const texto = normalize(procedimientoTextoUnificado(line));

  if(role === 'r_nutricionista'){
    if(containsAny(texto, ['pad'])) return 'BONO PAD';
    if(containsAny(texto, ['balon'])) return 'PROMO BALÓN GÁSTRICO';
    return 'CONSULTA';
  }

  if(role === 'r_psicologo'){
    if(containsAny(texto, ['balon'])) return 'PROMO BALÓN GÁSTRICO';
    return 'CONSULTA';
  }

  if(role === 'r_nutrilogo'){
    if(containsAny(texto, ['online', 'telemedicina', 'telemed', 'telemedico', 'remoto'])) return 'ONLINE';
    if(containsAny(texto, ['presensial', 'presencial'])) return 'PRESENCIAL';
    return 'PRESENCIAL';
  }

  if(role === 'r_cirujano'){
    return 'CONSULTA';
  }

  return 'CONSULTA';
}

/* =========================
   Build liquidaciones
========================= */
async function buildLiquidaciones(){
  const lines = [];

  for(const row of state.prodRows){
    const x = row.data || {};
    const raw = getRawContainer(x);

    const { fecha, hora, pacienteNombre, pacienteRut } = resolveFechaHoraPaciente(x, raw);

    const profCandidate = resolveProfesionalCandidate(x, raw);
    const procCandidate = resolveProcedimientoCandidate(x, raw);

    const profDoc =
      (profCandidate.idAny && state.profesionalesById.get(String(profCandidate.idAny).trim())) ||
      (profCandidate.idCanon && state.profesionalesById.get(profCandidate.idCanon)) ||
      (profCandidate.idCanon && state.profesionalesById.get(profCandidate.idCanon.replace(/K$/,''))) ||
      (profCandidate.nameAny && state.profesionalesByName.get(normalize(profCandidate.nameAny))) ||
      null;

    const procDoc =
      row.procDoc ||
      (procCandidate.paCode && state.procedimientosByCodigo.get(procCandidate.paCode)) ||
      (procCandidate.rawId && state.procedimientosById.get(String(procCandidate.rawId).trim())) ||
      (procCandidate.rawName && state.procedimientosByName.get(normalize(procCandidate.rawName))) ||
      null;

    const titularNombre = profDoc?.nombreProfesional || toUpperSafe(profCandidate.nameAny || '') || '—';
    const titularRut = profDoc?.rut || '';
    const tipoPersona = (profDoc?.tipoPersona || '').toLowerCase();
    const empresaNombre = (tipoPersona === 'juridica') ? (profDoc?.razonSocial || '') : '';
    const empresaRut = (tipoPersona === 'juridica') ? (profDoc?.rutEmpresa || '') : '';
    const roleId = cleanReminder(profDoc?.rolePrincipal || x.roleId || x.rolPrincipal || '');

    const procedimientoLabel =
      toUpperSafe(procDoc?.nombre || procCandidate.rawName || '') || '(Sin procedimiento)';
    const procedimientoId =
      cleanReminder(procDoc?.codigo || procDoc?.id || procCandidate.paCode || procCandidate.rawId || '');

    const origen = normalizeOrigin(x.origen || x.archivo || pickRaw(raw,'Origen') || pickRaw(raw,'Archivo') || '');
    const categoria = cleanReminder(procDoc?.categoria || x.categoria || '');

    const alertas = [];
    const pendientes = [];

    if(!profDoc) alertas.push('Profesional no existe en nómina (catálogo)');
    if(!procDoc) alertas.push('Procedimiento ambulatorio no existe / no mapeado');
    if(!fecha) pendientes.push('Fecha vacía');
    if(!pacienteNombre) pendientes.push('Paciente vacío');
    if(!roleId) pendientes.push('Profesional sin rol principal');

    let valorBase = 0;
    let pagoProfesional = 0;
    let utilidad = 0;
    let comisionPct = 0;
    let tarifaModoValor = 'fijo';
    let tarifaColumnaOrigen = '';

    if(procDoc){
      const calc = calcularPagoSegunOrigen(x, procDoc);

      valorBase = Number(calc.valorBase || 0) || 0;
      pagoProfesional = Number(calc.pagoProfesional || 0) || 0;
      utilidad = Number(calc.utilidad || 0) || 0;
      comisionPct = Number(calc.tarifa?.comisionPct || 0) || 0;
      tarifaModoValor = calc.tarifa?.modoValor || 'fijo';
      tarifaColumnaOrigen = calc.tarifa?.columnaOrigen || '';

      if(origen === 'MK'){
        if(tarifaModoValor === 'archivo' && !tarifaColumnaOrigen){
          pendientes.push('MK sin columna origen para porcentaje');
        }
        if(valorBase <= 0){
          pendientes.push('MK sin valor base desde archivo');
        }
        if(!hasRealValue(calc.tarifa?.comisionPct) && !hasRealValue(calc.tarifa?.valorProfesional)){
          pendientes.push('MK sin porcentaje / pago configurado');
        }
      } else {
        if(!hasRealValue(calc.tarifa?.valorProfesional) && !hasRealValue(calc.tarifa?.comisionPct)){
          pendientes.push('Reservo sin valor a pagar al profesional');
        }
      }
    }

    const baseLine = {
      prodId: row.id,

      fecha,
      hora,
      origen,
      categoria,

      pacienteNombre,
      pacienteRut,

      profesionalNombre: titularNombre,
      profesionalId: (profDoc?.id || profCandidate.idCanon || profCandidate.idAny || '').toString(),
      profesionalRut: titularRut,

      tipoPersona,
      empresaNombre,
      empresaRut,
      roleId,
      roleNombre: roleLabel(roleId),
      profesionDisplay: cleanReminder(profDoc?.profesionDisplay || professionFromRole(roleId)),

      procedimientoId,
      procedimientoNombre: procedimientoLabel,
      procedimientoExists: !!procDoc,

      tarifaModoValor,
      tarifaColumnaOrigen,
      comisionPct,

      valorBase,
      pagoProfesional,
      utilidad,

      modalidad: '',

      isAlerta: alertas.length > 0,
      isPendiente: pendientes.length > 0,
      alerts: alertas,
      pendings: pendientes,
      observacion: ''
    };

    baseLine.modalidad = classifyModalidadByRole(roleId, baseLine);

    baseLine.observacion = [
      ...(alertas.length ? [`ALERTA: ${alertas.join(' · ')}`] : []),
      ...(pendientes.length ? [`PENDIENTE: ${pendientes.join(' · ')}`] : [])
    ].join(' | ');

    lines.push(baseLine);
  }

  const map = new Map();

  for(const ln of lines){
    const key = ln.profesionalId
      ? `ID:${String(ln.profesionalId)}`
      : `DESCONOCIDO:${normalize(ln.profesionalNombre)}`;

    if(!map.has(key)){
      map.set(key, {
        key,

        nombre: ln.profesionalNombre,
        rut: ln.profesionalRut || '',
        tipoPersona: ln.tipoPersona || '',

        empresaNombre: ln.empresaNombre || '',
        empresaRut: ln.empresaRut || '',

        roleId: ln.roleId || '',
        roleNombre: ln.roleNombre || '',
        profesionDisplay: ln.profesionDisplay || '',

        casos: 0,
        totalBruto: 0,
        totalPagoProfesional: 0,
        totalUtilidad: 0,

        alertasCount: 0,
        pendientesCount: 0,

        ajustes: {
          descuentoAplica: false,
          descuentoCLP: 0,
          descuentoAsunto: '',
          balonAplica: false,
          balonCantidad: 0,
          balonValorUnitario: 0,
          balonSubtotal: 0,
          balonAsunto: 'Instalación de balón',
          totalValorizado: 0,
          totalBoleta: 0,
          retencionCLP: 0,
          liquido: 0
        },

        resumenModalidades: [],
        lines: []
      });
    }

    const agg = map.get(key);
    agg.casos += 1;
    agg.totalBruto += Number(ln.valorBase || 0) || 0;
    agg.totalPagoProfesional += Number(ln.pagoProfesional || 0) || 0;
    agg.totalUtilidad += Number(ln.utilidad || 0) || 0;

    if(ln.isAlerta) agg.alertasCount += 1;
    if(!ln.isAlerta && ln.isPendiente) agg.pendientesCount += 1;

    if(!agg.tipoPersona && ln.tipoPersona) agg.tipoPersona = ln.tipoPersona;
    if(!agg.empresaNombre && ln.empresaNombre) agg.empresaNombre = ln.empresaNombre;
    if(!agg.empresaRut && ln.empresaRut) agg.empresaRut = ln.empresaRut;
    if(!agg.roleId && ln.roleId) agg.roleId = ln.roleId;
    if(!agg.roleNombre && ln.roleNombre) agg.roleNombre = ln.roleNombre;
    if(!agg.profesionDisplay && ln.profesionDisplay) agg.profesionDisplay = ln.profesionDisplay;

    agg.lines.push(ln);
  }

  const resumen = [];
  for(const x of map.values()){
    const ajustesCfg = await loadAjustesProfesional(x.key);

    // agrupación por modalidad
    const modalidadesMap = new Map();
    for(const l of (x.lines || [])){
      const mod = l.modalidad || 'CONSULTA';
      if(!modalidadesMap.has(mod)){
        modalidadesMap.set(mod, {
          modalidad: mod,
          cantidad: 0,
          subtotal: 0
        });
      }
      const o = modalidadesMap.get(mod);
      o.cantidad += 1;
      o.subtotal += Number(l.pagoProfesional || 0) || 0;
    }

    const resumenModalidades = [...modalidadesMap.values()]
      .sort((a,b)=> (b.subtotal||0) - (a.subtotal||0));

    const totalValorizado = Number(x.totalPagoProfesional || 0) || 0;
    const totalBoleta = totalValorizado;
    const retencionCLP = 0;

    const descuentoCLP = ajustesCfg.descuentoAplica ? (Number(ajustesCfg.descuentoCLP || 0) || 0) : 0;

    const esCirujano = normalize(x.roleId) === 'r_cirujano';
    const balonCantidad = esCirujano && ajustesCfg.balonAplica ? (Number(ajustesCfg.balonCantidad || 0) || 0) : 0;
    const balonValorUnitario = esCirujano && ajustesCfg.balonAplica ? (Number(ajustesCfg.balonValorUnitario || 0) || 0) : 0;
    const balonSubtotal = esCirujano && ajustesCfg.balonAplica ? (balonCantidad * balonValorUnitario) : 0;

    const liquido = Math.max(0, totalBoleta - descuentoCLP - retencionCLP + balonSubtotal);

    let status = 'ok';
    if(x.alertasCount > 0) status = 'alerta';
    else if(x.pendientesCount > 0) status = 'pendiente';

    resumen.push({
      ...x,
      status,
      resumenModalidades,
      ajustes: {
        descuentoAplica: !!ajustesCfg.descuentoAplica,
        descuentoCLP,
        descuentoAsunto: ajustesCfg.descuentoAsunto || '',
        balonAplica: !!ajustesCfg.balonAplica && esCirujano,
        balonCantidad,
        balonValorUnitario,
        balonSubtotal,
        balonAsunto: ajustesCfg.balonAsunto || 'Instalación de balón',
        totalValorizado,
        totalBoleta,
        retencionCLP,
        liquido
      }
    });
  }

  const prio = (st)=> st === 'alerta' ? 0 : (st === 'pendiente' ? 1 : 2);
  resumen.sort((a,b)=>{
    const pa = prio(a.status);
    const pb = prio(b.status);
    if(pa !== pb) return pa - pb;
    return (b.ajustes?.liquido||0) - (a.ajustes?.liquido||0);
  });

  state.liquidResumen = resumen;
}

/* =========================
   Search
========================= */
function matchesSearch(agg, q){
  const s = normalize(q);
  if(!s) return true;

  const hay = normalize([
    agg.nombre,
    agg.rut,
    agg.tipoPersona,
    agg.empresaNombre,
    agg.empresaRut,
    agg.roleNombre,
    agg.profesionDisplay,
    ...(agg.resumenModalidades || []).map(r => [r.modalidad, r.cantidad, r.subtotal].join(' ')),
    ...agg.lines.map(l=> [
      l.origen,
      l.categoria,
      l.modalidad,
      l.procedimientoNombre,
      l.procedimientoId,
      l.pacienteNombre,
      ...(l.alerts || []),
      ...(l.pendings || [])
    ].join(' '))
  ].join(' '));

  return hay.includes(s);
}

/* =========================
   Paint
========================= */
function paint(){
  const rows = state.liquidResumen.filter(x=> matchesSearch(x, state.q));
  $('pillCount').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  const alertas = rows.reduce((a,b)=> a + (b.alertasCount||0), 0);
  const pendientes = rows.reduce((a,b)=> a + (b.pendientesCount||0), 0);
  const pill = $('pillEstado');

  if(!state.prodRows.length){
    pill.className = 'pill warn';
    pill.textContent = 'Sin producción ambulatoria confirmada';
  }else if(alertas > 0){
    pill.className = 'pill bad';
    pill.textContent = `Alertas: ${alertas}`;
  }else if(pendientes > 0){
    pill.className = 'pill warn';
    pill.textContent = `Pendientes: ${pendientes}`;
  }else{
    pill.className = 'pill ok';
    pill.textContent = 'OK (sin pendientes)';
  }

  const tb = $('tbody');
  tb.innerHTML = '';

  let i = 1;
  for(const agg of rows){
    const nombreTitular = agg.nombre || '—';
    const rutTitular = agg.rut || '—';

    const empresaSub = (agg.tipoPersona === 'juridica' && (agg.empresaNombre || agg.empresaRut))
      ? `<div class="mini muted">${escapeHtml(agg.empresaNombre || '')}</div>`
      : '';

    const rutEmpresaSub = (agg.tipoPersona === 'juridica' && agg.empresaRut)
      ? `<div class="mini muted mono">${escapeHtml(agg.empresaRut)}</div>`
      : '';

    const statusPill =
      agg.status === 'ok'
        ? pillHtml('ok','OK')
        : (agg.status === 'pendiente'
            ? pillHtml('warn',`PENDIENTE · ${agg.pendientesCount}`)
            : pillHtml('bad',`ALERTA · ${agg.alertasCount}`)
          );

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono">${i++}</td>
      <td>
        <div class="big">${escapeHtml(nombreTitular)}</div>
        ${empresaSub}
        <div class="mini muted">${escapeHtml(agg.key)}</div>
        <div class="mini muted">${escapeHtml(agg.profesionDisplay || agg.roleNombre || '')}</div>
      </td>
      <td class="mono">
        ${escapeHtml(rutTitular)}
        ${rutEmpresaSub}
      </td>
      <td>${escapeHtml((agg.tipoPersona || '—').toUpperCase())}</td>
      <td class="mono">${agg.casos}</td>
      <td><b>${clp(agg.ajustes?.liquido || 0)}</b></td>
      <td>${statusPill}</td>
      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Descargar PDF liquidación" aria-label="PDF">📄</button>
          <button class="iconBtn" type="button" title="Ver detalle" aria-label="Detalle">🔎</button>
          <button class="iconBtn" type="button" title="Exportar (profesional)" aria-label="ExportProf">⬇️</button>
          <button class="iconBtn" type="button" title="Ajustes del mes" aria-label="Ajustes">⚙️</button>
        </div>
      </td>
    `;

    tr.querySelector('[aria-label="PDF"]').addEventListener('click', async ()=>{
      try{
        const bytes = await generarPDFLiquidacionProfesional(agg);
        const fn = `LIQUIDACION_AMBULATORIA_${safeFileName(agg.nombre)}_${state.ano}_${String(state.mesNum).padStart(2,'0')}.pdf`;
        downloadBytes(fn, bytes, 'application/pdf');
        toast('PDF generado');
      }catch(err){
        console.error(err);
        toast('No se pudo generar el PDF (ver consola)');
      }
    });

    tr.querySelector('[aria-label="Detalle"]').addEventListener('click', ()=> openDetalle(agg));
    tr.querySelector('[aria-label="ExportProf"]').addEventListener('click', ()=> exportDetalleProfesional(agg));
    tr.querySelector('[aria-label="Ajustes"]').addEventListener('click', ()=> openAjustes(agg));

    tb.appendChild(tr);
  }

  $('lastLoad').textContent = `Items producción ambulatoria: ${state.prodRows.length} · Último cálculo: ${new Date().toLocaleString()}`;
}

/* =========================
   Modal detalle
========================= */
function openDetalle(agg){
  $('modalBackdrop').style.display = 'grid';

  $('modalTitle').textContent = agg.nombre || 'Detalle';

  const extraEmpresa = (agg.tipoPersona === 'juridica' && (agg.empresaNombre || agg.empresaRut))
    ? ` · Empresa: ${agg.empresaNombre || ''}${agg.empresaRut ? ' ('+agg.empresaRut+')' : ''}`
    : '';

  $('modalSub').textContent =
    `${monthNameEs(state.mesNum)} ${state.ano} · Casos: ${agg.casos}` +
    (agg.rut ? ` · RUT: ${agg.rut}` : '') +
    extraEmpresa;

  $('modalPillTotal').textContent = `LÍQUIDO: ${clp(agg.ajustes?.liquido || 0)}`;
  $('modalPillPendientes').textContent =
    agg.alertasCount > 0
      ? `Alertas: ${agg.alertasCount} · Pendientes: ${agg.pendientesCount}`
      : `Pendientes: ${agg.pendientesCount}`;

  const tb = $('modalTbody');
  tb.innerHTML = '';
  
  const resumenHost = $('modalResumen');
  if(resumenHost){
    resumenHost.innerHTML = buildModalResumenHtml(agg);
  }

  const lines = [...(agg.lines || [])].sort((a,b)=>{
    const fa = normalize(a.fecha);
    const fb = normalize(b.fecha);
    if(fa !== fb) return fa.localeCompare(fb);
    const pa = normalize(a.pacienteNombre);
    const pb = normalize(b.pacienteNombre);
    if(pa !== pb) return pa.localeCompare(pb);
    return normalize(a.procedimientoNombre).localeCompare(normalize(b.procedimientoNombre));
  });

  state.lastDetailExportLines = lines;

  for(const l of lines){
    const st = l.isAlerta ? pillHtml('bad','ALERTA') : (l.isPendiente ? pillHtml('warn','PENDIENTE') : pillHtml('ok','OK'));

    const obs = [
      ...(l.alerts?.length ? [`ALERTA: ${l.alerts.join(' · ')}`] : []),
      ...(l.pendings?.length ? [`PENDIENTE: ${l.pendings.join(' · ')}`] : [])
    ].join(' | ');

    const procWarn = (!l.procedimientoExists)
      ? `<div class="mini muted">No existe / no mapeado</div>`
      : '';

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono">${escapeHtml(l.fecha || '')} ${escapeHtml(l.hora || '')}</td>
      <td>
        ${escapeHtml(l.procedimientoNombre || '')}
        <div class="mini muted mono">${escapeHtml(l.procedimientoId || '')}</div>
        ${procWarn}
      </td>
      <td>${escapeHtml(l.pacienteNombre || '')}</td>
      <td class="mono">${escapeHtml(l.pacienteRut || '')}</td>
      <td><b>${clp(l.pagoProfesional || 0)}</b></td>
      <td>${st}</td>
      <td class="mini">${escapeHtml(obs || '')}</td>
    `;
    tb.appendChild(tr);
  }
}

function closeDetalle(){
  $('modalBackdrop').style.display = 'none';

  const resumenHost = $('modalResumen');
  if(resumenHost) resumenHost.innerHTML = '';
}

/* =========================
   Modal fecha de pago
========================= */
function closeFechaPago(){
  $('fechaPagoBackdrop').style.display = 'none';
}

function openFechaPago(){
  const actual = getFechaPagoTexto();

  $('fechaPagoTitle').textContent = 'Fecha de pago';
  $('fechaPagoSub').textContent = `${monthNameEs(state.mesNum)} ${state.ano}`;
  $('fechaPagoInput').value = actual || '';

  $('fechaPagoBackdrop').style.display = 'grid';
}

async function saveFechaPagoDesdeModal(){
  const valor = cleanReminder($('fechaPagoInput').value || '');

  if(!valor){
    toast('Ingresa una fecha de pago');
    return;
  }

  await saveFechaPagoMes(valor);
  toast('Fecha de pago guardada');
  closeFechaPago();
  paint();
}

/* =========================
   Modal ajustes
========================= */
function closeAjustes(){
  $('ajustesBackdrop').style.display = 'none';
  state.ajustesActualAgg = null;
  state.ajustesActualKey = '';
}

function formatAjustesSub(agg){
  return `${monthNameEs(state.mesNum)} ${state.ano} · ${agg.nombre || 'Profesional'} · ${agg.profesionDisplay || agg.roleNombre || ''}`;
}

function fillAjustesForm(agg){
  const a = agg.ajustes || {};

  $('ajustesTitle').textContent = 'Ajustes mensuales';
  $('ajustesSub').textContent = formatAjustesSub(agg);

  $('ajDescuentoAplica').checked = !!a.descuentoAplica;
  $('ajDescuentoCLP').value = a.descuentoCLP ? String(a.descuentoCLP) : '';
  $('ajDescuentoAsunto').value = a.descuentoAsunto || '';

  const esCirujano = normalize(agg.roleId) === 'r_cirujano';
  $('ajBalonAplica').checked = !!a.balonAplica;
  $('ajBalonCantidad').value = a.balonCantidad ? String(a.balonCantidad) : '';
  $('ajBalonValorUnitario').value = a.balonValorUnitario ? String(a.balonValorUnitario) : '';
  $('ajBalonAsunto').value = a.balonAsunto || 'Instalación de balón';

  $('ajBalonAplica').disabled = !esCirujano;
  $('ajBalonCantidad').disabled = !esCirujano;
  $('ajBalonValorUnitario').disabled = !esCirujano;
  $('ajBalonAsunto').disabled = !esCirujano;

  $('ajBalonHelp').textContent = esCirujano
    ? 'Este adicional se suma al líquido final del cirujano en el mes.'
    : 'Este adicional solo aplica a profesionales con rol CIRUJANO.';
}

function openAjustes(agg){
  state.ajustesActualAgg = agg;
  state.ajustesActualKey = agg.key;
  fillAjustesForm(agg);
  $('ajustesBackdrop').style.display = 'grid';
}

async function saveAjustesDesdeModal(){
  if(!state.ajustesActualAgg || !state.ajustesActualKey){
    toast('No hay profesional seleccionado');
    return;
  }

  const agg = state.ajustesActualAgg;
  const esCirujano = normalize(agg.roleId) === 'r_cirujano';

  const payload = {
    descuentoAplica: !!$('ajDescuentoAplica').checked,
    descuentoCLP: parseDecimalFlexible($('ajDescuentoCLP').value || ''),
    descuentoAsunto: cleanReminder($('ajDescuentoAsunto').value || ''),
    balonAplica: esCirujano ? !!$('ajBalonAplica').checked : false,
    balonCantidad: esCirujano ? (Number($('ajBalonCantidad').value || 0) || 0) : 0,
    balonValorUnitario: esCirujano ? parseDecimalFlexible($('ajBalonValorUnitario').value || '') : 0,
    balonAsunto: esCirujano ? cleanReminder($('ajBalonAsunto').value || 'Instalación de balón') : 'Instalación de balón'
  };

  await saveAjustesProfesional(state.ajustesActualKey, payload);
  toast('Ajustes guardados');
  closeAjustes();
  await recalc();
}

/* =========================
   CSV exports
========================= */
function exportResumenCSV(){
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',
    'rol','profesion',
    'casos',
    'totalValorizado',
    'descuentoCLP',
    'balonSubtotal',
    'liquido',
    'alertas','pendientes'
  ];

  const items = state.liquidResumen.map(a => ({
    mes: monthNameEs(state.mesNum),
    ano: String(state.ano),

    profesional: a.nombre || '',
    rut: a.rut || '',

    empresa: a.empresaNombre || '',
    rutEmpresa: a.empresaRut || '',

    tipoPersona: a.tipoPersona || '',
    rol: a.roleNombre || '',
    profesion: a.profesionDisplay || '',
    casos: Number(a.casos || 0),

    totalValorizado: Number(a.ajustes?.totalValorizado || 0),
    descuentoCLP: Number(a.ajustes?.descuentoCLP || 0),
    balonSubtotal: Number(a.ajustes?.balonSubtotal || 0),
    liquido: Number(a.ajustes?.liquido || 0),

    alertas: Number(a.alertasCount || 0),
    pendientes: Number(a.pendientesCount || 0)
  }));

  exportRowsToXLSX(
    `liquidaciones_ambulatorias_resumen_${state.ano}_${String(state.mesNum).padStart(2,'0')}.xlsx`,
    'Resumen',
    headers,
    items
  );

  toast('XLSX resumen exportado');
}

function exportDetalleCSV(){
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',
    'rol','profesion',
    'fecha','hora',
    'procedimiento','procedimientoId','procedimientoExiste',
    'modalidad',
    'paciente','rutPaciente',
    'origen','categoria',
    'tarifaModoValor','tarifaColumnaOrigen','comisionPct',
    'valorBase','pagoProfesional','utilidad',
    'estadoLinea',
    'observacion',
    'prodId'
  ];

  const items = [];
  for(const a of state.liquidResumen){
    for(const l of (a.lines || [])){
      items.push({
        mes: monthNameEs(state.mesNum),
        ano: String(state.ano),

        profesional: a.nombre || '',
        rut: a.rut || '',
        empresa: a.empresaNombre || '',
        rutEmpresa: a.empresaRut || '',
        tipoPersona: a.tipoPersona || '',
        rol: a.roleNombre || '',
        profesion: a.profesionDisplay || '',

        fecha: l.fecha || '',
        hora: l.hora || '',

        procedimiento: l.procedimientoNombre || '',
        procedimientoId: l.procedimientoId || '',
        procedimientoExiste: l.procedimientoExists ? 'SI' : 'NO',

        modalidad: l.modalidad || '',

        paciente: l.pacienteNombre || '',
        rutPaciente: l.pacienteRut || '',

        origen: l.origen || '',
        categoria: l.categoria || '',

        tarifaModoValor: l.tarifaModoValor || '',
        tarifaColumnaOrigen: l.tarifaColumnaOrigen || '',
        comisionPct: hasRealValue(l.comisionPct) ? Number(l.comisionPct) : '',

        valorBase: hasRealValue(l.valorBase) ? Number(l.valorBase) : '',
        pagoProfesional: hasRealValue(l.pagoProfesional) ? Number(l.pagoProfesional) : '',
        utilidad: hasRealValue(l.utilidad) ? Number(l.utilidad) : '',

        estadoLinea: pickDisplayEstadoLinea(l),
        observacion: l.observacion || '',
        prodId: l.prodId || ''
      });
    }
  }

  exportRowsToXLSX(
    `liquidaciones_ambulatorias_detalle_${state.ano}_${String(state.mesNum).padStart(2,'0')}.xlsx`,
    'Detalle',
    headers,
    items
  );

  toast('XLSX detalle exportado');
}

function exportDetalleProfesional(agg){
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',
    'rol','profesion',
    'fecha','hora',
    'procedimiento','procedimientoId','procedimientoExiste',
    'modalidad',
    'paciente','rutPaciente',
    'origen','categoria',
    'tarifaModoValor','tarifaColumnaOrigen','comisionPct',
    'valorBase','pagoProfesional','utilidad',
    'estadoLinea',
    'observacion',
    'prodId'
  ];

  const items = (agg.lines || []).map(l => ({
    mes: monthNameEs(state.mesNum),
    ano: String(state.ano),

    profesional: agg.nombre || '',
    rut: agg.rut || '',
    empresa: agg.empresaNombre || '',
    rutEmpresa: agg.empresaRut || '',
    tipoPersona: agg.tipoPersona || '',
    rol: agg.roleNombre || '',
    profesion: agg.profesionDisplay || '',

    fecha: l.fecha || '',
    hora: l.hora || '',

    procedimiento: l.procedimientoNombre || '',
    procedimientoId: l.procedimientoId || '',
    procedimientoExiste: l.procedimientoExists ? 'SI' : 'NO',

    modalidad: l.modalidad || '',

    paciente: l.pacienteNombre || '',
    rutPaciente: l.pacienteRut || '',

    origen: l.origen || '',
    categoria: l.categoria || '',

    tarifaModoValor: l.tarifaModoValor || '',
    tarifaColumnaOrigen: l.tarifaColumnaOrigen || '',
    comisionPct: hasRealValue(l.comisionPct) ? Number(l.comisionPct) : '',

    valorBase: hasRealValue(l.valorBase) ? Number(l.valorBase) : '',
    pagoProfesional: hasRealValue(l.pagoProfesional) ? Number(l.pagoProfesional) : '',
    utilidad: hasRealValue(l.utilidad) ? Number(l.utilidad) : '',

    estadoLinea: pickDisplayEstadoLinea(l),
    observacion: l.observacion || '',
    prodId: l.prodId || ''
  }));

  const safeName = safeFileName(agg.nombre || 'profesional');

  exportRowsToXLSX(
    `liquidacion_ambulatoria_${safeName}_${state.ano}_${String(state.mesNum).padStart(2,'0')}.xlsx`,
    'DetalleProfesional',
    headers,
    items
  );

  toast('XLSX profesional exportado');
}

/* =========================
   PDF
========================= */
const PDF_ASSET_LOGO_URL = './logoCRazul.jpeg';

async function fetchAsArrayBuffer(url){
  try{
    const res = await fetch(url);
    if(!res.ok) throw new Error('HTTP '+res.status);
    return await res.arrayBuffer();
  }catch(e){
    console.warn('fetchAsArrayBuffer falló para:', url, e);
    return null;
  }
}

function buildPdfResumenRows(agg){
  const rows = (agg?.resumenModalidades || []).map(r => ({
    modalidad: r?.modalidad || '',
    cantidad: Number(r?.cantidad || 0) || 0,
    subtotal: Number(r?.subtotal || 0) || 0
  }));

  if((agg?.ajustes?.balonSubtotal || 0) > 0){
    rows.push({
      modalidad: agg?.ajustes?.balonAsunto || 'INSTALACIÓN DE BALÓN',
      cantidad: Number(agg?.ajustes?.balonCantidad || 0) || 0,
      subtotal: Number(agg?.ajustes?.balonSubtotal || 0) || 0
    });
  }

  return rows;
}

function buildModalResumenHtml(agg){
  const rows = (agg?.resumenModalidades || []).map(r => ({
    modalidad: r?.modalidad || '',
    cantidad: Number(r?.cantidad || 0) || 0,
    subtotal: Number(r?.subtotal || 0) || 0
  }));

  if((agg?.ajustes?.balonSubtotal || 0) > 0){
    rows.push({
      modalidad: agg?.ajustes?.balonAsunto || 'INSTALACIÓN DE BALÓN',
      cantidad: Number(agg?.ajustes?.balonCantidad || 0) || 0,
      subtotal: Number(agg?.ajustes?.balonSubtotal || 0) || 0
    });
  }

  const body = rows.map(r => `
    <tr>
      <td style="padding:8px; border:1px solid #d1d5db;">${escapeHtml((r.modalidad || '').toUpperCase())}</td>
      <td class="mono" style="text-align:center; padding:8px; border:1px solid #d1d5db;">${escapeHtml(String(r.cantidad || 0))}</td>
      <td class="mono" style="text-align:right; padding:8px; border:1px solid #d1d5db;"><b>${escapeHtml(clp(r.subtotal || 0))}</b></td>
    </tr>
  `).join('');

  return `
    <div class="card" style="margin:12px 0 14px 0; overflow:hidden;">
      <div style="padding:10px 12px; background:#0f3b4a; color:#fff; font-weight:700;">
        Resumen de la liquidación
      </div>

      <div style="padding:12px;">
        <table style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead>
            <tr style="background:#1f8c73; color:#fff;">
              <th style="text-align:left; padding:8px; border:1px solid #d1d5db;">Modalidad</th>
              <th style="text-align:center; padding:8px; border:1px solid #d1d5db;">Cantidad</th>
              <th style="text-align:right; padding:8px; border:1px solid #d1d5db;">Subtotal</th>
            </tr>
          </thead>
          <tbody>
            ${body}
            <tr style="background:#ccfbf1; font-weight:700;">
              <td style="padding:8px; border:1px solid #d1d5db;">TOTAL</td>
              <td class="mono" style="text-align:center; padding:8px; border:1px solid #d1d5db;">${escapeHtml(String(agg?.casos || 0))}</td>
              <td class="mono" style="text-align:right; padding:8px; border:1px solid #d1d5db;"><b>${escapeHtml(clp(agg?.ajustes?.totalValorizado || 0))}</b></td>
            </tr>
          </tbody>
        </table>

        <div style="margin-top:12px; display:grid; gap:6px;">
          <div style="display:flex; justify-content:space-between; gap:12px; padding:8px 10px; border:1px solid #d1d5db; border-radius:8px;">
            <span>Fecha de pago</span>
            <b class="mono">${escapeHtml(getFechaPagoTexto() || '—')}</b>
          </div>
        
          <div style="display:flex; justify-content:space-between; gap:12px; padding:8px 10px; border:1px solid #d1d5db; border-radius:8px;">
            <span>$ Valorizado</span>
            <b class="mono">${escapeHtml(clp(agg?.ajustes?.totalValorizado || 0))}</b>
          </div>

          <div style="display:flex; justify-content:space-between; gap:12px; padding:8px 10px; border:1px solid #d1d5db; border-radius:8px;">
            <span>$ Boleta</span>
            <b class="mono">${escapeHtml(clp(agg?.ajustes?.totalBoleta || 0))}</b>
          </div>

          <div style="display:flex; justify-content:space-between; gap:12px; padding:8px 10px; border:1px solid #d1d5db; border-radius:8px;">
            <span>$ Retención</span>
            <b class="mono">${escapeHtml(clp(agg?.ajustes?.retencionCLP || 0))}</b>
          </div>

          <div style="display:flex; justify-content:space-between; gap:12px; padding:8px 10px; border:1px solid #d1d5db; border-radius:8px;">
            <span>${escapeHtml(agg?.ajustes?.descuentoAsunto || 'Descuento seguro complementario')}</span>
            <b class="mono">${escapeHtml(clp(agg?.ajustes?.descuentoCLP || 0))}</b>
          </div>

          <div style="display:flex; justify-content:space-between; gap:12px; padding:10px 12px; border:1px solid #99f6e4; background:#ecfeff; border-radius:8px; font-size:14px;">
            <span><b>Total a pagar</b></span>
            <b class="mono">${escapeHtml(clp(agg?.ajustes?.liquido || 0))}</b>
          </div>
        </div>
      </div>
    </div>
  `;
}

function getNroLiquidacion(agg){
  const hashBase = `${ajustesMonthId()}|${agg.key || ''}|${agg.nombre || ''}`;
  let n = 0;
  for(let i=0;i<hashBase.length;i++){
    n = (n * 31 + hashBase.charCodeAt(i)) % 100000;
  }
  return String(n).padStart(5,'0');
}

function getNombreRutPago(agg){
  const nombrePersona = (agg.nombre || '—').toUpperCase();

  if((agg.tipoPersona || '').toLowerCase() === 'juridica'){
    const nombreEmpresa = (agg.empresaNombre || '—').toUpperCase();
    return `${nombreEmpresa} | ${nombrePersona}`;
  }

  return nombrePersona;
}

function getRutPago(agg){
  if((agg.tipoPersona || '').toLowerCase() === 'juridica'){
    return (agg.empresaRut || agg.rut || '—').toUpperCase();
  }
  return (agg.rut || '—').toUpperCase();
}

function getDatosBoleta(){
  return {
    rut: '77.460.159-7',
    razonSocial: 'SERVICIOS MÉDICOS PROVIDENCIA GCS SPA',
    giro: 'ACTIVIDADES DE HOSPITALES Y CLÍNICAS PRIVADAS',
    direccion: 'MANUEL MONTT 427'
  };
}

function getFechaPagoTexto(){
  if(cleanReminder(state.fechaPagoManual || '')){
    return cleanReminder(state.fechaPagoManual);
  }

  // default: día 5 del mes siguiente al liquidado
  let mes = Number(state.mesNum || 0);
  let ano = Number(state.ano || 0);

  if(!mes || !ano) return '';

  mes += 1;
  if(mes === 13){
    mes = 1;
    ano += 1;
  }

  return `5/${mes}/${ano}`;
}

async function generarPDFLiquidacionProfesional(agg){
  const pdfDoc = await PDFDocument.create();

  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  // A4 vertical
  const W = 595.28;
  const H = 841.89;

  const rgb255 = (r,g,b)=> rgb(r/255, g/255, b/255);

  const RENNAT_BLUE  = rgb255(0, 39, 56);
  const RENNAT_GREEN = rgb255(31, 140, 115);
  const RENNAT_GRAY  = rgb255(210, 215, 220);
  const BOX_BLUE     = rgb255(179, 208, 230);
  const TEXT_MAIN    = rgb(0.08, 0.09, 0.11);
  const TEXT_MUTED   = rgb(0.45, 0.48, 0.52);
  const BORDER_SOFT  = rgb(0.72, 0.76, 0.80);

  const M = 30;
  const boxW = W - 2*M;

  const drawText = (page, text, x, y, size=10, bold=false, color=TEXT_MAIN) => {
    page.drawText(String(text ?? ''), {
      x, y, size,
      font: bold ? fontBold : font,
      color
    });
  };

  const measure = (text, size=10, bold=false) => {
    const f = bold ? fontBold : font;
    return f.widthOfTextAtSize(String(text ?? ''), size);
  };

  const drawBox = (page, x, yTop, w, h, fill=null, stroke=BORDER_SOFT, strokeW=1) => {
    page.drawRectangle({
      x, y: yTop - h, width: w, height: h,
      color: fill || undefined,
      borderColor: stroke,
      borderWidth: strokeW
    });
  };

  const drawVLine = (page, x, yTop, h, thick=1, col=BORDER_SOFT) => {
    page.drawLine({ start:{x, y:yTop}, end:{x, y:yTop-h}, thickness:thick, color:col });
  };

  const drawHLine = (page, x, y, w, thick=1, col=BORDER_SOFT) => {
    page.drawLine({ start:{x, y}, end:{x:x+w, y}, thickness:thick, color:col });
  };

  const drawCellText = (page, text, x, yTop, cellH, size=10, bold=false, color=TEXT_MAIN, pad=6) => {
    const yText = yTop - (cellH * 0.72);
    page.drawText(String(text ?? ''), {
      x: x + pad,
      y: yText,
      size,
      font: bold ? fontBold : font,
      color
    });
  };

  const drawCellTextCenter = (page, text, x, yTop, cellW, cellH, size=10, bold=false, color=TEXT_MAIN) => {
    const t = String(text ?? '');
    const wTxt = (bold ? fontBold : font).widthOfTextAtSize(t, size);
    const yText = yTop - (cellH * 0.72);
    page.drawText(t, {
      x: x + (cellW - wTxt) / 2,
      y: yText,
      size,
      font: bold ? fontBold : font,
      color
    });
  };

  const drawCellTextRight = (page, text, x, yTop, cellW, cellH, size=10, bold=false, color=TEXT_MAIN, pad=6) => {
    const t = String(text ?? '');
    const wTxt = (bold ? fontBold : font).widthOfTextAtSize(t, size);
    const yText = yTop - (cellH * 0.72);
    page.drawText(t, {
      x: x + cellW - pad - wTxt,
      y: yText,
      size,
      font: bold ? fontBold : font,
      color
    });
  };

  const wrapClip = (s, maxChars) => String(s ?? '').slice(0, maxChars);
  const money = (n)=> clp(n || 0);

  const page1 = pdfDoc.addPage([W, H]);
  let y = H - 14;

  const logoBytes = await fetchAsArrayBuffer(PDF_ASSET_LOGO_URL);
  if (logoBytes) {
    try {
      const urlLower = String(PDF_ASSET_LOGO_URL || '').toLowerCase();
      const isJpg = urlLower.endsWith('.jpg') || urlLower.endsWith('.jpeg');

      const logo = isJpg
        ? await pdfDoc.embedJpg(logoBytes)
        : await pdfDoc.embedPng(logoBytes);

      const logoW = 104;
      const logoH = (logo.height / logo.width) * logoW;

      page1.drawImage(logo, {
        x: M + 12,
        y: H - 12 - logoH,
        width: logoW,
        height: logoH
      });
    } catch (e) {
      console.warn('No se pudo embebeder logo:', e);
    }
  }

  y = H - 92;

  // Barra título
  drawBox(page1, M + 50, y, boxW - 100, 24, RENNAT_BLUE, RENNAT_BLUE, 1);
  const title = 'Liquidación de Pago Producción - Participaciones Mensuales';
  drawCellTextCenter(page1, title, M + 50, y, boxW - 100, 24, 10.5, true, rgb(1,1,1));

  y -= 36;

  // Datos cabecera
  const nroLiquidacion = getNroLiquidacion(agg);
  const rutPago = getRutPago(agg);
  const nombreRutPago = getNombreRutPago(agg);
  const profesion = (agg.profesionDisplay || agg.roleNombre || 'PROFESIONAL').toUpperCase();

  const cabRows = [
    ['Nro. Liquidación', nroLiquidacion],
    ['RUT Pago', rutPago],
    ['Nombre RUT de Pago', nombreRutPago],
    ['Profesión', profesion]
  ];

  const rowH = 20;
  const cabH = cabRows.length * rowH;
  const cabX = M + 50;
  const cabW = boxW - 100;
  const cabC1 = 150;
  const cabC2 = cabW - cabC1;

  drawBox(page1, cabX, y, cabW, cabH, rgb(1,1,1), BORDER_SOFT, 1);
  drawVLine(page1, cabX + cabC1, y, cabH, 1, BORDER_SOFT);

  for(let i=0;i<cabRows.length;i++){
    const r = cabRows[i];
    const yTop = y - i*rowH;
    if(i > 0) drawHLine(page1, cabX, yTop, cabW, 1, BORDER_SOFT);

    drawCellText(page1, r[0], cabX, yTop, rowH, 9, false, TEXT_MUTED, 6);
    drawCellTextCenter(page1, r[1], cabX + cabC1, yTop, cabC2, rowH, 9.5, i === 2, TEXT_MAIN);
  }

  y -= cabH + 18;

  // Tabla principal resumen modalidades
  const resumenRows = buildPdfResumenRows(agg);

  const headH = 22;
  const resRowH = 20;
  const tableW = cabW;
  const tableX = cabX;
  const col1 = 190;
  const col2 = 145;
  const col3 = tableW - col1 - col2;
  const totalH = headH + (resumenRows.length + 1) * resRowH;

  drawBox(page1, tableX, y, tableW, totalH, rgb(1,1,1), BORDER_SOFT, 1);
  drawBox(page1, tableX, y, tableW, headH, RENNAT_BLUE, RENNAT_BLUE, 1);

  drawVLine(page1, tableX + col1, y, totalH, 1, BORDER_SOFT);
  drawVLine(page1, tableX + col1 + col2, y, totalH, 1, BORDER_SOFT);

  drawCellTextCenter(page1, 'Modalidad', tableX, y, col1, headH, 8.5, true, rgb(1,1,1));
  drawCellTextCenter(page1, 'Cantidad Consultas', tableX + col1, y, col2, headH, 8.5, true, rgb(1,1,1));
  drawCellTextCenter(page1, '$ Subtotal', tableX + col1 + col2, y, col3, headH, 8.5, true, rgb(1,1,1));

  for(let i=0;i<resumenRows.length;i++){
    const r = resumenRows[i];
    const yTop = y - headH - i*resRowH;
    drawHLine(page1, tableX, yTop, tableW, 1, BORDER_SOFT);

    drawCellText(page1, wrapClip((r.modalidad || '').toUpperCase(), 34), tableX, yTop, resRowH, 8.5, false, TEXT_MAIN, 6);
    drawCellTextCenter(page1, String(r.cantidad || 0), tableX + col1, yTop, col2, resRowH, 8.5, true, TEXT_MAIN);
    drawCellTextRight(page1, money(r.subtotal || 0), tableX + col1 + col2, yTop, col3, resRowH, 8.5, true, TEXT_MAIN, 8);
  }

  const yTotalRow = y - headH - resumenRows.length * resRowH;
  drawHLine(page1, tableX, yTotalRow, tableW, 1, BORDER_SOFT);
  drawBox(page1, tableX, yTotalRow, tableW, resRowH, rgb255(30, 190, 171), rgb255(30, 190, 171), 1);
  drawVLine(page1, tableX + col1, yTotalRow, resRowH, 1, BORDER_SOFT);
  drawVLine(page1, tableX + col1 + col2, yTotalRow, resRowH, 1, BORDER_SOFT);
  drawCellTextCenter(page1, 'TOTAL', tableX, yTotalRow, col1, resRowH, 9, true, RENNAT_BLUE);
  drawCellTextCenter(page1, String(agg.casos || 0), tableX + col1, yTotalRow, col2, resRowH, 9, true, RENNAT_BLUE);
  drawCellTextRight(page1, money(agg.ajustes?.totalValorizado || 0), tableX + col1 + col2, yTotalRow, col3, resRowH, 9, true, RENNAT_BLUE, 8);

  y -= totalH + 16;

  // Resumen montos
  const resumenX = tableX;
  const resumenW = tableW;
  const resumenHeadH = 20;
  const resumenRowH = 18;
  
  const resumenMontoRows = [
    ['$ Valorizado:', money(agg.ajustes?.totalValorizado || 0)],
    ['$ Boleta:', money(agg.ajustes?.totalBoleta || 0)],
    ['$ Retención:', money(agg.ajustes?.retencionCLP || 0)],
    [agg.ajustes?.descuentoAsunto || 'Descuento seguro complementario', money(agg.ajustes?.descuentoCLP || 0)],
    ['$ Líquido:', money(agg.ajustes?.liquido || 0)]
  ];
  
  const rmH = resumenHeadH + resumenMontoRows.length * resumenRowH;
  
  // ✅ Inverso a DATOS BOLETA:
  // izquierda más ancha, derecha más angosta
  const rmC1 = tableW - 140;
  const rmC2 = 140;
  
  drawBox(page1, resumenX, y, resumenW, rmH, rgb(1,1,1), BORDER_SOFT, 1);
  drawBox(page1, resumenX, y, resumenW, resumenHeadH, RENNAT_BLUE, RENNAT_BLUE, 1);
  
  // ✅ Título centrado sobre todo el ancho real de la tabla
  drawCellTextCenter(page1, 'RESUMEN', resumenX, y, resumenW, resumenHeadH, 9.5, true, rgb(1,1,1));
  
  drawVLine(page1, resumenX + rmC1, y, rmH, 1, BORDER_SOFT);
  
  for(let i=0;i<resumenMontoRows.length;i++){
    const r = resumenMontoRows[i];
    const yTop = y - resumenHeadH - i*resumenRowH;
    drawHLine(page1, resumenX, yTop, resumenW, 1, BORDER_SOFT);
  
    const isLiquido = i === resumenMontoRows.length - 1;
  
    drawCellText(
      page1,
      wrapClip(r[0], 52),
      resumenX,
      yTop,
      resumenRowH,
      8.3,
      isLiquido,
      TEXT_MAIN,
      6
    );
  
    drawCellTextRight(
      page1,
      r[1],
      resumenX + rmC1,
      yTop,
      rmC2,
      resumenRowH,
      8.3,
      true,
      TEXT_MAIN,
      8
    );
  }
  
  y -= rmH + 16;

  // Datos boleta
  const datosBoleta = getDatosBoleta(agg);

  const boletaHeadH = 24;
  const boletaRowH = 18;
  const boletaRows = [
    ['RUT:', datosBoleta.rut || '—'],
    ['RAZÓN SOCIAL:', datosBoleta.razonSocial || '—'],
    ['GIRO:', datosBoleta.giro || '—'],
    ['DIRECCIÓN', datosBoleta.direccion || '—']
  ];

  const boletaH = boletaHeadH + boletaRows.length * boletaRowH;
  const boletaC1 = 140;
  const boletaC2 = tableW - boletaC1;

  drawBox(page1, tableX, y, tableW, boletaH, rgb(1,1,1), BORDER_SOFT, 1);
  drawBox(page1, tableX, y, tableW, boletaHeadH, BOX_BLUE, BOX_BLUE, 1);
  drawCellTextCenter(page1, 'DATOS BOLETA', tableX, y, tableW, boletaHeadH, 9.5, true, RENNAT_BLUE);

  drawVLine(page1, tableX + boletaC1, y, boletaH, 1, BORDER_SOFT);

  for(let i=0;i<boletaRows.length;i++){
    const r = boletaRows[i];
    const yTop = y - boletaHeadH - i*boletaRowH;
    drawHLine(page1, tableX, yTop, tableW, 1, BORDER_SOFT);

    drawCellText(page1, r[0], tableX, yTop, boletaRowH, 8.2, false, TEXT_MUTED, 6);
    drawCellTextCenter(page1, wrapClip(r[1], 54), tableX + boletaC1, yTop, boletaC2, boletaRowH, 8.2, false, TEXT_MAIN);
  }

  y -= boletaH + 30;

  // Fecha de pago
  const fechaPago = getFechaPagoTexto();
  const fpH = 22;
  drawBox(page1, tableX, y, tableW, fpH, rgb(1,1,1), BORDER_SOFT, 1);
  drawVLine(page1, tableX + boletaC1, y, fpH, 1, BORDER_SOFT);
  drawCellText(page1, 'FECHA DE PAGO', tableX, y, fpH, 8.6, false, RENNAT_BLUE, 6);
  drawCellTextCenter(page1, fechaPago, tableX + boletaC1, y, boletaC2, fpH, 9.2, true, TEXT_MAIN);

  y -= fpH + 18;

  // texto final
  const infoText = `Por favor enviar boleta o factura por el monto total al correo:\ncontabilidad@clinicarennt.cl`;
  const linesTxt = String(infoText).split('\n');
  let yTxt = y;
  
  for(const t of linesTxt){
    drawCellTextCenter(
      page1,
      t,
      tableX,
      yTxt,
      tableW,     // ancho del bloque
      14,         // alto de línea/celda
      7.8,        // tamaño fuente
      true,       // bold
      RENNAT_BLUE // color
    );
    yTxt -= 11;
  }

  // =========================
  // Página 2 detalle
  // =========================
  const W2 = 841.89;
  const H2 = 595.28;
  const page2 = pdfDoc.addPage([W2, H2]);

  let y2 = H2 - M;
  const barX2 = M;
  const barW2 = W2 - 2*M;
  const barH = 26;
  const T_DETALLE = 'Detalle de Prestaciones Ambulatorias';

  drawBox(page2, barX2, y2, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
  drawCellTextCenter(page2, T_DETALLE, barX2, y2, barW2, barH, 12, true, rgb(1,1,1));
  y2 -= (barH + 12);

  drawText(page2, `${agg.nombre || '—'} · ${monthNameEs(state.mesNum)} ${state.ano}`, M, y2, 10, false, TEXT_MUTED);
  y2 -= 12;

  const detX = M;
  const detW = W2 - 2*M;
  const detHeadH = 24;
  const detRowH = 18;

let detCols = [
  { key:'n',      label:'#',             w: 40  },
  { key:'fecha',  label:'FECHA',         w: 105 },
  { key:'proc',   label:'PROCEDIMIENTO', w: 285 },
  { key:'pac',    label:'PACIENTE',      w: 210 },
  { key:'rutPac', label:'RUT PACIENTE',  w: 120 },
  { key:'pp',     label:'SUBTOTAL',      w: 100 }
];

  {
    const sum = detCols.reduce((a,c)=>a + c.w, 0);
    const k = detW / sum;
    detCols = detCols.map(c => ({ ...c, w: Math.round(c.w * k) }));
    const sum2 = detCols.reduce((a,c)=>a + c.w, 0);
    const diff = detW - sum2;
    detCols[detCols.length - 1].w += diff;
  }

  function drawDetalleHeader(page, topY){
    drawBox(page, detX, topY, detW, detHeadH, RENNAT_GREEN, RENNAT_GREEN, 1);

    let cx = detX;
    for(let i=0;i<detCols.length;i++){
      if(i>0) drawVLine(page, cx, topY, detHeadH, 1, BORDER_SOFT);
      drawCellText(page, detCols[i].label, cx, topY, detHeadH, 9, true, rgb(1,1,1), 8);
      cx += detCols[i].w;
    }
  }

  function drawDetalleRow(page, row, topY, rowNumber){
    drawHLine(page, detX, topY, detW, 1, BORDER_SOFT);

    let xPos = detX;

    drawCellTextRight(page, String(rowNumber), xPos, topY, detCols[0].w, detRowH, 9, true, TEXT_MUTED, 8);
    xPos += detCols[0].w;
    
    const fechaTxt = `${row.fecha || ''}${row.hora ? ' ' + row.hora : ''}`;
    drawCellText(page, wrapClip(fechaTxt, 18), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[1].w;
    
    drawCellText(page, wrapClip(`${row.procedimientoId || ''} · ${row.procedimientoNombre || ''}`, 46), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[2].w;
    
    drawCellText(page, wrapClip(row.pacienteNombre || '', 30), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[3].w;
    
    drawCellText(page, wrapClip(row.pacienteRut || '', 16), xPos, topY, detRowH, 9, false, TEXT_MUTED, 8);
    xPos += detCols[4].w;
    
    drawCellTextRight(page, money(row.pagoProfesional || 0), xPos, topY, detCols[5].w, detRowH, 9, true, TEXT_MAIN, 8);
  }

  const allLinesSorted = [...(agg.lines || [])].sort((a,b)=>{
    const fa = normalize(a.fecha);
    const fb = normalize(b.fecha);
    if(fa !== fb) return fa.localeCompare(fb);
    const pa = normalize(a.pacienteNombre);
    const pb = normalize(b.pacienteNombre);
    if(pa !== pb) return pa.localeCompare(pb);
    return normalize(a.procedimientoNombre).localeCompare(normalize(b.procedimientoNombre));
  });

  let currentPage = page2;
  let cursorTopY = y2;
  let rowNumber = 1;
  let idx = 0;
  const bottomLimitDefault = M + 20;

  while (idx < allLinesSorted.length) {
    let availableH = cursorTopY - bottomLimitDefault - detHeadH;
    let canFit = Math.max(0, Math.floor(availableH / detRowH));

    if (canFit <= 0) {
      currentPage = pdfDoc.addPage([W2, H2]);
      cursorTopY = H2 - M;

      drawBox(currentPage, barX2, cursorTopY, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
      drawCellTextCenter(currentPage, T_DETALLE, barX2, cursorTopY, barW2, barH, 12, true, rgb(1,1,1));
      cursorTopY -= (barH + 12);

      drawText(currentPage, `${agg.nombre || '—'} · ${monthNameEs(state.mesNum)} ${state.ano}`, M, cursorTopY, 10, false, TEXT_MUTED);
      cursorTopY -= 12;
      continue;
    }

    const remaining = allLinesSorted.length - idx;
    const slice = allLinesSorted.slice(idx, idx + Math.min(canFit, remaining));
    const blockH = detHeadH + slice.length * detRowH;

    drawBox(currentPage, detX, cursorTopY, detW, blockH, rgb(1,1,1), BORDER_SOFT, 1);
    drawDetalleHeader(currentPage, cursorTopY);

    let cx = detX;
    for (let i = 0; i < detCols.length; i++) {
      if (i > 0) drawVLine(currentPage, cx, cursorTopY, blockH, 1, BORDER_SOFT);
      cx += detCols[i].w;
    }

    for (let r = 0; r < slice.length; r++) {
      const row = slice[r];
      const rowTop = cursorTopY - detHeadH - r * detRowH;
      drawDetalleRow(currentPage, row, rowTop, rowNumber++);
    }

    const yBottom = cursorTopY - blockH;
    drawHLine(currentPage, detX, yBottom, detW, 1, BORDER_SOFT);

    idx += slice.length;

    if (idx < allLinesSorted.length) {
      currentPage = pdfDoc.addPage([W2, H2]);
      cursorTopY = H2 - M;

      drawBox(currentPage, barX2, cursorTopY, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
      drawCellTextCenter(currentPage, T_DETALLE, barX2, cursorTopY, barW2, barH, 12, true, rgb(1,1,1));
      cursorTopY -= (barH + 12);

      drawText(currentPage, `${agg.nombre || '—'} · ${monthNameEs(state.mesNum)} ${state.ano}`, M, cursorTopY, 10, false, TEXT_MUTED);
      cursorTopY -= 12;
    }
  }

  const pdfBytes = await pdfDoc.save();
  return pdfBytes;
}

/* =========================
   Boot / UI init
========================= */
function initMonthYearSelectors(){
  const mesSel = $('mes');
  mesSel.innerHTML = '';
  for(let m=1;m<=12;m++){
    const opt = document.createElement('option');
    opt.value = String(m);
    opt.textContent = monthNameEs(m);
    mesSel.appendChild(opt);
  }

  const now = new Date();
  const mesActual = now.getMonth() + 1;
  const mesPrev = (mesActual === 1) ? 12 : (mesActual - 1);
  const anoPrev = (mesActual === 1) ? (now.getFullYear() - 1) : now.getFullYear();

  const y = anoPrev;
  const anoSel = $('ano');
  anoSel.innerHTML = '';
  for(let yy=y-2; yy<=y+3; yy++){
    const opt = document.createElement('option');
    opt.value = String(yy);
    opt.textContent = String(yy);
    anoSel.appendChild(opt);
  }

  state.mesNum = mesPrev;
  state.ano = anoPrev;

  mesSel.value = String(state.mesNum);
  anoSel.value = String(state.ano);

  mesSel.addEventListener('change', ()=>{
    state.mesNum = Number(mesSel.value);
    recalc();
  });

  anoSel.addEventListener('change', ()=>{
    state.ano = Number(anoSel.value);
    recalc();
  });
}

async function recalc(){
  try{
    $('btnRecalcular').disabled = true;

    // limpia cache de ajustes del mes si cambió
    const currentMonthId = ajustesMonthId();
    if(state._lastMonthId !== currentMonthId){
      state.ajustesCache = new Map();
      state._lastMonthId = currentMonthId;
    }

    await loadProduccionMes();
    await loadFechaPagoMes();
    await buildLiquidaciones();
    paint();

  }catch(err){
    console.error(err);
    toast('Error recalculando (ver consola)');
  }finally{
    $('btnRecalcular').disabled = false;
  }
}

/* =========================
   Eventos UI
========================= */
function bindUI(){
  $('q').addEventListener('input', (e)=>{
    state.q = (e.target.value || '').toString();
    paint();
  });

  $('btnFechaPago').addEventListener('click', openFechaPago);
  $('btnRecalcular').addEventListener('click', recalc);
  $('btnCSVResumen').addEventListener('click', exportResumenCSV);
  $('btnCSVDetalle').addEventListener('click', exportDetalleCSV);

  $('btnClose').addEventListener('click', closeDetalle);
  $('btnCerrar2').addEventListener('click', closeDetalle);
  $('modalBackdrop').addEventListener('click', (e)=>{
    if(e.target === $('modalBackdrop')) closeDetalle();
  });

  $('btnExportDetalleProf').addEventListener('click', ()=>{
    if(!state.lastDetailExportLines.length){
      toast('No hay detalle abierto');
      return;
    }

    const first = state.lastDetailExportLines[0];
    const agg = {
      nombre: first?.profesionalNombre || 'Profesional',
      rut: first?.profesionalRut || '',
      tipoPersona: first?.tipoPersona || '',
      empresaNombre: first?.empresaNombre || '',
      empresaRut: first?.empresaRut || '',
      roleId: first?.roleId || '',
      roleNombre: first?.roleNombre || '',
      profesionDisplay: first?.profesionDisplay || '',
      lines: state.lastDetailExportLines
    };

    exportDetalleProfesional(agg);
  });

  $('btnAjustesClose').addEventListener('click', closeAjustes);
  $('btnAjustesCancelar').addEventListener('click', closeAjustes);
  $('ajustesBackdrop').addEventListener('click', (e)=>{
    if(e.target === $('ajustesBackdrop')) closeAjustes();
  });
  $('btnAjustesGuardar').addEventListener('click', saveAjustesDesdeModal);
  
  $('btnFechaPagoClose').addEventListener('click', closeFechaPago);
  $('btnFechaPagoCancelar').addEventListener('click', closeFechaPago);
  $('fechaPagoBackdrop').addEventListener('click', (e)=>{
    if(e.target === $('fechaPagoBackdrop')) closeFechaPago();
  });
  $('btnFechaPagoGuardar').addEventListener('click', saveFechaPagoDesdeModal);
}

/* =========================
   Main Auth
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    await loadSidebar({ active: 'liquidaciones_ambulatorios' });
    setActiveNav('liquidaciones_ambulatorios');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    initMonthYearSelectors();
    bindUI();

    await Promise.all([
      loadProfesionales(),
      loadProcedimientosAmbulatorios()
    ]);

    await recalc();
  }
});
