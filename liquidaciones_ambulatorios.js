// liquidaciones_ambulatorios.js — COMPLETO
// ✅ Visualmente coherente con liquidaciones.js
// ✅ Lógica adaptada a procedimientos ambulatorios (PAxxxx)
// ✅ Lee producción confirmada del mes desde collectionGroup('items')
// ✅ Cruza con:
//    - profesionales
//    - procedimientos tipo="ambulatorio"
// ✅ Cálculo por ítem:
//    - valorBase
//    - pagoProfesional
//    - utilidad
// ✅ Export CSV resumen / detalle / profesional
// ✅ PDF con look similar al módulo de liquidaciones cirugías

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';
await loadSidebar({ active: 'liquidaciones_ambulatorios' });

import {
  collection, collectionGroup, getDocs, query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

import {
  PDFDocument,
  StandardFonts,
  rgb
} from 'https://cdn.skypack.dev/pdf-lib@1.17.1';

/* =========================
   AJUSTE ÚNICO
========================= */
// Se usa collectionGroup('items') igual que en el módulo de cirugías.
// Luego se filtran SOLO ítems ambulatorios.
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

function isTruthyBool(v){
  return v === true ||
    String(v || '').toLowerCase().trim() === 'true' ||
    String(v || '').trim() === '1';
}

function yyyymm(ano, mesNum){
  return `${String(ano)}${String(mesNum).padStart(2,'0')}`;
}

function pickDisplayEstadoLinea(l){
  if(l.isAlerta) return 'ALERTA';
  if(l.isPendiente) return 'PENDIENTE';
  return 'OK';
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
  lastDetailExportLines: []
};

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colProcedimientos = collection(db, 'procedimientos');

/* =========================
   Load catálogos
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

    const doc = {
      id: String(rutId || d.id),
      rutId: String(rutId || d.id),

      nombreProfesional: toUpperSafe(nombreProfesional || ''),
      rut: rutPersonal,

      razonSocial: toUpperSafe(razonSocial || ''),
      rutEmpresa,

      tipoPersona,
      estado
    };

    const keys = new Set();
    keys.add(String(doc.id || '').trim());
    keys.add(String(doc.rutId || '').trim());
    keys.add(String(doc.rut || '').trim());
    keys.add(canonRutAny(doc.id));
    keys.add(canonRutAny(doc.rutId));
    keys.add(canonRutAny(doc.rut));

    for(const k of keys){
      const kk = (k ?? '').toString().trim();
      if(kk) byId.set(kk, doc);
    }

    if(nombreProfesional) byName.set(normalize(nombreProfesional), doc);
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
      valor: Number(tarifaRaw.valor || 0) || 0,
      columnaOrigen: cleanReminder(tarifaRaw.columnaOrigen || ''),
      comisionPct: Number(tarifaRaw.comisionPct || 0) || 0,
      valorProfesional: Number(tarifaRaw.valorProfesional || 0) || 0
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
    const doc = normalizeProcDocAmb(d.id, x);

    const tipoN = normalize(doc.tipo);
    const codigoN = String(doc.codigo || '').toUpperCase().trim();

    const esAmb =
      tipoN === 'ambulatorio' ||
      /^PA\d+$/.test(codigoN);

    if(!esAmb) return;

    byId.set(String(doc.id), doc);

    if(doc.codigo){
      byCodigo.set(String(doc.codigo).trim().toUpperCase(), doc);
    }

    if(doc.nombre) byName.set(normalize(doc.nombre), doc);
    if(doc.tratamiento) byName.set(normalize(doc.tratamiento), doc);
  });

  state.procedimientosByName = byName;
  state.procedimientosById = byId;
  state.procedimientosByCodigo = byCodigo;
}

/* =========================
   Producción ambulatoria
========================= */
function isEstadoAnulada(v=''){
  const x = normalize(v);
  return x === 'anulada' || x === 'anulado' || x === 'cancelada' || x === 'cancelado';
}

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
    valor: Number(t.valor || 0) || 0,
    columnaOrigen: cleanReminder(t.columnaOrigen || ''),
    comisionPct: Number(t.comisionPct || 0) || 0,
    valorProfesional: Number(t.valorProfesional || 0) || 0
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

function calcularPagoProfesional(valorBase, tarifa){
  const valorBaseNum = Number(valorBase || 0) || 0;
  const valorProfesionalFijo = Number(tarifa?.valorProfesional || 0) || 0;
  const comisionPct = Number(tarifa?.comisionPct || 0) || 0;

  if(valorProfesionalFijo > 0){
    return valorProfesionalFijo;
  }

  if(comisionPct > 0 && valorBaseNum > 0){
    return Math.round(valorBaseNum * (comisionPct / 100));
  }

  return 0;
}

function calcularLineaAmbulatoria(x, procDoc){
  const raw = getRawContainer(x);
  const tarifa = getTarifaAmbulatoria(procDoc);

  let valorBase = 0;

  if(tarifa.modoValor === 'archivo'){
    valorBase = getValorDesdeRaw(raw, tarifa.columnaOrigen);
  }else{
    valorBase = Number(tarifa.valor || 0) || 0;
  }

  const pagoProfesional = calcularPagoProfesional(valorBase, tarifa);
  const utilidad = valorBase - pagoProfesional;

  return {
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
    where('confirmado','==', true)
  );

  const snap = await getDocs(qy);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};
    if(isEstadoAnulada(x.estado)) return;

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

    // Si viene marcado explícitamente como no_aplica, no entra a liquidación
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
   Build liquidaciones
========================= */
function buildLiquidaciones(){
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

    const procedimientoLabel =
      toUpperSafe(procDoc?.nombre || procCandidate.rawName || '') || '(Sin procedimiento)';
    const procedimientoId =
      cleanReminder(procDoc?.codigo || procDoc?.id || procCandidate.paCode || procCandidate.rawId || '');

    const origen = cleanReminder(x.origen || x.archivo || pickRaw(raw,'Origen') || pickRaw(raw,'Archivo') || '');
    const categoria = cleanReminder(procDoc?.categoria || x.categoria || '');

    const alertas = [];
    const pendientes = [];

    if(!profDoc) alertas.push('Profesional no existe en nómina (catálogo)');
    if(!procDoc) alertas.push('Procedimiento ambulatorio no existe / no mapeado');
    if(!fecha) pendientes.push('Fecha vacía');
    if(!pacienteNombre) pendientes.push('Paciente vacío');

    let valorBase = 0;
    let pagoProfesional = 0;
    let utilidad = 0;
    let comisionPct = 0;
    let tarifaModoValor = 'fijo';
    let tarifaColumnaOrigen = '';

    if(procDoc){
      const calc = calcularLineaAmbulatoria(x, procDoc);

      valorBase = Number(calc.valorBase || 0) || 0;
      pagoProfesional = Number(calc.pagoProfesional || 0) || 0;
      utilidad = Number(calc.utilidad || 0) || 0;
      comisionPct = Number(calc.tarifa?.comisionPct || 0) || 0;
      tarifaModoValor = calc.tarifa?.modoValor || 'fijo';
      tarifaColumnaOrigen = calc.tarifa?.columnaOrigen || '';

      if(tarifaModoValor === 'archivo' && !tarifaColumnaOrigen){
        pendientes.push('Tarifa archivo sin columna origen');
      }

      if(tarifaModoValor === 'archivo' && tarifaColumnaOrigen && valorBase <= 0){
        pendientes.push(`No se pudo leer valor desde columna "${tarifaColumnaOrigen}"`);
      }

      if(tarifaModoValor === 'fijo' && valorBase <= 0){
        pendientes.push('Tarifa fija sin valor');
      }

      if(pagoProfesional <= 0){
        pendientes.push('Pago profesional calculado en 0');
      }
    }

    const observacion = [
      ...(alertas.length ? [`ALERTA: ${alertas.join(' · ')}`] : []),
      ...(pendientes.length ? [`PENDIENTE: ${pendientes.join(' · ')}`] : [])
    ].join(' | ');

    lines.push({
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

      procedimientoId,
      procedimientoNombre: procedimientoLabel,
      procedimientoExists: !!procDoc,

      tarifaModoValor,
      tarifaColumnaOrigen,
      comisionPct,

      valorBase,
      pagoProfesional,
      utilidad,

      isAlerta: alertas.length > 0,
      isPendiente: pendientes.length > 0,
      alerts: alertas,
      pendings: pendientes,
      observacion,

      info: {
        raw,
        appEstado: x?.aplicacion?.estado || '',
        confirmado: !!x?.confirmado
      }
    });
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

        casos: 0,
        totalBruto: 0,
        totalPagoProfesional: 0,
        totalUtilidad: 0,

        alertasCount: 0,
        pendientesCount: 0,

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

    agg.lines.push(ln);
  }

  const resumen = [...map.values()].map(x=>{
    let status = 'ok';
    if(x.alertasCount > 0) status = 'alerta';
    else if(x.pendientesCount > 0) status = 'pendiente';

    return { ...x, status };
  });

  const prio = (st)=> st === 'alerta' ? 0 : (st === 'pendiente' ? 1 : 2);
  resumen.sort((a,b)=>{
    const pa = prio(a.status);
    const pb = prio(b.status);
    if(pa !== pb) return pa - pb;
    return (b.totalPagoProfesional||0) - (a.totalPagoProfesional||0);
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
    ...agg.lines.map(l=> [
      l.origen,
      l.categoria,
      l.procedimientoNombre,
      l.procedimientoId,
      l.pacienteNombre,
      l.tarifaModoValor,
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
      </td>
      <td class="mono">
        ${escapeHtml(rutTitular)}
        ${rutEmpresaSub}
      </td>
      <td>${escapeHtml((agg.tipoPersona || '—').toUpperCase())}</td>
      <td class="mono">${agg.casos}</td>
      <td><b>${clp(agg.totalPagoProfesional || 0)}</b></td>
      <td>${statusPill}</td>
      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Descargar PDF liquidación" aria-label="PDF">📄</button>
          <button class="iconBtn" type="button" title="Ver detalle" aria-label="Detalle">🔎</button>
          <button class="iconBtn" type="button" title="Exportar (profesional)" aria-label="ExportProf">⬇️</button>
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

  $('modalPillTotal').textContent = `PAGO PROF.: ${clp(agg.totalPagoProfesional || 0)}`;
  $('modalPillPendientes').textContent =
    agg.alertasCount > 0
      ? `Alertas: ${agg.alertasCount} · Pendientes: ${agg.pendientesCount}`
      : `Pendientes: ${agg.pendientesCount}`;

  const tb = $('modalTbody');
  tb.innerHTML = '';

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
      <td>
        ${escapeHtml(l.pacienteNombre || '')}
        <div class="mini muted mono">${escapeHtml(l.pacienteRut || '')}</div>
      </td>
      <td>
        ${escapeHtml((l.origen || '—').toUpperCase())}
        <div class="mini muted">${escapeHtml(l.categoria || '')}</div>
      </td>
      <td><b>${clp(l.valorBase || 0)}</b></td>
      <td><b>${clp(l.pagoProfesional || 0)}</b></td>
      <td>${st}</td>
      <td class="mini">${escapeHtml(obs || '')}</td>
    `;
    tb.appendChild(tr);
  }
}

function closeDetalle(){
  $('modalBackdrop').style.display = 'none';
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
    'casos',
    'totalBruto',
    'totalPagoProfesional',
    'totalUtilidad',
    'alertas','pendientes'
  ];

  const items = state.liquidResumen.map(a=>({
    mes: monthNameEs(state.mesNum),
    ano: String(state.ano),

    profesional: a.nombre || '',
    rut: a.rut || '',

    empresa: a.empresaNombre || '',
    rutEmpresa: a.empresaRut || '',

    tipoPersona: a.tipoPersona || '',
    casos: String(a.casos || 0),
    totalBruto: String(a.totalBruto || 0),
    totalPagoProfesional: String(a.totalPagoProfesional || 0),
    totalUtilidad: String(a.totalUtilidad || 0),

    alertas: String(a.alertasCount || 0),
    pendientes: String(a.pendientesCount || 0)
  }));

  const csv = toCSV(headers, items);
  download(`liquidaciones_ambulatorias_resumen_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV resumen exportado');
}

function exportDetalleCSV(){
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',

    'fecha','hora',
    'procedimiento','procedimientoId','procedimientoExiste',
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

        fecha: l.fecha || '',
        hora: l.hora || '',

        procedimiento: l.procedimientoNombre || '',
        procedimientoId: l.procedimientoId || '',
        procedimientoExiste: l.procedimientoExists ? 'SI' : 'NO',

        paciente: l.pacienteNombre || '',
        rutPaciente: l.pacienteRut || '',

        origen: l.origen || '',
        categoria: l.categoria || '',

        tarifaModoValor: l.tarifaModoValor || '',
        tarifaColumnaOrigen: l.tarifaColumnaOrigen || '',
        comisionPct: String(l.comisionPct || 0),

        valorBase: String(l.valorBase || 0),
        pagoProfesional: String(l.pagoProfesional || 0),
        utilidad: String(l.utilidad || 0),

        estadoLinea: pickDisplayEstadoLinea(l),
        observacion: l.observacion || '',
        prodId: l.prodId || ''
      });
    }
  }

  const csv = toCSV(headers, items);
  download(`liquidaciones_ambulatorias_detalle_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV detalle exportado');
}

function exportDetalleProfesional(agg){
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',

    'fecha','hora',
    'procedimiento','procedimientoId','procedimientoExiste',
    'paciente','rutPaciente',
    'origen','categoria',

    'tarifaModoValor','tarifaColumnaOrigen','comisionPct',
    'valorBase','pagoProfesional','utilidad',

    'estadoLinea',
    'observacion',
    'prodId'
  ];

  const items = (agg.lines || []).map(l=>({
    mes: monthNameEs(state.mesNum),
    ano: String(state.ano),

    profesional: agg.nombre || '',
    rut: agg.rut || '',
    empresa: agg.empresaNombre || '',
    rutEmpresa: agg.empresaRut || '',
    tipoPersona: agg.tipoPersona || '',

    fecha: l.fecha || '',
    hora: l.hora || '',

    procedimiento: l.procedimientoNombre || '',
    procedimientoId: l.procedimientoId || '',
    procedimientoExiste: l.procedimientoExists ? 'SI' : 'NO',

    paciente: l.pacienteNombre || '',
    rutPaciente: l.pacienteRut || '',

    origen: l.origen || '',
    categoria: l.categoria || '',

    tarifaModoValor: l.tarifaModoValor || '',
    tarifaColumnaOrigen: l.tarifaColumnaOrigen || '',
    comisionPct: String(l.comisionPct || 0),

    valorBase: String(l.valorBase || 0),
    pagoProfesional: String(l.pagoProfesional || 0),
    utilidad: String(l.utilidad || 0),

    estadoLinea: pickDisplayEstadoLinea(l),
    observacion: l.observacion || '',
    prodId: l.prodId || ''
  }));

  const csv = toCSV(headers, items);
  const safeName = safeFileName(agg.nombre || 'profesional');
  download(`liquidacion_ambulatoria_${safeName}_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV profesional exportado');
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

async function generarPDFLiquidacionProfesional(agg){
  const pdfDoc = await PDFDocument.create();

  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const W = 595.28;
  const H = 841.89;

  const rgb255 = (r,g,b)=> rgb(r/255, g/255, b/255);

  const RENNAT_BLUE  = rgb255(0, 39, 56);
  const RENNAT_GREEN = rgb255(31, 140, 115);
  const BORDER_SOFT  = rgb(0.82, 0.84, 0.86);
  const TEXT_MAIN    = rgb(0.08, 0.09, 0.11);
  const TEXT_MUTED   = rgb(0.45, 0.48, 0.52);
  const RENNAT_BLUE_SOFT  = rgb(0.18, 0.36, 0.45);
  const RENNAT_GREEN_SOFT = rgb(0.20, 0.50, 0.42);

  const M = 36;

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

  // =========================
  // Página 1
  // =========================
  const page1 = pdfDoc.addPage([W, H]);

  const boxW = W - 2*M;
  const barH = 28;
  const barX = M;

  let logoBottomY = H - M;
  const logoBytes = await fetchAsArrayBuffer(PDF_ASSET_LOGO_URL);

  if (logoBytes) {
    try {
      const urlLower = String(PDF_ASSET_LOGO_URL || '').toLowerCase();
      const isJpg = urlLower.endsWith('.jpg') || urlLower.endsWith('.jpeg');

      const logo = isJpg
        ? await pdfDoc.embedJpg(logoBytes)
        : await pdfDoc.embedPng(logoBytes);

      const logoW = 120;
      const logoH = (logo.height / logo.width) * logoW;

      const logoX = M;
      const logoY = H - M - logoH;

      page1.drawImage(logo, {
        x: logoX,
        y: logoY,
        width: logoW,
        height: logoH
      });

      logoBottomY = logoY;
    } catch (e) {
      console.warn('No se pudo embebeder logo:', e);
    }
  }

  const gapLogoTitulo = barH;
  const barTop = logoBottomY - gapLogoTitulo;
  const barW = W - 2*M;

  drawBox(page1, barX, barTop, barW, barH, RENNAT_BLUE, RENNAT_BLUE, 1);

  const mesTxt = `${monthNameEs(state.mesNum)} ${state.ano}`;
  const title = `LIQUIDACIÓN AMBULATORIA ${String(mesTxt).toUpperCase()}`;
  const titleSize = 13;
  const titleW = measure(title, titleSize, true);
  drawText(page1, title, barX + (barW - titleW)/2, barTop - 19, titleSize, true, rgb(1,1,1));

  let y = barTop - barH - 14;

  // Datos profesional
  const profNombre = (agg?.nombre || '').toString().trim();
  const profRut = (agg?.rut || '').toString().trim();

  const tipoPersona = (agg?.tipoPersona || '').toString().toLowerCase().trim();
  const esJuridica = (tipoPersona === 'juridica');

  const empresaNombre = (agg?.empresaNombre || '').toString().trim();
  const empresaRut = (agg?.empresaRut || '').toString().trim();

  const dataRows = [];
  dataRows.push(['PROFESIONAL', String(profNombre || '—').toUpperCase()]);
  dataRows.push(['RUT', String(profRut || '—').toUpperCase()]);
  if(esJuridica){
    dataRows.push(['EMPRESA', String(empresaNombre || '—').toUpperCase()]);
    dataRows.push(['RUT EMPRESA', String(empresaRut || '—').toUpperCase()]);
  }
  dataRows.push(['TIPO DE PERSONA', String(esJuridica ? 'JURIDICA' : 'NATURAL').toUpperCase()]);

  const rowH = 22;
  const dataH = dataRows.length * rowH;
  const c1 = Math.round(boxW * 0.45);
  const c2 = boxW - c1;

  drawBox(page1, M, y, boxW, dataH, rgb(1,1,1), BORDER_SOFT, 1);
  drawVLine(page1, M + c1, y, dataH, 1, BORDER_SOFT);

  for(let r=0; r<dataRows.length; r++){
    const yRowTop = y - r*rowH;
    const row = Array.isArray(dataRows[r]) ? dataRows[r] : ['',''];
    const label = row[0] ?? '';
    const value = row[1] ?? '';
    const isProfesionalRow = String(label).toLowerCase().trim() === 'profesional';

    if(isProfesionalRow){
      page1.drawRectangle({
        x: M,
        y: (yRowTop - rowH),
        width: boxW,
        height: rowH,
        color: RENNAT_GREEN
      });
      drawVLine(page1, M + c1, yRowTop, rowH, 1, BORDER_SOFT);
    }

    if(r > 0) drawHLine(page1, M, yRowTop, boxW, 1, BORDER_SOFT);

    const labelColor = isProfesionalRow ? rgb(1,1,1) : TEXT_MAIN;
    const valueColor = isProfesionalRow ? rgb(1,1,1) : TEXT_MAIN;

    drawCellText(page1, label, M, yRowTop, rowH, 10, false, labelColor, 8);
    drawCellText(page1, wrapClip(value, 50), M + c1, yRowTop, rowH, 10, true, valueColor, 8);
  }

  y = y - dataH - 16;

  // Resumen por procedimiento
  const agrupadoProc = new Map();
  for(const l of (agg.lines || [])){
    const key = `${l.procedimientoId || ''}|${l.procedimientoNombre || ''}`;
    if(!agrupadoProc.has(key)){
      agrupadoProc.set(key, {
        procedimientoId: l.procedimientoId || '',
        procedimientoNombre: l.procedimientoNombre || '—',
        casos: 0,
        bruto: 0,
        pago: 0,
        utilidad: 0
      });
    }

    const o = agrupadoProc.get(key);
    o.casos += 1;
    o.bruto += Number(l.valorBase || 0) || 0;
    o.pago += Number(l.pagoProfesional || 0) || 0;
    o.utilidad += Number(l.utilidad || 0) || 0;
  }

  const resumenProc = [...agrupadoProc.values()].sort((a,b)=> (b.pago||0) - (a.pago||0));

  const headH = 24;
  const resRowH = 20;
  const colProc = 260;
  const colCasos = 70;
  const colBruto = 90;
  const colPago = 90;
  const colUtil = boxW - colProc - colCasos - colBruto - colPago;

  const resH = headH + Math.min(resumenProc.length, 14) * resRowH;
  drawBox(page1, M, y, boxW, resH, rgb(1,1,1), BORDER_SOFT, 1);
  drawBox(page1, M, y, boxW, headH, RENNAT_BLUE, RENNAT_BLUE, 1);

  drawVLine(page1, M + colProc, y, resH, 1, BORDER_SOFT);
  drawVLine(page1, M + colProc + colCasos, y, resH, 1, BORDER_SOFT);
  drawVLine(page1, M + colProc + colCasos + colBruto, y, resH, 1, BORDER_SOFT);
  drawVLine(page1, M + colProc + colCasos + colBruto + colPago, y, resH, 1, BORDER_SOFT);

  drawCellText(page1, 'PROCEDIMIENTO', M, y, headH, 10, true, rgb(1,1,1), 8);
  drawCellTextRight(page1, 'CASOS', M + colProc, y, colCasos, headH, 10, true, rgb(1,1,1), 8);
  drawCellTextRight(page1, 'BRUTO', M + colProc + colCasos, y, colBruto, headH, 10, true, rgb(1,1,1), 8);
  drawCellTextRight(page1, 'PAGO PROF.', M + colProc + colCasos + colBruto, y, colPago, headH, 10, true, rgb(1,1,1), 8);
  drawCellTextRight(page1, 'UTILIDAD', M + colProc + colCasos + colBruto + colPago, y, colUtil, headH, 10, true, rgb(1,1,1), 8);

  const maxRowsResumen = Math.min(resumenProc.length, 14);
  for(let i=0; i<maxRowsResumen; i++){
    const r = resumenProc[i];
    const yTop = y - headH - i*resRowH;
    drawHLine(page1, M, yTop, boxW, 1, BORDER_SOFT);

    drawCellText(page1, wrapClip(`${r.procedimientoId} · ${r.procedimientoNombre}`, 40), M, yTop, resRowH, 9, false, TEXT_MAIN, 8);
    drawCellTextRight(page1, String(r.casos), M + colProc, yTop, colCasos, resRowH, 9, true, TEXT_MAIN, 8);
    drawCellTextRight(page1, money(r.bruto), M + colProc + colCasos, yTop, colBruto, resRowH, 9, true, TEXT_MAIN, 8);
    drawCellTextRight(page1, money(r.pago), M + colProc + colCasos + colBruto, yTop, colPago, resRowH, 9, true, TEXT_MAIN, 8);
    drawCellTextRight(page1, money(r.utilidad), M + colProc + colCasos + colBruto + colPago, yTop, colUtil, resRowH, 9, true, TEXT_MAIN, 8);
  }

  y = y - resH - 14;

  // Totales
  const totalBarH = 28;
  drawBox(page1, M, y, boxW, totalBarH, RENNAT_GREEN, RENNAT_GREEN, 1);
  drawCellText(page1, 'TOTAL BRUTO', M, y, totalBarH, 12, true, rgb(1,1,1), 10);
  drawCellTextRight(page1, money(agg.totalBruto || 0), M + (boxW - 200), y, 200, totalBarH, 13, true, rgb(1,1,1), 10);
  y = y - totalBarH - 10;

  drawBox(page1, M, y, boxW, totalBarH, RENNAT_GREEN, RENNAT_GREEN, 1);
  drawCellText(page1, 'TOTAL A PAGAR PROFESIONAL', M, y, totalBarH, 12, true, rgb(1,1,1), 10);
  drawCellTextRight(page1, money(agg.totalPagoProfesional || 0), M + (boxW - 200), y, 200, totalBarH, 13, true, rgb(1,1,1), 10);
  y = y - totalBarH - 10;

  drawBox(page1, M, y, boxW, totalBarH, RENNAT_GREEN, RENNAT_GREEN, 1);
  drawCellText(page1, 'UTILIDAD CLÍNICA', M, y, totalBarH, 12, true, rgb(1,1,1), 10);
  drawCellTextRight(page1, money(agg.totalUtilidad || 0), M + (boxW - 200), y, 200, totalBarH, 13, true, rgb(1,1,1), 10);

  // =========================
  // Página 2 detalle
  // =========================
  const W2 = 841.89;
  const H2 = 595.28;
  const page2 = pdfDoc.addPage([W2, H2]);

  let y2 = H2 - M;
  const barX2 = M;
  const barW2 = W2 - 2*M;
  const T_DETALLE = 'Detalle de Prestaciones Ambulatorias';

  drawBox(page2, barX2, y2, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
  drawText(page2, T_DETALLE, barX2 + (barW2 - measure(T_DETALLE, 13, true))/2, y2 - 19, 13, true, rgb(1,1,1));
  y2 -= (barH + 12);

  drawText(page2, `${profNombre || '—'} · ${mesTxt}`, M, y2, 10, false, TEXT_MUTED);
  y2 -= 12;

  const detX = M;
  const detW = W2 - 2*M;
  const detHeadH = 24;
  const detRowH = 18;

  let detCols = [
    { key:'n',    label:'#',             w: 40  },
    { key:'fecha',label:'FECHA',         w: 110 },
    { key:'proc', label:'PROCEDIMIENTO', w: 220 },
    { key:'pac',  label:'PACIENTE',      w: 190 },
    { key:'org',  label:'ORIGEN',        w: 90  },
    { key:'vb',   label:'BRUTO',         w: 80  },
    { key:'pp',   label:'PAGO PROF.',    w: 90  },
    { key:'ut',   label:'UTILIDAD',      w: 90  }
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

    drawCellText(page, wrapClip(`${row.procedimientoId || ''} · ${row.procedimientoNombre || ''}`, 34), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[2].w;

    drawCellText(page, wrapClip(row.pacienteNombre || '', 26), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[3].w;

    drawCellText(page, wrapClip((row.origen || '').toUpperCase(), 12), xPos, topY, detRowH, 9, false, TEXT_MUTED, 8);
    xPos += detCols[4].w;

    drawCellTextRight(page, money(row.valorBase || 0), xPos, topY, detCols[5].w, detRowH, 9, true, TEXT_MAIN, 8);
    xPos += detCols[5].w;

    drawCellTextRight(page, money(row.pagoProfesional || 0), xPos, topY, detCols[6].w, detRowH, 9, true, TEXT_MAIN, 8);
    xPos += detCols[6].w;

    drawCellTextRight(page, money(row.utilidad || 0), xPos, topY, detCols[7].w, detRowH, 9, true, RENNAT_GREEN_SOFT, 8);
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
      drawText(currentPage, T_DETALLE, barX2 + (barW2 - measure(T_DETALLE, 13, true))/2, cursorTopY - 19, 13, true, rgb(1,1,1));
      cursorTopY -= (barH + 12);

      drawText(currentPage, `${profNombre || '—'} · ${mesTxt}`, M, cursorTopY, 10, false, TEXT_MUTED);
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
      drawText(currentPage, T_DETALLE, barX2 + (barW2 - measure(T_DETALLE, 13, true))/2, cursorTopY - 19, 13, true, rgb(1,1,1));
      cursorTopY -= (barH + 12);

      drawText(currentPage, `${profNombre || '—'} · ${mesTxt}`, M, cursorTopY, 10, false, TEXT_MUTED);
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

    await loadProduccionMes();
    buildLiquidaciones();
    paint();

  }catch(err){
    console.error(err);
    toast('Error recalculando (ver consola)');
  }finally{
    $('btnRecalcular').disabled = false;
  }
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

    $('q').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

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
        lines: state.lastDetailExportLines
      };

      exportDetalleProfesional(agg);
    });

    await Promise.all([
      loadProfesionales(),
      loadProcedimientosAmbulatorios()
    ]);

    await recalc();
  }
});
