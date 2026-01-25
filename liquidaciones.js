// liquidaciones.js — COMPLETO (AJUSTADO A TU FIRESTORE REAL)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';
await loadSidebar({ active: 'liquidaciones' });

import {
  collection, collectionGroup, getDocs, query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

import {
  PDFDocument,
  StandardFonts,
  rgb
} from 'https://cdn.skypack.dev/pdf-lib@1.17.1';


/* =========================
   AJUSTE ÚNICO (SI CAMBIAS NOMBRES)
========================= */
// En tu esquema real, la producción está en: produccion/{ano}/meses/{mes}/pacientes/{rut}/items/{...}
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
function tipoPacienteNorm(v){
  const x = normalize(v);
  if(x.includes('fona')) return 'fonasa';
  if(x.includes('isap')) return 'isapre';
  if(x.includes('part')) return 'particular';
  return x || '';
}
function pillHtml(kind, text){
  // kind: ok | warn | bad
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

/* =========================
   PDF Liquidación (1 por profesional)
   - Documento para el profesional (no técnico)
   - Siempre: nombre + rut personal
   - Si jurídica: razón social + rut empresa en gris
========================= */

// Ajusta esto si quieres un logo (opcional). Si no existe, simplemente no lo dibuja.
const PDF_ASSET_LOGO_URL = 'logoCR.png'; // pon tu ruta real o déjalo así si lo subirás

async function fetchAsArrayBuffer(url){
  try{
    const res = await fetch(url);
    if(!res.ok) throw new Error('HTTP '+res.status);
    return await res.arrayBuffer();
  }catch(e){
    return null;
  }
}

function safeFileName(s){
  return normalize(s || 'profesional')
    .replace(/[^a-z0-9]+/g,'-')
    .replace(/^-+|-+$/g,'')
    .slice(0,60) || 'profesional';
}

function buildLineaEstado(l){
  if(l.isAlerta) return 'ALERTA';
  if(l.isPendiente) return 'PENDIENTE';
  return 'OK';
}

async function generarPDFLiquidacionProfesional(agg){
  // Documento nuevo (sin template) para que sea simple y claro
  const pdfDoc = await PDFDocument.create();

  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  // Página A4
  const page = pdfDoc.addPage([595.28, 841.89]);
  const { width, height } = page.getSize();

  // Colores suaves
  const teal = rgb(0.0, 0.65, 0.60);
  const gray = rgb(0.42, 0.45, 0.50);
  const ink  = rgb(0.07, 0.09, 0.13);
  const soft = rgb(0.95, 0.98, 0.99);

  // Margenes
  const M = 26;
  let y = height - M;

  // Header bar
  page.drawRectangle({ x:M, y:y-48, width:width-2*M, height:48, color:soft, borderColor:rgb(0.86,0.90,0.93), borderWidth:1 });
  page.drawRectangle({ x:M, y:y-48, width:6, height:48, color:teal });

  // Logo (opcional) — arriba a la derecha, con tamaño consistente
  const logoBytes = await fetchAsArrayBuffer(PDF_ASSET_LOGO_URL);
  if (logoBytes) {
    try {
      const logo = await pdfDoc.embedPng(logoBytes);

      // Tamaño recomendado (ajústalo a gusto)
      const logoW = 110;
      const logoH = (logo.height / logo.width) * logoW;

      // Ubicación: esquina superior derecha, respetando margen
      const logoX = width - M - logoW;
      const logoY = (y - 10) - logoH; // y actual es la parte "alta" del header

      page.drawImage(logo, { x: logoX, y: logoY, width: logoW, height: logoH });

      // Baja el cursor (y) para que el título/mes no se cruce con el logo
      // (solo si el logo ocupa más altura que tu header)
      // Ajuste suave: si quieres más aire, cambia 10 por 16.
      // OJO: NO cambiamos "y" globalmente aquí, solo el "y" local del flujo:
      // (como tú bajas y después con y -= 68, esto evita choques visuales)
    } catch (_e) {
      // si no es png o falla, no lo dibuja
    }
  }


  // Título
  page.drawText('LIQUIDACIÓN DE HONORARIOS', { x:M+14, y:y-30, size:14, font:fontBold, color:ink });
  page.drawText(`Mes: ${monthNameEs(state.mesNum)}  ${state.ano}`, { x:M+14, y:y-45, size:10, font, color:gray });

  y -= 68;

  // Bloque Datos Profesional
  const titularNombre = (agg?.nombre || '').toString();
  const titularRut = (agg?.rut || '').toString();
  const esJuridica = (agg?.tipoPersona || '').toLowerCase() === 'juridica';

  const empresaNombre = (agg?.empresaNombre || '').toString();
  const empresaRut = (agg?.empresaRut || '').toString();

  page.drawText('DATOS DEL PROFESIONAL', { x:M, y:y, size:11, font:fontBold, color:ink });
  y -= 10;

  page.drawRectangle({ x:M, y:y-62, width:width-2*M, height:62, color:rgb(1,1,1), borderColor:rgb(0.86,0.90,0.93), borderWidth:1 });

  page.drawText(`Nombre: ${titularNombre || '—'}`, { x:M+12, y:y-20, size:10.5, font:fontBold, color:ink });
  page.drawText(`RUT: ${titularRut || '—'}`, { x:M+12, y:y-38, size:10.5, font, color:ink });

  if(esJuridica && (empresaNombre || empresaRut)){
    // Subtítulo gris (empresa)
    page.drawText(`Empresa: ${empresaNombre || '—'}`, { x:M+12, y:y-54, size:9.5, font, color:gray });
    if(empresaRut){
      page.drawText(`RUT Empresa: ${empresaRut}`, { x:M+290, y:y-54, size:9.5, font, color:gray });
    }
  }

  y -= 82;

  // IMPORTANTE (para evitar confusión): datos de Clínica Rennat deben ir CLARAMENTE como emisor
  page.drawText('DATOS DEL EMISOR (CLÍNICA RENNAT)', { x:M, y:y, size:10.5, font:fontBold, color:ink });
  y -= 10;

  page.drawRectangle({ x:M, y:y-46, width:width-2*M, height:46, color:rgb(1,1,1), borderColor:rgb(0.86,0.90,0.93), borderWidth:1 });
  page.drawText('Estos datos corresponden al emisor del servicio (Clínica Rennat).', { x:M+12, y:y-18, size:9.2, font, color:gray });
  page.drawText('No corresponden a los datos del profesional.', { x:M+12, y:y-32, size:9.2, font, color:gray });

  y -= 66;

  // Resumen
  const total = Number(agg?.total || 0) || 0;

  page.drawRectangle({ x:M, y:y-40, width:width-2*M, height:40, color:soft, borderColor:rgb(0.86,0.90,0.93), borderWidth:1 });
  page.drawText('TOTAL A PAGAR', { x:M+12, y:y-26, size:11, font:fontBold, color:ink });
  page.drawText(clp(total), { x:width-M-12-fontBold.widthOfTextAtSize(clp(total), 14), y:y-28, size:14, font:fontBold, color:ink });

  y -= 62;

  // Alertas/Pendientes (si existen)
  const alertasCount = Number(agg?.alertasCount || 0) || 0;
  const pendientesCount = Number(agg?.pendientesCount || 0) || 0;

  if(alertasCount > 0 || pendientesCount > 0){
    const msg = [
      alertasCount>0 ? `ALERTAS: ${alertasCount}` : null,
      pendientesCount>0 ? `PENDIENTES: ${pendientesCount}` : null
    ].filter(Boolean).join(' · ');

    page.drawRectangle({ x:M, y:y-28, width:width-2*M, height:28, color:rgb(1, 0.93, 0.94), borderColor:rgb(0.98,0.80,0.82), borderWidth:1 });
    page.drawText(`ATENCIÓN: ${msg}`, { x:M+12, y:y-18, size:10, font:fontBold, color:rgb(0.62,0.07,0.22) });
    y -= 44;
  }

  // Tabla Detalle (simple y legible)
  page.drawText('DETALLE DE PROCEDIMIENTOS', { x:M, y:y, size:11, font:fontBold, color:ink });
  y -= 10;

  const colX = {
    fecha:  M,
    clinica:M + 86,
    tipo:   M + 220,
    proc:   M + 285,
    rol:    M + 490,
    montoR: (width - M)  // borde derecho real para alinear $ a la derecha
  };

  // Header tabla
  page.drawRectangle({ x:M, y:y-18, width:width-2*M, height:18, color:soft, borderColor:rgb(0.86,0.90,0.93), borderWidth:1 });
  const HFS = 8.0; // header font size
  
  page.drawText('Fecha', { x:colX.fecha+6, y:y-13, size:HFS, font:fontBold, color:gray });
  page.drawText('Clínica', { x:colX.clinica+6, y:y-13, size:HFS, font:fontBold, color:gray });
  page.drawText('Tipo', { x:colX.tipo+6, y:y-13, size:HFS, font:fontBold, color:gray });
  page.drawText('Procedimiento / Paciente', { x:colX.proc+6, y:y-13, size:HFS, font:fontBold, color:gray });
  page.drawText('Rol', { x:colX.rol+6, y:y-13, size:HFS, font:fontBold, color:gray });
  
  // $ alineado al extremo derecho (no se encime)
  page.drawText('$', { 
    x: colX.montoR - 12, 
    y: y-13, 
    size: HFS, 
    font: fontBold, 
    color: gray 
  });


  y -= 22;

  // Filas
  const lines = [...(agg.lines || [])].sort((a,b)=>{
    const fa = normalize(a.fecha);
    const fb = normalize(b.fecha);
    if(fa !== fb) return fa.localeCompare(fb);
    return normalize(a.roleNombre).localeCompare(normalize(b.roleNombre));
  });

  const rowH = 18;
  const RFS = 7.6;     // tamaño texto filas (más chico)
  const RFS_B = 7.6;   // monto en negrita

  for (const l of lines) {
    // Si se acaba la página, por ahora cortamos (después podemos paginar)
    if (y - rowH < M + 20) break;

    const fechaTxt = `${cleanReminder(l.fecha)} ${cleanReminder(l.hora)}`.trim();
    const clinTxt  = cleanReminder(l.clinicaNombre || '');
    const tipoTxt  = cleanReminder(l.tipoPaciente || '');
    const procTxt  = cleanReminder(l.procedimientoNombre || '');
    const pacTxt   = cleanReminder(l.pacienteNombre || '');
    const rolTxt   = cleanReminder(l.roleNombre || '');
    const montoTxt = clp(l.monto || 0);

    const estado = buildLineaEstado(l);

    // fila (fondo + borde suave)
    page.drawRectangle({
      x: M,
      y: y - rowH,
      width: width - 2 * M,
      height: rowH,
      color: rgb(1, 1, 1),
      borderColor: rgb(0.93, 0.95, 0.97),
      borderWidth: 1
    });

    // textos
    page.drawText(fechaTxt.slice(0, 18), { x: colX.fecha + 6,   y: y - 13, size: RFS, font, color: ink });
    page.drawText(clinTxt.slice(0, 22),  { x: colX.clinica + 6, y: y - 13, size: RFS, font, color: ink });
    page.drawText(tipoTxt.slice(0, 10),  { x: colX.tipo + 6,    y: y - 13, size: RFS, font, color: ink });

    const procPac = `${procTxt} — ${pacTxt}`.trim();
    page.drawText(procPac.slice(0, 40),  { x: colX.proc + 6,    y: y - 13, size: 7.4, font, color: ink });

    page.drawText(rolTxt.slice(0, 16),   { x: colX.rol + 6,     y: y - 13, size: RFS, font, color: ink });

    // monto alineado al borde derecho real
    page.drawText(montoTxt, {
      x: colX.montoR - 6 - fontBold.widthOfTextAtSize(montoTxt, RFS_B),
      y: y - 13,
      size: RFS_B,
      font: fontBold,
      color: ink
    });

    // badge mini (derecha) si no es OK
    if (estado !== 'OK') {
      const badge = (estado === 'ALERTA') ? 'ALERTA' : 'PEND';
      const bcol  = (estado === 'ALERTA') ? rgb(0.62, 0.07, 0.22) : rgb(0.60, 0.32, 0.05);
      page.drawText(badge, {
        x: width - M - 44,
        y: y - 13,
        size: 7.4,
        font: fontBold,
        color: bcol
      });
    }

    y -= rowH;
  }

  // ✅ cerrar PDF y devolver bytes
  const bytes = await pdfDoc.save();
  return bytes;
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


/* =========================
   Mapping roles (tu item trae map profesionales / profesionalesId)
========================= */
const ROLE_SPEC = [
  { roleId:'r_cirujano',    label:'CIRUJANO',    key:'cirujano',    idKey:'cirujanoId',    csvField:'Cirujano' },
  { roleId:'r_anestesista', label:'ANESTESISTA', key:'anestesista', idKey:'anestesistaId', csvField:'Anestesista' },
  { roleId:'r_ayudante_1',  label:'AYUDANTE 1',  key:'ayudante1',   idKey:'ayudante1Id',   csvField:'Ayudante 1' },
  { roleId:'r_ayudante_2',  label:'AYUDANTE 2',  key:'ayudante2',   idKey:'ayudante2Id',   csvField:'Ayudante 2' },
  { roleId:'r_arsenalera',  label:'ARSENALERA',  key:'arsenalera',  idKey:'arsenaleraId',  csvField:'Arsenalera' },
];

/* =========================
   State
========================= */
const state = {
  user: null,

  mesNum: null,
  ano: null,
  q: '',

  rolesMap: new Map(),            // roleId -> nombre
  clinicasById: new Map(),        // C001 -> NOMBRE
  clinicasByName: new Map(),      // normalize(nombre) -> C001

  // Catálogo profesionales (TU esquema real):
  // docId = rutId (sin guiones, sin ceros) normalmente
  // campos: nombreProfesional, razonSocial, rut, rutEmpresa, tipoPersona, estado...
  profesionalesByName: new Map(), // normalize(nombreProfesional) -> profDoc
  profesionalesById: new Map(),   // rutId string -> profDoc

  procedimientosByName: new Map(), // normalize(nombre) -> procDoc
  procedimientosById: new Map(),   // id -> procDoc

  prodRows: [],              // docs items del mes
  liquidResumen: [],
  lastDetailExportLines: []
};

/* =========================
   Firestore refs
========================= */
const colRoles = collection(db, 'roles');
const colClinicas = collection(db, 'clinicas');
const colProfesionales = collection(db, 'profesionales');
const colProcedimientos = collection(db, 'procedimientos');

/* =========================
   Load catalogs
========================= */
async function loadRoles(){
  const snap = await getDocs(colRoles);
  const map = new Map();
  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = toUpperSafe(cleanReminder(x.nombre) || d.id);
    map.set(d.id, nombre);
  });
  state.rolesMap = map;
}

async function loadClinicas(){
  const snap = await getDocs(colClinicas);
  const byId = new Map();
  const byName = new Map();

  snap.forEach(d=>{
    const x = d.data() || {};
    const id = cleanReminder(x.id) || d.id;
    const nombre = toUpperSafe(cleanReminder(x.nombre) || id);
    if(!id) return;
    byId.set(id, nombre);
    byName.set(normalize(nombre), id);
  });

  state.clinicasById = byId;
  state.clinicasByName = byName;
}

async function loadProfesionales(){
  const snap = await getDocs(colProfesionales);
  const byName = new Map();
  const byId = new Map();

  snap.forEach(d=>{
    const x = d.data() || {};

    // TU ESQUEMA REAL
    const rutId = cleanReminder(x.rutId) || d.id; // docId suele ser rutId
    const nombreProfesional = cleanReminder(x.nombreProfesional) || '';
    const razonSocial = cleanReminder(x.razonSocial) || '';
    const rutPersonal = cleanReminder(x.rut) || '';
    const rutEmpresa = cleanReminder(x.rutEmpresa) || '';
    const tipoPersona = (cleanReminder(x.tipoPersona) || '').toLowerCase(); // natural | juridica
    const estado = (cleanReminder(x.estado) || 'activo').toLowerCase();

    const doc = {
      id: String(rutId || d.id),
      rutId: String(rutId || d.id),

      // Siempre persona (titular)
      nombreProfesional: toUpperSafe(nombreProfesional || ''),
      rut: rutPersonal,

      // Empresa (si aplica)
      razonSocial: toUpperSafe(razonSocial || ''),
      rutEmpresa: rutEmpresa,

      tipoPersona: tipoPersona || '',
      estado
    };

    byId.set(String(doc.id), doc);
    if(nombreProfesional) byName.set(normalize(nombreProfesional), doc);
  });

  state.profesionalesByName = byName;
  state.profesionalesById = byId;
}

async function loadProcedimientos(){
  const snap = await getDocs(colProcedimientos);
  const byName = new Map();
  const byId = new Map();

  snap.forEach(d=>{
    const x = d.data() || {};
    const id = d.id;
    const nombre = cleanReminder(x.nombre) || '';
    const tipo = (cleanReminder(x.tipo) || '').toLowerCase();
    const tarifas = (x.tarifas && typeof x.tarifas === 'object') ? x.tarifas : null;

    const doc = {
      id,
      codigo: cleanReminder(x.codigo) || id,
      nombre: toUpperSafe(nombre || id),
      tipo,
      tarifas
    };

    byId.set(String(id), doc);
    if(nombre) byName.set(normalize(nombre), doc);
  });

  state.procedimientosByName = byName;
  state.procedimientosById = byId;
}

/* =========================
   Load Producción (collectionGroup items)
========================= */
function monthNameEs(m){
  return ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'][m-1] || '';
}

async function loadProduccionMes(){
  if(!state.mesNum || !state.ano) return;

  const colItemsGroup = collectionGroup(db, PROD_ITEMS_GROUP);

  // Tu item real tiene:
  // - ano (number)
  // - mesNum (number)
  // - confirmado (boolean)
  // - estado (string) "activa" / "anulada" etc.
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

    // Ignorar anuladas
    const est = normalize(x.estado || '');
    if(est === 'anulada' || est === 'anulado' || est === 'cancelada') return;

    out.push({ id: d.id, data: x });
  });

  state.prodRows = out;
}

/* =========================
   Tarifario: procedimientos.tarifas[clinicaId].pacientes[tipo].honorarios[roleId]
========================= */
function getHonorarioFromTarifa(procDoc, clinicaId, tipoPaciente, roleId){
  try{
    const tarifas = procDoc?.tarifas;
    if(!tarifas) return { ok:false, monto:0, reason:'Procedimiento sin tarifario' };

    const clin = tarifas?.[clinicaId];
    if(!clin) return { ok:false, monto:0, reason:`Sin tarifario para clínica ${clinicaId}` };

    const pac = clin?.pacientes?.[tipoPaciente];
    if(!pac) return { ok:false, monto:0, reason:`Sin tarifario para paciente ${tipoPaciente}` };

    const h = pac?.honorarios;
    if(!h || typeof h !== 'object') return { ok:false, monto:0, reason:'Sin honorarios' };

    const monto = Number(h?.[roleId] ?? 0) || 0;
    if(monto <= 0) return { ok:false, monto:0, reason:`Honorario ${roleId} = 0` };

    return { ok:true, monto, reason:'' };
  }catch(e){
    return { ok:false, monto:0, reason:'Error leyendo tarifario' };
  }
}

/* =========================
   Fallback raw (más robusto)
========================= */
function normKeyLoose(s=''){
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g,''); // 🔥 quita espacios, guiones, puntos, etc.
}

function pickRaw(raw, key){
  if(!raw || typeof raw !== 'object') return '';

  // 1) directo exacto
  if(raw[key] !== undefined) return raw[key];

  // 2) match “loose” (ignora espacios/puntuación)
  const nk = normKeyLoose(key);
  for(const k of Object.keys(raw)){
    if(normKeyLoose(k) === nk) return raw[k];
  }
  return '';
}

/* =========================
   Build liquidaciones
========================= */
function buildLiquidaciones(){
  const lines = [];

  for(const row of state.prodRows){
    const x = row.data || {};
    const raw = (x.raw && typeof x.raw === 'object') ? x.raw : {};

    // Fecha/hora
    const fecha = fmtDateISOorDMY(x.fechaISO || pickRaw(raw,'Fecha'));
    const hora = cleanReminder(x.horaHM || pickRaw(raw,'Hora'));

    // Clínica (label siempre con lo que venga, pero alert si no existe en catálogo)
    const clinicaId = cleanReminder(x.clinicaId) || '';
    const clinicaNameRaw = toUpperSafe(cleanReminder(x.clinica || pickRaw(raw,'Clínica')));
    const clinicaLabel = clinicaId
      ? (state.clinicasById.get(clinicaId) || clinicaNameRaw || clinicaId)
      : (clinicaNameRaw || '(Sin clínica)');

    const clinicaExists = !!(clinicaId && state.clinicasById.has(clinicaId));

    // Procedimiento
    const procId = cleanReminder(x.cirugiaId || x.ambulatorioId) || '';
    const cirugiaNameRaw = toUpperSafe(cleanReminder(x.cirugia || pickRaw(raw,'Cirugía')));

    const procDoc =
      (procId && state.procedimientosById.get(String(procId))) ||
      (cirugiaNameRaw && state.procedimientosByName.get(normalize(cirugiaNameRaw))) ||
      null;

    const procLabel = procDoc?.nombre || cirugiaNameRaw || '(Sin procedimiento)';
    const procRealId = procDoc?.id || procId || '';

    const procedimientoExists = !!procDoc;

    // Tipo paciente
    const pacienteTipo = tipoPacienteNorm(
      x.tipoPaciente ||
      pickRaw(raw,'Tipo de Paciente') ||
      pickRaw(raw,'Previsión')
    );

    const pacienteNombre = toUpperSafe(cleanReminder(x.nombrePaciente || pickRaw(raw,'Nombre Paciente')));

    // Info import
    const valor = Number(x.hmq || 0) ? (Number(x.valor || 0) || 0) : (asNumberLoose(pickRaw(raw,'Valor')));
    const hmq = Number(x.hmq || 0) || asNumberLoose(pickRaw(raw,'HMQ'));
    const dp  = Number(x.derechosPabellon || 0) || asNumberLoose(pickRaw(raw,'Derechos de Pabellón'));
    const ins = Number(x.insumos || 0) || asNumberLoose(pickRaw(raw,'Insumos'));

    // Por cada rol, generar línea si hay profesional
    for(const rf of ROLE_SPEC){
      const profNameRaw =
        toUpperSafe(cleanReminder(x.profesionales?.[rf.key] || pickRaw(raw, rf.csvField))) || '';

      const profIdRaw =
        cleanReminder(x.profesionalesId?.[rf.idKey]) || '';

      if(!profNameRaw && !profIdRaw) continue;

      // Buscar en catálogo: por ID o por nombre personal
      const profDoc =
        (profIdRaw && state.profesionalesById.get(String(profIdRaw))) ||
        (profNameRaw && state.profesionalesByName.get(normalize(profNameRaw))) ||
        null;

      // Datos de “titular” siempre = persona (cuando existe en catálogo),
      // si NO existe en catálogo, usamos lo que viene en producción.
      const titularNombre = profDoc?.nombreProfesional || profNameRaw || (profIdRaw ? String(profIdRaw) : '');
      const titularRut = profDoc?.rut || ''; // si no existe catálogo, rut puede quedar vacío
      const tipoPersona = (profDoc?.tipoPersona || '').toLowerCase();
      const empresaNombre = (tipoPersona === 'juridica') ? (profDoc?.razonSocial || '') : '';
      const empresaRut = (tipoPersona === 'juridica') ? (profDoc?.rutEmpresa || '') : '';

      // ALERTAS (maestros faltantes) vs PENDIENTES (tarifa)
      const alerts = [];
      const pendings = [];

      // Alertas de maestro (esto se corrige “en origen”)
      if(!profDoc) alerts.push('Profesional no existe en nómina (catálogo)');
      if(!clinicaId) alerts.push('clinicaId vacío (import)');
      else if(!clinicaExists) alerts.push('Clínica no existe en catálogo');
      if(!procedimientoExists) alerts.push('Procedimiento no mapeado (catálogo)');

      // Pendientes por datos faltantes (no maestro)
      if(!pacienteTipo) pendings.push('Tipo paciente vacío');

      // Tarifa (si hay base mínima)
      let monto = 0;
      if(clinicaId && clinicaExists && procDoc && pacienteTipo){
        const tar = getHonorarioFromTarifa(procDoc, clinicaId, pacienteTipo, rf.roleId);
        if(tar.ok) monto = tar.monto;
        else pendings.push(tar.reason || 'Tarifa incompleta');
      }else{
        // Si falta maestro, no lo marcamos como “pendiente tarifa”, porque es “alerta”
        // (así no se mezcla la causa real)
      }

      lines.push({
        prodId: row.id,

        fecha,
        hora,

        // clínica y procedimiento SIEMPRE con label de lo que venga
        clinicaId,
        clinicaNombre: clinicaLabel,
        clinicaExists,

        procedimientoId: procRealId,
        procedimientoNombre: procLabel,
        procedimientoExists,

        tipoPaciente: pacienteTipo,
        pacienteNombre,

        roleId: rf.roleId,
        roleNombre: state.rolesMap.get(rf.roleId) || rf.label,

        // Profesional (titular siempre persona)
        profesionalNombre: titularNombre,
        profesionalId: profDoc?.id || profIdRaw || '',
        profesionalRut: titularRut,
        tipoPersona,

        // Empresa (solo si juridica)
        empresaNombre,
        empresaRut,

        monto,

        // flags
        isAlerta: alerts.length > 0,
        isPendiente: pendings.length > 0,
        alerts,
        pendings,

        // texto para búsquedas/export
        observacion: [
          ...(alerts.length ? [`ALERTA: ${alerts.join(' · ')}`] : []),
          ...(pendings.length ? [`PENDIENTE: ${pendings.join(' · ')}`] : []),
        ].join(' | '),

        info: { valor, hmq, dp, ins }
      });
    }
  }

  // Agrupar por profesional
  const map = new Map();

  for(const ln of lines){
    // Si existe en catálogo => agrupar por ID (rutId)
    // Si no existe => agrupar por NOMBRE que viene en producción (para corregir fácil)
    const key = ln.profesionalId
      ? `ID:${String(ln.profesionalId)}`
      : `DESCONOCIDO:${normalize(ln.profesionalNombre)}`;

    if(!map.has(key)){
      map.set(key, {
        key,

        // Datos de UI:
        nombre: ln.profesionalNombre,
        rut: ln.profesionalRut || '',
        tipoPersona: ln.tipoPersona || '',

        empresaNombre: ln.empresaNombre || '',
        empresaRut: ln.empresaRut || '',

        casos: 0,
        total: 0,

        alertasCount: 0,
        pendientesCount: 0,

        lines: []
      });
    }

    const agg = map.get(key);
    agg.casos += 1;
    agg.total += Number(ln.monto || 0) || 0;

    if(ln.isAlerta) agg.alertasCount += 1;
    if(!ln.isAlerta && ln.isPendiente) agg.pendientesCount += 1; // prioridad: si hay alerta no lo contamos como pendiente

    // si dentro del mismo profesional hay líneas que sí traen empresa (jurídica),
    // consolidamos para UI (por si algunas líneas venían sin doc al principio y luego sí)
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

  // ORDEN: ALERTA arriba, luego PENDIENTE, luego OK (dentro por TOTAL desc)
  const prio = (st)=> st === 'alerta' ? 0 : (st === 'pendiente' ? 1 : 2);
  resumen.sort((a,b)=>{
    const pa = prio(a.status);
    const pb = prio(b.status);
    if(pa !== pb) return pa - pb;
    return (b.total||0) - (a.total||0);
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
      l.roleNombre,
      l.clinicaNombre,
      l.procedimientoNombre,
      l.pacienteNombre,
      l.tipoPaciente,
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
    pill.textContent = 'Sin producción confirmada';
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
    const tr = document.createElement('tr');

    // UI: SIEMPRE mostrar nombre titular + RUT personal (si existe).
    // Si juridica: subtítulo con empresa + rutEmpresa en gris.
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
      <td><b>${clp(agg.total)}</b></td>
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
        const fn = `LIQUIDACION_${safeFileName(agg.nombre)}_${state.ano}_${String(state.mesNum).padStart(2,'0')}.pdf`;
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

  $('lastLoad').textContent = `Items producción: ${state.prodRows.length} · Último cálculo: ${new Date().toLocaleString()}`;
}

/* =========================
   Modal detalle
========================= */
function openDetalle(agg){
  $('modalBackdrop').style.display = 'grid';

  // título siempre el nombre del profesional (persona)
  $('modalTitle').textContent = agg.nombre || 'Detalle';

  // subtítulo: mes/año + casos + rut personal + (rut empresa si aplica)
  const extraEmpresa = (agg.tipoPersona === 'juridica' && (agg.empresaNombre || agg.empresaRut))
    ? ` · Empresa: ${agg.empresaNombre || ''}${agg.empresaRut ? ' ('+agg.empresaRut+')' : ''}`
    : '';

  $('modalSub').textContent =
    `${monthNameEs(state.mesNum)} ${state.ano} · Casos: ${agg.casos}` +
    (agg.rut ? ` · RUT: ${agg.rut}` : '') +
    extraEmpresa;

  $('modalPillTotal').textContent = `TOTAL: ${clp(agg.total)}`;
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
    return normalize(a.roleNombre).localeCompare(normalize(b.roleNombre));
  });

  state.lastDetailExportLines = lines;

  for(const l of lines){
    const st = l.isAlerta ? pillHtml('bad','ALERTA') : (l.isPendiente ? pillHtml('warn','PENDIENTE') : pillHtml('ok','OK'));

    // Mostrar en observación: primero alertas, luego pendientes (separado)
    const obs = [
      ...(l.alerts?.length ? [`ALERTA: ${l.alerts.join(' · ')}`] : []),
      ...(l.pendings?.length ? [`PENDIENTE: ${l.pendings.join(' · ')}`] : []),
    ].join(' | ');

    // En clínica/procedimiento: mostrar lo que venga pero si falta maestro, dejar evidencia
    const clinWarn = (!l.clinicaId || !l.clinicaExists)
      ? `<div class="mini muted">${escapeHtml(l.clinicaId ? 'No existe en catálogo' : 'Sin clinicaId')}</div>`
      : '';

    const procWarn = (!l.procedimientoExists)
      ? `<div class="mini muted">No existe / no mapeado</div>`
      : '';

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono">${escapeHtml(l.fecha || '')} ${escapeHtml(l.hora || '')}</td>
      <td>${escapeHtml(l.clinicaNombre || '')}${clinWarn}</td>
      <td>
        ${escapeHtml(l.procedimientoNombre || '')}
        <div class="mini muted mono">${escapeHtml(l.procedimientoId || '')}</div>
        ${procWarn}
      </td>
      <td>
        ${escapeHtml(l.pacienteNombre || '')}
        <div class="mini muted">${escapeHtml((l.tipoPaciente||'').toUpperCase())}</div>
      </td>
      <td>
        ${escapeHtml(l.roleNombre || '')}
        <div class="mini muted mono">${escapeHtml(l.roleId || '')}</div>
      </td>
      <td><b>${clp(l.monto || 0)}</b></td>
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
   CSV Exports
========================= */
function exportResumenCSV(){
  // ✅ Agrego columnas empresa y rutEmpresa al final (no rompe lo anterior)
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',
    'casos','total',
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
    total: String(a.total || 0),

    alertas: String(a.alertasCount || 0),
    pendientes: String(a.pendientesCount || 0),
  }));

  const csv = toCSV(headers, items);
  download(`liquidaciones_resumen_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV resumen exportado');
}

function exportDetalleCSV(){
  // ✅ Mantengo lo anterior y agrego empresa/rutEmpresa + flags alerta/pendiente
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',

    'fecha','hora',
    'clinica','clinicaId','clinicaExiste',
    'procedimiento','procedimientoId','procedimientoExiste',
    'tipoPaciente','paciente',
    'rol','monto',
    'estadoLinea', // ALERTA | PENDIENTE | OK
    'observacion',
    'prodId'
  ];

  const items = [];
  for(const a of state.liquidResumen){
    for(const l of (a.lines || [])){
      const estadoLinea = l.isAlerta ? 'ALERTA' : (l.isPendiente ? 'PENDIENTE' : 'OK');

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

        clinica: l.clinicaNombre || '',
        clinicaId: l.clinicaId || '',
        clinicaExiste: l.clinicaExists ? 'SI' : 'NO',

        procedimiento: l.procedimientoNombre || '',
        procedimientoId: l.procedimientoId || '',
        procedimientoExiste: l.procedimientoExists ? 'SI' : 'NO',

        tipoPaciente: l.tipoPaciente || '',
        paciente: l.pacienteNombre || '',

        rol: l.roleNombre || '',
        monto: String(l.monto || 0),

        estadoLinea,
        observacion: l.observacion || '',

        prodId: l.prodId || ''
      });
    }
  }

  const csv = toCSV(headers, items);
  download(`liquidaciones_detalle_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV detalle exportado');
}

function exportDetalleProfesional(agg){
  const headers = [
    'mes','ano',
    'profesional','rut',
    'empresa','rutEmpresa',
    'tipoPersona',

    'fecha','hora',
    'clinica','clinicaId','clinicaExiste',
    'procedimiento','procedimientoId','procedimientoExiste',
    'tipoPaciente','paciente',
    'rol','monto',
    'estadoLinea',
    'observacion',
    'prodId'
  ];

  const items = (agg.lines || []).map(l=>{
    const estadoLinea = l.isAlerta ? 'ALERTA' : (l.isPendiente ? 'PENDIENTE' : 'OK');

    return ({
      mes: monthNameEs(state.mesNum),
      ano: String(state.ano),

      profesional: agg.nombre || '',
      rut: agg.rut || '',
      empresa: agg.empresaNombre || '',
      rutEmpresa: agg.empresaRut || '',
      tipoPersona: agg.tipoPersona || '',

      fecha: l.fecha || '',
      hora: l.hora || '',

      clinica: l.clinicaNombre || '',
      clinicaId: l.clinicaId || '',
      clinicaExiste: l.clinicaExists ? 'SI' : 'NO',

      procedimiento: l.procedimientoNombre || '',
      procedimientoId: l.procedimientoId || '',
      procedimientoExiste: l.procedimientoExists ? 'SI' : 'NO',

      tipoPaciente: l.tipoPaciente || '',
      paciente: l.pacienteNombre || '',

      rol: l.roleNombre || '',
      monto: String(l.monto || 0),

      estadoLinea,
      observacion: l.observacion || '',

      prodId: l.prodId || ''
    });
  });

  const csv = toCSV(headers, items);
  const safeName = normalize(agg.nombre || 'profesional').replace(/[^a-z0-9\-]/g,'-').slice(0,40);
  download(`liquidacion_${safeName}_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV profesional exportado');
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
  const y = now.getFullYear();
  const anoSel = $('ano');
  anoSel.innerHTML = '';
  for(let yy=y-2; yy<=y+3; yy++){
    const opt = document.createElement('option');
    opt.value = String(yy);
    opt.textContent = String(yy);
    anoSel.appendChild(opt);
  }

  state.mesNum = now.getMonth()+1;
  state.ano = y;

  mesSel.value = String(state.mesNum);
  anoSel.value = String(state.ano);

  mesSel.addEventListener('change', ()=>{ state.mesNum = Number(mesSel.value); recalc(); });
  anoSel.addEventListener('change', ()=>{ state.ano = Number(anoSel.value); recalc(); });
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

    await loadSidebar({ active: 'liquidaciones' });
    setActiveNav('liquidaciones');

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
      loadRoles(),
      loadClinicas(),
      loadProfesionales(),
      loadProcedimientos()
    ]);

    await recalc();
  }
});
