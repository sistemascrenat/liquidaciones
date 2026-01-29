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

  // 🔥 CANÓNICO (DEBE CALZAR CON cirugias.js)
  if(x.includes('fona')) return 'fonasa';
  if(x.includes('isap')) return 'particular_isapre';
  if(x.includes('part')) return 'particular_isapre';

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
  const pdfDoc = await PDFDocument.create();

  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  // A4 vertical
  const W = 595.28;
  const H = 841.89;

  // Paleta RENNAT (sobria)
  const RENNAT_BLUE  = rgb(0.08, 0.26, 0.36);
  const RENNAT_GREEN = rgb(0.12, 0.55, 0.45);
  const TEXT_MAIN    = rgb(0.08, 0.09, 0.11);
  const TEXT_MUTED   = rgb(0.45, 0.48, 0.52);
  const BORDER_SOFT  = rgb(0.82, 0.84, 0.86);

  const M = 36;

  // ===== Helpers internos PDF (solo layout) =====
  const drawHLine = (page, y, x1=M, x2=W-M, thick=1, col=BORDER_SOFT) => {
    page.drawLine({ start:{x:x1,y}, end:{x:x2,y}, thickness:thick, color:col });
  };

  const drawText = (page, text, x, y, size=10, bold=false, color=TEXT_MAIN) => {
    page.drawText(String(text ?? ''), { x, y, size, font: bold ? fontBold : font, color });
  };

  const measure = (text, size=10, bold=false) => {
    const f = bold ? fontBold : font;
    return f.widthOfTextAtSize(String(text ?? ''), size);
  };

  const clip = (s, maxChars) => String(s ?? '').slice(0, maxChars);

  // ✅ Abreviar "CLINICA/CLÍNICA" -> "C."
  const clinAbbrev = (name) => {
    const s = String(name ?? '').trim();
    if(!s) return '';
    return s
      .replace(/^CL[IÍ]NICA\s+/i, 'C. ')
      .replace(/^CLINICA\s+/i, 'C. ')
      .replace(/^CL[IÍ]N\.?\s+/i, 'C. ');
  };
  
  // ✅ Tipo paciente (humano) para mostrar en PDF
  const tipoPacienteHumano = (tpRaw) => {
    const tp = String(tpRaw ?? '').toLowerCase().trim();
  
    if(!tp || tp === 'sin_tipo') return 'SIN TIPO';
    if(tp.includes('fona')) return 'FONASA';
  
    // equivalencias que quieres unificar
    if(tp.includes('isap')) return 'ISAPRE';
    if(tp.includes('part')) return 'PARTICULAR';
  
    // fallback: uppercase acotado
    return tp.toUpperCase().slice(0, 14);
  };
  
  const money = (n)=> clp(n || 0);



  // ===== Datos cabecera =====
  const mesTxt = `${monthNameEs(state.mesNum)} ${state.ano}`;
  const titular = (agg?.nombre || '').toString();
  const rutTitular = (agg?.rut || '').toString();

  const esJuridica = (agg?.tipoPersona || '').toLowerCase() === 'juridica';
  const empresaNombre = (agg?.empresaNombre || '').toString();
  const empresaRut = (agg?.empresaRut || '').toString();

  // =========================
  // PÁGINA 1 — Resumen por Rol
  // =========================
  const page1 = pdfDoc.addPage([W, H]);
  let y = H - M;

  // Logo (opcional) arriba izquierda
  const logoBytes = await fetchAsArrayBuffer(PDF_ASSET_LOGO_URL);
  if (logoBytes) {
    try {
      const logo = await pdfDoc.embedPng(logoBytes);
      const logoW = 115;
      const logoH = (logo.height / logo.width) * logoW;
      page1.drawImage(logo, { x: M, y: y - logoH, width: logoW, height: logoH });
    } catch(_e){}
  }

  // Título centrado
  const t1 = 'LIQUIDACIÓN DE HONORARIOS';
  const t1Size = 16;
  drawText(page1, t1, (W - measure(t1,t1Size,true))/2, y - 24, t1Size, true, RENNAT_BLUE);

  // Línea verde bajo título
  drawHLine(page1, y - 30, M, W - M, 1.2, RENNAT_GREEN);

  // Mes/año (derecha)
  const mesLabel = `Mes/Año: ${mesTxt}`;
  drawText(page1, mesLabel, W - M - measure(mesLabel,10,false), y - 48, 10, false, TEXT_MUTED);

  y -= 78;

  // Datos profesional / empresa (simple, formal)
  const nombreMostrar = esJuridica ? (empresaNombre || titular || '—') : (titular || '—');
  const rutMostrar = esJuridica ? (empresaRut || rutTitular || '—') : (rutTitular || '—');

  drawText(page1, 'DATOS DEL PROFESIONAL', M, y, 11, true, RENNAT_BLUE);
  y -= 10;
  drawHLine(page1, y, M, W - M, 1, BORDER_SOFT);
  y -= 18;

  drawText(page1, 'Nombre:', M, y, 10, true, TEXT_MAIN);
  drawText(page1, nombreMostrar, M + 70, y, 10, false, TEXT_MAIN);
  y -= 16;

  drawText(page1, 'RUT:', M, y, 10, true, TEXT_MAIN);
  drawText(page1, rutMostrar, M + 70, y, 10, false, TEXT_MAIN);
  y -= 16;

  if(esJuridica){
    // mostramos también “titular persona” en chico, si existe
    if(titular){
      drawText(page1, 'Titular:', M, y, 9, false, TEXT_MUTED);
      drawText(page1, `${titular}${rutTitular ? ' · '+rutTitular : ''}`, M + 70, y, 9, false, TEXT_MUTED);
      y -= 14;
    }
  }

  y -= 8;

  // Agrupar líneas por rol
  const linesAll = [...(agg?.lines || [])];

  // Orden de roles: usa ROLE_SPEC si existe en state (mismo archivo) y luego otros
  const roleOrderIds = (Array.isArray(ROLE_SPEC) ? ROLE_SPEC.map(r=>r.roleId) : []);
  const roleLabelById = new Map(linesAll.map(l=>[l.roleId, l.roleNombre]));

  const groups = new Map(); // roleId -> lines
  for(const l of linesAll){
    const rid = l.roleId || 'sin_rol';
    if(!groups.has(rid)) groups.set(rid, []);
    groups.get(rid).push(l);
  }

  const roleIdsSorted = [
    ...roleOrderIds.filter(id=>groups.has(id)),
    ...[...groups.keys()].filter(id=>!roleOrderIds.includes(id)).sort()
  ];

  // Resumen por rol (DESGLOSE POR TIPO PACIENTE)
  drawText(page1, 'RESUMEN POR ROL', M, y, 11, true, RENNAT_BLUE);
  y -= 10;
  drawHLine(page1, y, M, W - M, 1, BORDER_SOFT);
  y -= 14;
  
  // Tabla: RUT | PROFESIONAL | TIPO PACIENTE | # | SUBTOTAL
  const col = {
    rut:  M,
    nom:  M + 65,
    tipo: M + 220,
    num:  W - M - 120,
    sub:  W - M
  };
  
  // Header tabla
  const headerY = y;
  drawText(page1, 'RUT', col.rut, headerY, 9, true, TEXT_MUTED);
  drawText(page1, 'PROFESIONAL', col.nom, headerY, 9, true, TEXT_MUTED);
  drawText(page1, 'TIPO', col.tipo, headerY, 9, true, TEXT_MUTED);
  drawText(page1, 'CANTIDAD', col.num + 10, headerY, 9, true, TEXT_MUTED);
  drawText(page1, 'SUBTOTAL', col.sub - measure('SUBTOTAL', 9, true), headerY, 9, true, TEXT_MUTED);
  
  y -= 10;
  drawHLine(page1, y, M, W - M, 1, BORDER_SOFT);
  y -= 14;
  
  const rowH = 16;
  
  const subtotalByRole = [];
  
  for (const rid of roleIdsSorted) {
    const ls = groups.get(rid) || [];
    if (!ls.length) continue;
  
    const rolName = roleLabelById.get(rid) || rid;
  
    // Título del rol
    if (y < M + 160) break;
    drawText(page1, String(rolName).toUpperCase(), M, y, 10, true, RENNAT_BLUE);
    y -= 12;
  
    // Desglose por tipoPaciente (FONASA / PARTICULAR / PARTICULAR_ISAPRE, etc.)
    const byTipo = new Map(); // tipo -> { casos, subtotal }
    for (const l of ls) {
      const tp = (l.tipoPaciente || '').toString().toLowerCase().trim() || 'sin_tipo';
      if (!byTipo.has(tp)) byTipo.set(tp, { casos: 0, subtotal: 0 });
      const o = byTipo.get(tp);
      o.casos += 1;
      o.subtotal += (Number(l.monto || 0) || 0);
    }
  
    const tiposSorted = [...byTipo.keys()].sort((a,b)=> a.localeCompare(b));
  
    let roleCasos = 0;
    let roleSubtotal = 0;
  
    const rutRow = rutTitular || '—';
    const nomRow = titular || '—';
  
    for (const tp of tiposSorted) {
      const o = byTipo.get(tp);
  
      roleCasos += o.casos;
      roleSubtotal += o.subtotal;
  
      if (y < M + 140) break;
  
      drawText(page1, rutRow, col.rut, y, 10, false, TEXT_MAIN);
      drawText(page1, clip(nomRow, 34), col.nom, y, 10, false, TEXT_MAIN);
  
      // mostramos tipo “bonito”
      const tpLabel = (tp === 'sin_tipo') ? 'SIN TIPO' : tp.toUpperCase();
      drawText(page1, clip(tpLabel, 16), col.tipo, y, 10, false, TEXT_MAIN);
  
      drawText(page1, String(o.casos), col.num + 10, y, 10, true, TEXT_MAIN);
  
      const subTxt = money(o.subtotal);
      drawText(page1, subTxt, col.sub - measure(subTxt, 10, true), y, 10, true, TEXT_MAIN);
  
      y -= rowH;
    }

    drawHLine(page1, y + 6, M, W - M, 0.8, BORDER_SOFT);
  
    // Subtotal del rol
    y -= 2;
    if (y < M + 140) break;
  
    drawText(page1, 'SUBTOTAL', col.tipo, y, 10, true, TEXT_MUTED);
    drawText(page1, String(roleCasos), col.num + 10, y, 10, true, TEXT_MAIN);
  
    const roleTxt = money(roleSubtotal);
    drawText(page1, roleTxt, col.sub - measure(roleTxt, 10, true), y, 10, true, TEXT_MAIN);
  
    y -= 10;
    drawHLine(page1, y, M, W - M, 0.8, BORDER_SOFT);
    y -= 12;
  
    subtotalByRole.push({ rid, rolName, casos: roleCasos, sub: roleSubtotal });
  }


  // TOTAL GENERAL
  const totalGeneral = Number(agg?.total || 0) || subtotalByRole.reduce((a,b)=>a+b.sub,0);
  y -= 10;
  drawHLine(page1, y, M, W - M, 1.2, RENNAT_GREEN);
  y -= 22;

  drawText(page1, 'TOTAL GENERAL', M, y, 12, true, RENNAT_BLUE);
  const totalTxt = money(totalGeneral);
  drawText(page1, totalTxt, W - M - measure(totalTxt,14,true), y - 2, 14, true, TEXT_MAIN);
  y -= 26;

  // Caja advertencia si hay pendientes o alertas
  const pendientesCount = Number(agg?.pendientesCount || 0) || 0;
  const alertasCount = Number(agg?.alertasCount || 0) || 0;

  if(pendientesCount > 0 || alertasCount > 0){
    const msgParts = [];
    if(alertasCount > 0) msgParts.push(`Alertas: ${alertasCount}`);
    if(pendientesCount > 0) msgParts.push(`Pendientes: ${pendientesCount}`);
    const msg = msgParts.join(' · ');

    // caja simple (borde suave)
    const boxH = 44;
    page1.drawRectangle({
      x: M,
      y: M + 70,
      width: W - 2*M,
      height: boxH,
      borderColor: BORDER_SOFT,
      borderWidth: 1,
      color: rgb(1,1,1)
    });

    drawText(page1, 'ATENCIÓN', M + 12, M + 70 + boxH - 18, 11, true, RENNAT_BLUE);
    drawText(page1, msg, M + 12, M + 70 + boxH - 34, 10, false, TEXT_MAIN);
    drawText(page1, 'Revisar pendientes/alertas en el detalle (página 2).', M + 12, M + 70 + 8, 9, false, TEXT_MUTED);
  }

  // =========================
  // PÁGINA 2 — Detalle de Casos
  // =========================
  const page2 = pdfDoc.addPage([W, H]);
  let y2 = H - M;

  // Encabezado página 2 (simple)
  const dTitle = 'DETALLE DE CASOS';
  drawText(page2, dTitle, M, y2 - 22, 14, true, RENNAT_BLUE);
  const sub2 = `${nombreMostrar} · ${mesTxt}`;
  drawText(page2, sub2, M, y2 - 40, 10, false, TEXT_MUTED);
  drawHLine(page2, y2 - 46, M, W - M, 1.2, RENNAT_GREEN);

  y2 -= 70;

  // Tabla detalle columnas: # | Clínica | Cirugía | Paciente | Tipo Paciente
  const dcol = {
    n:    M,
    clin: M + 30,   // antes 36 (más junto)
    cir:  M + 165,  // antes 190 (más junto)
    pac:  M + 295,  // antes 330 (más espacio para TIPO al final)
    tp:   W - M - 65 // antes -90 (TIPO más a la derecha, evita choque)
  };

  const drawDetailHeader = ()=>{
    drawText(page2, '#', dcol.n, y2, 9, true, TEXT_MUTED);
    drawText(page2, 'C.', dcol.clin, y2, 9, true, TEXT_MUTED);
    drawText(page2, 'CIRUGÍA', dcol.cir, y2, 9, true, TEXT_MUTED);
    drawText(page2, 'PACIENTE', dcol.pac, y2, 9, true, TEXT_MUTED);
    drawText(page2, 'TIPO', dcol.tp, y2, 9, true, TEXT_MUTED);
    y2 -= 10;
    drawHLine(page2, y2, M, W - M, 1, BORDER_SOFT);
    y2 -= 12;
  };

  const lineSort = (a,b)=>{
    const fa = normalize(a.fecha);
    const fb = normalize(b.fecha);
    if(fa !== fb) return fa.localeCompare(fb);
    const ha = normalize(a.hora);
    const hb = normalize(b.hora);
    if(ha !== hb) return ha.localeCompare(hb);
    return normalize(a.pacienteNombre).localeCompare(normalize(b.pacienteNombre));
  };

  const rowH2 = 14;

  for(const rid of roleIdsSorted){
    const ls = [...(groups.get(rid) || [])].sort(lineSort);
    if(!ls.length) continue;

    // si no queda espacio mínimo, igual seguimos pero cortamos
    if(y2 < M + 160){
      drawText(page2, '… (Hay más casos para este profesional; revisar export CSV si se requiere detalle completo)', M, y2, 9, false, TEXT_MUTED);
      break;
    }

    const rolName = (roleLabelById.get(rid) || rid || '').toString();

    // Título rol
    drawText(page2, `ROL: ${rolName.toUpperCase()}`, M, y2, 11, true, RENNAT_BLUE);
    y2 -= 8;
    drawHLine(page2, y2, M, W - M, 1, BORDER_SOFT);
    y2 -= 16;

    // Header tabla detalle
    drawDetailHeader();

    // Numeración reinicia por rol
    let idx = 1;

    for(const l of ls){
      if(y2 < M + 160){
        drawText(page2, '… (Se corta por espacio)', M, y2, 9, false, TEXT_MUTED);
        y2 = M + 150;
        break;
      }

      drawText(page2, String(idx++), dcol.n, y2, 10, true, TEXT_MAIN);
      
      // clínica: abreviamos "CLÍNICA" -> "C." y recortamos
      drawText(page2, clip(clinAbbrev(l.clinicaNombre || ''), 22), dcol.clin, y2, 10, false, TEXT_MAIN);
      
      drawText(page2, clip(l.procedimientoNombre || '', 20), dcol.cir, y2, 10, false, TEXT_MAIN);
      drawText(page2, clip(l.pacienteNombre || '', 22), dcol.pac, y2, 10, false, TEXT_MAIN);
      drawText(page2, clip((l.tipoPaciente || '').toString().toUpperCase(), 12), dcol.tp, y2, 9, false, TEXT_MUTED);

      y2 -= rowH2;
      y2 -= 2;
    }

    y2 -= 14; // espacio entre roles
  }

  // Datos del emisor al final (simple + formal)
  const emH = 104;
  const emY = M + 20;

  page2.drawRectangle({
    x: M,
    y: emY,
    width: W - 2*M,
    height: emH,
    borderColor: BORDER_SOFT,
    borderWidth: 1,
    color: rgb(1,1,1)
  });

  drawText(page2, 'DATOS CLÍNICA RENNAT', M + 12, emY + emH - 18, 10.5, true, RENNAT_BLUE);
  drawText(page2, 'RUT: 77.460.159-7', M + 12, emY + emH - 36, 9.5, false, TEXT_MUTED);
  drawText(page2, 'RAZÓN SOCIAL: SERVICIOS MÉDICOS GCS PROVIDENCIA SPA.', M + 12, emY + emH - 52, 9.5, false, TEXT_MUTED);
  drawText(page2, 'GIRO: ACTIVIDADES DE HOSPITALES Y CLÍNICAS PRIVADAS.', M + 12, emY + emH - 68, 9.5, false, TEXT_MUTED);
  drawText(page2, 'DIRECCIÓN: AV MANUEL MONTT 427. PISO 10. PROVIDENCIA.', M + 12, emY + emH - 84, 9.5, false, TEXT_MUTED);

  // Guardar PDF
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

// ✅ Resolver tipoPaciente contra las keys reales del tarifario.
function resolveTipoPacienteKey(pacientesObj, tipoPaciente){
  if(!pacientesObj || typeof pacientesObj !== 'object') return null;

  const tp = (tipoPaciente || '').toString();

  // 1) match directo
  if(pacientesObj[tp] !== undefined) return tp;

  const tpn = normalize(tp);
  const candidates = [];

  // ✅ Bucket "PARTICULAR / ISAPRE": aceptar equivalencias en ambos sentidos
  // (porque tu tarifario a veces tiene "particular" y no el combinado)
  const esBucketPartIsap =
    tpn === 'isapre' ||
    tpn === 'particular' ||
    tpn === 'particular_isapre' ||
    tpn === 'particularisapre' ||
    tpn.includes('isapre') ||
    (tpn.includes('particular') && tpn.includes('isap'));

  if(esBucketPartIsap){
    candidates.push(
      // combinado (tu canónico)
      'particular_isapre',
      'particular/isapre',
      'particular / isapre',
      'PARTICULAR / ISAPRE',
      'particularisapre',

      // ✅ equivalentes "legacy" que pueden existir como key real en el tarifario
      'particular',
      'isapre'
    );
  }

  // Siempre incluir el valor original recibido
  candidates.push(tp);

  // 3) Match “loose” contra keys existentes (ignora espacios, slash, guiones, etc.)
  const candLoose = candidates.map(c => normKeyLoose(c));
  for(const k of Object.keys(pacientesObj)){
    const kLoose = normKeyLoose(k);
    if(candLoose.includes(kLoose)) return k;
  }

  return null;
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

    const pacientesObj = clin?.pacientes;
    const pacKey = resolveTipoPacienteKey(pacientesObj, tipoPaciente);
    const pac = pacKey ? pacientesObj?.[pacKey] : null;

    if(!pac){
      // ✅ Mensaje más útil para depurar
      const disponibles = pacientesObj ? Object.keys(pacientesObj).join(', ') : '(sin pacientes)';
      return {
        ok:false,
        monto:0,
        reason:`Sin tarifario para paciente ${tipoPaciente} (keys: ${disponibles})`
      };
    }

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

  // ✅ Por defecto: MES ANTERIOR al actual (con ajuste de año)
  const now = new Date();

  // Mes actual en 1..12
  const mesActual = now.getMonth() + 1;

  // Mes anterior en 1..12
  const mesPrev = (mesActual === 1) ? 12 : (mesActual - 1);

  // Año: si estamos en Enero, el mes anterior es Diciembre del año anterior
  const anoPrev = (mesActual === 1) ? (now.getFullYear() - 1) : now.getFullYear();

  // Rango de años (centrado en el año "prev" para que aparezca seleccionado)
  const y = anoPrev;
  const anoSel = $('ano');
  anoSel.innerHTML = '';
  for(let yy=y-2; yy<=y+3; yy++){
    const opt = document.createElement('option');
    opt.value = String(yy);
    opt.textContent = String(yy);
    anoSel.appendChild(opt);
  }

  // ✅ set default state
  state.mesNum = mesPrev;
  state.ano = anoPrev;

  // ✅ set UI selects
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
