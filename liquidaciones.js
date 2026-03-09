// liquidaciones.js — COMPLETO (AJUSTADO A TU FIRESTORE REAL)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';
await loadSidebar({ active: 'liquidaciones' });

import {
  collection, collectionGroup, getDocs, query, where,
  doc, getDoc, setDoc, serverTimestamp
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

// ✅ Canoniza IDs/RUT para matching (soporta: "14.145.305-K", "14145305", "14145305K", etc.)
function canonRutAny(v=''){
  const s = (v ?? '').toString().toUpperCase().trim();
  if(!s) return '';

  // deja solo dígitos y K
  const only = s.replace(/[^0-9K]/g,'');

  // casos típicos:
  // - "14145305K" (con DV)
  // - "14145305"  (sin DV)
  return only;
}

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

function yyyymm(ano, mesNum){
  return `${String(ano)}${String(mesNum).padStart(2,'0')}`;
}

function pickTramo(tramos, n){
  const x = Number(n || 0) || 0;
  if(!Array.isArray(tramos)) return null;

  for(const t of tramos){
    const min = Number(t?.min ?? 0) || 0;
    const max = (t?.max === null || t?.max === undefined || t?.max === '') ? null : (Number(t.max) || 0);

    // ✅ Soporta distintos nombres de campo (por si tu doc config/bonos no usa "montoCLP")
    const montoRaw = (t?.montoCLP ?? t?.monto ?? t?.bonoCLP ?? t?.bono ?? 0);
    const monto = asNumberLoose(montoRaw); // ✅ soporta "1.050.000" / "$1.050.000" etc.

    if(x >= min && (max === null ? true : x <= max)){
      return { min, max, montoCLP: monto };
    }
  }
  return null;
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

  // 🔥 CANÓNICO (DEBE CALZAR CON el tarifario / cirugias.js)
  if(x.includes('fona')) return 'fonasa';

  // ✅ MLE (Modalidad Libre Elección)
  // Soporta entradas tipo: "MLE", "M.L.E", "MODALIDAD LIBRE ELECCION", "LIBRE ELECCION"
  if(x === 'mle' || x.includes('m.l.e') || x.includes('libre eleccion') || x.includes('libre elección')) {
    return 'mle';
  }

  // Particular / Isapre
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
const PDF_ASSET_LOGO_URL = './logoCRazul.jpeg'; // pon tu ruta real o déjalo así si lo subirás

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

  // ✅ Helper: RGB 0..255 -> 0..1 (pdf-lib)
  const rgb255 = (r,g,b)=> rgb(r/255, g/255, b/255);
  
  // Paleta RENNAT (sobria) — valores correctos para pdf-lib
  const RENNAT_BLUE  = rgb255(0, 39, 56);     
  const RENNAT_GREEN = rgb255(31, 140, 115);
  const RENNAT_GRAY  = rgb255(210, 215, 220); 
  const TEXT_MAIN    = rgb(0.08, 0.09, 0.11);
  const TEXT_MUTED   = rgb(0.45, 0.48, 0.52);
  const BORDER_SOFT  = rgb(0.82, 0.84, 0.86);
  const RENNAT_BLUE_SOFT  = rgb(0.18, 0.36, 0.45); 
  const RENNAT_GREEN_SOFT = rgb(0.20, 0.50, 0.42); 

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
    if(tp === 'mle' || tp.includes('mle') || tp.includes('libre eleccion') || tp.includes('libre elección')) return 'MLE';
    // equivalencias que quieres unificar
    if(tp.includes('isap')) return 'ISAPRE';
    if(tp.includes('part')) return 'PARTICULAR';
  
    // fallback: uppercase acotado
    return tp.toUpperCase().slice(0, 14);
  };
  
  const money = (n)=> clp(n || 0);

  // =========================
  // Caja DATOS CLÍNICA (para Página 1)
  // =========================
  function drawClinicaBoxPage1(page, topY){
    const boxW = W - 2*M;
  
    // ✅ Más compacta
    const emH = 82;
  
    // Marco
    page.drawRectangle({
      x: M,
      y: topY - emH,
      width: boxW,
      height: emH,
      borderColor: BORDER_SOFT,
      borderWidth: 1,
      color: rgb(1,1,1)
    });
  
    // ✅ Título más chico
    drawText(page, 'DATOS CLÍNICA RENNAT', M + 10, topY - 16, 9.2, true, RENNAT_BLUE);
  
    // ✅ Texto más chico + menos interlineado
    const fs = 8.2;
    const lh = 14;
  
    drawText(page, 'RUT: 77.460.159-7', M + 10, topY - (16 + lh*1), fs, false, TEXT_MUTED);
    drawText(page, 'RAZÓN SOCIAL: SERVICIOS MÉDICOS GCS PROVIDENCIA SPA.', M + 10, topY - (16 + lh*2), fs, false, TEXT_MUTED);
    drawText(page, 'GIRO: ACTIVIDADES DE HOSPITALES Y CLÍNICAS PRIVADAS.', M + 10, topY - (16 + lh*3), fs, false, TEXT_MUTED);
    drawText(page, 'DIRECCIÓN: AV MANUEL MONTT 427. PISO 10. PROVIDENCIA.', M + 10, topY - (16 + lh*4), fs, false, TEXT_MUTED);
  
    return emH;
  }


    // =========================
    // Caja DATOS CLÍNICA en horizontal (última página)
    // =========================
    const CLINICA_BOX = {
      emH: 104,
      emY: M + 20 // “desde abajo”: quedará a 20px sobre el margen inferior
    };
  
    function drawClinicaBoxHorizontal(page){
      const boxW = W2 - 2*M;
      const { emH, emY } = CLINICA_BOX;
  
      page.drawRectangle({
        x: M,
        y: emY,
        width: boxW,
        height: emH,
        borderColor: BORDER_SOFT,
        borderWidth: 1,
        color: rgb(1,1,1)
      });
  
      // Título
      drawText(page, 'DATOS CLÍNICA RENNAT', M + 12, emY + emH - 18, 10.5, true, RENNAT_BLUE);
  
      // Líneas
      drawText(page, 'RUT: 77.460.159-7', M + 12, emY + emH - 36, 9.5, false, TEXT_MUTED);
      drawText(page, 'RAZÓN SOCIAL: SERVICIOS MÉDICOS GCS PROVIDENCIA SPA.', M + 12, emY + emH - 52, 9.5, false, TEXT_MUTED);
      drawText(page, 'GIRO: ACTIVIDADES DE HOSPITALES Y CLÍNICAS PRIVADAS.', M + 12, emY + emH - 68, 9.5, false, TEXT_MUTED);
      drawText(page, 'DIRECCIÓN: AV MANUEL MONTT 427. PISO 10. PROVIDENCIA.', M + 12, emY + emH - 84, 9.5, false, TEXT_MUTED);
    }



  // =========================
  // PÁGINA 1 — Estilo “imagen 2”
  // Logo arriba derecha + barra título azul + tablas con grid
  // =========================
  const page1 = pdfDoc.addPage([W, H]);
  let y = H - M;

  // ===== Helpers “tabla” (grid real) =====
  const drawBox = (page, x, yTop, w, h, fill=null, stroke=BORDER_SOFT, strokeW=1) => {
    page.drawRectangle({
      x, y: yTop - h, width: w, height: h,
      color: fill || undefined,
      borderColor: stroke,
      borderWidth: strokeW
    });
  };

  const drawVLine = (page, x, yTop, h, thick=1, col=BORDER_SOFT) => {
    page.drawLine({ start:{x, y:yTop}, end:{x, y:yTop - h}, thickness:thick, color:col });
  };

  const drawHLine2 = (page, x, y, w, thick=1, col=BORDER_SOFT) => {
    page.drawLine({ start:{x, y}, end:{x:x + w, y}, thickness:thick, color:col });
  };

  // Texto “centrado verticalmente” dentro de una celda
  const drawCellText = (page, text, x, yTop, cellH, size=10, bold=false, color=TEXT_MAIN, pad=6) => {
    const t = String(text ?? '');
    const yText = yTop - (cellH * 0.72); // ajuste visual (baseline)
    page.drawText(t, { x: x + pad, y: yText, size, font: bold ? fontBold : font, color });
  };

  // Texto alineado a la derecha dentro de celda
  const drawCellTextRight = (page, text, x, yTop, cellW, cellH, size=10, bold=false, color=TEXT_MAIN, pad=6) => {
    const t = String(text ?? '');
    const wTxt = (bold ? fontBold : font).widthOfTextAtSize(t, size);
    const yText = yTop - (cellH * 0.72);
    page.drawText(t, { x: x + cellW - pad - wTxt, y: yText, size, font: bold ? fontBold : font, color });
  };

  const wrapClip = (s, maxChars) => String(s ?? '').slice(0, maxChars);

  // ===== Datos cabecera =====
  const mesTxt = `${monthNameEs(state.mesNum)} ${state.ano}`;
  
  // ✅ Profesional (persona natural)
  const profNombre = (agg?.nombre || '').toString().trim();
  const profRut    = (agg?.rut || '').toString().trim();
  
  // ✅ Empresa (solo si jurídica)
  const tipoPersona = (agg?.tipoPersona || '').toString().toLowerCase().trim(); // 'juridica' | 'natural' | ''
  const esJuridica  = (tipoPersona === 'juridica');
  
  const empresaNombre = (agg?.empresaNombre || '').toString().trim();
  const empresaRut    = (agg?.empresaRut || '').toString().trim();
  
  // ✅ Lo que se muestra en la tabla superior
  // - Jurídica: RUT pago = rutEmpresa ; Nombre RUT de pago = razón social
  // - Natural : RUT pago = rut profesional ; Nombre RUT de pago = nombre profesional
  const rutMostrar = esJuridica
    ? (empresaRut || '—')
    : (profRut || '—');
  
  const nombreMostrar = esJuridica
    ? (empresaNombre || profNombre || '—') // si faltara razón social, cae al nombre del profesional como fallback
    : (profNombre || '—');
  
  // ✅ Tipo visible
  const tipoMostrar = esJuridica ? 'JURIDICA' : 'NATURAL';

  // ===== Barra título (definimos primero porque el "gap" será barH) =====
  const barH = 28;
  const barW = W - 2*M;
  const barX = M;
  
  // ✅ Queremos que el espacio entre logo y barra sea del mismo porte que la barra
  const gapLogoTitulo = barH;
  
  // ✅ Si no hay logo, usamos como "base" el top normal
  let logoBottomY = H - M; // se recalcula si el logo existe
  
  
  // ===== Logo (arriba izquierda) =====
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
      const logoY = H - M - logoH;  // bottom del logo
  
      page1.drawImage(logo, {
        x: logoX,
        y: logoY,
        width: logoW,
        height: logoH
      });
  
      // ✅ Este es el borde inferior del logo (para calcular la barra)
      logoBottomY = logoY;
  
    } catch (e) {
      console.warn('No se pudo embebeder logo:', e);
    }
  } else {
    console.warn('No se pudo descargar logo (URL no accesible):', PDF_ASSET_LOGO_URL);
  }
  
  
  // ===== Barra título azul (debajo del logo) con gap EXACTO =====
  // ✅ Top de la barra = (borde inferior del logo) - gap
  const barTop = logoBottomY - gapLogoTitulo;
  
  drawBox(page1, barX, barTop, barW, barH, RENNAT_BLUE, RENNAT_BLUE, 1);

  // ✅ Título solicitado: "LIQUIDACIÓN DE PAGO MES AÑO"
  const title = `LIQUIDACIÓN DE PAGO ${String(mesTxt).toUpperCase()}`; // ej: "LIQUIDACIÓN DE PAGO ENERO 2026"
  const titleSize = 13;
  const titleW = measure(title, titleSize, true);
  drawText(page1, title, barX + (barW - titleW)/2, barTop - 19, titleSize, true, rgb(1,1,1));


  y = barTop - barH - 14;

  // =========================
  // TABLA: Datos del Profesional
  // =========================
  const boxW = W - 2*M;
  const rowH = 22;

  const dataRows = [];
  
  dataRows.push(['PROFESIONAL', String(profNombre || '—').toUpperCase()]);
  dataRows.push(['RUT', String(profRut || '—').toUpperCase()]);
  
  if(esJuridica){
    dataRows.push(['EMPRESA', String(empresaNombre || '—').toUpperCase()]);
    dataRows.push(['RUT EMPRESA', String(empresaRut || '—').toUpperCase()]);
  }
  
  dataRows.push(['TIPO DE PERSONA', String(tipoMostrar).toUpperCase()]);


  // altura tabla
  const dataH = dataRows.length * rowH;

  // caja exterior
  drawBox(page1, M, y, boxW, dataH, rgb(1,1,1), BORDER_SOFT, 1);

  // columnas
  const c1 = Math.round(boxW * 0.45); // etiqueta
  const c2 = boxW - c1;               // valor

  // líneas verticales
  drawVLine(page1, M + c1, y, dataH, 1, BORDER_SOFT);

  // líneas horizontales y texto
  for(let r=0; r<dataRows.length; r++){
    const yRowTop = y - r*rowH;
  
    // ✅ Blindado: si por cualquier razón viene undefined, no rompe
    const row = Array.isArray(dataRows[r]) ? dataRows[r] : ['',''];
    const label = row[0] ?? '';
    const value = row[1] ?? '';
  
    // ✅ Detecta la fila “Profesional”
    const isProfesionalRow = String(label).toLowerCase().trim() === 'profesional';
  
    // ✅ 1) Fondo verde SOLO para “Profesional”
    if(isProfesionalRow){
      // pinta el fondo de la fila completa (sin borde) - OJO: PDF-lib usa y desde abajo
      page1.drawRectangle({
        x: M,
        y: (yRowTop - rowH),
        width: boxW,
        height: rowH,
        color: RENNAT_GREEN
      });
  
      // como el fondo tapa líneas, redibujamos la línea vertical del separador SOLO en esta fila
      drawVLine(page1, M + c1, yRowTop, rowH, 1, BORDER_SOFT);
    }
  
    // ✅ 2) Línea horizontal superior (va después para que se vea)
    if(r > 0) drawHLine2(page1, M, yRowTop, boxW, 1, BORDER_SOFT);
  
    // ✅ 3) Texto: blanco si es “Profesional”
    const labelColor = isProfesionalRow ? rgb(1,1,1) : TEXT_MAIN;
    const valueColor = isProfesionalRow ? rgb(1,1,1) : TEXT_MAIN;
  
    drawCellText(page1, label, M, yRowTop, rowH, 10, false, labelColor, 8);
    drawCellText(page1, wrapClip(value, 50), M + c1, yRowTop, rowH, 10, true, valueColor, 8);
  }



  y = y - dataH - 16;

  // =========================
  // TABLA: Resumen por Rol (estilo imagen 2)
  // Encabezado azul + líneas marcadas
  // =========================

  // Agrupar líneas por rol (✅ PDF: AYUDANTE 1 + 2 se unifican en "AYUDANTE")
  const linesAll = [...(agg?.lines || [])];

  // ✅ Normaliza roleId SOLO para el resumen PDF
  const resumenRoleId = (rid) => {
    const r = String(rid || '').trim();
    if(r === 'r_ayudante_1' || r === 'r_ayudante_2') return 'r_ayudante';
    return r || 'sin_rol';
  };

  // ✅ Etiqueta SOLO para el resumen PDF
  const resumenRoleLabel = (rid) => {
    if(rid === 'r_ayudante') return 'AYUDANTE';
    // fallback: usa lo que venga en la línea (en mayúscula)
    // (si hay mezclas, igual quedará consistente por roleId real)
    const any = linesAll.find(l => String(l.roleId||'').trim() === rid);
    return (any?.roleNombre || rid || '').toString().toUpperCase();
  };

  // ✅ Orden de roles SOLO para el resumen PDF
  // (si quieres otro orden, lo ajustamos acá)
  const roleOrderIds = [
    'r_cirujano',
    'r_anestesista',
    'r_ayudante',     // 👈 unificado
    'r_arsenalera'
  ];

  // groups por roleId resumen
  const groups = new Map();
  for(const l of linesAll){
    const gid = resumenRoleId(l.roleId);
    if(!groups.has(gid)) groups.set(gid, []);
    groups.get(gid).push(l);
  }

  // ordenar: primero los conocidos, luego extras
  const roleIdsSorted = [
    ...roleOrderIds.filter(id=>groups.has(id)),
    ...[...groups.keys()].filter(id=>!roleOrderIds.includes(id)).sort()
  ];

  // Construimos filas: [rolTitulo], [tipoPaciente, cant, subtotal], [SUBTOTAL rol...]
  const resumenRows = [];
  const subtotalByRole = [];

  for (const rid of roleIdsSorted) {
    const ls = groups.get(rid) || [];
    if (!ls.length) continue;

    const rolName = resumenRoleLabel(rid);

    // título del rol como “fila separadora”
    resumenRows.push({ kind:'role', rol: rolName });

    // agrupar por tipoPaciente
    const byTipo = new Map();
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

    for (const tp of tiposSorted) {
      const o = byTipo.get(tp);
      roleCasos += o.casos;
      roleSubtotal += o.subtotal;

      let tpLabel = tipoPacienteHumano(tp);
      if (tpLabel === 'ISAPRE' || tpLabel === 'PARTICULAR') tpLabel = 'PARTICULAR O ISAPRE';

      resumenRows.push({ kind:'row', tipo: tpLabel, cant: o.casos, sub: o.subtotal });
    }

    resumenRows.push({ kind:'subtotal', tipo:'SUBTOTAL', cant: roleCasos, sub: roleSubtotal });

    subtotalByRole.push({ rid, rolName, casos: roleCasos, sub: roleSubtotal });
  }

  // Medidas tabla resumen
  const headH = 24;
  const resRowH = 20;

  // calculo altura dinámica
  const resH = headH + resumenRows.length * resRowH;

  // si se pasa de página, acotamos (igual que antes)
  const maxHAvailable = y - (M + 90);
  const resHFinal = Math.min(resH, maxHAvailable);

  // Marco tabla
  drawBox(page1, M, y, boxW, resHFinal, rgb(1,1,1), BORDER_SOFT, 1);

  // Header azul
  drawBox(page1, M, y, boxW, headH, RENNAT_BLUE, RENNAT_BLUE, 1);
  drawCellText(page1, 'TIPO', M, y, headH, 10, true, rgb(1,1,1), 8);

  const colCantW = 110;
  const colSubW  = 150;
  const colTipoW = boxW - colCantW - colSubW;

  // separadores verticales (toda la tabla)
  drawVLine(page1, M + colTipoW, y, resHFinal, 1, BORDER_SOFT);
  drawVLine(page1, M + colTipoW + colCantW, y, resHFinal, 1, BORDER_SOFT);

  drawCellTextRight(page1, 'CANTIDAD', M + colTipoW, y, colCantW, headH, 10, true, rgb(1,1,1), 8);
  drawCellTextRight(page1, 'SUBTOTAL', M + colTipoW + colCantW, y, colSubW, headH, 10, true, rgb(1,1,1), 8);

  // filas
  let yCursor = y - headH;
  let extraGap = 0; // ✅ espacio acumulado entre bloques de roles

  for(let i=0; i<resumenRows.length; i++){
    const r = resumenRows[i];
    const yTop = yCursor - i*resRowH - extraGap;

    // si nos pasamos del espacio, cortamos
    if((y - (headH + (i+1)*resRowH + extraGap)) < (M + 90)) break;

    // línea horizontal de fila
    drawHLine2(page1, M, yTop, boxW, 1, BORDER_SOFT);

    if(r.kind === 'role'){
      // fila “título rol” (texto azul, sin valores)
      drawCellText(
        page1,
        r.rol,
        M,
        yTop,
        resRowH,
        10,
        true,
        RENNAT_BLUE_SOFT, // 👈 azul grisáceo
        8
      );

    } else {
      const tipoTxt = r.tipo || '';

      const isSubtotal = r.kind === 'subtotal';
      const subtotalColor = isSubtotal ? RENNAT_GREEN_SOFT : TEXT_MAIN;
      
      drawCellText(
        page1,
        tipoTxt,
        M,
        yTop,
        resRowH,
        10,
        isSubtotal,
        subtotalColor,
        8
      );
      
      drawCellTextRight(
        page1,
        String(r.cant ?? ''),
        M + colTipoW,
        yTop,
        colCantW,
        resRowH,
        10,
        true,
        subtotalColor,
        8
      );
      
      drawCellTextRight(
        page1,
        money(r.sub ?? 0),
        M + colTipoW + colCantW,
        yTop,
        colSubW,
        resRowH,
        10,
        true,
        subtotalColor,
        8
      );

      // ✅ AGREGAR espacio visual después de cada SUBTOTAL
      if(isSubtotal){
        extraGap += 10;
      }
    }
  }

  // bajamos cursor real (altura usada)
  const usedRows = Math.min(resumenRows.length, Math.floor((resHFinal - headH)/resRowH));
  y = y - (headH + usedRows*resRowH + extraGap) - 16;
  // =========================
  // ✅ TOTAL + AJUSTES + TOTAL A PAGAR
  // =========================
  const totalProcedimientos = Number(agg?.ajustes?.totalProcedimientos ?? agg?.total ?? 0) || 0;
  
  const descuentoUF  = Number(agg?.ajustes?.descuentoUF || 0) || 0;
  const descuentoCLP = Number(agg?.ajustes?.descuentoCLP || 0) || 0;
  
  const cirugiasComoPrincipal = Number(agg?.ajustes?.cirugiasComoPrincipal || 0) || 0;
  const bonoCLP = Number(agg?.ajustes?.bonoCLP || 0) || 0;
  
  const ufValor = Number(agg?.ajustes?.ufValorCLP || 0) || 0;
  const totalAPagar = Number(agg?.ajustes?.totalAPagar ?? (totalProcedimientos - descuentoCLP + bonoCLP)) || 0;
  
  // 1) TOTAL PROCEDIMIENTOS
  const totalBarH = 28;
  drawBox(page1, M, y, boxW, totalBarH, RENNAT_GREEN, RENNAT_GREEN, 1);
  drawCellText(page1, 'TOTAL PROCEDIMIENTOS', M, y, totalBarH, 12, true, rgb(1,1,1), 10);
  drawCellTextRight(page1, money(totalProcedimientos), M + (boxW - 200), y, 200, totalBarH, 13, true, rgb(1,1,1), 10);
  y = y - totalBarH - 10;
  
  // 2) AJUSTES (solo si hay algo que mostrar)
  const ajustesRows = [];
  if(descuentoUF > 0 && descuentoCLP > 0 && ufValor > 0){
    ajustesRows.push({
      item: `DESCUENTO (${descuentoUF} UF · UF ${money(ufValor)})`,
      cant: 1,
      sub: -descuentoCLP
    });
  }
  if(bonoCLP > 0){
  
    // ✅ info de tramo calculada en buildLiquidaciones()
    const tramoIdx = Number(agg?.ajustes?.bonoTramoIndex || 0) || 0;
    const tramo = agg?.ajustes?.bonoTramo || null;
  
    // arma texto: "Tramo 1 (10 al 15)" (si max viene null -> "desde X")
    const tramoTxt = tramo
      ? (
          (tramo.max === null || tramo.max === undefined || tramo.max === '')
            ? `TRAMO ${tramoIdx || ''} (${tramo.min} A MÁS CIRUGÍAS)`
            : `TRAMO ${tramoIdx || ''} (${tramo.min} A ${tramo.max} CIRUGÍAS)`
        ).replace('TRAMO ', 'TRAMO ')
      : (tramoIdx ? `TRAMO ${tramoIdx}` : '');
  
    ajustesRows.push({
      item: `BONO CIRUJANO ${tramoTxt}`.trim(),
      // ✅ en CANTIDAD debe ir la cantidad de cirugías (ej 13)
      cant: Number(cirugiasComoPrincipal || 0) || 0,
      // ✅ subtotal se mantiene como monto del bono
      sub: bonoCLP
    });
  }
  
  if(ajustesRows.length){
    const headH2 = 22;
    const rowH2  = 20;
    const tH = headH2 + ajustesRows.length * rowH2;
  
    drawBox(page1, M, y, boxW, tH, rgb(1,1,1), BORDER_SOFT, 1);
  
    drawBox(page1, M, y, boxW, headH2, RENNAT_BLUE, RENNAT_BLUE, 1);
    drawCellText(page1, 'AJUSTES', M, y, headH2, 10, true, rgb(1,1,1), 8);
  
    const cCant = 110;
    const cSub  = 150;
    const cItem = boxW - cCant - cSub;
  
    drawVLine(page1, M + cItem, y, tH, 1, BORDER_SOFT);
    drawVLine(page1, M + cItem + cCant, y, tH, 1, BORDER_SOFT);
  
    drawCellTextRight(page1, 'CANTIDAD', M + cItem, y, cCant, headH2, 9, true, rgb(1,1,1), 8);
    drawCellTextRight(page1, 'SUBTOTAL', M + cItem + cCant, y, cSub, headH2, 9, true, rgb(1,1,1), 8);
  
    for(let i=0;i<ajustesRows.length;i++){
      const r = ajustesRows[i];
      const yTop = y - headH2 - i*rowH2;
  
      drawHLine2(page1, M, yTop, boxW, 1, BORDER_SOFT);
  
      drawCellText(page1, wrapClip(r.item, 60), M, yTop, rowH2, 9, true, TEXT_MAIN, 8);
      drawCellTextRight(page1, String(r.cant), M + cItem, yTop, cCant, rowH2, 9, true, TEXT_MAIN, 8);
      drawCellTextRight(page1, money(r.sub), M + cItem + cCant, yTop, cSub, rowH2, 9, true, TEXT_MAIN, 8);
    }
  
    drawHLine2(page1, M, y - tH, boxW, 1, BORDER_SOFT);
  
    y = y - tH - 10;
  }
  
  // 3) TOTAL A PAGAR
  drawBox(page1, M, y, boxW, totalBarH, RENNAT_GREEN, RENNAT_GREEN, 1);
  drawCellText(page1, 'TOTAL A PAGAR', M, y, totalBarH, 12, true, rgb(1,1,1), 10);
  drawCellTextRight(page1, money(totalAPagar), M + (boxW - 200), y, 200, totalBarH, 13, true, rgb(1,1,1), 10);
  
  y = y - totalBarH - 10;

  // =========================
  // ✅ DESGLOSE FECHAS DE PAGO (post TOTAL A PAGAR)
  // - 5 del mes cargado: PARTICULAR/ISAPRE (+ BONO si aplica)
  // - 27 del mes cargado: FONASA
  // - Monto Particular incluye: Particular/Isapre - Descuento + Bono (para que sume totalAPagar)
  // =========================
  {
    const lines = Array.isArray(agg?.lines) ? agg.lines : [];
  
    // ✅ Regla pagos:
    // - Día 27: SOLO Fonasa del Cirujano (roleId === 'r_cirujano')
    // - Día 5 : Todo lo demás (Particular/Isapre, MLE, y Fonasa de otros roles)
    const baseDia27 = lines.reduce((acc, l) => {
      const tp = String(l?.tipoPaciente || '').toLowerCase().trim();
      const isFonasa = tp.includes('fona');
      const isCirujano = String(l?.roleId || '') === 'r_cirujano';
      const m = Number(l?.monto || 0) || 0;
      return (isFonasa && isCirujano) ? (acc + m) : acc;
    }, 0);

    const baseDia5 = lines.reduce((acc, l) => {
      const tp = String(l?.tipoPaciente || '').toLowerCase().trim();
      const isFonasa = tp.includes('fona');
      const isCirujano = String(l?.roleId || '') === 'r_cirujano';
      const m = Number(l?.monto || 0) || 0;

      // Todo lo que NO sea "Fonasa Cirujano"
      return (isFonasa && isCirujano) ? acc : (acc + m);
    }, 0);
  
    // Bono y descuento (ya calculados arriba)
    const bono = Number(bonoCLP || 0) || 0;
    const desc = Number(descuentoCLP || 0) || 0;
  
    // ✅ Reparto para que el desglose sume EXACTO el totalAPagar:
    // Particular/Isapre: Particular - Descuento + Bono
    // Fonasa: Fonasa
    let montoPart = Math.round(baseDia5 - desc + bono); // día 5 (incluye MLE + Fonasa no-cirujano)
    let montoFona = Math.round(baseDia27);              // día 27 (Fonasa cirujano)
  
    // Si por descuento el montoPart quedara negativo, lo “arrastra” a Fonasa (sin dejar negativos)
    if (montoPart < 0) {
      montoFona = Math.max(0, montoFona + montoPart);
      montoPart = 0;
    }
  
    // Mostrar tabla solo si hay algo que desglosar (o si hay total a pagar)
    const shouldShow = (Number(totalAPagar || 0) > 0) || (baseDia5 > 0) || (baseDia27 > 0) || (bono > 0) || (desc > 0);
  
    if (shouldShow) {
  
      // helper fecha texto
      const MES_TXT = String(monthNameEs(state.mesNum)).toUpperCase();
      const ANO_TXT = String(state.ano);
      const fechaPagoTxt = (day) => `${day} DE ${MES_TXT} ${ANO_TXT}`;
  
      // armado asunto (sin mencionar bono si no aplica)
      const asuntoPart = 'PAGO DE PROCEDIMIENTOS';
      const asuntoFona = 'PAGO DE PROCEDIMIENTOS FONASA';
  
      // medidas tabla
      const headH3 = 22;
      const rowH3  = 22;
      const rows3 = [
        { fecha: fechaPagoTxt(5),  asunto: asuntoPart, monto: montoPart },
        { fecha: fechaPagoTxt(27), asunto: asuntoFona, monto: montoFona }
      ];
  
      const tableH = headH3 + rows3.length * rowH3;
  
      // caja exterior
      drawBox(page1, M, y, boxW, tableH, rgb(1,1,1), BORDER_SOFT, 1);
  
      // header azul RENNAT
      drawBox(page1, M, y, boxW, headH3, RENNAT_BLUE, RENNAT_BLUE, 1);
  
      // columnas
      const cFecha = 170;
      const cMonto = 160;
      const cAsun  = boxW - cFecha - cMonto;
  
      // separadores verticales
      drawVLine(page1, M + cFecha, y, tableH, 1, BORDER_SOFT);
      drawVLine(page1, M + cFecha + cAsun, y, tableH, 1, BORDER_SOFT);
  
      // headers (BLANCO / NEGRITA / MAYUS)
      drawCellText(page1, 'FECHA DE PAGO', M, y, headH3, 9.5, true, rgb(1,1,1), 8);
      drawCellText(page1, 'ASUNTO DEL PAGO', M + cFecha, y, headH3, 9.5, true, rgb(1,1,1), 8);
      drawCellTextRight(page1, 'MONTO', M + cFecha + cAsun, y, cMonto, headH3, 9.5, true, rgb(1,1,1), 8);
  
      // filas (GRIS / TEXTO AZUL / NEGRITA / MAYUS)
      for (let i=0; i<rows3.length; i++) {
        const r = rows3[i];
        const yTop = y - headH3 - i*rowH3;
      
        // ✅ Fondo gris "del logo"
        page1.drawRectangle({
          x: M,
          y: (yTop - rowH3),
          width: boxW,
          height: rowH3,
          color: RENNAT_GRAY // 👈 nuevo color
        });
      
        // redibujar separadores encima del fondo
        drawVLine(page1, M + cFecha, yTop, rowH3, 1, BORDER_SOFT);
        drawVLine(page1, M + cFecha + cAsun, yTop, rowH3, 1, BORDER_SOFT);
      
        // línea horizontal superior
        drawHLine2(page1, M, yTop, boxW, 1, BORDER_SOFT);
      
        // ✅ Textos AZUL RENNAT, negrita, mayúscula
        drawCellText(page1, String(r.fecha || '').toUpperCase(), M, yTop, rowH3, 10, true, RENNAT_BLUE, 8);
        drawCellText(page1, String(r.asunto || '').toUpperCase(), M + cFecha, yTop, rowH3, 10, true, RENNAT_BLUE, 8);
        drawCellTextRight(page1, money(r.monto || 0), M + cFecha + cAsun, yTop, cMonto, rowH3, 10, true, RENNAT_BLUE, 8);
      }
 
      // línea inferior
      drawHLine2(page1, M, y - tableH, boxW, 1, BORDER_SOFT);
  
      // avanzar cursor
      y = y - tableH - 10;
    }
  }

  // ✅ Caja DATOS CLÍNICA en Página 1
  const clinH = drawClinicaBoxPage1(page1, y);
  y = y - clinH - 12;

  // =========================
  // PÁGINA 2 — Detalle con la misma lógica (tabla con header azul)
  // =========================
  // A4 horizontal (detalle)
  const W2 = 841.89;
  const H2 = 595.28;
  
  const page2 = pdfDoc.addPage([W2, H2]);
  let y2 = H2 - M;

  // barra título azul (igual)
  // barra título azul (en horizontal: recalculamos ancho)
  const barX2 = M;
  const barW2 = W2 - 2*M;
  
  const T_DETALLE = 'Detalle de Procedimientos';

  // Barra título azul
  drawBox(page2, barX2, y2, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
  drawText(page2, T_DETALLE, barX2 + (barW2 - measure(T_DETALLE, 13, true))/2, y2 - 19, 13, true, rgb(1,1,1));
  y2 -= (barH + 12);

  // Subtítulo
  drawText(page2, `${profNombre || '—'} · ${mesTxt}`, M, y2, 10, false, TEXT_MUTED);
  y2 -= 12;


  // tabla detalle
  const detHeadH = 24;
  const detRowH  = 18;
  
  // ✅ En horizontal: ancho total útil
  const detX = M;
  const detW = W2 - 2*M;
  
  // Columnas base (proporciones)
  let detCols = [
    { key:'n',     label:'#',             w: 44  },
    { key:'fecha', label:'FECHA',         w: 110 },
    { key:'clin',  label:'CLÍNICA',       w: 160 },
    { key:'proc',  label:'PROCEDIMIENTO', w: 190 },
    { key:'pac',   label:'PACIENTE',      w: 250 }, 
    { key:'tipo',  label:'TIPO',          w: 150 }, 
    { key:'monto', label:'MONTO',         w: 90  } 
  ];


  
  // ✅ Auto-ajuste exacto: para que sumen detW y SIEMPRE cierre el borde derecho
  {
    const sum = detCols.reduce((a,c)=>a + c.w, 0);
    const k = detW / sum;
    detCols = detCols.map(c => ({ ...c, w: Math.round(c.w * k) }));
  
    // ajuste fino por redondeo (dejar exacto)
    const sum2 = detCols.reduce((a,c)=>a + c.w, 0);
    const diff = detW - sum2;
    detCols[detCols.length - 1].w += diff; // corrige en MONTO
  }


  // construimos filas (1 fila por línea, ordenadas)
  const lineSort = (a,b)=>{
    const fa = normalize(a.fecha);
    const fb = normalize(b.fecha);
    if(fa !== fb) return fa.localeCompare(fb);
    const ha = normalize(a.hora);
    const hb = normalize(b.hora);
    if(ha !== hb) return ha.localeCompare(hb);
    return normalize(a.pacienteNombre).localeCompare(normalize(b.pacienteNombre));
  };

  // =========================================================
  // ✅ DETALLE PAGINADO (Página 2, 3, 4...) – sin cortar filas
  // =========================================================

  // helper: dibuja header de la tabla detalle en la página dada
  function drawDetalleHeader(page, topY){
    // ✅ Header VERDE RENNAT
    drawBox(page, detX, topY, detW, detHeadH, RENNAT_GREEN, RENNAT_GREEN, 1);
  
    // labels + líneas verticales
    let cx = detX;
    for(let i=0;i<detCols.length;i++){
      if(i>0) drawVLine(page, cx, topY, detHeadH, 1, BORDER_SOFT);
      drawCellText(page, detCols[i].label, cx, topY, detHeadH, 9, true, rgb(1,1,1), 8);
      cx += detCols[i].w;
    }
  }

  // helper: dibuja 1 fila
  function drawDetalleRow(page, row, topY, rowNumber){
    // línea horizontal superior de la fila
    drawHLine2(page, detX, topY, detW, 1, BORDER_SOFT);
  
    let xPos = detX;
  
    // ✅ 0) # (enumerador)
    drawCellTextRight(page, String(rowNumber), xPos, topY, detCols[0].w, detRowH, 9, true, TEXT_MUTED, 8);
    xPos += detCols[0].w;
  
    // ✅ 1) FECHA
    const fechaTxt = `${row.fecha || ''}${row.hora ? ' ' + row.hora : ''}`;
    drawCellText(page, wrapClip(fechaTxt, 18), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[1].w;
  
    // ✅ 2) CLÍNICA
    drawCellText(page, wrapClip(clinAbbrev(row.clinicaNombre || ''), 22), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[2].w;
  
    // ✅ 3) PROCEDIMIENTO
    drawCellText(page, wrapClip(row.procedimientoNombre || '', 24), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[3].w;
  
    // ✅ 4) PACIENTE
    drawCellText(page, wrapClip(row.pacienteNombre || '', 28), xPos, topY, detRowH, 9, false, TEXT_MAIN, 8);
    xPos += detCols[4].w;
  
    // ✅ 5) TIPO (regla especial + font size condicional)
    let tipoTxt = (row.tipoPaciente || '').toString().toLowerCase().trim();
    let tipoShow = tipoTxt.toUpperCase();
  
    if (tipoTxt === 'particular_isapre' || (tipoTxt.includes('particular') && tipoTxt.includes('isap'))) {
      tipoShow = 'PARTICULAR O ISAPRE';
    }
  
    const tipoFontSize = (tipoShow === 'PARTICULAR O ISAPRE') ? 7.6 : 9;
    drawCellText(page, tipoShow, xPos, topY, detRowH, tipoFontSize, false, TEXT_MUTED, 8);
    xPos += detCols[5].w;
  
    // ✅ 6) MONTO
    drawCellTextRight(page, money(row.monto || 0), xPos, topY, detCols[6].w, detRowH, 9, true, TEXT_MAIN, 8);
  }

  // filas ordenadas completas
  const allLinesSorted = [...(agg?.lines || [])].sort(lineSort);

  // ✅ IMPORTANTE: la caja “DATOS CLÍNICA RENNAT” debe quedar SIEMPRE en la ÚLTIMA página
  // Por eso NO la dibujamos aún. La dibujaremos al final, en la última página real.

  // Límite inferior por defecto (páginas intermedias): podemos usar casi toda la hoja
  const bottomLimitDefault = M + 20;

  // En la última página reservaremos espacio para la caja, pero ese límite lo calcularemos al final.
  const boxGap = 16; // margen sobre la caja

  let currentPage = page2;
  let cursorTopY = y2; // top de la tabla en la página actual
  let rowNumber = 1; // ✅ numeración continua en todo el detalle (todas las páginas)



  // dibuja tabla paginada
  let idx = 0;

  while (idx < allLinesSorted.length) {

    // ¿Estamos en la última página? -> lo sabremos si todo lo que queda cabe acá.
    // Primero asumimos límite “intermedio” y calculamos cuántas filas caben.
    // Luego, si con ese cálculo cabe TODO lo que falta, entonces esta es la última página
    // y cambiamos el bottomLimit para reservar espacio para la caja clínica.

    // 1) Con límite default (página intermedia)
    let bottomLimit = bottomLimitDefault;

    // ¿Cuántas filas caben con límite default?
    let availableH = cursorTopY - bottomLimit - detHeadH;
    let canFit = Math.max(0, Math.floor(availableH / detRowH));

    // Si no cabe ni una fila -> crear página nueva
    if (canFit <= 0) {

      currentPage = pdfDoc.addPage([W2, H2]);
      cursorTopY = H2 - M;
      
      // Barra título en horizontal
      drawBox(currentPage, barX2, cursorTopY, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
      drawText(
        currentPage,
        T_DETALLE,
        barX2 + (barW2 - measure(T_DETALLE, 13, true)) / 2,
        cursorTopY - 19,
        13,
        true,
        rgb(1, 1, 1)
      );

      cursorTopY -= (barH + 12);
      
      // Subtítulo
      drawText(currentPage, `${nombreMostrar} · ${mesTxt}`, M, cursorTopY, 10, false, TEXT_MUTED);
      cursorTopY -= 12;


      continue;
    }

    // ¿Cuántas filas quedan en total?
    const remaining = allLinesSorted.length - idx;

    // ✅ Si con el límite default cabe TODO lo que falta, entonces ESTA es la última página:
    // reservamos espacio para la caja “DATOS CLÍNICA” y recalculamos canFit.
    if (canFit >= remaining) {
      // Dibujamos la caja clínica al final de la hoja (pero OJO: no aún; solo calculamos su espacio)
      // Para calcular espacio, usaremos dimensiones fijas de tu caja (emH=104; emY=M+20)

      // const { emH, emY } = CLINICA_BOX;
      // bottomLimit = emY + emH + boxGap;

      // Recalcular con el bottomLimit de última página
      // availableH = cursorTopY - bottomLimit - detHeadH;
      // canFit = Math.max(0, Math.floor(availableH / detRowH));

      // Si por reservar la caja quedó 0 filas, forzamos nueva página
      // if (canFit <= 0) {

        // currentPage = pdfDoc.addPage([W2, H2]);
        // cursorTopY = H2 - M;
        
        // Barra título en horizontal
        // drawBox(currentPage, barX2, cursorTopY, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
        // drawText(currentPage, t2, barX2 + (barW2 - measure(t2, 13, true)) / 2, cursorTopY - 19, 13, true, rgb(1, 1, 1));
        // cursorTopY -= (barH + 12);
        
        // Subtítulo
        // drawText(currentPage, `${nombreMostrar} · ${mesTxt}`, M, cursorTopY, 10, false, TEXT_MUTED);
        // cursorTopY -= 12;


        // continue;
      // }
    }

    // slice de filas para esta página
    const slice = allLinesSorted.slice(idx, idx + Math.min(canFit, remaining));

    // alto del bloque (header + filas)
    const blockH = detHeadH + slice.length * detRowH;

    // marco total del bloque
    drawBox(currentPage, detX, cursorTopY, detW, blockH, rgb(1, 1, 1), BORDER_SOFT, 1);

    // ✅ AHORA sí dibuja el header (queda arriba, visible)
    drawDetalleHeader(currentPage, cursorTopY);  

    // líneas verticales a todo el bloque
    let cx = detX;
    for (let i = 0; i < detCols.length; i++) {
      if (i > 0) drawVLine(currentPage, cx, cursorTopY, blockH, 1, BORDER_SOFT);
      cx += detCols[i].w;
    }

    // filas  
    for (let r = 0; r < slice.length; r++) {
      const row = slice[r];
      const rowTop = cursorTopY - detHeadH - r * detRowH;
      drawDetalleRow(currentPage, row, rowTop, rowNumber++);
    }


    // cerrar línea inferior del bloque
    const yBottom = cursorTopY - blockH;
    drawHLine2(currentPage, detX, yBottom, detW, 1, BORDER_SOFT);

    // avanzamos índice
    idx += slice.length;

    // ✅ solo crear nueva página si AÚN quedan filas
    if (idx < allLinesSorted.length) {
      currentPage = pdfDoc.addPage([W2, H2]);
      cursorTopY = H2 - M;

      // Barra título
      drawBox(currentPage, barX2, cursorTopY, barW2, barH, RENNAT_BLUE, RENNAT_BLUE, 1);
      drawText(currentPage, T_DETALLE, barX2 + (barW2 - measure(T_DETALLE, 13, true)) / 2, cursorTopY - 19, 13, true, rgb(1, 1, 1));
      cursorTopY -= (barH + 12);

      // Subtítulo
      drawText(currentPage, `${nombreMostrar} · ${mesTxt}`, M, cursorTopY, 10, false, TEXT_MUTED);
      cursorTopY -= 12;
    }

  // termina el while completamente
  }
  
  // ✅ fuera del while → última página real
  // drawClinicaBoxHorizontal(currentPage);
    
  // ✅ Cerrar generación PDF: guardar bytes y retornar
  const pdfBytes = await pdfDoc.save();
  return pdfBytes;
} // ✅ FIN generarPDFLiquidacionProfesional



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
  profesionalesByName: new Map(), // normalize(nombreProfesional) -> profDoc
  profesionalesById: new Map(),   // rutId string -> profDoc

  procedimientosByName: new Map(),   // normalize(nombre) -> procDoc
  procedimientosById: new Map(),     // docId -> procDoc
  procedimientosByCodigo: new Map(), // "PC0001" -> procDoc

  // ✅ NUEVO: Config UF y bonos
  ufDocId: '',             // YYYYMM
  ufValorCLP: 0,           // UF CLP del mes
  bonosTramosGlobal: [],   // config/bonos.tramos

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

    // TU ESQUEMA REAL (según tu Firestore)
    // ✅ docId = RUT
    const rutId = cleanReminder(x.rutId) || d.id;
    
    // Campos visibles en tu doc
    const nombreProfesional = cleanReminder(x.nombreProfesional) || '';
    const razonSocial = cleanReminder(x.razonSocial) || '';
    const rutEmpresa = cleanReminder(x.rutEmpresa) || '';
    const estado = (cleanReminder(x.estado) || 'activo').toLowerCase();
    
    // ✅ Si no guardas "rut" como campo, usa el docId (rutId) como RUT personal
    const rutPersonal = cleanReminder(x.rut) || String(rutId || '');
    
    // ✅ Tipo persona: si viene explícito, úsalo; si no, infiere jurídica si hay razón social
    const tipoPersona = (cleanReminder(x.tipoPersona) || (razonSocial ? 'juridica' : 'natural')).toLowerCase();
    
    // ✅ TU FIRESTORE: rolPrincipalId (no rolPrincipal)
    const rolPrincipalRaw = cleanReminder(x.rolPrincipalId || x.rolPrincipal) || '';
    const rolPrincipalNorm = normalize(rolPrincipalRaw);
    
    const rolPrincipal =
      rolPrincipalNorm === 'r_cirujano' || rolPrincipalNorm === 'cirujano' ? 'r_cirujano' :
      rolPrincipalNorm === 'r_anestesista' || rolPrincipalNorm === 'anestesista' ? 'r_anestesista' :
      rolPrincipalNorm === 'r_arsenalera' || rolPrincipalNorm === 'arsenalera' ? 'r_arsenalera' :
      rolPrincipalNorm === 'r_ayudante_1' || rolPrincipalNorm === 'ayudante1' || rolPrincipalNorm === 'ayudante 1' ? 'r_ayudante_1' :
      rolPrincipalNorm === 'r_ayudante_2' || rolPrincipalNorm === 'ayudante2' || rolPrincipalNorm === 'ayudante 2' ? 'r_ayudante_2' :
      (rolPrincipalRaw || '');
    
    // ✅ tieneBono: si existe el campo, respétalo
    // (si NO existe, quedará false y no aparecerá bono)
    const tieneBono =
      x.tieneBono === true ||
      String(x.tieneBono || '').toLowerCase().trim() === 'true' ||
      String(x.tieneBono || '').trim() === '1';
    
    // Ahora el doc
    const doc = {
      id: String(rutId || d.id),
      rutId: String(rutId || d.id),
    
      nombreProfesional: toUpperSafe(nombreProfesional || ''),
      rut: rutPersonal,
    
      razonSocial: toUpperSafe(razonSocial || ''),
      rutEmpresa: rutEmpresa,
    
      tipoPersona: tipoPersona || '',
      estado,
    
      rolPrincipal,
      tieneBono,
      bonosTramosOverride: Array.isArray(x.bonosTramosOverride) ? x.bonosTramosOverride : null,
      descuentoUF: Number(x.descuentoUF || 0) || 0
    };

    // ✅ Guardar en byId con múltiples variantes para que SIEMPRE matchee
    const keys = new Set();
    
    // doc.id / rutId (lo que usas como id)
    keys.add(String(doc.id || '').trim());
    keys.add(String(doc.rutId || '').trim());
    
    // rut personal (con y sin DV, con puntos, etc.)
    keys.add(String(doc.rut || '').trim());
    
    // además, versiones canonizadas
    keys.add(canonRutAny(doc.id));
    keys.add(canonRutAny(doc.rutId));
    keys.add(canonRutAny(doc.rut));
    
    // guarda
    for(const k of keys){
      const kk = (k ?? '').toString().trim();
      if(kk) byId.set(kk, doc);
    }
    
    if(nombreProfesional) byName.set(normalize(nombreProfesional), doc);

  });

  state.profesionalesByName = byName;
  state.profesionalesById = byId;
}

async function loadProcedimientos(){
  const snap = await getDocs(colProcedimientos);
  const byName = new Map();
  const byId = new Map();
  const byCodigo = new Map();
  
  snap.forEach(d=>{
    const x = d.data() || {};
    const id = d.id;
    const nombre = cleanReminder(x.nombre) || '';
    // ✅ Normaliza "Cirugía", "cirugías", "CIRUGIA", etc. -> "cirugia"
    const tipoRaw = cleanReminder(x.tipo) || '';
    const tipoN = normalize(tipoRaw); // usa tu helper normalize() (quita tildes y baja a minúscula)
    
    const tipo =
      (tipoN.includes('cirug') ? 'cirugia' :
       tipoN.includes('ambula') ? 'ambulatorio' :
       (tipoN || ''));

    const tarifas = (x.tarifas && typeof x.tarifas === 'object') ? x.tarifas : null;

    const doc = {
      id,
      codigo: cleanReminder(x.codigo) || id,
      nombre: toUpperSafe(nombre || id),
      tipo,
      tarifas
    };

    byId.set(String(id), doc);
    
    // ✅ índice por código (PC0001, etc.)
    if(doc.codigo){
      const c = String(doc.codigo).trim().toUpperCase();
      if(c) byCodigo.set(c, doc);
    }
    
    if(nombre) byName.set(normalize(nombre), doc);
  });

  state.procedimientosByName = byName;
  state.procedimientosById = byId;
  state.procedimientosByCodigo = byCodigo;
}

/* =========================
   Config UF + Bonos
========================= */
async function loadBonosConfig(){
  // ✅ Defaults por si config/bonos no existe o viene sin tramos
  const defaultTramos = [
    { min: 11, max: 15, montoCLP: 1000000 },
    { min: 16, max: 20, montoCLP: 1500000 },
    { min: 21, max: 30, montoCLP: 3000000 },
    { min: 31, max: null, montoCLP: 6000000 }
  ];

  try{
    const ref = doc(db, 'config', 'bonos');
    const snap = await getDoc(ref);

    const x = snap.exists() ? (snap.data() || {}) : {};
    const tramos = Array.isArray(x.tramos) ? x.tramos : [];

    // ✅ si viene vacío => usamos defaults
    state.bonosTramosGlobal = tramos.length ? tramos : defaultTramos;

    console.log('BONOS: tramos cargados =', state.bonosTramosGlobal);

  }catch(e){
    console.warn('No se pudo leer config/bonos, usando defaults', e);
    state.bonosTramosGlobal = defaultTramos;
  }
}

async function loadUFDelMes(){
  const id = yyyymm(state.ano, state.mesNum);
  state.ufDocId = id;

  try{
    // ✅ Ruta válida: config/uf/meses/{YYYYMM}
    const ref = doc(db, 'config', 'uf', 'meses', id);
    const snap = await getDoc(ref);
    const x = snap.exists() ? (snap.data() || {}) : {};
    state.ufValorCLP = Number(x.ufValorCLP || 0) || 0;
  }catch(e){
    console.warn('No se pudo leer config/uf/meses/'+id, e);
    state.ufValorCLP = 0;
  }
}

async function saveUFDelMes(nuevoValorCLP){
  const id = yyyymm(state.ano, state.mesNum);
  state.ufDocId = id;

  // ✅ Ruta válida: config/uf/meses/{YYYYMM}
  const ref = doc(db, 'config', 'uf', 'meses', id);

  const payload = {
    ufValorCLP: Number(nuevoValorCLP || 0) || 0,
    fecha: `${state.ano}-${String(state.mesNum).padStart(2,'0')}-01`,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  await setDoc(ref, payload, { merge:true });
  state.ufValorCLP = payload.ufValorCLP;
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

      // equivalentes legacy
      'particular',
      'isapre'
    );
  }

  // ✅ Bucket "MLE": aceptar equivalencias en ambos sentidos
  const esBucketMLE =
    tpn === 'mle' ||
    tpn.includes('mle') ||
    tpn.includes('libre eleccion') ||
    tpn.includes('libre elección') ||
    tpn.replace(/[^a-z0-9]+/g,'').includes('modalidadlibreeleccion');

  if(esBucketMLE){
    candidates.push(
      'mle',
      'MLE',
      'M.L.E',
      'm.l.e',
      'Modalidad Libre Eleccion',
      'MODALIDAD LIBRE ELECCION',
      'Libre Eleccion',
      'LIBRE ELECCION'
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

// Helper local: extrae PC0001/PC0002 etc incluso si viene mezclado en texto
function extractPC(v=''){
  const s = (v ?? '').toString().toUpperCase();
  const m = s.match(/PC\d{3,6}/);   // busca PC + 3..6 dígitos dentro del texto
  return m ? m[0] : '';
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

    // ✅ Preferir resolución manual si existe (staging/producción)
    const resolved = (x.resolved && typeof x.resolved === 'object') ? x.resolved : {};
    const sel = (x._selectedIds && typeof x._selectedIds === 'object') ? x._selectedIds : {};
    
    // Clínica (label siempre con lo que venga, pero alert si no existe en catálogo)
    const clinicaId =
      cleanReminder(resolved.clinicaId) ||
      cleanReminder(sel.clinicaId) ||
      cleanReminder(x.clinicaId) ||
      '';
    
    const clinicaNameRaw = toUpperSafe(cleanReminder(x.clinica || pickRaw(raw,'Clínica')));
    const clinicaLabel = clinicaId
      ? (state.clinicasById.get(clinicaId) || clinicaNameRaw || clinicaId)
      : (clinicaNameRaw || '(Sin clínica)');
    
    const clinicaExists = !!(clinicaId && state.clinicasById.has(clinicaId));

    // Procedimiento
    // ✅ OBJETIVO: encontrar SIEMPRE el procedimiento aunque Producción guarde en campos variados
    // - por código PC (PC0001)
    // - por id (procedimientoId/cirugiaId)
    // - por nombre (procedimientoNombre/cirugia/etc.)
    // - por normalizado (x.normalizado)
    
    const norm = (x.normalizado && typeof x.normalizado === 'object') ? x.normalizado : {};
    
    // 1) RAW: probar varias keys típicas
    const rawProcField = cleanReminder(
      pickRaw(raw,'Procedimiento') ||
      pickRaw(raw,'Procedimiento / Cirugía') ||
      pickRaw(raw,'Procedimiento/Cirugía') ||
      pickRaw(raw,'Prestación') ||
      pickRaw(raw,'Prestacion') ||
      pickRaw(raw,'Cirugía') ||
      pickRaw(raw,'Cirugia') ||
      pickRaw(raw,'Nombre Cirugía') ||
      pickRaw(raw,'Nombre Cirugia') ||
      pickRaw(raw,'Nombre Procedimiento')
    );
    
    const rawPC = extractPC(rawProcField);
    
    // 2) IDs/códigos desde item (x)
    const procIdFromX = cleanReminder(
      x.procedimientoId ||
      x.procId ||
      x.cirugiaId ||
      x.ambulatorioId ||
      x.procedimientoCodigo ||
      x.codigoProcedimiento ||
      x.codigo ||
      x.procedimiento ||       // a veces guardan el código acá
      ''
    );
    
    const xPC = extractPC(procIdFromX);
    
    // 3) IDs/códigos desde normalizado (si existe)
    const procIdFromNorm = cleanReminder(
      norm.procedimientoId ||
      norm.procId ||
      norm.cirugiaId ||
      norm.ambulatorioId ||
      norm.codigoProcedimiento ||
      norm.procedimientoCodigo ||
      norm.codigo ||
      norm.procedimiento ||
      ''
    );
    
    const normPC = extractPC(procIdFromNorm);
    
    // 4) IDs/códigos desde resolución manual (resolved/selected)
    const resolvedObj = (x.resolved && typeof x.resolved === 'object') ? x.resolved : {};
    const selObj = (x._selectedIds && typeof x._selectedIds === 'object') ? x._selectedIds : {};
    
    const resolvedProcAny = cleanReminder(
      resolvedObj.procedimientoId ||
      resolvedObj.procId ||
      resolvedObj.cirugiaId ||
      resolvedObj.ambulatorioId ||
      resolvedObj.codigoProcedimiento ||
      resolvedObj.procedimiento ||
      ''
    );
    const resolvedPC = extractPC(resolvedProcAny);
    
    const selectedProcAny = cleanReminder(
      selObj.procedimientoId ||
      selObj.procId ||
      selObj.cirugiaId ||
      selObj.ambulatorioId ||
      selObj.codigoProcedimiento ||
      selObj.procedimiento ||
      ''
    );
    const selectedPC = extractPC(selectedProcAny);
    
    // 5) Nombre (fallback) desde x / normalizado / raw
    const cirugiaNameRaw = toUpperSafe(cleanReminder(
      x.cirugia ||
      x.cirugiaNombre ||
      x.nombreCirugia ||
      x.procedimientoNombre ||
      x.nombreProcedimiento ||
      norm.cirugia ||
      norm.cirugiaNombre ||
      norm.procedimientoNombre ||
      rawProcField // si venía texto en Procedimiento, úsalo como nombre
    ));
    
    // ✅ Regla de oro:
    // - PRIORIDAD 1: código PC
    // - PRIORIDAD 2: id directo
    // - PRIORIDAD 3: nombre
    const procCodeCandidate =
      resolvedPC ||
      selectedPC ||
      normPC ||
      xPC ||
      rawPC ||
      '';
    
    const procIdCandidate =
      resolvedProcAny ||
      selectedProcAny ||
      procIdFromNorm ||
      procIdFromX ||
      '';
    
    // ✅ Lookup robusto
    const procDoc =
      (procCodeCandidate && state.procedimientosByCodigo?.get(procCodeCandidate)) ||
      (procIdCandidate && state.procedimientosById.get(String(procIdCandidate).trim())) ||
      (cirugiaNameRaw && state.procedimientosByName.get(normalize(cirugiaNameRaw))) ||
      null;
    
    // ✅ Para mostrar en UI/export:
    const procLabel = procDoc?.nombre || cirugiaNameRaw || '(Sin procedimiento)';
    const procRealId = (procDoc?.codigo || procDoc?.id || procCodeCandidate || procIdCandidate || '').toString();
    
    const procedimientoExists = !!procDoc;
    const procedimientoTipo = (procDoc?.tipo || '').toLowerCase().trim(); // "cirugia" | ...


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

      // ✅ Preferir IDs resueltos manualmente
      const resolved = (x.resolved && typeof x.resolved === 'object') ? x.resolved : {};
      const sel = (x._selectedIds && typeof x._selectedIds === 'object') ? x._selectedIds : {};
      const profIdsResolved = (resolved.profIds && typeof resolved.profIds === 'object') ? resolved.profIds : {};
      
      const profIdRaw =
        cleanReminder(profIdsResolved?.[rf.idKey]) ||      // resolved.profIds.cirujanoId, etc
        cleanReminder(resolved?.[rf.idKey]) ||             // resolved.cirujanoId directo, etc
        cleanReminder(sel?.profIds?.[rf.idKey]) ||         // _selectedIds.profIds.cirujanoId, etc
        cleanReminder(sel?.[rf.idKey]) ||                  // _selectedIds.cirujanoId directo, etc
        cleanReminder(x.profesionalesId?.[rf.idKey]) ||     // legacy
        '';

      if(!profNameRaw && !profIdRaw) continue;

      // Buscar en catálogo: por ID o por nombre personal
      // ✅ Normaliza el ID que venga desde producción (puede venir "14.145.305-K" o "14145305")
      const profIdCanon = canonRutAny(profIdRaw);
      
      const profDoc =
        // 1) match por ID tal cual
        (profIdRaw && state.profesionalesById.get(String(profIdRaw).trim())) ||
      
        // 2) match por ID canon (sin puntos/guiones)
        (profIdCanon && state.profesionalesById.get(profIdCanon)) ||
      
        // 3) si el docId está guardado sin DV (ej: 14145305), y el canon viene con DV (14145305K),
        // intentamos también con "solo dígitos" (remueve K al final si existe)
        (profIdCanon && state.profesionalesById.get(profIdCanon.replace(/K$/,''))) ||
      
        // 4) fallback por nombre
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

      // Si NO viene ningún procedimiento por ningún lado → es problema del item (import/producción), no del catálogo
      const procVacio = !procCodeCandidate && !procIdCandidate && !cirugiaNameRaw;
      
      if(procVacio){
        alerts.push('Procedimiento vacío en Producción (import)');
        console.log('[PROC VACIO]', {
          prodId: row.id,
          keysX: Object.keys(x || {}),
          keysRaw: Object.keys(raw || {}),
          normalizado: (x.normalizado && typeof x.normalizado === 'object') ? x.normalizado : null,
          resolved: x.resolved || null,
          selected: x._selectedIds || null
        });
      
      } else if(!procedimientoExists){
        alerts.push('Procedimiento no mapeado (catálogo)');
        console.log('[PROC NO MAP]', {
          prodId: row.id,
          procCodeCandidate,
          procIdCandidate,
          rawProcField,
          cirugiaNameRaw,
          resolved: x.resolved || null,
          selected: x._selectedIds || null
        });
      }

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
        procedimientoTipo,

        tipoPaciente: pacienteTipo,
        pacienteNombre,

        roleId: rf.roleId,
        roleNombre: state.rolesMap.get(rf.roleId) || rf.label,

        // Profesional (titular siempre persona)
        profesionalNombre: titularNombre,
        profesionalId: (profDoc?.id || profIdCanon || profIdRaw || '').toString(),
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
      
        // ✅ NUEVO: flags profesionales (para bono/descuento)
        rolPrincipal: '',
        tieneBono: false,
        bonosTramosOverride: null,
        descuentoUF: 0,
      
        // ✅ NUEVO: ajustes calculados (se setean al final)
        ajustes: {
          ufValorCLP: 0,
          descuentoUF: 0,
          descuentoCLP: 0,
          cirugiasComoPrincipal: 0,
          bonoCLP: 0,
          bonoTramo: null,
          totalProcedimientos: 0,
          totalAPagar: 0
        },
      
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

    // ✅ Traer flags del profesional desde catálogo (si existe)
    if(ln.profesionalId && state.profesionalesById.has(String(ln.profesionalId))){
      const pd = state.profesionalesById.get(String(ln.profesionalId));
      if(pd){
        if(!agg.rolPrincipal && pd.rolPrincipal) agg.rolPrincipal = pd.rolPrincipal;
        if(agg.tieneBono === false && pd.tieneBono === true) agg.tieneBono = true;
        if(!agg.bonosTramosOverride && pd.bonosTramosOverride) agg.bonosTramosOverride = pd.bonosTramosOverride;
        if(!agg.descuentoUF && pd.descuentoUF) agg.descuentoUF = pd.descuentoUF;
      }
    }


    agg.lines.push(ln);
  }

  const resumen = [...map.values()].map(x=>{
    let status = 'ok';
    if(x.alertasCount > 0) status = 'alerta';
    else if(x.pendientesCount > 0) status = 'pendiente';
  
    // =========================
    // ✅ AJUSTES (Descuento UF + Bono)
    // =========================
    const totalProcedimientos = Number(x.total || 0) || 0;
  
    // DESCUENTO (si descuentoUF > 0 y ufValorCLP > 0)
    const uf = Number(state.ufValorCLP || 0) || 0;
    const descuentoUF = Number(x.descuentoUF || 0) || 0;
    const descuentoCLP = (descuentoUF > 0 && uf > 0) ? Math.round(descuentoUF * uf) : 0;
  
    // BONO (solo si es cirujano principal + tieneBono)
    const cirugiasComoPrincipal = (x.rolPrincipal === 'r_cirujano')
      ? (x.lines || []).filter(l => l.roleId === 'r_cirujano' && (l.procedimientoTipo === 'cirugia')).length
      : 0;
  
    let bonoCLP = 0;
    let bonoTramo = null;
  
    const aplicaBono =
      (x.rolPrincipal === 'r_cirujano') &&
      (cirugiasComoPrincipal > 0) &&
      (x.tieneBono !== false); 

    let bonoTramoIndex = 0;
    
    if(aplicaBono){
      const tramos = Array.isArray(x.bonosTramosOverride) ? x.bonosTramosOverride : state.bonosTramosGlobal;
      const tramo = pickTramo(tramos, cirugiasComoPrincipal);
    
      if(tramo && (Number(tramo.montoCLP || 0) > 0)){
        bonoCLP = Number(tramo.montoCLP || 0) || 0;
        bonoTramo = tramo;
    
        // ✅ detectar índice del tramo (1-based) para mostrar "Tramo 1"
        const idx = tramos.findIndex(t=>{
          const min = Number(t?.min ?? 0) || 0;
          const max = (t?.max === null || t?.max === undefined || t?.max === '') ? null : (Number(t.max) || 0);
    
          const montoRaw = (t?.montoCLP ?? t?.monto ?? t?.bonoCLP ?? t?.bono ?? 0);
          const monto = asNumberLoose(montoRaw);
    
          return (min === tramo.min) &&
                 ((max ?? null) === (tramo.max ?? null)) &&
                 (monto === (Number(tramo.montoCLP || 0) || 0));
        });
    
        bonoTramoIndex = (idx >= 0) ? (idx + 1) : 0;
      }
    }

    const totalAPagar = Math.max(0, totalProcedimientos - descuentoCLP + bonoCLP);
  
    x.ajustes = {
      ufValorCLP: uf,
      descuentoUF,
      descuentoCLP,
      cirugiasComoPrincipal,
      bonoCLP,
      bonoTramo,
      bonoTramoIndex, // ✅ NUEVO
      totalProcedimientos,
      totalAPagar
    };

  
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
      <td><b>${clp(agg?.ajustes?.totalAPagar ?? agg.total)}</b></td>
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

  $('modalPillTotal').textContent = `TOTAL: ${clp(agg?.ajustes?.totalAPagar ?? agg.total)}`;
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

    // ✅ Cargar UF del mes seleccionado (config/uf/YYYYMM)
    await loadUFDelMes();

    buildLiquidaciones();
    
    // 🔎 DEBUG: confirmar tramos de bonos cargados
    console.log('BONOS tramos global (state.bonosTramosGlobal)=', state.bonosTramosGlobal);
    console.table((state.bonosTramosGlobal || []).map(t => ({
      min: t?.min,
      max: t?.max,
      montoCLP: t?.montoCLP,
      monto: t?.monto,
      bonoCLP: t?.bonoCLP
    })));

    // 🔎 DEBUG: ver si hay cirujanos con bono detectados
    const dbg = state.liquidResumen
      .filter(x => x.rolPrincipal === 'r_cirujano')
      .slice(0, 10)
      .map(x => ({
        nombre: x.nombre,
        rolPrincipal: x.rolPrincipal,
        tieneBono: x.tieneBono,
        cirugiasComoPrincipal: x.ajustes?.cirugiasComoPrincipal,
        bonoCLP: x.ajustes?.bonoCLP,
        totalProcedimientos: x.ajustes?.totalProcedimientos,
        totalAPagar: x.ajustes?.totalAPagar
      }));
    console.table(dbg);

    
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

    $('btnUF')?.addEventListener('click', ()=>{
      $('ufBackdrop').style.display = 'grid';
      $('ufSub').textContent = `Mes: ${monthNameEs(state.mesNum)} ${state.ano} · Doc: config/uf/meses/${yyyymm(state.ano, state.mesNum)}`;
      $('ufValor').value = state.ufValorCLP ? String(state.ufValorCLP) : '';
    });
    
    function closeUF(){
      $('ufBackdrop').style.display = 'none';
    }
    
    $('btnUfClose')?.addEventListener('click', closeUF);
    $('btnUfCancelar')?.addEventListener('click', closeUF);
    $('ufBackdrop')?.addEventListener('click', (e)=>{
      if(e.target === $('ufBackdrop')) closeUF();
    });

    
    $('btnUfGuardar')?.addEventListener('click', async ()=>{
      try{
        const v = asNumberLoose($('ufValor').value);
        if(!v || v < 1000){ toast('UF inválida (ej: 37000)'); return; }
        await saveUFDelMes(v);
        toast('UF guardada');
        $('ufBackdrop').style.display = 'none';
        await recalc();
      }catch(e){
        console.error(e);
        toast('No se pudo guardar UF (ver consola)');
      }
    });


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
      loadProcedimientos(),
      loadBonosConfig()
    ]);

    await recalc();
  }
});
