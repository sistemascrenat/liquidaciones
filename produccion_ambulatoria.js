// produccion_ambulatoria.js — COMPLETO

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, wireLogout, toast as uiToast } from './ui.js';
import { loadSidebar } from './layout.js';

import {
  collection,
  collectionGroup,
  doc,
  getDoc,
  getDocs,
  setDoc,
  writeBatch,
  serverTimestamp,
  query,
  where,
  limit,
  orderBy,
  startAfter
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js";

const $ = id => document.getElementById(id);

/* ======================
   TOAST / STATUS
====================== */

function toast(msg) {
  try {
    if (typeof uiToast === 'function') return uiToast(msg);
  } catch {}
  console.log(msg);
}

function setStatus(text) {
  if ($("statusInfo")) $("statusInfo").textContent = text || "—";
}

/* ======================
   FIRESTORE PATHS
====================== */

const colAmbImports = collection(db, "produccion_ambulatoria_imports");

/*
  Confirmado final:
  produccion_ambulatoria/{YYYY}/meses/{MM}/pacientes/{RUT}/items/{itemId}
*/

/* ======================
   DEFAULT MES / AÑO
====================== */

function setDefaultToPreviousMonth() {
  const meses = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
  ];

  const d = new Date();
  d.setDate(1);
  d.setMonth(d.getMonth() - 1);

  if ($("mes")) $("mes").value = meses[d.getMonth()];
  if ($("ano")) $("ano").value = String(d.getFullYear());
}

/* ======================
   STATE
====================== */

let dataReservo = [];
let dataMK = [];
let profesionales = [];
let procedimientos = [];
let consolidado = [];

let stateEdicion = {
  actual: null
};

let manualOverrides = {};

let stateImport = {
  user: null,
  importId: "",
  status: "idle", // idle | staged | confirmada | anulada
  monthName: "",
  monthNum: 0,
  year: 0,
  filenameReservo: "",
  filenameMK: ""
};

let uiState = {
  q: "",
  page: 0,
  pageSize: 60,
  mostrarNoAplica: false,
  incluirKinesiologia: false, // ✅ por defecto Kine queda oculta
  resolverFiltro: "base" // base | pendientes | aplica | no_aplica | revisar | todos
};

/* ======================
   HELPERS
====================== */

function clean(v) {
  return (v ?? "").toString().trim();
}

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizarTexto(t) {
  return clean(t)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
}

function normalizarRut(rut) {
  if (!rut) return "";
  return rut
    .toString()
    .replace(/\./g, "")
    .replace(/-/g, "")
    .replace(/\s+/g, "")
    .trim()
    .toUpperCase();
}

function normalizarRutKey(rut) {
  return normalizarRut(rut).replace(/[^0-9K]/g, "");
}

function normalizarPaciente(t) {
  return normalizarTexto(t).replace(/\s+/g, " ").trim();
}

function normalizarFecha(fecha) {
  if (fecha === null || fecha === undefined || fecha === "") return "";

  try {
    if (typeof fecha === "number" && window.XLSX?.SSF?.parse_date_code) {
      const p = window.XLSX.SSF.parse_date_code(fecha);
      if (p && p.y && p.m && p.d) {
        const mm = String(p.m).padStart(2, "0");
        const dd = String(p.d).padStart(2, "0");
        return `${p.y}-${mm}-${dd}`;
      }
    }

    const t = clean(fecha);

    let m = t.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (m) {
      return `${m[1]}-${String(m[2]).padStart(2,'0')}-${String(m[3]).padStart(2,'0')}`;
    }

    m = t.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
    if (m) {
      return `${m[3]}-${String(m[2]).padStart(2,'0')}-${String(m[1]).padStart(2,'0')}`;
    }

    const d = new Date(t);
    if (!isNaN(d)) return d.toISOString().slice(0, 10);

    return "";
  } catch {
    return "";
  }
}

function normalizarMonto(v) {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;

  let s = String(v).trim();
  if (!s || s === "-") return 0;

  s = s.replace(/\$/g, "").replace(/\s+/g, "");

  if (/^\(.*\)$/.test(s)) {
    s = "-" + s.slice(1, -1);
  }

  const lastDot = s.lastIndexOf(".");
  const lastComma = s.lastIndexOf(",");

  if (lastDot !== -1 && lastComma !== -1) {
    if (lastDot > lastComma) {
      s = s.replace(/,/g, "");
    } else {
      s = s.replace(/\./g, "").replace(",", ".");
    }
  } else if (lastComma !== -1) {
    const parts = s.split(",");
    if (parts.length === 2 && parts[1].length <= 2) {
      s = s.replace(",", ".");
    } else {
      s = s.replace(/,/g, "");
    }
  } else if (lastDot !== -1) {
    const parts = s.split(".");
    if (!(parts.length === 2 && parts[1].length <= 2)) {
      s = s.replace(/\./g, "");
    }
  }

  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function leerExcel(file) {
  return new Promise(resolve => {
    const reader = new FileReader();

    reader.onload = e => {
      const data = new Uint8Array(e.target.result);
      const workbook = XLSX.read(data, { type: 'array' });
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      const json = XLSX.utils.sheet_to_json(sheet, { defval: '' });
      resolve(json);
    };

    reader.readAsArrayBuffer(file);
  });
}

function monthIndex(name) {
  const m = normalizarTexto(name);
  const map = {
    'ENERO':1,'FEBRERO':2,'MARZO':3,'ABRIL':4,'MAYO':5,'JUNIO':6,
    'JULIO':7,'AGOSTO':8,'SEPTIEMBRE':9,'OCTUBRE':10,'NOVIEMBRE':11,'DICIEMBRE':12
  };
  return map[m] || 0;
}

function pad(n, w = 2) {
  const s = String(n ?? "");
  return s.length >= w ? s : ("0".repeat(w - s.length) + s);
}

function nowId() {
  const d = new Date();
  return [
    d.getFullYear(),
    pad(d.getMonth() + 1),
    pad(d.getDate())
  ].join("") + "_" + [
    pad(d.getHours()),
    pad(d.getMinutes()),
    pad(d.getSeconds())
  ].join("");
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function monthId(year, monthNum) {
  return `${year}-${pad(monthNum, 2)}`;
}

function makeImportId() {
  return `AMB_${stateImport.year}_${pad(stateImport.monthNum, 2)}_${nowId()}`;
}

function finalItemId(it) {
  const fecha = clean(it.fechaNorm || "SINFECHA");
  return `${fecha}_${clean(it.itemId || nowId())}`;
}

function esItemConfirmable(it) {
  return (
    it?.aplicacion?.estado === "aplica" &&
    it?.review?.estadoRevision === "ok"
  );
}

function badgeConfirmacionHTML(it) {
  if (it?.confirmadoEnProduccion) {
    return `<span class="ok">Confirmado</span>`;
  }

  if (esItemConfirmable(it)) {
    return `<span class="chip" style="background:#fff7ed;color:#9a3412;border:1px solid #fdba74;">Listo para confirmar</span>`;
  }

  return `<span class="muted">Pendiente</span>`;
}

function buildOriginalCsvForItem(reg) {
  return {
    fecha: reg.fecha || "",
    rut: reg.rut || "",
    paciente: reg.paciente || "",
    profesional: reg.profesional || "",
    prestacion: reg.prestacion || "",
    origen: reg.origen || ""
  };
}

/* ======================
   CARGA CATÁLOGOS
====================== */

async function cargarProfesionales() {
  const snap = await getDocs(collection(db, "profesionales"));

  profesionales = snap.docs.map(d => ({
    id: d.id,
    ...d.data()
  }));
}

async function cargarProcedimientos() {
  const snap = await getDocs(collection(db, "procedimientos"));

  procedimientos = snap.docs.map(d => ({
    id: d.id,
    ...d.data()
  }));
}

/* ======================
   BÚSQUEDA CATÁLOGOS
====================== */

function nombreProfesionalCatalogo(p) {
  return p?.nombreProfesional || p?.nombre || p?.nombreCompleto || p?.id || "";
}

function nombreProcedimientoCatalogo(p) {
  return p?.nombre || p?.procedimiento || p?.descripcion || p?.id || "";
}

function tokensNombreComparacion(texto, { ignorarTitulos = false } = {}) {
  const ignorar = new Set([
    "DR", "DRA", "DOC", "DOCTOR", "DOCTORA",
    "NUT", "NUTRICIONISTA",
    "KINE", "KINESIOLOGO", "KINESIOLOGA",
    "PS", "PSICOLOGO", "PSICOLOGA",
    "FONO", "FONOAUDIOLOGO", "FONOAUDIOLOGA",
    "TM", "TENS", "EU", "MAT", "MED"
  ]);

  return normalizarTexto(texto)
    .replace(/\./g, " ")
    .replace(/[^A-Z0-9Ñ\s]/g, " ")
    .split(/\s+/)
    .map(x => x.trim())
    .filter(x => x && x.length > 2 && (!ignorarTitulos || !ignorar.has(x)));
}

function analizarBusquedaProfesional(texto) {
  const textoOriginal = clean(texto);
  const tokensTexto = tokensNombreComparacion(textoOriginal, { ignorarTitulos: true });

  if (!tokensTexto.length) {
    return {
      profesional: null,
      score: 0,
      ambiguo: false,
      candidatos: [],
      alerta: textoOriginal ? "No se pudo interpretar el nombre del profesional" : "Profesional vacío"
    };
  }

  const candidatos = [];

  for (const p of profesionales) {
    const nombreCatalogo = nombreProfesionalCatalogo(p);
    const tokensCatalogo = tokensNombreComparacion(nombreCatalogo);

    if (!tokensCatalogo.length) continue;

    const interseccion = tokensCatalogo.filter(tk => tokensTexto.includes(tk));
    const coinc = interseccion.length;

    if (!coinc) continue;

    let score = coinc;

    // bonus si el primer token del archivo aparece en el catálogo
    if (tokensTexto[0] && tokensCatalogo.includes(tokensTexto[0])) score += 0.75;

    // bonus si el segundo token del archivo también aparece
    if (tokensTexto[1] && tokensCatalogo.includes(tokensTexto[1])) score += 0.5;

    // bonus si el texto completo del catálogo está parcialmente contenido
    const normArchivo = normalizarTexto(textoOriginal).replace(/\./g, " ");
    const normCatalogo = normalizarTexto(nombreCatalogo).replace(/\./g, " ");
    if (normArchivo.includes(normCatalogo) || normCatalogo.includes(normArchivo)) {
      score += 0.5;
    }

    candidatos.push({
      profesional: p,
      nombre: nombreCatalogo,
      score,
      coincidencias: interseccion
    });
  }

  candidatos.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.nombre.localeCompare(b.nombre, "es", { sensitivity: "base" });
  });

  const mejor = candidatos[0] || null;
  const segundo = candidatos[1] || null;

  // Seguridad mínima: exigir al menos 2 coincidencias útiles
  if (!mejor || mejor.score < 2) {
    return {
      profesional: null,
      score: mejor?.score || 0,
      ambiguo: false,
      candidatos,
      alerta: `No se encontró coincidencia suficientemente segura para "${textoOriginal}"`
    };
  }

  // Ambigüedad: si el segundo está muy cerca del primero, no decidir automático
  const esAmbiguo = !!(
    segundo &&
    segundo.score >= 2 &&
    Math.abs(mejor.score - segundo.score) <= 0.5
  );

  if (esAmbiguo) {
    return {
      profesional: null,
      score: mejor.score,
      ambiguo: true,
      candidatos,
      alerta:
        `Coincidencia ambigua en profesional: "${textoOriginal}" ` +
        `podría ser "${mejor.nombre}" o "${segundo.nombre}"`
    };
  }

  return {
    profesional: mejor.profesional,
    score: mejor.score,
    ambiguo: false,
    candidatos,
    alerta: null
  };
}

function buscarProfesional(texto) {
  return analizarBusquedaProfesional(texto)?.profesional || null;
}

function buscarProcedimiento(texto) {
  const t = normalizarTexto(texto);
  if (!t) return null;

  return procedimientos.find(p => {
    const nombre = normalizarTexto(nombreProcedimientoCatalogo(p));
    if (!nombre) return false;
    return nombre === t || t.includes(nombre) || nombre.includes(t);
  }) || null;
}

/* ======================
   REVISIÓN / APLICACIÓN
====================== */

function construirReview({ profesionalId, procedimientoId, alertas = [] }) {
  const pendienteProfesional = !profesionalId;
  const pendienteProcedimiento = !procedimientoId;

  return {
    estadoRevision: (!pendienteProfesional && !pendienteProcedimiento) ? "ok" : "pendiente",
    pendientes: {
      profesional: pendienteProfesional,
      procedimiento: pendienteProcedimiento
    },
    alertas
  };
}

function construirAplicacion(estado, motivo) {
  return { estado, motivo };
}

/* ======================
   RESERVO: ESTADOS
====================== */

function clasificarEstadoCitaReservo(v) {
  const t = normalizarTexto(v);
  if (!t) return "otro";
  if (t.includes("ATENDID")) return "atendido";
  if (t.includes("NO LLEGO")) return "no_llego";
  return "otro";
}

function clasificarEstadoPagoReservo(v) {
  const t = normalizarTexto(v);
  if (!t) return "otro";

  if (t.includes("NO PAG")) return "no_pagado";

  // ✅ NUEVO: "PLAN" se considera positivo igual que pagado
  if (t.includes("PAGAD")) return "pagado";
  if (t.includes("PLAN")) return "plan";
  if (t.includes("DESCARTAD")) return "descartado";

  return "otro";
}

function evaluarAplicacionReservo(raw) {
  const estadoCita = clasificarEstadoCitaReservo(raw["Estado cita"]);
  const estadoPago = clasificarEstadoPagoReservo(raw["Estado pago"]);

  const alertas = [];

  // ✅ Casos que SÍ aplican
  if (estadoCita === "atendido" && estadoPago === "pagado") {
    return {
      aplicacion: construirAplicacion("aplica", "Atendido y pagado"),
      alertas
    };
  }

  // ✅ NUEVO: PLAN se considera positivo
  if (estadoCita === "atendido" && estadoPago === "plan") {
    return {
      aplicacion: construirAplicacion("aplica", "Atendido y plan"),
      alertas
    };
  }

  if (estadoCita === "no_llego" && estadoPago === "pagado") {
    return {
      aplicacion: construirAplicacion("aplica", "No llegó y pagado"),
      alertas
    };
  }

  // ✅ NUEVO: PLAN se considera positivo
  if (estadoCita === "no_llego" && estadoPago === "plan") {
    return {
      aplicacion: construirAplicacion("aplica", "No llegó y plan"),
      alertas
    };
  }

  if (estadoCita === "atendido" && estadoPago === "descartado") {
    return {
      aplicacion: construirAplicacion("aplica", "Atendido y descartado"),
      alertas
    };
  }

  // Casos para revisar
  if (estadoCita === "no_llego" && estadoPago === "descartado") {
    alertas.push("Revisar: no llegó y descartado");
    return {
      aplicacion: construirAplicacion("revisar", "No llegó y descartado"),
      alertas
    };
  }

  if (estadoCita === "otro" && (estadoPago === "pagado" || estadoPago === "plan" || estadoPago === "descartado")) {
    alertas.push("Inconsistencia: pago/plan/descartado sin estado cita reconocido");
    return {
      aplicacion: construirAplicacion("revisar", "Pago/plan/descartado sin estado cita reconocido"),
      alertas
    };
  }

  if (estadoCita === "atendido" && estadoPago === "no_pagado") {
    return {
      aplicacion: construirAplicacion("no_aplica", "Atendido sin pago"),
      alertas
    };
  }

  return {
    aplicacion: construirAplicacion("no_aplica", "Combinación no válida para liquidar"),
    alertas
  };
}

/* ======================
   MK: APLICACIÓN
====================== */

function claveMK(item) {
  return [
    item.fechaNorm || "",
    item.rutNorm || "",
    normalizarTexto(item.profesional || ""),
    normalizarTexto(item.prestacion || ""),
    normalizarPaciente(item.paciente || ""),
    String(Math.abs(item.valor || 0))
  ].join("|");
}

function evaluarAplicacionMK(itemsMK) {
  const grupos = new Map();

  for (const it of itemsMK) {
    const key = claveMK(it);
    if (!grupos.has(key)) grupos.set(key, []);
    grupos.get(key).push(it);
  }

  for (const [, group] of grupos.entries()) {
    const positives = group.filter(x => x.valor > 0).sort((a,b) => a.sourceIndex - b.sourceIndex);
    const negatives = group.filter(x => x.valor < 0).sort((a,b) => a.sourceIndex - b.sourceIndex);
    const zeros = group.filter(x => x.valor === 0);

    for (const z of zeros) {
      z.aplicacion = construirAplicacion("no_aplica", "Valor cero");
    }

    const pares = Math.min(positives.length, negatives.length);

    for (let i = 0; i < pares; i++) {
      positives[i].aplicacion = construirAplicacion("no_aplica", "Positivo anulado por negativo");
      negatives[i].aplicacion = construirAplicacion("no_aplica", "Negativo de anulación");
    }

    for (let i = pares; i < positives.length; i++) {
      positives[i].aplicacion = construirAplicacion("aplica", "Positivo sin anulación");
    }

    for (let i = pares; i < negatives.length; i++) {
      negatives[i].aplicacion = construirAplicacion("revisar", "Negativo sin positivo equivalente");
      negatives[i]._extraAlertas = [...(negatives[i]._extraAlertas || []), "Inconsistencia: negativo sin positivo equivalente"];
    }
  }

  return itemsMK;
}

/* ======================
   MANUAL OVERRIDES
====================== */

function aplicarManualOverrides(items) {
  for (const it of items) {
    const ov = manualOverrides[it.itemId];
    if (!ov) continue;

    if (Object.prototype.hasOwnProperty.call(ov, "profesionalId")) {
      const p = profesionales.find(x => x.id === ov.profesionalId) || null;
      it.resolved.profesionalId = p?.id || null;
      it.resolved.profesionalNombre = p ? nombreProfesionalCatalogo(p) : null;
      it.resolved.confirmadoManualProfesional = !!ov.profesionalId;
    }

    if (Object.prototype.hasOwnProperty.call(ov, "procedimientoId")) {
      const p = procedimientos.find(x => x.id === ov.procedimientoId) || null;
      it.resolved.procedimientoId = p?.id || null;
      it.resolved.procedimientoNombre = p ? nombreProcedimientoCatalogo(p) : null;
      it.resolved.confirmadoManualProcedimiento = !!ov.procedimientoId;
    }

    it.profesionalDetectado = it.resolved.profesionalNombre;
    it.procedimientoDetectado = it.resolved.procedimientoNombre;

    it.review = construirReview({
      profesionalId: it.resolved.profesionalId,
      procedimientoId: it.resolved.procedimientoId,
      alertas: it.review?.alertas || []
    });
  }
}

/* ======================
   PROCESAR RESERVO
====================== */

function procesarReservo() {
  return dataReservo.map((r, i) => {
    const analisisProf = analizarBusquedaProfesional(r["Profesional"]);
    const profesionalDetectado = analisisProf?.profesional || null;
    const procedimientoDetectado = buscarProcedimiento(r["Tratamiento"]);

    const evalApp = evaluarAplicacionReservo(r);
    const alertas = [...evalApp.alertas];

    if (analisisProf?.alerta) alertas.push(analisisProf.alerta);

    if (!normalizarRut(r["Rut"])) alertas.push("RUT vacío o inválido");
    if (!normalizarTexto(r["Profesional"])) alertas.push("Profesional vacío");
    if (!normalizarTexto(r["Tratamiento"])) alertas.push("Procedimiento vacío");

    const resolved = {
      profesionalId: profesionalDetectado?.id || null,
      profesionalNombre: profesionalDetectado ? nombreProfesionalCatalogo(profesionalDetectado) : null,

      procedimientoId: procedimientoDetectado?.id || null,
      procedimientoNombre: procedimientoDetectado ? nombreProcedimientoCatalogo(procedimientoDetectado) : null,

      autoProfesional: !!profesionalDetectado,
      autoProcedimiento: !!procedimientoDetectado,
      confirmadoManualProfesional: false,
      confirmadoManualProcedimiento: false
    };

    return {
      itemId: `RES_${String(i + 1).padStart(4, "0")}`,
      sourceIndex: i,
      origen: "Reservo",

      fecha: r["Fecha"],
      fechaNorm: normalizarFecha(r["Fecha"]),

      rut: r["Rut"],
      rutNorm: normalizarRut(r["Rut"]),

      paciente: r["Paciente"],
      pacienteNorm: normalizarPaciente(r["Paciente"]),

      profesional: r["Profesional"],
      profesionalNorm: normalizarTexto(r["Profesional"]),
      profesionalDetectado: resolved.profesionalNombre,

      prestacion: r["Tratamiento"],
      procedimientoNorm: normalizarTexto(r["Tratamiento"]),
      procedimientoDetectado: resolved.procedimientoNombre,

      valor: normalizarMonto(r["Valor"]),

      dataReservo: r,
      dataMK: null,

      resolved,
      aplicacion: evalApp.aplicacion,
      review: construirReview({
        profesionalId: resolved.profesionalId,
        procedimientoId: resolved.procedimientoId,
        alertas
      }),

      confirmadoEnProduccion: false,
      confirmadoEl: null,
      confirmadoPor: null,
      finalItemId: null,
      pacienteId: null
    };
  });
}

/* ======================
   PROCESAR MK
====================== */

function procesarMK() {
  let items = dataMK.map((r, i) => {
    const analisisProf = analizarBusquedaProfesional(r["D Médico"]);
    const profesionalDetectado = analisisProf?.profesional || null;
    const procedimientoDetectado = buscarProcedimiento(r["D Artículo"]);

    const alertas = [];
    if (analisisProf?.alerta) alertas.push(analisisProf.alerta);
    if (!normalizarRut(r["Rut"])) alertas.push("RUT vacío o inválido");
    if (!normalizarTexto(r["D Médico"])) alertas.push("Profesional vacío");
    if (!normalizarTexto(r["D Artículo"])) alertas.push("Procedimiento vacío");

    const resolved = {
      profesionalId: profesionalDetectado?.id || null,
      profesionalNombre: profesionalDetectado ? nombreProfesionalCatalogo(profesionalDetectado) : null,

      procedimientoId: procedimientoDetectado?.id || null,
      procedimientoNombre: procedimientoDetectado ? nombreProcedimientoCatalogo(procedimientoDetectado) : null,

      autoProfesional: !!profesionalDetectado,
      autoProcedimiento: !!procedimientoDetectado,
      confirmadoManualProfesional: false,
      confirmadoManualProcedimiento: false
    };

    return {
      itemId: `MK_${String(i + 1).padStart(4, "0")}`,
      sourceIndex: i,
      origen: "MK",

      fecha: r["Fecha"],
      fechaNorm: normalizarFecha(r["Fecha"]),

      rut: r["Rut"],
      rutNorm: normalizarRut(r["Rut"]),

      paciente: r["Paciente"],
      pacienteNorm: normalizarPaciente(r["Paciente"]),

      profesional: r["D Médico"],
      profesionalNorm: normalizarTexto(r["D Médico"]),
      profesionalDetectado: resolved.profesionalNombre,

      prestacion: r["D Artículo"],
      procedimientoNorm: normalizarTexto(r["D Artículo"]),
      procedimientoDetectado: resolved.procedimientoNombre,

      valor: normalizarMonto(r["Total"]),

      dataReservo: null,
      dataMK: r,

      resolved,
      aplicacion: construirAplicacion("no_aplica", "Sin evaluar"),
      _baseAlertas: alertas,
      _extraAlertas: [],
      review: construirReview({
        profesionalId: resolved.profesionalId,
        procedimientoId: resolved.procedimientoId,
        alertas
      }),

      confirmadoEnProduccion: false,
      confirmadoEl: null,
      confirmadoPor: null,
      finalItemId: null,
      pacienteId: null
    };
  });

  items = evaluarAplicacionMK(items);

  items.forEach(it => {
    const finalAlerts = [...(it._baseAlertas || []), ...(it._extraAlertas || [])];
    it.review = construirReview({
      profesionalId: it.resolved.profesionalId,
      procedimientoId: it.resolved.procedimientoId,
      alertas: finalAlerts
    });
    delete it._baseAlertas;
    delete it._extraAlertas;
  });

  return items;
}

/* ======================
   RECALCULAR TODO
====================== */

function recalcularTodo() {
  const reservos = procesarReservo();
  const mks = procesarMK();

  consolidado = [...reservos, ...mks];

  aplicarManualOverrides(consolidado);
  render();
}

/* ======================
   NORMALIZAR ITEM REHIDRATADO
====================== */

function recomputeItemFromCurrentValues(reg) {
  const analisisProf = reg.resolved?.profesionalId
    ? {
        profesional: profesionales.find(p => p.id === reg.resolved.profesionalId) || null,
        alerta: null
      }
    : analizarBusquedaProfesional(reg.profesional);

  const profesionalDetectado = analisisProf?.profesional || null;

  const procedimientoDetectado = reg.resolved?.procedimientoId
    ? procedimientos.find(p => p.id === reg.resolved.procedimientoId) || null
    : buscarProcedimiento(reg.prestacion);

  reg.rutNorm = normalizarRut(reg.rut);
  reg.pacienteNorm = normalizarPaciente(reg.paciente);
  reg.profesionalNorm = normalizarTexto(reg.profesional);
  reg.procedimientoNorm = normalizarTexto(reg.prestacion);
  reg.fechaNorm = normalizarFecha(reg.fecha);
  reg.valor = normalizarMonto(reg.valor);

  reg.resolved = reg.resolved || {
    profesionalId: null,
    profesionalNombre: null,
    procedimientoId: null,
    procedimientoNombre: null,
    autoProfesional: false,
    autoProcedimiento: false,
    confirmadoManualProfesional: false,
    confirmadoManualProcedimiento: false
  };

  if (!reg.resolved.profesionalId && profesionalDetectado?.id) {
    reg.resolved.profesionalId = profesionalDetectado.id;
    reg.resolved.profesionalNombre = nombreProfesionalCatalogo(profesionalDetectado);
    reg.resolved.autoProfesional = true;
  }

  if (!reg.resolved.procedimientoId && procedimientoDetectado?.id) {
    reg.resolved.procedimientoId = procedimientoDetectado.id;
    reg.resolved.procedimientoNombre = nombreProcedimientoCatalogo(procedimientoDetectado);
    reg.resolved.autoProcedimiento = true;
  }

  reg.profesionalDetectado = reg.resolved.profesionalNombre || (profesionalDetectado ? nombreProfesionalCatalogo(profesionalDetectado) : null);
  reg.procedimientoDetectado = reg.resolved.procedimientoNombre || (procedimientoDetectado ? nombreProcedimientoCatalogo(procedimientoDetectado) : null);

  let alertas = [];

  if (reg.origen === "Reservo") {
    const evalApp = evaluarAplicacionReservo(reg.dataReservo || {});
    reg.aplicacion = evalApp.aplicacion;
    alertas = [...evalApp.alertas];
  } else if (reg.origen === "MK") {
    if (!reg.aplicacion) reg.aplicacion = construirAplicacion("no_aplica", "Sin evaluar");
    alertas = reg.review?.alertas || [];
  }

  if (analisisProf?.alerta) alertas.push(analisisProf.alerta);
  if (!reg.rutNorm) alertas.push("RUT vacío o inválido");
  if (!normalizarTexto(reg.profesional)) alertas.push("Profesional vacío");
  if (!normalizarTexto(reg.prestacion)) alertas.push("Procedimiento vacío");

  reg.review = construirReview({
    profesionalId: reg.resolved?.profesionalId || null,
    procedimientoId: reg.resolved?.procedimientoId || null,
    alertas: [...new Set(alertas)]
  });
}

/* ======================
   DETALLE / EDICIÓN
====================== */

function abrirDetalle(reg) {
  const modal = $("modalItemBackdrop");
  const itemSub = $("itemSub");
  const itemForm = $("itemForm");

  if (!modal || !itemSub || !itemForm) {
    console.warn("No existe el modal de detalle en el HTML");
    return;
  }

  stateEdicion.actual = reg;

  const opcionesProfesionales = profesionales.map(p => {
    const nombre = nombreProfesionalCatalogo(p);
    const selected = reg.resolved?.profesionalId === p.id ? "selected" : "";
    return `<option value="${p.id}" ${selected}>${escapeHtml(nombre)}</option>`;
  }).join("");

  const procedimientosAmb = procedimientosAmbulatorios();

  const opcionesProcedimientos = procedimientosAmb.map(p => {
    const nombre = nombreProcedimientoCatalogo(p);
    const selected = reg.resolved?.procedimientoId === p.id ? "selected" : "";
    return `<option value="${p.id}" ${selected}>${escapeHtml(p.id)} · ${escapeHtml(nombre)}</option>`;
  }).join("");

  itemSub.textContent = `${reg.origen || ""} · ${reg.fecha || ""} · ${reg.rut || ""}`;

  itemForm.innerHTML = `
    <div class="grid2">

      <section class="card" style="padding:12px;">
        <div class="sectionTitle">Resolución del item</div>

        <div class="kv">
          <div class="k">Origen</div><div class="v">${escapeHtml(reg.origen || "")}</div>
          <div class="k">Fecha</div><div class="v">${escapeHtml(reg.fecha || "")}</div>
          <div class="k">RUT</div><div class="v">${escapeHtml(reg.rut || "")}</div>
          <div class="k">Paciente</div><div class="v">${escapeHtml(reg.paciente || "")}</div>
          <div class="k">Profesional archivo</div><div class="v">${escapeHtml(reg.profesional || "")}</div>
          <div class="k">Procedimiento archivo</div><div class="v">${escapeHtml(reg.prestacion || "")}</div>
          <div class="k">Estado revisión</div><div class="v">${reg.review?.estadoRevision === "ok" ? "OK" : "Pendiente"}</div>
          <div class="k">Aplicación</div><div class="v">${escapeHtml(reg.aplicacion?.estado || "—")}</div>
          <div class="k">Motivo</div><div class="v">${escapeHtml(reg.aplicacion?.motivo || "—")}</div>
          <div class="k">Alertas</div><div class="v">${escapeHtml((reg.review?.alertas || []).join(" · ") || "—")}</div>
          <div class="k">Confirmado final</div><div class="v">${reg.confirmadoEnProduccion ? "Sí" : "No"}</div>
        </div>

        <div style="height:12px;"></div>

        <div class="field">
          <label>Asociar profesional</label>
          <select id="detalleProfesionalId">
            <option value="">(Selecciona profesional)</option>
            ${opcionesProfesionales}
          </select>
        </div>

        <div class="field" style="margin-top:10px;">
          <label>Asociar procedimiento ambulatorio</label>

          <input
            type="text"
            id="detalleProcedimientoBuscar"
            placeholder="Escribe código o nombre... ej: PA0022 o nutrición"
            value=""
            style="margin-bottom:8px;"
          >

          <select id="detalleProcedimientoId">
            <option value="">(Selecciona procedimiento ambulatorio)</option>
            ${opcionesProcedimientos}
          </select>
        </div>

        <div style="display:flex; justify-content:flex-end; margin-top:12px;">
          <button id="btnMoreInfo" type="button" class="btn soft">Editar más información</button>
        </div>
      </section>

      <section class="card" style="padding:12px;">
        <div class="sectionTitle">Datos originales</div>
        <pre style="white-space:pre-wrap; font-size:12px; margin:0;">${escapeHtml(JSON.stringify(reg.dataReservo || reg.dataMK || {}, null, 2))}</pre>
      </section>

    </div>
  `;

  modal.style.display = "block";

  if ($("btnMoreInfo")) {
    $("btnMoreInfo").onclick = () => abrirMasInformacion(reg);
  }

  const inputBuscarProc = $("detalleProcedimientoBuscar");
  const selectProc = $("detalleProcedimientoId");

  function renderOpcionesProcedimientoFiltradas() {
    if (!selectProc) return;

    const q = normalizarTexto(inputBuscarProc?.value || "");
    const lista = procedimientosAmbulatorios().filter(p => {
      if (!q) return true;

      const texto = normalizarTexto([
        p?.id || "",
        nombreProcedimientoCatalogo(p),
        p?.nombre || "",
        p?.tratamiento || "",
        p?.descripcion || ""
      ].join(" | "));

      return texto.includes(q);
    });

    const actual = reg.resolved?.procedimientoId || manualOverrides?.[reg.itemId]?.procedimientoId || "";

    selectProc.innerHTML = `
      <option value="">(Selecciona procedimiento ambulatorio)</option>
      ${lista.map(p => {
        const selected = actual === p.id ? "selected" : "";
        return `<option value="${p.id}" ${selected}>${escapeHtml(p.id)} · ${escapeHtml(nombreProcedimientoCatalogo(p))}</option>`;
      }).join("")}
    `;
  }

  if (inputBuscarProc) {
    inputBuscarProc.addEventListener("input", renderOpcionesProcedimientoFiltradas);
  }

  renderOpcionesProcedimientoFiltradas();
}

function abrirMasInformacion(reg) {
  const itemForm = $("itemForm");
  if (!itemForm) return;

  const original = reg.dataReservo || reg.dataMK || {};

  const filas = Object.keys(original).map(key => {
    const value = original[key] ?? "";
    return `
      <div class="field" style="margin-bottom:10px;">
        <label>${escapeHtml(key)}</label>
        <input type="text" data-extra-key="${escapeHtml(key)}" value="${escapeHtml(String(value))}">
      </div>
    `;
  }).join("");

  itemForm.innerHTML = `
    <div class="card" style="padding:12px;">
      <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; margin-bottom:10px;">
        <div>
          <div class="sectionTitle">Editar más información</div>
          <div class="help">Aquí puedes editar los campos originales del registro. Luego presiona “Guardar item”.</div>
        </div>
        <button id="btnVolverDetalle" type="button" class="btn">← Volver a resolución</button>
      </div>

      <div class="grid2">
        ${filas}
      </div>
    </div>
  `;

  if ($("btnVolverDetalle")) {
    $("btnVolverDetalle").onclick = () => abrirDetalle(reg);
  }
}

async function guardarDetalle() {
  if (!stateEdicion.actual) return;

  const reg = stateEdicion.actual;

  const extraInputs = document.querySelectorAll("[data-extra-key]");
  if (extraInputs.length) {
    const target = reg.dataReservo ? reg.dataReservo : reg.dataMK;

    extraInputs.forEach(inp => {
      const key = inp.getAttribute("data-extra-key");
      if (!key || !target) return;
      target[key] = inp.value;
    });

    if (reg.dataReservo) {
      reg.fecha = reg.dataReservo["Fecha"] ?? reg.fecha;
      reg.rut = reg.dataReservo["Rut"] ?? reg.rut;
      reg.paciente = reg.dataReservo["Paciente"] ?? reg.paciente;
      reg.profesional = reg.dataReservo["Profesional"] ?? reg.profesional;
      reg.prestacion = reg.dataReservo["Tratamiento"] ?? reg.prestacion;
      reg.valor = normalizarMonto(reg.dataReservo["Valor"]);
    } else if (reg.dataMK) {
      reg.fecha = reg.dataMK["Fecha"] ?? reg.fecha;
      reg.rut = reg.dataMK["Rut"] ?? reg.rut;
      reg.paciente = reg.dataMK["Paciente"] ?? reg.paciente;
      reg.profesional = reg.dataMK["D Médico"] ?? reg.profesional;
      reg.prestacion = reg.dataMK["D Artículo"] ?? reg.prestacion;
      reg.valor = normalizarMonto(reg.dataMK["Total"]);
    }
  }

  const profSel = $("detalleProfesionalId");
  const procSel = $("detalleProcedimientoId");

  if (profSel || procSel) {
    manualOverrides[reg.itemId] = {
      ...(manualOverrides[reg.itemId] || {}),
      ...(profSel ? { profesionalId: profSel.value || null } : {}),
      ...(procSel ? { procedimientoId: procSel.value || null } : {})
    };
  }

  aplicarManualOverrides([reg]);
  recomputeItemFromCurrentValues(reg);

  await persistirItemEditado(reg);

  cerrarDetalle();
  render();
}

function cerrarDetalle() {
  const modal = $("modalItemBackdrop");
  const itemForm = $("itemForm");

  if (modal) modal.style.display = "none";
  if (itemForm) itemForm.innerHTML = "";

  stateEdicion.actual = null;
}

/* ======================
   PERSISTENCIA ITEM EDITADO
====================== */

async function persistirItemEditado(reg) {
  if (!stateImport.importId) {
    render();
    return;
  }

  if (stateImport.status === "staged") {
    const ref = doc(db, "produccion_ambulatoria_imports", stateImport.importId, "items", reg.itemId);

    await setDoc(ref, {
      ...serializeAmbItem(reg),
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    toast("Item guardado en staging");
    return;
  }

  if (stateImport.status === "confirmada") {
    const refStaging = doc(db, "produccion_ambulatoria_imports", stateImport.importId, "items", reg.itemId);

    // ✅ Siempre guarda el item editado dentro del import
    await setDoc(refStaging, {
      ...serializeAmbItem(reg),
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    // ✅ Solo si YA estaba confirmado, reescribe también producción final
    if (reg.confirmadoEnProduccion === true) {
      const YYYY = String(stateImport.year);
      const MM = pad(stateImport.monthNum, 2);
      const rutKey = normalizarRutKey(reg.rut || "");
      const pacienteId = rutKey || `SINRUT_${stateImport.importId}`;
      const itemDocId = reg.finalItemId || finalItemId(reg);

      reg.finalItemId = itemDocId;
      reg.pacienteId = pacienteId;

      const refPaciente = doc(db, "produccion_ambulatoria", YYYY, "meses", MM, "pacientes", pacienteId);
      const refItem = doc(db, "produccion_ambulatoria", YYYY, "meses", MM, "pacientes", pacienteId, "items", itemDocId);

      await setDoc(refPaciente, {
        rut: reg.rut || null,
        rutNorm: reg.rutNorm || null,
        paciente: reg.paciente || null,
        pacienteNorm: reg.pacienteNorm || null,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: stateImport.user?.email || ""
      }, { merge: true });

      await setDoc(refItem, {
        ...serializeAmbItem(reg),
        finalItemId: itemDocId,
        pacienteId,
        estadoRegistro: "activo",
        actualizadoEl: serverTimestamp(),
        actualizadoPor: stateImport.user?.email || ""
      }, { merge: true });

      toast("Item confirmado actualizado en producción final");
    } else {
      toast("Item pendiente guardado en el import; aún no pasa a producción final");
    }
  }
}

function serializeAmbItem(reg) {
  return {
    itemId: reg.itemId,
    sourceIndex: reg.sourceIndex ?? 0,
    origen: reg.origen || null,

    fecha: reg.fecha || null,
    fechaNorm: reg.fechaNorm || null,

    rut: reg.rut || null,
    rutNorm: reg.rutNorm || null,

    paciente: reg.paciente || null,
    pacienteNorm: reg.pacienteNorm || null,

    profesional: reg.profesional || null,
    profesionalNorm: reg.profesionalNorm || null,
    profesionalDetectado: reg.profesionalDetectado || null,

    prestacion: reg.prestacion || null,
    procedimientoNorm: reg.procedimientoNorm || null,
    procedimientoDetectado: reg.procedimientoDetectado || null,

    valor: Number(reg.valor || 0) || 0,

    dataReservo: reg.dataReservo || null,
    dataMK: reg.dataMK || null,

    resolved: reg.resolved || null,
    aplicacion: reg.aplicacion || null,
    review: reg.review || null,

    // ✅ NUEVO: confirmación acumulada
    confirmadoEnProduccion: !!reg.confirmadoEnProduccion,
    confirmadoEl: reg.confirmadoEl || null,
    confirmadoPor: reg.confirmadoPor || null,
    finalItemId: reg.finalItemId || null,
    pacienteId: reg.pacienteId || null
  };
}

/* ======================
   RESOLVER PENDIENTES
====================== */

function rowMiniHTML(it, extra = "") {
  return `
    <div class="miniRow" style="${it.confirmadoEnProduccion ? 'border-left:4px solid #16a34a; padding-left:10px;' : ''}">
      <div>
        <div>
          <b>${escapeHtml(it.origen)}</b> · ${escapeHtml(it.fecha || "")} · ${escapeHtml(it.rut || "")}
          ${it.confirmadoEnProduccion
            ? `<span class="ok" style="margin-left:8px;">Confirmado final</span>`
            : ``}
        </div>
        <div class="muted tiny">${escapeHtml(it.paciente || "")}</div>
        <div class="tiny">Prof: ${escapeHtml(it.profesional || "")} · Proc: ${escapeHtml(it.prestacion || "")}</div>
        ${extra ? `<div class="tiny warn" style="margin-top:4px;">${escapeHtml(extra)}</div>` : ""}
      </div>
      <div class="tiny">
        <div><b>Revisión:</b> ${escapeHtml(it.review?.estadoRevision || "pendiente")}</div>
        <div><b>Aplicación:</b> ${escapeHtml(it.aplicacion?.estado || "—")}</div>
        <div><b>Motivo:</b> ${escapeHtml(it.aplicacion?.motivo || "—")}</div>
        <div><b>Estado final:</b> ${it.confirmadoEnProduccion ? "Ya confirmado" : (esItemConfirmable(it) ? "Listo para confirmar" : "Pendiente")}</div>
      </div>
      <div>
        <button class="btn small" type="button" data-edit-item="${escapeHtml(it.itemId)}">Editar</button>
      </div>
    </div>
  `;
}

function bindEditButtonsIn(container) {
  if (!container) return;
  container.querySelectorAll("[data-edit-item]").forEach(btn => {
    btn.onclick = () => {
      const id = btn.getAttribute("data-edit-item");
      const it = consolidado.find(x => x.itemId === id);
      if (!it) return;
      cerrarResolver();
      abrirDetalle(it);
    };
  });
}

function getResolverBaseItems() {
  return itemsOperables().filter(it =>
    it.review?.estadoRevision === "pendiente" ||
    it.aplicacion?.estado === "revisar"
  );
}

function getResolverItemsByFiltro() {
  const base = getResolverBaseItems();
  const operables = itemsOperables();

  switch (uiState.resolverFiltro) {
    case "pendientes":
      return base.filter(it => it.review?.estadoRevision === "pendiente");

    case "aplica":
      return operables.filter(it => it.aplicacion?.estado === "aplica");

    case "no_aplica":
      return operables.filter(it => it.aplicacion?.estado === "no_aplica");

    case "revisar":
      return operables.filter(it => it.aplicacion?.estado === "revisar");

    case "todos":
      return operables;

    case "base":
    default:
      return base;
  }
}

function resolverFiltroLabel() {
  switch (uiState.resolverFiltro) {
    case "pendientes": return "Pendientes";
    case "aplica": return "Aplican";
    case "no_aplica": return "No aplica";
    case "revisar": return "Revisar";
    case "todos": return "Todos";
    case "base":
    default:
      return "Por resolver";
  }
}

function resolverResumenLink(label, count, filtro) {
  const activo = uiState.resolverFiltro === filtro;
  const style = activo
    ? 'style="font-weight:900; text-decoration:underline;"'
    : 'style="font-weight:800;"';

  return `<button type="button" class="linkBtn" data-resolver-filter="${escapeHtml(filtro)}" ${style}>${escapeHtml(label)}: ${count}</button>`;
}

function bindResolverResumenFiltros() {
  const resumen = $("resolverResumen");
  if (!resumen) return;

  resumen.querySelectorAll("[data-resolver-filter]").forEach(btn => {
    btn.onclick = () => {
      const filtro = btn.getAttribute("data-resolver-filter") || "base";
      uiState.resolverFiltro = filtro;
      renderResolver();
    };
  });
}

function renderResolver() {
  const resumen = $("resolverResumen");
  const listResumen = $("resolverCoincidenciasList");
  const listProf = $("resolverProfesionalesList");
  const listProc = $("resolverPrestacionesList");
  const listAlert = $("resolverAlertasList");

  if (!resumen || !listResumen || !listProf || !listProc || !listAlert) return;

  const pendientes = consolidado.filter(x => x.review?.estadoRevision === "pendiente");
  const pendProf = consolidado.filter(x => x.review?.pendientes?.profesional);
  const pendProc = consolidado.filter(x => x.review?.pendientes?.procedimiento);
  const conAlerta = consolidado.filter(x => (x.review?.alertas || []).length > 0);
  const revisarApp = consolidado.filter(x => x.aplicacion?.estado === "revisar");
  const aplica = consolidado.filter(x => x.aplicacion?.estado === "aplica");
  const noAplica = consolidado.filter(x => x.aplicacion?.estado === "no_aplica");

  const resumenItems = getResolverItemsByFiltro();

  resumen.innerHTML = [
    resolverResumenLink("Items", consolidado.length, "base"),
    `<span> · </span>`,
    resolverResumenLink("Pendientes", pendientes.length, "pendientes"),
    `<span> · </span>`,
    resolverResumenLink("Aplican", aplica.length, "aplica"),
    `<span> · </span>`,
    resolverResumenLink("No aplica", noAplica.length, "no_aplica"),
    `<span> · </span>`,
    resolverResumenLink("Revisar", revisarApp.length, "revisar"),
    `<div class="help" style="margin-top:8px;">Filtro actual: <b>${escapeHtml(resolverFiltroLabel())}</b></div>`
  ].join("");

  listResumen.innerHTML = resumenItems.length
    ? resumenItems.map(it => rowMiniHTML(
        it,
        (it.review?.alertas || []).join(" · ") || it.aplicacion?.motivo || ""
      )).join("")
    : `<div class="muted tiny">No hay ítems para el filtro seleccionado.</div>`;

  listProf.innerHTML = pendProf.length
    ? pendProf.map(it => rowMiniHTML(it, "Falta asociar profesional")).join("")
    : `<div class="muted tiny">No hay pendientes de profesional.</div>`;

  listProc.innerHTML = pendProc.length
    ? pendProc.map(it => rowMiniHTML(it, "Falta asociar procedimiento")).join("")
    : `<div class="muted tiny">No hay pendientes de procedimiento.</div>`;

  listAlert.innerHTML = conAlerta.length
    ? conAlerta.map(it => rowMiniHTML(it, (it.review?.alertas || []).join(" · "))).join("")
    : `<div class="muted tiny">No hay alertas.</div>`;

  bindResolverResumenFiltros();

  bindEditButtonsIn(listResumen);
  bindEditButtonsIn(listProf);
  bindEditButtonsIn(listProc);
  bindEditButtonsIn(listAlert);
}

function abrirResolver() {
  const modal = $("modalResolverBackdrop");
  if (!modal) return;

  uiState.resolverFiltro = "base";
  renderResolver();
  modal.style.display = "block";
}

function cerrarResolver() {
  const modal = $("modalResolverBackdrop");
  if (modal) modal.style.display = "none";
}

/* ======================
   FILTRO / PAGINACIÓN
====================== */

const PRESTACIONES_EXCLUIDAS_RESERVO = [
  "CONSULTA KINESIOLOGIA TELEMEDICINA",
  "CONSULTA KINESIOLOGIA PRESENCIAL",
  "KINESIOLOGIA PACK PRESENCIAL",
  "KINESIOLOGIA PACK TELEMEDICINA",
  "INBODY GOOGLE",
  "CONSULTA KINESIOLOGIA PRESENCIAL INGRESO",
  "EDUCACION KINESIOLOGICA POST CIRUGIA TELEMEDICINA (PAD)",
  "CONSULTA KINESIOLOGIA TELEMEDICINA INGRESO",
  "ESPIROMETRIA PACK",
  "ESPIROMETRIA",
  "ESPIROMOETRIA"
];

function esPrestacionExcluidaReservo(it) {
  // ✅ Solo aplica a origen Reservo
  if (clean(it?.origen) !== "Reservo") return false;

  const texto = normalizarTexto(
    it?.prestacion ||
    it?.dataReservo?.["Tratamiento"] ||
    ""
  );

  if (!texto) return false;

  return PRESTACIONES_EXCLUIDAS_RESERVO.some(p => texto.includes(p));
}

function itemsOperables() {
  // ✅ Si está activado, entra todo
  if (uiState.incluirKinesiologia) return [...consolidado];

  // ✅ Si está apagado, excluimos las prestaciones especiales de Reservo
  return consolidado.filter(it => !esPrestacionExcluidaReservo(it));
}

function totalPrestacionesExcluidasOcultas() {
  return consolidado.filter(esPrestacionExcluidaReservo).length;
}

function itemSearchText(it) {
  return [
    it.itemId,
    it.origen,
    it.fecha,
    it.rut,
    it.rutNorm,
    it.paciente,
    it.profesional,
    it.prestacion,
    it.valor,
    it.review?.estadoRevision,
    it.aplicacion?.estado,
    it.aplicacion?.motivo,
    it.confirmadoEnProduccion ? "CONFIRMADO" : "PENDIENTE",
    ...(it.review?.alertas || [])
  ].map(x => normalizarTexto(x)).join(" | ");
}

function filteredItems() {
  let items = itemsOperables();

  if (uiState.mostrarNoAplica) {
    items = items.filter(it => it.aplicacion?.estado === "no_aplica");
  } else {
    items = items.filter(it => it.aplicacion?.estado !== "no_aplica");
  }

  if (!clean(uiState.q)) return items;

  return items.filter(it => {
    const text = itemSearchText(it);
    return matchBusquedaPrincipal(text, uiState.q);
  });
}

function esProcedimientoAmbulatorio(p) {
  const tipo = normalizarTexto(p?.tipo || "");
  const id = normalizarTexto(p?.id || "");
  return tipo === "AMBULATORIO" || /^PA\d+$/.test(id);
}

function procedimientosAmbulatorios() {
  return procedimientos
    .filter(esProcedimientoAmbulatorio)
    .sort((a, b) => {
      const aId = normalizarTexto(a?.id || "");
      const bId = normalizarTexto(b?.id || "");
      return aId.localeCompare(bId, 'es', { numeric: true, sensitivity: 'base' });
    });
}

function matchBusquedaPrincipal(searchText, rawQuery) {
  const q = normalizarTexto(rawQuery);
  if (!q) return true;

  const gruposOr = q
    .split(".")
    .map(g => g.trim())
    .filter(Boolean);

  if (!gruposOr.length) return true;

  return gruposOr.some(grupo => {
    const terminosAnd = grupo
      .split(",")
      .map(t => t.trim())
      .filter(Boolean);

    if (!terminosAnd.length) return false;

    return terminosAnd.every(term => searchText.includes(term));
  });
}

/* ======================
   RENDER TABLA
====================== */

function render() {
  const thead = $("thead");
  const tbody = $("tbody");

  if (!thead || !tbody) return;

  thead.innerHTML = `
    <tr>
      <th>#</th>
      <th>Origen</th>
      <th>Fecha</th>
      <th>Rut</th>
      <th>Paciente</th>
      <th>Profesional archivo</th>
      <th>Procedimiento archivo</th>
      <th>Valor</th>
      <th>Revisión</th>
      <th>Aplicación</th>
      <th>Motivo</th>
      <th>Alertas</th>
      <th>Acciones</th>
    </tr>
  `;

  const items = filteredItems();
  const totalPages = Math.max(1, Math.ceil(items.length / uiState.pageSize));
  if (uiState.page >= totalPages) uiState.page = totalPages - 1;
  if (uiState.page < 0) uiState.page = 0;

  const from = uiState.page * uiState.pageSize;
  const to = from + uiState.pageSize;
  const pageItems = items.slice(from, to);

  tbody.innerHTML = "";

  for (let i = 0; i < pageItems.length; i++) {
    const r = pageItems[i];
    const tr = document.createElement("tr");

    const estado = r.review?.estadoRevision || "pendiente";
    const alertasTexto = (r.review?.alertas || []).join(" · ");

    const estadoFinalHtml = badgeConfirmacionHTML(r);
    
    tr.innerHTML = `
      <td>${from + i + 1}</td>
      <td>${escapeHtml(r.origen || "")}</td>
      <td>${escapeHtml(r.fecha || "")}</td>
      <td>${escapeHtml(r.rut || "")}</td>
      <td>${escapeHtml(r.paciente || "")}</td>
      <td>${escapeHtml(r.profesional || "")}</td>
      <td>${escapeHtml(r.prestacion || "")}</td>
      <td>${escapeHtml(r.valor ?? "")}</td>
      <td>${estado === "ok" ? `<span class="ok">OK</span>` : `<span class="warn">Pendiente</span>`}</td>
      <td>${escapeHtml(r.aplicacion?.estado || "—")}</td>
      <td class="wrap">${escapeHtml(r.aplicacion?.motivo || "—")}</td>
      <td class="wrap">
        ${escapeHtml(alertasTexto || "—")}
        <div style="margin-top:6px;">${estadoFinalHtml}</div>
      </td>
      <td>
        <button class="btnDetalle btn small" type="button">Editar</button>
      </td>
    `;

    const btnDetalle = tr.querySelector(".btnDetalle");
    if (btnDetalle) {
      btnDetalle.onclick = () => abrirDetalle(r);
    }

    tbody.appendChild(tr);
  }

  const operables = itemsOperables();
 const ocultosKine = totalPrestacionesExcluidasOcultas();

  const pendientes = operables.filter(x => x.review?.estadoRevision === "pendiente").length;
  const alertas = operables.filter(x => (x.review?.alertas || []).length > 0).length;
  const ok = operables.filter(x => x.review?.estadoRevision === "ok").length;
  const noAplica = operables.filter(x => x.aplicacion?.estado === "no_aplica").length;
  const pendProf = operables.filter(x => x.review?.pendientes?.profesional).length;
  const reservoAplica = operables.filter(x => x.origen === "Reservo" && x.aplicacion?.estado === "aplica").length;
  const mkAplica = operables.filter(x => x.origen === "MK" && x.aplicacion?.estado === "aplica").length;
  const confirmados = operables.filter(x => x.confirmadoEnProduccion).length;
  const confirmables = operables.filter(it =>
    esItemConfirmable(it) && !it.confirmadoEnProduccion
  ).length;

  if ($("countPill")) $("countPill").textContent = `Vista: ${items.length} · Total import: ${consolidado.length}`;
  if ($("pillPendientes")) $("pillPendientes").textContent = `Pendientes: ${pendientes}`;
  if ($("pillAlertas")) $("pillAlertas").textContent = `Alertas: ${alertas}`;
  if ($("pillProf")) $("pillProf").textContent = `Pend. profesional: ${pendProf}`;
  if ($("pillCoincidencias")) $("pillCoincidencias").textContent = `OK: ${ok} · Nuevos confirmables: ${confirmables}`;
  if ($("pillFusionados")) $("pillFusionados").textContent = `No aplica: ${noAplica}`;
  if ($("pillReservoValidos")) $("pillReservoValidos").textContent = `Reservo válidos: ${reservoAplica}`;
  if ($("pillMKValidos")) $("pillMKValidos").textContent = `MK válidos: ${mkAplica}`;

  if ($("pagerInfo")) {
    $("pagerInfo").textContent =
      `${items.length} resultados · página ${uiState.page + 1} de ${totalPages}`;
  }

  if ($("statusInfo")) {
    const totalAmbulatorios = procedimientosAmbulatorios().length;
    const vista = uiState.mostrarNoAplica ? "Vista: NO APLICA" : "Vista: APLICABLES / REVISAR";
    const est = stateImport.status ? ` · Estado import: ${stateImport.status}` : "";
    const imp = stateImport.importId ? ` · ImportID: ${stateImport.importId}` : "";
    const kineTxt = uiState.incluirKinesiologia
      ? ` · Prestaciones excluidas: activas`
      : ` · Prestaciones excluidas: ocultas (${ocultosKine})`;

    $("statusInfo").textContent = consolidado.length
      ? `${vista}${est}${imp}${kineTxt} · Confirmados: ${confirmados} · Nuevos confirmables: ${confirmables} · Catálogos: ${profesionales.length} profesionales · ${totalAmbulatorios} procedimientos ambulatorios`
      : "—";
  }

  if ($("btnToggleNoAplica")) {
    $("btnToggleNoAplica").textContent = uiState.mostrarNoAplica
      ? "Ver aplicables"
      : "Ver no aplica";
  }

  if ($("btnToggleKine")) {
    $("btnToggleKine").textContent = uiState.incluirKinesiologia
      ? "Ocultar prestaciones excluidas"
      : "Activar prestaciones excluidas";
  }

  if ($("btnResolver")) $("btnResolver").disabled = consolidado.length === 0;

  if ($("btnConfirmar")) {
    const canConfirm =
      (stateImport.status === "staged" || stateImport.status === "confirmada") &&
      confirmables > 0;

    $("btnConfirmar").disabled = !canConfirm;
    $("btnConfirmar").title = canConfirm
      ? `Listo para confirmar (${confirmables} items nuevos)`
      : `Bloqueado: estado=${stateImport.status || "—"} / nuevos confirmables=${confirmables}`;
  }

  if ($("btnAnular")) {
    $("btnAnular").disabled = !(stateImport.importId && (stateImport.status === "staged" || stateImport.status === "confirmada"));
  }

  renderPagerTabs(totalPages);
}

function renderPagerTabs(totalPages) {
  const wrap = $("pagerTabs");
  if (!wrap) return;

  wrap.innerHTML = "";

  for (let i = 0; i < totalPages; i++) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "btn small";
    btn.textContent = String(i + 1);
    if (i === uiState.page) {
      btn.classList.add("primary");
    }
    btn.onclick = () => {
      uiState.page = i;
      render();
    };
    wrap.appendChild(btn);
  }
}

/* ======================
   PROCESAR
====================== */

async function procesarArchivos() {
  if (!dataReservo.length && !dataMK.length) {
    alert("Debes cargar al menos un archivo");
    return;
  }

  try {
    await cargarProfesionales();
    await cargarProcedimientos();

    stateImport.monthName = clean($("mes")?.value || "");
    stateImport.monthNum = monthIndex(stateImport.monthName);
    stateImport.year = Number($("ano")?.value || 0) || 0;

    if (!stateImport.monthNum || !stateImport.year) {
      toast("Mes o año inválido");
      return;
    }

    stateImport.importId = makeImportId();
    stateImport.status = "staged";

    uiState.page = 0;
    recalcularTodo();

    await saveStagingToFirestore();
    await fillImportSuggestions();

    if ($("importSelect")) $("importSelect").value = stateImport.importId;
    if ($("importId")) $("importId").value = stateImport.importId;

    setStatus(`🟡 Staging listo: ${consolidado.length} filas · ImportID: ${stateImport.importId}`);
    render();

  } catch (err) {
    console.error("Error en procesarArchivos():", err);
    toast("No se pudo guardar el staging. Revisa la consola.");
  }
}

/* ======================
   STAGING SAVE
====================== */

async function saveStagingToFirestore() {
  const importId = stateImport.importId;
  if (!importId) throw new Error("Falta importId");

  const refImport = doc(db, "produccion_ambulatoria_imports", importId);

  await setDoc(refImport, {
    id: importId,
    mes: stateImport.monthName,
    mesNum: stateImport.monthNum,
    ano: stateImport.year,
    monthId: monthId(stateImport.year, stateImport.monthNum),
    filenameReservo: stateImport.filenameReservo || "",
    filenameMK: stateImport.filenameMK || "",
    estado: "staged",
    filas: consolidado.length,
    creadoEl: serverTimestamp(),
    creadoPor: stateImport.user?.email || "",
    actualizadoEl: serverTimestamp(),
    actualizadoPor: stateImport.user?.email || ""
  }, { merge: true });

  const itemsCol = collection(db, "produccion_ambulatoria_imports", importId, "items");

  // Más chico para no saturar Firestore
  const chunkSize = 150;
  let idx = 0;
  let guardados = 0;

  try {
    while (idx < consolidado.length) {
      const batch = writeBatch(db);
      const slice = consolidado.slice(idx, idx + chunkSize);

      slice.forEach((it, k) => {
        const itemId = it.itemId || `${it.origen || "ITEM"}_${String(idx + k + 1).padStart(4, "0")}`;
        it.itemId = itemId;

        if (typeof it.confirmadoEnProduccion === "undefined") it.confirmadoEnProduccion = false;
        if (typeof it.confirmadoEl === "undefined") it.confirmadoEl = null;
        if (typeof it.confirmadoPor === "undefined") it.confirmadoPor = null;
        if (typeof it.finalItemId === "undefined") it.finalItemId = null;
        if (typeof it.pacienteId === "undefined") it.pacienteId = null;

        batch.set(doc(itemsCol, itemId), {
          ...serializeAmbItem(it),
          idx: idx + k + 1,
          estado: "staged",
          creadoEl: serverTimestamp(),
          creadoPor: stateImport.user?.email || "",
          actualizadoEl: serverTimestamp(),
          actualizadoPor: stateImport.user?.email || ""
        }, { merge: true });
      });

      await batch.commit();

      guardados += slice.length;
      idx += chunkSize;

      setStatus(`🟡 Guardando staging... ${guardados}/${consolidado.length}`);
      console.log(`STAGING OK: ${guardados}/${consolidado.length}`);

      await sleep(250);
    }

    await setDoc(refImport, {
      estado: "staged",
      filas: consolidado.length,
      totalGuardadosStaging: guardados,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    setStatus(`🟡 Staging listo: ${guardados}/${consolidado.length} filas · ImportID: ${importId}`);
    toast(`Staging guardado correctamente: ${guardados} filas`);
  } catch (err) {
    console.error("Error guardando staging:", err);

    await setDoc(refImport, {
      estado: "staged_error",
      totalGuardadosStaging: guardados,
      errorStaging: String(err?.message || err || "Error desconocido"),
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    setStatus(`⚠️ Error guardando staging: ${guardados}/${consolidado.length}`);
    toast(`Error guardando staging. Avance: ${guardados}/${consolidado.length}`);
    throw err;
  }
}

/* ======================
   LOAD STAGING
====================== */

async function loadStagingFromFirestore(importId) {
  if (!importId) {
    toast("Falta ImportID");
    return;
  }

  await cargarProfesionales();
  await cargarProcedimientos();

  const refImport = doc(db, "produccion_ambulatoria_imports", importId);
  const snapImp = await getDoc(refImport);

  if (!snapImp.exists()) {
    toast("No existe ese import");
    return;
  }

  const imp = snapImp.data() || {};

  stateImport.importId = importId;
  stateImport.status = clean(imp.estado) || "staged";
  stateImport.monthName = clean(imp.mes) || "";
  stateImport.monthNum = Number(imp.mesNum || 0) || 0;
  stateImport.year = Number(imp.ano || 0) || 0;
  stateImport.filenameReservo = clean(imp.filenameReservo || "");
  stateImport.filenameMK = clean(imp.filenameMK || "");

  if ($("mes") && stateImport.monthName) $("mes").value = stateImport.monthName;
  if ($("ano") && stateImport.year) $("ano").value = String(stateImport.year);
  if ($("importId")) $("importId").value = importId;

  const itemsCol = collection(db, "produccion_ambulatoria_imports", importId, "items");
  const qy = query(itemsCol, orderBy("idx", "asc"));
  const snapItems = await getDocs(qy);

  console.log("IMPORT DOC PADRE:", imp);
  console.log("CANTIDAD DOCS items LEIDOS:", snapItems.size);

  const staged = [];
  snapItems.forEach(d => {
    const x = d.data() || {};
    staged.push({
      itemId: clean(x.itemId || d.id),
      sourceIndex: Number(x.sourceIndex || 0) || 0,
      origen: x.origen || "",
      fecha: x.fecha || "",
      fechaNorm: x.fechaNorm || "",
      rut: x.rut || "",
      rutNorm: x.rutNorm || "",
      paciente: x.paciente || "",
      pacienteNorm: x.pacienteNorm || "",
      profesional: x.profesional || "",
      profesionalNorm: x.profesionalNorm || "",
      profesionalDetectado: x.profesionalDetectado || null,
      prestacion: x.prestacion || "",
      procedimientoNorm: x.procedimientoNorm || "",
      procedimientoDetectado: x.procedimientoDetectado || null,
      valor: Number(x.valor || 0) || 0,
      dataReservo: x.dataReservo || null,
      dataMK: x.dataMK || null,
      resolved: x.resolved || {
        profesionalId: null,
        profesionalNombre: null,
        procedimientoId: null,
        procedimientoNombre: null,
        autoProfesional: false,
        autoProcedimiento: false,
        confirmadoManualProfesional: false,
        confirmadoManualProcedimiento: false
      },
      aplicacion: x.aplicacion || null,
      review: x.review || null,

      confirmadoEnProduccion: x.confirmadoEnProduccion === true || x.estado === "confirmada",
      confirmadoEl: x.confirmadoEl || null,
      confirmadoPor: x.confirmadoPor || null,
      finalItemId: x.finalItemId || null,
      pacienteId: x.pacienteId || null
    });
  });

  consolidado = staged;

  console.log("CARGADOS EN consolidado:", consolidado.length);

  const resumenDebug = {
    total: consolidado.length,
    aplica: consolidado.filter(x => x.aplicacion?.estado === "aplica").length,
    no_aplica: consolidado.filter(x => x.aplicacion?.estado === "no_aplica").length,
    revisar: consolidado.filter(x => x.aplicacion?.estado === "revisar").length,
    sin_aplicacion: consolidado.filter(x => !x.aplicacion?.estado).length,
    review_ok: consolidado.filter(x => x.review?.estadoRevision === "ok").length,
    review_pendiente: consolidado.filter(x => x.review?.estadoRevision === "pendiente").length,
    confirmados: consolidado.filter(x => x.confirmadoEnProduccion).length,
    reservo: consolidado.filter(x => x.origen === "Reservo").length,
    mk: consolidado.filter(x => x.origen === "MK").length
  };
  
  console.log("RESUMEN IMPORT CARGADO:", resumenDebug);

  for (const it of consolidado) {
    recomputeItemFromCurrentValues(it);

    // ✅ Si el documento padre quedó "staged" pero los items ya tienen confirmados,
    // corregimos el estado cargado en memoria para que la UI refleje la realidad.
    const confirmadosDetectados = consolidado.filter(x => x.confirmadoEnProduccion).length;
  
    if (stateImport.status === "staged" && confirmadosDetectados > 0) {
      stateImport.status = "confirmada";
    }

    manualOverrides[it.itemId] = {
      profesionalId: it.resolved?.confirmadoManualProfesional ? (it.resolved?.profesionalId || null) : null,
      procedimientoId: it.resolved?.confirmadoManualProcedimiento ? (it.resolved?.procedimientoId || null) : null
    };
  }

  if (consolidado.some(x => x.origen === "MK")) {
    const mkItems = consolidado.filter(x => x.origen === "MK");
    evaluarAplicacionMK(mkItems);
    mkItems.forEach(it => {
      recomputeItemFromCurrentValues(it);
    });
  }

  uiState.page = 0;
  uiState.q = "";
  if ($("q")) $("q").value = "";

  setStatus(
    stateImport.status === "confirmada"
      ? `✅ Importación confirmada: ${stateImport.importId}`
      : stateImport.status === "anulada"
        ? `⛔ Importación anulada: ${stateImport.importId}`
        : `🟡 Staging cargado: ${stateImport.importId}`
  );

  render();
  toast(`Import cargado: ${importId}`);
}

/* ======================
   IMPORT SUGGESTIONS
====================== */

function formatImportDate(ts) {
  try {
    const d = ts?.toDate ? ts.toDate() : (ts instanceof Date ? ts : null);
    if (!d) return "Sin fecha";
    return new Intl.DateTimeFormat('es-CL', {
      weekday:'long',
      year:'numeric',
      month:'long',
      day:'numeric',
      hour:'2-digit',
      minute:'2-digit'
    }).format(d);
  } catch {
    return "Sin fecha";
  }
}

async function fillImportSuggestions() {
  const sel = $("importSelect");
  if (!sel) return;

  sel.innerHTML = `<option value="">(Selecciona una importación del mes)</option>`;
  if ($("importId")) $("importId").value = "";

  const ano = Number($("ano")?.value || 0) || 0;
  const mesName = clean($("mes")?.value || "");
  const mesNum = monthIndex(mesName);

  if (!ano || !mesNum) return;

  try {
    const qy = query(
      colAmbImports,
      where("ano", "==", ano),
      where("mesNum", "==", mesNum),
      limit(50)
    );

    const snap = await getDocs(qy);

    const docs = [];
    snap.forEach(d => {
      const x = d.data() || {};
      const id = clean(x.id || d.id);
      if (!id) return;

      const ts = x.creadoEl;
      const ms = ts?.toMillis ? ts.toMillis() : (ts instanceof Date ? ts.getTime() : 0);

      docs.push({ id, x, ms });
    });

    docs.sort((a,b) => (b.ms || 0) - (a.ms || 0));

    for (const it of docs) {
      const x = it.x || {};
      const id = it.id;

      const estado = clean(x.estado || "");
      const filas = Number(x.filas || 0) || 0;
      const when = formatImportDate(x.creadoEl);

      const opt = document.createElement("option");
      opt.value = id;
      opt.textContent = `${when} — ${estado || "—"} — ${filas} filas`;
      sel.appendChild(opt);
    }
  } catch (err) {
    console.warn("fillImportSuggestions()", err);
    toast("No se pudieron cargar importaciones");
  }
}

/* ======================
   REEMPLAZAR MES ANTES DE CONFIRMAR
====================== */

async function reemplazarMesAntesDeConfirmar(YYYY, MM, newImportId) {
  const ano = Number(YYYY) || 0;
  const mesNum = Number(MM) || 0;
  if (!ano || !mesNum) return 0;

  const cg = collectionGroup(db, "items");
  let last = null;
  let total = 0;

  const baseWheres = [
    where("ano", "==", ano),
    where("mesNum", "==", mesNum),
    where("estadoRegistro", "==", "activo")
  ];

  while (true) {
    const qy = last
      ? query(cg, ...baseWheres, orderBy("__name__"), startAfter(last), limit(300))
      : query(cg, ...baseWheres, orderBy("__name__"), limit(300));

    const snap = await getDocs(qy);
    if (snap.empty) break;

    const batch = writeBatch(db);

    snap.forEach(d => {
      const path = d.ref.path || "";
      if (!path.startsWith("produccion_ambulatoria/")) return;

      const data = d.data() || {};
      if (clean(data.importId) === clean(newImportId)) return;

      batch.set(d.ref, {
        estadoRegistro: "reemplazada",
        reemplazadoEl: serverTimestamp(),
        reemplazadoPor: stateImport.user?.email || "",
        reemplazadoPorImportId: newImportId || null,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: stateImport.user?.email || ""
      }, { merge: true });

      total++;
    });

    await batch.commit();
    last = snap.docs[snap.docs.length - 1];
  }

  return total;
}

/* ======================
   CONFIRMAR
====================== */

async function confirmarImportacion() {
  if (!(stateImport.status === "staged" || stateImport.status === "confirmada")) {
    toast("Este import no está disponible para confirmar");
    return;
  }

  if (!stateImport.importId) {
    toast("Falta importId");
    return;
  }

  for (const reg of consolidado) {
    aplicarManualOverrides([reg]);
    recomputeItemFromCurrentValues(reg);
  }

  const operables = itemsOperables();

  const itemsConfirmables = operables.filter(it =>
    esItemConfirmable(it) && !it.confirmadoEnProduccion
  );

  const totalConfirmadosActuales = operables.filter(it => it.confirmadoEnProduccion).length;
  const totalPend = operables.filter(x => x.review?.estadoRevision === "pendiente").length;
  const totalRevisar = operables.filter(x => x.aplicacion?.estado === "revisar").length;
  const totalNoAplica = operables.filter(x => x.aplicacion?.estado === "no_aplica").length;
  const totalQuedanFuera = operables.length - (totalConfirmadosActuales + itemsConfirmables.length);

  if (!itemsConfirmables.length) {
    // ✅ Caso especial:
    // no hay nuevos items por confirmar, pero puede que este import
    // ya tenga items confirmados y el doc padre aún haya quedado en "staged".
    if (totalConfirmadosActuales > 0) {
      await setDoc(doc(db, "produccion_ambulatoria_imports", stateImport.importId), {
        estado: "confirmada",
        confirmadoEl: serverTimestamp(),
        confirmadoPor: stateImport.user?.email || "",
        confirmadoEn: `produccion_ambulatoria/${String(stateImport.year)}/meses/${pad(stateImport.monthNum, 2)}/pacientes/{RUT}/items/{itemId}`,
        totalItems: consolidado.length,
        totalConfirmados: totalConfirmadosActuales,
        totalPendientes: consolidado.length - totalConfirmadosActuales,
        totalConfirmadosNuevosUltimaEjecucion: 0,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: stateImport.user?.email || ""
      }, { merge: true });

      stateImport.status = "confirmada";

      setStatus(
        `✅ Import regularizado como confirmado: ${stateImport.importId} · ` +
        `${totalConfirmadosActuales} items ya estaban en producción final`
      );

      render();
      toast("Este import ya tenía ítems confirmados. Se actualizó su estado a confirmada.");
      return;
    }

    toast("No hay nuevos ítems válidos para pasar a producción final.");
    return;
  }

  const ok = confirm(
    `Se pasarán ${itemsConfirmables.length} ítems nuevos a producción final.\n` +
    `\n` +
    `Ya estaban confirmados: ${totalConfirmadosActuales}\n` +
    `Quedarán todavía fuera: ${totalQuedanFuera}\n` +
    `- Pendientes revisión: ${totalPend}\n` +
    `- En revisar: ${totalRevisar}\n` +
    `- No aplica: ${totalNoAplica}\n` +
    `\n` +
    `¿Continuar?`
  );

  if (!ok) return;

  const YYYY = String(stateImport.year);
  const MM = pad(stateImport.monthNum, 2);

  try {
    setStatus(`🟠 Iniciando confirmación... 0/${itemsConfirmables.length}`);
    toast(`Iniciando confirmación de ${itemsConfirmables.length} ítems...`);

    if (stateImport.status === "staged") {
      const replacedCount = await reemplazarMesAntesDeConfirmar(YYYY, MM, stateImport.importId);
      if (replacedCount > 0) {
        toast(`Mes ${YYYY}-${MM}: ${replacedCount} items previos marcados como reemplazados.`);
      }
    }

    await setDoc(doc(db, "produccion_ambulatoria", YYYY), {
      ano: stateImport.year,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    await setDoc(doc(db, "produccion_ambulatoria", YYYY, "meses", MM), {
      ano: stateImport.year,
      mesNum: stateImport.monthNum,
      mes: stateImport.monthName,
      monthId: monthId(stateImport.year, stateImport.monthNum),
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    // Más chico para no saturar Firestore
    const batchSize = 120;
    let i = 0;
    let confirmadosNuevos = 0;

    while (i < itemsConfirmables.length) {
      const batch = writeBatch(db);
      const slice = itemsConfirmables.slice(i, i + batchSize);

      for (const reg of slice) {
        const rutKey = normalizarRutKey(reg.rut || "");
        const pacienteId = rutKey || `SINRUT_${stateImport.importId}`;
        const itemDocId = finalItemId(reg);

        reg.confirmadoEnProduccion = true;
        reg.confirmadoEl = new Date().toISOString();
        reg.confirmadoPor = stateImport.user?.email || "";
        reg.finalItemId = itemDocId;
        reg.pacienteId = pacienteId;

        const refPaciente = doc(
          db,
          "produccion_ambulatoria",
          YYYY,
          "meses",
          MM,
          "pacientes",
          pacienteId
        );

        const refItem = doc(
          db,
          "produccion_ambulatoria",
          YYYY,
          "meses",
          MM,
          "pacientes",
          pacienteId,
          "items",
          itemDocId
        );

        const refStaging = doc(
          db,
          "produccion_ambulatoria_imports",
          stateImport.importId,
          "items",
          reg.itemId
        );

        batch.set(refPaciente, {
          rut: reg.rut || null,
          rutNorm: reg.rutNorm || null,
          paciente: reg.paciente || null,
          pacienteNorm: reg.pacienteNorm || null,
          actualizadoEl: serverTimestamp(),
          actualizadoPor: stateImport.user?.email || ""
        }, { merge: true });

        batch.set(refItem, {
          ...serializeAmbItem(reg),
          finalItemId: itemDocId,
          pacienteId,
          importId: stateImport.importId,
          ano: stateImport.year,
          mesNum: stateImport.monthNum,
          monthId: monthId(stateImport.year, stateImport.monthNum),
          estadoRegistro: "activo",
          reemplazadoEl: null,
          reemplazadoPor: null,
          reemplazadoPorImportId: null,
          creadoEl: serverTimestamp(),
          creadoPor: stateImport.user?.email || "",
          actualizadoEl: serverTimestamp(),
          actualizadoPor: stateImport.user?.email || ""
        }, { merge: true });

        batch.set(refStaging, {
          ...serializeAmbItem(reg),
          estado: "confirmada",
          actualizadoEl: serverTimestamp(),
          actualizadoPor: stateImport.user?.email || ""
        }, { merge: true });
      }

      await batch.commit();

      confirmadosNuevos += slice.length;
      i += batchSize;

      setStatus(`🟠 Confirmando... ${confirmadosNuevos}/${itemsConfirmables.length}`);
      console.log(`CONFIRMACION OK: ${confirmadosNuevos}/${itemsConfirmables.length}`);

      await sleep(250);
    }

    const totalConfirmadosAcumulados = consolidado.filter(x => x.confirmadoEnProduccion).length;
    const totalPendientesAcumulados = consolidado.length - totalConfirmadosAcumulados;

    await setDoc(doc(db, "produccion_ambulatoria_imports", stateImport.importId), {
      estado: "confirmada",
      confirmadoEl: serverTimestamp(),
      confirmadoPor: stateImport.user?.email || "",
      confirmadoEn: `produccion_ambulatoria/${YYYY}/meses/${MM}/pacientes/{RUT}/items/{itemId}`,
      totalItems: consolidado.length,
      totalConfirmados: totalConfirmadosAcumulados,
      totalPendientes: totalPendientesAcumulados,
      totalConfirmadosNuevosUltimaEjecucion: confirmadosNuevos,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    stateImport.status = "confirmada";

    setStatus(
      `✅ Confirmada: ${stateImport.importId} · ` +
      `${totalConfirmadosAcumulados} items ya están en producción final · ` +
      `${totalPendientesAcumulados} siguen pendientes dentro del import`
    );

    render();
    toast(`✅ Confirmación completada: ${confirmadosNuevos} nuevos items pasaron a producción`);
  } catch (err) {
    console.error("Error en confirmarImportacion():", err);

    await setDoc(doc(db, "produccion_ambulatoria_imports", stateImport.importId), {
      estado: "confirmada_error",
      errorConfirmacion: String(err?.message || err || "Error desconocido"),
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    }, { merge: true });

    setStatus(`⚠️ Error durante confirmación: ${stateImport.importId}`);
    toast(`Error durante la confirmación. Revisa consola e import ${stateImport.importId}`);
  }
}

/* ======================
   ANULAR
====================== */

async function anularImportacion() {
  if (!stateImport.importId) {
    toast("No hay importación para anular");
    return;
  }

  const ok = confirm(`¿Anular importación?\n\n${stateImport.importId}\n\n(No se borra; se marca como anulada)`);
  if (!ok) return;

  await setDoc(doc(db, "produccion_ambulatoria_imports", stateImport.importId), {
    estado: "anulada",
    anuladaEl: serverTimestamp(),
    anuladaPor: stateImport.user?.email || "",
    actualizadoEl: serverTimestamp(),
    actualizadoPor: stateImport.user?.email || ""
  }, { merge: true });

  const cg = collectionGroup(db, "items");
  let last = null;
  let total = 0;

  while (true) {
    const qy = last
      ? query(
          cg,
          where("importId", "==", stateImport.importId),
          where("ano", "==", stateImport.year),
          where("mesNum", "==", stateImport.monthNum),
          orderBy("__name__"),
          startAfter(last),
          limit(300)
        )
      : query(
          cg,
          where("importId", "==", stateImport.importId),
          where("ano", "==", stateImport.year),
          where("mesNum", "==", stateImport.monthNum),
          orderBy("__name__"),
          limit(300)
        );

    const snap = await getDocs(qy);
    if (snap.empty) break;

    const batch = writeBatch(db);

    snap.forEach(d => {
      const path = d.ref.path || "";
      if (!path.startsWith("produccion_ambulatoria/")) return;

      batch.set(d.ref, {
        estadoRegistro: "anulada",
        anuladaEl: serverTimestamp(),
        anuladaPor: stateImport.user?.email || "",
        actualizadoEl: serverTimestamp(),
        actualizadoPor: stateImport.user?.email || ""
      }, { merge: true });

      total++;
    });

    await batch.commit();
    last = snap.docs[snap.docs.length - 1];
  }

  stateImport.status = "anulada";
  setStatus(`⛔ Importación anulada: ${stateImport.importId} (${total} items marcados anulados en producción ambulatoria)`);
  render();
  toast("Importación anulada");
}

/* ======================
   EVENTOS
====================== */

if ($("btnCargar")) {
  $("btnCargar").onclick = procesarArchivos;
}

if ($("fileReservo")) {
  $("fileReservo").addEventListener("change", async e => {
    const file = e.target.files?.[0];
    if (!file) return;
    stateImport.filenameReservo = file.name;
    dataReservo = await leerExcel(file);
  });
}

if ($("fileMK")) {
  $("fileMK").addEventListener("change", async e => {
    const file = e.target.files?.[0];
    if (!file) return;
    stateImport.filenameMK = file.name;
    dataMK = await leerExcel(file);
  });
}

if ($("q")) {
  $("q").addEventListener("input", e => {
    uiState.q = e.target.value || "";
    uiState.page = 0;
    render();
  });
}

if ($("btnPrev")) {
  $("btnPrev").onclick = () => {
    uiState.page = Math.max(0, uiState.page - 1);
    render();
  };
}

if ($("btnNext")) {
  $("btnNext").onclick = () => {
    const totalPages = Math.max(1, Math.ceil(filteredItems().length / uiState.pageSize));
    uiState.page = Math.min(totalPages - 1, uiState.page + 1);
    render();
  };
}

if ($("btnToggleNoAplica")) {
  $("btnToggleNoAplica").onclick = () => {
    uiState.mostrarNoAplica = !uiState.mostrarNoAplica;
    uiState.page = 0;
    render();
  };
}

if ($("btnToggleKine")) {
  $("btnToggleKine").onclick = () => {
    uiState.incluirKinesiologia = !uiState.incluirKinesiologia;
    uiState.page = 0;
    render();
  };
}

/* resolver */
if ($("btnResolver")) $("btnResolver").onclick = abrirResolver;
if ($("btnResolverClose")) $("btnResolverClose").onclick = cerrarResolver;
if ($("btnResolverCancelar")) $("btnResolverCancelar").onclick = cerrarResolver;
if ($("btnResolverRevisar")) $("btnResolverRevisar").onclick = renderResolver;

if ($("modalResolverBackdrop")) {
  $("modalResolverBackdrop").addEventListener("click", e => {
    if (e.target === $("modalResolverBackdrop")) cerrarResolver();
  });
}

/* detalle */
if ($("btnItemClose")) $("btnItemClose").onclick = cerrarDetalle;
if ($("btnItemCancelar")) $("btnItemCancelar").onclick = cerrarDetalle;
if ($("btnGuardarItem")) $("btnGuardarItem").onclick = guardarDetalle;
if ($("btnGuardarTodo")) $("btnGuardarTodo").onclick = guardarDetalle;

if ($("modalItemBackdrop")) {
  $("modalItemBackdrop").addEventListener("click", e => {
    if (e.target === $("modalItemBackdrop")) cerrarDetalle();
  });
}

if ($("btnCargarImport")) {
  $("btnCargarImport").onclick = async () => {
    const importId = clean($("importId")?.value || $("importSelect")?.value || "");
    if (!importId) {
      toast("Ingresa o selecciona un ImportID");
      return;
    }
    await loadStagingFromFirestore(importId);
  };
}

if ($("btnConfirmar")) {
  $("btnConfirmar").onclick = confirmarImportacion;
}

if ($("btnAnular")) {
  $("btnAnular").onclick = anularImportacion;
}

if ($("btnLimpiarCola")) {
  $("btnLimpiarCola").onclick = () => {
    manualOverrides = {};
    recalcularTodo();
  };
}

if ($("importSelect")) {
  $("importSelect").addEventListener("change", async () => {
    const importId = clean($("importSelect").value || "");
    if ($("importId")) $("importId").value = importId;
    if (importId) await loadStagingFromFirestore(importId);
  });
}

if ($("mes")) {
  $("mes").addEventListener("change", async () => {
    await fillImportSuggestions();
  });
}

if ($("ano")) {
  $("ano").addEventListener("change", async () => {
    await fillImportSuggestions();
  });
}

/* ======================
   BOOT
====================== */

requireAuth({
  onUser: async (user) => {
    stateImport.user = user;

    await loadSidebar({ active: 'produccion_ambulatoria' });
    setActiveNav('produccion_ambulatoria');

    if ($("who")) {
      $("who").textContent = `Conectado: ${user.email}`;
    }

    wireLogout();
    setDefaultToPreviousMonth();

    await cargarProfesionales();
    await cargarProcedimientos();

    render();
    await fillImportSuggestions();

    const autoImportId = clean($("importSelect")?.value || "");
    if (autoImportId) {
      if ($("importId")) $("importId").value = autoImportId;
      await loadStagingFromFirestore(autoImportId);
    }
  }
});
