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
  actual: null,

  /*
    Indica que el detalle fue abierto desde
    el modal “Resolver alertas”.

    Después de guardar, permite volver automáticamente
    a la lista con los casos restantes.
  */
  volverAResolver: false
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
  resolverFiltro: "base", // base | pendientes | aplica | no_aplica | revisar | todos
  pillFiltro: "" // "" | alertas | aplica | no_aplica | no_aplica_observaciones | cambios_pendientes
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
    it?.review?.estadoRevision === "ok" &&
    !tieneAlertaOperativa(it)
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

function procedimientoForzadoPorTexto(texto) {
  const t = normalizarTexto(texto);

  if (
    t.includes("CONSULTA BARIATRICA TELEMEDICINA") &&
    (t.includes("ISAPRE") || t.includes("PARTICULAR"))
  ) {
    return procedimientos.find(p =>
      p.id === "PA0076" ||
      clean(p.codigo) === "PA0076"
    ) || null;
  }

  return null;
}

function analizarBusquedaProcedimiento(texto) {
  const textoOriginal = clean(texto);
  const t = normalizarTexto(textoOriginal);

  if (!t) {
    return {
      procedimiento: null,
      tipoMatch: "vacio",
      alerta: "Procedimiento vacío"
    };
  }

  const forzado = procedimientoForzadoPorTexto(textoOriginal);
  if (forzado) {
    return {
      procedimiento: forzado,
      tipoMatch: "forzado",
      alerta: null
    };
  }

  // 1) Coincidencia exacta por ID o nombre
  for (const p of procedimientos) {
    const id = normalizarTexto(p?.id || "");
    const nombre = normalizarTexto(nombreProcedimientoCatalogo(p));

    if (!nombre && !id) continue;

    if (t === id || t === nombre) {
      return {
        procedimiento: p,
        tipoMatch: "exacto",
        alerta: null
      };
    }
  }

  // 2) Coincidencia parcial: se sugiere, pero debe revisarse
  const candidatos = procedimientos
    .map(p => {
      const id = normalizarTexto(p?.id || "");
      const nombre = normalizarTexto(nombreProcedimientoCatalogo(p));

      let score = 0;

      if (id && t.includes(id)) score += 10;
      if (nombre && t.includes(nombre)) score += 5;
      if (nombre && nombre.includes(t)) score += 4;

      return { procedimiento: p, id, nombre, score };
    })
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score);

  const mejor = candidatos[0];

  if (!mejor) {
    return {
      procedimiento: null,
      tipoMatch: "sin_match",
      alerta: `No se encontró procedimiento para "${textoOriginal}"`
    };
  }

  return {
    procedimiento: mejor.procedimiento,
    tipoMatch: "parcial",
    alerta:
      `Asociación automática parcial de procedimiento: archivo "${textoOriginal}" ` +
      `→ catálogo "${nombreProcedimientoCatalogo(mejor.procedimiento)}". Revisar antes de confirmar.`
  };
}

function buscarProcedimiento(texto) {
  return analizarBusquedaProcedimiento(texto)?.procedimiento || null;
}

function labelRolCanon(canon) {
  switch (canon) {
    case "CIRUJANO": return "Cirujano/a";
    case "PSICOLOGIA": return "Psicólogo/a";
    case "KINESIOLOGIA": return "Kinesiólogo/a";
    case "NUTRICION": return "Nutricionista";
    case "NUTRIOLOGIA": return "Nutriólogo/a";
    default: return canon || "Rol desconocido";
  }
}

function canonRolDesdeTexto(texto = "") {
  const t = normalizarTexto(texto);

  if (!t) return "";

  if (t.includes("CIRUJAN")) return "CIRUJANO";
  if (t.includes("PSICOLOG")) return "PSICOLOGIA";
  if (t.includes("KINESIO")) return "KINESIOLOGIA";
  if (t.includes("NUTRIOLOG")) return "NUTRIOLOGIA";
  if (t.includes("NUTRICION")) return "NUTRICION";

  return "";
}

function textosRolProfesional(prof) {
  const valores = [];

  const push = (v) => {
    if (Array.isArray(v)) {
      v.forEach(push);
      return;
    }
    if (v === null || v === undefined) return;

    const s = String(v).trim();
    if (s) valores.push(s);
  };

  if (!prof) return valores;

  // Campos más probables del catálogo de profesionales
  push(prof.rolPrincipalId);
  push(prof.rolesSecundariosIds);
  push(prof.rolPrincipal);
  push(prof.rolPrincipalNombre);
  push(prof.rolesSecundarios);
  push(prof.rolesSecundariosNombres);
  push(prof.rol);
  push(prof.roles);
  push(prof.especialidad);
  push(prof.especialidades);

  return valores;
}

function rolesCanonProfesional(prof) {
  const out = new Set();

  textosRolProfesional(prof).forEach(txt => {
    const canon = canonRolDesdeTexto(txt);
    if (canon) out.add(canon);
  });

  return [...out];
}

function rolEsperadoProcedimiento(proc, textoArchivo = "") {
  const universo = [
    proc?.categoria,
    proc?.nombre,
    proc?.tratamiento,
    proc?.procedimiento,
    proc?.descripcion,
    textoArchivo
  ]
    .map(x => normalizarTexto(x))
    .filter(Boolean)
    .join(" | ");

  if (!universo) {
    return { canon: "", label: "" };
  }

  // Orden importante: primero lo más específico
  if (universo.includes("CIRUGIA BARIATR")) {
    return { canon: "CIRUJANO", label: labelRolCanon("CIRUJANO") };
  }

  if (universo.includes("PSICOLOG")) {
    return { canon: "PSICOLOGIA", label: labelRolCanon("PSICOLOGIA") };
  }

  if (universo.includes("KINESIO") || universo.includes("ESPIROMETR")) {
    return { canon: "KINESIOLOGIA", label: labelRolCanon("KINESIOLOGIA") };
  }

  if (universo.includes("NUTRIOLOG")) {
    return { canon: "NUTRIOLOGIA", label: labelRolCanon("NUTRIOLOGIA") };
  }

  if (universo.includes("NUTRICION") || universo.includes("BIOIMPEDANCIOMETR")) {
    return { canon: "NUTRICION", label: labelRolCanon("NUTRICION") };
  }

  return { canon: "", label: "" };
}

function construirAlertaRolProcedimiento({ profesional, procedimiento, textoProcedimientoArchivo = "" }) {
  if (!profesional || !procedimiento) return null;

  const esperado = rolEsperadoProcedimiento(procedimiento, textoProcedimientoArchivo);
  if (!esperado.canon) return null;

  const rolesProf = rolesCanonProfesional(profesional);
  if (!rolesProf.length) return null;

  const coincide = rolesProf.includes(esperado.canon);
  if (coincide) return null;

  const nombreProf = nombreProfesionalCatalogo(profesional) || "Profesional";
  const nombreProc = nombreProcedimientoCatalogo(procedimiento) || clean(textoProcedimientoArchivo) || "Procedimiento";
  const rolesProfLabel = rolesProf.map(labelRolCanon).join(" / ");

  return `Rol no corresponde al procedimiento: "${nombreProc}" exige ${esperado.label} y "${nombreProf}" tiene ${rolesProfLabel}`;
}

function esAlertaBloqueante(alerta = "") {
  const t = normalizarTexto(alerta);

  return (
    t.includes("ROL NO CORRESPONDE AL PROCEDIMIENTO") ||
    t.includes("ASOCIACION AUTOMATICA PARCIAL") ||
    t.includes("COINCIDENCIA AUTOMATICA NO EXACTA")
  );
}

/* ======================
   REVISIÓN / APLICACIÓN
====================== */

function construirReview({ profesionalId, procedimientoId, alertas = [] }) {
  const pendienteProfesional = !profesionalId;
  const pendienteProcedimiento = !procedimientoId;
  const pendienteRol = alertas.some(esAlertaBloqueante);

  return {
    estadoRevision: (!pendienteProfesional && !pendienteProcedimiento && !pendienteRol) ? "ok" : "pendiente",
    pendientes: {
      profesional: pendienteProfesional,
      procedimiento: pendienteProcedimiento,
      rolProcedimiento: pendienteRol
    },
    alertas
  };
}

function construirAplicacion(estado, motivo) {
  return { estado, motivo };
}

function copiarSeguro(valor) {
  try {
    return JSON.parse(JSON.stringify(valor ?? null));
  } catch {
    return null;
  }
}

function valorHistorial(valor) {
  if (valor === null || valor === undefined || valor === "") {
    return "—";
  }

  if (typeof valor === "object") {
    try {
      return JSON.stringify(valor);
    } catch {
      return String(valor);
    }
  }

  return String(valor);
}

function snapshotEditable(reg) {
  const raw = reg.dataReservo || reg.dataMK || {};

  return {
    profesionalId:
      clean(reg.resolved?.profesionalId),

    profesionalNombre:
      clean(
        reg.resolved?.profesionalNombre ||
        reg.profesionalDetectado
      ),

    procedimientoId:
      clean(reg.resolved?.procedimientoId),

    procedimientoNombre:
      clean(
        reg.resolved?.procedimientoNombre ||
        reg.procedimientoDetectado
      ),

    estadoCita:
      reg.origen === "Reservo"
        ? clean(raw["Estado cita"])
        : "",

    estadoPago:
      reg.origen === "Reservo"
        ? clean(raw["Estado pago"])
        : "",

    fecha: clean(reg.fecha),
    rut: clean(reg.rut),
    paciente: clean(reg.paciente),

    profesionalArchivo:
      clean(reg.profesional),

    procedimientoArchivo:
      clean(reg.prestacion),

    valor:
      Number(reg.valor || 0),
    
    aplicacionEstado:
      clean(reg.aplicacion?.estado),
    
    aplicacionMotivo:
      clean(reg.aplicacion?.motivo),
    
    decisionManualEstado:
      clean(reg.decisionManualAplicacion?.estado),
    
    decisionManualMotivo:
      clean(reg.decisionManualAplicacion?.motivo)
  };
}

function snapshotLiquidacion(reg) {
  return {
    rutNorm:
      clean(reg.rutNorm),

    fechaNorm:
      clean(reg.fechaNorm),

    pacienteNorm:
      clean(reg.pacienteNorm),

    profesionalId:
      clean(reg.resolved?.profesionalId),

    procedimientoId:
      clean(reg.resolved?.procedimientoId),

    valor:
      Number(reg.valor || 0),

    aplicacionEstado:
      clean(reg.aplicacion?.estado),

    estadoRevision:
      clean(reg.review?.estadoRevision)
  };
}

function snapshotsIguales(a, b) {
  return JSON.stringify(a || null) ===
    JSON.stringify(b || null);
}

function crearCambiosHistorial(antes = {}, despues = {}) {
  const etiquetas = {
    profesionalId: "Profesional",
    profesionalNombre: "Nombre profesional",
    procedimientoId: "Procedimiento",
    procedimientoNombre: "Nombre procedimiento",
    estadoCita: "Estado de cita",
    estadoPago: "Estado de pago",
    fecha: "Fecha",
    rut: "RUT",
    paciente: "Paciente",
    profesionalArchivo: "Profesional del archivo",
    procedimientoArchivo: "Procedimiento del archivo",
    valor: "Valor",
    aplicacionEstado: "Aplicación",
    aplicacionMotivo: "Motivo de aplicación",
    decisionManualEstado: "Decisión manual",
    decisionManualMotivo: "Motivo de decisión manual"
  };

  const cambios = [];

  for (const campo of Object.keys(etiquetas)) {
    const anterior = valorHistorial(
      antes?.[campo]
    );

    const nuevo = valorHistorial(
      despues?.[campo]
    );

    if (anterior === nuevo) continue;

    cambios.push({
      campo,
      etiqueta: etiquetas[campo],
      antes: anterior,
      despues: nuevo
    });
  }

  return cambios;
}

function registrarHistorialLocal(reg, {
  tipo = "edicion",
  cambios = [],
  observacion = ""
} = {}) {
  reg.historial = Array.isArray(reg.historial)
    ? reg.historial
    : [];

  reg.historial.push({
    id: `${Date.now()}_${Math.random()
      .toString(36)
      .slice(2, 8)}`,

    tipo,
    fecha: new Date().toISOString(),
    usuario: stateImport.user?.email || "",
    observacion: clean(observacion),
    cambios: copiarSeguro(cambios) || []
  });
}

function tieneAlertaOperativa(reg) {
  const estadoAplicacion =
    clean(reg?.aplicacion?.estado);

  /*
    Un NO APLICA nunca es una alerta operativa.

    Puede tener datos incompletos, pero como no se enviará
    a liquidaciones, esos datos quedan como observaciones.
  */

  if (estadoAplicacion === "no_aplica") {
    return false;
  }

  /*
    REVISAR siempre es alerta porque todavía no sabemos
    si el registro corresponde liquidar.
  */

  if (estadoAplicacion === "revisar") {
    return true;
  }

  /*
    Si APLICA, cualquier dato pendiente o alerta interna
    impide enviarlo a liquidaciones.
  */

  if (estadoAplicacion === "aplica") {
    return (
      reg?.review?.estadoRevision === "pendiente" ||
      (reg?.review?.alertas || []).length > 0
    );
  }

  /*
    Si todavía no tiene clasificación, también requiere revisión.
  */

  return true;
}

function tieneObservacionNoAplica(reg) {
  if (reg?.aplicacion?.estado !== "no_aplica") {
    return false;
  }

  return (
    reg?.review?.estadoRevision === "pendiente" ||
    (reg?.review?.alertas || []).length > 0
  );
}

function publicadoAlgunaVez(reg) {
  return (
    reg?.publicadoEnLiquidaciones === true ||
    reg?.confirmadoEnProduccion === true ||
    !!reg?.ultimaVersionPublicada ||
    !!reg?.publicadoPacienteId ||
    !!reg?.publicadoFinalItemId
  );
}

function estaPendienteLiquidacion(reg) {
  if (reg?.cambiosPendientesLiquidacion === true) {
    return true;
  }

  const publicado = publicadoAlgunaVez(reg);

  if (!publicado) {
    return esItemConfirmable(reg);
  }

  if (!reg?.ultimaVersionPublicada) {
    return true;
  }

  return !snapshotsIguales(
    snapshotLiquidacion(reg),
    reg.ultimaVersionPublicada
  );
}

function etiquetaGrupoVisual(reg) {
  /*
    Primero revisamos NO APLICA.

    Esto evita que un NO APLICA con procedimiento pendiente
    termine ubicado incorrectamente dentro de Alertas.
  */

  if (reg?.aplicacion?.estado === "no_aplica") {
    return "no_aplica";
  }

  if (tieneAlertaOperativa(reg)) {
    return "alerta";
  }

  if (reg?.aplicacion?.estado === "aplica") {
    return "aplica";
  }

  return "alerta";
}

function ordenGrupoVisual(reg) {
  const grupo = etiquetaGrupoVisual(reg);

  if (grupo === "alerta") return 1;
  if (grupo === "aplica") return 2;
  if (grupo === "no_aplica") return 3;

  return 4;
}

/* ======================
   RESERVO: ESTADOS
====================== */

function clasificarEstadoCitaReservo(v) {
  const t = normalizarTexto(v);

  if (!t) return "otro";
  if (t.includes("ATENDID")) return "atendido";
  if (t.includes("NO LLEGO")) return "no_llego";
  if (t.includes("SUSPEND")) return "suspendido";

  return "otro";
}

function clasificarEstadoPagoReservo(v) {
  const t = normalizarTexto(v);

  if (!t) return "otro";
  if (t.includes("NO PAG")) return "no_pagado";
  if (t.includes("PAGAD")) return "pagado";
  if (t.includes("PLAN")) return "plan";
  if (t.includes("DESCARTAD")) return "descartado";

  return "otro";
}

function esProfesionalExcluidoReservo(nombre = "") {
  const t = normalizarTexto(nombre);

  return (
    t.includes("ELIZABETH ROMO") ||
    t.includes("NICOLAS SANDOVAL")
  );
}

function esInstalacionBalonAllurion(tratamiento = "") {
  const t = normalizarTexto(tratamiento);

  return (
    t.includes("INSTALACION") &&
    t.includes("BALON") &&
    t.includes("ALLURION")
  );
}

function esControlPad(tratamiento = "") {
  const t = normalizarTexto(tratamiento);

  return (
    t.includes("CONTROL POST CIRUGIA") &&
    t.includes("PAD")
  );
}

function esEvaluacionNutricionalPadTelemedicina(tratamiento = "") {
  const t = normalizarTexto(tratamiento);

  return (
    t.includes("EVALUACION NUTRICIONAL") &&
    t.includes("POST CIRUGIA") &&
    t.includes("TELEMEDICINA") &&
    t.includes("PAD")
  );
}

function evaluarAplicacionReservo(raw = {}) {
  const estadoCita = clasificarEstadoCitaReservo(raw["Estado cita"]);
  const estadoPago = clasificarEstadoPagoReservo(raw["Estado pago"]);

  const profesional = clean(raw["Profesional"]);
  const tratamiento = clean(raw["Tratamiento"]);
  const valor = normalizarMonto(raw["Valor"]);

  const alertas = [];

  /*
    EXCEPCIONES ESPECÍFICAS
    Tienen prioridad sobre las reglas generales.
  */

  if (
    esProfesionalExcluidoReservo(profesional) &&
    estadoPago === "pagado"
  ) {
    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        "Profesional excluido cuando el pago está pagado"
      ),
      alertas
    };
  }

  if (esInstalacionBalonAllurion(tratamiento)) {
    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        "Instalación Balón Gástrico Allurion"
      ),
      alertas
    };
  }

  if (
    esControlPad(tratamiento) &&
    estadoPago === "descartado"
  ) {
    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        "Control Post Cirugía PAD con pago descartado"
      ),
      alertas
    };
  }

  /*
    SUSPENDIDOS
  */

  if (estadoCita === "suspendido") {
    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        `Cita suspendida con pago ${estadoPago}`
      ),
      alertas
    };
  }

  /*
    NO LLEGÓ
  */

  if (
    estadoCita === "no_llego" &&
    (estadoPago === "pagado" || estadoPago === "plan")
  ) {
    return {
      aplicacion: construirAplicacion(
        "aplica",
        estadoPago === "pagado"
          ? "No llegó y pagado"
          : "No llegó y plan"
      ),
      alertas
    };
  }

  if (
    estadoCita === "no_llego" &&
    estadoPago === "descartado"
  ) {
    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        "No llegó y pago descartado"
      ),
      alertas
    };
  }

  /*
    ATENDIDOS Y PAGADOS/PLAN
  */

  if (
    estadoCita === "atendido" &&
    (estadoPago === "pagado" || estadoPago === "plan")
  ) {
    return {
      aplicacion: construirAplicacion(
        "aplica",
        estadoPago === "pagado"
          ? "Atendido y pagado"
          : "Atendido y plan"
      ),
      alertas
    };
  }

  /*
    ATENDIDO + DESCARTADO

    Por defecto NO APLICA.
    La única excepción es Evaluación Nutricional PAD
    Telemedicina con valor exacto de $8.500.
  */

  if (
    estadoCita === "atendido" &&
    estadoPago === "descartado"
  ) {
    if (
      esEvaluacionNutricionalPadTelemedicina(tratamiento) &&
      Math.abs(valor) === 8500
    ) {
      return {
        aplicacion: construirAplicacion(
          "aplica",
          "Evaluación Nutricional PAD Telemedicina descartada por $8.500"
        ),
        alertas
      };
    }

    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        "Atendido con pago descartado"
      ),
      alertas
    };
  }

  if (
    estadoCita === "atendido" &&
    estadoPago === "no_pagado"
  ) {
    return {
      aplicacion: construirAplicacion(
        "no_aplica",
        "Atendido sin pago"
      ),
      alertas
    };
  }

  /*
    ESTADOS QUE EL SISTEMA NO PUEDE DECIDIR
  */

  if (
    estadoCita === "otro" &&
    ["pagado", "plan", "descartado"].includes(estadoPago)
  ) {
    alertas.push(
      "Estado de cita no reconocido: decidir manualmente si aplica"
    );

    return {
      aplicacion: construirAplicacion(
        "revisar",
        "Estado de cita no reconocido"
      ),
      alertas
    };
  }

  return {
    aplicacion: construirAplicacion(
      "no_aplica",
      "Combinación no válida para liquidar"
    ),
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
      if (ov.procedimientoId) {
        it.resolved.autoProcedimientoTipoMatch = "manual";
      }
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
    const analisisProc = analizarBusquedaProcedimiento(r["Tratamiento"]);
    const procedimientoDetectado = analisisProc?.procedimiento || null;

    const evalApp = evaluarAplicacionReservo(r);
    const alertas = [...evalApp.alertas];
    
    if (analisisProf?.alerta) alertas.push(analisisProf.alerta);
    if (analisisProc?.alerta) alertas.push(analisisProc.alerta);
    
    
    if (!normalizarRut(r["Rut"])) alertas.push("RUT vacío o inválido");
    if (!normalizarTexto(r["Profesional"])) alertas.push("Profesional vacío");
    if (!normalizarTexto(r["Tratamiento"])) alertas.push("Procedimiento vacío");
    
    const alertaRolProcedimiento = construirAlertaRolProcedimiento({
      profesional: profesionalDetectado,
      procedimiento: procedimientoDetectado,
      textoProcedimientoArchivo: r["Tratamiento"]
    });
    
    if (alertaRolProcedimiento) {
      alertas.push(alertaRolProcedimiento);
    }

    const resolved = {
      profesionalId: profesionalDetectado?.id || null,
      profesionalNombre: profesionalDetectado ? nombreProfesionalCatalogo(profesionalDetectado) : null,

      procedimientoId: procedimientoDetectado?.id || null,
      procedimientoNombre: procedimientoDetectado ? nombreProcedimientoCatalogo(procedimientoDetectado) : null,

      autoProfesional: !!profesionalDetectado,
      autoProcedimiento: !!procedimientoDetectado,
      autoProcedimientoTipoMatch: analisisProc?.tipoMatch || null,
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
    const analisisProc = analizarBusquedaProcedimiento(r["D Artículo"]);
    const procedimientoDetectado = analisisProc?.procedimiento || null;

    const alertas = [];
    if (analisisProf?.alerta) alertas.push(analisisProf.alerta);
    if (analisisProc?.alerta) alertas.push(analisisProc.alerta);
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
      autoProcedimientoTipoMatch: analisisProc?.tipoMatch || null,
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

function limpiarAlertasAutoProcedimiento(alertas = []) {
  return (alertas || []).filter(a => {
    const t = normalizarTexto(a);
    return !(
      t.includes("ASOCIACION AUTOMATICA PARCIAL") ||
      t.includes("COINCIDENCIA AUTOMATICA NO EXACTA")
    );
  });
}

/* ======================
   NORMALIZAR ITEM REHIDRATADO
====================== */

function recomputeItemFromCurrentValues(reg) {
  reg.resolved = reg.resolved || {
    profesionalId: null,
    profesionalNombre: null,
    procedimientoId: null,
    procedimientoNombre: null,
    autoProfesional: false,
    autoProcedimiento: false,
    autoProcedimientoTipoMatch: null,
    confirmadoManualProfesional: false,
    confirmadoManualProcedimiento: false
  };

  reg.rutNorm = normalizarRut(reg.rut);
  reg.pacienteNorm = normalizarPaciente(reg.paciente);
  reg.profesionalNorm = normalizarTexto(reg.profesional);
  reg.procedimientoNorm = normalizarTexto(reg.prestacion);
  reg.fechaNorm = normalizarFecha(reg.fecha);
  reg.valor = normalizarMonto(reg.valor);

  /*
    PROFESIONAL
  */

  let profesionalDetectado = null;
  let alertaProfesional = null;

  if (reg.resolved.profesionalId) {
    profesionalDetectado =
      profesionales.find(p => p.id === reg.resolved.profesionalId) ||
      null;
  } else {
    const analisis = analizarBusquedaProfesional(reg.profesional);

    profesionalDetectado = analisis?.profesional || null;
    alertaProfesional = analisis?.alerta || null;

    if (profesionalDetectado?.id) {
      reg.resolved.profesionalId = profesionalDetectado.id;
      reg.resolved.profesionalNombre =
        nombreProfesionalCatalogo(profesionalDetectado);
      reg.resolved.autoProfesional = true;
    }
  }

  /*
    PROCEDIMIENTO
  */

  let procedimientoDetectado = null;
  let alertaProcedimiento = null;
  let tipoMatchProcedimiento = null;

  if (reg.resolved.procedimientoId) {
    procedimientoDetectado =
      procedimientos.find(p => p.id === reg.resolved.procedimientoId) ||
      null;

    tipoMatchProcedimiento =
      reg.resolved.confirmadoManualProcedimiento
        ? "manual"
        : reg.resolved.autoProcedimientoTipoMatch;
  } else {
    const analisis = analizarBusquedaProcedimiento(reg.prestacion);

    procedimientoDetectado = analisis?.procedimiento || null;
    alertaProcedimiento = analisis?.alerta || null;
    tipoMatchProcedimiento = analisis?.tipoMatch || null;

    if (procedimientoDetectado?.id) {
      reg.resolved.procedimientoId = procedimientoDetectado.id;
      reg.resolved.procedimientoNombre =
        nombreProcedimientoCatalogo(procedimientoDetectado);
      reg.resolved.autoProcedimiento = true;
      reg.resolved.autoProcedimientoTipoMatch =
        tipoMatchProcedimiento;
    }
  }

  if (profesionalDetectado) {
    reg.resolved.profesionalNombre =
      nombreProfesionalCatalogo(profesionalDetectado);
  }

  if (procedimientoDetectado) {
    reg.resolved.procedimientoNombre =
      nombreProcedimientoCatalogo(procedimientoDetectado);
  }

  reg.profesionalDetectado =
    reg.resolved.profesionalNombre || null;

  reg.procedimientoDetectado =
    reg.resolved.procedimientoNombre || null;

  /*
    APLICACIÓN

    Si existe decisión manual, no volvemos a reemplazarla
    con la clasificación automática.
  */

  let alertas = [];

  if (reg.decisionManualAplicacion?.estado) {
    reg.aplicacion = construirAplicacion(
      reg.decisionManualAplicacion.estado,
      reg.decisionManualAplicacion.motivo ||
        "Decisión manual"
    );
  } else if (reg.origen === "Reservo") {
    const evaluacion = evaluarAplicacionReservo(
      reg.dataReservo || {}
    );

    reg.aplicacion = evaluacion.aplicacion;
    alertas.push(...(evaluacion.alertas || []));
  } else if (reg.origen === "MK") {
    reg.aplicacion =
      reg.aplicacion ||
      construirAplicacion("no_aplica", "Sin evaluar");
  }

  /*
    ALERTAS DE DATOS
  */

  if (!reg.rutNorm) {
    alertas.push("RUT vacío o inválido");
  }

  if (!reg.resolved.profesionalId) {
    if (alertaProfesional) {
      alertas.push(alertaProfesional);
    } else {
      alertas.push("Profesional pendiente de resolver");
    }
  }

  if (!reg.resolved.procedimientoId) {
    if (alertaProcedimiento) {
      alertas.push(alertaProcedimiento);
    } else {
      alertas.push("Procedimiento pendiente de resolver");
    }
  }

  if (
    reg.resolved.procedimientoId &&
    !reg.resolved.confirmadoManualProcedimiento &&
    tipoMatchProcedimiento === "parcial"
  ) {
    alertas.push(
      `Asociación automática parcial de procedimiento: archivo "${reg.prestacion}" → catálogo "${reg.resolved.procedimientoNombre || ""}". Revisar antes de enviar.`
    );
  }

  const profesionalCatalogo =
    profesionales.find(
      p => p.id === reg.resolved.profesionalId
    ) || null;

  const procedimientoCatalogo =
    procedimientos.find(
      p => p.id === reg.resolved.procedimientoId
    ) || null;

  const alertaRol = construirAlertaRolProcedimiento({
    profesional: profesionalCatalogo,
    procedimiento: procedimientoCatalogo,
    textoProcedimientoArchivo: reg.prestacion || ""
  });

  if (alertaRol) {
    alertas.push(alertaRol);
  }

  /*
    Si profesional o procedimiento fueron escogidos
    manualmente, quitamos alertas automáticas antiguas.
  */

  if (reg.resolved.confirmadoManualProfesional) {
    alertas = alertas.filter(a => {
      const t = normalizarTexto(a);

      return !(
        t.includes("COINCIDENCIA AMBIGUA EN PROFESIONAL") ||
        t.includes("NO SE ENCONTRO COINCIDENCIA") ||
        t.includes("PROFESIONAL PENDIENTE")
      );
    });
  }

  if (reg.resolved.confirmadoManualProcedimiento) {
    alertas = limpiarAlertasAutoProcedimiento(alertas);

    alertas = alertas.filter(a => {
      const t = normalizarTexto(a);

      return !(
        t.includes("PROCEDIMIENTO PENDIENTE") ||
        t.includes("NO SE ENCONTRO PROCEDIMIENTO")
      );
    });
  }

  /*
    Una decisión manual APLICA/NO APLICA resuelve
    únicamente la duda de aplicación.
    No elimina problemas de profesional/procedimiento/RUT.
  */

  if (reg.decisionManualAplicacion?.estado) {
    alertas = alertas.filter(a => {
      const t = normalizarTexto(a);

      return !(
        t.includes("DECIDIR MANUALMENTE SI APLICA") ||
        t.includes("ESTADO DE CITA NO RECONOCIDO")
      );
    });
  }

  alertas = [...new Set(alertas.filter(Boolean))];

  reg.review = construirReview({
    profesionalId: reg.resolved.profesionalId || null,
    procedimientoId: reg.resolved.procedimientoId || null,
    alertas
  });
}

/* ======================
   DETALLE / EDICIÓN
====================== */

function abrirDetalle(
  reg,
  {
    volverAResolver = false
  } = {}
) {
  const modal = $("modalItemBackdrop");
  const itemSub = $("itemSub");
  const itemForm = $("itemForm");

  if (!modal || !itemSub || !itemForm) {
    console.warn("No existe el modal de detalle en el HTML");
    return;
  }

  stateEdicion.actual = reg;
  stateEdicion.volverAResolver =
    volverAResolver === true;

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
        <input
          type="text"
          data-extra-key="${escapeHtml(key)}"
          value="${escapeHtml(String(value))}"
        >
      </div>
    `;
  }).join("");

  const historial = Array.isArray(reg.historial)
    ? [...reg.historial].reverse()
    : [];

  const historialHTML = historial.length
    ? historial.map(h => `
        <div style="
          padding:10px;
          border:1px solid rgba(0,0,0,.08);
          border-radius:10px;
          margin-top:8px;
        ">
          <div style="font-weight:900;">
            ${escapeHtml(h.usuario || "Usuario")} ·
            ${escapeHtml(
              h.fecha
                ? new Date(h.fecha).toLocaleString("es-CL")
                : ""
            )}
          </div>

          ${
            h.observacion
              ? `<div class="muted tiny" style="margin-top:4px;">
                   Motivo: ${escapeHtml(h.observacion)}
                 </div>`
              : ""
          }

          ${(h.cambios || []).map(c => `
            <div class="tiny" style="margin-top:5px;">
              <b>${escapeHtml(c.etiqueta || c.campo || "Campo")}:</b>
              ${escapeHtml(c.antes || "—")}
              →
              ${escapeHtml(c.despues || "—")}
            </div>
          `).join("")}
        </div>
      `).join("")
    : `<div class="muted tiny">Este registro todavía no tiene modificaciones manuales.</div>`;

  itemForm.innerHTML = `
    <div class="card" style="padding:12px;">
      <div style="
        display:flex;
        justify-content:space-between;
        align-items:center;
        gap:10px;
        margin-bottom:12px;
      ">
        <div>
          <div class="sectionTitle">Editar información</div>
          <div class="help">
            El dato original importado se conserva. Estos serán los valores operativos utilizados para liquidaciones.
          </div>
        </div>

        <button
          id="btnVolverDetalle"
          type="button"
          class="btn"
        >
          ← Volver a resolución
        </button>
      </div>

      <section
        style="
          padding:12px;
          border:1px solid rgba(0,0,0,.08);
          border-radius:12px;
          margin-bottom:14px;
          background:#f8fafc;
        "
      >
        <div class="sectionTitle">Decisión de aplicación</div>

        <div class="grid2">
          <div class="field">
            <label>¿Corresponde enviar a liquidaciones?</label>
            <select id="detalleAplicacionManual">
              <option value="">Usar clasificación automática</option>
              <option
                value="aplica"
                ${reg.decisionManualAplicacion?.estado === "aplica" ? "selected" : ""}
              >
                APLICA
              </option>
              <option
                value="no_aplica"
                ${reg.decisionManualAplicacion?.estado === "no_aplica" ? "selected" : ""}
              >
                NO APLICA
              </option>
            </select>
          </div>

          <div class="field">
            <label>Motivo u observación del cambio</label>
            <input
              id="detalleMotivoCambio"
              type="text"
              placeholder="Ej: prestación sí correspondía liquidar"
              value=""
            >
          </div>
        </div>

        <div class="help">
          Al guardar, la alerta de aplicación desaparecerá si la decisión ya quedó resuelta. No necesitas una segunda confirmación.
        </div>
      </section>

      <div class="sectionTitle">Campos operativos del registro</div>

      <div class="grid2">
        ${filas}
      </div>

      <div style="height:16px;"></div>

      <section
        style="
          padding:12px;
          border:1px solid rgba(0,0,0,.08);
          border-radius:12px;
        "
      >
        <div class="sectionTitle">
          Historial (${historial.length})
        </div>

        ${historialHTML}
      </section>
    </div>
  `;

  if ($("btnVolverDetalle")) {
    $("btnVolverDetalle").onclick = () => abrirDetalle(reg);
  }
}

async function guardarDetalle() {
  if (!stateEdicion.actual) return;

  const reg = stateEdicion.actual;
  const antes = snapshotEditable(reg);

  /*
    Conserva una fotografía realmente inmutable
    del archivo antes de la primera modificación.
  */

  if (!reg.originalImportado) {
    reg.originalImportado = copiarSeguro(
      reg.dataReservo || reg.dataMK || {}
    );
  }

  /*
    EDICIÓN DE CAMPOS DEL ARCHIVO OPERATIVO
  */

  const extraInputs =
    document.querySelectorAll("[data-extra-key]");

  if (extraInputs.length) {
    const target = reg.dataReservo
      ? reg.dataReservo
      : reg.dataMK;

    extraInputs.forEach(inp => {
      const key = inp.getAttribute("data-extra-key");

      if (!key || !target) return;
      target[key] = inp.value;
    });

    if (reg.dataReservo) {
      reg.fecha =
        reg.dataReservo["Fecha"] ?? reg.fecha;

      reg.rut =
        reg.dataReservo["Rut"] ?? reg.rut;

      reg.paciente =
        reg.dataReservo["Paciente"] ?? reg.paciente;

      reg.profesional =
        reg.dataReservo["Profesional"] ?? reg.profesional;

      reg.prestacion =
        reg.dataReservo["Tratamiento"] ?? reg.prestacion;

      reg.valor =
        normalizarMonto(reg.dataReservo["Valor"]);
    } else if (reg.dataMK) {
      reg.fecha =
        reg.dataMK["Fecha"] ?? reg.fecha;

      reg.rut =
        reg.dataMK["Rut"] ?? reg.rut;

      reg.paciente =
        reg.dataMK["Paciente"] ?? reg.paciente;

      reg.profesional =
        reg.dataMK["D Médico"] ?? reg.profesional;

      reg.prestacion =
        reg.dataMK["D Artículo"] ?? reg.prestacion;

      reg.valor =
        normalizarMonto(reg.dataMK["Total"]);
    }
  }

  /*
    PROFESIONAL Y PROCEDIMIENTO
  */

  const profSel = $("detalleProfesionalId");
  const procSel = $("detalleProcedimientoId");

  if (profSel) {
    const profesionalId = profSel.value || null;
    const profesional = profesionales.find(
      p => p.id === profesionalId
    ) || null;

    reg.resolved = reg.resolved || {};

    reg.resolved.profesionalId =
      profesional?.id || null;

    reg.resolved.profesionalNombre =
      profesional
        ? nombreProfesionalCatalogo(profesional)
        : null;

    reg.resolved.confirmadoManualProfesional =
      !!profesionalId;

    reg.resolved.autoProfesional = false;
  }

  if (procSel) {
    const procedimientoId = procSel.value || null;
    const procedimiento = procedimientos.find(
      p => p.id === procedimientoId
    ) || null;

    reg.resolved = reg.resolved || {};

    reg.resolved.procedimientoId =
      procedimiento?.id || null;

    reg.resolved.procedimientoNombre =
      procedimiento
        ? nombreProcedimientoCatalogo(procedimiento)
        : null;

    reg.resolved.confirmadoManualProcedimiento =
      !!procedimientoId;

    reg.resolved.autoProcedimiento = false;

    reg.resolved.autoProcedimientoTipoMatch =
      procedimientoId ? "manual" : null;
  }

  /*
    APLICA / NO APLICA
  */

  const aplicacionManual =
    clean($("detalleAplicacionManual")?.value || "");

  const motivoCambio =
    clean($("detalleMotivoCambio")?.value || "");

  if (aplicacionManual) {
    if (!motivoCambio) {
      toast(
        "Debes indicar el motivo de la decisión manual"
      );
      return;
    }

    reg.decisionManualAplicacion = {
      estado: aplicacionManual,
      motivo: motivoCambio,
      fecha: new Date().toISOString(),
      usuario: stateImport.user?.email || ""
    };
  } else if ($("detalleAplicacionManual")) {
    reg.decisionManualAplicacion = null;
  }

  /*
    RECALCULAR Y CREAR HISTORIAL
  */

  recomputeItemFromCurrentValues(reg);

  const despues = snapshotEditable(reg);
  const cambios = crearCambiosHistorial(
    antes,
    despues
  );

  if (!cambios.length) {
    toast("No se detectaron cambios");
    return;
  }

  registrarHistorialLocal(reg, {
    tipo: "edicion",
    cambios,
    observacion: motivoCambio
  });

  /*
    Guardar no actualiza liquidaciones.
    Solo marca que existe una diferencia pendiente.
  */

  reg.cambiosPendientesLiquidacion = true;

  await persistirItemEditado(reg);
  
  /*
    Guardamos esta información antes de cerrar el detalle,
    porque cerrarDetalle() limpia stateEdicion.
  */
  
  const volverAResolver =
    stateEdicion.volverAResolver === true;
  
  const sigueConAlerta =
    tieneAlertaOperativa(reg);
  
  cerrarDetalle();
  render();
  
  if (sigueConAlerta) {
    toast(
      "Cambio guardado, pero el registro todavía mantiene alertas pendientes."
    );
  } else {
    toast(
      "Cambio guardado. La alerta quedó resuelta."
    );
  }
  
  /*
    Si abrimos el registro desde Resolver alertas,
    volvemos automáticamente a la lista.
  
    Si ya no quedan alertas, permanecemos en la vista
    principal porque el trabajo terminó.
  */
  
  if (volverAResolver) {
    const alertasRestantes =
      obtenerAlertasOperativas();
  
    if (alertasRestantes.length > 0) {
      abrirResolver();
    } else {
      toast(
        "Todas las alertas de la importación quedaron resueltas."
      );
    }
  }
}
      
function cerrarDetalle() {
  const modal =
    $("modalItemBackdrop");

  const itemForm =
    $("itemForm");

  if (modal) {
    modal.style.display = "none";
  }

  if (itemForm) {
    itemForm.innerHTML = "";
  }

  stateEdicion.actual = null;
  stateEdicion.volverAResolver = false;
}

/* ======================
   PERSISTENCIA ITEM EDITADO
====================== */

async function persistirItemEditado(reg) {
  if (!stateImport.importId) {
    toast("No hay una importación cargada");
    return;
  }

  if (
    stateImport.status === "anulada" ||
    stateImport.status === "confirmada_error"
  ) {
    toast("Esta importación no está disponible para editar");
    return;
  }

  const refStaging = doc(
    db,
    "produccion_ambulatoria_imports",
    stateImport.importId,
    "items",
    reg.itemId
  );

  await setDoc(refStaging, {
    ...serializeAmbItem(reg),
    estado: reg.confirmadoEnProduccion
      ? "confirmada"
      : "staged",
    actualizadoEl: serverTimestamp(),
    actualizadoPor: stateImport.user?.email || ""
  }, { merge: true });

  /*
    Guardar solamente actualiza el import bruto/editable.
    Producción final se modifica después mediante
    “Actualizar liquidaciones”.
  */

  await setDoc(
    doc(
      db,
      "produccion_ambulatoria_imports",
      stateImport.importId
    ),
    {
      tieneCambiosPendientesLiquidacion:
        consolidado.some(estaPendienteLiquidacion),
      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || ""
    },
    { merge: true }
  );
}

function serializeAmbItem(reg) {
  const profesionalId =
    reg.resolved?.profesionalId || null;

  const procedimientoId =
    reg.resolved?.procedimientoId || null;

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
    profesionalDetectado:
      reg.profesionalDetectado || null,

    prestacion: reg.prestacion || null,
    procedimientoNorm:
      reg.procedimientoNorm || null,
    procedimientoDetectado:
      reg.procedimientoDetectado || null,

    valor: Number(reg.valor || 0) || 0,

    /*
      originalImportado nunca se modifica.
      dataReservo/dataMK contienen el estado operativo actual.
    */

    originalImportado:
      reg.originalImportado ||
      copiarSeguro(reg.dataReservo || reg.dataMK || {}),

    dataReservo: reg.dataReservo || null,
    dataMK: reg.dataMK || null,

    resolved: reg.resolved || null,

    profesionalId,
    rutProfesional: profesionalId,

    procedimientoId,
    ambulatorioId: procedimientoId,

    procedimientoNombre:
      reg.resolved?.procedimientoNombre ||
      reg.procedimientoDetectado ||
      null,

    profesionalNombre:
      reg.resolved?.profesionalNombre ||
      reg.profesionalDetectado ||
      null,

    normalizado: {
      profesionalId,
      rutProfesional: profesionalId,
      procedimientoId,
      ambulatorioId: procedimientoId,

      procedimientoNombre:
        reg.resolved?.procedimientoNombre ||
        reg.procedimientoDetectado ||
        null,

      profesionalNombre:
        reg.resolved?.profesionalNombre ||
        reg.profesionalDetectado ||
        null
    },

    aplicacion: reg.aplicacion || null,
    decisionManualAplicacion:
      reg.decisionManualAplicacion || null,

    review: reg.review || null,

    historial: Array.isArray(reg.historial)
      ? reg.historial
      : [],

    cambiosPendientesLiquidacion:
      reg.cambiosPendientesLiquidacion === true,

    ultimaVersionPublicada:
      reg.ultimaVersionPublicada || null,

    publicadoEnLiquidaciones:
      reg.publicadoEnLiquidaciones === true ||
      reg.confirmadoEnProduccion === true,

    publicadoPacienteId:
      reg.publicadoPacienteId ||
      reg.pacienteId ||
      null,

    publicadoFinalItemId:
      reg.publicadoFinalItemId ||
      reg.finalItemId ||
      null,

    confirmadoEnProduccion:
      !!reg.confirmadoEnProduccion,

    confirmadoEl:
      reg.confirmadoEl || null,

    confirmadoPor:
      reg.confirmadoPor || null,

    finalItemId:
      reg.finalItemId || null,

    pacienteId:
      reg.pacienteId || null
  };
}

/* ======================
   RESOLVER PENDIENTES
====================== */

function causasAlertaRegistro(reg) {
  const causas = [];

  /*
    Aplicación todavía sin decidir.
  */

  if (reg?.aplicacion?.estado === "revisar") {
    causas.push(
      reg.aplicacion?.motivo ||
      "Debes decidir si el registro aplica o no aplica"
    );
  }

  /*
    Profesional pendiente.
  */

  if (
    reg?.review?.pendientes?.profesional === true ||
    !reg?.resolved?.profesionalId
  ) {
    causas.push("Falta asociar el profesional");
  }

  /*
    Procedimiento pendiente.
  */

  if (
    reg?.review?.pendientes?.procedimiento === true ||
    !reg?.resolved?.procedimientoId
  ) {
    causas.push("Falta asociar el procedimiento");
  }

  /*
    Alertas específicas guardadas en el registro.
  */

  for (const alerta of reg?.review?.alertas || []) {
    if (!clean(alerta)) continue;
    causas.push(clean(alerta));
  }

  /*
    Quitamos textos repetidos.
  */

  return [...new Set(causas)];
}

function tarjetaAlertaResolverHTML(reg, numero) {
  const causas =
    causasAlertaRegistro(reg);

  const aplicacion =
    reg?.aplicacion?.estado === "aplica"
      ? "APLICA"
      : reg?.aplicacion?.estado === "revisar"
        ? "POR DEFINIR"
        : etiquetaAplicacion(
            reg?.aplicacion?.estado || ""
          );

  const causasHTML = causas.length
    ? causas.map(causa => `
        <li style="margin-top:5px;">
          ${escapeHtml(causa)}
        </li>
      `).join("")
    : `
        <li>
          El registro necesita revisión manual.
        </li>
      `;

  return `
    <article
      class="miniRow"
      style="
        display:block;
        border-left:5px solid #f59e0b;
        padding:14px;
      "
    >
      <div style="
        display:flex;
        justify-content:space-between;
        align-items:flex-start;
        gap:14px;
      ">
        <div style="min-width:0; flex:1;">
          <div style="
            display:flex;
            align-items:center;
            gap:8px;
            flex-wrap:wrap;
          ">
            <span
              class="pill warn"
              style="
                min-width:28px;
                justify-content:center;
                padding:3px 8px;
              "
            >
              ${numero}
            </span>

            <strong style="font-size:14px;">
              ${escapeHtml(reg.paciente || "Paciente sin nombre")}
            </strong>

            <span class="muted tiny">
              ${escapeHtml(reg.rut || "Sin RUT")}
            </span>
          </div>

          <div
            class="tiny"
            style="margin-top:8px;"
          >
            <b>Fecha:</b>
            ${escapeHtml(reg.fecha || "—")}
          </div>

          <div
            class="tiny"
            style="margin-top:4px;"
          >
            <b>Profesional archivo:</b>
            ${escapeHtml(reg.profesional || "—")}
          </div>

          <div
            class="tiny"
            style="margin-top:4px;"
          >
            <b>Profesional resuelto:</b>
            ${escapeHtml(
              reg.resolved?.profesionalNombre ||
              reg.profesionalDetectado ||
              "Pendiente"
            )}
          </div>

          <div
            class="tiny"
            style="margin-top:4px;"
          >
            <b>Procedimiento archivo:</b>
            ${escapeHtml(reg.prestacion || "—")}
          </div>

          <div
            class="tiny"
            style="margin-top:4px;"
          >
            <b>Procedimiento resuelto:</b>
            ${escapeHtml(
              reg.resolved?.procedimientoNombre ||
              reg.procedimientoDetectado ||
              "Pendiente"
            )}
          </div>

          <div style="
            display:flex;
            gap:8px;
            flex-wrap:wrap;
            margin-top:9px;
          ">
            <span class="pill">
              Aplicación: ${escapeHtml(aplicacion)}
            </span>

            <span class="pill warn">
              Revisión pendiente
            </span>
          </div>

          <div
            style="
              margin-top:12px;
              padding:10px;
              border-radius:10px;
              background:#fffbeb;
              border:1px solid #fde68a;
            "
          >
            <div
              class="tiny"
              style="
                font-weight:900;
                color:#92400e;
              "
            >
              ¿QUÉ DEBES RESOLVER?
            </div>

            <ul
              class="tiny"
              style="
                margin:5px 0 0 18px;
                padding:0;
                color:#92400e;
              "
            >
              ${causasHTML}
            </ul>
          </div>
        </div>

        <button
          class="btn primary"
          type="button"
          data-resolver-item="${escapeHtml(reg.itemId)}"
          style="flex:0 0 auto;"
        >
          Resolver
        </button>
      </div>
    </article>
  `;
}

function bindBotonesResolver(container) {
  if (!container) return;

  container
    .querySelectorAll("[data-resolver-item]")
    .forEach(btn => {
      btn.onclick = () => {
        const itemId =
          btn.getAttribute(
            "data-resolver-item"
          );

        const reg = consolidado.find(
          item => item.itemId === itemId
        );

        if (!reg) {
          toast(
            "No se encontró el registro seleccionado"
          );
          return;
        }

        /*
          Cerramos la lista y abrimos el detalle.

          Indicamos que, después de guardar,
          debemos volver a “Resolver alertas”.
        */

        cerrarResolver();

        abrirDetalle(reg, {
          volverAResolver: true
        });
      };
    });
}

function obtenerAlertasOperativas() {
  return itemsOperables()
    .filter(tieneAlertaOperativa)
    .sort((a, b) => {
      const fecha =
        clean(b.fechaNorm).localeCompare(
          clean(a.fechaNorm)
        );

      if (fecha !== 0) return fecha;

      return (
        Number(a.sourceIndex || 0) -
        Number(b.sourceIndex || 0)
      );
    });
}

function renderResolver() {
  const resumen =
    $("resolverResumen");

  const lista =
    $("resolverCoincidenciasList");

  if (!resumen || !lista) return;

  const alertas =
    obtenerAlertasOperativas();

  resumen.innerHTML = `
    <div style="
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:12px;
      flex-wrap:wrap;
    ">
      <div>
        <div
          class="muted tiny"
          style="margin-bottom:4px;"
        >
          Trabajo obligatorio antes de actualizar liquidaciones
        </div>

        <div style="
          font-size:18px;
          font-weight:900;
        ">
          Alertas pendientes:
          <span class="${alertas.length ? "warn" : "ok"}">
            ${alertas.length}
          </span>
        </div>
      </div>

      ${
        alertas.length
          ? `
              <span class="pill warn">
                Debes resolver ${alertas.length}
              </span>
            `
          : `
              <span class="pill ok">
                Todo resuelto
              </span>
            `
      }
    </div>

    <div
      class="help"
      style="margin-top:8px;"
    >
      Cada registro aparece una sola vez. Presiona
      <b>Resolver</b>, realiza los cambios y luego
      presiona <b>Guardar cambios</b>.
    </div>
  `;

  if (!alertas.length) {
    lista.innerHTML = `
      <div style="
        padding:26px 16px;
        text-align:center;
        border:1px solid #bbf7d0;
        background:#f0fdf4;
        border-radius:12px;
      ">
        <div style="
          font-size:32px;
          margin-bottom:8px;
        ">
          ✓
        </div>

        <div
          class="ok"
          style="font-size:16px;"
        >
          No quedan alertas pendientes
        </div>

        <div
          class="muted tiny"
          style="margin-top:6px;"
        >
          Los registros resueltos ya están disponibles
          en Aplican o No aplican.
        </div>
      </div>
    `;

    return;
  }

  lista.innerHTML = alertas
    .map((reg, index) =>
      tarjetaAlertaResolverHTML(
        reg,
        index + 1
      )
    )
    .join("");

  bindBotonesResolver(lista);
}

function abrirResolver() {
  const modal =
    $("modalResolverBackdrop");

  if (!modal) return;

  renderResolver();
  modal.style.display = "block";
}

function cerrarResolver() {
  const modal =
    $("modalResolverBackdrop");

  if (modal) {
    modal.style.display = "none";
  }
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
  "ESPIROMOETRIA",
  "KIT KINE",
  "CONSULTA SIN COSTO"
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

function aplicarFiltroPill(items) {
  switch (uiState.pillFiltro) {
    case "alertas":
      return items.filter(
        tieneAlertaOperativa
      );

    case "aplica":
      return items.filter(it =>
        it.aplicacion?.estado === "aplica" &&
        !tieneAlertaOperativa(it)
      );

    case "no_aplica":
      return items.filter(it =>
        it.aplicacion?.estado === "no_aplica"
      );

    case "no_aplica_observaciones":
      return items.filter(it =>
        tieneObservacionNoAplica(it)
      );

    case "cambios_pendientes":
      return items.filter(
        estaPendienteLiquidacion
      );

    case "":
    default:
      return items;
  }
}

function filteredItems() {
  let items = itemsOperables();

  items = aplicarFiltroPill(items);

  if (clean(uiState.q)) {
    items = items.filter(it => {
      const text = itemSearchText(it);
      return matchBusquedaPrincipal(
        text,
        uiState.q
      );
    });
  }

  /*
    Orden obligatorio:
    1. Alertas
    2. Aplican
    3. No aplican
  */

  return [...items].sort((a, b) => {
    const grupo =
      ordenGrupoVisual(a) -
      ordenGrupoVisual(b);

    if (grupo !== 0) return grupo;

    const fecha =
      clean(b.fechaNorm).localeCompare(
        clean(a.fechaNorm)
      );

    if (fecha !== 0) return fecha;

    return Number(a.sourceIndex || 0) -
      Number(b.sourceIndex || 0);
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

function badgeResolucionHTML(tipo, reg) {
  const resolved = reg.resolved || {};

  const isProf = tipo === "profesional";

  const id = isProf
    ? resolved.profesionalId
    : resolved.procedimientoId;

  const manual = isProf
    ? resolved.confirmadoManualProfesional
    : resolved.confirmadoManualProcedimiento;

  const auto = isProf
    ? resolved.autoProfesional
    : resolved.autoProcedimiento;

  const tipoMatch = isProf
    ? null
    : resolved.autoProcedimientoTipoMatch;

  if (!id) {
    return `<div class="tiny warn">PENDIENTE</div>`;
  }

  if (manual) {
    return `<div class="tiny ok">MANUAL</div>`;
  }

  if (auto && tipoMatch === "parcial") {
    return `<div class="tiny warn">AUTO · REVISAR</div>`;
  }

  if (auto) {
    return `<div class="tiny ok">AUTO</div>`;
  }

  return `<div class="tiny muted">RESUELTO</div>`;
}

function textoProfesionalResueltoHTML(reg) {
  const nombre = reg.profesionalDetectado || reg.resolved?.profesionalNombre || "—";

  return `
    <div>${escapeHtml(nombre)}</div>
    ${badgeResolucionHTML("profesional", reg)}
  `;
}

function textoProcedimientoResueltoHTML(reg) {
  const nombre = reg.procedimientoDetectado || reg.resolved?.procedimientoNombre || "—";

  return `
    <div>${escapeHtml(nombre)}</div>
    ${badgeResolucionHTML("procedimiento", reg)}
  `;
}

function estadoRevisionVisibleHTML(reg) {
  if (reg?.aplicacion?.estado === "no_aplica") {
    if (tieneObservacionNoAplica(reg)) {
      return `
        <span class="muted">
          No requerida
        </span>
        <div class="tiny warn">
          Con observaciones
        </div>
      `;
    }

    return `
      <span class="muted">
        No requerida
      </span>
    `;
  }

  if (reg?.review?.estadoRevision === "ok") {
    return `<span class="ok">OK</span>`;
  }

  return `<span class="warn">Pendiente</span>`;
}

function alertasVisiblesHTML(reg) {
  const alertas =
    reg?.review?.alertas || [];

  if (!alertas.length) {
    if (reg?.aplicacion?.estado === "no_aplica") {
      return `
        <span class="muted">
          Sin observaciones
        </span>
      `;
    }

    return `<span class="muted">—</span>`;
  }

  const texto =
    escapeHtml(alertas.join(" · "));

  if (reg?.aplicacion?.estado === "no_aplica") {
    return `
      <div class="muted tiny">
        <b>Observación opcional:</b>
        ${texto}
      </div>
    `;
  }

  return `
    <div class="warn tiny">
      <b>Alerta:</b>
      ${texto}
    </div>
  `;
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
      <th>Profesional resuelto</th>
      <th>Procedimiento archivo</th>
      <th>Procedimiento resuelto</th>
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

    const pendienteLiquidacion =
      estaPendienteLiquidacion(r);
    
    const estadoFinalHtml = pendienteLiquidacion
      ? `<span class="warn">Cambio sin enviar</span>`
      : (
          r.publicadoEnLiquidaciones ||
          r.confirmadoEnProduccion
            ? `<span class="ok">Actualizado en liquidaciones</span>`
            : `<span class="muted">Sin publicar</span>`
        );
    
    tr.dataset.grupo = etiquetaGrupoVisual(r);
    
    tr.innerHTML = `
      <td>${from + i + 1}</td>
      <td>${escapeHtml(r.origen || "")}</td>
      <td>${escapeHtml(r.fecha || "")}</td>
      <td>${escapeHtml(r.rut || "")}</td>
      <td>${escapeHtml(r.paciente || "")}</td>
      <td>${escapeHtml(r.profesional || "")}</td>
      <td>${textoProfesionalResueltoHTML(r)}</td>
      <td>${escapeHtml(r.prestacion || "")}</td>
      <td>${textoProcedimientoResueltoHTML(r)}</td>
      <td>${escapeHtml(r.valor ?? "")}</td>
      <td>
        ${estadoRevisionVisibleHTML(r)}
      </td>
      <td>${escapeHtml(r.aplicacion?.estado || "—")}</td>
      <td class="wrap">${escapeHtml(r.aplicacion?.motivo || "—")}</td>
      <td class="wrap">
        ${alertasVisiblesHTML(r)}
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
  const ocultosKine =
    totalPrestacionesExcluidasOcultas();
  
  const alertas = operables.filter(
    tieneAlertaOperativa
  ).length;
  
  const aplican = operables.filter(it =>
    it.aplicacion?.estado === "aplica" &&
    !tieneAlertaOperativa(it)
  ).length;
  
  const noAplica = operables.filter(it =>
    it.aplicacion?.estado === "no_aplica"
  ).length;
  
  const observacionesNoAplica =
    operables.filter(
      tieneObservacionNoAplica
    ).length;
  
  const cambiosPendientes =
    operables.filter(
      estaPendienteLiquidacion
    ).length;
  
  const publicados = operables.filter(it =>
    it.publicadoEnLiquidaciones === true ||
    it.confirmadoEnProduccion === true
  ).length;
  
  if ($("countPill")) {
    $("countPill").textContent =
      `Vista: ${items.length} · Total import: ${consolidado.length}`;
  }
  
  if ($("pillAlertas")) {
    $("pillAlertas").textContent =
      `Alertas: ${alertas}`;
  }
  
  if ($("pillReservoValidos")) {
    $("pillReservoValidos").textContent =
      `Aplican: ${aplican}`;
  }
  
  if ($("pillFusionados")) {
    $("pillFusionados").textContent =
      `No aplican: ${noAplica}`;
  }

  if ($("pillNoAplicaObservaciones")) {
    $("pillNoAplicaObservaciones").textContent =
      `No aplican con Observaciones: ${observacionesNoAplica}`;
  }
  
  if ($("pillConfirmables")) {
    $("pillConfirmables").textContent =
      `Cambios sin enviar: ${cambiosPendientes}`;
  }
  
  /*
    Ocultamos contadores antiguos que ya no corresponden
    al flujo simplificado.
  */
  
  [
    "pillProf",
    "pillProc",
    "pillMKValidos",
    "pillOk"
  ].forEach(id => {
    const el = $(id);
    if (el) el.style.display = "none";
  });

  if ($("pagerInfo")) {
    $("pagerInfo").textContent =
      `${items.length} resultados · página ${uiState.page + 1} de ${totalPages}`;
  }

  if ($("statusInfo")) {
    const totalAmbulatorios =
      procedimientosAmbulatorios().length;
  
    const est = stateImport.status
      ? ` · Estado import: ${stateImport.status}`
      : "";
  
    const imp = stateImport.importId
      ? ` · ImportID: ${stateImport.importId}`
      : "";
  
    const kineTxt = uiState.incluirKinesiologia
      ? " · Prestaciones excluidas: activas"
      : ` · Prestaciones excluidas: ocultas (${ocultosKine})`;
  
    $("statusInfo").textContent =
      consolidado.length
        ? (
            `Alertas: ${alertas}` +
            ` · Aplican: ${aplican}` +
            ` · No aplican: ${noAplica}` +
            ` · Publicados: ${publicados}` +
            ` · Cambios sin enviar: ${cambiosPendientes}` +
            est +
            imp +
            kineTxt +
            ` · Catálogos: ${profesionales.length} profesionales` +
            ` · ${totalAmbulatorios} procedimientos ambulatorios`
          )
        : "—";
  }

  if ($("btnToggleKine")) {
    $("btnToggleKine").textContent = uiState.incluirKinesiologia
      ? "Ocultar prestaciones excluidas"
      : "Activar prestaciones excluidas";
  }

  if ($("btnResolver")) $("btnResolver").disabled = consolidado.length === 0;

  if ($("btnConfirmar")) {
    const cambiosPendientes =
      itemsOperables().filter(
        estaPendienteLiquidacion
      ).length;
  
    const puedeActualizar =
      (
        stateImport.status === "staged" ||
        stateImport.status === "confirmada"
      ) &&
      cambiosPendientes > 0;
  
    $("btnConfirmar").disabled =
      !puedeActualizar;
  
    $("btnConfirmar").textContent =
      cambiosPendientes > 0
        ? `Actualizar liquidaciones (${cambiosPendientes})`
        : "Liquidaciones actualizadas";
  
    $("btnConfirmar").title =
      puedeActualizar
        ? `${cambiosPendientes} cambios pendientes de enviar`
        : "No hay cambios pendientes de enviar";
  }

  if ($("btnAnular")) {
    $("btnAnular").disabled = !(stateImport.importId && (stateImport.status === "staged" || stateImport.status === "confirmada"));
  }

  renderPagerTabs(totalPages);
}

function togglePillFiltro(nombreFiltro) {
  uiState.pillFiltro =
    uiState.pillFiltro === nombreFiltro
      ? ""
      : nombreFiltro;

  uiState.page = 0;
  render();
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

  // ✅ Limpia resoluciones manuales anteriores.
  // Evita que un cambio hecho en otro import se aplique por error
  // a un nuevo RES_0001, RES_0002, MK_0001, etc.
  manualOverrides = {};
  stateEdicion.actual = null;

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
function sincronizarResolvedDesdeEspejos(reg) {
  const procedimientoId = clean(
    reg.procedimientoId ||
    reg.ambulatorioId ||
    reg.normalizado?.procedimientoId ||
    reg.normalizado?.ambulatorioId ||
    reg.resolved?.procedimientoId ||
    ""
  );

  const profesionalId = clean(
    reg.profesionalId ||
    reg.rutProfesional ||
    reg.normalizado?.profesionalId ||
    reg.normalizado?.rutProfesional ||
    reg.resolved?.profesionalId ||
    ""
  );

  const proc = procedimientos.find(p =>
    clean(p.id) === procedimientoId ||
    clean(p.codigo) === procedimientoId
  ) || null;

  const prof = profesionales.find(p =>
    clean(p.id) === profesionalId ||
    clean(p.rutId) === profesionalId ||
    clean(p.rut) === profesionalId
  ) || null;

  reg.resolved = {
    ...(reg.resolved || {}),
    profesionalId: profesionalId || reg.resolved?.profesionalId || null,
    profesionalNombre: prof
      ? nombreProfesionalCatalogo(prof)
      : (reg.resolved?.profesionalNombre || reg.profesionalDetectado || null),

    procedimientoId: procedimientoId || reg.resolved?.procedimientoId || null,
    procedimientoNombre: proc
      ? nombreProcedimientoCatalogo(proc)
      : (reg.resolved?.procedimientoNombre || reg.procedimientoDetectado || null)
  };

  reg.profesionalDetectado = reg.resolved.profesionalNombre;
  reg.procedimientoDetectado = reg.resolved.procedimientoNombre;
}

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
      
      // ✅ Campos espejo guardados para que el modal use lo mismo que tabla/liquidación
      profesionalId: x.profesionalId || null,
      rutProfesional: x.rutProfesional || null,
      procedimientoId: x.procedimientoId || null,
      ambulatorioId: x.ambulatorioId || null,
      procedimientoNombre: x.procedimientoNombre || null,
      profesionalNombre: x.profesionalNombre || null,
      normalizado: x.normalizado || null,
      
      valor: Number(x.valor || 0) || 0,
      originalImportado:
        x.originalImportado ||
        copiarSeguro(x.dataReservo || x.dataMK || {}),
      
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
      
      decisionManualAplicacion:
        x.decisionManualAplicacion || null,
      
      review: x.review || null,
      
      historial: Array.isArray(x.historial)
        ? x.historial
        : [],
      
      cambiosPendientesLiquidacion:
        x.cambiosPendientesLiquidacion === true,
      
      ultimaVersionPublicada:
        x.ultimaVersionPublicada || null,
      
      publicadoEnLiquidaciones:
        x.publicadoEnLiquidaciones === true ||
        x.confirmadoEnProduccion === true ||
        x.estado === "confirmada",
      
      publicadoPacienteId:
        x.publicadoPacienteId ||
        x.pacienteId ||
        null,
      
      publicadoFinalItemId:
        x.publicadoFinalItemId ||
        x.finalItemId ||
        null,
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
    // ✅ Primero fuerza resolved desde los espejos corregidos
    sincronizarResolvedDesdeEspejos(it);
  
    // ✅ Luego recalcula estado/alertas usando resolved ya correcto
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
  if (
    !(
      stateImport.status === "staged" ||
      stateImport.status === "confirmada"
    )
  ) {
    toast(
      "Esta importación no está disponible para actualizar liquidaciones"
    );
    return;
  }

  if (!stateImport.importId) {
    toast("Falta ImportID");
    return;
  }

  /*
    Recalcular sin borrar decisiones manuales.
  */

  for (const reg of consolidado) {
    recomputeItemFromCurrentValues(reg);
  }

  const operables = itemsOperables();

  const cambios = operables.filter(
    estaPendienteLiquidacion
  );

  if (!cambios.length) {
    toast(
      "No hay cambios pendientes de actualizar en liquidaciones"
    );
    return;
  }

  const conAlertas = cambios.filter(
    tieneAlertaOperativa
  );

  const paraPublicar = cambios.filter(reg =>
    esItemConfirmable(reg)
  );
  
  const paraRetirar = cambios.filter(reg => {
    const publicadoAntes =
      reg.publicadoEnLiquidaciones === true ||
      reg.confirmadoEnProduccion === true;
  
    return (
      publicadoAntes &&
      !tieneAlertaOperativa(reg) &&
      reg.aplicacion?.estado === "no_aplica"
    );
  });
  
  const noPublicadosResueltos = cambios.filter(reg => {
    const publicadoAntes =
      reg.publicadoEnLiquidaciones === true ||
      reg.confirmadoEnProduccion === true;
  
    return (
      !publicadoAntes &&
      !tieneAlertaOperativa(reg) &&
      reg.aplicacion?.estado === "no_aplica"
    );
  });
  
  if (conAlertas.length) {
    toast(
      `Hay ${conAlertas.length} cambios con alertas. Se actualizarán solamente los registros resueltos.`
    );
  }

  const totalProcesables =
    paraPublicar.length +
    paraRetirar.length +
    noPublicadosResueltos.length;

  if (!totalProcesables) {
    toast(
      "Todos los cambios pendientes mantienen alertas sin resolver"
    );
    return;
  }

  const ok = confirm(
    `Se actualizarán las liquidaciones con el estado actual del import.\n\n` +
    `Agregar o actualizar: ${paraPublicar.length}\n` +
    `Retirar de liquidaciones: ${paraRetirar.length}\n` +
    `No aplican sin publicación previa: ${noPublicadosResueltos.length}\n` +
    `Con alertas que quedarán pendientes: ${conAlertas.length}\n\n` +
    `¿Continuar?`
  );

  if (!ok) return;

  const YYYY = String(stateImport.year);
  const MM = pad(stateImport.monthNum, 2);

  try {
    setStatus(
      `Actualizando liquidaciones... 0/${totalProcesables}`
    );

    /*
      Solamente en la primera publicación reemplazamos
      importaciones anteriores del mismo mes.
    */

    if (stateImport.status === "staged") {
      const reemplazados =
        await reemplazarMesAntesDeConfirmar(
          YYYY,
          MM,
          stateImport.importId
        );

      if (reemplazados > 0) {
        toast(
          `${reemplazados} registros anteriores del mes fueron reemplazados`
        );
      }
    }

    await setDoc(
      doc(db, "produccion_ambulatoria", YYYY),
      {
        ano: stateImport.year,
        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      },
      { merge: true }
    );

    await setDoc(
      doc(
        db,
        "produccion_ambulatoria",
        YYYY,
        "meses",
        MM
      ),
      {
        ano: stateImport.year,
        mesNum: stateImport.monthNum,
        mes: stateImport.monthName,
        monthId: monthId(
          stateImport.year,
          stateImport.monthNum
        ),
        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      },
      { merge: true }
    );

    let procesados = 0;

    /*
      1. AGREGAR O ACTUALIZAR
    */

    for (const reg of paraPublicar) {
      const rutKey =
        normalizarRutKey(reg.rut || "");

      const pacienteId =
        rutKey ||
        `SINRUT_${stateImport.importId}`;

      const itemDocId =
        finalItemId(reg);

      /*
        Si cambió RUT o fecha, puede cambiar la ruta.
        En ese caso retiramos la publicación anterior.
      */

      const pacienteAnterior =
        reg.publicadoPacienteId ||
        reg.pacienteId ||
        null;

      const itemAnterior =
        reg.publicadoFinalItemId ||
        reg.finalItemId ||
        null;

      if (
        pacienteAnterior &&
        itemAnterior &&
        (
          pacienteAnterior !== pacienteId ||
          itemAnterior !== itemDocId
        )
      ) {
        const refAnterior = doc(
          db,
          "produccion_ambulatoria",
          YYYY,
          "meses",
          MM,
          "pacientes",
          pacienteAnterior,
          "items",
          itemAnterior
        );

        await setDoc(refAnterior, {
          estadoRegistro: "retirado",
          retiradoEl: serverTimestamp(),
          retiradoPor:
            stateImport.user?.email || "",
          motivoRetiro:
            "Ruta modificada desde el import",
          actualizadoEl: serverTimestamp(),
          actualizadoPor:
            stateImport.user?.email || ""
        }, { merge: true });
      }

      reg.confirmadoEnProduccion = true;
      reg.publicadoEnLiquidaciones = true;
      reg.confirmadoEl =
        new Date().toISOString();
      reg.confirmadoPor =
        stateImport.user?.email || "";

      reg.finalItemId = itemDocId;
      reg.pacienteId = pacienteId;

      reg.publicadoFinalItemId = itemDocId;
      reg.publicadoPacienteId = pacienteId;

      reg.ultimaVersionPublicada =
        snapshotLiquidacion(reg);

      reg.cambiosPendientesLiquidacion =
        false;

      registrarHistorialLocal(reg, {
        tipo: "publicacion_liquidaciones",
        observacion:
          "Registro agregado o actualizado en liquidaciones",
        cambios: []
      });

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

      const batch = writeBatch(db);

      batch.set(refPaciente, {
        rut: reg.rut || null,
        rutNorm: reg.rutNorm || null,
        paciente: reg.paciente || null,
        pacienteNorm:
          reg.pacienteNorm || null,
        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      }, { merge: true });

      batch.set(refItem, {
        ...serializeAmbItem(reg),

        finalItemId: itemDocId,
        pacienteId,

        importId: stateImport.importId,
        ano: stateImport.year,
        mesNum: stateImport.monthNum,

        monthId: monthId(
          stateImport.year,
          stateImport.monthNum
        ),

        estadoRegistro: "activo",

        retiradoEl: null,
        retiradoPor: null,
        motivoRetiro: null,

        reemplazadoEl: null,
        reemplazadoPor: null,
        reemplazadoPorImportId: null,

        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      }, { merge: true });

      batch.set(refStaging, {
        ...serializeAmbItem(reg),
        estado: "confirmada",
        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      }, { merge: true });

      await batch.commit();

      procesados++;

      setStatus(
        `Actualizando liquidaciones... ${procesados}/${totalProcesables}`
      );
    }

    /*
      2. RETIRAR REGISTROS QUE ANTES APLICABAN
    */

    for (const reg of paraRetirar) {
      const pacienteId =
        reg.publicadoPacienteId ||
        reg.pacienteId;

      const itemDocId =
        reg.publicadoFinalItemId ||
        reg.finalItemId;

      if (pacienteId && itemDocId) {
        const refPublicado = doc(
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

        await setDoc(refPublicado, {
          estadoRegistro: "retirado",
          retiradoEl: serverTimestamp(),
          retiradoPor:
            stateImport.user?.email || "",
          motivoRetiro:
            reg.aplicacion?.motivo ||
            "El registro dejó de aplicar",
          actualizadoEl: serverTimestamp(),
          actualizadoPor:
            stateImport.user?.email || ""
        }, { merge: true });
      }

      reg.publicadoEnLiquidaciones = false;
      reg.confirmadoEnProduccion = false;
      reg.cambiosPendientesLiquidacion =
        false;

      reg.ultimaVersionPublicada =
        snapshotLiquidacion(reg);

      registrarHistorialLocal(reg, {
        tipo: "retiro_liquidaciones",
        observacion:
          reg.aplicacion?.motivo ||
          "Registro retirado de liquidaciones",
        cambios: []
      });

      await setDoc(
        doc(
          db,
          "produccion_ambulatoria_imports",
          stateImport.importId,
          "items",
          reg.itemId
        ),
        {
          ...serializeAmbItem(reg),
          estado: "confirmada",
          actualizadoEl: serverTimestamp(),
          actualizadoPor:
            stateImport.user?.email || ""
        },
        { merge: true }
      );

      procesados++;

      setStatus(
        `Actualizando liquidaciones... ${procesados}/${totalProcesables}`
      );
    }

    /*
      3. NO APLICA QUE NUNCA FUE PUBLICADO

      No hay que crear ni retirar nada.
      Solamente dejamos constancia de que su estado
      actual ya fue revisado en la sincronización.
    */

    for (const reg of noPublicadosResueltos) {
      reg.cambiosPendientesLiquidacion =
        false;

      reg.ultimaVersionPublicada =
        snapshotLiquidacion(reg);

      registrarHistorialLocal(reg, {
        tipo: "sincronizacion_no_aplica",
        observacion:
          "Registro revisado y mantenido fuera de liquidaciones",
        cambios: []
      });

      await setDoc(
        doc(
          db,
          "produccion_ambulatoria_imports",
          stateImport.importId,
          "items",
          reg.itemId
        ),
        {
          ...serializeAmbItem(reg),
          estado: "confirmada",
          actualizadoEl: serverTimestamp(),
          actualizadoPor:
            stateImport.user?.email || ""
        },
        { merge: true }
      );

      procesados++;

      setStatus(
        `Actualizando liquidaciones... ${procesados}/${totalProcesables}`
      );
    }

    const pendientesRestantes =
      consolidado.filter(
        estaPendienteLiquidacion
      ).length;

    const alertasRestantes =
      consolidado.filter(
        tieneAlertaOperativa
      ).length;

    await setDoc(
      doc(
        db,
        "produccion_ambulatoria_imports",
        stateImport.importId
      ),
      {
        estado: "confirmada",

        confirmadoEl: serverTimestamp(),
        confirmadoPor:
          stateImport.user?.email || "",

        ultimaSincronizacionLiquidaciones:
          serverTimestamp(),

        totalItems: consolidado.length,

        totalPublicados:
          consolidado.filter(it =>
            it.publicadoEnLiquidaciones === true ||
            it.confirmadoEnProduccion === true
          ).length,

        totalAlertasPendientes:
          alertasRestantes,

        totalCambiosPendientesLiquidacion:
          pendientesRestantes,

        tieneCambiosPendientesLiquidacion:
          pendientesRestantes > 0,

        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      },
      { merge: true }
    );

    stateImport.status = "confirmada";

    setStatus(
      `Liquidaciones actualizadas · ` +
      `${paraPublicar.length} agregados/actualizados · ` +
      `${paraRetirar.length} retirados · ` +
      `${alertasRestantes} alertas pendientes`
    );

    render();

    toast(
      `Liquidaciones actualizadas correctamente: ${procesados} registros procesados`
    );
  } catch (err) {
    console.error(
      "Error en confirmarImportacion():",
      err
    );

    await setDoc(
      doc(
        db,
        "produccion_ambulatoria_imports",
        stateImport.importId
      ),
      {
        estado: "confirmada_error",
        errorConfirmacion:
          String(
            err?.message ||
            err ||
            "Error desconocido"
          ),
        actualizadoEl: serverTimestamp(),
        actualizadoPor:
          stateImport.user?.email || ""
      },
      { merge: true }
    );

    setStatus(
      `Error actualizando liquidaciones: ${stateImport.importId}`
    );

    toast(
      "Ocurrió un error actualizando liquidaciones. Revisa la consola."
    );
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

if ($("btnToggleKine")) {
  $("btnToggleKine").onclick = () => {
    uiState.incluirKinesiologia = !uiState.incluirKinesiologia;
    uiState.page = 0;
    render();
  };
}

if ($("pillTodos")) {
  $("pillTodos").onclick = () => {
    uiState.pillFiltro = "";
    uiState.page = 0;
    render();
  };
}

if ($("pillAlertas")) {
  $("pillAlertas").onclick = () =>
    togglePillFiltro("alertas");
}

if ($("pillReservoValidos")) {
  $("pillReservoValidos").onclick = () =>
    togglePillFiltro("aplica");
}

if ($("pillFusionados")) {
  $("pillFusionados").onclick = () =>
    togglePillFiltro("no_aplica");
}

if ($("pillNoAplicaObservaciones")) {
  $("pillNoAplicaObservaciones").onclick = () =>
    togglePillFiltro(
      "no_aplica_observaciones"
    );
}

if ($("pillConfirmables")) {
  $("pillConfirmables").onclick = () =>
    togglePillFiltro(
      "cambios_pendientes"
    );
}

/* resolver */
if ($("btnResolver")) {
  $("btnResolver").onclick =
    abrirResolver;
}

if ($("btnResolverClose")) {
  $("btnResolverClose").onclick =
    cerrarResolver;
}

if ($("btnResolverCancelar")) {
  $("btnResolverCancelar").onclick =
    cerrarResolver;
}

if ($("modalResolverBackdrop")) {
  $("modalResolverBackdrop").addEventListener("click", e => {
    if (e.target === $("modalResolverBackdrop")) cerrarResolver();
  });
}

/* detalle */
if ($("btnItemClose")) $("btnItemClose").onclick = cerrarDetalle;
if ($("btnItemCancelar")) $("btnItemCancelar").onclick = cerrarDetalle;
if ($("btnGuardarItem")) {
  $("btnGuardarItem").onclick = guardarDetalle;
}

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

window.sincronizarAmbulatoriosRetroactivo = async function({
  ano,
  mesNum,
  dryRun = true
} = {}) {
  if (!ano || !mesNum) {
    throw new Error("Debes indicar ano y mesNum. Ej: { ano: 2026, mesNum: 5, dryRun: true }");
  }

  const cg = collectionGroup(db, "items");

  const qy = query(
    cg,
    where("ano", "==", Number(ano)),
    where("mesNum", "==", Number(mesNum)),
    where("estadoRegistro", "==", "activo")
  );

  const snap = await getDocs(qy);

  let revisados = 0;
  let deAmbulatoria = 0;
  let paraActualizarProduccion = 0;
  let paraActualizarImport = 0;
  let actualizadosProduccion = 0;
  let actualizadosImport = 0;

  for (const d of snap.docs) {
    const path = d.ref.path || "";
    if (!path.startsWith("produccion_ambulatoria/")) continue;

    revisados++;
    deAmbulatoria++;

    const x = d.data() || {};
    const resolved = x.resolved && typeof x.resolved === "object" ? x.resolved : {};

    // ✅ Prioriza los campos espejo ya corregidos.
    // Si usamos primero resolved, podemos volver a copiar el dato viejo al modal.
    const profesionalId = clean(
      x.profesionalId ||
      x.rutProfesional ||
      resolved.profesionalId ||
      ""
    );
    
    const procForzado = procedimientoForzadoPorTexto(
      x.prestacion ||
      x.dataReservo?.["Tratamiento"] ||
      x.dataMK?.["D Artículo"] ||
      x.procedimientoNombre ||
      resolved.procedimientoNombre ||
      ""
    );
    
    const procedimientoId = clean(
      procForzado?.id ||
      procForzado?.codigo ||
      x.procedimientoId ||
      x.ambulatorioId ||
      resolved.procedimientoId ||
      ""
    );
    
    const procDoc = procForzado || procedimientos.find(p =>
      clean(p.id) === procedimientoId ||
      clean(p.codigo) === procedimientoId
    ) || null;
    
    const procedimientoNombre = procDoc
      ? nombreProcedimientoCatalogo(procDoc)
      : clean(x.procedimientoNombre || x.procedimientoDetectado || resolved.procedimientoNombre || "");

    if (!profesionalId && !procedimientoId) continue;

    const payload = {
      profesionalId,
      rutProfesional: profesionalId,

      procedimientoId,
      ambulatorioId: procedimientoId,
      procedimientoNombre,
      procedimientoDetectado: procedimientoNombre,

      normalizado: {
        ...(x.normalizado || {}),
        profesionalId,
        rutProfesional: profesionalId,
        procedimientoId,
        ambulatorioId: procedimientoId
      },

      resolved: {
        ...(x.resolved || {}),
        profesionalId,
        procedimientoId,
        procedimientoNombre
      },

      actualizadoEl: serverTimestamp(),
      actualizadoPor: stateImport.user?.email || "script-sincronizar-ambulatorios"
    };

    const necesitaProduccion =
      x.profesionalId !== profesionalId ||
      x.rutProfesional !== profesionalId ||
      x.procedimientoId !== procedimientoId ||
      x.ambulatorioId !== procedimientoId ||
      x.normalizado?.procedimientoId !== procedimientoId ||
      x.normalizado?.ambulatorioId !== procedimientoId ||
      x.normalizado?.profesionalId !== profesionalId;

    if (necesitaProduccion) {
      paraActualizarProduccion++;

      console.log("🟡 Producción final a sincronizar:", {
        path,
        paciente: x.paciente || x.pacienteNorm || "",
        profesionalAntes: x.profesionalId || x.rutProfesional || "",
        profesionalDespues: profesionalId,
        procedimientoAntes: x.procedimientoId || x.ambulatorioId || "",
        procedimientoDespues: procedimientoId
      });

      if (!dryRun) {
        await setDoc(d.ref, payload, { merge: true });
        actualizadosProduccion++;
      }
    }

    // ✅ IMPORTANTE:
    // Esto se ejecuta aunque producción final ya esté OK.
    // Sirve para corregir tabla/modal de Producción Ambulatoria.
    if (x.importId && x.itemId) {
      paraActualizarImport++;

      const refImportItem = doc(
        db,
        "produccion_ambulatoria_imports",
        x.importId,
        "items",
        x.itemId
      );

      console.log("🟢 Import/staging a sincronizar:", {
        importId: x.importId,
        itemId: x.itemId,
        profesionalId,
        procedimientoId
      });

      if (!dryRun) {
        await setDoc(refImportItem, payload, { merge: true });
        actualizadosImport++;
      }
    }
  }

  const resumen = {
    modo: dryRun ? "PRUEBA / NO GUARDA" : "REAL / GUARDADO",
    ano,
    mesNum,
    revisados,
    deAmbulatoria,
    paraActualizarProduccion,
    actualizadosProduccion,
    paraActualizarImport,
    actualizadosImport
  };

  console.log("✅ RESUMEN SINCRONIZACIÓN AMBULATORIA:", resumen);
  return resumen;
};

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
