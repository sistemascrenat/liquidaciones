import { db } from './firebase-init.js'
import { requireAuth } from './auth.js'
import { setActiveNav, wireLogout } from './ui.js'
import { loadSidebar } from './layout.js'

import {
  collection,
  getDocs
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js"

const $ = id => document.getElementById(id)

/* ======================
   DEFAULT MES / AÑO
====================== */

function setDefaultToPreviousMonth() {
  const meses = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
  ]

  const d = new Date()
  d.setDate(1)
  d.setMonth(d.getMonth() - 1)

  if ($("mes")) $("mes").value = meses[d.getMonth()]
  if ($("ano")) $("ano").value = String(d.getFullYear())
}

/* ======================
   DATA
====================== */

let dataReservo = []
let dataMK = []
let profesionales = []
let procedimientos = []
let consolidado = []

let stateEdicion = {
  actual: null
}

let manualOverrides = {}

let uiState = {
  q: "",
  page: 0,
  pageSize: 60,
  mostrarNoAplica: false
}

/* ======================
   HELPERS
====================== */

function clean(v) {
  return (v ?? "").toString().trim()
}

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
}

function normalizarTexto(t) {
  return clean(t)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toUpperCase()
}

function normalizarRut(rut) {
  if (!rut) return ""
  return rut
    .toString()
    .replace(/\./g, "")
    .replace(/-/g, "")
    .replace(/\s+/g, "")
    .trim()
    .toUpperCase()
}

function normalizarPaciente(t) {
  return normalizarTexto(t).replace(/\s+/g, " ").trim()
}

function normalizarFecha(fecha) {
  if (fecha === null || fecha === undefined || fecha === "") return ""

  try {
    if (typeof fecha === "number" && window.XLSX?.SSF?.parse_date_code) {
      const p = window.XLSX.SSF.parse_date_code(fecha)
      if (p && p.y && p.m && p.d) {
        const mm = String(p.m).padStart(2, "0")
        const dd = String(p.d).padStart(2, "0")
        return `${p.y}-${mm}-${dd}`
      }
    }

    const t = clean(fecha)

    let m = t.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/)
    if (m) {
      return `${m[1]}-${String(m[2]).padStart(2,'0')}-${String(m[3]).padStart(2,'0')}`
    }

    m = t.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/)
    if (m) {
      return `${m[3]}-${String(m[2]).padStart(2,'0')}-${String(m[1]).padStart(2,'0')}`
    }

    const d = new Date(t)
    if (!isNaN(d)) return d.toISOString().slice(0, 10)

    return ""
  } catch {
    return ""
  }
}

function normalizarMonto(v) {
  if (v === null || v === undefined) return 0
  if (typeof v === "number") return Number.isFinite(v) ? v : 0

  let s = String(v).trim()
  if (!s || s === "-") return 0

  s = s.replace(/\$/g, "").replace(/\s+/g, "")

  // negativos con paréntesis
  if (/^\(.*\)$/.test(s)) {
    s = "-" + s.slice(1, -1)
  }

  // si trae ambos separadores, usa el último como decimal
  const lastDot = s.lastIndexOf(".")
  const lastComma = s.lastIndexOf(",")

  if (lastDot !== -1 && lastComma !== -1) {
    if (lastDot > lastComma) {
      s = s.replace(/,/g, "")
    } else {
      s = s.replace(/\./g, "").replace(",", ".")
    }
  } else if (lastComma !== -1) {
    const parts = s.split(",")
    if (parts.length === 2 && parts[1].length <= 2) {
      s = s.replace(",", ".")
    } else {
      s = s.replace(/,/g, "")
    }
  } else if (lastDot !== -1) {
    const parts = s.split(".")
    if (!(parts.length === 2 && parts[1].length <= 2)) {
      s = s.replace(/\./g, "")
    }
  }

  const n = Number(s)
  return Number.isFinite(n) ? n : 0
}

function leerExcel(file) {
  return new Promise(resolve => {
    const reader = new FileReader()

    reader.onload = e => {
      const data = new Uint8Array(e.target.result)
      const workbook = XLSX.read(data, { type: 'array' })
      const sheet = workbook.Sheets[workbook.SheetNames[0]]
      const json = XLSX.utils.sheet_to_json(sheet, { defval: '' })
      resolve(json)
    }

    reader.readAsArrayBuffer(file)
  })
}

/* ======================
   CARGA CATÁLOGOS
====================== */

async function cargarProfesionales() {
  const snap = await getDocs(collection(db, "profesionales"))

  profesionales = snap.docs.map(d => ({
    id: d.id,
    ...d.data()
  }))
}

async function cargarProcedimientos() {
  const snap = await getDocs(collection(db, "procedimientos"))

  procedimientos = snap.docs.map(d => ({
    id: d.id,
    ...d.data()
  }))
}

/* ======================
   BÚSQUEDA CATÁLOGOS
====================== */

function nombreProfesionalCatalogo(p) {
  return p?.nombreProfesional || p?.nombre || p?.nombreCompleto || p?.id || ""
}

function nombreProcedimientoCatalogo(p) {
  return p?.nombre || p?.procedimiento || p?.descripcion || p?.id || ""
}

function buscarProfesional(texto) {
  const t = normalizarTexto(texto)
  if (!t) return null

  return profesionales.find(p => {
    const nombre = normalizarTexto(nombreProfesionalCatalogo(p))
    if (!nombre) return false
    const palabras = nombre.split(" ").filter(Boolean)
    return palabras.some(w => w.length > 2 && t.includes(w))
  }) || null
}

function buscarProcedimiento(texto) {
  const t = normalizarTexto(texto)
  if (!t) return null

  return procedimientos.find(p => {
    const nombre = normalizarTexto(nombreProcedimientoCatalogo(p))
    if (!nombre) return false
    return nombre === t || t.includes(nombre) || nombre.includes(t)
  }) || null
}

/* ======================
   REVISIÓN / APLICACIÓN
====================== */

function construirReview({ profesionalId, procedimientoId, alertas = [] }) {
  const pendienteProfesional = !profesionalId
  const pendienteProcedimiento = !procedimientoId

  return {
    estadoRevision: (!pendienteProfesional && !pendienteProcedimiento) ? "ok" : "pendiente",
    pendientes: {
      profesional: pendienteProfesional,
      procedimiento: pendienteProcedimiento
    },
    alertas
  }
}

function construirAplicacion(estado, motivo) {
  return { estado, motivo }
}

/* ======================
   RESERVO: ESTADOS
====================== */

function clasificarEstadoCitaReservo(v) {
  const t = normalizarTexto(v)
  if (!t) return "otro"
  if (t.includes("ATENDID")) return "atendido"
  return "no_atendido"
}

function clasificarEstadoPagoReservo(v) {
  const t = normalizarTexto(v)
  if (!t) return "otro"
  if (t.includes("NO PAG")) return "no_pagado"
  if (t.includes("PAGAD")) return "pagado"
  return "otro"
}

function evaluarAplicacionReservo(raw) {
  const estadoCita = clasificarEstadoCitaReservo(raw["Estado cita"])
  const estadoPago = clasificarEstadoPagoReservo(raw["Estado pago"])

  const alertas = []

  if (estadoCita === "atendido" && estadoPago === "pagado") {
    return {
      aplicacion: construirAplicacion("aplica", "Atendido y pagado"),
      alertas
    }
  }

  if (estadoCita === "atendido" && estadoPago !== "pagado") {
    return {
      aplicacion: construirAplicacion("no_aplica", "Atendido sin pago"),
      alertas
    }
  }

  if (estadoCita !== "atendido" && estadoPago === "pagado") {
    alertas.push("Inconsistencia: pagado sin atendido")
    return {
      aplicacion: construirAplicacion("revisar", "Pagado sin atendido"),
      alertas
    }
  }

  return {
    aplicacion: construirAplicacion("no_aplica", "No atendido y sin pago"),
    alertas
  }
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
  ].join("|")
}

function evaluarAplicacionMK(itemsMK) {
  const grupos = new Map()

  for (const it of itemsMK) {
    const key = claveMK(it)
    if (!grupos.has(key)) grupos.set(key, [])
    grupos.get(key).push(it)
  }

  for (const [, group] of grupos.entries()) {
    const positives = group.filter(x => x.valor > 0).sort((a,b) => a.sourceIndex - b.sourceIndex)
    const negatives = group.filter(x => x.valor < 0).sort((a,b) => a.sourceIndex - b.sourceIndex)
    const zeros = group.filter(x => x.valor === 0)

    // ceros
    for (const z of zeros) {
      z.aplicacion = construirAplicacion("no_aplica", "Valor cero")
    }

    // emparejar anulaciones
    const pares = Math.min(positives.length, negatives.length)

    for (let i = 0; i < pares; i++) {
      positives[i].aplicacion = construirAplicacion("no_aplica", "Positivo anulado por negativo")
      negatives[i].aplicacion = construirAplicacion("no_aplica", "Negativo de anulación")
    }

    for (let i = pares; i < positives.length; i++) {
      positives[i].aplicacion = construirAplicacion("aplica", "Positivo sin anulación")
    }

    for (let i = pares; i < negatives.length; i++) {
      negatives[i].aplicacion = construirAplicacion("revisar", "Negativo sin positivo equivalente")
      negatives[i]._extraAlertas = [...(negatives[i]._extraAlertas || []), "Inconsistencia: negativo sin positivo equivalente"]
    }
  }

  return itemsMK
}

/* ======================
   MANUAL OVERRIDES
====================== */

function aplicarManualOverrides(items) {
  for (const it of items) {
    const ov = manualOverrides[it.itemId]
    if (!ov) continue

    if (Object.prototype.hasOwnProperty.call(ov, "profesionalId")) {
      const p = profesionales.find(x => x.id === ov.profesionalId) || null
      it.resolved.profesionalId = p?.id || null
      it.resolved.profesionalNombre = p ? nombreProfesionalCatalogo(p) : null
      it.resolved.confirmadoManualProfesional = !!ov.profesionalId
    }

    if (Object.prototype.hasOwnProperty.call(ov, "procedimientoId")) {
      const p = procedimientos.find(x => x.id === ov.procedimientoId) || null
      it.resolved.procedimientoId = p?.id || null
      it.resolved.procedimientoNombre = p ? nombreProcedimientoCatalogo(p) : null
      it.resolved.confirmadoManualProcedimiento = !!ov.procedimientoId
    }

    it.profesionalDetectado = it.resolved.profesionalNombre
    it.procedimientoDetectado = it.resolved.procedimientoNombre

    it.review = construirReview({
      profesionalId: it.resolved.profesionalId,
      procedimientoId: it.resolved.procedimientoId,
      alertas: it.review?.alertas || []
    })
  }
}

/* ======================
   PROCESAR RESERVO
====================== */

function procesarReservo() {
  return dataReservo.map((r, i) => {
    const profesionalDetectado = buscarProfesional(r["Profesional"])
    const procedimientoDetectado = buscarProcedimiento(r["Tratamiento"])

    const evalApp = evaluarAplicacionReservo(r)
    const alertas = [...evalApp.alertas]

    if (!normalizarRut(r["Rut"])) alertas.push("RUT vacío o inválido")
    if (!normalizarTexto(r["Profesional"])) alertas.push("Profesional vacío")
    if (!normalizarTexto(r["Tratamiento"])) alertas.push("Procedimiento vacío")

    const resolved = {
      profesionalId: profesionalDetectado?.id || null,
      profesionalNombre: profesionalDetectado ? nombreProfesionalCatalogo(profesionalDetectado) : null,

      procedimientoId: procedimientoDetectado?.id || null,
      procedimientoNombre: procedimientoDetectado ? nombreProcedimientoCatalogo(procedimientoDetectado) : null,

      autoProfesional: !!profesionalDetectado,
      autoProcedimiento: !!procedimientoDetectado,
      confirmadoManualProfesional: false,
      confirmadoManualProcedimiento: false
    }

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
      })
    }
  })
}

/* ======================
   PROCESAR MK
====================== */

function procesarMK() {
  let items = dataMK.map((r, i) => {
    const profesionalDetectado = buscarProfesional(r["D Médico"])
    const procedimientoDetectado = buscarProcedimiento(r["D Artículo"])

    const alertas = []
    if (!normalizarRut(r["Rut"])) alertas.push("RUT vacío o inválido")
    if (!normalizarTexto(r["D Médico"])) alertas.push("Profesional vacío")
    if (!normalizarTexto(r["D Artículo"])) alertas.push("Procedimiento vacío")

    const resolved = {
      profesionalId: profesionalDetectado?.id || null,
      profesionalNombre: profesionalDetectado ? nombreProfesionalCatalogo(profesionalDetectado) : null,

      procedimientoId: procedimientoDetectado?.id || null,
      procedimientoNombre: procedimientoDetectado ? nombreProcedimientoCatalogo(procedimientoDetectado) : null,

      autoProfesional: !!profesionalDetectado,
      autoProcedimiento: !!procedimientoDetectado,
      confirmadoManualProfesional: false,
      confirmadoManualProcedimiento: false
    }

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
      })
    }
  })

  items = evaluarAplicacionMK(items)

  items.forEach(it => {
    const finalAlerts = [...(it._baseAlertas || []), ...(it._extraAlertas || [])]
    it.review = construirReview({
      profesionalId: it.resolved.profesionalId,
      procedimientoId: it.resolved.procedimientoId,
      alertas: finalAlerts
    })
    delete it._baseAlertas
    delete it._extraAlertas
  })

  return items
}

/* ======================
   RECALCULAR TODO
====================== */

function recalcularTodo() {
  const reservos = procesarReservo()
  const mks = procesarMK()

  consolidado = [...reservos, ...mks]

  aplicarManualOverrides(consolidado)
  render()
}

/* ======================
   DETALLE / EDICIÓN
====================== */

function abrirDetalle(reg) {
  const modal = $("modalItemBackdrop")
  const itemSub = $("itemSub")
  const itemForm = $("itemForm")

  if (!modal || !itemSub || !itemForm) {
    console.warn("No existe el modal de detalle en el HTML")
    return
  }

  stateEdicion.actual = reg

  const opcionesProfesionales = profesionales.map(p => {
    const nombre = nombreProfesionalCatalogo(p)
    const selected = reg.resolved?.profesionalId === p.id ? "selected" : ""
    return `<option value="${p.id}" ${selected}>${escapeHtml(nombre)}</option>`
  }).join("")

  const procedimientosAmb = procedimientosAmbulatorios()

  const opcionesProcedimientos = procedimientosAmb.map(p => {
    const nombre = nombreProcedimientoCatalogo(p)
    const selected = reg.resolved?.procedimientoId === p.id ? "selected" : ""
    return `<option value="${p.id}" ${selected}>${escapeHtml(p.id)} · ${escapeHtml(nombre)}</option>`
  }).join("")

  itemSub.textContent = `${reg.origen || ""} · ${reg.fecha || ""} · ${reg.rut || ""}`

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
  `

  modal.style.display = "block"

  if ($("btnMoreInfo")) {
    $("btnMoreInfo").onclick = () => abrirMasInformacion(reg)
  }

  const inputBuscarProc = $("detalleProcedimientoBuscar")
  const selectProc = $("detalleProcedimientoId")

  function renderOpcionesProcedimientoFiltradas() {
    if (!selectProc) return

    const q = normalizarTexto(inputBuscarProc?.value || "")
    const lista = procedimientosAmbulatorios().filter(p => {
      if (!q) return true

      const texto = normalizarTexto([
        p?.id || "",
        nombreProcedimientoCatalogo(p),
        p?.nombre || "",
        p?.tratamiento || "",
        p?.descripcion || ""
      ].join(" | "))

      return texto.includes(q)
    })

    const actual = reg.resolved?.procedimientoId || manualOverrides?.[reg.itemId]?.procedimientoId || ""

    selectProc.innerHTML = `
      <option value="">(Selecciona procedimiento ambulatorio)</option>
      ${lista.map(p => {
        const selected = actual === p.id ? "selected" : ""
        return `<option value="${p.id}" ${selected}>${escapeHtml(p.id)} · ${escapeHtml(nombreProcedimientoCatalogo(p))}</option>`
      }).join("")}
    `
  }

  if (inputBuscarProc) {
    inputBuscarProc.addEventListener("input", renderOpcionesProcedimientoFiltradas)
  }

  renderOpcionesProcedimientoFiltradas()
}

function abrirMasInformacion(reg) {
  const itemForm = $("itemForm")
  if (!itemForm) return

  const original = reg.dataReservo || reg.dataMK || {}

  const filas = Object.keys(original).map(key => {
    const value = original[key] ?? ""
    return `
      <div class="field" style="margin-bottom:10px;">
        <label>${escapeHtml(key)}</label>
        <input type="text" data-extra-key="${escapeHtml(key)}" value="${escapeHtml(String(value))}">
      </div>
    `
  }).join("")

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
  `

  if ($("btnVolverDetalle")) {
    $("btnVolverDetalle").onclick = () => abrirDetalle(reg)
  }
}

function guardarDetalle() {
  if (!stateEdicion.actual) return

  const reg = stateEdicion.actual

  // guardar edición raw si estás en "más información"
  const extraInputs = document.querySelectorAll("[data-extra-key]")
  if (extraInputs.length) {
    const target = reg.dataReservo ? reg.dataReservo : reg.dataMK

    extraInputs.forEach(inp => {
      const key = inp.getAttribute("data-extra-key")
      if (!key || !target) return
      target[key] = inp.value
    })
  }

  // guardar overrides manuales si estás en la vista de resolución
  const profSel = $("detalleProfesionalId")
  const procSel = $("detalleProcedimientoId")

  if (profSel || procSel) {
    manualOverrides[reg.itemId] = {
      ...(manualOverrides[reg.itemId] || {}),
      ...(profSel ? { profesionalId: profSel.value || null } : {}),
      ...(procSel ? { procedimientoId: procSel.value || null } : {})
    }
  }

  cerrarDetalle()
  recalcularTodo()
}

function cerrarDetalle() {
  const modal = $("modalItemBackdrop")
  const itemForm = $("itemForm")

  if (modal) modal.style.display = "none"
  if (itemForm) itemForm.innerHTML = ""

  stateEdicion.actual = null
}

/* ======================
   RESOLVER PENDIENTES
====================== */

function rowMiniHTML(it, extra = "") {
  return `
    <div class="miniRow">
      <div>
        <div><b>${escapeHtml(it.origen)}</b> · ${escapeHtml(it.fecha || "")} · ${escapeHtml(it.rut || "")}</div>
        <div class="muted tiny">${escapeHtml(it.paciente || "")}</div>
        <div class="tiny">Prof: ${escapeHtml(it.profesional || "")} · Proc: ${escapeHtml(it.prestacion || "")}</div>
        ${extra ? `<div class="tiny warn" style="margin-top:4px;">${escapeHtml(extra)}</div>` : ""}
      </div>
      <div class="tiny">
        <div><b>Revisión:</b> ${escapeHtml(it.review?.estadoRevision || "pendiente")}</div>
        <div><b>Aplicación:</b> ${escapeHtml(it.aplicacion?.estado || "—")}</div>
        <div><b>Motivo:</b> ${escapeHtml(it.aplicacion?.motivo || "—")}</div>
      </div>
      <div>
        <button class="btn small" type="button" data-edit-item="${escapeHtml(it.itemId)}">Editar</button>
      </div>
    </div>
  `
}

function bindEditButtonsIn(container) {
  if (!container) return
  container.querySelectorAll("[data-edit-item]").forEach(btn => {
    btn.onclick = () => {
      const id = btn.getAttribute("data-edit-item")
      const it = consolidado.find(x => x.itemId === id)
      if (!it) return
      cerrarResolver()
      abrirDetalle(it)
    }
  })
}

function renderResolver() {
  const resumen = $("resolverResumen")
  const listResumen = $("resolverCoincidenciasList")
  const listProf = $("resolverProfesionalesList")
  const listProc = $("resolverPrestacionesList")
  const listAlert = $("resolverAlertasList")

  if (!resumen || !listResumen || !listProf || !listProc || !listAlert) return

  const pendientes = consolidado.filter(x => x.review?.estadoRevision === "pendiente")
  const pendProf = consolidado.filter(x => x.review?.pendientes?.profesional)
  const pendProc = consolidado.filter(x => x.review?.pendientes?.procedimiento)
  const conAlerta = consolidado.filter(x => (x.review?.alertas || []).length > 0)
  const revisarApp = consolidado.filter(x => x.aplicacion?.estado === "revisar")
  const noAplica = consolidado.filter(x => x.aplicacion?.estado === "no_aplica")

  resumen.textContent =
    `Items: ${consolidado.length} · Pendientes: ${pendientes.length} · ` +
    `Aplican: ${consolidado.filter(x => x.aplicacion?.estado === "aplica").length} · ` +
    `No aplica: ${noAplica.length} · Revisar: ${revisarApp.length}`

  listResumen.innerHTML = revisarApp.length
    ? revisarApp.map(it => rowMiniHTML(it, (it.review?.alertas || []).join(" · ") || it.aplicacion?.motivo || "")).join("")
    : `<div class="muted tiny">No hay ítems marcados para revisar por inconsistencia.</div>`

  listProf.innerHTML = pendProf.length
    ? pendProf.map(it => rowMiniHTML(it, "Falta asociar profesional")).join("")
    : `<div class="muted tiny">No hay pendientes de profesional.</div>`

  listProc.innerHTML = pendProc.length
    ? pendProc.map(it => rowMiniHTML(it, "Falta asociar procedimiento")).join("")
    : `<div class="muted tiny">No hay pendientes de procedimiento.</div>`

  listAlert.innerHTML = conAlerta.length
    ? conAlerta.map(it => rowMiniHTML(it, (it.review?.alertas || []).join(" · "))).join("")
    : `<div class="muted tiny">No hay alertas.</div>`

  bindEditButtonsIn(listResumen)
  bindEditButtonsIn(listProf)
  bindEditButtonsIn(listProc)
  bindEditButtonsIn(listAlert)
}

function abrirResolver() {
  const modal = $("modalResolverBackdrop")
  if (!modal) return
  renderResolver()
  modal.style.display = "block"
}

function cerrarResolver() {
  const modal = $("modalResolverBackdrop")
  if (modal) modal.style.display = "none"
}

/* ======================
   FILTRO / PAGINACIÓN
====================== */

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
    ...(it.review?.alertas || [])
  ].map(x => normalizarTexto(x)).join(" | ")
}

function filteredItems() {
  let items = [...consolidado]

  // Vista principal:
  // - por defecto ocultar no_aplica
  // - al activar el toggle, mostrar SOLO no_aplica
  if (uiState.mostrarNoAplica) {
    items = items.filter(it => it.aplicacion?.estado === "no_aplica")
  } else {
    items = items.filter(it => it.aplicacion?.estado !== "no_aplica")
  }

  // Buscador principal:
  // coma (,) = AND
  // punto (.) = OR
  if (!clean(uiState.q)) return items

  return items.filter(it => {
    const text = itemSearchText(it)
    return matchBusquedaPrincipal(text, uiState.q)
  })
}

function esProcedimientoAmbulatorio(p) {
  const tipo = normalizarTexto(p?.tipo || "")
  const id = normalizarTexto(p?.id || "")
  return tipo === "AMBULATORIO" || /^PA\d+$/.test(id)
}

function procedimientosAmbulatorios() {
  return procedimientos
    .filter(esProcedimientoAmbulatorio)
    .sort((a, b) => {
      const aId = normalizarTexto(a?.id || "")
      const bId = normalizarTexto(b?.id || "")
      return aId.localeCompare(bId, 'es', { numeric: true, sensitivity: 'base' })
    })
}

function matchBusquedaPrincipal(searchText, rawQuery) {
  const q = normalizarTexto(rawQuery)
  if (!q) return true

  // LÓGICA:
  // punto (.) = OR entre grupos
  // coma (,)  = AND dentro de cada grupo
  //
  // Ej: juan,laser.botox
  // => (juan Y laser) O (botox)

  const gruposOr = q
    .split(".")
    .map(g => g.trim())
    .filter(Boolean)

  if (!gruposOr.length) return true

  return gruposOr.some(grupo => {
    const terminosAnd = grupo
      .split(",")
      .map(t => t.trim())
      .filter(Boolean)

    if (!terminosAnd.length) return false

    return terminosAnd.every(term => searchText.includes(term))
  })
}

/* ======================
   RENDER TABLA
====================== */

function render() {
  const thead = $("thead")
  const tbody = $("tbody")

  if (!thead || !tbody) return

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
  `

  const items = filteredItems()
  const totalPages = Math.max(1, Math.ceil(items.length / uiState.pageSize))
  if (uiState.page >= totalPages) uiState.page = totalPages - 1
  if (uiState.page < 0) uiState.page = 0

  const from = uiState.page * uiState.pageSize
  const to = from + uiState.pageSize
  const pageItems = items.slice(from, to)

  tbody.innerHTML = ""

  for (let i = 0; i < pageItems.length; i++) {
    const r = pageItems[i]
    const tr = document.createElement("tr")

    const estado = r.review?.estadoRevision || "pendiente"
    const alertasTexto = (r.review?.alertas || []).join(" · ")

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
      <td class="wrap">${escapeHtml(alertasTexto || "—")}</td>
      <td>
        <button class="btnDetalle btn small" type="button">Editar</button>
      </td>
    `

    const btnDetalle = tr.querySelector(".btnDetalle")
    if (btnDetalle) {
      btnDetalle.onclick = () => abrirDetalle(r)
    }

    tbody.appendChild(tr)
  }

  // pills globales
  const pendientes = consolidado.filter(x => x.review?.estadoRevision === "pendiente").length
  const alertas = consolidado.filter(x => (x.review?.alertas || []).length > 0).length
  const ok = consolidado.filter(x => x.review?.estadoRevision === "ok").length
  const noAplica = consolidado.filter(x => x.aplicacion?.estado === "no_aplica").length
  const pendProf = consolidado.filter(x => x.review?.pendientes?.profesional).length
  const reservoAplica = consolidado.filter(x => x.origen === "Reservo" && x.aplicacion?.estado === "aplica").length
  const mkAplica = consolidado.filter(x => x.origen === "MK" && x.aplicacion?.estado === "aplica").length

  if ($("countPill")) $("countPill").textContent = `${items.length} filas`
  if ($("pillPendientes")) $("pillPendientes").textContent = `Pendientes: ${pendientes}`
  if ($("pillAlertas")) $("pillAlertas").textContent = `Alertas: ${alertas}`
  if ($("pillProf")) $("pillProf").textContent = `Profesionales: ${pendProf}`
  if ($("pillCoincidencias")) $("pillCoincidencias").textContent = `OK: ${ok}`
  if ($("pillFusionados")) $("pillFusionados").textContent = `No aplica: ${noAplica}`
  if ($("pillReservoValidos")) $("pillReservoValidos").textContent = `Reservo válidos: ${reservoAplica}`
  if ($("pillMKValidos")) $("pillMKValidos").textContent = `MK válidos: ${mkAplica}`

  if ($("pagerInfo")) {
    $("pagerInfo").textContent =
      `${items.length} resultados · página ${uiState.page + 1} de ${totalPages}`
  }

  if ($("statusInfo")) {
    const totalAmbulatorios = procedimientosAmbulatorios().length
    const vista = uiState.mostrarNoAplica ? "Vista: NO APLICA" : "Vista: APLICABLES / REVISAR"
    $("statusInfo").textContent = consolidado.length
      ? `${vista} · Catálogos: ${profesionales.length} profesionales · ${totalAmbulatorios} procedimientos ambulatorios`
      : "—"
  }

  if ($("btnToggleNoAplica")) {
    $("btnToggleNoAplica").textContent = uiState.mostrarNoAplica
      ? "Ver aplicables"
      : "Ver no aplica"
  }

  if ($("btnResolver")) $("btnResolver").disabled = consolidado.length === 0
  if ($("btnConfirmar")) $("btnConfirmar").disabled = consolidado.length === 0
  if ($("btnAnular")) $("btnAnular").disabled = consolidado.length === 0

  renderPagerTabs(totalPages)
}

function renderPagerTabs(totalPages) {
  const wrap = $("pagerTabs")
  if (!wrap) return

  wrap.innerHTML = ""

  for (let i = 0; i < totalPages; i++) {
    const btn = document.createElement("button")
    btn.type = "button"
    btn.className = "btn small"
    btn.textContent = String(i + 1)
    if (i === uiState.page) {
      btn.classList.add("primary")
    }
    btn.onclick = () => {
      uiState.page = i
      render()
    }
    wrap.appendChild(btn)
  }
}

/* ======================
   PROCESAR
====================== */

async function procesarArchivos() {
  if (!dataReservo.length && !dataMK.length) {
    alert("Debes cargar al menos un archivo")
    return
  }

  await cargarProfesionales()
  await cargarProcedimientos()

  uiState.page = 0
  recalcularTodo()
}

/* ======================
   EVENTOS
====================== */

if ($("btnCargar")) {
  $("btnCargar").onclick = procesarArchivos
}

if ($("fileReservo")) {
  $("fileReservo").addEventListener("change", async e => {
    const file = e.target.files?.[0]
    if (!file) return
    dataReservo = await leerExcel(file)
  })
}

if ($("fileMK")) {
  $("fileMK").addEventListener("change", async e => {
    const file = e.target.files?.[0]
    if (!file) return
    dataMK = await leerExcel(file)
  })
}

if ($("q")) {
  $("q").addEventListener("input", e => {
    uiState.q = e.target.value || ""
    uiState.page = 0
    render()
  })
}

if ($("btnPrev")) {
  $("btnPrev").onclick = () => {
    uiState.page = Math.max(0, uiState.page - 1)
    render()
  }
}

if ($("btnNext")) {
  $("btnNext").onclick = () => {
    const totalPages = Math.max(1, Math.ceil(filteredItems().length / uiState.pageSize))
    uiState.page = Math.min(totalPages - 1, uiState.page + 1)
    render()
  }
}

if ($("btnToggleNoAplica")) {
  $("btnToggleNoAplica").onclick = () => {
    uiState.mostrarNoAplica = !uiState.mostrarNoAplica
    uiState.page = 0
    render()
  }
}

/* resolver */
if ($("btnResolver")) $("btnResolver").onclick = abrirResolver
if ($("btnResolverClose")) $("btnResolverClose").onclick = cerrarResolver
if ($("btnResolverCancelar")) $("btnResolverCancelar").onclick = cerrarResolver
if ($("btnResolverRevisar")) $("btnResolverRevisar").onclick = renderResolver

if ($("modalResolverBackdrop")) {
  $("modalResolverBackdrop").addEventListener("click", e => {
    if (e.target === $("modalResolverBackdrop")) cerrarResolver()
  })
}

/* detalle */
if ($("btnItemClose")) $("btnItemClose").onclick = cerrarDetalle
if ($("btnItemCancelar")) $("btnItemCancelar").onclick = cerrarDetalle
if ($("btnGuardarItem")) $("btnGuardarItem").onclick = guardarDetalle

if ($("btnGuardarTodo")) {
  $("btnGuardarTodo").onclick = guardarDetalle
}

if ($("modalItemBackdrop")) {
  $("modalItemBackdrop").addEventListener("click", e => {
    if (e.target === $("modalItemBackdrop")) cerrarDetalle()
  })
}

/* botones aún no implementados con persistencia */
if ($("btnCargarImport")) {
  $("btnCargarImport").onclick = () => {
    console.warn("Cargar Import aún no está implementado con Firestore en esta versión.")
  }
}

if ($("btnConfirmar")) {
  $("btnConfirmar").onclick = () => {
    console.warn("Confirmar importación aún no está implementado con Firestore en esta versión.")
  }
}

if ($("btnAnular")) {
  $("btnAnular").onclick = () => {
    console.warn("Anular importación aún no está implementado con Firestore en esta versión.")
  }
}

if ($("btnLimpiarCola")) {
  $("btnLimpiarCola").onclick = () => {
    manualOverrides = {}
    recalcularTodo()
  }
}

/* ======================
   BOOT
====================== */

requireAuth({
  onUser: async (user) => {
    await loadSidebar({ active: 'produccion_ambulatoria' })
    setActiveNav('produccion_ambulatoria')

    if ($("who")) {
      $("who").textContent = `Conectado: ${user.email}`
    }

    wireLogout()
    setDefaultToPreviousMonth()
    render()
  }
})
