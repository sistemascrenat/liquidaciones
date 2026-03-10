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

function setDefaultToPreviousMonth(){

const meses = [
"Enero","Febrero","Marzo","Abril","Mayo","Junio",
"Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]

const d = new Date()

d.setDate(1)
d.setMonth(d.getMonth()-1)

if($("mes")) $("mes").value = meses[d.getMonth()]
if($("ano")) $("ano").value = String(d.getFullYear())

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

/* ======================
   NORMALIZAR RUT
====================== */

function normalizarRut(rut){

if(!rut) return ""

return rut
.toString()
.replace(/\./g,"")
.replace(/-/g,"")
.trim()
.toUpperCase()

}

/* ======================
   NORMALIZAR FECHA
====================== */

function normalizarFecha(fecha){

if(!fecha) return ""

try{

let d = new Date(fecha)

if(isNaN(d)) return ""

return d.toISOString().slice(0,10)

}catch{

return ""

}

}

/* ======================
   NORMALIZAR TEXTO
====================== */

function normalizarTexto(t){

if(!t) return ""

return t
.toString()
.trim()
.toUpperCase()

}

/* ======================
   LEER EXCEL
====================== */

function leerExcel(file){

return new Promise(resolve=>{

const reader = new FileReader()

reader.onload = e => {

const data = new Uint8Array(e.target.result)

const workbook = XLSX.read(data,{type:'array'})

const sheet = workbook.Sheets[workbook.SheetNames[0]]

const json = XLSX.utils.sheet_to_json(sheet,{defval:''})

resolve(json)

}

reader.readAsArrayBuffer(file)

})

}

/* ======================
   CARGAR PROFESIONALES
====================== */

async function cargarProfesionales(){

const snap = await getDocs(collection(db,"profesionales"))

profesionales = snap.docs.map(d=>({
id:d.id,
...d.data()
}))

}

/* ======================
   CARGAR PROCEDIMIENTOS
====================== */

async function cargarProcedimientos(){

const snap = await getDocs(collection(db,"procedimientos"))

procedimientos = snap.docs.map(d=>({
id:d.id,
...d.data()
}))

}
/* ======================
   BUSCAR PROFESIONAL
====================== */

function buscarProfesional(texto){

texto = normalizarTexto(texto)
if(!texto) return null

return profesionales.find(p=>{

let nombre =
  normalizarTexto(p.nombreProfesional) ||
  normalizarTexto(p.nombre) ||
  normalizarTexto(p.nombreCompleto)

if(!nombre) return false

let palabras = nombre.split(" ").filter(Boolean)

return palabras.some(w => w.length > 2 && texto.includes(w))

}) || null

}

/* ======================
   BUSCAR PROCEDIMIENTO
====================== */

function buscarProcedimiento(texto){

texto = normalizarTexto(texto)
if(!texto) return null

return procedimientos.find(p=>{

let nombre =
  normalizarTexto(p.nombre) ||
  normalizarTexto(p.procedimiento) ||
  normalizarTexto(p.descripcion)

if(!nombre) return false

return nombre === texto || texto.includes(nombre) || nombre.includes(texto)

}) || null

}

/* ======================
   ESTADO DE REVISIÓN
====================== */

function construirReview({ profesionalId, procedimientoId, alertas = [] }){

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

/* ======================
   ALERTAS RESERVO
====================== */

function alertaReservo(r){

let estadoCita = normalizarTexto(r["Estado cita"])
let estadoPago = normalizarTexto(r["Estado pago"])

if(estadoCita.includes("ATEND") && estadoPago.includes("PAG")){

return null

}

if(estadoPago.includes("PAG") && !estadoCita.includes("ATEND")){

return "Pagado pero no atendido"

}

if(estadoCita.includes("ATEND") && !estadoPago.includes("PAG")){

return "Atendido pero no pagado"

}

return "Estado inconsistente"

}

/* ======================
   PROCESAR RESERVO
====================== */

function procesarReservo(){

return dataReservo.map((r, i)=>{

let profesionalDetectado = buscarProfesional(r["Profesional"])
let procedimientoDetectado = buscarProcedimiento(r["Tratamiento"])

let alertas = []
let alerta = alertaReservo(r)
if(alerta) alertas.push(alerta)

const resolved = {
profesionalId: profesionalDetectado?.id || null,
profesionalNombre: profesionalDetectado?.nombreProfesional || profesionalDetectado?.nombre || profesionalDetectado?.nombreCompleto || null,

procedimientoId: procedimientoDetectado?.id || null,
procedimientoNombre: procedimientoDetectado?.nombre || procedimientoDetectado?.procedimiento || procedimientoDetectado?.descripcion || null,

autoProfesional: !!profesionalDetectado,
autoProcedimiento: !!procedimientoDetectado,
confirmadoManualProfesional: false,
confirmadoManualProcedimiento: false
}

return {

itemId: `RES_${String(i+1).padStart(4,"0")}`,
origen:"Reservo",

fecha:r["Fecha"],
fechaNorm:normalizarFecha(r["Fecha"]),

rut:r["Rut"],
rutNorm:normalizarRut(r["Rut"]),

paciente:r["Paciente"],

profesional:r["Profesional"],
profesionalDetectado: resolved.profesionalNombre,

prestacion:r["Tratamiento"],
procedimientoDetectado: resolved.procedimientoNombre,

valor:Number(r["Valor"]) || 0,

dataReservo:r,
dataMK:null,

resolved,
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

function procesarMK(){

return dataMK
.filter(r=> Number(r["Total"]) > 0 )
.map((r, i)=>{

let profesionalDetectado = buscarProfesional(r["D Médico"])
let procedimientoDetectado = buscarProcedimiento(r["D Artículo"])

let alertas = []

if(!normalizarRut(r["Rut"])) alertas.push("RUT vacío o inválido")
if(!normalizarTexto(r["D Médico"])) alertas.push("Profesional vacío")
if(!normalizarTexto(r["D Artículo"])) alertas.push("Procedimiento vacío")

const resolved = {
profesionalId: profesionalDetectado?.id || null,
profesionalNombre: profesionalDetectado?.nombreProfesional || profesionalDetectado?.nombre || profesionalDetectado?.nombreCompleto || null,

procedimientoId: procedimientoDetectado?.id || null,
procedimientoNombre: procedimientoDetectado?.nombre || procedimientoDetectado?.procedimiento || procedimientoDetectado?.descripcion || null,

autoProfesional: !!profesionalDetectado,
autoProcedimiento: !!procedimientoDetectado,
confirmadoManualProfesional: false,
confirmadoManualProcedimiento: false
}

return {

itemId: `MK_${String(i+1).padStart(4,"0")}`,
origen:"MK",

fecha:r["Fecha"],
fechaNorm:normalizarFecha(r["Fecha"]),

rut:r["Rut"],
rutNorm:normalizarRut(r["Rut"]),

paciente:r["Paciente"],

profesional:r["D Médico"],
profesionalDetectado: resolved.profesionalNombre,

prestacion:r["D Artículo"],
procedimientoDetectado: resolved.procedimientoNombre,

valor:Number(r["Total"]) || 0,

dataReservo:null,
dataMK:r,

resolved,
review: construirReview({
profesionalId: resolved.profesionalId,
procedimientoId: resolved.procedimientoId,
alertas
})

}

})

}

/* ======================
   DETALLE / EDICIÓN
====================== */

function abrirDetalle(reg){

const modal = $("modalItemBackdrop")
const itemSub = $("itemSub")
const itemForm = $("itemForm")

if(!modal || !itemSub || !itemForm){
  console.warn("No existe el modal de detalle en el HTML")
  return
}

stateEdicion.actual = reg

itemSub.textContent = `${reg.origen || ""} · ${reg.fecha || ""} · ${reg.rut || ""}`

const opcionesProfesionales = profesionales.map(p=>{
  const nombre = p.nombreProfesional || p.nombre || p.nombreCompleto || p.id
  const selected = reg.resolved?.profesionalId === p.id ? "selected" : ""
  return `<option value="${p.id}" ${selected}>${nombre}</option>`
}).join("")

const opcionesProcedimientos = procedimientos.map(p=>{
  const nombre = p.nombre || p.procedimiento || p.descripcion || p.id
  const selected = reg.resolved?.procedimientoId === p.id ? "selected" : ""
  return `<option value="${p.id}" ${selected}>${nombre}</option>`
}).join("")

itemForm.innerHTML = `
  <div class="grid2">

    <section class="card" style="padding:12px;">
      <div class="sectionTitle">Resolución del item</div>
      <div class="kv">
        <div class="k">Origen</div><div class="v">${reg.origen || ""}</div>
        <div class="k">Fecha</div><div class="v">${reg.fecha || ""}</div>
        <div class="k">RUT</div><div class="v">${reg.rut || ""}</div>
        <div class="k">Paciente</div><div class="v">${reg.paciente || ""}</div>
        <div class="k">Profesional archivo</div><div class="v">${reg.profesional || ""}</div>
        <div class="k">Procedimiento archivo</div><div class="v">${reg.prestacion || ""}</div>
        <div class="k">Estado revisión</div><div class="v">${reg.review?.estadoRevision || "pendiente"}</div>
        <div class="k">Alertas</div><div class="v">${(reg.review?.alertas || []).join(" · ") || "—"}</div>
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
        <label>Asociar procedimiento</label>
        <select id="detalleProcedimientoId">
          <option value="">(Selecciona procedimiento)</option>
          ${opcionesProcedimientos}
        </select>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:12px;">
        <button id="btnMoreInfo" type="button" class="btn soft">Editar más información</button>
      </div>
    </section>

    <section class="card" style="padding:12px;">
      <div class="sectionTitle">Datos originales</div>
      <pre style="white-space:pre-wrap; font-size:12px; margin:0;">${JSON.stringify(reg.dataReservo || reg.dataMK || {}, null, 2)}</pre>
    </section>

  </div>
`

modal.style.display = "block"

if($("btnMoreInfo")){
  $("btnMoreInfo").onclick = ()=>{
    abrirMasInformacion(reg)
  }
}

}

function guardarDetalle(){

if(!stateEdicion.actual) return

const reg = stateEdicion.actual

const profesionalId = $("detalleProfesionalId")?.value || ""
const procedimientoId = $("detalleProcedimientoId")?.value || ""

const profesional = profesionales.find(p => p.id === profesionalId) || null
const procedimiento = procedimientos.find(p => p.id === procedimientoId) || null

/* =========================
   Guardar edición de campos extra
========================= */

const extraInputs = document.querySelectorAll("[data-extra-key]")

if(extraInputs.length){

  const target = reg.dataReservo ? reg.dataReservo : reg.dataMK

  extraInputs.forEach(inp=>{
    const key = inp.getAttribute("data-extra-key")
    if(!key || !target) return
    target[key] = inp.value
  })

  /* recalcular algunos campos principales si fueron editados */
  const raw = target || {}

  reg.fecha = raw["Fecha"] ?? reg.fecha
  reg.fechaNorm = normalizarFecha(raw["Fecha"] ?? reg.fecha)

  reg.rut = raw["Rut"] ?? reg.rut
  reg.rutNorm = normalizarRut(raw["Rut"] ?? reg.rut)

  reg.paciente = raw["Paciente"] ?? reg.paciente

  if(reg.origen === "Reservo"){
    reg.profesional = raw["Profesional"] ?? reg.profesional
    reg.prestacion = raw["Tratamiento"] ?? reg.prestacion
    reg.valor = Number(raw["Valor"]) || 0

    const alerta = alertaReservo(raw)
    reg.review = construirReview({
      profesionalId: reg.resolved?.profesionalId || null,
      procedimientoId: reg.resolved?.procedimientoId || null,
      alertas: alerta ? [alerta] : []
    })
  }else{
    reg.profesional = raw["D Médico"] ?? reg.profesional
    reg.prestacion = raw["D Artículo"] ?? reg.prestacion
    reg.valor = Number(raw["Total"]) || 0

    const alertasMK = []
    if(!normalizarRut(raw["Rut"])) alertasMK.push("RUT vacío o inválido")
    if(!normalizarTexto(raw["D Médico"])) alertasMK.push("Profesional vacío")
    if(!normalizarTexto(raw["D Artículo"])) alertasMK.push("Procedimiento vacío")

    reg.review = construirReview({
      profesionalId: reg.resolved?.profesionalId || null,
      procedimientoId: reg.resolved?.procedimientoId || null,
      alertas: alertasMK
    })
  }

}

/* =========================
   Guardar resolución manual
========================= */

reg.resolved = {
...(reg.resolved || {}),
profesionalId: profesional?.id || null,
profesionalNombre: profesional?.nombreProfesional || profesional?.nombre || profesional?.nombreCompleto || null,

procedimientoId: procedimiento?.id || null,
procedimientoNombre: procedimiento?.nombre || procedimiento?.procedimiento || procedimiento?.descripcion || null,

confirmadoManualProfesional: !!profesional,
confirmadoManualProcedimiento: !!procedimiento
}

reg.profesionalDetectado = reg.resolved.profesionalNombre
reg.procedimientoDetectado = reg.resolved.procedimientoNombre

reg.review = construirReview({
profesionalId: reg.resolved.profesionalId,
procedimientoId: reg.resolved.procedimientoId,
alertas: reg.review?.alertas || []
})

cerrarDetalle()
render()

}

function abrirMasInformacion(reg){

const itemForm = $("itemForm")
if(!itemForm) return

const original = reg.dataReservo || reg.dataMK || {}

const filas = Object.keys(original).map(key=>{

const value = original[key] ?? ""

return `
  <div class="field" style="margin-bottom:10px;">
    <label>${key}</label>
    <input type="text" data-extra-key="${key}" value="${String(value).replaceAll('"','&quot;')}">
  </div>
`

}).join("")

itemForm.innerHTML = `
  <div class="card" style="padding:12px;">
    <div class="sectionTitle">Editar más información</div>
    <div class="help" style="margin-bottom:10px;">
      Aquí puedes editar los campos originales del registro. Luego presiona “Guardar item”.
    </div>
    <div class="grid2">
      ${filas}
    </div>
  </div>
`

}

function cerrarDetalle(){

const modal = $("modalItemBackdrop")
const itemForm = $("itemForm")

if(modal) modal.style.display = "none"
if(itemForm) itemForm.innerHTML = ""

stateEdicion.actual = null

}

function abrirFusion(reg){

if(!reg.coincidencia){
  alert("Este registro no tiene coincidencia para fusionar.")
  return
}

const modal = $("modalMatchBackdrop")
const sub = $("matchSub")
const boxReservo = $("matchReservo")
const boxMK = $("matchMK")
const profSelect = $("matchProfesionalSelect")
const obs = $("matchObservacion")

if(!modal || !sub || !boxReservo || !boxMK || !profSelect || !obs){
  console.warn("No existe el modal de fusión en el HTML")
  return
}

const a = reg.origen === "Reservo" ? reg : reg.coincidencia
const b = reg.origen === "MK" ? reg : reg.coincidencia

sub.textContent = `Coincidencia por RUT ${a.rut || ""} y fecha ${a.fechaNorm || a.fecha || ""}`

boxReservo.innerHTML = `
  <div class="kv">
    <div class="k">Origen</div><div class="v">${a.origen || ""}</div>
    <div class="k">Fecha</div><div class="v">${a.fecha || ""}</div>
    <div class="k">RUT</div><div class="v">${a.rut || ""}</div>
    <div class="k">Paciente</div><div class="v">${a.paciente || ""}</div>
    <div class="k">Profesional</div><div class="v">${a.profesional || ""}</div>
    <div class="k">Prestación</div><div class="v">${a.prestacion || ""}</div>
    <div class="k">Valor</div><div class="v">${a.valor ?? ""}</div>
    <div class="k">Alerta</div><div class="v">${a.alerta || "—"}</div>
  </div>
`

boxMK.innerHTML = `
  <div class="kv">
    <div class="k">Origen</div><div class="v">${b.origen || ""}</div>
    <div class="k">Fecha</div><div class="v">${b.fecha || ""}</div>
    <div class="k">RUT</div><div class="v">${b.rut || ""}</div>
    <div class="k">Paciente</div><div class="v">${b.paciente || ""}</div>
    <div class="k">Profesional</div><div class="v">${b.profesional || ""}</div>
    <div class="k">Prestación</div><div class="v">${b.prestacion || ""}</div>
    <div class="k">Valor</div><div class="v">${b.valor ?? ""}</div>
    <div class="k">Alerta</div><div class="v">${b.alerta || "—"}</div>
  </div>
`

profSelect.innerHTML = `<option value="">(Selecciona profesional)</option>` +
  profesionales.map(p => `<option value="${p.id}">${p.nombre || p.nombreProfesional || p.id}</option>`).join("")

obs.value = ""

modal.style.display = "block"

stateFusion.actual = reg

}

function cerrarFusion(){

const modal = $("modalMatchBackdrop")
if(modal) modal.style.display = "none"
stateFusion.actual = null

}

/* ======================
   RENDER TABLA
====================== */

function render(){

let thead = $("thead")
let tbody = $("tbody")

if(!thead || !tbody) return

thead.innerHTML = `
<tr>
<td>#</td>
<td>Origen</td>
<td>Fecha</td>
<td>Rut</td>
<td>Paciente</td>
<td>Profesional archivo</td>
<td>Procedimiento archivo</td>
<td>Valor</td>
<td>Estado</td>
<td>Alertas</td>
<td>Acciones</td>
</tr>
`

tbody.innerHTML = ""

let pendientes = 0
let alertas = 0
let ok = 0

for(let i=0;i<consolidado.length;i++){

let r = consolidado[i]
let tr = document.createElement("tr")

const estado = r.review?.estadoRevision || "pendiente"
const alertasTexto = (r.review?.alertas || []).join(" · ")

if(estado === "pendiente") pendientes++
else ok++

if((r.review?.alertas || []).length) alertas++

tr.innerHTML = `
<td>${i+1}</td>
<td>${r.origen || ""}</td>
<td>${r.fecha || ""}</td>
<td>${r.rut || ""}</td>
<td>${r.paciente || ""}</td>
<td>${r.profesional || ""}</td>
<td>${r.prestacion || ""}</td>
<td>${r.valor ?? ""}</td>
<td>${estado === "ok" ? `<span class="ok">OK</span>` : `<span class="warn">Pendiente</span>`}</td>
<td>${alertasTexto || "—"}</td>
<td>
  <button class="btnDetalle" type="button">Editar</button>
</td>
`

let btnDetalle = tr.querySelector(".btnDetalle")

if(btnDetalle){
  btnDetalle.onclick = ()=>{
    abrirDetalle(r)
  }
}

tbody.appendChild(tr)

}

if($("countPill")){
  $("countPill").textContent = `${consolidado.length} filas`
}

if($("pillPendientes")){
  $("pillPendientes").textContent = `Pendientes: ${pendientes}`
}

if($("pillAlertas")){
  $("pillAlertas").textContent = `Alertas: ${alertas}`
}

if($("pillProf")){
  const pendProf = consolidado.filter(x => x.review?.pendientes?.profesional).length
  $("pillProf").textContent = `Profesionales: ${pendProf}`
}

if($("pillCoincidencias")){
  $("pillCoincidencias").textContent = `OK: ${ok}`
}

if($("pillFusionados")){
  $("pillFusionados").textContent = `Procedimientos: ${procedimientos.length}`
}

if($("pillReservoValidos")){
  $("pillReservoValidos").textContent = `Reservo válidos: ${consolidado.filter(x => x.origen === "Reservo").length}`
}

if($("pillMKValidos")){
  $("pillMKValidos").textContent = `MK válidos: ${consolidado.filter(x => x.origen === "MK").length}`
}

}

/* ======================
   PROCESAR
====================== */

$("btnCargar").onclick = async()=>{

if(!dataReservo.length && !dataMK.length){
alert("Debes cargar al menos un archivo")
return
}

await cargarProfesionales()
await cargarProcedimientos()

let reservos = procesarReservo()
let mks = procesarMK()

consolidado = [...reservos,...mks]

render()

}

/* ======================
   CARGA ARCHIVOS
====================== */

$("fileReservo").addEventListener("change", async e=>{

let file = e.target.files[0]

if(!file) return

dataReservo = await leerExcel(file)

})

$("fileMK").addEventListener("change", async e=>{

let file = e.target.files[0]

if(!file) return

dataMK = await leerExcel(file)

})

/* ======================
   BOOT
====================== */

requireAuth({
onUser: async(user)=>{

await loadSidebar({ active: 'produccion_ambulatoria' })
setActiveNav('produccion_ambulatoria')

if($("who")){
$("who").textContent = `Conectado: ${user.email}`
}

wireLogout()
setDefaultToPreviousMonth()

if($("btnItemClose")) $("btnItemClose").onclick = cerrarDetalle
if($("btnItemCancelar")) $("btnItemCancelar").onclick = cerrarDetalle
if($("btnGuardarItem")) $("btnGuardarItem").onclick = guardarDetalle

if($("modalItemBackdrop")){
  $("modalItemBackdrop").addEventListener("click", (e)=>{
    if(e.target === $("modalItemBackdrop")) cerrarDetalle()
  })
}

}
})
