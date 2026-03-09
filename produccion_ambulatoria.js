import { db } from './firebase-init.js'
import { loadSidebar } from './layout.js'

import {
collection,
getDocs
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js"

await loadSidebar({ active: 'produccion_ambulatoria' })

const $ = id => document.getElementById(id)

/* ======================
   DATA
====================== */

let dataReservo = []
let dataMK = []
let profesionales = []
let consolidado = []

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
   BUSCAR PROFESIONAL
====================== */

function buscarProfesional(texto){

texto = normalizarTexto(texto)

return profesionales.find(p=>{

let nombre = normalizarTexto(p.nombre)

let palabras = nombre.split(" ")

return palabras.some(w => texto.includes(w))

})

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

return dataReservo.map(r=>{

let profesionalDetectado = buscarProfesional(r["Profesional"])

return {

origen:"Reservo",

fecha:r["Fecha"],
fechaNorm:normalizarFecha(r["Fecha"]),

rut:r["Rut"],
rutNorm:normalizarRut(r["Rut"]),

paciente:r["Paciente"],

profesional:r["Profesional"],
profesionalDetectado:profesionalDetectado?.nombre || null,

prestacion:r["Tratamiento"],

valor:Number(r["Valor"]) || 0,

alerta:alertaReservo(r),

dataReservo:r,

fusionado:false,
coincidencia:null

}

})

}

/* ======================
   PROCESAR MK
====================== */

function procesarMK(){

return dataMK
.filter(r=> Number(r["Total"]) > 0 )
.map(r=>{

let profesionalDetectado = buscarProfesional(r["D Médico"])

return {

origen:"MK",

fecha:r["Fecha"],
fechaNorm:normalizarFecha(r["Fecha"]),

rut:r["Rut"],
rutNorm:normalizarRut(r["Rut"]),

paciente:r["Paciente"],

profesional:r["D Médico"],
profesionalDetectado:profesionalDetectado?.nombre || null,

prestacion:r["D Artículo"],

valor:Number(r["Total"]) || 0,

dataMK:r,

fusionado:false,
coincidencia:null

}

})

}

/* ======================
   DETECTAR COINCIDENCIAS
====================== */

function detectarCoincidencias(lista){

for(let i=0;i<lista.length;i++){

let r = lista[i]

for(let j=i+1;j<lista.length;j++){

let m = lista[j]

if(
r.rutNorm &&
r.rutNorm === m.rutNorm &&
r.fechaNorm &&
r.fechaNorm === m.fechaNorm &&
r.origen !== m.origen
){

r.coincidencia = m
m.coincidencia = r

}

}

}

}

/* ======================
   FUSIONAR
====================== */

function fusionarRegistro(reg){

if(!reg.coincidencia) return

reg.fusionado = true
reg.coincidencia.fusionado = true

}

/* ======================
   RENDER TABLA
====================== */

function render(){

let tbody = $("tabla").querySelector("tbody")

tbody.innerHTML = ""

for(let r of consolidado){

let tr = document.createElement("tr")

if(r.alerta) tr.classList.add("alerta")

if(r.coincidencia) tr.classList.add("coincidencia")

if(r.fusionado) tr.classList.add("fusionado")

tr.innerHTML = `

<td>${r.origen}</td>
<td>${r.fecha}</td>
<td>${r.rut}</td>
<td>${r.paciente}</td>
<td>${r.profesional}</td>
<td>${r.prestacion}</td>
<td>${r.valor}</td>
<td>${r.alerta || ""}</td>

<td>

<button class="btnFusionar">Fusionar</button>
<button class="btnDetalle">Más info</button>

</td>

`

let btnFusionar = tr.querySelector(".btnFusionar")

btnFusionar.onclick = ()=>{

fusionarRegistro(r)
render()

}

tbody.appendChild(tr)

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

let reservos = procesarReservo()
let mks = procesarMK()

consolidado = [...reservos,...mks]

detectarCoincidencias(consolidado)

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
