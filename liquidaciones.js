// liquidaciones.js — COMPLETO
// ✅ Fuente única: Producción confirmada (mes/año)
// ✅ Cálculo: cruza con Tarifario de procedimientos (cirugías / ambulatorios)
// ✅ Agrupa por profesional y genera detalle por línea (rol)
// ✅ Pendientes: clínica/procedimiento no mapeado, profesional no existe, tarifa incompleta
// ✅ Export CSV (resumen + detalle)
// ✅ Sidebar común via layout.js

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';

import {
  collection, getDocs, query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   AJUSTE ÚNICO SI TU PATH ES DISTINTO
   (lo demás NO lo toques)
========================= */
const COL_PROD_ITEMS = 'produccion_items'; // 👈 si tu colección real se llama "produccion", cambia esto

/* =========================
   Const / helpers
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
  // viene dd/mm/aaaa => lo devolvemos igual
  if(/^\d{2}\/\d{2}\/\d{4}$/.test(s)) return s;
  // viene yyyy-mm-dd
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.split('-').reverse().join('/');
  return s;
}
function tipoPacienteNorm(v){
  const x = normalize(v);
  if(x.includes('fona')) return 'fonasa';
  if(x.includes('isap')) return 'isapre';
  if(x.includes('part')) return 'particular';
  // fallback: si viene algo raro, dejamos normalizado
  return x || '';
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

/* =========================
   Role mapping desde Producción CSV
========================= */
const CSV_ROLE_FIELDS = [
  { field: 'Cirujano',      roleId: 'r_cirujano',      label: 'CIRUJANO' },
  { field: 'Anestesista',   roleId: 'r_anestesista',   label: 'ANESTESISTA' },
  { field: 'Ayudante 1',    roleId: 'r_ayudante_1',    label: 'AYUDANTE 1' },
  { field: 'Ayudante 2',    roleId: 'r_ayudante_2',    label: 'AYUDANTE 2' },
  { field: 'Arsenalera',    roleId: 'r_arsenalera',    label: 'ARSENALERA' },
];

/* =========================
   State
========================= */
const state = {
  user: null,

  // filtros UI
  mesNum: null, // 1..12
  ano: null,
  q: '',

  // catálogos
  rolesMap: new Map(),        // roleId -> nombre
  clinicasById: new Map(),    // C001 -> NOMBRE
  clinicasByName: new Map(),  // normalize(nombre) -> C001
  profesionalesByName: new Map(), // normalize(nombre) -> profDoc
  procedimientosByName: new Map(), // normalize(nombre) -> procDoc (PCxxxx o PAxxxx)
  procedimientosById: new Map(),   // id -> procDoc

  // data
  prodRows: [],        // filas producción confirmadas del mes
  liquidResumen: [],   // [{key, nombre, rut, tipo, casos, total, pendientesCount, status, lines:[]}]
  lastDetailExportLines: []
};

/* =========================
   Firestore refs
========================= */
const colRoles = collection(db, 'roles');
const colClinicas = collection(db, 'clinicas');
const colProfesionales = collection(db, 'profesionales');
const colProcedimientos = collection(db, 'procedimientos');
const colProduccion = collection(db, COL_PROD_ITEMS);

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

  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = cleanReminder(x.nombre) || cleanReminder(x.empresa) || cleanReminder(x.razonSocial) || '';
    if(!nombre) return;

    byName.set(normalize(nombre), {
      id: d.id,
      nombre: toUpperSafe(nombre),
      rut: cleanReminder(x.rut) || '',
      tipo: cleanReminder(x.tipo) || cleanReminder(x.personaTipo) || '', // natural / juridica (según tu modelo)
      estado: (cleanReminder(x.estado) || 'activo').toLowerCase()
    });
  });

  state.profesionalesByName = byName;
}

async function loadProcedimientos(){
  const snap = await getDocs(colProcedimientos);

  const byName = new Map();
  const byId = new Map();

  snap.forEach(d=>{
    const x = d.data() || {};
    const id = d.id;
    const nombre = cleanReminder(x.nombre) || '';
    const tipo = (cleanReminder(x.tipo) || '').toLowerCase(); // cirugia / ambulatorio
    const tarifas = (x.tarifas && typeof x.tarifas === 'object') ? x.tarifas : null;

    const doc = {
      id,
      codigo: cleanReminder(x.codigo) || id,
      nombre: toUpperSafe(nombre),
      tipo,
      tarifas // OJO: tu estructura real usa x.tarifas[clinicaId].pacientes[tipo]
    };

    byId.set(id, doc);
    if(nombre) byName.set(normalize(nombre), doc);
  });

  state.procedimientosByName = byName;
  state.procedimientosById = byId;
}

/* =========================
   Load Producción confirmada (mes/año)
========================= */
function monthNameEs(m){
  return ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'][m-1] || '';
}

async function loadProduccionMes(){
  if(!state.mesNum || !state.ano) return;

  // Campos esperados en producción:
  // - ano: 2025
  // - mesNum: 10
  // - estadoImportacion: 'confirmada'
  // - anulado: true/false (si existe)
  // - raw: map con columnas CSV (o campos normalizados)
  //
  // Si tu schema difiere, me dices y lo ajusto.
  const qy = query(
    colProduccion,
    where('ano','==', Number(state.ano)),
    where('mesNum','==', Number(state.mesNum)),
    where('estadoImportacion','==','confirmada')
  );

  const snap = await getDocs(qy);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};
    if(x.anulado === true) return; // no borramos; solo ignoramos anulados

    // raw podría venir como map con headers originales del CSV
    const raw = (x.raw && typeof x.raw === 'object') ? x.raw : x;

    out.push({
      id: d.id,
      raw
    });
  });

  state.prodRows = out;
}

/* =========================
   Cruzar tarifa desde procedimientos.tarifas
   Estructura que tú tienes (según tu screenshot):
   procedimientos/{PC0001}.tarifas[ C002 ].pacientes[ particular ].honorarios[ r_cirujano ] = 1050000
   + derechosPabellon, insumos (que NO se pagan a profesional)
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
   Construir liquidación desde producción
========================= */
function pickRaw(raw, key){
  // soporta header exacto y variantes comunes
  // (tu CSV trae: "Tipo de Paciente", "Nombre Paciente", "Clínica", "Cirugía", etc.)
  const direct = raw?.[key];
  if(direct !== undefined) return direct;

  // fallback por normalización
  const nk = normalize(key);
  for(const k of Object.keys(raw || {})){
    if(normalize(k) === nk) return raw[k];
  }
  return '';
}

function buildLiquidaciones(){
  const lines = []; // líneas “pago por rol”

  for(const row of state.prodRows){
    const raw = row.raw || {};

    const suspendida = normalize(pickRaw(raw,'Suspendida')) === 'si' || normalize(pickRaw(raw,'Suspendida')) === 'true';
    if(suspendida) continue; // opcional: no considerar suspendidas

    const confirmado = normalize(pickRaw(raw,'Confirmado'));
    // si viene Confirmado y es NO, lo ignoramos
    if(confirmado && (confirmado === 'no' || confirmado === 'false')) continue;

    const fecha = fmtDateISOorDMY(pickRaw(raw,'Fecha'));
    const hora = cleanReminder(pickRaw(raw,'Hora'));
    const clinicaName = toUpperSafe(cleanReminder(pickRaw(raw,'Clínica')));
    const cirugiaName = toUpperSafe(cleanReminder(pickRaw(raw,'Cirugía')));
    const pacienteTipo = tipoPacienteNorm(pickRaw(raw,'Tipo de Paciente') || pickRaw(raw,'Previsión'));
    const pacienteNombre = toUpperSafe(cleanReminder(pickRaw(raw,'Nombre Paciente')));
    const valor = asNumberLoose(pickRaw(raw,'Valor')); // total (informativo)
    const hmq = asNumberLoose(pickRaw(raw,'HMQ'));     // total honorarios (informativo)
    const dp = asNumberLoose(pickRaw(raw,'Derechos de Pabellón')); // informativo
    const ins = asNumberLoose(pickRaw(raw,'Insumos')); // informativo

    // Map clínicaId por nombre
    const clinicaId = state.clinicasByName.get(normalize(clinicaName)) || '';
    const clinicaLabel = clinicaId ? (state.clinicasById.get(clinicaId) || clinicaName || clinicaId) : (clinicaName || '(Sin clínica)');

    // Map procedimiento por nombre (tu procedimiento en Firestore es "Manga", y CSV trae "Manga" etc.)
    const procDoc = state.procedimientosByName.get(normalize(cirugiaName)) || null;
    const procId = procDoc?.id || '';
    const procLabel = procDoc?.nombre || cirugiaName || '(Sin procedimiento)';

    // Por cada rol del CSV, generamos línea si hay nombre de profesional
    for(const rf of CSV_ROLE_FIELDS){
      const profName = toUpperSafe(cleanReminder(pickRaw(raw, rf.field)));
      if(!profName) continue;

      const profDoc = state.profesionalesByName.get(normalize(profName)) || null;

      // Reglas de pendiente y monto:
      let monto = 0;
      const pend = [];

      if(!clinicaId) pend.push('Clínica no mapeada');
      if(!procDoc) pend.push('Procedimiento no mapeado');
      if(!pacienteTipo) pend.push('Tipo paciente vacío');

      if(clinicaId && procDoc && pacienteTipo){
        const tar = getHonorarioFromTarifa(procDoc, clinicaId, pacienteTipo, rf.roleId);
        if(tar.ok) monto = tar.monto;
        else pend.push(tar.reason || 'Tarifa incompleta');
      }else{
        // igual dejamos monto=0, queda pendiente
      }

      if(!profDoc) pend.push('Profesional no existe en catálogo');

      lines.push({
        prodId: row.id,
        fecha,
        hora,
        clinicaId,
        clinicaNombre: clinicaLabel,
        procedimientoId: procId,
        procedimientoNombre: procLabel,
        tipoPaciente: pacienteTipo,
        pacienteNombre,
        roleId: rf.roleId,
        roleNombre: state.rolesMap.get(rf.roleId) || rf.label,
        profesionalNombre: profDoc?.nombre || profName,
        profesionalId: profDoc?.id || '',
        profesionalRut: profDoc?.rut || '',
        profesionalTipo: profDoc?.tipo || '',
        monto,
        pendiente: pend.length > 0,
        observacion: pend.join(' · '),

        // campos informativos del CSV (por si quieres verlos en detalle)
        info: { valor, hmq, dp, ins }
      });
    }
  }

  // Agrupar por profesionalKey
  const map = new Map();

  for(const ln of lines){
    const key = ln.profesionalId ? `prof:${ln.profesionalId}` : `name:${normalize(ln.profesionalNombre)}`;
    if(!map.has(key)){
      map.set(key, {
        key,
        nombre: ln.profesionalNombre,
        rut: ln.profesionalRut,
        tipo: ln.profesionalTipo,
        casos: 0,
        total: 0,
        pendientesCount: 0,
        lines: []
      });
    }
    const agg = map.get(key);
    agg.casos += 1;
    agg.total += Number(ln.monto || 0) || 0;
    if(ln.pendiente) agg.pendientesCount += 1;
    agg.lines.push(ln);
  }

  const resumen = [...map.values()].map(x=>{
    const status = x.pendientesCount > 0 ? 'pendiente' : 'ok';
    return { ...x, status };
  });

  // Orden: OK primero por total desc, luego pendientes
  resumen.sort((a,b)=>{
    if(a.status !== b.status){
      if(a.status === 'ok') return -1;
      if(b.status === 'ok') return 1;
    }
    return (b.total||0) - (a.total||0);
  });

  // guardar
  state.liquidResumen = resumen;
}

/* =========================
   Search
========================= */
function matchesSearch(agg, q){
  const s = normalize(q);
  if(!s) return true;

  const hay = normalize([
    agg.nombre, agg.rut, agg.tipo,
    ...agg.lines.map(l=> `${l.roleNombre} ${l.clinicaNombre} ${l.procedimientoNombre} ${l.pacienteNombre} ${l.tipoPaciente} ${l.observacion}`)
  ].join(' '));

  return hay.includes(s);
}

/* =========================
   Paint
========================= */
function paint(){
  const rows = state.liquidResumen.filter(x=> matchesSearch(x, state.q));
  $('pillCount').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  // estado global
  const pendientes = rows.reduce((a,b)=> a + (b.pendientesCount||0), 0);
  const pill = $('pillEstado');
  if(!state.prodRows.length){
    pill.className = 'pill warn';
    pill.textContent = 'Sin producción confirmada';
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

    const tipoTxt = agg.tipo ? toUpperSafe(agg.tipo) : '—';
    const rutTxt = agg.rut || '—';

    const statusPill =
      agg.status === 'ok'
        ? pillHtml('ok','OK')
        : pillHtml('warn',`PENDIENTE · ${agg.pendientesCount}`);

    tr.innerHTML = `
      <td class="mono">${i++}</td>
      <td>
        <div class="big">${escapeHtml(agg.nombre || '—')}</div>
        <div class="mini muted">${escapeHtml(agg.key)}</div>
      </td>
      <td class="mono">${escapeHtml(rutTxt)}</td>
      <td>${escapeHtml(tipoTxt)}</td>
      <td class="mono">${agg.casos}</td>
      <td><b>${clp(agg.total)}</b></td>
      <td>${statusPill}</td>
      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Ver detalle" aria-label="Detalle">🔎</button>
          <button class="iconBtn" type="button" title="Exportar (profesional)" aria-label="ExportProf">⬇️</button>
        </div>
      </td>
    `;

    tr.querySelector('[aria-label="Detalle"]').addEventListener('click', ()=> openDetalle(agg));
    tr.querySelector('[aria-label="ExportProf"]').addEventListener('click', ()=> exportDetalleProfesional(agg));

    tb.appendChild(tr);
  }

  $('lastLoad').textContent = `Filas producción: ${state.prodRows.length} · Último cálculo: ${new Date().toLocaleString()}`;
}

/* =========================
   Modal detalle
========================= */
function openDetalle(agg){
  $('modalBackdrop').style.display = 'grid';

  $('modalTitle').textContent = agg.nombre || 'Detalle';
  $('modalSub').textContent = `${monthNameEs(state.mesNum)} ${state.ano} · Casos: ${agg.casos} · ${agg.rut ? 'RUT: '+agg.rut : ''}`;

  $('modalPillTotal').textContent = `TOTAL: ${clp(agg.total)}`;
  $('modalPillPendientes').textContent = `Pendientes: ${agg.pendientesCount}`;

  const tb = $('modalTbody');
  tb.innerHTML = '';

  // orden por fecha
  const lines = [...(agg.lines || [])].sort((a,b)=>{
    const fa = normalize(a.fecha);
    const fb = normalize(b.fecha);
    if(fa !== fb) return fa.localeCompare(fb);
    return normalize(a.roleNombre).localeCompare(normalize(b.roleNombre));
  });

  state.lastDetailExportLines = lines;

  for(const l of lines){
    const st = l.pendiente ? pillHtml('warn','PENDIENTE') : pillHtml('ok','OK');
    const obs = l.observacion || '';

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono">${escapeHtml(l.fecha || '')} ${escapeHtml(l.hora || '')}</td>
      <td>${escapeHtml(l.clinicaNombre || '')}</td>
      <td>${escapeHtml(l.procedimientoNombre || '')}<div class="mini muted mono">${escapeHtml(l.procedimientoId || '')}</div></td>
      <td>${escapeHtml(l.pacienteNombre || '')}<div class="mini muted">${escapeHtml((l.tipoPaciente||'').toUpperCase())}</div></td>
      <td>${escapeHtml(l.roleNombre || '')}<div class="mini muted mono">${escapeHtml(l.roleId || '')}</div></td>
      <td><b>${clp(l.monto || 0)}</b></td>
      <td>${st}</td>
      <td class="mini">${escapeHtml(obs)}</td>
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
  const headers = ['mes','ano','profesional','rut','tipo','casos','total','pendientes'];
  const items = state.liquidResumen.map(a=>({
    mes: monthNameEs(state.mesNum),
    ano: String(state.ano),
    profesional: a.nombre || '',
    rut: a.rut || '',
    tipo: a.tipo || '',
    casos: String(a.casos || 0),
    total: String(a.total || 0),
    pendientes: String(a.pendientesCount || 0)
  }));
  const csv = toCSV(headers, items);
  download(`liquidaciones_resumen_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV resumen exportado');
}

function exportDetalleCSV(){
  // detalle global (todos los profesionales)
  const headers = [
    'mes','ano',
    'profesional','rut','tipo',
    'fecha','hora',
    'clinica','procedimiento','tipoPaciente','paciente',
    'rol','monto','pendiente','observacion',
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
        tipo: a.tipo || '',
        fecha: l.fecha || '',
        hora: l.hora || '',
        clinica: l.clinicaNombre || '',
        procedimiento: l.procedimientoNombre || '',
        tipoPaciente: l.tipoPaciente || '',
        paciente: l.pacienteNombre || '',
        rol: l.roleNombre || '',
        monto: String(l.monto || 0),
        pendiente: l.pendiente ? 'SI' : 'NO',
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
    'profesional','rut','tipo',
    'fecha','hora',
    'clinica','procedimiento','tipoPaciente','paciente',
    'rol','monto','pendiente','observacion',
    'prodId'
  ];

  const items = (agg.lines || []).map(l=>({
    mes: monthNameEs(state.mesNum),
    ano: String(state.ano),
    profesional: agg.nombre || '',
    rut: agg.rut || '',
    tipo: agg.tipo || '',
    fecha: l.fecha || '',
    hora: l.hora || '',
    clinica: l.clinicaNombre || '',
    procedimiento: l.procedimientoNombre || '',
    tipoPaciente: l.tipoPaciente || '',
    paciente: l.pacienteNombre || '',
    rol: l.roleNombre || '',
    monto: String(l.monto || 0),
    pendiente: l.pendiente ? 'SI' : 'NO',
    observacion: l.observacion || '',
    prodId: l.prodId || ''
  }));

  const csv = toCSV(headers, items);
  const safeName = normalize(agg.nombre || 'profesional').replace(/[^a-z0-9\-]/g,'-').slice(0,40);
  download(`liquidacion_${safeName}_${state.ano}_${String(state.mesNum).padStart(2,'0')}.csv`, csv, 'text/csv');
  toast('CSV profesional exportado');
}

/* =========================
   Boot / UI init
========================= */
function initMonthYearSelectors(){
  // Meses
  const mesSel = $('mes');
  mesSel.innerHTML = '';
  for(let m=1;m<=12;m++){
    const opt = document.createElement('option');
    opt.value = String(m);
    opt.textContent = monthNameEs(m);
    mesSel.appendChild(opt);
  }

  // Año (rango simple)
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

  // defaults
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

    // Sidebar común
    await loadSidebar({ active: 'liquidaciones' });
    setActiveNav('liquidaciones');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    initMonthYearSelectors();

    // buscador
    $('q').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // botones
    $('btnRecalcular').addEventListener('click', recalc);
    $('btnCSVResumen').addEventListener('click', exportResumenCSV);
    $('btnCSVDetalle').addEventListener('click', exportDetalleCSV);

    // modal
    $('btnClose').addEventListener('click', closeDetalle);
    $('btnCerrar2').addEventListener('click', closeDetalle);
    $('modalBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalBackdrop')) closeDetalle();
    });
    $('btnExportDetalleProf').addEventListener('click', ()=>{
      // export del último detalle abierto (si existe)
      if(!state.lastDetailExportLines.length){
        toast('No hay detalle abierto');
        return;
      }
      // reconstruimos un agg temporal (solo para export)
      const first = state.lastDetailExportLines[0];
      const agg = {
        nombre: first?.profesionalNombre || 'Profesional',
        rut: first?.profesionalRut || '',
        tipo: first?.profesionalTipo || '',
        lines: state.lastDetailExportLines
      };
      exportDetalleProfesional(agg);
    });

    // cargar catálogos (1 sola vez)
    await Promise.all([
      loadRoles(),
      loadClinicas(),
      loadProfesionales(),
      loadProcedimientos()
    ]);

    // primer cálculo
    await recalc();
  }
});
