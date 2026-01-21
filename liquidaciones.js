// liquidaciones.js — COMPLETO (AJUSTADO A TU FIRESTORE REAL)
// ✅ Fuente única: Producción confirmada (collectionGroup: items) del mes/año
// ✅ Usa IDs si existen: clinicaId, cirugiaId/ambulatorioId, profesionalesId.*
// ✅ Fallback a raw (CSV) si faltan IDs
// ✅ Cálculo: cruza con Tarifario de procedimientos
// ✅ Agrupa por profesional y genera detalle por línea (rol)
// ✅ Pendientes: clínica/proc no mapeado, profesional no existe, tarifa incompleta
// ✅ Export CSV (resumen + detalle)
// ✅ Sidebar común via layout.js

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';
await loadSidebar({ active: 'liquidaciones' });

import {
  collection, collectionGroup, getDocs, query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   AJUSTE ÚNICO (SI CAMBIAS NOMBRES)
========================= */
// En tu esquema real, la producción está en: produccion/{ano}/meses/{mes}/pacientes/{rut}/items/{...}
// Así que lo correcto es usar collectionGroup('items')
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
  // a veces viene "Vidatres" u otra isapre => tu import guarda tipoPaciente:"Isapre"
  // si viene texto raro, dejamos normalizado
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

  rolesMap: new Map(),          // roleId -> nombre
  clinicasById: new Map(),      // C001 -> NOMBRE
  clinicasByName: new Map(),    // normalize(nombre) -> C001

  profesionalesByName: new Map(), // normalize(nombre) -> profDoc
  profesionalesById: new Map(),   // id (string) -> profDoc

  procedimientosByName: new Map(), // normalize(nombre) -> procDoc
  procedimientosById: new Map(),   // id -> procDoc

  prodRows: [],          // docs items del mes
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
    const nombre = cleanReminder(x.nombre) || cleanReminder(x.empresa) || cleanReminder(x.razonSocial) || '';
    const doc = {
      id: d.id,
      nombre: toUpperSafe(nombre || d.id),
      rut: cleanReminder(x.rut) || '',
      tipo: cleanReminder(x.tipo) || cleanReminder(x.personaTipo) || '',
      estado: (cleanReminder(x.estado) || 'activo').toLowerCase()
    };

    byId.set(String(d.id), doc);
    if(nombre) byName.set(normalize(nombre), doc);
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

    // Ignorar anuladas (si tu import usa otro texto, agrega aquí)
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
   Fallback raw (por si faltan IDs)
========================= */
function pickRaw(raw, key){
  const direct = raw?.[key];
  if(direct !== undefined) return direct;

  const nk = normalize(key);
  for(const k of Object.keys(raw || {})){
    if(normalize(k) === nk) return raw[k];
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

    // Fecha/hora: tu item trae fechaISO y horaHM
    const fecha = fmtDateISOorDMY(x.fechaISO || pickRaw(raw,'Fecha'));
    const hora = cleanReminder(x.horaHM || pickRaw(raw,'Hora'));

    // Clínica: usa clinicaId si viene
    const clinicaId = cleanReminder(x.clinicaId) || '';
    const clinicaName = toUpperSafe(cleanReminder(x.clinica || pickRaw(raw,'Clínica')));
    const clinicaLabel = clinicaId
      ? (state.clinicasById.get(clinicaId) || clinicaName || clinicaId)
      : (clinicaName || '(Sin clínica)');

    // Procedimiento: usa cirugiaId/ambulatorioId si viene
    const procId = cleanReminder(x.cirugiaId || x.ambulatorioId) || '';
    const cirugiaName = toUpperSafe(cleanReminder(x.cirugia || pickRaw(raw,'Cirugía')));
    const procDoc =
      (procId && state.procedimientosById.get(String(procId))) ||
      state.procedimientosByName.get(normalize(cirugiaName)) ||
      null;

    const procLabel = procDoc?.nombre || cirugiaName || '(Sin procedimiento)';
    const procRealId = procDoc?.id || procId || '';

    // Tipo paciente: tu item trae tipoPaciente (ej: "Isapre")
    const pacienteTipo = tipoPacienteNorm(x.tipoPaciente || pickRaw(raw,'Tipo de Paciente') || pickRaw(raw,'Previsión'));
    const pacienteNombre = toUpperSafe(cleanReminder(x.nombrePaciente || pickRaw(raw,'Nombre Paciente')));

    const valor = Number(x.hmq || 0) ? (Number(x.valor || 0) || 0) : (asNumberLoose(pickRaw(raw,'Valor')));
    const hmq = Number(x.hmq || 0) || asNumberLoose(pickRaw(raw,'HMQ'));
    const dp  = Number(x.derechosPabellon || 0) || asNumberLoose(pickRaw(raw,'Derechos de Pabellón'));
    const ins = Number(x.insumos || 0) || asNumberLoose(pickRaw(raw,'Insumos'));

    // Por cada rol, generar línea si hay profesional
    for(const rf of ROLE_SPEC){
      const profName =
        toUpperSafe(cleanReminder(x.profesionales?.[rf.key] || pickRaw(raw, rf.csvField))) || '';

      const profId =
        cleanReminder(x.profesionalesId?.[rf.idKey]) || '';

      if(!profName && !profId) continue;

      const profDoc =
        (profId && state.profesionalesById.get(String(profId))) ||
        (profName && state.profesionalesByName.get(normalize(profName))) ||
        null;

      let monto = 0;
      const pend = [];

      // Reglas de pendiente
      if(!clinicaId) pend.push('ClínicaId vacío (import)');
      if(!procDoc) pend.push('Procedimiento no mapeado (nombre/id)');
      if(!pacienteTipo) pend.push('Tipo paciente vacío');

      // Tarifa
      if(clinicaId && procDoc && pacienteTipo){
        const tar = getHonorarioFromTarifa(procDoc, clinicaId, pacienteTipo, rf.roleId);
        if(tar.ok) monto = tar.monto;
        else pend.push(tar.reason || 'Tarifa incompleta');
      }

      if(!profDoc) pend.push('Profesional no existe en catálogo');

      lines.push({
        prodId: row.id,

        fecha,
        hora,

        clinicaId,
        clinicaNombre: clinicaLabel,

        procedimientoId: procRealId,
        procedimientoNombre: procLabel,

        tipoPaciente: pacienteTipo,
        pacienteNombre,

        roleId: rf.roleId,
        roleNombre: state.rolesMap.get(rf.roleId) || rf.label,

        profesionalNombre: profDoc?.nombre || profName || (profId ? String(profId) : ''),
        profesionalId: profDoc?.id || profId || '',
        profesionalRut: profDoc?.rut || '',
        profesionalTipo: profDoc?.tipo || '',

        monto,
        pendiente: pend.length > 0,
        observacion: pend.join(' · '),

        info: { valor, hmq, dp, ins }
      });
    }
  }

  // Agrupar por profesional
  const map = new Map();

  for(const ln of lines){
    const key = ln.profesionalId ? `RUT Personal:${ln.profesionalId}` : `Nombre:${normalize(ln.profesionalNombre)}`;
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

  resumen.sort((a,b)=>{
    if(a.status !== b.status){
      if(a.status === 'ok') return -1;
      if(b.status === 'ok') return 1;
    }
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

  $('lastLoad').textContent = `Items producción: ${state.prodRows.length} · Último cálculo: ${new Date().toLocaleString()}`;
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
        tipo: first?.profesionalTipo || '',
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
