// ambulatorios.js — COMPLETO
// ✅ Colección: procedimientos (docId PA0001, PA0002...; tipo="ambulatorio")
// ✅ Tarifa embebida en el MISMO doc, en objeto simple:
//    tarifa.{ precio, honorarios{}, derechosClinica, insumos, actualizadoEl, actualizadoPor }
// ✅ Tabla: chip "PRECIO · COSTO · UTILIDAD" (si no hay nada => TARIFA: PENDIENTE)
// ✅ Buscador: coma=AND, guión=OR
// ✅ CSV: plantilla / export / import (1 fila por procedimiento)
// ✅ Sidebar común: layout.js (await loadSidebar({ active:'ambulatorios' }))
// ✅ Formato CLP: $ con puntos de miles

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe, parseCSV, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';

import {
  collection, getDocs, setDoc, deleteDoc,
  doc, serverTimestamp,
  query, where,
  updateDoc
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  q: '',
  rolesCatalog: [],       // [{id:'r_cirujano', nombre:'CIRUJANO'}]
  all: [],                // [{id, codigo, nombre, estado, rolesIds, tarifa:{...}}]
  editProcId: null,
  activeTar: {
    procId: null,
    precio: 0,
    hmqPorRol: {},
    dc: 0,   // derechos clínica
    ins: 0,
    rolesPermitidos: []
  }
};

const $ = (id)=> document.getElementById(id);

/* =========================
   Utils
========================= */
function normalize(s=''){
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .trim();
}
function uniq(arr){ return [...new Set((arr || []).filter(Boolean))]; }

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

/* =========================
   CLP formatting + number inputs
========================= */
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
function wireMoneyInput(el, onChange){
  if(!el) return;

  const repaint = ()=>{
    const n = asNumberLoose(el.value);
    el.value = (n > 0) ? clp(n) : '';
    onChange?.(n);
  };

  el.addEventListener('blur', repaint);
  el.addEventListener('change', repaint);

  el.addEventListener('input', ()=>{
    const digits = (el.value ?? '').toString().replace(/[^\d]/g,'');
    el.value = digits;
    onChange?.(Number(digits || 0));
  });
}

function spanPrice(txt){
  return `<span style="color:#facc15;font-weight:900;">${txt}</span>`;
}
function spanCost(txt){
  return `<span style="color:#ef4444;font-weight:900;">${txt}</span>`;
}
function spanProfit(txt){
  return `<span style="color:#22c55e;font-weight:900;">${txt}</span>`;
}

/* =========================
   Firestore refs
========================= */
const colProcedimientos = collection(db, 'procedimientos');
const colRoles = collection(db, 'roles');

/* =========================
   Catalog loaders
========================= */
async function loadRoles(){
  const snap = await getDocs(colRoles);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = cleanReminder(x.nombre);
    if(!nombre) return;
    out.push({ id: d.id, nombre: toUpperSafe(nombre) });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;
}

/* =========================
   Procedimientos loader (ambulatorios)
========================= */
function normalizeProcDoc(id, x){
  const tarifaRaw = (x?.tarifa && typeof x.tarifa === 'object') ? x.tarifa : {};

  const honorariosRaw =
    (tarifaRaw.honorarios && typeof tarifaRaw.honorarios === 'object')
      ? tarifaRaw.honorarios
      : {};

  const rolesPermitidos = Array.isArray(x.rolesIds) ? x.rolesIds.filter(Boolean)
                       : (Array.isArray(x.roles) ? x.roles.filter(Boolean) : []);

  const honorarios = {};
  for(const k of Object.keys(honorariosRaw)){
    if(rolesPermitidos.length && !rolesPermitidos.includes(k)) continue;
    const n = Number(honorariosRaw[k] || 0) || 0;
    if(n > 0) honorarios[k] = n;
  }

  const precio = Number(tarifaRaw.precio ?? 0) || 0;
  const dc = Number(tarifaRaw.derechosClinica ?? 0) || 0;
  const ins = Number(tarifaRaw.insumos ?? 0) || 0;
  const hmq = sumHmq(honorarios, rolesPermitidos);
  const costo = hmq + dc + ins;
  const utilidad = precio - costo;
  const hasAny = (precio > 0) || (hmq > 0) || (dc > 0) || (ins > 0);

  return {
    id,
    _raw: x || {},
    codigo: cleanReminder(x.codigo) || id,
    tipo: cleanReminder(x.tipo) || 'ambulatorio',
    nombre: cleanReminder(x.nombre) || '',
    estado: (cleanReminder(x.estado) || 'activa').toLowerCase(),
    rolesIds: rolesPermitidos,
    tarifa: {
      precio,
      honorarios,
      hmq,
      derechosClinica: dc,
      insumos: ins,
      costo,
      utilidad,
      hasAny
    }
  };
}

function sumHmq(hmqPorRol, rolesPermitidos=[]){
  const allowed = new Set((rolesPermitidos || []).filter(Boolean));
  let s = 0;
  for(const k of Object.keys(hmqPorRol || {})){
    if(allowed.size && !allowed.has(k)) continue;
    s += Number(hmqPorRol[k] || 0) || 0;
  }
  return s;
}

async function loadAll(){
  const qy = query(colProcedimientos, where('tipo','==','ambulatorio'));
  const snap = await getDocs(qy);

  const out = [];
  snap.forEach(d=>{
    out.push(normalizeProcDoc(d.id, d.data() || {}));
  });

  out.sort((a,b)=>{
    if(a.estado !== b.estado){
      if(a.estado === 'activa') return -1;
      if(b.estado === 'activa') return 1;
    }
    return normalize(a.codigo).localeCompare(normalize(b.codigo));
  });

  state.all = out;
  paint();
}

/* =========================
   Role helpers
========================= */
function roleNameById(id){
  const r = state.rolesCatalog.find(x=>x.id===id);
  return r?.nombre || id || '';
}

/* =========================
   Search: coma=AND, guión=OR
========================= */
function splitAnd(raw){
  return (raw || '')
    .toString()
    .split(',')
    .map(s=>normalize(s))
    .filter(Boolean);
}
function splitOr(term){
  return (term || '')
    .toString()
    .split('-')
    .map(s=>normalize(s))
    .filter(Boolean);
}

function rowMatches(p, rawQuery){
  const andTerms = splitAnd(rawQuery);
  if(!andTerms.length) return true;

  const rolesNames = (p.rolesIds || []).map(roleNameById);
  const t = p.tarifa || {};
  const tarifaText = [
    String(t.precio || ''),
    String(t.costo || ''),
    String(t.utilidad || ''),
    String(t.derechosClinica || ''),
    String(t.insumos || ''),
    t.hasAny ? 'tarifa' : 'tarifa pendiente'
  ].join(' ');

  const hay = normalize([
    p.codigo,
    p.nombre,
    p.estado,
    'procedimiento ambulatorio',
    ...rolesNames,
    ...(p.rolesIds || []),
    tarifaText
  ].join(' '));

  return andTerms.every(block=>{
    const ors = splitOr(block);
    if(!ors.length) return true;
    return ors.some(x=> hay.includes(x));
  });
}

/* =========================
   UI helpers
========================= */
function estadoBadge(p){
  const est = (p.estado || 'activa').toLowerCase();
  const cls = (est === 'activa') ? 'activo' : 'inactivo';
  const label = (est === 'activa') ? 'ACTIVA' : 'INACTIVA';
  return `<span class="state ${cls}">${label}</span>`;
}

function rolesBlock(p){
  const names = (p.rolesIds || []).map(roleNameById).filter(Boolean);
  if(!names.length) return `<span class="muted">—</span>`;
  return `
    <div class="mini" style="line-height:1.35;">
      ${names.map(x=> `<b>${escapeHtml(x)}</b>`).join(' · ')}
    </div>
  `;
}

function tarifaChip(p){
  const t = p.tarifa || {};
  if(!t.hasAny){
    return `<span class="pill">TARIFA: PENDIENTE</span>`;
  }

  const precio = Number(t.precio || 0) || 0;
  const costo = Number(t.costo || 0) || 0;
  const utilidad = Number(t.utilidad || 0) || 0;

  if(precio <= 0){
    return `
      <span class="pill">
        <b>PRECIO: PENDIENTE</b>
        <span class="muted" style="margin-left:8px;">Costo ${clp(costo)}</span>
      </span>
    `;
  }

  return `
    <span class="pill" title="Precio ${escapeHtml(clp(precio))} · Costo ${escapeHtml(clp(costo))} · Utilidad ${escapeHtml(clp(utilidad))}">
      <b>${clp(precio)}</b>
      <span class="muted" style="margin-left:8px;">Costo ${clp(costo)}</span>
      <span class="muted" style="margin-left:8px;">Utilidad ${clp(utilidad)}</span>
    </span>
  `;
}

/* =========================
   Paint table
========================= */
function paint(){
  const rows = state.all.filter(p=> rowMatches(p, state.q));
  if($('count')) $('count').textContent = `${rows.length} ambulatorio${rows.length===1?'':'s'}`;

  const tb = $('tbody');
  if(!tb) return;
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="mono"><b>${escapeHtml(p.codigo || p.id)}</b></div>
      </td>

      <td>
        <div class="cellBlock">
          <div class="cellTitle">${escapeHtml(p.nombre || '—')}</div>
          <div class="cellSub">
            <span class="muted">Procedimiento · Ambulatorio</span>
          </div>
        </div>
      </td>

      <td>${rolesBlock(p)}</td>

      <td>${tarifaChip(p)}</td>

      <td>${estadoBadge(p)}</td>

      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn" type="button" title="Tarifa" aria-label="Tarifa">💲</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openProcModal('edit', p));
    tr.querySelector('button[aria-label="Tarifa"]').addEventListener('click', ()=> openTarModal(p));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeProc(p.id));

    tb.appendChild(tr);
  }
}

/* =========================
   Modal: Crear/Editar procedimiento
========================= */
function paintProcRolesUI(selectedIds=[]){
  const wrap = $('procRolesWrap');
  if(!wrap) return;
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay roles. Crea roles en <b>Roles</b>.</div>`;
    return;
  }

  const wanted = new Set((selectedIds || []).filter(Boolean));

  for(const r of state.rolesCatalog){
    const id = `pr_${r.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';
    label.innerHTML = `
      <input type="checkbox" id="${id}" data-role-id="${escapeHtml(r.id)}" ${wanted.has(r.id)?'checked':''}/>
      <span class="pill">${escapeHtml(r.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function getProcRolesSelected(){
  const wrap = $('procRolesWrap');
  if(!wrap) return [];
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-role-id'));
  });
  return uniq(out);
}

function openProcModal(mode, p=null){
  $('modalProcBackdrop').style.display = 'grid';

  if(mode === 'create'){
    state.editProcId = null;
    $('modalProcTitle').textContent = 'Crear procedimiento ambulatorio';
    $('modalProcSub').textContent = 'Define código, nombre y roles.';
    $('procCodigo').disabled = false;
    $('procCodigo').value = '';
    $('procNombre').value = '';
    $('procEstado').value = 'activa';
    paintProcRolesUI([]);
  }else{
    state.editProcId = p?.id || null;
    $('modalProcTitle').textContent = 'Editar procedimiento ambulatorio';
    $('modalProcSub').textContent = state.editProcId ? `ID: ${state.editProcId}` : '';
    $('procCodigo').value = p?.codigo || p?.id || '';
    $('procCodigo').disabled = true;
    $('procNombre').value = p?.nombre || '';
    $('procEstado').value = (p?.estado || 'activa');
    paintProcRolesUI(p?.rolesIds || []);
  }

  $('procNombre').focus();
}

function closeProcModal(){
  $('modalProcBackdrop').style.display = 'none';
}

async function saveProc(){
  const codigo = cleanReminder($('procCodigo').value).toUpperCase();
  const nombre = cleanReminder($('procNombre').value);
  const estado = (cleanReminder($('procEstado').value) || 'activa').toLowerCase();
  const rolesIds = getProcRolesSelected();

  if(!codigo || !/^PA\d{4}$/i.test(codigo)){
    toast('Código inválido. Usa formato PA0001');
    $('procCodigo').focus();
    return;
  }
  if(!nombre){
    toast('Falta nombre del procedimiento ambulatorio');
    $('procNombre').focus();
    return;
  }

  const id = state.editProcId || codigo;

  const payload = {
    id,
    codigo,
    tipo: 'ambulatorio',
    nombre,
    estado,
    rolesIds: uniq(rolesIds),
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  if(!state.editProcId){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  await setDoc(doc(db,'procedimientos', id), payload, { merge:true });
  toast(state.editProcId ? 'Ambulatorio actualizado' : 'Ambulatorio creado');
  closeProcModal();
  await loadAll();
}

async function removeProc(id){
  const ok = confirm(`¿Eliminar procedimiento ambulatorio?\n\n${id}`);
  if(!ok) return;

  await deleteDoc(doc(db,'procedimientos', id));
  toast('Eliminado');
  await loadAll();
}

/* =========================
   Modal tarifa
========================= */
function paintTarRolesList(proc){
  const list = $('tarRolesList');
  list.innerHTML = '';

  const rolesIds = proc.rolesIds || [];
  if(!rolesIds.length){
    list.innerHTML = `<div class="muted">Este procedimiento no tiene roles definidos.</div>`;
    return;
  }

  for(const roleId of rolesIds){
    const roleName = roleNameById(roleId);
    const row = document.createElement('div');
    row.style.display = 'grid';
    row.style.gridTemplateColumns = '1fr 220px';
    row.style.gap = '10px';
    row.style.alignItems = 'center';

    row.innerHTML = `
      <div>
        <div style="font-weight:900;">${escapeHtml(roleName)}</div>
        <div class="mini mono muted">${escapeHtml(roleId)}</div>
      </div>
      <div>
        <input data-role-money="${escapeHtml(roleId)}" inputmode="numeric" placeholder="0"/>
      </div>
    `;

    list.appendChild(row);
  }
}

function computeTarTotals(){
  const hmq = sumHmq(state.activeTar.hmqPorRol, state.activeTar.rolesPermitidos);
  const dc = Number(state.activeTar.dc || 0) || 0;
  const ins = Number(state.activeTar.ins || 0) || 0;
  const costo = hmq + dc + ins;

  const precio = Number(state.activeTar.precio || 0) || 0;
  const utilidad = precio - costo;

  if($('hmqPill')) $('hmqPill').textContent = `HMQ: ${clp(hmq)}`;

  $('tarTotales').innerHTML = `
    HMQ: <b>${clp(hmq)}</b><br/>
    Derechos clínica: <b>${clp(dc)}</b><br/>
    INS: <b>${clp(ins)}</b><br/>
    COSTO TOTAL: <b>${clp(costo)}</b><br/>
    <div style="height:6px;"></div>
    PRECIO: <b>${precio > 0 ? clp(precio) : '—'}</b>
  `;

  if(precio > 0){
    $('tarResumen').innerHTML =
      `Precio ${spanPrice(clp(precio))} · ` +
      `Costo ${spanCost(clp(costo))}`;
  } else {
    $('tarResumen').innerHTML =
      `Precio ${spanPrice('PENDIENTE')} · ` +
      `Costo ${spanCost(clp(costo))}`;
  }

  if($('tarUtilidad')){
    if(precio > 0){
      $('tarUtilidad').innerHTML = spanProfit(clp(utilidad));
    } else {
      $('tarUtilidad').textContent = '—';
    }
  }

  if($('tarMargen')){
    if(precio > 0){
      const margen = (utilidad / precio) * 100;
      const color = utilidad >= 0 ? '#22c55e' : '#ef4444';
      $('tarMargen').innerHTML =
        `<span style="color:${color};font-weight:700;">Margen: ${margen.toFixed(1)}%</span>`;
    } else {
      $('tarMargen').textContent = '';
    }
  }

  if($('tarHint')){
    $('tarHint').textContent =
      `TOTAL = HMQ (honorarios por rol) + derechos clínica + insumos. Puedes guardar parcial.`;
  }
}

function loadTarifarioIntoState(proc){
  state.activeTar.procId = proc.id;
  state.activeTar.rolesPermitidos = (proc.rolesIds || []).filter(Boolean);

  const t = proc.tarifa || {};

  state.activeTar.hmqPorRol = { ...(t.honorarios || {}) };
  state.activeTar.precio = Number(t.precio || 0) || 0;
  state.activeTar.dc = Number(t.derechosClinica || 0) || 0;
  state.activeTar.ins = Number(t.insumos || 0) || 0;

  $('tarRolesList').querySelectorAll('input[data-role-money]').forEach(inp=>{
    const rid = inp.getAttribute('data-role-money');
    const n = Number(state.activeTar.hmqPorRol[rid] || 0) || 0;
    inp.value = (n > 0) ? clp(n) : '';
  });

  $('tarPrecio').value = (state.activeTar.precio > 0) ? clp(state.activeTar.precio) : '';
  $('tarDC').value = (state.activeTar.dc > 0) ? clp(state.activeTar.dc) : '';
  $('tarINS').value = (state.activeTar.ins > 0) ? clp(state.activeTar.ins) : '';

  computeTarTotals();
}

function openTarModal(proc){
  $('modalTarBackdrop').style.display = 'grid';
  $('modalTarTitle').textContent = 'Tarifa ambulatoria';
  $('modalTarSub').textContent = `${proc.codigo} · ${proc.nombre}`;

  paintTarRolesList(proc);

  state.activeTar.procId = proc.id;
  state.activeTar.precio = 0;
  state.activeTar.hmqPorRol = {};
  state.activeTar.dc = 0;
  state.activeTar.ins = 0;
  state.activeTar.rolesPermitidos = (proc.rolesIds || []).filter(Boolean);

  $('tarRolesList').querySelectorAll('input[data-role-money]').forEach(inp=>{
    const rid = inp.getAttribute('data-role-money');
    wireMoneyInput(inp, (n)=>{
      state.activeTar.hmqPorRol[rid] = Number(n || 0) || 0;
      computeTarTotals();
    });
  });

  wireMoneyInput($('tarPrecio'), (n)=>{
    state.activeTar.precio = Number(n || 0) || 0;
    computeTarTotals();
  });

  wireMoneyInput($('tarDC'), (n)=>{
    state.activeTar.dc = Number(n || 0) || 0;
    computeTarTotals();
  });

  wireMoneyInput($('tarINS'), (n)=>{
    state.activeTar.ins = Number(n || 0) || 0;
    computeTarTotals();
  });

  loadTarifarioIntoState(proc);
}

function closeTarModal(){
  $('modalTarBackdrop').style.display = 'none';
}

async function saveTarifario(){
  const procId = state.activeTar.procId;
  if(!procId){
    toast('Error: procId vacío');
    return;
  }

  const honorarios = {};
  for(const k of Object.keys(state.activeTar.hmqPorRol || {})){
    const n = Number(state.activeTar.hmqPorRol[k] || 0) || 0;
    if(n > 0) honorarios[k] = n;
  }

  const precio = Number(state.activeTar.precio || 0) || 0;
  const derechosClinica = Number(state.activeTar.dc || 0) || 0;
  const insumos = Number(state.activeTar.ins || 0) || 0;

  const procRef = doc(db, 'procedimientos', procId);

  await updateDoc(procRef, {
    tarifa: {
      precio,
      honorarios,
      derechosClinica,
      insumos,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    },
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  });

  toast('Tarifa guardada');
  closeTarModal();
  await loadAll();
}

/* =========================
   CSV
========================= */
function download(filename, text, mime='text/plain'){
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(()=> URL.revokeObjectURL(url), 1500);
}

function plantillaCSV(){
  const roleCols = state.rolesCatalog.map(r=> `hmq_${r.id}`);
  const headers = [
    'codigo','nombre','estado',
    'rolesIds',
    'precio',
    'derechosClinica',
    'insumos',
    ...roleCols
  ];

  const example = {
    codigo: 'PA0001',
    nombre: 'Procedimiento ejemplo',
    estado: 'activa',
    rolesIds: 'r_medico|r_anestesista|r_arsenalera',
    precio: '350000',
    derechosClinica: '50000',
    insumos: '25000'
  };

  for(const rc of roleCols) example[rc] = '0';
  if(roleCols[0]) example[roleCols[0]] = '120000';

  const csv = toCSV(headers, [example]);
  download('plantilla_tarifas_ambulatorios.csv', csv, 'text/csv');
  toast('Plantilla descargada');
}

function exportCSV(){
  const roleCols = state.rolesCatalog.map(r=> `hmq_${r.id}`);
  const headers = [
    'codigo','nombre','estado',
    'rolesIds',
    'precio','costo','utilidad',
    'derechosClinica','insumos','hmq_total',
    ...roleCols
  ];

  const items = [];

  for(const p of state.all){
    const t = p.tarifa || {};
    const honorarios = (t.honorarios && typeof t.honorarios === 'object') ? t.honorarios : {};
    const hmq_total = sumHmq(honorarios, p.rolesIds || []);
    const derechosClinica = Number(t.derechosClinica ?? 0) || 0;
    const insumos = Number(t.insumos ?? 0) || 0;
    const costo = hmq_total + derechosClinica + insumos;
    const precio = Number(t.precio || 0) || 0;
    const utilidad = precio - costo;

    const row = {
      codigo: p.codigo,
      nombre: p.nombre,
      estado: p.estado,
      rolesIds: (p.rolesIds || []).join('|'),
      precio: String(precio),
      costo: String(costo),
      utilidad: String(utilidad),
      derechosClinica: String(derechosClinica),
      insumos: String(insumos),
      hmq_total: String(hmq_total)
    };

    for(const r of state.rolesCatalog){
      const key = `hmq_${r.id}`;
      row[key] = String(Number(honorarios[r.id] || 0) || 0);
    }

    items.push(row);
  }

  const csv = toCSV(headers, items);
  download(`ambulatorios_tarifas_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

async function importCSV(file){
  const text = await file.text();
  const rows = parseCSV(text);
  if(rows.length < 2){
    toast('CSV vacío o inválido');
    return;
  }

  const headers = rows[0].map(h=>cleanReminder(h).toLowerCase());
  const idx = (name)=> headers.indexOf(name.toLowerCase());

  const I = {
    codigo: idx('codigo'),
    nombre: idx('nombre'),
    estado: idx('estado'),
    rolesIds: idx('rolesids'),
    precio: idx('precio'),
    derechosClinica: idx('derechosclinica'),
    insumos: idx('insumos')
  };

  if(I.codigo < 0 || I.nombre < 0){
    toast('CSV debe incluir: codigo, nombre');
    return;
  }

  const roleCols = headers.filter(h=> h.startsWith('hmq_'));

  let upserts = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];

    const codigo = cleanReminder(row[I.codigo] ?? '').toUpperCase();
    const nombre = cleanReminder(row[I.nombre] ?? '');

    if(!codigo || !/^PA\d{4}$/i.test(codigo) || !nombre){
      skipped++;
      continue;
    }

    const estado = (cleanReminder(I.estado>=0 ? row[I.estado] : 'activa') || 'activa').toLowerCase();
    const rolesIds = uniq(
      (cleanReminder(I.rolesIds>=0 ? row[I.rolesIds] : '') || '')
        .split('|').map(x=>cleanReminder(x)).filter(Boolean)
    );

    const precio = Number(cleanReminder(I.precio>=0 ? row[I.precio] : '0') || 0) || 0;
    const derechosClinica = Number(cleanReminder(I.derechosClinica>=0 ? row[I.derechosClinica] : '0') || 0) || 0;
    const insumos = Number(cleanReminder(I.insumos>=0 ? row[I.insumos] : '0') || 0) || 0;

    const honorarios = {};
    for(const col of roleCols){
      const j = idx(col);
      if(j < 0) continue;
      const rid = col.replace(/^hmq_/,'');
      const val = Number(cleanReminder(row[j] ?? '0') || 0) || 0;
      if(val > 0) honorarios[rid] = val;
    }

    const payload = {
      id: codigo,
      codigo,
      tipo: 'ambulatorio',
      nombre,
      estado,
      rolesIds,
      tarifa: {
        precio,
        honorarios,
        derechosClinica,
        insumos,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: state.user?.email || ''
      },
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || '',
      creadoEl: serverTimestamp(),
      creadoPor: state.user?.email || ''
    };

    await setDoc(doc(db,'procedimientos', codigo), payload, { merge:true });
    upserts++;
  }

  toast(`Import listo: ${upserts} guardados / ${skipped} omitidos`);
  await loadAll();
}

/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    await loadSidebar({ active: 'ambulatorios' });
    setActiveNav('ambulatorios');

    if($('who')) $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    $('btnProcClose')?.addEventListener('click', closeProcModal);
    $('btnProcCancelar')?.addEventListener('click', closeProcModal);
    $('btnProcGuardar')?.addEventListener('click', saveProc);

    $('modalProcBackdrop')?.addEventListener('click', (e)=>{
      if(e.target === $('modalProcBackdrop')) closeProcModal();
    });

    $('btnTarClose')?.addEventListener('click', closeTarModal);
    $('btnTarCancelar')?.addEventListener('click', closeTarModal);
    $('btnTarGuardar')?.addEventListener('click', saveTarifario);

    $('modalTarBackdrop')?.addEventListener('click', (e)=>{
      if(e.target === $('modalTarBackdrop')) closeTarModal();
    });

    $('btnCrear')?.addEventListener('click', ()=> openProcModal('create'));

    $('buscador')?.addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    $('btnDescargarPlantilla')?.addEventListener('click', plantillaCSV);
    $('btnExportar')?.addEventListener('click', exportCSV);

    $('btnImportar')?.addEventListener('click', ()=> $('fileCSV')?.click());
    $('fileCSV')?.addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
      await importCSV(file);
    });

    await loadRoles();
    paintProcRolesUI([]);
    await loadAll();
  }
});
