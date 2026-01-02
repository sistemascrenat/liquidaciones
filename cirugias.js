// cirugias.js — COMPLETO
// ✅ Procedimientos/Cirugías en Firestore: procedimientos/{PC0001} con tipo="cirugia"
// ✅ Roles vienen de colección roles (docId tipo r_cirujano)
// ✅ Clínicas vienen de colección clinicas (docId tipo C001)
// ✅ Tarifario por Clínica + TipoPaciente con 3 componentes: HMQ (roles), DP, INS => TOTAL
// ✅ Chip en tabla: "TARIFARIO: OK (CLÍNICA X)" (si esa clínica está completa en Part/Isapre/Fonasa)
// ✅ Buscador: coma=AND, guión=OR

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe } from './utils.js';

// ✅ Sidebar común (layout.js)
import { loadSidebar } from './layout.js';

import {
  collection, getDocs, setDoc, deleteDoc, doc, getDoc,
  serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   STATE
========================= */
const state = {
  user: null,
  all: [],            // cirugías normalizadas
  rolesCatalog: [],   // [{id, nombre}]
  clinicasCatalog: [],// [{id, nombre}]
  editId: null,       // PC0001
  q: '',

  // tarifario modal
  tarProcId: null,    // PC0001
  tarClinicaId: '',
  tarPaciente: 'particular'
};

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
function uniq(arr){
  return [...new Set((arr || []).filter(Boolean))];
}
function money(n){
  const x = Number(n ?? 0) || 0;
  try { return x.toLocaleString('es-CL'); } catch { return String(x); }
}
function onlyDigits(s=''){ return (s ?? '').toString().replace(/\D/g,''); }

/* =========================
   Firestore refs
========================= */
const colProcedimientos = collection(db, 'procedimientos');
const colRoles = collection(db, 'roles');
const colClinicas = collection(db, 'clinicas');

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

async function loadClinicas(){
  const snap = await getDocs(colClinicas);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = cleanReminder(x.nombre);
    if(!nombre) return;
    out.push({ id: d.id, nombre: toUpperSafe(nombre) });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.clinicasCatalog = out;
}

function roleNameById(id){
  return state.rolesCatalog.find(r=>r.id===id)?.nombre || id || '';
}
function clinicaNameById(id){
  return state.clinicasCatalog.find(c=>c.id===id)?.nombre || id || '';
}

/* =========================
   Normalización de doc
========================= */
function normalizeCirugiaDoc(id, x){
  const tipo = (cleanReminder(x.tipo) || 'cirugia').toLowerCase();
  const codigo = cleanReminder(x.codigo) || id;

  const rolesIds = Array.isArray(x.rolesIds) ? x.rolesIds.filter(Boolean) : [];
  const clinicasIds = Array.isArray(x.clinicasIds) ? x.clinicasIds.filter(Boolean) : [];

  // tarifas por clínica
  // tarifas[clinicaId].pacientes[particular|isapre|fonasa] = { honorarios:{roleId:monto}, derechosPabellon, insumos }
  const tarifas = (x.tarifas && typeof x.tarifas === 'object') ? x.tarifas : {};

  return {
    id: id || codigo,
    tipo,
    codigo,
    nombre: cleanReminder(x.nombre) || '',
    estado: (cleanReminder(x.estado) || 'activa').toLowerCase(),

    rolesIds: uniq(rolesIds),
    clinicasIds: uniq(clinicasIds),

    tarifas
  };
}

async function loadAll(){
  const snap = await getDocs(colProcedimientos);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const tipo = (cleanReminder(x.tipo) || '').toLowerCase();
    if(tipo !== 'cirugia') return;
    out.push(normalizeCirugiaDoc(d.id, x));
  });

  // Activas primero, luego por código
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
   Search: coma=AND, guión=OR
========================= */
// Devuelve grupos AND, cada grupo tiene tokens OR
// Ej: "pc0001, manga-bypass" => [ ["pc0001"], ["manga","bypass"] ]
function parseQuery(raw){
  const andParts = (raw || '')
    .toString()
    .split(',')
    .map(s=>s.trim())
    .filter(Boolean);

  const groups = andParts.map(part=>{
    return part
      .split('-')
      .map(x=>normalize(x))
      .filter(Boolean);
  });

  return groups.filter(g=>g.length);
}

function rowMatches(p, rawQuery){
  const groups = parseQuery(rawQuery);
  if(!groups.length) return true;

  const rolesNames = (p.rolesIds || []).map(roleNameById);
  const clinicasNames = (p.clinicasIds || []).map(clinicaNameById);

  // haystack para búsqueda general
  const hay = normalize([
    p.codigo,
    p.nombre,
    p.estado,
    ...p.rolesIds,
    ...rolesNames,
    ...p.clinicasIds,
    ...clinicasNames
  ].join(' '));

  // AND entre grupos, OR dentro del grupo
  return groups.every(orTokens => orTokens.some(t => hay.includes(t)));
}

/* =========================
   UI: roles/clinicas check
========================= */
function paintRolesChecklist(selectedIds=[]){
  const wrap = $('rolesWrap');
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay roles. Crea roles primero.</div>`;
    return;
  }

  const selected = new Set((selectedIds||[]).filter(Boolean));

  for(const r of state.rolesCatalog){
    const id = `rol_${r.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';
    label.innerHTML = `
      <input type="checkbox" id="${id}" data-id="${escapeHtml(r.id)}" ${selected.has(r.id)?'checked':''}/>
      <span class="pill">${escapeHtml(r.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function readRolesChecklist(){
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-id'));
  });
  return uniq(out);
}

function paintClinicasChecklist(selectedIds=[]){
  const wrap = $('clinicasWrap');
  wrap.innerHTML = '';

  if(!state.clinicasCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay clínicas. Crea clínicas primero.</div>`;
    return;
  }

  const selected = new Set((selectedIds||[]).filter(Boolean));

  for(const c of state.clinicasCatalog){
    const id = `cli_${c.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';
    label.innerHTML = `
      <input type="checkbox" id="${id}" data-id="${escapeHtml(c.id)}" ${selected.has(c.id)?'checked':''}/>
      <span class="pill">${escapeHtml(c.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function readClinicasChecklist(){
  const wrap = $('clinicasWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-id'));
  });
  return uniq(out);
}

/* =========================
   MODAL: Crear/Editar
========================= */
function openModal(mode, p=null){
  $('modalBackdrop').style.display = 'grid';

  if(mode === 'create'){
    state.editId = null;
    $('modalTitle').textContent = 'Crear cirugía';
    $('modalSub').textContent = 'Completa los datos y guarda.';
    $('codigo').value = '';
    $('nombre').value = '';
    $('estado').value = 'activa';
    paintRolesChecklist([]);
    paintClinicasChecklist([]);
    $('codigo').focus();
  }else{
    state.editId = p?.id || null;
    $('modalTitle').textContent = 'Editar cirugía';
    $('modalSub').textContent = state.editId ? `ID: ${state.editId}` : '';
    $('codigo').value = p?.codigo || '';
    $('nombre').value = p?.nombre || '';
    $('estado').value = p?.estado || 'activa';
    paintRolesChecklist(p?.rolesIds || []);
    paintClinicasChecklist(p?.clinicasIds || []);
    $('nombre').focus();
  }
}

function closeModal(){
  $('modalBackdrop').style.display = 'none';
}

/* =========================
   SAVE / DELETE
========================= */
function normalizeProcId(codigo){
  // Queremos PC0001 tal cual, pero aseguramos mayúscula + sin espacios
  const c = (codigo || '').toString().trim().toUpperCase();
  return c;
}

async function saveCirugia(){
  const codigo = normalizeProcId(cleanReminder($('codigo').value));
  const nombre = cleanReminder($('nombre').value);
  const estado = (cleanReminder($('estado').value) || 'activa').toLowerCase();

  const rolesIds = readRolesChecklist();
  const clinicasIds = readClinicasChecklist();

  if(!codigo || !/^PC\d{4,}$/i.test(codigo)){
    toast('Código inválido. Ej: PC0001');
    $('codigo').focus();
    return;
  }
  if(!nombre){
    toast('Falta nombre de cirugía');
    $('nombre').focus();
    return;
  }
  if(!rolesIds.length){
    toast('Selecciona al menos 1 rol');
    return;
  }

  const isEdit = !!state.editId;
  const id = isEdit ? state.editId : codigo;

  // Si cambias el código en edición, lo bloqueamos (para no duplicar docs por error)
  if(isEdit && codigo !== state.editId){
    toast('En edición, el código/ID no se cambia (evita duplicados).');
    $('codigo').value = state.editId;
  }

  // Traemos doc existente para no perder tarifas ya cargadas si solo cambias roles/clinicas
  const prevSnap = await getDoc(doc(db, 'procedimientos', id));
  const prev = prevSnap.exists() ? (prevSnap.data() || {}) : {};
  const prevTarifas = (prev.tarifas && typeof prev.tarifas === 'object') ? prev.tarifas : {};

  const payload = {
    tipo: 'cirugia',
    codigo: id,
    nombre,
    estado,

    rolesIds: uniq(rolesIds),
    clinicasIds: uniq(clinicasIds),

    // mantenemos tarifas existentes
    tarifas: prevTarifas,

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };
  if(!isEdit){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  await setDoc(doc(db, 'procedimientos', id), payload, { merge:true });

  toast(isEdit ? 'Cirugía actualizada' : 'Cirugía creada');
  closeModal();
  await loadAll();
}

async function removeCirugia(id){
  const p = state.all.find(x=>x.id===id);
  const ok = confirm(`¿Eliminar cirugía?\n\n${p?.codigo || id}\n${p?.nombre || ''}`);
  if(!ok) return;
  await deleteDoc(doc(db,'procedimientos',id));
  toast('Eliminada');
  await loadAll();
}

/* =========================
   TARIFARIO: helpers
========================= */
function ensureTarStruct(proc, clinicaId){
  const tarifas = (proc.tarifas && typeof proc.tarifas==='object') ? proc.tarifas : {};
  tarifas[clinicaId] = tarifas[clinicaId] || { pacientes: {} };
  const pac = tarifas[clinicaId].pacientes || {};
  tarifas[clinicaId].pacientes = pac;

  for(const k of ['particular','isapre','fonasa']){
    pac[k] = pac[k] || { honorarios:{}, derechosPabellon: 0, insumos: 0 };
    pac[k].honorarios = (pac[k].honorarios && typeof pac[k].honorarios==='object') ? pac[k].honorarios : {};
    pac[k].derechosPabellon = Number(pac[k].derechosPabellon ?? 0) || 0;
    pac[k].insumos = Number(pac[k].insumos ?? 0) || 0;
  }

  return tarifas;
}

function calcHmqFor(proc, clinicaId, paciente){
  const t = proc?.tarifas?.[clinicaId]?.pacientes?.[paciente];
  const h = t?.honorarios || {};
  let sum = 0;
  for(const rid of (proc.rolesIds || [])){
    sum += Number(h[rid] ?? 0) || 0;
  }
  return sum;
}

function isTarCompleteForClinic(proc, clinicaId){
  // completo = para particular/isapre/fonasa:
  // - cada rol debe tener monto > 0 (o >=0? aquí usamos >0 para exigir carga real)
  // - derechosPabellon > 0
  // - insumos > 0
  const pacs = ['particular','isapre','fonasa'];
  for(const pk of pacs){
    const t = proc?.tarifas?.[clinicaId]?.pacientes?.[pk];
    if(!t) return false;

    const h = t.honorarios || {};
    for(const rid of (proc.rolesIds || [])){
      const v = Number(h[rid]);
      if(!(v > 0)) return false;
    }
    if(!(Number(t.derechosPabellon) > 0)) return false;
    if(!(Number(t.insumos) > 0)) return false;
  }
  return true;
}

function tarifarioBadge(proc){
  const okClinics = (proc.clinicasIds || []).filter(cid => isTarCompleteForClinic(proc, cid));
  if(!okClinics.length){
    return `<span class="pill">TARIFARIO: PENDIENTE</span>`;
  }
  if(okClinics.length === 1){
    return `<span class="pill">TARIFARIO: OK (${escapeHtml(clinicaNameById(okClinics[0]))})</span>`;
  }
  return `<span class="pill">TARIFARIO: OK (${okClinics.length} clínicas)</span>`;
}

/* =========================
   TARIFARIO MODAL
========================= */
function openTarifario(proc){
  state.tarProcId = proc.id;
  $('tarBackdrop').style.display = 'grid';

  $('tarTitle').textContent = 'Tarifario';
  $('tarSub').textContent = `${proc.codigo} · ${proc.nombre}`;

  // cargar clínicas disponibles (las marcadas en la cirugía)
  const sel = $('tarClinica');
  sel.innerHTML = '';
  const clinicas = (proc.clinicasIds || []);
  if(!clinicas.length){
    sel.innerHTML = `<option value="">(Sin clínicas seleccionadas)</option>`;
  }else{
    sel.innerHTML = clinicas
      .map(cid => `<option value="${escapeHtml(cid)}">${escapeHtml(clinicaNameById(cid))}</option>`)
      .join('');
  }

  // set defaults
  state.tarClinicaId = clinicas[0] || '';
  state.tarPaciente = 'particular';
  $('tarClinica').value = state.tarClinicaId;
  $('tarPaciente').value = state.tarPaciente;

  paintTarifarioUI();
}

function closeTarifario(){
  $('tarBackdrop').style.display = 'none';
  state.tarProcId = null;
  state.tarClinicaId = '';
  state.tarPaciente = 'particular';
}

function currentProc(){
  return state.all.find(p => p.id === state.tarProcId) || null;
}

function paintTarifarioUI(){
  const proc = currentProc();
  if(!proc) return;

  const clinicaId = $('tarClinica').value || '';
  const paciente = $('tarPaciente').value || 'particular';

  state.tarClinicaId = clinicaId;
  state.tarPaciente = paciente;

  // asegurar estructura local (en memoria)
  proc.tarifas = ensureTarStruct(proc, clinicaId);

  const t = proc.tarifas[clinicaId].pacientes[paciente];

  // roles table
  const body = $('tarRolesBody');
  body.innerHTML = '';

  for(const rid of (proc.rolesIds || [])){
    const tr = document.createElement('tr');
    const name = roleNameById(rid);
    const val = Number(t.honorarios?.[rid] ?? 0) || 0;

    tr.innerHTML = `
      <td><b>${escapeHtml(name)}</b><div class="muted" style="font-size:12px;">${escapeHtml(rid)}</div></td>
      <td>
        <input
          type="number" min="0" step="1"
          data-role-id="${escapeHtml(rid)}"
          value="${val}"
        />
      </td>
    `;
    body.appendChild(tr);
  }

  $('tarDerechos').value = String(Number(t.derechosPabellon ?? 0) || 0);
  $('tarInsumos').value = String(Number(t.insumos ?? 0) || 0);

  // wire inputs
  body.querySelectorAll('input[data-role-id]').forEach(inp=>{
    inp.addEventListener('input', ()=> refreshTarSums());
  });
  $('tarDerechos').addEventListener('input', ()=> refreshTarSums());
  $('tarInsumos').addEventListener('input', ()=> refreshTarSums());

  refreshTarSums();
}

function refreshTarSums(){
  const proc = currentProc();
  if(!proc) return;

  const clinicaId = state.tarClinicaId;
  const paciente = state.tarPaciente;

  const t = proc.tarifas?.[clinicaId]?.pacientes?.[paciente];
  if(!t) return;

  // leer inputs y reflejar en memoria
  $('tarRolesBody').querySelectorAll('input[data-role-id]').forEach(inp=>{
    const rid = inp.getAttribute('data-role-id');
    const v = Number(inp.value ?? 0) || 0;
    t.honorarios[rid] = v;
  });

  t.derechosPabellon = Number($('tarDerechos').value ?? 0) || 0;
  t.insumos = Number($('tarInsumos').value ?? 0) || 0;

  const hmq = calcHmqFor(proc, clinicaId, paciente);
  const dp = Number(t.derechosPabellon ?? 0) || 0;
  const ins = Number(t.insumos ?? 0) || 0;
  const total = hmq + dp + ins;

  $('sumHmq').textContent = `$${money(hmq)}`;
  $('sumDp').textContent = `$${money(dp)}`;
  $('sumIns').textContent = `$${money(ins)}`;
  $('sumTotal').textContent = `$${money(total)}`;

  $('tarResumen').textContent = `HMQ: $${money(hmq)} · DP: $${money(dp)} · INS: $${money(ins)} · TOTAL: $${money(total)}`;
}

async function saveTarifario(){
  const proc = currentProc();
  if(!proc) return;

  // ya está actualizado en memoria; persistimos "tarifas" completo
  await setDoc(doc(db, 'procedimientos', proc.id), {
    tarifas: proc.tarifas,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  toast('Tarifario guardado');
  closeTarifario();
  await loadAll();
}

/* =========================
   Paint table
========================= */
function rolesMini(proc){
  const names = (proc.rolesIds || []).map(roleNameById).filter(Boolean);
  if(!names.length) return `<span class="muted">—</span>`;
  return `<span class="mini">${names.map(n=>`<b>${escapeHtml(n)}</b>`).join(' · ')}</span>`;
}

function cirugiaBlock(proc){
  return `
    <div class="cellBlock">
      <div class="cellTitle">${escapeHtml(proc.nombre || '—')}</div>
      <div class="cellSub">
        <span>Procedimiento · Cirugía</span>
      </div>
      <div class="mini" style="margin-top:6px;">
        ${tarifarioBadge(proc)}
      </div>
    </div>
  `;
}

function estadoHtml(proc){
  const e = (proc.estado || 'activa').toLowerCase();
  const cls = (e === 'activa') ? 'activo' : 'inactivo';
  const label = (e === 'activa') ? 'ACTIVA' : 'INACTIVA';
  return `<span class="state ${cls}">${label}</span>`;
}

function paint(){
  const rows = state.all.filter(p => rowMatches(p, state.q));
  $('count').textContent = `${rows.length} cirugía${rows.length===1?'':'s'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');

    const clinCount = (p.clinicasIds || []).length;

    tr.innerHTML = `
      <td>
        <div class="mono"><b>${escapeHtml(p.codigo)}</b></div>
      </td>
      <td>${cirugiaBlock(p)}</td>
      <td>${rolesMini(p)}</td>
      <td><b>${clinCount}</b></td>
      <td>${estadoHtml(p)}</td>
      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn" type="button" title="Tarifario" aria-label="Tarifario">💲</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openModal('edit', p));
    tr.querySelector('button[aria-label="Tarifario"]').addEventListener('click', ()=> openTarifario(p));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeCirugia(p.id));

    tb.appendChild(tr);
  }
}

/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    // Sidebar común + activo
    await loadSidebar({ active: 'cirugias' });

    $('who').textContent = `Conectado: ${user.email}`;
    setActiveNav('cirugias');
    wireLogout();

    // UI events
    $('btnCrear').addEventListener('click', ()=> openModal('create'));
    $('btnModalClose').addEventListener('click', closeModal);
    $('btnModalCancelar').addEventListener('click', closeModal);
    $('btnModalGuardar').addEventListener('click', saveCirugia);

    $('modalBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalBackdrop')) closeModal();
    });

    $('buscador').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // Tarifario modal events
    $('btnTarClose').addEventListener('click', closeTarifario);
    $('btnTarCancelar').addEventListener('click', closeTarifario);
    $('btnTarGuardar').addEventListener('click', saveTarifario);

    $('tarClinica').addEventListener('change', paintTarifarioUI);
    $('tarPaciente').addEventListener('change', paintTarifarioUI);

    $('tarBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('tarBackdrop')) closeTarifario();
    });

    // Load catalogs + data
    await loadRoles();
    await loadClinicas();
    await loadAll();
  }
});
