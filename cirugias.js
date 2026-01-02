// cirugias.js — Módulo Cirugías (Catálogo + Tarifario por clínica + tipo paciente)
// - Procedimientos en: collection "procedimientos" (docId = "PC0001", "PC0002"...)
// - tipo: "cirugia"
// - Roles: vienen de collection "roles" (docId = r_cirujano, r_anestesista...)
// - Tarifario por clínica (collection "clinicas") y tipo paciente (particular/isapre/fonasa)
//
// Buscador:
// - coma "," = AND
// - guión "-" = OR dentro del mismo bloque
//   Ej: "apendice, cirujano-anestesista" => (apendice) AND (cirujano OR anestesista)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe } from './utils.js';

import {
  collection, getDocs, doc, getDoc, setDoc, deleteDoc,
  serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  all: [],               // cirugías normalizadas
  rolesCatalog: [],      // [{id, nombre, estado}]
  clinicasCatalog: [],   // [{id, nombre, estado}]
  q: '',
  editId: null,          // PCxxxx en edición

  // Tarifario modal
  tarProcId: null,       // PCxxxx
  tarClinicaId: null     // C001, etc
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

function isValidPC(code){
  // PC0001
  return /^PC\d{4}$/i.test((code||'').toString().trim());
}

/* =========================
   Search parser (AND/OR)
========================= */
function parseQuery(raw){
  // AND groups by comma
  // each group: OR tokens by hyphen
  // returns: [ [token1, token2], [token3] ... ] (outer=AND, inner=OR)
  const andGroups = (raw || '')
    .toString()
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  const out = andGroups.map(g =>
    g.split('-')
     .map(t => normalize(t))
     .filter(Boolean)
  ).filter(group => group.length);

  return out; // AND of OR-groups
}

function roleNameById(id){
  const r = state.rolesCatalog.find(x=>x.id===id);
  return r?.nombre || id || '';
}

function clinicaNameById(id){
  const c = state.clinicasCatalog.find(x=>x.id===id);
  return c?.nombre || id || '';
}

function rowMatches(proc, rawQuery){
  const groups = parseQuery(rawQuery);
  if(!groups.length) return true;

  const rolesNames = (proc.roles || []).map(roleNameById);
  const clinNames = (proc.clinicasIds || []).map(clinicaNameById);

  const hay = normalize([
    proc.id, proc.codigo, proc.nombre,
    proc.estado,
    ...rolesNames,
    ...(proc.roles || []),
    ...clinNames,
    ...(proc.clinicasIds || []),
  ].join(' '));

  // AND: cada grupo debe cumplirse
  // OR: dentro del grupo basta un token
  return groups.every(orGroup => orGroup.some(t => hay.includes(t)));
}

/* =========================
   Firestore refs
========================= */
const colProcedimientos = collection(db, 'procedimientos');
const colRoles = collection(db, 'roles');
const colClinicas = collection(db, 'clinicas');

/* =========================
   Normalize doc
========================= */
function normalizeProcDoc(id, x){
  const tipo = (cleanReminder(x.tipo) || '').toLowerCase();
  if(tipo && tipo !== 'cirugia') {
    // dejamos pasar otros tipos, pero el loader filtra
  }

  const codigo = cleanReminder(x.codigo) || id || '';
  const roles = Array.isArray(x.roles) ? x.roles.filter(Boolean) : [];
  const precios = (x.precios && typeof x.precios === 'object') ? x.precios : {};

  const clinicasIds = Object.keys(precios || {}).filter(Boolean);

  return {
    id: id || codigo,
    tipo: tipo || 'cirugia',
    codigo: codigo || id || '',
    nombre: cleanReminder(x.nombre) || '',
    estado: (cleanReminder(x.estado) || 'activa').toLowerCase(), // activa/inactiva

    roles: uniq(roles),

    // precios[clinicaId][tipoPaciente][roleId] = number
    precios,

    clinicasIds
  };
}

/* =========================
   Loaders
========================= */
async function loadRoles(){
  const snap = await getDocs(colRoles);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = cleanReminder(x.nombre);
    if(!nombre) return;
    out.push({
      id: d.id,
      nombre: toUpperSafe(nombre),
      estado: (cleanReminder(x.estado) || 'activo').toLowerCase()
    });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;
  paintRolesChecklist();
}

async function loadClinicas(){
  const snap = await getDocs(colClinicas);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = cleanReminder(x.nombre) || cleanReminder(x.id) || d.id;
    out.push({
      id: d.id,
      nombre: toUpperSafe(nombre),
      estado: (cleanReminder(x.estado) || 'activa').toLowerCase()
    });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.clinicasCatalog = out;
  paintClinicaSelect();
}

async function loadAll(){
  const snap = await getDocs(colProcedimientos);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const tipo = (cleanReminder(x.tipo) || '').toLowerCase();
    if(tipo && tipo !== 'cirugia') return; // filtramos cirugías
    // si no tiene tipo, asumimos que no es cirugía (pero puedes cambiar esto si quieres)
    // aquí asumimos que sí la guardaremos con tipo="cirugia"
    out.push(normalizeProcDoc(d.id, x));
  });

  // Orden: activos primero, luego por código
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
   UI helpers
========================= */
function setCount(n){
  $('count').textContent = `${n} cirugía${n===1?'':'s'}`;
}

function statusPill(estado){
  const e = (estado || 'activa').toLowerCase();
  const cls = (e === 'activa') ? 'activo' : 'inactivo';
  const label = (e === 'activa') ? 'ACTIVA' : 'INACTIVA';
  return `<span class="state ${cls}">${label}</span>`;
}

function tarifarioBadge(proc){
  // indicador simple:
  // - sin precios => rojo
  // - con algunas clinicas => amarillo/verde según si hay celdas vacías
  const clinCount = (proc.clinicasIds || []).length;
  if(!clinCount) return `<span class="pill">TARIFARIO: NO</span>`;

  // revisa si hay algún vacío en roles/tipos
  const tipos = ['particular','isapre','fonasa'];
  let anyMissing = false;

  for(const cid of proc.clinicasIds){
    const byClin = proc.precios?.[cid] || {};
    for(const tp of tipos){
      const byTp = byClin?.[tp] || {};
      for(const rid of (proc.roles || [])){
        const v = byTp?.[rid];
        if(v === undefined || v === null || v === '' || Number(v) === 0){
          // permitimos 0? normalmente no, lo marcamos missing
          anyMissing = true;
          break;
        }
      }
      if(anyMissing) break;
    }
    if(anyMissing) break;
  }

  return anyMissing
    ? `<span class="pill">TARIFARIO: PARCIAL</span>`
    : `<span class="pill">TARIFARIO: OK</span>`;
}

function rolesMini(proc){
  const ids = (proc.roles || []);
  if(!ids.length) return `<span class="muted">—</span>`;
  const names = ids.map(roleNameById).filter(Boolean);
  return `<span class="mini">${names.map(n=>`<b>${escapeHtml(n)}</b>`).join(' · ')}</span>`;
}

function clinicasMini(proc){
  const n = (proc.clinicasIds || []).length;
  return n ? `<b>${n}</b>` : `<span class="muted">0</span>`;
}

/* =========================
   Roles Checklist (modal)
========================= */
function paintRolesChecklist(){
  const wrap = $('rolesWrap');
  if(!wrap) return;
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay roles creados. Ve a <b>Roles</b>.</div>`;
    return;
  }

  for(const r of state.rolesCatalog){
    const id = `rk_${r.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';
    label.innerHTML = `
      <input type="checkbox" id="${id}" data-role-id="${escapeHtml(r.id)}"/>
      <span class="pill">${escapeHtml(r.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function getCheckedRoles(){
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-role-id'));
  });
  return uniq(out);
}

function setCheckedRoles(ids){
  const wanted = new Set((ids||[]).filter(Boolean));
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  checks.forEach(ch=>{
    const id = ch.getAttribute('data-role-id');
    ch.checked = wanted.has(id);
  });
}

/* =========================
   Clínica select (tarifario)
========================= */
function paintClinicaSelect(){
  const sel = $('selClinica');
  if(!sel) return;

  sel.innerHTML = '';
  if(!state.clinicasCatalog.length){
    sel.innerHTML = `<option value="">(Sin clínicas)</option>`;
    return;
  }

  sel.innerHTML = `<option value="">Selecciona clínica…</option>` +
    state.clinicasCatalog
      .map(c=> `<option value="${escapeHtml(c.id)}">${escapeHtml(c.nombre)} (${escapeHtml(c.id)})</option>`)
      .join('');
}

/* =========================
   Modal: Crear/Editar
========================= */
function openModal(mode, proc=null){
  $('modalBackdrop').style.display = 'grid';

  if(mode === 'create'){
    state.editId = null;
    $('modalTitle').textContent = 'Crear cirugía';
    $('modalSub').textContent = 'Completa los datos. Se puede guardar sin precios.';
    $('codigo').value = '';
    $('nombre').value = '';
    $('estado').value = 'activa';
    setCheckedRoles([]);
  }else{
    state.editId = proc?.id || null;
    $('modalTitle').textContent = 'Editar cirugía';
    $('modalSub').textContent = state.editId ? `ID: ${state.editId}` : '';
    $('codigo').value = proc?.codigo || proc?.id || '';
    $('nombre').value = proc?.nombre || '';
    $('estado').value = (proc?.estado || 'activa');
    setCheckedRoles(proc?.roles || []);
  }

  $('codigo').focus();
}

function closeModal(){
  $('modalBackdrop').style.display = 'none';
}

/* =========================
   Modal: Tarifario
========================= */
function openTarifario(proc){
  state.tarProcId = proc?.id || null;
  state.tarClinicaId = '';

  $('tarBackdrop').style.display = 'grid';
  $('tarTitle').textContent = 'Tarifario';
  $('tarSub').textContent = `${proc?.codigo || proc?.id || ''} · ${proc?.nombre || ''}`;

  $('selClinica').value = '';
  $('tarHint').textContent = 'Selecciona una clínica para editar precios.';

  paintTarTable(proc, null);
}

function closeTarifario(){
  $('tarBackdrop').style.display = 'none';
  state.tarProcId = null;
  state.tarClinicaId = '';
}

function paintTarTable(proc, clinicaId){
  const tb = $('tarBody');
  tb.innerHTML = '';

  const roles = (proc?.roles || []);
  if(!roles.length){
    tb.innerHTML = `<tr><td colspan="4" class="muted">Esta cirugía no tiene roles asignados. Edita la cirugía y selecciona roles.</td></tr>`;
    return;
  }

  const tipos = ['particular','isapre','fonasa'];
  const precios = proc?.precios || {};
  const byClin = clinicaId ? (precios[clinicaId] || {}) : {};

  for(const rid of roles){
    const tr = document.createElement('tr');

    const rn = roleNameById(rid);
    const row = {
      particular: byClin?.particular?.[rid] ?? '',
      isapre: byClin?.isapre?.[rid] ?? '',
      fonasa: byClin?.fonasa?.[rid] ?? ''
    };

    tr.innerHTML = `
      <td>
        <div class="cellBlock">
          <div class="cellTitle">${escapeHtml(rn)}</div>
          <div class="mini muted">${escapeHtml(rid)}</div>
        </div>
      </td>
      ${tipos.map(tp=>`
        <td>
          <input
            type="number"
            inputmode="numeric"
            min="0"
            step="1"
            class="tarInput"
            data-role="${escapeHtml(rid)}"
            data-tipo="${tp}"
            value="${escapeHtml(String(row[tp] ?? ''))}"
            placeholder="(pendiente)"
            ${clinicaId ? '' : 'disabled'}
          />
        </td>
      `).join('')}
    `;

    tb.appendChild(tr);
  }
}

function readTarInputs(){
  const proc = state.all.find(x=>x.id===state.tarProcId);
  if(!proc) return null;

  const clinicaId = state.tarClinicaId;
  if(!clinicaId) return null;

  const inputs = document.querySelectorAll('#tarBody .tarInput[data-role][data-tipo]');
  const next = JSON.parse(JSON.stringify(proc.precios || {}));

  if(!next[clinicaId]) next[clinicaId] = {};
  if(!next[clinicaId].particular) next[clinicaId].particular = {};
  if(!next[clinicaId].isapre) next[clinicaId].isapre = {};
  if(!next[clinicaId].fonasa) next[clinicaId].fonasa = {};

  inputs.forEach(inp=>{
    const rid = inp.getAttribute('data-role');
    const tp = inp.getAttribute('data-tipo');
    const raw = (inp.value ?? '').toString().trim();

    // vacío => lo dejamos null (pendiente)
    if(raw === ''){
      if(next[clinicaId]?.[tp]) delete next[clinicaId][tp][rid];
      return;
    }

    const num = Number(raw);
    if(Number.isFinite(num) && num >= 0){
      next[clinicaId][tp][rid] = Math.trunc(num);
    }
  });

  return next;
}

/* =========================
   Save / Delete
========================= */
async function saveCirugia(){
  const codigo = cleanReminder($('codigo').value).toUpperCase();
  const nombre = cleanReminder($('nombre').value);
  const estado = (cleanReminder($('estado').value) || 'activa').toLowerCase();
  const roles = getCheckedRoles();

  if(!codigo || !isValidPC(codigo)){
    toast('Código inválido. Debe ser PC0001');
    $('codigo').focus();
    return;
  }
  if(!nombre){
    toast('Falta nombre de la cirugía');
    $('nombre').focus();
    return;
  }
  if(!roles.length){
    // permitimos guardar sin roles? tú decides:
    // por defecto lo permitimos, pero avisamos
    const ok = confirm('Esta cirugía no tiene roles asignados.\n\n¿Guardar igual?');
    if(!ok) return;
  }

  const isEdit = !!state.editId;

  // Si estás editando y cambiaste el código, lo manejamos:
  // - Opción simple: obligar a que el docId sea el código.
  // - Si cambia, creamos nuevo doc y borramos el anterior (si quieres).
  const targetId = codigo;

  const payload = {
    tipo: 'cirugia',
    id: targetId,
    codigo: targetId,
    nombre,
    estado,
    roles: uniq(roles),

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  if(!isEdit){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }else{
    // Si el id original difiere del nuevo código, copiamos precios/otros desde el doc viejo
    const old = state.all.find(x=>x.id===state.editId);
    if(old){
      payload.precios = old.precios || {};
      // También podrías querer mantener algo más aquí si agregas campos en el futuro.
    }
  }

  // Guardar en el docId = PC0001
  await setDoc(doc(db,'procedimientos',targetId), payload, { merge:true });

  // Si renombraste (editId distinto), opcional borrar el anterior:
  if(isEdit && state.editId && state.editId !== targetId){
    const okDel = confirm(`Cambiaste el código.\n\n¿Eliminar el registro anterior (${state.editId})?\n(Si no, quedarán ambos)`);
    if(okDel){
      await deleteDoc(doc(db,'procedimientos',state.editId));
    }
  }

  toast(isEdit ? 'Cirugía actualizada' : 'Cirugía creada');
  closeModal();
  await loadAll();
}

async function removeCirugia(id){
  const proc = state.all.find(x=>x.id===id);
  const ok = confirm(`¿Eliminar cirugía?\n\n${proc?.codigo || id}\n${proc?.nombre || ''}`);
  if(!ok) return;

  await deleteDoc(doc(db,'procedimientos',id));
  toast('Eliminada');
  await loadAll();
}

async function saveTarifario(){
  const proc = state.all.find(x=>x.id===state.tarProcId);
  if(!proc){
    toast('No encontré la cirugía en memoria');
    return;
  }
  if(!state.tarClinicaId){
    toast('Selecciona una clínica');
    return;
  }

  const preciosNext = readTarInputs();
  if(!preciosNext){
    toast('No pude leer el tarifario');
    return;
  }

  await setDoc(doc(db,'procedimientos',proc.id), {
    precios: preciosNext,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  toast('Tarifario guardado');
  closeTarifario();
  await loadAll();
}

/* =========================
   Paint
========================= */
function paint(){
  const rows = state.all.filter(p=> rowMatches(p, state.q));
  setCount(rows.length);

  const tb = $('tbody');
  tb.innerHTML = '';

  if(!rows.length){
    tb.innerHTML = `<tr><td colspan="6" class="muted">No hay cirugías para mostrar.</td></tr>`;
    return;
  }

  for(const p of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="cellBlock">
          <div class="cellTitle mono">${escapeHtml(p.codigo || p.id)}</div>
          <div class="mini">${tarifarioBadge(p)}</div>
        </div>
      </td>

      <td>
        <div class="cellBlock">
          <div class="cellTitle">${escapeHtml(p.nombre || '—')}</div>
          <div class="mini muted">Procedimiento · Cirugía</div>
        </div>
      </td>

      <td>${rolesMini(p)}</td>

      <td class="mono">${clinicasMini(p)}</td>

      <td>${statusPill(p.estado)}</td>

      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn" type="button" title="Tarifario" aria-label="Tarifario">💲</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openModal('edit', p));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeCirugia(p.id));
    tr.querySelector('button[aria-label="Tarifario"]').addEventListener('click', ()=> openTarifario(p));

    tb.appendChild(tr);
  }
}

/* =========================
   Sidebar ready hook
========================= */
function onSidebarReady(){
  // El sidebar lo monta layout.js
  setActiveNav('cirugias');
  wireLogout();
}

/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;
    $('who').textContent = `Conectado: ${user.email}`;

    // si el sidebar ya está, marcamos nav y logout
    // y además nos suscribimos por si llega después
    window.addEventListener('sidebar:ready', onSidebarReady);
    onSidebarReady();

    // Toolbar
    $('btnCrear').addEventListener('click', ()=> openModal('create'));
    $('buscador').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // Modals
    $('btnModalClose').addEventListener('click', closeModal);
    $('btnModalCancelar').addEventListener('click', closeModal);
    $('btnModalGuardar').addEventListener('click', saveCirugia);

    $('modalBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalBackdrop')) closeModal();
    });

    $('btnTarClose').addEventListener('click', closeTarifario);
    $('btnTarCancelar').addEventListener('click', closeTarifario);
    $('btnTarGuardar').addEventListener('click', saveTarifario);

    $('tarBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('tarBackdrop')) closeTarifario();
    });

    // selector clínica -> pinta tabla habilitada
    $('selClinica').addEventListener('change', ()=>{
      const proc = state.all.find(x=>x.id===state.tarProcId);
      state.tarClinicaId = $('selClinica').value || '';
      if(!state.tarClinicaId){
        $('tarHint').textContent = 'Selecciona una clínica para editar precios.';
        paintTarTable(proc, null);
        return;
      }
      const cn = clinicaNameById(state.tarClinicaId);
      $('tarHint').textContent = `Editando clínica: ${cn} (${state.tarClinicaId})`;
      paintTarTable(proc, state.tarClinicaId);
    });

    // Load catalogs + data
    await loadRoles();
    await loadClinicas();
    await loadAll();
  }
});
