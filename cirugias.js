// cirugias.js — COMPLETO
// ✅ Procedimientos tipo "cirugía" en colección: procedimientos (docId = PC0001, PC0002...)
// ✅ Tarifas por (clínica, tipoPaciente) con descomposición: HMQ (roles), DP, INS
// ✅ Tabla muestra chips: "CLÍNICA · PACIENTE · $TOTAL" (o PENDIENTE si no hay nada)
// ✅ Buscador: coma=AND, guión=OR
// ✅ CSV: plantilla, export, import (filas por tarifa)
// ✅ Sidebar común via layout.js

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe, parseCSV, toCSV } from './utils.js';
import { loadSidebar } from './layout.js';

import {
  collection, getDocs, setDoc, deleteDoc,
  doc, getDoc, serverTimestamp,
  query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  q: '',
  rolesCatalog: [],           // [{id:'r_cirujano', nombre:'CIRUJANO'}]
  clinicasCatalog: [],        // [{id:'C001', nombre:'CLINICA ...'}]
  clinicasMap: new Map(),     // id -> nombre
  all: [],                    // procedimientos cirugías [{id, codigo, nombre, estado, rolesIds, tarifas:[]}]
  editProcId: null,
  activeTar: {
    procId: null,
    clinicaId: null,
    tipoPaciente: 'particular',
    // data
    hmqPorRol: {}, // {roleId: montoNumber}
    dp: 0,
    ins: 0
  }
};

const $ = (id)=> document.getElementById(id);

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
  // acepta "1.234.567", "$ 1.234", "1234567"
  const s = (v ?? '').toString().replace(/[^\d]/g,'');
  return Number(s || 0) || 0;
}
function clp(n){
  const x = Number(n || 0) || 0;
  // puntos miles
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

  // mientras escribe, dejamos solo dígitos (no forzamos $ para no pelear con el caret)
  el.addEventListener('input', ()=>{
    const digits = (el.value ?? '').toString().replace(/[^\d]/g,'');
    el.value = digits;
    onChange?.(Number(digits || 0));
  });
}

/* =========================
   Firestore refs
========================= */
const colProcedimientos = collection(db, 'procedimientos');
const colRoles = collection(db, 'roles');
const colClinicas = collection(db, 'clinicas');

/* =========================
   Load catalogs
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
    const id = cleanReminder(x.id) || d.id;
    if(!id) return;
    out.push({ id, nombre: toUpperSafe(nombre || id) });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.clinicasCatalog = out;
  state.clinicasMap = new Map(out.map(c=> [c.id, c.nombre]));
}

/* =========================
   Procedimientos loader (cirugías)
   - Guarda tarifas embebidas en memoria (no en el doc) para pintar tabla
========================= */
function normalizeProcDoc(id, x){
  return {
    id,
    codigo: cleanReminder(x.codigo) || id,
    tipo: cleanReminder(x.tipo) || 'cirugia',
    nombre: cleanReminder(x.nombre) || '',
    estado: (cleanReminder(x.estado) || 'activa').toLowerCase(),
    rolesIds: Array.isArray(x.rolesIds) ? x.rolesIds.filter(Boolean) : [],
    tarifas: [] // se llena después
  };
}

async function loadAll(){
  // Solo cirugías: tipo == 'cirugia'
  const qy = query(colProcedimientos, where('tipo','==','cirugia'));
  const snap = await getDocs(qy);

  const out = [];
  snap.forEach(d=>{
    out.push(normalizeProcDoc(d.id, d.data() || {}));
  });

  // Orden: activas primero, luego por código
  out.sort((a,b)=>{
    if(a.estado !== b.estado){
      if(a.estado === 'activa') return -1;
      if(b.estado === 'activa') return 1;
    }
    return normalize(a.codigo).localeCompare(normalize(b.codigo));
  });

  // Cargar tarifas por cada procedimiento
  // Estructura: procedimientos/{PC0001}/tarifas/{C001_particular}
  for(const p of out){
    const tarifasCol = collection(db, 'procedimientos', p.id, 'tarifas');
    const ts = await getDocs(tarifasCol);
    const arr = [];
    ts.forEach(td=>{
      const t = td.data() || {};
      const clinicaId = cleanReminder(t.clinicaId) || '';
      const tipoPaciente = (cleanReminder(t.tipoPaciente) || '').toLowerCase();
      const dp = Number(t.dp ?? 0) || 0;
      const ins = Number(t.ins ?? 0) || 0;
      const hmqPorRol = (t.hmqPorRol && typeof t.hmqPorRol === 'object') ? t.hmqPorRol : {};
      const hmq = sumHmq(hmqPorRol);
      const total = hmq + dp + ins;

      // consideramos "tarifa existente" si hay algún monto > 0
      const hasAny = (hmq > 0) || (dp > 0) || (ins > 0);

      arr.push({
        id: td.id,
        clinicaId,
        clinicaNombre: state.clinicasMap.get(clinicaId) || clinicaId || '(Sin clínica)',
        tipoPaciente: tipoPaciente || '',
        hmq, dp, ins, total,
        hasAny
      });
    });

    // ordenar tarifas: clínica, paciente
    arr.sort((a,b)=>{
      const ca = normalize(a.clinicaNombre);
      const cb = normalize(b.clinicaNombre);
      if(ca !== cb) return ca.localeCompare(cb);
      return normalize(a.tipoPaciente).localeCompare(normalize(b.tipoPaciente));
    });

    p.tarifas = arr;
  }

  state.all = out;
  paint();
}

function roleNameById(id){
  const r = state.rolesCatalog.find(x=>x.id===id);
  return r?.nombre || id || '';
}

/* =========================
   Search: coma=AND, guión=OR
   Ej:
   "manga, huingan - pc0001" => AND: ["manga","huingan - pc0001"]
   dentro de cada término AND, el "-" funciona como OR
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

  // searchable text (incluye tarifas por clínica/paciente)
  const rolesNames = (p.rolesIds || []).map(roleNameById);
  const tarifasText = (p.tarifas || [])
    .filter(t=>t.hasAny)
    .map(t=> `${t.clinicaNombre} ${t.tipoPaciente} ${t.total}`)
    .join(' ');

  const hay = normalize([
    p.codigo, p.nombre,
    p.estado, 'procedimiento cirugia',
    ...rolesNames,
    ...((p.rolesIds||[]).map(x=>x)),
    tarifasText,
    // nombres de clínicas aunque no tengan tarifa
    ...((p.tarifas||[]).map(t=>t.clinicaNombre)),
    ...((p.tarifas||[]).map(t=>t.clinicaId)),
    ...((p.tarifas||[]).map(t=>t.tipoPaciente))
  ].join(' '));

  // AND real: cada bloque AND debe cumplirse
  // Cada bloque AND puede ser OR por guión
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

function tarifaChips(p){
  const tarifas = (p.tarifas || []).filter(t=>t.hasAny && t.total > 0);

  if(!tarifas.length){
    return `<span class="pill">TARIFA: PENDIENTE</span>`;
  }

  // mostramos hasta 3 chips y luego "+N"
  const maxShow = 3;
  const shown = tarifas.slice(0, maxShow);
  const rest = tarifas.length - shown.length;

  const chips = shown.map(t=>{
    const clin = t.clinicaNombre || t.clinicaId || 'CLÍNICA';
    const pac = (t.tipoPaciente || '').toUpperCase();
    return `
      <span class="pill" title="${escapeHtml(clin)} · ${escapeHtml(pac)}">
        ${escapeHtml(clin)} · ${escapeHtml(pac)} · <b>${clp(t.total)}</b>
      </span>
    `;
  }).join(' ');

  const more = rest > 0
    ? ` <span class="pill">+${rest}</span>`
    : '';

  return `<div style="display:flex; flex-wrap:wrap; gap:8px;">${chips}${more}</div>`;
}

/* =========================
   Paint table
========================= */
function paint(){
  const rows = state.all.filter(p=> rowMatches(p, state.q));
  $('count').textContent = `${rows.length} cirugía${rows.length===1?'':'s'}`;

  const tb = $('tbody');
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
            <span class="muted">Procedimiento · Cirugía</span>
          </div>
        </div>
      </td>

      <td>${rolesBlock(p)}</td>

      <td>${tarifaChips(p)}</td>

      <td>${estadoBadge(p)}</td>

      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn" type="button" title="Tarifario" aria-label="Tarifario">💲</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openProcModal('edit', p));
    tr.querySelector('button[aria-label="Tarifario"]').addEventListener('click', ()=> openTarModal(p));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeProc(p.id));

    tb.appendChild(tr);
  }
}

/* =========================
   Modal: Crear/Editar procedimiento
========================= */
function paintProcRolesUI(selectedIds=[]){
  const wrap = $('procRolesWrap');
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
    $('modalProcTitle').textContent = 'Crear cirugía';
    $('modalProcSub').textContent = 'Define código, nombre y roles.';
    $('procCodigo').disabled = false;
    $('procCodigo').value = '';
    $('procNombre').value = '';
    $('procEstado').value = 'activa';
    paintProcRolesUI([]);
  }else{
    state.editProcId = p?.id || null;
    $('modalProcTitle').textContent = 'Editar cirugía';
    $('modalProcSub').textContent = state.editProcId ? `ID: ${state.editProcId}` : '';
    $('procCodigo').value = p?.codigo || p?.id || '';
    $('procCodigo').disabled = true; // docId fijo
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

  if(!codigo || !/^PC\d{4}$/i.test(codigo)){
    toast('Código inválido. Usa formato PC0001');
    $('procCodigo').focus();
    return;
  }
  if(!nombre){
    toast('Falta nombre de cirugía');
    $('procNombre').focus();
    return;
  }

  const id = state.editProcId || codigo;

  const payload = {
    id,
    codigo,
    tipo: 'cirugia',
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
  toast(state.editProcId ? 'Cirugía actualizada' : 'Cirugía creada');
  closeProcModal();
  await loadAll();
}

async function removeProc(id){
  const ok = confirm(`¿Eliminar cirugía?\n\n${id}`);
  if(!ok) return;

  // ⚠️ (simple) borra doc principal; no borra subcolecciones automáticamente
  await deleteDoc(doc(db,'procedimientos', id));
  toast('Eliminada');
  await loadAll();
}

/* =========================
   Tarifario modal
========================= */
function sumHmq(hmqPorRol){
  let s = 0;
  for(const k of Object.keys(hmqPorRol || {})){
    s += Number(hmqPorRol[k] || 0) || 0;
  }
  return s;
}

function tipoPacienteLabel(v){
  const x = (v || '').toLowerCase();
  if(x === 'particular') return 'PARTICULAR';
  if(x === 'isapre') return 'ISAPRE';
  if(x === 'fonasa') return 'FONASA';
  return (v || '').toUpperCase();
}

function tarifaDocId(clinicaId, tipoPaciente){
  return `${clinicaId}_${tipoPaciente}`; // ej: C001_particular
}

function paintClinicasSelect(){
  const sel = $('tarClinica');
  sel.innerHTML = '';

  if(!state.clinicasCatalog.length){
    sel.innerHTML = `<option value="">(Sin clínicas)</option>`;
    return;
  }

  sel.innerHTML = state.clinicasCatalog
    .map(c=> `<option value="${escapeHtml(c.id)}">${escapeHtml(c.nombre)}</option>`)
    .join('');
}

function paintTarRolesList(proc){
  const list = $('tarRolesList');
  list.innerHTML = '';

  const rolesIds = proc.rolesIds || [];
  if(!rolesIds.length){
    list.innerHTML = `<div class="muted">Esta cirugía no tiene roles definidos.</div>`;
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
  const hmq = sumHmq(state.activeTar.hmqPorRol);
  const dp = Number(state.activeTar.dp || 0) || 0;
  const ins = Number(state.activeTar.ins || 0) || 0;
  const total = hmq + dp + ins;

  $('hmqPill').textContent = `HMQ: ${clp(hmq)}`;

  $('tarTotales').innerHTML = `
    HMQ: <b>${clp(hmq)}</b><br/>
    DP: <b>${clp(dp)}</b><br/>
    INS: <b>${clp(ins)}</b><br/>
    TOTAL: <b>${clp(total)}</b>
  `;

  const clinId = state.activeTar.clinicaId || '';
  const clinName = state.clinicasMap.get(clinId) || clinId || '(Clínica)';
  const tp = tipoPacienteLabel(state.activeTar.tipoPaciente);

  $('tarResumen').textContent = `${clinName} · ${tp} · ${clp(total)}`;

  // Hint: “OK de clínica” significa que para esa clínica estén completos los 3 pacientes con HMQ+DP+INS (si tú quieres).
  // Pero como ahora quieres: NO “pendiente” si ya hay algo, el hint aclara.
  $('tarHint').textContent =
    `La tarifa se calcula como TOTAL = HMQ (roles) + DP + INS. ` +
    `Puedes guardar parcial si te falta info; igual quedará un total.`;
}

async function loadTarifarioIntoState(procId, clinicaId, tipoPaciente){
  state.activeTar.procId = procId;
  state.activeTar.clinicaId = clinicaId;
  state.activeTar.tipoPaciente = tipoPaciente;

  // defaults
  state.activeTar.hmqPorRol = {};
  state.activeTar.dp = 0;
  state.activeTar.ins = 0;

  // leer doc si existe
  const tid = tarifaDocId(clinicaId, tipoPaciente);
  const ref = doc(db, 'procedimientos', procId, 'tarifas', tid);
  const snap = await getDoc(ref);

  if(snap.exists()){
    const t = snap.data() || {};
    const hmqPorRol = (t.hmqPorRol && typeof t.hmqPorRol === 'object') ? t.hmqPorRol : {};
    state.activeTar.hmqPorRol = { ...hmqPorRol };
    state.activeTar.dp = Number(t.dp ?? 0) || 0;
    state.activeTar.ins = Number(t.ins ?? 0) || 0;
  }

  // pintar inputs
  // roles
  const roleInputs = $('tarRolesList').querySelectorAll('input[data-role-money]');
  roleInputs.forEach(inp=>{
    const rid = inp.getAttribute('data-role-money');
    const n = Number(state.activeTar.hmqPorRol[rid] || 0) || 0;
    inp.value = (n > 0) ? clp(n) : '';
  });

  // dp/ins
  $('tarDP').value = (state.activeTar.dp > 0) ? clp(state.activeTar.dp) : '';
  $('tarINS').value = (state.activeTar.ins > 0) ? clp(state.activeTar.ins) : '';

  computeTarTotals();
}

function openTarModal(proc){
  $('modalTarBackdrop').style.display = 'grid';
  $('modalTarTitle').textContent = 'Tarifario';
  $('modalTarSub').textContent = `${proc.codigo} · ${proc.nombre}`;

  paintClinicasSelect();
  paintTarRolesList(proc);

  // defaults: primera clínica
  const firstClin = state.clinicasCatalog?.[0]?.id || '';
  $('tarClinica').value = firstClin;
  $('tarPaciente').value = 'particular';

  state.activeTar.procId = proc.id;
  state.activeTar.clinicaId = firstClin;
  state.activeTar.tipoPaciente = 'particular';
  state.activeTar.hmqPorRol = {};
  state.activeTar.dp = 0;
  state.activeTar.ins = 0;

  // wire money inputs roles
  $('tarRolesList').querySelectorAll('input[data-role-money]').forEach(inp=>{
    const rid = inp.getAttribute('data-role-money');
    wireMoneyInput(inp, (n)=>{
      state.activeTar.hmqPorRol[rid] = Number(n || 0) || 0;
      computeTarTotals();
    });
  });

  wireMoneyInput($('tarDP'), (n)=>{
    state.activeTar.dp = Number(n || 0) || 0;
    computeTarTotals();
  });

  wireMoneyInput($('tarINS'), (n)=>{
    state.activeTar.ins = Number(n || 0) || 0;
    computeTarTotals();
  });

  // cambios de selects => cargar doc si existe
  $('tarClinica').onchange = async ()=>{
    state.activeTar.clinicaId = $('tarClinica').value || '';
    await loadTarifarioIntoState(proc.id, state.activeTar.clinicaId, state.activeTar.tipoPaciente);
  };
  $('tarPaciente').onchange = async ()=>{
    state.activeTar.tipoPaciente = $('tarPaciente').value || 'particular';
    await loadTarifarioIntoState(proc.id, state.activeTar.clinicaId, state.activeTar.tipoPaciente);
  };

  // cargar initial
  loadTarifarioIntoState(proc.id, state.activeTar.clinicaId, state.activeTar.tipoPaciente);
}

function closeTarModal(){
  $('modalTarBackdrop').style.display = 'none';
}

async function saveTarifario(){
  const procId = state.activeTar.procId;
  const clinicaId = state.activeTar.clinicaId;
  const tipoPaciente = state.activeTar.tipoPaciente;

  if(!procId){
    toast('Error: procId vacío');
    return;
  }
  if(!clinicaId){
    toast('Selecciona clínica');
    return;
  }

  // payload
  const hmqPorRol = {};
  for(const k of Object.keys(state.activeTar.hmqPorRol || {})){
    const n = Number(state.activeTar.hmqPorRol[k] || 0) || 0;
    if(n > 0) hmqPorRol[k] = n; // guardamos solo los >0 (limpio)
  }

  const dp = Number(state.activeTar.dp || 0) || 0;
  const ins = Number(state.activeTar.ins || 0) || 0;
  const hmq = sumHmq(hmqPorRol);
  const total = hmq + dp + ins;

  const tid = tarifaDocId(clinicaId, tipoPaciente);

  const payload = {
    id: tid,
    clinicaId,
    tipoPaciente,
    hmqPorRol,
    dp,
    ins,
    hmq,
    total,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  // si no existía antes, setea creadoEl/Por también
  const ref = doc(db, 'procedimientos', procId, 'tarifas', tid);
  const prev = await getDoc(ref);
  if(!prev.exists()){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  await setDoc(ref, payload, { merge:true });
  toast('Tarifario guardado');
  closeTarModal();
  await loadAll();
}

/* =========================
   CSV (tarifas)
   - export: una fila por (procedimiento, clínica, tipoPaciente)
   - incluye columnas: dp, ins, hmq_total, total y hmq_{roleId} por cada rol del catálogo
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
  // columnas base + hmq_roleId por roles conocidos
  const roleCols = state.rolesCatalog.map(r=> `hmq_${r.id}`);
  const headers = [
    'codigo','nombre','estado',
    'rolesIds',
    'clinicaId','tipoPaciente',
    'dp','ins',
    ...roleCols
  ];

  // ejemplo mínimo
  const example = {
    codigo: 'PC0001',
    nombre: 'Manga',
    estado: 'activa',
    rolesIds: 'r_cirujano|r_anestesista|r_arsenalera|r_asistente_cirujano|r_ayudante_1|r_ayudante_2',
    clinicaId: 'C001',
    tipoPaciente: 'particular',
    dp: '1760500',
    ins: '924630'
  };
  for(const rc of roleCols) example[rc] = ''; // vacío salvo algunos
  example['hmq_r_cirujano'] = '1050000';
  example['hmq_r_anestesista'] = '235000';
  example['hmq_r_arsenalera'] = '110000';
  example['hmq_r_asistente_cirujano'] = '509870';
  example['hmq_r_ayudante_1'] = '200000';
  example['hmq_r_ayudante_2'] = '200000';

  const csv = toCSV(headers, [example]);
  download('plantilla_tarifas_cirugias.csv', csv, 'text/csv');
  toast('Plantilla descargada');
}

function exportCSV(){
  const roleCols = state.rolesCatalog.map(r=> `hmq_${r.id}`);
  const headers = [
    'codigo','nombre','estado',
    'rolesIds',
    'clinicaId','clinicaNombre','tipoPaciente',
    'dp','ins','hmq_total','total',
    ...roleCols
  ];

  const items = [];

  for(const p of state.all){
    const tarifas = (p.tarifas || []).filter(t=>t.hasAny);

    if(!tarifas.length){
      // exporta una fila “sin tarifas” igual (útil para completar después)
      const row = {
        codigo: p.codigo,
        nombre: p.nombre,
        estado: p.estado,
        rolesIds: (p.rolesIds || []).join('|'),
        clinicaId: '',
        clinicaNombre: '',
        tipoPaciente: '',
        dp: '0',
        ins: '0',
        hmq_total: '0',
        total: '0'
      };
      for(const rc of roleCols) row[rc] = '0';
      items.push(row);
      continue;
    }

    for(const t of tarifas){
      // para export completo, leemos el doc real para obtener hmqPorRol
      // (porque en memoria solo teníamos totales)
      // simplificación: si quieres ultra-perf luego lo cacheamos
      // eslint-disable-next-line no-await-in-loop
      // NOTE: aquí sin await para no romper, pero no tenemos async.
    }
  }

  // Para no complicar con awaits, hacemos export “rápido” desde la subcolección real:
  // recorrer procedimientos y leer subcolección tarifas para construir filas completas.
  // => lo hacemos en función async.
  exportCSVAsync(headers, roleCols).catch(err=>{
    console.error(err);
    toast('Error exportando CSV (ver consola)');
  });
}

async function exportCSVAsync(headers, roleCols){
  const items = [];

  for(const p of state.all){
    const tarifasCol = collection(db, 'procedimientos', p.id, 'tarifas');
    const ts = await getDocs(tarifasCol);

    if(ts.empty){
      const row = {
        codigo: p.codigo,
        nombre: p.nombre,
        estado: p.estado,
        rolesIds: (p.rolesIds || []).join('|'),
        clinicaId: '',
        clinicaNombre: '',
        tipoPaciente: '',
        dp: '0',
        ins: '0',
        hmq_total: '0',
        total: '0'
      };
      for(const rc of roleCols) row[rc] = '0';
      items.push(row);
      continue;
    }

    ts.forEach(td=>{
      const t = td.data() || {};
      const clinicaId = cleanReminder(t.clinicaId) || '';
      const tipoPaciente = (cleanReminder(t.tipoPaciente) || '').toLowerCase();

      const hmqPorRol = (t.hmqPorRol && typeof t.hmqPorRol === 'object') ? t.hmqPorRol : {};
      const hmq_total = sumHmq(hmqPorRol);
      const dp = Number(t.dp ?? 0) || 0;
      const ins = Number(t.ins ?? 0) || 0;
      const total = hmq_total + dp + ins;

      const row = {
        codigo: p.codigo,
        nombre: p.nombre,
        estado: p.estado,
        rolesIds: (p.rolesIds || []).join('|'),
        clinicaId,
        clinicaNombre: state.clinicasMap.get(clinicaId) || '',
        tipoPaciente,
        dp: String(dp),
        ins: String(ins),
        hmq_total: String(hmq_total),
        total: String(total)
      };

      for(const r of state.rolesCatalog){
        const key = `hmq_${r.id}`;
        row[key] = String(Number(hmqPorRol[r.id] || 0) || 0);
      }

      items.push(row);
    });
  }

  const csv = toCSV(headers, items);
  download(`cirugias_tarifas_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
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
    clinicaId: idx('clinicaid'),
    tipoPaciente: idx('tipopaciente'),
    dp: idx('dp'),
    ins: idx('ins')
  };

  if(I.codigo < 0 || I.nombre < 0){
    toast('CSV debe incluir: codigo, nombre');
    return;
  }

  // role columns: hmq_r_xxx
  const roleCols = headers.filter(h=> h.startsWith('hmq_'));

  let upserts = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];

    const codigo = cleanReminder(row[I.codigo] ?? '').toUpperCase();
    const nombre = cleanReminder(row[I.nombre] ?? '');
    if(!codigo || !/^PC\d{4}$/i.test(codigo) || !nombre){
      skipped++; continue;
    }

    const estado = (cleanReminder(I.estado>=0 ? row[I.estado] : 'activa') || 'activa').toLowerCase();
    const rolesIds = uniq(
      (cleanReminder(I.rolesIds>=0 ? row[I.rolesIds] : '') || '')
        .split('|').map(x=>cleanReminder(x)).filter(Boolean)
    );

    // upsert procedimiento
    const procPayload = {
      id: codigo,
      codigo,
      tipo: 'cirugia',
      nombre,
      estado,
      rolesIds,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    };
    await setDoc(doc(db,'procedimientos', codigo), procPayload, { merge:true });

    // tarifa si viene
    const clinicaId = cleanReminder(I.clinicaId>=0 ? row[I.clinicaId] : '');
    const tipoPaciente = (cleanReminder(I.tipoPaciente>=0 ? row[I.tipoPaciente] : '') || '').toLowerCase();

    if(clinicaId && tipoPaciente){
      const dp = Number(cleanReminder(I.dp>=0 ? row[I.dp] : '0') || 0) || 0;
      const ins = Number(cleanReminder(I.ins>=0 ? row[I.ins] : '0') || 0) || 0;

      const hmqPorRol = {};
      for(const col of roleCols){
        const j = idx(col);
        if(j < 0) continue;
        const rid = col.replace(/^hmq_/,''); // r_cirujano
        const val = Number(cleanReminder(row[j] ?? '0') || 0) || 0;
        if(val > 0) hmqPorRol[rid] = val;
      }

      const hmq = sumHmq(hmqPorRol);
      const total = hmq + dp + ins;

      const tid = tarifaDocId(clinicaId, tipoPaciente);
      const tarPayload = {
        id: tid,
        clinicaId,
        tipoPaciente,
        hmqPorRol,
        dp,
        ins,
        hmq,
        total,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: state.user?.email || ''
      };

      await setDoc(doc(db,'procedimientos', codigo, 'tarifas', tid), tarPayload, { merge:true });
    }

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

    // Sidebar común
    await loadSidebar({ active: 'cirugias' });
    setActiveNav('cirugias');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    // Modales: cierre
    $('btnProcClose').addEventListener('click', closeProcModal);
    $('btnProcCancelar').addEventListener('click', closeProcModal);
    $('btnProcGuardar').addEventListener('click', saveProc);

    $('modalProcBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalProcBackdrop')) closeProcModal();
    });

    $('btnTarClose').addEventListener('click', closeTarModal);
    $('btnTarCancelar').addEventListener('click', closeTarModal);
    $('btnTarGuardar').addEventListener('click', saveTarifario);

    $('modalTarBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalTarBackdrop')) closeTarModal();
    });

    // botones toolbar
    $('btnCrear').addEventListener('click', ()=> openProcModal('create'));

    // buscador
    $('buscador').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // CSV
    $('btnDescargarPlantilla').addEventListener('click', plantillaCSV);
    $('btnExportar').addEventListener('click', exportCSV);

    $('btnImportar').addEventListener('click', ()=> $('fileCSV').click());
    $('fileCSV').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
      await importCSV(file);
    });

    // Load catalogs + data
    await loadRoles();
    await loadClinicas();

    // modal create/edit requiere roles ya cargados
    paintProcRolesUI([]);

    await loadAll();
  }
});

