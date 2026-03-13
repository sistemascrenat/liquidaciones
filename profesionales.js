// profesionales.js — COMPLETO
// ✅ Tabla estilo "Profesional/Empresa | RUT | Contacto" + Acciones (✏️ 🗑️)
// ✅ Modal para crear/editar
// ✅ Rol principal + roles secundarios (desde colección roles)
// ✅ Descuentos: muestra Sí/No en tabla (no monto/razón)
// ✅ Buscador con coma "," como AND (Y)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe } from './utils.js';
import { loadSidebar } from './layout.js';


import {
  collection, getDocs, getDoc, setDoc, deleteDoc,
  doc, serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  all: [],
  editRutId: null,
  q: '',                 // ahora guardamos el texto crudo, no normalizado
  rolesCatalog: [],      // [{id, nombre}]

  // ✅ BONOS
  bonosGlobal: null,     // { tramos: [{min, max, montoCLP}] }
  _bonosEditingRutId: null // profesional actualmente en “Administrar bonos”
};

const $ = (id)=> document.getElementById(id);

function normalize(s=''){
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .trim();
}

function uniq(arr){
  return [...new Set((arr || []).filter(Boolean))];
}

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

/* =========================
   Rut helpers
========================= */
function rutToId(rut){
  return (rut ?? '').toString().replace(/\D/g,'');
}

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colRoles = collection(db, 'roles');

// ✅ Tabla global de bonos (manda como default)
const docBonosConfig = doc(db, 'config', 'bonos');

/* =========================
   Modal
========================= */
function openModal(mode, p=null){
  $('modalBackdrop').style.display = 'grid';

  if(mode === 'create'){
    state.editRutId = null;
    $('modalTitle').textContent = 'Crear profesional';
    $('modalSub').textContent = 'Completa los datos y guarda.';
    clearForm();
  }else{
    state.editRutId = p?.rutId || null;
    $('modalTitle').textContent = 'Editar profesional';
    $('modalSub').textContent = state.editRutId ? `ID: ${state.editRutId}` : '';
    setForm(p);
  }

  applyTipoPersonaUI();
  applyBonoUI();
  $('nombreProfesional').focus();
}

function closeModal(){
  $('modalBackdrop').style.display = 'none';
}

function applyTipoPersonaUI(){
  const tipo = ($('tipoPersona').value || 'natural').toLowerCase();
  const isJ = (tipo === 'juridica');

  // solo jurídica
  const onlyJ = ['razonSocial','rutEmpresa','correoEmpresa','direccionEmpresa','ciudadEmpresa','telefonoEmpresa'];
  for(const id of onlyJ){
    const el = $(id);
    if(!el) continue;
    el.disabled = !isJ;
    el.style.opacity = isJ ? '1' : '.55';
    if(!isJ) el.value = '';
  }
}

function applyBonoUI(){
  const rolPrincipalId = cleanReminder($('rolPrincipal').value);
  const isCir = isCirujanoByRolPrincipalId(rolPrincipalId);

  const cb = $('tieneBono');
  const btn = $('btnBonos');

  if(!cb || !btn) return;

  if(isCir){
    cb.disabled = false;
    cb.style.opacity = '1';

    // ✅ default true SOLO si es cirujano y no existe valor explícito en edición
    // Si estamos creando, viene sin datos => true.
    // Si estamos editando y el doc no tiene campo => true (lo resolverá normalizeProfesionalDoc)
    if(state.editRutId === null){
      cb.checked = true;
    }

    btn.disabled = false;
    btn.style.display = 'inline-flex';
  }else{
    // ✅ Si deja de ser cirujano: se fuerza false (aunque quede guardado, se ignora)
    cb.checked = false;
    cb.disabled = true;
    cb.style.opacity = '.55';

    btn.disabled = true;
    btn.style.display = 'none';
  }
}

/* =========================
   Roles UI
========================= */
function paintRolesUI(){
  // rol principal select
  const sel = $('rolPrincipal');
  sel.innerHTML = '';

  if(!state.rolesCatalog.length){
    sel.innerHTML = `<option value="">(Sin roles)</option>`;
  }else{
    sel.innerHTML =
      `<option value="">Selecciona rol principal…</option>` +
      state.rolesCatalog.map(r=> `<option value="${escapeHtml(r.id)}">${escapeHtml(r.nombre)}</option>`).join('');
  }

  // roles secundarios checkboxes
  const wrap = $('rolesSecWrap');
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay roles creados. Ve a <b>Roles</b>.</div>`;
    return;
  }

  for(const r of state.rolesCatalog){
    const id = `rs_${r.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';
    label.innerHTML = `
      <input type="checkbox" id="${id}" data-role-id="${escapeHtml(r.id)}"/>
      <span class="pill">${escapeHtml(r.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function getRolesSecundariosIds(){
  const wrap = $('rolesSecWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-role-id'));
  });
  return uniq(out);
}

function setRolesSecundariosIds(ids){
  const wanted = new Set((ids || []).filter(Boolean));
  const wrap = $('rolesSecWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  checks.forEach(ch=>{
    const id = ch.getAttribute('data-role-id');
    ch.checked = wanted.has(id);
  });
}

function roleNameById(id){
  const r = state.rolesCatalog.find(x=>x.id===id);
  return r?.nombre || id || '';
}

/* =========================
   BONOS — reglas
========================= */

// ⚠️ Ajuste si tu rol ID real no es este.
// En tu plantilla usas r_cirujano, así que asumo ese.
const ROLE_CIRUJANO_ID = 'r_cirujano';

// Escala por defecto (la que pediste)
function bonosDefaultTramos(){
  return [
    { min: 11, max: 15, montoCLP: 1000000 },
    { min: 16, max: 20, montoCLP: 1500000 },
    { min: 21, max: 30, montoCLP: 3000000 },
    { min: 31, max: null, montoCLP: 6000000 } // null = sin tope
  ];
}

function isCirujanoByRolPrincipalId(rolPrincipalId){
  return (rolPrincipalId || '') === ROLE_CIRUJANO_ID;
}

// ✅ “manda”: Global como default, salvo override del profesional
function getBonosTramosEffective(p){
  return (p?.bonosTramosOverride?.length ? p.bonosTramosOverride :
    (state.bonosGlobal?.tramos?.length ? state.bonosGlobal.tramos : bonosDefaultTramos())
  );
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
    out.push({ id: d.id, nombre: toUpperSafe(nombre) });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;
  paintRolesUI();
}

async function loadBonosGlobal(){
  const snap = await getDoc(docBonosConfig);
  if(!snap.exists()){
    // si no existe config global, se asume la default (sin escribir aún)
    state.bonosGlobal = { tramos: bonosDefaultTramos() };
    return;
  }
  const x = snap.data() || {};
  const tramos = Array.isArray(x.tramos) ? x.tramos : [];
  state.bonosGlobal = { tramos: tramos.length ? tramos : bonosDefaultTramos() };
}

function normalizeProfesionalDoc(id, x){
  const tipoPersona = (cleanReminder(x.tipoPersona) || '').toLowerCase() || 'natural';
  const isJ = (tipoPersona === 'juridica');

  const rut = cleanReminder(x.rut);
  const rutId = cleanReminder(x.rutId) || id || rutToId(rut);

  const rolPrincipalId = cleanReminder(x.rolPrincipalId) || '';
  const isCir = isCirujanoByRolPrincipalId(rolPrincipalId);

  // ✅ “default si no existe valor explícito”
  // - si es cirujano y no existe el campo: true
  // - si NO es cirujano: false (forzado/ignorado aunque exista guardado)
  const rawTieneBono = (x.tieneBono === undefined || x.tieneBono === null)
    ? (isCir ? true : false)
    : !!x.tieneBono;

  const tieneBono = isCir ? rawTieneBono : false;

  return {
    rutId,
    tipoPersona,
    estado: (cleanReminder(x.estado) || 'activo').toLowerCase(),

    // persona/empresa
    nombreProfesional: toUpperSafe(cleanReminder(x.nombreProfesional) || ''),
    razonSocial: isJ ? toUpperSafe(cleanReminder(x.razonSocial) || '') : '',

    // RUTs
    rut: rut || '',
    rutEmpresa: isJ ? (cleanReminder(x.rutEmpresa) || '') : '',

    // contacto
    telefono: cleanReminder(x.telefono) || '',
    telefonoEmpresa: isJ ? (cleanReminder(x.telefonoEmpresa) || '') : '',
    correoPersonal: cleanReminder(x.correoPersonal) || '',
    correoEmpresa: isJ ? (cleanReminder(x.correoEmpresa) || '') : '',

    // otros
    direccion: cleanReminder(x.direccion) || '',
    direccionEmpresa: isJ ? (cleanReminder(x.direccionEmpresa) || '') : '',
    ciudadEmpresa: isJ ? (cleanReminder(x.ciudadEmpresa) || '') : '',
    giro: cleanReminder(x.giro) || '',

    // roles
    rolPrincipalId,
    rolesSecundariosIds: Array.isArray(x.rolesSecundariosIds) ? x.rolesSecundariosIds.filter(Boolean) : [],

    // descuentos
    tieneDescuento: !!x.tieneDescuento,
    descuentoUF: Number(x.descuentoUF ?? 0) || 0,
    descuentoRazon: (x.descuentoRazon ?? '') ? cleanReminder(x.descuentoRazon) : '',

    // ✅ BONOS
    tieneBono,
    bonosTramosOverride: Array.isArray(x.bonosTramosOverride) ? x.bonosTramosOverride : []
  };

}

async function loadAll(){
  const snap = await getDocs(colProfesionales);
  const out = [];
  snap.forEach(d=>{
    out.push(normalizeProfesionalDoc(d.id, d.data() || {}));
  });

  // Orden: si es jurídica, por razón social; si es natural, por nombre profesional
  out.sort((a, b) => {
    // 1️⃣ Estado: activos primero
    if (a.estado !== b.estado) {
      if (a.estado === 'activo') return -1;
      if (b.estado === 'activo') return 1;
    }
  
    // 2️⃣ Orden A→Z por NOMBRE DEL PROFESIONAL (siempre)
    // (aunque sea persona jurídica, ordena por el contacto/profesional)
    const nameA = normalize(a.nombreProfesional || '');
    const nameB = normalize(b.nombreProfesional || '');
    
    const cmp = nameA.localeCompare(nameB);
    if(cmp !== 0) return cmp;
    
    // 3️⃣ Desempate: por razón social (si existe), para orden consistente
    const rsA = normalize(a.razonSocial || '');
    const rsB = normalize(b.razonSocial || '');
    return rsA.localeCompare(rsB);

  });


  state.all = out;
  paint();
}

/* =========================
   Search
   - coma "," = AND (Y)
   - guion "-" = OR (O) dentro de cada grupo AND
   Ej:
     "rodrigo-ignacio"            => rodrigo OR ignacio
     "cirujano, activo"           => cirujano AND activo
     "cirujano, activo-inactivo"  => cirujano AND (activo OR inactivo)
========================= */

// helpers para búsquedas más robustas (rut/teléfono/roles)
function digitsOnly(s=''){ return (s ?? '').toString().replace(/\D/g,''); }
function normRoleId(s=''){
  // r_cirujano => "r cirujano" (y también soporta r-cirujano)
  return normalize((s ?? '').toString().replace(/[_-]+/g,' '));
}

/**
 * Devuelve grupos AND, donde cada grupo contiene términos OR.
 * - Se separa por coma para AND
 * - Dentro de cada parte, se separa por "-" para OR
 *
 * raw: "a-b, c, d-e-f"
 * => [ ["a","b"], ["c"], ["d","e","f"] ]
 */
function parseQuery(raw){
  const text = (raw || '').toString();

  // AND groups
  const andParts = text
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  // dentro de cada AND group: OR terms
  const groups = andParts.map(part =>
    part
      .split('-')
      .map(s => normalize(s))
      .filter(Boolean)
  );

  // elimina grupos vacíos por si hay cosas raras tipo ", ,"
  return groups.filter(g => g.length);
}

function rowMatches(p, rawQuery){
  const groups = parseQuery(rawQuery);
  if(!groups.length) return true;

  const showMain = (p.tipoPersona === 'juridica' && p.razonSocial) ? p.razonSocial : p.nombreProfesional;

  // roles: nombre (catálogo) + id (por si el catálogo no calza)
  const priName = roleNameById(p.rolPrincipalId);
  const secNames = (p.rolesSecundariosIds || []).map(roleNameById);

  const priId = p.rolPrincipalId || '';
  const secIds = (p.rolesSecundariosIds || []);

  // rut: con formato + solo dígitos
  const rutDigits = digitsOnly(p.rut);
  const rutEmpDigits = digitsOnly(p.rutEmpresa);

  // "hay" es todo el texto donde buscamos
  const hay = normalize([
    // nombre visible + (si jurídica) nombre del contacto
    showMain,
    (p.tipoPersona === 'juridica' ? p.nombreProfesional : ''),

    // rut / rut empresa (con y sin formato)
    p.rut, p.rutEmpresa,
    rutDigits, rutEmpDigits,
    p.rutId,

    // correos / teléfonos (normal y solo dígitos)
    p.correoPersonal, p.correoEmpresa,
    p.telefono, p.telefonoEmpresa,
    digitsOnly(p.telefono), digitsOnly(p.telefonoEmpresa),

    // roles por nombre
    priName,
    ...secNames,

    // roles por id (y “normalizados”)
    priId, ...secIds,
    normRoleId(priId),
    ...secIds.map(normRoleId),

    // flags
    (p.tieneDescuento ? 'descuento si true' : 'descuento no false'),
    p.estado, p.tipoPersona
  ].join(' '));

  // Lógica:
  // - AND: todos los grupos deben cumplirse
  // - un grupo se cumple si alguno de sus términos OR aparece
  return groups.every(orTerms => orTerms.some(t => hay.includes(t)));
}


/* =========================
   Form set/clear
========================= */
function clearForm(){
  $('tipoPersona').value = 'natural';
  $('estado').value = 'activo';

  $('nombreProfesional').value = '';
  $('razonSocial').value = '';

  $('rut').value = '';
  $('rutEmpresa').value = '';

  $('correoPersonal').value = '';
  $('correoEmpresa').value = '';

  $('telefono').value = '';
  $('telefonoEmpresa').value = '';

  $('direccion').value = '';
  $('direccionEmpresa').value = '';
  $('ciudadEmpresa').value = '';
  $('giro').value = '';

  $('rolPrincipal').value = '';
  setRolesSecundariosIds([]);

  $('tieneDescuento').checked = false;
  $('descuentoUF').value = '0';
  $('descuentoRazon').value = '';

  // ✅ BONOS
  if($('tieneBono')) $('tieneBono').checked = false; // luego applyBonoUI lo deja true si cirujano

}

function setForm(p){
  $('tipoPersona').value = (p.tipoPersona || 'natural');
  $('estado').value = (p.estado || 'activo');

  $('nombreProfesional').value = p.nombreProfesional || '';
  $('razonSocial').value = p.razonSocial || '';

  $('rut').value = p.rut || '';
  $('rutEmpresa').value = p.rutEmpresa || '';

  $('correoPersonal').value = p.correoPersonal || '';
  $('correoEmpresa').value = p.correoEmpresa || '';

  $('telefono').value = p.telefono || '';
  $('telefonoEmpresa').value = p.telefonoEmpresa || '';

  $('direccion').value = p.direccion || '';
  $('direccionEmpresa').value = p.direccionEmpresa || '';
  $('ciudadEmpresa').value = p.ciudadEmpresa || '';
  $('giro').value = p.giro || '';

  $('rolPrincipal').value = p.rolPrincipalId || '';
  setRolesSecundariosIds(p.rolesSecundariosIds || []);

  $('tieneDescuento').checked = !!p.tieneDescuento;
  $('descuentoUF').value = String(Number(p.descuentoUF ?? 0) || 0);
  $('descuentoRazon').value = p.descuentoRazon || '';

  // ✅ BONOS (blindado)
  const elTieneBono = $('tieneBono');
  if(elTieneBono){
    elTieneBono.checked = !!p.tieneBono;
  }
  
  applyTipoPersonaUI();
  applyBonoUI();
}


/* =========================
   Save / Delete
========================= */
async function saveProfesional(){
  const tipoPersona = ($('tipoPersona').value || 'natural').toLowerCase();
  const isJ = (tipoPersona === 'juridica');

  const nombreProfesional = toUpperSafe(cleanReminder($('nombreProfesional').value));
  const razonSocial = toUpperSafe(cleanReminder($('razonSocial').value));

  const rut = cleanReminder($('rut').value);
  const rutEmpresa = cleanReminder($('rutEmpresa').value);

  const correoPersonal = cleanReminder($('correoPersonal').value);
  const correoEmpresa = cleanReminder($('correoEmpresa').value);

  const telefono = cleanReminder($('telefono').value);
  const telefonoEmpresa = cleanReminder($('telefonoEmpresa').value);

  const direccion = cleanReminder($('direccion').value);
  const direccionEmpresa = cleanReminder($('direccionEmpresa').value);
  const ciudadEmpresa = cleanReminder($('ciudadEmpresa').value);

  const giro = cleanReminder($('giro').value);
  const estado = (cleanReminder($('estado').value) || 'activo').toLowerCase();

  const rolPrincipalId = cleanReminder($('rolPrincipal').value);
  const rolesSecundariosIds = getRolesSecundariosIds();

  const tieneDescuento = !!$('tieneDescuento').checked;
  const descuentoUF = Number($('descuentoUF').value ?? 0) || 0;
  const descuentoRazon = cleanReminder($('descuentoRazon').value);

  // ✅ BONOS (lo que te faltaba)
  const tieneBonoUI = !!($('tieneBono')?.checked);
  const isCir = isCirujanoByRolPrincipalId(rolPrincipalId);

  // Validaciones base
  if(!rut){
    toast('Falta RUT');
    $('rut').focus();
    return;
  }
  const rutId = rutToId(rut);
  if(!rutId){
    toast('RUT inválido (no pude generar rutId)');
    $('rut').focus();
    return;
  }
  if(!nombreProfesional){
    toast('Falta nombre profesional');
    $('nombreProfesional').focus();
    return;
  }
  if(isJ && !razonSocial){
    toast('Falta razón social (jurídica)');
    $('razonSocial').focus();
    return;
  }
  if(state.rolesCatalog.length && !rolPrincipalId){
    toast('Selecciona un rol principal');
    $('rolPrincipal').focus();
    return;
  }

  const payload = {
    tipoPersona,
    estado,

    rut,
    rutId,

    nombreProfesional,
    razonSocial: isJ ? (razonSocial || null) : null,

    rutEmpresa: isJ ? (rutEmpresa || null) : null,

    correoPersonal: correoPersonal || null,
    correoEmpresa: isJ ? (correoEmpresa || null) : null,

    telefono: telefono || null,
    telefonoEmpresa: isJ ? (telefonoEmpresa || null) : null,

    direccion: direccion || null,
    direccionEmpresa: isJ ? (direccionEmpresa || null) : null,
    ciudadEmpresa: isJ ? (ciudadEmpresa || null) : null,

    giro: giro || null,

    rolPrincipalId: rolPrincipalId || null,
    rolesSecundariosIds: uniq(rolesSecundariosIds),

    // ✅ DESCUENTO
    tieneDescuento,
    descuentoUF,
    descuentoRazon: descuentoRazon || null,

    // ✅ BONO (regla: solo cirujano)
    // - si es cirujano: respeta checkbox
    // - si NO es cirujano: fuerza false (se ignora aunque exista guardado)
    tieneBono: isCir ? tieneBonoUI : false,

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  // ✅ Si deja de ser cirujano, limpiamos override de bonos (para “ignorar”)
  if(!isCir){
    payload.bonosTramosOverride = [];
  }

  const isEdit = !!state.editRutId;
  if(!isEdit){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  await setDoc(doc(db,'profesionales',rutId), payload, { merge:true });

  toast(isEdit ? 'Profesional actualizado' : 'Profesional creado');
  closeModal();
  await loadAll();
}

async function removeProfesional(rutId){
  const p = state.all.find(x=>x.rutId===rutId);
  const main = (p?.tipoPersona === 'juridica' && p?.razonSocial) ? p.razonSocial : (p?.nombreProfesional || '');
  const ok = confirm(`¿Eliminar?\n\n${main}\n${p?.rut || ''}`);
  if(!ok) return;
  await deleteDoc(doc(db,'profesionales',rutId));
  toast('Eliminado');
  await loadAll();
}

/* =========================
   Paint table
========================= */
function labelTipoEstado(p){
  const t = (p.tipoPersona || '').toString().toLowerCase();
  const tipo = (t === 'juridica')
    ? 'PERSONA JURÍDICA'
    : 'PERSONA NATURAL';

  const est = (p.estado || 'activo').toString().toLowerCase(); // activo / inactivo
  return `
    ${tipo} · <span class="state ${est}">${est.toUpperCase()}</span>
  `;
}


function rolesMini(p){
  const pri = p.rolPrincipalId ? roleNameById(p.rolPrincipalId) : '';
  const secs = (p.rolesSecundariosIds || [])
    .filter(Boolean)
    .map(roleNameById)
    .filter(Boolean);

  if(!pri && !secs.length) return `<span class="muted">—</span>`;

  const priHtml = pri ? `<b>${escapeHtml(pri)}</b>` : `<span class="muted">—</span>`;
  const secsHtml = secs.length
    ? secs.map(x=> `<b>${escapeHtml(x)}</b>`).join(' · ')
    : `<span class="muted">—</span>`;

  return `
    <span class="mini">
      <span class="muted">ROL PRINCIPAL:</span> ${priHtml}
      <span class="dot">·</span>
      <span class="muted">ROLES SECUNDARIOS:</span> ${secsHtml}
    </span>
  `;
}

function descuentoMini(p){
  return p.tieneDescuento
    ? `<span class="pill">DESCUENTO: SÍ</span>`
    : `<span class="muted">DESCUENTO: NO</span>`;
}

function contactoBlock(p){
  const rows = [];

  if(p.telefono) rows.push({ ico:'📞', val: p.telefono });
  if(p.tipoPersona === 'juridica' && p.telefonoEmpresa) rows.push({ ico:'📞', val: p.telefonoEmpresa });

  if(p.correoPersonal) rows.push({ ico:'✉️', val: p.correoPersonal });
  if(p.tipoPersona === 'juridica' && p.correoEmpresa) rows.push({ ico:'🏢', val: p.correoEmpresa });

  if(!rows.length) return `<div class="mini muted">—</div>`;

  return rows.map(r=>`
    <div class="kRow">
      <span class="kIco">${r.ico}</span>
      <span class="kVal">${escapeHtml(r.val)}</span>
    </div>
  `).join('');
}

function rutBlock(p){
  const a = [];
  a.push(`<div class="mono"><b>${escapeHtml(p.rut || '')}</b></div>`);
  if(p.tipoPersona === 'juridica' && p.rutEmpresa){
    a.push(`<div class="mini mono">Rut Empresa: <b>${escapeHtml(p.rutEmpresa)}</b></div>`);
  }
  return a.join('');
}

function profEmpresaBlock(p){
  const isJ = (p.tipoPersona === 'juridica');
  const title = (isJ && p.razonSocial) ? p.razonSocial : (p.nombreProfesional || '—');

  const subLines = [];
  if(isJ && p.nombreProfesional){
    subLines.push(`<span>PROFESIONAL: <b>${escapeHtml(p.nombreProfesional)}</b></span>`);
  }
  subLines.push(`<span>${labelTipoEstado(p)}</span>`);

  return `
    <div class="cellBlock">
      <div class="cellTitle">${escapeHtml(title)}</div>
      <div class="cellSub">
        ${subLines.join('<span class="dot">·</span>')}
      </div>
      <div class="mini" style="margin-top:6px;">
        ${rolesMini(p)}
      </div>
      <div class="mini" style="margin-top:6px;">
        ${descuentoMini(p)}
      </div>
    </div>
  `;
}

function paint(){
  const rows = state.all.filter(p=> rowMatches(p, state.q));

  $('count').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');

    tr.innerHTML = `
      <td>${profEmpresaBlock(p)}</td>
      <td>${rutBlock(p)}</td>
      <td>${contactoBlock(p)}</td>
      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openModal('edit', p));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeProfesional(p.rutId));

    tb.appendChild(tr);
  }
}

/* =========================
   XLSX
========================= */
function exportXLSX(){
  const items = state.all.map(p => ({
    tipoPersona: p.tipoPersona || 'natural',
    estado: p.estado || 'activo',
    nombreProfesional: p.nombreProfesional || '',
    razonSocial: p.razonSocial || '',
    rut: p.rut || '',
    rutEmpresa: p.rutEmpresa || '',
    correoPersonal: p.correoPersonal || '',
    correoEmpresa: p.correoEmpresa || '',
    telefono: p.telefono || '',
    telefonoEmpresa: p.telefonoEmpresa || '',
    rolPrincipalId: p.rolPrincipalId || '',
    rolesSecundariosIds: (p.rolesSecundariosIds || []).join('|'),
    tieneDescuento: p.tieneDescuento ? 'true' : 'false',
    descuentoUF: Number(p.descuentoUF ?? 0) || 0,
    descuentoRazon: p.descuentoRazon || '',
    tieneBono: p.tieneBono ? 'true' : 'false'
  }));

  const ws = XLSX.utils.json_to_sheet(items, {
    header: [
      'tipoPersona','estado',
      'nombreProfesional','razonSocial',
      'rut','rutEmpresa',
      'correoPersonal','correoEmpresa',
      'telefono','telefonoEmpresa',
      'rolPrincipalId','rolesSecundariosIds',
      'tieneDescuento','descuentoUF','descuentoRazon',
      'tieneBono'
    ]
  });

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Profesionales');
  XLSX.writeFile(wb, `profesionales_${new Date().toISOString().slice(0,10)}.xlsx`);

  toast('XLSX exportado');
}

function plantillaXLSX(){
  const rows = [
    {
      tipoPersona: 'natural',
      estado: 'activo',
      nombreProfesional: 'Juan Pérez',
      razonSocial: '',
      rut: '14.123.456-1',
      rutEmpresa: '',
      correoPersonal: 'juanperez@gmail.com',
      correoEmpresa: '',
      telefono: '+56988775599',
      telefonoEmpresa: '',
      rolPrincipalId: 'r_cirujano',
      rolesSecundariosIds: 'r_asistente_cirujano|r_cirujano',
      tieneDescuento: 'false',
      descuentoUF: 0,
      descuentoRazon: '',
      tieneBono: 'true'
    },
    {
      tipoPersona: 'juridica',
      estado: 'activo',
      nombreProfesional: 'Andrea González',
      razonSocial: 'González SPA',
      rut: '17.321.765-4',
      rutEmpresa: '77.998.233-1',
      correoPersonal: 'andrea@correo.com',
      correoEmpresa: 'gonzalezspa@empresa.cl',
      telefono: '+56988997755',
      telefonoEmpresa: '+56222223333',
      rolPrincipalId: 'r_cirujano',
      rolesSecundariosIds: 'r_asistente_cirujano',
      tieneDescuento: 'false',
      descuentoUF: 0,
      descuentoRazon: '',
      tieneBono: 'true'
    }
  ];

  const ws = XLSX.utils.json_to_sheet(rows, {
    header: [
      'tipoPersona','estado',
      'nombreProfesional','razonSocial',
      'rut','rutEmpresa',
      'correoPersonal','correoEmpresa',
      'telefono','telefonoEmpresa',
      'rolPrincipalId','rolesSecundariosIds',
      'tieneDescuento','descuentoUF','descuentoRazon',
      'tieneBono'
    ]
  });

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Plantilla');
  XLSX.writeFile(wb, 'plantilla_profesionales.xlsx');

  toast('Plantilla XLSX descargada');
}

function boolFromCell(v, fallback=false){
  if(v === undefined || v === null || v === '') return fallback;
  const s = String(v).trim().toLowerCase();
  return ['true','1','si','sí','yes','x'].includes(s);
}

async function importXLSX(file){
  const ab = await file.arrayBuffer();
  const wb = XLSX.read(ab, { type: 'array' });

  const firstSheetName = wb.SheetNames?.[0];
  if(!firstSheetName){
    toast('Archivo XLSX vacío o inválido');
    return;
  }

  const ws = wb.Sheets[firstSheetName];

  // defval:'' => evita undefined y hace más estable la importación
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });

  if(!rows.length){
    toast('XLSX vacío o inválido');
    return;
  }

  let upserts = 0;
  let skipped = 0;

  for(const raw of rows){
    // normaliza nombres de columnas por si vienen con mayúsculas/minúsculas distintas
    const row = {};
    Object.keys(raw).forEach(k => {
      row[(cleanReminder(k) || '').toLowerCase()] = raw[k];
    });

    const rut = cleanReminder(row.rut ?? '');
    const rutId = rutToId(rut);
    const nombreProfesional = cleanReminder(row.nombreprofesional ?? '');

    if(!rutId || !nombreProfesional){
      skipped++;
      continue;
    }

    const tipoPersona = (cleanReminder(row.tipopersona ?? 'natural') || 'natural').toLowerCase();
    const isJ = (tipoPersona === 'juridica');

    const rolPrincipalId = cleanReminder(row.rolprincipalid ?? '') || '';
    const isCir = isCirujanoByRolPrincipalId(rolPrincipalId);

    const tieneBonoRaw = cleanReminder(row.tienebono ?? '');

    const payload = {
      tipoPersona,
      estado: (cleanReminder(row.estado ?? 'activo') || 'activo').toLowerCase(),

      rut,
      rutId,

      nombreProfesional,

      razonSocial: isJ ? (cleanReminder(row.razonsocial ?? '') || null) : null,
      rutEmpresa: isJ ? (cleanReminder(row.rutempresa ?? '') || null) : null,

      correoPersonal: cleanReminder(row.correopersonal ?? '') || null,
      correoEmpresa: isJ ? (cleanReminder(row.correoempresa ?? '') || null) : null,

      telefono: cleanReminder(row.telefono ?? '') || null,
      telefonoEmpresa: isJ ? (cleanReminder(row.telefonoempresa ?? '') || null) : null,

      rolPrincipalId: rolPrincipalId || null,

      rolesSecundariosIds: uniq(
        (cleanReminder(row.rolessecundariosids ?? '') || '')
          .split('|')
          .map(x => cleanReminder(x))
          .filter(Boolean)
      ),

      tieneDescuento: boolFromCell(row.tienedescuento, false),
      descuentoUF: Number(row.descuentouf ?? 0) || 0,
      descuentoRazon: cleanReminder(row.descuentorazon ?? '') || null,

      // bono: solo si es cirujano; si viene vacío y es cirujano => true
      tieneBono: (() => {
        if(!isCir) return false;
        if(!tieneBonoRaw) return true;
        return boolFromCell(tieneBonoRaw, true);
      })(),

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    };

    // Si NO es cirujano, limpia override
    if(!isCir){
      payload.bonosTramosOverride = [];
    }

    await setDoc(doc(db, 'profesionales', rutId), payload, { merge:true });
    upserts++;
  }

  toast(`Import listo: ${upserts} guardados / ${skipped} omitidos`);
  await loadAll();
}

/* =========================
   BONOS — Modal administrar
========================= */

// Crea el modal una sola vez (DOM)
function ensureBonosModalDOM(){
  if(document.getElementById('bonosBackdrop')) return;

  const div = document.createElement('div');
  div.id = 'bonosBackdrop';
  div.className = 'modalBackdrop';
  div.style.display = 'none';
  div.innerHTML = `
    <div class="modalCard" style="max-width:760px;">
      <div class="modalHead">
        <div>
          <div class="modalTitle">Administrar bonos</div>
          <div class="modalSub" id="bonosSub">Escala por tramo de cirugías</div>
        </div>
        <button class="iconBtn" type="button" id="btnBonosClose" title="Cerrar">✖️</button>
      </div>

      <div class="modalBody">
        <div class="mini muted" style="margin-bottom:10px;">
          Regla: aplica solo a <b>Médico Cirujano</b>. Global manda como default, salvo override del profesional.
        </div>

        <table class="table" style="width:100%;">
          <thead>
            <tr>
              <th>Desde</th>
              <th>Hasta</th>
              <th>Monto (CLP)</th>
            </tr>
          </thead>
          <tbody id="bonosTbody"></tbody>
        </table>

        <div class="mini muted" style="margin-top:10px;">
          Nota: “Hasta” vacío significa “sin tope” (31+).
        </div>
      </div>

      <div class="modalFoot">
        <button class="btn" type="button" id="btnBonosCancelar">Cancelar</button>
        <div style="flex:1;"></div>
        <button class="btn" type="button" id="btnBonosGuardar">Guardar</button>
        <button class="btn primary" type="button" id="btnBonosGuardarTodos">Guardar para todos</button>
      </div>
    </div>
  `;

  document.body.appendChild(div);

  // wire close
  const close = ()=> closeBonosModal();
  document.getElementById('btnBonosClose').addEventListener('click', close);
  document.getElementById('btnBonosCancelar').addEventListener('click', close);
  div.addEventListener('click', (e)=>{ if(e.target === div) close(); });

  document.getElementById('btnBonosGuardar').addEventListener('click', saveBonosOnlyThis);
  document.getElementById('btnBonosGuardarTodos').addEventListener('click', saveBonosForAll);
}

function paintBonosTable(tramos){
  const tb = document.getElementById('bonosTbody');
  tb.innerHTML = '';

  (tramos || []).forEach((t, i)=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><input class="inp" data-k="min" data-i="${i}" value="${Number(t.min ?? 0) || 0}"></td>
      <td><input class="inp" data-k="max" data-i="${i}" value="${t.max===null || t.max===undefined ? '' : (Number(t.max)||'')}"></td>
      <td><input class="inp" data-k="montoCLP" data-i="${i}" value="${Number(t.montoCLP ?? 0) || 0}"></td>
    `;
    tb.appendChild(tr);
  });
}

function readBonosTable(){
  const tb = document.getElementById('bonosTbody');
  const inputs = tb.querySelectorAll('input[data-k][data-i]');
  const map = new Map(); // i -> obj

  inputs.forEach(inp=>{
    const i = Number(inp.getAttribute('data-i'));
    const k = inp.getAttribute('data-k');
    if(!map.has(i)) map.set(i, {});
    map.get(i)[k] = inp.value;
  });

  const tramos = [];
  [...map.keys()].sort((a,b)=>a-b).forEach(i=>{
    const o = map.get(i) || {};
    const min = Number(o.min ?? 0) || 0;
    const maxRaw = (o.max ?? '').toString().trim();
    const max = maxRaw ? (Number(maxRaw) || null) : null;
    const montoCLP = Number(o.montoCLP ?? 0) || 0;

    if(min <= 0 || montoCLP <= 0) return; // omitimos filas inválidas
    tramos.push({ min, max, montoCLP });
  });

  // Orden por min asc
  tramos.sort((a,b)=> (a.min||0) - (b.min||0));
  return tramos.length ? tramos : bonosDefaultTramos();
}

function openBonosModalForProfesional(p){
  ensureBonosModalDOM();

  // Solo cirujano
  if(!isCirujanoByRolPrincipalId(p?.rolPrincipalId)){
    toast('Bono solo aplica a Médico Cirujano');
    return;
  }

  state._bonosEditingRutId = p.rutId;

  const main = (p?.tipoPersona === 'juridica' && p?.razonSocial) ? p.razonSocial : (p?.nombreProfesional || '');
  document.getElementById('bonosSub').textContent = `Profesional: ${main} · ${p?.rut || ''}`;

  // Cargar tramos efectivos (override si existe; si no, global; si no, default)
  const tramos = getBonosTramosEffective(p);
  paintBonosTable(tramos);

  document.getElementById('bonosBackdrop').style.display = 'grid';
}

function closeBonosModal(){
  const el = document.getElementById('bonosBackdrop');
  if(el) el.style.display = 'none';
  state._bonosEditingRutId = null;
}

async function saveBonosOnlyThis(){
  const rutId = state._bonosEditingRutId;
  if(!rutId) return;

  const p = state.all.find(x=>x.rutId===rutId);
  if(!p) return;

  // si deja de ser cirujano, no permite
  if(!isCirujanoByRolPrincipalId(p.rolPrincipalId)){
    toast('No aplica: no es Médico Cirujano');
    closeBonosModal();
    return;
  }

  const tramos = readBonosTable();

  await setDoc(doc(db,'profesionales',rutId), {
    bonosTramosOverride: tramos,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  toast('Bonos guardados para este profesional');
  closeBonosModal();
  await loadAll();
}

async function saveBonosForAll(){
  const tramos = readBonosTable();

  await setDoc(docBonosConfig, {
    tramos,
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  }, { merge:true });

  // actualiza cache local
  state.bonosGlobal = { tramos };

  toast('Bonos guardados para TODOS (global)');
  closeBonosModal();
  await loadAll();
}


/* =========================
   Boot
========================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;
    $('who').textContent = `Conectado: ${user.email}`;

    loadSidebar('profesionales');
    setActiveNav('profesionales');
    
    wireLogout();

    // Modal
    $('btnCrear').addEventListener('click', ()=> openModal('create'));
    $('btnModalClose').addEventListener('click', closeModal);
    $('btnModalCancelar').addEventListener('click', closeModal);
    $('btnModalGuardar').addEventListener('click', saveProfesional);

    $('modalBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalBackdrop')) closeModal();
    });

    $('tipoPersona').addEventListener('change', ()=>{
      applyTipoPersonaUI();
      applyBonoUI();
    });

    // ✅ Bono UI reacciona al cambio de rol principal
    $('rolPrincipal').addEventListener('change', ()=>{
      applyBonoUI();
    });

    // ✅ Abrir modal administrar bonos
    const btnBonos = $('btnBonos');
    if(btnBonos){
      btnBonos.addEventListener('click', ()=>{
        const rutId = state.editRutId;
        if(!rutId){
          toast('Guarda el profesional primero para administrar bonos');
          return;
        }
    
        // ✅ Usa el rol ACTUAL del formulario (no el cache viejo)
        const rolPrincipalIdNow = cleanReminder($('rolPrincipal').value);
        if(!isCirujanoByRolPrincipalId(rolPrincipalIdNow)){
          toast('Bono solo aplica a Médico Cirujano');
          return;
        }
    
        // tomamos el doc actual desde state.all solo para nombre/rut y tramos override
        const p = state.all.find(x=>x.rutId===rutId);
        if(!p){
          toast('No encontré el profesional en la lista (recarga la página)');
          return;
        }
        
        // ✅ “pLive” respeta el rol actual del formulario (sin guardar aún)
        const pLive = {
          ...p,
          rolPrincipalId: rolPrincipalIdNow,
          tieneBono: !!($('tieneBono')?.checked)
        };
        
        openBonosModalForProfesional(pLive);

      });
    }



    // Search (coma=AND)
    $('buscador').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // XLSX
    $('btnExportar').addEventListener('click', exportXLSX);
    $('btnDescargarPlantilla').addEventListener('click', plantillaXLSX);

    // ✅ Importar XLSX
    const btnImp = $('btnImportar');
    if(btnImp){
      btnImp.addEventListener('click', ()=> $('fileXLSX').click());
    }

    $('fileXLSX').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
      await importXLSX(file);
    });

    // Load
    await loadRoles();
    await loadBonosGlobal();
    await loadAll();
  }
});
