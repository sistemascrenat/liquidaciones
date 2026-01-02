// profesionales.js — COMPLETO
// ✅ Tabla estilo "Profesional/Empresa | RUT | Contacto" + Acciones (✏️ 🗑️)
// ✅ Modal para crear/editar
// ✅ Rol principal + roles secundarios (desde colección roles)
// ✅ Descuentos: muestra Sí/No en tabla (no monto/razón)
// ✅ Buscador con coma "," como AND (Y)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe, parseCSV, toCSV } from './utils.js';

import {
  collection, getDocs, setDoc, deleteDoc,
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
  rolesCatalog: []       // [{id, nombre}]
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

function normalizeProfesionalDoc(id, x){
  const tipoPersona = (cleanReminder(x.tipoPersona) || '').toLowerCase() || 'natural';
  const isJ = (tipoPersona === 'juridica');

  const rut = cleanReminder(x.rut);
  const rutId = cleanReminder(x.rutId) || id || rutToId(rut);

  return {
    rutId,
    tipoPersona,
    estado: (cleanReminder(x.estado) || 'activo').toLowerCase(),

    // persona/empresa
    nombreProfesional: cleanReminder(x.nombreProfesional) || '',
    razonSocial: isJ ? (cleanReminder(x.razonSocial) || '') : '',

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
    rolPrincipalId: cleanReminder(x.rolPrincipalId) || '',
    rolesSecundariosIds: Array.isArray(x.rolesSecundariosIds) ? x.rolesSecundariosIds.filter(Boolean) : [],

    // descuentos
    tieneDescuento: !!x.tieneDescuento,
    descuentoUF: Number(x.descuentoUF ?? 0) || 0,
    descuentoRazon: (x.descuentoRazon ?? '') ? cleanReminder(x.descuentoRazon) : ''
  };
}

async function loadAll(){
  const snap = await getDocs(colProfesionales);
  const out = [];
  snap.forEach(d=>{
    out.push(normalizeProfesionalDoc(d.id, d.data() || {}));
  });

  // Orden: si es jurídica, por razón social; si es natural, por nombre profesional
  out.sort((a,b)=>{
    const A = normalize(a.tipoPersona==='juridica' ? a.razonSocial : a.nombreProfesional);
    const B = normalize(b.tipoPersona==='juridica' ? b.razonSocial : b.nombreProfesional);
    if(A !== B) return A.localeCompare(B);
    return normalize(a.nombreProfesional || '').localeCompare(normalize(b.nombreProfesional || ''));
  });

  state.all = out;
  paint();
}

/* =========================
   Search (coma = AND)
========================= */
function queryTerms(raw){
  // "ignovacion, cirujano" => ["ignovacion","cirujano"]
  return (raw || '')
    .toString()
    .split(',')
    .map(s=> normalize(s))
    .filter(Boolean);
}

function rowMatches(p, rawQuery){
  const terms = queryTerms(rawQuery);
  if(!terms.length) return true;

  const showMain = (p.tipoPersona === 'juridica' && p.razonSocial) ? p.razonSocial : p.nombreProfesional;

  const hay = normalize([
    showMain,
    (p.tipoPersona === 'juridica' ? p.nombreProfesional : ''),
    p.rut, p.rutEmpresa,
    p.correoPersonal, p.correoEmpresa,
    p.telefono, p.telefonoEmpresa,
    roleNameById(p.rolPrincipalId),
    ...(p.rolesSecundariosIds || []).map(roleNameById),
    (p.tieneDescuento ? 'descuento si true' : 'descuento no false'),
    p.estado, p.tipoPersona
  ].join(' '));

  // AND: todos los términos deben existir
  return terms.every(t => hay.includes(t));
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

  applyTipoPersonaUI();
}

/* =========================
   Save / Delete
========================= */
async function saveProfesional(){
  const tipoPersona = ($('tipoPersona').value || 'natural').toLowerCase();
  const isJ = (tipoPersona === 'juridica');

  const nombreProfesional = cleanReminder($('nombreProfesional').value);
  const razonSocial = cleanReminder($('razonSocial').value);

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

    tieneDescuento,
    descuentoUF,
    descuentoRazon: descuentoRazon || null,

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

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

function exportCSV(){
  const headers = [
    'tipoPersona','estado',
    'nombreProfesional','razonSocial',
    'rut','rutEmpresa',
    'correoPersonal','correoEmpresa',
    'telefono','telefonoEmpresa',
    'rolPrincipalId','rolesSecundariosIds',
    'tieneDescuento','descuentoUF','descuentoRazon'
  ];

  const items = state.all.map(p=>({
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
    descuentoUF: (Number(p.descuentoUF ?? 0) || 0).toString(),
    descuentoRazon: p.descuentoRazon || ''
  }));

  const csv = toCSV(headers, items);
  download(`profesionales_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

function plantillaCSV(){
  const csv = `tipoPersona,estado,nombreProfesional,razonSocial,rut,rutEmpresa,correoPersonal,correoEmpresa,telefono,telefonoEmpresa,rolPrincipalId,rolesSecundariosIds,tieneDescuento,descuentoUF,descuentoRazon
natural,activo,Juan Pérez,,14.123.456-1,,juanperez@gmail.com,,+56988775599,,r_cirujano,r_asistente_cirujano|r_cirujano,false,0,
juridica,activo,Andrea González,González SPA,17.321.765-4,77.998.233-1,andrea@correo.com,gonzalezspa@empresa.cl,+56988997755,+56222223333,r_cirujano,r_asistente_cirujano,false,0,
`;
  download('plantilla_profesionales.csv', csv, 'text/csv');
  toast('Plantilla descargada');
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
    tipoPersona: idx('tipopersona'),
    estado: idx('estado'),
    nombreProfesional: idx('nombreprofesional'),
    razonSocial: idx('razonsocial'),
    rut: idx('rut'),
    rutEmpresa: idx('rutempresa'),
    correoPersonal: idx('correopersonal'),
    correoEmpresa: idx('correoempresa'),
    telefono: idx('telefono'),
    telefonoEmpresa: idx('telefonoempresa'),
    rolPrincipalId: idx('rolprincipalid'),
    rolesSecundariosIds: idx('rolessecundariosids'),
    tieneDescuento: idx('tienedescuento'),
    descuentoUF: idx('descuentouf'),
    descuentoRazon: idx('descuentorazon')
  };

  if(I.rut < 0 || I.nombreProfesional < 0){
    toast('CSV debe incluir al menos: rut, nombreProfesional');
    return;
  }

  let upserts = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];

    const rut = cleanReminder(row[I.rut] ?? '');
    const rutId = rutToId(rut);
    const nombreProfesional = cleanReminder(row[I.nombreProfesional] ?? '');

    if(!rutId || !nombreProfesional){ skipped++; continue; }

    const tipoPersona = (cleanReminder(I.tipoPersona>=0 ? row[I.tipoPersona] : 'natural') || 'natural').toLowerCase();
    const isJ = (tipoPersona === 'juridica');

    const payload = {
      tipoPersona,
      estado: (cleanReminder(I.estado>=0 ? row[I.estado] : 'activo') || 'activo').toLowerCase(),

      rut,
      rutId,

      nombreProfesional,
      razonSocial: isJ ? (cleanReminder(I.razonSocial>=0 ? row[I.razonSocial] : '') || null) : null,
      rutEmpresa: isJ ? (cleanReminder(I.rutEmpresa>=0 ? row[I.rutEmpresa] : '') || null) : null,

      correoPersonal: (cleanReminder(I.correoPersonal>=0 ? row[I.correoPersonal] : '') || null),
      correoEmpresa: isJ ? (cleanReminder(I.correoEmpresa>=0 ? row[I.correoEmpresa] : '') || null) : null,

      telefono: (cleanReminder(I.telefono>=0 ? row[I.telefono] : '') || null),
      telefonoEmpresa: isJ ? (cleanReminder(I.telefonoEmpresa>=0 ? row[I.telefonoEmpresa] : '') || null) : null,

      rolPrincipalId: (cleanReminder(I.rolPrincipalId>=0 ? row[I.rolPrincipalId] : '') || null),
      rolesSecundariosIds: uniq(
        (cleanReminder(I.rolesSecundariosIds>=0 ? row[I.rolesSecundariosIds] : '') || '')
          .split('|').map(x=>cleanReminder(x)).filter(Boolean)
      ),

      tieneDescuento: String(cleanReminder(I.tieneDescuento>=0 ? row[I.tieneDescuento] : 'false') || 'false').toLowerCase() === 'true',
      descuentoUF: Number(cleanReminder(I.descuentoUF>=0 ? row[I.descuentoUF] : '0') || 0) || 0,
      descuentoRazon: (cleanReminder(I.descuentoRazon>=0 ? row[I.descuentoRazon] : '') || null),

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    };

    await setDoc(doc(db,'profesionales',rutId), payload, { merge:true });
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
    $('who').textContent = `Conectado: ${user.email}`;
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

    $('tipoPersona').addEventListener('change', applyTipoPersonaUI);

    // Search (coma=AND)
    $('buscador').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // CSV
    $('btnExportar').addEventListener('click', exportCSV);
    $('btnDescargarPlantilla').addEventListener('click', plantillaCSV);

    // ✅ Importar (botón real)
    const btnImp = $('btnImportar');
    if(btnImp){
      btnImp.addEventListener('click', ()=> $('fileCSV').click());
    }

    $('fileCSV').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
      await importCSV(file);
    });

    // Load
    await loadRoles();
    await loadAll();
  }
});
