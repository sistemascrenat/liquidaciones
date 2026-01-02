// profesionales.js — COMPLETO (modal form + rol principal + roles secundarios + lectura correcta Firestore)
// Firestore:
// - profesionales/{rutId}  (doc id = rutId)
// - roles/{roleId}         (catálogo roles)
// Campos en profesionales (según tu Firebase):
// tipoPersona: 'natural' | 'juridica'
// nombreProfesional, razonSocial
// rut, rutEmpresa, rutId
// correoPersonal, correoEmpresa
// telefono
// direccion, direccionEmpresa
// ciudadEmpresa
// giro
// estado: 'activo' | 'inactivo'
// rolPrincipalId: 'r_xxx'
// rolesSecundariosIds: ['r_xxx', ...]
// tieneDescuento: boolean
// descuentoUF: number
// descuentoRazon: string|null
// creadoEl, actualizadoEl (timestamps)

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

  // profesionales (docs ya normalizados a nuestra UI)
  all: [],        // [{rutId, rut, tipoPersona, nombreProfesional, razonSocial, ...}]
  editRutId: null,
  q: '',

  // roles catálogo
  rolesCatalog: [] // [{id, nombre}]
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
  return (rut ?? '').toString().replace(/\D/g,''); // deja solo números
}

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colRoles = collection(db, 'roles');

/* =========================
   Modal helpers
========================= */
function openModal(mode, p=null){
  // mode: 'create' | 'edit'
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

  // campos solo jurídica (deshabilitar + "grisar" para que sea obvio)
  const onlyJ = ['razonSocial','rutEmpresa','correoEmpresa','direccionEmpresa','ciudadEmpresa'];
  for(const id of onlyJ){
    const el = $(id);
    el.disabled = !isJ;
    el.style.opacity = isJ ? '1' : '.55';
    if(!isJ) el.value = '';
  }
}

/* =========================
   Roles UI
========================= */
function paintRolesUI(){
  // rol principal: select
  const sel = $('rolPrincipal');
  sel.innerHTML = '';

  if(!state.rolesCatalog.length){
    sel.innerHTML = `<option value="">(Sin roles)</option>`;
  }else{
    sel.innerHTML = `<option value="">Selecciona rol principal…</option>` +
      state.rolesCatalog.map(r=> `<option value="${escapeHtml(r.id)}">${escapeHtml(r.nombre)}</option>`).join('');
  }

  // roles secundarios: checkboxes
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
    if(ch.checked){
      out.push(ch.getAttribute('data-role-id'));
    }
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
    // roles: { nombre: "CIRUJANO" } por ejemplo
    const nombre = cleanReminder(x.nombre);
    if(!nombre) return;
    out.push({ id: d.id, nombre: toUpperSafe(nombre) });
  });
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;

  paintRolesUI();
}

function normalizeProfesionalDoc(id, x){
  // id = doc id (idealmente rutId)
  const tipoPersona = (cleanReminder(x.tipoPersona) || '').toLowerCase() || 'natural';
  const rut = cleanReminder(x.rut);
  const rutId = cleanReminder(x.rutId) || id || rutToId(rut);

  // nombreProfesional SIEMPRE, razonSocial solo jurídica
  const nombreProfesional = cleanReminder(x.nombreProfesional) || cleanReminder(x.nombre) || '';
  const razonSocial = cleanReminder(x.razonSocial) || cleanReminder(x.nombreEmpresa) || '';

  const rolPrincipalId = cleanReminder(x.rolPrincipalId) || '';
  const rolesSecundariosIds = Array.isArray(x.rolesSecundariosIds) ? x.rolesSecundariosIds.filter(Boolean) : [];

  return {
    rutId,
    rut,
    tipoPersona,
    nombreProfesional,
    razonSocial: (tipoPersona === 'juridica' ? razonSocial : ''),

    rutEmpresa: (tipoPersona === 'juridica' ? cleanReminder(x.rutEmpresa) : ''),
    correoPersonal: cleanReminder(x.correoPersonal) || cleanReminder(x.email) || '',
    correoEmpresa: (tipoPersona === 'juridica' ? cleanReminder(x.correoEmpresa) : ''),
    telefono: cleanReminder(x.telefono),
    direccion: cleanReminder(x.direccion),
    direccionEmpresa: (tipoPersona === 'juridica' ? cleanReminder(x.direccionEmpresa) : ''),
    ciudadEmpresa: (tipoPersona === 'juridica' ? cleanReminder(x.ciudadEmpresa) : ''),
    giro: cleanReminder(x.giro),

    estado: (cleanReminder(x.estado) || 'activo').toLowerCase(),

    rolPrincipalId,
    rolesSecundariosIds,

    tieneDescuento: !!x.tieneDescuento,
    descuentoUF: Number(x.descuentoUF ?? 0) || 0,
    descuentoRazon: (x.descuentoRazon ?? '') ? cleanReminder(x.descuentoRazon) : '',

    // campos extra si existen
    clinicasIds: Array.isArray(x.clinicasIds) ? x.clinicasIds : []
  };
}

async function loadAll(){
  const snap = await getDocs(colProfesionales);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    out.push(normalizeProfesionalDoc(d.id, x));
  });

  // orden por nombre profesional, luego razón social
  out.sort((a,b)=>{
    const A = normalize(a.nombreProfesional || '');
    const B = normalize(b.nombreProfesional || '');
    if(A !== B) return A.localeCompare(B);
    return normalize(a.razonSocial || '').localeCompare(normalize(b.razonSocial || ''));
  });

  state.all = out;
  paint();
}

/* =========================
   Search match
========================= */
function rowMatches(p, q){
  if(!q) return true;
  const hay = normalize([
    p.nombreProfesional,
    p.razonSocial,
    p.rut,
    p.rutEmpresa,
    p.correoPersonal,
    p.correoEmpresa,
    p.telefono,
    roleNameById(p.rolPrincipalId),
    ...(p.rolesSecundariosIds || []).map(roleNameById)
  ].join(' '));

  return hay.includes(q);
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

  if(state.rolesCatalog.length){
    if(!rolPrincipalId){
      toast('Selecciona un rol principal');
      $('rolPrincipal').focus();
      return;
    }
  }

  // --- payload Firestore EXACTO a tu esquema ---
  const payload = {
    tipoPersona,
    estado,

    rut,
    rutId,

    nombreProfesional,
    razonSocial: isJ ? razonSocial : null,

    rutEmpresa: isJ ? (rutEmpresa || null) : null,

    correoPersonal: correoPersonal || null,
    correoEmpresa: isJ ? (correoEmpresa || null) : null,

    telefono: telefono || null,

    direccion: direccion || null,
    direccionEmpresa: isJ ? (direccionEmpresa || null) : null,
    ciudadEmpresa: isJ ? (ciudadEmpresa || null) : null,

    giro: giro || null,

    rolPrincipalId: rolPrincipalId || null,
    rolesSecundariosIds: uniq(rolesSecundariosIds),

    tieneDescuento,
    descuentoUF: descuentoUF,
    descuentoRazon: descuentoRazon || null,

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  // al crear: creadoEl/creadoPor (si ya existe, no los tocamos)
  const isEdit = !!state.editRutId;
  if(!isEdit){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  // Firestore doc id = rutId
  await setDoc(doc(db,'profesionales',rutId), payload, { merge: true });

  toast(isEdit ? 'Profesional actualizado' : 'Profesional creado');
  closeModal();
  await loadAll();
}

async function removeProfesional(rutId){
  const p = state.all.find(x=>x.rutId===rutId);
  const ok = confirm(`¿Eliminar profesional?\n\n${p?.nombreProfesional || ''}${p?.razonSocial ? `\n${p.razonSocial}`:''}\n${p?.rut || ''}`);
  if(!ok) return;
  await deleteDoc(doc(db,'profesionales',rutId));
  toast('Eliminado');
  await loadAll();
}

/* =========================
   Paint table
========================= */
function discountLabel(p){
  if(!p.tieneDescuento) return `<span class="muted">—</span>`;
  const uf = Number(p.descuentoUF ?? 0) || 0;
  const razon = p.descuentoRazon ? ` <span class="muted" title="${escapeHtml(p.descuentoRazon)}">(${escapeHtml(p.descuentoRazon)})</span>` : '';
  return `<span class="pill">${escapeHtml(uf.toString())} UF</span>${razon}`;
}

function pillsFromRoleIds(ids){
  const xs = (ids || []).filter(Boolean);
  if(!xs.length) return `<span class="muted">—</span>`;
  return xs
    .map(id=> `<span class="pill">${escapeHtml(roleNameById(id))}</span>`)
    .join(' ');
}

function paint(){
  const q = state.q;
  const rows = state.all.filter(p=>rowMatches(p,q));

  $('count').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');

    const razon = (p.tipoPersona === 'juridica') ? (p.razonSocial || '—') : '—';
    const rutEmpresa = (p.tipoPersona === 'juridica') ? (p.rutEmpresa || '') : '';
    const correoEmpresa = (p.tipoPersona === 'juridica') ? (p.correoEmpresa || '') : '';

    const rolPrincipal = p.rolPrincipalId ? roleNameById(p.rolPrincipalId) : '—';
    const rolesSecHtml = pillsFromRoleIds(p.rolesSecundariosIds || []);

    tr.innerHTML = `
      <td><b>${escapeHtml(p.nombreProfesional || '—')}</b></td>
      <td>${escapeHtml(razon)}</td>
      <td>${escapeHtml(p.rut || '')}</td>
      <td>${escapeHtml(rutEmpresa)}</td>
      <td>${escapeHtml(p.correoPersonal || '')}</td>
      <td>${escapeHtml(correoEmpresa)}</td>
      <td>${escapeHtml(p.telefono || '')}</td>
      <td>${p.rolPrincipalId ? `<span class="pill">${escapeHtml(rolPrincipal)}</span>` : `<span class="muted">—</span>`}</td>
      <td>${rolesSecHtml}</td>
      <td>${discountLabel(p)}</td>
      <td></td>
    `;

    const td = tr.children[10];

    const btnEdit = document.createElement('button');
    btnEdit.className = 'iconBtn';
    btnEdit.type = 'button';
    btnEdit.title = 'Editar';
    btnEdit.setAttribute('aria-label','Editar');
    btnEdit.innerHTML = '✏️';
    btnEdit.addEventListener('click', ()=> openModal('edit', p));

    const btnDel = document.createElement('button');
    btnDel.className = 'iconBtn danger';
    btnDel.type = 'button';
    btnDel.title = 'Eliminar';
    btnDel.setAttribute('aria-label','Eliminar');
    btnDel.innerHTML = '🗑️';
    btnDel.addEventListener('click', ()=> removeProfesional(p.rutId));

    td.appendChild(btnEdit);
    td.appendChild(btnDel);

    tb.appendChild(tr);
  }
}

/* =========================
   Import / Export CSV (compat)
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
    'telefono',
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
  const csv = `tipoPersona,estado,nombreProfesional,razonSocial,rut,rutEmpresa,correoPersonal,correoEmpresa,telefono,rolPrincipalId,rolesSecundariosIds,tieneDescuento,descuentoUF,descuentoRazon
natural,activo,Juan Pérez,,12.345.678-9,,jperez@correo.cl,,+56911112222,r_cirujano,r_asistente_cirujano|r_cirujano,false,0,
juridica,activo,Paloma Martinez,Ignovacion SPA,17.315.517-4,77.644.246-1,paloma@correo.com,pagos@empresa.cl,+56981406262,r_cirujano,r_asistente_cirujano,false,0,
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

    // setDoc merge mantiene creadoEl si existe
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

    // Modal wiring
    $('btnCrear').addEventListener('click', ()=> openModal('create'));
    $('btnModalClose').addEventListener('click', closeModal);
    $('btnModalCancelar').addEventListener('click', closeModal);
    $('btnModalGuardar').addEventListener('click', saveProfesional);

    // Cerrar al clicar backdrop (fuera del card)
    $('modalBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalBackdrop')) closeModal();
    });

    $('tipoPersona').addEventListener('change', applyTipoPersonaUI);

    // Search
    $('buscador').addEventListener('input', (e)=>{
      state.q = normalize(e.target.value);
      paint();
    });

    // CSV
    $('btnExportar').addEventListener('click', exportCSV);
    $('btnDescargarPlantilla').addEventListener('click', plantillaCSV);
    $('fileCSV').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
      await importCSV(file);
    });

    // IMPORTANT: roles first (so UI is ready), then professionals
    await loadRoles();
    await loadAll();
  }
});
