// profesionales.js
// Profesionales: CRUD + buscar + importar/exportar CSV
// Firestore: colección "profesionales" (docId = rutId)
// Roles: desde colección "roles" (id = r_xxx, nombre = "Cirujano", etc.)
// UI: Formulario como MODAL (Crear / Editar)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe, parseCSV, toCSV } from './utils.js';

import {
  collection, getDocs, doc, getDoc, setDoc, updateDoc, deleteDoc,
  serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,

  // profesionales (ya normalizados para tabla / edición)
  all: [],     // [{rutId, rut, tipoPersona, nombreProfesional, razonSocial, rolPrincipalId, rolesSecundariosIds, ...}]
  q: '',

  // roles catálogo (desde Firestore)
  rolesCatalog: [] // [{id, nombre}]
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

// Convierte rut a rutId (solo dígitos)
function rutToRutId(rut=''){
  return (rut ?? '').toString().replace(/\D/g,'').trim();
}

function labelTipo(tipo){
  const t = (tipo || '').toString().toLowerCase();
  return t === 'juridica' ? 'Jurídica' : 'Natural';
}

function roleNameById(id){
  const r = state.rolesCatalog.find(x=>x.id === id);
  return r?.nombre || id || '';
}

function rolesIdsToPills(ids){
  const xs = Array.isArray(ids) ? ids : (ids ? [ids] : []);
  if(!xs.length) return `<span class="muted">—</span>`;
  return xs.map(id=> `<span class="pill">${escapeHtml(roleNameById(id))}</span>`).join(' ');
}

function rowMatches(p, q){
  if(!q) return true;

  const hay = normalize([
    p.nombreProfesional,
    p.razonSocial,
    p.rut,
    p.rutEmpresa,
    roleNameById(p.rolPrincipalId),
    (p.rolesSecundariosIds||[]).map(roleNameById).join(' ')
  ].join(' '));

  return hay.includes(q);
}

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colRoles = collection(db, 'roles');

/* =========================
   Modal controls
========================= */
function openModal(mode, p=null){
  // mode: 'new' | 'edit'
  $('modalProfesional').classList.add('show');
  $('modalProfesional').setAttribute('aria-hidden','false');

  if(mode === 'new'){
    $('modalTitulo').textContent = 'Crear profesional';
    $('modalSub').textContent = 'Completa los datos y guarda.';
    $('modalHint').textContent = '';
    clearModal();
    // defaults
    $('m_tipoPersona').value = 'natural';
    $('m_estado').value = 'activo';
    toggleJuridicaFields();
    // roles
    paintRolesUI();
  }else{
    $('modalTitulo').textContent = 'Editar profesional';
    $('modalSub').textContent = 'Modifica y guarda los cambios.';
    loadToModal(p);
    toggleJuridicaFields();
    paintRolesUI(p); // marca checks
    $('modalHint').textContent = `DocId (rutId): ${p?.rutId || ''}`;
  }
}

function closeModal(){
  $('modalProfesional').classList.remove('show');
  $('modalProfesional').setAttribute('aria-hidden','true');
}

function clearModal(){
  $('m_tipoPersona').value = 'natural';
  $('m_estado').value = 'activo';

  $('m_nombreProfesional').value = '';
  $('m_razonSocial').value = '';

  $('m_rut').value = '';
  $('m_rutEmpresa').value = '';

  $('m_correoPersonal').value = '';
  $('m_correoEmpresa').value = '';

  $('m_telefono').value = '';

  $('m_direccionEmpresa').value = '';
  $('m_ciudadEmpresa').value = '';

  $('m_tieneDescuento').checked = false;
  $('m_descuentoUF').value = 0;
  $('m_descuentoRazon').value = '';

  // roles se repintan
  $('m_rolPrincipal').innerHTML = '';
  $('m_rolesSecWrap').innerHTML = '';
}

function toggleJuridicaFields(){
  const tipo = ($('m_tipoPersona').value || 'natural').toLowerCase();
  const isJ = tipo === 'juridica';

  $('wrap_razonSocial').style.display = isJ ? '' : 'none';
  $('wrap_rutEmpresa').style.display = isJ ? '' : 'none';
  $('wrap_correoEmpresa').style.display = isJ ? '' : 'none';
  $('wrap_direccionEmpresa').style.display = isJ ? '' : 'none';
  $('wrap_ciudadEmpresa').style.display = isJ ? '' : 'none';

  // Si es natural, limpiamos “cosas empresa” para evitar basura accidental
  if(!isJ){
    $('m_razonSocial').value = '';
    $('m_rutEmpresa').value = '';
    $('m_correoEmpresa').value = '';
    $('m_direccionEmpresa').value = '';
    $('m_ciudadEmpresa').value = '';
  }
}

/* =========================
   Roles UI (modal)
========================= */
function paintRolesUI(p=null){
  // principal select
  const sel = $('m_rolPrincipal');
  sel.innerHTML = '';

  if(!state.rolesCatalog.length){
    sel.innerHTML = `<option value="">(No hay roles)</option>`;
  }else{
    sel.innerHTML = `<option value="">Selecciona rol principal...</option>` +
      state.rolesCatalog.map(r=> `<option value="${escapeHtml(r.id)}">${escapeHtml(r.nombre)}</option>`).join('');
  }

  // secundarios: checkboxes
  const wrap = $('m_rolesSecWrap');
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay roles creados. Ve a <b>Roles</b>.</div>`;
  }else{
    for(const r of state.rolesCatalog){
      const id = `sec_${r.id}`;
      const label = document.createElement('label');
      label.className = 'roleCheck';
      label.innerHTML = `
        <input type="checkbox" id="${id}" data-roleid="${escapeHtml(r.id)}"/>
        <span class="pill">${escapeHtml(r.nombre)}</span>
      `;
      wrap.appendChild(label);
    }
  }

  // si viene p (editar), setear valores
  if(p){
    if(p.rolPrincipalId) sel.value = p.rolPrincipalId;

    const wanted = new Set((p.rolesSecundariosIds || []).map(String));
    const checks = wrap.querySelectorAll('input[type="checkbox"][data-roleid]');
    checks.forEach(ch=>{
      const rid = ch.getAttribute('data-roleid');
      ch.checked = wanted.has(rid);
    });
  }
}

function getRolesSecundariosIds(){
  const wrap = $('m_rolesSecWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-roleid]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-roleid'));
  });
  return uniq(out);
}

/* =========================
   Loaders
========================= */
async function loadRoles(){
  const snap = await getDocs(colRoles);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};
    // asumimos: roles/{id} con campo nombre
    const nombre = cleanReminder(x.nombre);
    if(!nombre) return;
    out.push({ id: d.id, nombre });
  });

  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;
}

async function loadAll(){
  const snap = await getDocs(colProfesionales);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};

    // EN TU FIREBASE: docId = rutId
    const rutId = x.rutId || d.id;

    out.push({
      rutId: cleanReminder(rutId),
      rut: cleanReminder(x.rut),
      rutEmpresa: cleanReminder(x.rutEmpresa),
      tipoPersona: (x.tipoPersona || 'natural').toString().toLowerCase(),

      nombreProfesional: cleanReminder(x.nombreProfesional),
      razonSocial: cleanReminder(x.razonSocial),

      correoPersonal: cleanReminder(x.correoPersonal),
      correoEmpresa: cleanReminder(x.correoEmpresa),

      telefono: cleanReminder(x.telefono),

      direccionEmpresa: cleanReminder(x.direccionEmpresa),
      ciudadEmpresa: cleanReminder(x.ciudadEmpresa),

      rolPrincipalId: cleanReminder(x.rolPrincipalId),
      rolesSecundariosIds: Array.isArray(x.rolesSecundariosIds) ? x.rolesSecundariosIds.map(String) : [],

      tieneDescuento: !!x.tieneDescuento,
      descuentoUF: Number(x.descuentoUF || 0),
      descuentoRazon: cleanReminder(x.descuentoRazon),

      estado: cleanReminder(x.estado || 'activo')
    });
  });

  // orden: por razón social si jurídica, si no por nombreProfesional
  out.sort((a,b)=>{
    const an = normalize(a.tipoPersona==='juridica' ? (a.razonSocial||'') : (a.nombreProfesional||''));
    const bn = normalize(b.tipoPersona==='juridica' ? (b.razonSocial||'') : (b.nombreProfesional||''));
    return an.localeCompare(bn);
  });

  state.all = out;
  paint();
}

/* =========================
   Paint table
========================= */
function paint(){
  const rows = state.all.filter(p=>rowMatches(p, state.q));

  $('count').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');

    const profesionalLabel = p.nombreProfesional || '—';
    const razonSocialLabel = (p.tipoPersona === 'juridica') ? (p.razonSocial || '—') : '—';

    const rolPrincipal = p.rolPrincipalId ? roleNameById(p.rolPrincipalId) : '—';
    const rolesSec = rolesIdsToPills(p.rolesSecundariosIds || []);

    const desc = p.tieneDescuento
      ? `<span class="pill">Sí</span> <span class="muted">UF ${escapeHtml(String(p.descuentoUF || 0))}</span>`
      : `<span class="muted">No</span>`;

    tr.innerHTML = `
      <td><b>${escapeHtml(profesionalLabel)}</b></td>
      <td>${escapeHtml(razonSocialLabel)}</td>
      <td>${escapeHtml(p.rut || '')}</td>
      <td>${escapeHtml(labelTipo(p.tipoPersona))}</td>
      <td><span class="pill">${escapeHtml(rolPrincipal)}</span></td>
      <td>${rolesSec}</td>
      <td>${desc}</td>
      <td></td>
    `;

    const td = tr.children[7];

    const btnEdit = document.createElement('button');
    btnEdit.className = 'btn';
    btnEdit.textContent = 'Editar';
    btnEdit.addEventListener('click', ()=> openModal('edit', p));

    const btnDel = document.createElement('button');
    btnDel.className = 'btn danger';
    btnDel.textContent = 'Eliminar';
    btnDel.addEventListener('click', ()=> removeProfesional(p.rutId));

    td.appendChild(btnEdit);
    td.appendChild(btnDel);

    tb.appendChild(tr);
  }
}

/* =========================
   Save / Delete (modal)
========================= */
function loadToModal(p){
  clearModal();

  $('m_tipoPersona').value = (p.tipoPersona || 'natural');
  $('m_estado').value = p.estado || 'activo';

  $('m_nombreProfesional').value = p.nombreProfesional || '';
  $('m_razonSocial').value = p.razonSocial || '';

  $('m_rut').value = p.rut || '';
  $('m_rutEmpresa').value = p.rutEmpresa || '';

  $('m_correoPersonal').value = p.correoPersonal || '';
  $('m_correoEmpresa').value = p.correoEmpresa || '';

  $('m_telefono').value = p.telefono || '';

  $('m_direccionEmpresa').value = p.direccionEmpresa || '';
  $('m_ciudadEmpresa').value = p.ciudadEmpresa || '';

  $('m_tieneDescuento').checked = !!p.tieneDescuento;
  $('m_descuentoUF').value = Number(p.descuentoUF || 0);
  $('m_descuentoRazon').value = p.descuentoRazon || '';

  // roles se setean en paintRolesUI(p)
}

async function saveFromModal(){
  const tipoPersona = ($('m_tipoPersona').value || 'natural').toLowerCase();
  const isJ = tipoPersona === 'juridica';

  const nombreProfesional = cleanReminder($('m_nombreProfesional').value);
  const razonSocial = cleanReminder($('m_razonSocial').value);

  const rut = cleanReminder($('m_rut').value);
  const rutId = rutToRutId(rut);

  const rolPrincipalId = cleanReminder($('m_rolPrincipal').value);

  if(!rut){
    toast('Falta RUT');
    $('m_rut').focus();
    return;
  }
  if(!rutId){
    toast('RUT inválido');
    $('m_rut').focus();
    return;
  }

  // Regla: nombreProfesional siempre requerido
  if(!nombreProfesional){
    toast('Falta nombre profesional');
    $('m_nombreProfesional').focus();
    return;
  }

  // Regla: si jurídica, razón social requerida
  if(isJ && !razonSocial){
    toast('Falta razón social (jurídica)');
    $('m_razonSocial').focus();
    return;
  }

  // Rol principal obligatorio (según tu requerimiento)
  if(!rolPrincipalId){
    toast('Selecciona rol principal');
    $('m_rolPrincipal').focus();
    return;
  }

  const rolesSecundariosIds = getRolesSecundariosIds();

  const tieneDescuento = !!$('m_tieneDescuento').checked;
  const descuentoUF = Number($('m_descuentoUF').value || 0);
  const descuentoRazon = cleanReminder($('m_descuentoRazon').value);

  const payload = {
    rutId,
    rut,

    tipoPersona,
    estado: cleanReminder($('m_estado').value || 'activo'),

    nombreProfesional,
    razonSocial: isJ ? razonSocial : null,

    rutEmpresa: isJ ? cleanReminder($('m_rutEmpresa').value) : null,

    correoPersonal: cleanReminder($('m_correoPersonal').value) || null,
    correoEmpresa: isJ ? (cleanReminder($('m_correoEmpresa').value) || null) : null,

    telefono: cleanReminder($('m_telefono').value) || null,

    direccionEmpresa: isJ ? (cleanReminder($('m_direccionEmpresa').value) || null) : null,
    ciudadEmpresa: isJ ? (cleanReminder($('m_ciudadEmpresa').value) || null) : null,

    rolPrincipalId,
    rolesSecundariosIds,

    tieneDescuento,
    descuentoUF: tieneDescuento ? descuentoUF : 0,
    descuentoRazon: tieneDescuento ? (descuentoRazon || null) : null,

    actualizadoEl: serverTimestamp(),
  };

  const ref = doc(db, 'profesionales', rutId);

  // Si existe => update. Si no existe => set con creadoEl.
  const snap = await getDoc(ref);

  if(snap.exists()){
    payload.actualizadoPor = state.user?.email || '';
    await updateDoc(ref, payload);
    toast('Profesional actualizado');
  }else{
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
    await setDoc(ref, payload);
    toast('Profesional creado');
  }

  closeModal();
  await loadAll();
}

async function removeProfesional(rutId){
  const p = state.all.find(x=>x.rutId === rutId);
  const ok = confirm(`¿Eliminar profesional?\n\n${p?.nombreProfesional || ''}\n${p?.rut || ''}`);
  if(!ok) return;
  await deleteDoc(doc(db,'profesionales',rutId));
  toast('Eliminado');
  await loadAll();
}

/* =========================
   Import / Export CSV
   (mantengo simple; si quieres, lo afinamos a tu formato exacto)
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
    'rutId','rut','tipoPersona','estado',
    'nombreProfesional','razonSocial','rutEmpresa',
    'correoPersonal','correoEmpresa','telefono',
    'rolPrincipalId','rolesSecundariosIds',
    'tieneDescuento','descuentoUF','descuentoRazon'
  ];

  const items = state.all.map(p=>({
    rutId: p.rutId || '',
    rut: p.rut || '',
    tipoPersona: p.tipoPersona || 'natural',
    estado: p.estado || 'activo',

    nombreProfesional: p.nombreProfesional || '',
    razonSocial: p.razonSocial || '',
    rutEmpresa: p.rutEmpresa || '',

    correoPersonal: p.correoPersonal || '',
    correoEmpresa: p.correoEmpresa || '',
    telefono: p.telefono || '',

    rolPrincipalId: p.rolPrincipalId || '',
    rolesSecundariosIds: (p.rolesSecundariosIds || []).join('|'),

    tieneDescuento: p.tieneDescuento ? 'true' : 'false',
    descuentoUF: String(p.descuentoUF || 0),
    descuentoRazon: p.descuentoRazon || ''
  }));

  const csv = toCSV(headers, items);
  download(`profesionales_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

function plantillaCSV(){
  const csv =
`rut,nombreProfesional,tipoPersona,razonSocial,rutEmpresa,correoPersonal,correoEmpresa,telefono,rolPrincipalId,rolesSecundariosIds,tieneDescuento,descuentoUF,descuentoRazon
16128922-1,Ignacio Pastor,natural,,,nacho@gmail.com,,+56952270713,r_cirujano,r_asistente_cirujano|r_cirujano,false,0,
17315517-4,Paloma Martinez,juridica,Ignovacion SPA,77644246-1,paloma@correo.com,pagos@empresa.cl,+56981406262,r_cirujano,r_asistente_cirujano,false,0,
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

  const iRut = idx('rut');
  const iNombre = idx('nombreprofesional');
  const iTipo = idx('tipopersona');
  const iRazon = idx('razonsocial');
  const iRutEmp = idx('rutempresa');
  const iCorreoP = idx('correopersonal');
  const iCorreoE = idx('correoempresa');
  const iTel = idx('telefono');
  const iRolP = idx('rolprincipalid');
  const iRolesS = idx('rolessecundariosids');
  const iTiene = idx('tienedescuento');
  const iUf = idx('descuentouf');
  const iRazonD = idx('descuentorazon');

  if(iRut < 0 || iNombre < 0){
    toast('CSV debe incluir: rut y nombreProfesional');
    return;
  }

  let creates = 0, updates = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];

    const rut = cleanReminder(row[iRut] ?? '');
    const rutId = rutToRutId(rut);
    const nombreProfesional = cleanReminder(row[iNombre] ?? '');

    if(!rutId || !nombreProfesional){ skipped++; continue; }

    const tipoPersona = cleanReminder(iTipo>=0 ? row[iTipo] : 'natural').toLowerCase() || 'natural';
    const isJ = tipoPersona === 'juridica';

    const rolPrincipalId = cleanReminder(iRolP>=0 ? row[iRolP] : '');
    const rolesSecundariosIds = iRolesS>=0
      ? uniq((row[iRolesS] ?? '').toString().split('|').map(x=>cleanReminder(x)).filter(Boolean))
      : [];

    const tieneDescuento = (iTiene>=0 ? cleanReminder(row[iTiene]) : '').toLowerCase() === 'true';
    const descuentoUF = Number(iUf>=0 ? row[iUf] : 0) || 0;
    const descuentoRazon = cleanReminder(iRazonD>=0 ? row[iRazonD] : '');

    const payload = {
      rutId,
      rut,
      tipoPersona,
      nombreProfesional,
      razonSocial: isJ ? (cleanReminder(iRazon>=0 ? row[iRazon] : '') || null) : null,
      rutEmpresa: isJ ? (cleanReminder(iRutEmp>=0 ? row[iRutEmp] : '') || null) : null,
      correoPersonal: cleanReminder(iCorreoP>=0 ? row[iCorreoP] : '') || null,
      correoEmpresa: isJ ? (cleanReminder(iCorreoE>=0 ? row[iCorreoE] : '') || null) : null,
      telefono: cleanReminder(iTel>=0 ? row[iTel] : '') || null,
      rolPrincipalId: rolPrincipalId || null,
      rolesSecundariosIds,
      tieneDescuento,
      descuentoUF: tieneDescuento ? descuentoUF : 0,
      descuentoRazon: tieneDescuento ? (descuentoRazon || null) : null,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || '',
      estado: 'activo'
    };

    const ref = doc(db, 'profesionales', rutId);
    const snap = await getDoc(ref);

    if(snap.exists()){
      await updateDoc(ref, payload);
      updates++;
    }else{
      payload.creadoEl = serverTimestamp();
      payload.creadoPor = state.user?.email || '';
      await setDoc(ref, payload);
      creates++;
    }
  }

  toast(`Import listo: +${creates} / ↻${updates} / omitidos ${skipped}`);
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

    // eventos UI
    $('buscador').addEventListener('input', (e)=>{
      state.q = normalize(e.target.value);
      paint();
    });

    $('btnExportar').addEventListener('click', exportCSV);
    $('btnDescargarPlantilla').addEventListener('click', plantillaCSV);

    $('fileCSV').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
      await importCSV(file);
    });

    // modal events
    $('btnCrear').addEventListener('click', ()=> openModal('new'));

    $('btnCerrarModal').addEventListener('click', closeModal);
    $('btnCancelarModal').addEventListener('click', closeModal);

    $('m_tipoPersona').addEventListener('change', ()=>{
      toggleJuridicaFields();
    });

    $('btnGuardarModal').addEventListener('click', saveFromModal);

    // cerrar modal clic fuera
    $('modalProfesional').addEventListener('click', (e)=>{
      if(e.target === $('modalProfesional')) closeModal();
    });

    // ESC
    window.addEventListener('keydown', (e)=>{
      if(e.key === 'Escape' && $('modalProfesional').classList.contains('show')) closeModal();
    });

    // primero roles, luego profesionales (para pintar nombres)
    await loadRoles();
    await loadAll();
  }
});
