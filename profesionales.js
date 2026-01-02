// profesionales.js
// Profesionales: CRUD + buscar + importar/exportar CSV
// Firestore: colección "profesionales"
// Roles: selector múltiple desde colección "roles"

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe, parseCSV, toCSV } from './utils.js';

import {
  collection, getDocs, addDoc, updateDoc, deleteDoc,
  doc, serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,

  // profesionales
  all: [],        // [{id, tipoPersona, nombre, rut, email, telefono, direccion, giro, contactoEmpresa, roles[]}]
  editId: null,
  q: '',

  // catálogo roles (desde Firestore)
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

function rolesToText(roles){
  const xs = Array.isArray(roles) ? roles : (roles ? [roles] : []);
  return xs.filter(Boolean).join(' | ');
}

function parseRolesCell(v){
  const raw = (v ?? '').toString().trim();
  if(!raw) return [];
  // permite separar por |, ; o ,
  return uniq(
    raw.split('|')
      .flatMap(x=> x.split(';'))
      .flatMap(x=> x.split(','))
      .map(x=> toUpperSafe(cleanReminder(x)))
      .filter(Boolean)
  );
}

function rowMatches(p, q){
  if(!q) return true;
  const hay = normalize([
    p.nombre, p.rut, p.email,
    (p.roles || []).join(' ')
  ].join(' '));
  return hay.includes(q);
}

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colRoles = collection(db, 'roles');

/* =========================
   Roles (catálogo) UI
========================= */
function paintRolesPicker(){
  const wrap = $('rolesWrap');
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `
      <div class="muted">
        No hay roles creados. Ve a <b>Roles</b> para crear el primero.
      </div>
    `;
    return;
  }

  for(const r of state.rolesCatalog){
    const id = `role_${r.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';

    label.innerHTML = `
      <input type="checkbox" id="${id}" data-role="${escapeHtml(r.nombre)}"/>
      <span class="pill">${escapeHtml(r.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function getSelectedRoles(){
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked){
      out.push(toUpperSafe(ch.getAttribute('data-role') || ''));
    }
  });
  return uniq(out);
}

function setSelectedRoles(rolesArr){
  const wanted = new Set((rolesArr || []).map(x=>toUpperSafe(x)));
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role]');
  checks.forEach(ch=>{
    const v = toUpperSafe(ch.getAttribute('data-role') || '');
    ch.checked = wanted.has(v);
  });
}

/* =========================
   Loaders
========================= */
async function loadRoles(){
  const snap = await getDocs(colRoles);
  const out = [];
  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = toUpperSafe(cleanReminder(x.nombre));
    if(!nombre) return;
    out.push({ id: d.id, nombre });
  });
  // orden alfabético
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;
  paintRolesPicker();
}

async function loadAll(){
  const snap = await getDocs(colProfesionales);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};

    // ✅ Compatibilidad nombre/razón social (tu problema actual)
    const tipoInferido =
      cleanReminder(x.tipoPersona) ||
      (x.razonSocial || x.nombreEmpresa || x.razon_social || x.giro || x.contactoEmpresa ? 'JURIDICA' : 'NATURAL');

    const nombreInferido =
      cleanReminder(x.nombre) ||
      cleanReminder(x.razonSocial) ||
      cleanReminder(x.nombreEmpresa) ||
      cleanReminder(x.razon_social) ||
      '';

    // ✅ Compatibilidad rol antiguo (string) vs roles nuevo (array)
    const roles =
      Array.isArray(x.roles) ? x.roles.map(r=>toUpperSafe(cleanReminder(r))) :
      (x.rol ? [toUpperSafe(cleanReminder(x.rol))] : []);

    out.push({
      id: d.id,
      tipoPersona: (tipoInferido || 'NATURAL'),
      nombre: cleanReminder(nombreInferido),
      rut: cleanReminder(x.rut),
      email: cleanReminder(x.email),
      telefono: cleanReminder(x.telefono),
      direccion: cleanReminder(x.direccion),
      giro: cleanReminder(x.giro),
      contactoEmpresa: cleanReminder(x.contactoEmpresa),
      roles: uniq(roles).filter(Boolean)
    });
  });

  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.all = out;
  paint();
}

/* =========================
   Save / Delete
========================= */
async function saveProfesional(){
  const nombre = cleanReminder($('nombre').value);
  const rut    = cleanReminder($('rut').value);
  const email  = cleanReminder($('email').value);
  const telefono = cleanReminder($('telefono').value);
  const direccion = cleanReminder($('direccion').value);
  const giro = cleanReminder($('giro').value);
  const contactoEmpresa = cleanReminder($('contactoEmpresa').value);
  const tipoPersona = toUpperSafe($('tipoPersona').value || 'NATURAL');

  const roles = getSelectedRoles();

  if(!nombre){
    toast('Falta nombre / razón social');
    $('nombre').focus();
    return;
  }

  // Si no hay roles creados aún, permitimos guardar igual,
  // pero avisamos (para no trabarte el flujo).
  if(!roles.length && state.rolesCatalog.length){
    toast('Selecciona al menos 1 rol');
    return;
  }

  const payload = {
    tipoPersona,
    nombre,
    rut,
    email,
    telefono,
    direccion,
    giro,
    contactoEmpresa,

    // nuevo estándar
    roles,

    // opcional: mantener rol legacy por compatibilidad (toma el primero)
    rol: roles[0] || '',

    updatedAt: serverTimestamp(),
    updatedBy: state.user?.email || ''
  };

  if(state.editId){
    await updateDoc(doc(db,'profesionales',state.editId), payload);
    toast('Profesional actualizado');
  }else{
    payload.createdAt = serverTimestamp();
    payload.createdBy = state.user?.email || '';
    await addDoc(colProfesionales, payload);
    toast('Profesional creado');
  }

  clearForm();
  await loadAll();
}

async function removeProfesional(id){
  const p = state.all.find(x=>x.id===id);
  const ok = confirm(`¿Eliminar profesional?\n\n${p?.nombre || ''}`);
  if(!ok) return;
  await deleteDoc(doc(db,'profesionales',id));
  toast('Eliminado');
  await loadAll();
}

/* =========================
   UI helpers
========================= */
function clearForm(){
  state.editId = null;
  $('nombre').value = '';
  $('rut').value = '';
  $('email').value = '';
  $('telefono').value = '';
  $('direccion').value = '';
  $('giro').value = '';
  $('contactoEmpresa').value = '';
  $('tipoPersona').value = 'NATURAL';

  setSelectedRoles([]);

  $('btnGuardar').textContent = 'Guardar profesional';
}

function setForm(p){
  state.editId = p.id;

  $('nombre').value = p.nombre || '';
  $('rut').value = p.rut || '';
  $('email').value = p.email || '';
  $('telefono').value = p.telefono || '';
  $('direccion').value = p.direccion || '';
  $('giro').value = p.giro || '';
  $('contactoEmpresa').value = p.contactoEmpresa || '';
  $('tipoPersona').value = (p.tipoPersona || 'NATURAL');

  setSelectedRoles(p.roles || []);

  $('btnGuardar').textContent = 'Actualizar profesional';
  $('nombre').focus();
}

function paint(){
  const q = state.q;
  const rows = state.all.filter(p=>rowMatches(p,q));

  $('count').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');

    const rolesHtml = (p.roles || []).length
      ? (p.roles || []).map(r=> `<span class="pill">${escapeHtml(r)}</span>`).join(' ')
      : `<span class="muted">—</span>`;

    tr.innerHTML = `
      <td><b>${escapeHtml(p.nombre)}</b></td>
      <td>${escapeHtml(p.rut || '')}</td>
      <td>${escapeHtml(p.email || '')}</td>
      <td>${rolesHtml}</td>
      <td></td>
    `;

    const td = tr.children[4];

    const btnEdit = document.createElement('button');
    btnEdit.className = 'btn';
    btnEdit.textContent = 'Editar';
    btnEdit.addEventListener('click', ()=> setForm(p));

    const btnDel = document.createElement('button');
    btnDel.className = 'btn danger';
    btnDel.textContent = 'Eliminar';
    btnDel.addEventListener('click', ()=> removeProfesional(p.id));

    td.appendChild(btnEdit);
    td.appendChild(btnDel);

    tb.appendChild(tr);
  }
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
   Import / Export CSV
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
    'nombre','rut','email','telefono','direccion',
    'tipoPersona','giro','contactoEmpresa','roles'
  ];

  const items = state.all.map(p=>({
    nombre: p.nombre || '',
    rut: p.rut || '',
    email: p.email || '',
    telefono: p.telefono || '',
    direccion: p.direccion || '',
    tipoPersona: p.tipoPersona || 'NATURAL',
    giro: p.giro || '',
    contactoEmpresa: p.contactoEmpresa || '',
    roles: rolesToText(p.roles || [])
  }));

  const csv = toCSV(headers, items);
  download(`profesionales_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

function plantillaCSV(){
  const csv = `nombre,rut,email,telefono,direccion,tipoPersona,giro,contactoEmpresa,roles
Juan Pérez,12.345.678-9,jperez@correo.cl,+56911112222,Providencia 123,NATURAL,,,MEDICO|AUXILIAR
Empresa Demo SPA,76.123.456-7,contacto@empresa.cl,+56933334444,Las Condes 456,JURIDICA,Salud,María Soto,MEDICO
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
  const idx = (name)=> headers.indexOf(name);

  const iNombre = idx('nombre');
  const iRut    = idx('rut');
  const iEmail  = idx('email');
  const iTel    = idx('telefono');
  const iDir    = idx('direccion');
  const iTipo   = idx('tipopersona');
  const iGiro   = idx('giro');
  const iContacto = idx('contactoempresa');
  const iRoles  = idx('roles');

  if(iNombre < 0){
    toast('CSV debe incluir columna: nombre');
    return;
  }

  const existing = [...state.all];

  let creates = 0, updates = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];

    const nombre = cleanReminder(row[iNombre] ?? '');
    const rut    = cleanReminder(iRut>=0 ? row[iRut] : '');
    const email  = cleanReminder(iEmail>=0 ? row[iEmail] : '');
    const telefono = cleanReminder(iTel>=0 ? row[iTel] : '');
    const direccion = cleanReminder(iDir>=0 ? row[iDir] : '');
    const tipoPersona = toUpperSafe(cleanReminder(iTipo>=0 ? row[iTipo] : 'NATURAL')) || 'NATURAL';
    const giro = cleanReminder(iGiro>=0 ? row[iGiro] : '');
    const contactoEmpresa = cleanReminder(iContacto>=0 ? row[iContacto] : '');
    const roles = iRoles>=0 ? parseRolesCell(row[iRoles]) : [];

    if(!nombre){ skipped++; continue; }

    const match = existing.find(x =>
      (rut && x.rut && normalize(x.rut)===normalize(rut)) ||
      (email && x.email && normalize(x.email)===normalize(email))
    );

    const payload = {
      tipoPersona,
      nombre, rut, email, telefono, direccion,
      giro, contactoEmpresa,
      roles,
      rol: roles[0] || '',
      updatedAt: serverTimestamp(),
      updatedBy: state.user?.email || ''
    };

    if(match){
      await updateDoc(doc(db,'profesionales',match.id), payload);
      updates++;
    }else{
      payload.createdAt = serverTimestamp();
      payload.createdBy = state.user?.email || '';
      await addDoc(colProfesionales, payload);
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

    $('btnGuardar').addEventListener('click', saveProfesional);
    $('btnLimpiar').addEventListener('click', clearForm);

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

    // IMPORTANTE: primero roles, luego profesionales (para poder marcar checkboxes al editar)
    await loadRoles();
    await loadAll();
  }
});
