// profesionales.js
// Módulo Profesionales: CRUD + buscar + importar/exportar CSV
// Firestore: colección "profesionales"
// Mejoras:
// - Tipo persona NATURAL/JURIDICA
// - Campos empresa (giro, contactoEmpresa) visibles solo si JURIDICA
// - Teléfono + dirección
// - Import/Export CSV actualizado

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
  all: [],        // [{id, ...campos}]
  editId: null,
  q: ''
};

const $ = (id)=> document.getElementById(id);

/* =========================
   Helpers
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

function labelTipo(tipo='NATURAL'){
  const t = (tipo || 'NATURAL').toUpperCase();
  return t === 'JURIDICA' ? 'Jurídica' : 'Natural';
}

function rowMatches(p, q){
  if(!q) return true;
  const hay = normalize([
    p.tipoPersona, p.nombre, p.rut, p.email, p.telefono, p.direccion,
    p.giro, p.contactoEmpresa, p.rol
  ].join(' '));
  return hay.includes(q);
}

/* =========================
   Firestore
========================= */
const colRef = collection(db, 'profesionales');

async function loadAll(){
  const snap = await getDocs(colRef);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};
    out.push({
      id: d.id,
      tipoPersona: cleanReminder(x.tipoPersona) || 'NATURAL',
      nombre: cleanReminder(x.nombre),
      rut: cleanReminder(x.rut),
      email: cleanReminder(x.email),
      telefono: cleanReminder(x.telefono),
      direccion: cleanReminder(x.direccion),

      // empresa
      giro: cleanReminder(x.giro),
      contactoEmpresa: cleanReminder(x.contactoEmpresa),

      rol: cleanReminder(x.rol) || 'MEDICO',
    });
  });

  // Orden por nombre/razón social
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));

  state.all = out;
  paint();
}

async function saveProfesional(){
  const tipoPersona = toUpperSafe($('tipoPersona').value || 'NATURAL'); // NATURAL/JURIDICA
  const nombre      = cleanReminder($('nombre').value);
  const rut         = cleanReminder($('rut').value);
  const email       = cleanReminder($('email').value);
  const telefono    = cleanReminder($('telefono').value);
  const direccion   = cleanReminder($('direccion').value);

  // empresa
  const giro            = cleanReminder($('giro').value);
  const contactoEmpresa = cleanReminder($('contactoEmpresa').value);

  const rol = toUpperSafe($('rol').value || 'MEDICO');

  if(!nombre){
    toast('Falta nombre / razón social');
    $('nombre').focus();
    return;
  }

  const payload = {
    tipoPersona,
    nombre,
    rut,
    email,
    telefono,
    direccion,

    // empresa
    giro,
    contactoEmpresa,

    rol,
    updatedAt: serverTimestamp(),
    updatedBy: state.user?.email || ''
  };

  if(state.editId){
    await updateDoc(doc(db,'profesionales',state.editId), payload);
    toast('Profesional actualizado');
  }else{
    payload.createdAt = serverTimestamp();
    payload.createdBy = state.user?.email || '';
    await addDoc(colRef, payload);
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
   UI
========================= */
function toggleEmpresaUI(){
  const tipo = ($('tipoPersona')?.value || 'NATURAL').toUpperCase();
  const show = (tipo === 'JURIDICA');
  const b1 = $('boxEmpresa');
  const b2 = $('boxEmpresa2');
  if(b1) b1.style.display = show ? '' : 'none';
  if(b2) b2.style.display = show ? '' : 'none';

  // Si no es jurídica, limpia campos de empresa para evitar basura accidental
  if(!show){
    if($('giro')) $('giro').value = '';
    if($('contactoEmpresa')) $('contactoEmpresa').value = '';
  }
}

function clearForm(){
  state.editId = null;

  $('tipoPersona').value = 'NATURAL';
  $('nombre').value = '';
  $('rut').value = '';
  $('email').value = '';
  $('telefono').value = '';
  $('direccion').value = '';
  $('giro').value = '';
  $('contactoEmpresa').value = '';
  $('rol').value = 'MEDICO';

  $('btnGuardar').textContent = 'Guardar profesional';
  toggleEmpresaUI();
}

function setForm(p){
  state.editId = p.id;

  $('tipoPersona').value = (p.tipoPersona || 'NATURAL');
  $('nombre').value = p.nombre || '';
  $('rut').value = p.rut || '';
  $('email').value = p.email || '';
  $('telefono').value = p.telefono || '';
  $('direccion').value = p.direccion || '';

  $('giro').value = p.giro || '';
  $('contactoEmpresa').value = p.contactoEmpresa || '';

  $('rol').value = (p.rol || 'MEDICO');

  $('btnGuardar').textContent = 'Actualizar profesional';
  toggleEmpresaUI();
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

    tr.innerHTML = `
      <td><span class="pill">${escapeHtml(labelTipo(p.tipoPersona))}</span></td>
      <td><b>${escapeHtml(p.nombre || '')}</b></td>
      <td>${escapeHtml(p.rut || '')}</td>
      <td>${escapeHtml(p.email || '')}</td>
      <td>${escapeHtml(p.telefono || '')}</td>
      <td><span class="pill">${escapeHtml(p.rol || '')}</span></td>
      <td></td>
    `;

    const td = tr.children[6];

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

/* =========================
   Import / Export
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
    'tipoPersona','nombre','rut','email','telefono','direccion','giro','contactoEmpresa','rol'
  ];

  const items = state.all.map(p=>({
    tipoPersona: p.tipoPersona || 'NATURAL',
    nombre: p.nombre || '',
    rut: p.rut || '',
    email: p.email || '',
    telefono: p.telefono || '',
    direccion: p.direccion || '',
    giro: p.giro || '',
    contactoEmpresa: p.contactoEmpresa || '',
    rol: p.rol || ''
  }));

  const csv = toCSV(headers, items);
  download(`profesionales_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

function plantillaCSV(){
  const csv =
`tipoPersona,nombre,rut,email,telefono,direccion,giro,contactoEmpresa,rol
NATURAL,Juan Pérez,12.345.678-9,jperez@correo.cl,+56912345678,Av. Siempre Viva 123,,,MEDICO
JURIDICA,Clínica X SpA,76.123.456-7,contacto@clinicax.cl,+56223456789,Providencia 456,Servicios médicos,María Soto,OTRO
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

  const iTipo = idx('tipoPersona');
  const iNombre = idx('nombre');
  const iRut = idx('rut');
  const iEmail = idx('email');
  const iTelefono = idx('telefono');
  const iDireccion = idx('direccion');
  const iGiro = idx('giro');
  const iContacto = idx('contactoEmpresa');
  const iRol = idx('rol');

  if(iNombre < 0){
    toast('CSV debe incluir columna: nombre');
    return;
  }

  // Upsert simple:
  // 1) Si hay rut y existe -> update
  // 2) si no, si hay email y existe -> update
  // 3) si no -> create
  const existing = [...state.all];
  let creates = 0, updates = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];

    const tipoPersona = toUpperSafe(iTipo>=0 ? row[iTipo] : 'NATURAL') || 'NATURAL';
    const nombre      = cleanReminder(row[iNombre] ?? '');
    const rut         = cleanReminder(iRut>=0 ? row[iRut] : '');
    const email       = cleanReminder(iEmail>=0 ? row[iEmail] : '');
    const telefono    = cleanReminder(iTelefono>=0 ? row[iTelefono] : '');
    const direccion   = cleanReminder(iDireccion>=0 ? row[iDireccion] : '');
    const giro        = cleanReminder(iGiro>=0 ? row[iGiro] : '');
    const contactoEmpresa = cleanReminder(iContacto>=0 ? row[iContacto] : '');
    const rol         = toUpperSafe(iRol>=0 ? row[iRol] : 'MEDICO') || 'MEDICO';

    if(!nombre){ skipped++; continue; }

    const match = existing.find(x =>
      (rut && x.rut && normalize(x.rut)===normalize(rut)) ||
      (email && x.email && normalize(x.email)===normalize(email))
    );

    const payload = {
      tipoPersona, nombre, rut, email, telefono, direccion,
      giro, contactoEmpresa,
      rol,
      updatedAt: serverTimestamp(),
      updatedBy: state.user?.email || ''
    };

    if(match){
      await updateDoc(doc(db,'profesionales',match.id), payload);
      updates++;
    }else{
      payload.createdAt = serverTimestamp();
      payload.createdBy = state.user?.email || '';
      await addDoc(colRef, payload);
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

    $('tipoPersona').addEventListener('change', toggleEmpresaUI);

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

    toggleEmpresaUI();
    await loadAll();
  }
});
