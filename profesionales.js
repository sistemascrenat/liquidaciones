// profesionales.js
// Módulo 1 completo: CRUD + buscar + importar/exportar CSV
// Firestore: colección "profesionales"

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
  all: [],        // [{id, nombre, rut, email, rol}]
  editId: null,   // id en edición
  q: ''
};

const $ = (id)=> document.getElementById(id);

function normalize(s=''){
  return (s ?? '').toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim();
}

function rowMatches(p, q){
  if(!q) return true;
  const hay = normalize(`${p.nombre} ${p.rut} ${p.email} ${p.rol}`);
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
      nombre: cleanReminder(x.nombre),
      rut: cleanReminder(x.rut),
      email: cleanReminder(x.email),
      rol: cleanReminder(x.rol) || 'MEDICO',
    });
  });
  // orden por nombre
  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.all = out;
  paint();
}

async function saveProfesional(){
  const nombre = cleanReminder($('nombre').value);
  const rut    = cleanReminder($('rut').value);
  const email  = cleanReminder($('email').value);
  const rol    = toUpperSafe($('rol').value || 'MEDICO');

  if(!nombre){
    toast('Falta nombre');
    $('nombre').focus();
    return;
  }

  const payload = {
    nombre,
    rut,
    email,
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
function clearForm(){
  state.editId = null;
  $('nombre').value = '';
  $('rut').value = '';
  $('email').value = '';
  $('rol').value = 'MEDICO';
  $('btnGuardar').textContent = 'Guardar profesional';
}

function setForm(p){
  state.editId = p.id;
  $('nombre').value = p.nombre;
  $('rut').value = p.rut;
  $('email').value = p.email;
  $('rol').value = (p.rol || 'MEDICO');
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

    tr.innerHTML = `
      <td><b>${escapeHtml(p.nombre)}</b></td>
      <td>${escapeHtml(p.rut || '')}</td>
      <td>${escapeHtml(p.email || '')}</td>
      <td><span class="pill">${escapeHtml(p.rol || '')}</span></td>
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
  const headers = ['nombre','rut','email','rol'];
  const items = state.all.map(p=>({
    nombre: p.nombre || '',
    rut: p.rut || '',
    email: p.email || '',
    rol: p.rol || ''
  }));
  const csv = toCSV(headers, items);
  download(`profesionales_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

function plantillaCSV(){
  const csv = `nombre,rut,email,rol
Juan Pérez,12.345.678-9,jperez@correo.cl,MEDICO
María Soto,11.111.111-1,msoto@correo.cl,AUXILIAR
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
  const iRol    = idx('rol');

  if(iNombre < 0){
    toast('CSV debe incluir columna: nombre');
    return;
  }

  // estrategia simple: upsert por (rut) si existe, si no por (email), si no crea.
  const existing = [...state.all];

  let creates = 0, updates = 0, skipped = 0;

  for(let r=1;r<rows.length;r++){
    const row = rows[r];
    const nombre = cleanReminder(row[iNombre] ?? '');
    const rut    = cleanReminder(iRut>=0 ? row[iRut] : '');
    const email  = cleanReminder(iEmail>=0 ? row[iEmail] : '');
    const rol    = toUpperSafe(iRol>=0 ? row[iRol] : 'MEDICO') || 'MEDICO';

    if(!nombre){ skipped++; continue; }

    const match = existing.find(x =>
      (rut && x.rut && normalize(x.rut)===normalize(rut)) ||
      (email && x.email && normalize(x.email)===normalize(email))
    );

    const payload = {
      nombre, rut, email, rol,
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

    await loadAll();
  }
});
