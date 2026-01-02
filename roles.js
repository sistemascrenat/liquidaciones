import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast } from './ui.js';
import { cleanReminder, parseCSV, toCSV } from './utils.js';

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
  editId: null,  // docId (r_...)
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

function slugRoleIdFromName(nombre){
  const n = normalize(nombre)
    .replace(/[^a-z0-9]+/g,'_')
    .replace(/^_+|_+$/g,'');
  return `r_${n || ''}`.replace(/_+/g,'_');
}

function labelAplicaA(v){
  const x = (v || 'ambos').toString().toLowerCase();
  if(x === 'cirugias') return 'Cirugías';
  if(x === 'ambulatorios') return 'Ambulatorios';
  return 'Ambos';
}

/* =========================
   Firestore refs
========================= */
const colRoles = collection(db, 'roles');

/* =========================
   Load
========================= */
function normalizeRoleDoc(docId, x){
  return {
    idDoc: docId,
    id: cleanReminder(x.id) || docId,
    nombre: cleanReminder(x.nombre) || '',
    estado: (cleanReminder(x.estado) || 'activo').toLowerCase(),
    aplicaA: (cleanReminder(x.aplicaA) || 'ambos').toLowerCase(),
    nota: cleanReminder(x.nota) || ''
  };
}

async function loadAll(){
  const snap = await getDocs(colRoles);
  const out = [];
  snap.forEach(d=>{
    out.push(normalizeRoleDoc(d.id, d.data() || {}));
  });

  out.sort((a,b)=>{
    if(a.estado !== b.estado){
      if(a.estado === 'activo') return -1;
      if(b.estado === 'activo') return 1;
    }
    return normalize(a.nombre).localeCompare(normalize(b.nombre));
  });

  state.all = out;
  paint();
}

/* =========================
   Search ("," AND) + ("-" OR)
========================= */
function parseQuery(raw){
  return (raw || '')
    .toString()
    .split(',')
    .map(part => part.trim())
    .filter(Boolean)
    .map(part => part
      .split('-')
      .map(t => normalize(t))
      .filter(Boolean)
    )
    .filter(group => group.length);
}

function rowMatches(r, rawQuery){
  const groups = parseQuery(rawQuery);
  if(!groups.length) return true;

  const hay = normalize([
    r.idDoc, r.id,
    r.nombre,
    r.estado,
    r.aplicaA,
    labelAplicaA(r.aplicaA),
    r.nota
  ].join(' '));

  return groups.every(orTerms => orTerms.some(t => hay.includes(t)));
}

/* =========================
   Modal
========================= */
function openModal(mode, role=null){
  $('modalBackdrop').style.display = 'grid';

  if(mode === 'create'){
    state.editId = null;
    $('modalTitle').textContent = 'Crear rol';
    $('modalSub').textContent = 'Completa los datos y guarda.';
    clearForm();
  }else{
    state.editId = role?.idDoc || null;
    $('modalTitle').textContent = 'Editar rol';
    $('modalSub').textContent = state.editId ? `ID: ${state.editId}` : '';
    setForm(role);
  }

  refreshIdSugerido();
  $('nombre').focus();
}

function closeModal(){
  $('modalBackdrop').style.display = 'none';
}

function clearForm(){
  $('nombre').value = '';
  $('aplicaA').value = 'ambos';
  $('estado').value = 'activo';
  $('nota').value = '';
}

function setForm(r){
  $('nombre').value = r?.nombre || '';
  $('aplicaA').value = (r?.aplicaA || 'ambos');
  $('estado').value = (r?.estado || 'activo');
  $('nota').value = r?.nota || '';
}

function refreshIdSugerido(){
  const nombre = $('nombre').value || '';
  const sug = slugRoleIdFromName(nombre);
  $('idSugerido').textContent = state.editId ? state.editId : sug;
}

/* =========================
   Save / Delete
========================= */
async function saveRole(){
  const nombre = cleanReminder($('nombre').value);
  const estado = (cleanReminder($('estado').value) || 'activo').toLowerCase();
  const aplicaA = (cleanReminder($('aplicaA').value) || 'ambos').toLowerCase();
  const nota = cleanReminder($('nota').value);

  if(!nombre){
    toast('Falta nombre del rol');
    $('nombre').focus();
    return;
  }

  const isEdit = !!state.editId;
  const docId = isEdit ? state.editId : slugRoleIdFromName(nombre);

  if(!docId || docId === 'r_'){
    toast('No pude generar el ID del rol');
    return;
  }

  if(!isEdit){
    const exists = state.all.some(x => x.idDoc === docId);
    if(exists){
      toast(`Ya existe el rol: ${docId}`);
      return;
    }
  }

  const normNombre = normalize(nombre);
  const dupName = state.all.some(x =>
    normalize(x.nombre) === normNombre &&
    x.idDoc !== docId
  );
  if(dupName){
    toast('Ya existe un rol con ese nombre');
    return;
  }

  const payload = {
    id: docId,
    nombre,
    estado,
    aplicaA,
    nota: nota || null,

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  if(!isEdit){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  await setDoc(doc(db,'roles',docId), payload, { merge:true });

  toast(isEdit ? 'Rol actualizado' : 'Rol creado');
  closeModal();
  await loadAll();
}

async function removeRole(docId){
  const r = state.all.find(x=>x.idDoc === docId);
  const ok = confirm(`¿Eliminar rol?\n\n${r?.nombre || docId}\n${docId}`);
  if(!ok) return;

  await deleteDoc(doc(db,'roles',docId));
  toast('Rol eliminado');
  await loadAll();
}

/* =========================
   Paint
========================= */
function estadoHtml(est){
  const e = (est || 'activo').toLowerCase();
  return `<span class="state ${e}">${escapeHtml(e.toUpperCase())}</span>`;
}

function roleCell(r){
  return `
    <div class="cellBlock">
      <div class="cellTitle">${escapeHtml(r.nombre || '—')}</div>
      <div class="cellSub">
        <span class="muted">ID:</span> <span class="mono"><b>${escapeHtml(r.idDoc)}</b></span>
        <span class="dot">·</span>
        <span class="muted">APLICA A:</span> <b>${escapeHtml(labelAplicaA(r.aplicaA))}</b>
      </div>
      ${r.nota ? `<div class="mini" style="margin-top:4px;">${escapeHtml(r.nota)}</div>` : `<div class="mini muted" style="margin-top:4px;">&nbsp;</div>`}
    </div>
  `;
}

function paint(){
  const rows = state.all.filter(r => rowMatches(r, state.q));
  $('count').textContent = `${rows.length} rol${rows.length===1?'':'es'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const r of rows){
    const tr = document.createElement('tr');

    tr.innerHTML = `
      <td>${roleCell(r)}</td>
      <td><b>${escapeHtml(labelAplicaA(r.aplicaA))}</b></td>
      <td>${estadoHtml(r.estado)}</td>
      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openModal('edit', r));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeRole(r.idDoc));

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

function plantillaCSV(){
  const csv =
`id,nombre,aplicaA,estado,nota
r_cirujano,Cirujano,cirugias,activo,
r_anestesista,Anestesista,cirugias,activo,
r_kinesiologo,Kinesiólogo,ambulatorios,activo,
`;
  download('plantilla_roles.csv', csv, 'text/csv');
  toast('Plantilla descargada');
}

function exportCSV(){
  const headers = ['id','nombre','aplicaA','estado','nota'];
  const items = state.all.map(r => ({
    id: r.idDoc,
    nombre: r.nombre || '',
    aplicaA: r.aplicaA || 'ambos',
    estado: r.estado || 'activo',
    nota: r.nota || ''
  }));

  const csv = toCSV(headers, items);
  download(`roles_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
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
    id: idx('id'),
    nombre: idx('nombre'),
    aplicaA: idx('aplicaa'),
    estado: idx('estado'),
    nota: idx('nota')
  };

  if(I.nombre < 0){
    toast('CSV debe incluir al menos: nombre');
    return;
  }

  let upserts = 0, skipped = 0;

  for(let r=1; r<rows.length; r++){
    const row = rows[r];

    const nombre = cleanReminder(row[I.nombre] ?? '');
    if(!nombre){ skipped++; continue; }

    const docId = cleanReminder(I.id >= 0 ? row[I.id] : '') || slugRoleIdFromName(nombre);
    if(!docId || docId === 'r_'){ skipped++; continue; }

    const payload = {
      id: docId,
      nombre,
      aplicaA: (cleanReminder(I.aplicaA>=0 ? row[I.aplicaA] : 'ambos') || 'ambos').toLowerCase(),
      estado: (cleanReminder(I.estado>=0 ? row[I.estado] : 'activo') || 'activo').toLowerCase(),
      nota: (cleanReminder(I.nota>=0 ? row[I.nota] : '') || null),

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    };

    await setDoc(doc(db,'roles',docId), payload, { merge:true });
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

    // sidebar viene desde layout.js; solo pintamos who y activamos el link roles como fallback
    const who = document.getElementById('who');
    if(who) who.textContent = `Conectado: ${user.email}`;

    try{
      const a = document.querySelector('[data-nav="roles"]');
      if(a) a.classList.add('active');
    }catch(_){}

    // Modal
    $('btnCrear').addEventListener('click', ()=> openModal('create'));
    $('btnModalClose').addEventListener('click', closeModal);
    $('btnModalCancelar').addEventListener('click', closeModal);
    $('btnModalGuardar').addEventListener('click', saveRole);

    $('modalBackdrop').addEventListener('click', (e)=>{
      if(e.target === $('modalBackdrop')) closeModal();
    });

    $('nombre').addEventListener('input', refreshIdSugerido);

    // Search
    $('buscador').addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    // CSV
    $('btnExportar').addEventListener('click', exportCSV);
    $('btnDescargarPlantilla').addEventListener('click', plantillaCSV);

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

    await loadAll();
  }
});
