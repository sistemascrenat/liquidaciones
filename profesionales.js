// profesionales.js
// Profesionales: CRUD + buscar + importar/exportar CSV
// Firestore: colección "profesionales" (docId = rutId)
// Roles: colección "roles" (id = r_xxx, nombre = visible)
// Clínicas: colección "clinicas" (id = C001, C002, nombre = visible)
// Campos según tu Firestore real:
// - tipoPersona: "natural" | "juridica"
// - nombreProfesional, razonSocial (juridica), rut, rutId, rutEmpresa
// - correoPersonal, correoEmpresa (juridica), telefono
// - direccionEmpresa, ciudadEmpresa (juridica)
// - rolPrincipalId (string), rolesSecundariosIds (array ids)
// - clinicasIds (array ids)
// - tieneDescuento (bool), descuentoUF (number), descuentoRazon (string)
// - estado (activo/inactivo)
// - creadoEl/actualizadoEl (timestamps) + audit (creadoPor/actualizadoPor)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder, toUpperSafe, parseCSV, toCSV } from './utils.js';

import {
  collection, getDocs, doc, setDoc, updateDoc, deleteDoc,
  serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  q: '',
  editId: null,                 // rutId actual en edición
  all: [],                      // docs profesionales normalizados
  rolesCatalog: [],             // [{id, nombre}]
  rolesMap: new Map(),          // id -> nombre
  clinicasCatalog: [],          // [{id, nombre}]
  clinicasMap: new Map()        // id -> nombre
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
   Rut helpers (docId = rutId)
========================= */
function rutToRutId(rut){
  // deja solo dígitos (sin DV). Tu ejemplo: "16.128.922-1" => "161289221"
  // En tu DB rutId incluye DV? según tu captura rutId = "161289221" (incluye DV "1" al final)
  // Por tanto: dígitos + DV si DV es numérico, o K/k si necesitas:
  // Aquí mantendremos números y letra K.
  const raw = (rut ?? '').toString().trim();
  if(!raw) return '';
  return raw
    .replace(/\./g,'')
    .replace(/-/g,'')
    .replace(/\s+/g,'')
    .toLowerCase()
    .replace(/[^0-9k]/g,''); // permite k
}

function ensurePrincipalInsideSecondaries(rolPrincipalId, rolesSecundariosIds){
  const xs = Array.isArray(rolesSecundariosIds) ? [...rolesSecundariosIds] : [];
  if(rolPrincipalId && !xs.includes(rolPrincipalId)) xs.push(rolPrincipalId);
  return uniq(xs);
}

/* =========================
   Firestore refs
========================= */
const colProfesionales = collection(db, 'profesionales');
const colRoles = collection(db, 'roles');
const colClinicas = collection(db, 'clinicas');

/* =========================
   UI - Tipo persona toggles
========================= */
function syncTipoPersonaUI(){
  const tipo = (cleanReminder($('tipoPersona')?.value) || 'natural').toLowerCase();

  const showJ = (tipo === 'juridica');

  $('wrapRazonSocial').style.display = showJ ? '' : 'none';
  $('wrapRutEmpresa').style.display = showJ ? '' : 'none';
  $('wrapCorreoEmpresa').style.display = showJ ? '' : 'none';
  $('wrapDireccionEmpresa').style.display = showJ ? '' : 'none';
  $('wrapCiudadEmpresa').style.display = showJ ? '' : 'none';
}

function syncDescuentoUI(){
  const tiene = String($('tieneDescuento')?.value || 'false') === 'true';
  $('descuentoUF').disabled = !tiene;
  $('descuentoRazon').disabled = !tiene;

  if(!tiene){
    $('descuentoUF').value = '0';
    $('descuentoRazon').value = '';
  }
}

/* =========================
   Roles UI
========================= */
function paintRolPrincipalSelect(){
  const sel = $('rolPrincipalId');
  sel.innerHTML = '';

  if(!state.rolesCatalog.length){
    sel.innerHTML = `<option value="">(No hay roles en Firestore)</option>`;
    return;
  }

  sel.innerHTML = `<option value="">— Selecciona rol principal —</option>`;
  for(const r of state.rolesCatalog){
    const opt = document.createElement('option');
    opt.value = r.id;
    opt.textContent = r.nombre;
    sel.appendChild(opt);
  }
}

function paintRolesSecundariosPicker(){
  const wrap = $('rolesWrap');
  wrap.innerHTML = '';

  if(!state.rolesCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay roles creados. Ve al módulo <b>Roles</b> para crear.</div>`;
    return;
  }

  for(const r of state.rolesCatalog){
    const id = `role_${r.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';

    label.innerHTML = `
      <input type="checkbox" id="${id}" data-role-id="${escapeHtml(r.id)}"/>
      <span class="pill">${escapeHtml(r.nombre)}</span>
    `;
    wrap.appendChild(label);
  }
}

function getSelectedRolesSecundariosIds(){
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked){
      out.push(ch.getAttribute('data-role-id') || '');
    }
  });
  return uniq(out).filter(Boolean);
}

function setSelectedRolesSecundariosIds(ids){
  const wanted = new Set((ids || []).filter(Boolean));
  const wrap = $('rolesWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-role-id]');
  checks.forEach(ch=>{
    const id = ch.getAttribute('data-role-id') || '';
    ch.checked = wanted.has(id);
  });
}

/* =========================
   Clínicas UI
========================= */
function paintClinicasPicker(){
  const wrap = $('clinicasWrap');
  wrap.innerHTML = '';

  if(!state.clinicasCatalog.length){
    wrap.innerHTML = `<div class="muted">No hay clínicas creadas.</div>`;
    return;
  }

  for(const c of state.clinicasCatalog){
    const id = `clin_${c.id}`;
    const label = document.createElement('label');
    label.className = 'roleCheck';
    label.innerHTML = `
      <input type="checkbox" id="${id}" data-clin-id="${escapeHtml(c.id)}"/>
      <span class="pill">${escapeHtml(c.nombre || c.id)}</span>
    `;
    wrap.appendChild(label);
  }
}

function getSelectedClinicasIds(){
  const wrap = $('clinicasWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-clin-id]');
  const out = [];
  checks.forEach(ch=>{
    if(ch.checked) out.push(ch.getAttribute('data-clin-id') || '');
  });
  return uniq(out).filter(Boolean);
}

function setSelectedClinicasIds(ids){
  const wanted = new Set((ids || []).filter(Boolean));
  const wrap = $('clinicasWrap');
  const checks = wrap.querySelectorAll('input[type="checkbox"][data-clin-id]');
  checks.forEach(ch=>{
    const id = ch.getAttribute('data-clin-id') || '';
    ch.checked = wanted.has(id);
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
    // esperado: roles/{r_xxx} con { nombre: "CIRUJANO" }
    const nombre = cleanReminder(x.nombre) || cleanReminder(x.titulo) || d.id;
    out.push({ id: d.id, nombre: nombre });
  });

  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.rolesCatalog = out;
  state.rolesMap = new Map(out.map(r=>[r.id, r.nombre]));

  paintRolPrincipalSelect();
  paintRolesSecundariosPicker();
}

async function loadClinicas(){
  const snap = await getDocs(colClinicas);
  const out = [];

  snap.forEach(d=>{
    const x = d.data() || {};
    const nombre = cleanReminder(x.nombre) || cleanReminder(x.titulo) || d.id;
    out.push({ id: d.id, nombre });
  });

  out.sort((a,b)=> normalize(a.nombre).localeCompare(normalize(b.nombre)));
  state.clinicasCatalog = out;
  state.clinicasMap = new Map(out.map(c=>[c.id, c.nombre]));

  paintClinicasPicker();
}

function normalizeProfesionalDoc(id, x){
  const tipoPersona = (cleanReminder(x.tipoPersona) || 'natural').toLowerCase();
  const isJ = (tipoPersona === 'juridica');

  const rut = cleanReminder(x.rut);
  const rutId = cleanReminder(x.rutId) || id || rutToRutId(rut);

  const nombreProfesional = cleanReminder(x.nombreProfesional);
  const razonSocial = cleanReminder(x.razonSocial);

  const rolPrincipalId = cleanReminder(x.rolPrincipalId || '');
  const rolesSecundariosIds = Array.isArray(x.rolesSecundariosIds) ? x.rolesSecundariosIds.filter(Boolean) : [];

  const clinicasIds = Array.isArray(x.clinicasIds) ? x.clinicasIds.filter(Boolean) : [];

  const tieneDescuento = !!x.tieneDescuento;
  const descuentoUF = (x.descuentoUF === 0) ? 0 : (Number(x.descuentoUF) || 0);
  const descuentoRazon = cleanReminder(x.descuentoRazon || '');

  return {
    id: rutId || id,
    rutId,
    tipoPersona,
    estado: cleanReminder(x.estado || 'activo') || 'activo',

    nombreProfesional,
    razonSocial: isJ ? razonSocial : null,

    rut,
    rutEmpresa: isJ ? cleanReminder(x.rutEmpresa) : null,

    telefono: cleanReminder(x.telefono),
    correoPersonal: cleanReminder(x.correoPersonal),
    correoEmpresa: isJ ? cleanReminder(x.correoEmpresa) : null,

    direccionEmpresa: isJ ? cleanReminder(x.direccionEmpresa) : null,
    ciudadEmpresa: isJ ? cleanReminder(x.ciudadEmpresa) : null,

    rolPrincipalId,
    rolesSecundariosIds,

    clinicasIds,

    tieneDescuento,
    descuentoUF,
    descuentoRazon,

    creadoEl: x.creadoEl || null,
    actualizadoEl: x.actualizadoEl || null
  };
}

async function loadAll(){
  const snap = await getDocs(colProfesionales);
  const out = [];

  snap.forEach(d=>{
    out.push(normalizeProfesionalDoc(d.id, d.data() || {}));
  });

  // sort: jurídicas por razón social, naturales por nombre
  out.sort((a,b)=>{
    const an = (a.tipoPersona === 'juridica') ? (a.razonSocial || '') : (a.nombreProfesional || '');
    const bn = (b.tipoPersona === 'juridica') ? (b.razonSocial || '') : (b.nombreProfesional || '');
    return normalize(an).localeCompare(normalize(bn));
  });

  state.all = out;
  paint();
}

/* =========================
   Search
========================= */
function rowMatches(p, q){
  if(!q) return true;

  const principalName = state.rolesMap.get(p.rolPrincipalId) || p.rolPrincipalId || '';
  const secundariosNames = (p.rolesSecundariosIds || []).map(id=> state.rolesMap.get(id) || id).join(' ');
  const clinNames = (p.clinicasIds || []).map(id=> state.clinicasMap.get(id) || id).join(' ');

  const hay = normalize([
    p.tipoPersona,
    p.nombreProfesional,
    p.razonSocial,
    p.rut,
    p.telefono,
    p.correoPersonal,
    p.correoEmpresa,
    principalName,
    secundariosNames,
    clinNames
  ].join(' '));

  return hay.includes(q);
}

/* =========================
   Paint table
========================= */
function pill(text){
  return `<span class="pill">${escapeHtml(text)}</span>`;
}

function paint(){
  const q = state.q;
  const rows = state.all.filter(p=>rowMatches(p, q));

  $('count').textContent = `${rows.length} profesional${rows.length===1?'':'es'}`;

  const tb = $('tbody');
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');

    const isJ = (p.tipoPersona === 'juridica');

    const main = isJ ? (p.razonSocial || '—') : (p.nombreProfesional || '—');
    const sub  = isJ ? (p.nombreProfesional ? `Contacto: ${p.nombreProfesional}` : '') : '';

    const contacto = [
      p.telefono ? `📞 ${p.telefono}` : '',
      p.correoPersonal ? `✉️ ${p.correoPersonal}` : '',
      (isJ && p.correoEmpresa) ? `🏢 ${p.correoEmpresa}` : ''
    ].filter(Boolean).join('<br/>') || `<span class="muted">—</span>`;

    const principalRole = state.rolesMap.get(p.rolPrincipalId) || (p.rolPrincipalId || '—');

    const sec = (p.rolesSecundariosIds || []).length
      ? (p.rolesSecundariosIds || [])
          .map(id=> state.rolesMap.get(id) || id)
          .map(n=> pill(n))
          .join(' ')
      : `<span class="muted">—</span>`;

    const clin = (p.clinicasIds || []).length
      ? (p.clinicasIds || [])
          .map(id=> state.clinicasMap.get(id) || id)
          .map(n=> pill(n))
          .join(' ')
      : `<span class="muted">—</span>`;

    tr.innerHTML = `
      <td>
        <b>${escapeHtml(main)}</b>
        ${sub ? `<div class="muted" style="font-size:12px;margin-top:2px;">${escapeHtml(sub)}</div>` : ''}
        <div class="muted" style="font-size:12px;margin-top:4px;">
          ${escapeHtml(p.tipoPersona || '')} · ${escapeHtml(p.estado || '')}
          ${p.tieneDescuento ? ` · ${escapeHtml(String(p.descuentoUF || 0))} UF` : ''}
        </div>
      </td>
      <td>${escapeHtml(p.rut || '')}${isJ && p.rutEmpresa ? `<div class="muted" style="font-size:12px;margin-top:2px;">Emp: ${escapeHtml(p.rutEmpresa)}</div>` : ''}</td>
      <td>${contacto}</td>
      <td>${escapeHtml(principalRole)}</td>
      <td>${sec}</td>
      <td>${clin}</td>
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
   Form set/clear
========================= */
function clearForm(){
  state.editId = null;

  $('tipoPersona').value = 'natural';
  $('estado').value = 'activo';

  $('nombreProfesional').value = '';
  $('razonSocial').value = '';

  $('rut').value = '';
  $('rutEmpresa').value = '';

  $('telefono').value = '';
  $('correoPersonal').value = '';
  $('correoEmpresa').value = '';

  $('direccionEmpresa').value = '';
  $('ciudadEmpresa').value = '';

  $('rolPrincipalId').value = '';
  setSelectedRolesSecundariosIds([]);

  setSelectedClinicasIds([]);

  $('tieneDescuento').value = 'false';
  $('descuentoUF').value = '0';
  $('descuentoRazon').value = '';

  $('btnGuardar').textContent = 'Guardar profesional';

  syncTipoPersonaUI();
  syncDescuentoUI();
}

function setForm(p){
  state.editId = p.id;

  $('tipoPersona').value = (p.tipoPersona || 'natural');
  $('estado').value = (p.estado || 'activo');

  $('nombreProfesional').value = p.nombreProfesional || '';
  $('razonSocial').value = p.razonSocial || '';

  $('rut').value = p.rut || '';
  $('rutEmpresa').value = p.rutEmpresa || '';

  $('telefono').value = p.telefono || '';
  $('correoPersonal').value = p.correoPersonal || '';
  $('correoEmpresa').value = p.correoEmpresa || '';

  $('direccionEmpresa').value = p.direccionEmpresa || '';
  $('ciudadEmpresa').value = p.ciudadEmpresa || '';

  $('rolPrincipalId').value = p.rolPrincipalId || '';
  setSelectedRolesSecundariosIds(p.rolesSecundariosIds || []);

  setSelectedClinicasIds(p.clinicasIds || []);

  $('tieneDescuento').value = p.tieneDescuento ? 'true' : 'false';
  $('descuentoUF').value = String(p.descuentoUF ?? 0);
  $('descuentoRazon').value = p.descuentoRazon || '';

  $('btnGuardar').textContent = 'Actualizar profesional';

  syncTipoPersonaUI();
  syncDescuentoUI();

  $('nombreProfesional').focus();
}

/* =========================
   Save / Delete
========================= */
function validateBeforeSave(payload){
  if(!payload.nombreProfesional){
    toast('Falta nombre del profesional');
    $('nombreProfesional').focus();
    return false;
  }
  if(!payload.rut){
    toast('Falta RUT');
    $('rut').focus();
    return false;
  }
  if(!payload.rutId){
    toast('RUT inválido (no puedo calcular rutId)');
    $('rut').focus();
    return false;
  }
  if(payload.tipoPersona === 'juridica' && !payload.razonSocial){
    toast('Falta Razón Social (persona jurídica)');
    $('razonSocial').focus();
    return false;
  }
  if(!payload.rolPrincipalId){
    toast('Selecciona rol principal');
    $('rolPrincipalId').focus();
    return false;
  }
  return true;
}

async function saveProfesional(){
  const tipoPersona = (cleanReminder($('tipoPersona').value) || 'natural').toLowerCase();
  const isJ = (tipoPersona === 'juridica');

  const rut = cleanReminder($('rut').value);
  const rutId = rutToRutId(rut);

  const rolPrincipalId = cleanReminder($('rolPrincipalId').value);

  // roles secundarios (ids)
  let rolesSecundariosIds = getSelectedRolesSecundariosIds();
  // según tu ejemplo, el principal suele venir incluido:
  rolesSecundariosIds = ensurePrincipalInsideSecondaries(rolPrincipalId, rolesSecundariosIds);

  // clínicas
  const clinicasIds = getSelectedClinicasIds();

  // descuento
  const tieneDescuento = String($('tieneDescuento').value) === 'true';
  const descuentoUF = tieneDescuento ? (Number($('descuentoUF').value) || 0) : 0;
  const descuentoRazon = tieneDescuento ? cleanReminder($('descuentoRazon').value) : null;

  const payload = {
    tipoPersona,
    estado: cleanReminder($('estado').value) || 'activo',

    nombreProfesional: cleanReminder($('nombreProfesional').value),
    razonSocial: isJ ? cleanReminder($('razonSocial').value) : null,

    rut,
    rutId,

    rutEmpresa: isJ ? cleanReminder($('rutEmpresa').value) : null,

    telefono: cleanReminder($('telefono').value) || null,

    correoPersonal: cleanReminder($('correoPersonal').value) || null,
    correoEmpresa: isJ ? (cleanReminder($('correoEmpresa').value) || null) : null,

    direccionEmpresa: isJ ? (cleanReminder($('direccionEmpresa').value) || null) : null,
    ciudadEmpresa: isJ ? (cleanReminder($('ciudadEmpresa').value) || null) : null,

    rolPrincipalId,
    rolesSecundariosIds,

    clinicasIds,

    tieneDescuento,
    descuentoUF,
    descuentoRazon,

    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  if(!validateBeforeSave(payload)) return;

  const ref = doc(db, 'profesionales', rutId);

  if(state.editId){
    // update (merge)
    await updateDoc(ref, payload);
    toast('Profesional actualizado');
  }else{
    // create with fixed id rutId
    await setDoc(ref, {
      ...payload,
      creadoEl: serverTimestamp(),
      creadoPor: state.user?.email || ''
    }, { merge: true });
    toast('Profesional creado');
  }

  clearForm();
  await loadAll();
}

async function removeProfesional(id){
  const p = state.all.find(x=>x.id===id);
  const label = (p?.tipoPersona === 'juridica')
    ? (p?.razonSocial || p?.nombreProfesional || '')
    : (p?.nombreProfesional || '');

  const ok = confirm(`¿Eliminar profesional?\n\n${label}\n\nDocId (rutId): ${id}`);
  if(!ok) return;

  await deleteDoc(doc(db,'profesionales',id));
  toast('Eliminado');
  await loadAll();
}

/* =========================
   CSV Import / Export
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

function arrToCell(arr){
  return (arr || []).filter(Boolean).join(' | ');
}

function cellToArr(v){
  const raw = (v ?? '').toString().trim();
  if(!raw) return [];
  return uniq(
    raw.split('|')
      .flatMap(x=> x.split(';'))
      .flatMap(x=> x.split(','))
      .map(x=> cleanReminder(x))
      .filter(Boolean)
  );
}

function exportCSV(){
  const headers = [
    'rut','nombreProfesional','tipoPersona','razonSocial','rutEmpresa',
    'telefono','correoPersonal','correoEmpresa','direccionEmpresa','ciudadEmpresa',
    'estado',
    'rolPrincipalId','rolesSecundariosIds',
    'clinicasIds',
    'tieneDescuento','descuentoUF','descuentoRazon'
  ];

  const items = state.all.map(p=>({
    rut: p.rut || '',
    nombreProfesional: p.nombreProfesional || '',
    tipoPersona: p.tipoPersona || 'natural',
    razonSocial: p.razonSocial || '',
    rutEmpresa: p.rutEmpresa || '',
    telefono: p.telefono || '',
    correoPersonal: p.correoPersonal || '',
    correoEmpresa: p.correoEmpresa || '',
    direccionEmpresa: p.direccionEmpresa || '',
    ciudadEmpresa: p.ciudadEmpresa || '',
    estado: p.estado || 'activo',
    rolPrincipalId: p.rolPrincipalId || '',
    rolesSecundariosIds: arrToCell(p.rolesSecundariosIds || []),
    clinicasIds: arrToCell(p.clinicasIds || []),
    tieneDescuento: String(!!p.tieneDescuento),
    descuentoUF: String(p.descuentoUF ?? 0),
    descuentoRazon: p.descuentoRazon || ''
  }));

  const csv = toCSV(headers, items);
  download(`profesionales_${new Date().toISOString().slice(0,10)}.csv`, csv, 'text/csv');
  toast('CSV exportado');
}

function plantillaCSV(){
  const csv =
`rut,nombreProfesional,tipoPersona,razonSocial,rutEmpresa,telefono,correoPersonal,correoEmpresa,direccionEmpresa,ciudadEmpresa,estado,rolPrincipalId,rolesSecundariosIds,clinicasIds,tieneDescuento,descuentoUF,descuentoRazon
16.128.922-1,Ignacio Pastor,natural,,, +56952270713,nacho@correo.cl,,,,activo,r_cirujano,"r_asistente_cirujano | r_cirujano","C001 | C002",false,0,
17.315.517-4,Paloma Martinez,juridica,Ignovacion SPA,77.644.246-1,+56981406262,paloma@correo.com,pagos@empresa.cl,Monjitas 360,Santiago,activo,r_cirujano,"r_asistente_cirujano | r_cirujano","C001 | C002 | C003",false,0,
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
  const iNom = idx('nombreProfesional');
  const iTipo = idx('tipoPersona');
  const iRS = idx('razonSocial');
  const iRutEmp = idx('rutEmpresa');
  const iTel = idx('telefono');
  const iCP = idx('correoPersonal');
  const iCE = idx('correoEmpresa');
  const iDirE = idx('direccionEmpresa');
  const iCiuE = idx('ciudadEmpresa');
  const iEstado = idx('estado');
  const iRolP = idx('rolPrincipalId');
  const iRolesS = idx('rolesSecundariosIds');
  const iClin = idx('clinicasIds');
  const iTD = idx('tieneDescuento');
  const iDUF = idx('descuentoUF');
  const iDR = idx('descuentoRazon');

  if(iRut < 0 || iNom < 0){
    toast('CSV debe incluir columnas: rut, nombreProfesional');
    return;
  }

  let creates = 0, updates = 0, skipped = 0;

  for(let r=1; r<rows.length; r++){
    const row = rows[r];

    const rut = cleanReminder(row[iRut] ?? '');
    const rutId = rutToRutId(rut);

    const nombreProfesional = cleanReminder(row[iNom] ?? '');
    const tipoPersona = (cleanReminder(iTipo>=0 ? row[iTipo] : 'natural') || 'natural').toLowerCase();
    const isJ = (tipoPersona === 'juridica');

    const razonSocial = isJ ? cleanReminder(iRS>=0 ? row[iRS] : '') : null;
    const rutEmpresa = isJ ? cleanReminder(iRutEmp>=0 ? row[iRutEmp] : '') : null;

    const telefono = cleanReminder(iTel>=0 ? row[iTel] : '') || null;
    const correoPersonal = cleanReminder(iCP>=0 ? row[iCP] : '') || null;
    const correoEmpresa = isJ ? (cleanReminder(iCE>=0 ? row[iCE] : '') || null) : null;

    const direccionEmpresa = isJ ? (cleanReminder(iDirE>=0 ? row[iDirE] : '') || null) : null;
    const ciudadEmpresa = isJ ? (cleanReminder(iCiuE>=0 ? row[iCiuE] : '') || null) : null;

    const estado = cleanReminder(iEstado>=0 ? row[iEstado] : 'activo') || 'activo';

    const rolPrincipalId = cleanReminder(iRolP>=0 ? row[iRolP] : '');
    let rolesSecundariosIds = iRolesS>=0 ? cellToArr(row[iRolesS]) : [];
    rolesSecundariosIds = ensurePrincipalInsideSecondaries(rolPrincipalId, rolesSecundariosIds);

    const clinicasIds = iClin>=0 ? cellToArr(row[iClin]) : [];

    const tieneDescuento = String(cleanReminder(iTD>=0 ? row[iTD] : 'false')).toLowerCase() === 'true';
    const descuentoUF = tieneDescuento ? (Number(cleanReminder(iDUF>=0 ? row[iDUF] : '0')) || 0) : 0;
    const descuentoRazon = tieneDescuento ? (cleanReminder(iDR>=0 ? row[iDR] : '') || null) : null;

    if(!rut || !rutId || !nombreProfesional){
      skipped++;
      continue;
    }
    if(isJ && !razonSocial){
      skipped++;
      continue;
    }
    if(!rolPrincipalId){
      skipped++;
      continue;
    }

    const payload = {
      tipoPersona,
      estado,

      rut,
      rutId,

      nombreProfesional,
      razonSocial,

      rutEmpresa,

      telefono,
      correoPersonal,
      correoEmpresa,

      direccionEmpresa,
      ciudadEmpresa,

      rolPrincipalId,
      rolesSecundariosIds,

      clinicasIds,

      tieneDescuento,
      descuentoUF,
      descuentoRazon,

      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    };

    const exists = state.all.some(p=> p.id === rutId);

    if(exists){
      await updateDoc(doc(db,'profesionales',rutId), payload);
      updates++;
    }else{
      await setDoc(doc(db,'profesionales',rutId), {
        ...payload,
        creadoEl: serverTimestamp(),
        creadoPor: state.user?.email || ''
      }, { merge: true });
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

    // Listeners base
    $('btnGuardar').addEventListener('click', saveProfesional);
    $('btnLimpiar').addEventListener('click', clearForm);

    $('tipoPersona').addEventListener('change', syncTipoPersonaUI);
    $('tieneDescuento').addEventListener('change', syncDescuentoUI);

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

    // Load catalogs first
    await loadRoles();
    await loadClinicas();
    await loadAll();

    // default UI
    syncTipoPersonaUI();
    syncDescuentoUI();
    clearForm();
  }
});
