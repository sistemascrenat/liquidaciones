// profesionales.js
// Gestión de profesionales (CRUD) — alineado con dashboard viejo:
// - tipoPersona natural/juridica + datos empresa
// - descuentos UF + razón
// - clínicas asignadas (checklist, default: todas activas al crear)
// - listado con buscador, hint, limpiar
// - editar / activar-desactivar / eliminar
// - NO incluye administración/selección de roles (eso queda para otro módulo)

import { app, auth, db } from './firebase-init.js';
import {
  onAuthStateChanged,
  signOut
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js";

import {
  collection,
  getDocs,
  getDoc,
  doc,
  setDoc,
  deleteDoc,
  query,
  where,
  serverTimestamp
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js";

/* =========================
   DOM
========================= */
const pillUser = document.getElementById('pillUser');
const btnLogout = document.getElementById('btnLogout');

const btnNuevoProfesional = document.getElementById('btnNuevoProfesional');

const tablaProfesionales = document.getElementById('tablaProfesionales');

const profSearch = document.getElementById('profSearch');
const profSearchClear = document.getElementById('profSearchClear');
const profSearchHint = document.getElementById('profSearchHint');

const modalOverlay = document.getElementById('modalOverlay');
const modalProfesional = document.getElementById('modalProfesional');

const btnCerrarModalProf = document.getElementById('btnCerrarModalProf');
const btnCancelarModalProf = document.getElementById('btnCancelarModalProf');
const btnGuardarModalProf = document.getElementById('btnGuardarModalProf');

const mProfTitle = document.getElementById('modalProfTitle');
const mProfSubtitle = document.getElementById('modalProfSubtitle');
const mProfError = document.getElementById('mProfError');

const mProfRut = document.getElementById('mProfRut');
const mProfNombre = document.getElementById('mProfNombre');

const mProfTipoPersona = document.getElementById('mProfTipoPersona');
const mProfEstado = document.getElementById('mProfEstado');

const mProfCorreoPersonal = document.getElementById('mProfCorreoPersonal');
const mProfTelefono = document.getElementById('mProfTelefono');

const mProfDireccion = document.getElementById('mProfDireccion');

const empresaFields = document.getElementById('empresaFields');
const mProfRutEmpresa = document.getElementById('mProfRutEmpresa');
const mProfRazon = document.getElementById('mProfRazon');
const mProfGiro = document.getElementById('mProfGiro');
const mProfCorreoEmpresa = document.getElementById('mProfCorreoEmpresa');
const mProfDireccionEmpresa = document.getElementById('mProfDireccionEmpresa');
const mProfCiudadEmpresa = document.getElementById('mProfCiudadEmpresa');

const mProfClinicas = document.getElementById('mProfClinicas');

const mProfTieneDesc = document.getElementById('mProfTieneDesc');
const mProfDescMonto = document.getElementById('mProfDescMonto');
const mProfDescRazon = document.getElementById('mProfDescRazon');

/* =========================
   State
========================= */
const state = {
  user: null,
  profesionalesCache: [],
  cacheClinicas: [], // [{id,nombre,estado}]
  modal: {
    mode: 'create', // create|edit
    docId: null
  }
};

/* =========================
   Helpers
========================= */
function showError(el, msg) {
  if (!el) return;
  el.textContent = msg || '';
  el.style.display = msg ? 'block' : 'none';
}

function lockScroll(lock) {
  document.body.style.overflow = lock ? 'hidden' : '';
}

function openOverlay() {
  modalOverlay.style.display = 'flex';
  lockScroll(true);
}

function closeAllModals() {
  modalOverlay.style.display = 'none';
  modalProfesional.style.display = 'none';
  lockScroll(false);
  showError(mProfError, '');
}

modalOverlay?.addEventListener('click', (e) => {
  if (e.target === modalOverlay) closeAllModals();
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && modalOverlay?.style.display === 'flex') closeAllModals();
});

btnCerrarModalProf?.addEventListener('click', closeAllModals);
btnCancelarModalProf?.addEventListener('click', closeAllModals);

btnLogout?.addEventListener('click', async () => {
  await signOut(auth);
});

// Normaliza RUT para usarlo como docId
function normalizaRut(rutRaw = '') {
  return rutRaw
    .toString()
    .trim()
    .replace(/\./g, '')
    .replace(/-/g, '')
    .replace(/\s+/g, '')
    .toUpperCase();
}

function normTxt(s='') {
  return (s ?? '')
    .toString()
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .trim();
}

function splitTokens(q='') {
  return normTxt(q)
    .split(',')
    .flatMap(part => part.trim().split(/\s+/))
    .filter(Boolean);
}

function parseUFInput(v='') {
  // permite "0,5" o "0.5"
  const s = (v || '').toString().trim().replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function formatUF(n=0) {
  const x = Number(n) || 0;
  return x.toLocaleString('es-CL', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

function applyTipoPersonaUI() {
  const tipo = (mProfTipoPersona?.value || 'natural').trim().toLowerCase();
  const isJ = (tipo === 'juridica');
  empresaFields.style.display = isJ ? 'block' : 'none';
}

mProfTipoPersona?.addEventListener('change', applyTipoPersonaUI);

// UX: al destildar descuento, limpia campos
mProfTieneDesc?.addEventListener('change', () => {
  if (!mProfTieneDesc.checked) {
    mProfDescMonto.value = '';
    mProfDescRazon.value = '';
  }
});

// Checklist genérico
function renderChecklist(containerEl, items, selectedIds=[]) {
  if (!containerEl) return;
  const set = new Set(selectedIds || []);
  containerEl.innerHTML = '';

  if (!items.length) {
    containerEl.innerHTML = `<div style="font-size:12px;color:var(--muted);">No hay clínicas cargadas.</div>`;
    return;
  }

  for (const it of items) {
    const id = it.id;
    const label = it.nombre || it.id;

    const row = document.createElement('label');
    row.className = 'check-row';

    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = id;
    cb.checked = set.has(id);

    const span = document.createElement('span');
    span.textContent = label;

    row.appendChild(cb);
    row.appendChild(span);
    containerEl.appendChild(row);
  }
}

function getCheckedValues(containerEl) {
  if (!containerEl) return [];
  return Array.from(containerEl.querySelectorAll('input[type="checkbox"]'))
    .filter(x => x.checked)
    .map(x => x.value);
}

/* =========================
   Loaders
========================= */
async function ensureClinicasLoaded() {
  if (state.cacheClinicas.length) return;

  const snap = await getDocs(collection(db, 'clinicas'));
  const clin = [];
  snap.forEach(d => clin.push({ id: d.id, ...d.data() }));
  clin.sort((a,b) => (a.nombre || a.id).localeCompare((b.nombre || b.id), 'es'));

  state.cacheClinicas = clin;
}

function getClinicasActivasIds() {
  return (state.cacheClinicas || [])
    .filter(c => (c.estado || 'activa') === 'activa')
    .map(c => c.id);
}

function clinicasText(ids=[]) {
  const byId = new Map((state.cacheClinicas || []).map(c => [c.id, (c.nombre || c.id)]));
  return (ids || []).map(id => byId.get(id) || id).join(' | ');
}

function displayNombre(p) {
  // ✅ robusto (corrige “no está leyendo nombre / razón social”)
  // prioriza nombreProfesional; si no, usa nombre; si es jurídica, usa razonSocial; fallback a rut
  return (
    p.nombreProfesional ||
    p.nombre ||
    p.razonSocial ||
    p.nombreEmpresa ||
    p.razon_social ||
    p.rut ||
    '—'
  );
}

async function loadProfesionales() {
  try {
    await ensureClinicasLoaded();

    const snap = await getDocs(collection(db, 'profesionales'));
    const rows = [];
    snap.forEach(d => rows.push({ id: d.id, ...d.data() }));

    state.profesionalesCache = rows;

    const q = profSearch?.value || '';
    const visibles = filtrarProfesionales(rows, q);

    renderProfesionalesTable(visibles);

    if (profSearchHint) {
      profSearchHint.textContent = (q.trim())
        ? `Mostrando ${visibles.length} de ${rows.length}.`
        : `Total: ${rows.length}.`;
    }
    if (profSearchClear) {
      profSearchClear.style.display = (q.trim()) ? 'inline-flex' : 'none';
    }

  } catch (err) {
    console.error('Error cargando profesionales:', err);
    tablaProfesionales.innerHTML = `<p style="color:#e11d48;font-size:13px;">
      Error al cargar profesionales. Revisa consola.
    </p>`;
  }
}

function filtrarProfesionales(rows, q) {
  const tokens = splitTokens(q);
  if (!tokens.length) return rows;

  return rows.filter(p => {
    const tipo = (p.tipoPersona || 'natural');
    const hay = normTxt([
      displayNombre(p),
      p.rut, p.id, p.rutId,
      p.rutEmpresa,
      p.razonSocial,
      p.giro,
      p.correoPersonal, p.correoEmpresa,
      p.telefono,
      tipo,
      p.estado,
      // clinicas
      clinicasText(p.clinicasIds || [])
    ].filter(Boolean).join(' | '));

    return tokens.every(t => hay.includes(t));
  });
}

/* =========================
   Table render
========================= */
function renderProfesionalesTable(rows) {
  if (!tablaProfesionales) return;

  if (!rows?.length) {
    tablaProfesionales.innerHTML = `
      <p style="font-size:13px;color:var(--muted);">
        No hay resultados para tu búsqueda.
      </p>`;
    return;
  }

  const htmlRows = rows.map(p => {
    const tipo = (p.tipoPersona || 'natural').toLowerCase();
    const isJ  = (tipo === 'juridica');
    const estado = (p.estado || 'activo');

    const desc = p.tieneDescuento
      ? `${formatUF(p.descuentoUF ?? 0)} UF (${p.descuentoRazon || '—'})`
      : 'No';

    const nombre = displayNombre(p);
    const docId = p.rutId || p.id; // en tu modelo antiguo rutId suele existir

    return `
      <tr>
        <td>${escapeHtml(nombre)}</td>
        <td>${escapeHtml(p.rut || '—')}</td>

        <td>${isJ ? escapeHtml(p.rutEmpresa || '—') : '—'}</td>
        <td>${isJ ? escapeHtml(p.razonSocial || '—') : '—'}</td>

        <td>${escapeHtml(tipo)}</td>
        <td>${escapeHtml(p.correoPersonal || p.correoEmpresa || '—')}</td>
        <td>${escapeHtml(p.telefono || '—')}</td>

        <td>${escapeHtml(desc)}</td>
        <td>${escapeHtml(estado)}</td>

        <td class="text-right">
          <button class="btn btn-soft" data-action="edit-prof" data-id="${escapeHtml(docId)}">Editar</button>
          <button class="btn btn-soft" data-action="toggle-prof" data-id="${escapeHtml(docId)}">
            ${estado === 'inactivo' ? 'Activar' : 'Desactivar'}
          </button>
          <button class="btn btn-soft" data-action="del-prof" data-id="${escapeHtml(docId)}">Eliminar</button>
        </td>
      </tr>
    `;
  }).join('');

  tablaProfesionales.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Nombre</th>
          <th>RUT</th>
          <th>RUT empresa</th>
          <th>Razón social</th>
          <th>Tipo</th>
          <th>Correo</th>
          <th>Teléfono</th>
          <th>Descuento</th>
          <th>Estado</th>
          <th class="text-right">Acciones</th>
        </tr>
      </thead>
      <tbody>${htmlRows}</tbody>
    </table>
  `;
}

tablaProfesionales?.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;

  const action = btn.dataset.action;
  const id = btn.dataset.id;

  try {
    if (action === 'edit-prof') {
      await openModalProfesionalEdit(id);
      return;
    }

    if (action === 'toggle-prof') {
      const ref = doc(db, 'profesionales', id);
      const snap = await getDoc(ref);
      if (!snap.exists()) return alert('Profesional no encontrado.');

      const estado = (snap.data().estado || 'activo');
      const nuevoEstado = (estado === 'inactivo') ? 'activo' : 'inactivo';

      await setDoc(ref, { estado: nuevoEstado, actualizadoEl: serverTimestamp() }, { merge: true });
      await loadProfesionales();
      return;
    }

    if (action === 'del-prof') {
      const ok = confirm(
        '¿Eliminar profesional DEFINITIVAMENTE?\n' +
        'Esto borra el registro y NO se puede recuperar.'
      );
      if (!ok) return;

      await deleteDoc(doc(db, 'profesionales', id));
      await loadProfesionales();
      return;
    }
  } catch (err) {
    console.error(err);
    alert('Error ejecutando acción. Revisa consola.');
  }
});

/* =========================
   Modal: open create/edit
========================= */
async function openModalProfesionalCreate() {
  await ensureClinicasLoaded();

  state.modal.mode = 'create';
  state.modal.docId = null;

  openOverlay();
  modalProfesional.style.display = 'block';

  mProfTitle.textContent = 'Nuevo profesional';
  mProfSubtitle.textContent = 'Crear un nuevo profesional';
  showError(mProfError, '');

  // limpiar
  mProfRut.disabled = false;
  mProfRut.value = '';
  mProfNombre.value = '';
  mProfTipoPersona.value = 'natural';
  mProfEstado.value = 'activo';

  mProfCorreoPersonal.value = '';
  mProfTelefono.value = '';
  mProfDireccion.value = '';

  mProfRutEmpresa.value = '';
  mProfRazon.value = '';
  mProfGiro.value = '';
  mProfCorreoEmpresa.value = '';
  mProfDireccionEmpresa.value = '';
  mProfCiudadEmpresa.value = '';

  // clínicas: default = todas activas
  const activas = getClinicasActivasIds();
  renderChecklist(mProfClinicas, state.cacheClinicas, activas);

  // descuento
  mProfTieneDesc.checked = false;
  mProfDescMonto.value = '';
  mProfDescRazon.value = '';

  applyTipoPersonaUI();

  setTimeout(() => mProfRut?.focus(), 50);
}

async function openModalProfesionalEdit(docId) {
  await ensureClinicasLoaded();

  state.modal.mode = 'edit';
  state.modal.docId = docId;

  openOverlay();
  modalProfesional.style.display = 'block';

  mProfTitle.textContent = 'Editar profesional';
  mProfSubtitle.textContent = `ID: ${docId}`;
  showError(mProfError, '');

  const ref = doc(db, 'profesionales', docId);
  const snap = await getDoc(ref);

  if (!snap.exists()) {
    showError(mProfError, 'No se encontró el profesional. Refresca e intenta de nuevo.');
    return;
  }

  const data = { id: snap.id, ...snap.data() };

  // poblar
  mProfRut.value = data.rut || '';
  mProfRut.disabled = true;

  mProfNombre.value = data.nombreProfesional || data.nombre || '';
  mProfTipoPersona.value = (data.tipoPersona || 'natural').toLowerCase();
  mProfEstado.value = (data.estado || 'activo');

  mProfCorreoPersonal.value = data.correoPersonal || '';
  mProfTelefono.value = data.telefono || '';
  mProfDireccion.value = data.direccion || '';

  mProfRutEmpresa.value = data.rutEmpresa || '';
  mProfRazon.value = data.razonSocial || '';
  mProfGiro.value = data.giro || '';
  mProfCorreoEmpresa.value = data.correoEmpresa || '';
  mProfDireccionEmpresa.value = data.direccionEmpresa || '';
  mProfCiudadEmpresa.value = data.ciudadEmpresa || '';

  const clinicasIds = data.clinicasIds || [];
  renderChecklist(mProfClinicas, state.cacheClinicas, clinicasIds);

  mProfTieneDesc.checked = !!data.tieneDescuento;
  mProfDescMonto.value = data.tieneDescuento ? formatUF(data.descuentoUF || 0) : '';
  mProfDescRazon.value = data.tieneDescuento ? (data.descuentoRazon || '') : '';

  applyTipoPersonaUI();

  setTimeout(() => mProfNombre?.focus(), 50);
}

/* =========================
   Modal: save
========================= */
btnGuardarModalProf?.addEventListener('click', async () => {
  try {
    showError(mProfError, '');

    const rutRaw = (mProfRut.value || '').trim();
    const rutId = normalizaRut(rutRaw);

    const nombre = (mProfNombre.value || '').trim();

    if (!rutId) { showError(mProfError, 'RUT inválido.'); return; }
    if (!nombre) { showError(mProfError, 'Nombre profesional es obligatorio.'); return; }

    const docId = (state.modal.mode === 'edit') ? state.modal.docId : rutId;

    const tipoPersona = (mProfTipoPersona.value || 'natural').trim().toLowerCase();
    const isJuridica = (tipoPersona === 'juridica');

    const clinicasIds = getCheckedValues(mProfClinicas);

    const tieneDescuento = !!mProfTieneDesc.checked;
    const descuentoUF = tieneDescuento ? parseUFInput(mProfDescMonto.value) : 0;
    const descuentoRazon = tieneDescuento ? (mProfDescRazon.value || '').trim() : '';

    // Validación mínima jurídica
    if (isJuridica) {
      const rutEmp = (mProfRutEmpresa.value || '').trim();
      const razon  = (mProfRazon.value || '').trim();
      if (!rutEmp && !razon) {
        showError(mProfError, 'Si es Persona jurídica, ingresa al menos RUT empresa o Razón social.');
        return;
      }
    }

    const patch = {
      rut: rutRaw,
      rutId: docId,

      nombreProfesional: nombre,

      tipoPersona,

      correoPersonal: (mProfCorreoPersonal.value || '').trim() || null,
      telefono: (mProfTelefono.value || '').trim() || null,

      // empresa (solo si jurídica)
      rutEmpresa:       isJuridica ? ((mProfRutEmpresa.value || '').trim() || null) : null,
      razonSocial:      isJuridica ? ((mProfRazon.value || '').trim() || null) : null,
      giro:             isJuridica ? ((mProfGiro.value || '').trim() || null) : null,
      correoEmpresa:    isJuridica ? ((mProfCorreoEmpresa.value || '').trim() || null) : null,
      direccionEmpresa: isJuridica ? ((mProfDireccionEmpresa.value || '').trim() || null) : null,
      ciudadEmpresa:    isJuridica ? ((mProfCiudadEmpresa.value || '').trim() || null) : null,

      // compat
      direccion: (mProfDireccion.value || '').trim() || null,

      clinicasIds,

      tieneDescuento,
      descuentoUF: tieneDescuento ? descuentoUF : 0,
      descuentoRazon: tieneDescuento ? (descuentoRazon || null) : null,

      estado: (mProfEstado.value || 'activo'),
      actualizadoEl: serverTimestamp()
    };

    if (state.modal.mode === 'create') patch.creadoEl = serverTimestamp();

    await setDoc(doc(db, 'profesionales', docId), patch, { merge: true });

    closeAllModals();
    await loadProfesionales();
    alert('Profesional guardado ✅');

  } catch (err) {
    console.error(err);
    showError(mProfError, 'No se pudo guardar. Revisa consola.');
  }
});

/* =========================
   Search UI
========================= */
profSearch?.addEventListener('input', async () => {
  const q = profSearch.value || '';
  const visibles = filtrarProfesionales(state.profesionalesCache || [], q);

  renderProfesionalesTable(visibles);

  if (profSearchHint) {
    profSearchHint.textContent = (q.trim())
      ? `Mostrando ${visibles.length} de ${state.profesionalesCache.length}.`
      : `Total: ${state.profesionalesCache.length}.`;
  }
  if (profSearchClear) {
    profSearchClear.style.display = (q.trim()) ? 'inline-flex' : 'none';
  }
});

profSearchClear?.addEventListener('click', () => {
  if (!profSearch) return;
  profSearch.value = '';
  profSearch.dispatchEvent(new Event('input'));
  profSearch.focus();
});

/* =========================
   Buttons
========================= */
btnNuevoProfesional?.addEventListener('click', async () => {
  try {
    await openModalProfesionalCreate();
  } catch (err) {
    console.error(err);
    alert('No se pudo abrir el modal. Revisa consola.');
  }
});

/* =========================
   Auth bootstrap
========================= */
onAuthStateChanged(auth, async (user) => {
  if (!user) {
    // Ajusta el destino si tu login está en otro archivo
    window.location.href = 'index.html';
    return;
  }

  state.user = user;
  pillUser.textContent = user.email || 'Usuario';

  // Carga inicial
  await loadProfesionales();
});
