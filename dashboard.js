// dashboard.js
// Lógica principal del Panel de Liquidaciones Clínica Rennat

/* =========================================================
   0) IMPORTES FIREBASE
   ========================================================= */
import { app, auth, db } from './firebase-init.js';
import {
  signInWithEmailAndPassword,
  onAuthStateChanged,
  signOut
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js";
import {
  collection,
  getDocs,
  getDoc,
  query,
  where,
  orderBy,
  limit,
  doc,
  setDoc,
  writeBatch,
  addDoc,
  deleteDoc,
  updateDoc,
  serverTimestamp
} from "https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js";
// ✅ NUEVO: Librería para generar Excel (abre en Google Sheets)
// (Formato .xlsx con pestañas, sin depender de Google API)
import * as XLSX from "https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm";




/* =========================================================
   1) REFERENCIAS DOM (COMUNES)
   ========================================================= */

const loginShell = document.getElementById('loginShell');
const appShell   = document.getElementById('appShell');

const loginEmail    = document.getElementById('loginEmail');
const loginPassword = document.getElementById('loginPassword');
const btnLogin      = document.getElementById('btnLogin');
const loginError    = document.getElementById('loginError');

const btnLogout  = document.getElementById('btnLogout');
const pillPeriodo = document.getElementById('pillPeriodo');
const pillUser    = document.getElementById('pillUser');

const navItems = document.querySelectorAll('.nav-item');
const views    = document.querySelectorAll('.view');

// Contenedores para datos
const homeKpis               = document.getElementById('homeKpis');
const homeLastLiquidaciones  = document.getElementById('homeLastLiquidaciones');
const tablaProfesionales     = document.getElementById('tablaProfesionales');
const tablaProcedimientos    = document.getElementById('tablaProcedimientos');
// Botones “Nuevo”
const btnNuevoProfesional   = document.getElementById('btnNuevoProfesional');
const btnNuevoProcedimiento = document.getElementById('btnNuevoProcedimiento');

const btnAdministrarRoles = document.getElementById('btnAdministrarRoles');
const btnExportProfesionales = document.getElementById('btnExportProfesionales');

// Clínicas (vista)
const btnNuevaClinica   = document.getElementById('btnNuevaClinica');
const tablaClinicas     = document.getElementById('tablaClinicas');


const tablaProduccion        = document.getElementById('tablaProduccion');
const tablaLiquidaciones     = document.getElementById('tablaLiquidaciones');

const prodMesSelect   = document.getElementById('prodMes');
const prodProfSelect  = document.getElementById('prodProfesional');
const btnRefrescarProduccion = document.getElementById('btnRefrescarProduccion');

const liqMesSelect    = document.getElementById('liqMes');
const btnCalcularLiquidaciones = document.getElementById('btnCalcularLiquidaciones');

// Importar profesionales desde CSV (vista Configuración)
const fileProfesionales       = document.getElementById('fileProfesionales');
const btnImportProfesionales  = document.getElementById('btnImportProfesionales');
const importProfResultado     = document.getElementById('importProfResultado');

// Importar clínicas desde CSV
const fileClinicas            = document.getElementById('fileClinicas');
const btnImportClinicas       = document.getElementById('btnImportClinicas');
const importClinicasResultado = document.getElementById('importClinicasResultado');

// Importar procedimientos + tarifas desde CSV
const fileTarifas             = document.getElementById('fileTarifas');
const btnImportTarifas        = document.getElementById('btnImportTarifas');
const importTarifasResultado  = document.getElementById('importTarifasResultado');

// Importar roles desde CSV
const fileRoles               = document.getElementById('fileRoles');
const btnImportRoles          = document.getElementById('btnImportRoles');
const importRolesResultado    = document.getElementById('importRolesResultado');

/* =========================
   MODALES (DOM refs)
========================= */
const modalOverlay = document.getElementById('modalOverlay');

// Modal Profesional
const modalProfesional = document.getElementById('modalProfesional');
const btnCerrarModalProf = document.getElementById('btnCerrarModalProf');
const btnCancelarModalProf = document.getElementById('btnCancelarModalProf');
const btnGuardarModalProf = document.getElementById('btnGuardarModalProf');

const mProfTitle = document.getElementById('modalProfTitle');
const mProfSubtitle = document.getElementById('modalProfSubtitle');
const mProfError = document.getElementById('mProfError');

const mProfRut = document.getElementById('mProfRut');
const mProfNombre = document.getElementById('mProfNombre');
const mProfRazon = document.getElementById('mProfRazon');
const mProfGiro = document.getElementById('mProfGiro');
const mProfDireccion = document.getElementById('mProfDireccion');
const mProfRolPrincipal = document.getElementById('mProfRolPrincipal');
const mProfRolesSec = document.getElementById('mProfRolesSec');
const mProfClinicas = document.getElementById('mProfClinicas');
const mProfEstado = document.getElementById('mProfEstado');
const mProfTieneDesc = document.getElementById('mProfTieneDesc');
const mProfDescMonto = document.getElementById('mProfDescMonto');
const mProfDescRazon = document.getElementById('mProfDescRazon');

// Modal Procedimiento
const modalProcedimiento = document.getElementById('modalProcedimiento');
const btnCerrarModalProc = document.getElementById('btnCerrarModalProc');
const btnCancelarModalProc = document.getElementById('btnCancelarModalProc');
const btnGuardarModalProc = document.getElementById('btnGuardarModalProc');

const mProcTitle = document.getElementById('modalProcTitle');
const mProcSubtitle = document.getElementById('modalProcSubtitle');
const mProcError = document.getElementById('mProcError');

const mProcNombre = document.getElementById('mProcNombre');
const mProcCodigo = document.getElementById('mProcCodigo');
const mProcTipo = document.getElementById('mProcTipo');
const mProcValorBase = document.getElementById('mProcValorBase');

// Modal Roles
const modalRoles = document.getElementById('modalRoles');
const btnCerrarModalRoles = document.getElementById('btnCerrarModalRoles');
const btnCancelarModalRoles = document.getElementById('btnCancelarModalRoles');
const mRolNombre = document.getElementById('mRolNombre');
const btnGuardarRol = document.getElementById('btnGuardarRol');
const btnNuevoRol = document.getElementById('btnNuevoRol');
const tablaRoles = document.getElementById('tablaRoles');
const mRolesError = document.getElementById('mRolesError');

// Modal Clínica
const modalClinica = document.getElementById('modalClinica');
const btnCerrarModalClinica = document.getElementById('btnCerrarModalClinica');
const btnCancelarModalClinica = document.getElementById('btnCancelarModalClinica');
const btnGuardarModalClinica = document.getElementById('btnGuardarModalClinica');

const mClinicaTitle = document.getElementById('modalClinicaTitle');
const mClinicaSubtitle = document.getElementById('modalClinicaSubtitle');
const mClinicaError = document.getElementById('mClinicaError');

const mClinicaCodigo = document.getElementById('mClinicaCodigo');
const mClinicaNombre = document.getElementById('mClinicaNombre');
const mClinicaEstado = document.getElementById('mClinicaEstado');




/* =========================================================
   2) HELPERS GENERALES
   ========================================================= */

/* =========================================================
   DOCUMENTACIÓN RÁPIDA (para mantenimiento)
   ---------------------------------------------------------
   - Helpers UI: showView / setActiveNav
   - Helpers dinero: onlyDigits / parseMoneyInput / formatMoneyCLP
   - Overlay/modales: openOverlay / closeAllModals
   - Cache: ensureRolesClinicasLoaded (roles + clinicas)
   - CRUD: profesionales / procedimientos / roles / clinicas
   - Importadores: parseProfesionalesCsv / parseCsv / slugify / moneyToNumber
   ========================================================= */

/**
 * Devuelve el texto del período actual en formato "Mes Año"
 * Ej: "Diciembre 2025"
 * Usado en la pill del header al iniciar sesión.
 */
function getCurrentPeriodoText() {
  const now = new Date();
  const meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio',
                 'Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  return `${meses[now.getMonth()]} ${now.getFullYear()}`;
}

/**
 * Activa una vista (section.view) por id y oculta las demás.
 * @param {string} id - id de la sección (ej: "view-home")
 * Usado por el menú lateral.
 */
function showView(id) {
  views.forEach(v => v.classList.toggle('active', v.id === id));
}

/**
 * Marca como activo el item del sidebar asociado a targetView.
 * @param {string} targetView - id de la vista (ej: "view-home")
 */
function setActiveNav(targetView) {
  navItems.forEach(item => {
    const v = item.getAttribute('data-view');
    item.classList.toggle('active', v === targetView);
  });
}

/* =========================================================
   2.1) PLANTILLA MODELO (XLSX con pestañas)
   ========================================================= */

const btnDescargarPlantilla = document.getElementById('btnDescargarPlantilla');

btnDescargarPlantilla?.addEventListener('click', () => {
  try {
    descargarPlantillaModelo();
  } catch (err) {
    console.error(err);
    alert('No se pudo generar la plantilla. Revisa consola.');
  }
});

function descargarPlantillaModelo() {
  // 1) Creamos libro
  const wb = XLSX.utils.book_new();

  // 2) Pestaña: Profesionales
  // Columnas alineadas con tu parser parseProfesionalesCsv()
  const profesionalesHeaders = [[
    "rut",
    "nombreProfesional",
    "razonSocial",
    "giro",
    "direccion",
    "rolPrincipal",
    "rolesSecundarios",   // separados por | (ej: Anestesia|Ayudante)
    "clinicas",           // separados por | (ej: Rennat|Santa Maria)
    "tieneDescuento",     // SI/NO
    "descuentoUF",
    "descuentoRazon",
    "estado"              // activo/inactivo
  ]];

  const wsProfesionales = XLSX.utils.aoa_to_sheet(profesionalesHeaders);
  XLSX.utils.sheet_add_aoa(wsProfesionales, [[
    "16128922-1",
    "Ignacio Pastor",
    "",
    "",
    "",
    "Cirujano",
    "Ayudante|Anestesia",
    "Clinica Rennat|Clinica X",
    "NO",
    "0,5",
    "",
    "activo"
  ]], { origin: "A2" });

  XLSX.utils.book_append_sheet(wb, wsProfesionales, "Profesionales");

  // 3) Pestaña: Roles
  const rolesHeaders = [["rol"]];
  const wsRoles = XLSX.utils.aoa_to_sheet(rolesHeaders);
  XLSX.utils.sheet_add_aoa(wsRoles, [["Cirujano"], ["Anestesia"], ["Ayudante"]], { origin: "A2" });
  XLSX.utils.book_append_sheet(wb, wsRoles, "Roles");

  // 4) Pestaña: Clínicas
  const clinicasHeaders = [["clinica"]];
  const wsClinicas = XLSX.utils.aoa_to_sheet(clinicasHeaders);
  XLSX.utils.sheet_add_aoa(wsClinicas, [["Clínica Rennat"], ["Clínica X"]], { origin: "A2" });
  XLSX.utils.book_append_sheet(wb, wsClinicas, "Clínicas");

  // 5) Pestaña: Tarifas
  // Columnas alineadas con tu importTarifasCsv()
  const tarifasHeaders = [[
    "clinica",
    "cirugia",
    "precioTotal",
    "derechosPabellon",
    "hmq",
    "insumos"
  ]];
  const wsTarifas = XLSX.utils.aoa_to_sheet(tarifasHeaders);
  XLSX.utils.sheet_add_aoa(wsTarifas, [[
    "Clínica Rennat",
    "Artroscopia de rodilla",
    "2500000",
    "400000",
    "900000",
    "200000"
  ]], { origin: "A2" });
  XLSX.utils.book_append_sheet(wb, wsTarifas, "Tarifas");

  // 6) Descarga
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const filename = `Plantilla_ClinicaRennat_${y}-${m}-${d}.xlsx`;

  XLSX.writeFile(wb, filename);
}

/* =========================================================
   MODALES: estado + helpers
   ========================================================= */

const modalState = {
  open: null, // 'prof' | 'proc' | null
  profMode: 'create', // 'create' | 'edit'
  profRutId: null,    // id doc (rut normalizado)
  procMode: 'create',
  procId: null,
  cacheRoles: [],     // [{id,nombre}]
  cacheClinicas: []   // [{id,nombre}]
};

/**
 * Bloquea/desbloquea scroll del body mientras un modal está abierto.
 * @param {boolean} lock
 */
function lockScroll(lock) {
  document.body.style.overflow = lock ? 'hidden' : '';
}

/**
 * Muestra/oculta un mensaje de error en un elemento del DOM.
 * @param {HTMLElement} el - contenedor del error
 * @param {string} msg - mensaje (si es vacío, se oculta)
 */
function showError(el, msg) {
  if (!el) return;
  el.textContent = msg || '';
  el.style.display = msg ? 'block' : 'none';
}

/**
 * Abre el overlay (fondo oscuro) y bloquea el scroll.
 * Nota: NO muestra un modal específico; solo el overlay.
 */
function openOverlay() {
  if (!modalOverlay) return;
  modalOverlay.style.display = 'flex';
  lockScroll(true);
}

/**
 * Cierra overlay + todos los modales, y limpia errores.
 * Se llama desde: botones cancelar/cerrar, click fuera, tecla Escape.
 */
function closeAllModals() {
  modalState.open = null;

  if (modalProfesional) modalProfesional.style.display = 'none';
  if (modalProcedimiento) modalProcedimiento.style.display = 'none';
  if (modalRoles) modalRoles.style.display = 'none';
  if (modalClinica) modalClinica.style.display = 'none';

  if (modalOverlay) modalOverlay.style.display = 'none';
  lockScroll(false);

  showError(mProfError, '');
  showError(mProcError, '');
  showError(mRolesError, '');
  showError(mClinicaError, '');
}

btnCerrarModalRoles?.addEventListener('click', closeAllModals);
btnCancelarModalRoles?.addEventListener('click', closeAllModals);

btnCerrarModalClinica?.addEventListener('click', closeAllModals);
btnCancelarModalClinica?.addEventListener('click', closeAllModals);

/**
 * Deja solo números en un string.
 * @param {string} v
 * @returns {string}
 */
function onlyDigits(v='') {
  return v.toString().replace(/[^\d]/g,'');
}

/**
 * Convierte un input monetario en número entero (CLP).
 * Ej: "1.200.000" => 1200000
 */
function parseMoneyInput(v='') {
  return Number(onlyDigits(v)) || 0;
}

/**
 * Formatea un número en formato CLP (locale es-CL).
 */
function formatMoneyCLP(n=0) {
  return (Number(n)||0).toLocaleString('es-CL');
}

/* ================== UF HELPERS (DESCUENTO) ================== */
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
/* ================== FIN UF HELPERS ================== */

// Cerrar por click fuera (overlay)
modalOverlay?.addEventListener('click', (e) => {
  // Si clickeas el overlay (no el contenido), cierra
  if (e.target === modalOverlay) closeAllModals();
});

// Cerrar por Escape
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && modalOverlay?.style.display === 'flex') {
    closeAllModals();
  }
});

// Botones cerrar/cancelar
btnCerrarModalProf?.addEventListener('click', closeAllModals);
btnCancelarModalProf?.addEventListener('click', closeAllModals);
btnCerrarModalProc?.addEventListener('click', closeAllModals);
btnCancelarModalProc?.addEventListener('click', closeAllModals);

/**
 * Carga roles y clínicas desde Firestore si no están en cache.
 * Esto evita llamar a Firestore muchas veces al abrir modales.
 * Se usa para poblar select/checkboxes del modal de Profesional.
 */
async function ensureRolesClinicasLoaded() {
  // Roles
  if (!modalState.cacheRoles.length) {
    const snap = await getDocs(collection(db, 'roles'));
    const roles = [];
    snap.forEach(d => roles.push({ id: d.id, ...d.data() }));
    // orden por nombre si viene
    roles.sort((a,b) => (a.nombre||a.id).localeCompare((b.nombre||b.id), 'es'));
    modalState.cacheRoles = roles;
  }

  // Clínicas
  if (!modalState.cacheClinicas.length) {
    const snap = await getDocs(collection(db, 'clinicas'));
    const clin = [];
    snap.forEach(d => clin.push({ id: d.id, ...d.data() }));
    clin.sort((a,b) => (a.nombre||a.id).localeCompare((b.nombre||b.id), 'es'));
    modalState.cacheClinicas = clin;
  }
}

/**
 * Renderiza el select de Rol Principal usando modalState.cacheRoles.
 * @param {string|null} selectedId - id a seleccionar por defecto
 */
function renderSelectRolPrincipal(selectedId=null) {
  if (!mProfRolPrincipal) return;
  mProfRolPrincipal.innerHTML = '';
  const optNone = document.createElement('option');
  optNone.value = '';
  optNone.textContent = '— Sin rol principal —';
  mProfRolPrincipal.appendChild(optNone);

  for (const r of modalState.cacheRoles) {
    const opt = document.createElement('option');
    opt.value = r.id;
    opt.textContent = r.nombre || r.id;
    mProfRolPrincipal.appendChild(opt);
  }
  mProfRolPrincipal.value = selectedId || '';
}

/**
 * Renderiza una lista de checkboxes en un contenedor.
 * @param {HTMLElement} containerEl
 * @param {Array} items - items con {id, nombre}
 * @param {Array<string>} selectedIds - ids preseleccionados
 */
function renderChecklist(containerEl, items, selectedIds=[]) {
  if (!containerEl) return;
  const set = new Set(selectedIds || []);
  containerEl.innerHTML = '';

  if (!items.length) {
    containerEl.innerHTML = `<div style="font-size:12px;color:var(--muted);">No hay ítems cargados.</div>`;
    return;
  }

  for (const it of items) {
    const id = it.id;
    const label = it.nombre || it.nombreProfesional || it.nombre || it.id;

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

/**
 * Lee los checkboxes marcados dentro de un contenedor.
 * @returns {Array<string>}
 */
function getCheckedValues(containerEl) {
  if (!containerEl) return [];
  return Array.from(containerEl.querySelectorAll('input[type="checkbox"]'))
    .filter(x => x.checked)
    .map(x => x.value);
}

// helper de UI para mostrar/ocultar “empresaFields”
function applyTipoPersonaUI() {
  const tipoSel = document.getElementById('mProfTipoPersona');
  const boxEmp  = document.getElementById('empresaFields');
  if (!tipoSel || !boxEmp) return;

  const tipo = (tipoSel.value || 'natural').trim();
  boxEmp.style.display = (tipo === 'juridica') ? 'block' : 'none';
}

/**
 * Abre modal profesional en modo CREATE.
 * - Carga roles/clinicas
 * - Limpia campos
 * - Muestra modalProfesional
 */
async function openModalProfesionalCreate() {
  await ensureRolesClinicasLoaded();

  modalState.open = 'prof';
  modalState.profMode = 'create';
  modalState.profRutId = null;

  openOverlay();
  modalProcedimiento.style.display = 'none';
  modalProfesional.style.display = 'block';

  mProfTitle.textContent = 'Nuevo profesional';
  mProfSubtitle.textContent = 'Crear un nuevo profesional';

  // limpiar campos
  mProfRut.disabled = false;
  mProfRut.value = '';
  mProfNombre.value = '';
  mProfRazon.value = '';
  mProfGiro.value = '';
  mProfDireccion.value = '';

  renderSelectRolPrincipal(null);
  renderChecklist(mProfRolesSec, modalState.cacheRoles, []);
  // ✅ por defecto: TODAS las clínicas marcadas
  const allClinicasIds = (modalState.cacheClinicas || []).map(c => c.id);
  renderChecklist(mProfClinicas, modalState.cacheClinicas, allClinicasIds);

  mProfEstado.value = 'activo';
  mProfTieneDesc.checked = false;
  mProfDescMonto.value = '';
  mProfDescRazon.value = '';

  // ✅ NUEVO: defaults tipo persona + campos nuevos
  const el = (id) => document.getElementById(id);
  el('mProfTipoPersona') && (el('mProfTipoPersona').value = 'natural');
  el('mProfCorreoPersonal') && (el('mProfCorreoPersonal').value = '');
  el('mProfTelefono') && (el('mProfTelefono').value = '');

  el('mProfRutEmpresa') && (el('mProfRutEmpresa').value = '');
  el('mProfCorreoEmpresa') && (el('mProfCorreoEmpresa').value = '');
  el('mProfDireccionEmpresa') && (el('mProfDireccionEmpresa').value = '');
  el('mProfCiudadEmpresa') && (el('mProfCiudadEmpresa').value = '');

  applyTipoPersonaUI();
  el('mProfTipoPersona')?.addEventListener('change', applyTipoPersonaUI);


  showError(mProfError, '');

  setTimeout(() => mProfRut?.focus(), 50);
}

/**
 * Abre modal profesional en modo EDIT.
 * @param {string} rutId - docId en profesionales (rut normalizado)
 * - Carga roles/clinicas
 * - Lee Firestore profesionales/{rutId}
 * - Rellena campos
 */
async function openModalProfesionalEdit(rutId) {
  await ensureRolesClinicasLoaded();

  modalState.open = 'prof';
  modalState.profMode = 'edit';
  modalState.profRutId = rutId;

  openOverlay();
  modalProcedimiento.style.display = 'none';
  modalProfesional.style.display = 'block';

  mProfTitle.textContent = 'Editar profesional';
  mProfSubtitle.textContent = `ID: ${rutId}`;

  showError(mProfError, '');

  // ✅ Lectura directa por ID (rápida y correcta)
  const ref = doc(db, 'profesionales', rutId);
  const snap = await getDoc(ref);

  if (!snap.exists()) {
    showError(mProfError, 'No se encontró el profesional. Refresca e intenta de nuevo.');
    return;
  }

  const data = { id: snap.id, ...snap.data() };


  // poblar campos
  mProfRut.value = data.rut || '';
  mProfRut.disabled = true; // no cambiar rut
  mProfNombre.value = data.nombreProfesional || '';
  mProfRazon.value = data.razonSocial || '';
  mProfGiro.value = data.giro || '';
  mProfDireccion.value = data.direccion || '';

  // ✅ nuevos campos (si existen en HTML)
   const el = (id) => document.getElementById(id);
   
   el('mProfTipoPersona')    && (el('mProfTipoPersona').value = data.tipoPersona || 'natural');
   el('mProfCorreoPersonal') && (el('mProfCorreoPersonal').value = data.correoPersonal || '');
   el('mProfTelefono')       && (el('mProfTelefono').value = data.telefono || '');
   
   el('mProfRutEmpresa')      && (el('mProfRutEmpresa').value = data.rutEmpresa || '');
   el('mProfCorreoEmpresa')   && (el('mProfCorreoEmpresa').value = data.correoEmpresa || '');
   el('mProfDireccionEmpresa')&& (el('mProfDireccionEmpresa').value = data.direccionEmpresa || '');
   el('mProfCiudadEmpresa')   && (el('mProfCiudadEmpresa').value = data.ciudadEmpresa || '');


  renderSelectRolPrincipal(data.rolPrincipalId || '');
  renderChecklist(mProfRolesSec, modalState.cacheRoles, data.rolesSecundariosIds || []);
  renderChecklist(mProfClinicas, modalState.cacheClinicas, data.clinicasIds || []);

  mProfEstado.value = (data.estado || 'activo');
  mProfTieneDesc.checked = !!data.tieneDescuento;
  mProfDescMonto.value = data.tieneDescuento ? formatUF(data.descuentoUF || 0) : '';
  mProfDescRazon.value = data.tieneDescuento ? (data.descuentoRazon || '') : '';

  applyTipoPersonaUI();
  document.getElementById('mProfTipoPersona')?.addEventListener('change', applyTipoPersonaUI);

  setTimeout(() => mProfNombre?.focus(), 50);
}

// Guardar profesional (create/edit)
btnGuardarModalProf?.addEventListener('click', async () => {
  try {
    showError(mProfError, '');

    const rutRaw = (mProfRut.value || '').trim();
    const rutId = normalizaRut(rutRaw);

    const nombre = (mProfNombre.value || '').trim();
    if (!rutId) { showError(mProfError, 'RUT inválido.'); return; }
    if (!nombre) { showError(mProfError, 'Nombre profesional es obligatorio.'); return; }

    // Si edit: rutId no cambia
    const docId = (modalState.profMode === 'edit') ? modalState.profRutId : rutId;

    // Roles/clinicas desde UI
    const rolPrincipalId = (mProfRolPrincipal.value || '').trim() || null;
    const rolesSecundariosIds = getCheckedValues(mProfRolesSec);
    const clinicasIds = getCheckedValues(mProfClinicas);

    // Descuento
    const tieneDescuento = !!mProfTieneDesc.checked;
    const descuentoUF = tieneDescuento ? parseUFInput(mProfDescMonto.value) : 0;
    const descuentoRazon = tieneDescuento ? (mProfDescRazon.value || '').trim() : '';

    // ✅ campos opcionales (si existen en el DOM, los leemos; si no, quedan null)
    const el = (id) => document.getElementById(id);
   
    const mProfTipoPersona     = el('mProfTipoPersona');     // select natural/juridica
    const mProfCorreoPersonal  = el('mProfCorreoPersonal');
    const mProfTelefono        = el('mProfTelefono');
   
    const mProfRutEmpresa      = el('mProfRutEmpresa');
    const mProfCorreoEmpresa   = el('mProfCorreoEmpresa');
    const mProfDireccionEmp    = el('mProfDireccionEmpresa');
    const mProfCiudadEmp       = el('mProfCiudadEmpresa');
   
    const tipoPersona = (mProfTipoPersona?.value || 'natural').trim();

    const isJuridica = (tipoPersona === 'juridica');
   
    const patch = {
      rut: rutRaw,
      rutId: docId,
      nombreProfesional: nombre,
   
      // ✅ tipo persona + contacto
      tipoPersona,
   
      correoPersonal: (mProfCorreoPersonal?.value || '').trim() || null,
      telefono: (mProfTelefono?.value || '').trim() || null,
   
      // Empresa: solo si es jurídica; si es natural, lo dejamos en null para no ensuciar datos
      rutEmpresa: isJuridica ? ((mProfRutEmpresa?.value || '').trim() || null) : null,
      correoEmpresa: isJuridica ? ((mProfCorreoEmpresa?.value || '').trim() || null) : null,
      direccionEmpresa: isJuridica ? ((mProfDireccionEmp?.value || '').trim() || null) : null,
      ciudadEmpresa: isJuridica ? ((mProfCiudadEmp?.value || '').trim() || null) : null,
   
      // compatibilidad con campo viejo (si lo sigues usando en UI)
      direccion: (mProfDireccion.value || '').trim() || null,


      rolPrincipalId,
      rolesSecundariosIds,
      clinicasIds,

      tieneDescuento,
      descuentoUF: tieneDescuento ? descuentoUF : 0,
      descuentoRazon: tieneDescuento ? (descuentoRazon || null) : null,

      estado: (mProfEstado.value || 'activo'),
      actualizadoEl: serverTimestamp()
    };

    // Si es create, ponemos creadoEl si no existía
    if (modalState.profMode === 'create') {
      patch.creadoEl = serverTimestamp();
    }

    // ✅ Validación mínima por tipo persona
    if (tipoPersona === 'juridica') {
      const rutEmp = (mProfRutEmpresa?.value || '').trim();
      const razon = (mProfRazon.value || '').trim();
      if (!rutEmp && !razon) {
        showError(mProfError, 'Si es Persona jurídica, ingresa al menos RUT empresa o Razón social.');
        return;
      }
    }

    await setDoc(doc(db, 'profesionales', docId), patch, { merge: true });

    closeAllModals();
    await loadProfesionales();
    alert('Profesional guardado ✅');

  } catch (err) {
    console.error(err);
    showError(mProfError, 'No se pudo guardar. Revisa consola.');
  }
});

// UX: al destildar descuento, limpia campos
mProfTieneDesc?.addEventListener('change', () => {
  if (!mProfTieneDesc.checked) {
    mProfDescMonto.value = '';
    mProfDescRazon.value = '';
  }
});

/**
 * Abre modal procedimiento en modo CREATE y limpia campos.
 */
function openModalProcedimientoCreate() {
  modalState.open = 'proc';
  modalState.procMode = 'create';
  modalState.procId = null;

  openOverlay();
  modalProfesional.style.display = 'none';
  modalProcedimiento.style.display = 'block';

  mProcTitle.textContent = 'Nuevo procedimiento';
  mProcSubtitle.textContent = 'Crear un procedimiento';

  mProcNombre.value = '';
  mProcCodigo.value = '';
  mProcTipo.value = 'ambulatorio';
  mProcValorBase.value = '';

  showError(mProcError, '');

  setTimeout(() => mProcNombre?.focus(), 50);
}

/* =========================================================
   ROLES (MODAL ADMIN)
   - Crear / Editar / Eliminar (ELIMINACIÓN REAL)
   ========================================================= */

const rolesUIState = {
  mode: 'create',   // 'create' | 'edit'
  editId: null
};

/**
 * Abre modal Roles (admin).
 * - Pone estado create
 * - Carga tabla roles (loadRolesTable)
 */
function openModalRoles() {
  modalState.open = 'roles';
  openOverlay();

  if (modalProfesional) modalProfesional.style.display = 'none';
  if (modalProcedimiento) modalProcedimiento.style.display = 'none';
  if (modalClinica) modalClinica.style.display = 'none';

  modalRoles.style.display = 'block';

  rolesUIState.mode = 'create';
  rolesUIState.editId = null;
  mRolNombre.value = '';
  showError(mRolesError, '');

  loadRolesTable();
  setTimeout(() => mRolNombre?.focus(), 50);
}

btnAdministrarRoles?.addEventListener('click', () => {
  try { openModalRoles(); } catch(e){ console.error(e); alert('No se pudo abrir roles.'); }
});

/* ================== EXPORTAR PROFESIONALES (XLSX) ================== */
btnExportProfesionales?.addEventListener('click', async () => {
  try {
    await exportarProfesionalesXLSX();
  } catch (err) {
    console.error(err);
    alert('No se pudo exportar. Revisa consola.');
  }
});

async function exportarProfesionalesXLSX() {
  // Asegura caches para resolver nombres
  await ensureRolesClinicasLoaded();

  // Mapas id -> nombre
  const rolesById = new Map(modalState.cacheRoles.map(r => [r.id, (r.nombre || r.id)]));
  const clinById  = new Map(modalState.cacheClinicas.map(c => [c.id, (c.nombre || c.id)]));

  // Leer profesionales
  const snap = await getDocs(collection(db, 'profesionales'));
  const profesionales = [];
  snap.forEach(d => profesionales.push({ id: d.id, ...d.data() }));

  // Armar filas
  const rows = profesionales
    .map(p => {
      const tipo = p.tipoPersona || 'natural';

      const clinicasNombres = (p.clinicasIds || [])
        .map(id => clinById.get(id) || id)
        .join(' | ');

      const rolesSecNombres = (p.rolesSecundariosIds || [])
        .map(id => rolesById.get(id) || id)
        .join(' | ');

      return {
        rut: p.rut || '',
        nombreProfesional: p.nombreProfesional || '',
        tipoPersona: tipo, // natural | juridica

        correoPersonal: p.correoPersonal || '',
        telefono: p.telefono || '',

        rutEmpresa: p.rutEmpresa || '',
        razonSocial: p.razonSocial || '',
        giro: p.giro || '',
        correoEmpresa: p.correoEmpresa || '',
        direccionEmpresa: p.direccionEmpresa || '',
        ciudadEmpresa: p.ciudadEmpresa || '',

        rolPrincipal: p.rolPrincipalId ? (rolesById.get(p.rolPrincipalId) || p.rolPrincipalId) : '',
        rolesSecundarios: rolesSecNombres,
        clinicas: clinicasNombres,

        tieneDescuento: p.tieneDescuento ? 'SI' : 'NO',
        descuentoUF: p.tieneDescuento ? (p.descuentoUF ?? 0) : 0,
        descuentoRazon: p.tieneDescuento ? (p.descuentoRazon || '') : '',

        estado: p.estado || 'activo'
      };
    });

  // XLSX
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet(rows, { skipHeader: false });
  XLSX.utils.book_append_sheet(wb, ws, "Profesionales");

  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  XLSX.writeFile(wb, `Profesionales_ClinicaRennat_${y}-${m}-${d}.xlsx`);
}
/* ================== FIN EXPORTAR PROFESIONALES ================== */

btnNuevoRol?.addEventListener('click', () => {
  rolesUIState.mode = 'create';
  rolesUIState.editId = null;
  mRolNombre.value = '';
  showError(mRolesError, '');
  mRolNombre?.focus();
});

/**
 * Carga roles desde Firestore y dibuja tabla con acciones Editar/Eliminar.
 */
async function loadRolesTable() {
  try {
    const snap = await getDocs(collection(db, 'roles'));
    const rows = [];
    snap.forEach(d => rows.push({ id: d.id, ...d.data() }));

    rows.sort((a,b) => (a.nombre || '').localeCompare((b.nombre || ''), 'es'));

    if (!rows.length) {
      tablaRoles.innerHTML = `<p style="font-size:13px;color:var(--muted);">Aún no hay roles.</p>`;
      return;
    }

    const html = rows.map(r => `
      <tr>
        <td>${r.nombre || '—'}</td>
        <td class="text-right">
          <button class="btn btn-soft" data-action="edit-role" data-id="${r.id}">Editar</button>
          <button class="btn btn-soft" data-action="del-role" data-id="${r.id}">Eliminar</button>
        </td>
      </tr>
    `).join('');

    tablaRoles.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Rol</th>
            <th class="text-right">Acciones</th>
          </tr>
        </thead>
        <tbody>${html}</tbody>
      </table>
    `;
  } catch (err) {
    console.error(err);
    tablaRoles.innerHTML = `<p style="color:#e11d48;font-size:13px;">Error cargando roles.</p>`;
  }
}

tablaRoles?.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;

  const action = btn.dataset.action;
  const id = btn.dataset.id;

  try {
    if (action === 'edit-role') {
      const snap = await getDoc(doc(db, 'roles', id));
      if (!snap.exists()) return alert('Rol no encontrado.');
      const data = snap.data();

      rolesUIState.mode = 'edit';
      rolesUIState.editId = id;
      mRolNombre.value = data.nombre || '';
      mRolNombre?.focus();
      return;
    }

    if (action === 'del-role') {
      // Validación mínima: si está siendo usado en profesionales, ideal bloquear.
      // (Si quieres, luego agregamos check real por query antes de borrar)
      const ok = confirm('¿Eliminar este rol definitivamente? Esta acción no se puede deshacer.');
      if (!ok) return;

      await deleteDoc(doc(db, 'roles', id));

      // IMPORTANTE: refrescar caches para que el modal profesional no quede con roles viejos
      modalState.cacheRoles = [];
      await ensureRolesClinicasLoaded();
      await loadRolesTable();
      await loadProfesionales(); // para que se refresque vista
      return;
    }
  } catch (err) {
    console.error(err);
    alert('Error ejecutando acción. Revisa consola.');
  }
});

btnGuardarRol?.addEventListener('click', async () => {
  try {
    showError(mRolesError, '');
    const nombre = (mRolNombre.value || '').trim();
    if (!nombre) { showError(mRolesError, 'El nombre del rol es obligatorio.'); return; }

    if (rolesUIState.mode === 'create') {
      // ✅ ID determinístico (CONSISTENTE con import CSV): r_<slug>
      const rolId = `r_${slugify(nombre)}`;
   
      // Si ya existe, solo actualiza nombre
      await setDoc(doc(db, 'roles', rolId), {
        id: rolId,
        nombre,
        estado: 'activo',
        creadoEl: serverTimestamp(),
        actualizadoEl: serverTimestamp()
      }, { merge: true });
   
    } else {
      // ✅ Edit: si cambias el nombre, NO cambiamos el ID (para no romper referencias)
      await updateDoc(doc(db, 'roles', rolesUIState.editId), {
        nombre,
        actualizadoEl: serverTimestamp()
      });
    }

    rolesUIState.mode = 'create';
    rolesUIState.editId = null;
    mRolNombre.value = '';

    modalState.cacheRoles = []; // invalida cache
    await ensureRolesClinicasLoaded();
    await loadRolesTable();
    await loadProfesionales(); // refresca tabla (rol principal se resuelve)
  } catch (err) {
    console.error(err);
    showError(mRolesError, 'No se pudo guardar el rol. Revisa consola.');
  }
});


btnGuardarModalProc?.addEventListener('click', async () => {
  try {
    showError(mProcError, '');

    const nombre = (mProcNombre.value || '').trim();
    if (!nombre) { showError(mProcError, 'Nombre es obligatorio.'); return; }

    const codigo = (mProcCodigo.value || '').trim() || null;
    const tipo = (mProcTipo.value || 'ambulatorio').trim();
    const valorBase = parseMoneyInput(mProcValorBase.value);

    await addDoc(collection(db, 'procedimientos'), {
      nombre,
      codigo,
      tipo,
      valorBase: Number.isFinite(valorBase) ? valorBase : 0,
      creadoEl: serverTimestamp(),
      actualizadoEl: serverTimestamp()
    });

    closeAllModals();
    await loadProcedimientos();
    alert('Procedimiento creado ✅');
  } catch (err) {
    console.error(err);
    showError(mProcError, 'No se pudo guardar. Revisa consola.');
  }
});


/* =========================================================
   3) MÓDULO AUTH (LOGIN / LOGOUT / ESTADO)
   ========================================================= */

// 3.1) Login manual (botón)
btnLogin?.addEventListener('click', async () => {
  loginError.style.display = 'none';
  loginError.textContent = '';

  const email = loginEmail.value.trim();
  const pass  = loginPassword.value.trim();

  if (!email || !pass) {
    loginError.textContent = 'Ingresa correo y contraseña.';
    loginError.style.display = 'block';
    return;
  }

  try {
    await signInWithEmailAndPassword(auth, email, pass);
  } catch (err) {
    console.error(err);
    loginError.textContent = 'No se pudo iniciar sesión. Revisa tus datos.';
    loginError.style.display = 'block';
  }
});

// 3.2) Logout
btnLogout?.addEventListener('click', async () => {
  await signOut(auth);
});

// 3.3) Listener de estado de autenticación
onAuthStateChanged(auth, user => {
  if (user) {
    // Mostrar app
    loginShell.style.display = 'none';
    appShell.style.display = 'grid';

    pillPeriodo.textContent = getCurrentPeriodoText();
    pillUser.textContent = user.email || 'Usuario sin correo';

    // Cargar datos iniciales
    initSelectsPeriodo();
    loadHomeData();
    loadProfesionales();
    loadProcedimientos();
    loadClinicas();
    loadLiquidaciones();
  } else {
    // Mostrar login
    loginShell.style.display = 'flex';
    appShell.style.display = 'none';
    loginPassword.value = '';
  }
});

/* =========================================================
   4) MÓDULO NAVEGACIÓN (SIDEBAR / VISTAS)
   ========================================================= */

navItems.forEach(item => {
  item.addEventListener('click', () => {
    const viewId = item.getAttribute('data-view');
    setActiveNav(viewId);
    showView(viewId);
  });
});

/* =========================================================
   5) MÓDULO PERÍODO (SELECTS MES)
   ========================================================= */
/**
 * Inicializa selects de mes para Producción y Liquidaciones.
 * Rellena valores AAAA-MM y selecciona el mes actual.
 */
function initSelectsPeriodo() {
  const now = new Date();
  const year = now.getFullYear();

  const mesesShort = [
    '01 - Enero','02 - Febrero','03 - Marzo','04 - Abril','05 - Mayo','06 - Junio',
    '07 - Julio','08 - Agosto','09 - Septiembre','10 - Octubre','11 - Noviembre','12 - Diciembre'
  ];

  [prodMesSelect, liqMesSelect].forEach(sel => {
    if (!sel) return;
    sel.innerHTML = '';
    mesesShort.forEach((label, idx) => {
      const opt = document.createElement('option');
      // valor: AAAA-MM (ej 2025-03)
      const month = String(idx + 1).padStart(2, '0');
      opt.value = `${year}-${month}`;
      opt.textContent = `${label} ${year}`;
      sel.appendChild(opt);
    });
    sel.value = `${year}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  });
}

/* =========================================================
   6) MÓDULO HOME (KPIs + ÚLTIMAS LIQUIDACIONES)
   ========================================================= */
/**
 * Carga KPIs de Home y últimas 5 liquidaciones.
 * - Cuenta profesionales y procedimientos
 * - Lista últimas liquidaciones
 */
async function loadHomeData() {
  try {
    // Total profesionales
    const profSnap = await getDocs(collection(db, 'profesionales'));
    const totalProfesionales = profSnap.size;

    // Total procedimientos
    const procSnap = await getDocs(collection(db, 'procedimientos'));
    const totalProcedimientos = procSnap.size;

    // Últimas 5 liquidaciones
    const qLiq = query(
      collection(db, 'liquidaciones'),
      orderBy('fechaCreacion', 'desc'),
      limit(5)
    );
    const liqSnap = await getDocs(qLiq);
    const liqRows = [];
    liqSnap.forEach(doc => liqRows.push({ id: doc.id, ...doc.data() }));

    // KPIs
    homeKpis.innerHTML = `
      <div style="display:flex;gap:12px;flex-wrap:wrap;">
        <div class="card" style="flex:1;min-width:140px;margin-bottom:0;">
          <div class="card-title" style="font-size:14px;">Profesionales activos</div>
          <div style="font-size:26px;font-weight:700;margin-top:4px;">${totalProfesionales}</div>
        </div>
        <div class="card" style="flex:1;min-width:140px;margin-bottom:0;">
          <div class="card-title" style="font-size:14px;">Procedimientos configurados</div>
          <div style="font-size:26px;font-weight:700;margin-top:4px;">${totalProcedimientos}</div>
        </div>
      </div>
    `;

    // Últimas liquidaciones
    if (!liqRows.length) {
      homeLastLiquidaciones.innerHTML = `
        <p style="font-size:13px;color:var(--muted);">
          Aún no hay liquidaciones registradas.
        </p>`;
    } else {
      const rowsHtml = liqRows.map(l => `
        <tr>
          <td>${l.profesionalNombre || '—'}</td>
          <td>${l.periodo || '—'}</td>
          <td class="text-right">$${(l.montoTotal ?? 0).toLocaleString('es-CL')}</td>
          <td>${l.estado || 'borrador'}</td>
        </tr>
      `).join('');

      homeLastLiquidaciones.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Profesional</th>
              <th>Período</th>
              <th class="text-right">Monto total</th>
              <th>Estado</th>
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      `;
    }

  } catch (err) {
    console.error('Error cargando home:', err);
    homeKpis.innerHTML = `<p style="color:#e11d48;font-size:13px;">
      Error al cargar indicadores. Revisa la consola.
    </p>`;
  }
}

/* =========================================================
   7) MÓDULO PROFESIONALES (LISTADO BÁSICO)
   ========================================================= */
/**
 * Carga la tabla de profesionales desde Firestore.
 * Incluye botones: Editar / Activar-Desactivar.
 */
async function loadProfesionales() {
  try {
    // ✅ asegura caches para resolver nombres en tabla
    await ensureRolesClinicasLoaded();
    const rolesById = new Map(modalState.cacheRoles.map(r => [r.id, (r.nombre || r.id)]));
    const clinById  = new Map(modalState.cacheClinicas.map(c => [c.id, (c.nombre || c.id)]));
    const snap = await getDocs(collection(db, 'profesionales'));
    const rows = [];
    snap.forEach(d => rows.push({ id: d.id, ...d.data() }));

    if (!rows.length) {
      tablaProfesionales.innerHTML = `
        <p style="font-size:13px;color:var(--muted);">
          Aún no hay profesionales registrados.
        </p>`;
      return;
    }

      const visibles = rows;
      const htmlRows = visibles.map(p => {
      const desc = p.tieneDescuento
        ? `${formatUF(p.descuentoUF ?? 0)} UF (${p.descuentoRazon || '—'})`
        : 'No';
      const estado = p.estado || 'activo';
      return `
        <tr>
          <td>${p.nombreProfesional || p.nombre || '—'}</td>
          <td>${p.rut || '—'}</td>
          <td>${p.razonSocial || '—'}</td>
          <td>${(p.tipoPersona || 'natural')}</td>
          <td>${(p.correoPersonal || p.correoEmpresa || '—')}</td>
          <td>${(p.telefono || '—')}</td>
          <td>${
            (p.clinicasIds || []).length
              ? (p.clinicasIds.map(id => clinById.get(id) || id).join(', '))
              : '—'
          }</td>
          <td>${p.rolPrincipalId ? (rolesById.get(p.rolPrincipalId) || p.rolPrincipalId) : '—'}</td>
          <td>${desc}</td>
          <td>${estado}</td>
          <td class="text-right">
            <button class="btn btn-soft" data-action="edit-prof" data-id="${p.rutId || p.id}">Editar</button>
            <button class="btn btn-soft" data-action="toggle-prof" data-id="${p.rutId || p.id}">
              ${estado === 'inactivo' ? 'Activar' : 'Desactivar'}
            </button>
            <button class="btn btn-soft" data-action="del-prof" data-id="${p.rutId || p.id}">Eliminar</button>
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
            <th>Razón social</th>
            <th>Tipo</th>
            <th>Correo</th>
            <th>Teléfono</th>
            <th>Clínicas</th>
            <th>Rol principal</th>
            <th>Descuento</th>
            <th>Estado</th>
            <th class="text-right">Acciones</th>
          </tr>
        </thead>
        <tbody>${htmlRows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error('Error cargando profesionales:', err);
    tablaProfesionales.innerHTML = `<p style="color:#e11d48;font-size:13px;">
      Error al cargar profesionales.
    </p>`;
  }
}

tablaProfesionales?.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;

  const action = btn.dataset.action;
  const rutId = btn.dataset.id;

  try {
    if (action === 'toggle-prof') {
      const ref = doc(db, 'profesionales', rutId);
      const nuevoEstado = (btn.textContent.includes('Activar')) ? 'activo' : 'inactivo';
      await setDoc(ref, { estado: nuevoEstado, actualizadoEl: serverTimestamp() }, { merge: true });
      await loadProfesionales();
      return;
    }

    if (action === 'edit-prof') {
      await openModalProfesionalEdit(rutId);
      return;
    }

      if (action === 'del-prof') {
        const ok = confirm(
          '¿Eliminar profesional DEFINITIVAMENTE?\n' +
          'Esto borra el registro de la Base de Datos y NO se puede recuperar.'
        );
        if (!ok) return;
      
        await deleteDoc(doc(db, 'profesionales', rutId));
      
        await loadProfesionales();
        return;
      }



    // Si llega aquí, es una acción no soportada
    console.warn('Acción no soportada en tablaProfesionales:', action);
  } catch (err) {
    console.error(err);
    alert('Error ejecutando acción. Revisa consola.');
  }
});


/* =========================================================
   7.1) CREAR PROFESIONAL (BOTÓN + NUEVO)
   ========================================================= */

btnNuevoProfesional?.addEventListener('click', async () => {
  try {
    await openModalProfesionalCreate();
  } catch (err) {
    console.error(err);
    alert('No se pudo abrir el modal. Revisa consola.');
  }
});

/* =========================================================
   8) MÓDULO PROCEDIMIENTOS (LISTADO BÁSICO)
   ========================================================= */
/**
 * Carga la tabla de procedimientos desde Firestore.
 */
async function loadProcedimientos() {
  try {
    const snap = await getDocs(collection(db, 'procedimientos'));
    const rows = [];
    snap.forEach(doc => rows.push({ id: doc.id, ...doc.data() }));

    if (!rows.length) {
      tablaProcedimientos.innerHTML = `
        <p style="font-size:13px;color:var(--muted);">
          Aún no hay procedimientos configurados.
        </p>`;
      return;
    }

    const htmlRows = rows.map(p => `
      <tr>
        <td>${p.codigo || '—'}</td>
        <td>${p.nombre || '—'}</td>
        <td>${p.tipo || '—'}</td>
        <td class="text-right">$${(p.valorBase ?? 0).toLocaleString('es-CL')}</td>
      </tr>
    `).join('');

    tablaProcedimientos.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Código</th>
            <th>Nombre</th>
            <th>Tipo</th>
            <th class="text-right">Valor base</th>
          </tr>
        </thead>
        <tbody>${htmlRows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error('Error cargando procedimientos:', err);
    tablaProcedimientos.innerHTML = `<p style="color:#e11d48;font-size:13px;">
      Error al cargar procedimientos.
    </p>`;
  }
}

/* =========================================================
   8.1) CREAR PROCEDIMIENTO (BOTÓN + NUEVO)
   ========================================================= */

btnNuevoProcedimiento?.addEventListener('click', () => {
  try {
    openModalProcedimientoCreate();
  } catch (err) {
    console.error(err);
    alert('No se pudo abrir el modal. Revisa consola.');
  }
});

/* =========================================================
   CLÍNICAS (VISTA + MODAL)
   - Código numérico C001, C002... (docId)
   ========================================================= */

const clinicaUIState = {
  mode: 'create',   // 'create' | 'edit'
  editId: null
};

/**
 * Carga clínicas desde Firestore y dibuja tabla con acciones.
 */
async function loadClinicas() {
  try {
    const snap = await getDocs(collection(db, 'clinicas'));
    const rows = [];
    snap.forEach(d => rows.push({ id: d.id, ...d.data() }));

    // Orden: primero por nombre, luego por id
    rows.sort((a,b) => (a.nombre || a.id).localeCompare((b.nombre || b.id), 'es'));

    if (!rows.length) {
      tablaClinicas.innerHTML = `<p style="font-size:13px;color:var(--muted);">Aún no hay clínicas.</p>`;
      return;
    }

    const htmlRows = rows.map(c => `
      <tr>
        <td>${c.id || '—'}</td>
        <td>${c.nombre || '—'}</td>
        <td>${c.estado || 'activa'}</td>
        <td class="text-right">
          <button class="btn btn-soft" data-action="edit-cli" data-id="${c.id}">Editar</button>
          <button class="btn btn-soft" data-action="toggle-cli" data-id="${c.id}">
            ${(c.estado === 'inactiva') ? 'Activar' : 'Desactivar'}
          </button>
        </td>
      </tr>
    `).join('');

    tablaClinicas.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Código</th>
            <th>Clínica</th>
            <th>Estado</th>
            <th class="text-right">Acciones</th>
          </tr>
        </thead>
        <tbody>${htmlRows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error(err);
    tablaClinicas.innerHTML = `<p style="color:#e11d48;font-size:13px;">Error al cargar clínicas.</p>`;
  }
}

/**
 * Busca el mayor C### existente en clinicas y devuelve el siguiente.
 * Ej: si existe C009 => devuelve C010
 */
async function getNextClinicaCodigo() {
  // Lee todas y busca el mayor C### existente
  const snap = await getDocs(collection(db, 'clinicas'));
  let max = 0;

  snap.forEach(d => {
    const id = d.id || '';
    const m = /^C(\d{3})$/.exec(id);
    if (m) max = Math.max(max, Number(m[1]));
  });

  const next = String(max + 1).padStart(3, '0');
  return `C${next}`;
}

/**
 * Abre modal clínica en modo CREATE.
 * @param {string} codigoSugerido - ej "C005"
 */
function openModalClinicaCreate(codigoSugerido) {
  modalState.open = 'clinica';
  clinicaUIState.mode = 'create';
  clinicaUIState.editId = null;

  openOverlay();
  if (modalProfesional) modalProfesional.style.display = 'none';
  if (modalProcedimiento) modalProcedimiento.style.display = 'none';
  if (modalRoles) modalRoles.style.display = 'none';

  modalClinica.style.display = 'block';

  mClinicaTitle.textContent = 'Nueva clínica';
  mClinicaSubtitle.textContent = 'Crear una nueva clínica';

  mClinicaCodigo.disabled = false;
  mClinicaCodigo.value = codigoSugerido || 'C001';
  mClinicaNombre.value = '';
  mClinicaEstado.value = 'activa';
  showError(mClinicaError, '');

  setTimeout(() => mClinicaNombre?.focus(), 50);
}

/**
 * Abre modal clínica en modo EDIT.
 * @param {string} id - docId existente ej "C001"
 */
async function openModalClinicaEdit(id) {
  modalState.open = 'clinica';
  clinicaUIState.mode = 'edit';
  clinicaUIState.editId = id;

  openOverlay();
  if (modalProfesional) modalProfesional.style.display = 'none';
  if (modalProcedimiento) modalProcedimiento.style.display = 'none';
  if (modalRoles) modalRoles.style.display = 'none';

  modalClinica.style.display = 'block';

  const snap = await getDoc(doc(db, 'clinicas', id));
  if (!snap.exists()) {
    showError(mClinicaError, 'No se encontró la clínica.');
    return;
  }
  const data = snap.data();

  mClinicaTitle.textContent = 'Editar clínica';
  mClinicaSubtitle.textContent = `Código: ${id}`;

  // En editar no cambiamos docId
  mClinicaCodigo.value = id;
  mClinicaCodigo.disabled = true;

  mClinicaNombre.value = data.nombre || '';
  mClinicaEstado.value = data.estado || 'activa';
  showError(mClinicaError, '');
}

btnNuevaClinica?.addEventListener('click', async () => {
  try {
    const next = await getNextClinicaCodigo();
    openModalClinicaCreate(next);
  } catch (err) {
    console.error(err);
    alert('No se pudo generar el código de clínica.');
  }
});

tablaClinicas?.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;

  const action = btn.dataset.action;
  const id = btn.dataset.id;

  try {
    if (action === 'edit-cli') {
      await openModalClinicaEdit(id);
      return;
    }

    if (action === 'toggle-cli') {
      const ref = doc(db, 'clinicas', id);
      const snap = await getDoc(ref);
      if (!snap.exists()) return alert('Clínica no encontrada.');
      const estado = snap.data().estado || 'activa';
      const nuevo = (estado === 'inactiva') ? 'activa' : 'inactiva';
      await setDoc(ref, { estado: nuevo, actualizadoEl: serverTimestamp() }, { merge: true });

      // refrescar caches para el modal profesional
      modalState.cacheClinicas = [];
      await ensureRolesClinicasLoaded();

      await loadClinicas();
      await loadProfesionales();
      return;
    }
  } catch (err) {
    console.error(err);
    alert('Error ejecutando acción. Revisa consola.');
  }
});

btnGuardarModalClinica?.addEventListener('click', async () => {
  try {
    showError(mClinicaError, '');

    const codigo = (mClinicaCodigo.value || '').trim().toUpperCase();
    const nombre = (mClinicaNombre.value || '').trim();
    const estado = (mClinicaEstado.value || 'activa').trim();

    if (!/^C\d{3}$/.test(codigo)) {
      showError(mClinicaError, 'Código inválido. Debe ser formato C001.');
      return;
    }
    if (!nombre) {
      showError(mClinicaError, 'Nombre de clínica es obligatorio.');
      return;
    }

    if (clinicaUIState.mode === 'create') {
      // Validar que no exista
      const exists = await getDoc(doc(db, 'clinicas', codigo));
      if (exists.exists()) {
        showError(mClinicaError, 'Ese código ya existe. Usa otro.');
        return;
      }
      await setDoc(doc(db, 'clinicas', codigo), {
        id: codigo,
        codigo,
        nombre,
        estado,
        creadoEl: serverTimestamp(),
        actualizadoEl: serverTimestamp()
      }, { merge: true });
    } else {
      // Edit
      await updateDoc(doc(db, 'clinicas', clinicaUIState.editId), {
        nombre,
        estado,
        actualizadoEl: serverTimestamp()
      });
    }

    // refrescar caches para el modal profesional
    modalState.cacheClinicas = [];
    await ensureRolesClinicasLoaded();

    closeAllModals();
    await loadClinicas();
    await loadProfesionales();
  } catch (err) {
    console.error(err);
    showError(mClinicaError, 'No se pudo guardar. Revisa consola.');
  }
});


/* =========================================================
   9) MÓDULO PRODUCCIÓN (LECTURA SIMPLE)
   ========================================================= */

btnRefrescarProduccion?.addEventListener('click', () => {
  loadProduccion();
});

/**
 * Carga datos de producción (stub por ahora).
 * Ideal: filtrar por periodo seleccionado.
 */
async function loadProduccion() {
  tablaProduccion.innerHTML = `<p style="font-size:13px;color:var(--muted);">
    Cargando producción...
  </p>`;

  try {
    // TODO: ajustar query según tu estructura real (ej: colección "produccion")
    const snap = await getDocs(collection(db, 'produccion'));
    const rows = [];
    snap.forEach(doc => rows.push({ id: doc.id, ...doc.data() }));

    if (!rows.length) {
      tablaProduccion.innerHTML = `<p style="font-size:13px;color:var(--muted);">
        No hay registros de producción para el período seleccionado.
      </p>`;
      return;
    }

    const htmlRows = rows.map(r => `
      <tr>
        <td>${r.fecha || '—'}</td>
        <td>${r.profesionalNombre || '—'}</td>
        <td>${r.procedimientoNombre || '—'}</td>
        <td>${r.pacienteIniciales || '—'}</td>
        <td class="text-right">$${(r.valor ?? 0).toLocaleString('es-CL')}</td>
      </tr>
    `).join('');

    tablaProduccion.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th>Profesional</th>
            <th>Procedimiento</th>
            <th>Paciente</th>
            <th class="text-right">Valor caso</th>
          </tr>
        </thead>
        <tbody>${htmlRows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error('Error cargando producción:', err);
    tablaProduccion.innerHTML = `<p style="color:#e11d48;font-size:13px;">
      Error al cargar producción.
    </p>`;
  }
}

/* =========================================================
   10) MÓDULO LIQUIDACIONES (LECTURA + BOTÓN CALCULAR)
   ========================================================= */
/**
 * Carga la tabla de liquidaciones desde Firestore.
 */
async function loadLiquidaciones() {
  tablaLiquidaciones.innerHTML = `<p style="font-size:13px;color:var(--muted);">
    Cargando liquidaciones...
  </p>`;

  try {
    const snap = await getDocs(collection(db, 'liquidaciones'));
    const rows = [];
    snap.forEach(doc => rows.push({ id: doc.id, ...doc.data() }));

    if (!rows.length) {
      tablaLiquidaciones.innerHTML = `<p style="font-size:13px;color:var(--muted);">
        Aún no hay liquidaciones registradas.
      </p>`;
      return;
    }

    const htmlRows = rows.map(l => `
      <tr>
        <td>${l.profesionalNombre || '—'}</td>
        <td>${l.periodo || '—'}</td>
        <td class="text-right">$${(l.montoTotal ?? 0).toLocaleString('es-CL')}</td>
        <td>${l.estado || 'borrador'}</td>
      </tr>
    `).join('');

    tablaLiquidaciones.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Profesional</th>
            <th>Período</th>
            <th class="text-right">Monto total</th>
            <th>Estado</th>
          </tr>
        </thead>
        <tbody>${htmlRows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error('Error cargando liquidaciones:', err);
    tablaLiquidaciones.innerHTML = `<p style="color:#e11d48;font-size:13px;">
      Error al cargar liquidaciones.
    </p>`;
  }
}

/* ================== CALCULAR LIQUIDACIONES (STUB) ================== */

btnCalcularLiquidaciones?.addEventListener('click', async () => {
  const periodo = liqMesSelect.value;
  alert(`Aquí implementaremos el cálculo de liquidaciones para el período ${periodo}.`);
  // En la siguiente iteración hacemos:
  // 1. Leer producción del período.
  // 2. Agrupar por profesional.
  // 3. Aplicar reglas desde "profesionales" y "procedimientos".
  // 4. Guardar/actualizar documentos en "liquidaciones".
  // 5. Llamar loadLiquidaciones() nuevamente.
});


/* ================== IMPORTAR PROFESIONALES DESDE CSV ================== */

// Normaliza RUT para usarlo como ID de documento (sin puntos, guión ni espacios, en mayúsculas)
/**
 * Normaliza RUT para usarlo como docId (sin puntos ni guión).
 */
function normalizaRut(rutRaw = '') {
  return rutRaw
    .toString()
    .trim()
    .replace(/\./g, '')
    .replace(/-/g, '')
    .replace(/\s+/g, '')
    .toUpperCase();
}

// Parsea un CSV simple a objetos { rol, rut, razonSocial, nombre }
/**
 * Parse específico para CSV de Profesionales:
 * - Detecta delimitador ; o ,
 * - Soporta listas separadas por |
 * - Requiere columnas rut y nombreProfesional
 */
function parseProfesionalesCsv(text) {
  const lines = text
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(l => l.length > 0);

  if (!lines.length) return [];

  const delimiter = lines[0].includes(';') ? ';' : ',';
  const headers = lines[0].split(delimiter).map(h => h.trim());

  const H = (name) => headers.findIndex(h => h.toLowerCase() === name.toLowerCase());

  const idxRut = H('rut');
  const idxNom = H('nombreProfesional');
  if (idxRut === -1 || idxNom === -1) {
    throw new Error('CSV debe tener columnas: rut, nombreProfesional');
  }

  const idxRazon = H('razonSocial');
  const idxTipo = H('tipoPersona');
  const idxCorreoP = H('correoPersonal');
  const idxTel = H('telefono');
   
  const idxRutEmp = H('rutEmpresa');
  const idxCorreoE = H('correoEmpresa');
  const idxDirEmp = H('direccionEmpresa');
  const idxCiudadEmp = H('ciudadEmpresa');

  const idxGiro = H('giro');
  const idxDir = H('direccion');
  const idxRolP = H('rolPrincipal');
  const idxRolS = H('rolesSecundarios');
  const idxClin = H('clinicas');
  const idxTD = H('tieneDescuento');
  const idxDUF = H('descuentoUF'); // ✅ UF (única)
  const idxDR = H('descuentoRazon');
  const idxEstado = H('estado');

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(delimiter);

    const rut = (parts[idxRut] || '').trim();
    const nombreProfesional = (parts[idxNom] || '').trim();
    if (!rut || !nombreProfesional) continue;

    const tieneDescuento = ((parts[idxTD] || 'NO').trim().toUpperCase() === 'SI');
    const rawUF = (idxDUF >= 0 ? (parts[idxDUF] || '0') : '0').toString().trim().replace(',', '.');
    const descuentoUF = tieneDescuento ? (Number(rawUF) || 0) : 0;
    const descuentoRazon = tieneDescuento ? ((parts[idxDR] || '').trim()) : '';

    const parseList = (v) => (v || '')
      .split('|')
      .map(x => x.trim())
      .filter(Boolean);

    rows.push({
      rut,
      nombreProfesional,
      razonSocial: idxRazon >= 0 ? (parts[idxRazon] || '').trim() : '',
      tipoPersona: idxTipo >= 0 ? ((parts[idxTipo] || 'natural').trim().toLowerCase()) : 'natural',
      correoPersonal: idxCorreoP >= 0 ? (parts[idxCorreoP] || '').trim() : '',
      telefono: idxTel >= 0 ? (parts[idxTel] || '').trim() : '',
      rutEmpresa: idxRutEmp >= 0 ? (parts[idxRutEmp] || '').trim() : '',
      correoEmpresa: idxCorreoE >= 0 ? (parts[idxCorreoE] || '').trim() : '',
      direccionEmpresa: idxDirEmp >= 0 ? (parts[idxDirEmp] || '').trim() : '',
      ciudadEmpresa: idxCiudadEmp >= 0 ? (parts[idxCiudadEmp] || '').trim() : '',
      giro: idxGiro >= 0 ? (parts[idxGiro] || '').trim() : '',
      direccion: idxDir >= 0 ? (parts[idxDir] || '').trim() : '',
      rolPrincipal: idxRolP >= 0 ? (parts[idxRolP] || '').trim() : '',
      rolesSecundarios: idxRolS >= 0 ? parseList(parts[idxRolS]) : [],
      clinicas: idxClin >= 0 ? parseList(parts[idxClin]) : [],
      tieneDescuento,
      descuentoUF,
      descuentoRazon,
      estado: idxEstado >= 0 ? ((parts[idxEstado] || 'activo').trim().toLowerCase()) : 'activo'
    });
  }

  return rows;
}

/**
 * Convierte texto en un slug seguro para IDs.
 */
function slugify(text = '') {
  return text
    .toString()
    .trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

/**
 * Parse CSV genérico: primera línea headers, resto filas.
 */
function parseCsv(text) {
  const lines = text
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(l => l.length > 0);

  if (!lines.length) return { headers: [], rows: [] };

  const delimiter = lines[0].includes(';') ? ';' : ',';
  const headers = lines[0].split(delimiter).map(h => h.trim());

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(delimiter).map(p => p.trim());
    const obj = {};
    headers.forEach((h, idx) => obj[h] = parts[idx] ?? '');
    rows.push(obj);
  }
  return { headers, rows };
}

/**
 * Convierte valores con $/puntos/comas a número entero.
 * Útil para importaciones de tarifas.
 */
function moneyToNumber(v) {
  if (v == null) return 0;
  return Number(
    v.toString()
      .replace(/\$/g, '')
      .replace(/\./g, '')
      .replace(/,/g, '')
      .replace(/\s+/g, '')
      .trim()
  ) || 0;
}


btnImportProfesionales?.addEventListener('click', async () => {
  if (!fileProfesionales?.files?.length) {
    alert('Selecciona primero un archivo CSV de profesionales.');
    return;
  }

  const file = fileProfesionales.files[0];
  importProfResultado.textContent = 'Leyendo archivo…';
  importProfResultado.style.color = 'var(--muted)';

  const reader = new FileReader();

  reader.onload = async (e) => {
    try {
      const text = e.target.result;
      const rows = parseProfesionalesCsv(text);

      if (!rows.length) {
        importProfResultado.textContent = 'El archivo no tiene filas válidas.';
        importProfResultado.style.color = '#e11d48';
        return;
      }

      importProfResultado.textContent = `Importando ${rows.length} profesionales…`;
      importProfResultado.style.color = 'var(--muted)';

      let ok = 0;
      let fail = 0;

      for (const row of rows) {
        const id = normalizaRut(row.rut);
        if (!id) {
          fail++;
          continue;
        }

        const docRef = doc(db, 'profesionales', id);

        try {
         // Roles (IDs determinísticos)
         const rolPrincipalId = row.rolPrincipal ? `r_${slugify(row.rolPrincipal)}` : null;
         const rolesSecundariosIds = (row.rolesSecundarios || []).map(x => `r_${slugify(x)}`);
         
         // Clínicas: resolver por nombre -> C### (y crear si no existe)
         // ✅ Default pedido: si el CSV NO trae clínicas => asignar TODAS las clínicas activas
         let clinicasIds = [];
         
         // cargar clínicas actuales 1 vez (para este profesional) y mapear nombre -> id
         const snapCli = await getDocs(collection(db, 'clinicas'));
         const existentes = [];
         snapCli.forEach(d => existentes.push({ id: d.id, ...d.data() }));
         
         const normName = (s='') => s.toString().trim().toLowerCase()
           .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
         
         const byNombre = new Map();
         for (const c of existentes) if (c.nombre) byNombre.set(normName(c.nombre), c.id);
         
         const activasIds = existentes
           .filter(c => (c.estado || 'activa') === 'activa')
           .map(c => c.id);
         
         if ((row.clinicas || []).length) {
           // respetar clínicas del CSV
           for (const nombreCli of row.clinicas) {
             const key = normName(nombreCli);
             let idCli = byNombre.get(key);
         
             if (!idCli) {
               // crear nueva C###
               idCli = await getNextClinicaCodigo();
               await setDoc(doc(db, 'clinicas', idCli), {
                 id: idCli,
                 codigo: idCli,
                 nombre: nombreCli,
                 estado: 'activa',
                 creadoEl: serverTimestamp(),
                 actualizadoEl: serverTimestamp()
               }, { merge: true });
               byNombre.set(key, idCli);
               modalState.cacheClinicas = [];
               activasIds.push(idCli);
             }
         
             clinicasIds.push(idCli);
           }
         } else {
           // ✅ default: todas las clínicas activas
           clinicasIds = [...activasIds];
         }

         
         await setDoc(docRef, {
           rut: row.rut,
           rutId: id,
           nombreProfesional: row.nombreProfesional,
           razonSocial: row.razonSocial || null,
           giro: row.giro || null,
           direccion: row.direccion || null,
         
           rolPrincipalId,
           rolesSecundariosIds,
           clinicasIds,
         
           tieneDescuento: !!row.tieneDescuento,
           descuentoUF: row.tieneDescuento ? (row.descuentoUF || 0) : 0,
           descuentoRazon: row.tieneDescuento ? (row.descuentoRazon || null) : null,
         
           estado: (row.estado || 'activo'),
           actualizadoEl: serverTimestamp()
         }, { merge: true });


          ok++;
        } catch (e) {
          console.error('Error guardando profesional', row, e);
          fail++;
        }
      }

      importProfResultado.textContent =
        `Importación completada. OK: ${ok}, con problemas: ${fail}.`;
      importProfResultado.style.color = 'var(--ink)';

      // Recargar tabla de profesionales para ver los datos nuevos
      loadProfesionales();

    } catch (err) {
      console.error('Error en importación CSV:', err);
      importProfResultado.textContent =
        'Error al importar profesionales: ' + (err.message || err);
      importProfResultado.style.color = '#e11d48';
    }
  };

  reader.onerror = () => {
    importProfResultado.textContent = 'No se pudo leer el archivo.';
    importProfResultado.style.color = '#e11d48';
  };

  reader.readAsText(file, 'utf-8');
});

/* ================== IMPORTAR ROLES DESDE CSV ================== */
btnImportRoles?.addEventListener('click', async () => {
  if (!fileRoles?.files?.length) {
    alert('Selecciona primero un CSV de roles.');
    return;
  }

  const file = fileRoles.files[0];
  importRolesResultado.textContent = 'Leyendo archivo…';
  importRolesResultado.style.color = 'var(--muted)';

  const reader = new FileReader();

  reader.onload = async (e) => {
    try {
      const text = e.target.result;
      const { rows } = parseCsv(text);

      // Acepta columna: rol / Rol
      const cleanNames = rows
        .map(r => (r.rol || r.Rol || '').trim())
        .filter(Boolean);

      if (!cleanNames.length) throw new Error('CSV vacío o sin columna "rol".');

      let creados = 0;
      let actualizados = 0;

      for (const nombre of cleanNames) {
        const rolId = `r_${slugify(nombre)}`;

        // Upsert rol (ID determinístico)
        await setDoc(doc(db, 'roles', rolId), {
          id: rolId,
          nombre,
          estado: 'activo',
          actualizadoEl: serverTimestamp()
        }, { merge: true });

        // No distinguimos perfecto create/update sin leer antes,
        // pero para UI esto es suficiente.
        actualizados++;
      }

      importRolesResultado.textContent =
        `Roles importados/actualizados: ${actualizados}.`;
      importRolesResultado.style.color = 'var(--ink)';

      // refrescar caches
      modalState.cacheRoles = [];
      await ensureRolesClinicasLoaded();

      await loadRolesTable();
      await loadProfesionales();

    } catch (err) {
      console.error(err);
      importRolesResultado.textContent = 'Error: ' + (err.message || err);
      importRolesResultado.style.color = '#e11d48';
    }
  };

  reader.onerror = () => {
    importRolesResultado.textContent = 'No se pudo leer el archivo.';
    importRolesResultado.style.color = '#e11d48';
  };

  reader.readAsText(file, 'utf-8');
});


/* ================== IMPORTAR CLÍNICAS DESDE CSV ================== */
btnImportClinicas?.addEventListener('click', async () => {
  if (!fileClinicas?.files?.length) {
    alert('Selecciona primero un CSV de clínicas.');
    return;
  }

  const file = fileClinicas.files[0];
  importClinicasResultado.textContent = 'Leyendo archivo…';
  importClinicasResultado.style.color = 'var(--muted)';

  const reader = new FileReader();

  reader.onload = async (e) => {
    try {
      const text = e.target.result;
      const { rows } = parseCsv(text);

      const cleanNames = rows
        .map(r => (r.clinica || r.Clinica || r['Clínica'] || '').trim())
        .filter(Boolean);

      if (!cleanNames.length) throw new Error('CSV vacío o sin columna "clinica".');

      // 1) cargar clínicas existentes y mapear por nombre normalizado -> C###
      const snap = await getDocs(collection(db, 'clinicas'));
      const existentes = [];
      snap.forEach(d => existentes.push({ id: d.id, ...d.data() }));

      const normName = (s='') => s.toString().trim().toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g,'');

      const byNombre = new Map();
      for (const c of existentes) {
        if (c.nombre) byNombre.set(normName(c.nombre), c.id);
      }

      let creadas = 0;
      let actualizadas = 0;

      for (const nombre of cleanNames) {
        const key = normName(nombre);
        const foundId = byNombre.get(key);

        if (foundId) {
          await setDoc(doc(db, 'clinicas', foundId), {
            nombre,
            estado: 'activa',
            actualizadoEl: serverTimestamp()
          }, { merge: true });
          actualizadas++;
          continue;
        }

        const next = await getNextClinicaCodigo(); // C### siguiente
        await setDoc(doc(db, 'clinicas', next), {
          id: next,
          codigo: next,
          nombre,
          estado: 'activa',
          creadoEl: serverTimestamp(),
          actualizadoEl: serverTimestamp()
        }, { merge: true });

        byNombre.set(key, next);
        creadas++;
      }

      importClinicasResultado.textContent =
        `Clínicas importadas. Nuevas: ${creadas}, actualizadas: ${actualizadas}.`;
      importClinicasResultado.style.color = 'var(--ink)';

      // refrescar caches para el modal profesional
      modalState.cacheClinicas = [];
      await ensureRolesClinicasLoaded();

      await loadClinicas();
      await loadProfesionales();

    } catch (err) {
      console.error(err);
      importClinicasResultado.textContent = 'Error: ' + (err.message || err);
      importClinicasResultado.style.color = '#e11d48';
    }
  };

  reader.onerror = () => {
    importClinicasResultado.textContent = 'No se pudo leer el archivo.';
    importClinicasResultado.style.color = '#e11d48';
  };

  reader.readAsText(file, 'utf-8');
});


/* ================== IMPORTAR PROCEDIMIENTOS + TARIFAS DESDE CSV ================== */
btnImportTarifas?.addEventListener('click', async () => {
  if (!fileTarifas?.files?.length) {
    alert('Selecciona primero un CSV de tarifas.');
    return;
  }

  const file = fileTarifas.files[0];
  importTarifasResultado.textContent = 'Leyendo archivo…';
  importTarifasResultado.style.color = 'var(--muted)';

  const reader = new FileReader();

  reader.onload = async (e) => {
    try {
      const text = e.target.result;
      const { rows } = parseCsv(text);

      // Normalizador (para comparar nombres)
      const normName = (s='') => (s ?? '')
        .toString().trim().toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g,'');

      // 1) Limpieza / normalización de filas
      const cleanRows = (rows || []).map(r => {
        const clinica = (r.clinica || r.Clinica || r['Clínica'] || '').trim();
        const cirugia = (r.cirugia || r.Cirugia || r['Cirugía'] || r['Cirugías'] || '').trim();

        const precioTotal = moneyToNumber(r.precioTotal || r['precioTotal'] || r['Precios'] || r['Precio']);
        const derechosPabellon = moneyToNumber(r.derechosPabellon || r['Derechos de Pabellón'] || r['derechosPabellon']);
        const hmq = moneyToNumber(r.hmq || r.HMQ);
        const insumos = moneyToNumber(r.insumos || r.Insumos);

        return { clinica, cirugia, precioTotal, derechosPabellon, hmq, insumos };
      }).filter(x => x.clinica && x.cirugia);

      if (!cleanRows.length) throw new Error('CSV vacío o sin columnas mínimas (clinica, cirugia).');

      // 2) Deduplicación dentro del mismo CSV (clínica+cirugía)
      const seen = new Set();
      const dedupRows = [];
      for (const r of cleanRows) {
        const key = `${normName(r.clinica)}__${normName(r.cirugia)}`;
        if (seen.has(key)) continue;
        seen.add(key);
        dedupRows.push(r);
      }

      // 3) Preparar batch + commit parcial
      let batch = writeBatch(db);
      let ops = 0;

      const commitIfNeeded = async () => {
        // Seguridad: mantener margen bajo el máximo
        if (ops >= 450) {
          await batch.commit();
          batch = writeBatch(db);
          ops = 0;
        }
      };

      // 4) Cache de clínicas (1 sola lectura)
      importTarifasResultado.textContent = `Leyendo clínicas… (cache)`;
      const snapCliInit = await getDocs(collection(db, 'clinicas'));
      const clinicaByNombre = new Map(); // nombreNormalizado -> C###
      const clinicaById = new Set();     // C### existentes

      snapCliInit.forEach(d => {
        const data = d.data() || {};
        clinicaById.add(d.id);
        if (data.nombre) clinicaByNombre.set(normName(data.nombre), d.id);
      });

      let upsertProc = 0;
      let upsertTar = 0;
      let createdClinicas = 0;

      importTarifasResultado.textContent = `Importando… (${dedupRows.length} filas)`;

      for (const r of dedupRows) {
        // ✅ Resolver clinicaId
        // - Si viene "C001" => usar directo
        // - Si viene nombre => buscar en cache por nombre normalizado
        let clinicaId = null;
        const rawClin = (r.clinica || '').trim();

        if (/^C\d{3}$/i.test(rawClin)) {
          clinicaId = rawClin.toUpperCase();

          // (Opcional pero útil) Si viene C### y no existe, la creamos con nombre = C###
          if (!clinicaById.has(clinicaId)) {
            batch.set(doc(db, 'clinicas', clinicaId), {
              id: clinicaId,
              codigo: clinicaId,
              nombre: clinicaId, // placeholder; luego puedes editar desde UI
              estado: 'activa',
              creadoEl: serverTimestamp(),
              actualizadoEl: serverTimestamp()
            }, { merge: true });
            ops++; createdClinicas++; await commitIfNeeded();
            clinicaById.add(clinicaId);
          }
        } else {
          // Nombre de clínica
          clinicaId = clinicaByNombre.get(normName(rawClin)) || null;

          if (!clinicaId) {
            clinicaId = await getNextClinicaCodigo(); // C###
            batch.set(doc(db, 'clinicas', clinicaId), {
              id: clinicaId,
              codigo: clinicaId,
              nombre: rawClin,
              estado: 'activa',
              creadoEl: serverTimestamp(),
              actualizadoEl: serverTimestamp()
            }, { merge: true });
            ops++; createdClinicas++; await commitIfNeeded();

            // Actualiza cache
            clinicaByNombre.set(normName(rawClin), clinicaId);
            clinicaById.add(clinicaId);
          }
        }

        // IDs de docs
        const procedimientoId = `p_${slugify(r.cirugia)}`;
        const tarifaId = `t_${clinicaId}__${procedimientoId}`;

        // procedimiento (upsert)
        batch.set(doc(db, 'procedimientos', procedimientoId), {
          id: procedimientoId,
          nombre: r.cirugia,
          tipo: 'ambulatorio',
          actualizadoEl: serverTimestamp()
        }, { merge: true });
        ops++; upsertProc++; await commitIfNeeded();

        // tarifa cruce (upsert)
        batch.set(doc(db, 'tarifas', tarifaId), {
          id: tarifaId,
          clinicaId,
          clinicaNombre: (/^C\d{3}$/i.test(rawClin)) ? null : rawClin, // si venía C###, no inventamos nombre real
          procedimientoId,
          procedimientoNombre: r.cirugia,
          precioTotal: r.precioTotal || 0,
          derechosPabellon: r.derechosPabellon || 0,
          hmq: r.hmq || 0,
          insumos: r.insumos || 0,
          actualizadoEl: serverTimestamp()
        }, { merge: true });
        ops++; upsertTar++; await commitIfNeeded();
      }

      // Commit final
      if (ops > 0) await batch.commit();

      importTarifasResultado.textContent =
        `Listo ✅ Clínicas creadas: ${createdClinicas}. ` +
        `Procedimientos upsert: ${upsertProc}. Tarifas upsert: ${upsertTar}.`;
      importTarifasResultado.style.color = 'var(--ink)';

      // recarga
      loadProcedimientos();

    } catch (err) {
      console.error(err);
      importTarifasResultado.textContent = 'Error: ' + (err?.message || err);
      importTarifasResultado.style.color = '#e11d48';
    }
  };

  reader.onerror = () => {
    importTarifasResultado.textContent = 'No se pudo leer el archivo.';
    importTarifasResultado.style.color = '#e11d48';
  };

  reader.readAsText(file, 'utf-8');
});




/* =========================================================
   FIN DASHBOARD
   ========================================================= */
