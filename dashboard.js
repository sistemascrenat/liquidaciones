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



/* =========================================================
   2) HELPERS GENERALES
   ========================================================= */

function getCurrentPeriodoText() {
  const now = new Date();
  const meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio',
                 'Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  return `${meses[now.getMonth()]} ${now.getFullYear()}`;
}

function showView(id) {
  views.forEach(v => v.classList.toggle('active', v.id === id));
}

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
    "descuentoMonto",
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
    "0",
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

function lockScroll(lock) {
  document.body.style.overflow = lock ? 'hidden' : '';
}

function showError(el, msg) {
  if (!el) return;
  el.textContent = msg || '';
  el.style.display = msg ? 'block' : 'none';
}

function openOverlay() {
  if (!modalOverlay) return;
  modalOverlay.style.display = 'flex';
  lockScroll(true);
}

function closeAllModals() {
  modalState.open = null;

  if (modalProfesional) modalProfesional.style.display = 'none';
  if (modalProcedimiento) modalProcedimiento.style.display = 'none';

  if (modalOverlay) modalOverlay.style.display = 'none';
  lockScroll(false);

  showError(mProfError, '');
  showError(mProcError, '');
}

function onlyDigits(v='') {
  return v.toString().replace(/[^\d]/g,'');
}

function parseMoneyInput(v='') {
  return Number(onlyDigits(v)) || 0;
}

function formatMoneyCLP(n=0) {
  return (Number(n)||0).toLocaleString('es-CL');
}

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

function getCheckedValues(containerEl) {
  if (!containerEl) return [];
  return Array.from(containerEl.querySelectorAll('input[type="checkbox"]'))
    .filter(x => x.checked)
    .map(x => x.value);
}

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
  renderChecklist(mProfClinicas, modalState.cacheClinicas, []);

  mProfEstado.value = 'activo';
  mProfTieneDesc.checked = false;
  mProfDescMonto.value = '';
  mProfDescRazon.value = '';

  showError(mProfError, '');

  setTimeout(() => mProfRut?.focus(), 50);
}

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

  renderSelectRolPrincipal(data.rolPrincipalId || '');
  renderChecklist(mProfRolesSec, modalState.cacheRoles, data.rolesSecundariosIds || []);
  renderChecklist(mProfClinicas, modalState.cacheClinicas, data.clinicasIds || []);

  mProfEstado.value = (data.estado || 'activo');
  mProfTieneDesc.checked = !!data.tieneDescuento;
  mProfDescMonto.value = data.tieneDescuento ? formatMoneyCLP(data.descuentoMonto || 0) : '';
  mProfDescRazon.value = data.tieneDescuento ? (data.descuentoRazon || '') : '';

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
    const descuentoMonto = tieneDescuento ? parseMoneyInput(mProfDescMonto.value) : 0;
    const descuentoRazon = tieneDescuento ? (mProfDescRazon.value || '').trim() : '';

    const patch = {
      rut: rutRaw,
      rutId: docId,
      nombreProfesional: nombre,
      razonSocial: (mProfRazon.value || '').trim() || null,
      giro: (mProfGiro.value || '').trim() || null,
      direccion: (mProfDireccion.value || '').trim() || null,

      rolPrincipalId,
      rolesSecundariosIds,
      clinicasIds,

      tieneDescuento,
      descuentoMonto: tieneDescuento ? descuentoMonto : 0,
      descuentoRazon: tieneDescuento ? (descuentoRazon || null) : null,
      descuentoMoneda: 'CLP',

      estado: (mProfEstado.value || 'activo'),
      actualizadoEl: serverTimestamp()
    };

    // Si es create, ponemos creadoEl si no existía
    if (modalState.profMode === 'create') {
      patch.creadoEl = serverTimestamp();
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
    loadLiquidaciones(); // estado inicial
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

async function loadProfesionales() {
  try {
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

    const htmlRows = rows.map(p => {
      const desc = p.tieneDescuento ? `$${(p.descuentoMonto ?? 0).toLocaleString('es-CL')} (${p.descuentoRazon || '—'})` : 'No';
      const estado = p.estado || 'activo';
      return `
        <tr>
          <td>${p.nombreProfesional || p.nombre || '—'}</td>
          <td>${p.rut || '—'}</td>
          <td>${p.razonSocial || '—'}</td>
          <td>${(p.clinicasIds || []).length ? (p.clinicasIds.join(', ')) : '—'}</td>
          <td>${p.rolPrincipalId || '—'}</td>
          <td>${desc}</td>
          <td>${estado}</td>
          <td class="text-right">
            <button class="btn btn-soft" data-action="edit-prof" data-id="${p.rutId || p.id}">Editar</button>
            <button class="btn btn-soft" data-action="toggle-prof" data-id="${p.rutId || p.id}">
              ${estado === 'inactivo' ? 'Activar' : 'Desactivar'}
            </button>
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
   9) MÓDULO PRODUCCIÓN (LECTURA SIMPLE)
   ========================================================= */

btnRefrescarProduccion?.addEventListener('click', () => {
  loadProduccion();
});

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
  const idxGiro = H('giro');
  const idxDir = H('direccion');
  const idxRolP = H('rolPrincipal');
  const idxRolS = H('rolesSecundarios');
  const idxClin = H('clinicas');
  const idxTD = H('tieneDescuento');
  const idxDM = H('descuentoMonto');
  const idxDR = H('descuentoRazon');
  const idxEstado = H('estado');

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(delimiter);

    const rut = (parts[idxRut] || '').trim();
    const nombreProfesional = (parts[idxNom] || '').trim();
    if (!rut || !nombreProfesional) continue;

    const tieneDescuento = ((parts[idxTD] || 'NO').trim().toUpperCase() === 'SI');
    const descuentoMonto = tieneDescuento ? (Number((parts[idxDM] || '0').toString().replace(/[^\d]/g, '')) || 0) : 0;
    const descuentoRazon = tieneDescuento ? ((parts[idxDR] || '').trim()) : '';

    const parseList = (v) => (v || '')
      .split('|')
      .map(x => x.trim())
      .filter(Boolean);

    rows.push({
      rut,
      nombreProfesional,
      razonSocial: idxRazon >= 0 ? (parts[idxRazon] || '').trim() : '',
      giro: idxGiro >= 0 ? (parts[idxGiro] || '').trim() : '',
      direccion: idxDir >= 0 ? (parts[idxDir] || '').trim() : '',
      rolPrincipal: idxRolP >= 0 ? (parts[idxRolP] || '').trim() : '',
      rolesSecundarios: idxRolS >= 0 ? parseList(parts[idxRolS]) : [],
      clinicas: idxClin >= 0 ? parseList(parts[idxClin]) : [],
      tieneDescuento,
      descuentoMonto,
      descuentoRazon,
      estado: idxEstado >= 0 ? ((parts[idxEstado] || 'activo').trim().toLowerCase()) : 'activo'
    });
  }

  return rows;
}


function slugify(text = '') {
  return text
    .toString()
    .trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

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
         const rolPrincipalId = row.rolPrincipal ? `r_${slugify(row.rolPrincipal)}` : null;
         const rolesSecundariosIds = (row.rolesSecundarios || []).map(x => `r_${slugify(x)}`);
         const clinicasIds = (row.clinicas || []).map(x => `c_${slugify(x)}`);
         
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
           descuentoMonto: row.tieneDescuento ? (row.descuentoMonto || 0) : 0,
           descuentoRazon: row.tieneDescuento ? (row.descuentoRazon || null) : null,
           descuentoMoneda: 'CLP',
         
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

      const cleanRows = rows
        .map(r => ({ rol: (r.rol || r.Rol || '').trim() }))
        .filter(r => r.rol);

      if (!cleanRows.length) throw new Error('CSV vacío o sin columna "rol".');

      const batch = writeBatch(db);
      let count = 0;

      for (const r of cleanRows) {
        const rolId = `r_${slugify(r.rol)}`;
        batch.set(doc(db, 'roles', rolId), {
          id: rolId,
          nombre: r.rol,
          estado: 'activo',
          actualizadoEl: new Date()
        }, { merge: true });
        count++;
      }

      await batch.commit();
      importRolesResultado.textContent = `Roles importados/actualizados: ${count}.`;
      importRolesResultado.style.color = 'var(--ink)';

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

      const cleanRows = rows
        .map(r => ({
          clinica: (r.clinica || r.Clinica || r['Clínica'] || '').trim()
        }))
        .filter(r => r.clinica);

      if (!cleanRows.length) throw new Error('CSV vacío o sin columna "clinica".');

      const batch = writeBatch(db);
      let count = 0;

      for (const r of cleanRows) {
        const clinicaId = `c_${slugify(r.clinica)}`;
        batch.set(doc(db, 'clinicas', clinicaId), {
          id: clinicaId,
          nombre: r.clinica,
          estado: 'activa',
          actualizadoEl: new Date()
        }, { merge: true });
        count++;
      }

      await batch.commit();
      importClinicasResultado.textContent = `Clínicas importadas/actualizadas: ${count}.`;
      importClinicasResultado.style.color = 'var(--ink)';

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

      const cleanRows = rows.map(r => {
        const clinica = (r.clinica || r.Clinica || r['Clínica'] || '').trim();
        const cirugia = (r.cirugia || r.Cirugia || r['Cirugía'] || r['Cirugías'] || '').trim();

        const precioTotal = moneyToNumber(r.precioTotal || r['precioTotal'] || r['Precios'] || r['Precio']);
        const derechosPabellon = moneyToNumber(r.derechosPabellon || r['Derechos de Pabellón'] || r['derechosPabellon']);
        const hmq = moneyToNumber(r.hmq || r.HMQ);
        const insumos = moneyToNumber(r.insumos || r.Insumos);

        return { clinica, cirugia, precioTotal, derechosPabellon, hmq, insumos };
      }).filter(x => x.clinica && x.cirugia);

      if (!cleanRows.length) throw new Error('CSV vacío o sin columnas mínimas (clinica, cirugia).');

      let batch = writeBatch(db);
      let ops = 0;

      const commitIfNeeded = async () => {
        if (ops >= 450) {
          await batch.commit();
          batch = writeBatch(db);
          ops = 0;
        }
      };

      let upsertProc = 0;
      let upsertTar = 0;

      for (const r of cleanRows) {
        const clinicaId = `c_${slugify(r.clinica)}`;
        const procedimientoId = `p_${slugify(r.cirugia)}`;
        const tarifaId = `t_${clinicaId}__${procedimientoId}`;

        // clínica
        batch.set(doc(db, 'clinicas', clinicaId), {
          id: clinicaId,
          nombre: r.clinica,
          estado: 'activa',
          actualizadoEl: new Date()
        }, { merge: true });
        ops++; await commitIfNeeded();

        // procedimiento
        batch.set(doc(db, 'procedimientos', procedimientoId), {
          id: procedimientoId,
          nombre: r.cirugia,
          tipo: 'ambulatorio',
          actualizadoEl: new Date()
        }, { merge: true });
        ops++; upsertProc++; await commitIfNeeded();

        // tarifa cruce
        batch.set(doc(db, 'tarifas', tarifaId), {
          id: tarifaId,
          clinicaId,
          clinicaNombre: r.clinica,
          procedimientoId,
          procedimientoNombre: r.cirugia,
          precioTotal: r.precioTotal || 0,
          derechosPabellon: r.derechosPabellon || 0,
          hmq: r.hmq || 0,
          insumos: r.insumos || 0,
          actualizadoEl: new Date()
        }, { merge: true });
        ops++; upsertTar++; await commitIfNeeded();
      }

      if (ops > 0) await batch.commit();

      importTarifasResultado.textContent =
        `Listo. Procedimientos upsert: ${upsertProc}. Tarifas upsert: ${upsertTar}.`;
      importTarifasResultado.style.color = 'var(--ink)';

      loadProcedimientos();

    } catch (err) {
      console.error(err);
      importTarifasResultado.textContent = 'Error: ' + (err.message || err);
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
