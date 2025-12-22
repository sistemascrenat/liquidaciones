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
      // leer doc actual
      const ref = doc(db, 'profesionales', rutId);
      // no importaste getDoc/updateDoc, así que hacemos merge directo con estado nuevo:
      const nuevoEstado = (btn.textContent.includes('Activar')) ? 'activo' : 'inactivo';
      await setDoc(ref, { estado: nuevoEstado, actualizadoEl: serverTimestamp() }, { merge: true });
      await loadProfesionales();
      return;
    }

    if (action === 'edit-prof') {
      const nombreProfesional = prompt('Nuevo nombre (deja vacío para no cambiar):') || '';
      const razonSocial = prompt('Nueva razón social (deja vacío para no cambiar):') || '';
      const giro = prompt('Nuevo giro (deja vacío para no cambiar):') || '';
      const direccion = prompt('Nueva dirección (deja vacío para no cambiar):') || '';

      const tieneDescuento = (prompt('¿Tiene descuento? (SI/NO)', 'NO') || 'NO').trim().toUpperCase() === 'SI';
      let descuentoMonto = 0;
      let descuentoRazon = '';
      if (tieneDescuento) {
        descuentoMonto = Number((prompt('Monto descuento (numero):', '0') || '0').replace(/[^\d]/g, '')) || 0;
        descuentoRazon = (prompt('Razón descuento:', '') || '').trim();
      }

      const patch = {
        actualizadoEl: serverTimestamp(),
        tieneDescuento,
        descuentoMonto: tieneDescuento ? descuentoMonto : 0,
        descuentoRazon: tieneDescuento ? (descuentoRazon || null) : null
      };
      if (nombreProfesional.trim()) patch.nombreProfesional = nombreProfesional.trim();
      if (razonSocial.trim()) patch.razonSocial = razonSocial.trim();
      if (giro.trim()) patch.giro = giro.trim();
      if (direccion.trim()) patch.direccion = direccion.trim();

      await setDoc(doc(db, 'profesionales', rutId), patch, { merge: true });
      await loadProfesionales();
      return;
    }
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
    const rut = prompt('RUT del profesional (obligatorio, ej 16128922-1):');
    if (!rut?.trim()) return;

    const rutId = normalizaRut(rut);
    if (!rutId) { alert('RUT inválido.'); return; }

    const nombreProfesional = prompt('Nombre profesional (obligatorio):');
    if (!nombreProfesional?.trim()) return;

    const razonSocial = prompt('Razón social (opcional):') || '';
    const giro = prompt('Giro (opcional):') || '';
    const direccion = prompt('Dirección (opcional):') || '';

    // Descuento
    const tieneDescuento = (prompt('¿Tiene descuento? (SI/NO)', 'NO') || 'NO').trim().toUpperCase() === 'SI';
    let descuentoMonto = 0;
    let descuentoRazon = '';
    if (tieneDescuento) {
      descuentoMonto = Number((prompt('Monto descuento (numero, ej 50000):', '0') || '0').replace(/[^\d]/g, '')) || 0;
      descuentoRazon = (prompt('Razón descuento:', '') || '').trim();
    }

    // Por ahora: rol/clinicas se configuran por import masivo o edición posterior
    await setDoc(doc(db, 'profesionales', rutId), {
      rut: rut.trim(),
      rutId,
      nombreProfesional: nombreProfesional.trim(),
      razonSocial: razonSocial.trim() || null,
      giro: giro.trim() || null,
      direccion: direccion.trim() || null,

      rolPrincipalId: null,
      rolesSecundariosIds: [],
      clinicasIds: [],

      tieneDescuento,
      descuentoMonto,
      descuentoRazon: descuentoRazon || null,
      descuentoMoneda: 'CLP',

      estado: 'activo',
      creadoEl: serverTimestamp(),
      actualizadoEl: serverTimestamp()
    }, { merge: true });

    await loadProfesionales();
    alert('Profesional guardado ✅');
  } catch (err) {
    console.error('Error creando profesional:', err);
    alert('No se pudo guardar el profesional. Revisa consola.');
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

btnNuevoProcedimiento?.addEventListener('click', async () => {
  try {
    const nombre = prompt('Nombre del procedimiento (obligatorio):');
    if (!nombre?.trim()) return;

    const codigo = prompt('Código (opcional):') || '';
    const tipo = prompt('Tipo (ej: ambulatorio) (opcional):') || 'ambulatorio';
    const valorBaseRaw = prompt('Valor base (opcional, ej 250000):') || '';
    const valorBase = valorBaseRaw.trim() === '' ? 0 : Number(valorBaseRaw);

    await addDoc(collection(db, 'procedimientos'), {
      nombre: nombre.trim(),
      codigo: codigo.trim() || null,
      tipo: tipo.trim() || 'ambulatorio',
      valorBase: Number.isFinite(valorBase) ? valorBase : 0,
      creadoEl: serverTimestamp(),
      actualizadoEl: serverTimestamp()
    });

    await loadProcedimientos();
    alert('Procedimiento creado ✅');
  } catch (err) {
    console.error('Error creando procedimiento:', err);
    alert('No se pudo crear el procedimiento. Revisa consola.');
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
          await setDoc(docRef, {
            rut: row.rut,
            rutId: id,
            nombre: row.nombre,
            razonSocial: row.razonSocial || null,
            rol: row.rol || 'sin-clasificar',
            estado: 'activo',
            actualizadoEl: new Date()
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
