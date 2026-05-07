// produccion-liquidaciones.js
// Módulo complementario para preparar datos usados por liquidaciones.js

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { cleanReminder, toUpperSafe } from './utils.js';
import { loadSidebar } from './layout.js';

import {
  collection,
  collectionGroup,
  getDocs,
  query,
  where,
  updateDoc,
  serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

await loadSidebar({ active: 'liquidaciones' });

/* =========================
   Config
========================= */

const PROD_ITEMS_GROUP = 'items';

const ROLE_FIELDS = [
  {
    label: 'Cirujano',
    key: 'cirujano',
    idKey: 'cirujanoId',
    roleId: 'r_cirujano'
  },
  {
    label: 'Anestesista',
    key: 'anestesista',
    idKey: 'anestesistaId',
    roleId: 'r_anestesista'
  },
  {
    label: 'Ayudante 1',
    key: 'ayudante1',
    idKey: 'ayudante1Id',
    roleId: 'r_ayudante_1'
  },
  {
    label: 'Ayudante 2',
    key: 'ayudante2',
    idKey: 'ayudante2Id',
    roleId: 'r_ayudante_2'
  },
  {
    label: 'Arsenalera',
    key: 'arsenalera',
    idKey: 'arsenaleraId',
    roleId: 'r_arsenalera'
  }
];

const TIPOS_PACIENTE = [
  { value: '', label: '— Seleccionar —' },
  { value: 'fonasa', label: 'FONASA' },
  { value: 'mle', label: 'MLE' },
  { value: 'particular_isapre', label: 'PARTICULAR / ISAPRE' }
];

/* =========================
   State
========================= */

const state = {
  user: null,
  mesNum: null,
  ano: null,
  q: '',

  rows: [],
  dirty: new Set(),

  clinicas: [],
  procedimientos: [],
  profesionales: []
};

/* =========================
   Helpers
========================= */

const $ = (id) => document.getElementById(id);

function normalize(s = '') {
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function escapeHtml(s = '') {
  return (s ?? '').toString()
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function fmtDate(v) {
  const s = cleanReminder(v);
  if (!s) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s.split('-').reverse().join('/');
  return s;
}

function pickRaw(raw, key) {
  if (!raw || typeof raw !== 'object') return '';

  if (raw[key] !== undefined) return raw[key];

  const nk = normalize(key).replace(/[^a-z0-9]/g, '');
  for (const k of Object.keys(raw)) {
    const kk = normalize(k).replace(/[^a-z0-9]/g, '');
    if (kk === nk) return raw[k];
  }

  return '';
}

function getResolved(row) {
  const x = row.data || {};
  return x.resolved && typeof x.resolved === 'object' ? x.resolved : {};
}

function getSelectedIds(row) {
  const x = row.data || {};
  return x._selectedIds && typeof x._selectedIds === 'object' ? x._selectedIds : {};
}

function getRaw(row) {
  const x = row.data || {};
  return x.raw && typeof x.raw === 'object' ? x.raw : {};
}

function getPacienteNombre(row) {
  const x = row.data || {};
  const raw = getRaw(row);

  return toUpperSafe(
    cleanReminder(
      x.nombrePaciente ||
      x.pacienteNombre ||
      pickRaw(raw, 'Nombre Paciente') ||
      pickRaw(raw, 'Paciente')
    )
  );
}

function getFecha(row) {
  const x = row.data || {};
  const raw = getRaw(row);

  return fmtDate(
    x.fechaISO ||
    x.fecha ||
    pickRaw(raw, 'Fecha')
  );
}

function getHora(row) {
  const x = row.data || {};
  const raw = getRaw(row);

  return cleanReminder(
    x.horaHM ||
    x.hora ||
    pickRaw(raw, 'Hora')
  );
}

function getTipoPaciente(row) {
  const x = row.data || {};
  const raw = getRaw(row);
  const resolved = getResolved(row);

  const v = cleanReminder(
    resolved.tipoPaciente ||
    x.tipoPaciente ||
    pickRaw(raw, 'Tipo de Paciente') ||
    pickRaw(raw, 'Previsión') ||
    pickRaw(raw, 'Prevision')
  );

  const n = normalize(v);

  if (n.includes('fona')) return 'fonasa';
  if (n === 'mle' || n.includes('libre eleccion')) return 'mle';
  if (n.includes('isap') || n.includes('part')) return 'particular_isapre';

  return '';
}

function getClinicaId(row) {
  const x = row.data || {};
  const resolved = getResolved(row);
  const selected = getSelectedIds(row);

  return cleanReminder(
    resolved.clinicaId ||
    selected.clinicaId ||
    x.clinicaId ||
    ''
  );
}

function getProcedimientoId(row) {
  const x = row.data || {};
  const resolved = getResolved(row);
  const selected = getSelectedIds(row);
  const norm = x.normalizado && typeof x.normalizado === 'object' ? x.normalizado : {};

  return cleanReminder(
    resolved.procedimientoId ||
    resolved.cirugiaId ||
    selected.procedimientoId ||
    selected.cirugiaId ||
    norm.procedimientoId ||
    norm.cirugiaId ||
    x.procedimientoId ||
    x.cirugiaId ||
    x.procedimientoCodigo ||
    x.codigoProcedimiento ||
    ''
  );
}

function getProfId(row, idKey) {
  const x = row.data || {};
  const resolved = getResolved(row);
  const selected = getSelectedIds(row);

  const resolvedProfIds =
    resolved.profIds && typeof resolved.profIds === 'object'
      ? resolved.profIds
      : {};

  const selectedProfIds =
    selected.profIds && typeof selected.profIds === 'object'
      ? selected.profIds
      : {};

  return cleanReminder(
    resolvedProfIds[idKey] ||
    resolved[idKey] ||
    selectedProfIds[idKey] ||
    selected[idKey] ||
    x.profesionalesId?.[idKey] ||
    ''
  );
}

function rowMatches(row) {
  const q = normalize(state.q);
  if (!q) return true;

  const x = row.data || {};
  const raw = getRaw(row);

  const hay = normalize([
    row.id,
    getPacienteNombre(row),
    getFecha(row),
    x.clinica,
    x.cirugia,
    x.procedimientoNombre,
    pickRaw(raw, 'Clínica'),
    pickRaw(raw, 'Procedimiento'),
    pickRaw(raw, 'Cirugía'),
    ...Object.values(x.profesionales || {}),
    ...Object.values(x.profesionalesId || {})
  ].join(' '));

  return hay.includes(q);
}

function isRowComplete(row) {
  if (!getTipoPaciente(row)) return false;
  if (!getClinicaId(row)) return false;
  if (!getProcedimientoId(row)) return false;

  for (const rf of ROLE_FIELDS) {
    if (!getProfId(row, rf.idKey)) return false;
  }

  return true;
}

function markDirty(rowId) {
  state.dirty.add(rowId);

  const tr = document.querySelector(`tr[data-row-id="${CSS.escape(rowId)}"]`);
  if (tr) tr.classList.add('rowDirty');

  const btn = tr?.querySelector('[data-action="save-row"]');
  if (btn) btn.textContent = 'Guardar *';
}

/* =========================
   Select builders
========================= */

function optionHtml(value, label, selectedValue) {
  const selected = String(value || '') === String(selectedValue || '') ? 'selected' : '';
  return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(label)}</option>`;
}

function buildSelect(options, selectedValue, attrs = '') {
  return `
    <select ${attrs}>
      ${options.map(o => optionHtml(o.value, o.label, selectedValue)).join('')}
    </select>
  `;
}

function buildClinicaSelect(selected) {
  const opts = [
    { value: '', label: '— Seleccionar —' },
    ...state.clinicas.map(c => ({
      value: c.id,
      label: c.nombre
    }))
  ];

  return buildSelect(opts, selected, 'data-field="clinicaId"');
}

function buildProcedimientoSelect(selected) {
  const opts = [
    { value: '', label: '— Seleccionar —' },
    ...state.procedimientos.map(p => ({
      value: p.id,
      label: `${p.codigo ? p.codigo + ' · ' : ''}${p.nombre}`
    }))
  ];

  return buildSelect(opts, selected, 'data-field="procedimientoId"');
}

function buildProfesionalSelect(selected, roleId, idKey) {
  const filtered = state.profesionales.filter(p => {
    if (!p.rolPrincipalId && !p.rolPrincipal) return true;

    const rp = cleanReminder(p.rolPrincipalId || p.rolPrincipal);
    return rp === roleId;
  });

  const opts = [
    { value: '', label: '— Seleccionar —' },
    ...filtered.map(p => ({
      value: p.id,
      label: `${p.nombreProfesional}${p.rut ? ' · ' + p.rut : ''}`
    }))
  ];

  return buildSelect(opts, selected, `data-prof-id-key="${idKey}"`);
}

/* =========================
   Load catalogs
========================= */

async function loadClinicas() {
  const snap = await getDocs(collection(db, 'clinicas'));

  state.clinicas = snap.docs
    .map(d => {
      const x = d.data() || {};
      return {
        id: cleanReminder(x.id) || d.id,
        nombre: toUpperSafe(cleanReminder(x.nombre) || d.id)
      };
    })
    .sort((a, b) => normalize(a.nombre).localeCompare(normalize(b.nombre)));
}

async function loadProcedimientos() {
  const snap = await getDocs(collection(db, 'procedimientos'));

  state.procedimientos = snap.docs
    .map(d => {
      const x = d.data() || {};
      return {
        id: d.id,
        codigo: cleanReminder(x.codigo) || d.id,
        nombre: toUpperSafe(cleanReminder(x.nombre) || d.id),
        tipo: cleanReminder(x.tipo || '')
      };
    })
    .sort((a, b) => normalize(a.nombre).localeCompare(normalize(b.nombre)));
}

async function loadProfesionales() {
  const snap = await getDocs(collection(db, 'profesionales'));

  state.profesionales = snap.docs
    .map(d => {
      const x = d.data() || {};

      const id = cleanReminder(x.rutId) || d.id;
      const nombreProfesional = toUpperSafe(cleanReminder(x.nombreProfesional) || d.id);

      return {
        id,
        rut: cleanReminder(x.rut) || id,
        nombreProfesional,
        estado: cleanReminder(x.estado || 'activo').toLowerCase(),
        rolPrincipalId: cleanReminder(x.rolPrincipalId || ''),
        rolPrincipal: cleanReminder(x.rolPrincipal || '')
      };
    })
    .filter(p => p.estado !== 'inactivo')
    .sort((a, b) => normalize(a.nombreProfesional).localeCompare(normalize(b.nombreProfesional)));
}

/* =========================
   Load producción confirmada
========================= */

async function loadProduccion() {
  const qy = query(
    collectionGroup(db, PROD_ITEMS_GROUP),
    where('ano', '==', Number(state.ano)),
    where('mesNum', '==', Number(state.mesNum)),
    where('confirmado', '==', true)
  );

  const snap = await getDocs(qy);

  const rows = [];

  snap.forEach(d => {
    const x = d.data() || {};
    const estado = normalize(x.estado || '');

    if (estado === 'anulada' || estado === 'anulado' || estado === 'cancelada') return;

    rows.push({
      id: d.id,
      ref: d.ref,
      data: x
    });
  });

  rows.sort((a, b) => {
    const fa = normalize(getFecha(a));
    const fb = normalize(getFecha(b));
    if (fa !== fb) return fa.localeCompare(fb);

    return normalize(getPacienteNombre(a)).localeCompare(normalize(getPacienteNombre(b)));
  });

  state.rows = rows;
  state.dirty.clear();
}

/* =========================
   Paint
========================= */

function paint() {
  const tb = $('tbody');
  tb.innerHTML = '';

  const rows = state.rows.filter(rowMatches);

  let pendientes = 0;

  rows.forEach((row, idx) => {
    const complete = isRowComplete(row);
    if (!complete) pendientes++;

    const tipoPaciente = getTipoPaciente(row);
    const clinicaId = getClinicaId(row);
    const procedimientoId = getProcedimientoId(row);

    const tr = document.createElement('tr');
    tr.dataset.rowId = row.id;

    const estadoHtml = complete
      ? `<span class="pill ok">Completo</span>`
      : `<span class="pill warn">Pendiente</span>`;

    tr.innerHTML = `
      <td class="mono">${idx + 1}</td>

      <td class="mono">
        ${escapeHtml(getFecha(row))}
        <div class="mini muted">${escapeHtml(getHora(row))}</div>
      </td>

      <td>
        <b>${escapeHtml(getPacienteNombre(row) || '—')}</b>
        <div class="mini muted mono">${escapeHtml(row.id)}</div>
      </td>

      <td>
        ${buildSelect(TIPOS_PACIENTE, tipoPaciente, 'data-field="tipoPaciente"')}
      </td>

      <td>
        ${buildClinicaSelect(clinicaId)}
      </td>

      <td>
        ${buildProcedimientoSelect(procedimientoId)}
      </td>

      ${ROLE_FIELDS.map(rf => `
        <td>
          ${buildProfesionalSelect(getProfId(row, rf.idKey), rf.roleId, rf.idKey)}
        </td>
      `).join('')}

      <td data-status-cell>
        ${estadoHtml}
      </td>

      <td>
        <button type="button" class="iconBtn" data-action="save-row">Guardar</button>
      </td>
    `;

    tr.querySelectorAll('select').forEach(sel => {
      sel.addEventListener('change', () => {
        markDirty(row.id);
        refreshRowStatus(tr, row);
      });
    });

    tr.querySelector('[data-action="save-row"]').addEventListener('click', async () => {
      await saveRow(row, tr);
    });

    tb.appendChild(tr);
  });

  $('pillCount').textContent = `${rows.length} caso${rows.length === 1 ? '' : 's'}`;
  $('pillPendientes').textContent = `Pendientes: ${pendientes}`;
  $('pillPendientes').className = pendientes > 0 ? 'pill warn' : 'pill ok';
}

function refreshRowStatus(tr, row) {
  const fakeRow = readRowFromDOM(row, tr);
  const complete = isRowComplete(fakeRow);

  const cell = tr.querySelector('[data-status-cell]');
  if (!cell) return;

  cell.innerHTML = complete
    ? `<span class="pill ok">Completo</span>`
    : `<span class="pill warn">Pendiente</span>`;
}

/* =========================
   Save
========================= */

function readRowFromDOM(row, tr) {
  const base = structuredClone(row.data || {});

  const resolved = {
    ...(base.resolved && typeof base.resolved === 'object' ? base.resolved : {})
  };

  resolved.profIds = {
    ...(resolved.profIds && typeof resolved.profIds === 'object' ? resolved.profIds : {})
  };

  const tipoPacienteSel = tr.querySelector('[data-field="tipoPaciente"]');
  const clinicaSel = tr.querySelector('[data-field="clinicaId"]');
  const procSel = tr.querySelector('[data-field="procedimientoId"]');

  resolved.tipoPaciente = tipoPacienteSel?.value || '';
  resolved.clinicaId = clinicaSel?.value || '';
  resolved.procedimientoId = procSel?.value || '';

  for (const rf of ROLE_FIELDS) {
    const sel = tr.querySelector(`[data-prof-id-key="${rf.idKey}"]`);
    resolved.profIds[rf.idKey] = sel?.value || '';
  }

  return {
    ...row,
    data: {
      ...base,
      resolved
    }
  };
}

async function saveRow(row, tr) {
  try {
    const next = readRowFromDOM(row, tr);
    const resolved = next.data.resolved || {};

    await updateDoc(row.ref, {
      resolved: {
        ...resolved,
        revisadoLiquidacion: isRowComplete(next),
        revisadoLiquidacionPor: state.user?.email || '',
        revisadoLiquidacionEl: serverTimestamp()
      },
      actualizadoLiquidacionPor: state.user?.email || '',
      actualizadoLiquidacionEl: serverTimestamp()
    });

    row.data = next.data;

    state.dirty.delete(row.id);
    tr.classList.remove('rowDirty');

    const btn = tr.querySelector('[data-action="save-row"]');
    if (btn) btn.textContent = 'Guardar';

    refreshRowStatus(tr, row);

    toast('Fila guardada');
  } catch (err) {
    console.error(err);
    toast('No se pudo guardar la fila');
  }
}

async function saveAllDirty() {
  const dirtyIds = [...state.dirty];

  if (!dirtyIds.length) {
    toast('No hay cambios pendientes');
    return;
  }

  const btn = $('btnGuardarTodo');
  const original = btn.textContent;

  try {
    btn.disabled = true;
    btn.textContent = 'Guardando...';

    for (const rowId of dirtyIds) {
      const row = state.rows.find(r => r.id === rowId);
      const tr = document.querySelector(`tr[data-row-id="${CSS.escape(rowId)}"]`);
      if (!row || !tr) continue;

      await saveRow(row, tr);
    }

    toast('Cambios guardados');
    await reloadAndPaint();
  } catch (err) {
    console.error(err);
    toast('Error guardando cambios');
  } finally {
    btn.disabled = false;
    btn.textContent = original;
  }
}

/* =========================
   Selectores mes/año
========================= */

function monthNameEs(m) {
  return [
    'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
  ][m - 1] || '';
}

function initMonthYearSelectors() {
  const now = new Date();
  const mesActual = now.getMonth() + 1;

  const mesDefault = mesActual === 1 ? 12 : mesActual - 1;
  const anoDefault = mesActual === 1 ? now.getFullYear() - 1 : now.getFullYear();

  const mesSel = $('mes');
  mesSel.innerHTML = '';

  for (let m = 1; m <= 12; m++) {
    const opt = document.createElement('option');
    opt.value = String(m);
    opt.textContent = monthNameEs(m);
    mesSel.appendChild(opt);
  }

  const anoSel = $('ano');
  anoSel.innerHTML = '';

  for (let y = anoDefault - 2; y <= anoDefault + 3; y++) {
    const opt = document.createElement('option');
    opt.value = String(y);
    opt.textContent = String(y);
    anoSel.appendChild(opt);
  }

  state.mesNum = mesDefault;
  state.ano = anoDefault;

  mesSel.value = String(state.mesNum);
  anoSel.value = String(state.ano);

  mesSel.addEventListener('change', () => {
    state.mesNum = Number(mesSel.value);
    reloadAndPaint();
  });

  anoSel.addEventListener('change', () => {
    state.ano = Number(anoSel.value);
    reloadAndPaint();
  });
}

/* =========================
   Reload
========================= */

async function reloadAndPaint() {
  const btn = $('btnCargar');
  const original = btn?.textContent || 'Cargar';

  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = 'Cargando...';
    }

    $('lastLoad').textContent = 'Cargando producción confirmada...';

    await loadProduccion();
    paint();

    $('lastLoad').textContent =
      `Última carga: ${new Date().toLocaleString()} · ${state.rows.length} casos confirmados`;

    toast('Producción cargada');
  } catch (err) {
    console.error(err);
    $('lastLoad').textContent = 'Error cargando producción';
    toast('No se pudo cargar producción');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = original;
    }
  }
}

/* =========================
   Main
========================= */

requireAuth({
  onUser: async (user) => {
    state.user = user;

    await loadSidebar({ active: 'liquidaciones' });
    setActiveNav('liquidaciones');

    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    initMonthYearSelectors();

    $('q').addEventListener('input', (e) => {
      state.q = e.target.value || '';
      paint();
    });

    $('btnCargar').addEventListener('click', reloadAndPaint);
    $('btnGuardarTodo').addEventListener('click', saveAllDirty);

    await Promise.all([
      loadClinicas(),
      loadProcedimientos(),
      loadProfesionales()
    ]);

    await reloadAndPaint();
  }
});
