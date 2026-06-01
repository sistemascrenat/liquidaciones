// ambulatorios.js — COMPLETO
// ✅ Colección: procedimientos (docId PA0001, PA0002...; tipo="ambulatorio")
// ✅ Estructura por prestación ambulatoria:
//    archivo, categoria, tratamiento, estado
//    tarifa.{ modoValor, valor, columnaOrigen, comisionPct, valorProfesional, actualizadoEl, actualizadoPor }
// ✅ modoValor soportado:
//    - "fijo"    => usa tarifa.valor
//    - "archivo" => el valor viene desde una columna del archivo liquidado (ej: "Total")
// ✅ Tabla: muestra valor / comisión / pago profesional / utilidad
// ✅ Buscador: coma=AND, guión=OR
// ✅ XLSX: plantilla / export / import
// ✅ Sidebar común: layout.js (await loadSidebar({ active:'ambulatorios' }))
// ✅ Formato CLP: $ con puntos de miles

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { setActiveNav, toast, wireLogout } from './ui.js';
import { cleanReminder } from './utils.js';
import { loadSidebar } from './layout.js';

import {
  collection, getDocs, setDoc, deleteDoc,
  doc, serverTimestamp,
  query, where,
  getDoc, writeBatch
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  q: '',
  all: [],
  editProcId: null,
  selectedIds: new Set()
};

const $ = (id)=> document.getElementById(id);

/* =========================
   Utils
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

function hasRealValue(v){
  return !(v === undefined || v === null || String(v).trim() === '');
}

function asNumberLoose(v){
  const s = (v ?? '').toString().replace(/[^\d]/g,'');
  return Number(s || 0) || 0;
}

function asNullableMoney(v){
  const raw = (v ?? '').toString().trim();
  if(!raw) return null;

  const digits = raw.replace(/[^\d]/g,'');
  if(digits === '') return null;

  return Number(digits);
}

function asNullablePercent(v){
  const raw = (v ?? '').toString().trim();
  if(!raw) return null;

  const normalized = raw.replace(/[^\d.,]/g,'').replace(',','.');
  if(normalized === '') return null;

  const n = Number(normalized);
  return Number.isNaN(n) ? null : n;
}

function clp(n){
  if(!hasRealValue(n)) return '';
  const x = Number(n);
  if(Number.isNaN(x)) return '';
  const s = Math.round(x).toString();
  const withDots = s.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return `$${withDots}`;
}

function pct(n){
  if(!hasRealValue(n)) return '';
  const x = Number(n);
  if(Number.isNaN(x)) return '';
  return `${x}%`;
}

function wireMoneyInput(el){
  if(!el) return;

  const repaint = ()=>{
    const raw = (el.value ?? '').toString().trim();
    const digits = raw.replace(/[^\d]/g,'');

    if(digits === ''){
      el.value = '';
      return;
    }

    const n = Number(digits);
    el.value = clp(n);
  };

  el.addEventListener('blur', repaint);
  el.addEventListener('change', repaint);
  el.addEventListener('input', ()=>{
    const digits = (el.value ?? '').toString().replace(/[^\d]/g,'');
    el.value = digits;
  });
}

function wirePercentInput(el){
  if(!el) return;

  el.addEventListener('input', ()=>{
    let raw = (el.value ?? '').toString().replace(/[^\d.,]/g,'').replace(',','.');
    if(raw === ''){
      el.value = '';
      return;
    }

    const n = Number(raw);
    if(Number.isNaN(n)) return;

    if(n < 0) el.value = '0';
    else if(n > 100) el.value = '100';
    else el.value = raw;
  });

  el.addEventListener('blur', ()=>{
    const raw = (el.value ?? '').toString().trim();
    if(!raw){
      el.value = '';
      return;
    }

    const n = Number(raw.replace(',','.'));
    if(Number.isNaN(n)){
      el.value = '';
      return;
    }

    el.value = String(n);
  });
}

function spanPrice(txt){
  return `<span style="color:#facc15;font-weight:900;">${txt}</span>`;
}
function spanCost(txt){
  return `<span style="color:#ef4444;font-weight:900;">${txt}</span>`;
}
function spanProfit(txt){
  return `<span style="color:#22c55e;font-weight:900;">${txt}</span>`;
}

/* =========================
   Firestore refs
========================= */
const colProcedimientos = collection(db, 'procedimientos');

const colProcedimientosArchivados = collection(db, 'procedimientosArchivados');

function getSelectedIds(){
  return Array.from(state.selectedIds || []);
}

function toggleSelected(id, checked){
  if(!id) return;

  if(checked){
    state.selectedIds.add(id);
  }else{
    state.selectedIds.delete(id);
  }

  updateBulkButtons();
}

function updateBulkButtons(){
  const total = state.selectedIds.size;

  if($('btnEliminarSeleccionados')){
    $('btnEliminarSeleccionados').disabled = total === 0;
    $('btnEliminarSeleccionados').textContent =
      total ? `🗑️ Eliminar seleccionados (${total})` : '🗑️ Eliminar seleccionados';
  }

  if($('btnArchivarSeleccionados')){
    $('btnArchivarSeleccionados').disabled = total === 0;
    $('btnArchivarSeleccionados').textContent =
      total ? `📦 Archivar seleccionados (${total})` : '📦 Archivar seleccionados';
  }
}

function syncSelectAllCheckbox(rows){
  const chk = $('chkSelectAll');
  if(!chk) return;

  const visibleIds = rows.map(p => p.id).filter(Boolean);
  const selectedVisible = visibleIds.filter(id => state.selectedIds.has(id));

  chk.checked = visibleIds.length > 0 && selectedVisible.length === visibleIds.length;
  chk.indeterminate = selectedVisible.length > 0 && selectedVisible.length < visibleIds.length;
}

/* =========================
   Search: coma=AND, guión=OR
========================= */
function splitAnd(raw){
  return (raw || '')
    .toString()
    .split(',')
    .map(s=>normalize(s))
    .filter(Boolean);
}
function splitOr(term){
  return (term || '')
    .toString()
    .split('-')
    .map(s=>normalize(s))
    .filter(Boolean);
}

/* =========================
   Normalización doc
========================= */
function normalizeModoValor(v=''){
  const x = normalize(v);
  if(x === 'archivo') return 'archivo';
  return 'fijo';
}

function normalizeProcDoc(id, x){
  const tarifaRaw = (x?.tarifa && typeof x.tarifa === 'object') ? x.tarifa : {};

  const modoValor = normalizeModoValor(tarifaRaw.modoValor);
  const valor = hasRealValue(tarifaRaw.valor) ? Number(tarifaRaw.valor) : null;
  const columnaOrigen = cleanReminder(tarifaRaw.columnaOrigen || '');
  const comisionPct = hasRealValue(tarifaRaw.comisionPct) ? Number(tarifaRaw.comisionPct) : null;
  const valorProfesional = hasRealValue(tarifaRaw.valorProfesional) ? Number(tarifaRaw.valorProfesional) : null;

  let utilidad = null;
  if(modoValor === 'fijo' && valor !== null && valorProfesional !== null){
    utilidad = valor - valorProfesional;
  }

  const hasTarifa =
    (modoValor === 'archivo')
      ? !!columnaOrigen || comisionPct !== null || valorProfesional !== null
      : valor !== null || comisionPct !== null || valorProfesional !== null;

  return {
    id,
    codigo: cleanReminder(x.codigo) || id,
    tipo: cleanReminder(x.tipo) || 'ambulatorio',
    archivo: cleanReminder(x.archivo) || '',
    categoria: cleanReminder(x.categoria) || '',
    tratamiento: cleanReminder(x.tratamiento || x.nombre) || '',
    nombre: cleanReminder(x.tratamiento || x.nombre) || '',
    estado: (cleanReminder(x.estado) || 'activa').toLowerCase(),
    tarifa: {
      modoValor,
      valor,
      columnaOrigen,
      comisionPct,
      valorProfesional,
      utilidad,
      hasTarifa
    }
  };
}

async function loadAll(){
  const qy = query(colProcedimientos, where('tipo','==','ambulatorio'));
  const snap = await getDocs(qy);

  const out = [];
  snap.forEach(d=>{
    out.push(normalizeProcDoc(d.id, d.data() || {}));
  });

  out.sort((a,b)=>{
    if(a.estado !== b.estado){
      if(a.estado === 'activa') return -1;
      if(b.estado === 'activa') return 1;
    }
    return normalize(a.codigo).localeCompare(normalize(b.codigo));
  });

  state.all = out;
  paint();
}

/* =========================
   Match búsqueda
========================= */
function rowMatches(p, rawQuery){
  const andTerms = splitAnd(rawQuery);
  if(!andTerms.length) return true;

  const t = p.tarifa || {};

  const hay = normalize([
    p.codigo,
    p.archivo,
    p.categoria,
    p.tratamiento,
    p.estado,
    'procedimiento ambulatorio',
    t.modoValor,
    String(t.valor || ''),
    String(t.comisionPct || ''),
    String(t.valorProfesional || ''),
    String(t.utilidad ?? ''),
    t.columnaOrigen || '',
    t.modoValor === 'archivo' ? 'valor desde archivo' : 'valor fijo'
  ].join(' '));

  return andTerms.every(block=>{
    const ors = splitOr(block);
    if(!ors.length) return true;
    return ors.some(x=> hay.includes(x));
  });
}

/* =========================
   UI helpers
========================= */
function estadoBadge(p){
  const est = (p.estado || 'activa').toLowerCase();
  const cls = (est === 'activa') ? 'activo' : 'inactivo';
  const label = (est === 'activa') ? 'ACTIVA' : 'INACTIVA';
  return `<span class="state ${cls}">${label}</span>`;
}

function tarifaChip(p){
  const t = p.tarifa || {};

  if(!t.hasTarifa){
    return `<span class="pill">TARIFA: PENDIENTE</span>`;
  }

  if(t.modoValor === 'archivo'){
    const col = t.columnaOrigen || '—';
    const vp = t.valorProfesional;
    const com = t.comisionPct;

    return `
      <div style="display:flex; flex-wrap:wrap; gap:8px;">
        <span class="pill"><b>VALOR:</b> DESDE ARCHIVO</span>
        <span class="pill"><b>COLUMNA:</b> ${escapeHtml(col)}</span>
        <span class="pill"><b>COMISIÓN:</b> ${hasRealValue(com) ? escapeHtml(pct(com)) : '—'}</span>
        <span class="pill"><b>PAGO PROF.:</b> ${hasRealValue(vp) ? escapeHtml(clp(vp)) : '—'}</span>
      </div>
    `;
  }

  const valor = t.valor;
  const pago = t.valorProfesional;
  const utilidad = t.utilidad;
  const com = t.comisionPct;

  return `
    <div style="display:flex; flex-wrap:wrap; gap:8px;">
      <span class="pill"><b>${hasRealValue(valor) ? clp(valor) : '—'}</b></span>
      <span class="pill">Comisión ${hasRealValue(com) ? escapeHtml(pct(com)) : '—'}</span>
      <span class="pill">Pago prof. ${hasRealValue(pago) ? escapeHtml(clp(pago)) : '—'}</span>
      <span class="pill">Utilidad ${hasRealValue(utilidad) ? escapeHtml(clp(utilidad)) : '—'}</span>
    </div>
  `;
}

function getNextPACode(){
  let maxNum = 0;

  for(const p of (state.all || [])){
    const raw = String(p?.codigo || p?.id || '').toUpperCase().trim();
    const m = raw.match(/^PA(\d{4})$/);
    if(!m) continue;

    const num = Number(m[1]) || 0;
    if(num > maxNum) maxNum = num;
  }

  const next = maxNum + 1;
  return `PA${String(next).padStart(4, '0')}`;
}

/* =========================
   Paint table
========================= */
function paint(){
  const rows = state.all.filter(p=> rowMatches(p, state.q));
  if($('count')) $('count').textContent = `${rows.length} ambulatorio${rows.length===1?'':'s'}`;

  syncSelectAllCheckbox(rows);
  updateBulkButtons();

  const tb = $('tbody');
  if(!tb) return;
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="text-align:center;">
        <input
          type="checkbox"
          class="chkRow"
          data-id="${escapeHtml(p.id)}"
          ${state.selectedIds.has(p.id) ? 'checked' : ''}
          title="Seleccionar"
        />
      </td>

      <td><div class="mono"><b>${escapeHtml(p.codigo || p.id)}</b></div></td>

      <td>
        <div class="cellBlock">
          <div class="cellTitle">${escapeHtml(p.archivo || '—')}</div>
        </div>
      </td>

      <td>
        <div class="cellBlock">
          <div class="cellTitle">${escapeHtml(p.categoria || '—')}</div>
        </div>
      </td>

      <td>
        <div class="cellBlock">
          <div class="cellTitle">${escapeHtml(p.tratamiento || '—')}</div>
          <div class="cellSub">
            <span class="muted">Procedimiento · Ambulatorio</span>
          </div>
        </div>
      </td>

      <td>${tarifaChip(p)}</td>

      <td>${estadoBadge(p)}</td>

      <td>
        <div class="actionsMini">
          <button class="iconBtn" type="button" title="Editar" aria-label="Editar">✏️</button>
          <button class="iconBtn danger" type="button" title="Eliminar" aria-label="Eliminar">🗑️</button>
        </div>
      </td>
    `;

    tr.querySelector('.chkRow')?.addEventListener('change', (e)=>{
      toggleSelected(p.id, e.target.checked);
    });

    tr.querySelector('button[aria-label="Editar"]').addEventListener('click', ()=> openProcModal('edit', p));
    tr.querySelector('button[aria-label="Eliminar"]').addEventListener('click', ()=> removeProc(p.id));

    tb.appendChild(tr);
  }
}

/* =========================
   Modal crear / editar
========================= */
function toggleModoValorUI(){
  const modo = normalizeModoValor($('tarModoValor')?.value || 'fijo');

  const rowValor = $('rowTarValor');
  const rowColumna = $('rowTarColumna');

  if(rowValor) rowValor.style.display = (modo === 'fijo') ? '' : 'none';
  if(rowColumna) rowColumna.style.display = (modo === 'archivo') ? '' : 'none';

  computeTarPreview();
}

function computeTarPreview(){
  const modo = normalizeModoValor($('tarModoValor')?.value || 'fijo');
  const valor = asNullableMoney($('tarValor')?.value || '');
  const columna = cleanReminder($('tarColumnaOrigen')?.value || '');
  const comisionPct = asNullablePercent($('tarComisionPct')?.value || '');
  const valorProfesional = asNullableMoney($('tarValorProfesional')?.value || '');

  if($('tarResumen')){
    if(modo === 'archivo'){
      $('tarResumen').innerHTML =
        `Valor ${spanPrice('DESDE ARCHIVO')} · ` +
        `Columna ${spanCost(escapeHtml(columna || '—'))}`;
    } else {
      const utilidad = (valor !== null && valorProfesional !== null)
        ? (valor - valorProfesional)
        : null;

      $('tarResumen').innerHTML =
        `Valor ${spanPrice(hasRealValue(valor) ? clp(valor) : '—')} · ` +
        `Pago profesional ${spanCost(hasRealValue(valorProfesional) ? clp(valorProfesional) : '—')} · ` +
        `Utilidad ${spanProfit(hasRealValue(utilidad) ? clp(utilidad) : '—')}`;
    }
  }

  if($('tarTotales')){
    if(modo === 'archivo'){
      $('tarTotales').innerHTML = `
        MODO: <b>DESDE ARCHIVO</b><br/>
        COLUMNA ORIGEN: <b>${escapeHtml(columna || '—')}</b><br/>
        COMISIÓN: <b>${hasRealValue(comisionPct) ? escapeHtml(pct(comisionPct)) : '—'}</b><br/>
        PAGO PROFESIONAL: <b>${hasRealValue(valorProfesional) ? clp(valorProfesional) : '—'}</b>
      `;
    } else {
      const utilidad = (valor !== null && valorProfesional !== null)
        ? (valor - valorProfesional)
        : null;

      $('tarTotales').innerHTML = `
        VALOR: <b>${hasRealValue(valor) ? clp(valor) : '—'}</b><br/>
        COMISIÓN: <b>${hasRealValue(comisionPct) ? escapeHtml(pct(comisionPct)) : '—'}</b><br/>
        PAGO PROFESIONAL: <b>${hasRealValue(valorProfesional) ? clp(valorProfesional) : '—'}</b><br/>
        <div style="height:6px;"></div>
        UTILIDAD: <b>${hasRealValue(utilidad) ? clp(utilidad) : '—'}</b>
      `;
    }
  }

  if($('tarHint')){
    $('tarHint').textContent =
      (modo === 'archivo')
        ? 'Modo archivo: el valor se leerá desde la columna indicada al liquidar.'
        : 'Modo fijo: utilidad = valor - valor a pagar profesional.';
  }
}

function openProcModal(mode, p=null){
  $('modalProcBackdrop').style.display = 'grid';

  if(mode === 'create'){
    state.editProcId = null;
    $('modalProcTitle').textContent = 'Crear procedimiento ambulatorio';
    $('modalProcSub').textContent = 'Define archivo, categoría, tratamiento y tarifa.';
    $('procCodigo').disabled = false;
    $('procCodigo').value = getNextPACode();
    $('procArchivo').value = '';
    $('procCategoria').value = '';
    $('procTratamiento').value = '';
    $('procEstado').value = 'activa';

    $('tarModoValor').value = 'fijo';
    $('tarValor').value = '';
    $('tarColumnaOrigen').value = '';
    $('tarComisionPct').value = '';
    $('tarValorProfesional').value = '';
  }else{
    state.editProcId = p?.id || null;
    $('modalProcTitle').textContent = 'Editar procedimiento ambulatorio';
    $('modalProcSub').textContent = state.editProcId ? `ID: ${state.editProcId}` : '';
    $('procCodigo').value = p?.codigo || p?.id || '';
    $('procCodigo').disabled = true;
    $('procArchivo').value = p?.archivo || '';
    $('procCategoria').value = p?.categoria || '';
    $('procTratamiento').value = p?.tratamiento || '';
    $('procEstado').value = p?.estado || 'activa';

    $('tarModoValor').value = p?.tarifa?.modoValor || 'fijo';
    $('tarValor').value = hasRealValue(p?.tarifa?.valor) ? clp(p.tarifa.valor) : '';
    $('tarColumnaOrigen').value = p?.tarifa?.columnaOrigen || '';
    $('tarComisionPct').value = hasRealValue(p?.tarifa?.comisionPct) ? String(p.tarifa.comisionPct) : '';
    $('tarValorProfesional').value = hasRealValue(p?.tarifa?.valorProfesional) ? clp(p.tarifa.valorProfesional) : '';
  }

  toggleModoValorUI();
  computeTarPreview();
  $('procArchivo').focus();
}

function closeProcModal(){
  $('modalProcBackdrop').style.display = 'none';
}

async function saveProc(){
  const codigo = cleanReminder($('procCodigo').value).toUpperCase();
  const archivo = cleanReminder($('procArchivo').value);
  const categoria = cleanReminder($('procCategoria').value);
  const tratamiento = cleanReminder($('procTratamiento').value);
  const estado = (cleanReminder($('procEstado').value) || 'activa').toLowerCase();

  const modoValor = normalizeModoValor($('tarModoValor').value);
  const valor = asNullableMoney($('tarValor').value);
  const columnaOrigen = cleanReminder($('tarColumnaOrigen').value);
  const comisionPct = asNullablePercent($('tarComisionPct').value);
  const valorProfesional = asNullableMoney($('tarValorProfesional').value);

  if(!codigo || !/^PA\d{4}$/i.test(codigo)){
    toast('Código inválido. Usa formato PA0001');
    $('procCodigo').focus();
    return;
  }
  if(!archivo){
    toast('Falta archivo');
    $('procArchivo').focus();
    return;
  }
  if(!categoria){
    toast('Falta categoría');
    $('procCategoria').focus();
    return;
  }
  if(!tratamiento){
    toast('Falta tratamiento');
    $('procTratamiento').focus();
    return;
  }
  if(modoValor === 'archivo' && !columnaOrigen){
    toast('En modo archivo debes indicar la columna origen');
    $('tarColumnaOrigen').focus();
    return;
  }

  const id = state.editProcId || codigo;

  const payload = {
    id,
    codigo,
    tipo: 'ambulatorio',
    archivo,
    categoria,
    tratamiento,
    nombre: tratamiento,
    estado,
    tarifa: {
      modoValor,
      valor: (modoValor === 'fijo') ? valor : null,
      columnaOrigen: (modoValor === 'archivo') ? columnaOrigen : '',
      comisionPct,
      valorProfesional,
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || ''
    },
    actualizadoEl: serverTimestamp(),
    actualizadoPor: state.user?.email || ''
  };

  if(!state.editProcId){
    payload.creadoEl = serverTimestamp();
    payload.creadoPor = state.user?.email || '';
  }

  await setDoc(doc(db,'procedimientos', id), payload, { merge:true });
  toast(state.editProcId ? 'Ambulatorio actualizado' : 'Ambulatorio creado');
  closeProcModal();
  await loadAll();
}

async function removeProc(id){
  const ok = confirm(`¿Eliminar procedimiento ambulatorio?\n\n${id}`);
  if(!ok) return;

  await deleteDoc(doc(db,'procedimientos', id));
  toast('Eliminado');
  await loadAll();
}

async function eliminarSeleccionados(){
  const ids = getSelectedIds();

  if(!ids.length){
    toast('Selecciona al menos un ambulatorio');
    return;
  }

  const ok = confirm(`¿Eliminar ${ids.length} procedimiento${ids.length===1?'':'s'} ambulatorio${ids.length===1?'':'s'} seleccionado${ids.length===1?'':'s'}?\n\nEsta acción no se puede deshacer.`);
  if(!ok) return;

  const batch = writeBatch(db);

  for(const id of ids){
    batch.delete(doc(db, 'procedimientos', id));
  }

  await batch.commit();

  state.selectedIds.clear();
  toast('Procedimientos eliminados');
  await loadAll();
}

async function archivarSeleccionados(){
  const ids = getSelectedIds();

  if(!ids.length){
    toast('Selecciona al menos un ambulatorio');
    return;
  }

  const ok = confirm(`¿Archivar ${ids.length} procedimiento${ids.length===1?'':'s'} ambulatorio${ids.length===1?'':'s'} seleccionado${ids.length===1?'':'s'}?`);
  if(!ok) return;

  const batch = writeBatch(db);

  for(const id of ids){
    const refOriginal = doc(db, 'procedimientos', id);
    const snap = await getDoc(refOriginal);

    if(!snap.exists()) continue;

    const data = snap.data() || {};

    const refArchivado = doc(db, 'procedimientosArchivados', id);

    batch.set(refArchivado, {
      ...data,
      archivadoEl: serverTimestamp(),
      archivadoPor: state.user?.email || '',
      coleccionOriginal: 'procedimientos'
    }, { merge:true });

    batch.delete(refOriginal);
  }

  await batch.commit();

  state.selectedIds.clear();
  toast('Procedimientos archivados');
  await loadAll();
}

/* =========================
   XLSX
========================= */
function downloadBlob(filename, blob){
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(()=> URL.revokeObjectURL(url), 1500);
}

function jsonToSheetWithWidths(rows){
  const ws = XLSX.utils.json_to_sheet(rows);
  const headers = rows.length ? Object.keys(rows[0]) : [];
  ws['!cols'] = headers.map(h=>({
    wch: Math.max(
      String(h || '').length + 2,
      ...rows.map(r => String(r?.[h] ?? '').length + 2)
    )
  }));
  return ws;
}

function plantillaXLSX(){
  const rows = [
    {
      codigo: 'PA0001',
      archivo: 'RESERVO',
      categoria: 'Nutrición',
      tratamiento: 'Consulta Nutrición Bariátrica Telemedicina',
      estado: 'activa',
      modoValor: 'fijo',
      valor: 37300,
      columnaOrigen: '',
      comisionPct: 65,
      valorProfesional: 24245
    },
    {
      codigo: 'PA0002',
      archivo: 'MK',
      categoria: 'Cirugía Bariátrica',
      tratamiento: 'CONSULTA CIRUGIA GENERAL',
      estado: 'activa',
      modoValor: 'archivo',
      valor: 0,
      columnaOrigen: 'Total',
      comisionPct: 50,
      valorProfesional: 0
    }
  ];

  const wb = XLSX.utils.book_new();
  const ws = jsonToSheetWithWidths(rows);
  XLSX.utils.book_append_sheet(wb, ws, 'Plantilla');

  const arrayBuffer = XLSX.write(wb, { bookType:'xlsx', type:'array' });
  downloadBlob(
    'plantilla_ambulatorios.xlsx',
    new Blob([arrayBuffer], {
      type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    })
  );

  toast('Plantilla XLSX descargada');
}

function exportXLSX(){
  const rows = state.all.map(p=>{
    const t = p.tarifa || {};
    return {
      codigo: p.codigo,
      archivo: p.archivo,
      categoria: p.categoria,
      tratamiento: p.tratamiento,
      estado: p.estado,
      modoValor: t.modoValor || 'fijo',
      valor: Number(t.valor || 0) || 0,
      columnaOrigen: t.columnaOrigen || '',
      comisionPct: Number(t.comisionPct || 0) || 0,
      valorProfesional: Number(t.valorProfesional || 0) || 0,
      utilidad: (t.modoValor === 'fijo') ? (Number(t.utilidad || 0) || 0) : ''
    };
  });

  const wb = XLSX.utils.book_new();
  const ws = jsonToSheetWithWidths(rows.length ? rows : [{
    codigo:'', archivo:'', categoria:'', tratamiento:'', estado:'',
    modoValor:'fijo', valor:0, columnaOrigen:'', comisionPct:0, valorProfesional:0, utilidad:''
  }]);
  XLSX.utils.book_append_sheet(wb, ws, 'Ambulatorios');

  const arrayBuffer = XLSX.write(wb, { bookType:'xlsx', type:'array' });
  downloadBlob(
    `ambulatorios_${new Date().toISOString().slice(0,10)}.xlsx`,
    new Blob([arrayBuffer], {
      type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    })
  );

  toast('XLSX exportado');
}

function setImporting(isImporting, msg=''){
  const box = $('importStatus');

  if(box){
    box.style.display = isImporting ? 'block' : 'none';
    box.textContent = msg || '⏳ Importando XLSX... no cierres ni actualices esta página.';
  }

  if($('btnImportar')) $('btnImportar').disabled = isImporting;
  if($('btnCrear')) $('btnCrear').disabled = isImporting;
  if($('btnExportar')) $('btnExportar').disabled = isImporting;
  if($('btnEliminarSeleccionados')) $('btnEliminarSeleccionados').disabled = isImporting;
  if($('btnArchivarSeleccionados')) $('btnArchivarSeleccionados').disabled = isImporting;
}

async function importXLSX(file){
  const buffer = await file.arrayBuffer();
  const wb = XLSX.read(buffer, { type:'array' });
  const firstSheetName = wb.SheetNames?.[0];

  if(!firstSheetName){
    toast('Archivo XLSX vacío');
    return;
  }

  const ws = wb.Sheets[firstSheetName];
  const rows = XLSX.utils.sheet_to_json(ws, { defval:'' });

  if(!rows.length){
    toast('XLSX vacío o inválido');
    return;
  }

  const normalizedRows = rows.map(obj=>{
    const out = {};
    for(const k of Object.keys(obj || {})){
      out[cleanReminder(k).toLowerCase()] = obj[k];
    }
    return out;
  });

  const first = normalizedRows[0] || {};
  if(!('codigo' in first) || !('archivo' in first) || !('categoria' in first) || !('tratamiento' in first)){
    toast('XLSX debe incluir columnas: codigo, archivo, categoria, tratamiento');
    return;
  }

  let upserts = 0, skipped = 0;

  for(const row of normalizedRows){
    const codigo = cleanReminder(row.codigo ?? '').toUpperCase();
    const archivo = cleanReminder(row.archivo ?? '');
    const categoria = cleanReminder(row.categoria ?? '');
    const tratamiento = cleanReminder(row.tratamiento ?? '');
    const estado = (cleanReminder(row.estado ?? 'activa') || 'activa').toLowerCase();

    if(!codigo || !/^PA\d{4}$/i.test(codigo) || !archivo || !categoria || !tratamiento){
      skipped++;
      continue;
    }

    const modoValor = normalizeModoValor(row.modovalor || 'fijo');
    const valor = hasRealValue(row.valor) ? Number(row.valor) : null;
    const columnaOrigen = cleanReminder(row.columnaorigen || '');
    const comisionPct = hasRealValue(row.comisionpct) ? Number(row.comisionpct) : null;
    let valorProfesional = hasRealValue(row.valorprofesional) ? Number(row.valorprofesional) : null;

    // Si es fijo y no viene valorProfesional, pero sí valor + comisión, se calcula.
    if(
      modoValor === 'fijo' &&
      valorProfesional === null &&
      valor !== null &&
      comisionPct !== null
    ){
      valorProfesional = Math.round(valor * (comisionPct / 100));
    }

    if(modoValor === 'archivo' && !columnaOrigen){
      skipped++;
      continue;
    }

    const payload = {
      id: codigo,
      codigo,
      tipo: 'ambulatorio',
      archivo,
      categoria,
      tratamiento,
      nombre: tratamiento,
      estado,
      tarifa: {
        modoValor,
        valor: (modoValor === 'fijo') ? valor : null,
        columnaOrigen: (modoValor === 'archivo') ? columnaOrigen : '',
        comisionPct,
        valorProfesional,
        actualizadoEl: serverTimestamp(),
        actualizadoPor: state.user?.email || ''
      },
      actualizadoEl: serverTimestamp(),
      actualizadoPor: state.user?.email || '',
      creadoEl: serverTimestamp(),
      creadoPor: state.user?.email || ''
    };

    await setDoc(doc(db,'procedimientos', codigo), payload, { merge:true });
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

    await loadSidebar({ active: 'ambulatorios' });
    setActiveNav('ambulatorios');

    if($('who')) $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    $('btnProcClose')?.addEventListener('click', closeProcModal);
    $('btnProcCancelar')?.addEventListener('click', closeProcModal);
    $('btnProcGuardar')?.addEventListener('click', saveProc);

    $('modalProcBackdrop')?.addEventListener('click', (e)=>{
      if(e.target === $('modalProcBackdrop')) closeProcModal();
    });

    $('btnCrear')?.addEventListener('click', ()=> openProcModal('create'));

    $('btnEliminarSeleccionados')?.addEventListener('click', eliminarSeleccionados);
    $('btnArchivarSeleccionados')?.addEventListener('click', archivarSeleccionados);
    
    $('chkSelectAll')?.addEventListener('change', (e)=>{
      const checked = e.target.checked;
      const rows = state.all.filter(p=> rowMatches(p, state.q));
    
      for(const p of rows){
        if(checked){
          state.selectedIds.add(p.id);
        }else{
          state.selectedIds.delete(p.id);
        }
      }
    
      paint();
    });

    $('buscador')?.addEventListener('input', (e)=>{
      state.q = (e.target.value || '').toString();
      paint();
    });

    $('btnDescargarPlantilla')?.addEventListener('click', plantillaXLSX);
    $('btnExportar')?.addEventListener('click', exportXLSX);

    $('btnImportar')?.addEventListener('click', ()=> $('fileXLSX')?.click());
    $('fileXLSX')?.addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      e.target.value = '';
      if(!file) return;
    
      try{
        setImporting(true, '⏳ Importando XLSX... no cierres ni actualices esta página.');
        await importXLSX(file);
      }catch(err){
        console.error('Error importando XLSX:', err);
        toast('Error importando XLSX. Revisa la consola.');
      }finally{
        setImporting(false);
      }
    });

    $('tarModoValor')?.addEventListener('change', toggleModoValorUI);
    wireMoneyInput($('tarValor'));
    wireMoneyInput($('tarValorProfesional'));
    wirePercentInput($('tarComisionPct'));

    $('tarValor')?.addEventListener('input', computeTarPreview);
    $('tarValor')?.addEventListener('change', computeTarPreview);
    $('tarColumnaOrigen')?.addEventListener('input', computeTarPreview);
    $('tarComisionPct')?.addEventListener('input', computeTarPreview);
    $('tarComisionPct')?.addEventListener('change', computeTarPreview);
    $('tarValorProfesional')?.addEventListener('input', computeTarPreview);
    $('tarValorProfesional')?.addEventListener('change', computeTarPreview);

    updateBulkButtons();
    await loadAll();
  }
});
