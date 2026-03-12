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
  query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================
   State
========================= */
const state = {
  user: null,
  q: '',
  all: [],
  editProcId: null
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

function asNumberLoose(v){
  const s = (v ?? '').toString().replace(/[^\d]/g,'');
  return Number(s || 0) || 0;
}

function clp(n){
  const x = Number(n || 0) || 0;
  const s = Math.round(x).toString();
  const withDots = s.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return `$${withDots}`;
}

function pct(n){
  const x = Number(n || 0) || 0;
  return `${x}%`;
}

function wireMoneyInput(el){
  if(!el) return;

  const repaint = ()=>{
    const n = asNumberLoose(el.value);
    el.value = (n > 0) ? clp(n) : '';
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
    const n = Number(raw || 0);
    if(Number.isNaN(n)) return;
    if(n < 0) el.value = '0';
    else if(n > 100) el.value = '100';
    else el.value = raw;
  });

  el.addEventListener('blur', ()=>{
    const n = Number((el.value ?? '').toString().replace(',','.')) || 0;
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
  const valor = Number(tarifaRaw.valor ?? 0) || 0;
  const columnaOrigen = cleanReminder(tarifaRaw.columnaOrigen || '');
  const comisionPct = Number(tarifaRaw.comisionPct ?? 0) || 0;
  const valorProfesional = Number(tarifaRaw.valorProfesional ?? 0) || 0;

  let utilidad = null;
  if(modoValor === 'fijo'){
    utilidad = valor - valorProfesional;
  }

  const hasTarifa =
    (modoValor === 'archivo')
      ? !!columnaOrigen || comisionPct > 0 || valorProfesional > 0
      : valor > 0 || comisionPct > 0 || valorProfesional > 0;

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
    const vp = Number(t.valorProfesional || 0) || 0;
    const com = Number(t.comisionPct || 0) || 0;

    return `
      <div style="display:flex; flex-wrap:wrap; gap:8px;">
        <span class="pill"><b>VALOR:</b> DESDE ARCHIVO</span>
        <span class="pill"><b>COLUMNA:</b> ${escapeHtml(col)}</span>
        <span class="pill"><b>COMISIÓN:</b> ${escapeHtml(pct(com))}</span>
        <span class="pill"><b>PAGO PROF.:</b> ${vp > 0 ? escapeHtml(clp(vp)) : '—'}</span>
      </div>
    `;
  }

  const valor = Number(t.valor || 0) || 0;
  const pago = Number(t.valorProfesional || 0) || 0;
  const utilidad = Number(t.utilidad || 0) || 0;
  const com = Number(t.comisionPct || 0) || 0;

  return `
    <div style="display:flex; flex-wrap:wrap; gap:8px;">
      <span class="pill"><b>${clp(valor)}</b></span>
      <span class="pill">Comisión ${escapeHtml(pct(com))}</span>
      <span class="pill">Pago prof. ${escapeHtml(clp(pago))}</span>
      <span class="pill">Utilidad ${escapeHtml(clp(utilidad))}</span>
    </div>
  `;
}

/* =========================
   Paint table
========================= */
function paint(){
  const rows = state.all.filter(p=> rowMatches(p, state.q));
  if($('count')) $('count').textContent = `${rows.length} ambulatorio${rows.length===1?'':'s'}`;

  const tb = $('tbody');
  if(!tb) return;
  tb.innerHTML = '';

  for(const p of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `
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
  const valor = asNumberLoose($('tarValor')?.value || '');
  const columna = cleanReminder($('tarColumnaOrigen')?.value || '');
  const comisionPct = Number((($('tarComisionPct')?.value || '').toString().replace(',','.'))) || 0;
  const valorProfesional = asNumberLoose($('tarValorProfesional')?.value || '');

  if($('tarResumen')){
    if(modo === 'archivo'){
      $('tarResumen').innerHTML =
        `Valor ${spanPrice('DESDE ARCHIVO')} · ` +
        `Columna ${spanCost(escapeHtml(columna || '—'))}`;
    } else {
      const utilidad = valor - valorProfesional;
      $('tarResumen').innerHTML =
        `Valor ${spanPrice(clp(valor))} · ` +
        `Pago profesional ${spanCost(clp(valorProfesional))} · ` +
        `Utilidad ${spanProfit(clp(utilidad))}`;
    }
  }

  if($('tarTotales')){
    if(modo === 'archivo'){
      $('tarTotales').innerHTML = `
        MODO: <b>DESDE ARCHIVO</b><br/>
        COLUMNA ORIGEN: <b>${escapeHtml(columna || '—')}</b><br/>
        COMISIÓN: <b>${escapeHtml(pct(comisionPct))}</b><br/>
        PAGO PROFESIONAL: <b>${valorProfesional > 0 ? clp(valorProfesional) : '—'}</b>
      `;
    } else {
      const utilidad = valor - valorProfesional;
      $('tarTotales').innerHTML = `
        VALOR: <b>${clp(valor)}</b><br/>
        COMISIÓN: <b>${escapeHtml(pct(comisionPct))}</b><br/>
        PAGO PROFESIONAL: <b>${clp(valorProfesional)}</b><br/>
        <div style="height:6px;"></div>
        UTILIDAD: <b>${clp(utilidad)}</b>
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
    $('procCodigo').value = '';
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
    $('tarValor').value = (Number(p?.tarifa?.valor || 0) > 0) ? clp(p.tarifa.valor) : '';
    $('tarColumnaOrigen').value = p?.tarifa?.columnaOrigen || '';
    $('tarComisionPct').value = String(Number(p?.tarifa?.comisionPct || 0) || 0);
    $('tarValorProfesional').value = (Number(p?.tarifa?.valorProfesional || 0) > 0) ? clp(p.tarifa.valorProfesional) : '';
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
  const valor = asNumberLoose($('tarValor').value);
  const columnaOrigen = cleanReminder($('tarColumnaOrigen').value);
  const comisionPct = Number((($('tarComisionPct').value || '').toString().replace(',','.'))) || 0;
  const valorProfesional = asNumberLoose($('tarValorProfesional').value);

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
      valor: (modoValor === 'fijo') ? valor : 0,
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
    const valor = Number(row.valor || 0) || 0;
    const columnaOrigen = cleanReminder(row.columnaorigen || '');
    const comisionPct = Number((row.comisionpct ?? 0)) || 0;
    let valorProfesional = Number((row.valorprofesional ?? 0)) || 0;

    // Si es fijo y no viene valorProfesional, pero sí valor + comisión, se calcula.
    if(modoValor === 'fijo' && valorProfesional <= 0 && valor > 0 && comisionPct > 0){
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
        valor: (modoValor === 'fijo') ? valor : 0,
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
      await importXLSX(file);
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

    await loadAll();
  }
});
