// estadisticas.js — COMPLETO
// ✅ Lee producción: produccion/{YYYY}/meses/{MM}/pacientes/{RUT}/items/{...}
// ✅ Lee tarifario: procedimientos (tipo='cirugia') con tarifas.{clinica}.pacientes.{tipoPaciente}
// ✅ Calcula rentabilidad: precio - (hmq + dp + ins)
// ✅ KPIs + Ranking cirugías + Ranking clínicas + mix tipo paciente
// ✅ Filtros: año/mes, rango fechas, tipos paciente (chips), búsqueda

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { loadSidebar } from './layout.js';
import { setActiveNav, toast, wireLogout } from './ui.js';

import {
  collection, doc, getDoc, getDocs, query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================================================
   0) MAPEO PRODUCCIÓN (AJUSTA SOLO SI TUS CAMPOS DIFIEREN)
   =========================================================
   La idea: de cada item de producción necesitamos:
   - fechaISO (YYYY-MM-DD)
   - clinicaId (o clinica nombre)
   - tipoPaciente (fonasa | particular_isapre | mle)
   - procedimientoId (PC0001...) o al menos el "codigo" de cirugía
   - (opcional) procedimientoNombre
*/
const MAP = {
  // Si tu item ya trae fechaISO y horaHM:
  fechaISO: ['fechaISO', 'FechaISO', 'fecha_iso', 'fecha'], // fallback: si viene 'Fecha' en dd/mm/aaaa no lo parseamos aquí
  // clínica:
  clinicaId: ['clinicaId', 'clinica', 'Clinica', 'CLINICA', 'clinica_id'],
  clinicaNombre: ['clinicaNombre', 'ClinicaNombre', 'nombreClinica'],
  // tipo paciente:
  tipoPaciente: ['tipoPaciente', 'TipoPaciente', 'tipo_paciente', 'prevision', 'Previsión'],
  // procedimiento (idealmente id PC0001):
  procedimientoId: ['procedimientoId', 'procedimiento', 'Procedimiento', 'cirugiaId', 'codigoCirugia', 'Cirugía'],
  procedimientoNombre: ['procedimientoNombre', 'cirugiaNombre', 'CirugíaNombre', 'Cirugía'],
  // flags opcionales:
  anulado: ['anulado', 'Anulado'],
  confirmado: ['confirmado', 'Confirmado']
};

/* =========================================================
   1) Estado
   ========================================================= */
const state = {
  user: null,
  year: null,
  month: null,
  q: '',
  from: '',
  to: '',
  tipos: new Set(['particular_isapre','fonasa','mle']),
  // caches
  procedimientosMap: new Map(), // procId -> { id,codigo,nombre,tarifasMap,rolesIds }
  clinicasMap: new Map(),       // id -> nombre (si lo cargas luego)
  // datos base
  items: [],       // items producción normalizados
  facts: [],       // items + rentabilidad aplicada
};

const $ = (id)=> document.getElementById(id);

/* =========================================================
   2) Utils
   ========================================================= */
function clean(s=''){ return (s ?? '').toString().trim(); }
function norm(s=''){
  return clean(s).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
}
function clp(n){
  const x = Number(n || 0) || 0;
  const s = Math.round(x).toString();
  return '$' + s.replace(/\B(?=(\d{3})+(?!\d))/g,'.');
}
function pct(n){
  if(!isFinite(n)) return '—';
  return `${n.toFixed(1)}%`;
}
function isoFromAny(v){
  // acepta:
  // - YYYY-MM-DD
  // - Date
  // - timestamp ms
  const s = clean(v);
  if(!s) return '';
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  // si viene dd/mm/aaaa, conviértelo
  if(/^\d{2}\/\d{2}\/\d{4}$/.test(s)){
    const [dd,mm,yyyy] = s.split('/');
    return `${yyyy}-${mm}-${dd}`;
  }
  const d = (v instanceof Date) ? v : new Date(v);
  if(!isNaN(d.getTime())){
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,'0');
    const da = String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${da}`;
  }
  return '';
}
function pick(obj, keys=[]){
  for(const k of keys){
    if(obj && obj[k] !== undefined && obj[k] !== null && clean(obj[k]) !== ''){
      return obj[k];
    }
  }
  return '';
}
function normalizeTipoPaciente(v=''){
  const x = norm(v);
  // tu sistema usa canónico: particular_isapre / fonasa / mle
  if(x.includes('fonasa')) return 'fonasa';
  if(x.includes('mle')) return 'mle';
  // cualquier cosa particular/isapre cae aquí:
  if(x.includes('isapre') || x.includes('particular')) return 'particular_isapre';
  // si ya viene correcto:
  if(x === 'particular_isapre') return 'particular_isapre';
  return x || ''; // fallback
}
function tipoLabel(tp){
  const x = normalizeTipoPaciente(tp);
  if(x === 'particular_isapre') return 'PARTICULAR / ISAPRE';
  if(x === 'fonasa') return 'FONASA';
  if(x === 'mle') return 'MLE';
  return (tp || '—').toString().toUpperCase();
}
function withinRange(fechaISO, fromISO, toISO){
  if(!fechaISO) return false;
  if(fromISO && fechaISO < fromISO) return false;
  if(toISO && fechaISO > toISO) return false;
  return true;
}
function showProgress(on, pct=0){
  const wrap = $('progressWrap');
  const bar = $('progressBar');
  if(!wrap || !bar) return;
  wrap.style.display = on ? 'block' : 'none';
  bar.style.width = `${Math.max(0, Math.min(100, pct))}%`;
}

/* =========================================================
   3) Cargar tarifario real (procedimientos tipo cirugia)
   ========================================================= */
async function loadProcedimientosCirugias(){
  state.procedimientosMap.clear();

  const qy = query(collection(db,'procedimientos'), where('tipo','==','cirugia'));
  const snap = await getDocs(qy);

  snap.forEach(d=>{
    const x = d.data() || {};
    const id = d.id;
    state.procedimientosMap.set(id, {
      id,
      codigo: clean(x.codigo) || id,
      nombre: clean(x.nombre) || '',
      rolesIds: Array.isArray(x.rolesIds) ? x.rolesIds.filter(Boolean) : [],
      tarifasMap: (x.tarifas && typeof x.tarifas === 'object') ? x.tarifas : {}
    });
  });
}

function findTarifa(procId, clinicaId, tipoPaciente){
  const p = state.procedimientosMap.get(procId);
  if(!p) return null;

  const clinNode = p.tarifasMap?.[clinicaId] || null;
  const pacientes = clinNode?.pacientes || null;
  if(!pacientes) return null;

  const tp = normalizeTipoPaciente(tipoPaciente);

  // regla canónica del módulo: preferir particular_isapre si existe
  let nodo = pacientes?.[tp] || null;

  if(!nodo && tp === 'particular_isapre'){
    nodo = pacientes?.['particular_isapre']
      || pacientes?.['particular']
      || pacientes?.['isapre']
      || null;
  }

  if(!nodo) return null;

  const precio = Number(nodo.precio ?? 0) || 0;
  const dp = Number(nodo.derechosPabellon ?? 0) || 0;
  const ins = Number(nodo.insumos ?? 0) || 0;
  const honorarios = (nodo.honorarios && typeof nodo.honorarios === 'object') ? nodo.honorarios : {};

  // HMQ = suma honorarios roles (si quieres “solo roles permitidos”, puedes filtrar por p.rolesIds)
  let hmq = 0;
  for(const k of Object.keys(honorarios)){
    const n = Number(honorarios[k] || 0) || 0;
    if(n > 0) hmq += n;
  }

  const costo = hmq + dp + ins;
  const utilidad = (precio || 0) - costo;
  const margen = (precio > 0) ? (utilidad / precio * 100) : null;

  const hasAny = (precio>0 || costo>0);
  return { precio, dp, ins, hmq, costo, utilidad, margen, hasAny };
}

/* =========================================================
   4) Cargar producción real por mes
   ========================================================= */
async function loadProduccionMonth(YYYY, MM){
  // produccion/{YYYY}/meses/{MM}/pacientes/{RUT}/items/{...}
  const colPac = collection(db, 'produccion', String(YYYY), 'meses', String(MM), 'pacientes');
  const snapPac = await getDocs(colPac);

  const out = [];
  let i = 0;
  const totalPac = snapPac.size || 1;

  for(const pDoc of snapPac.docs){
    i++;
    showProgress(true, Math.round((i/totalPac)*100));

    const rut = pDoc.id;
    const colItems = collection(db, 'produccion', String(YYYY), 'meses', String(MM), 'pacientes', rut, 'items');
    const snapItems = await getDocs(colItems);

    snapItems.forEach(it=>{
      const x = it.data() || {};

      const fechaISO = isoFromAny(pick(x, MAP.fechaISO)) || isoFromAny(x?.raw?.Fecha) || '';
      const clinicaId = clean(pick(x, MAP.clinicaId));
      const clinicaNombre = clean(pick(x, MAP.clinicaNombre));
      const tipoPaciente = normalizeTipoPaciente(pick(x, MAP.tipoPaciente));

      // procedimientoId ideal: PC0001
      const procedimientoId = clean(pick(x, MAP.procedimientoId));
      const procedimientoNombre = clean(pick(x, MAP.procedimientoNombre));

      const anulado = Boolean(pick(x, MAP.anulado));
      const confirmado = (pick(x, MAP.confirmado) === '' ? true : Boolean(pick(x, MAP.confirmado))); // si no existe, asumimos true

      out.push({
        id: it.id,
        rut,
        year: String(YYYY),
        month: String(MM),
        fechaISO,
        clinicaId: clinicaId || (clinicaNombre ? clinicaNombre : ''),
        clinicaNombre: clinicaNombre || clinicaId || '',
        tipoPaciente,
        procedimientoId: procedimientoId || '', // si viene texto, igual lo guardamos
        procedimientoNombre,
        confirmado,
        anulado,
        _raw: x
      });
    });
  }

  showProgress(false, 0);
  return out;
}

/* =========================================================
   5) Normalizar a FACTS (aplicar tarifa y rentabilidad)
   ========================================================= */
function buildFacts(){
  const q = norm(state.q);
  const fromISO = clean(state.from);
  const toISO = clean(state.to);

  const tiposAllowed = new Set([...state.tipos].map(normalizeTipoPaciente));

  const facts = [];
  let pendientes = 0;

  for(const it of state.items){
    if(it.anulado) continue;

    if(fromISO || toISO){
      if(!withinRange(it.fechaISO, fromISO, toISO)) continue;
    }

    const tp = normalizeTipoPaciente(it.tipoPaciente);
    if(tp && !tiposAllowed.has(tp)) continue;

    // búsqueda simple
    if(q){
      const hay = norm([
        it.procedimientoId,
        it.procedimientoNombre,
        it.clinicaId,
        it.clinicaNombre,
        tp,
        tipoLabel(tp)
      ].join(' '));
      if(!hay.includes(q)) continue;
    }

    // resolver procedimientoId real: si te llega “Manga” en vez de PC0001
    // estrategia: si coincide con un docId, listo; si no, intentar por nombre/código
    let procId = it.procedimientoId;
    if(procId && !state.procedimientosMap.has(procId)){
      const needle = norm(procId);
      let found = null;
      for(const [id,p] of state.procedimientosMap.entries()){
        if(norm(id) === needle) { found = id; break; }
        if(norm(p.codigo) === needle) { found = id; break; }
        if(norm(p.nombre) === needle) { found = id; break; }
      }
      procId = found || procId; // si no, queda “no resolvido”
    }

    const clinId = clean(it.clinicaId);
    const tarifa = (procId && clinId) ? findTarifa(procId, clinId, tp) : null;

    const precio = tarifa?.precio ?? 0;
    const costo = tarifa?.costo ?? 0;
    const utilidad = tarifa?.utilidad ?? 0;
    const margen = tarifa?.margen ?? null;

    const hasTarifa = Boolean(tarifa && tarifa.hasAny);

    if(!hasTarifa) pendientes++;

    facts.push({
      ...it,
      procIdResolved: procId,
      procNombreResolved: state.procedimientosMap.get(procId)?.nombre || it.procedimientoNombre || it.procedimientoId || '—',
      clinNombreResolved: it.clinicaNombre || it.clinicaId || '—',
      tpResolved: tp,
      hasTarifa,
      precio,
      costo,
      utilidad,
      margen,
      hmq: tarifa?.hmq ?? 0,
      dp: tarifa?.dp ?? 0,
      ins: tarifa?.ins ?? 0
    });
  }

  state.facts = facts;
  return { pendientes, total: facts.length };
}

/* =========================================================
   6) Agregaciones (KPIs y rankings)
   ========================================================= */
function aggregate(){
  const facts = state.facts;

  let casos = 0;
  let ingresos = 0;
  let costos = 0;
  let utilidad = 0;
  let hmq = 0, dp = 0, ins = 0;

  const mix = { particular_isapre:0, fonasa:0, mle:0, otros:0 };
  const byProc = new Map();
  const byClin = new Map();
  let pendientes = 0;

  for(const f of facts){
    casos++;
    if(!f.hasTarifa) pendientes++;

    ingresos += Number(f.precio||0);
    costos += Number(f.costo||0);
    utilidad += Number(f.utilidad||0);
    hmq += Number(f.hmq||0);
    dp += Number(f.dp||0);
    ins += Number(f.ins||0);

    if(f.tpResolved === 'particular_isapre') mix.particular_isapre++;
    else if(f.tpResolved === 'fonasa') mix.fonasa++;
    else if(f.tpResolved === 'mle') mix.mle++;
    else mix.otros++;

    // proc
    const pk = f.procNombreResolved || '—';
    const p = byProc.get(pk) || { casos:0, ingresos:0, costos:0, utilidad:0 };
    p.casos++;
    p.ingresos += Number(f.precio||0);
    p.costos += Number(f.costo||0);
    p.utilidad += Number(f.utilidad||0);
    byProc.set(pk,p);

    // clin
    const ck = f.clinNombreResolved || '—';
    const c = byClin.get(ck) || { casos:0, utilidad:0, ingresos:0 };
    c.casos++;
    c.utilidad += Number(f.utilidad||0);
    c.ingresos += Number(f.precio||0);
    byClin.set(ck,c);
  }

  const margen = (ingresos > 0) ? (utilidad/ingresos*100) : null;

  const procRows = [...byProc.entries()].map(([k,v])=>{
    const m = (v.ingresos>0) ? (v.utilidad/v.ingresos*100) : null;
    return { name:k, ...v, margen:m };
  }).sort((a,b)=> (b.utilidad - a.utilidad));

  const clinRows = [...byClin.entries()].map(([k,v])=>{
    const m = (v.ingresos>0) ? (v.utilidad/v.ingresos*100) : null;
    return { name:k, ...v, margen:m };
  }).sort((a,b)=> (b.utilidad - a.utilidad));

  return {
    kpis: { casos, pendientes, ingresos, costos, utilidad, margen, hmq, dp, ins },
    mix,
    procRows,
    clinRows
  };
}

/* =========================================================
   7) Render
   ========================================================= */
function setText(id, txt){ const el=$(id); if(el) el.textContent = txt; }
function setHTML(id, html){ const el=$(id); if(el) el.innerHTML = html; }

function paint(){
  const { pendientes, total } = buildFacts();
  const agg = aggregate();
  const k = agg.kpis;

  setText('kCasos', String(k.casos));
  setText('kPend', `Tarifas pendientes: ${pendientes}`);
  setText('kIngresos', clp(k.ingresos));
  setText('kCostos', clp(k.costos));
  setText('kUtilidad', clp(k.utilidad));
  setText('kMargen', k.margen===null ? 'Margen: —' : `Margen: ${pct(k.margen)}`);

  setText('kDetalleCosto', `HMQ ${clp(k.hmq)} · DP ${clp(k.dp)} · INS ${clp(k.ins)}`);

  // Mix
  const mix = agg.mix;
  const totalMix = mix.particular_isapre + mix.fonasa + mix.mle + mix.otros;
  const p1 = totalMix ? (mix.particular_isapre/totalMix*100) : 0;
  const p2 = totalMix ? (mix.fonasa/totalMix*100) : 0;
  const p3 = totalMix ? (mix.mle/totalMix*100) : 0;

  setText('mixBox', `PI ${mix.particular_isapre} · FONASA ${mix.fonasa} · MLE ${mix.mle}`);
  setText('mixSub', `PI ${pct(p1)} · FONASA ${pct(p2)} · MLE ${pct(p3)}`);

  // Calidad
  const ok = total ? (total - pendientes) : 0;
  setText('qualityBox', `${ok} OK · ${pendientes} pendientes`);
  setText('qualitySub', pendientes ? 'Pendiente = falta precio o costos en tarifario para ese contexto.' : 'Todo el rango tiene tarifario aplicable.');

  // Ranking procedimientos
  setText('rankProcCount', `${agg.procRows.length} cirugías`);
  const tbProc = $('tbProc');
  tbProc.innerHTML = '';
  for(const r of agg.procRows.slice(0, 40)){
    const m = (r.margen==null) ? '—' : pct(r.margen);
    const cls = (r.margen==null) ? 'warn' : (r.margen>=0 ? 'ok' : 'bad');
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(r.name)}</td>
      <td class="right mono">${r.casos}</td>
      <td class="right mono">${clp(r.ingresos)}</td>
      <td class="right mono">${clp(r.costos)}</td>
      <td class="right mono">${clp(r.utilidad)}</td>
      <td class="right"><span class="badge ${cls}">${m}</span></td>
    `;
    tbProc.appendChild(tr);
  }

  // Ranking clínicas
  const tbClin = $('tbClin');
  tbClin.innerHTML = '';
  for(const r of agg.clinRows.slice(0, 40)){
    const m = (r.margen==null) ? '—' : pct(r.margen);
    const cls = (r.margen==null) ? 'warn' : (r.margen>=0 ? 'ok' : 'bad');
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(r.name)}</td>
      <td class="right mono">${r.casos}</td>
      <td class="right mono">${clp(r.utilidad)}</td>
      <td class="right"><span class="badge ${cls}">${m}</span></td>
    `;
    tbClin.appendChild(tr);
  }

  // Export habilitado
  $('btnExport').disabled = (state.facts.length === 0);
}

function escapeHtml(s=''){
  return (s ?? '').toString()
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#039;");
}

/* =========================================================
   8) UI: filtros
   ========================================================= */
function paintYearMonth(){
  const now = new Date();
  const yNow = now.getFullYear();

  const ySel = $('fYear');
  ySel.innerHTML = '';
  for(let y=yNow-2; y<=yNow+1; y++){
    const opt = document.createElement('option');
    opt.value = String(y);
    opt.textContent = String(y);
    ySel.appendChild(opt);
  }
  ySel.value = String(yNow);

  const mSel = $('fMonth');
  mSel.innerHTML = '';
  for(let m=1; m<=12; m++){
    const mm = String(m).padStart(2,'0');
    const opt = document.createElement('option');
    opt.value = mm;
    opt.textContent = mm;
    mSel.appendChild(opt);
  }
  mSel.value = String(now.getMonth()+1).padStart(2,'0');
}

function paintTipoChips(){
  const wrap = $('chipsTipos');
  wrap.innerHTML = '';

  const tipos = [
    { id:'particular_isapre', label:'PARTICULAR / ISAPRE' },
    { id:'fonasa', label:'FONASA' },
    { id:'mle', label:'MLE' },
  ];

  for(const t of tipos){
    const d = document.createElement('div');
    d.className = 'chip ' + (state.tipos.has(t.id) ? 'active' : '');
    d.textContent = t.label;
    d.addEventListener('click', ()=>{
      if(state.tipos.has(t.id)) state.tipos.delete(t.id);
      else state.tipos.add(t.id);
      // no dejamos vacío total, para no “desaparecer todo” sin querer
      if(state.tipos.size === 0) state.tipos.add(t.id);
      paintTipoChips();
      paint();
    });
    wrap.appendChild(d);
  }
}

function resetFilters(){
  $('fFrom').value = '';
  $('fTo').value = '';
  $('fQ').value = '';
  state.q = '';
  state.from = '';
  state.to = '';
  state.tipos = new Set(['particular_isapre','fonasa','mle']);
  paintTipoChips();
  paint();
}

/* =========================================================
   9) Export CSV (resumen por cirugía)
   ========================================================= */
function toCSV(headers, rows){
  const esc = (v)=> {
    const s = (v ?? '').toString();
    if(/[",\n]/.test(s)) return `"${s.replaceAll('"','""')}"`;
    return s;
  };
  const out = [];
  out.push(headers.map(esc).join(','));
  for(const r of rows){
    out.push(headers.map(h=> esc(r[h])).join(','));
  }
  return out.join('\n');
}
function download(name, text, mime='text/csv'){
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(()=> URL.revokeObjectURL(url), 1200);
}

function exportResumen(){
  const agg = aggregate();
  const rows = agg.procRows.map(r=>({
    cirugia: r.name,
    casos: r.casos,
    precio: Math.round(r.ingresos),
    costo: Math.round(r.costos),
    utilidad: Math.round(r.utilidad),
    margen_pct: (r.margen==null) ? '' : r.margen.toFixed(1)
  }));

  const headers = ['cirugia','casos','precio','costo','utilidad','margen_pct'];
  const csv = toCSV(headers, rows);
  const y = state.year, m = state.month;
  download(`resumen_rentabilidad_${y}-${m}.csv`, csv);
}

/* =========================================================
   10) Carga principal
   ========================================================= */
async function loadAll(){
  const YYYY = $('fYear').value;
  const MM = $('fMonth').value;

  state.year = YYYY;
  state.month = MM;

  state.q = $('fQ').value || '';
  state.from = $('fFrom').value || '';
  state.to = $('fTo').value || '';

  $('btnLoad').disabled = true;
  $('btnExport').disabled = true;
  setText('statusLine', 'Cargando tarifario (procedimientos)…');

  try{
    await loadProcedimientosCirugias();

    setText('statusLine', `Cargando producción ${YYYY}-${MM}…`);
    showProgress(true, 0);

    const items = await loadProduccionMonth(YYYY, MM);
    state.items = items;

    setText('statusLine', `OK: ${items.length} items leídos. Calculando…`);
    paint();

    setText('statusLine', `Listo. (${items.length} items leídos, ${state.facts.length} en el análisis con filtros).`);
    $('btnExport').disabled = (state.facts.length === 0);
  }catch(err){
    console.error(err);
    toast('Error cargando datos. Revisa consola.');
    setText('statusLine', 'Error al cargar. Revisa consola.');
  }finally{
    showProgress(false, 0);
    $('btnLoad').disabled = false;
  }
}

/* =========================================================
   11) Boot
   ========================================================= */
requireAuth({
  onUser: async (user)=>{
    state.user = user;

    // Sidebar común (si la usas en el resto)
    await loadSidebar({ active: 'estadisticas' });
    setActiveNav('estadisticas');
    wireLogout();

    if($('who')) $('who').textContent = `Conectado: ${user.email}`;

    paintYearMonth();
    paintTipoChips();

    $('btnLoad').addEventListener('click', loadAll);
    $('btnReset').addEventListener('click', resetFilters);
    $('btnExport').addEventListener('click', exportResumen);

    $('fQ').addEventListener('input', ()=>{
      state.q = $('fQ').value || '';
      paint();
    });
    $('fFrom').addEventListener('change', ()=>{
      state.from = $('fFrom').value || '';
      paint();
    });
    $('fTo').addEventListener('change', ()=>{
      state.to = $('fTo').value || '';
      paint();
    });

    // carga inicial automática
    await loadAll();
  }
});
