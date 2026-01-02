// produccion.js — Módulo Producción (CSV + Staging + Confirmación)

import { db } from './firebase-init.js';
import { requireAuth } from './auth.js';
import { toast, wireLogout, setActiveNav } from './ui.js';
import { parseCSV } from './utils.js';
import { loadSidebar } from './layout.js';

import {
  collection, doc, setDoc, getDocs, serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

const $ = id => document.getElementById(id);

const state = {
  user: null,
  importId: null,
  filas: [],
  resumen: {}
};

function clp(n){
  const x = Number(n||0);
  return '$' + Math.round(x).toString().replace(/\B(?=(\d{3})+(?!\d))/g,'.');
}

async function cargarCSV(){
  const file = $('fileCSV').files[0];
  if(!file) return toast('Selecciona un CSV');

  const mes = $('mes').value;
  const anio = $('anio').value;

  const text = await file.text();
  const rows = parseCSV(text);

  const headers = rows[0];
  const data = rows.slice(1);

  const importId = `PROD_${anio}_${mes}_${Date.now()}`;
  state.importId = importId;

  let total = 0, monto = 0, cero = 0;

  const items = [];

  data.forEach((row,i)=>{
    const raw = {};
    headers.forEach((h,j)=> raw[h] = row[j] || '');

    const montoFila = Number((raw.MONTO||'').replace(/[^\d]/g,'')) || 0;
    total++;
    monto += montoFila;
    if(montoFila===0) cero++;

    items.push({
      fila: i+1,
      raw,
      normalizado:{
        fecha: raw.FECHA || '',
        profesional: raw.PROFESIONAL || '',
        rol: raw.ROL || '',
        clinica: raw.CLINICA || '',
        procedimiento: raw.PROCEDIMIENTO || '',
        tipoPaciente: raw.PACIENTE || '',
        monto: montoFila
      },
      flags:{
        montoCero: montoFila===0
      }
    });
  });

  await setDoc(doc(db,'produccion_imports',importId),{
    id: importId,
    mes,
    anio,
    estado:'borrador',
    creadoEl: serverTimestamp(),
    creadoPor: state.user.email
  });

  for(const it of items){
    await setDoc(
      doc(db,'produccion_items',`${importId}_${it.fila}`),
      { ...it, importId }
    );
  }

  state.filas = items;

  $('estadoImport').textContent = 'BORRADOR';
  $('resumenBox').style.display = 'block';
  $('rTotal').textContent = total;
  $('rMonto').textContent = clp(monto);
  $('rCero').textContent = cero;

  pintarTabla();
  toast('CSV cargado en staging');
}

function pintarTabla(){
  const tb = $('tbody');
  tb.innerHTML = '';

  state.filas.forEach(f=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${f.fila}</td>
      <td>${f.normalizado.fecha}</td>
      <td>${f.normalizado.profesional}</td>
      <td>${f.normalizado.rol}</td>
      <td>${f.normalizado.clinica}</td>
      <td>${f.normalizado.procedimiento}</td>
      <td>${f.normalizado.tipoPaciente}</td>
      <td>${clp(f.normalizado.monto)}</td>
      <td>${f.flags.montoCero ? 'INFO' : 'OK'}</td>
    `;
    tb.appendChild(tr);
  });
}

async function confirmar(){
  if(!state.importId) return;
  await setDoc(
    doc(db,'produccion_imports',state.importId),
    { estado:'confirmado', confirmadoEl: serverTimestamp() },
    { merge:true }
  );
  $('estadoImport').textContent = 'CONFIRMADO';
  toast('Producción confirmada');
}

async function anular(){
  if(!state.importId) return;
  if(!confirm('¿Anular importación?')) return;

  await setDoc(
    doc(db,'produccion_imports',state.importId),
    { estado:'anulado', anuladoEl: serverTimestamp() },
    { merge:true }
  );
  $('estadoImport').textContent = 'ANULADO';
  toast('Producción anulada');
}

requireAuth({
  onUser: async (user)=>{
    state.user = user;
    await loadSidebar({ active:'produccion' });
    setActiveNav('produccion');
    $('who').textContent = `Conectado: ${user.email}`;
    wireLogout();

    $('btnCargar').addEventListener('click', cargarCSV);
    $('btnConfirmar').addEventListener('click', confirmar);
    $('btnAnular').addEventListener('click', anular);
  }
});
