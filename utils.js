// utils.js
export const nowISO = ()=> new Date().toISOString();

export const cleanReminder = (s='') => (s ?? '').toString().trim();

export const toUpperSafe = (s='') => (s ?? '').toString().trim().toUpperCase();

export function slugify(s=''){
  return (s ?? '')
    .toString()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g,'-')
    .replace(/(^-|-$)/g,'')
    .trim();
}

export function parseCSV(text){
  // CSV simple con comas. Soporta comillas básicas.
  const rows = [];
  let cur = '', inQ = false;
  const out = [];
  for(let i=0;i<text.length;i++){
    const ch = text[i];
    if(ch === '"'){ inQ = !inQ; continue; }
    if(!inQ && (ch === ',' || ch === '\n' || ch === '\r')){
      out.push(cur); cur='';
      if(ch === '\n'){
        rows.push(out.slice()); out.length=0;
      }
      continue;
    }
    cur += ch;
  }
  if(cur.length || out.length){ out.push(cur); rows.push(out.slice()); }
  // limpia filas vacías
  return rows.map(r=>r.map(c=>cleanReminder(c))).filter(r=>r.some(c=>c!==''));
}

export function toCSV(headers, items){
  const esc = (v)=> {
    const s = (v ?? '').toString();
    if(/[",\n\r]/.test(s)) return `"${s.replaceAll('"','""')}"`;
    return s;
  };
  const lines = [];
  lines.push(headers.map(esc).join(','));
  for(const it of items){
    lines.push(headers.map(h=>esc(it[h] ?? '')).join(','));
  }
  return lines.join('\n');
}
