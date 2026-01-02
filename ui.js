// ui.js
import { logout } from './auth.js';

export function setActiveNav(id){
  document.querySelectorAll('[data-nav]').forEach(a=>{
    a.classList.toggle('active', a.dataset.nav === id);
  });
}

export function toast(msg){
  const el = document.getElementById('toast');
  if(!el) return;
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(el._t);
  el._t = setTimeout(()=> el.classList.remove('show'), 2600);
}

export function wireLogout(){
  const btn = document.getElementById('btnLogout');
  if(btn){
    btn.addEventListener('click', async ()=>{
      await logout();
      location.href = 'login.html';
    });
  }
}
