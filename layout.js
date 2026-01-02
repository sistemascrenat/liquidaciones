// layout.js — layout compartido (sidebar + navegación)

import { wireLogout } from './ui.js';

/**
 * Carga el sidebar y lo monta en el DOM
 * @param {string} activeNav - data-nav activo (ej: 'profesionales')
 */
export async function loadSidebar(activeNav){
  const container = document.getElementById('sidebar-slot');
  if(!container){
    console.warn('[layout] No existe #sidebar-slot');
    return;
  }

  try{
    const res = await fetch('sidebar.html');
    const html = await res.text();
    container.innerHTML = html;

    // marcar activo
    if(activeNav){
      const link = container.querySelector(`[data-nav="${activeNav}"]`);
      if(link) link.classList.add('active');
    }

    // módulos en construcción
    container.querySelectorAll('a[href="#"]').forEach(a=>{
      a.addEventListener('click', (e)=>{
        e.preventDefault();
        alert('Módulo en construcción');
      });
    });

    // logout
    wireLogout();

  }catch(err){
    console.error('[layout] Error cargando sidebar:', err);
  }
}
