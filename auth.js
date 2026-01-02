// auth.js
import { app } from './firebase-init.js';
import {
  getAuth, onAuthStateChanged,
  signInWithEmailAndPassword, signOut
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';

export const auth = getAuth(app);

export function requireAuth({ onUser, redirectTo = 'login.html' } = {}){
  onAuthStateChanged(auth, (user)=>{
    if(!user){
      location.href = redirectTo;
      return;
    }
    onUser && onUser(user);
  });
}

export async function login(email, password){
  return signInWithEmailAndPassword(auth, email, password);
}

export async function logout(){
  return signOut(auth);
}
