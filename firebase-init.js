// firebase-init.js
import { initializeApp } from "https://www.gstatic.com/firebasejs/11.7.3/firebase-app.js";
import { getFirestore } from "https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js";

// Config de tu proyecto (la que me enviaste)
const firebaseConfig = {
  apiKey: "AIzaSyAZE4H0iAgLv5f3gsYBupSE7_0ykpjfTUE",
  authDomain: "sistemas-crennat.firebaseapp.com",
  projectId: "sistemas-crennat",
  storageBucket: "sistemas-crennat.firebasestorage.app",
  messagingSenderId: "496974042856",
  appId: "1:496974042856:web:44e539285d507223691b5d"
};

// Inicializamos Firebase y exportamos para el resto del código
export const app = initializeApp(firebaseConfig);
export const db   = getFirestore(app);
