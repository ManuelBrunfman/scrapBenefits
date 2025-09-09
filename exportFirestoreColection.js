// exportFirestore.js
const { initializeApp } = require("firebase/app");
const { getFirestore, collection, getDocs } = require("firebase/firestore");
const fs = require("fs");

// ⚠️ Config de Firebase
const firebaseConfig = {
  apiKey: "AIzaSyCzMoC0LjjxRaNHDO1sI1Vtwv7AavOdXR8",
  authDomain: "la-bancaria-web.firebaseapp.com",
  projectId: "la-bancaria-web",
  storageBucket: "la-bancaria-web.appspot.com",
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

async function exportCollection(colName) {
  const colRef = collection(db, colName);
  const snapshot = await getDocs(colRef);

  const data = snapshot.docs.map((doc) => ({
    id: doc.id,
    ...doc.data(),
  }));

  fs.writeFileSync(`${colName}.json`, JSON.stringify(data, null, 2));
  console.log(`✅ Colección exportada en ${colName}.json`);
}

// Cambiá "beneficios" por el nombre de tu colección
exportCollection("beneficios");
