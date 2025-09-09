// normalizeMeta.js
// npm i firebase-admin
const admin = require('firebase-admin');
if (!admin.apps.length) admin.initializeApp();
const db = admin.firestore();

const CAPS = (s='') => s.normalize('NFKC').trim();

const catMap = new Map([
  ['retail / comercios','Retail / Comercios'],
  ['retail/comercios','Retail / Comercios'],
  ['excursiones y actividades','Excursiones y Actividades'],
  ['deportes y gimnasios','Deportes y Gimnasios'],
  ['gastronomia','Gastronomía'],
]);
const provMap = new Map([
  ['caba','CABA'],
  ['bs as','Buenos Aires'],
  ['buenos aires','Buenos Aires'],
]);

function fixCategory(x) {
  if (!x) return null;
  const k = CAPS(x).toLowerCase();
  return catMap.get(k) || x;
}
function fixProvince(x) {
  if (!x) return null;
  const k = CAPS(x).toLowerCase();
  return provMap.get(k) || x;
}

function sortKey(cat, prov, title) {
  const order = [
    'Alojamiento','Gastronomía','Excursiones y Actividades','Transporte',
    'Salud','Deportes y Gimnasios','Retail / Comercios','Educación','Otros',
  ];
  const ci = Math.max(0, order.indexOf(cat));
  const provKey = prov === 'Nacional' ? '00-Nacional' : `10-${prov||'zz'}`;
  return `${String(ci).padStart(2,'0')}|${provKey}|${(title||'').toLowerCase()}`;
}

(async () => {
  const snap = await db.collection('beneficios').get();
  let n = 0;
  for (const doc of snap.docs) {
    const d = doc.data();
    const cat = fixCategory(d.category || d.categoria || null);
    const prov = fixProvince(d.province || d.provincia || null);
    const title = d.title || d.titulo || '';
    await doc.ref.set({
      category: cat, categoria: cat,
      province: prov, provincia: prov,
      sortKey: sortKey(cat||'Otros', prov, title),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    n++;
  }
  console.log('✅ Normalizados:', n);
  process.exit(0);
})();
