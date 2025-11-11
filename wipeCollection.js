const admin = require('firebase-admin');

function argFlag(name, def = undefined) {
  const found = process.argv.find(a => a.startsWith(`--${name}`));
  if (!found) return def;
  const [_, v] = found.split('=');
  return v === undefined ? true : v;
}

const COLLECTION = argFlag('collection', 'beneficios');
const DRY_RUN = !!argFlag('dry-run', false);

(async function main(){
  try {
    if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      console.error('Falta GOOGLE_APPLICATION_CREDENTIALS');
      process.exit(1);
    }
    if (!admin.apps.length) admin.initializeApp();

    const db = admin.firestore();
    console.log(`Proyecto: ${admin.app().options.projectId || '(desconocido)'} | Coleccion: ${COLLECTION} | DRY_RUN: ${DRY_RUN}`);

    const colRef = db.collection(COLLECTION);
    let total = 0;
    const BATCH_LIMIT = 400;

    while (true) {
      const snap = await colRef.limit(BATCH_LIMIT).get();
      if (snap.empty) break;
      if (DRY_RUN) {
        snap.docs.forEach(d => console.log('[dry] delete', d.ref.path));
        total += snap.size;
        break;
      }
      const batch = db.batch();
      snap.docs.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
      total += snap.size;
      console.log(`Borrados hasta ahora: ${total}`);
    }

    if (DRY_RUN) {
      console.log(`Dry-run: se hubieran borrado ${total} documentos.`);
    } else {
      console.log(`Coleccion '${COLLECTION}' vaciada. Documentos eliminados: ${total}`);
    }
  } catch (err) {
    console.error('Error al vaciar la coleccion:', err);
    process.exit(1);
  }
})();
