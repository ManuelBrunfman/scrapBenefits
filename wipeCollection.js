const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

function argFlag(name, def = undefined) {
  const found = process.argv.find(a => a.startsWith(`--${name}`));
  if (!found) return def;
  const [_, v] = found.split('=');
  return v === undefined ? true : v;
}

const COLLECTION = argFlag('collection', 'beneficios');
const DRY_RUN = !!argFlag('dry-run', false);
const CREDENTIALS_FLAG = argFlag('credentials', undefined);

function resolveServiceAccountPath() {
  if (CREDENTIALS_FLAG === true) {
    console.warn('La bandera --credentials requiere un path. Ej: --credentials=./serviceAccount.json');
  }
  if (typeof CREDENTIALS_FLAG === 'string') {
    const cliPath = path.isAbsolute(CREDENTIALS_FLAG) ? CREDENTIALS_FLAG : path.resolve(process.cwd(), CREDENTIALS_FLAG);
    if (fs.existsSync(cliPath)) {
      process.env.GOOGLE_APPLICATION_CREDENTIALS = cliPath;
      return cliPath;
    }
    console.error(`--credentials apunta a ${cliPath} pero no existe.`);
    return null;
  }

  const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (envPath) {
    const envResolved = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
    if (fs.existsSync(envResolved)) return envResolved;
    console.warn(`GOOGLE_APPLICATION_CREDENTIALS apunta a ${envResolved} pero no existe. Se intenta fallback local.`);
  }

  const fallback = path.resolve(__dirname, 'serviceAccount.json');
  if (fs.existsSync(fallback)) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = fallback;
    return fallback;
  }
  return null;
}

(async function main(){
  try {
    const credentialsPath = resolveServiceAccountPath();
    if (!credentialsPath) {
      console.error('No se encontro el archivo de credenciales (serviceAccount.json). Usa --credentials=/ruta/archivo.json o setea GOOGLE_APPLICATION_CREDENTIALS.');
      process.exit(1);
    }
    if (!admin.apps.length) {
      const serviceAccount = JSON.parse(fs.readFileSync(credentialsPath, 'utf8'));
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: serviceAccount.project_id,
        databaseURL: serviceAccount.project_id ? `https://${serviceAccount.project_id}.firebaseio.com` : undefined,
      });
    }

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
