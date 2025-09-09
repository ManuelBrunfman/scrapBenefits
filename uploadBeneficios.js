// uploadBeneficios.js
// Sube/actualiza (upsert) beneficios a Firestore (colección 'beneficios').
// Uso:
//   node uploadBeneficios.js                     # usa beneficios.json por defecto
//   node uploadBeneficios.js ./data/otro.json    # ruta custom
//
// Opcionales:
//   --dry-run        => simula, no escribe
//   --collection=... => cambia el nombre de colección (por defecto 'beneficios')

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const admin = require('firebase-admin');

function argFlag(name, def = undefined) {
  const found = process.argv.find(a => a.startsWith(`--${name}`));
  if (!found) return def;
  const [_, v] = found.split('=');
  return v === undefined ? true : v;
}
const DRY_RUN = !!argFlag('dry-run', false);
const COLLECTION = argFlag('collection', 'beneficios');

function getJsonPathFromCli() {
  const p = process.argv[2] || 'beneficios_mejorado.json';
  const abs = path.isAbsolute(p) ? p : path.join(process.cwd(), p);
  if (!fs.existsSync(abs)) {
    console.error(`❌ No se encontró el archivo JSON en: ${abs}`);
    process.exit(1);
  }
  return abs;
}

function safeStr(x) {
  return (typeof x === 'string' ? x : (x ?? '')).trim();
}

// Normaliza URL para que IDs sean estables (evita duplicados por "/" final, utm, #, www, mayúsculas del host)
function normalizeUrlForId(u) {
  try {
    const url = new URL(u);
    // host en minúsculas y sin www.
    url.hostname = url.hostname.replace(/^www\./, '').toLowerCase();
    // sin fragmento y sin trackers comunes
    url.hash = '';
    for (const [k] of url.searchParams) {
      if (/^(utm_|fbclid$|gclid$|mc_cid$|mc_eid$)/i.test(k)) url.searchParams.delete(k);
    }
    // sin "/" final (pero preserva la raíz "/")
    url.pathname = url.pathname.replace(/\/+$/, '') || '/';
    return url.toString();
  } catch {
    return safeStr(u).replace(/\/+$/, '');
  }
}

// ID estable a partir de link (preferido) o título
function toIdFromLinkOrTitle(link, title) {
  const baseRaw = safeStr(link) || safeStr(title);
  const base = baseRaw.startsWith('http') ? normalizeUrlForId(baseRaw) : baseRaw;
  const h = crypto.createHash('sha1').update(base).digest('hex').slice(0, 16);
  const slug = safeStr(title)
    .normalize('NFKD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '-').replace(/(^-|-$)/g, '')
    .toLowerCase().slice(0, 50) || 'beneficio';
  return `${slug}-${h}`;
}

// Normaliza soportando claves en español o inglés
function normalizeItem(raw) {
  const titulo      = safeStr(raw.titulo ?? raw.title);
  const link        = safeStr(raw.link ?? raw.url);
  const imagen_url  = safeStr(raw.imagen_url ?? raw.imageUrl);
  const categoria   = safeStr(raw.categoria ?? raw.category);
  const provincia   = safeStr(raw.provincia ?? raw.province);
  const descripcion = safeStr(raw.descripcion ?? raw.description);

  if (!titulo || !link) {
    throw new Error(`Item inválido: falta titulo/title o link/url. Título="${titulo}" Link="${link}"`);
  }

  const imageUrl = imagen_url || null;

  return {
    // EN (por compatibilidad con UIs que usen EN)
    title: titulo,
    url: link,
    imageUrl,
    category: categoria || null,
    province: provincia || null,
    description: descripcion || null,

    // ES (compat con tus datos originales)
    titulo,
    link,
    imagen_url: imageUrl,
    categoria: categoria || null,
    provincia: provincia || null,
    descripcion: descripcion || null,

    // housekeeping
    source: 'bulk-upload',
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function run() {
  // 1) Inicializar Admin SDK con la variable GOOGLE_APPLICATION_CREDENTIALS
  if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    console.error('❌ Falta la variable GOOGLE_APPLICATION_CREDENTIALS apuntando a tu serviceAccount.json');
    console.error('   Ej: export GOOGLE_APPLICATION_CREDENTIALS="/ruta/clave.json"');
    process.exit(1);
  }
  if (!admin.apps.length) admin.initializeApp();

  const db = admin.firestore();

  // (opcional) Mostrar a qué proyecto te conectaste
  const opts = admin.app().options;
  console.log(`🔗 Proyecto: ${opts.projectId || '(desconocido)'}  | Colección: ${COLLECTION}  | DRY_RUN: ${DRY_RUN}`);

  // 2) Leer JSON
  const jsonPath = getJsonPathFromCli();
  const arr = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
  if (!Array.isArray(arr)) {
    throw new Error('El JSON de entrada debe ser un array de objetos.');
  }

  // 3) Subir en lotes
  const BATCH_LIMIT = 400; // margen bajo el límite de 500
  let batch = db.batch();
  let pending = 0;
  let total = 0;

  for (const raw of arr) {
    const docData = normalizeItem(raw);
    const docId = toIdFromLinkOrTitle(docData.url, docData.title);
    const ref = db.collection(COLLECTION).doc(docId);

    if (DRY_RUN) {
      // Solo muestra lo que haría
      console.log(`• [dry] set ${COLLECTION}/${docId}  (${docData.title})`);
    } else {
      batch.set(ref, docData, { merge: true });
      pending++;
      total++;
      if (pending >= BATCH_LIMIT) {
        await batch.commit();
        console.log(`✔ Committed ${total} documentos...`);
        batch = db.batch();
        pending = 0;
      }
    }
  }

  if (!DRY_RUN && pending > 0) {
    await batch.commit();
  }

  if (DRY_RUN) {
    console.log(`🧪 Dry-run completo. Documentos a escribir: ${arr.length}`);
  } else {
    console.log(`✅ Listo: subidos/actualizados ${arr.length} documentos a '${COLLECTION}'.`);
  }
}

run().catch(err => {
  console.error('❌ Error en el upload:', err);
  process.exit(1);
});
