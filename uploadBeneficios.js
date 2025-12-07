// PATH: upload-beneficios.js
// Sube/actualiza (upsert) beneficios a Firestore (colección 'beneficios' por defecto).
// - Categorías canónicas (8): Alojamiento, Gastronomía, Excursiones y Actividades,
//   Transporte, Retail, Comercio, Servicios, Deportes
// - Normaliza URL para ID estable (quita www, #, query, barra final y sufijo "-N")
// - Excluye ítems agregadores tipo "Disfrutá ..."
// - Pre-dedupe por URL canónica (conserva el "mejor": con imagen, provincia específica, categoría)
// - Fix puntual: Bagú Ushuaia → Tierra del Fuego si viene como "Nacional"
// Uso:
//   node upload-beneficios.js ./beneficios_mejorado.json --collection=beneficios --dry-run
//   node upload-beneficios.js ./beneficios_mejorado.json --collection=beneficios

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
const KEEP_MISSING = !!argFlag('keep-missing', false);
const CREDENTIALS_FLAG = argFlag('credentials', undefined);

function getJsonPathFromCli() {
  const p = process.argv[2] || 'beneficios_ocr.json';
  const abs = path.isAbsolute(p) ? p : path.join(process.cwd(), p);
  if (!fs.existsSync(abs)) {
    console.error(`❌ No se encontró el archivo JSON en: ${abs}`);
    process.exit(1);
  }
  return abs;
}

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
    console.error('--credentials apunta a ' + cliPath + ' pero no existe.');
    return null;
  }

  const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (envPath) {
    const envResolved = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
    if (fs.existsSync(envResolved)) return envResolved;
    console.warn('GOOGLE_APPLICATION_CREDENTIALS apunta a ' + envResolved + ' pero no existe. Se intenta fallback local.');
  }

  const fallback = path.resolve(__dirname, 'serviceAccount.json');
  if (fs.existsSync(fallback)) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = fallback;
    return fallback;
  }
  return null;
}

const DISFRUTA_RE = /^disfrut[aá]\b/i;
const safeStr = (x) => (typeof x === 'string' ? x : (x ?? '')).trim();
const SHOULD_PRUNE = !KEEP_MISSING;

// --- categorías canónicas ---
function normalizeStr(s){ return (typeof s==='string'?s:'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim().toLowerCase(); }
function canonicalCategory(raw, title){
  const s = normalizeStr(raw), t = normalizeStr(title);
  if (s.includes('aloj')) return 'Alojamiento';
  if (s.includes('gastro')) return 'Gastronomía';
  if (s.includes('excurs') || s.includes('actividad')) return 'Excursiones y Actividades';
  if (s.includes('transp')) return 'Transporte';
  if (s.includes('retail')) return 'Retail';
  if (s.includes('comerc')) return 'Comercio';
  if (s.includes('deporte') || s.includes('gimnas')) return 'Deportes';
  if (s.includes('salud') || s.includes('educac') || s.includes('serv')) return 'Servicios';
  if (/(hotel|hosteri|hostería|apart|cabañ|departamento|hostel|resort|spa)/i.test(t)) return 'Alojamiento';
  if (/(resto|restaurant|parrilla|gastro|cervec|cocina|bar)/i.test(t)) return 'Gastronomía';
  if (/(termas|excurs|actividad|paseo|catamara|reserva|rafting|trekking|delta|ballena)/i.test(t)) return 'Excursiones y Actividades';
  if (/(micro|bus|chevalier|rutatl|crucero del norte|hertz|rent ?car|avion|a[eé]reo|cochera)/i.test(t)) return 'Transporte';
  if (/(megatlon|gimnas|gym|deporte)/i.test(t)) return 'Deportes';
  if (/(indumentaria|tienda|local|retail)/i.test(t)) return 'Retail';
  if (/(comercio)/i.test(t)) return 'Comercio';
  return 'Servicios';
}

function normalizeUrlForId(u) {
  try {
    const url = new URL(u);
    url.hostname = url.hostname.replace(/^www\./, '').toLowerCase();
    url.hash = '';
    url.search = '';
    let pathname = url.pathname.replace(/\/+$/, '') || '/';
    pathname = pathname.replace(/-\d+(?=\/|$)/, ''); // -2, -3, etc.
    url.pathname = pathname;
    return url.toString();
  } catch {
    return safeStr(u).toLowerCase().replace(/\/+$/, '').replace(/-\d+(?=\/|$)/, '');
  }
}

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

  // Excluir agregadores "Disfrutá ..."
  if (DISFRUTA_RE.test(titulo)) return null;

  // Fix puntual: Bagú Ushuaia → Tierra del Fuego si viniera mal como Nacional
  let provinciaFix = provincia;
  if (/bag[uú]\s+ushuaia/i.test(titulo) && (!provinciaFix || /nacional/i.test(provinciaFix))) {
    provinciaFix = 'Tierra del Fuego';
  }

  // Canonicalizamos la URL para ID y guardado
  const canonicalUrl = normalizeUrlForId(link);
  const imageUrl = imagen_url || null;
  const categoriaCan = canonicalCategory(categoria, titulo);

  return {
    // EN
    title: titulo,
    url: canonicalUrl,
    imageUrl,
    category: categoriaCan,
    province: provinciaFix || null,
    description: descripcion || null,

    // ES (compat)
    titulo,
    link: canonicalUrl,
    imagen_url: imageUrl,
    categoria: categoriaCan,
    provincia: provinciaFix || null,
    descripcion: descripcion || null,

    canonical: canonicalUrl,
    source: 'bulk-upload',
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function run() {
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
  const jsonPath = getJsonPathFromCli();
  const arr = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
  if (!Array.isArray(arr)) throw new Error('El JSON de entrada debe ser un array.');

  // Pre-dedupe por URL canónica (nos quedamos con el de "mejor puntaje")
  const bestByCanon = new Map();
  const score = (it) => (it.imageUrl ? 1 : 0) + (it.province && it.province !== 'Nacional' ? 1 : 0) + (it.category ? 1 : 0);

  for (const raw of arr) {
    const norm = normalizeItem(raw);
    if (!norm) continue; // filtrado de Disfrutá
    const key = norm.url || norm.link; // ya canónico
    const prev = bestByCanon.get(key);
    if (!prev || score(norm) > score(prev)) bestByCanon.set(key, norm);
  }

  const items = Array.from(bestByCanon.values());
  const canonicalSet = new Set(items.map(it => it.url || it.link).filter(Boolean));
  const toPrune = SHOULD_PRUNE ? await collectDocsToPrune(db, canonicalSet) : [];

  const BATCH_LIMIT = 400;
  let batch = db.batch();
  let pending = 0;
  let total = 0;

  console.log(`🔗 Proyecto: ${admin.app().options.projectId || '(desconocido)'} | Colección: ${COLLECTION} | DRY_RUN: ${DRY_RUN}`);
  console.log(`🧮 Ítems a procesar (tras filtro y de-dupe): ${items.length}`);

  for (const docData of items) {
    const docId = toIdFromLinkOrTitle(docData.url, docData.title);
    const ref = db.collection(COLLECTION).doc(docId);
    if (DRY_RUN) {
      console.log(`• [dry] set ${COLLECTION}/${docId}  (${docData.title})`);
      total++;
      continue;
    }
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

  if (!DRY_RUN && pending > 0) await batch.commit();

  if (DRY_RUN) console.log(`🧪 Dry-run completo. Documentos: ${total}`);
  else console.log(`✅ Listo: subidos/actualizados ${total} documentos a '${COLLECTION}'.`);

  if (!SHOULD_PRUNE) {
    console.log('⚠️  Prune deshabilitado (--keep-missing). Mantengo documentos antiguos.');
    return;
  }

  if (!toPrune.length) {
    console.log('ℹ️  Colección sincronizada: no hay documentos a borrar.');
    return;
  }

  if (DRY_RUN) {
    for (const doc of toPrune) {
      console.log(`🗑️  [dry] delete ${doc.ref.path} (${doc.title || doc.canonical})`);
    }
    console.log(`ℹ️  Dry-run: no se borraron ${toPrune.length} documentos.`);
    return;
  }

  for (let i = 0; i < toPrune.length; i += BATCH_LIMIT) {
    const chunk = toPrune.slice(i, i + BATCH_LIMIT);
    const delBatch = db.batch();
    for (const { ref } of chunk) delBatch.delete(ref);
    await delBatch.commit();
    console.log(`🗑️  Borrados ${Math.min(i + BATCH_LIMIT, toPrune.length)} / ${toPrune.length} documentos obsoletos.`);
  }
}

run().catch(err => {
  console.error('❌ Error en el upload:', err);
  process.exit(1);
});

async function collectDocsToPrune(db, canonicalSet) {
  if (!canonicalSet.size) return [];

  const snap = await db.collection(COLLECTION).get();
  const results = [];

  for (const doc of snap.docs) {
    const data = doc.data();
    const source = safeStr(data.source).toLowerCase();
    if (source && source !== 'bulk-upload') continue;
    if (!source) continue; // evitamos tocar documentos manuales

    const candidate = safeStr(data.canonical || data.url || data.link);
    if (!candidate || !/^https?:\/\//i.test(candidate)) continue;

    const canonicalKey = normalizeUrlForId(candidate);
    if (canonicalSet.has(canonicalKey)) continue;

    results.push({
      ref: doc.ref,
      canonical: canonicalKey,
      title: safeStr(data.title ?? data.titulo)
    });
  }

  return results;
}
