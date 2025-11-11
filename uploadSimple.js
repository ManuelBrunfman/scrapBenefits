const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

function argFlag(name, def = undefined) {
  const found = process.argv.find(a => a.startsWith(`--${name}`));
  if (!found) return def;
  const [_, v] = found.split('=');
  return v === undefined ? true : v;
}

const DRY_RUN = !!argFlag('dry-run', false);
const COLLECTION = argFlag('collection', 'beneficios');

const safeStr = (value) => (typeof value === 'string' ? value : (value ?? '')).trim();

function getJsonPathFromCli() {
  const cliPath = process.argv[2];
  if (!cliPath) {
    console.error('Uso: node uploadSimple.js <archivo.json> [--collection=beneficios] [--dry-run]');
    process.exit(1);
  }
  const abs = path.isAbsolute(cliPath) ? cliPath : path.join(process.cwd(), cliPath);
  if (!fs.existsSync(abs)) {
    console.error(`No se encontro el archivo JSON: ${abs}`);
    process.exit(1);
  }
  return abs;
}

function normalizeUrlForId(u) {
  try {
    const url = new URL(u);
    url.hostname = url.hostname.replace(/^www\./, '').toLowerCase();
    url.hash = '';
    url.search = '';
    let pathname = url.pathname.replace(/\/+$/, '') || '/';
    pathname = pathname.replace(/-\d+(?=\/|$)/, '');
    url.pathname = pathname;
    return url.toString();
  } catch {
    return safeStr(u).toLowerCase().replace(/\/+$/, '').replace(/-\d+(?=\/|$)/, '');
  }
}

function toIdFromLinkOrTitle(link, title) {
  const baseRaw = safeStr(link) || safeStr(title);
  const base = baseRaw.startsWith('http') ? normalizeUrlForId(baseRaw) : baseRaw;
  const hash = crypto.createHash('sha1').update(base).digest('hex').slice(0, 16);
  const normalizedTitle = safeStr(title).normalize('NFKD');
  const slug = normalizedTitle
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '-').replace(/(^-|-$)/g, '')
    .toLowerCase().slice(0, 50) || 'beneficio';
  return `${slug}-${hash}`;
}

(async function main(){
  try {
    if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      console.error('Falta GOOGLE_APPLICATION_CREDENTIALS');
      process.exit(1);
    }
    if (!admin.apps.length) admin.initializeApp();

    const jsonPath = getJsonPathFromCli();
    const raw = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    if (!Array.isArray(raw)) {
      console.error('El JSON de entrada debe ser un array.');
      process.exit(1);
    }

    const db = admin.firestore();
    console.log(`Proyecto: ${admin.app().options.projectId || '(desconocido)'} | Coleccion: ${COLLECTION} | DRY_RUN: ${DRY_RUN}`);

    const items = raw.map(item => {
      const titulo = safeStr(item.titulo ?? item.title);
      const link = safeStr(item.link ?? item.url);
      if (!titulo || !link) {
        throw new Error(`Item invalido: falta titulo/title o link/url. Titulo="${titulo}" Link="${link}"`);
      }

      const imagen_url = safeStr(item.imagen_url ?? item.imageUrl) || null;
      const categoria = safeStr(item.categoria ?? item.category) || null;
      const provincia = safeStr(item.provincia ?? item.province) || null;
      const descripcion = safeStr(item.descripcion ?? item.description) || null;
      const fecha_scraping = safeStr(item.fecha_scraping ?? item.updatedAt ?? item.createdAt) || null;
      const confidence = typeof item.confidence === 'number' ? item.confidence : (parseFloat(item.confidence) || null);

      const canonicalUrl = normalizeUrlForId(link);
      const docId = toIdFromLinkOrTitle(canonicalUrl, titulo);

      return {
        docId,
        data: {
          title: titulo,
          url: canonicalUrl,
          imageUrl: imagen_url,
          category: categoria,
          province: provincia,
          description: descripcion,
          fecha_scraping,
          confidence,
          titulo,
          link: canonicalUrl,
          imagen_url,
          categoria,
          provincia,
          descripcion,
          canonical: canonicalUrl,
          source: 'manual-upload',
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        }
      };
    });

    let total = 0;
    const BATCH_LIMIT = 400;
    let batch = db.batch();

    for (const { docId, data } of items) {
      if (DRY_RUN) {
        console.log(`[dry] set ${COLLECTION}/${docId}`);
        total++;
        continue;
      }
      const ref = db.collection(COLLECTION).doc(docId);
      batch.set(ref, data, { merge: true });
      total++;
      if (total % BATCH_LIMIT === 0) {
        await batch.commit();
        console.log(`Subidos ${total} documentos...`);
        batch = db.batch();
      }
    }

    if (!DRY_RUN && total % BATCH_LIMIT !== 0) {
      await batch.commit();
    }

    if (DRY_RUN) console.log(`Dry-run completo. Documentos a subir: ${total}`);
    else console.log(`Listo: subidos/actualizados ${total} documentos en '${COLLECTION}'.`);
  } catch (err) {
    console.error('Error durante el upload:', err);
    process.exit(1);
  }
})();
