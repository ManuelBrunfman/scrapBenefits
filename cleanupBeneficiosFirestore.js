// PATH: scripts/cleanupBeneficiosFirestore.js
// Limpieza en Firestore:
//  1) Borra docs con títulos "Disfrutá ..." (agregadores).
//  2) De-dupe por URL canónica (quita "www", "#", utm_*, barra final, y sufijo "-2").
//     Conserva el doc con mejor puntaje (tiene imagen, provincia específica, categoría).
//  3) Fix puntual: títulos que contengan "Bagú Ushuaia" quedan con provincia = "Tierra del Fuego" si venían en "Nacional".
// Uso:
//   export GOOGLE_APPLICATION_CREDENTIALS="D:/firebase/serviceAccount.json"
//   node scripts/cleanupBeneficiosFirestore.js --collection=beneficios --dry-run

const admin = require('firebase-admin');

const fs = require('fs');
const path = require('path');

function resolveServiceAccountPath(){
  const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (envPath){
    const candidate = path.isAbsolute(envPath) ? envPath : path.resolve(__dirname, envPath);
    if (fs.existsSync(candidate)) return candidate;
    console.warn(`GOOGLE_APPLICATION_CREDENTIALS apunta a ${candidate} pero no existe. Se intenta fallback local.`);
  }
  const fallback = path.resolve(__dirname, 'serviceAccount.json');
  if (fs.existsSync(fallback)){
    process.env.GOOGLE_APPLICATION_CREDENTIALS = fallback;
    return fallback;
  }
  return null;
}
function argFlag(name, def = undefined) {
  const found = process.argv.find(a => a.startsWith(`--${name}`));
  if (!found) return def;
  const [_, v] = found.split('=');
  return v === undefined ? true : v;
}

const DRY_RUN = !!argFlag('dry-run', false);
const COLLECTION = argFlag('collection', 'beneficios');

const DISFRUTA_RE = /^disfrut[aá]\b/i;
const safeStr = (x) => (typeof x === 'string' ? x : (x ?? '')).trim();

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hostname = url.hostname.replace(/^www\./, '').toLowerCase();
    url.hash = '';
    for (const [k] of url.searchParams) {
      if (/^(utm_|fbclid$|gclid$|mc_cid$|mc_eid$)/i.test(k)) url.searchParams.delete(k);
    }
    url.pathname = url.pathname.replace(/\/+$/, '') || '/';
    url.pathname = url.pathname.replace(/-2(?=\/|$)/, '');
    return url.toString();
  } catch {
    return safeStr(u).toLowerCase().replace(/\/+$/, '').replace(/-2(?=\/|$)/, '');
  }
}

const score = (it) => (it.imageUrl ? 1 : 0) + (it.province && it.province !== 'Nacional' ? 1 : 0) + (it.category ? 1 : 0);

(async function main() {
  try {
    const credentialsPath = resolveServiceAccountPath();
    if (!credentialsPath){
      console.error('❌ No se encontro el archivo de credenciales (serviceAccount.json).');
      process.exit(1);
    }
    if (!admin.apps.length){
      const serviceAccount = JSON.parse(fs.readFileSync(credentialsPath, 'utf8'));
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: serviceAccount.project_id,
        databaseURL: serviceAccount.project_id ? `https://${serviceAccount.project_id}.firebaseio.com` : undefined
      });
    }
    const db = admin.firestore();
    console.log(`🔗 Proyecto: ${admin.app().options.projectId || '(desconocido)'} | Colección: ${COLLECTION} | DRY_RUN: ${DRY_RUN}`);

    const snap = await db.collection(COLLECTION).get();
    console.log(`📥 Leídos: ${snap.size}`);

    const byCanon = new Map();
    const toDelete = [];
    const toUpdate = [];

    for (const d of snap.docs) {
      const data = d.data();
      const title = safeStr(data.title ?? data.titulo);
      const url = safeStr(data.url ?? data.link);
      const imageUrl = safeStr(data.imageUrl ?? data.imagen_url);
      const category = safeStr(data.category ?? data.categoria);
      let province = safeStr(data.province ?? data.provincia);

      // 1) Borrado de agregadores "Disfrutá ..."
      if (DISFRUTA_RE.test(title)) {
        toDelete.push(d.ref);
        continue;
      }

      // 3) Fix Bagú Ushuaia
      if (/bag[uú]\s+ushuaia/i.test(title) && (!province || /nacional/i.test(province))) {
        province = 'Tierra del Fuego';
        toUpdate.push({ ref: d.ref, patch: { province, provincia: province } });
      }

      const canon = normalizeUrl(url);
      const cur = { id: d.id, ref: d.ref, title, url: canon, imageUrl: imageUrl || null, category: category || null, province: province || null };
      const prev = byCanon.get(canon);
      if (!prev || score(cur) > score(prev)) byCanon.set(canon, cur);
    }

    // 2) De-dupe: cualquier doc que no sea el "mejor" para su URL canónica → borrar
    const keepIds = new Set(Array.from(byCanon.values()).map(v => v.id));
    for (const d of snap.docs) {
      if (!keepIds.has(d.id) && !toDelete.find(r => r.path === d.ref.path)) {
        toDelete.push(d.ref);
      }
    }

    console.log(`🧹 A borrar: ${toDelete.length} | A actualizar: ${toUpdate.length}`);

    if (DRY_RUN) {
      for (const r of toDelete) console.log('[dry] delete', r.path);
      for (const u of toUpdate) console.log('[dry] update', u.ref.path, u.patch);
      console.log('🧪 Dry-run: sin cambios en Firestore');
      return;
    }

    // Ejecutar en lotes
    const BATCH_LIMIT = 400;

    // Updates
    for (let i = 0; i < toUpdate.length; i += BATCH_LIMIT) {
      const batch = db.batch();
      for (const { ref, patch } of toUpdate.slice(i, i + BATCH_LIMIT)) batch.set(ref, patch, { merge: true });
      await batch.commit();
      console.log(`✔ Provincias corregidas: ${Math.min(i + BATCH_LIMIT, toUpdate.length)} / ${toUpdate.length}`);
    }

    // Deletes
    for (let i = 0; i < toDelete.length; i += BATCH_LIMIT) {
      const batch = db.batch();
      for (const ref of toDelete.slice(i, i + BATCH_LIMIT)) batch.delete(ref);
      await batch.commit();
      console.log(`✔ Borrados: ${Math.min(i + BATCH_LIMIT, toDelete.length)} / ${toDelete.length}`);
    }

    console.log('✅ Limpieza terminada.');
  } catch (err) {
    console.error('❌ Error en cleanup:', err);
    process.exit(1);
  }
})();
