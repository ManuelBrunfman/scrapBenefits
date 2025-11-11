// PATH: scraper_beneficios_ocr.js
const puppeteer = require('puppeteer');
const fs = require('fs').promises;
const fetch = require('node-fetch');
const { createWorker } = require('tesseract.js');
const CITY_PROVINCE_MAP = require('./data/ciudades_provincias_map.json');

// =============================
// Diccionario ciudades/provincias
// =============================
const PROVINCIAS = [
  "Buenos Aires","Ciudad Autónoma de Buenos Aires","Catamarca","Chaco","Chubut","Córdoba","Corrientes","Entre Ríos",
  "Formosa","Jujuy","La Pampa","La Rioja","Mendoza","Misiones","Neuquén","Río Negro","Salta","San Juan",
  "San Luis","Santa Cruz","Santa Fe","Santiago del Estero","Tierra del Fuego","Tucumán"
];
const CIUDADES_EXTRA = {
  "carlos paz": "Córdoba",
  "villa carlos paz": "Córdoba",
  "san rafael": "Mendoza",
  "bariloche": "Río Negro",
  "ushuaia": "Tierra del Fuego",
  "puerto madryn": "Chubut",
  "merlo": "San Luis",
  "gualeguaychu": "Entre Ríos",
  "mar del plata": "Buenos Aires",
  "ciudad autonoma de buenos aires": "Ciudad Autónoma de Buenos Aires",
  "capital federal": "Ciudad Autónoma de Buenos Aires",
  "caba": "Ciudad Autónoma de Buenos Aires",
  "mdq": "Buenos Aires",
  "rosario": "Santa Fe",
  "posadas": "Misiones",
  "resistencia": "Chaco",
  "neuquen": "Neuquén",
  "trelew": "Chubut",
  "comodoro rivadavia": "Chubut",
  "bahia blanca": "Buenos Aires",
  "santa rosa": "La Pampa",
  "san martin de los andes": "Neuquén",
  "rio gallegos": "Santa Cruz",
  "baradero": "Buenos Aires",
  "concordia": "Entre Ríos"
};
const CITY_KEYS = Object.keys(CITY_PROVINCE_MAP)
  .filter(key => key.length >= 4 && key !== 'pla' && /[\s-]/.test(key))
  .sort((a, b) => b.length - a.length);

// =============================
// Helpers
// =============================
const normalize = (s='') => s
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g,'')
  .replace(/\s+/g,' ')
  .trim();
const DISFRUTA_RE = /^disfrut[aá]\b/;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Palabras clave aproximadas por categoría y mapeo desde taxonomías del sitio.
const SITE_CATEGORY_MAP = {
  'deportes': 'Deportes',
  'accion-social': 'Servicios',
  'obra-social': 'Servicios',
  'servicios': 'Servicios',
  'salud': 'Servicios',
  'educacion': 'Servicios',
  'capacitacion': 'Servicios',
  'balnearios': 'Alojamiento',
  'hoteles': 'Alojamiento',
  'hospedaje': 'Alojamiento',
  'turismo': 'Excursiones y Actividades',
  'excursiones': 'Excursiones y Actividades',
  'parques-recreativos': 'Excursiones y Actividades',
  'cultura': 'Excursiones y Actividades',
  'espectaculos': 'Excursiones y Actividades',
  'entretenimiento': 'Excursiones y Actividades',
  'restaurantes': 'Gastronomía',
  'gastronomia': 'Gastronomía',
  'bares': 'Gastronomía',
  'cafeterias': 'Gastronomía',
  'transporte': 'Transporte',
  'retail': 'Retail',
  'comercio': 'Comercio',
  'tiendas': 'Retail'
};

const CATEGORY_PRIORITY = {
  'Deportes': 8,
  'Transporte': 7,
  'Alojamiento': 6,
  'Gastronomía': 5,
  'Excursiones y Actividades': 4,
  'Retail': 3,
  'Comercio': 2,
  'Servicios': 1
};

const CATEGORY_KEYWORDS = {
  'Alojamiento': ['aloj', 'hotel', 'hostel', 'hosteri', 'posada', 'hosped', 'apart', 'caban', 'bungalow', 'resort', 'lodge', 'cottage'],
  'Gastronomía': ['gastro', 'restaur', 'resto', 'cervec', 'cafeter', 'cafe', 'pizzer', 'parrill', 'cocina', 'bodega', 'helader', 'pasteler', 'confiter', 'patio de comidas', 'sushi', 'trattor'],
  'Excursiones y Actividades': ['excurs', 'actividad', 'aventur', 'turismo', 'tour', 'paseo', 'rafting', 'cabalg', 'trek', 'sender', 'parque', 'museo', 'espectaculo', 'teatro', 'circuit', 'experienc', 'fiesta', 'snow', 'ski'],
  'Transporte': ['transp', 'pasaj', 'aereo', 'vuelo', 'aeroline', 'bus', 'micro', 'colectivo', 'taxi', 'remis', 'transfer', 'rent a car', 'rentacar', 'alquiler auto', 'alquiler de auto', 'nafta', 'combustib', 'peaje'],
  'Retail': ['retail', 'indument', 'tienda', 'local', 'moda', 'ropa', 'calzado', 'outlet', 'marroquin', 'accesor', 'bazaar', 'decorac', 'hogar', 'electrodom', 'mueb'],
  'Comercio': ['comerc', 'supermerc', 'almacen', 'kiosc', 'perfum', 'farmac', 'ferreter', 'vinotec', 'mercado', 'minimercado'],
  'Servicios': ['servic', 'asesor', 'salud', 'medic', 'odont', 'dent', 'estetic', 'bellez', 'spa', 'peluquer', 'segur', 'educac', 'colegi', 'universidad', 'idioma', 'internet', 'clinica', 'jubil', 'coaching', 'tecno', 'soporte'],
  'Deportes': ['deport', 'gimnas', 'gym', 'fitness', 'entren', 'yoga', 'pilates', 'crossfit', 'club', 'canch', 'padel', 'tenis', 'natac', 'running', 'futbol', 'megatlon', 'sportclub', 'newgym']
};
const ALPHANUM_RE = /[a-z0-9]/;

function includesWithBoundary(haystack, needle) {
  if (!needle) return false;
  let idx = haystack.indexOf(needle);
  while (idx !== -1) {
    const before = idx === 0 ? ' ' : haystack[idx - 1];
    const afterIndex = idx + needle.length;
    const after = afterIndex >= haystack.length ? ' ' : haystack[afterIndex];
    if (!ALPHANUM_RE.test(before) && !ALPHANUM_RE.test(after)) {
      return true;
    }
    idx = haystack.indexOf(needle, idx + 1);
  }
  return false;
}

function detectarProvincia(texto){
  const baseNorm = normalize(texto);
  if (!baseNorm) return null;
  const norm = baseNorm.replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();

  for (const [ciudad, prov] of Object.entries(CIUDADES_EXTRA)) {
    const ciudadNorm = normalize(ciudad);
    if (includesWithBoundary(norm, ciudadNorm)) return prov;
  }

  for (const prov of PROVINCIAS) {
    const provNorm = normalize(prov);
    if (includesWithBoundary(norm, provNorm)) return prov;
  }

  for (const city of CITY_KEYS) {
    if (!includesWithBoundary(norm, city)) continue;
    const provincias = CITY_PROVINCE_MAP[city];
    if (!provincias || !provincias.length) continue;
    if (provincias.length === 1) return provincias[0];
    const matchByProvince = provincias.find(prov => includesWithBoundary(norm, normalize(prov)));
    if (matchByProvince) return matchByProvince;
  }

  return null;
}

function categoriaDesdeDataset(raw = '') {
  const tokens = raw
    .split(/\s+/)
    .map(token => normalize(token.replace(/[,;]/g, '')))
    .filter(Boolean);

  if (!tokens.length) return null;

  for (const token of tokens) {
    const canonical = SITE_CATEGORY_MAP[token];
    if (canonical) {
      return canonical;
    }
  }

  return null;
}

function detectarCategoria(categoriasRaw, ...textos) {
  const datasetCategory = categoriaDesdeDataset(categoriasRaw);
  if (datasetCategory) return datasetCategory;

  const joined = textos.filter(Boolean).join(' ');
  const norm = normalize(joined);
  if (!norm) return 'desconocida';

  const normSimple = norm.replace(/[-_/]+/g, ' ');

  for (const [categoria, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
    for (const keyword of keywords) {
      const normalizedKeyword = normalize(keyword);
      if (!normalizedKeyword) continue;
      if (normSimple.includes(normalizedKeyword)) {
        return categoria;
      }
    }
  }

  // Heurísticas adicionales por marcas o contextos frecuentes
  if (/(megatlon|sportclub|sport club|biggim|onfit)/i.test(joined)) return 'Deportes';
  if (/(chevalier|via bariloche|flecha bus|andisur|ferrobus)/i.test(joined)) return 'Transporte';
  if (/(bagu\s+ushuaia|bagu\s+hotel)/i.test(joined)) return 'Alojamiento';
  if (/(jubil|asesor|planificar tu retiro)/i.test(norm)) return 'Servicios';

  return 'desconocida';
}

async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise(resolve => {
      let totalHeight = 0; const distance = 450;
      const timer = setInterval(() => {
        const scrollHeight = document.body.scrollHeight;
        window.scrollBy(0, distance); totalHeight += distance;
        if (totalHeight >= scrollHeight) { clearInterval(timer); resolve(); }
      }, 200);
    });
  });
}

async function scrapeList(page) {
  return await page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll('.portfolio-items .col.element'));
    return cards.map(card => {
      const linkEl = card.querySelector('a');
      const title = (card.querySelector('h3')?.textContent || '').trim();
      const link = linkEl?.href || linkEl?.getAttribute('href') || '';
      const imgEl = card.querySelector('img');
      const imgList = imgEl?.getAttribute('data-src') || imgEl?.getAttribute('data-lazy-src') || imgEl?.src || '';
      const categoriasRaw = card.getAttribute('data-project-cat') || '';
      return { titulo: title, link, imagenLista: imgList, categoriasRaw };
    });
  });
}

async function scrapeDetail(page, link) {
  await page.goto(link, { waitUntil: 'networkidle2', timeout: 45000 });
  await sleep(400);
  return await page.evaluate(() => {
    const mainText = (document.querySelector('article')?.innerText || document.body.innerText || '').trim();
    const title = (document.querySelector('h1')?.textContent || '').trim();
    const ogImage = document.querySelector('meta[property="og:image"]')?.getAttribute('content') || '';
    const firstImg = document.querySelector('article img, .entry-content img, main img');
    return { title, mainText, imagen_url: ogImage || (firstImg?.src || '') };
  });
}

async function ocrImage(url, worker) {
  try {
    const res = await fetch(url);
    const buffer = await res.buffer();
    const { data: { text } } = await worker.recognize(buffer);
    return text;
  } catch (err) {
    console.error("⚠️ Error OCR:", err.message);
    return '';
  }
}

// =============================
// RUN
// =============================
async function run() {
  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  const worker = await createWorker('spa'); // OCR en español

  try {
    console.log('🚀 Iniciando scraping...');
    await page.goto('https://labancaria.org/beneficios/', { waitUntil: 'networkidle2', timeout: 45000 });
    await autoScroll(page);

    const listRaw = await scrapeList(page);
    const list = listRaw.filter(it => !DISFRUTA_RE.test(normalize(it.titulo)));

    const out = [];

    for (let i = 0; i < list.length; i++) {
      const item = list[i];
      console.log(`\n➡️  [${i+1}/${list.length}] ${item.titulo}`);

      try {
        const detail = await scrapeDetail(page, item.link);
        const textoDetalle = `${detail.title || ''} ${detail.mainText || ''}`;
        const ocrSource = detail.imagen_url || item.imagenLista;
        let textoOCR = '';

        if (ocrSource) {
          textoOCR = await ocrImage(ocrSource, worker);
        }

        const provincia = detectarProvincia(`${textoDetalle} ${textoOCR}`) || "Nacional";

        const categoria = detectarCategoria(item.categoriasRaw, textoDetalle, textoOCR, item.titulo);

        const categoriaFuente = item.categoriasRaw?.trim();
        console.log(
          `   → Provincia: ${provincia} | Categoría: ${categoria}` +
          (categoriaFuente ? ` (tags: ${categoriaFuente})` : '')
        );
        if (textoOCR) {
          console.log(`     OCR extraído (${textoOCR.length} caracteres)`);
        }

        const record = {
          titulo: detail.title || item.titulo,
          link: item.link,
          imagen_url: detail.imagen_url || item.imagenLista,
          provincia,
          categoria,
          fecha_scraping: new Date().toISOString()
        };
        out.push(record);
      } catch (err) {
        console.error("⚠️ Error en item:", item.titulo, err.message);
      }
    }

    await fs.writeFile('beneficios_ocr.json', JSON.stringify(out, null, 2), 'utf8');
    console.log('\n✅ Guardado en beneficios_ocr.json');
  } catch (err) {
    console.error('❌ Error general:', err);
  } finally {
    await browser.close();
    await worker.terminate();
  }
}

run();
