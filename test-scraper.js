// scraper_beneficios_mejorado.js (corregido)
// Scraper de beneficios con:
//  - Scoring mejorado por keywords/marcas/estructura
//  - OCR seguro (descarga imagen en PESTAÑA NUEVA)
//  - Fallbacks: "Categoría desconocida" / "Provincia desconocida"
//  - Sin LOCATION_PATTERNS (se había usado sin definir)
//  - Regex y normalización unificadas
//
// Uso:
//   npm i puppeteer tesseract.js
//   node scraper_beneficios_mejorado.js            # sin OCR (rápido)
//   node scraper_beneficios_mejorado.js --ocr      # con OCR condicional
//   node scraper_beneficios_mejorado.js --ocr --max=40

const puppeteer = require('puppeteer');
const fs = require('fs').promises;

// =============================
// CLI
// =============================
const args = process.argv.slice(2);
const USE_OCR = args.includes('--ocr');
const MAX_ITEMS = (() => {
  const m = args.find(a => a.startsWith('--max='));
  return m ? parseInt(m.split('=')[1], 10) : Infinity;
})();

let Tesseract = null;
if (USE_OCR) {
  try { Tesseract = require('tesseract.js'); }
  catch (e) {
    console.error('❌ Falta tesseract.js. Instalá con: npm i tesseract.js');
    process.exit(1);
  }
}

// =============================
// CONFIG
// =============================
const CATEGORY_ORDER = [
  'Alojamiento',
  'Excursiones y Actividades',
  'Transporte',
  'Gastronomía',
  'Retail / Comercios',
  'Deportes y Gimnasios',
  'Salud',
  'Educación',
  'Servicios',
  'Categoría desconocida'
];

const CAT_KW = {
  'Alojamiento': [
    'alojamiento','hospedaje','estadia','estadía','descanso',
    'hotel','hosteria','hostería','posada','cabaña','cabañas','casa de campo',
    'departamento','departamentos','apart','apart hotel','apart-hotel','duplex','dúplex',
    'bungalow','resort','spa','spa & resort','lodge','boutique',
    'habitacion','habitaciones','suite','deluxe','standard','superior',
    'noche','noches','pension','pensión','media pension','pensión completa','all inclusive',
    'check in','check-in','check out','check-out',
    'reserva','reservas','disponibilidad','tarifa','tarifas',
    'complejo turistico','complejo turístico','complejo','tower','class'
  ],
  'Excursiones y Actividades': [
    'excursion','excursión','excursiones','tour','paseo','itinerario','visita','entrada','ticket',
    'parque','termas','avistaje','catamaran','catamarán','fluvial','museo','circuito','trekking',
    'aventura','turismo','viaje de bodas','luna de miel','honeymoon'
  ],
  'Transporte': [
    'transfer','traslado','remis','taxi','alquiler de auto','alquiler auto','rent a car','rentacar','rent car',
    'pasaje','micro','bus','omnibus','ómnibus','aeropuerto','terminal',
    'hertz','chevalier','crucero del norte','rutatlantica','rutatlántica'
  ],
  'Gastronomía': [
    'restaurant','restaurante','parrilla','resto bar',
    'cafe','café','cerveceria','cervecería','pizzeria','pizzería',
    'almuerzo','cena','desayuno','comida','platos','cocina',
    'gastronomia','gastronomía','buffet','confiteria','confitería'
  ],
  'Retail / Comercios': [
    'tienda','local','indumentaria','calzado','boutique','outlet','descuento',
    'artesania','artesanía','compras','shopping','ropa','zapatos','accesorios'
  ],
  'Deportes y Gimnasios': [
    'gimnasio','gym','fitness','entrenamiento','natacion','natación','pilates','yoga',
    'megatlon','sport','deporte','crossfit','spinning'
  ],
  'Salud': [
    'obra social','clinica','clínica','sanatorio','odontologia','odontología','farmacia',
    'optica','óptica','laboratorio','medico','médico','hospital','salud'
  ],
  'Educación': [
    'curso','taller','capacitacion','capacitación','instituto','universidad','idioma',
    'colegio','escuela','formacion','formación','educacion','educación'
  ],
  'Servicios': [
    'jubilacion','jubilación','jubilarte','jubilado','jubilada','pension','pensión',
    'reafiliate','reafiliación','afiliación','afiliado','afiliada',
    'sepelio','funeral','cobertura','seguro','seguros','aseguradora',
    'asesoramiento','asesorar','consultoría','tramite','trámite','gestión',
    'beneficio social','servicio social','prestación','asistencia'
  ]
};

// Evitar falsos positivos al leer el detalle
const TEMPLATE_BLACKLIST = ['menu','menú']; // (sacamos 'bar' para no bloquear Gastronomía real)

// Marcas (boost fuerte)
const BRAND_MAP = [
  { re: /\bmegatlon\b/i, cat: 'Deportes y Gimnasios' },
  { re: /\bhertz\b/i, cat: 'Transporte' },
  { re: /\bchevalier\b|\bcrucero del norte\b|\brutatl[áa]ntica\b/i, cat: 'Transporte' },
  { re: /\burbana\s*class\b/i, cat: 'Alojamiento' }, // typo fix
  { re: /\bpremium\s*tower\b/i, cat: 'Alojamiento' },
  { re: /\bhoward\s*johnson\b/i, cat: 'Alojamiento' },
  { re: /\bbag[uú]\b/i, cat: 'Alojamiento' },
  { re: /\bs[ií]\s*turismo\b/i, cat: 'Excursiones y Actividades' },
  { re: /\bel\s*surco\b/i, cat: 'Servicios' } // Aseguradora
];

const PROVINCIAS = [
  'Buenos Aires','CABA','Catamarca','Chaco','Chubut','Córdoba','Corrientes','Entre Ríos','Formosa','Jujuy',
  'La Pampa','La Rioja','Mendoza','Misiones','Neuquén','Río Negro','Salta','San Juan','San Luis','Santa Cruz',
  'Santa Fe','Santiago del Estero','Tierra del Fuego','Tucumán',
  'Nacional',
  'Provincia desconocida'
];

// Mapa ciudad → provincia ampliado
const CITY_TO_PROV = {
  'san lorenzo': 'Salta',
  'lago puelo': 'Chubut',
  'bariloche': 'Río Negro',
  'san carlos de bariloche': 'Río Negro',
  'puerto madryn': 'Chubut',
  'trelew': 'Chubut',
  'esquel': 'Chubut',
  'ushuaia': 'Tierra del Fuego',
  'el calafate': 'Santa Cruz',
  'el chalten': 'Santa Cruz',
  'el chaltén': 'Santa Cruz',
  'merlo': 'San Luis',
  'villa carlos paz': 'Córdoba',
  'san rafael': 'Mendoza',
  'mendoza': 'Mendoza',
  'tigre': 'Buenos Aires',
  'mar del plata': 'Buenos Aires',
  'valeria del mar': 'Buenos Aires',
  'villa la angostura': 'Neuquén',
  'neuquen': 'Neuquén',
  'neuquén': 'Neuquén',
  'iguazu': 'Misiones',
  'iguazú': 'Misiones',
  'rosario': 'Santa Fe',
  'resistencia': 'Chaco',
  'corrientes': 'Corrientes',
  'concepcion del uruguay': 'Entre Ríos',
  'concepción del uruguay': 'Entre Ríos',
  'san jose': 'Entre Ríos',
  'san josé': 'Entre Ríos',
  'exaltacion de la cruz': 'Buenos Aires',
  'exaltación de la cruz': 'Buenos Aires',
  'potrero de garay': 'Córdoba',
  'villa general belgrano': 'Córdoba',
  'dina huapi': 'Río Negro',
  'puerto iguazu':'Misiones',
  'puerto iguazú':'Misiones',
  'la plata':'Buenos Aires',
  'tandil':'Buenos Aires',
  'sierra de la ventana':'Buenos Aires',
  'carilo':'Buenos Aires',
  'cariló':'Buenos Aires',
  'pinamar':'Buenos Aires',
  'monte hermoso':'Buenos Aires',
  'necochea':'Buenos Aires',
  'miramar':'Buenos Aires',
  'san martin de los andes':'Neuquén',
  'san martín de los andes':'Neuquén',
  'junin de los andes':'Neuquén',
  'junín de los andes':'Neuquén',
  'villa traful':'Neuquén',
  'villa gesell': 'Buenos Aires',
  'carlos paz': 'Córdoba',
  'río hondo': 'Santiago del Estero',
  'tilcara': 'Jujuy',
  'villa unión': 'La Rioja',
  'foz de iguazu': 'Brasil',
  'foz de iguazú': 'Brasil'
};

// =============================
// HELPERS
// =============================
const normalize = (s='') => s
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

const norm = (s='') => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
const esc  = (s='') => s.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');

function addScore(scores, reasons, cat, pts, why) {
  scores[cat] = (scores[cat] || 0) + pts;
  reasons.push(`[+${pts}] ${cat}: ${why}`);
}

function scoreByKeywords(text, where, weight, scores, reasons) {
  const base = normalize(text);
  for (const cat of CATEGORY_ORDER) {
    const kws = CAT_KW[cat] || [];
    for (let kw of kws) {
      if (where === 'detalle' && TEMPLATE_BLACKLIST.includes(normalize(kw))) continue;
      const pat = normalize(kw).replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&').replace(/\s+/g, '\\s*');
      const re = new RegExp(`(^|[^a-z0-9])${pat}([^a-z0-9]|$)`, 'i');
      if (re.test(base)) addScore(scores, reasons, cat, weight, `${where}: "${kw}"`);
    }
  }
}

function scoreByBrands(text, scores, reasons, weight=4) {
  for (const { re, cat } of BRAND_MAP) {
    if (re.test(text)) addScore(scores, reasons, cat, weight, 'marca/entidad');
  }
}

function detectCategoryFromStructure(title) {
  const t = normalize(title);
  if (/(hosteria|hostería|hotel|apart|cabañas?|posada)\b/i.test(t)) return { category: 'Alojamiento', confidence: 0.9 };
  if (/(restaurant|restaurante|parrilla|resto\s*bar)\b/i.test(t)) return { category: 'Gastronomía', confidence: 0.9 };
  if (/(jubil|pension|reafiliat|sepelio|funeral|seguro)/i.test(t)) return { category: 'Servicios', confidence: 0.85 };
  if (/viaje\s*de\s*bodas|luna\s*de\s*miel|honeymoon/i.test(t)) return { category: 'Excursiones y Actividades', confidence: 0.9 };
  if (/obra\s*social|beneficio\s*social/i.test(t)) return { category: 'Salud', confidence: 0.85 };
  if (/\b\d+%\s*(de\s*)?(descuento|off|dto)\b/i.test(t) && /(hotel|alojamiento|estadia|estad[íi]a)/i.test(t)) {
    return { category: 'Alojamiento', confidence: 0.7 };
  }
  return null;
}

function packageRule(title, detail, scores, reasons) {
  const t = normalize(title);
  const d = normalize(detail);
  if (/(paquete|combo|promo|full|express|escapada)/i.test(t)) {
    const hasNights = /\b(\d+)\s*(noche|noches|dia|dias|d[íi]as)\b/.test(d) || /\bnoches?\b/.test(d);
    const hasLodging = /(hotel|hosteria|hoster[íi]a|cabaña|cabañas|alojamiento|apart)/i.test(d);
    const hasTours = /(excursion|excursiones|tour|visita|entrada|paseo|itinerario)/i.test(d);
    const hasTransfer = /(traslado|transfer|aeropuerto)/i.test(d);
    if (hasNights || hasLodging) addScore(scores, reasons, 'Alojamiento', 3, 'paquete: alojamiento/noches');
    if (hasTours) addScore(scores, reasons, 'Excursiones y Actividades', 2.5, 'paquete: tours/entradas');
    if (hasTransfer) addScore(scores, reasons, 'Transporte', 1.5, 'paquete: traslados');
  }
}

function bestCategory(scores) {
  const entries = Object.entries(scores);
  if (!entries.length) return { cat: 'Categoría desconocida', top: 0, second: 0 };
  entries.sort((a,b) => b[1]-a[1] || CATEGORY_ORDER.indexOf(a[0]) - CATEGORY_ORDER.indexOf(b[0]));
  const [cat, top] = entries[0];
  const second = entries[1]?.[1] || 0;
  return { cat: cat || 'Categoría desconocida', top, second };
}

function computeConfidence(top, second) {
  const denom = Math.max(1, top + second);
  const c = (top - second) / denom;
  return Math.max(0, Math.min(1, +c.toFixed(3)));
}

function normalizeLink(href) {
  try {
    const u = new URL(href);
    let pathname = u.pathname.replace(/\/$/, '');
    pathname = pathname.replace(/-\d+$/, '');
    u.pathname = pathname; u.hash = ''; u.search = '';
    return u.toString();
  } catch { return href; }
}

function parseProvinceFromTitle(title) {
  const t = normalize(title);
  for (const prov of PROVINCIAS) {
    if (prov === 'Provincia desconocida') continue;
    const re = new RegExp(`(^|[^a-z])${normalize(prov)}([^a-z]|$)`, 'i');
    if (re.test(t)) return prov;
  }
  for (const [city, prov] of Object.entries(CITY_TO_PROV)) {
    const re = new RegExp(`(^|[^a-z])${city}([^a-z]|$)`, 'i');
    if (re.test(t)) return prov;
  }
  return null;
}

function enhancedTitleParsing(title) {
  const p1 = /^(.+?)\.\s*([^-]+)\s*-\s*(.+)$/i; // "Nombre. Ciudad-Provincia"
  const m1 = title.match(p1);
  if (m1) {
    const [, name, city, province] = m1;
    return { businessName: name.trim(), city: city.trim(), province: province.trim() };
  }
  const p2 = /^(.+?)\.\s*(.+?),\s*(.+)$/i; // "Nombre. Ciudad, Provincia"
  const m2 = title.match(p2);
  if (m2) {
    const [, name, city, province] = m2;
    return { businessName: name.trim(), city: city.trim(), province: province.trim() };
  }
  return null;
}

function detectProvinceSmart({ title, text, url }) {
  // 1) Título (patrones/menções)
  const parsed = enhancedTitleParsing(title || '');
  if (parsed?.province) {
    const p = parseProvinceFromTitle(parsed.province) || parsed.province;
    if (p && PROVINCIAS.includes(p)) return p;
  }
  const fromTitle = parseProvinceFromTitle(title || '');
  if (fromTitle) return fromTitle;

  // 2) Texto completo (normalizado)
  const t = norm(text || '');
  if (/\bnacional\b|\btodo el pais\b|\btoda la argentina\b/.test(t)) return 'Nacional';

  for (const prov of PROVINCIAS) {
    if (prov === 'Provincia desconocida' || prov === 'Nacional') continue;
    const re = new RegExp(`\\b${esc(norm(prov))}\\b`, 'i');
    if (re.test(t)) return prov;
  }
  for (const [city, prov] of Object.entries(CITY_TO_PROV)) {
    const re = new RegExp(`\\b${esc(city)}\\b`, 'i');
    if (re.test(norm(text || ''))) return prov;
  }

  // 3) URL
  const slug = norm(url || '');
  for (const prov of PROVINCIAS) {
    if (prov === 'Provincia desconocida') continue;
    if (slug.includes(norm(prov))) return prov;
  }
  for (const [city, prov] of Object.entries(CITY_TO_PROV)) {
    if (slug.includes(city)) return prov;
  }

  // 4) fallback
  return 'Provincia desconocida';
}

// =============================
// SCRAPING
// =============================
async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise(resolve => {
      let total = 0; const step = 600;
      const timer = setInterval(() => {
        window.scrollBy(0, step); total += step;
        if (total >= document.body.scrollHeight) { clearInterval(timer); resolve(); }
      }, 200);
    });
  });
}

async function scrapeList(page) {
  await autoScroll(page);
  const items = await page.evaluate(() => {
    const out = []; const links = document.querySelectorAll('a[href*="/beneficios/"]');
    const seen = new Set();
    links.forEach(link => {
      const href = link.href;
      if (!href || !href.includes('/beneficios/') || href.endsWith('/beneficios/')) return;
      if (seen.has(href)) return; seen.add(href);
      const container = link.closest('article') || link.closest('.elementor-post') || link.closest('div[class*="beneficio"]') || link.parentElement;
      const titleEl = container?.querySelector('h2, h3, h4, .elementor-post__title');
      const titulo = (titleEl?.textContent || link.textContent || '').trim();
      const descEl = container?.querySelector('.elementor-post__excerpt, p');
      const descripcion = (descEl?.textContent || '').trim();
      const imgEl = container?.querySelector('img');
      const imagenLista = imgEl ? (imgEl.src || imgEl.getAttribute('data-src') || '') : '';
      out.push({ titulo, link: href, descripcion, imagenLista });
    });
    return out;
  });
  return items;
}

async function scrapeDetail(page, url) {
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
  return await page.evaluate(() => {
    // Limpiar navegación
    const removeElements = [
      'header', 'nav', '.menu', '.navbar', '.navigation', '.nav',
      'footer', '.footer', '.copyright',
      '.sidebar', '.widget', '.breadcrumb', '.breadcrumbs',
      '.share', '.social', '.related', '.comments'
    ];
    const bodyClone = document.body.cloneNode(true);
    removeElements.forEach(selector => {
      bodyClone.querySelectorAll(selector).forEach(el => el.remove());
    });

    const contentSelectors = [
      '.entry-content', '.post-content', 'article .content', 'main article', '[role="main"]', '.beneficio-detalle', 'article'
    ];

    let mainText = '';
    for (const selector of contentSelectors) {
      const element = bodyClone.querySelector(selector);
      if (element) { mainText = element.innerText || element.textContent || ''; break; }
    }
    if (!mainText) mainText = bodyClone.innerText || bodyClone.textContent || '';

    const title = (document.querySelector('h1')?.textContent || '').trim();
    const ogImage = document.querySelector('meta[property="og:image"]')?.getAttribute('content') || '';

    const imgs = Array.from(document.querySelectorAll('article img, .entry-content img, main img')).map(img => ({
      src: img.getAttribute('src') || img.getAttribute('data-src') || '',
      alt: img.getAttribute('alt') || '',
      w: (img.naturalWidth || 0),
      h: (img.naturalHeight || 0)
    })).filter(x => x.src);

    const captions = Array.from(document.querySelectorAll('figure figcaption, .wp-caption-text'))
      .map(el => el.textContent?.trim() || '')
      .filter(Boolean)
      .join(' ');

    const tags = Array.from(document.querySelectorAll('a[rel="tag"], .post-categories a'))
      .map(el => el.textContent?.trim() || '')
      .filter(Boolean)
      .join(' ');

    const metaDesc = document.querySelector('meta[name="description"]')?.getAttribute('content') || '';

    return { title, mainText, ogImage, images: imgs, captions, tags, metaDesc };
  });
}

// OCR: descargar imagen en PESTAÑA NUEVA (no romper la actual)
async function fetchBuffer(browser, src) {
  let p;
  try {
    p = await browser.newPage();
    const res = await p.goto(src, { timeout: 30000, waitUntil: 'networkidle2' });
    if (!res) return null;
    const buf = await res.buffer();
    return buf;
  } catch {
    return null;
  } finally {
    if (p) await p.close();
  }
}

function pickOcrCandidates(images, limit=2) {
  const sorted = [...images].sort((a,b) => (b.w*b.h) - (a.w*a.h));
  return sorted.slice(0, limit);
}

// =============================
// CLASIFICACIÓN
// =============================
function classify({ title, detailText, slug, images, ocrText }) {
  const scores = {}; const reasons = [];
  const titleNorm = normalize(title);
  const detailNorm = normalize(detailText);
  const slugNorm = normalize(slug);
  const imagesAlt = normalize(images.map(x => x.alt).join(' '));
  const imagesNames = normalize(images.map(x => (x.src || '').split('/').pop().split('?')[0]).join(' '));
  const ocrNorm = normalize(ocrText || '');

  const struct = detectCategoryFromStructure(title);
  if (struct) addScore(scores, reasons, struct.category, 5, 'estructura del título');

  scoreByKeywords(titleNorm, 'título', 3, scores, reasons);
  scoreByKeywords(detailNorm, 'detalle', 1.5, scores, reasons);
  scoreByKeywords(imagesAlt, 'imagen.alt', 1.5, scores, reasons);
  scoreByKeywords(imagesNames, 'imagen.filename', 1.5, scores, reasons);
  scoreByKeywords(slugNorm, 'url', 1.5, scores, reasons);

  scoreByBrands(`${title} ${detailText} ${slug}`, scores, reasons, 4);
  packageRule(title, detailText, scores, reasons);
  if (ocrNorm) scoreByKeywords(ocrNorm, 'ocr', 2.5, scores, reasons);

  const { cat, top, second } = bestCategory(scores);
  const confidence = computeConfidence(top, second);
  return { categoria: cat || 'Categoría desconocida', confidence, reasons, rawScores: scores };
}

function validateAndEnrichResult(item) {
  const corrections = [
    { pattern: /jubil|retir|pension|reafiliat/i, wrongCats: ['Gastronomía', 'Transporte'], rightCat: 'Servicios' },
    { pattern: /sepelio|funeral|entierro|deceso/i, wrongCats: ['Gastronomía', 'Alojamiento'], rightCat: 'Servicios' },
    { pattern: /obra\s*social|beneficio\s*social/i, wrongCats: ['Gastronomía', 'Transporte'], rightCat: 'Salud' },
    { pattern: /viaje\s*de\s*bodas|luna\s*de\s*miel|honeymoon|casamiento/i, wrongCats: ['Gastronomía', 'Excursiones y Actividades'], rightCat: 'Servicios' },
    { pattern: /seguro|asegurad|cobertura\s*de/i, wrongCats: ['Gastronomía', 'Alojamiento'], rightCat: 'Servicios' },
    { pattern: /hosteria|hostería/i, wrongCats: ['Gastronomía', 'Transporte', 'Servicios'], rightCat: 'Alojamiento' },
    { pattern: /hotel\b/i, wrongCats: ['Gastronomía', 'Transporte', 'Servicios'], rightCat: 'Alojamiento' },
    { pattern: /cabañas?/i, wrongCats: ['Gastronomía', 'Transporte', 'Servicios'], rightCat: 'Alojamiento' },
    { pattern: /termas?\b/i, wrongCats: ['Gastronomía', 'Alojamiento'], rightCat: 'Excursiones y Actividades' }
  ];

  for (const rule of corrections) {
    if (rule.pattern.test(item.titulo) && rule.wrongCats.includes(item.categoria)) {
      const prev = item.categoria;
      item.categoria = rule.rightCat;
      item.confidence = Math.max(0.6, item.confidence);
      item.reasons.push(`[CORREGIDO] De ${prev} a ${rule.rightCat} por validación`);
    }
  }

  // Validación de provincia (no forzamos cambio, solo avisamos)
  const titleProvince = parseProvinceFromTitle(item.titulo);
  if (titleProvince && titleProvince !== item.provincia && item.provincia !== 'Provincia desconocida') {
    console.warn(`⚠️ Provincia inconsistente: título sugiere ${titleProvince} pero se detectó ${item.provincia}`);
  }
  return item;
}

// =============================
// MAIN
// =============================
async function autoPause(ms=350){ return new Promise(r => setTimeout(r, ms)); }

async function run() {
  console.log('🚀 Iniciando scraping' + (USE_OCR ? ' + OCR' : '') + '...');

  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    defaultViewport: { width: 1366, height: 768 }
  });

  const page = await browser.newPage();
  try {
    await page.goto('https://labancaria.org/beneficios/', { waitUntil: 'networkidle2', timeout: 45000 });
    await autoScroll(page);

    const listRaw = await scrapeList(page);
    const list = listRaw.map(it => ({ ...it, link: normalizeLink(it.link) }))
                        .filter((_, i) => i < MAX_ITEMS);

    console.log(`🔗 Procesando ${list.length} beneficios${Number.isFinite(MAX_ITEMS) ? ` (límite ${MAX_ITEMS})` : ''}.`);

    const out = [];

    for (let i = 0; i < list.length; i++) {
      const item = list[i];
      console.log(`\n➡️  [${i + 1}/${list.length}] ${item.titulo}`);
      try {
        const detail = await scrapeDetail(page, item.link);

        let ocrText = '';

        // 1) Clasificación preliminar (sin OCR)
        const preview = classify({
          title: detail.title || item.titulo,
          detailText: detail.mainText,
          slug: item.link,
          images: detail.images,
          ocrText: ''
        });

        // 2) Provincia preliminar
        let provincia = detectProvinceSmart({
          title: detail.title || item.titulo,
          text: `${item.titulo} ${item.descripcion} ${detail.title} ${detail.mainText} ${detail.tags} ${detail.captions}`,
          url: item.link
        });

        // 3) OCR condicional (si categoría desconocida/Otros o provincia desconocida/Nacional)
        if (USE_OCR) {
          const catIsWeak = preview.categoria === 'Categoría desconocida' || preview.categoria === 'Otros' || preview.confidence < 0.3;
          const provIsWeak = !provincia || provincia === 'Provincia desconocida' || provincia === 'Nacional';

          if (catIsWeak || provIsWeak) {
            console.log(`   🔍 OCR necesario: cat=${preview.categoria} (conf=${preview.confidence}) | prov=${provincia}`);
            const candidates = pickOcrCandidates(detail.images, 2);

            for (const img of candidates) {
              try {
                const buf = await fetchBuffer(browser, img.src);
                if (!buf) continue;
                const res = await Tesseract.recognize(buf, 'spa+eng');
                const txt = res?.data?.text || '';
                if (txt) {
                  ocrText += ' ' + txt;

                  // Provincia vía OCR
                  if (provIsWeak) {
                    const txtN = norm(txt);
                    let found = false;
                    for (const prov of PROVINCIAS) {
                      if (prov === 'Provincia desconocida') continue;
                      const re = new RegExp(`\\b${esc(norm(prov))}\\b`, 'i');
                      if (re.test(txtN)) { provincia = prov; found = true; break; }
                    }
                    if (!found) {
                      for (const [city, prov] of Object.entries(CITY_TO_PROV)) {
                        const re = new RegExp(`\\b${esc(city)}\\b`, 'i');
                        if (re.test(txtN)) { provincia = prov; break; }
                      }
                    }
                    if (provincia) console.log(`   📍 Provincia via OCR: ${provincia}`);
                  }
                }
              } catch (e) {
                console.warn('   ⚠️ OCR error:', e.message);
              }
            }
          } else {
            console.log(`   ✓ Sin OCR: categoría ${preview.categoria} (conf ${preview.confidence}) | provincia ${provincia}`);
          }
        }

        // 4) Clasificación final (con OCR si hubo)
        const cls = ocrText ? classify({
          title: detail.title || item.titulo,
          detailText: detail.mainText || item.descripcion || '',
          slug: item.link,
          images: detail.images,
          ocrText
        }) : preview;

        // 5) Fallbacks obligatorios
        if (!provincia) provincia = 'Provincia desconocida';
        if (!cls.categoria) cls.categoria = 'Categoría desconocida';

        const imagen_url = detail.ogImage || (detail.images[0]?.src || '') || item.imagenLista || '';

        const record = validateAndEnrichResult({
          titulo: (detail.title || item.titulo || '').trim(),
          link: item.link,
          imagen_url,
          categoria: cls.categoria,
          provincia,
          descripcion: (item.descripcion || '').trim(),
          fecha_scraping: new Date().toISOString(),
          confidence: cls.confidence,
          reasons: cls.reasons.slice(0, 8)
        });

        out.push(record);
      } catch (e) {
        console.warn('   ⚠️ Error en detalle:', e.message);
      }
      await autoPause();
    }

    // Dedupe por link normalizado
    const seen = new Set();
    const beneficios = out.filter(x => (seen.has(x.link) ? false : (seen.add(x.link), true)));

    await fs.writeFile('beneficios_mejorado.json', JSON.stringify(beneficios, null, 2), 'utf8');
    console.log('\n✅ Guardado en beneficios_mejorado.json');

    // Resumen
    const byCat = {}, byProv = {};
    for (const b of beneficios) {
      byCat[b.categoria] = (byCat[b.categoria] || 0) + 1;
      byProv[b.provincia] = (byProv[b.provincia] || 0) + 1;
    }

    console.log('\n📊 Por Categoría:');
    for (const cat of CATEGORY_ORDER) if (byCat[cat]) console.log(`  - ${cat}: ${byCat[cat]}`);

    console.log('\n📍 Por Provincia:');
    Object.entries(byProv)
      .sort((a,b) => a[0] === 'Provincia desconocida' ? -1 : b[0] === 'Provincia desconocida' ? 1 : a[0].localeCompare(b[0]))
      .forEach(([k,v]) => console.log(`  - ${k}: ${v}`));

    console.log('\n📝 Muestra (5):');
    beneficios.slice(0,5).forEach((b,i) => console.log(` ${i+1}. ${b.titulo} | ${b.categoria} | ${b.provincia} | conf=${b.confidence}`));

    const servicios = beneficios.filter(b => b.categoria === 'Servicios');
    if (servicios.length > 0) {
      console.log('\n🔧 Beneficios de Servicios detectados:');
      servicios.slice(0, 10).forEach((b,i) => console.log(`  ${i+1}. ${b.titulo}`));
    }

  } catch (err) {
    console.error('❌ Error general:', err);
  } finally {
    await new Promise(r => setTimeout(r, 700));
    await browser.close();
    console.log('\n👋 Navegador cerrado.');
  }
}

run();
