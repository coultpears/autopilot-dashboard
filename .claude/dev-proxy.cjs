// Dev-only proxy: serves static files from repo root + forwards /api/* → netlify functions:serve (port 9999).
// NOT used in production — Netlify's own redirect handles /api → /.netlify/functions.

const http = require('http');
const fs = require('fs');
const path = require('path');

const STATIC_ROOT = path.resolve(__dirname, '..');
const FUNCTIONS_ORIGIN = 'http://localhost:9999';
const PORT = Number(process.env.DEV_PROXY_PORT || 5001);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg':  'image/svg+xml',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.ico':  'image/x-icon',
  '.map':  'application/json',
};

async function proxyApi(req, res) {
  // /api/grid-history?period=3mo → /.netlify/functions/grid-history?period=3mo
  const suffix = req.url.replace(/^\/api\//, '');
  const target = FUNCTIONS_ORIGIN + '/.netlify/functions/' + suffix;
  try {
    const resp = await fetch(target, {
      method: req.method,
      headers: { ...req.headers, host: 'localhost:9999' },
    });
    res.statusCode = resp.status;
    resp.headers.forEach((v, k) => res.setHeader(k, v));
    // CORS for browser clients (some browsers are strict on localhost origins)
    res.setHeader('Access-Control-Allow-Origin', '*');
    const buf = Buffer.from(await resp.arrayBuffer());
    res.end(buf);
  } catch (e) {
    res.statusCode = 502;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'proxy failed: ' + e.message, target }));
  }
}

function serveStatic(req, res) {
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/' || urlPath === '') urlPath = '/index.html';
  const filePath = path.join(STATIC_ROOT, urlPath);
  // Basic path traversal guard
  if (!filePath.startsWith(STATIC_ROOT)) {
    res.statusCode = 403; return res.end('forbidden');
  }
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.statusCode = 404;
      res.setHeader('Content-Type', 'text/plain');
      return res.end('404 not found: ' + urlPath);
    }
    const ext = path.extname(filePath).toLowerCase();
    res.setHeader('Content-Type', MIME[ext] || 'application/octet-stream');
    // Permissive CORS so fetch() from localhost:5001 → localhost:9999 just works
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/api/')) return proxyApi(req, res);
  return serveStatic(req, res);
});

server.listen(PORT, () => {
  console.log(`dev-proxy listening http://localhost:${PORT} (static=${STATIC_ROOT}, api→${FUNCTIONS_ORIGIN})`);
});
