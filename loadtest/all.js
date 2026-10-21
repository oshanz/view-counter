// Mixed load across every endpoint at once, each as its own constant-arrival-rate
// scenario tagged with the endpoint, so the summary has one row per endpoint.
// Rates are per endpoint (RATE_<NAME> overrides, requests/s); /v2 must be
// enabled (NATS_URL set).
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const DURATION = __ENV.DURATION || '30s';
const MAX_VUS = Number(__ENV.MAX_VUS || 500);
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };

const ENDPOINTS = {
  v1_hit: { rate: 100, want: 204 },
  v1_views: { rate: 200, want: 200 },
  v1_top: { rate: 50, want: 200 },
  v2_hit: { rate: 500, want: 202 },
  v2_views: { rate: 1000, want: 200 },
  v2_top: { rate: 50, want: 200 },
  v2_ingest: { rate: 10, want: 200 },
};

const scenarios = {};
const thresholds = {};
for (const [name, e] of Object.entries(ENDPOINTS)) {
  const rate = Number(__ENV[`RATE_${name.toUpperCase()}`] || e.rate);
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    exec: name,
    rate,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: Math.min(MAX_VUS, Math.max(5, Math.round(rate / 10))),
    maxVUs: MAX_VUS,
    tags: { endpoint: name },
  };
  thresholds[`http_req_failed{endpoint:${name}}`] = ['rate<0.01'];
  thresholds[`http_req_duration{endpoint:${name}}`] = ['p(95)<500'];
  thresholds[`dropped_iterations{scenario:${name}}`] = ['count<50'];
}

export const options = {
  scenarios,
  thresholds,
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const run = Date.now().toString(36);
const hot = `all-${run}-hot`;

function ingestIdle() {
  const res = http.get(`${BASE}/v2/ingest`, { responseType: 'text' });
  if (res.status !== 200) return false;
  const lag = res.json();
  return lag.pending === 0 && lag.ack_pending === 0;
}

// Seed the hot key through both write paths so every read has data.
export function setup() {
  if (http.get(`${BASE}/v2/ingest`).status !== 200) {
    throw new Error('GET /v2/ingest failed; start the service with NATS_URL set');
  }
  const reqs = [];
  for (let i = 0; i < 200; i++) {
    reqs.push(['POST', `${BASE}/v1/hit`, JSON.stringify({ key: hot, fingerprint: `${run}-s1-${i}` }), JSON_HDR]);
    reqs.push(['POST', `${BASE}/v2/hit`, JSON.stringify({ key: hot, fingerprint: `${run}-s2-${i}` }), JSON_HDR]);
  }
  for (let i = 0; i < reqs.length; i += 200) {
    for (const res of http.batch(reqs.slice(i, i + 200))) {
      if (res.status !== 204 && res.status !== 202) throw new Error(`seed hit returned ${res.status}`);
    }
  }
  for (let i = 0; i < 100 && !ingestIdle(); i++) sleep(0.2);
}

let n = 0;
const key = (prefix) => (n++ % 5 === 0 ? `${prefix}-${run}-${n % 1000}` : hot);
const today = () => new Date().toISOString().slice(0, 10);
const ok = (name) => (res) => check(res, { [`${name} status`]: (r) => r.status === ENDPOINTS[name].want });

export function v1_hit() {
  ok('v1_hit')(http.post(`${BASE}/v1/hit`,
    JSON.stringify({ key: key('v1'), fingerprint: `${run}-a-${__VU}-${__ITER}` }), JSON_HDR));
}
export function v1_views() {
  const range = __ITER % 3 === 0 ? `&from=2026-01-01&to=${today()}` : '';
  ok('v1_views')(http.get(`${BASE}/v1/views?key=${hot}${range}`));
}
export function v1_top() {
  ok('v1_top')(http.get(`${BASE}/v1/top?n=100`));
}
export function v2_hit() {
  ok('v2_hit')(http.post(`${BASE}/v2/hit`,
    JSON.stringify({ key: key('v2'), fingerprint: `${run}-b-${__VU}-${__ITER}` }), JSON_HDR));
}
export function v2_views() {
  const range = __ITER % 10 === 0 ? `&from=${today()}&to=${today()}` : '';
  ok('v2_views')(http.get(`${BASE}/v2/views?key=${hot}${range}`));
}
export function v2_top() {
  ok('v2_top')(http.get(`${BASE}/v2/top?n=100`));
}
export function v2_ingest() {
  ok('v2_ingest')(http.get(`${BASE}/v2/ingest`));
}
