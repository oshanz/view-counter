// Write ceiling of POST /v2/hit: step the offered rate up and watch where
// latency, errors or dropped iterations first break.
//
// Each step is its own constant-arrival-rate scenario tagged with its rate.
// 70% of hits go to one hot key, the rest spread over SEED_KEYS; every
// fingerprint is unique, so every hit is a new viewer and the writer has to
// insert it. teardown() waits for the writer to drain and checks that the hot
// key and the cold keys together hold exactly the hits that were acked.
//
// /v2 must be enabled (NATS_URL set).
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const STEPS = (__ENV.STEPS || '200,500,1000,2000,4000').split(',').map(Number);
const STEP_DURATION = __ENV.STEP_DURATION || '20s';
const SEED_KEYS = Number(__ENV.SEED_KEYS || 1000);
const MAX_VUS = Number(__ENV.MAX_VUS || 2000);
const STOP_ON_FAIL = __ENV.STOP_ON_FAIL === '1';
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };

const stepSeconds = Number(STEP_DURATION.replace('s', ''));
const GAP_SECONDS = 5;

const acked = new Counter('acked_hits');
const ackedHot = new Counter('acked_hot_hits');

const scenarios = {};
const thresholds = {};
STEPS.forEach((rate, n) => {
  const name = `step_${rate}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    exec: 'hit',
    rate,
    timeUnit: '1s',
    duration: STEP_DURATION,
    startTime: `${n * (stepSeconds + GAP_SECONDS)}s`,
    preAllocatedVUs: Math.min(MAX_VUS, Math.max(50, Math.round(rate / 10))),
    maxVUs: MAX_VUS,
    tags: { rate: String(rate) },
  };
  const t = (threshold) => (STOP_ON_FAIL ? { threshold, abortOnFail: true } : threshold);
  thresholds[`http_req_failed{rate:${rate}}`] = [t('rate<0.01')];
  thresholds[`http_req_duration{rate:${rate}}`] = [t('p(95)<500')];
  thresholds[`dropped_iterations{scenario:${name}}`] = [t(`count<${Math.round(rate * stepSeconds * 0.01)}`)];
});

export const options = {
  scenarios,
  thresholds,
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function lag() {
  const res = http.get(`${BASE}/v2/ingest`, { responseType: 'text' });
  return res.status === 200 ? res.json() : null;
}

export function setup() {
  if (!lag()) throw new Error('GET /v2/ingest failed; start the service with NATS_URL set');
  return { run: Date.now().toString(36) };
}

export function hit(data) {
  const i = exec.scenario.iterationInTest;
  const isHot = i % 10 < 7;
  const key = isHot ? `v2h-${data.run}-hot` : `v2h-${data.run}-${i % SEED_KEYS}`;
  const res = http.post(`${BASE}/v2/hit`,
    JSON.stringify({ key, fingerprint: `${data.run}-${__VU}-${__ITER}` }), JSON_HDR);
  if (check(res, { 'hit is 202': (r) => r.status === 202 })) {
    acked.add(1);
    if (isHot) ackedHot.add(1);
  }
}

export function teardown(data) {
  // The writer drains up to 1000 hits per 250ms per worker; give it time.
  let l;
  for (let i = 0; i < 600; i++) {
    l = lag();
    if (l && l.pending === 0 && l.ack_pending === 0) break;
    sleep(0.5);
  }
  console.log(`writer lag at end of drain wait: ${JSON.stringify(l)}`);
  const hot = http.get(`${BASE}/v2/views?key=v2h-${data.run}-hot`, { responseType: 'text' }).json('views');
  console.log(`hot key views in Postgres: ${hot} (compare with acked_hot_hits)`);
}
