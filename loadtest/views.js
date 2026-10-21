// Read ceiling of GET /v2/views: step the offered rate up and watch where
// latency, errors or dropped iterations first break.
//
// Each step is its own constant-arrival-rate scenario tagged with its rate, so
// the summary shows one set of numbers per step instead of one blended number.
// The mix is 70% total of one hot key, 20% total of a cold key (spread over
// SEED_KEYS), 10% daily series of the hot key.
//
// setup() seeds data through /v2/hit and waits for the writer to drain, so
// /v2 must be enabled (NATS_URL set).
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const STEPS = (__ENV.STEPS || '500,1000,2500,5000,7500,10000').split(',').map(Number);
const STEP_DURATION = __ENV.STEP_DURATION || '20s';
const SEED_KEYS = Number(__ENV.SEED_KEYS || 1000);
const HOT_HITS = Number(__ENV.HOT_HITS || 1000);
const MAX_VUS = Number(__ENV.MAX_VUS || 2000);
const STOP_ON_FAIL = __ENV.STOP_ON_FAIL === '1';
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };

const stepSeconds = Number(STEP_DURATION.replace('s', ''));
const GAP_SECONDS = 5;

const scenarios = {};
const thresholds = {};
STEPS.forEach((rate, n) => {
  const name = `step_${rate}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    exec: 'read',
    rate,
    timeUnit: '1s',
    duration: STEP_DURATION,
    startTime: `${n * (stepSeconds + GAP_SECONDS)}s`,
    preAllocatedVUs: Math.min(MAX_VUS, Math.max(50, Math.round(rate / 10))),
    maxVUs: MAX_VUS,
    tags: { rate: String(rate) },
  };
  // STOP_ON_FAIL=1 aborts the whole run at the first failing step, which
  // is how to find the ceiling without running the remaining steps.
  const t = (threshold) => (STOP_ON_FAIL ? { threshold, abortOnFail: true } : threshold);
  thresholds[`http_req_failed{rate:${rate}}`] = [t('rate<0.01')];
  thresholds[`http_req_duration{rate:${rate}}`] = [t('p(95)<500')];
  // Allow 1% dropped: k6 is still allocating VUs in the first moments of a step.
  thresholds[`dropped_iterations{scenario:${name}}`] = [t(`count<${Math.round(rate * stepSeconds * 0.01)}`)];
});

export const options = {
  scenarios,
  thresholds,
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function ingestIdle() {
  const res = http.get(`${BASE}/v2/ingest`, { responseType: 'text' });
  if (res.status !== 200) return false;
  const lag = res.json();
  return lag.pending === 0 && lag.ack_pending === 0;
}

export function setup() {
  const probe = http.get(`${BASE}/v2/ingest`);
  if (probe.status !== 200) {
    throw new Error(`GET /v2/ingest returned ${probe.status}; start the service with NATS_URL set`);
  }

  const run = Date.now().toString(36);
  const hot = `v2v-${run}-hot`;
  const reqs = [];
  for (let i = 0; i < HOT_HITS; i++) {
    reqs.push(['POST', `${BASE}/v2/hit`, JSON.stringify({ key: hot, fingerprint: `${run}-h-${i}` }), JSON_HDR]);
  }
  for (let i = 0; i < SEED_KEYS; i++) {
    reqs.push(['POST', `${BASE}/v2/hit`, JSON.stringify({ key: `v2v-${run}-${i}`, fingerprint: `${run}-c-${i}` }), JSON_HDR]);
  }
  for (let i = 0; i < reqs.length; i += 200) {
    for (const res of http.batch(reqs.slice(i, i + 200))) {
      if (res.status !== 202) throw new Error(`seed hit returned ${res.status}`);
    }
  }

  // The writer batches every 250ms; wait until everything is in page_views.
  for (let i = 0; i < 100 && !ingestIdle(); i++) sleep(0.2);
  if (!ingestIdle()) throw new Error('writer did not drain the seed hits');

  const total = http.get(`${BASE}/v2/views?key=${hot}`, { responseType: 'text' }).json('views');
  if (total !== HOT_HITS) throw new Error(`hot key has ${total} views, want ${HOT_HITS}`);
  return { run, hot };
}

export function read(data) {
  const i = exec.scenario.iterationInTest;
  const today = new Date().toISOString().slice(0, 10);
  let url;
  switch (i % 10) {
    case 0:
      url = `${BASE}/v2/views?key=${data.hot}&from=${today}&to=${today}`;
      break;
    case 1:
    case 2:
      url = `${BASE}/v2/views?key=v2v-${data.run}-${i % SEED_KEYS}`;
      break;
    default:
      url = `${BASE}/v2/views?key=${data.hot}`;
  }
  const res = http.get(url);
  check(res, { 'views is 200': (r) => r.status === 200 });
}
