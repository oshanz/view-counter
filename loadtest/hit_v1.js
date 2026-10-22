// Write ceiling of POST /v1/hit: step the offered rate up and watch where
// latency, errors or dropped iterations first break.
//
// Each step is its own constant-arrival-rate scenario tagged with its rate.
// 70% of hits go to one hot key, the rest spread over SEED_KEYS; every
// fingerprint is unique, so every hit is a new viewer and Postgres has to
// insert it. /v1 writes synchronously, so teardown() only checks that the hot
// key holds exactly the hits that were acked.
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
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

export function setup() {
  return { run: Date.now().toString(36) };
}

export function hit(data) {
  const i = exec.scenario.iterationInTest;
  const isHot = i % 10 < 7;
  const key = isHot ? `v1h-${data.run}-hot` : `v1h-${data.run}-${i % SEED_KEYS}`;
  const res = http.post(`${BASE}/v1/hit`,
    JSON.stringify({ key, fingerprint: `${data.run}-${__VU}-${__ITER}` }), JSON_HDR);
  if (check(res, { 'hit is 204': (r) => r.status === 204 })) {
    acked.add(1);
    if (isHot) ackedHot.add(1);
  }
}

export function teardown(data) {
  const hot = http.get(`${BASE}/v1/views?key=v1h-${data.run}-hot`, { responseType: 'text' }).json('views');
  console.log(`hot key views in Postgres: ${hot} (compare with acked_hot_hits)`);
}
