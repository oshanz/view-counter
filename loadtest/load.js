// Baseline at the target rates: 1 write/s and 1 read/s.
// Also usable as a soak test: DURATION_S=3600 mise run loadtest
//
// Every write uses a unique fingerprint on one key, so at the end the total
// must equal the number of writes sent. Nothing may be lost or double counted.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const WRITE_RATE = Number(__ENV.WRITE_RATE || 1);
const READ_RATE = Number(__ENV.READ_RATE || 1);
const DURATION_S = Number(__ENV.DURATION_S || 60);
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };

export const options = {
  scenarios: {
    // One VU sends exactly WRITE_RATE * DURATION_S writes, paced by hand, so
    // the final total can be checked exactly (an arrival-rate executor may
    // fire one iteration more or fewer at the edges).
    writes: {
      executor: 'per-vu-iterations',
      exec: 'write',
      vus: 1,
      iterations: WRITE_RATE * DURATION_S,
      maxDuration: `${DURATION_S + 2}s`,
    },
    reads: {
      executor: 'constant-arrival-rate',
      exec: 'read',
      rate: READ_RATE,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: 5,
      maxVUs: 50,
    },
    verify: {
      executor: 'shared-iterations',
      exec: 'verify',
      vus: 1,
      iterations: 1,
      startTime: `${DURATION_S + 3}s`,
    },
  },
  thresholds: {
    'http_req_failed': ['rate==0'],
    'http_req_duration{kind:write}': ['p(95)<100', 'p(99)<250'],
    'http_req_duration{kind:read}': ['p(95)<200', 'p(99)<500'],
    'checks': ['rate==1'],
    // A dropped read means the service could not keep up with the rate.
    'dropped_iterations': ['count==0'],
  },
};

export function setup() {
  return { key: `k6${Date.now().toString(36)}` };
}

export function write(data) {
  const fp = `${data.key}-${exec.vu.iterationInScenario}`;
  const res = http.post(`${BASE}/v1/hit`,
    JSON.stringify({ key: data.key, fingerprint: fp }), {
      ...JSON_HDR, tags: { kind: 'write' },
    });
  check(res, { 'hit is 204': (r) => r.status === 204 });
  sleep(Math.max(0, 1 / WRITE_RATE - res.timings.duration / 1000));
}

export function read(data) {
  const i = exec.scenario.iterationInTest % 3;
  const today = new Date().toISOString().slice(0, 10);
  const url = [
    `${BASE}/v1/views?key=${data.key}`,
    `${BASE}/v1/views?key=${data.key}&from=2026-01-01&to=${today}`,
    `${BASE}/v1/top`,
  ][i];
  const res = http.get(url, { tags: { kind: 'read' } });
  check(res, { 'read is 200': (r) => r.status === 200 });
}

export function verify(data) {
  const expected = WRITE_RATE * DURATION_S;

  const res = http.get(`${BASE}/v1/views?key=${data.key}`);
  check(res, {
    [`total equals ${expected} writes sent`]: (r) => r.json('views') === expected,
  });

  // A repeat of an already counted fingerprint is accepted but not counted.
  const dup = http.post(`${BASE}/v1/hit`,
    JSON.stringify({ key: data.key, fingerprint: `${data.key}-0` }), JSON_HDR);
  const after = http.get(`${BASE}/v1/views?key=${data.key}`);
  check(dup, { 'duplicate is 204': (r) => r.status === 204 });
  check(after, { 'duplicate not counted': (r) => r.json('views') === expected });
}
