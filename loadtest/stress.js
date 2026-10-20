// Headroom: ramp writes from the target rate up to 200/s and reads to 50/s,
// with most writes landing on one hot key (a viral video) to exercise row
// contention on page_views.
//
// The service is expected to stay correct and fast well past the 1/s target;
// the thresholds are looser than the baseline and the interesting number is
// the rate at which they start to fail.
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const MAX_WRITE = Number(__ENV.MAX_WRITE_RATE || 200);
const MAX_READ = Number(__ENV.MAX_READ_RATE || 50);
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };

const ramp = (max) => [
  { target: 1, duration: '10s' },
  { target: Math.round(max / 4), duration: '20s' },
  { target: Math.round(max / 2), duration: '20s' },
  { target: max, duration: '20s' },
  { target: max, duration: '30s' },
  { target: 0, duration: '10s' },
];

export const options = {
  scenarios: {
    writes: {
      executor: 'ramping-arrival-rate',
      exec: 'write',
      startRate: 1,
      timeUnit: '1s',
      stages: ramp(MAX_WRITE),
      preAllocatedVUs: 20,
      maxVUs: 400,
    },
    reads: {
      executor: 'ramping-arrival-rate',
      exec: 'read',
      startRate: 1,
      timeUnit: '1s',
      stages: ramp(MAX_READ),
      preAllocatedVUs: 10,
      maxVUs: 200,
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.01'],
    'http_req_duration{kind:write}': ['p(95)<250'],
    'http_req_duration{kind:read}': ['p(95)<500'],
    'dropped_iterations': ['count==0'],
  },
};

const run = Date.now().toString(36);

export function write() {
  const i = exec.scenario.iterationInTest;
  // 80% of traffic hits one key, the rest is spread over 1000 keys.
  const key = i % 5 === 0 ? `k6s-${run}-${i % 1000}` : `k6s-${run}-hot`;
  const res = http.post(`${BASE}/v1/hit`,
    JSON.stringify({ key, fingerprint: `${run}-${i}` }), {
      ...JSON_HDR, tags: { kind: 'write' },
    });
  check(res, { 'hit is 204': (r) => r.status === 204 });
}

export function read() {
  const i = exec.scenario.iterationInTest % 3;
  const today = new Date().toISOString().slice(0, 10);
  const url = [
    `${BASE}/v1/views?key=k6s-${run}-hot`,
    `${BASE}/v1/views?key=k6s-${run}-hot&from=2026-01-01&to=${today}`,
    `${BASE}/v1/top?n=100`,
  ][i];
  const res = http.get(url, { tags: { kind: 'read' } });
  check(res, { 'read is 200': (r) => r.status === 200 });
}
