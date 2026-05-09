import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 20,
  duration: '1m',
  thresholds: {
    http_req_duration: ['p(95)<50'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';
  const res = http.get(`${baseUrl}/leaderboard?leaderboardId=global&limit=100`);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'has items': (r) => Array.isArray(r.json('items')),
  });
}
