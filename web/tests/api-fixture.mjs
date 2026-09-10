import { createServer } from 'node:http';

export const facade = { display_name: 'Example Workflow', tagline: 'Repository workflow monitoring', issue_command: '/example', branch_prefix: 'example' };
export const configuration = { deployment_mode: 'minimal', policy_source: 'test-policy.yml', facade, github: { auth_mode: 'none', ingress_mode: 'webhook', repositories: [] }, plugin: null, capabilities: [], warnings: [] };

const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://fixture');
  let body;
  switch (url.pathname) {
    case '/healthz':
      res.statusCode = url.searchParams.get('unavailable') === '1' ? 503 : 200;
      body = { status: res.statusCode === 200 ? 'ok' : 'degraded', service: 'donkeyspace-api', fixture: 'dashboard-routing-test' };
      break;
    case '/api/facade': body = facade; break;
    case '/api/configuration': body = configuration; break;
    case '/api/github-ingress/status': body = { configured_mode: 'webhook', webhook: { endpoint_enabled: false, app: null, deliveries_24h: 0 }, polling: { enabled: false, running: false }, poll_deliveries: { deliveries_24h: 0 } }; break;
    default: body = [];
  }
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
});
server.listen(Number(process.env.PORT ?? 18138), process.env.HOST ?? '127.0.0.1');
process.on('SIGTERM', () => server.close());
