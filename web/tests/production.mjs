// Exercise the actual production Dockerfile and Nginx configuration with a
// generic API fixture. All resources use a fresh name and are removed in finally.
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const root = fileURLToPath(new URL('..', import.meta.url));
const name = `ds-dashboard-test-${randomUUID().slice(0, 12)}`;
const image = `${name}:web`;
const containers = [];
let network = false;
const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8', timeout: 180_000 }).trim();
try {
  execFileSync('docker', ['build', '-t', image, root], { stdio: 'inherit', timeout: 240_000 });
  docker('network', 'create', name); network = true;
  const api = `${name}-api`;
  containers.push(api);
  docker('run', '-d', '--name', api, '--network', name, '--network-alias', 'api',
    '-e', 'PORT=8080', '-e', 'HOST=0.0.0.0',
    '-v', `${resolve(root, 'tests/api-fixture.mjs')}:/fixture.mjs:ro,z`, 'node:25-bookworm', 'node', '/fixture.mjs');
  const web = `${name}-web`;
  containers.push(web);
  docker('run', '-d', '--name', web, '--network', name, '-p', '127.0.0.1::80', image);
  const port = docker('port', web, '80/tcp').split(':').at(-1);
  const url = `http://127.0.0.1:${port}`;
  let ready = false;
  for (let attempt = 0; attempt < 60; attempt++) {
    try { ready = (await fetch(`${url}/healthz`, { signal: AbortSignal.timeout(1000) })).ok; } catch { /* starting */ }
    if (ready) break;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  if (!ready) throw new Error('Isolated production web server did not become ready');
  execFileSync(process.execPath, ['node_modules/@playwright/test/cli.js', 'test'], {
    cwd: root, stdio: 'inherit', timeout: 180_000,
    env: { ...process.env, DONKEYSPACE_TEST_WEB_URL: url },
  });
} finally {
  for (const container of containers.reverse()) docker('rm', '--force', container);
  if (network) docker('network', 'rm', name);
  // This tag is created only by this test; shared image layers remain cached.
  try { docker('image', 'rm', image); } catch { /* build may have failed */ }
}
