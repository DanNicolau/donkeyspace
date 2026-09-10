// Called only by the isolated umbrella harness; no credentials enter the browser.
import { chromium, expect } from '@playwright/test';
import { createInterface } from 'node:readline';
import { resolve } from 'node:path';
const [url, title, artifacts] = process.argv.slice(2);
if (!url?.startsWith('http://127.0.0.1:')) throw new Error('Expected isolated loopback web origin');
const commands = createInterface({ input: process.stdin })[Symbol.asyncIterator]();
const signal = async (expected) => {
  if ((await commands.next()).value !== expected) throw new Error(`Expected ${expected} from test harness`);
};
const browser = await chromium.launch();
try {
  const page = await browser.newPage({ viewport: { width: 1280, height: 850 } });
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  await page.route('**/api/facade', async (route) => { await gate; await route.continue(); });
  await page.goto(url);
  await expect(page.getByRole('heading', { name: 'Loading dashboard configuration…' })).toBeVisible();
  await page.screenshot({ path: resolve(artifacts, 'live-loading.png'), fullPage: true });
  release();
  await expect(page.getByRole('heading', { name: 'Umbrella validation', exact: true })).toBeVisible();
  await expect(page.getByRole('link', { name: title, exact: true })).toBeVisible();
  await page.unroute('**/api/facade');
  await page.screenshot({ path: resolve(artifacts, 'live-loaded.png'), fullPage: true });
  process.stdout.write('ready\n');
  await signal('down');
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Dashboard unavailable' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Umbrella validation', exact: true })).toHaveCount(0);
  await page.screenshot({ path: resolve(artifacts, 'live-unavailable.png'), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole('button', { name: 'Retry connection' })).toBeVisible();
  await page.screenshot({ path: resolve(artifacts, 'live-unavailable-mobile.png'), fullPage: true });
  process.stdout.write('down\n');
  await signal('up');
  const configuration = page.waitForResponse((response) => response.url().endsWith('/api/configuration') && response.status() === 200);
  await page.getByRole('button', { name: 'Retry connection' }).click();
  await configuration;
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.getByRole('heading', { name: 'Umbrella validation', exact: true })).toBeVisible();
  await expect(page.getByRole('link', { name: title, exact: true })).toBeVisible();
  await page.setViewportSize({ width: 1280, height: 850 });
  await page.screenshot({ path: resolve(artifacts, 'live-recovered.png'), fullPage: true });
  const health = await page.request.get(`${url}/healthz`);
  expect(health.status()).toBe(200);
  expect(health.headers()['content-type']).toContain('application/json');
  expect(await health.json()).toMatchObject({ service: 'donkeyspace-api' });
  process.stdout.write('passed\n');
} finally {
  await browser.close();
  process.stdin.destroy();
}
