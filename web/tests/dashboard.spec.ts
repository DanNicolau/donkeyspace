import { test, expect, type Page } from '@playwright/test';

const facade = { display_name: 'Example Workflow', tagline: 'Repository workflow monitoring', issue_command: '/example', branch_prefix: 'example' };
const ready = (page: Page) => expect(page.getByRole('heading', { name: facade.display_name, exact: true })).toBeVisible();
const unavailable = (page: Page) => expect(page.getByRole('heading', { name: 'Dashboard unavailable', exact: true })).toBeVisible();

// Each test uses a fresh browser context and only generic fixture responses.
test('only renders effective facade after a delayed response arrives', async ({ page }, info) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  await page.route('**/api/facade', async (route) => { await gate; await route.fulfill({ json: facade }); });
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Loading dashboard configuration…' })).toBeVisible();
  await expect(page.getByText('Agent Platform', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('heading', { name: 'Issue workflows', exact: true })).toHaveCount(0);
  await expect(page).toHaveTitle('Dashboard');
  await page.screenshot({ path: info.outputPath('loading.png'), fullPage: true });
  release();
  await ready(page);
  await expect(page).toHaveTitle(facade.display_name);
  await page.screenshot({ path: info.outputPath('loaded.png'), fullPage: true });
});

for (const failure of ['http', 'network', 'html', 'broken-json', 'wrong-shape', 'null', 'blank-field', 'missing-field']) {
  test(`shows ${failure} failure and retries without duplicate requests`, async ({ page }, info) => {
    let requests = 0;
    let retry = false;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    await page.route('**/api/facade', async (route) => {
      requests++;
      if (retry) { await gate; await route.fulfill({ json: facade }); return; }
      if (failure === 'network') return route.abort('connectionrefused');
      if (failure === 'http') return route.fulfill({ status: 503, contentType: 'text/html', body: 'upstream unavailable' });
      if (failure === 'html') return route.fulfill({ contentType: 'text/html', body: '<html>SPA or login page</html>' });
      if (failure === 'broken-json') return route.fulfill({ contentType: 'application/json', body: '{bad' });
      const value = failure === 'null' ? null : failure === 'blank-field' ? { ...facade, display_name: ' ' } : failure === 'missing-field' ? { display_name: facade.display_name } : { ...facade, tagline: 42 };
      await route.fulfill({ json: value });
    });
    await page.goto('/');
    await unavailable(page);
    await expect(page.getByRole('alert')).toContainText('Check that the API is running');
    await expect(page.getByRole('heading', { name: 'Issue workflows', exact: true })).toHaveCount(0);
    if (failure === 'http') await page.screenshot({ path: info.outputPath('unavailable.png'), fullPage: true });
    // React development StrictMode cancels and remounts the initial query.
    // Count user retries independently of those intentionally aborted mounts.
    const beforeRetry = requests;
    retry = true;
    await page.getByRole('button', { name: 'Retry connection' }).click();
    await expect(page.getByRole('heading', { name: 'Loading dashboard configuration…' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Retry connection' })).toHaveCount(0);
    await expect.poll(() => requests).toBe(beforeRetry + 1);
    release();
    await ready(page);
    await expect(page.getByRole('alert')).toHaveCount(0);
    expect(requests).toBe(beforeRetry + 1);
  });
}

test('bounds a hung request then accepts a fresh retry', async ({ page }) => {
  let requests = 0;
  let retry = false;
  await page.route('**/api/facade', async (route) => {
    requests++;
    if (retry) await route.fulfill({ json: facade });
    // Leave the first request unresolved until the app aborts its deadline.
  });
  await page.goto('/');
  await expect(page.getByText('The API did not respond within 10 seconds.')).toBeVisible({ timeout: 13_000 });
  const beforeRetry = requests;
  retry = true;
  await page.getByRole('button', { name: 'Retry connection' }).click();
  await ready(page);
  expect(requests).toBe(beforeRetry + 1);
});

test('a refresh failure hides cached branding until retry succeeds', async ({ page }) => {
  let fail = false;
  await page.route('**/api/facade', async (route) => {
    await route.fulfill(fail ? { status: 502, body: '' } : { json: facade });
  });
  await page.clock.install();
  await page.goto('/');
  await ready(page);
  fail = true;
  await page.clock.fastForward(30_001);
  await unavailable(page);
  await expect(page.getByRole('heading', { name: facade.display_name, exact: true })).toHaveCount(0);
  await expect(page).toHaveTitle('Dashboard');
  fail = false;
  await page.getByRole('button', { name: 'Retry connection' }).click();
  await ready(page);
});

test('malformed configuration is visible without crashing or inventing settings', async ({ page }) => {
  await page.route('**/api/configuration', (route) => route.fulfill({ json: { warnings: 'bad' } }));
  await page.goto('/operations');
  await ready(page);
  await expect(page.getByRole('alert')).toContainText('Configuration unavailable');
  await expect(page.locator('.configuration-panel')).toContainText('Configuration unavailable');
  await expect(page.locator('.configuration-panel')).not.toContainText('Disabled');
  await page.unroute('**/api/configuration');
  await page.getByRole('button', { name: 'Retry configuration' }).click();
  await expect(page.locator('.configuration-panel')).toContainText('test-policy.yml');
});

test('connection retry also recovers configuration after a whole-API outage', async ({ page }) => {
  let fail = true;
  await page.route(/\/api\/(facade|configuration)$/, (route) => fail ? route.fulfill({ status: 502, body: '' }) : route.continue());
  await page.goto('/');
  await unavailable(page);
  fail = false;
  const configuration = page.waitForResponse((response) => response.url().endsWith('/api/configuration') && response.status() === 200);
  await page.getByRole('button', { name: 'Retry connection' }).click();
  await configuration;
  await ready(page);
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('connection retry replaces an in-flight configuration request', async ({ page }) => {
  let retry = false;
  await page.route('**/api/facade', (route) => retry ? route.continue() : route.fulfill({ status: 502, body: '' }));
  await page.route('**/api/configuration', async (route) => {
    if (retry) await route.continue();
    // Initial configuration never responds. Retry must abort and replace it.
  });
  await page.goto('/');
  await unavailable(page);
  retry = true;
  const configuration = page.waitForResponse((response) => response.url().endsWith('/api/configuration') && response.status() === 200);
  await page.getByRole('button', { name: 'Retry connection' }).click();
  await configuration;
  await ready(page);
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('rejects a configuration enum encoded as an array', async ({ page, request }) => {
  const valid = await (await request.get('/api/configuration')).json();
  await page.route('**/api/configuration', (route) => route.fulfill({ json: { ...valid, deployment_mode: ['minimal'] } }));
  await page.goto('/operations');
  await ready(page);
  await expect(page.getByRole('alert')).toContainText('invalid dashboard configuration');
  await expect(page.locator('.configuration-panel')).toContainText('Configuration unavailable');
});

test('health link returns upstream JSON and preserves failure status through this web origin', async ({ page, request }) => {
  await page.goto('/');
  await ready(page);
  await page.getByRole('link', { name: 'API health' }).click();
  await expect(page).toHaveURL(/\/healthz$/);
  const response = await request.get('/healthz?probe=dashboard');
  expect(response.status()).toBe(200);
  expect(response.headers()['content-type']).toContain('application/json');
  expect(await response.json()).toMatchObject({ service: 'donkeyspace-api', fixture: 'dashboard-routing-test' });
  const failure = await request.get('/healthz?unavailable=1');
  expect(failure.status()).toBe(503);
  expect(await failure.json()).toMatchObject({ status: 'degraded' });
  // The exact health route must not swallow normal dashboard deep links.
  const spa = await request.get('/repositories/example/test/issues/1');
  expect(spa.headers()['content-type']).toContain('text/html');
});
