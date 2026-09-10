import { defineConfig } from '@playwright/test';

const external = process.env.DONKEYSPACE_TEST_WEB_URL;
export default defineConfig({
  testDir: './tests',
  outputDir: process.env.DONKEYSPACE_TEST_OUTPUT ?? 'test-results',
  testMatch: '**/*.spec.ts',
  fullyParallel: true,
  workers: 2,
  retries: 0,
  timeout: 25_000,
  use: { baseURL: external ?? 'http://127.0.0.1:15173', viewport: { width: 1280, height: 850 }, trace: 'retain-on-failure' },
  webServer: external ? undefined : [
    { command: 'node tests/api-fixture.mjs', url: 'http://127.0.0.1:18138/healthz', reuseExistingServer: false },
    { command: 'npm run dev -- --host 127.0.0.1 --port 15173 --strictPort', url: 'http://127.0.0.1:15173', reuseExistingServer: false, env: { DONKEYSPACE_API_PROXY_TARGET: 'http://127.0.0.1:18138' } },
  ],
});
