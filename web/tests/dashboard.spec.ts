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

test('parallel approval cards keep per-target commands and legacy text has no invented command', async ({ page }) => {
  const approval = (target: string) => ({ target_task: 'rtl', target_work_item: target, purpose: 'accept_result', trigger: 'required', approval_subject: `Review ${target}`, result_summary: 'Human wording contains no commands.', changed_files: [], proposed_publication: null, accepted_publication: null, projected_issues: [], downstream_tasks: [], state: 'pending', approve_command: `/example approve rtl/${target}`, revise_command: `/example revise rtl/${target}` });
  const workflow = { id: 1, owner: 'test', repository: 'test', issue_number: 1, issue_title: 'Parallel review', issue_url: 'https://github.com/test/test/issues/1', provider_state: 'open', current_state: 'needs_human', coordinator_job_id: null, coordinator_status: 'paused', outcome: 'needs_human', summary: 'Review output.', pending_approval: 'Legacy explanation only.', approvals: [approval('one'), approval('two')], external_sync: { status: 'ready' }, tasks: [], pull_request_url: null, no_pr_reason: 'Awaiting approval.', updated_at: '2026-01-01T00:00:00Z' };
  await page.route('**/api/workflows/test/test/issues/1', route => route.fulfill({ json: workflow }));
  await page.goto('/repositories/test/test/issues/1');
  for (const target of ['one', 'two']) {
    const card = page.locator('.approval-card').filter({ hasText: `Review ${target}` });
    await expect(card.locator('code')).toHaveText([`/example approve rtl/${target}`, `/example revise rtl/${target}`]);
  }
  workflow.approvals = [];
  await page.reload();
  await expect(page.getByText('Legacy explanation only.', { exact: true })).toBeVisible();
  await expect(page.locator('.approval-commands code')).toHaveCount(0);
});

test('upstream revisions explain invalidation scope before showing the command', async ({ page }, info) => {
  const workflow = { id: 1, owner: 'test', repository: 'test', issue_number: 1, issue_title: 'Upstream revision', issue_url: 'https://github.com/test/test/issues/1', provider_state: 'open', current_state: 'needs_human', coordinator_status: 'paused', outcome: 'needs_human', summary: 'Validation requires a contract change.', approvals: [], tasks: [], external_sync: { status: 'ready' }, no_pr_reason: 'Awaiting a decision.', updated_at: '2026-01-01T00:00:00Z', revision_targets: [{ target: 'plan', affected: ['plan', 'build/left', 'check/left', 'build/right', 'check/right'], revise_command: '/example revise plan' }] };
  await page.route('**/api/workflows/test/test/issues/1', route => route.fulfill({ json: workflow }));
  await page.goto('/repositories/test/test/issues/1');
  await expect(page.getByText('Revise completed upstream work', { exact: true })).toBeVisible();
  await page.getByText('plan', { exact: true }).click();
  await expect(page.getByText('Supersedes:', { exact: true }).locator('..')).toContainText('build/right, check/right');
  await expect(page.locator('code').filter({ hasText: '/example revise plan' })).toBeVisible();
  await expect(page.getByText(/Required approval will be requested for revised output/)).toBeVisible();
  await expect(page.getByText('/example approve plan', { exact: true })).toHaveCount(0);
  await page.screenshot({ path: info.outputPath('upstream-revision.png'), fullPage: true });
  workflow.revision_targets = [];
  workflow.current_state = 'in_progress';
  await page.reload();
  await expect(page.getByText('Revise completed upstream work', { exact: true })).toHaveCount(0);
});

test('current blockers show all questions and distinguish draft publication states', async ({ page }, info) => {
  const first = { job_id: 'attempt-one', task: 'check', work_item: 'one', outcome: 'needs_info', reason: 'The output behavior needs clarification.', questions: ['Which reset polarity should this interface use?', 'Should output saturate at the maximum value?'], action: 'Reply on the parent issue with answers to these questions to continue the retained workflow.', response_command: null as string | null, evidence: { state: 'none', message: 'No draft produced: this attempt contains no supporting files in its write scope.', files: [] as { path: string; url: string }[] } };
  const second = { ...first, job_id: 'attempt-two', work_item: 'two', questions: ['Is the clock shared between the two components?'], evidence: { state: 'failed', message: 'Draft publication failed. Retry publication to make supporting files available.', files: [] as { path: string; url: string }[] } };
  const workflow = { id: 1, owner: 'test', repository: 'test', issue_number: 1, issue_title: 'Clarify interface behavior', issue_url: 'https://github.com/test/test/issues/1', provider_state: 'open', current_state: 'needs_info', coordinator_status: 'paused', outcome: 'needs_info', summary: 'Awaiting answers before continuing the retained workflow.', approvals: [], tasks: [], external_sync: { status: 'ready' }, no_pr_reason: 'check/one: Which reset polarity should this interface use?', updated_at: '2026-01-01T00:00:00Z', blockers: [first, second] };
  await page.route('**/api/workflows', route => route.fulfill({ json: [workflow] }));
  await page.route('**/api/workflows/test/test/issues/1', route => route.fulfill({ json: workflow }));
  await page.route('**/api/workflows/test/test/issues/1/events?*', route => route.fulfill({ json: { events: [{ id: 1, event_type: 'task_completed', level: 'milestone', source: 'worker', task: 'check', work_item: 'one', summary: 'Clarification requested.', reason: first.questions.join('\n'), created_at: workflow.updated_at, links: [] }], next_before_id: null } }));
  await page.goto('/');
  await expect(page.locator('.blocker-preview')).toContainText('check/one: Which reset polarity');
  await expect(page.locator('.summary-grid').getByText('Needs attention').locator('..')).toContainText('1');
  await page.getByRole('link', { name: 'View 2 current blockers' }).click();
  const blockers = page.locator('#current-blockers');
  await expect(blockers.locator('.blocker-card')).toHaveCount(2);
  for (const question of [...first.questions, ...second.questions]) await expect(blockers.getByText(question, { exact: true })).toBeVisible();
  await expect(blockers).toContainText('No draft produced');
  await expect(blockers).toContainText('Draft publication failed');
  await expect(blockers.getByRole('link')).toHaveCount(0);
  await page.screenshot({ path: info.outputPath('blockers-no-draft-and-failed.png'), fullPage: true });
  first.evidence = { state: 'pending', message: 'Draft publication pending. Questions can be answered while publication is pending.', files: [] };
  await page.reload();
  await expect(blockers).toContainText('Draft publication pending');
  await expect(blockers.getByText(first.questions[1], { exact: true })).toBeVisible();
  first.evidence = { state: 'published', message: 'Supporting files at the exact attempt revision; these may include unchanged drafts.', files: [{ path: 'docs/interface.md', url: 'https://github.com/test/test/blob/accepted-sha/docs/interface.md' }] };
  workflow.current_state = 'needs_human';
  first.response_command = '/example revise check/one';
  first.action = 'Answer these questions through the relevant revision control on the parent issue. Other pending approvals remain required.';
  second.response_command = '/example revise check/two'; second.action = first.action; workflow.outcome = 'needs_human';
  await page.reload();
  await expect(blockers.getByRole('link', { name: 'docs/interface.md' })).toHaveAttribute('href', first.evidence.files[0].url);
  await expect(blockers.locator('code')).toHaveText(['/example revise check/one', '/example revise check/two']);
  await page.screenshot({ path: info.outputPath('blockers-published.png'), fullPage: true });
  workflow.blockers = []; workflow.current_state = 'in_progress'; workflow.outcome = 'implemented';
  await page.reload();
  await expect(blockers).toHaveCount(0);
  await page.locator('.timeline-row').getByText('Reason', { exact: true }).click();
  await expect(page.locator('.timeline-row pre')).toContainText(first.questions[1]);
});
