const { chromium } = require('/tmp/qa/node_modules/playwright');
const fs = require('fs');

(async () => {
  const baseURL = process.env.DASHBOARD_QA_URL;
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage({viewport: {width: 1440, height: 1100}, deviceScaleFactor: 1});
    const consoleErrors = [], pageErrors = [];
    page.on('console', msg => { if (msg.type() === 'error') consoleErrors.push(msg.text()); });
    page.on('pageerror', err => pageErrors.push(String(err)));
    const response = await page.goto(`${baseURL}/console/`, {waitUntil: 'networkidle'});
    if (!response || !response.ok()) throw new Error('live console navigation failed');
    await page.locator('[data-dashboard-root] .topbar').waitFor();
    const text = await page.locator('[data-dashboard-root]').innerText();
    if (!text.toLowerCase().includes('read-only')) throw new Error('read-only badge not rendered');
    const mutationCount = await page.locator('button[data-action]:not([data-action="refresh"]), input[type=submit], form').count();
    if (mutationCount !== 0) throw new Error(`unexpected mutation controls=${mutationCount}`);
    if (consoleErrors.length || pageErrors.length) throw new Error(`browser errors console=${consoleErrors.join('|')} page=${pageErrors.join('|')}`);
    const cluster = await (await page.request.get(`${baseURL}/api/v1/sbs/cluster`)).json();
    if (!cluster.rbac_checked || !cluster.redaction_applied || !cluster.read_only_mode_enforced) throw new Error('RBAC/redaction/read-only contract flags missing');
    const post = await page.request.post(`${baseURL}/api/v1/sbs/cluster`);
    if (post.status() !== 405) throw new Error(`read-only POST returned ${post.status()}`);
    await page.screenshot({path: '/output/live-console.png', fullPage: true, animations: 'disabled'});
    const evidence = {
      schema_version: 'namrbd.gui.live-browser-qa.v1', result: 'ok', http_status: response.status(),
      read_only_visible: true, mutation_control_count: 0, console_error_count: 0, page_error_count: 0,
      rbac_contract_reported: true, redaction_contract_reported: true, read_only_post_rejected: true,
      authenticated_role_denial_executed: false,
      authenticated_role_denial_blocked_reason: 'sbs-service admin HTTP authentication is not implemented',
      screenshot: 'live-console.png', error_count: 0, first_error: null, last_error: null
    };
    fs.writeFileSync('/output/summary.json', JSON.stringify(evidence, null, 2) + '\n');
  } finally { await browser.close(); }
})().catch(err => { console.error(err); process.exit(1); });
