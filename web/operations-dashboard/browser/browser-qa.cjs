const { chromium } = require('/tmp/qa/node_modules/playwright');
const fs = require('fs');

(async () => {
  const baseURL = process.env.DASHBOARD_QA_URL;
  const browser = await chromium.launch({headless: true});
  const evidence = {schema_version: 'namrbd.gui.browser-qa.v1', states: [], mutation_controls_enabled: false};
  try {
    for (const fixture of ['ok', 'degraded', 'stale']) {
      const page = await browser.newPage({viewport: {width: 1440, height: 1100}, deviceScaleFactor: 1});
      const consoleErrors = [], pageErrors = [];
      await page.addInitScript(() => {
        const fixedTime = Date.parse('2026-08-18T00:00:00Z');
        const NativeDate = Date;
        globalThis.Date = class extends NativeDate {
          constructor(...args) { super(...(args.length ? args : [fixedTime])); }
          static now() { return fixedTime; }
        };
      });
      page.on('console', msg => { if (msg.type() === 'error') consoleErrors.push(msg.text()); });
      page.on('pageerror', err => pageErrors.push(String(err)));
      const response = await page.goto(`${baseURL}/index.html?fixture=${fixture}`, {waitUntil: 'networkidle'});
      if (!response || !response.ok()) throw new Error(`${fixture}: navigation failed`);
      await page.locator('[data-dashboard-root] .topbar').waitFor();
      await page.addStyleTag({content: '*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important}'});
      const text = await page.locator('[data-dashboard-root]').innerText();
      if (!text.includes(`fixture ${fixture}`)) throw new Error(`${fixture}: fixture identity not rendered`);
      if (!text.toLowerCase().includes('read-only')) throw new Error(`${fixture}: read-only badge not rendered`);
      const mutationCount = await page.locator('button[data-action]:not([data-action="refresh"]), input[type=submit], form').count();
      if (mutationCount !== 0) throw new Error(`${fixture}: unexpected mutation controls=${mutationCount}`);
      if (consoleErrors.length || pageErrors.length) throw new Error(`${fixture}: browser errors console=${consoleErrors.join('|')} page=${pageErrors.join('|')}`);
      await page.screenshot({path: `/output/screenshots/${fixture}.png`, fullPage: true, animations: 'disabled'});
      evidence.states.push({fixture, http_status: response.status(), read_only_visible: true, mutation_control_count: 0, console_error_count: 0, page_error_count: 0, screenshot: `screenshots/${fixture}.png`});
      await page.close();
    }
    fs.writeFileSync('/output/browser-evidence.json', JSON.stringify(evidence, null, 2) + '\n');
  } finally { await browser.close(); }
})().catch(err => { console.error(err); process.exit(1); });
