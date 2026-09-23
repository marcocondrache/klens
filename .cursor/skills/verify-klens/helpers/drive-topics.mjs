import { chromium } from "playwright";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const skillDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const runDir = process.env.KLENS_VERIFY_RUN_DIR || join(skillDir, "run");

function readRun(name) {
  return readFileSync(join(runDir, name), "utf8").trim();
}

function fail(message) {
  throw new Error(message);
}

const url = readRun("url");
const runId = readRun("run-id");
const cluster = readRun("cluster");
const topic = readRun("topic");
const recordKey = readRun("record-key");
const recordValue = readRun("record-value");
const evidence = join(skillDir, "artifacts", runId, "topics");
mkdirSync(evidence, { recursive: true });

const chrome = process.env.CHROME || "/usr/bin/google-chrome";
const browser = await chromium.launch({
  executablePath: chrome,
  headless: true,
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });

try {
  await page.goto(`${url}/login`, { waitUntil: "domcontentloaded" });
  await page.waitForURL(new RegExp(`/cluster/${cluster}/topics(?:\\?|$)`), { timeout: 30_000 });
  const sso = await page.getByText("Continue with SSO").count();
  if (sso !== 0) {
    fail("Continue with SSO is visible. Auth-off /login should leave the login page.");
  }
  writeFileSync(join(evidence, "login-redirect.url.txt"), page.url() + "\n");

  const heading = page.getByRole("heading", { level: 1, name: "Topics" });
  await heading.waitFor({ state: "visible", timeout: 30_000 });
  await page.getByRole("link", { name: "klens" }).waitFor({ state: "visible" });
  const unreachable = await page.getByText("Cluster unreachable").count();
  if (unreachable !== 0) {
    fail("Cluster unreachable is visible. Topics is not ready.");
  }

  await page.screenshot({ path: join(evidence, "landing.png") });
  writeFileSync(join(evidence, "landing.aria.yml"), await page.locator("body").ariaSnapshot());

  const search = page.getByPlaceholder("Search topics…");
  await search.fill(topic);
  const row = page.getByRole("row", { name: topic });
  await row.waitFor({ state: "visible", timeout: 15_000 });
  await page.screenshot({ path: join(evidence, "filtered.png") });
  writeFileSync(join(evidence, "filtered.aria.yml"), await page.locator("body").ariaSnapshot());

  await row.click();
  await page.waitForURL(new RegExp(`/cluster/${cluster}/topics/${topic}$`), { timeout: 15_000 });
  await page.getByRole("heading", { level: 1 }).filter({ hasText: topic }).waitFor({ state: "visible" });
  await page.getByRole("tab", { name: "Data" }).waitFor({ state: "visible" });
  await page.getByRole("cell", { name: recordKey, exact: true }).waitFor({ state: "visible", timeout: 20_000 });
  await page.getByRole("cell", { name: recordValue, exact: true }).waitFor({ state: "visible", timeout: 20_000 });

  await page.screenshot({ path: join(evidence, "open.png") });
  writeFileSync(join(evidence, "open.aria.yml"), await page.locator("body").ariaSnapshot());
  writeFileSync(join(evidence, "open.url.txt"), page.url() + "\n");

  const topicsJson = await page.request.get(`${url}/api/clusters/${cluster}/topics`);
  if (!topicsJson.ok()) fail(`GET topics returned ${topicsJson.status()}`);
  writeFileSync(join(evidence, "topics.json"), await topicsJson.text());

  const recordsJson = await page.request.get(
    `${url}/api/clusters/${cluster}/topics/${encodeURIComponent(topic)}/records?order=NEWEST&limit=50`,
  );
  if (!recordsJson.ok()) fail(`GET records returned ${recordsJson.status()}`);
  const recordsBody = await recordsJson.text();
  writeFileSync(join(evidence, "records.json"), recordsBody);
  const records = JSON.parse(recordsBody);
  const hit = (records.records || []).some(
    (record) => record.key === recordKey && record.value === recordValue,
  );
  if (!hit) fail(`records JSON has no ${recordKey} / ${recordValue}`);

  writeFileSync(
    join(evidence, "NOTES.txt"),
    [
      "feature: topics",
      `run: ${runId}`,
      `url: ${page.url()}`,
      "login: /login redirected to the topics page and Continue with SSO was absent",
      `search: filtered to ${topic}`,
      `open: Data tab shows key ${recordKey} and value ${recordValue}`,
      "",
    ].join("\n"),
  );
} finally {
  await browser.close();
}

process.stdout.write(`topics proof written to ${evidence}\n`);
