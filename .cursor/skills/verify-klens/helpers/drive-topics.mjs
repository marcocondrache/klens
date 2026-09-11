#!/usr/bin/env node
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const helpersDir = dirname(fileURLToPath(import.meta.url));
const skillDir = join(helpersDir, "..");
const runEnvPath = process.env.KLENS_VERIFY_ENV || join(skillDir, "run", "env");

function parseEnv(text) {
  const out = {};
  for (const line of text.split("\n")) {
    const cut = line.indexOf("=");
    if (cut <= 0) continue;
    out[line.slice(0, cut)] = line.slice(cut + 1);
  }
  return out;
}

async function graphql(base, query) {
  const response = await fetch(`${base}/graphql`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query }),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`graphql ${response.status}: ${body}`);
  }
  return body;
}

const fileEnv = parseEnv(await readFile(runEnvPath, "utf8"));
const base = process.env.KLENS_VERIFY_URL || fileEnv.KLENS_VERIFY_URL;
const topic = process.env.KLENS_VERIFY_TOPIC || fileEnv.KLENS_VERIFY_TOPIC || "klens-verify-topics";
const artifactDir = join(
  process.env.ARTIFACT_DIR || fileEnv.ARTIFACT_DIR || join(skillDir, "artifacts", "manual"),
  "topics",
);
const chrome = process.env.CHROME || "/usr/bin/google-chrome";

if (!base) {
  throw new Error("KLENS_VERIFY_URL missing. Run helpers/launch.sh first.");
}

await mkdir(artifactDir, { recursive: true });

const browser = await chromium.launch({
  executablePath: chrome,
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const notes = [];

try {
  await page.goto(base, { waitUntil: "domcontentloaded" });
  await page.getByRole("heading", { name: "Topics", exact: true }).waitFor({ timeout: 30_000 });
  await page.getByText(topic, { exact: true }).waitFor({ timeout: 30_000 });

  const landingUrl = page.url();
  if (!landingUrl.includes("/cluster/") || !landingUrl.endsWith("/topics")) {
    throw new Error(`expected /cluster/<name>/topics, got ${landingUrl}`);
  }

  await page.screenshot({ path: join(artifactDir, "landing.png"), fullPage: true });
  await writeFile(join(artifactDir, "landing.aria.yml"), await page.locator("body").ariaSnapshot());
  notes.push(`landed ${landingUrl}`);

  const search = page.getByPlaceholder("Search topics…");
  await search.fill(topic);
  await page.waitForFunction(
    (expected) => new URL(window.location.href).searchParams.get("q") === expected,
    topic,
  );
  await page.getByText(topic, { exact: true }).waitFor();
  await page.screenshot({ path: join(artifactDir, "filtered.png"), fullPage: true });
  await writeFile(join(artifactDir, "filtered.aria.yml"), await page.locator("body").ariaSnapshot());
  notes.push(`filtered ${page.url()}`);

  await page.getByText(topic, { exact: true }).click();
  await page.getByRole("heading", { name: topic }).waitFor({ timeout: 15_000 });
  const openUrl = page.url();
  if (!openUrl.endsWith(`/topics/${topic}`)) {
    throw new Error(`expected topic detail URL, got ${openUrl}`);
  }
  await page.screenshot({ path: join(artifactDir, "open.png"), fullPage: true });
  await writeFile(join(artifactDir, "open.aria.yml"), await page.locator("body").ariaSnapshot());
  notes.push(`opened ${openUrl}`);

  const topicsJson = await graphql(
    base,
    'query { topics(cluster: "local") { name internal messageCount } }',
  );
  await writeFile(join(artifactDir, "topics.json"), `${topicsJson}\n`);
  if (!topicsJson.includes(topic)) {
    throw new Error(`graphql topics missing ${topic}: ${topicsJson}`);
  }

  await writeFile(join(artifactDir, "NOTES.txt"), `${notes.join("\n")}\n`);
  console.log(`drive-topics: ok ${artifactDir}`);
  for (const line of notes) console.log(`  ${line}`);
} finally {
  await browser.close();
}
