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

const fileEnv = parseEnv(await readFile(runEnvPath, "utf8"));
const base = process.env.KLENS_VERIFY_URL || fileEnv.KLENS_VERIFY_URL;
const topic = process.env.KLENS_VERIFY_TOPIC || fileEnv.KLENS_VERIFY_TOPIC || "klens-verify-topics";
const artifactDir = join(
  process.env.ARTIFACT_DIR || fileEnv.ARTIFACT_DIR || join(skillDir, "artifacts", "manual"),
  "command-palette",
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

function selectedOptions() {
  return page.locator('[cmdk-item][aria-selected="true"]');
}

try {
  await page.goto(base, { waitUntil: "domcontentloaded" });
  await page.getByRole("heading", { name: "Topics", exact: true }).waitFor({ timeout: 30_000 });
  notes.push(`landed ${page.url()}`);

  await page.getByRole("button", { name: /^Search/ }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("combobox").waitFor();

  const idleSelected = (await selectedOptions().allInnerTexts()).map((text) => text.trim());
  if (!idleSelected.some((text) => text === "Topics" || text.startsWith("Topics"))) {
    throw new Error(`expected idle selection on Go to Topics, got ${JSON.stringify(idleSelected)}`);
  }
  notes.push(`idle selected ${JSON.stringify(idleSelected)}`);

  await dialog.getByRole("combobox").fill(topic);
  await dialog.getByRole("option", { name: new RegExp(`^${topic}`) }).waitFor({ timeout: 15_000 });

  const afterSearch = (await selectedOptions().allInnerTexts()).map((text) => text.trim());
  if (afterSearch.length !== 1 || !afterSearch[0].startsWith(topic)) {
    throw new Error(`expected first hit selected, got ${JSON.stringify(afterSearch)}`);
  }
  if ((await dialog.getByRole("option", { name: "Topics", exact: true }).count()) !== 0) {
    throw new Error("Go to Topics stayed visible after catalog hits");
  }
  notes.push(`search selected ${JSON.stringify(afterSearch)}`);

  await page.screenshot({ path: join(artifactDir, "search-hit.png") });
  await writeFile(join(artifactDir, "search-hit.aria.yml"), await dialog.ariaSnapshot());

  await page.keyboard.press("ArrowDown");
  const afterDown = (await selectedOptions().allInnerTexts()).map((text) => text.trim());
  if (afterDown.length !== 1 || !afterDown[0].startsWith(topic)) {
    throw new Error(`ArrowDown left the hit, got ${JSON.stringify(afterDown)}`);
  }
  notes.push(`arrowdown selected ${JSON.stringify(afterDown)}`);

  await page.keyboard.press("Enter");
  await page.getByRole("heading", { name: topic }).waitFor({ timeout: 15_000 });
  const opened = page.url();
  if (!opened.endsWith(`/topics/${topic}`)) {
    throw new Error(`expected topic URL, got ${opened}`);
  }
  notes.push(`enter opened ${opened}`);

  await page.getByRole("button", { name: /^Search/ }).click();
  await dialog.getByRole("combobox").waitFor();
  await dialog.getByRole("combobox").fill("zzzz-no-such-entity");
  await dialog.getByText("No matches in local.").waitFor({ timeout: 10_000 });
  if ((await dialog.getByRole("option", { name: "Topics", exact: true }).count()) !== 0) {
    throw new Error("Go to Topics stayed visible on an empty search");
  }
  notes.push("empty state shown");
  await page.screenshot({ path: join(artifactDir, "no-matches.png") });

  await writeFile(join(artifactDir, "NOTES.txt"), `${notes.join("\n")}\n`);
  console.log(`drive-command-palette: ok ${artifactDir}`);
  for (const line of notes) console.log(`  ${line}`);
} finally {
  await browser.close();
}
