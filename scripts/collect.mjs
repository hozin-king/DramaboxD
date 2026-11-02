#!/usr/bin/env node
// Collector for the Dracin sync pipeline (v1).
// Reads seeds, runs the built CLI (dist/cli.js) per provider+seed, and writes
// a stream-URL-free payload to dracin-payload.json for the sync workflow step.
// No new dependencies: node built-ins only. This script NEVER posts to the
// sync endpoint itself — the GitHub Actions workflow does that separately.
// --dry-run: run only the first provider + first seed (cheap local check).
import { execFile } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CLI = path.join(ROOT, "dist", "cli.js");
const SEEDS_URL = "https://hozinking.my.id/api/dracin/seeds";
const SEEDS_FILE = process.env.DRACIN_SEEDS_FILE ?? path.join(ROOT, "dracin-seeds.json");
const OUT_FILE = path.join(ROOT, "dracin-payload.json");
const TOP_TITLES = 3; // bounded count per seed (CLI has no --count flag)
const EPISODE_PROVIDERS = new Set(["melolo", "freereels"]); // v1 limit
const STRIP_KEYS = new Set([
  "streamUrl", "stream_url", "m3u8", "m3u8Url", "mp4",
  "main_url", "backup_url", "backup_url_1", "video_url", "play_url",
  "cdnList", "videoPathList",
]);
const delayMs = Number(process.env.DRACIN_DELAY_MS ?? 1000) || 1000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function runCli(args) {
  return new Promise((resolve, reject) => {
    execFile(
      process.execPath,
      [CLI, ...args],
      { timeout: 240_000, maxBuffer: 64 * 1024 * 1024 },
      (err, stdout, stderr) => {
        if (err) reject(new Error((stderr || "").trim() || err.message));
        else resolve(stdout);
      },
    );
  });
}

async function cliJson(args) {
  return JSON.parse(await runCli([...args, "--json"]));
}

// Defense in depth: drop any stream-URL field, even if the CLI ever adds one.
// The payload below is built from a whitelist anyway; this only strips leftovers.
function stripUrls(value) {
  if (Array.isArray(value)) return value.map(stripUrls);
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (!STRIP_KEYS.has(k)) out[k] = stripUrls(v);
    }
    return out;
  }
  return value;
}

function validSeeds(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const out = {};
  for (const [provider, queries] of Object.entries(value)) {
    if (!Array.isArray(queries)) continue;
    const qs = queries.filter((q) => typeof q === "string" && q.trim() !== "");
    if (qs.length > 0) out[provider] = qs;
  }
  return Object.keys(out).length > 0 ? out : null;
}

async function loadSeeds() {
  try {
    const res = await fetch(SEEDS_URL, { signal: AbortSignal.timeout(10_000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const seeds = validSeeds(await res.json());
    if (!seeds) throw new Error("unexpected shape");
    return seeds;
  } catch (err) {
    console.log(`[seeds] remote fetch failed (${err.message}) — falling back to local seeds file`);
    const seeds = validSeeds(JSON.parse(await readFile(SEEDS_FILE, "utf8")));
    if (!seeds) throw new Error("dracin-seeds.json has an invalid shape");
    return seeds;
  }
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const seeds = await loadSeeds();
  let entries = Object.entries(seeds);
  if (dryRun) entries = entries.slice(0, 1).map(([p, qs]) => [p, qs.slice(0, 1)]);

  const titles = [];
  const seen = new Set();
  const providers = {};
  for (const [provider, queries] of entries) {
    let status = "ok";
    let searched = 0;
    let kept = 0;
    let error;
    try {
      for (const query of queries) {
        const results = await cliJson(["search", query, "--provider", provider]);
        await sleep(delayMs);
        searched += results.length;
        for (const s of results.slice(0, TOP_TITLES)) {
          try {
            const key = `${provider}:${s.id}`;
            if (seen.has(key)) continue;
            seen.add(key);
            let detail = null;
            let episodes = [];
            if (EPISODE_PROVIDERS.has(provider)) {
              detail = await cliJson(["detail", s.id, "--provider", provider]);
              await sleep(delayMs);
              try {
                const eps = await cliJson(["episodes", s.id, "--provider", provider]);
                episodes = eps.map((ep) => ({
                  idx: ep.index,
                  item_id: ep.id !== undefined ? String(ep.id) : String(ep.index),
                  duration_s: ep.duration ?? 0,
                  locked: ep.locked ?? false,
                }));
              } catch (err) {
                // Episodes may fail per title (e.g. upstream risk control);
                // keep the title metadata, mark the provider partial.
                status = "partial";
                console.log(`[episodes] ${provider} ${s.id} failed (${err.message}) — metadata only`);
              }
              await sleep(delayMs);
            }
            titles.push({
              platform: provider,
              title_id: String(s.id),
              title: detail?.title ?? s.title,
              cover_url: detail?.cover ?? s.cover,
              synopsis: detail?.synopsis,
              language: detail?.language ?? s.language,
              episode_count: detail?.episodeCount ?? s.episodeCount ?? 0,
              free_count: episodes.filter((e) => !e.locked).length,
              locked_count: episodes.filter((e) => e.locked).length,
              episodes,
            });
            kept++;
          } catch (err) {
            status = "partial";
            console.log(`[title] ${provider} ${s.id} failed (${err.message}) — skipped`);
          }
        }
      }
    } catch (err) {
      status = "failed";
      error = err.message;
      console.log(`[provider] ${provider} failed: ${err.message}`);
    }
    providers[provider] = { status, searched, titles: kept, ...(error ? { error } : {}) };
  }

  const payload = stripUrls({ generated_at: new Date().toISOString(), titles, providers });
  await writeFile(OUT_FILE, JSON.stringify(payload, null, 2));
  for (const [p, st] of Object.entries(providers)) {
    console.log(`${p}: status=${st.status} searched=${st.searched} titles=${st.titles}`);
  }
  console.log(`total titles: ${titles.length} -> dracin-payload.json`);
  if (dryRun) {
    console.log("[dry-run] first provider + first seed only; this script never posts to the sync endpoint.");
  }
}

main().catch((err) => {
  console.error(`collector failed: ${err.message}`);
  process.exit(1);
});
