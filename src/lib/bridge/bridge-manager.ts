/**
 * Bridge Manager — singleton orchestrator for the multi-IM bridge system.
 *
 * Manages adapter lifecycles, routes inbound messages through the
 * conversation engine, and coordinates permission handling.
 *
 * Uses globalThis to survive Next.js HMR in development.
 */

import type {
  BridgeStatus,
  InboundMessage,
  OutboundMessage,
  StreamingPreviewState,
} from "./types.js";
import { createAdapter, getRegisteredTypes } from "./channel-adapter.js";
import type { BaseChannelAdapter } from "./channel-adapter.js";
// Side-effect import: triggers self-registration of all adapter factories
import "./adapters/index.js";
import * as router from "./channel-router.js";
import * as engine from "./conversation-engine.js";
import * as broker from "./permission-broker.js";
import { deliver, deliverRendered } from "./delivery-layer.js";
import { markdownToTelegramChunks } from "./markdown/telegram.js";
import { markdownToDiscordChunks } from "./markdown/discord.js";
import { getBridgeContext } from "./context.js";
import { escapeHtml } from "./adapters/telegram-utils.js";
import {
  validateWorkingDirectory,
  validateSessionId,
  isDangerousInput,
  sanitizeInput,
  validateMode,
} from "./security/validators.js";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import * as readline from "readline";
import { sendImage, sendFile } from "./adapters/feishu-file-capabilities.js";

const GLOBAL_KEY = "__bridge_manager__";

// ── B3: File path detection helpers ──────────────────────────────

const FILE_EXTENSIONS =
  "(pdf|png|jpg|jpeg|gif|xlsx|docx|pptx|html|md|csv|svg|webp)";

/** Check if a file path is safe to send (not in sensitive dirs/names). */
export function isPathSafe(filePath: string): boolean {
  const resolved = path.resolve(filePath);
  const home = os.homedir();
  const BLOCKED_PREFIXES = [
    path.join(home, ".ssh"),
    path.join(home, ".aws"),
    path.join(home, ".gnupg"),
    path.join(home, ".kube"),
    path.join(home, ".docker"),
    "/etc/",
  ];
  if (BLOCKED_PREFIXES.some((p) => resolved.startsWith(p))) return false;
  const BLOCKED_NAMES = [
    ".env",
    "credentials",
    "id_rsa",
    "id_ed25519",
    ".netrc",
  ];
  if (BLOCKED_NAMES.some((n) => path.basename(resolved).includes(n)))
    return false;
  const dangerCheck = isDangerousInput(filePath);
  if (dangerCheck.dangerous) return false;
  return true;
}

/** Check if a file path refers to an image by extension. */
export function isImageFile(filePath: string): boolean {
  const ext = path.extname(filePath).slice(1).toLowerCase();
  return [
    "png",
    "jpg",
    "jpeg",
    "gif",
    "bmp",
    "webp",
    "svg",
    "tiff",
    "heic",
  ].includes(ext);
}

/** Extract local file paths mentioned in Claude's response text. */
export function extractLocalFilePaths(text: string): string[] {
  const patterns = [
    new RegExp(
      `(?:保存到|saved to|wrote to|created|generated|生成)\\s+(?:文件\\s*)?[\`"]?([~/\\\\/][^\\s\`"\\n]{1,256}\\.${FILE_EXTENSIONS})[\`"]?`,
      "gi",
    ),
    new RegExp(
      `(?:文件|file|output)[\\s:：]*[\`"]?([~/\\\\/][^\\s\`"\\n]{1,256}\\.${FILE_EXTENSIONS})[\`"]?`,
      "gi",
    ),
    new RegExp(
      `^\\s*[-*]?\\s*[\`"]?(\\/[^\\s\`"\\n]{1,256}\\.${FILE_EXTENSIONS})[\`"]?\\s*$`,
      "gmi",
    ),
  ];
  const candidates = new Set<string>();
  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) {
      let fp = match[1].replace(/[.。,，;；）)】\]]+$/, "");
      if (fp.startsWith("~")) fp = fp.replace("~", os.homedir());
      candidates.add(fp);
    }
  }
  return [...candidates].filter((fp) => {
    try {
      if (!isPathSafe(fp)) return false;
      const stat = fs.statSync(fp);
      if (!stat.isFile()) return false;
      if (stat.size > 30 * 1024 * 1024) {
        console.warn(
          `[bridge] File too large for auto-send: ${fp} (${(stat.size / 1024 / 1024).toFixed(1)}MB)`,
        );
        return false;
      }
      return true;
    } catch {
      return false;
    }
  });
}

// ── C1: /cwd --new argument parsing ──────────────────────────────

/** Parse /cwd args, extracting path and --new flag. */
export function parseCwdArgs(args: string): {
  path: string;
  hasNew: boolean;
} {
  const hasNew = /\s+--new\s*$/.test(args) || /^--new\s+/.test(args);
  const pathArg = args
    .replace(/\s+--new\s*$/, "")
    .replace(/^--new\s+/, "")
    .trim();
  return { path: pathArg, hasNew };
}

// ── C2: Session scanning helpers ─────────────────────────────────

/** Convert an absolute path to Claude's project directory name format. */
export function pathToProjectDir(absPath: string): string {
  return absPath.replace(/\//g, "-");
}

export interface SessionInfo {
  id: string;
  lastModified: Date;
  size: number;
  summary: string;
}

/**
 * Scan Claude Code session files for a given working directory.
 * @param workDir - the working directory to scan for
 * @param projectDirOverride - override the project directory path (for testing)
 */
export async function scanClaudeSessions(
  workDir: string,
  projectDirOverride?: string,
): Promise<SessionInfo[]> {
  const projectDir =
    projectDirOverride ||
    path.join(os.homedir(), ".claude/projects", pathToProjectDir(workDir));
  try {
    const files = fs.readdirSync(projectDir);
    const sessions: SessionInfo[] = [];
    for (const file of files) {
      if (!file.endsWith(".jsonl")) continue;
      try {
        const sessionId = file.replace(".jsonl", "");
        const fp = path.join(projectDir, file);
        const stat = fs.statSync(fp);
        const summary = await readFirstUserMessage(fp);
        sessions.push({
          id: sessionId,
          lastModified: stat.mtime,
          size: stat.size,
          summary,
        });
      } catch (fileErr) {
        console.warn(
          `[bridge-manager] Failed to read session ${file}:`,
          fileErr,
        );
      }
    }
    return sessions.sort(
      (a, b) => b.lastModified.getTime() - a.lastModified.getTime(),
    );
  } catch (err: unknown) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw err;
  }
}

/** Read the first meaningful user message from a Claude session JSONL file. */
export async function readFirstUserMessage(filePath: string): Promise<string> {
  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const lines = readline.createInterface({ input: fileStream });
  try {
    for await (const line of lines) {
      try {
        const entry = JSON.parse(line);
        if (entry.type === "user" && entry.message?.content) {
          let text = "";
          if (typeof entry.message.content === "string") {
            text = entry.message.content;
          } else if (Array.isArray(entry.message.content)) {
            text = entry.message.content
              .filter((b: { type: string }) => b.type === "text")
              .map((b: { text: string }) => b.text)
              .join(" ");
          }
          // Skip queue-operation entries
          if (text.startsWith("queue-operation")) continue;
          lines.close();
          return text.slice(0, 80).replace(/\n/g, " ").trim() || "[empty]";
        }
      } catch {
        /* skip malformed lines */
      }
    }
    return "[no user message]";
  } catch {
    return "[error]";
  } finally {
    lines.close();
    fileStream.destroy();
  }
}

// ── C3: /resume target resolution ────────────────────────────────

/** Resolve the target session ID for /resume command. Pure logic, no side effects. */
export function resolveResumeTarget(
  args: string,
  cachedSessions: SessionInfo[] | null,
): { sessionId: string | null; error?: string } {
  // Pure numeric → index from cache
  if (/^\d+$/.test(args)) {
    if (!cachedSessions || cachedSessions.length === 0) {
      return { sessionId: null, error: "请先执行 /sessions 获取会话列表" };
    }
    const idx = parseInt(args) - 1;
    if (idx < 0 || idx >= cachedSessions.length) {
      return {
        sessionId: null,
        error: `序号超出范围（1-${cachedSessions.length}）`,
      };
    }
    return { sessionId: cachedSessions[idx].id };
  }
  // Full UUID format
  if (validateSessionId(args)) {
    return { sessionId: args };
  }
  // Short prefix match
  if (!cachedSessions || cachedSessions.length === 0) {
    return { sessionId: null, error: "请先执行 /sessions 获取会话列表" };
  }
  const matches = cachedSessions.filter((s) => s.id.startsWith(args));
  if (matches.length === 0) {
    return {
      sessionId: null,
      error: `未找到前缀为 "${escapeHtml(args)}" 的会话`,
    };
  }
  if (matches.length > 1) {
    const list = matches
      .slice(0, 5)
      .map((s) => `<code>${s.id.slice(0, 12)}...</code>`)
      .join(", ");
    return {
      sessionId: null,
      error: `匹配到 ${matches.length} 个会话，请更精确：${list}`,
    };
  }
  return { sessionId: matches[0].id };
}

/** Format a Date as a relative time string. */
function formatTimeAgo(date: Date): string {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
  if (seconds < 60) return "刚刚";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}分钟前`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}小时前`;
  if (seconds < 604800) return `${Math.floor(seconds / 86400)}天前`;
  return date.toLocaleDateString("zh-CN");
}

// Sessions cache for /resume command (binding.id → last session list)
const sessionsCache = new Map<string, SessionInfo[]>();

// Projects cache for /cwd <number> command
let projectsCache: string[] = [];

// Session aliases: keyed by codepilotSessionId → user-defined name.
// Persisted to ~/.claude-to-im/data/aliases.json for restart survival.
const sessionAliases = new Map<string, string>();

const ALIASES_FILE = path.join(
  os.homedir(),
  ".claude-to-im",
  "data",
  "aliases.json",
);

function loadAliases(): void {
  try {
    const raw = fs.readFileSync(ALIASES_FILE, "utf8");
    const obj = JSON.parse(raw) as Record<string, string>;
    for (const [k, v] of Object.entries(obj)) {
      if (typeof k === "string" && typeof v === "string") {
        sessionAliases.set(k, v);
      }
    }
  } catch {
    // File doesn't exist yet — ok
  }
}

function saveAliases(): void {
  try {
    fs.writeFileSync(
      ALIASES_FILE,
      JSON.stringify(Object.fromEntries(sessionAliases), null, 2),
      "utf8",
    );
  } catch (err) {
    console.warn(
      "[bridge-manager] Failed to save aliases:",
      err instanceof Error ? err.message : err,
    );
  }
}

/**
 * Decode a Claude project directory name back to an absolute path.
 * Encoding is lossy (/ → - but - also exists in dir names), so we verify
 * against the filesystem. Tries progressively shorter prefix matches.
 */
export function decodeProjectDir(encoded: string): string | null {
  // encoded: "-Users-aocai-AI-skill-hub" → should be "/Users/aocai/AI/skill-hub"
  // Strategy: start from left, greedily match filesystem directories
  const parts = encoded.slice(1).split("-"); // remove leading -, split by -
  let current = "";
  let result = "/";

  for (let i = 0; i < parts.length; i++) {
    const candidate = current ? `${current}-${parts[i]}` : parts[i];
    const asDir = result + candidate;

    if (fs.existsSync(asDir)) {
      // This segment exists as a directory, commit it
      result = asDir + "/";
      current = "";
    } else if (i < parts.length - 1) {
      // Doesn't exist yet, might be part of a hyphenated name
      current = candidate;
    } else {
      // Last part: commit whatever we have
      result = result + candidate;
      current = "";
    }
  }

  if (current) {
    result = result + current;
  }

  // Remove trailing /
  result = result.replace(/\/$/, "");

  // Verify the decoded path actually exists
  return fs.existsSync(result) ? result : null;
}

/**
 * List all known project directories from ~/.claude/projects/.
 * Returns decoded absolute paths sorted by directory mtime (most recent first).
 */
export function listKnownProjects(): string[] {
  const projectsRoot = path.join(os.homedir(), ".claude/projects");
  try {
    const entries = fs.readdirSync(projectsRoot, { withFileTypes: true });
    const projects: { absPath: string; mtime: number }[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      if (!entry.name.startsWith("-")) continue; // must start with -
      const absPath = decodeProjectDir(entry.name);
      if (!absPath) continue; // couldn't decode or path doesn't exist
      try {
        const stat = fs.statSync(path.join(projectsRoot, entry.name));
        projects.push({ absPath, mtime: stat.mtimeMs });
      } catch {
        continue;
      }
    }
    return projects.sort((a, b) => b.mtime - a.mtime).map((p) => p.absPath);
  } catch {
    return [];
  }
}

// ── Previous SDK Session ID storage (for /back command) ─────
// Keyed by binding.id → previous sdkSessionId before /new cleared it.
const previousSdkSessionIds = new Map<string, string>();

// ── Streaming preview helpers ──────────────────────────────────

/** Generate a non-zero random 31-bit integer for use as draft_id. */
function generateDraftId(): number {
  return Math.floor(Math.random() * 0x7ffffffe) + 1; // 1 .. 2^31-1
}

interface StreamConfig {
  intervalMs: number;
  minDeltaChars: number;
  maxChars: number;
}

/** Default stream config per channel type. */
const STREAM_DEFAULTS: Record<string, StreamConfig> = {
  telegram: { intervalMs: 700, minDeltaChars: 20, maxChars: 3900 },
  discord: { intervalMs: 1500, minDeltaChars: 40, maxChars: 1900 },
  feishu: { intervalMs: 300, minDeltaChars: 10, maxChars: 4000 },
};

function getStreamConfig(channelType = "telegram"): StreamConfig {
  const { store } = getBridgeContext();
  const defaults = STREAM_DEFAULTS[channelType] || STREAM_DEFAULTS.telegram;
  const prefix = `bridge_${channelType}_stream_`;
  const intervalMs =
    parseInt(store.getSetting(`${prefix}interval_ms`) || "", 10) ||
    defaults.intervalMs;
  const minDeltaChars =
    parseInt(store.getSetting(`${prefix}min_delta_chars`) || "", 10) ||
    defaults.minDeltaChars;
  const maxChars =
    parseInt(store.getSetting(`${prefix}max_chars`) || "", 10) ||
    defaults.maxChars;
  return { intervalMs, minDeltaChars, maxChars };
}

/** Fire-and-forget: send a preview draft. Only degrades on permanent failure. */
function flushPreview(
  adapter: BaseChannelAdapter,
  state: StreamingPreviewState,
  config: StreamConfig,
): void {
  if (state.degraded || !adapter.sendPreview) return;

  const text =
    state.pendingText.length > config.maxChars
      ? state.pendingText.slice(0, config.maxChars) + "..."
      : state.pendingText;

  state.lastSentText = text;
  state.lastSentAt = Date.now();

  adapter
    .sendPreview(state.chatId, text, state.draftId)
    .then((result) => {
      if (result === "degrade") state.degraded = true;
      // 'skip' — transient failure, next flush will retry naturally
    })
    .catch(() => {
      // Network error — transient, don't degrade
    });
}

// ── Channel-aware rendering dispatch ──────────────────────────

import type { ChannelAddress, SendResult } from "./types.js";

/**
 * Render response text and deliver via the appropriate channel format.
 * Telegram: Markdown → HTML chunks via deliverRendered.
 * Other channels: plain text via deliver (no HTML).
 *
 * If the adapter implements `deliverFinal`, that method is called instead
 * of the built-in logic, allowing adapters (e.g. Feishu) to update an
 * existing streaming card rather than posting a new message.
 */
async function deliverResponse(
  adapter: BaseChannelAdapter,
  address: ChannelAddress,
  responseText: string,
  sessionId: string,
  replyToMessageId?: string,
): Promise<SendResult> {
  // Delegate to adapter-specific final delivery if implemented
  if (adapter.deliverFinal) {
    return adapter.deliverFinal(address, responseText, sessionId);
  }
  if (adapter.channelType === "telegram") {
    const chunks = markdownToTelegramChunks(responseText, 4096);
    if (chunks.length > 0) {
      return deliverRendered(adapter, address, chunks, {
        sessionId,
        replyToMessageId,
      });
    }
    return { ok: true };
  }
  if (adapter.channelType === "discord") {
    // Discord: native markdown, chunk at 2000 chars with fence repair
    const chunks = markdownToDiscordChunks(responseText, 2000);
    for (let i = 0; i < chunks.length; i++) {
      const result = await deliver(
        adapter,
        {
          address,
          text: chunks[i].text,
          parseMode: "Markdown",
          replyToMessageId,
        },
        { sessionId },
      );
      if (!result.ok) return result;
    }
    return { ok: true };
  }
  if (adapter.channelType === "feishu") {
    // Feishu: pass markdown through for adapter to format as post/card
    return deliver(
      adapter,
      {
        address,
        text: responseText,
        parseMode: "Markdown",
        replyToMessageId,
      },
      { sessionId },
    );
  }
  // Generic fallback: deliver as plain text (deliver() handles chunking internally)
  return deliver(
    adapter,
    {
      address,
      text: responseText,
      parseMode: "plain",
      replyToMessageId,
    },
    { sessionId },
  );
}

interface AdapterMeta {
  lastMessageAt: string | null;
  lastError: string | null;
}

interface BridgeManagerState {
  adapters: Map<string, BaseChannelAdapter>;
  adapterMeta: Map<string, AdapterMeta>;
  running: boolean;
  startedAt: string | null;
  loopAborts: Map<string, AbortController>;
  activeTasks: Map<string, AbortController>;
  /** Per-session processing chains for concurrency control */
  sessionLocks: Map<string, Promise<void>>;
  autoStartChecked: boolean;
}

function getState(): BridgeManagerState {
  const g = globalThis as unknown as Record<string, BridgeManagerState>;
  if (!g[GLOBAL_KEY]) {
    g[GLOBAL_KEY] = {
      adapters: new Map(),
      adapterMeta: new Map(),
      running: false,
      startedAt: null,
      loopAborts: new Map(),
      activeTasks: new Map(),
      sessionLocks: new Map(),
      autoStartChecked: false,
    };
  }
  // Backfill sessionLocks for states created before this field existed
  if (!g[GLOBAL_KEY].sessionLocks) {
    g[GLOBAL_KEY].sessionLocks = new Map();
  }
  return g[GLOBAL_KEY];
}

/**
 * Process a function with per-session serialization.
 * Different sessions run concurrently; same-session requests are serialized.
 */
function processWithSessionLock(
  sessionId: string,
  fn: () => Promise<void>,
): Promise<void> {
  const state = getState();
  const prev = state.sessionLocks.get(sessionId) || Promise.resolve();
  const current = prev.then(fn, fn);
  state.sessionLocks.set(sessionId, current);
  // Cleanup when the chain completes.
  // Suppress rejection on the cleanup chain — callers handle errors on `current` directly.
  current
    .finally(() => {
      if (state.sessionLocks.get(sessionId) === current) {
        state.sessionLocks.delete(sessionId);
      }
    })
    .catch(() => {});
  return current;
}

/**
 * Start the bridge system.
 * Checks feature flags, registers enabled adapters, starts polling loops.
 */
export async function start(): Promise<void> {
  const state = getState();
  if (state.running) return;

  // Load persisted aliases before processing any messages
  loadAliases();

  const { store, lifecycle } = getBridgeContext();

  const bridgeEnabled = store.getSetting("remote_bridge_enabled") === "true";
  if (!bridgeEnabled) {
    console.log(
      "[bridge-manager] Bridge not enabled (remote_bridge_enabled != true)",
    );
    return;
  }

  // Iterate all registered adapter types and create those that are enabled
  for (const channelType of getRegisteredTypes()) {
    const settingKey = `bridge_${channelType}_enabled`;
    if (store.getSetting(settingKey) !== "true") continue;

    const adapter = createAdapter(channelType);
    if (!adapter) continue;

    const configError = adapter.validateConfig();
    if (!configError) {
      registerAdapter(adapter);
    } else {
      console.warn(
        `[bridge-manager] ${channelType} adapter not valid:`,
        configError,
      );
    }
  }

  // Start all registered adapters, track how many succeeded
  let startedCount = 0;
  for (const [type, adapter] of state.adapters) {
    try {
      await adapter.start();
      console.log(`[bridge-manager] Started adapter: ${type}`);
      startedCount++;
    } catch (err) {
      console.error(`[bridge-manager] Failed to start adapter ${type}:`, err);
    }
  }

  // Only mark as running if at least one adapter started successfully
  if (startedCount === 0) {
    console.warn(
      "[bridge-manager] No adapters started successfully, bridge not activated",
    );
    state.adapters.clear();
    state.adapterMeta.clear();
    return;
  }

  // Mark running BEFORE starting consumer loops — runAdapterLoop checks
  // state.running in its while-condition, so it must be true first.
  state.running = true;
  state.startedAt = new Date().toISOString();

  // Notify host that bridge is starting (e.g., suppress competing polling)
  lifecycle.onBridgeStart?.();

  // Now start the consumer loops (state.running is already true)
  for (const [, adapter] of state.adapters) {
    if (adapter.isRunning()) {
      runAdapterLoop(adapter);
    }
  }

  console.log(
    `[bridge-manager] Bridge started with ${startedCount} adapter(s)`,
  );
}

/**
 * Stop the bridge system gracefully.
 */
export async function stop(): Promise<void> {
  const state = getState();
  if (!state.running) return;

  const { lifecycle } = getBridgeContext();

  state.running = false;

  // Abort all event loops
  for (const [, abort] of state.loopAborts) {
    abort.abort();
  }
  state.loopAborts.clear();

  // Stop all adapters
  for (const [type, adapter] of state.adapters) {
    try {
      await adapter.stop();
      console.log(`[bridge-manager] Stopped adapter: ${type}`);
    } catch (err) {
      console.error(`[bridge-manager] Error stopping adapter ${type}:`, err);
    }
  }

  state.adapters.clear();
  state.adapterMeta.clear();
  state.startedAt = null;

  // Notify host that bridge stopped
  lifecycle.onBridgeStop?.();

  console.log("[bridge-manager] Bridge stopped");
}

/**
 * Lazy auto-start: checks bridge_auto_start setting once and starts if enabled.
 * Called from POST /api/bridge with action 'auto-start' (triggered by Electron on startup).
 */
export function tryAutoStart(): void {
  const state = getState();
  if (state.autoStartChecked) return;
  state.autoStartChecked = true;

  if (state.running) return;

  const { store } = getBridgeContext();
  const autoStart = store.getSetting("bridge_auto_start");
  if (autoStart !== "true") return;

  start().catch((err) => {
    console.error("[bridge-manager] Auto-start failed:", err);
  });
}

/**
 * Get the current bridge status.
 */
export function getStatus(): BridgeStatus {
  const state = getState();
  return {
    running: state.running,
    startedAt: state.startedAt,
    adapters: Array.from(state.adapters.entries()).map(([type, adapter]) => {
      const meta = state.adapterMeta.get(type);
      return {
        channelType: adapter.channelType,
        running: adapter.isRunning(),
        connectedAt: state.startedAt,
        lastMessageAt: meta?.lastMessageAt ?? null,
        error: meta?.lastError ?? null,
      };
    }),
  };
}

/**
 * Register a channel adapter.
 */
export function registerAdapter(adapter: BaseChannelAdapter): void {
  const state = getState();
  state.adapters.set(adapter.channelType, adapter);
}

/**
 * Run the event loop for a single adapter.
 * Messages for different sessions are dispatched concurrently;
 * messages for the same session are serialized via session locks.
 */
function runAdapterLoop(adapter: BaseChannelAdapter): void {
  const state = getState();
  const abort = new AbortController();
  state.loopAborts.set(adapter.channelType, abort);

  (async () => {
    while (state.running && adapter.isRunning()) {
      try {
        const msg = await adapter.consumeOne();
        if (!msg) continue; // Adapter stopped

        // Callback queries and commands are lightweight — process inline.
        // Regular messages use per-session locking for concurrency.
        if (msg.callbackData || msg.text.trim().startsWith("/")) {
          await handleMessage(adapter, msg);
        } else {
          const binding = router.resolve(msg.address);
          // Fire-and-forget into session lock — loop continues to accept
          // messages for other sessions immediately.
          processWithSessionLock(binding.codepilotSessionId, () =>
            handleMessage(adapter, msg),
          ).catch((err) => {
            console.error(
              `[bridge-manager] Session ${binding.codepilotSessionId.slice(0, 8)} error:`,
              err,
            );
          });
        }
      } catch (err) {
        if (abort.signal.aborted) break;
        const errMsg = err instanceof Error ? err.message : String(err);
        console.error(
          `[bridge-manager] Error in ${adapter.channelType} loop:`,
          err,
        );
        // Track last error per adapter
        const meta = state.adapterMeta.get(adapter.channelType) || {
          lastMessageAt: null,
          lastError: null,
        };
        meta.lastError = errMsg;
        state.adapterMeta.set(adapter.channelType, meta);
        // Brief delay to prevent tight error loops
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  })().catch((err) => {
    if (!abort.signal.aborted) {
      const errMsg = err instanceof Error ? err.message : String(err);
      console.error(
        `[bridge-manager] ${adapter.channelType} loop crashed:`,
        err,
      );
      const meta = state.adapterMeta.get(adapter.channelType) || {
        lastMessageAt: null,
        lastError: null,
      };
      meta.lastError = errMsg;
      state.adapterMeta.set(adapter.channelType, meta);
    }
  });
}

/**
 * Handle a single inbound message.
 */
async function handleMessage(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
): Promise<void> {
  const { store } = getBridgeContext();

  // Update lastMessageAt for this adapter
  const adapterState = getState();
  const meta = adapterState.adapterMeta.get(adapter.channelType) || {
    lastMessageAt: null,
    lastError: null,
  };
  meta.lastMessageAt = new Date().toISOString();
  adapterState.adapterMeta.set(adapter.channelType, meta);

  // Acknowledge the update offset after processing completes (or fails).
  // This ensures the adapter only advances its committed offset once the
  // message has been fully handled, preventing message loss on crash.
  const ack = () => {
    if (msg.updateId != null && adapter.acknowledgeUpdate) {
      adapter.acknowledgeUpdate(msg.updateId);
    }
  };

  // Handle callback queries (permission buttons)
  if (msg.callbackData) {
    const handled = broker.handlePermissionCallback(
      msg.callbackData,
      msg.address.chatId,
      msg.callbackMessageId,
    );
    if (handled) {
      // Send confirmation
      const confirmMsg: OutboundMessage = {
        address: msg.address,
        text: "Permission response recorded.",
        parseMode: "plain",
      };
      await deliver(adapter, confirmMsg);
    }
    ack();
    return;
  }

  const rawText = msg.text.trim();
  const hasAttachments = msg.attachments && msg.attachments.length > 0;

  // Handle image-only download failures — surface error to user instead of silently dropping
  if (!rawText && !hasAttachments) {
    const rawData = msg.raw as
      | { imageDownloadFailed?: boolean; failedCount?: number }
      | undefined;
    if (rawData?.imageDownloadFailed) {
      await deliver(adapter, {
        address: msg.address,
        text: `Failed to download ${rawData.failedCount ?? 1} image(s). Please try sending again.`,
        parseMode: "plain",
        replyToMessageId: msg.messageId,
      });
    }
    ack();
    return;
  }

  // Check for IM commands (before sanitization — commands are validated individually)
  if (rawText.startsWith("/")) {
    await handleCommand(adapter, msg, rawText);
    ack();
    return;
  }

  // Sanitize general message text before routing to conversation engine
  const { text, truncated } = sanitizeInput(rawText);
  if (truncated) {
    console.warn(
      `[bridge-manager] Input truncated from ${rawText.length} to ${text.length} chars for chat ${msg.address.chatId}`,
    );
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: "inbound",
      messageId: msg.messageId,
      summary: `[TRUNCATED] Input truncated from ${rawText.length} chars`,
    });
  }

  if (!text && !hasAttachments) {
    ack();
    return;
  }

  // Regular message — route to conversation engine
  const binding = router.resolve(msg.address);

  // Notify adapter that message processing is starting (e.g., typing indicator)
  adapter.onMessageStart?.(msg.address.chatId);

  // Create an AbortController so /stop can cancel this task externally
  const taskAbort = new AbortController();
  const state = getState();
  state.activeTasks.set(binding.codepilotSessionId, taskAbort);

  // ── Streaming preview setup ──────────────────────────────────
  let previewState: StreamingPreviewState | null = null;
  const caps = adapter.getPreviewCapabilities?.(msg.address.chatId) ?? null;
  if (caps?.supported) {
    previewState = {
      draftId: generateDraftId(),
      chatId: msg.address.chatId,
      lastSentText: "",
      lastSentAt: 0,
      degraded: false,
      throttleTimer: null,
      pendingText: "",
    };
  }

  const streamCfg = previewState ? getStreamConfig(adapter.channelType) : null;

  // Build the onPartialText callback (or undefined if preview not supported)
  const onPartialText =
    previewState && streamCfg
      ? (fullText: string) => {
          const ps = previewState!;
          const cfg = streamCfg!;
          if (ps.degraded) return;

          // Truncate to maxChars + ellipsis
          ps.pendingText =
            fullText.length > cfg.maxChars
              ? fullText.slice(0, cfg.maxChars) + "..."
              : fullText;

          const delta = ps.pendingText.length - ps.lastSentText.length;
          const elapsed = Date.now() - ps.lastSentAt;

          if (delta < cfg.minDeltaChars && ps.lastSentAt > 0) {
            // Not enough new content — schedule trailing-edge timer if not already set
            if (!ps.throttleTimer) {
              ps.throttleTimer = setTimeout(() => {
                ps.throttleTimer = null;
                if (!ps.degraded) flushPreview(adapter, ps, cfg);
              }, cfg.intervalMs);
            }
            return;
          }

          if (elapsed < cfg.intervalMs && ps.lastSentAt > 0) {
            // Too soon — schedule trailing-edge timer to ensure latest text is sent
            if (!ps.throttleTimer) {
              ps.throttleTimer = setTimeout(() => {
                ps.throttleTimer = null;
                if (!ps.degraded) flushPreview(adapter, ps, cfg);
              }, cfg.intervalMs - elapsed);
            }
            return;
          }

          // Clear any pending trailing-edge timer and flush immediately
          if (ps.throttleTimer) {
            clearTimeout(ps.throttleTimer);
            ps.throttleTimer = null;
          }
          flushPreview(adapter, ps, cfg);
        }
      : undefined;

  try {
    // Pass permission callback so requests are forwarded to IM immediately
    // during streaming (the stream blocks until permission is resolved).
    // Use text or empty string for image-only messages (prompt is still required by streamClaude)
    const promptText = text || (hasAttachments ? "Describe this image." : "");

    const result = await engine.processMessage(
      binding,
      promptText,
      async (perm) => {
        await broker.forwardPermissionRequest(
          adapter,
          msg.address,
          perm.permissionRequestId,
          perm.toolName,
          perm.toolInput,
          binding.codepilotSessionId,
          perm.suggestions,
          msg.messageId,
        );
      },
      taskAbort.signal,
      hasAttachments ? msg.attachments : undefined,
      onPartialText,
    );

    // Send response text — render via channel-appropriate format
    if (result.responseText) {
      const alias = sessionAliases.get(binding.codepilotSessionId);
      // Show alias label at top only when user has explicitly set one via /name.
      // No session ID is appended otherwise.
      const responseToSend = alias
        ? `📌 ${alias}\n\n${result.responseText}`
        : result.responseText;
      await deliverResponse(
        adapter,
        msg.address,
        responseToSend,
        binding.codepilotSessionId,
        msg.messageId,
      );

      // B3: Auto-detect and send files mentioned in Claude's response
      if (adapter.channelType === "feishu") {
        const filePaths = extractLocalFilePaths(result.responseText);
        const feishuClient = (
          adapter as unknown as { getLarkClient(): unknown }
        ).getLarkClient();
        if (feishuClient) {
          for (const fp of filePaths) {
            try {
              if (isImageFile(fp)) {
                await sendImage(feishuClient, msg.address.chatId, fp);
              } else {
                await sendFile(feishuClient, msg.address.chatId, fp);
              }
            } catch (err) {
              await deliver(adapter, {
                address: msg.address,
                text: `File auto-send failed: ${path.basename(fp)}. Use /send ${fp} to retry`,
                parseMode: "plain",
              });
            }
          }
        }
      }
    } else if (result.hasError) {
      const errorResponse: OutboundMessage = {
        address: msg.address,
        text: `<b>Error:</b> ${escapeHtml(result.errorMessage)}`,
        parseMode: "HTML",
        replyToMessageId: msg.messageId,
      };
      await deliver(adapter, errorResponse);
    }

    // Persist the actual SDK session ID for future resume.
    // If the result has an error and no session ID was captured, clear the
    // stale ID so the next message starts fresh instead of retrying a broken resume.
    if (binding.id) {
      try {
        const update = computeSdkSessionUpdate(
          result.sdkSessionId,
          result.hasError,
        );
        if (update !== null) {
          store.updateChannelBinding(binding.id, { sdkSessionId: update });
        }
      } catch {
        /* best effort */
      }
    }
  } finally {
    // Clean up preview state
    if (previewState) {
      if (previewState.throttleTimer) {
        clearTimeout(previewState.throttleTimer);
        previewState.throttleTimer = null;
      }
      adapter.endPreview?.(msg.address.chatId, previewState.draftId);
    }

    state.activeTasks.delete(binding.codepilotSessionId);
    // Notify adapter that message processing ended
    adapter.onMessageEnd?.(msg.address.chatId);
    // Commit the offset only after full processing (success or failure)
    ack();
  }
}

/**
 * Handle IM slash commands.
 */
async function handleCommand(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
  text: string,
): Promise<void> {
  const { store } = getBridgeContext();

  // Extract command and args (handle /command@botname format)
  const parts = text.split(/\s+/);
  const command = parts[0].split("@")[0].toLowerCase();
  const args = parts.slice(1).join(" ").trim();

  // Run dangerous-input detection on the full command text
  const dangerCheck = isDangerousInput(text);
  if (dangerCheck.dangerous) {
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: "inbound",
      messageId: msg.messageId,
      summary: `[BLOCKED] Dangerous input detected: ${dangerCheck.reason}`,
    });
    console.warn(
      `[bridge-manager] Blocked dangerous command input from chat ${msg.address.chatId}: ${dangerCheck.reason}`,
    );
    await deliver(adapter, {
      address: msg.address,
      text: `Command rejected: invalid input detected.`,
      parseMode: "plain",
      replyToMessageId: msg.messageId,
    });
    return;
  }

  let response = "";

  switch (command) {
    case "/start":
      response = [
        "<b>🤖 Claude Bridge</b>",
        "",
        "直接发消息即可与 Claude 对话。",
        "",
        "<b>常用命令：</b>",
        "/cwd — 查看/切换项目",
        "/sessions — 查看历史会话",
        "/resume &lt;序号&gt; — 恢复会话",
        "/new — 新建会话",
        "/name &lt;名称&gt; — 给会话命名",
        "/help — 查看全部命令",
      ].join("\n");
      break;

    case "/n":
    case "/new": {
      let workDir: string | undefined;
      if (args) {
        const validated = validateWorkingDirectory(args);
        if (!validated) {
          response =
            "Invalid path. Must be an absolute path without traversal sequences.";
          break;
        }
        workDir = validated;
      }
      // Save current sdkSessionId before clearing (for /back)
      const oldBinding = router.resolve(msg.address);
      if (oldBinding.sdkSessionId) {
        previousSdkSessionIds.set(oldBinding.id, oldBinding.sdkSessionId);
      }
      const binding = router.createBinding(msg.address, workDir);
      // Explicitly clear sdkSessionId so the next message starts a fresh Claude session
      router.updateBinding(binding.id, { sdkSessionId: "" });
      response = `New session created.\nSession: <code>${binding.codepilotSessionId.slice(0, 8)}...</code>\nCWD: <code>${escapeHtml(binding.workingDirectory || "~")}</code>`;
      break;
    }

    case "/b":
    case "/back": {
      const binding = router.resolve(msg.address);
      const prevId = previousSdkSessionIds.get(binding.id);
      if (!prevId) {
        response = "⚠️ 没有可恢复的历史会话（仅在本次 daemon 运行期间有效）";
        break;
      }
      // Save current as previous before switching back
      if (binding.sdkSessionId) {
        previousSdkSessionIds.set(binding.id, binding.sdkSessionId);
      }
      router.updateBinding(binding.id, { sdkSessionId: prevId });
      response = `✅ 已恢复到会话 <code>${prevId.slice(0, 8)}...</code>\n下一条消息将从该会话继续`;
      break;
    }

    case "/r":
    case "/resume": {
      if (!args || !args.trim()) {
        response = "Usage: /resume &lt;number|ID prefix|full ID&gt;";
        break;
      }
      const resumeBinding = router.resolve(msg.address);
      const cached = sessionsCache.get(resumeBinding.id) || null;
      const resumeResult = resolveResumeTarget(args.trim(), cached);
      if (resumeResult.error) {
        response = resumeResult.error;
      } else if (resumeResult.sessionId) {
        // Save current sdkSessionId for /back
        if (resumeBinding.sdkSessionId) {
          previousSdkSessionIds.set(
            resumeBinding.id,
            resumeBinding.sdkSessionId,
          );
        }
        router.updateBinding(resumeBinding.id, {
          sdkSessionId: resumeResult.sessionId,
        });
        response = `Resumed session <code>${resumeResult.sessionId.slice(0, 8)}...</code>`;
      }
      break;
    }

    case "/bind": {
      if (!args) {
        response = "Usage: /bind &lt;session_id&gt;";
        break;
      }
      if (!validateSessionId(args)) {
        response =
          "Invalid session ID format. Expected a 32-64 character hex/UUID string.";
        break;
      }
      const binding = router.bindToSession(msg.address, args);
      if (binding) {
        response = `Bound to session <code>${args.slice(0, 8)}...</code>`;
      } else {
        response = "Session not found.";
      }
      break;
    }

    case "/cwd": {
      // No args → list known projects
      if (!args) {
        const projects = listKnownProjects();
        if (projects.length === 0) {
          response = "未找到任何项目目录";
          break;
        }
        projectsCache = projects;
        const binding = router.resolve(msg.address);
        const currentCwd = binding.workingDirectory || "";
        const lines = ["<b>📂 项目列表：</b>", ""];
        for (let i = 0; i < Math.min(projects.length, 15); i++) {
          const marker = projects[i] === currentCwd ? " ◀" : "";
          lines.push(
            `${i + 1}. <code>${escapeHtml(projects[i])}</code>${marker}`,
          );
        }
        if (projects.length > 15) {
          lines.push(`... 共 ${projects.length} 个项目`);
        }
        lines.push(
          "",
          "用 /cwd &lt;序号&gt; 切换，/cwd &lt;序号&gt; --new 切换并新建会话",
        );
        response = lines.join("\n");
        break;
      }

      const { path: cwdPathArg, hasNew } = parseCwdArgs(args);

      // Numeric → pick from cache
      let targetPath: string;
      if (/^\d+$/.test(cwdPathArg)) {
        if (projectsCache.length === 0) {
          response = "请先执行 /cwd 查看项目列表";
          break;
        }
        const idx = parseInt(cwdPathArg) - 1;
        if (idx < 0 || idx >= projectsCache.length) {
          response = `序号超出范围（1-${projectsCache.length}）`;
          break;
        }
        targetPath = projectsCache[idx];
      } else {
        // Absolute path
        const validated = validateWorkingDirectory(cwdPathArg);
        if (!validated) {
          response =
            "Invalid path. Must be an absolute path without traversal sequences or special characters.";
          break;
        }
        targetPath = validated;
      }

      const binding = router.resolve(msg.address);
      router.updateBinding(binding.id, {
        workingDirectory: targetPath,
        sdkSessionId: "",
      });
      if (hasNew) {
        router.createBinding(msg.address, targetPath);
        response = `Working directory set to <code>${escapeHtml(targetPath)}</code>\nNew session created.`;
      } else {
        response = `Working directory set to <code>${escapeHtml(targetPath)}</code>\n(SDK session reset — next message starts fresh context)`;
      }
      break;
    }

    case "/mode": {
      if (!validateMode(args)) {
        response = "Usage: /mode plan|code|ask";
        break;
      }
      const binding = router.resolve(msg.address);
      router.updateBinding(binding.id, { mode: args });
      response = `Mode set to <b>${args}</b>`;
      break;
    }

    case "/status": {
      const binding = router.resolve(msg.address);
      response = [
        "<b>Bridge Status</b>",
        "",
        `Session: <code>${binding.codepilotSessionId.slice(0, 8)}...</code>`,
        `CWD: <code>${escapeHtml(binding.workingDirectory || "~")}</code>`,
        `Mode: <b>${binding.mode}</b>`,
        `Model: <code>${binding.model || "default"}</code>`,
      ].join("\n");
      break;
    }

    case "/sessions": {
      const sessBinding = router.resolve(msg.address);
      const workDir = args || sessBinding.workingDirectory;

      if (workDir) {
        // Scan Claude Code history sessions
        const sessions = await scanClaudeSessions(workDir);
        if (sessions.length === 0) {
          response = `📂 ${escapeHtml(workDir)} 下无历史会话`;
        } else {
          sessionsCache.set(sessBinding.id, sessions);
          const lines = [`<b>📂 ${escapeHtml(workDir)} 历史会话：</b>`, ""];
          const activeSdkId = sessBinding.sdkSessionId;
          const activeAlias = sessionAliases.get(
            sessBinding.codepilotSessionId,
          );
          for (let i = 0; i < Math.min(sessions.length, 10); i++) {
            const s = sessions[i];
            const ago = formatTimeAgo(s.lastModified);
            const isActive = activeSdkId && s.id === activeSdkId;
            const activeTag = isActive
              ? activeAlias
                ? ` 📌 ${escapeHtml(activeAlias)}`
                : " ◀ 当前"
              : "";
            lines.push(
              `${i + 1}. "${escapeHtml(s.summary)}" (${ago})${activeTag}`,
            );
          }
          lines.push("", "用 /resume &lt;序号&gt; 恢复，/name 给当前会话命名");
          response = lines.join("\n");
        }
      } else {
        // No working directory — fallback to listing bindings
        const bindings = router.listBindings(adapter.channelType);
        if (bindings.length === 0) {
          response = "No sessions found.";
        } else {
          const lines = ["<b>Sessions:</b>", ""];
          for (const b of bindings.slice(0, 10)) {
            const active = b.active ? "active" : "inactive";
            lines.push(
              `<code>${b.codepilotSessionId.slice(0, 8)}...</code> [${active}] ${escapeHtml(b.workingDirectory || "~")}`,
            );
          }
          response = lines.join("\n");
        }
      }
      break;
    }

    case "/stop": {
      const binding = router.resolve(msg.address);
      const st = getState();
      const taskAbort = st.activeTasks.get(binding.codepilotSessionId);
      if (taskAbort) {
        taskAbort.abort();
        st.activeTasks.delete(binding.codepilotSessionId);
        response = "Stopping current task...";
      } else {
        response = "No task is currently running.";
      }
      break;
    }

    case "/perm": {
      // Text-based permission approval fallback (for channels without inline buttons)
      // Usage: /perm allow <id> | /perm allow_session <id> | /perm deny <id>
      const permParts = args.split(/\s+/);
      const permAction = permParts[0];
      const permId = permParts.slice(1).join(" ");
      if (
        !permAction ||
        !permId ||
        !["allow", "allow_session", "deny"].includes(permAction)
      ) {
        response =
          "Usage: /perm allow|allow_session|deny &lt;permission_id&gt;";
        break;
      }
      const callbackData = `perm:${permAction}:${permId}`;
      const handled = broker.handlePermissionCallback(
        callbackData,
        msg.address.chatId,
      );
      if (handled) {
        response = `Permission ${permAction}: recorded.`;
      } else {
        response = `Permission not found or already resolved.`;
      }
      break;
    }

    case "/send": {
      if (!args) {
        response = "Usage: /send /path/to/file";
        break;
      }
      const sendFilePath = args.trim();
      if (!isPathSafe(sendFilePath)) {
        response =
          "Safety restriction: sending files from this path is not allowed.";
        break;
      }
      try {
        const stat = fs.statSync(sendFilePath);
        if (!stat.isFile()) {
          response = "Path is not a file.";
          break;
        }
        if (stat.size > 30 * 1024 * 1024) {
          response = `File ${path.basename(sendFilePath)} (${(stat.size / 1024 / 1024).toFixed(1)}MB) exceeds 30MB limit`;
          break;
        }
        if (adapter.channelType === "feishu") {
          const feishuClient = (
            adapter as unknown as { getLarkClient(): unknown }
          ).getLarkClient();
          if (!feishuClient) {
            response = "Feishu client not initialized.";
            break;
          }
          let sendResult;
          if (isImageFile(sendFilePath)) {
            sendResult = await sendImage(
              feishuClient,
              msg.address.chatId,
              sendFilePath,
            );
          } else {
            sendResult = await sendFile(
              feishuClient,
              msg.address.chatId,
              sendFilePath,
            );
          }
          response = sendResult.ok
            ? `Sent ${path.basename(sendFilePath)}`
            : `Send failed: ${sendResult.error}`;
        } else {
          response = "File sending is currently only supported for Feishu.";
        }
      } catch {
        response = `File not found: ${sendFilePath}`;
      }
      break;
    }

    case "/name": {
      if (!args) {
        response = "用法：/name 会话名称";
        break;
      }
      const nameBinding = router.resolve(msg.address);
      sessionAliases.set(nameBinding.codepilotSessionId, args.trim());
      saveAliases();
      response = `✅ 当前会话已命名为「${escapeHtml(args.trim())}」`;
      break;
    }

    case "/help":
      response = [
        "<b>📋 命令帮助</b>",
        "",
        "<b>会话管理</b>",
        "/new (/n) — 新建会话",
        "/sessions — 查看历史会话列表",
        "/resume (/r) &lt;序号&gt; — 恢复指定会话",
        "/back (/b) — 返回上一个会话",
        "/name &lt;名称&gt; — 给当前会话命名",
        "/stop — 停止当前任务",
        "",
        "<b>项目切换</b>",
        "/cwd — 查看项目列表",
        "/cwd &lt;序号&gt; — 切换到指定项目",
        "/cwd &lt;序号&gt; --new — 切换项目并新建会话",
        "",
        "<b>设置</b>",
        "/mode plan|code|ask — 切换模式",
        "/status — 查看当前状态",
        "",
        "<b>文件</b>",
        "/send &lt;路径&gt; — 发送本地文件到聊天",
        "",
        "<b>权限</b>",
        "/perm allow|deny &lt;id&gt; — 批准/拒绝权限请求",
        "/help - Show this help",
      ].join("\n");
      break;

    default:
      response = `Unknown command: ${escapeHtml(command)}\nType /help for available commands.`;
  }

  if (response) {
    await deliver(adapter, {
      address: msg.address,
      text: response,
      parseMode: "HTML",
      replyToMessageId: msg.messageId,
    });
  }
}

// ── SDK Session Update Logic ─────────────────────────────────

/**
 * Compute the sdkSessionId value to persist after a conversation result.
 * Returns the new value to write, or null if no update is needed.
 *
 * Rules:
 * - If result has sdkSessionId AND no error → save the new ID
 * - If result has error (regardless of sdkSessionId) → clear to empty string
 * - Otherwise → no update needed
 */
export function computeSdkSessionUpdate(
  sdkSessionId: string | null | undefined,
  hasError: boolean,
): string | null {
  if (sdkSessionId && !hasError) {
    return sdkSessionId;
  }
  if (hasError) {
    return "";
  }
  return null;
}

// ── Test-only export ─────────────────────────────────────────
// Exposed so integration tests can exercise handleMessage directly
// without wiring up the full adapter loop.
/** @internal */
export const _testOnly = { handleMessage };
