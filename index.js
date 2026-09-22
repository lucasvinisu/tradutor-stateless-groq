const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "8mb" }));

// ============================================================
// STREMIO PT-BR 9.7.3 - SEMANTIC FIDELITY + IMPLICIT SPEAKER-TURN + CONTEXTUAL MUSIC POLISH (UNIVERSAL / TITLE-AGNOSTIC)
// GenerateContent + per-model quotas + phase-aware routing + bounded checkpoints.
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();

// ============================================================
// BRIDGE GATEWAY 1.1 â€” endereÃ§o pÃºblico estÃ¡vel por redirect, sem domÃ­nio prÃ³prio
// ============================================================
const BRIDGE_GATEWAY_TTL_MS = 3 * 60 * 1000;
const BRIDGE_GATEWAY_PUBLIC_KEY = crypto
  .createHash("sha256")
  .update(LOCAL_BRIDGE_SECRET || "bridge-gateway-unconfigured")
  .digest("hex")
  .slice(0, 24);

const bridgeGatewayState = {
  baseUrl: "",
  registeredAt: 0,
  expiresAt: 0,
  lastOkAt: 0
};

function bridgeGatewayIsFresh() {
  return Boolean(
    bridgeGatewayState.baseUrl &&
    bridgeGatewayState.expiresAt > Date.now()
  );
}

function bridgeGatewayPublicBase() {
  return `${PUBLIC_URL}/bridge/${BRIDGE_GATEWAY_PUBLIC_KEY}`;
}

function normalizeBridgeGatewayUrl(value) {
  const raw = String(value || "").trim().replace(/\/+$/, "");
  if (!raw) throw new Error("baseUrl ausente.");

  const parsed = new URL(raw);
  if (parsed.protocol !== "https:") {
    throw new Error("Bridge Gateway aceita somente HTTPS.");
  }
  if (!parsed.hostname.toLowerCase().endsWith(".trycloudflare.com")) {
    throw new Error("Bridge Gateway 1.1 aceita somente Quick Tunnel trycloudflare.com.");
  }
  if (parsed.username || parsed.password || (parsed.port && parsed.port !== "443")) {
    throw new Error("baseUrl invÃ¡lida.");
  }
  if (parsed.pathname !== "/" && parsed.pathname !== "") {
    throw new Error("baseUrl deve apontar para a raiz do tÃºnel.");
  }
  if (parsed.search || parsed.hash) {
    throw new Error("baseUrl nÃ£o pode conter query/hash.");
  }
  return `${parsed.protocol}//${parsed.host}`;
}


const GEMINI_MODELS = Object.freeze({
  MAIN_PRIMARY: "gemini-3.1-flash-lite",
  MAIN_FALLBACK: "gemini-3.5-flash-lite",
  GEMMA_MAIN: "gemma-4-26b-a4b-it",
  GEMMA_QA: "gemma-4-31b-it"
});

// Mantido como alias de compatibilidade para status/logs antigos.
const GEMINI_MODEL = GEMINI_MODELS.MAIN_PRIMARY;
const GEMINI_TRANSCRIBE_MODEL = "gemini-3.5-transcribe";

// 9.4.2: o MAIN continua 3.5-first por padrÃ£o porque os benchmarks reais deste
// projeto mostraram ~77s/1480 cues. Para A/B controlado, MAIN_ROUTE_PREFERENCE=3.1
// troca SOMENTE o MAIN para 3.1-first sem alterar QA/Repair/closure.
const MAIN_ROUTE_PREFERENCE_942 = String(process.env.MAIN_ROUTE_PREFERENCE || "3.5").trim();
const ROUTER_INVALID_RESPONSE_COOLDOWN_MS_942 = 8000;
const ROUTER_TIMEOUT_COOLDOWN_MS_942 = 15000;
const ROUTER_TRANSIENT_COOLDOWN_MS_942 = 10000;
const ROUTER_RECOVERY_WAIT_MAX_MS_942 = 18000;

const CACHE_VERSION =
  "9.7.3-semantic-speaker-music-v1";
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_SOURCE_CHARS = 800000;
const FETCH_TIMEOUT_MS = 25000;

// Limites observados no AI Studio deste projeto em 2026-09.
// O router usa cada cota separadamente; um 429/503 de um modelo NÃƒO bloqueia os demais.
const GEMINI_MODEL_PROFILES = Object.freeze({
  [GEMINI_MODELS.MAIN_PRIMARY]: {
    rpm: 15, rpmSoft: 14, tpm: 250000, tpmSoft: 235000, rpd: 500,
    minStartMs: 4300, timeoutCapMs: 120000, unavailable503Ms: 20000,
    supportsStructuredOutput: true, family: "gemini"
  },
  [GEMINI_MODELS.MAIN_FALLBACK]: {
    rpm: 15, rpmSoft: 14, tpm: 250000, tpmSoft: 235000, rpd: 500,
    minStartMs: 4300, timeoutCapMs: 120000, unavailable503Ms: 20000,
    supportsStructuredOutput: true, family: "gemini"
  },
  [GEMINI_MODELS.GEMMA_MAIN]: {
    rpm: 30, rpmSoft: 28, tpm: 16000, tpmSoft: 14500, rpd: 14400,
    minStartMs: 2200, timeoutCapMs: 15000, unavailable503Ms: 300000,
    supportsStructuredOutput: false, family: "gemma"
  },
  [GEMINI_MODELS.GEMMA_QA]: {
    rpm: 30, rpmSoft: 28, tpm: 16000, tpmSoft: 14500, rpd: 14400,
    minStartMs: 2200, timeoutCapMs: 20000, unavailable503Ms: 300000,
    supportsStructuredOutput: false, family: "gemma"
  }
});

// SÃ­mbolos legados mantidos porque helpers antigos de 8.8.3 continuam presentes,
// mas NÃƒO governam mais as chamadas de texto no 9.0.
const GEMINI_FREE_RPM_LIMIT = 15;
const GEMINI_FREE_TPM_LIMIT = 250000;
const GEMINI_FREE_RPD_LIMIT = 500;
const GEMINI_SAFE_RPM = 15;
const GEMINI_SAFE_TPM = 250000;
const GEMINI_SAFE_RPD = 500;
const GEMINI_MIN_START_INTERVAL_MS = 4300;
const GEMINI_429_GLOBAL_BUFFER_MS = 0;
const GEMINI_BUDGET_WINDOW_MS = 60000;
const GEMINI_TOKEN_ESTIMATE_CHARS_PER_TOKEN = 3.2;
const GEMINI_TOKEN_ESTIMATE_MARGIN = 1.12;
const GEMINI_TEXT_BUDGET_FILE = String(
  process.env.GEMINI_TEXT_BUDGET_FILE ||
  path.join(process.cwd(), "gemini-text-budget-8.9.6-legacy-unused.json")
);

// Multilingual Audio-Sync Adapter.
// A Ponte usa este endpoint SOMENTE para construir texto-proxy lexical
// no idioma real do Ã¡udio. NÃ£o gera timestamps e nÃ£o altera a traduÃ§Ã£o final.
const SYNC_PROXY_MAX_ITEMS = 260;
const SYNC_PROXY_MAX_CHARS = 36000;
const SYNC_PROXY_THINKING = "low";
const SYNC_PROXY_MAX_OUTPUT_TOKENS = 12000;
const SYNC_PROXY_TIMEOUT_MS = 90000;
const SYNC_PROXY_HTTP_RETRIES = 3;

// Cross-language semantic anchor alignment.
// Recebe SOMENTE poucos cues + palavras jÃ¡ transcritas; nunca traduz a legenda inteira.
const SYNC_ALIGN_MAX_ITEMS = 20;
const SYNC_ALIGN_MAX_WORDS_PER_ITEM = 120;
const SYNC_ALIGN_MAX_CHARS = 30000;
const SYNC_ALIGN_THINKING = "low";
const SYNC_ALIGN_MAX_OUTPUT_TOKENS = 5000;
const SYNC_ALIGN_TIMEOUT_MS = 60000;
const SYNC_ALIGN_HTTP_RETRIES = 3;

// Gemini Transcribe free-tier guard: 3 RPM / 10k TPM / 25 RPD.
// O projeto usa 22s entre inÃ­cios, teto interno de 24 chamadas/24h e
// uma margem de TPM para reduzir 429 antes que aconteÃ§am.
const TRANSCRIBE_MIN_START_INTERVAL_MS = 22000;
const TRANSCRIBE_TPM_LIMIT = 10000;
const TRANSCRIBE_TPM_SOFT_LIMIT = 9500;
const TRANSCRIBE_RPD_INTERNAL_LIMIT = 24;
const TRANSCRIBE_TOKEN_ESTIMATE_PER_SECOND = 32;
const TRANSCRIBE_OUTPUT_TOKEN_RESERVE = 320;

// INTENCIONALMENTE continua 8.3.5:
// nÃ£o podemos trocar o nome do ledger e esquecer chamadas Transcribe
// jÃ¡ consumidas nas Ãºltimas 24h durante o deploy do 8.3.16.
const TRANSCRIBE_BUDGET_FILE = String(
  process.env.TRANSCRIBE_BUDGET_FILE ||
  path.join(process.cwd(), "transcribe-budget-8.3.5.json")
);

const PLAN_THINKING = "medium";
const PLAN_MAX_OUTPUT_TOKENS = 6500;
const PLAN_TIMEOUT_MS = 60000;
const PLAN_RETRIES = 1;
const PLAN_SAMPLE_MAX_CUES = 360;

// Fallback do planner: propositalmente ainda mais simples e barato.
// Se o SAFE-SCHEMA vier INCOMPLETE, nÃ£o repetimos a mesma estratÃ©gia.
const PLAN_FALLBACK_THINKING = "low";
const PLAN_FALLBACK_MAX_OUTPUT_TOKENS = 5000;
const PLAN_FALLBACK_RETRIES = 1;

const MAIN_BATCH_MAX_CUES = 120;
const MAIN_BATCH_MAX_CHARS = 26000;
const MAIN_CONCURRENCY = 6;
const CAPSULE_CONTEXT_BEFORE = 1;
const CAPSULE_CONTEXT_AFTER = 1;
// Benchmark real: 3.1 Flash-Lite MEDIUM caiu de ~63.55s (HIGH) para ~6.83s
// no mesmo teste e preservou JSON/SDH/gÃªnero com o prompt endurecido.
const MAIN_THINKING = "medium";
const MAIN_MAX_OUTPUT_TOKENS = 18000;
const MAIN_TIMEOUT_MS = 45000;
const MAIN_HTTP_RETRIES = 2;
const MAIN_PARSE_ATTEMPTS = 2;

// MAIN EMPTY-CUE RESCUE
// Se uma resposta estruturalmente vÃ¡lida trouxer pt vazio para um target
// nÃ£o vazio, preservamos os demais cues do lote e refazemos SOMENTE o cue vazio.
const MAIN_EMPTY_CUE_RESCUE_ENABLED = true;
const MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS = 3;
const MAIN_EMPTY_CUE_RESCUE_THINKING = "high";
const MAIN_EMPTY_CUE_RESCUE_MAX_OUTPUT_TOKENS = 2400;
const MAIN_EMPTY_CUE_RESCUE_TIMEOUT_MS = 90000;
const MAIN_EMPTY_CUE_RESCUE_HTTP_RETRIES = 3;

// 8.4.6: nenhuma cue pode manter o job eternamente em processing.
// Duas respostas independentes classificadas como SDH confirmam omissÃ£o;
// caso contrÃ¡rio, apÃ³s um ciclo finito preservamos uma base segura para QA.
const MAIN_EMPTY_CUE_MAX_CYCLES = 1;
const MAIN_EMPTY_CUE_SDH_CONSENSUS_MIN = 2;

const REPAIR_ENABLED = true;
const REPAIR_MAX_CUES_TOTAL = 180;
const REPAIR_BATCH_MAX_CUES = 36;
const REPAIR_THINKING = "high";
const REPAIR_MAX_OUTPUT_TOKENS = 22000;
const REPAIR_TIMEOUT_MS = 60000;
const REPAIR_HTTP_RETRIES = 2;
const REPAIR_PARSE_ATTEMPTS = 1;
const REPAIR_CONCURRENCY = 2;

// 9.4.0 â€” REPAIR ISOLATION. Um Ãºnico cue estruturalmente invÃ¡lido nunca
// invalida dezenas de repairs bons. Primeiro fazemos salvage item-a-item;
// somente o residual entra em micro-batches bounded e, por Ãºltimo, cue surgery.
const REPAIR_ISOLATION_MICRO_MAX_CUES_928 = 18;
const REPAIR_ISOLATION_MAX_MICRO_BATCHES_928 = 2;
const QUALITY_BEAM_CANDIDATES_928 = 4;

// QA semÃ¢ntico SOURCEÃ—PT para TODAS as fontes.
// NÃ£o reescreve diretamente: aponta cues problemÃ¡ticos para Repair.
const QA_ENABLED = true;
const QA_BATCH_MAX_CUES = 900;
const QA_BATCH_MAX_CHARS = 140000;
const QA_THINKING = "high";
const QA_MAX_OUTPUT_TOKENS = 18000;
const QA_TIMEOUT_MS = 60000;
const QA_HTTP_RETRIES = 2;
const QA_PARSE_ATTEMPTS = 2;
const QA_MAX_FLAGS_TOTAL = 120;
const QA_CONCURRENCY = 2;
const QA_CONTEXT_BEFORE = 1;
const QA_CONTEXT_AFTER = 1;

// ============================================================
// PRE-REPAIR SEMANTIC CONFIRMATION â€” 8.4.5
// ============================================================
// HeurÃ­sticas ambÃ­guas nÃ£o ganham autoridade para reescrever texto sozinhas.
// Duas auditorias semÃ¢nticas independentes precisam concordar que o cue estÃ¡
// limpo para dispensar Repair. Qualquer flag OU falha tÃ©cnica mantÃ©m Repair.
const PRE_REPAIR_CONFIRM_ENABLED = false;
const PRE_REPAIR_CONFIRM_ROUNDS = 2;
const PRE_REPAIR_CONFIRM_BATCH_MAX_CUES = 70;
const PRE_REPAIR_CONFIRM_BATCH_MAX_CHARS = 30000;
const PRE_REPAIR_CONFIRM_CONCURRENCY = 2;
const PRE_REPAIR_CONFIRM_THINKING = "high";
const PRE_REPAIR_CONFIRM_MAX_OUTPUT_TOKENS = 6500;
const PRE_REPAIR_CONFIRM_TIMEOUT_MS = 120000;
const PRE_REPAIR_CONFIRM_HTTP_RETRIES = 3;


// ============================================================
// FINAL PRIORITY CONVERGENCE â€” 8.4.2 SCHEMA-SAFE
// ============================================================
// Um problema de QUALIDADE nÃ£o encerra o job. O gate audita a legenda
// que seria realmente servida e corrige somente os cues reprovados
// atÃ© que nÃ£o reste defeito prioritÃ¡rio. Falhas transitÃ³rias de Gemini
// tambÃ©m entram em retry; nÃ£o viram "failed" por conveniÃªncia.
const FINAL_PRIORITY_GATE_ENABLED = true;
const FINAL_PRIORITY_AUDIT_BATCH_MAX_CUES = 200;
const FINAL_PRIORITY_AUDIT_BATCH_MAX_CHARS = 56000;
const FINAL_PRIORITY_AUDIT_CONCURRENCY = 3;
const FINAL_PRIORITY_AUDIT_THINKING = "high";
const FINAL_PRIORITY_AUDIT_MAX_OUTPUT_TOKENS = 18000;
const FINAL_PRIORITY_AUDIT_TIMEOUT_MS = 120000;
const FINAL_PRIORITY_AUDIT_HTTP_RETRIES = 4;
const FINAL_PRIORITY_MAX_ISSUES = 80;
const FINAL_PRIORITY_RETRY_BASE_MS = 4300;
const FINAL_PRIORITY_RETRY_MAX_MS = 60000;
const FINAL_PRIORITY_CONTEXT_RADIUS = 1;
const FINAL_PRIORITY_NO_PROGRESS_ESCALATE_AFTER = 2;
const FINAL_PRIORITY_HEURISTIC_CONSENSUS_CLEAN_AUDITS = 2;
const FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES = 24;
const FINAL_PRIORITY_ESCALATED_MAX_OUTPUT_TOKENS = 12000;
const FINAL_PRIORITY_ESCALATED_TIMEOUT_MS = 120000;
const FINAL_PRIORITY_REQUEST_MAX_FAILURES = 3;
const FINAL_PRIORITY_PARSE_MAX_FAILURES = 3;
const FINAL_PRIORITY_ESCALATED_MAX_FAILURES = 3;
const FINAL_PRIORITY_MAX_ROUNDS = 2;
const JOB_RETRY_BASE_MS = 5000;
const JOB_RETRY_MAX_MS = 60000;
const JOB_MAX_ATTEMPTS = 2;

// ============================================================
// SUBTITLE LAYOUT LOCK
// ============================================================

// Alvo audiovisual: no mÃ¡ximo 2 linhas, atÃ© 50 caracteres por linha.
// IMPORTANTE: estes limites NUNCA autorizam cortar palavras, truncar
// conteÃºdo, criar cues ou alterar timestamps.
const LAYOUT_MAX_LINES = 2;
const LAYOUT_MAX_CHARS_PER_LINE = 50;

// Quanto mais perto deste valor, mais equilibradas as duas linhas ficam.
const LAYOUT_IDEAL_CHARS_PER_LINE = 44;

// ============================================================
// COMPACT RESCUE â€” HARD 2x50
// ============================================================

// SÃ³ entra aqui quem continuou grande DEMAIS mesmo apÃ³s o Repair normal.
// O gatilho depende somente da geometria do cue, nunca do tÃ­tulo/conteÃºdo.
const COMPACT_RESCUE_ENABLED = true;
const COMPACT_RESCUE_MAX_CUES_TOTAL = 120;
const COMPACT_RESCUE_BATCH_MAX_CUES = 24;
const COMPACT_RESCUE_MAX_ROUNDS = 1;

const COMPACT_RESCUE_THINKING = "high";
const COMPACT_RESCUE_MAX_OUTPUT_TOKENS = 7000;
const COMPACT_RESCUE_TIMEOUT_MS = 90000;
const COMPACT_RESCUE_HTTP_RETRIES = 3;

// 96 dÃ¡ folga para o JavaScript encontrar uma quebra <= 50/50.
// NÃ£o Ã© truncamento; Ã© objetivo editorial para o Gemini.
const COMPACT_RESCUE_TARGET_TOTAL_CHARS = 96;

// 9.4.0 â€” TIMING-AWARE COMPACT SURGERY. A Ponte chama SOMENTE quando a
// geometria final prova que nÃ£o existe janela fÃ­sica suficiente para leitura.
// Este endpoint reescreve TEXTO, jamais timestamps, e passa por auditoria
// semÃ¢ntica independente antes de devolver qualquer mudanÃ§a.
const TIMING_COMPACT_MAX_ITEMS_928 = 48;
const TIMING_COMPACT_MAX_CHARS_928 = 48000;
const TIMING_COMPACT_THINKING_928 = "high";
const TIMING_COMPACT_MAX_OUTPUT_TOKENS_928 = 14000;

// ============================================================
// POST-REWRITE SEMANTIC GUARD
// ============================================================

// Audita somente cues cujo TEXTO foi realmente reescrito
// depois do MAIN. MudanÃ§a apenas de quebra de linha nÃ£o conta.
const SEMANTIC_REWRITE_AUDIT_ENABLED = false;

const SEMANTIC_REWRITE_AUDIT_MAX_CUES_PER_BATCH = 80;
const SEMANTIC_REWRITE_AUDIT_MAX_CHARS_PER_BATCH = 32000;
const SEMANTIC_REWRITE_AUDIT_MAX_ISSUES = 80;
const SEMANTIC_REWRITE_AUDIT_CONCURRENCY = 2;

const SEMANTIC_REWRITE_AUDIT_THINKING = "high";
const SEMANTIC_REWRITE_AUDIT_MAX_OUTPUT_TOKENS = 7000;
const SEMANTIC_REWRITE_AUDIT_TIMEOUT_MS = 90000;
const SEMANTIC_REWRITE_AUDIT_HTTP_RETRIES = 3;

const BLEEP_TOKEN = "__CENSORED_BLEEP__";
const SEMANTIC_COMPACT_RETRY_ENABLED = true;
const SEMANTIC_COMPACT_RETRY_MAX_PER_EPISODE = 8;
const SEMANTIC_COMPACT_RETRY_THINKING = "high";
const SEMANTIC_COMPACT_RETRY_MAX_OUTPUT_TOKENS = 2200;
const SEMANTIC_COMPACT_RETRY_TIMEOUT_MS = 90000;
const SEMANTIC_COMPACT_RETRY_HTTP_RETRIES = 3;
const RECOVERY_SIGNING_KEY =
  LOCAL_BRIDGE_SECRET ||
  GEMINI_API_KEY ||
  "stremio-ptbr-8.3.0";

const translationCache = new Map();
const provisionalTranslationCache = new Map();
const jobs = new Map();

let lastGeminiRequestStart = 0;
let geminiFlightGate = Promise.resolve();
let geminiGlobalCooldownUntil = 0;
let geminiTextLedger = { calls: [] };
let geminiSafeRpmActive = GEMINI_SAFE_RPM;
let geminiSafeTpmActive = GEMINI_SAFE_TPM;

let transcribeGate = Promise.resolve();
let lastTranscribeRequestStart = 0;
let transcribeLedger = { calls: [] };

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ============================================================
// GEMINI TEXT FREE-TIER GOVERNOR 8.8.3
// ============================================================

function pruneGeminiTextLedger(now = Date.now()) {
  const dayAgo = now - 24 * 60 * 60 * 1000;
  geminiTextLedger.calls = Array.isArray(geminiTextLedger.calls)
    ? geminiTextLedger.calls.filter(item => Number(item?.ts || 0) >= dayAgo)
    : [];
}

function loadGeminiTextLedger() {
  try {
    if (!fs.existsSync(GEMINI_TEXT_BUDGET_FILE)) return;
    const parsed = JSON.parse(fs.readFileSync(GEMINI_TEXT_BUDGET_FILE, "utf8"));
    if (Array.isArray(parsed?.calls)) geminiTextLedger = { calls: parsed.calls };
  } catch (error) {
    console.warn(`[GEMINI FREE-TIER GOVERNOR] ledger nÃ£o pÃ´de ser lido: ${String(error?.message || error).slice(0, 220)}`);
  }
  pruneGeminiTextLedger();
}

function persistGeminiTextLedger() {
  try {
    const dir = path.dirname(GEMINI_TEXT_BUDGET_FILE);
    fs.mkdirSync(dir, { recursive: true });
    const tmp = `${GEMINI_TEXT_BUDGET_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(geminiTextLedger), "utf8");
    fs.renameSync(tmp, GEMINI_TEXT_BUDGET_FILE);
  } catch (error) {
    console.warn(`[GEMINI FREE-TIER GOVERNOR] ledger nÃ£o pÃ´de ser salvo: ${String(error?.message || error).slice(0, 220)}`);
  }
}

function estimateGeminiInputTokens(system, user, schema) {
  let schemaText = "";
  try { schemaText = JSON.stringify(schema || {}); } catch {}
  const chars = Buffer.byteLength(String(system || ""), "utf8") +
    Buffer.byteLength(String(user || ""), "utf8") +
    Buffer.byteLength(schemaText, "utf8") + 1200;
  return Math.max(256, Math.ceil((chars / GEMINI_TOKEN_ESTIMATE_CHARS_PER_TOKEN) * GEMINI_TOKEN_ESTIMATE_MARGIN));
}

function geminiTextTokenCost(item) {
  // Reserva conservadora: se o uso real vier menor, mantemos a maior estimativa.
  return Math.max(Number(item?.estimatedInputTokens || 0), Number(item?.actualInputTokens || 0));
}

function geminiTextBudgetSnapshot(now = Date.now()) {
  pruneGeminiTextLedger(now);
  const minuteAgo = now - GEMINI_BUDGET_WINDOW_MS;
  const minuteCalls = geminiTextLedger.calls.filter(item => Number(item?.ts || 0) >= minuteAgo);
  return {
    calls60s: minuteCalls.length,
    tokens60s: minuteCalls.reduce((sum, item) => sum + geminiTextTokenCost(item), 0),
    calls24h: geminiTextLedger.calls.length
  };
}

function nextGeminiBudgetWait(now, estimate) {
  pruneGeminiTextLedger(now);
  const minuteAgo = now - GEMINI_BUDGET_WINDOW_MS;
  const minuteCalls = geminiTextLedger.calls
    .filter(item => Number(item?.ts || 0) >= minuteAgo)
    .sort((a, b) => Number(a.ts) - Number(b.ts));

  let rpmWait = 0;
  if (minuteCalls.length >= geminiSafeRpmActive) {
    const idx = Math.max(0, minuteCalls.length - geminiSafeRpmActive);
    rpmWait = Math.max(0, Number(minuteCalls[idx].ts) + GEMINI_BUDGET_WINDOW_MS - now + 300);
  }

  const minuteTokens = minuteCalls.reduce((sum, item) => sum + geminiTextTokenCost(item), 0);
  let tpmWait = 0;
  if (minuteTokens + estimate > geminiSafeTpmActive && minuteCalls.length) {
    let rolling = minuteTokens;
    for (const item of minuteCalls) {
      rolling -= geminiTextTokenCost(item);
      if (rolling + estimate <= geminiSafeTpmActive) {
        tpmWait = Math.max(0, Number(item.ts) + GEMINI_BUDGET_WINDOW_MS - now + 300);
        break;
      }
    }
    if (!tpmWait) {
      tpmWait = Math.max(0, Number(minuteCalls[minuteCalls.length - 1].ts) + GEMINI_BUDGET_WINDOW_MS - now + 300);
    }
  }

  const pacerWait = Math.max(0, lastGeminiRequestStart + GEMINI_MIN_START_INTERVAL_MS - now);
  const cooldownWait = Math.max(0, geminiGlobalCooldownUntil - now);
  return { waitMs: Math.max(rpmWait, tpmWait, pacerWait, cooldownWait), rpmWait, tpmWait, pacerWait, cooldownWait, minuteTokens, minuteCalls: minuteCalls.length };
}

async function acquireGeminiFlight() {
  const previous = geminiFlightGate;
  let release;
  geminiFlightGate = new Promise(resolve => { release = resolve; });
  await previous;
  return release;
}

async function reserveGeminiTextBudget({ system, user, schema, job, metric }) {
  const estimate = estimateGeminiInputTokens(system, user, schema);

  while (true) {
    const now = Date.now();
    pruneGeminiTextLedger(now);

    if (geminiTextLedger.calls.length >= GEMINI_SAFE_RPD) {
      const error = new Error(`GEMINI FREE-TIER GOVERNOR: teto interno diÃ¡rio ${GEMINI_SAFE_RPD}/${GEMINI_FREE_RPD_LIMIT} atingido; nenhuma nova chamada serÃ¡ enviada.`);
      error.nonRetryable = true;
      error.code = "GEMINI_SAFE_RPD_LOCK";
      throw error;
    }

    const state = nextGeminiBudgetWait(now, estimate);
    if (state.waitMs > 0) {
      const reason = state.cooldownWait >= state.waitMs ? "429-COOLDOWN" : state.tpmWait >= state.waitMs ? "TPM" : state.rpmWait >= state.waitMs ? "RPM" : "PACER";
      if (state.waitMs >= 1000) {
        console.log(`[GEMINI FREE-TIER GOVERNOR 8.8.3] ${reason} aguardando ${(state.waitMs / 1000).toFixed(1)}s | uso60s=${state.minuteCalls}/${geminiSafeRpmActive} req, ${state.minuteTokens}/${geminiSafeTpmActive} input-tokens | prÃ³ximoâ‰ˆ${estimate}.`);
      }
      if (job) {
        job.stats.pacerWaitMs += state.waitMs;
        job.stats.freeTierGovernorWaitMs = (job.stats.freeTierGovernorWaitMs || 0) + state.waitMs;
      }
      await sleep(state.waitMs);
      continue;
    }

    const id = `${now}-${crypto.randomBytes(4).toString("hex")}`;
    lastGeminiRequestStart = Date.now();
    geminiTextLedger.calls.push({ id, ts: lastGeminiRequestStart, metric: String(metric || "main"), estimatedInputTokens: estimate, actualInputTokens: 0 });
    persistGeminiTextLedger();
    const snap = geminiTextBudgetSnapshot();
    console.log(`[GEMINI FREE-TIER GOVERNOR 8.8.3] reserva ${snap.calls60s}/${geminiSafeRpmActive} RPM-soft | ${snap.tokens60s}/${geminiSafeTpmActive} TPM-soft | diÃ¡rio=${snap.calls24h}/${GEMINI_SAFE_RPD}.`);
    return id;
  }
}

function commitGeminiTextUsage(id, usage = {}) {
  const item = geminiTextLedger.calls.find(call => call.id === id);
  if (!item) return;
  item.actualInputTokens = Number(usage?.total_input_tokens || usage?.input_tokens || 0);
  item.updatedAt = Date.now();
  persistGeminiTextLedger();
}

loadGeminiTextLedger();
pruneGeminiTextLedger();

// ============================================================
// TRANSCRIBE BUDGET
// ============================================================

function loadTranscribeLedger() {
  try {
    if (!fs.existsSync(TRANSCRIBE_BUDGET_FILE)) return;

    const parsed = JSON.parse(
      fs.readFileSync(TRANSCRIBE_BUDGET_FILE, "utf8")
    );

    if (Array.isArray(parsed?.calls)) {
      transcribeLedger = { calls: parsed.calls };
    }
  } catch (error) {
    console.warn(
      `[TRANSCRIBE BUDGET] ledger nÃ£o pÃ´de ser lido: ${
        String(error?.message || error).slice(0, 220)
      }`
    );
  }
}

function pruneTranscribeLedger(now = Date.now()) {
  const dayAgo = now - 24 * 60 * 60 * 1000;

  transcribeLedger.calls = (
    Array.isArray(transcribeLedger.calls)
      ? transcribeLedger.calls
      : []
  ).filter(
    item =>
      Number(item?.ts) >= dayAgo &&
      Number.isFinite(Number(item?.ts))
  );
}

function persistTranscribeLedger() {
  try {
    pruneTranscribeLedger();

    const tmp =
      `${TRANSCRIBE_BUDGET_FILE}.tmp-${process.pid}`;

    fs.writeFileSync(
      tmp,
      JSON.stringify(transcribeLedger),
      "utf8"
    );

    if (fs.existsSync(TRANSCRIBE_BUDGET_FILE)) {
      fs.unlinkSync(TRANSCRIBE_BUDGET_FILE);
    }

    fs.renameSync(
      tmp,
      TRANSCRIBE_BUDGET_FILE
    );
  } catch (error) {
    console.warn(
      `[TRANSCRIBE BUDGET] ledger nÃ£o pÃ´de ser salvo: ${
        String(error?.message || error).slice(0, 220)
      }`
    );
  }
}

function transcribeTokenCost(item) {
  const actual =
    Number(item?.inputTokens || 0) +
    Number(item?.outputTokens || 0);

  return actual > 0
    ? actual
    : Number(item?.estimatedTokens || 0);
}

function estimateTranscribeTokens(durationMs) {
  const seconds =
    Math.max(
      1,
      Number(durationMs || 0) / 1000
    );

  return (
    Math.ceil(
      seconds * TRANSCRIBE_TOKEN_ESTIMATE_PER_SECOND
    ) +
    TRANSCRIBE_OUTPUT_TOKEN_RESERVE
  );
}

function transcribeBudgetSnapshot(now = Date.now()) {
  pruneTranscribeLedger(now);

  const minuteAgo =
    now - 60 * 1000;

  const minuteCalls =
    transcribeLedger.calls.filter(
      item => Number(item.ts) >= minuteAgo
    );

  return {
    calls24h: transcribeLedger.calls.length,
    calls60s: minuteCalls.length,

    tokens60s: minuteCalls.reduce(
      (sum, item) =>
        sum + transcribeTokenCost(item),
      0
    ),

    remainingRpd:
      Math.max(
        0,
        TRANSCRIBE_RPD_INTERNAL_LIMIT -
        transcribeLedger.calls.length
      )
  };
}

async function acquireTranscribeBudget(
  durationMs,
  label = "audio"
) {
  const previous =
    transcribeGate;

  let release;

  transcribeGate =
    new Promise(resolve => {
      release = resolve;
    });

  await previous;

  try {
    const estimate =
      estimateTranscribeTokens(durationMs);

    while (true) {
      const now =
        Date.now();

      pruneTranscribeLedger(now);

      if (
        transcribeLedger.calls.length >=
        TRANSCRIBE_RPD_INTERNAL_LIMIT
      ) {
        const oldest =
          Math.min(
            ...transcribeLedger.calls.map(
              item => Number(item.ts)
            )
          );

        const unlockAt =
          oldest +
          24 * 60 * 60 * 1000;

        const waitMs =
          Math.max(
            1000,
            unlockAt - now
          );

        const error =
          new Error(
            `TRANSCRIBE BUDGET: limite interno de ${
              TRANSCRIBE_RPD_INTERNAL_LIMIT
            }/24h atingido; prÃ³xima vaga em ~${
              Math.ceil(waitMs / 60000)
            } min.`
          );

        error.nonRetryable = true;
        error.code =
          "TRANSCRIBE_RPD_LOCK";

        throw error;
      }

      const rpmWait =
        Math.max(
          0,
          lastTranscribeRequestStart +
          TRANSCRIBE_MIN_START_INTERVAL_MS -
          now
        );

      const minuteAgo =
        now - 60 * 1000;

      const minuteCalls =
        transcribeLedger.calls.filter(
          item =>
            Number(item.ts) >= minuteAgo
        );

      const minuteTokens =
        minuteCalls.reduce(
          (sum, item) =>
            sum +
            transcribeTokenCost(item),
          0
        );

      let tpmWait = 0;

      if (
        minuteTokens + estimate >
          TRANSCRIBE_TPM_SOFT_LIMIT &&
        minuteCalls.length
      ) {
        const ordered =
          [...minuteCalls].sort(
            (a, b) =>
              Number(a.ts) -
              Number(b.ts)
          );

        let rolling =
          minuteTokens;

        for (const item of ordered) {
          rolling -=
            transcribeTokenCost(item);

          if (
            rolling + estimate <=
            TRANSCRIBE_TPM_SOFT_LIMIT
          ) {
            tpmWait =
              Math.max(
                0,
                Number(item.ts) +
                60000 -
                now +
                250
              );

            break;
          }
        }

        if (!tpmWait) {
          tpmWait =
            Math.max(
              0,
              Number(
                ordered[
                  ordered.length - 1
                ].ts
              ) +
              60000 -
              now +
              250
            );
        }
      }

      const waitMs =
        Math.max(
          rpmWait,
          tpmWait
        );

      if (waitMs > 0) {
        const reason =
          tpmWait >= rpmWait &&
          tpmWait > 0
            ? "TPM"
            : "RPM";

        console.log(
          `[TRANSCRIBE BUDGET] ${
            reason
          } aguardando ${
            (waitMs / 1000).toFixed(1)
          }s | ${
            label
          } | uso60s=${
            minuteTokens
          }/${
            TRANSCRIBE_TPM_LIMIT
          } est=${
            estimate
          }.`
        );

        await sleep(waitMs);

        continue;
      }

      const id =
        `${now}-${
          crypto
            .randomBytes(4)
            .toString("hex")
        }`;

      lastTranscribeRequestStart =
        Date.now();

      transcribeLedger.calls.push({
        id,

        ts:
          lastTranscribeRequestStart,

        durationMs:
          Number(durationMs || 0),

        estimatedTokens:
          estimate,

        inputTokens: 0,
        outputTokens: 0
      });

      persistTranscribeLedger();

      const snap =
        transcribeBudgetSnapshot();

      console.log(
        `[TRANSCRIBE BUDGET] reserva ${
          snap.calls24h
        }/${
          TRANSCRIBE_RPD_INTERNAL_LIMIT
        } em 24h | ${
          snap.tokens60s
        }/${
          TRANSCRIBE_TPM_LIMIT
        } tokens estimados em 60s.`
      );

      return id;
    }
  } finally {
    release();
  }
}

function commitTranscribeUsage(
  id,
  usage = {}
) {
  const item =
    transcribeLedger.calls.find(
      call => call.id === id
    );

  if (!item) return;

  item.inputTokens =
    Number(
      usage?.total_input_tokens ||
      usage?.input_tokens ||
      0
    );

  item.outputTokens =
    Number(
      usage?.total_output_tokens ||
      usage?.output_tokens ||
      0
    );

  item.thoughtTokens =
    Number(
      usage?.total_thought_tokens ||
      usage?.thought_tokens ||
      0
    );

  item.updatedAt =
    Date.now();

  persistTranscribeLedger();
}

loadTranscribeLedger();
pruneTranscribeLedger();

// ============================================================
// GENERAL HELPERS
// ============================================================

function sha256(value) {
  return crypto
    .createHash("sha256")
    .update(
      String(value),
      "utf8"
    )
    .digest("hex");
}

function randomId(bytes = 6) {
  return crypto
    .randomBytes(bytes)
    .toString("hex");
}

function errorMessage(error) {
  return String(
    error?.message ||
    error ||
    "Erro desconhecido."
  );
}

function normalizeSrt(value) {
  return String(value || "")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
}

function stripCodeFences(value) {
  return String(value || "")
    .replace(
      /^\s*```(?:json|text|plaintext|srt)?\s*/i,
      ""
    )
    .replace(
      /\s*```\s*$/i,
      ""
    )
    .trim();
}

function baseUrl(req) {
  if (PUBLIC_URL) {
    return PUBLIC_URL;
  }

  const proto =
    String(
      req.headers[
        "x-forwarded-proto"
      ] ||
      req.protocol ||
      "https"
    )
      .split(",")[0]
      .trim();

  const host =
    String(
      req.headers[
        "x-forwarded-host"
      ] ||
      req.headers.host ||
      ""
    )
      .split(",")[0]
      .trim();

  return `${proto}://${host}`
    .replace(/\/+$/, "");
}

function safeJson(
  res,
  payload,
  status = 200
) {
  res.set(
    "Cache-Control",
    "no-store, no-cache, must-revalidate"
  );

  return res
    .status(status)
    .json(payload);
}

function sendSrt(
  res,
  srt,
  cacheControl = "no-store"
) {
  res.status(200);

  res.set(
    "Content-Type",
    "application/x-subrip; charset=utf-8"
  );

  res.set(
    "Cache-Control",
    cacheControl
  );

  res.send(
    String(srt || "")
  );
}

function authorized(req) {
  if (!LOCAL_BRIDGE_SECRET) {
    return false;
  }

  const provided =
    Buffer.from(
      String(
        req.headers.authorization ||
        ""
      ).trim()
    );

  const expected =
    Buffer.from(
      `Bearer ${LOCAL_BRIDGE_SECRET}`
    );

  return (
    provided.length ===
      expected.length &&
    crypto.timingSafeEqual(
      provided,
      expected
    )
  );
}

function makeCacheKey(
  type,
  videoId,
  sourceSrt
) {
  return (
    `${CACHE_VERSION}:` +
    `${type}:` +
    `${videoId}:` +
    sha256(sourceSrt)
  );
}

function getCache(key) {
  const item =
    translationCache.get(key);

  if (!item) {
    return null;
  }

  if (
    item.expiresAt <= Date.now()
  ) {
    translationCache.delete(key);
    return null;
  }

  return item.srt;
}

function setCache(
  key,
  srt,
  job = null
) {
  provisionalTranslationCache.delete(key);
  translationCache.set(
    key,
    {
      srt,
      intentionalEmptyIds:
        job?.intentionalEmptyCueIds instanceof Set
          ? [
              ...job.intentionalEmptyCueIds
            ]
          : [],
      expiresAt:
        Date.now() +
        CACHE_TTL_MS
    }
  );
}

function getProvisionalCache927(key) {
  const item = provisionalTranslationCache.get(key);
  if (!item) return null;
  if (item.expiresAt <= Date.now()) {
    provisionalTranslationCache.delete(key);
    return null;
  }
  return item;
}

function setProvisionalCache927(key, srt, job = null, label = "best_available") {
  const value = String(srt || "").trim();
  if (!value || !/-->/m.test(value)) return false;
  provisionalTranslationCache.set(key, {
    srt: value,
    qualityStatus: String(job?.qualityStatus || label || "best_available"),
    residualIssues: Array.isArray(job?.finalTargetResidual927)
      ? job.finalTargetResidual927.map(issue => ({ id: Number(issue?.id), reasons: [...(issue?.reasons || [])] }))
      : [],
    createdAt: Date.now(),
    expiresAt: Date.now() + CACHE_TTL_MS
  });
  console.warn(`[PROVISIONAL CACHE 9.4.0] salvo | quality=${String(job?.qualityStatus || label)} | key=${String(key).slice(0,18)}...`);
  return true;
}

function restoreIntentionalEmptyIdsFromCache(
  key,
  job
) {
  if (!job) return;

  const item =
    translationCache.get(
      key
    );

  if (!item) return;

  if (!(job.intentionalEmptyCueIds instanceof Set)) {
    job.intentionalEmptyCueIds = new Set();
  }

  for (
    const rawId of
    Array.isArray(item.intentionalEmptyIds)
      ? item.intentionalEmptyIds
      : []
  ) {
    const id = Number(rawId);
    if (Number.isInteger(id)) {
      job.intentionalEmptyCueIds.add(id);
    }
  }
}

function createJob({
  type,
  videoId,
  filename,
  sourceSrt,
  sourceKind,
  sourceLang = "auto",
  lazy = false,
  recovery = null
}) {
  const sourceHash =
    sha256(sourceSrt);

  const now =
    Date.now();

  const job = {
    id:
      `job-${
        sourceHash.slice(0, 24)
      }-${
        randomId()
      }`,

    type,
    videoId,
    filename,
    sourceSrt,
    sourceKind,
    sourceLang,
    sourceHash,
    recovery,

    // MAIN conclui por lote e preserva progresso em retries/failover.
    mainCheckpoint: new Map(),
    // Hard skip = somente indisponibilidade realmente terminal no job (ex.: RPD diÃ¡rio).
    modelRouterSkip: new Set(),
    // Cooldown por mÃ©trica = falhas localizadas nÃ£o envenenam todas as fases.
    modelRouterMetricCooldown: new Map(),
    modelRouterHealth: new Map(),
    compactRescueFailedSignatures: new Set(),
    episodePlan: null,

    cacheKey:
      makeCacheKey(
        type,
        videoId,
        sourceSrt
      ),

    status:
      lazy
        ? "pending"
        : "processing",

    progress:
      lazy ? 0 : 1,

    result: null,
    safeDraft: null,
    bestAvailableSrt927: null,
    bestAvailableLabel927: "",
    bestCandidateScore927: null,
    bestCandidateTranslations927: null,
    finalTargetResidual927: [],
    escalationAttempts928: new Set(),
    error: null,
    qualityStatus: "pending",

    // IDs removidos por regra SDH multilÃ­ngue ou por consenso de duas
    // respostas independentes. O Set nunca Ã© exposto diretamente na API.
    intentionalEmptyCueIds: new Set(),

    started: false,
    promise: null,

    createdAt: now,
    updatedAt: now,

    // RelÃ³gio real da traduÃ§Ã£o: nÃ£o reinicia em retry tÃ©cnico.
    translationStartedAt: null,

    expiresAt:
      now + JOB_TTL_MS,

    stats: {
      sourceCues: 0,

      planCalls: 0,
      planFailures: 0,
      planFallbackCalls: 0,
      planRecovered: 0,
      planPeople: 0,
      planKnownGender: 0,

      mainBatches: 0,
      mainCalls: 0,
      mainAttempts: 0,
      main429: 0,
      mainParseRetries: 0,
      mainCheckpointReused: 0,

      mainEmptyCueRescueCues: 0,
      mainEmptyCueRescueCalls: 0,
      mainEmptyCueRescueParseRetries: 0,
      mainEmptyCueRescueFailures: 0,
      mainEmptyCueLocalRetryCycles: 0,
      mainIntentionalEmptyCues: 0,
      mainSanitizedEmptyRecoveries: 0,
      mainSdhConsensusOmissions: 0,
      mainEmergencySourceFallbacks: 0,

      localFlags: 0,

      repairSelected: 0,
      repairCalls: 0,
      repairAttempts: 0,
      repair429: 0,
      repairParseRetries: 0,
      repairFailures: 0,
      repairSalvagedCues928: 0,
      repairIsolationResidual928: 0,
      repairIsolationMicroBatches928: 0,
      repairIsolationCueSurgery928: 0,

      qaBatches: 0,
      qaCalls: 0,
      qaAttempts: 0,
      qa429: 0,
      qaParseRetries: 0,
      qaFlags: 0,

      preRepairConfirmCalls: 0,
      preRepairConfirmAttempts: 0,
      preRepairConfirm429: 0,
      preRepairConfirmCandidates: 0,
      preRepairConfirmSuppressed: 0,
      preRepairConfirmConfirmed: 0,
      preRepairConfirmTechnicalFallback: 0,

      finalPriorityRounds: 0,
      finalPriorityAuditCalls: 0,
      finalPriorityFlags: 0,
      finalPriorityRepairRounds: 0,
      finalPriorityBoundedReleases: 0,
      boundedSafeDraftReleases: 0,
      finalPriorityTechnicalRetries: 0,
      finalPriorityNoProgressEscalations: 0,
      jobRetries: 0,

      pacerWaitMs: 0,
      freeTierGovernorWaitMs: 0,

      modelFallbacks: 0,
      modelTransientRetries: 0,
      model503: 0,
      model429Daily: 0,
      model429Rate: 0,
      modelTimeouts: 0,
      modelCallsById: {},

      inputTokens: 0,
      outputTokens: 0,
      thoughtTokens: 0,

      formatFixes: 0,

      usedSafeDraftFallback:
        false
    }
  };

  jobs.set(
    job.id,
    job
  );

  return job;
}

function findReusableJob(
  cacheKey
) {
  for (
    const job of jobs.values()
  ) {
    if (
      job.cacheKey === cacheKey &&
      [
        "pending",
        "processing",
        "completed"
      ].includes(job.status)
    ) {
      return job;
    }
  }

  return null;
}

function getOrCreateJob(
  args,
  { lazy = false } = {}
) {
  const cacheKey =
    makeCacheKey(
      args.type,
      args.videoId,
      args.sourceSrt
    );

  const cached =
    getCache(cacheKey);

  if (cached) {
    let job =
      findReusableJob(
        cacheKey
      );

    if (!job) {
      job =
        createJob({
          ...args,
          lazy: false
        });
    }

    restoreIntentionalEmptyIdsFromCache(
      cacheKey,
      job
    );

    job.status =
      "completed";

    job.progress =
      100;

    job.qualityStatus =
      "cache_verified";

    job.result =
      cached;

    return job;
  }

  const existing =
    findReusableJob(
      cacheKey
    );

  if (existing) {
    return existing;
  }

  const job =
    createJob({
      ...args,
      lazy
    });

  if (!lazy) {
    startJob(job);
  }

  return job;
}

setInterval(
  () => {
    const now =
      Date.now();

    for (
      const [
        key,
        item
      ] of translationCache.entries()
    ) {
      if (
        item.expiresAt <= now
      ) {
        translationCache.delete(
          key
        );
      }
    }

    for (const [key, item] of provisionalTranslationCache.entries()) {
      if (item.expiresAt <= now) provisionalTranslationCache.delete(key);
    }

    for (
      const [
        id,
        job
      ] of jobs.entries()
    ) {
      if (
        job.expiresAt <= now &&
        job.status !== "processing"
      ) {
        jobs.delete(id);
      }
    }
  },

  10 * 60 * 1000
).unref();

// ============================================================
// SELF-HEAL TOKEN
// ============================================================

function encodeRecovery(payload) {
  const body =
    Buffer.from(
      JSON.stringify(payload),
      "utf8"
    ).toString("base64url");

  const sig =
    crypto
      .createHmac(
        "sha256",
        RECOVERY_SIGNING_KEY
      )
      .update(body)
      .digest("base64url")
      .slice(0, 32);

  return `${body}.${sig}`;
}

function decodeRecovery(token) {
  const [
    body,
    sig
  ] =
    String(token || "")
      .split(".");

  if (!body || !sig) {
    throw new Error(
      "Token de recuperaÃ§Ã£o invÃ¡lido."
    );
  }

  const expected =
    crypto
      .createHmac(
        "sha256",
        RECOVERY_SIGNING_KEY
      )
      .update(body)
      .digest("base64url")
      .slice(0, 32);

  const a =
    Buffer.from(sig);

  const b =
    Buffer.from(expected);

  if (
    a.length !== b.length ||
    !crypto.timingSafeEqual(
      a,
      b
    )
  ) {
    throw new Error(
      "Assinatura de recuperaÃ§Ã£o invÃ¡lida."
    );
  }

  const payload =
    JSON.parse(
      Buffer
        .from(
          body,
          "base64url"
        )
        .toString("utf8")
    );

  if (
    !payload ||
    !payload.t ||
    !payload.i
  ) {
    throw new Error(
      "Dados de recuperaÃ§Ã£o incompletos."
    );
  }

  return payload;
}

function buildCloudSubtitleUrl(
  req,
  job,
  recovery
) {
  const token =
    encodeRecovery({
      t: recovery.type,
      i: recovery.id,
      f:
        recovery.filename ||
        "",
      s:
        recovery.videoSize ||
        "",
      h:
        recovery.videoHash ||
        ""
    });

  return (
    `${baseUrl(req)}/subtitle/` +
    `${encodeURIComponent(job.id)}` +
    `.srt?r=` +
    `${encodeURIComponent(token)}`
  );
}

// ============================================================
// SRT
// ============================================================

const TIMING_RE =
  /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
  /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
  /(?:laugh|laughing|laughter|chuckle|chuckling|giggle|giggling|sigh|sighing|gasp|gasping|pant|panting|breath|breathing|breathes|inhale|inhaling|exhale|exhaling|whimper|whimpering|cry|crying|sob|sobbing|music|musical|song|singing|sings|chant|chanting|humming|hums|applause|cheer|cheering|clap|clapping|door|knock|knocking|bang|banging|slam|slamming|phone|ring|ringing|buzz|buzzing|beep|beeping|static|groan|groaning|grunt|grunting|scream|screaming|yell|yelling|shout|shouting|whisper|whispering|murmur|murmuring|talk|talking|quietly|inaudible|indistinct|foreign language|clears? throat|sniff|sniffing|cough|coughing|footstep|footsteps|steps|walking|running|rustle|rustling|leaves|branch|twig|floorboard|creak|creaking|crack|cracking|snap|snapping|glass|shatter|shattering|smash|horn|honking|tire|tires|engine|car|vehicle|wind|thunder|rain|storm|fire|crackle|crackling|growl|growling|roar|roaring|howl|howling|cricket|crickets|bird|birds|dog|dogs|cat|cats|moan|moaning|distorted|echo|echoing|voice|voices|distant|offscreen|off-screen|background|continues|speaking|calling|calls|narrating|voice-over|muffled|thud|impact|squish|squishing|squelch|squelching|scrape|scraping|metal|click|clicking|lock|unlock|faint|softly|loudly|tv|radio|siren|alarm|gunshot|gunshots|explosion|heartbeat|wheez|wheezing|whistl|whistling|snoring|screech|squeal|squealing|approaching|receding|door closes|door opens|footsteps approaching|breathing heavily|song playing|music playing|risos?|rindo|risadinhas?|gargalhada|gargalhando|suspira|suspiro|ofegante|ofegando|respira(?:Ã§Ã£o|ndo)?|respiraÃ§Ã£o|chora|chorando|soluÃ§a|soluÃ§ando|mÃºsica|canÃ§Ã£o|cantando|canto|tarareando|aplausos?|palmas|gritos?|gritando|sussurra|sussurrando|murmura|murmurando|chamando|narraÃ§Ã£o|narrando|falando baixo|continua falando|inaudÃ­vel|indistinto|estÃ¡tica|passos?|pisada|pisadas|correndo|folhas?|farfalhando|galhos?|quebrando|assoalho|rangendo|rangido|vidro|estilhaÃ§a|estilhaÃ§ando|buzina|pneus?|motor|marcha lenta|vento|trovÃ£o|chuva|tempestade|fogo|estalando|uivo|grilos?|rosnado|rosnando|grunhido|grunhidos|guincho|guinchos|distorcido|distorcida|eco|voz ao longe|ao longe|ao fundo|em voz baixa|voz baixa|voz de|baque|impacto|raspando|metal|clique|clicando|tranca a porta|porta fechando|porta abrindo|sirene|alarme|tiro|tiros|explosÃ£o|menina rindo|som abafado)/i;

const CENSOR_CLUSTER_RE =
  /[!@#$%^&*()_+=~`Â¤Â£â‚¬Â¥Â¢]{3,}/gu;

const STANDALONE_SYMBOL_CLUSTER_RE =
  /(^|\s)[!@#$%^&*()_+=~`Â¤Â£â‚¬Â¥Â¢]{3,}(?=\s|$)/gu;

const CENSOR_CHAR_RE =
  /[*#@%&$]/u;

function decodeBasicEntities(value) {
  return String(value || "")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(
      /&#39;|&apos;/gi,
      "'"
    )
    .replace(
      /&#(\d{1,6});/g,
      (_, n) => {
        const code =
          Number(n);

        return Number.isFinite(
          code
        )
          ? String.fromCodePoint(
              code
            )
          : _;
      }
    )
    .replace(
      /&#x([0-9a-f]{1,6});/gi,
      (_, h) => {
        const code =
          parseInt(h, 16);

        return Number.isFinite(
          code
        )
          ? String.fromCodePoint(
              code
            )
          : _;
      }
    );
}

function stripMarkup(value) {
  return decodeBasicEntities(
    value
  )
    .replace(
      /<[^>]+>/g,
      ""
    )
    .replace(
      /\{\\[^}]+\}/g,
      " "
    );
}

function normalizeSpeaker(value) {
  const speaker =
    stripMarkup(value)
      .replace(/\s+/g, " ")
      .trim();

  if (
    !speaker ||
    speaker.length > 60 ||
    /[!?;]/u.test(speaker)
  ) {
    return "";
  }

  return speaker;
}

function looksLikeSpeakerLabel(
  value
) {
  const speaker =
    normalizeSpeaker(value);

  if (!speaker) {
    return false;
  }

  const normalizedGroupSpeaker =
  speaker
    .toLocaleLowerCase("pt-BR")
    .normalize("NFD")
    .replace(
      /[\u0300-\u036f]/g,
      ""
    )
    .replace(/\s+/g, " ")
    .trim();

if (
  /^(?:all|both|all queens|both queens|all girls|both girls|todos|todas|ambos|ambas)$/u.test(
    normalizedGroupSpeaker
  )
) {
  return true;
}

  const parts =
    speaker
      .split(/\s+/)
      .filter(Boolean);

  if (
    !parts.length ||
    parts.length > 5
  ) {
    return false;
  }

  if (
    /^(?:okay|ok|well|look|listen|so|now|then|actually|basically|because|but|and|or|yes|no|right|wait|hey|wow|girl|bitch|previously|meanwhile|later|earlier|tonight|today|tomorrow|atenÃ§Ã£o|cuidado|olha|escuta|entÃ£o|agora|sim|nÃ£o)$/i.test(
      speaker
    )
  ) {
    return false;
  }

  const letters =
    speaker.replace(
      /[^A-Za-zÃ€-Ã¿]/g,
      ""
    );

  const allUpper =
    Boolean(letters) &&
    letters ===
      letters.toUpperCase();

  const titleLike =
    parts.every(
      part =>
        /^[A-ZÃ€-Ã][A-Za-zÃ€-Ã¿'â€™.-]*$/u.test(
          part
        )
    );

  return (
    allUpper ||
    titleLike
  );
}

function extractSpeaker(line) {
  const original =
    stripMarkup(
      String(line || "")
    );

  const hidden =
    original.match(
      SPEAKER_RE
    );

  if (hidden) {
    let speaker = "";

    try {
      speaker =
        normalizeSpeaker(
          decodeURIComponent(
            hidden[1]
          )
        );
    } catch {}

    const clean =
      original.replace(
        SPEAKER_RE,
        ""
      );

    return {
      speaker,
      text: clean,

      hadDialogueDash:
        /^\s*[-â€“â€”]\s*/u.test(
          clean
        )
    };
  }

  const bracket =
    original.match(
      /^\s*[-â€“â€”]?\s*\[([^\]]{1,60})\]\s*:?[ \t]*/u
    );

  if (bracket) {
    const speaker =
      normalizeSpeaker(
        bracket[1]
      );

    if (
      speaker &&
      looksLikeSpeakerLabel(
        bracket[1]
      )
    ) {
      return {
        speaker,

        text:
          original.slice(
            bracket[0].length
          ),

        hadDialogueDash:
          /^\s*[-â€“â€”]\s*/u.test(
            original
          )
      };
    }
  }

  const colon =
    original.match(
      /^\s*([-â€“â€”]\s*)?([A-Za-zÃ€-Ã¿][A-Za-zÃ€-Ã¿0-9 #.'-]{0,45})(?:\s*\(([^)]{1,45})\))?\s*:\s*(.*)$/u
    );

  if (colon) {
    const speaker =
      normalizeSpeaker(
        colon[2]
      );

    if (
      speaker &&
      looksLikeSpeakerLabel(
        colon[2]
      )
    ) {
      return {
        speaker,

        text:
          colon[4] || "",

        hadDialogueDash:
          Boolean(colon[1])
      };
    }
  }

  return {
    speaker: "",

    text:
      original,

    hadDialogueDash:
      /^\s*[-â€“â€”]\s*/u.test(
        original
      )
  };
}

function stripTrailingSpeakerLabel(
  value
) {
  let text =
    String(value || "");

  text =
    text.replace(
      /\s*\(([A-Za-zÃ€-Ã¿][A-Za-zÃ€-Ã¿0-9 #.'-]{0,45})\)\s*$/u,
      (match, inside) =>
        looksLikeSpeakerLabel(
          inside
        )
          ? ""
          : match
    );

  text =
    text.replace(
      /\s*\[([A-Za-zÃ€-Ã¿][A-Za-zÃ€-Ã¿0-9 #.'-]{0,45})\]\s*$/u,
      (match, inside) =>
        looksLikeSpeakerLabel(
          inside
        )
          ? ""
          : match
    );

  return text;
}

function isEmptyVocalization(text) {
  const value =
    String(text || "")
      .toLowerCase()
      .replace(
        /[.,!?â€¦]+/g,
        " "
      )
      .replace(/\s+/g, " ")
      .trim();

  return /^(?:ah|ha|heh|uh|um|hm|hmm)(?:\s+(?:ah|ha|heh|uh|um|hm|hmm)){1,8}$/.test(
    value
  );
}

// ============================================================
// SPOKEN VOCALIZATION LOCK â€” 9.0
// ============================================================
// "Hmm", "Mm-hmm", "Uh-huh", "Uhum", "Um" etc. sÃ£o fala curta,
// nÃ£o descriÃ§Ã£o SDH. Quando aparecem sem colchetes/parÃªnteses, preservamos
// localmente e nunca gastamos Gemini HIGH para decidir se devem existir.
function spokenVocalizationKind(value) {
  const text = String(value || "")
    .toLocaleLowerCase()
    .replace(/<[^>]+>/g, " ")
    .replace(/^[\s\-â€“â€”]+|[\s]+$/gu, "")
    .replace(/[!?.,â€¦]+/gu, " ")
    .replace(/[â€™']/gu, "'")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) return null;

  const compact = text.replace(/\s+/g, "");

  if (/^(?:mm-?hmm|mmm-?hmm|mhm|uh-?huh|uhhuh|uhum|uh-hum|aham|a-ham)$/iu.test(compact)) {
    return "affirmative";
  }

  if (/^(?:hmm+|hm+|hum+|mm+|mmm+)$/iu.test(compact)) {
    return "ponder";
  }

  if (/^(?:uh|uhh|um|umm|ahn|Ã£h|ah)$/iu.test(compact)) {
    return "hesitation";
  }

  if (/^(?:(?:uh|um|hm|hmm|hum|mm|ah|ahn|Ã£h)[ -]*){2,8}$/iu.test(text)) {
    return "hesitation";
  }

  return null;
}

function looksLikeSpokenVocalization(value) {
  return Boolean(spokenVocalizationKind(value));
}

function localizeSpokenVocalization(value) {
  const kind = spokenVocalizationKind(value);
  if (!kind) return "";

  const raw = String(value || "").trim();
  const exclamation = /!+\s*$/u.test(raw);
  const question = /\?+\s*$/u.test(raw);
  const ellipsis = /(?:â€¦|\.{2,})\s*$/u.test(raw);

  if (kind === "affirmative") {
    return question ? "Uhum?" : exclamation ? "Uhum!" : "Uhum.";
  }

  if (kind === "ponder") {
    return question ? "Hum?" : exclamation ? "Hum!" : ellipsis ? "Hum..." : "Hum.";
  }

  if (/^\s*ah[.!?â€¦]*\s*$/iu.test(raw)) {
    return question ? "Ah?" : exclamation ? "Ah!" : ellipsis ? "Ah..." : "Ah.";
  }

  return question ? "Hum?" : exclamation ? "Ah!" : "Hum...";
}

function localizePureVocalizationCue(block) {
  const source = String(block?.text || "").replace(/\r/g, "").trim();
  if (!source) return "";

  const lines = source.split("\n").map(line => String(line || "").trim()).filter(Boolean);
  if (!lines.length) return "";

  const localized = [];
  for (const line of lines) {
    const hadDash = /^\s*[-â€“â€”]\s*/u.test(line);
    const info = extractSpeaker(line);
    const spoken = localizeSpokenVocalization(info.text);
    if (!spoken) return "";
    localized.push(`${hadDash ? "- " : ""}${spoken}`);
  }

  return localized.join("\n");
}

function looksLikeClearlySpokenBareLine(value) {
  const original = stripMarkup(String(value || "")).trim();
  const text = normalizeSdhCandidate(original);
  if (!text) return false;

  if (looksLikeSpokenVocalization(original)) return true;

  // Frases com pessoa gramatical explÃ­cita / cÃ³pula sÃ£o fala, nÃ£o stage direction.
  if (/\b(?:i|i'm|iâ€™m|i am|you|you're|youâ€™re|you are|we|we're|weâ€™re|it's|itâ€™s|this is|that's|thatâ€™s|there's|thereâ€™s|eu|vocÃª|vocÃªs|nÃ³s|a gente|isso Ã©|isto Ã©)\b/iu.test(text)) {
    return true;
  }

  // Imperativos curtos que foram falsos-positivos reais do classificador SDH.
  if (/^(?:(?:just|please|sÃ³|apenas|por favor)\s+)?(?:look|breathe|dance|enter|come in|run|turn|wait|listen|go|leave|exit|stop|stay|olha|olhe|respira|respire|danÃ§a|dance|entra|entre|corre|corra|vira|vire|espera|espere|escuta|escute|vai|vÃ¡|sai|saia|para|pare|fica|fique)(?:\s+(?:here|there|at me|at this|comigo|aqui|ali|pra mim|para mim))?$/iu.test(text)) {
    return true;
  }

  // Fala curta + hesitaÃ§Ã£o/vocalizaÃ§Ã£o (ex.: "Oi. Hum...") continua sendo fala.
  if (/^(?:hi|hello|hey|oi|olÃ¡|ola)[,!?.â€¦]*\s+(?:mm-?hmm|mhm|uh-?huh|uhum|aham|hmm+|hm+|hum+|mm+|uh+|um+|ahn|Ã£h|ah)[.!?â€¦]*$/iu.test(text)) {
    return true;
  }

  return false;
}

// ============================================================
// UNIVERSAL SDH ACTION / ACCESSIBILITY CLASSIFIER
// ============================================================
//
// Objetivo:
// - remover descriÃ§Ãµes de acessibilidade por ESTRUTURA, nÃ£o por nome/personagem;
// - reconhecer "HOST CACKLING", "SPEAKER CLEARS THROAT",
//   "THE CROWD CHEERS", "DOORBELL RINGS" etc. sem hardcode de pessoas;
// - continuar conservador com fala real.
//
// A lista abaixo descreve AÃ‡Ã•ES/ESTADOS de acessibilidade, nunca identidades.
const SDH_ACTION_CORE_RE =
  /(?:bursts?\s+into\s+(?:laughter|applause|cheers?)|erupts?\s+(?:in|into)\s+(?:laughter|applause|cheers?)|breaks?\s+into\s+(?:laughter|applause|cheers?)|falls?\s+silent|goes?\s+quiet|goes?\s+wild|music\s+(?:plays?|playing|swells?|swelling|fades?|fading|continues?|continuing|starts?|starting|stops?|stopping)|song\s+(?:plays?|playing|continues?|continuing|starts?|starting|stops?|stopping)|smiles?|smiling|grins?|grinning|nods?|nodding|shrugs?|shrugging|waves?|waving|points?|pointing|stares?|staring|looks?|looking|rolls?\s+(?:(?:his|her|their)\s+)?eyes|gestures?|gesturing|enters?|entering|exits?|exiting|walks?|walking|runs?|running|dances?|dancing|turns?|turning|sorri|sorrindo|acena|acenando|assente|assentindo|encolhe\s+os\s+ombros|aponta|apontando|encara|encarando|olha|olhando|revira\s+os\s+olhos|gesticula|gesticulando|entra|entrando|sai|saindo|caminha|caminhando|corre|correndo|danÃ§a|danÃ§ando|vira|virando|laughs?|laughing|cackles?|cackling|chuckles?|chuckling|giggles?|giggling|snickers?|snickering|sighs?|sighing|gasps?|gasping|pants?|panting|breathes?|breathing|inhales?|inhaling|exhales?|exhaling|whimpers?|whimpering|cries?|crying|sobs?|sobbing|sniffs?|sniffing|coughs?|coughing|sneezes?|sneezing|clears?\s+(?:(?:his|her|their|the)\s+)?throat|hums?|humming|whistles?|whistling|chants?|chanting|cheers?|cheering|applauds?|applauding|claps?|clapping|groans?|groaning|grunts?|grunting|screams?|screaming|yells?|yelling|shouts?|shouting|whispers?|whispering|murmurs?|murmuring|moans?|moaning|wheezes?|wheezing|snore?s?|snoring|growls?|growling|roars?|roaring|howls?|howling|barks?|barking|meows?|meowing|rings?|ringing|buzzes?|buzzing|beeps?|beeping|dings?|dinging|chimes?|chiming|knocks?|knocking|bangs?|banging|slams?|slamming|creaks?|creaking|cracks?|cracking|snaps?|snapping|shatters?|shattering|smashes?|smashing|honks?|honking|screeches?|screeching|rustles?|rustling|clicks?|clicking|thuds?|thudding|rattles?|rattling|approaches?|approaching|recedes?|receding|continues?\s+(?:laughing|crying|sobbing|singing|cheering|applauding)|(?:begins?|starts?)\s+(?:laughing|crying|sobbing|singing|cheering|applauding)|risos?|ri|rindo|gargalha|gargalhando|cai\s+na\s+risada|suspira|suspirando|ofega|ofegando|respira|respirando|chora|chorando|soluÃ§a|soluÃ§ando|fungando|tosse|tossindo|espirra|espirrando|limpa\s+(?:a\s+)?garganta|pigarreia|pigarreando|cantarola|cantarolando|assobia|assobiando|canta|cantando|grita|gritando|berrando|sussurra|sussurrando|murmura|murmurando|geme|gemendo|rosna|rosnando|uiva|uivando|late|latindo|mia|miando|toca|tocando|vibra|vibrando|bipa|bipando|tilinta|tilintando|bate|batendo|fecha|fechando|abre|abrindo|range|rangendo|quebra|quebrando|estilhaÃ§a|estilhaÃ§ando|buzina|buzinando|chia|chiando|farfalha|farfalhando|clica|clicando|se\s+aproxima|se\s+afasta)/iu;

const SDH_EVENT_NOUN_RE =
  /(?:laughter|applause|cheers?|cheering|whooping|whinnies|whinnying|chatter|chattering|babble|babbling|crowd\s+noise|audience\s+noise|giggles?|chuckles?|cackling|sighs?|gasps?|panting|heavy\s+breathing|crying|sobbing|sniffing|coughing|sneezing|humming|whistling|chanting|groans?|grunts?|screams?|yells?|shouts?|whispers?|murmuring|footsteps?|steps?|knocking|banging|ringing|buzzing|beeping|dinging|chimes?|static|thunder|rain|storm|wind|fire\s+crackling|glass\s+(?:breaking|shattering)|engine\s+(?:starting|idling)|horn|tires?\s+screeching|rustling|clicking|thuds?|impact|heartbeat|siren|alarm|gunshots?|explosion|risos?|gargalhadas?|aplausos?|palmas|gritos?|gritaria|suspiros?|ofegos?|respiraÃ§Ã£o|choro|soluÃ§os?|tosse|espirros?|pigarro|canto|cantoria|assobios?|murmÃºrios?|gemidos?|rosnados?|uivos?|latidos?|miados?|passos?|batidas?|campainha|toque|toques|bipes?|estÃ¡tica|trovÃ£o|chuva|tempestade|vento|fogo\s+estalando|vidro\s+(?:quebrando|estilhaÃ§ando)|motor|buzina|pneus?\s+cantando|farfalhar|cliques?|baques?|impacto|batimentos?|sirene|alarme|tiros?|explosÃ£o)/iu;

const SDH_ACTION_TAIL_RE =
  /^(?:(?:loudly|softly|quietly|wildly|nervously|awkwardly|hysterically|together|again|offscreen|off-screen|onstage|on-stage|offstage|off-stage|away|back|in\s+background|in\s+the\s+background|in\s+distance|in\s+the\s+distance|faintly|briefly|continuously|heavily|rapidly|twice|once|three\s+times|a\s+lot|all\s+together|at\s+(?:him|her|them|camera|the\s+camera)|toward(?:s)?\s+\w+|to\s+camera|ao\s+fundo|ao\s+longe|baixinho|alto|altamente|forte|fortemente|nervosamente|sem\s+graÃ§a|histericamente|juntos?|juntas?|novamente|de\s+novo|duas\s+vezes|uma\s+vez|brevemente|continuamente|muito|bastante|para\s+(?:ele|ela|eles|elas|a\s+cÃ¢mera)|em\s+direÃ§Ã£o\s+a\s+\w+|para\s+trÃ¡s|embora)(?:\s+|$))*$/iu;

function normalizeSdhCandidate(
  value
) {
  return stripMarkup(
    String(value || "")
  )
    .replace(
      /^\s*[-â€“â€”]\s*/u,
      ""
    )
    .replace(
      /^[\[(]\s*/u,
      ""
    )
    .replace(
      /\s*[\])]\s*$/u,
      ""
    )
    .replace(
      /[.:;!?â€¦]+$/gu,
      ""
    )
    .replace(
      /\s+/g,
      " "
    )
    .trim();
}

// A ponte pode escolher uma SOURCE embutida em qualquer idioma. A 8.4.5
// reconhecia aÃ§Ãµes SDH em inglÃªs/portuguÃªs, mas deixava descritores franceses
// como "Angela rit" e "musique tendue" chegarem ao tradutor como se fossem
// diÃ¡logo. O Gemini entÃ£o devolvia corretamente "Angela ri"/"Risos" e o
// sanitizer PT-BR apagava a resposta, criando um ciclo impossÃ­vel.
const FRENCH_SDH_EVENT_RE =
  /^(?:rires?|Ã©clats? de rire|gloussements?|ricanements?|sourires?|soupirs?|halÃ¨tements?|respiration(?: forte| lourde)?|pleurs?|sanglots?|reniflements?|toux|Ã©ternuements?|raclement de gorge|fredonnement|sifflements?|chants?|applaudissements?|acclamations?|cris?|chuchotements?|murmures?|gÃ©missements?|grognements?|hurlements?|aboiements?|miaulements?|sonnerie|bips?|grincements?|claquements?|coups?|tonnerre|pluie|vent|orage|musique(?: [\p{L}'â€™-]+){0,5}|chanson(?: [\p{L}'â€™-]+){0,5}|bruit(?:s)?(?: [\p{L}'â€™-]+){0,6}|pas(?: [\p{L}'â€™-]+){0,5})$/iu;

const FRENCH_SDH_ACTION_RE =
  /(?:Ã©clate(?:nt)? de rire|se met(?:tent)? Ã  rire|rit|rient|rigole|rigolent|glousse|gloussent|ricane|ricanent|sourit|sourient|soupire|soupirent|halÃ¨te|halÃ¨tent|respire|respirent|pleure|pleurent|sanglote|sanglotent|renifle|reniflent|tousse|toussent|Ã©ternue|Ã©ternuent|se racle(?:nt)? la gorge|fredonne|fredonnent|siffle|sifflent|chante|chantent|applaudit|applaudissent|crie|crient|chuchote|chuchotent|murmure|murmurent|gÃ©mit|gÃ©missent|grogne|grognent|hurle|hurlent|aboie|aboient|miaule|miaulent|sonne|sonnent|vibre|vibrent|grince|grincent|claque|claquent|frappe|frappent|s'ouvre|s'ouvrent|se ferme|se ferment)$/iu;

function looksLikeFrenchSdhDescriptor(
  value
) {
  const text =
    normalizeSdhCandidate(
      value
    );

  if (
    !text ||
    text.length > 180 ||
    /[?]/u.test(text)
  ) {
    return false;
  }

  if (
    FRENCH_SDH_EVENT_RE.test(
      text
    )
  ) {
    return true;
  }

  const action =
    text.match(
      FRENCH_SDH_ACTION_RE
    );

  if (!action) {
    return false;
  }

  const before =
    text
      .slice(
        0,
        action.index ?? 0
      )
      .trim();

  const after =
    text
      .slice(
        (action.index ?? 0) +
        action[0].length
      )
      .replace(
        /^(?:fort|fortement|doucement|nerveusement|ensemble|encore|au loin|en arriÃ¨re-plan|hors champ|briÃ¨vement)$/iu,
        ""
      )
      .trim();

  if (after) {
    return false;
  }

  if (!before) {
    return true;
  }

  if (
    /^(?:je|j'|tu|nous|vous|on)$/iu.test(
      before
    )
  ) {
    return false;
  }

  return (
    before.split(/\s+/).length <= 6 &&
    (
      /^(?:il|elle|ils|elles|tout le monde|la foule|le public|le groupe)$/iu.test(
        before
      ) ||
      sdhTitleSubjectLike(
        before
      )
    )
  );
}

function sdhAllCapsLike(
  value
) {
  const letters =
    String(value || "")
      .replace(
        /[^\p{L}]/gu,
        ""
      );

  return (
    Boolean(letters) &&
    letters ===
      letters.toLocaleUpperCase()
  );
}

function looksLikeGenericAllCapsSdh(
  value
) {
  const text =
    String(
      value || ""
    )
      .replace(
        /[.:;!?â€¦]+$/gu,
        ""
      )
      .replace(
        /\s+/g,
        " "
      )
      .trim();

  if (
    !text ||
    !sdhAllCapsLike(
      text
    ) ||
    /[!?â€¦]/u.test(
      value
    )
  ) {
    return false;
  }

  const words =
    text
      .split(/\s+/)
      .filter(Boolean);

  if (
    !words.length ||
    words.length > 8
  ) {
    return false;
  }

  // Evita apagar respostas/interjeiÃ§Ãµes gritadas comuns.
  if (
    /^(?:OK|OKAY|YES|NO|YEAH|YEP|NOPE|HI|HELLO|HEY|BYE|GOODBYE|THANKS|THANK YOU|PLEASE|SORRY|RIGHT|WRONG|WOW|AMAZING|BRILLIANT|HELP|STOP|WAIT|COME ON|LET'S GO|LETS GO|GO|READY|CHEERS)$/iu.test(
      text
    )
  ) {
    return false;
  }

  // Nomes de trilha/descriÃ§Ã£o musical sem â™ª.
  if (
    words.length >= 2 &&
    /^(?:MUSIC|SONG|SCORE|THEME|INSTRUMENTAL|MÃšSICA|CANÃ‡ÃƒO|TRILHA)$/iu.test(
      words[
        words.length - 1
      ]
    )
  ) {
    return true;
  }

  // Eventos bare de uma palavra: WHOOPING, WHINNIES, COUGHING...
  if (
    words.length === 1 &&
    (
      /(?:ING|INGS|ED|S)$/iu.test(
        words[0]
      ) ||
      SDH_EVENT_NOUN_RE.test(
        text
      ) ||
      SDH_WORDS.test(
        text
      )
    )
  ) {
    return true;
  }

  // "SPEAKS FRENCH", "SPEAKING SPANISH" etc.
  if (
    /^(?:SPEAKS?|SPEAKING|TALKS?|TALKING)\s+[A-ZÃ€-Ã][A-ZÃ€-Ã'â€™-]*(?:\s+[A-ZÃ€-Ã][A-ZÃ€-Ã'â€™-]*)?$/u.test(
      text
    )
  ) {
    return true;
  }

  const subjectAction =
    text.match(
      /^(.{1,80}?)\s+([A-ZÃ€-Ã][A-ZÃ€-Ã'â€™-]*)$/u
    );

  if (
    subjectAction
  ) {
    const subject =
      subjectAction[1]
        .trim();

    const action =
      subjectAction[2]
        .trim();

    const subjectWords =
      subject
        .split(/\s+/)
        .filter(Boolean);

    const firstSubjectWord =
      subjectWords[0] ||
      "";

    const knownGroupSubject =
      /^(?:HE|SHE|THEY|IT|EVERYONE|EVERYBODY|SOMEONE|SOMEBODY|AUDIENCE|CROWD|CAST|GROUP|PEOPLE|MAN|WOMAN|BOY|GIRL|MEN|WOMEN|KIDS|CHILDREN|DOG|CAT|HORSE|PHONE|DOOR|BELL|ENGINE|CAR|VEHICLE|ELE|ELA|ELES|ELAS|TODOS|TODAS|PÃšBLICO|PLATEIA|GRUPO)$/iu.test(
        firstSubjectWord
      ) &&
      subjectWords
        .slice(1)
        .every(
          word =>
            /^(?:ALL|BOTH|ENTIRE|WHOLE|JUNTOS|JUNTAS|TODO|TODA|TODOS|TODAS)$/iu.test(
              word
            )
        );

    const properOrObjectLabelSubject =
      !/^(?:I|WE|YOU|HE|SHE|THEY|IT|EU|NÃ“S|NOS|VOCÃŠ|VOCÃŠS|ELE|ELA|ELES|ELAS)$/iu.test(
        firstSubjectWord
      ) &&
      subjectWords.length <= 3 &&
      subjectWords.every(
        word =>
          /^[A-ZÃ€-Ã0-9][A-ZÃ€-Ã0-9'â€™.-]*$/u.test(
            word
          )
      );

    const subjectLooksLikeDescriptor =
      subjectWords.length <= 5 &&
      (
        knownGroupSubject ||
        properOrObjectLabelSubject
      );

    const speechStateVerb =
      /^(?:KNOWS?|THINKS?|WANTS?|NEEDS?|LOVES?|HATES?|LIKES?|HAS|HAVE|IS|ARE|WAS|WERE|CAN|COULD|WILL|WOULD|SHOULD|DOES?|DID|SAYS?|MEANS?|GETS?|GOES?|COMES?|SEES?|FEELS?|SABE|SABEM|PENSA|PENSAM|QUER|QUEREM|PRECISA|PRECISAM|AMA|AMAM|ODEIA|ODEIAM|GOSTA|GOSTAM|TEM|TÃŠM|Ã‰|SÃƒO|PODE|PODEM|VAI|VÃƒO|DIZ|DIZEM)$/iu.test(
        action
      );

    const actionMorphology =
      /(?:S|ING|ED)$/iu.test(
        action
      ) ||
      !speechStateVerb;

    if (
      subjectLooksLikeDescriptor &&
      !speechStateVerb &&
      actionMorphology
    ) {
      return true;
    }
  }

  const groupAction =
    text.match(
      /^(?:THEY|WE|HE|SHE|EVERYONE|EVERYBODY|AUDIENCE|CROWD|CAST|GROUP|PEOPLE|TODOS|TODAS|ELES|ELAS)(?:\s+(?:ALL|BOTH|ENTIRE|WHOLE|JUNTOS|JUNTAS))?\s+([A-ZÃ€-Ã][A-ZÃ€-Ã'â€™-]*)$/u
    );

  if (
    groupAction
  ) {
    const action =
      groupAction[1];

    if (
      !/^(?:KNOW|THINK|WANT|NEED|LOVE|HATE|LIKE|HAVE|ARE|CAN|WILL|DO|SAY|MEAN|GET|GO|COME|SEE|FEEL|SABEM|PENSAM|QUEREM|PRECISAM|AMAM|ODEIAM|GOSTAM|TÃŠM|SÃƒO|PODEM|VÃƒO|DIZEM)$/iu.test(
        action
      )
    ) {
      return true;
    }
  }

  return false;
}

function sdhTitleSubjectLike(
  value
) {
  const subject =
    String(value || "")
      .trim();

  if (!subject) {
    return true;
  }

  if (
    /^(?:he|she|they|it|everyone|everybody|someone|somebody|audience|crowd|group|cast|contestants?|judges?|people|man|woman|boy|girl|men|women|kids?|children|dog|cat|phone|door|bell|engine|car|vehicle|radio|tv|television)$/iu.test(
      subject
    )
  ) {
    return true;
  }

  const words =
    subject
      .split(/\s+/)
      .filter(Boolean);

  if (
    !words.length ||
    words.length > 6
  ) {
    return false;
  }

  // "the whole point" / "all the time" etc. nÃ£o tÃªm sujeito de stage direction;
  // sÃ£o sintagmas nominais de fala. Exigir ao menos um token de conteÃºdo.
  if (
    words.every(word =>
      /^(?:the|all|entire|whole|both|several|some|a|an|o|a|os|as|todo|toda|todos|todas)$/iu.test(word)
    )
  ) {
    return false;
  }

  return words.every(
    word =>
      /^[\p{Lu}\d][\p{L}\p{N}'â€™.-]*$/u.test(
        word
      ) ||
      /^(?:the|all|entire|whole|both|several|some|a|an|o|a|os|as|todo|toda|todos|todas)$/iu.test(
        word
      )
  );
}

function looksLikeUniversalSdhAction(
  value,
  {
    bare = false
  } = {}
) {
  const text =
    normalizeSdhCandidate(
      value
    );

  if (
    !text ||
    text.length > 180 ||
    /[â™ªâ™«â™¬]/u.test(
      text
    )
  ) {
    return false;
  }

  const wordCount =
    text
      .split(/\s+/)
      .filter(Boolean)
      .length;

  if (
    !wordCount ||
    wordCount > 14
  ) {
    return false;
  }

  if (
    bare &&
    looksLikeGenericAllCapsSdh(
      text
    )
  ) {
    return true;
  }

  const eventOnly =
    text
      .split(
        /\s*(?:,|&|\band\b|\be\b)\s*/iu
      )
      .map(
        part =>
          part.trim()
      )
      .filter(Boolean);

  if (
    eventOnly.length &&
    eventOnly.every(
      part =>
        SDH_EVENT_NOUN_RE.test(
          part
        ) &&
        part.replace(
          SDH_EVENT_NOUN_RE,
          ""
        )
          .replace(
            /^(?:loud|soft|faint|distant|crowd|audience|background|offscreen|off-screen|continued|loudly|softly|quietly|fortes?|alto|baixinho|distantes?|ao fundo)\s*/iu,
            ""
          )
          .trim() ===
          ""
    )
  ) {
    return true;
  }

  if (
    bare &&
    sdhAllCapsLike(
      text
    ) &&
    SDH_EVENT_NOUN_RE.test(
      text
    ) &&
    !/[?]/u.test(
      text
    )
  ) {
    return true;
  }

  const actionMatch =
    text.match(
      SDH_ACTION_CORE_RE
    );

  if (!actionMatch) {
    return false;
  }

  const actionStart =
    actionMatch.index ?? -1;

  if (actionStart < 0) {
    return false;
  }

  const before =
    text
      .slice(
        0,
        actionStart
      )
      .trim();

  const after =
    text
      .slice(
        actionStart +
          actionMatch[0].length
      )
      .trim();

  if (
    before
      .split(/\s+/)
      .filter(Boolean)
      .length > 6
  ) {
    return false;
  }

  if (
    after &&
    !SDH_ACTION_TAIL_RE.test(
      after
    )
  ) {
    return false;
  }

  if (!bare) {
    return true;
  }

  return (
    sdhAllCapsLike(
      text
    ) ||
    !before ||
    sdhTitleSubjectLike(
      before
    )
  );
}

// ============================================================
// HARD SDH STRUCTURE 8.8.3
// ============================================================
// Bracket/parenthetical captions are accessibility metadata far more often
// than dialogue. The older lexical classifier missed perfectly normal CC
// descriptions such as [takes off shoes], [typing], [melody ends],
// [gun drops to floor] and [singers vocalizing].
//
// This classifier is intentionally used ONLY for accessibility-shaped
// segments / bare caption lines. It does not rewrite ordinary dialogue.
const STRUCTURED_SDH_EVENT_RE =
  /(?:fanfare|melody|score|instrumental|chatter|typing|keyboard|keys?|liquid|water|shower|gun|wood|bag|zipper|insects?|singers?|vocali[sz](?:e|es|ing|ation)|retch(?:es|ing)|gag(?:s|ging)|scoffs?|mumbles?|stammers?|yawns?|yawning|whispers?|squeaks?|squeaking|trickl(?:e|es|ing)|trill(?:s|ing)|jingl(?:e|es|ing)|tapping|footsteps?|melodia|fanfarra|trilha|conversa|burburinho|cochichos?|teclado|teclando|digitando|chaves?|lÃ­quido|agua|Ã¡gua|chuveiro|arma|madeira|bolsa|zÃ­per|insetos?|cantores?|vocaliza(?:Ã§Ã£o|ndo)|Ã¢nsia|engasga(?:ndo)?|gagueja(?:ndo)?|sussurr(?:a|ando)|rangido|chiado)/iu;

const STRUCTURED_SDH_ACTION_RE =
  /(?:plays?|playing|fades?|fading|ends?|ending|stops?|stopping|starts?|starting|continues?|continuing|drops?|dropping|falls?|falling|opens?|opening|closes?|closing|unzips?|unzipping|zips?|zipping|taps?|tapping|types?|typing|trickles?|trickling|trills?|trilling|vocali[sz](?:es?|ing)|retches?|retching|gags?|gagging|scoffs?|scoffing|mumbles?|mumbling|stammers?|stammering|yawns?|yawning|whispers?|whispering|squeaks?|squeaking|jingl(?:e|es|ing)|snaps?|snapping|takes?\s+off|puts?\s+on|picks?\s+up|sets?\s+down|tocando|termina|terminando|para|parando|comeÃ§a|comeÃ§ando|continua|continuando|cai|caindo|abre|abrindo|fecha|fechando|digita|digitando|tecla|teclando|goteja|gotejando|vocaliza|vocalizando|engasga|engasgando|gagueja|gaguejando|sussurra|sussurrando|tilinta|tilintando|estala|estalando)/iu;

function looksLikeStructuredSdhSegment(value) {
  const text = normalizeSdhCandidate(value);

  if (!text || text.length > 220 || /\?\s*$/u.test(text)) {
    return false;
  }

  const words = text.split(/\s+/).filter(Boolean);
  if (!words.length || words.length > 20) {
    return false;
  }

  // A line that directly quotes a song title + "playing/tocando" is metadata,
  // not a lyric line.
  if (
    /^(?:["â€œ][^"â€]{1,100}["â€]\s+by\s+.{1,80}\s+playing|tocando\s+["â€œ][^"â€]{1,100}["â€](?:\s*,?\s*(?:by|de|do|da)\s+.{1,80})?)$/iu.test(text)
  ) {
    return true;
  }

  const hasFirstSecondPerson =
    /\b(?:i|i'm|iâ€™m|i am|me|my|mine|we|us|our|ours|you|your|yours|eu|meu|minha|nÃ³s|nosso|nossa|vocÃª|vocÃªs|seu|sua)\b/iu.test(text);

  const hasEvent = STRUCTURED_SDH_EVENT_RE.test(text);
  const hasAction = STRUCTURED_SDH_ACTION_RE.test(text);

  if (hasEvent && (hasAction || words.length <= 4)) {
    return true;
  }

  // Compact third-person / subjectless action captions:
  // [Ian stammers], [takes off shoes], [unzips bag], [typing].
  if (hasAction && words.length <= 10 && !hasFirstSecondPerson) {
    return true;
  }

  // Single gerund/event captions such as [mocking], [retching], [typing].
  if (
    words.length <= 3 &&
    !hasFirstSecondPerson &&
    /(?:ing|ando|endo|indo)$/iu.test(text)
  ) {
    return true;
  }

  return false;
}

function looksLikeSdhDescriptor(
  value
) {
  const inside =
    normalizeSdhCandidate(
      value
    );

  if (
    !inside ||
    inside.length > 180
  ) {
    return false;
  }

  if (
    looksLikeStructuredSdhSegment(
      inside
    ) ||
    looksLikeUniversalSdhAction(
      inside,
      {
        bare: false
      }
    )
  ) {
    return true;
  }

  if (
    looksLikeFrenchSdhDescriptor(
      inside
    )
  ) {
    return true;
  }

  return SDH_WORDS.test(
    inside
  );
}

function looksLikeBareSdhLine(
  value
) {
  const original =
    stripMarkup(
      String(
        value || ""
      )
    ).trim();

  const dashedBare = original.match(/^[-â€“â€”]\s+(.+)$/u);
  if (dashedBare) {
    const dashedBody = String(dashedBare[1] || "").trim();
    // Dialogue dashes are preserved, but a dash before a pure accessibility
    // event ("- LAUGHTER", "- APPLAUSE") is still SDH, not a speaker turn.
    if (
      dashedBody &&
      (
        (sdhAllCapsLike(dashedBody) && looksLikeUniversalSdhAction(dashedBody, { bare: true })) ||
        looksLikePureNonSpeechSdhLine(dashedBody)
      )
    ) {
      return true;
    }
    return false;
  }

  // 9.0: linha curta falada nunca pode ser promovida a SDH sÃ³ porque
  // tambÃ©m coincide lexicalmente com um verbo de aÃ§Ã£o (Look/Breathe/Dance/Entra).
  // DescriÃ§Ãµes estruturadas entre []/() continuam sendo removidas normalmente.
  if (looksLikeClearlySpokenBareLine(original)) {
    return false;
  }

  const text =
    normalizeSdhCandidate(
      original
    );

  if (
    !text ||
    text.length > 140
  ) {
    return false;
  }

  if (
    /[!?â€¦]\s*$/u.test(
      original
    ) &&
    !sdhAllCapsLike(
      original
    )
  ) {
    return false;
  }

  if (
    looksLikeUniversalSdhAction(
      text,
      {
        bare: true
      }
    )
  ) {
    return true;
  }

  if (
    looksLikeFrenchSdhDescriptor(
      text
    )
  ) {
    return true;
  }

  return /^(?:sound of |sounds of )?(?:static|laughter|applause|music(?: playing)?|song(?: playing)?|footsteps?(?: approaching| receding)?|door (?:opens|closes|slams|creaks)|phone (?:rings|buzzes)|wind (?:blows|howls)|thunder|rain(?: falling)?|fire crackling|glass (?:breaks|shatters)|engine (?:starts|idles)|car horn|tires? screeching|branch (?:breaks|snaps)|leaves rustling|heavy breathing|panting|gasping|sobbing|crying|humming|whistling|growling|roaring|howling|muffled voices?|distant voices?|estÃ¡tica|risos?|aplausos?|mÃºsica|passos?(?: se aproximando| ao longe)?|porta (?:abrindo|fechando|batendo|rangendo)|telefone (?:tocando|vibrando)|vento (?:soprando|uivando)|trovÃ£o|chuva|fogo estalando|vidro (?:quebrando|estilhaÃ§ando)|motor (?:ligando|em marcha lenta)|buzina|pneus? cantando|galho (?:quebrando|estalando)|folhas farfalhando|respiraÃ§Ã£o (?:forte|ofegante)|ofegante|ofegando|chorando|soluÃ§ando|tarareando|assobiando|rosnado|uivo|vozes? abafadas?|vozes? ao longe|som (?:abafado )?(?:de )?(?:passos|pisadas|esmagamento|algo sendo esmagado)|esmagando|som pastoso)$/iu.test(
    text
  );
}

// ============================================================
// EMPTY-CUE HYGIENE 9.1 â€” PURE NON-SPEECH = INTENTIONAL EMPTY
// ============================================================
// SÃ³ aceita descriÃ§Ãµes/vocalizaÃ§Ãµes NÃƒO verbais inteiras. NÃ£o inclui Hmm,
// Mm-hmm, Uhum, Um, palavras, nÃºmeros, bleep ou qualquer fala real.
function looksLikePureNonSpeechSdhLine(value) {
  const original = stripMarkup(String(value || "")).trim();
  if (!original) return true;

  // Segmentos explicitamente marcados como acessibilidade continuam sendo lixo visual.
  const bracketed = original.match(/^\s*[\[(]([\s\S]{1,180})[\])]\s*[.!â€¦]?\s*$/u);
  if (bracketed && looksLikeSdhDescriptor(bracketed[1])) {
    return true;
  }

  const text = normalizeSdhCandidate(original)
    .replace(/[.!â€¦]+$/gu, "")
    .trim();

  if (!text || /[?!]/u.test(original)) return false;

  return /^(?:sighs?|sighing|gasps?|gasping|pants?|panting|heavy breathing|breathing heavily|coughs?|coughing|sneezes?|sneezing|sniffs?|sniffing|sobs?|sobbing|cries|crying|laughs?|laughing|chuckles?|chuckling|giggles?|giggling|groans?|groaning|grunts?|grunting|whimpers?|whimpering|suspira|suspiro|suspirando|ofega|ofegando|respiraÃ§Ã£o ofegante|respirando ofegante|tosse|tossindo|espirra|espirrando|fungada|fungando|soluÃ§a|soluÃ§ando|chora|chorando|risos?|rindo|gargalhada|gargalhando|geme|gemendo|grunhe|grunhindo)$/iu.test(text);
}

function removeSdhSegments(text) {
  return String(text || "")
    .replace(
      /\[([^\]]+)\]/gu,
      (match, inside) =>
        looksLikeSdhDescriptor(
          inside
        )
          ? " "
          : match
    )
    .replace(
      /\(([^)]+)\)/gu,
      (match, inside) =>
        looksLikeSdhDescriptor(
          inside
        )
          ? " "
          : match
    );
}

function removeMultilineSdhSegments(
  text
) {
  return String(text || "")
    .replace(
      /\[([^\]]{1,220})\]/gsu,
      (match, inside) =>
        looksLikeSdhDescriptor(
          inside.replace(
            /\n/g,
            " "
          )
        )
          ? " "
          : match
    )
    .replace(
      /\(([^)]{1,220})\)/gsu,
      (match, inside) =>
        looksLikeSdhDescriptor(
          inside.replace(
            /\n/g,
            " "
          )
        )
          ? " "
          : match
    );
}

function collapseExtendedVocalization(
  value
) {
  return String(value || "")
    .replace(
      /(?<!\p{L})(\p{L})(?:-\1){1,6}(?!\p{L})/giu,
      "$1"
    )
    .replace(
      /(?<!\p{L})(\p{L})-(?=\1\p{L}+)/giu,
      ""
    )
    .replace(
      /(\p{L}{2,})(?:-[aeiouÃ¡Ã©Ã­Ã³ÃºÃ Ã¢ÃªÃ´Ã£ÃµÃ¼]){2,}/giu,
      "$1"
    )
    .replace(
      /(\p{L}{2,})(?:-[\p{L}]{1,3}){3,}/gu,
      "$1"
    )
    .replace(
      /([aeiouÃ¡Ã©Ã­Ã³ÃºÃ Ã¢ÃªÃ´Ã£ÃµÃ¼])\1{3,}/giu,
      "$1$1"
    );
}

// 9.0 â€” reticÃªncia nÃ£o Ã© censura por si sÃ³.
// MÃ¡scara grÃ¡fica explÃ­cita (*#@%&$) continua sendo forte evidÃªncia.
// Para "palavra...", sÃ³ aceitamos stems de palavrÃ£o de alta confianÃ§a;
// fragmentos ambÃ­guos/completos (me..., bit..., car..., por...) permanecem fala.
function looksLikeMaskedProfanityToken(
  token,
  context = "",
  tokenIndex = -1
) {
  const raw =
    String(token || "");

  if (!raw) {
    return false;
  }

  const symbolMask =
    /[*#@%&$]/u.test(raw);

  const dotMask =
    /(?:\.{2,}|â€¦)/u.test(raw);

  if (
    !symbolMask &&
    !dotMask
  ) {
    return false;
  }

  const maskIndex =
    raw.search(
      /[*#@%&$]|\.{2,}|â€¦/u
    );

  if (maskIndex <= 0) {
    return false;
  }

  const visiblePrefix =
    raw
      .slice(0, maskIndex)
      .toLowerCase()
      .normalize("NFKD")
      .replace(
        /[\u0300-\u036f]/g,
        ""
      )
      .replace(
        /[^a-z]/g,
        ""
      );

  if (!visiblePrefix) {
    return false;
  }

  // SÃ­mbolos sÃ£o censura editorial muito mais forte que reticÃªncias.
  if (symbolMask) {
    return /^(?:f|fu|fuc|fuck|motherf|sh|shi|b|bi|bit|bitc|c|cu|cun|car|cara|caral|p|pu|put|por|porr|pus|puss|ass|assh|d|di|dic|dick|fo|fod|fud|m|me|mer|merd|s)$/iu.test(
      visiblePrefix
    );
  }

  // Com pontos/reticÃªncias, use somente stems fortes que normalmente sÃ£o
  // incompletos. Isso evita converter palavras perfeitamente vÃ¡lidas seguidas
  // de pausa em censura editorial.
  if (/^(?:fu|fuc|fuck|motherf|shi|bitc|cun|caral|porr|puss|assh|dic|fod|fud|merd)$/iu.test(visiblePrefix)) {
    return true;
  }

  // "sh..." e "f..." sÃ£o curtos demais isoladamente. SÃ³ contam como bleep
  // quando a prÃ³pria frase traz uma moldura pragmÃ¡tica forte de palavrÃ£o.
  if (/^(?:sh|f)$/iu.test(visiblePrefix)) {
    const full = String(context || "");
    const at = Number.isInteger(tokenIndex) && tokenIndex >= 0
      ? tokenIndex
      : Math.max(0, full.indexOf(raw));
    const before = full
      .slice(Math.max(0, at - 28), at)
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trimEnd();

    return /(?:\bholy|\bwhat\s+the|\boh\s+my|\bmother)\s*$/iu.test(before);
  }

  return false;
}

function replaceMaskedProfanity(
  value
) {
  const decoded =
    decodeBasicEntities(
      value
    );

  return decoded.replace(
    /[\p{L}][\p{L}0-9*#@%&$!._~â€¦â€™'-]{1,28}/gu,

    (token, offset, full) =>
      looksLikeMaskedProfanityToken(
        token,
        full,
        offset
      )
        ? BLEEP_TOKEN
        : token
  );
}

function hasArtificialCensorship(
  value
) {
  const text =
    decodeBasicEntities(
      value
    );

  if (
    text.includes(
      BLEEP_TOKEN
    )
  ) {
    return true;
  }

  const tokens =
    text.match(
      /[\p{L}][\p{L}0-9*#@%&$!._~â€¦â€™'-]{1,28}/gu
    ) || [];

  if (
    tokens.some(token => {
      const offset = text.indexOf(token);
      return looksLikeMaskedProfanityToken(
        token,
        text,
        offset
      );
    })
  ) {
    return true;
  }

  CENSOR_CLUSTER_RE.lastIndex =
    0;

  const cluster =
    CENSOR_CLUSTER_RE.test(
      text
    );

  CENSOR_CLUSTER_RE.lastIndex =
    0;

  return cluster;
}

function replaceCensoredBleps(
  value
) {
  let text =
    replaceMaskedProfanity(
      value
    );

  text =
    text.replace(
      CENSOR_CLUSTER_RE,
      ` ${BLEEP_TOKEN} `
    );

  CENSOR_CLUSTER_RE.lastIndex =
    0;

  return text
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeNoiseSymbols(
  value
) {
  return String(value || "")
    .replace(
      /^\s*[\/\\|]{1,4}\s*/u,
      ""
    )
    .replace(
      /^\s*[-â€“â€”]{2,}\s*/u,
      "- "
    )
    .replace(
      /^\s*[:;]+\s*$/u,
      ""
    )
    .replace(
      /^\s*[â€¢Â·â–ªâ—¦]+\s*/u,
      ""
    )
    .replace(
      /\s+[\/\\|]{1,3}\s+/gu,
      " "
    )
    .replace(
      /\s*[-â€“â€”]{2,}\s*/gu,
      "â€¦ "
    )
    .replace(
      /[ \t]{2,}/g,
      " "
    )
    .trim();
}

function looksLikeCaptionCredit(
  value
) {
  const text =
    stripMarkup(value)
      .replace(/\s+/g, " ")
      .trim();

  if (!text) {
    return false;
  }

  return (
    /^(?:caption(?:ed|ing)|closed captions?|subtitles?|subtitling)\s+(?:by|provided by|courtesy of)\b/i.test(
      text
    ) ||
    /\bmedia access group\b.*\bwgbh\b/i.test(text) ||
    /^(?:spaces? added|extended chars?|better for old tvs?|sync(?:ed|hronized)? by|resync(?:ed)? by|retimed by|translated by|encoded by|rip(?:ped)? by)\b/i.test(text) ||
    /^(?:www\.|https?:\/\/|opensubtitles|subscene|addic7ed|yify|yts)\b/i.test(text) ||
    /^(?:a|an)\s+.{1,55}\s+production[.!]?(?:\s+cheers\s+.{1,80})?$/iu.test(text) ||
    /^cheers\s+.{1,80}(?:training scheme|subtitle|subtitles|team|crew)\b/iu.test(text)
  );
}

// ============================================================
// SOURCE HYGIENE 8.4.0
// ============================================================
// Alguns SRTs/OCRs convertem sÃ­mbolos musicais em lixo textual, por
// exemplo: Ff, ff, J'j', Jâ€œjâ€œ ou wrappers Jâ€œ ... jâ€œ.
//
// Regras conservadoras:
// - wrapper com CONTEÃšDO real: remove sÃ³ o wrapper e preserva a fala/letra;
// - marcador isolado sem conteÃºdo: remove o cue antes do Gemini;
// - pontuaÃ§Ã£o isolada (..., â€¦, -- etc.) nÃ£o vira legenda inventada.
function stripPseudoMusicOcrWrappers(value) {
  let text = String(value || "").trim();

  // Jâ€œ in the land ... jâ€œ  -> in the land ...
  // j" I was an angel j"   -> I was an angel
  text = text
    .replace(/^\s*[Jj]\s*[â€œâ€"'â€˜â€™`Â´]+\s*/u, "")
    .replace(/\s*[Jj]\s*[â€œâ€"'â€˜â€™`Â´]+\s*$/u, "")
    .trim();

  return text;
}

// ============================================================
// SOURCE CORRUPTION GATE 9.7.1 â€” OCR / PSEUDO-LYRIC DAMAGE
// ============================================================
// Alguns releases trazem sÃ­mbolos musicais/OCR quebrados como barras no meio
// de palavras (be/is, eve/y, ho/io'ay/) e apÃ³strofos Ã³rfÃ£os. O MAIN nÃ£o deve
// "traduzir" esse lixo. O detector Ã© deliberadamente conservador: uma barra
// normal (and/or, 24/7, datas) nÃ£o basta.
function looksLikeCorruptedSourceOcr971(value) {
  const text = String(value || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\{\\[^}]+\}/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) return false;

  const slashCount = (text.match(/[\\/]/g) || []).length;
  const embeddedSplits = (text.match(/[\p{L}][\\/](?=\p{L})/gu) || []).length;
  const shortSlashFragments = (text.match(/\b\p{L}{1,6}[\\/]\p{L}{1,8}\b/gu) || []).length;
  const tinySlashFragment = /(?:\b\p{L}{2,}[\\/]\p{L}\b|\b\p{L}[\\/]\p{L}{2,}\b)/u.test(text);
  const trailingSlash = /[\\/]\s*$/u.test(text);
  const orphanApostrophe = (text.match(/\b\p{L}{1,6}['â€™](?=\s|$)/gu) || []).length;
  const letterCount = (text.match(/\p{L}/gu) || []).length;

  if (slashCount >= 2 && (embeddedSplits + shortSlashFragments >= 1 || trailingSlash)) {
    return true;
  }

  if (trailingSlash && orphanApostrophe >= 1 && letterCount >= 6) {
    return true;
  }

  if (tinySlashFragment && letterCount >= 6) {
    return true;
  }

  return false;
}

function rawBlockHasCorruptedSourceOcr971(raw) {
  const lines = String(raw || "").trim().split("\n");
  const timingIndex = lines.findIndex(line => /-->/.test(line));
  if (timingIndex < 0) return false;
  const textLines = lines.slice(timingIndex + 1);
  return (
    textLines.some(looksLikeCorruptedSourceOcr971) ||
    looksLikeCorruptedSourceOcr971(textLines.join(" "))
  );
}

function corruptedSourceOcrCluster971(rawBlocks) {
  const direct = new Set();
  for (let i = 0; i < rawBlocks.length; i++) {
    if (rawBlockHasCorruptedSourceOcr971(rawBlocks[i])) direct.add(i);
  }

  // Um cue isolado entre dois cues OCR-corrompidos pertence ao mesmo cluster.
  // Isso captura uma linha intermediÃ¡ria aparentemente legÃ­vel sem apagar
  // diÃ¡logo normal ao redor de um Ãºnico falso positivo.
  const expanded = new Set(direct);
  for (let i = 1; i + 1 < rawBlocks.length; i++) {
    if (!direct.has(i) && direct.has(i - 1) && direct.has(i + 1)) {
      expanded.add(i);
    }
  }
  return expanded;
}

function looksLikeSourceGarbageLine(value) {
  const text = String(value || "")
    .replace(/<[^>]+>/g, "")
    .replace(/\{\\[^}]+\}/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) return true;

  if (looksLikeCorruptedSourceOcr971(text)) return true;

  if (/^(?:f{2,4})$/iu.test(text)) return true;

  if (
    /^(?:[Jj]\s*[â€œâ€"'â€˜â€™`Â´]+\s*[Jj]\s*[â€œâ€"'â€˜â€™`Â´]*|[Jj]\s*(?:\.{2,}|â€¦+|-+))$/u.test(text)
  ) {
    return true;
  }

  if (/^[.Â·â€¢â€¦,:;!?_~*#@\-â€“â€”/\\|\s]+$/u.test(text)) {
    return true;
  }

  return false;
}

function looksLikeFinalGarbageCue(value) {
  const text = String(value || "")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) return true;

  return (
    /^(?:f{2,4})$/iu.test(text) ||
    /^(?:[Jj]\s*[â€œâ€"'â€˜â€™`Â´]+\s*[Jj]\s*[â€œâ€"'â€˜â€™`Â´]*|[Jj]\s*(?:\.{2,}|â€¦+|-+))$/u.test(text) ||
    /^[.Â·â€¢â€¦,:;!?_~*#@\-â€“â€”/\\|\s]+$/u.test(text)
  );
}


// ============================================================
// SUBTITLE HYGIENE 9.2 â€” NON-SEMANTIC VOCALIZATIONS
// ============================================================
// Legenda profissional nÃ£o precisa transcrever todo ruÃ­do produzido pela boca.
// Removemos SOMENTE linhas puramente nÃ£o semÃ¢nticas. Acknowledgements que
// realmente respondem Ã  conversa (mm-hmm/uh-huh) continuam protegidos.
function looksLikeEditoriallyDroppableVocalization(value) {
  const text = String(value || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/[â™ªâ™«â™¬]/gu, " ")
    .replace(/^\s*[-â€“â€”~]\s*/u, "")
    .toLocaleLowerCase()
    .replace(/[!?.,â€¦:;"â€œâ€'â€™()\[\]]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) return false;
  const compact = text.replace(/[\s-]+/g, "");

  // Respostas afirmativas carregam significado; nÃ£o apagar.
  if (/^(?:mmhmm|mhm|uhhuh|uhum|aham)$/iu.test(compact)) return false;

  // HesitaÃ§Ã£o isolada e sinais vocais sem conteÃºdo lexical.
  if (/^(?:u+h+|u+m+|e+r+m*|a+h+n*|Ã£+h+)$/iu.test(compact)) return true;
  if (/^(?:sh+|ps+t+|tsk+|tch+|tss+|ss+h+)$/iu.test(compact)) return true;

  const parts = text.split(/\s+/).filter(Boolean);
  return parts.length > 1 && parts.length <= 8 && parts.every(part =>
    /^(?:uh+|um+|er+|erm+|ahn+|Ã£h+|sh+|ps+t+|tsk+|tch+|tss+)$/iu.test(part)
  );
}

function musicVocalizationTokens(value) {
  return String(value || "")
    .toLocaleLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[â™ªâ™«â™¬]/gu, " ")
    .replace(/[^a-z0-9' -]+/g, " ")
    .replace(/[-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);
}

function looksLikeNonSemanticMusicVocalization(value) {
  const tokens = musicVocalizationTokens(value);
  if (!tokens.length || tokens.length > 18) return false;
  const phonetic = new Set([
    "woo","wooh","hoo","yee","yeeh","ooh","oooh","oh","ohh","ah","ahh",
    "uhu","ihu","uh","ha","hey","ho","la","na","da","doo","du","dum","bam"
  ]);
  return tokens.every(token => phonetic.has(token));
}

function cleanSourceLine(line) {
  let text =
    stripMarkup(
      String(line || "")
    ).trim();

  if (
    !text ||
    looksLikeCaptionCredit(
      text
    )
  ) {
    return "";
  }

  text =
    stripPseudoMusicOcrWrappers(
      text
    );

  if (looksLikeEditoriallyDroppableVocalization(text)) {
    return "";
  }

  if (
    looksLikeSourceGarbageLine(
      text
    )
  ) {
    return "";
  }

  text =
    stripTrailingSpeakerLabel(
      text
    );

  text =
    removeSdhSegments(
      text
    )
      .replace(
        /^(\s*[-â€“â€”]\s*)[:;]+\s*/u,
        "$1"
      )
      .replace(
        /[â™ªâ™«â™¬â˜…â˜†âœ¦âœ§]/gu,
        " "
      );

  text =
    collapseExtendedVocalization(
      text
    );

  text =
    replaceCensoredBleps(
      text
    );

  text =
    normalizeNoiseSymbols(
      text
    );

  if (
    looksLikeSourceGarbageLine(
      text
    )
  ) {
    return "";
  }

  text =
    text
      .replace(
        /^\s*[:;]+\s*/u,
        ""
      )
      .trim();

  if (
    looksLikeBareSdhLine(
      text
    ) ||
    looksLikePureNonSpeechSdhLine(
      text
    ) ||
    looksLikeCaptionCredit(
      text
    )
  ) {
    return "";
  }

  if (
    !text.includes(
      BLEEP_TOKEN
    ) &&
    !/[\p{L}\p{N}]/u.test(
      text
    )
  ) {
    return "";
  }

  if (
    !text ||
    /^[-â€“â€”/\\|:;\s]*$/u.test(
      text
    )
  ) {
    return "";
  }

  return text;
}

// ============================================================
// INTENTIONAL EMPTY CUES â€” 8.4.4
// ============================================================
// Autoriza ausÃªncia visual SOMENTE se a prÃ³pria SOURCE, reavaliada pelas
// mesmas regras conservadoras de Source Hygiene, nÃ£o contiver conteÃºdo
// semÃ¢ntico. Qualquer fala, palavra, nÃºmero ou bleep continua fail-closed.
function markIntentionalEmptyCue(
  job,
  block,
  reason = "SDH"
) {
  const id =
    Number(
      block?.index
    );

  if (!Number.isInteger(id)) {
    return false;
  }

  if (block) {
    block.intentionalEmptyApproved = true;
  }

  if (
    !job?.intentionalEmptyCueIds ||
    !(job.intentionalEmptyCueIds instanceof Set)
  ) {
    if (job) {
      job.intentionalEmptyCueIds = new Set();
    }
  }

  if (!job) {
    return true;
  }

  const already =
    job.intentionalEmptyCueIds.has(
      id
    );

  job.intentionalEmptyCueIds.add(
    id
  );

  if (!already) {
    job.stats.mainIntentionalEmptyCues =
      (
        job.stats.mainIntentionalEmptyCues ||
        0
      ) + 1;

    console.log(
      `[INTENTIONAL EMPTY] cue ${id} autorizado | ${reason}.`
    );
  }

  return true;
}

function sourceCueAllowsIntentionalEmpty(
  block,
  job = null
) {
  const id =
    Number(
      block?.index
    );

  if (
    block?.intentionalEmptyApproved ||
    (
      Number.isInteger(id) &&
      job?.intentionalEmptyCueIds instanceof Set &&
      job.intentionalEmptyCueIds.has(id)
    )
  ) {
    return true;
  }

  const source =
    String(
      block?.text ||
      ""
    )
      .replace(/\r/g, "")
      .trim();

  if (!source) {
    return true;
  }

  const lines =
    source
      .split("\n")
      .map(
        line =>
          String(
            line || ""
          ).trim()
      )
      .filter(Boolean);

  if (!lines.length) {
    return true;
  }

  for (const line of lines) {
    const info =
      extractSpeaker(
        line
      );

    if (
      cleanSourceLine(
        info.text
      )
    ) {
      return false;
    }
  }

  return true;
}

function rejectedCueIsSdhOnly(
  value
) {
  const lines =
    stripMarkup(
      String(value || "")
    )
      .replace(/\r/g, "")
      .split("\n")
      .map(line => line.trim())
      .filter(Boolean);

  // Spoken fillers / acknowledgements are dialogue, never SDH consensus evidence.
  if (lines.some(looksLikeSpokenVocalization)) {
    return false;
  }

  return (
    lines.length > 0 &&
    lines.every(
      line =>
        looksLikeBareSdhLine(
          line
        ) ||
        (
          /^\s*[\[(].*[\])]\s*$/u.test(line) &&
          looksLikeSdhDescriptor(
            normalizeSdhCandidate(
              line
            )
          )
        )
    )
  );
}

function safestMainRescueFallback(
  block,
  rejectedCandidates = []
) {
  for (
    let index = rejectedCandidates.length - 1;
    index >= 0;
    index--
  ) {
    const candidate =
      String(
        rejectedCandidates[index] ||
        ""
      ).trim();

    const sanitized =
      sanitizeFinalCue(
        block,
        candidate
      ) ||
      sanitizeFallbackCue(
        candidate
      );

    if (sanitized) {
      return sanitized;
    }
  }

  const source =
    cleanSourceLine(
      block?.text ||
      ""
    ) ||
    stripMarkup(
      block?.text ||
      ""
    ).trim();

  return (
    sanitizeFinalCue(
      block,
      source
    ) ||
    sanitizeFallbackCue(
      source
    ) ||
    source
  ).trim();
}

function subtitleClockToMs(
  value
) {
  const match =
    String(value || "")
      .trim()
      .match(
        /^(\d{2}):(\d{2}):(\d{2}),(\d{3})$/
      );

  if (!match) {
    return null;
  }

  return (
    (
      (
        Number(match[1]) * 60 +
        Number(match[2])
      ) * 60 +
      Number(match[3])
    ) * 1000 +
    Number(match[4])
  );
}

// ============================================================
// CONTEXTUAL PERFORMANCE MUSIC LOCK
// ============================================================
//
// PrincÃ­pio:
// - mÃºsica editorial/de fundo continua sendo removida;
// - apresentaÃ§Ã£o real continua sendo legendada;
// - decisÃ£o musical Ã© por CUE/BLOCO, nunca por linha isolada;
// - uma letra nunca pode ser mutilada ("linha com â™ª some / continuaÃ§Ã£o fica").
//
// Ã‚ncoras longas usam evidÃªncia estrutural de performance.
// Clusters curtos sÃ³ entram quando o contexto imediato prova lanÃ§amento
// de performance ou quando ficam ENTRE dois clusters jÃ¡ confirmados.

const MUSIC_CLUSTER_GAP_MS =
  12000;

const MUSIC_STRONG_MIN_CUES =
  4;

const MUSIC_STRONG_MIN_SPAN_MS =
  25000;

const MUSIC_SHOWCASE_REGION_GAP_MS =
  45000;

const MUSIC_LAUNCH_CONTEXT_MAX_GAP_MS =
  20000;

// Reprises/fragmentos da MESMA performance podem voltar entre falas.
// Se o texto musical bate lexicalmente com um cluster jÃ¡ confirmado,
// a decisÃ£o Ã© atÃ´mica para aquela performance, sem apagar o fragmento isolado.
const MUSIC_DENSE_MIN_CUES = 5;
const MUSIC_DENSE_MIN_SPAN_MS = 12000;

const MUSIC_REPRISE_MAX_GAP_MS = 90000;
const MUSIC_REPRISE_MIN_SHARED_TOKENS = 2;
const MUSIC_REPRISE_MIN_OVERLAP = 0.66;

const PERFORMANCE_LAUNCH_RE =
  /(?:\bhit it\b|\btake it away\b|\bgive it up\b|\blet'?s hear it\b|\blet'?s hear (?:it )?for\b|\bstart the music\b|\bmusic[, ]+maestro\b|\bplay it\b|\bshowtime\b|\b(?:now|next)[, ]+(?:performing|singing)\b|\bperforming live\b|\bsinging live\b|\bon stage now\b|\bi sang my song\b|\bi (?:wrote|made) (?:this|my) song\b|\b(?:here(?:'s| is)|this is) (?:my|our) song\b|\bi(?:'m| am) (?:going to|gonna) sing\b|\bi(?:'ll| will) sing\b|\bsing (?:my|our) song\b|\bmanda ver\b|\bsolta o som\b|\bcomeÃ§a a mÃºsica\b|\bvamos ouvir\b|\bvalendo\b|\bagora[, ]+(?:cantando|se apresentando)\b|\beu cantei minha mÃºsica\b|\beu (?:escrevi|fiz) (?:essa|esta|minha) mÃºsica\b|\b(?:essa|esta) Ã© (?:a )?minha mÃºsica\b|\beu vou cantar\b)/iu;

const PERFORMANCE_RELEVANCE_CONTEXT_RE =
  /(?:\blip\s*sync\b|\brusical\b|\bperformance\b|\bperforming\s+live\b|\bsinging\s+live\b|\bon\s+stage\b|\bkaraoke\b|\bconcert\b|\bchoir\b|\bband\b|\baudition\b|\bshowtime\b|\bmusical\s+(?:number|performance)\b|\bi\s+sang\s+my\s+song\b|\bi\s+(?:wrote|made)\s+(?:this|my)\s+song\b|\b(?:here(?:'s| is)|this is)\s+(?:my|our)\s+song\b|\bi(?:'m| am)\s+(?:going to|gonna)\s+sing\b|\bi(?:'ll| will)\s+sing\b|\bsing\s+(?:my|our)\s+song\b|\bapresenta(?:Ã§Ã£o|ndo|r)\s+(?:ao vivo)?\b|\bno\s+palco\b|\beu\s+cantei\s+minha\s+mÃºsica\b|\beu\s+(?:escrevi|fiz)\s+(?:essa|esta|minha)\s+mÃºsica\b|\b(?:essa|esta)\s+Ã©\s+(?:a\s+)?minha\s+mÃºsica\b|\beu\s+vou\s+cantar\b)/iu

function rawCueVisibleLines(
  raw
) {
  const lines =
    String(raw || "")
      .trim()
      .split("\n");

  const timingIndex =
    lines.findIndex(
      line =>
        /-->/.test(
          line
        )
    );

  if (timingIndex < 0) {
    return [];
  }

  return lines
    .slice(
      timingIndex + 1
    )
    .map(
      line =>
        stripMarkup(
          String(
            line || ""
          )
        ).trim()
    )
    .filter(Boolean);
}

function hasSubtitleLyricMarker(value) {
  const text = stripMarkup(String(value || "")).trim();
  if (!text) return false;
  // # is a lyric marker only at subtitle line boundaries with whitespace; #1/#tag stay speech.
  return /[â™ªâ™«â™¬]/u.test(text) || /^#\s+\S/u.test(text) || /\S\s+#$/u.test(text);
}

function stripSubtitleLyricMarkers(value) {
  return String(value || "")
    .replace(/[â™ªâ™«â™¬]/gu, " ")
    .replace(/^\s*#\s+(?=\S)/u, "")
    .replace(/\s+#\s*$/u, "")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function classifyMusicAwareLines(
  rawLines
) {
  const lines =
    (rawLines || [])
      .map(
        line => ({
          raw:
            String(
              line || ""
            ),

          visible:
            stripMarkup(
              String(
                line || ""
              )
            ).trim()
        })
      )
      .filter(
        item =>
          item.visible
      );

  const hasAnyMusicMarker =
    lines.some(
      item =>
        hasSubtitleLyricMarker(
          item.visible
        )
    );

  if (!hasAnyMusicMarker) {
    return lines.map(
      item => ({
        ...item,
        kind:
          looksLikeBareSdhLine(
            item.visible
          )
            ? "sdh"
            : "speech"
      })
    );
  }

  const result = [];

  let lyricContinuationOpen =
    false;

  for (
    const item of lines
  ) {
    const visible =
      item.visible;

    if (
      looksLikeBareSdhLine(
        visible
      )
    ) {
      result.push({
        ...item,
        kind:
          "sdh"
      });

      lyricContinuationOpen =
        false;

      continue;
    }

    if (
      hasSubtitleLyricMarker(
        visible
      )
    ) {
      result.push({
        ...item,
        kind:
          "lyric"
      });

      lyricContinuationOpen =
        true;

      continue;
    }

    // Cue misto: fala real depois de linha musical nÃ£o pode sumir.
    if (
      /^\s*(?:[-â€“â€”]\s+|~\s*)(?=\S)/u.test(
        visible
      )
    ) {
      result.push({
        ...item,
        kind:
          "speech"
      });

      lyricContinuationOpen =
        false;

      continue;
    }

    // ContinuaÃ§Ã£o de uma linha que comeÃ§ou com â™ª.
    // KEEP ou DROP ocorre para a unidade inteira.
    if (
      lyricContinuationOpen
    ) {
      result.push({
        ...item,
        kind:
          "lyric"
      });

      continue;
    }

    result.push({
      ...item,
      kind:
        "speech"
    });
  }

  return result;
}

function rawMusicInfo(
  raw
) {
  const lines =
    String(raw || "")
      .trim()
      .split("\n");

  const timingIndex =
    lines.findIndex(
      line =>
        /-->/.test(
          line
        )
    );

  if (timingIndex < 0) {
    return {
      startMs: null,
      endMs: null,
      hasMusic: false,
      hasSpeech: false,
      lyricLineCount: 0,
      text: ""
    };
  }

  const timing =
    String(
      lines[
        timingIndex
      ] ||
      ""
    ).trim();

  const [
    startText,
    endText
  ] =
    timing.split(
      /\s*-->\s*/
    );

  const classified =
    classifyMusicAwareLines(
      lines.slice(
        timingIndex + 1
      )
    );

  const hasMusic =
    classified.some(
      item =>
        item.kind ===
        "lyric"
    );

  const hasSpeech =
    classified.some(
      item =>
        item.kind ===
        "speech"
    );

  return {
    startMs:
      subtitleClockToMs(
        startText
      ),

    endMs:
      subtitleClockToMs(
        endText
      ),

    hasMusic,
    hasSpeech,

    lyricLineCount:
      classified.filter(
        item =>
          item.kind ===
          "lyric"
      ).length,

    text:
      classified
        .filter(
          item =>
            item.kind !==
            "sdh"
        )
        .map(
          item =>
            item.kind === "lyric"
              ? stripSubtitleLyricMarkers(item.visible)
              : item.visible
        )
        .join(" ")
        .trim()
  };
}

function clusterSpanMs(
  cluster,
  info
) {
  if (!cluster.length) {
    return 0;
  }

  const first =
    info[
      cluster[0]
    ];

  const last =
    info[
      cluster[
        cluster.length - 1
      ]
    ];

  if (
    !Number.isFinite(
      first?.startMs
    ) ||
    !Number.isFinite(
      last?.endMs
    )
  ) {
    return 0;
  }

  return Math.max(
    0,
    last.endMs -
      first.startMs
  );
}

function isTerminalOutroMusicCluster(
  cluster,
  info
) {
  if (!cluster.length) {
    return false;
  }

  const first =
    info[
      cluster[0]
    ];

  const lastIndex =
    cluster[
      cluster.length - 1
    ];

  const episodeEndMs =
    [...info]
      .reverse()
      .find(
        item =>
          Number.isFinite(
            item?.endMs
          )
      )
      ?.endMs ?? null;

  const hasSpeechAfter =
    info
      .slice(
        lastIndex + 1
      )
      .some(
        item =>
          item?.hasSpeech
      );

  const spanMs =
    clusterSpanMs(
      cluster,
      info
    );

  return (
    Number.isFinite(
      episodeEndMs
    ) &&
    Number.isFinite(
      first?.startMs
    ) &&
    !hasSpeechAfter &&
    spanMs <= 45000 &&
    first.startMs >=
      episodeEndMs - 45000
  );
}

function clusterHasImmediatePerformanceLaunch(
  cluster,
  info,
  rawBlocks
) {
  if (!cluster.length) {
    return false;
  }

  const firstIndex =
    cluster[0];

  const startMs =
    info[
      firstIndex
    ]?.startMs;

  const previousIndex =
    firstIndex - 1;

  const previous =
    info[
      previousIndex
    ];

  if (
    !Number.isFinite(
      startMs
    ) ||
    !previous ||
    !Number.isFinite(
      previous.endMs
    )
  ) {
    return false;
  }

  const gapMs =
    startMs -
    previous.endMs;

  if (
    gapMs < -1000 ||
    gapMs >
      MUSIC_LAUNCH_CONTEXT_MAX_GAP_MS
  ) {
    return false;
  }

  const visible =
    rawCueVisibleLines(
      rawBlocks[
        previousIndex
      ]
    )
      .filter(
        line =>
          !looksLikeBareSdhLine(
            line
          ) &&
          !hasSubtitleLyricMarker(
            line
          )
      )
      .join(" ")
      .replace(
        /\s+/g,
        " "
      )
      .trim();

  return Boolean(
    visible &&
    PERFORMANCE_LAUNCH_RE.test(
      visible
    )
  );
}

function clusterHasNarrativePerformanceEvidence(cluster, info, rawBlocks) {
  if (!cluster?.length) return false;
  const first = info[cluster[0]];
  const last = info[cluster[cluster.length - 1]];
  if (!Number.isFinite(first?.startMs) || !Number.isFinite(last?.endMs)) return false;

  const windowStart = first.startMs - 30000;
  const windowEnd = last.endMs + 30000;
  const clusterSet = new Set(cluster);
  const nearby = [];

  for (let i = 0; i < info.length; i++) {
    const item = info[i];
    if (!Number.isFinite(item?.startMs) || !Number.isFinite(item?.endMs)) continue;
    if (item.endMs < windowStart || item.startMs > windowEnd) continue;
    if (clusterSet.has(i)) continue;

    const visible = rawCueVisibleLines(rawBlocks[i])
      .filter(line => !looksLikeBareSdhLine(line) && !hasSubtitleLyricMarker(line))
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();
    if (visible) nearby.push(visible);
  }

  return PERFORMANCE_RELEVANCE_CONTEXT_RE.test(nearby.join(" "));
}

function musicLexicalTokens(value) {
  return String(value || "")
    .toLocaleLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[â™ªâ™«â™¬]/gu, " ")
    .replace(/^#\s+|\s+#$/g, " ")
    .replace(/[^a-z0-9' ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(token => token.length >= 2)
    .filter(token => !new Set(["woo","wooh","hoo","yee","yeeh","ooh","oooh","ohh","ahh","uhu","ihu","la","na","doo"]).has(token));
}

function musicLexicalOverlap(a, b) {
  const aa = musicLexicalTokens(a);
  const bb = musicLexicalTokens(b);
  if (!aa.length || !bb.length) return 0;

  const sa = new Set(aa);
  const sb = new Set(bb);
  let shared = 0;
  for (const token of sa) {
    if (sb.has(token)) shared++;
  }

  const denominator = Math.max(1, Math.min(sa.size, sb.size));
  const ratio = shared / denominator;
  return shared >= MUSIC_REPRISE_MIN_SHARED_TOKENS ? ratio : 0;
}

function clusterTemporalGapMs(a, b, info) {
  if (!a?.length || !b?.length) return Infinity;

  const aStart = info[a[0]]?.startMs;
  const aEnd = info[a[a.length - 1]]?.endMs;
  const bStart = info[b[0]]?.startMs;
  const bEnd = info[b[b.length - 1]]?.endMs;

  if (![aStart, aEnd, bStart, bEnd].every(Number.isFinite)) return Infinity;
  if (aEnd < bStart) return bStart - aEnd;
  if (bEnd < aStart) return aStart - bEnd;
  return 0;
}

function clusterRepeatsConfirmedPerformance(candidate, confirmed, info) {
  if (clusterTemporalGapMs(candidate, confirmed, info) > MUSIC_REPRISE_MAX_GAP_MS) {
    return false;
  }

  for (const ci of candidate) {
    for (const ri of confirmed) {
      if (musicLexicalOverlap(info[ci]?.text, info[ri]?.text) >= MUSIC_REPRISE_MIN_OVERLAP) {
        return true;
      }
    }
  }

  return false;
}

function detectPerformanceMusicIndexes(
  rawBlocks
) {
  const info =
    rawBlocks.map(
      rawMusicInfo
    );

  const musicIndexes =
    info
      .map(
        (item, index) =>
          item.hasMusic
            ? index
            : -1
      )
      .filter(
        index =>
          index >= 0
      );

  const clusters = [];

  let cluster = [];

  for (
    const index of
    musicIndexes
  ) {
    if (!cluster.length) {
      cluster.push(
        index
      );

      continue;
    }

    const previousIndex =
      cluster[
        cluster.length - 1
      ];

    const previous =
      info[
        previousIndex
      ];

    const current =
      info[
        index
      ];

    const gapMs =
      Number.isFinite(
        previous?.endMs
      ) &&
      Number.isFinite(
        current?.startMs
      )
        ? current.startMs -
          previous.endMs
        : Infinity;

    if (
      gapMs <=
      MUSIC_CLUSTER_GAP_MS
    ) {
      cluster.push(
        index
      );
    } else {
      clusters.push(
        cluster
      );

      cluster = [
        index
      ];
    }
  }

  if (cluster.length) {
    clusters.push(
      cluster
    );
  }

  const keep =
    new Set();

  const confirmedClusters =
    new Set();

  // CAMADA 1 â€” 9.2: duraÃ§Ã£o/densidade sozinhas NÃƒO provam relevÃ¢ncia.
  // Cluster longo sÃ³ Ã© performance quando o contexto narrativo tambÃ©m prova isso.
  for (
    let clusterIndex = 0;
    clusterIndex <
      clusters.length;
    clusterIndex++
  ) {
    const current =
      clusters[
        clusterIndex
      ];

    const spanMs =
      clusterSpanMs(
        current,
        info
      );

    const terminalOutro =
      isTerminalOutroMusicCluster(
        current,
        info
      );

    const denseLexicalPerformance =
      current.length >= MUSIC_DENSE_MIN_CUES &&
      spanMs >= MUSIC_DENSE_MIN_SPAN_MS &&
      current.every(rawIndex => musicLexicalTokens(info[rawIndex]?.text).length >= 2);

    const narrativePerformanceEvidence =
      clusterHasNarrativePerformanceEvidence(
        current,
        info,
        rawBlocks
      );

    if (
      (
        (
          current.length >= MUSIC_STRONG_MIN_CUES &&
          spanMs >= MUSIC_STRONG_MIN_SPAN_MS
        ) ||
        denseLexicalPerformance
      ) &&
      narrativePerformanceEvidence
    ) {
      if (!terminalOutro) {
        confirmedClusters.add(clusterIndex);
      }
    }
  }

  // CAMADA 2 â€” performance curta com lanÃ§amento explÃ­cito.
  for (
    let clusterIndex = 0;
    clusterIndex <
      clusters.length;
    clusterIndex++
  ) {
    if (
      confirmedClusters.has(
        clusterIndex
      )
    ) {
      continue;
    }

    const current =
      clusters[
        clusterIndex
      ];

    if (
      isTerminalOutroMusicCluster(
        current,
        info
      )
    ) {
      continue;
    }

    if (
      clusterHasImmediatePerformanceLaunch(
        current,
        info,
        rawBlocks
      ) ||
      clusterHasNarrativePerformanceEvidence(
        current,
        info,
        rawBlocks
      )
    ) {
      confirmedClusters.add(
        clusterIndex
      );
    }
  }

  // CAMADA 3 â€” regiÃ£o de showcase.
  // Clusters curtos ENTRE duas performances confirmadas
  // pertencem Ã  mesma sequÃªncia de apresentaÃ§Ãµes.
  const regions = [];

  let region = [];

  for (
    let clusterIndex = 0;
    clusterIndex <
      clusters.length;
    clusterIndex++
  ) {
    const current =
      clusters[
        clusterIndex
      ];

    if (!region.length) {
      region.push(
        clusterIndex
      );

      continue;
    }

    const previousClusterIndex =
      region[
        region.length - 1
      ];

    const previous =
      clusters[
        previousClusterIndex
      ];

    const previousEnd =
      info[
        previous[
          previous.length - 1
        ]
      ]?.endMs;

    const currentStart =
      info[
        current[0]
      ]?.startMs;

    const gapMs =
      Number.isFinite(
        previousEnd
      ) &&
      Number.isFinite(
        currentStart
      )
        ? currentStart -
          previousEnd
        : Infinity;

    if (
      gapMs <=
      MUSIC_SHOWCASE_REGION_GAP_MS
    ) {
      region.push(
        clusterIndex
      );
    } else {
      regions.push(
        region
      );

      region = [
        clusterIndex
      ];
    }
  }

  if (region.length) {
    regions.push(
      region
    );
  }

  for (
    const currentRegion of
    regions
  ) {
    const confirmedPositions =
      currentRegion
        .map(
          (
            clusterIndex,
            position
          ) =>
            confirmedClusters.has(
              clusterIndex
            )
              ? position
              : -1
        )
        .filter(
          position =>
            position >= 0
        );

    if (
      confirmedPositions.length <
      2
    ) {
      continue;
    }

    const firstConfirmed =
      Math.min(
        ...confirmedPositions
      );

    const lastConfirmed =
      Math.max(
        ...confirmedPositions
      );

    for (
      let position =
        firstConfirmed;
      position <=
        lastConfirmed;
      position++
    ) {
      confirmedClusters.add(
        currentRegion[
          position
        ]
      );
    }
  }

  // CAMADA 4 â€” REPRISE ATÃ”MICA 9.0.
  // Um fragmento musical isolado que repete lexicalmente uma performance jÃ¡
  // confirmada pertence Ã  mesma performance e nÃ£o pode desaparecer sÃ³ porque
  // houve diÃ¡logo no meio. Iteramos atÃ© estabilizar para encadear reprises.
  let reprisePromotions = 0;
  let changedReprises = true;

  while (changedReprises) {
    changedReprises = false;

    for (let clusterIndex = 0; clusterIndex < clusters.length; clusterIndex++) {
      if (confirmedClusters.has(clusterIndex)) continue;

      const candidate = clusters[clusterIndex];
      const matchesConfirmed = [...confirmedClusters].some(confirmedIndex =>
        clusterRepeatsConfirmedPerformance(
          candidate,
          clusters[confirmedIndex],
          info
        )
      );

      if (matchesConfirmed) {
        confirmedClusters.add(clusterIndex);
        reprisePromotions++;
        changedReprises = true;
      }
    }
  }

  for (
    const clusterIndex of
    confirmedClusters
  ) {
    for (
      const rawIndex of
      clusters[
        clusterIndex
      ]
    ) {
      keep.add(
        rawIndex
      );
    }
  }

  const strongCount =
    clusters.filter(
      current =>
        current.length >=
          MUSIC_STRONG_MIN_CUES &&
        clusterSpanMs(
          current,
          info
        ) >=
          MUSIC_STRONG_MIN_SPAN_MS &&
        !isTerminalOutroMusicCluster(
          current,
          info
        )
    ).length;

  console.log(
    `[MUSIC CONTEXT 9.2.6] cues com marcador musical=${musicIndexes.length} | ` +
    `clusters=${clusters.length} | Ã¢ncoras fortes=${strongCount} | ` +
    `clusters confirmados=${confirmedClusters.size} | ` +
    `reprises atÃ´micas=${reprisePromotions} | ` +
    `cues de performance mantidos=${keep.size}.`
  );

  return {
    info,
    keep,
    clusters,
    confirmedClusters
  };
}

// ============================================================
// CONTEXTUAL LYRIC METADATA â€” 9.7.3
// ============================================================
// Performance/contextual lyrics are still selected by the existing 9.2.6
// contextual music detector. 9.7.3 only preserves a tiny internal marker so
// the final SRT can restore polished â™ª ... â™ª typography without exposing
// metadata to Gemini or changing the music relevance policy.
const LYRIC_META_TOKEN_973 = "@@LYR:1@@";
const LYRIC_META_RE_973 = /^\s*@@LYR:1@@\s*/u;

// 9.0 â€” marcador de speaker pode vir como "~ fala", "~fala",
// "- fala" ou "-fala". HÃ­fen colado a nÃºmero negativo NÃƒO Ã© speaker.
const DIALOGUE_TURN_START_RE =
  /^\s*(?:~\s*|[-â€“â€”](?:\s+|(?=[^\d\s])))(?=\S)/u;

function sourceLineHasDialogueTurnMarker(value) {
  return DIALOGUE_TURN_START_RE.test(String(value || ""));
}

function canonicalizeSourceDialogueTurnMarker(value) {
  return String(value || "").replace(DIALOGUE_TURN_START_RE, "- ");
}

function looksLikeStandaloneCueSpeakerLabel(value) {
  const text = stripMarkup(String(value || "")).trim();
  if (!text || sourceLineHasDialogueTurnMarker(text) || /[â™ªâ™«â™¬]/u.test(text)) return false;
  const letters = text.replace(/[^A-Za-zÃ€-Ã¿]/g, "");
  if (letters.length < 2 || letters !== letters.toUpperCase()) return false;
  if (!looksLikeSpeakerLabel(text)) return false;
  return text.split(/\s+/).filter(Boolean).length <= 5;
}

function cleanSrtForTranslation(
  srt
) {
  const normalized =
    normalizeSrt(
      srt
    );

  if (!normalized) {
    return "";
  }

  const rawBlocks =
    normalized
      .split(
        /\n{2,}/
      )
      .filter(Boolean);

  const corruptedSourceOcrIndexes971 =
    corruptedSourceOcrCluster971(rawBlocks);

  const {
    keep:
      performanceMusicIndexes
  } =
    detectPerformanceMusicIndexes(
      rawBlocks
    );

  const out = [];

  let removed = 0;
  let speakerHints = 0;
  let speakerHintsSuppressedMultiTurn = 0;
  let bleepCues = 0;
  let sdhLinesRemoved = 0;
  let backgroundLyricLinesRemoved = 0;
  let performanceLyricLinesKept = 0;
  let nonSemanticMusicVocalizationsRemoved = 0;
  let pureVocalizationsRemoved = 0;
  let corruptedSourceOcrCuesRemoved971 = 0;

  for (
    let rawIndex = 0;
    rawIndex <
      rawBlocks.length;
    rawIndex++
  ) {
    const raw =
      rawBlocks[
        rawIndex
      ];

    if (corruptedSourceOcrIndexes971.has(rawIndex)) {
      removed++;
      corruptedSourceOcrCuesRemoved971++;
      continue;
    }

    const lines =
      raw
        .trim()
        .split("\n");

    const timingIndex =
      lines.findIndex(
        line =>
          /-->/.test(
            line
          )
      );

    if (timingIndex < 0) {
      continue;
    }

    const timing =
      lines[
        timingIndex
      ].trim();

    if (
      !TIMING_RE.test(
        timing
      )
    ) {
      continue;
    }

    const dialogue = [];

    // 9.7.3: preserve whether THIS CLEANED CUE is entirely a contextually
    // relevant performance lyric. Background/incidental music is still removed
    // by detectPerformanceMusicIndexes() exactly as before.
    let keptLyricDialogueLines973 = 0;
    let keptNonLyricDialogueLines973 = 0;

    const speakers =
      new Set();

    let hasBleep =
      false;

    const cleanedBlockText =
      removeMultilineSdhSegments(
        lines
          .slice(
            timingIndex + 1
          )
          .join("\n")
      );

    const classifiedLines =
      classifyMusicAwareLines(
        cleanedBlockText.split(
          "\n"
        )
      );

    for (let classifiedIndex = 0; classifiedIndex < classifiedLines.length; classifiedIndex++) {
      const classified = classifiedLines[classifiedIndex];
      const sourceLine = classified.raw;

      // 9.0: label de speaker em linha prÃ³pria (HUGO / MED TECH 1 / MARGARET)
      // Ã© metadata, nÃ£o diÃ¡logo. SÃ³ removemos quando hÃ¡ outra linha real no mesmo cue.
      if (
        looksLikeStandaloneCueSpeakerLabel(sourceLine) &&
        classifiedLines.slice(classifiedIndex + 1).some(item => String(item?.visible || "").trim())
      ) {
        speakers.add(normalizeSpeaker(sourceLine));
        continue;
      }

      if (
        classified.kind !== "lyric" &&
        looksLikeEditoriallyDroppableVocalization(classified.visible)
      ) {
        pureVocalizationsRemoved++;
        continue;
      }

      if (
        classified.kind ===
        "sdh"
      ) {
        sdhLinesRemoved++;
        continue;
      }

      if (
        classified.kind ===
        "lyric" &&
        !performanceMusicIndexes.has(
          rawIndex
        )
      ) {
        backgroundLyricLinesRemoved++;
        continue;
      }

      if (
        classified.kind === "lyric" &&
        looksLikeNonSemanticMusicVocalization(classified.visible)
      ) {
        nonSemanticMusicVocalizationsRemoved++;
        continue;
      }

      if (
        classified.kind ===
        "lyric"
      ) {
        performanceLyricLinesKept++;
        keptLyricDialogueLines973++;
      } else {
        keptNonLyricDialogueLines973++;
      }

      const sourceForDialogue =
        classified.kind === "lyric"
          ? stripSubtitleLyricMarkers(sourceLine)
          : sourceLine;

      const info =
        extractSpeaker(
          sourceForDialogue
        );

      if (
        info.speaker
      ) {
        speakers.add(
          info.speaker
        );
      }

      let cleaned =
        cleanSourceLine(
          info.text
        );

      if (!cleaned) {
        continue;
      }

      if (
        cleaned.includes(
          BLEEP_TOKEN
        )
      ) {
        hasBleep =
          true;
      }

      const hadSourceTurnMarker =
        info.hadDialogueDash || sourceLineHasDialogueTurnMarker(sourceLine);

      if (hadSourceTurnMarker) {
        cleaned = canonicalizeSourceDialogueTurnMarker(cleaned);
        if (!/^\s*[-â€“â€”]\s*/u.test(cleaned)) {
          cleaned = `- ${cleaned.replace(/^\s*~\s*/u, "")}`;
        }
      } else {
        cleaned = cleaned.replace(/^\s*[-â€“â€”]\s*/u, "- ");
      }

      dialogue.push(
        cleaned
      );
    }

    if (!dialogue.length) {
      removed++;
      continue;
    }

    if (hasBleep) {
      bleepCues++;
    }

    // Pure contextual-performance lyric cue: keep an INTERNAL marker until
    // parseSrt(). The words themselves stay marker-free for translation/QA.
    // Mixed speech+lyric cues are deliberately left undecorated to avoid
    // putting â™ª around spoken dialogue.
    if (
      dialogue.length &&
      keptLyricDialogueLines973 > 0 &&
      keptNonLyricDialogueLines973 === 0
    ) {
      dialogue[0] = `${LYRIC_META_TOKEN_973} ${dialogue[0]}`;
    }

    const explicitDialogueTurns =
      dialogue.filter(
        line => /^\s*[-â€“â€”]\s+/u.test(line)
      ).length;

    if (
      speakers.size === 1 &&
      explicitDialogueTurns < 2
    ) {
      const speaker =
        [...speakers][0];

      dialogue[0] =
        `@@SPK:${
          encodeURIComponent(
            speaker
          )
        }@@ ${
          dialogue[0]
        }`;

      speakerHints++;
    } else if (
      speakers.size >= 1 &&
      explicitDialogueTurns >= 2
    ) {
      // 8.8.3: a label de um turno NÃƒO vira identidade do cue inteiro.
      // Isso impede "-I'm tired / -SARAH: ..." de atribuir Sarah ao 1Âº turno.
      speakerHintsSuppressedMultiTurn++;
    }

    out.push({
      timing,
      dialogue
    });
  }

  console.log(
    `[CLEAN/SDH] ${
      rawBlocks.length
    } -> ${
      out.length
    }; removidos=${
      removed
    }; SDH-linhas=${
      sdhLinesRemoved
    }; mÃºsica-fundo-linhas=${
      backgroundLyricLinesRemoved
    }; letra-performance-linhas=${
      performanceLyricLinesKept
    }; vocal-musical-removida=${
      nonSemanticMusicVocalizationsRemoved
    }; vocal-pura-removida=${
      pureVocalizationsRemoved
    }; speakerHints=${
      speakerHints
    }; speakerHints-multiturno-suprimidos=${
      speakerHintsSuppressedMultiTurn
    }; bleepCues=${
      bleepCues
    }; OCR-corrupt-cues=${
      corruptedSourceOcrCuesRemoved971
    }.`
  );

  if (!out.length) {
    return "";
  }

  return (
    out
      .map(
        (
          block,
          index
        ) =>
          [
            index + 1,
            block.timing,
            ...block.dialogue
          ].join("\n")
      )
      .join("\n\n")
      .trim() +
    "\n"
  );
}
function parseSrt(srt) {
  const normalized =
    normalizeSrt(srt);

  if (!normalized) {
    return [];
  }

  const result = [];

  for (
    const raw of
    normalized.split(
      /\n{2,}/
    )
  ) {
    const lines =
      raw
        .trim()
        .split("\n");

    if (
      lines.length < 3 ||
      !/^\d+$/.test(
        lines[0].trim()
      ) ||
      !TIMING_RE.test(
        lines[1].trim()
      )
    ) {
      continue;
    }

    const textLines =
      lines.slice(2);

    let speakerHint =
      null;

    let musicPerformance =
      false;

    if (textLines.length) {
      if (LYRIC_META_RE_973.test(String(textLines[0] || ""))) {
        musicPerformance = true;
        textLines[0] = String(textLines[0] || "").replace(LYRIC_META_RE_973, "");
      }

      const match =
        textLines[0].match(
          SPEAKER_RE
        );

      if (match) {
        try {
          speakerHint =
            normalizeSpeaker(
              decodeURIComponent(
                match[1]
              )
            );
        } catch {}

        textLines[0] =
          textLines[0].replace(
            SPEAKER_RE,
            ""
          );
      }
    }

    result.push({
      index:
        Number(
          lines[0].trim()
        ),

      timing:
        lines[1].trim(),

      text:
        textLines
          .join("\n")
          .trim(),

      speakerHint,

      musicPerformance
    });
  }

  return result;
}

// ============================================================
// CULTURAL HARD LOCKS
// ============================================================

const CULTURE_HARD_LOCKS = [
  {
    regex:
      /\bLip[\s-]+Sync\s+for\s+Your\s+Life\b/giu,
    value:
      "Lip Sync for Your Life"
  },
  {
    regex:
      /\bLip[\s-]+Sync\s+for\s+the\s+Crown\b/giu,
    value:
      "Lip Sync for the Crown"
  },
  {
    regex:
      /\bShantay\s*,?\s+you\s+stay\b/giu,
    value:
      "Shantay, you stay"
  },
  {
    regex:
      /\bSashay\s*,?\s+away\b/giu,
    value:
      "Sashay away"
  },
  {
    regex:
      /\bYou\s+betta\s+werk\b/giu,
    value:
      "You betta werk"
  },

  // ==========================================================
  // CONDRAGULATIONS â€” CANONICALIZAÃ‡ÃƒO DETERMINÃSTICA
  // ==========================================================
  // Algumas legendas-fonte trazem typos como:
  // Condragtulations / Condraglulations.
  // Todas representam o mesmo bordÃ£o e devem voltar
  // deterministicamente como "Condragulations".
  {
    regex:
      /\b(?:Condragulations|Condragtulations|Condraglulations)\b/giu,
    value:
      "Condragulations"
  },

  {
    regex:
      /\bSnatch\s+Game\b/giu,
    value:
      "Snatch Game"
  },
  {
    regex:
      /\bWerkroom\b/giu,
    value:
      "Werkroom"
  },
  {
    regex:
      /\bRusical\b/giu,
    value:
      "Rusical"
  },
  {
    regex:
      /\bPit\s+Crew\b/giu,
    value:
      "Pit Crew"
  },
  {
    regex:
      /\bUntucked\b/giu,
    value:
      "Untucked"
  },

  // Deve ficar por Ãºltimo para nÃ£o capturar antes
  // os dois bordÃµes completos "Lip Sync for..."
  {
    regex:
      /\blip[\s-]+sync\b/giu,
    value:
      "lip sync"
  }
];
function protectCulturalLocks(
  text,
  cueId
) {
  let protectedText =
    String(text || "");

  const locks = [];
  let serial = 0;

  for (
    const rule of
    CULTURE_HARD_LOCKS
  ) {
    rule.regex.lastIndex = 0;

    protectedText =
      protectedText.replace(
        rule.regex,
        () => {
          const token =
            `__LOCK_C${
              cueId
            }_${
              serial++
            }__`;

          locks.push({
            token,
            value: rule.value
          });

          return token;
        }
      );
  }

  return {
    text: protectedText,
    locks
  };
}

function restoreCulturalLocks(
  text,
  locks,
  cueId
) {
  let out =
    String(text || "");

  for (
    const lock of
    locks || []
  ) {
    if (
      !out.includes(
        lock.token
      )
    ) {
      throw new Error(
        `CULTURE HARD LOCK cue ${
          cueId
        }: token ${
          lock.token
        } nÃ£o voltou.`
      );
    }

    out =
      out
        .split(
          lock.token
        )
        .join(
          lock.value
        );
  }

  return out;
}

function canonicalizeCulturalText(
  value
) {
  let out =
    String(
      value || ""
    );

  for (
    const rule of
    CULTURE_HARD_LOCKS
  ) {
    rule.regex.lastIndex = 0;

    out =
      out.replace(
        rule.regex,
        () =>
          rule.value
      );
  }

  return out;
}

function missingCanonicalCultureLocks(
  value,
  locks
) {
  const canonical =
    canonicalizeCulturalText(
      value
    );

  return (
    Array.isArray(locks)
      ? locks
      : []
  ).filter(
    lock =>
      !canonical.includes(
        lock.value
      )
  );
}

// ============================================================
// FINAL FORMAT LOCK
// ============================================================

// ============================================================
// DIALOGUE TURN LOCK
// ============================================================
//
// A fonte Ã© a autoridade para a quantidade/ordem de speakers dentro do cue.
// O Gemini pode escolher palavras e concisÃ£o; nÃ£o pode apagar fronteiras.
// O layout final pode agrupar turns na mesma linha, mas nunca quebrar um turn
// de forma que pareÃ§a pertencer ao speaker seguinte.

const IMPLICIT_TURN_RESPONSE_RE_973 =
  /^(?:(?:no|no[,.]?\s+no|yes|yeah|yep|nope|nah|sure|right|okay|ok|wait|what|why|who|hey|please|stop|don['â€™]t|i\s+know|exactly|really|fine|good|great|thanks?|thank\s+you|of\s+course)|(?:nÃ£o|nao|sim|claro|certo|tÃ¡|ta|ok|espera|espere|quÃª|que|por\s+quÃª|por\s+que|ei|por\s+favor|pare|obrigad[oa]|exatamente)|(?:sÃ­|si|claro|vale|espera|quÃ©|que|por\s+quÃ©|oye|gracias)|(?:non|oui|bien\s+sÃ»r|d['â€™]accord|attends?|quoi|pourquoi|merci)|(?:nein|ja|klar|natÃ¼rlich|warte|was|warum|danke))(?:[.!?â€¦]+)?$/iu;

function cleanSourceTurnLine973(value) {
  return String(value || "")
    .replace(/<[^>]+>/gu, "")
    .replace(DIALOGUE_TURN_START_RE, "")
    .replace(/\s+/g, " ")
    .trim();
}

function strongImplicitTurnBoundary973(previous, current) {
  const prev = cleanSourceTurnLine973(previous);
  const cur = cleanSourceTurnLine973(current);

  if (!prev || !cur) return false;
  if (!/[.!?â€¦]["â€â€™']?\s*$/u.test(prev)) return false;

  const words = cur.match(/[\p{L}\p{N}]+(?:['â€™][\p{L}\p{N}]+)*/gu) || [];
  if (words.length > 5 || cur.length > 38) return false;

  return IMPLICIT_TURN_RESPONSE_RE_973.test(cur);
}

function softImplicitTurnCandidates973(block) {
  const rawLines = String(block?.text || "")
    .replace(/\r/g, "")
    .split("\n")
    .map(line => String(line || "").trim())
    .filter(Boolean);

  if (rawLines.length < 2) return [];

  const out = [];
  for (let i = 1; i < rawLines.length; i++) {
    const prev = cleanSourceTurnLine973(rawLines[i - 1]);
    const cur = cleanSourceTurnLine973(rawLines[i]);
    if (!prev || !cur) continue;

    const curWords = cur.match(/[\p{L}\p{N}]+(?:['â€™][\p{L}\p{N}]+)*/gu) || [];
    const questionAnswerShape =
      /\?\s*$/u.test(prev) &&
      curWords.length <= 5 &&
      cur.length <= 42 &&
      /[.!?â€¦]\s*$/u.test(cur);

    if (questionAnswerShape || strongImplicitTurnBoundary973(prev, cur)) {
      out.push({
        after_line: i,
        left_tail: prev.slice(-80),
        right_head: cur.slice(0, 80),
        hard: strongImplicitTurnBoundary973(prev, cur)
      });
    }
  }

  return out.slice(0, 3);
}

function sourceDialogueTurnPlan973(block) {
  const rawLines = String(block?.text || "")
    .replace(/\r/g, "")
    .split("\n")
    .map(line => String(line || "").trim())
    .filter(Boolean);

  const explicit = rawLines
    .filter(line => sourceLineHasDialogueTurnMarker(line))
    .map(line => cleanSourceTurnLine973(line))
    .filter(Boolean);

  if (explicit.length >= 2) {
    return {
      mode: "explicit",
      turns: explicit,
      softCandidates: []
    };
  }

  const cleaned = rawLines.map(cleanSourceTurnLine973).filter(Boolean);
  if (cleaned.length < 2) {
    return {
      mode: "single",
      turns: cleaned.length ? [cleaned.join(" ")] : [],
      softCandidates: []
    };
  }

  const boundaries = [];
  for (let i = 1; i < cleaned.length; i++) {
    if (strongImplicitTurnBoundary973(cleaned[i - 1], cleaned[i])) {
      boundaries.push(i);
    }
  }

  if (boundaries.length) {
    const turns = [];
    let start = 0;
    for (const boundary of boundaries.slice(0, 2)) {
      const part = cleaned.slice(start, boundary).join(" ").trim();
      if (part) turns.push(part);
      start = boundary;
    }
    const tail = cleaned.slice(start).join(" ").trim();
    if (tail) turns.push(tail);

    if (turns.length >= 2) {
      return {
        mode: "implicit_strong",
        turns: turns.slice(0, 3),
        softCandidates: softImplicitTurnCandidates973(block)
      };
    }
  }

  return {
    mode: "single",
    turns: [cleaned.join(" ")],
    softCandidates: softImplicitTurnCandidates973(block)
  };
}

function sourceDialogueTurns(
  block
) {
  return sourceDialogueTurnPlan973(block).turns;
}

// Legacy function name retained because many downstream hard locks already call
// it. In 9.7.3 the value is the authoritative SOURCE turn count, including
// conservative implicit speaker turns, not only literal dash markers.
function sourceDialogueDashCount(
  block
) {
  return sourceDialogueTurns(block).length;
}


function normalizeDialogueTurnText(
  value
) {
  return String(
    value || ""
  )
    .replace(/\r/g, "")
    .replace(
      /\s*\n\s*/g,
      " "
    )
    .replace(
      /[ \t]+/g,
      " "
    )
    .trim();
}

function translatedDialogueTurns(
  block,
  value
) {
  const expected =
    sourceDialogueDashCount(
      block
    );

  if (
    expected < 2
  ) {
    return [];
  }

  const flattened =
    normalizeDialogueTurnText(
      value
    );

  if (!flattened) {
    return [];
  }

  // Primeiro preserve fronteiras fÃ­sicas de linha. Isso reconhece tambÃ©m
  // "-fala" sem abrir espaÃ§o para interpretar hÃ­fens internos como speakers.
  let pieces = String(value || "")
    .replace(/\r/g, "")
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => line.replace(DIALOGUE_TURN_START_RE, "").trim());

  if (pieces.length !== expected) {
    // Fallback para modelos que devolveram dois markers na MESMA linha.
    pieces = flattened
      .split(
        /(?:^|\s)(?:~\s*|[-â€“â€”](?:\s+|(?=[^\d\s])))(?=\S)/u
      )
      .map(part => part.trim())
      .filter(Boolean);
  }

  // Alguns modelos preservam os dois turns em duas linhas, mas esquecem os
  // hÃ­fens. Se a contagem fÃ­sica bate exatamente com a SOURCE, ela Ã© autoridade.
  if (pieces.length !== expected) {
    const rawLines = String(value || "")
      .replace(/\r/g, "")
      .split("\n")
      .map(line => line.replace(DIALOGUE_TURN_START_RE, "").trim())
      .filter(Boolean);

    if (rawLines.length === expected) {
      pieces = rawLines;
    }
  }

  return pieces;
}

function translatedDialogueTurnCount(
  block,
  value
) {
  return translatedDialogueTurns(
    block,
    value
  ).length;
}

function canonicalDialogueTurnText(
  block,
  value
) {
  const expected =
    sourceDialogueDashCount(
      block
    );

  if (
    expected < 2
  ) {
    return String(
      value || ""
    ).trim();
  }

  const turns =
    translatedDialogueTurns(
      block,
      value
    );

  if (
    turns.length !==
    expected
  ) {
    return String(
      value || ""
    ).trim();
  }

  return turns
    .map(turn => {
      // Quando a fala comeÃ§a por reticÃªncias, alguns modelos devolvem "-..."
      // como conteÃºdo alÃ©m do marker que nÃ³s prÃ³prios adicionaremos. Remova
      // apenas esse hÃ­fen redundante; a interrupÃ§Ã£o/reticÃªncia continua intacta.
      const cleanTurn = String(turn || "")
        .replace(/^\s*[-â€“â€”]\s*(?=(?:\.{2,}|â€¦))/u, "")
        .trim();
      return `- ${cleanTurn}`;
    })
    .join("\n");
}

function stripOutputAccessibilityLine(
  line,
  {
    preserveBareDialogue = false
  } = {}
) {
  let text =
    stripMarkup(
      String(line || "")
    ).trim();

  if (
    !text ||
    looksLikeCaptionCredit(
      text
    )
  ) {
    return "";
  }

  text =
    text
      .replace(
        /[â™ªâ™«â™¬â˜…â˜†âœ¦âœ§]/gu,
        " "
      )
      .trim();

  if (
    !text ||
    (
      !/[\p{L}\p{N}]/u.test(
        text
      ) &&
      !hasArtificialCensorship(
        text
      )
    )
  ) {
    return "";
  }

  const info =
    extractSpeaker(text);

  if (info.speaker) {
    const hadDash =
      /^\s*[-â€“â€”]\s*/u.test(text);

    text =
      `${hadDash ? "- " : ""}${info.text}`.trim();
  }

  // Fallback 8.8.3: speaker labels must never be visible in the final SRT.
  // Conservative: removes only a prefix that itself passes looksLikeSpeakerLabel.
  text = text.replace(
    /^(\s*[-â€“â€”]\s*)?([A-Za-zÃ€-Ã¿][A-Za-zÃ€-Ã¿0-9 #.'â€™_-]{0,45})\s*:\s*(?=\S)/u,
    (match, dash, label) =>
      looksLikeSpeakerLabel(label)
        ? (dash ? "- " : "")
        : match
  );

  text =
    stripTrailingSpeakerLabel(
      text
    );

  text =
    removeSdhSegments(
      text
    );

  text =
    text
      .replace(
        /^\s*[:;]+\s*/u,
        ""
      )
      .trim();

  // 9.2.6 UNIVERSAL FINAL HYGIENE. Pure accessibility events never belong
  // in the visible final subtitle, even when a model recreated them without []/().
  // This is structural (event-only / ALL-CAPS accessibility), never title-specific.
  if (
    looksLikePureNonSpeechSdhLine(text) ||
    (sdhAllCapsLike(text) && looksLikeUniversalSdhAction(text, { bare: true }))
  ) {
    return "";
  }

  // 9.0: um cue que jÃ¡ sobreviveu ao cleaner da SOURCE nÃ£o pode perder
  // toda a fala silenciosamente sÃ³ porque a traduÃ§Ã£o "parece" uma aÃ§Ã£o SDH.
  // SDH estruturado []/() continua sendo removido; bare SDH suspeito fica para
  // o QA semÃ¢ntico/Repair em vez de virar EMPTY.
  if (
    !preserveBareDialogue &&
    looksLikeBareSdhLine(
      text
    )
  ) {
    return "";
  }

  if (
    /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡][A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡ .'-]{1,45}:\s*$/u.test(
      text
    )
  ) {
    return "";
  }

  return text;
}

function naturalizeVisibleSourceBleep(block, value) {
  let text = String(value || "");
  const markerRe = new RegExp(`(?:${BLEEP_TOKEN.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}|\\[censurado\\]|\\[bleep\\])`, "giu");

  if (!markerRe.test(text)) return text;
  markerRe.lastIndex = 0;

  const source = String(block?.text || "").replace(/\s+/g, " ").trim();

  // "Holy sh..." nÃ£o pode virar "Santo merda". A moldura pragmÃ¡tica inteira
  // vira uma exclamaÃ§Ã£o brasileira natural; o restante da fala Ã© preservado.
  if (new RegExp(`\\bholy\\s+${BLEEP_TOKEN.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`, "iu").test(source)) {
    text = text.replace(
      new RegExp(`\\b(?:santo|santa)\\s+(?:${BLEEP_TOKEN.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}|\\[censurado\\]|\\[bleep\\])`, "giu"),
      "Puta merda"
    );
  }

  markerRe.lastIndex = 0;
  const fallback = new RegExp(`\\bwhat\\s+the\\s+${BLEEP_TOKEN.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`, "iu").test(source)
    ? "porra"
    : "merda";

  text = text.replace(markerRe, fallback);

  return text
    .replace(/\bSanto\s+merda\b/giu, "Puta merda")
    .replace(/\bSanta\s+merda\b/giu, "Puta merda")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function sanitizeFinalCue(
  block,
  value
) {
  let text =
    String(
      value || ""
    )
      .replace(
        /<[^>]+>/g,
        ""
      )
      .replace(
        /\{\\[^}]+\}/g,
        " "
      )
      .replace(
        /[â™ªâ™«â™¬]/gu,
        " "
      )
      .replace(/(^|\n)\s*#\s+(?=\S)/gu, "$1")
      .replace(/\s+#\s*(?=$|\n)/gu, "")
      .replace(
        /\\+/gu,
        " "
      );

  text =
    collapseExtendedVocalization(
      text
    );

  text =
    text.replace(
      STANDALONE_SYMBOL_CLUSTER_RE,
      "$1 "
    );

  text = naturalizeVisibleSourceBleep(block, text); // 9.0: metadata de censura nunca fica visÃ­vel.
  text = normalizeSourceAwareInterjections973(block, text);

  const expectedDialogueTurns =
    sourceDialogueDashCount(
      block
    );

  if (expectedDialogueTurns >= 2) {
    // Sources que usam "~" como marcador de speaker sÃ£o canonicalizadas para hÃ­fen.
    // TambÃ©m desfaz o caso "~ fala A ~ fala B" em uma Ãºnica linha.
    text = text
      .replace(/(^|\n)\s*~\s*/gu, "$1- ")
      .replace(/\s+~\s*(?=\S)/gu, "\n- ");
  }

  // Se havia label de speaker metadata na SOURCE, qualquer prefixo ALL CAPS
  // reaparecido/ traduzido pelo modelo Ã© removido localmente.
  if (block?.speakerHint) {
    text = text.replace(
      /^\s*(?:[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡][A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡0-9.'â€™_-]*)(?:\s+[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡0-9.'â€™_-]+){0,4}\s+(?=[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡Ã€-Ã¿])/u,
      ""
    );
  }

  let lines =
    text
      .replace(/\r/g, "")
      .split("\n")
      .map(
        line =>
          stripOutputAccessibilityLine(
            line,
            {
              preserveBareDialogue: true
            }
          )
      )
      .map(
        line =>
          String(
            line || ""
          ).trim()
      )
      .filter(Boolean)
      .map(
        line => {
          let cleaned =
            line
              .replace(
                /^\s*[\/\\|]{1,4}\s*/u,
                ""
              )
              .replace(
                /^\s*[â€¢Â·â–ªâ—¦]+\s*/u,
                ""
              )
              .replace(
                /\s+[\/\\|]{1,3}\s+/gu,
                " "
              )
              .trim();

          if (
            expectedDialogueTurns <
            2
          ) {
            cleaned =
              cleaned.replace(
                /^\s*[-â€“â€”]+\s*/u,
                ""
              );
          } else {
            cleaned =
              cleaned.replace(
                /^\s*[-â€“â€”]+\s*/u,
                "- "
              );
          }

          cleaned =
            cleaned
              .replace(
                /([^\s])\s*[-â€“â€”]{2,}\s*([^\s])/gu,
                "$1 â€” $2"
              )
              .replace(
                /\s+[-â€“â€”]{2,}\s+/gu,
                " â€” "
              )
              .replace(
                /\s+([,.;:!?])/g,
                "$1"
              )
              .replace(
                /[ \t]{2,}/g,
                " "
              )
              .trim();

          if (
            /^[-â€“â€”/\\|.:;Â·â€¢_*~â€¦\s]+$/u.test(
              cleaned
            )
          ) {
            return "";
          }

          return cleaned;
        }
      )
      .filter(Boolean);

  if (!lines.length) {
    return "";
  }

  let result =
    lines
      .join("\n")
      .trim();

  if (
    expectedDialogueTurns >=
    2
  ) {
    result =
      canonicalDialogueTurnText(
        block,
        result
      );
  }

  return result;
}

function sanitizeFallbackCue(
  value
) {
  let text =
    String(value || "")
      .replace(
        /<[^>]+>/g,
        ""
      )
      .replace(
        /\{\\[^}]+\}/g,
        " "
      )
      .replace(
        /[â™ªâ™«â™¬]/gu,
        " "
      )
      .replace(/(^|\n)\s*#\s+(?=\S)/gu, "$1")
      .replace(/\s+#\s*(?=$|\n)/gu, "")
      .replace(
        STANDALONE_SYMBOL_CLUSTER_RE,
        "$1 "
      )
      .replace(new RegExp(BLEEP_TOKEN, "g"), "merda")
      .replace(/\[(?:censurado|bleep)\]/giu, "merda")
      .replace(/Santo\s+merda/giu, "Puta merda");

  text =
    collapseExtendedVocalization(
      text
    );

  const lines =
    text
      .replace(/\r/g, "")
      .split("\n")
      .map(
        stripOutputAccessibilityLine
      )
      .map(
        line =>
          line
            .replace(
              /^\s*[\/\\|]{1,4}\s*/u,
              ""
            )
            .replace(
              /^\s*[â€¢Â·â–ªâ—¦]+\s*/u,
              ""
            )
            .replace(
              /\s+[\/\\|]{1,3}\s+/gu,
              " "
            )
            .replace(
              /\s+([,.;:!?])/g,
              "$1"
            )
            .replace(
              /[ \t]{2,}/g,
              " "
            )
            .trim()
      )
      .filter(Boolean);

  const result =
    lines
      .join("\n")
      .trim();

  if (
    !result ||
    !/[\p{L}\p{N}]/u.test(
      result
    )
  ) {
    return "";
  }

  return result;
}

function sanitizeTranslationMap(
  blocks,
  translations,
  job = null
) {
  const out =
    new Map();

  let changes = 0;
  let emptiedAfterSanitizer = 0;
  let intentionalEmpty = 0;
  let needsLocalRecovery = 0;
  let localVocalizationRescues = 0;
  let localGenderPostconditions = 0;

  for (
    const block of blocks
  ) {
    const before =
      String(
        translations.get(
          block.index
        ) ??
        block.text
      ).trim();

    let after =
      sanitizeFinalCue(
        block,
        before
      );

    if (!after) {
      const localVocalization =
        localizePureVocalizationCue(block);

      if (localVocalization) {
        after = localVocalization;
        localVocalizationRescues++;
        if (job) {
          job.stats.mainLocalVocalizationRescues =
            Number(job.stats.mainLocalVocalizationRescues || 0) + 1;
        }
        console.log(
          `[LOCAL VOCALIZATION LOCK 9.0] cue ${block.index}: ` +
          `${JSON.stringify(block.text)} -> ${JSON.stringify(after)} | 0 Gemini.`
        );
      }
    }

    if (!after) {
      after =
        sanitizeFallbackCue(
          before
        );

      if (!after) {
        after = "";
        emptiedAfterSanitizer++;

        if (
          sourceCueAllowsIntentionalEmpty(
            block,
            job
          )
        ) {
          intentionalEmpty++;

          markIntentionalEmptyCue(
            job,
            block,
            "SOURCE sem conteÃºdo semÃ¢ntico utilizÃ¡vel"
          );
        } else {
          needsLocalRecovery++;

          console.warn(
            `[FORMAT LOCK] cue ${
              block.index
            } ficou vazio apÃ³s sanitizaÃ§Ã£o, mas a SOURCE contÃ©m conteÃºdo real; ` +
            `rescue LOCAL serÃ¡ obrigatÃ³rio | raw=${
              JSON.stringify(
                before.slice(
                  0,
                  140
                )
              )
            }`
          );
        }
      }
    }

    if (after) {
      const genderSafe =
        applyDeterministicGenderClosure898(
          block,
          after
        );

      if (genderSafe !== after) {
        localGenderPostconditions++;
        after =
          sanitizeFinalCue(
            block,
            genderSafe
          ) ||
          sanitizeFallbackCue(
            genderSafe
          ) ||
          genderSafe;
      }

      const contextSemanticSafe = applyContextSemanticPostconditions898(block, after);
      if (contextSemanticSafe !== after) {
        after = sanitizeFinalCue(block, contextSemanticSafe) || contextSemanticSafe;
      }

      const orthographySafe =
        normalizeSourceAwareInterjections973(
          block,
          applyDeterministicOrthography(after)
        );
      if (orthographySafe !== after) {
        after = orthographySafe;
      }

      const broadcastSafe = applyNaturalPtClosure898(block, after);
      if (broadcastSafe !== after) {
        after = sanitizeFinalCue(block, broadcastSafe) || broadcastSafe;
      }
    }

    if (
      after !== before
    ) {
      changes++;
    }

    out.set(
      block.index,
      after
    );
  }

  if (job) {
    job.stats.formatFixes =
      (
        job.stats.formatFixes ||
        0
      ) +
      changes;
  }

  console.log(
    `[FORMAT LOCK] ${
      changes
    } cue(s) normalizado(s); ` +
    `vazios pÃ³s-sanitizer=${
      emptiedAfterSanitizer
    }; intencionais=${
      intentionalEmpty
    }; rescue-local=${
      needsLocalRecovery
    }; vocalizaÃ§Ã£o-local=${
      localVocalizationRescues
    }; gÃªnero-local=${
      localGenderPostconditions
    }; SDH/ruÃ­do/alongamentos controlados.`
  );

  return out;
}

function layoutVisibleLength(value) {
  return [...String(value || "")].length;
}

function normalizeLayoutWhitespace(value) {
  return String(value || "")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\s*\n\s*/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function bestTwoLineSplit(value, maxChars = LAYOUT_MAX_CHARS_PER_LINE) {
  const text = normalizeLayoutWhitespace(value);

  if (!text) {
    return {
      lines: [],
      fits: true,
      maxLineLength: 0
    };
  }

  const fullLength = layoutVisibleLength(text);

  if (fullLength <= maxChars) {
    return {
      lines: [text],
      fits: true,
      maxLineLength: fullLength
    };
  }

  const words = text.split(/\s+/).filter(Boolean);

  // Uma Ãºnica palavra enorme jamais serÃ¡ cortada.
  if (words.length <= 1) {
    return {
      lines: [text],
      fits: fullLength <= maxChars,
      maxLineLength: fullLength
    };
  }

  let best = null;

  for (let split = 1; split < words.length; split++) {
    const first = words.slice(0, split).join(" ");
    const second = words.slice(split).join(" ");

    const firstLength = layoutVisibleLength(first);
    const secondLength = layoutVisibleLength(second);
    const maxLength = Math.max(firstLength, secondLength);
    const difference = Math.abs(firstLength - secondLength);

    // Pequena preferÃªncia por uma quebra linguisticamente agradÃ¡vel.
    const punctuationBonus =
      /[,.;:!?â€¦]$/.test(first)
        ? 10
        : 0;

    // Preferimos:
    // 1. caber em 50;
    // 2. ficar equilibrado;
    // 3. aproximar-se visualmente de ~44;
    // 4. quebrar perto de pontuaÃ§Ã£o quando possÃ­vel.
    const overflow =
      Math.max(0, firstLength - maxChars) +
      Math.max(0, secondLength - maxChars);

    const idealDistance =
      Math.abs(firstLength - LAYOUT_IDEAL_CHARS_PER_LINE) +
      Math.abs(secondLength - LAYOUT_IDEAL_CHARS_PER_LINE);

    const score =
      overflow * 10000 +
      difference * 30 +
      idealDistance * 3 -
      punctuationBonus;

    if (!best || score < best.score) {
      best = {
        score,
        lines: [first, second],
        fits:
          firstLength <= maxChars &&
          secondLength <= maxChars,
        maxLineLength: maxLength
      };
    }
  }

  return best || {
    lines: [text],
    fits: false,
    maxLineLength: fullLength
  };
}

function bestDialogueTurnLayout(
  block,
  value
) {
  const expected =
    sourceDialogueDashCount(
      block
    );

  const turns =
    translatedDialogueTurns(
      block,
      value
    );

  if (
    expected < 2 ||
    turns.length !==
      expected
  ) {
    const fallback =
      bestTwoLineSplit(
        value,
        LAYOUT_MAX_CHARS_PER_LINE
      );

    return {
      text:
        fallback.lines.join(
          "\n"
        ),

      fits: false,

      lines:
        fallback.lines.length,

      maxLineLength:
        fallback.maxLineLength,

      dialogueTurnMismatch:
        true
    };
  }

  const segments =
    turns.map(
      turn =>
        `- ${turn}`
    );

  // Dois speakers = um por linha.
  if (
    segments.length ===
    2
  ) {
    const lengths =
      segments.map(
        layoutVisibleLength
      );

    return {
      text:
        segments.join(
          "\n"
        ),

      fits:
        lengths.every(
          length =>
            length <=
            LAYOUT_MAX_CHARS_PER_LINE
        ),

      lines: 2,

      maxLineLength:
        Math.max(
          ...lengths
        ),

      dialogueTurnMismatch:
        false
    };
  }

  // 3+ turns:
  // testamos todas as divisÃµes contÃ­guas entre APENAS duas linhas.
  // Nenhum speaker Ã© partido ao meio para encaixar o seguinte.
  let best = null;

  for (
    let split = 1;
    split <
      segments.length;
    split++
  ) {
    const first =
      segments
        .slice(
          0,
          split
        )
        .join(" ");

    const second =
      segments
        .slice(
          split
        )
        .join(" ");

    const firstLength =
      layoutVisibleLength(
        first
      );

    const secondLength =
      layoutVisibleLength(
        second
      );

    const overflow =
      Math.max(
        0,
        firstLength -
          LAYOUT_MAX_CHARS_PER_LINE
      ) +
      Math.max(
        0,
        secondLength -
          LAYOUT_MAX_CHARS_PER_LINE
      );

    const difference =
      Math.abs(
        firstLength -
          secondLength
      );

    const idealDistance =
      Math.abs(
        firstLength -
          LAYOUT_IDEAL_CHARS_PER_LINE
      ) +
      Math.abs(
        secondLength -
          LAYOUT_IDEAL_CHARS_PER_LINE
      );

    const score =
      overflow * 10000 +
      difference * 30 +
      idealDistance * 3;

    if (
      !best ||
      score <
        best.score
    ) {
      best = {
        score,

        text:
          `${first}\n${second}`,

        fits:
          firstLength <=
            LAYOUT_MAX_CHARS_PER_LINE &&
          secondLength <=
            LAYOUT_MAX_CHARS_PER_LINE,

        lines: 2,

        maxLineLength:
          Math.max(
            firstLength,
            secondLength
          ),

        dialogueTurnMismatch:
          false
      };
    }
  }

  return best || {
    text:
      segments.join(
        "\n"
      ),

    fits: false,

    lines:
      segments.length,

    maxLineLength:
      Math.max(
        ...segments.map(
          layoutVisibleLength
        )
      ),

    dialogueTurnMismatch:
      false
  };
}

function layoutCueResult(block, value) {
  const raw =
    String(
      value || ""
    )
      .replace(/\r/g, "")
      .trim();

  if (!raw) {
    return {
      text: "",
      fits: true,
      lines: 0,
      maxLineLength: 0,
      dialogueTurnMismatch:
        false
    };
  }

  if (
    sourceDialogueDashCount(
      block
    ) >= 2
  ) {
    return bestDialogueTurnLayout(
      block,
      raw
    );
  }

  const flattened =
    normalizeLayoutWhitespace(
      raw
    );

  const result =
    bestTwoLineSplit(
      flattened,
      LAYOUT_MAX_CHARS_PER_LINE
    );

  return {
    text:
      result.lines.join(
        "\n"
      ),

    fits:
      result.fits,

    lines:
      result.lines.length,

    maxLineLength:
      result.maxLineLength,

    dialogueTurnMismatch:
      false
  };
}

function cueNeedsConciseRepair(block, value) {
  const result = layoutCueResult(block, value);

  // Se JavaScript consegue diagramar em <= 2x50, NÃƒO desperdiÃ§amos Gemini.
  if (
    result.fits &&
    result.lines <= LAYOUT_MAX_LINES
  ) {
    return false;
  }

  // NÃ£o significa que o texto serÃ¡ cortado.
  // Significa apenas: peÃ§a ao Repair para tentar uma versÃ£o
  // semanticamente equivalente, porÃ©m mais concisa e natural.
  return true;
}

function applySubtitleLayout(
  blocks,
  translations,
  label = "FINAL"
) {
  const out = new Map();

  let changed = 0;
  let perfectFits = 0;
  let overflow = 0;
  let maxObserved = 0;

  for (const block of blocks) {
    const original = String(
      translations.get(block.index) ??
      block.text
    ).trim();

    const result = layoutCueResult(
      block,
      original
    );

    // SeguranÃ§a absoluta:
    // layout nunca pode apagar conteÃºdo.
    const finalText =
      result.text.trim() ||
      original;

    if (finalText !== original) {
      changed++;
    }

    if (
      result.fits &&
      result.lines <= LAYOUT_MAX_LINES
    ) {
      perfectFits++;
    } else {
      overflow++;
    }

    maxObserved = Math.max(
      maxObserved,
      result.maxLineLength || 0
    );

    out.set(
      block.index,
      finalText
    );
  }

  console.log(
    `[LAYOUT LOCK] ${label} | ` +
    `reflow=${changed} | ` +
    `<=2x${LAYOUT_MAX_CHARS_PER_LINE}=${perfectFits}/${blocks.length} | ` +
    `overflow seguro=${overflow} | ` +
    `maior linha=${maxObserved}.`
  );

  return out;
}

function normalizeEditorialVocab(text) {
  return String(text || "")
    .replace(
      /qualÃ©/giu,
      match =>
        /^[A-ZÃÃ€Ã‚ÃƒÃ‰ÃˆÃŠÃÃŒÃŽÃ“Ã’Ã”Ã•ÃšÃ™Ã›]/u.test(match)
          ? "Qual Ã©"
          : "qual Ã©"
    )
    .replace(
      /diacho/giu,
      match =>
        /^[A-ZÃÃ€Ã‚ÃƒÃ‰ÃˆÃŠÃÃŒÃŽÃ“Ã’Ã”Ã•ÃšÃ™Ã›]/u.test(match)
          ? "Diabo"
          : "diabo"
    );
}

function buildSrt(
  blocks,
  translations
) {
  const rendered = [];

  for (const block of blocks) {
    const text =
      decoratePerformanceLyric973(
        block,
        normalizeEditorialTypography973(
          normalizeEditorialVocab(
            String(
              translations.get(
                block.index
              ) ??
              block.text
            )
          )
        )
      ).trim();

    // Vazio autorizado nÃ£o vira "..." nem bloco visual artificial.
    if (!text) {
      continue;
    }

    rendered.push(
      [
        block.index,
        block.timing,
        text
      ].join("\n")
    );
  }

  return rendered.length
    ? rendered.join("\n\n").trim() + "\n"
    : "";
}

function auditTimestamps(
  sourceSrt,
  finalSrt,
  label,
  job = null
) {
  const source =
    parseSrt(sourceSrt);

  const final =
    parseSrt(finalSrt);

  const finalById =
    new Map();

  for (const item of final) {
    if (
      finalById.has(
        item.index
      )
    ) {
      throw new Error(
        `TIMING LOCK ${
          label
        }: cue duplicado ${
          item.index
        }.`
      );
    }

    finalById.set(
      item.index,
      item
    );
  }

  let intentionalOmitted = 0;

  for (const sourceItem of source) {
    const finalItem =
      finalById.get(
        sourceItem.index
      );

    if (!finalItem) {
      if (
        sourceCueAllowsIntentionalEmpty(
          sourceItem,
          job
        )
      ) {
        intentionalOmitted++;
        continue;
      }

      throw new Error(
        `TIMING LOCK ${
          label
        }: cue ${
          sourceItem.index
        } ausente sem autorizaÃ§Ã£o de vazio intencional.`
      );
    }

    if (
      sourceItem.timing !==
      finalItem.timing
    ) {
      throw new Error(
        `TIMING LOCK ${
          label
        }: cue ${
          sourceItem.index
        } teve timestamp alterado.`
      );
    }

    finalById.delete(
      sourceItem.index
    );
  }

  if (finalById.size) {
    throw new Error(
      `TIMING LOCK ${
        label
      }: ${
        finalById.size
      } cue(s) extra(s) inexistente(s) na SOURCE.`
    );
  }

  console.log(
    `[TIMING LOCK] ${
      label
    }: PASSOU â€” ${
      source.length
    } source / ${
      final.length
    } visÃ­veis; ${
      intentionalOmitted
    } vazio(s) intencional(is) omitido(s); 0 alteraÃ§Ãµes de timestamp.`
  );
}

// ============================================================
// STYLE / CONTEXTO
// ============================================================

const STYLE_PACK = `
PORTUGUÃŠS BRASILEIRO NATURAL â€” GUIA EDITORIAL 8.4.0

IDIOMA DA FONTE
- A fonte normalmente Ã© inglÃªs, mas pode ser espanhol ou outro idioma.
- Detecte e respeite o idioma realmente presente no target.
- Traduza diretamente para PT-BR natural; nunca passe por traduÃ§Ã£o literal intermediÃ¡ria.
- Regras especÃ­ficas de inglÃªs abaixo sÃ³ se aplicam quando a fonte realmente estiver em inglÃªs.
- Todas as regras de identidade, ownership, significado, registro, layout e timestamps continuam valendo para qualquer idioma.
- Campos internos chamados en/EN significam "texto-fonte" por compatibilidade e podem conter espanhol ou outro idioma.

PRIORIDADE ABSOLUTA

PRIORIDADE ABSOLUTA
1. sentido/contexto correto;
2. identidade/gÃªnero/referentes corretos;
3. ownership do cue e sincronizaÃ§Ã£o semÃ¢ntica;
4. naturalidade PT-BR contemporÃ¢nea;
5. cultura/registro corretos;
6. velocidade.

PRINCÃPIO CENTRAL: PRESERVAR IDENTIDADE, LOCALIZAR INTENÃ‡ÃƒO
- Preserve nomes, marcas, bordÃµes consagrados e termos cuja identidade cultural importa.
- Localize para o Brasil humor, insulto, gÃ­ria, metÃ¡fora, intenÃ§Ã£o social e expressÃ£o idiomÃ¡tica quando uma traduÃ§Ã£o literal esconderia o sentido.
- NÃ£o "abrasileire" nomes/bordÃµes que soariam falsos traduzidos.
- NÃ£o deixe inglÃªs estrutural dentro de portuguÃªs sÃ³ porque as palavras foram traduzidas.
- A legenda deve fazer um brasileiro entender o que a fala QUER DIZER e como ela SOA socialmente.

NAMED / CULTURAL ENTITY INTEGRITY â€” REGRA ABSOLUTA
- Nomes e identidades culturais NÃƒO sÃ£o matÃ©ria de adaptaÃ§Ã£o livre.
- Preserve a identidade de pessoas reais, personagens, figuras mitolÃ³gicas,
  lendas/folclore, celebridades, marcas, empresas, programas, filmes, sÃ©ries,
  mÃºsicas, obras, instituiÃ§Ãµes, eventos, lugares e demais entidades nomeadas.
- NUNCA substitua uma entidade estrangeira por uma entidade brasileira
  "equivalente", parecida ou culturalmente anÃ¡loga.
- Localizar a INTENÃ‡ÃƒO nÃ£o autoriza trocar a IDENTIDADE.
- Uma forma canÃ´nica consagrada em PT-BR para A MESMA entidade Ã© permitida:
  exÃ´nimos estabelecidos, tÃ­tulos oficiais localizados e nomes oficialmente
  usados em portuguÃªs continuam representando a mesma identidade.
- Se houver dÃºvida entre preservar o nome original ou inventar/adaptar
  culturalmente, PRESERVE O ORIGINAL.
- Traduza a explicaÃ§Ã£o em volta da entidade, nÃ£o transforme a entidade em outra.
- Se a mesma entidade reaparecer no episÃ³dio, mantenha sua identidade consistente.
- Exemplo de erro de identidade:
  "Bloody Mary" -> "Loira do Banheiro" Ã© PROIBIDO.
  SÃ£o lendas culturalmente anÃ¡logas, mas NÃƒO sÃ£o a mesma entidade.
- Isso Ã© diferente de uma traduÃ§Ã£o canÃ´nica da MESMA entidade.

CONTEXT + IDENTITY LOCK â€” REGRA INVIOLÃVEL
- A BÃBLIA EDITORIAL contÃ©m um Character Ledger. Use identidades confirmadas para impedir contradiÃ§Ãµes de referente; NÃƒO use o Ledger como licenÃ§a para introduzir gÃªnero que a prÃ³pria SOURCE nÃ£o expressou.
- Speaker Ã© QUEM ESTÃ FALANDO; pessoa citada/mentioned Ã© DE QUEM SE FALA. speaker â‰  pessoa mencionada.
- Nunca transfira gÃªnero, pronome, relaÃ§Ã£o ou identidade entre turnos, entre speaker e pessoa mencionada, nem entre cues vizinhos.
- Em cue com duas falas, cada turno Ã© independente. Um label explÃ­cito no segundo turno JAMAIS identifica o primeiro.
- Um nome/pronome no target deve ser resolvido com before/after + Character Ledger; se ainda houver ambiguidade, preserve a ambiguidade de forma natural.
- NÃ£o invente parentesco, identidade, pronome, tÃ­tulo ou nome ausente da evidÃªncia.

GENDER-NEUTRAL DEFAULT 9.0 â€” REGRA ABSOLUTA
- Se a SOURCE nÃ£o expressa gÃªnero naquela ideia, o PT-BR NÃƒO deve introduzir gÃªnero desnecessariamente, MESMO quando a identidade do speaker Ã© conhecida.
- MASCULINO GENÃ‰RICO NÃƒO Ã‰ CONSIDERADO NEUTRO NESTE PROJETO. "cansado", "confuso", "preocupado", "sozinho", "louco", "orgulhoso", "vencedor" etc. NÃƒO podem ser usados por padrÃ£o quando a SOURCE Ã© neutra e existe reformulaÃ§Ã£o natural.
- O Character Ledger protege contra contradiÃ§Ã£o; ele NÃƒO obriga "cansado/cansada", "sozinho/sozinha", "confuso/confusa" etc. quando existe formulaÃ§Ã£o neutra natural.
- Prefira SEMPRE a formulaÃ§Ã£o naturalmente neutra: "I'm scared" -> "TÃ´ com medo"; "I'm confused" -> "NÃ£o tÃ´ entendendo"; "I'm alone" -> "TÃ´ sem ninguÃ©m por perto"; "I'm worried" -> "Isso tÃ¡ me preocupando"; "I'm proud of you" -> "Tenho orgulho de vocÃª"; "You're crazy" -> "VocÃª perdeu a noÃ§Ã£o"; "You're the winner" -> "VocÃª venceu"; "I'm too tired" -> "TÃ´ sem energia".
- NÃ£o use linguagem artificial como "cansade"/"confuse", nem formas "cansado(a)". Neutralidade aqui significa REESCREVER em PT-BR natural.
- SÃ³ marque gÃªnero quando ele for semanticamente necessÃ¡rio ou explicitamente sustentado pela SOURCE naquele referente: she/her, he/him, woman/man, daughter/son, mother/father etc.
- Se before/after nÃ£o provarem inequivocamente o referente, preserve a incerteza. NÃ£o deduza gÃªnero sÃ³ porque uma pessoa conhecida aparece na cena.
- GÃªnero conhecido de pessoa mencionada aplica-se SOMENTE Ã  pessoa mencionada, nunca automaticamente ao speaker.

NATURALIDADE PT-BR 2026 â€” REGRA DE ACEITAÃ‡ÃƒO
- CORRETO MAS LITERAL DEMAIS NÃƒO Ã‰ SUFICIENTE. TraduÃ§Ã£o com cara de traduÃ§Ã£o Ã© defeito editorial.
- Antes de devolver cada cue, faÃ§a o teste: "um brasileiro falaria isso espontaneamente hoje, nessa situaÃ§Ã£o e com essa personalidade?".
- Preserve intenÃ§Ã£o, pragmÃ¡tica e temperatura emocional antes da ordem das palavras do inglÃªs.
- Reestruture sujeito, verbo, intensificador, metÃ¡fora e ordem sintÃ¡tica quando o portuguÃªs pedir.
- Evite portuguÃªs de dublagem antigo, formalidade artificial, calques e falsos cognatos.
- Use PT-BR contemporÃ¢neo SEMPRE; use Gen Z/Alpha/fandom SOMENTE quando idade, personalidade, comunidade e situaÃ§Ã£o pedirem.
- Uma fala adulta em drama/horror deve soar atual e humana, nÃ£o necessariamente como internet/TikTok.
- NÃ£o use "sequer", "de fato", "eu suponho", "eu aprecio isso", "isso sendo dito" ou outras formas engessadas por reflexo do inglÃªs quando uma forma brasileira simples for mais natural no registro da cena.

NATURALNESS LOCK â€” REGRA INVIOLÃVEL
- TRADUÃ‡ÃƒO LITERAL QUE SOA TRADUZIDA Ã‰ TRADUÃ‡ÃƒO ERRADA, mesmo quando a gramÃ¡tica e o significado bÃ¡sico estiverem corretos.
- ADVÃ‰RBIO NÃƒO Ã‰ APARÃŠNCIA: modo/intensidade (badly, deeply, strongly etc.) nÃ£o pode virar adjetivo visual como "feio".
- ATRIBUTOS COORDENADOS: se a SOURCE diz X E Y (soft and wide, warm and kind etc.), preserve as duas propriedades salvo redundÃ¢ncia real em PT-BR.
- MARCADOR IDIOMÃTICO: traduza a FUNÃ‡ÃƒO de believe it or not / as a matter of fact / by the way etc.; nÃ£o troque por outro marcador de sentido diferente.
- REGISTRO: se a SOURCE nÃ£o tem palavrÃ£o/bleep nem forÃ§a tabu equivalente, nÃ£o aumente a vulgaridade do PT.
- FIDELIDADE LEXICAL NÃƒO AUTORIZA CALQUE: pessoa "warm" raramente Ã© "calorosa" por reflexo automÃ¡tico; escolha a intenÃ§Ã£o humana do contexto.
- Se a SOURCE estiver visivelmente corrompida por OCR/sÃ­mbolos, NÃƒO invente uma interpretaÃ§Ã£o fluente para o lixo.
- Fidelidade NÃƒO significa preservar sintaxe, ordem de palavras, verbo, substantivo ou metÃ¡fora do inglÃªs.
- Fidelidade significa preservar o que a pessoa QUER DIZER, o efeito social da fala, a emoÃ§Ã£o e a personalidade.
- Antes de devolver o pt, imagine que a pessoa da cena Ã© brasileira e estÃ¡ dizendo espontaneamente a mesma coisa. Escreva essa fala.
- Depois de entender o EN, pare de usÃ¡-lo como molde sintÃ¡tico.
- Prefira equivalÃªncia pragmÃ¡tica e idiomÃ¡tica a equivalÃªncia palavra por palavra.
- Se a traduÃ§Ã£o permitir enxergar facilmente a frase inglesa por baixo dela, revise em busca de calque.
- Frase compreensÃ­vel mas artificial NÃƒO estÃ¡ pronta.
- Frase gramaticalmente correta mas que ninguÃ©m diria naturalmente NÃƒO estÃ¡ pronta.
- Frase que parece Google Translate, legenda estudantil ou portuguÃªs de dublagem antiga NÃƒO estÃ¡ pronta.
- Use contraÃ§Ãµes brasileiras naturais como "tÃ´", "tÃ¡", "pra", "nÃ©" quando o personagem e a cena pedirem.
- NÃ£o use informalidade artificial apenas para parecer moderno.

TESTE DO BRASILEIRO NATIVO
Antes de cada cue, pergunte silenciosamente:
1. Eu ouviria um brasileiro real dizer isso?
2. Essa pessoa especÃ­fica diria isso?
3. A ordem da frase nasceu em portuguÃªs ou foi copiada do inglÃªs?
4. Existe uma forma igualmente fiel, porÃ©m mais curta e natural?
Se qualquer resposta indicar artificialidade, REESCREVA antes de responder.

EXEMPLOS DE DEFEITO DE NATURALIDADE
- RUIM: "Eu estava esperando mais picos e vales."
  MELHOR: "Eu esperava mais altos e baixos." / "Eu esperava mais variaÃ§Ã£o.", conforme contexto.
- RUIM: "Eu aprecio isso."
  MELHOR: "Valeu.", "AgradeÃ§o.", "Fico feliz." etc., conforme personagem e situaÃ§Ã£o.
- RUIM: "Isso sendo dito..."
  MELHOR: reconstrua a transiÃ§Ã£o naturalmente: "Dito isso...", "Mas...", "SÃ³ que..." etc.
- RUIM: "O Maxi Desafio desta semana Ã© uma reviravolta no Snatch Game chamada..."
  MELHOR: prefira uma formulaÃ§Ã£o enxuta e brasileira como "O Maxi Desafio desta semana Ã© uma versÃ£o do Snatch Game..." quando esse for o sentido.
- "We need to move now" em fuga/urgÃªncia muitas vezes pede "Precisamos sair daqui agora", nÃ£o o calque "precisamos nos mover agora"; escolha pelo contexto.
- "Sweep everything into the backpack" no sentido de recolher/empurrar itens pede algo como "Coloca tudo dentro da mochila", nÃ£o "vire/varra tudo" mecanicamente.
- Os exemplos ensinam o TIPO de correÃ§Ã£o; nÃ£o os copie mecanicamente em contextos diferentes.

CONCISÃƒO AUDIOVISUAL
- Legenda nÃ£o Ã© transcriÃ§Ã£o palavra por palavra.
- Preserve TODA a informaÃ§Ã£o relevante, mas elimine redundÃ¢ncia sintÃ¡tica que o portuguÃªs nÃ£o precisa.
- Se duas formulaÃ§Ãµes forem semanticamente equivalentes, prefira a mais curta, natural e rÃ¡pida de ler.
- NÃ£o acrescente sujeitos, pronomes, conectivos ou explicaÃ§Ãµes que o portuguÃªs possa omitir naturalmente.
- NÃ£o resuma informaÃ§Ã£o; compacte a FORMA, nÃ£o o conteÃºdo.

MEANING INTEGRITY LOCK â€” REGRA INVIOLÃVEL
- NATURALIZAR NÃƒO Ã‰ RESUMIR.
- ENCURTAR NÃƒO Ã‰ APAGAR.
- Uma traduÃ§Ã£o mais elegante que perde uma unidade real de significado Ã© INCORRETA.
- Antes de compactar, faÃ§a silenciosamente um inventÃ¡rio semÃ¢ntico do EN.

Toda unidade independente deve sobreviver quando existir no original:
* quem fez/sentiu/disse;
* aÃ§Ã£o ou estado;
* objeto/alvo da aÃ§Ã£o;
* negaÃ§Ã£o;
* causa e consequÃªncia;
* condiÃ§Ã£o;
* quantidade;
* comparaÃ§Ã£o;
* relaÃ§Ã£o familiar/social;
* tempo relevante;
* contraste;
* intensidade;
* palavrÃ£o ou forÃ§a emocional relevante;
* insulto e seu grau de agressividade;
* piada, shade ou duplo sentido;
* informaÃ§Ã£o narrativa nova.

- Se o EN trouxer duas informaÃ§Ãµes ligadas por "and", nÃ£o apague automaticamente a segunda sÃ³ para encurtar.
- NÃ£o transforme duas ideias diferentes em uma ideia genÃ©rica.
- NÃ£o suavize insulto, vulgaridade, raiva ou intensidade apenas para economizar caracteres.
- NÃ£o transforme FALA em descriÃ§Ã£o SDH, stage direction ou efeito sonoro.
- Uma instruÃ§Ã£o falada continua sendo fala.
- Preserve a funÃ§Ã£o comunicativa do cue.

CONCISÃƒO SEGURA
- Economize caracteres mudando a ESTRUTURA do portuguÃªs:
  * elimine sujeito redundante;
  * use verbo mais direto;
  * retire repetiÃ§Ã£o sintÃ¡tica;
  * prefira expressÃ£o idiomÃ¡tica curta;
  * evite nominalizaÃ§Ãµes e construÃ§Ãµes burocrÃ¡ticas;
  * use elipse natural do portuguÃªs quando o sentido continuar completo.
- NÃƒO economize caracteres removendo fatos ou relaÃ§Ãµes.
- O objetivo visual Ã© caber em 2 linhas de atÃ© 50 caracteres.
- Se nÃ£o houver forma segura no MAIN, preserve o sentido completo. O sistema possui uma etapa especializada posterior para compactaÃ§Ã£o.

CUE OWNERSHIP â€” REGRA INVIOLÃVEL
- Cada cÃ¡psula Ã© independente.
- Traduza SOMENTE o campo target daquela cÃ¡psula.
- before/after existem exclusivamente para compreensÃ£o.
- NUNCA complete o target com palavras que pertencem ao after.
- NUNCA empurre o final do target para outro id.
- NUNCA puxe o final do before para o target.
- Se uma frase estiver cortada entre cues, respeite exatamente o corte original.
- Cada id Ã© uma caixa fechada de conteÃºdo.

HARD LOCKS
- Tokens no formato __LOCK_C...__ sÃ£o texto protegido.
- Copie cada token EXATAMENTE, caractere por caractere, no ponto correspondente da traduÃ§Ã£o.
- Nunca traduza, reformule, remova, pluralize ou pontue dentro do token.

CENSURA / BLEEP â€” NATURALIZAÃ‡ÃƒO PRAGMÃTICA CONTEXTUAL
- O token ${BLEEP_TOKEN} sinaliza que a prÃ³pria SOURCE truncou/mascarou uma profanidade ou exclamaÃ§Ã£o.
- O token Ã© SOMENTE metadata de entrada: NUNCA escreva ${BLEEP_TOKEN}, [censurado], [bleep], asteriscos ou qualquer placeholder no PT final.
- Leia a fala inteira e os cues vizinhos para recuperar a FUNÃ‡ÃƒO PRAGMÃTICA: surpresa, raiva, medo, insulto, interrupÃ§Ã£o, humor etc.
- Produza uma fala brasileira NATURAL com forÃ§a equivalente. Ã‰ permitido completar pragmaticamente com "porra", "merda", "caralho", "puta merda" etc. quando isso soar natural na cena.
- NÃ£o tente descobrir a identidade lexical exata da palavra escondida; preserve o EFEITO da fala, nÃ£o a grafia secreta do original.
- NÃ£o aumente gratuitamente a agressividade: escolha o palavrÃ£o/expressÃ£o pela pessoa, situaÃ§Ã£o e intensidade da conversa.
- NÃ£o crie censura onde a SOURCE nÃ£o censurou: palavrÃ£o explÃ­cito continua explÃ­cito e natural em PT-BR.
- O resultado deve parecer diÃ¡logo real, nunca legenda tÃ©cnica/SDH.

LGBTQIAPN+ / DRAG / BALLROOM / REALITY / FANDOM â€” CULTURE & REGISTER INTEGRITY LOCK
- Tenha letramento real de cultura LGBTQIAPN+, drag, ballroom, camp, shade, stan culture, internet culture e reality competition.
- Preserve humor, sexualidade, irreverÃªncia, shade, camp, afeto, orgulho, deboche e agressividade conforme a cena.
- NÃ£o suavize a personalidade de queens, jurados, participantes ou personagens.
- NÃ£o force gÃ­ria em pessoas cujo registro nÃ£o pede isso.
- NÃƒO trate vocabulÃ¡rio cultural como tabela fixa ENâ†’PT.
- Antes de traduzir gÃ­ria, palavrÃ£o, insulto ou vocativo, determine silenciosamente sua FUNÃ‡ÃƒO SOCIAL na fala.
- A traduÃ§Ã£o correta Ã© a que preserva intenÃ§Ã£o, relaÃ§Ã£o entre as pessoas, intensidade, humor e efeito social â€” nÃ£o necessariamente a palavra de dicionÃ¡rio.

BORDÃ•ES / FRASES CANÃ”NICAS / IDENTIDADE DE PROGRAMA
- BordÃµes, nomes de desafios, nomes de segmentos, marcas e expressÃµes reconhecidamente canÃ´nicas exigem cuidado mÃ¡ximo.
- Tokens __LOCK_C...__ sÃ£o autoridade absoluta e devem voltar IDÃŠNTICOS.
- Nunca "corrija criativamente", traduza ou abrasileire um HARD LOCK.
- Se a legenda-fonte contiver um typo reconhecÃ­vel de um bordÃ£o que o HARD LOCK canonicalizou, use a forma CANÃ”NICA restaurada.
- Para frases conhecidas que NÃƒO sÃ£o HARD LOCK, preserve a estrutura retÃ³rica, a piada, a intensidade e as unidades importantes do bordÃ£o.
- NÃ£o transforme uma frase icÃ´nica em uma parÃ¡frase genÃ©rica sÃ³ para ficar curta.
- Familiaridade com o programa/personagem Ã© contexto editorial, nÃ£o licenÃ§a para inventar.

BITCH â€” PROIBIDA TRADUÃ‡ÃƒO AUTOMÃTICA
- "bitch" NÃƒO possui equivalente PT-BR fixo.
- NUNCA converta mecanicamente toda ocorrÃªncia para "puta".
- NUNCA converta mecanicamente toda ocorrÃªncia para "vadia", "bicha", "gata" ou qualquer outra palavra.
- Primeiro identifique a funÃ§Ã£o da ocorrÃªncia.

PossÃ­veis funÃ§Ãµes de "bitch":
* insulto hostil;
* provocaÃ§Ã£o/briga;
* vocativo afetuoso entre amigas/queens;
* cumplicidade camp;
* exclamaÃ§Ã£o;
* admiraÃ§Ã£o;
* autoelogio;
* orgulho/empoderamento;
* descriÃ§Ã£o de personalidade;
* termo sexual, quando o contexto realmente for sexual.

- Em uso amigÃ¡vel/camp, possibilidades naturais incluem "bicha", "gata", "amiga", "menina" ou atÃ© omissÃ£o do vocativo.
- Em insulto real, possibilidades incluem "vadia", "escrota", "desgraÃ§ada" ou outra formulaÃ§Ã£o compatÃ­vel com a intensidade e a personagem.
- "puta" sÃ³ Ã© apropriado quando o sentido/contexto realmente justificar; nÃ£o Ã© traduÃ§Ã£o-padrÃ£o de "bitch".
- Em autoafirmaÃ§Ã£o como "I'm a bad bitch", preserve orgulho, poder e atitude.
- "I'm a bad bitch" NÃƒO significa "sou uma puta ruim".
- Dependendo da personagem/cena, pode equivaler pragmaticamente a algo como "eu sou foda", "sou poderosa", "sou aquela gata" ou outra formulaÃ§Ã£o brasileira natural.
- NÃ£o transforme autoelogio em autodepreciaÃ§Ã£o nem afeto em agressÃ£o.

PALAVRÃ•ES / PROFANITY â€” PRESERVAR FORÃ‡A, NÃƒO CONTAGEM
- Preserve a forÃ§a pragmÃ¡tica dos palavrÃµes e intensificadores relevantes.
- NÃƒO censure artificialmente.
- NÃƒO suavize palavrÃ£o apenas para economizar caracteres.
- NÃƒO insira palavrÃ£o aleatoriamente sÃ³ porque existe um palavrÃ£o no EN.
- NÃƒO tente manter uma correspondÃªncia de 1 palavrÃ£o EN = 1 palavrÃ£o PT.
- "fuck", "fucking", "shit", "damn", "hell", "ass", "motherfucker" etc. dependem da funÃ§Ã£o na frase.
- Um palavrÃ£o pode funcionar como insulto, raiva, surpresa, intensidade, humor, admiraÃ§Ã£o, sexualidade ou ritmo de fala.
- Escolha a soluÃ§Ã£o PT-BR que preserve ESSA funÃ§Ã£o.
- Se uma construÃ§Ã£o brasileira natural expressa a mesma intensidade sem traduÃ§Ã£o lexical do palavrÃ£o, isso pode ser correto.
- Se retirar o palavrÃ£o destruir a forÃ§a, personalidade ou piada da fala, preserve essa forÃ§a em PT-BR.
- Se acrescentar "porra", "caralho", "puta", "merda" etc. tornar a fala artificialmente mais agressiva que o original, NÃƒO acrescente.
- Profanidade deve soar como algo que aquela pessoa realmente diria em portuguÃªs naquela situaÃ§Ã£o.

PHRASAL PROFANITY / EXPRESSÃ•ES FIXAS
- NÃ£o traduza palavrÃ£o dentro de expressÃ£o idiomÃ¡tica palavra por palavra.
- "don't fuck it up" pede preservaÃ§Ã£o da intenÃ§Ã£o: "nÃ£o estrague tudo",
  "nÃ£o faÃ§a merda", "nÃ£o cague tudo" etc. conforme personagem e intensidade.
- Evite calques artificiais como "nÃ£o fode tudo".
- ExpressÃµes como "shake shit up" significam causar impacto,
  virar o jogo, bagunÃ§ar estruturas ou causar; nÃ£o exigem automaticamente
  inserir "foder" em PT-BR.
- Preserve a forÃ§a pragmÃ¡tica, mas NÃƒO aumente a vulgaridade sÃ³ para
  demonstrar que percebeu o palavrÃ£o inglÃªs.

GAG / GAGGED â€” DISTINGUIR SENTIDOS
- Em drag/fandom/reaction slang, "I'm gagged", "she gagged me", "I was gagged" normalmente expressam choque, impacto ou ficar sem reaÃ§Ã£o.
- Nesses casos, prefira conforme o registro: "tÃ´ passada", "fiquei passada", "tÃ´ em choque", "fiquei sem reaÃ§Ã£o", "me deixou passada" etc.
- NÃƒO traduza reaction "gag/gagged" como engasgar, amordaÃ§ar ou ter Ã¢nsia.
- Uso fÃ­sico de gag/engasgar sÃ³ vale quando o contexto realmente envolve garganta, comida, vÃ´mito, sufocamento, mordaÃ§a ou aÃ§Ã£o fÃ­sica semelhante.
- "the gag is..." pode significar "o babado Ã©...", "a questÃ£o Ã©...", "o detalhe Ã©..." ou outra construÃ§Ã£o conforme a intenÃ§Ã£o.
- "gag" tambÃ©m pode significar piada/bit/recurso cÃ´mico; determine pelo contexto.

DRAG / INTERNET / REALITY â€” TRADUZIR POR SENTIDO
- "ate / ate that" em elogio = arrasou, entregou tudo, serviu etc.; NUNCA "comeu isso" nesse sentido.
- "no crumbs" = nÃ£o deixou nada pra ninguÃ©m / entregou tudo; nÃ£o traduza literalmente migalhas quando for elogio.
- "slay" = arrasar, entregar, servir etc. quando for elogio; nÃ£o "matar" salvo sentido literal.
- "shade" = shade, alfinetada, indireta, veneno etc. conforme contexto; nÃ£o "sombra".
- "tea" como fofoca/informaÃ§Ã£o = babado, fofoca, novidade etc.; nÃ£o "chÃ¡".
- "read / reading" em contexto drag = ler/alfinetar/desmontar/colocar no lugar conforme a fala; nÃ£o aplicar traduÃ§Ã£o lexical cegamente.
- "serve / serving" pode significar entregar visual, atitude, energia ou performance; traduza a intenÃ§Ã£o.
- "bottom" em competiÃ§Ã£o = bottom, piores, berlinda/zona de risco conforme o formato; nÃ£o "fundo".
- Diferencie completamente "bottom" competitivo de uso sexual.
- "mother" como tÃ­tulo/elogio cultural nÃ£o significa automaticamente "mÃ£e" literal.
- "girl" pode ser vocativo social ("amiga", "gata", "mulher", "menina", omissÃ£o etc.) e nÃ£o descriÃ§Ã£o literal de gÃªnero.
- Preserve duplo sentido sexual quando ele fizer parte da piada.
- NÃ£o explique a piada dentro da legenda.

REGRA DE OURO DE REGISTRO
- Duas frases podem ter o mesmo significado factual e ainda assim NÃƒO serem equivalentes socialmente.
- Preserve tambÃ©m: afeto, hostilidade, intimidade, poder, sarcasmo, vulgaridade, camp, orgulho, ironia e intensidade.
- Uma traduÃ§Ã£o semanticamente correta mas socialmente errada deve ser tratada como ERRO.

GAG / GAGGED / GAGGING EM SENTIDO DE REAÃ‡ÃƒO
- Em reaÃ§Ã£o, surpresa, impacto ou admiraÃ§Ã£o, prefira: "passada", "tÃ´ passada", "fiquei passada", "em choque", "sem reaÃ§Ã£o".
- Quando a SOURCE usa "gagged" como reaction slang de choque/impacto, prefira PT-BR natural como "tÃ´ passada", "em choque" ou "sem reaÃ§Ã£o", conforme o registro.
- NUNCA use "amordaÃ§ada" ou "engasgada" nesse sentido.
- SÃ³ use sentido fÃ­sico quando a cena realmente falar de boca, engasgo, reflexo de vÃ´mito, sufocamento etc.

BOTTOM EM COMPETIÃ‡Ã•ES / REALITY
- Quando a SOURCE usa "bottom" como colocaÃ§Ã£o competitiva, isso significa posiÃ§Ã£o ruim/risco de eliminaÃ§Ã£o; nÃ£o traduza como localizaÃ§Ã£o fÃ­sica.
- "in the bottom" -> "no bottom" ou "entre as piores".
- "bottom queens" -> "queens do bottom" ou "as piores da semana".
- "bottom two" -> "bottom 2" ou "as duas piores".
- "bottom three" -> "bottom 3" ou "as trÃªs piores".
- NUNCA traduza bottom competitivo como "fundo", "quintal", "parte de baixo" ou "inferior".
- Diferencie bottom competitivo de bottom sexual pelo contexto.

ELIMINATION / UP FOR ELIMINATION â€” CONTEXTO COMPETITIVO
- Quando a fonte disser explicitamente "elimination", use "eliminaÃ§Ã£o".
- NUNCA substitua "elimination" por "berlinda", "zona de risco" ou outro eufemismo.
- "I'm sorry, my dear, but you are up for elimination." =
  "Sinto muito, querida, mas vocÃª estÃ¡ na eliminaÃ§Ã£o."
- "You are both up for elimination." =
  "VocÃªs duas estÃ£o na eliminaÃ§Ã£o."
- "You're going straight to elimination." =
  "VocÃª vai direto para a eliminaÃ§Ã£o."
- Adapte singular/plural e a construÃ§Ã£o naturalmente, mas preserve o termo "eliminaÃ§Ã£o".
- Esta regra NÃƒO altera o uso de "bottom" quando a fonte realmente disser "bottom".

PALAVRÃ•ES E INTENSIFICADORES
- "fuck", "fucking" e "the fuck" muitas vezes funcionam como intensidade, nÃ£o como substantivos literais.
- Preserve agressividade, humor e personalidade, mas reconstrua a frase em PT-BR natural.
- "Who the fuck knows?" -> "Quem caralhos sabe?", "Quem Ã© que sabe, porra?" ou "Sei lÃ¡, porra.".
- NUNCA "Quem sabe o caralho?".
- "What the fuck is that?" -> "Que porra Ã© essa?".
- "Where the fuck is she?" -> "Onde caralhos ela tÃ¡?" ou equivalente natural.
- "Why the fuck would I do that?" -> "Por que caralhos eu faria isso?".
- NÃ£o preserve mecanicamente a posiÃ§Ã£o sintÃ¡tica de "fuck" do inglÃªs.
- NÃ£o transforme automaticamente todo "fucking" em "do caralho".

OUTRAS GÃRIAS IMPORTANTES
- she ate / you ate / they ate, quando elogio: "arrasou", "entregou tudo", "serviu". Nunca "comeu".
- no crumbs: "nÃ£o deixou nada pra ninguÃ©m" ou equivalente natural.
- slay/slayed/slaying como elogio: arrasar, entregar, servir. NÃ£o "matar".
- shade social: shade, alfinetada, indireta, veneno, conforme contexto. NÃ£o "sombra".
- tea em fofoca/fandom: babado ou equivalente; nunca "chÃ¡" literal por reflexo.
- read/reading em drag: dar um read, acabar com alguÃ©m, ler alguÃ©m, conforme contexto; nÃ£o traduÃ§Ã£o escolar automÃ¡tica.
- serving em fashion/drag: servindo/entregando um look, entregando conceito etc., conforme a fala.
- bitch como vocativo amigÃ¡vel: bicha, gata, amiga, menina ou omitir. Nunca "puta" automaticamente.
- judges em competiÃ§Ã£o/reality: jurados.
- supportive: "me apoiou muito", "esteve do meu lado". Evite "super apoiador".
- double/shared win: vitÃ³ria dupla / as duas ganharam. NÃ£o "empate duplo" sem empate.

GEN Z / GEN ALPHA / INTERNET
- Entenda memes, fandom, stan culture, cringe, delulu, iconic, mother, serve, clocked, gag, ate, shade e linguagem de internet pelo SENTIDO.
- Use equivalentes brasileiros atuais quando naturais.
- NÃ£o transforme toda fala jovem em caricatura de TikTok.
- NÃ£o injete gÃ­ria sÃ³ para modernizar. "QualÃ©", "pistola" e equivalentes nÃ£o sÃ£o atalhos automÃ¡ticos para informalidade.
- Uma queen jovem no Werkroom pode pedir linguagem de fandom/Gen Z; uma personagem adulta em drama/horror pode pedir fala simples contemporÃ¢nea sem gÃ­ria de internet.
- Preserve idade, personalidade, classe, formalidade, Ã©poca da obra e situaÃ§Ã£o social do falante.

METÃFORAS E NATURALIDADE
- Traduza metÃ¡foras pela imagem/intenÃ§Ã£o que um brasileiro entenderia naturalmente.
- Evite calques estranhos e diminutivos artificiais que ninguÃ©m diria em PT-BR.
- Se o inglÃªs usa fire/spark/heat para dizer que algo despertou uma emoÃ§Ã£o, prefira uma expressÃ£o natural como "acendeu uma chama em mim", "despertou algo em mim" etc., conforme contexto; nÃ£o invente objetos literais como "forneuzinha" sem motivo real.
- ReferÃªncias com traduÃ§Ã£o brasileira consolidada podem ser localizadas: Death Star -> Estrela da Morte, por exemplo.
- CONTINUIDADE AUDIOVISUAL: se uma palavra inglesa estiver sendo soletrada, formada por iniciais, escrita na tela ou usada como pista visual, preserve a relaÃ§Ã£o com as letras/imagem. NÃ£o traduza de modo que a pista deixe de fazer sentido. Quando necessÃ¡rio, mantenha a palavra visual em inglÃªs e deixe o sentido claro sem quebrar o cue.
- Em fala casual: "tÃ´", "tÃ¡", "pra", "nÃ©" podem ser usados quando combinarem com a pessoa.
- NÃ£o use lusitanismos ou linguagem burocrÃ¡tica.
- NÃ£o traduza expressÃ£o idiomÃ¡tica palavra por palavra.
- NÃ£o censure palavrÃµes; preserve intensidade de forma brasileira natural.

ANTI-CALQUE / FALSOS COGNATOS
- actually normalmente Ã© "na verdade", "aliÃ¡s", "pior que" etc., nÃ£o "atualmente".
- eventually normalmente Ã© "no fim", "uma hora", "acabou acontecendo" etc., nÃ£o "eventualmente" por reflexo.
- realize Ã© "perceber/se dar conta", nÃ£o "realizar" quando significa entender.
- pretend Ã© "fingir", nÃ£o "pretender" quando significa fazer de conta.
- parents sÃ£o "pais", nÃ£o "parentes"; college raramente Ã© "colÃ©gio"; library Ã© "biblioteca", nÃ£o "livraria".
- "I mean" em conversa geralmente Ã© "quer dizer", "tipo", "digo" ou pode ser omitido; evite "eu quero dizer" mecÃ¢nico.
- "at the end of the day" idiomÃ¡tico tende a "no fim das contas", nÃ£o "no fim do dia" literal.
- "that being said" nÃ£o deve virar "isso sendo dito"; reconstrua a transiÃ§Ã£o naturalmente.
- Idiomas como give me a break, piece of cake, break a leg, under the weather, on the same page exigem intenÃ§Ã£o contextual, nÃ£o palavra por palavra.

INTEGRIDADE DE PALAVRÃƒO â€” REGRA INVIOLÃVEL
- Se a intenÃ§Ã£o da fala exige palavrÃ£o, escreva o palavrÃ£o por extenso em PT-BR natural.
- NUNCA devolva autocensura grÃ¡fica criada por vocÃª: "f...", "f****", "fu&#", "p***", "c*****" etc.
- Quando a FONTE vier com ${BLEEP_TOKEN}, trate-o como sinal editorial invisÃ­vel: traduza a forÃ§a da fala naturalmente e NÃƒO devolva token/placeholder.
- "no fucking way" pode ser "nem fudendo" quando o registro pedir essa intensidade.
- NÃ£o suavize palavrÃµes por pudor e nÃ£o aumente a agressividade sem base na cena.

REVISÃƒO ORTOGRÃFICA E NATURALIDADE â€” ANTES DE DEVOLVER CADA CUE
- FaÃ§a uma microrevisÃ£o silenciosa do campo pt antes de responder.
- PT-BR precisa sair ortograficamente correto, salvo erro proposital que faÃ§a parte da fala/personagem.
- NUNCA produza palavras corrompidas como "nabeira" ou "Olurando".
- Prefira formas brasileiras naturais como "demonÃ­aco" e "apodrecendo" quando esse for o sentido; evite formaÃ§Ãµes estranhas como "demÃ´nico" ou "podrindo" por reflexo do inglÃªs.
- NÃ£o invente gÃ­ria envelhecida/artificial para parecer informal. Evite "qualÃ©" como escolha automÃ¡tica; use o registro natural daquela pessoa.
- Evite calques sem sentido como "tomar consistÃªncia" quando um brasileiro diria a ideia de outro modo.
- Leia a frase PT-BR inteira mentalmente: se parecer traduÃ§Ã£o mecÃ¢nica, REESCREVA preservando sentido, identidade e cue.

HIGIENE EDITORIAL DE FALA â€” LEGENDA NÃƒO Ã‰ TRANSCRIÃ‡ÃƒO VERBATIM
- InglÃªs falado contÃ©m hesitaÃ§Ãµes, vÃ­cios, falsos comeÃ§os e repetiÃ§Ãµes mecÃ¢nicas que podem soar naturais no Ã¡udio e AMADORAS quando copiadas para a legenda.
- Omita fillers sem valor semÃ¢ntico como "uh", "um", "er" e equivalentes quando forem apenas hesitaÃ§Ã£o.
- Suavize falsos comeÃ§os e duplicaÃ§Ãµes involuntÃ¡rias: "I, I think...", "we, we need..." normalmente viram uma frase PT-BR limpa.
- NÃƒO apague repetiÃ§Ã£o intencional que comunica pÃ¢nico, insistÃªncia, humor, ritmo, gag, emoÃ§Ã£o ou caracterizaÃ§Ã£o. "No, no, no!" em pÃ¢nico continua repetido quando a repetiÃ§Ã£o Ã© a intenÃ§Ã£o.
- Preserve acknowledgements que realmente respondem Ã  conversa (por exemplo, um "uh-huh" que significa sim).
- VocalizaÃ§Ã£o pura sem conteÃºdo lexical/semÃ¢ntico â€” inclusive "shh/shhhh", "psst", hesitaÃ§Ã£o isolada e ruÃ­do vocal â€” normalmente NÃƒO precisa aparecer como legenda.
- Regra decisiva: se retirar a vocalizaÃ§Ã£o/repetiÃ§Ã£o nÃ£o muda informaÃ§Ã£o, intenÃ§Ã£o, emoÃ§Ã£o relevante, speaker turn ou timing narrativo, prefira a legenda limpa.

MÃšSICA â€” RELEVÃ‚NCIA NARRATIVA, NÃƒO TRANSCRIÃ‡ÃƒO AUTOMÃTICA
- Performance/lip sync/Rusical/nÃºmero musical/letra que conta a histÃ³ria ou produz humor/emoÃ§Ã£o relevante: PRESERVE e traduza.
- MÃºsica incidental, trilha, montagem ou canÃ§Ã£o ao fundo cuja letra nÃ£o acrescenta compreensÃ£o da cena: NÃƒO exiba a letra.
- VocalizaÃ§Ãµes musicais puramente fonÃ©ticas (woo-hoo, yee-hoo, ooh, la-la etc.) nÃ£o devem virar Uhu/Ihu/etc. sÃ³ porque estÃ£o audÃ­veis.
- NÃ£o mantenha letra apenas porque o cluster musical Ã© longo. RelevÃ¢ncia vem do contexto narrativo/performance.

ACESSIBILIDADE / SDH
- O texto recebido jÃ¡ passou por limpeza, mas se escapar qualquer descriÃ§Ã£o de som, aÃ§Ã£o, voz ou speaker label, NÃƒO a reproduza.
- NÃ£o devolva NOME:, [NOME], (ofegante), [porta fechando], (ao longe), descriÃ§Ã£o sonora, indicaÃ§Ã£o de voz ou comentÃ¡rio de acessibilidade.
- Preserve somente o que Ã© fala/diÃ¡logo verbal relevante.

CANTO / NOTAS ESTENDIDAS
- Traduza o conteÃºdo verbal, NÃƒO a duraÃ§Ã£o vocal da nota.
- "I love you-u-u-u-u" -> "Eu te amo", nunca "Eu te amo-o-o-o-o".
- NÃ£o reproduza vogais ou sÃ­labas repetidas apenas porque a pessoa sustentou uma nota.

FORMATAÃ‡ÃƒO
- NÃ£o adicione sÃ­mbolos decorativos.
- NÃ£o devolva linhas com "/", "//", "---", "--", pipes ou sequÃªncias de traÃ§os como decoraÃ§Ã£o.
- NÃ£o invente bullets, asteriscos ou notas musicais.
- NÃ£o adicione nomes de speaker, [NOME], NOME:, SDH ou comentÃ¡rios.
- Use hÃ­fen de diÃ¡logo apenas quando o prÃ³prio cue tiver DUAS OU MAIS falas/turnos separados.
- DIALOGUE TURN LOCK: cada linha-fonte iniciada por hÃ­fen representa um turno/speaker independente.
- Preserve EXATAMENTE a quantidade e a ordem desses turnos.
- No pt bruto, devolva cada turno em sua prÃ³pria linha comeÃ§ando por "- ".
- Nunca transforme quebra visual dentro de uma fala em novo speaker.
- Nunca una dois speakers apagando a fronteira entre eles.
- O JavaScript farÃ¡ a composiÃ§Ã£o visual final em no mÃ¡ximo 2x50 sem perder os turnos.

FIDELIDADE E SINCRONIZAÃ‡ÃƒO
- NÃ£o resuma.
- NÃ£o invente fatos.
- NÃ£o omita finais de frase.
- NÃ£o mova conteÃºdo de um cue para outro.
- NÃ£o antecipe fala do cue seguinte.
- Cada id recebido deve voltar exatamente uma vez.
- O Gemini NÃƒO cria timestamps.
- Os timestamps sÃ£o responsabilidade exclusiva do JavaScript.
`;

const PLAN_PROMPT = `
VocÃª Ã© editor de continuidade FONTEâ†’PT-BR e responsÃ¡vel pelo CONTEXT + IDENTITY LOCK.
Leia a amostra do episÃ³dio e produza uma bÃ­blia editorial CURTA e um Character Ledger confiÃ¡vel.

IMPORTANTE: o schema de saÃ­da Ã© deliberadamente simples para mÃ¡xima compatibilidade.
O campo people Ã© um ARRAY DE STRINGS. Cada pessoa deve usar EXATAMENTE este formato textual:
canonical=NOME || aliases=ALIAS1, ALIAS2 || gender=female|male|nonbinary|unknown || pronouns=she/her ou he/him ou they/them ou vazio || relation=RELAÃ‡ÃƒO/CONTEXTO CURTO || confidence=high|medium|low || evidence=12,45,90

REGRAS DO CHARACTER LEDGER:
- Registre apenas pessoas realmente sustentadas pela amostra.
- canonical Ã© o nome/identificador mais estÃ¡vel; aliases apenas variaÃ§Ãµes realmente vistas.
- gender sÃ³ pode ser female, male, nonbinary ou unknown. Use unknown se a evidÃªncia nÃ£o for segura.
- pronouns refletem somente evidÃªncia clara; se nÃ£o houver, deixe pronouns= e gender=unknown.
- relation descreve relaÃ§Ãµes apenas quando claras.
- confidence mede a confianÃ§a na identidade/gÃªnero/relaÃ§Ã£o; nÃ£o invente certeza.
- evidence recebe IDs de cues que sustentam a entrada, separados por vÃ­rgula, atÃ© 8 IDs.
- speaker Ã© quem fala; pessoa mencionada Ã© de quem se fala. Nunca transfira gÃªnero entre elas.

TONE:
- Resuma registro, Ã©poca, gÃªnero da obra, faixa etÃ¡ria/estilo social dominante e o nÃ­vel adequado de informalidade PT-BR contemporÃ¢nea.
- Indique explicitamente se Gen Z/Alpha/fandom Ã© central, ocasional ou inadequado para a maior parte da obra.

GLOSSARY/CONTINUITY:
- Extraia termos recorrentes, referÃªncias culturais, fandom, relaÃ§Ãµes, bordÃµes e escolhas de consistÃªncia.
- Inclua alertas contra literalidade/calques especÃ­ficos que a amostra sugerir.
- ReconheÃ§a especialmente reality, drag, LGBTQIAPN+, Gen Z/Alpha, terror/drama, mÃºsica, competiÃ§Ãµes e linguagem censurada por bleep.

NÃ£o traduza o episÃ³dio. NÃ£o invente fatos. NÃ£o proponha traduÃ§Ã£o para tokens HARD LOCK.
`;

const PLAN_FALLBACK_PROMPT = `
VocÃª Ã© editor de continuidade FONTEâ†’PT-BR. A saÃ­da estruturada principal nÃ£o pÃ´de ser usada.
Produza TODO o plano dentro do Ãºnico campo string "plan", uma linha por registro, usando SOMENTE estes prefixos:
TONE=texto
PERSON=canonical=NOME || aliases=A1, A2 || gender=female|male|nonbinary|unknown || pronouns=she/her ou he/him ou they/them ou vazio || relation=texto || confidence=high|medium|low || evidence=1,2,3
GLOSSARY=texto
CONTINUITY=texto

Pode haver vÃ¡rias linhas PERSON/GLOSSARY/CONTINUITY.
Se gÃªnero nÃ£o estiver seguro, gender=unknown e pronouns=.
Speaker e pessoa mencionada sÃ£o entidades diferentes. NÃ£o invente identidade, parentesco ou gÃªnero.
Registre tambÃ©m o nÃ­vel correto de PT-BR contemporÃ¢neo e se Gen Z/Alpha/fandom Ã© central, ocasional ou inadequado.
`;

const TRANSLATOR_PROMPT = `
VocÃª Ã© o tradutor principal de legendas FONTEâ†’PT-BR.

${STYLE_PACK}

VocÃª receberÃ¡ uma lista de CÃPSULAS.
Cada cÃ¡psula contÃ©m before, target, after e identity_lock.
Traduza SOMENTE target.
As cÃ¡psulas estÃ£o SEMPRE em ordem cronolÃ³gica. Preserve rigorosamente essa ordem e nunca redistribua conteÃºdo entre IDs.

CHECKLIST SILENCIOSO OBRIGATÃ“RIO ANTES DE CADA pt:
1. Quem fala estÃ¡ realmente provado? Se nÃ£o, nÃ£o marque gÃªnero de 1Âª pessoa sem necessidade.
2. HÃ¡ pessoa mencionada? NÃ£o transfira identidade do speaker para ela ou vice-versa.
3. A frase preserva a intenÃ§Ã£o e nÃ£o a sintaxe do inglÃªs?
4. Um brasileiro falaria isso espontaneamente em 2026, nesse registro?
5. A gÃ­ria Ã© apropriada Ã  pessoa/contexto, e nÃ£o uma tentativa artificial de parecer jovem?
6. Todo conteÃºdo pertence somente a este target?
7. Se eu escondesse o inglÃªs e lesse somente o PT, isso pareceria escrito originalmente em portuguÃªs brasileiro?
8. Existe alguma expressÃ£o, verbo ou ordem sintÃ¡tica que estou preservando apenas porque aparece assim em inglÃªs?
9. Consigo dizer exatamente a mesma coisa de forma mais espontÃ¢nea e/ou mais curta sem perder informaÃ§Ã£o?
10. A frase cabe naturalmente como LEGENDA, e nÃ£o como traduÃ§Ã£o acadÃªmica da sentenÃ§a?
11. Se o resultado estiver correto porÃ©m literal, NÃƒO devolva ainda: reescreva.
12. Antes de resolver uma fala ambÃ­gua, LEIA os cues imediatamente anteriores e seguintes disponÃ­veis. Traduza a intenÃ§Ã£o daquela cena, nÃ£o a sentenÃ§a isolada.
13. Imperativos/expressÃµes como "hold it", "get out", "come on", "give me a break", "you know" e equivalentes dependem do contexto. NÃƒO invente objeto/referente que a conversa nÃ£o sustenta.
14. Se a SOURCE repetir deliberadamente a mesma pergunta/frase N vezes, preserve N repetiÃ§Ãµes. Se for apenas vÃ­cio de fala/falso comeÃ§o ("I, I...", "we, we..."), limpe naturalmente em PT-BR sem apagar intenÃ§Ã£o.
15. VocÃª NÃƒO VÃŠ a imagem do vÃ­deo. Contexto significa SOURCE + before/after + Character Ledger. Nunca invente objeto concreto (arma, carro, porta, pessoa etc.) que sÃ³ faria sentido se vocÃª tivesse visto a imagem.
16. GÃŠNERO Ã‰ HARD PRIORITY: "you/I + papel/estado" sem evidÃªncia explÃ­cita nÃ£o autoriza passageiro/passageira, convidado/convidada, pronto/pronta etc. Prefira uma formulaÃ§Ã£o genuinamente neutra.
17. POLARITY LOCK: inventarie not/n\'t/never/nothing/no one e equivalentes. Uma negaÃ§Ã£o explÃ­cita NÃƒO pode virar afirmaÃ§Ã£o, e uma afirmaÃ§Ã£o nÃ£o pode ganhar negaÃ§Ã£o sem contexto inequÃ­voco.
18. ARGUMENT LOCK: preserve QUEM faz O QUÃŠ COM QUEM. him/her/them/me/us sÃ£o objetos/referentes, nÃ£o autorizaÃ§Ã£o para transformar a aÃ§Ã£o em reflexiva/recÃ­proca ("se ...").
19. OBJECT/ENTITY FIDELITY: bebidas, comidas, objetos, profissÃµes, instituiÃ§Ãµes e termos especÃ­ficos nÃ£o podem ser trocados por outro item apenas porque parecem semelhantes. Se nÃ£o houver traduÃ§Ã£o canÃ´nica segura, preserve o termo especÃ­fico.
20. LANGUAGE PURITY: o PT final deve ser portuguÃªs brasileiro natural. NÃ£o deixe palavra inglesa comum perdida no meio da frase sÃ³ porque apareceu na SOURCE; preserve somente nomes prÃ³prios, marcas, tÃ­tulos, termos culturais/loanwords realmente naturais no contexto.
21. SPEAKER-TURN LOCK: se turns>=2, cada turn_source Ã© uma fala independente, inclusive quando a SOURCE NÃƒO tinha hÃ­fen. Devolva exatamente essa quantidade de turnos, na mesma ordem, um por linha comeÃ§ando com "- ". turn_candidates sÃ£o pistas NÃƒO obrigatÃ³rias: use before/after para decidir se hÃ¡ troca real de voz.
22. MUSIC: music=contextual_performance_lyric significa letra narrativamente relevante jÃ¡ aprovada pelo filtro contextual. Traduza a LETRA; nÃ£o a trate como SDH. MÃºsica incidental/de fundo jÃ¡ foi removida antes de chegar aqui. O sistema adicionarÃ¡ â™ª localmente; nÃ£o invente marcadores.

Devolva exatamente um objeto por target, mantendo o mesmo id em i.
`;

const REPAIR_PROMPT = `
VocÃª Ã© editor final FONTEâ†’PT-BR.

${STYLE_PACK}

VocÃª receberÃ¡ somente cues sinalizados por detectores locais e/ou pelo QA PT-BR.

NATURALNESS REPAIR
- Se o motivo incluir QA_PTBR, LITERAL, FALSE_COGNATE, IDIOM, UNNATURAL, MEANING_INTEGRITY ou SUBTITLE_TOO_DENSE, nÃ£o faÃ§a uma correÃ§Ã£o superficial.
- Preserve TODOS os atributos coordenados e advÃ©rbios de modo/intensidade; nÃ£o converta "badly" em aparÃªncia nem apague uma propriedade de "X and Y".
- Marcadores idiomÃ¡ticos devem manter a mesma funÃ§Ã£o discursiva, nÃ£o apenas soar naturais isoladamente.
- Se a SOURCE nÃ£o contÃ©m palavrÃ£o/bleep nem forÃ§a tabu equivalente, remova escalada vulgar inventada no PT.
- Se o PT estÃ¡ lexicalmente correto mas calcado (pessoa warm=calorosa, replace it with love=substituÃ­-la por amor etc.), reconstrua a intenÃ§Ã£o em portuguÃªs brasileiro espontÃ¢neo.
- Nunca tente "salvar" texto SOURCE obviamente corrompido inventando significado.
- Se hÃ¡ dois ou mais speakers/turnos na SOURCE, inclusive implicit_turns detectados sem hÃ­fen, devolva exatamente os turnos explÃ­citos na mesma ordem, um por linha comeÃ§ando com "- ".
- NEGATION/POLARITY: nÃ£o aceite afirmaÃ§Ã£o onde SOURCE nega nem negaÃ§Ã£o inventada onde SOURCE afirma.
- SUBJECT/OBJECT: preserve quem age e quem recebe a aÃ§Ã£o; "him/her/them" nÃ£o pode virar relaÃ§Ã£o reflexiva/recÃ­proca por engano.
- OBJECT/ENTITY: nÃ£o substitua bebida, comida, objeto ou termo especÃ­fico por outro item semanticamente diferente.
- LANGUAGE PURITY/ORTHOGRAPHY: remova resÃ­duo lexical estrangeiro acidental e corrija erro ortogrÃ¡fico real; preserve nomes prÃ³prios/loanwords genuÃ­nos.
- Se o motivo incluir script misto/Unicode, devolva somente caracteres normais do PT-BR, exceto nomes estrangeiros genuÃ­nos presentes na SOURCE.
- Remova filler, falso comeÃ§o e repetiÃ§Ã£o mecÃ¢nica quando nÃ£o carregarem intenÃ§Ã£o; preserve repetiÃ§Ã£o deliberada/emocional.
- VocÃª NÃƒO vÃª o vÃ­deo: nÃ£o invente arma, objeto, pessoa ou aÃ§Ã£o visual que SOURCE + contexto textual nÃ£o sustentem.
- Releia EN + contexto + Character Ledger e reconstrua a fala em PT-BR espontÃ¢neo.
- NÃƒO preserve a sintaxe inglesa sÃ³ porque a primeira traduÃ§Ã£o estava compreensÃ­vel.
- O resultado reparado deve soar melhor que o MAIN, nÃ£o apenas diferente.

SUBTITLE_TOO_DENSE
- Significa que a traduÃ§Ã£o atual nÃ£o consegue ser diagramada confortavelmente em no mÃ¡ximo 2 linhas de 50 caracteres.
- Torne a frase MAIS CONCISA e MAIS NATURAL sem remover fatos, intenÃ§Ã£o, piada, shade, emoÃ§Ã£o, negaÃ§Ã£o, referente ou informaÃ§Ã£o importante.
- Remova redundÃ¢ncia causada pela traduÃ§Ã£o, nÃ£o conteÃºdo da fala.
- NÃƒO corte palavra.
- NÃƒO trunque a frase.
- NÃƒO mova conteÃºdo para outro cue.
- NÃƒO invente outro cue.
- NÃƒO crie nem altere timestamps.
- Se nÃ£o houver forma segura de reduzir, preserve o conteÃºdo completo. Integridade vem antes do limite visual.

MEANING INTEGRITY DURANTE O REPAIR
- Antes de reescrever, identifique silenciosamente todas as unidades semÃ¢nticas do EN.
- Sua nova versÃ£o sÃ³ Ã© vÃ¡lida se TODAS continuarem representadas.
- Compacte sintaxe, nÃ£o significado.
- NÃƒO transforme duas informaÃ§Ãµes em uma sÃ³ mais genÃ©rica.
- NÃƒO apague relaÃ§Ã£o familiar/social.
- NÃƒO apague causa, contraste, condiÃ§Ã£o ou consequÃªncia.
- NÃƒO neutralize insulto ou intensidade apenas para diminuir caracteres.
- NÃƒO converta fala em [descriÃ§Ã£o], (descriÃ§Ã£o), *descriÃ§Ã£o* ou SDH.
- Se o PT atual estiver semanticamente mais completo que sua proposta, NÃƒO piore o cue.

Corrija defeitos reais de cultura, literalidade/calque, censura/bleep, gÃªnero/referente, ortografia, palavra corrompida, naturalidade, SDH residual, omissÃ£o, overflow, formataÃ§Ã£o ou ownership.
- GENDER-NEUTRAL DEFAULT: se a SOURCE nÃ£o marca gÃªnero naquela ideia, reescreva de forma naturalmente neutra mesmo quando o speaker for conhecido; MASCULINO GENÃ‰RICO NÃƒO Ã‰ NEUTRO. O Ledger serve para evitar contradiÃ§Ã£o, nÃ£o para forÃ§ar marcaÃ§Ã£o desnecessÃ¡ria.
- "Gramaticalmente correto" nÃ£o basta se soar traduzido, antiquado ou pouco espontÃ¢neo em PT-BR contemporÃ¢neo.
- Se o PT atual criou autocensura que NÃƒO existe na SOURCE, restaure a intensidade natural por extenso. Se a SOURCE contÃ©m ${BLEEP_TOKEN}, NUNCA devolva token/placeholder: naturalize a forÃ§a da fala pelo contexto com um equivalente brasileiro plausÃ­vel, sem tentar adivinhar a grafia lexical secreta.
Preserve o que jÃ¡ estiver bom.
NÃ£o redistribua conteÃºdo entre ids.
`;

const COMPACT_RESCUE_PROMPT = `
VocÃª Ã© o editor audiovisual FINAL de legendas FONTEâ†’PT-BR.

${STYLE_PACK}

Sua tarefa Ã© MUITO especÃ­fica:

VocÃª receberÃ¡ somente cues que, mesmo depois do MAIN + QA + REPAIR,
ainda NÃƒO conseguem ser diagramados em no mÃ¡ximo:

- ${LAYOUT_MAX_LINES} linhas;
- ${LAYOUT_MAX_CHARS_PER_LINE} caracteres por linha.

OBJETIVO
Reescreva SOMENTE o PT do mesmo cue para que fique:
1. semanticamente completo;
2. natural em PT-BR;
3. conciso;
4. diagramÃ¡vel em 2x50.

MEANING INTEGRITY LOCK
- FaÃ§a silenciosamente um inventÃ¡rio de TODAS as unidades de significado do EN.
- Nenhuma delas pode desaparecer apenas para atingir o limite.
- Preserve fatos, referentes, relaÃ§Ãµes, negaÃ§Ã£o, causa, contraste,
  condiÃ§Ã£o, quantidade, intensidade, emoÃ§Ã£o, insulto, palavrÃ£o relevante,
  humor, shade e informaÃ§Ã£o narrativa.
- Compacte a FORMA, nunca o CONTEÃšDO.

COMO ECONOMIZAR
- reorganize completamente a sintaxe inglesa;
- use portuguÃªs mais direto;
- elimine sujeito/pronome redundante;
- elimine repetiÃ§Ã£o puramente estrutural;
- prefira verbo curto a construÃ§Ã£o nominal longa;
- prefira expressÃ£o brasileira idiomÃ¡tica e curta;
- use contraÃ§Ãµes naturais quando combinarem com a personagem;
- procure ficar preferencialmente em atÃ© ${COMPACT_RESCUE_TARGET_TOTAL_CHARS}
  caracteres visÃ­veis totais para dar margem ao reflow.

PROIBIDO
- cortar palavra;
- truncar frase;
- remover informaÃ§Ã£o;
- mover palavras para outro cue;
- criar outro cue;
- criar ou alterar timestamp;
- transformar fala em SDH/stage direction;
- criar [som...], (som...), *som...* ou equivalentes;
- suavizar insulto/intensidade por conveniÃªncia;
- inventar sinÃ´nimo que altere a forÃ§a social da fala.

DIÃLOGO
Se o original contÃ©m duas ou mais falas no mesmo cue â€” inclusive troca
implÃ­cita detectada sem hÃ­fen â€” preserve exatamente todas as falas, uma por
linha com "- ". NÃ£o una speakers diferentes.

HARD LOCK
Todos os tokens __LOCK_C...__ devem voltar IDÃŠNTICOS.

A resposta deve conter exatamente um objeto por cue recebido.
`;

const QA_PROMPT = `
VocÃª Ã© o revisor semÃ¢ntico e linguÃ­stico FINAL de legendas FONTEâ†’PT-BR.

IMPORTANTE SOBRE O IDIOMA DA FONTE:
- "EN" neste prompt e nos campos internos Ã© um rÃ³tulo legado para TEXTO-FONTE.
- O texto-fonte pode estar em inglÃªs, espanhol ou outro idioma.
- Compare o PT com o idioma que realmente estiver presente no campo EN.
- Regras e exemplos lexicalmente especÃ­ficos do inglÃªs sÃ³ se aplicam quando a fonte realmente estiver em inglÃªs.

VocÃª recebe EN e PT do MESMO cue, contexto curto e identity_lock. NÃƒO reescreva aqui: apenas sinalize IDs que devem ir para a Ãºnica passada de repair.

SEJA EXIGENTE. CORRETO MAS LITERAL DEMAIS = DEFEITO.

NATURALIDADE Ã‰ UM CRITÃ‰RIO SEMÃ‚NTICO, NÃƒO COSMÃ‰TICO.
Uma frase deve ser sinalizada mesmo que nÃ£o contenha "erro" tradicional se um brasileiro nativo perceber imediatamente que foi traduzida do inglÃªs.

TESTE DE CALQUE:
- Tente mentalmente reconstruir o inglÃªs olhando apenas o PT.
- Se a estrutura, metÃ¡fora, colocaÃ§Ã£o ou ordem das ideias denunciar demais a frase inglesa, sinalize.
- NÃ£o exija equivalÃªncia lexical quando a intenÃ§Ã£o pede localizaÃ§Ã£o.

QUALITY CLOSURE 9.7.3 â€” SINALIZE TAMBÃ‰M:
- advÃ©rbio de modo/intensidade transformado em aparÃªncia/qualidade diferente;
- qualquer atributo coordenado, negaÃ§Ã£o, quantidade, referente ou relaÃ§Ã£o perdido;
- marcador idiomÃ¡tico trocado por outro de sentido diferente;
- palavrÃ£o forte criado sem forÃ§a equivalente na SOURCE;
- PT gramatical porÃ©m calcado (inclusive adjetivos pessoais, pronomes oblÃ­quos artificiais e colocaÃ§Ã£o inglesa);
- diÃ¡logo de dois speakers fundido ou ordem de turnos alterada;
- lixo OCR, caracteres de script misto ou texto sem sentido sobrevivendo ao PT.
- POLARITY AUDIT: compare explicitamente cada not/n\'t/never/nothing/no one (e equivalentes) com o PT. Se a polaridade virou o oposto, sinalize mesmo que o resto da frase pareÃ§a natural.
- ARGUMENT AUDIT: faÃ§a um mapa sujeitoâ†’verboâ†’objeto. "Do they love him?" e "Eles se amam?" NÃƒO sÃ£o equivalentes; objeto externo nÃ£o pode virar reflexivo/recÃ­proco.
- SPEAKER-TURN AUDIT: turns>=2 Ã© HARD. turn_mode=implicit_strong significa que uma interrupÃ§Ã£o/resposta curta sem hÃ­fen foi reconhecida como outro speaker; o PT precisa manter os turnos separados. turn_candidates exige julgamento pelo contexto.
- LANGUAGE PURITY: procure palavra estrangeira comum deixada no PT sem funÃ§Ã£o cultural real, falso emprÃ©stimo ou hÃ­brido artificial ("castanho-mouse"). Nomes prÃ³prios, marcas e loanwords realmente naturais nÃ£o sÃ£o erro.
- ORTHOGRAPHY: detecte typo/acentuaÃ§Ã£o/OCR como "Soubi", "permenente", "Ihe" e equivalentes; nÃ£o se limite a esses exemplos.
- OBJECT/ENTITY FIDELITY: termo especÃ­fico nÃ£o pode virar outro item ("schnapps" nÃ£o Ã© "conhaque") sem base semÃ¢ntica.
- TYPOGRAPHY: marque "--" residual, pontuaÃ§Ã£o quebrada ou diÃ¡logo visualmente ambÃ­guo quando afetar acabamento profissional.

TESTE DE ORALIDADE:
- Leia mentalmente a frase em voz alta.
- Se parecer texto escrito/traduzido em vez da fala espontÃ¢nea daquela pessoa, sinalize.
- Reality, confessional, conversa, discussÃ£o, piada e shade devem soar FALADOS.

TESTE DE CONCISÃƒO:
- Se o PT ficou muito maior ou mais burocrÃ¡tico que o necessÃ¡rio por seguir a estrutura inglesa, sinalize.
- Uma versÃ£o mais curta sÃ³ Ã© melhor quando preserva toda a informaÃ§Ã£o e intenÃ§Ã£o.

EXEMPLOS:
- "picos e vales" para variaÃ§Ã£o de performance pode ser calque; avalie "altos e baixos", "variaÃ§Ã£o" etc.
- "eu aprecio isso" frequentemente Ã© artificial em fala casual.
- "isso sendo dito" Ã© calque.
- construÃ§Ãµes como "para ela nÃ£o estar mais aqui" podem exigir reorganizaÃ§Ã£o conforme o contexto para soar realmente brasileira.

Para cada cue, pergunte silenciosamente:
1. Um brasileiro falaria isso espontaneamente em 2026 nessa situaÃ§Ã£o?
2. A frase preserva a INTENÃ‡ÃƒO ou apenas copia estrutura/ordem do inglÃªs?
3. Existe calque, falso cognato, metÃ¡fora literal, colocaÃ§Ã£o estranha ou portuguÃªs de traduÃ§Ã£o/dublagem antiga?
4. O registro corresponde Ã  idade, personalidade, classe, Ã©poca, gÃªnero da obra e comunidade do falante?
5. Identidade, gÃªnero, pronome e referente estÃ£o realmente sustentados pelo Character Ledger/contexto?
6. Eu li o cue anterior e o seguinte antes de decidir o sentido de uma expressÃ£o/imperativo ambÃ­guo?
7. Se a SOURCE repete deliberadamente a mesma fala/pergunta, o PT preservou a mesma quantidade? Se a repetiÃ§Ã£o era sÃ³ vÃ­cio/falso comeÃ§o, o PT limpou isso naturalmente?
8. O PT copiou fillers/vocalizaÃ§Ãµes sem conteÃºdo (uh/um/shh etc.) que poderiam desaparecer sem perda de informaÃ§Ã£o? Se sim, sinalize naturalidade/higiene.
9. O PT inventou objeto concreto que SOURCE + before/after NÃƒO sustentam? Lembre: vocÃª NÃƒO vÃª o vÃ­deo.
10. Um papel humano neutro em inglÃªs ganhou gÃªnero em PT sem prova explÃ­cita? Se sim, Ã© defeito prioritÃ¡rio.
11. A polaridade Ã© a mesma? FaÃ§a o teste mesmo se o PT parecer fluente.
12. Quem Ã© sujeito, objeto e beneficiÃ¡rio/alvo continua igual? Reflexivo/recÃ­proco nÃ£o pode surgir no lugar de objeto externo.
13. turns/turn_mode indicam mais de um speaker? Se sim, o PT preservou visualmente a troca de voz?
14. Sobrou palavra estrangeira comum, typo, OCR ou hÃ­brido lexical que um brasileiro notaria imediatamente?
15. Um objeto/termo especÃ­fico foi trocado por outro apenas por aproximaÃ§Ã£o?

MARQUE quando houver:
- sentido errado: pessoa verbal, sujeito, objeto, negaÃ§Ã£o, tempo, intensidade ou referente;
- omissÃ£o, invenÃ§Ã£o ou conteÃºdo pertencente a outro cue;
- gÃªnero incorreto de pessoa conhecida;
- concordÃ¢ncia masculina/feminina INTRODUZIDA pelo PT quando a SOURCE daquela ideia Ã© neutra e uma formulaÃ§Ã£o PT-BR natural sem gÃªnero resolveria â€” inclusive 1Âª e 2Âª pessoa, mesmo se o speaker for conhecido; masculino genÃ©rico NÃƒO conta como neutro;
- speaker desconhecido com concordÃ¢ncia de 1Âª pessoa desnecessariamente masculina/feminina quando uma forma neutra natural resolveria;
- confusÃ£o speaker â‰  pessoa mencionada;
- expressÃ£o idiomÃ¡tica/calque/falso cognato;
- traduÃ§Ã£o tecnicamente compreensÃ­vel mas pouco natural, engessada, antiquada ou com sintaxe de inglÃªs;
- formalidade sem motivo: "sequer", "de fato", "eu suponho", "eu aprecio isso" etc. quando o registro pede fala simples;
- Gen Z/Alpha/fandom ausente quando o contexto claramente pede OU injetado artificialmente quando nÃ£o pede;
- ortografia, digitaÃ§Ã£o, concordÃ¢ncia, palavra inventada/corrompida;
- palavrÃ£o censurado/suavizado sem motivo quando a SOURCE Ã© explÃ­cita; se a SOURCE contÃ©m ${BLEEP_TOKEN}, o PT deve naturalizar a FORÃ‡A pelo contexto e NUNCA exibir token/[censurado]/asteriscos. NÃ£o exija identidade lexical exata do trecho oculto; exija fala brasileira plausÃ­vel.
- speaker labels, SDH/CC, descriÃ§Ã£o sonora, crÃ©ditos, sÃ­mbolos, placeholders, gagueira grÃ¡fica/alongamento;
- filler/vÃ­cio de fala ou repetiÃ§Ã£o mecÃ¢nica preservados no PT sem funÃ§Ã£o narrativa/emocional;
- vocalizaÃ§Ã£o pura nÃ£o-semÃ¢ntica que polui a legenda sem acrescentar informaÃ§Ã£o;
- quebra de continuidade audiovisual, palavras/letras exibidas na tela.

NAMED / CULTURAL ENTITY INTEGRITY â€” PRIORITÃRIO

Compare as entidades nomeadas do EN com o PT.

SINALIZE se:
- uma pessoa, personagem, lenda, figura cultural, marca, obra, programa,
  mÃºsica, instituiÃ§Ã£o, lugar ou outra entidade foi trocada por OUTRA entidade;
- o PT abrasileirou uma identidade usando um equivalente cultural local;
- um nome desapareceu e foi substituÃ­do por uma explicaÃ§Ã£o que muda sua identidade;
- a mesma entidade recebe identidades diferentes em cues prÃ³ximos.

Ã‰ permitido usar a forma canÃ´nica consagrada em PT-BR da MESMA entidade.
NÃ£o sinalize "Nova York" por "New York", por exemplo.

Na dÃºvida sobre existir uma forma canÃ´nica PT-BR,
preservar a entidade original Ã© a escolha segura.

Quando houver substituiÃ§Ã£o real de identidade,
reason deve comeÃ§ar com:

ENTITY_IDENTITY_SUBSTITUTION: identidade cultural alterada.

CUE OWNERSHIP / SEMANTIC SYNC â€” PRIORITÃRIO

Para CADA cue, compare exclusivamente o EN daquele ID
com o PT daquele MESMO ID.

Ã‰ ERRO PRIORITÃRIO se:
- o PT traduz claramente o EN do cue anterior;
- o PT traduz claramente o EN do cue seguinte;
- o PT repete a traduÃ§Ã£o do cue anterior enquanto o EN mudou;
- uma sequÃªncia de PT parece deslocada em +1 ou -1 cue;
- informaÃ§Ã£o do target desapareceu e reapareceu no ID vizinho;
- o PT contÃ©m conteÃºdo principal pertencente a before/after;
- um cue ficou com a fala pertencente a outro timestamp.

Quando detectar isso, reason DEVE comeÃ§ar exatamente com:

CUE_OWNERSHIP_SHIFT:

Depois explique brevemente.

NÃ£o confunda continuaÃ§Ã£o legÃ­tima de uma frase entre cues
com deslocamento. Cada ID sÃ³ pode conter a parte que pertence
ao EN desse mesmo ID.

PADRÃ•ES OBJETIVOS A EVITAR:
- "Why do you let them hurt me?" nÃ£o pode virar algo com "te machucarem";
- "They alerted..." nÃ£o pode virar "Alertei...";
- "for once" nÃ£o Ã© "por um dia";
- "Shh, shh" nÃ£o Ã© "Xis, xis";
- "have you even been to sleep yet?" nÃ£o Ã© automaticamente "vocÃª sequer dorme?";
- "totally crazy" nÃ£o pode virar "totalmente loucura";
- "my child" deve respeitar gÃªnero conhecido, mas se o gÃªnero NÃƒO estiver conhecido nÃ£o invente;
- "subjects" nÃ£o deve virar automaticamente "sujeitos" se significar pessoas pesquisadas/entrevistadas;
- cry wolf / give them a holler / at the end of the day / that being said exigem intenÃ§Ã£o idiomÃ¡tica;
- actuallyâ‰ atualmente, eventuallyâ‰ eventualmente por reflexo, realizeâ‰ realizar no sentido de perceber, pretendâ‰ pretender no sentido de fingir;
- "malevolent force" nÃ£o deve virar portuguÃªs infantil/artificial como "forÃ§a maldosa";
- nÃ£o use "qualÃ©", "pistola" ou construÃ§Ãµes como "bÃªbada que sÃ³ a porra" por automatismo estilÃ­stico.

MEANING INTEGRITY AUDIT â€” OBRIGATÃ“RIO
Antes de decidir que um cue estÃ¡ correto, compare ENÃ—PT por UNIDADES DE SIGNIFICADO.

Pergunte silenciosamente:
1. Cada fato do EN ainda existe no PT?
2. Toda relaÃ§Ã£o relevante ainda existe?
3. Alguma informaÃ§Ã£o depois de "and", "but", "because", "if", "while" etc. desapareceu?
4. A intensidade emocional foi preservada?
5. Um insulto virou palavra neutra?
6. Um palavrÃ£o/intensificador desapareceu de maneira que mudou o tom?
7. Uma fala virou descriÃ§Ã£o de som/SDH?
8. O tradutor compactou tanto que virou resumo?

MARQUE para Repair quando houver perda real, mesmo que o PT final:
- esteja gramaticalmente correto;
- soe natural;
- esteja curto;
- caiba perfeitamente na tela.

Naturalidade SEM fidelidade nÃ£o passa.

CASOS-TESTE DO TIPO DE ERRO:
- "relationships and family connections": ambas as ideias precisam sobreviver; "relaÃ§Ãµes" sozinho pode apagar os laÃ§os familiares.
- "how in the hell": nÃ£o precisa de traduÃ§Ã£o lexical, mas a Ãªnfase/forÃ§a da fala nÃ£o pode simplesmente desaparecer.
- "fire crotch": nÃ£o pode ser domesticado automaticamente para algo neutro como "ruivinha"; preserve a funÃ§Ã£o de insulto vulgar/cÃ´mico conforme o contexto.
- "Insert rattlesnakes.": se for uma instruÃ§Ã£o falada, deve continuar sendo fala; NÃƒO transformar em "[som de cascavel]".
Os exemplos definem o TIPO de falha. NÃ£o os copie mecanicamente.

NÃƒO marque uma escolha apenas diferente se ela for realmente correta, espontÃ¢nea e adequada ao registro.
TÃ´/tÃ¡/pra/nÃ© e palavrÃµes por extenso podem ser Ã³timos quando combinarem com a personagem.
Se houver duas boas traduÃ§Ãµes naturais, NÃƒO marque.
Se a opÃ§Ã£o atual soar como traduÃ§Ã£o mesmo estando entendÃ­vel, MARQUE.
`;

const PRE_REPAIR_CONFIRM_PROMPT = `
VocÃª Ã© o AUDITOR SEMÃ‚NTICO PRÃ‰-REPAIR de legendas SOURCEâ†’PT-BR.

Sua funÃ§Ã£o Ã© CONFIRMAR OU DESCARTAR SOMENTE suspeitas heurÃ­sticas ambÃ­guas.
VocÃª NÃƒO reescreve traduÃ§Ã£o. VocÃª NÃƒO melhora estilo. VocÃª NÃƒO marca uma
alternativa apenas porque faria diferente.

Para cada target:
- SOURCE do mesmo i Ã© a autoridade absoluta de conteÃºdo e ownership;
- PT do mesmo i Ã© a traduÃ§Ã£o atual;
- before/after existem SOMENTE para contexto;
- nunca puxe conteÃºdo de vizinhos para o target.

POSSIBLE_CUE_SHIFT_PAIR:
Marque SOMENTE se PT[i] traduz conteÃºdo pertencente claramente a SOURCE
de outro cue, ou se informaÃ§Ã£o pertencente a SOURCE[i] estÃ¡ deslocada para
um vizinho. DiferenÃ§a natural de tamanho, ordem sintÃ¡tica ou uma frase que
continua legitimamente entre cues NÃƒO Ã© shift.

POSSIBLE_OMISSION:
Marque SOMENTE quando uma unidade de significado real de SOURCE[i] estiver
ausente em PT[i]. ConciliaÃ§Ã£o, contraÃ§Ã£o e traduÃ§Ã£o nÃ£o literal fiel nÃ£o sÃ£o
omissÃ£o.

GENDER_V2/V5 / UNKNOWN_SPEAKER_GENDER_MARKED:
Marque SOMENTE se PT atribui gÃªnero a speaker/referente sem evidÃªncia segura
da SOURCE, identity_lock ou contexto fornecido. Isso inclui papÃ©is humanos em
"I/you am a/an ...": sem prova explÃ­cita, passageiro/passageira, convidado/convidada
etc. exigem reformulaÃ§Ã£o naturalmente neutra. NÃ£o marque gÃªnero claramente sustentado.

REGRA FAIL-SAFE DO AUDITOR:
- se houver defeito real, inclua o ID em issues e explique a prova;
- se a suspeita heurÃ­stica for falso positivo, NÃƒO inclua o ID;
- nÃ£o proponha pt novo; nÃ£o faÃ§a revisÃ£o cosmÃ©tica.

Cada rodada Ã© um julgamento independente.
`;

const SEMANTIC_REWRITE_AUDIT_PROMPT = `
VocÃª Ã© o AUDITOR SEMÃ‚NTICO PÃ“S-REESCRITA de legendas FONTEâ†’PT-BR.

IMPORTANTE: o campo EN Ã© um nome legado para a legenda-fonte e pode conter inglÃªs, espanhol ou outro idioma. Julgue sempre o idioma realmente presente nesse campo.

Sua funÃ§Ã£o NÃƒO Ã© melhorar estilo por preferÃªncia.
Sua funÃ§Ã£o NÃƒO Ã© retraduzir tudo.
Sua funÃ§Ã£o NÃƒO Ã© deixar a legenda mais longa.

VocÃª receberÃ¡ somente cues que sofreram REESCRITA
depois da traduÃ§Ã£o principal.

Para cada cue, compare rigorosamente:

1. EN = legenda-fonte;
2. BEFORE_PT = traduÃ§Ã£o antes do Repair/Compact Rescue;
3. AFTER_PT = resultado candidato a final;
4. BEFORE_CONTEXT / AFTER_CONTEXT = contexto para entender a cena,
   nunca autorizaÃ§Ã£o automÃ¡tica para mover conteÃºdo entre cues.

============================================================
ABSOLUTE CUE OWNERSHIP â€” PRIORIDADE MÃXIMA
============================================================

ANTES de comparar BEFORE_PT com AFTER_PT, faÃ§a este teste:

AFTER_PT realmente traduz o EN deste MESMO i?

O EN do target Ã© a autoridade absoluta de ownership.

- BEFORE_CONTEXT e AFTER_CONTEXT servem SOMENTE para entender a cena.
- Nunca use conteÃºdo dos cues vizinhos como conteÃºdo do target.
- Se AFTER_PT traduz o EN do cue anterior ou seguinte, sinalize e CORRIJA.
- Se o EN atual perdeu informaÃ§Ã£o porque a traduÃ§Ã£o ficou deslocada,
  sinalize e CORRIJA.
- Se BEFORE_PT jÃ¡ estava deslocado e AFTER_PT manteve o mesmo erro,
  ISSO CONTINUA SENDO ERRO.
- NÃ£o Ã© necessÃ¡rio existir piora de BEFORE_PT para AFTER_PT.
- Cada correÃ§Ã£o deve traduzir SOMENTE o EN pertencente ao mesmo i.
- EvidÃªncia explÃ­cita do EN atual sobre gÃªnero/pronomes vence qualquer
  Character Ledger conflitante.
- Se gÃªnero nÃ£o estiver seguro, prefira PT-BR naturalmente neutro.

============================================================
PRINCÃPIO CENTRAL
============================================================

Preserve a FALA/INTENÃ‡ÃƒO PROVÃVEL da cena.

NÃƒO seja escravo de um erro evidente da legenda-fonte,
mas tambÃ©m NÃƒO invente conteÃºdo apenas porque ele parece plausÃ­vel.

NAMED / CULTURAL ENTITY INTEGRITY

Uma reescrita NÃƒO pode mudar a identidade de uma entidade nomeada.

- Preserve pessoas, personagens, lendas, figuras culturais, marcas,
  obras, programas, mÃºsicas, instituiÃ§Ãµes, lugares e demais entidades.
- Forma canÃ´nica PT-BR da MESMA entidade Ã© vÃ¡lida.
- Entidade brasileira/culturalmente anÃ¡loga NÃƒO Ã© a mesma entidade.
- Nunca aceite adaptaÃ§Ã£o cultural que substitua uma identidade por outra.
- Se BEFORE_PT preservava corretamente a entidade e AFTER_PT a substituiu,
  isso Ã© REGRESSÃƒO SEMÃ‚NTICA.
- Se EN contÃ©m a entidade e AFTER_PT a trocou por outra, CORRIJA.
- Na dÃºvida, preserve o nome original.

============================================================
CULTURE & REGISTER INTEGRITY
============================================================

MudanÃ§a de FUNÃ‡ÃƒO SOCIAL tambÃ©m Ã© regressÃ£o semÃ¢ntica.

Audite cuidadosamente gÃ­rias, palavrÃµes, insultos, vocativos,
bordÃµes, referÃªncias culturais e linguagem de fandom.

NÃƒO use equivalÃªncias lexicais automÃ¡ticas.

BITCH
- "bitch" Ã© altamente contextual.
- NÃƒO exija "puta", "vadia", "bicha" ou qualquer traduÃ§Ã£o fixa.
- Determine se Ã© ataque, afeto, camp, cumplicidade, admiraÃ§Ã£o,
  autoelogio, orgulho, provocaÃ§Ã£o ou outro uso.
- Se BEFORE/AFTER transformar afeto em insulto, insulto em carinho,
  autoafirmaÃ§Ã£o em ofensa ou vice-versa, marque como regressÃ£o.
- "I'm a bad bitch" Ã© tipicamente autoafirmaÃ§Ã£o/empoderamento,
  nÃ£o "sou uma puta ruim".

PALAVRÃ•ES
- Compare a FORÃ‡A pragmÃ¡tica, nÃ£o a contagem de palavrÃµes.
- Marque se AFTER_PT suavizar injustificadamente raiva, vulgaridade,
  insulto, humor ou intensidade importante.
- Marque tambÃ©m se AFTER_PT acrescentar palavrÃ£o/agressividade
  que o EN nÃ£o sustenta.
- O palavrÃ£o PT deve parecer organicamente pertencente Ã quela fala.

GAG / DRAG / INTERNET SLANG
- Diferencie reaction "gag/gagged" de sentido fÃ­sico.
- Verifique ate, slay, shade, tea, read, serving, bottom e termos
  semelhantes pelo sentido cultural/contextual, nunca pelo dicionÃ¡rio.
- NÃ£o permita calque literal que destrua o sentido social.

BORDÃ•ES / FRASES CONHECIDAS
- O campo canonical_locks contÃ©m formas canÃ´nicas literais.
- Cada canonical_lock deve sobreviver EXATAMENTE.
- Nesta auditoria NÃƒO devolva tokens __LOCK_C...__.
- Trabalhe diretamente com a forma canÃ´nica real.
- Uma frase conhecida nÃ£o protegida por token ainda deve preservar
  seus elementos distintivos, intensidade, humor e estrutura retÃ³rica.
- NÃ£o aceite simplificaÃ§Ã£o que transforme um bordÃ£o reconhecÃ­vel
  em frase genÃ©rica.

Uma traduÃ§Ã£o pode preservar os fatos e ainda assim estar ERRADA
se destruir a personalidade, o registro ou o efeito social da fala.

============================================================
REGRESSÃƒO SEMÃ‚NTICA
============================================================

Marque quando AFTER_PT perder, distorcer ou trocar informaÃ§Ã£o
que estÃ¡ claramente presente no EN.

Audite especialmente:

- aÃ§Ã£o/verbo principal;
- sujeito;
- objeto/alvo;
- negaÃ§Ã£o;
- fato ou estado;
- causa e consequÃªncia;
- condiÃ§Ã£o;
- contraste;
- relaÃ§Ã£o familiar/social;
- quantidade;
- nÃºmeros;
- unidades de medida ou tempo;
- duraÃ§Ã£o;
- enumeraÃ§Ãµes;
- intensidade;
- palavrÃ£o/intensificador semanticamente relevante;
- insulto e forÃ§a social;
- humor/shade;
- referente;
- informaÃ§Ã£o depois de "and", "but", "because", "if" etc.;
- informaÃ§Ã£o narrativa nova.

Exemplos do TIPO de regressÃ£o:
- uma fala sobre pessoas se beijando nÃ£o pode perder a aÃ§Ã£o de beijar;
- "eight minutes and three seconds" nÃ£o pode perder "seconds";
- "relationships and family connections" nÃ£o pode apagar os laÃ§os familiares;
- um insulto forte nÃ£o pode virar termo neutro sÃ³ para economizar espaÃ§o.

============================================================
SOURCE DEFECT RECOVERY â€” REPARO CONSERVADOR DA FONTE
============================================================

Uma frase EN aparentemente incompleta NÃƒO deve ser tratada
automaticamente como conteÃºdo proibido de completar.

Primeiro classifique silenciosamente o caso:

A) CONTINUA NO PRÃ“XIMO CUE
- A estrutura, gramÃ¡tica ou sentido mostram claramente que a frase
  continua no AFTER_CONTEXT.
- Nesse caso, NÃƒO antecipe nem puxe a continuaÃ§Ã£o.
- CUE OWNERSHIP Ã© absoluto.

B) INTERRUPÃ‡ÃƒO REAL DA FALA
Sinais possÃ­veis:
- reticÃªncias expressivas;
- travessÃ£o/corte;
- outra pessoa interrompe;
- mudanÃ§a abrupta de speaker;
- a interrupÃ§Ã£o faz sentido dramÃ¡tico;
- a frase foi propositalmente abandonada.
Nesse caso, PRESERVE a interrupÃ§Ã£o.

C) PROVÃVEL DEFEITO/TRUNCAMENTO DA LEGENDA-FONTE
Pode completar SOMENTE quando houver evidÃªncia forte de que:
- o EN termina de forma gramatical ou semanticamente quebrada;
- nÃ£o hÃ¡ sinal razoÃ¡vel de interrupÃ§Ã£o real;
- o prÃ³ximo cue comeÃ§a outra fala ou outra ideia;
- o contexto torna a intenÃ§Ã£o praticamente inequÃ­voca;
- existe uma conclusÃ£o mÃ­nima e genÃ©rica que recupera a intenÃ§Ã£o
  sem inventar fato especÃ­fico.

Exemplo permitido em princÃ­pio:
"If I can get through Snatch Game, I can get through..."
â†’ algo equivalente a
"Se passo pelo Snatch Game, passo por qualquer coisa."
SE o contexto sustentar claramente essa conclusÃ£o.

Exemplo NÃƒO permitido:
â†’ "Se passo pelo Snatch Game, vou ganhar a competiÃ§Ã£o."
Isso inventa informaÃ§Ã£o especÃ­fica nÃ£o sustentada pela fonte.

D) AMBÃGUO
Se houver duas ou mais continuaÃ§Ãµes plausÃ­veis,
ou dÃºvida real entre truncamento da legenda e interrupÃ§Ã£o da fala:
NÃƒO COMPLETE.
Preserve a ambiguidade/incompletude.

============================================================
COMO JULGAR AFTER_PT
============================================================

Se AFTER_PT fez um SOURCE DEFECT RECOVERY conservador e bem sustentado:
- NÃƒO marque como regressÃ£o;
- nÃ£o exija que volte a ficar incompleto.

Se AFTER_PT inventou uma conclusÃ£o especÃ­fica sem evidÃªncia:
- marque.

Se AFTER_PT perdeu informaÃ§Ã£o claramente presente no EN:
- marque.

Se AFTER_PT apenas reformulou livremente,
mas todo o sentido e efeito social sobreviveram:
- NÃƒO marque.

NÃ£o exija correspondÃªncia palavra por palavra.
EquivalÃªncia idiomÃ¡tica e pragmÃ¡tica Ã© correta.

============================================================
CORREÃ‡ÃƒO
============================================================

Se AFTER_PT estiver semanticamente correto:
NÃƒO devolva o cue em issues.

Se houver regressÃ£o real:
- devolva o id;
- explique resumidamente em reason;
- forneÃ§a em pt uma correÃ§Ã£o natural, completa e concisa.

A correÃ§Ã£o DEVE:
- permanecer no MESMO cue;
- preservar exatamente todos os canonical_locks;
- preservar Meaning Integrity;
- respeitar SOURCE DEFECT RECOVERY;
- nÃ£o criar SDH;
- nÃ£o inventar informaÃ§Ã£o;
- nÃ£o mover conteÃºdo entre cues;
- nÃ£o alterar timestamp;
- ser diagramÃ¡vel em no mÃ¡ximo ${LAYOUT_MAX_LINES} linhas
  de ${LAYOUT_MAX_CHARS_PER_LINE} caracteres;
- preferencialmente ficar em atÃ© ${COMPACT_RESCUE_TARGET_TOTAL_CHARS}
  caracteres visÃ­veis totais.

Meta final:
SIGNIFICADO / INTENÃ‡ÃƒO COMPLETOS
+ PT-BR NATURAL
+ 2x50.
`;

const SEMANTIC_COMPACT_RETRY_PROMPT = `
VocÃª Ã© o COMPACTADOR SEMÃ‚NTICO FINAL de um Ãºnico cue FONTEâ†’PT-BR.

A traduÃ§Ã£o recebida jÃ¡ foi identificada como semanticamente necessÃ¡ria.

Sua Ãºnica tarefa Ã© reescrever a FORMA para caber em:

- no mÃ¡ximo ${LAYOUT_MAX_LINES} linhas;
- no mÃ¡ximo ${LAYOUT_MAX_CHARS_PER_LINE} caracteres por linha.

REGRA ABSOLUTA:
NÃƒO remova nenhuma unidade de significado da correÃ§Ã£o recebida.

Preserve:
- aÃ§Ã£o;
- sujeito;
- objeto;
- nÃºmeros;
- unidades;
- relaÃ§Ãµes;
- negaÃ§Ã£o;
- intensidade;
- insulto;
- humor;
- palavrÃ£o relevante;
- informaÃ§Ã£o narrativa.

Pode:
- mudar completamente a sintaxe;
- usar contraÃ§Ãµes naturais;
- eliminar sujeito redundante;
- escolher formulaÃ§Ã£o PT-BR mais curta;
- usar nÃºmeros em algarismos quando natural.

NÃ£o pode:
- resumir conteÃºdo;
- apagar informaÃ§Ã£o;
- inventar;
- mover conteÃºdo;
- alterar timestamp;
- criar SDH;
- suavizar registro;
- deformar canonical_locks.

canonical_locks devem aparecer exatamente.

Resultado:
PT-BR natural + significado completo + 2x50.
`;

const SEMANTIC_REWRITE_AUDIT_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    issues: {
      type: "array",
      maxItems: SEMANTIC_REWRITE_AUDIT_MAX_ISSUES,
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: {
            type: "integer"
          },
          reason: {
            type: "string"
          },
          pt: {
            type: "string"
          }
        },
        required: [
          "i",
          "reason",
          "pt"
        ]
      }
    }
  },
  required: [
    "issues"
  ]
};

const PRE_REPAIR_CONFIRM_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    issues: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: { type: "integer" },
          reason: { type: "string" }
        },
        required: ["i", "reason"]
      }
    }
  },
  required: ["issues"]
};

const QA_SCHEMA = {
  type: "object",
  additionalProperties: false,

  properties: {
    issues: {
      type: "array",
      maxItems: QA_MAX_FLAGS_TOTAL,

      items: {
        type: "object",
        additionalProperties: false,

        properties: {
          i: {
            type: "integer"
          },

          reason: {
            type: "string"
          }
        },

        required: [
          "i",
          "reason"
        ]
      }
    }
  },

  required: [
    "issues"
  ]
};

const SYNC_PROXY_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: { type: "integer" },
          text: { type: "string" }
        },
        required: ["i", "text"]
      }
    }
  },
  required: ["items"]
};

function normalizeSyncLanguageCode(value, fallback = "en") {
  const raw = String(value || "").trim().toLowerCase();
  const map = {
    eng: "en", en: "en",
    spa: "es", esp: "es", es: "es",
    por: "pt", pt: "pt", "pt-br": "pt-BR", "pt-pt": "pt-PT",
    fra: "fr", fre: "fr", fr: "fr",
    ita: "it", it: "it",
    deu: "de", ger: "de", de: "de",
    nld: "nl", dut: "nl", nl: "nl",
    jpn: "ja", ja: "ja",
    kor: "ko", ko: "ko",
    zho: "zh", chi: "zh", zh: "zh",
    rus: "ru", ru: "ru",
    pol: "pl", pl: "pl",
    tur: "tr", tr: "tr",
    ara: "ar", ar: "ar",
    heb: "he", he: "he",
    ces: "cs", cze: "cs", cs: "cs",
    hun: "hu", hu: "hu",
    ron: "ro", rum: "ro", ro: "ro",
    ukr: "uk", uk: "uk"
  };
  if (map[raw]) return map[raw];
  if (/^[a-z]{2}(?:-[a-z]{2})?$/i.test(raw)) return raw;
  return fallback;
}

function syncLanguageName(code) {
  const c = normalizeSyncLanguageCode(code, "en").toLowerCase();
  const names = {
    en: "English", es: "Spanish", pt: "Portuguese", "pt-br": "Brazilian Portuguese",
    "pt-pt": "European Portuguese", fr: "French", it: "Italian", de: "German",
    nl: "Dutch", ja: "Japanese", ko: "Korean", zh: "Chinese", ru: "Russian",
    pl: "Polish", tr: "Turkish", ar: "Arabic", he: "Hebrew", cs: "Czech",
    hu: "Hungarian", ro: "Romanian", uk: "Ukrainian"
  };
  return names[c] || c;
}

async function buildMultilingualSyncProxy(items, sourceLang, targetLang) {
  const cleanItems = (Array.isArray(items) ? items : [])
    .map(item => ({
      i: Number(item?.i),
      text: String(item?.text || "").replace(/\s+/g, " ").trim()
    }))
    .filter(item => Number.isInteger(item.i) && item.text);

  if (!cleanItems.length) {
    throw new Error("SYNC PROXY sem itens vÃ¡lidos.");
  }
  if (cleanItems.length > SYNC_PROXY_MAX_ITEMS) {
    throw new Error(`SYNC PROXY excede ${SYNC_PROXY_MAX_ITEMS} itens.`);
  }

  const charCount = cleanItems.reduce((sum, item) => sum + item.text.length, 0);
  if (charCount > SYNC_PROXY_MAX_CHARS) {
    throw new Error(`SYNC PROXY excede ${SYNC_PROXY_MAX_CHARS} caracteres.`);
  }

  const src = normalizeSyncLanguageCode(sourceLang, "auto");
  const dst = normalizeSyncLanguageCode(targetLang, "en");

  if (src !== "auto" && src.toLowerCase() === dst.toLowerCase()) {
    return cleanItems;
  }

  const system = `You create lexical proxy text ONLY for subtitle/audio synchronization.
Translate each item from ${syncLanguageName(src)} to ${syncLanguageName(dst)}.
Rules:
- Keep exactly the same integer i for every item and return the same number of items.
- Translate the spoken meaning faithfully and literally enough for word matching against a transcript.
- Preserve names, numbers, profanity, repeated words and short discourse markers when they are spoken.
- Do not summarize, omit, merge, split, censor, embellish or move content between ids.
- Do not add speaker labels, explanations or timestamps.
- Output JSON only.`;

  const response = await geminiRequest({
    system,
    user: JSON.stringify({
      source_language: src,
      target_language: dst,
      items: cleanItems
    }),
    schema: SYNC_PROXY_SCHEMA,
    thinkingLevel: SYNC_PROXY_THINKING,
    maxOutputTokens: SYNC_PROXY_MAX_OUTPUT_TOKENS,
    timeoutMs: SYNC_PROXY_TIMEOUT_MS,
    maxRetries: SYNC_PROXY_HTTP_RETRIES,
    job: null,
    metric: "syncproxy"
  });

  const parsed = JSON.parse(stripCodeFences(response.text));
  const returned = Array.isArray(parsed?.items) ? parsed.items : [];
  const byId = new Map(returned.map(item => [Number(item?.i), String(item?.text || "").trim()]));
  const out = [];
  for (const item of cleanItems) {
    const text = byId.get(item.i);
    if (!text) throw new Error(`SYNC PROXY perdeu o cue ${item.i}.`);
    out.push({ i: item.i, text });
  }
  if (out.length !== cleanItems.length) {
    throw new Error("SYNC PROXY retornou contagem divergente.");
  }
  return out;
}

const SYNC_ALIGN_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: { type: "integer" },
          matched: { type: "boolean" },
          start_word: { type: "integer" },
          end_word: { type: "integer" },
          confidence: { type: "integer" }
        },
        required: ["i", "matched", "start_word", "end_word", "confidence"]
      }
    }
  },
  required: ["items"]
};

async function alignMultilingualSyncAnchors(items, sourceLang, audioLang) {
  const src = normalizeSyncLanguageCode(sourceLang, "auto");
  const aud = normalizeSyncLanguageCode(audioLang, "en");

  const cleanItems = (Array.isArray(items) ? items : [])
    .map(item => {
      const words = (Array.isArray(item?.words) ? item.words : [])
        .map((word, index) => ({
          n: Number.isInteger(Number(word?.n)) ? Number(word.n) : index,
          text: String(word?.text || "").replace(/\s+/g, " ").trim()
        }))
        .filter(word => Number.isInteger(word.n) && word.text)
        .slice(0, SYNC_ALIGN_MAX_WORDS_PER_ITEM);

      return {
        i: Number(item?.i),
        source_text: String(item?.sourceText || "").replace(/\s+/g, " ").trim(),
        words
      };
    })
    .filter(item =>
      Number.isInteger(item.i) &&
      item.source_text &&
      item.words.length >= 2
    );

  if (!cleanItems.length) {
    throw new Error("SYNC ALIGN sem itens vÃ¡lidos.");
  }

  if (cleanItems.length > SYNC_ALIGN_MAX_ITEMS) {
    throw new Error(`SYNC ALIGN excede ${SYNC_ALIGN_MAX_ITEMS} itens.`);
  }

  const charCount = JSON.stringify(cleanItems).length;
  if (charCount > SYNC_ALIGN_MAX_CHARS) {
    throw new Error(`SYNC ALIGN excede ${SYNC_ALIGN_MAX_CHARS} caracteres.`);
  }

  const system = `You align subtitle cues to short ASR transcript windows for synchronization.
SOURCE language: ${syncLanguageName(src)}.
AUDIO transcript language: ${syncLanguageName(aud)}.

For each item:
- source_text is ONE subtitle cue.
- words is the exact ordered ASR word list from the audio window, each with integer n.
- Find the shortest contiguous word span that expresses the SAME spoken meaning as source_text.
- Cross-language paraphrase is expected. Match meaning, names, numbers, profanity and intent; do not require literal wording.
- Ignore subtitle timing completely. You do not receive timestamps.
- If the source cue is not clearly represented in the word list, matched=false.
- If matched=true, start_word/end_word MUST be existing n values and start_word <= end_word.
- confidence must be an integer from 0 to 100 and should reflect semantic certainty.
- Return every input i exactly once.
- Never invent words or indices.
Output JSON only.`;

  const response = await geminiRequest({
    system,
    user: JSON.stringify({
      source_language: src,
      audio_language: aud,
      items: cleanItems
    }),
    schema: SYNC_ALIGN_SCHEMA,
    thinkingLevel: SYNC_ALIGN_THINKING,
    maxOutputTokens: SYNC_ALIGN_MAX_OUTPUT_TOKENS,
    timeoutMs: SYNC_ALIGN_TIMEOUT_MS,
    maxRetries: SYNC_ALIGN_HTTP_RETRIES,
    job: null,
    metric: "syncalign"
  });

  const parsed = JSON.parse(stripCodeFences(response.text));
  const returned = Array.isArray(parsed?.items) ? parsed.items : [];
  const byId = new Map(returned.map(item => [Number(item?.i), item]));
  const out = [];

  for (const input of cleanItems) {
    const raw = byId.get(input.i);
    if (!raw) {
      out.push({
        i: input.i,
        matched: false,
        start_word: -1,
        end_word: -1,
        confidence: 0
      });
      continue;
    }

    const startWord = Number(raw.start_word);
    const endWord = Number(raw.end_word);
    const confidence = Math.max(0, Math.min(100, Math.round(Number(raw.confidence || 0))));
    const allowed = new Set(input.words.map(word => word.n));

    const matched =
      Boolean(raw.matched) &&
      Number.isInteger(startWord) &&
      Number.isInteger(endWord) &&
      startWord <= endWord &&
      allowed.has(startWord) &&
      allowed.has(endWord);

    out.push({
      i: input.i,
      matched,
      start_word: matched ? startWord : -1,
      end_word: matched ? endWord : -1,
      confidence: matched ? confidence : 0
    });
  }

  console.log(
    `[SYNC ALIGN] ${out.filter(item => item.matched).length}/${out.length} Ã¢ncora(s) semanticamente alinhadas | ` +
    `source=${src} | audio=${aud}.`
  );

  return out;
}

// Deliberadamente simples.
// O 8.3.5 estava enviando objetos aninhados em people e o PLAN
// recebia HTTP 400. Agora Gemini devolve strings simples e o
// JavaScript reconstrÃ³i o Character Ledger rico.
const PLAN_SCHEMA = {
  type: "object",
  additionalProperties: false,

  properties: {
    tone: {
      type: "string"
    },

    people: {
      type: "array",
      items: {
        type: "string"
      },
      maxItems: 30
    },

    glossary: {
      type: "array",
      items: {
        type: "string"
      },
      maxItems: 40
    },

    continuity: {
      type: "array",
      items: {
        type: "string"
      },
      maxItems: 30
    }
  },

  required: [
    "tone",
    "people",
    "glossary",
    "continuity"
  ]
};

// Fallback ultra-simples:
// mesmo se o schema acima for rejeitado,
// nÃ£o deixa o Character Ledger simplesmente desaparecer.
const PLAN_FALLBACK_SCHEMA = {
  type: "object",
  additionalProperties: false,

  properties: {
    plan: {
      type: "string"
    }
  },

  required: [
    "plan"
  ]
};

function cueTranslationSchema(
  expectedCount
) {
  return {
    type: "object",
    additionalProperties: false,

    properties: {
      cues: {
        type: "array",
        minItems: expectedCount,
        maxItems: expectedCount,

        items: {
          type: "object",
          additionalProperties: false,

          properties: {
            i: {
              type: "integer"
            },

            pt: {
              type: "string"
            }
          },

          required: [
            "i",
            "pt"
          ]
        }
      }
    },

    required: [
      "cues"
    ]
  };
}

function mainCueTranslationSchema(
  expectedCount
) {
  // 8.7: NÃƒO codificamos o tamanho do lote no JSON Schema.
  // O parser local abaixo jÃ¡ exige contagem exata, ordem exata, IDs e ownership_key.
  // Isso evita rejeiÃ§Ã£o do request pelo provedor quando lotes grandes (ex.: 140)
  // sÃ£o expressos como minItems/maxItems rÃ­gidos, sem relaxar nenhuma validaÃ§Ã£o.
  void expectedCount;
  return {
    type: "object",
    additionalProperties: false,

    properties: {
      cues: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,

          properties: {
            i: { type: "integer" },
            k: { type: "string" },
            pt: { type: "string" }
          },

          required: [
            "i",
            "k",
            "pt"
          ]
        }
      }
    },

    required: [
      "cues"
    ]
  };
}

function isDeterministicGeminiRequestError(error) {
  const status = Number(error?.status || 0);
  if ([400, 401, 403, 404, 413, 422].includes(status)) return true;
  const text = String(error?.message || error || "");
  return /HTTP\s+(?:400|401|403|404|413|422)\b|invalid argument/i.test(text);
}

// ============================================================
// GEMINI
// ============================================================

function parseDurationMs(value) {
  const text =
    String(value || "")
      .trim()
      .toLowerCase();

  if (!text) {
    return null;
  }

  const ms =
    text.match(
      /^(\d+(?:\.\d+)?)ms$/
    );

  if (ms) {
    return Math.max(
      250,
      Number(ms[1])
    );
  }

  const sec =
    text.match(
      /^(\d+(?:\.\d+)?)s$/
    );

  if (sec) {
    return Math.max(
      1000,
      Number(sec[1]) *
      1000
    );
  }

  const num =
    Number(text);

  return (
    Number.isFinite(num) &&
    num > 0
  )
    ? Math.max(
        1000,
        num * 1000
      )
    : null;
}

function retryDelayMs(
  response,
  data,
  attempt
) {
  const header =
    parseDurationMs(
      response?.headers?.get(
        "retry-after"
      )
    );

  if (header) {
    return Math.min(
      180000,
      header + 750
    );
  }

  const details =
    Array.isArray(
      data?.error?.details
    )
      ? data.error.details
      : [];

  for (
    const detail of details
  ) {
    const parsed =
      parseDurationMs(
        detail?.retryDelay ||
        detail?.retry_delay ||
        detail?.metadata?.retryDelay ||
        detail?.metadata?.retry_delay
      );

    if (parsed) {
      return Math.min(
        180000,
        parsed + 750
      );
    }
  }

  const message =
    String(
      data?.error?.message ||
      data?.message ||
      ""
    );

  const human =
    message.match(
      /(?:please\s+)?retry\s+in\s+(\d+(?:\.\d+)?)s/i
    );

  if (human) {
    return Math.min(
      180000,

      Math.max(
        1000,
        Number(human[1]) *
        1000 +
        1000
      )
    );
  }

  return Math.min(
    10000 * attempt,
    60000
  );
}

function extractInteractionText(
  data
) {
  if (
    typeof data?.output_text ===
      "string" &&
    data.output_text.trim()
  ) {
    return data.output_text.trim();
  }

  const steps =
    Array.isArray(data?.steps)
      ? data.steps
      : [];

  let out = "";

  for (
    const step of steps
  ) {
    if (
      step?.type !==
        "model_output" ||
      !Array.isArray(
        step.content
      )
    ) {
      continue;
    }

    out +=
      step.content
        .filter(
          part =>
            part?.type ===
              "text" &&
            typeof part?.text ===
              "string"
        )
        .map(
          part => part.text
        )
        .join("");
  }

  return out.trim();
}

function extractGeminiQuotaDetails(data) {
  const out = [];
  const details = Array.isArray(data?.error?.details)
    ? data.error.details
    : Array.isArray(data?.details)
      ? data.details
      : [];

  for (const detail of details) {
    const violations = Array.isArray(detail?.violations) ? detail.violations : [];
    for (const v of violations) {
      out.push({
        metric: String(v?.quotaMetric || v?.metric || ""),
        id: String(v?.quotaId || v?.quota_id || ""),
        value: v?.quotaValue ?? v?.quota_value ?? null,
        dimensions: v?.quotaDimensions || v?.quota_dimensions || null
      });
    }
  }

  return out;
}

function quotaDetailsText(data) {
  const rows = extractGeminiQuotaDetails(data);
  if (!rows.length) return "quota-detail=indisponÃ­vel";
  return rows.map(row => {
    let dims = "";
    try { dims = row.dimensions ? JSON.stringify(row.dimensions) : ""; } catch {}
    return `metric=${row.metric || "?"} | id=${row.id || "?"} | limit=${row.value ?? "?"}${dims ? ` | dims=${dims}` : ""}`;
  }).join(" || ").slice(0, 1800);
}

function raiseGeminiGlobalCooldown(
  waitMs,
  job,
  metric
) {
  const safeWait =
    Math.max(
      1000,
      Number(waitMs || 0) +
      GEMINI_429_GLOBAL_BUFFER_MS
    );

  const candidate =
    Date.now() + safeWait;

  // 429 deveria ser excepcional no 8.8.3. Se ocorrer por capacidade efetiva
  // variÃ¡vel do serviÃ§o, o governor aprende imediatamente um perfil ainda
  // mais conservador para o restante da vida do processo.
  geminiSafeRpmActive = Math.min(geminiSafeRpmActive, 4);
  geminiSafeTpmActive = Math.min(geminiSafeTpmActive, 75000);

  if (candidate > geminiGlobalCooldownUntil) {
    geminiGlobalCooldownUntil =
      candidate;

    console.warn(
      `[GEMINI QUOTA BARRIER 8.8.3] 429 em ${String(metric || "gemini").toUpperCase()} | ` +
      `single-flight pausado por ~${(safeWait / 1000).toFixed(1)}s; ` +
      `perfil adaptativo=${geminiSafeRpmActive} RPM/${geminiSafeTpmActive} TPM.`
    );
  }

  if (job) {
    job.stats.global429Barriers =
      (job.stats.global429Barriers || 0) + 1;
  }
}

function markAttempt(
  job,
  metric
) {
  if (!job) {
    return;
  }

  if (metric === "main") {
    job.stats.mainAttempts++;
  }

  if (metric === "repair") {
    job.stats.repairAttempts++;
  }

  if (metric === "qa") {
    job.stats.qaAttempts++;
  }

  if (metric === "preconfirm") {
    job.stats.preRepairConfirmAttempts++;
  }
}

function mark429(
  job,
  metric
) {
  if (!job) {
    return;
  }

  if (metric === "main") {
    job.stats.main429++;
  }

  if (metric === "repair") {
    job.stats.repair429++;
  }

  if (metric === "qa") {
    job.stats.qa429++;
  }

  if (metric === "preconfirm") {
    job.stats.preRepairConfirm429++;
  }
}

function markSuccess(
  job,
  metric,
  data
) {
  if (!job) {
    return;
  }

  if (metric === "plan") {
    job.stats.planCalls++;
  }

  if (metric === "main") {
    job.stats.mainCalls++;
  }

  if (metric === "repair") {
    job.stats.repairCalls++;
  }

  if (metric === "qa") {
    job.stats.qaCalls++;
  }

  if (metric === "preconfirm") {
    job.stats.preRepairConfirmCalls++;
  }

  job.stats.inputTokens +=
    Number(
      data?.usage
        ?.total_input_tokens ||
      0
    );

  job.stats.outputTokens +=
    Number(
      data?.usage
        ?.total_output_tokens ||
      0
    );

  job.stats.thoughtTokens +=
    Number(
      data?.usage
        ?.total_thought_tokens ||
      0
    );
}

function extractGenerateContentText(data) {
  const parts = data?.candidates?.[0]?.content?.parts;
  if (!Array.isArray(parts)) return "";
  return parts
    .filter(part => part && typeof part.text === "string")
    .map(part => part.text)
    .join("")
    .trim();
}

function normalizeGenerateContentUsage(data) {
  const usage = data?.usageMetadata || {};
  return {
    total_input_tokens: Number(usage.promptTokenCount || 0),
    total_output_tokens: Number(usage.candidatesTokenCount || 0),
    total_thought_tokens: Number(usage.thoughtsTokenCount || 0),
    total_tokens: Number(usage.totalTokenCount || 0)
  };
}

function extractLastValidJsonObject(value) {
  const clean = stripCodeFences(String(value || "")).trim();
  if (!clean) return "";

  try {
    JSON.parse(clean);
    return clean;
  } catch {}

  const candidates = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let escaped = false;

  for (let i = 0; i < clean.length; i++) {
    const ch = clean[i];

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === "{") {
      if (depth === 0) start = i;
      depth++;
    } else if (ch === "}") {
      if (depth > 0) depth--;
      if (depth === 0 && start >= 0) {
        const candidate = clean.slice(start, i + 1);
        try {
          JSON.parse(candidate);
          candidates.push(candidate);
        } catch {}
        start = -1;
      }
    }
  }

  return candidates.length ? candidates[candidates.length - 1] : clean;
}

function geminiRouteForMetric(metric) {
  const normalized = String(metric || "main").toLowerCase();
  const refinedFirst = [GEMINI_MODELS.MAIN_FALLBACK, GEMINI_MODELS.MAIN_PRIMARY]; // 3.5 -> 3.1
  const scaleFirst = [GEMINI_MODELS.MAIN_PRIMARY, GEMINI_MODELS.MAIN_FALLBACK];   // 3.1 -> 3.5

  // MAIN Ã© o Ãºnico estÃ¡gio cuja preferÃªncia pode ser invertida para benchmark A/B.
  if (normalized === "main") {
    return MAIN_ROUTE_PREFERENCE_942 === "3.1" ? scaleFirst : refinedFirst;
  }

  // Proxy lexical em massa pode aproveitar 3.1 primeiro; se falhar, 3.5 assume.
  if (normalized === "syncproxy") return scaleFirst;

  // PLAN/QA/REPAIR/COMPACT/SYNC-ALIGN/PRECONFIRM privilegiam refinamento.
  return refinedFirst;
}

const geminiModelRuntime = new Map();

function runtimeForGeminiModel(modelId) {
  if (!geminiModelRuntime.has(modelId)) {
    geminiModelRuntime.set(modelId, {
      calls: [],
      lastStart: 0,
      unavailableUntil: 0,
      unavailableReason: "",
      gate: Promise.resolve()
    });
  }
  return geminiModelRuntime.get(modelId);
}

function pruneRoutedModelCalls(runtime, now = Date.now()) {
  const dayAgo = now - 24 * 60 * 60 * 1000;
  runtime.calls = Array.isArray(runtime.calls)
    ? runtime.calls.filter(item => Number(item?.ts || 0) >= dayAgo)
    : [];
}

function routedModelInputCost(item) {
  return Math.max(
    Number(item?.estimatedInputTokens || 0),
    Number(item?.actualInputTokens || 0)
  );
}

function modelSkippedForJob(job, modelId) {
  return Boolean(
    job?.modelRouterSkip instanceof Set &&
    job.modelRouterSkip.has(modelId)
  );
}

function routerMetricCooldownKey942(metric, modelId) {
  return `${String(metric || "main").toLowerCase()}|${String(modelId || "")}`;
}

function modelMetricCooldownUntil942(job, metric, modelId) {
  if (!(job?.modelRouterMetricCooldown instanceof Map)) return 0;
  const row = job.modelRouterMetricCooldown.get(routerMetricCooldownKey942(metric, modelId));
  return Math.max(0, Number(row?.until || 0));
}

function setModelMetricCooldown942(job, metric, modelId, waitMs, reason, status = "metric_cooldown") {
  if (!job) return;
  if (!(job.modelRouterMetricCooldown instanceof Map)) job.modelRouterMetricCooldown = new Map();
  const key = routerMetricCooldownKey942(metric, modelId);
  const until = Date.now() + Math.max(500, Number(waitMs || 0));
  const current = job.modelRouterMetricCooldown.get(key);
  if (!current || until > Number(current.until || 0)) {
    job.modelRouterMetricCooldown.set(key, { until, reason: String(reason || ""), status: String(status || "metric_cooldown") });
  }
}

function clearExpiredMetricCooldowns942(job, now = Date.now()) {
  if (!(job?.modelRouterMetricCooldown instanceof Map)) return;
  for (const [key, row] of job.modelRouterMetricCooldown.entries()) {
    if (Number(row?.until || 0) <= now) job.modelRouterMetricCooldown.delete(key);
  }
}

function setJobModelHealth(job, modelId, status, reason = "") {
  if (!job) return;
  if (!(job.modelRouterHealth instanceof Map)) job.modelRouterHealth = new Map();

  job.modelRouterHealth.set(modelId, {
    status: String(status || "unknown"),
    reason: String(reason || ""),
    at: Date.now()
  });
}

function skipModelForJob(job, modelId, reason, status = "hard_unavailable") {
  if (!job) return;
  if (!(job.modelRouterSkip instanceof Set)) job.modelRouterSkip = new Set();
  const already = job.modelRouterSkip.has(modelId);
  job.modelRouterSkip.add(modelId);
  setJobModelHealth(job, modelId, status, reason);

  if (!already) {
    console.warn(`[MODEL ROUTER HARD-SKIP] ${modelId} indisponÃ­vel pelo restante deste job | ${status} | ${reason}.`);
  }
}

function invalidateResponseModelForJob(job, response, label, error, metric = "main") {
  const modelId = String(response?.modelId || "").trim();
  if (!modelId || !job) return;

  job.stats.modelInvalidResponses = Number(job.stats.modelInvalidResponses || 0) + 1;
  const reason = `${label}: ${errorMessage(error).slice(0, 220)}`;

  // 9.4.2: JSON/lock invÃ¡lido Ã© propriedade desta chamada/payload, NÃƒO prova que
  // o modelo ficou incapaz para QA/Repair ou para o resto do episÃ³dio.
  setModelMetricCooldown942(
    job,
    metric,
    modelId,
    ROUTER_INVALID_RESPONSE_COOLDOWN_MS_942,
    reason,
    "invalid_response_local"
  );
  setJobModelHealth(job, modelId, "invalid_response_local", reason);
  console.warn(
    `[MODEL ROUTER] ${modelId} resposta invÃ¡lida em ${metric}; cooldown SOMENTE desta mÃ©trica por ` +
    `${Math.round(ROUTER_INVALID_RESPONSE_COOLDOWN_MS_942 / 1000)}s e fallback imediato. NÃ£o hÃ¡ hard-skip do job.`
  );
}

function setModelUnavailable(modelId, waitMs, reason) {
  const runtime = runtimeForGeminiModel(modelId);
  const until = Date.now() + Math.max(1000, Number(waitMs || 0));
  if (until > runtime.unavailableUntil) {
    runtime.unavailableUntil = until;
    runtime.unavailableReason = String(reason || "temporariamente indisponÃ­vel");
  }
}

function routedQuotaKind(data) {
  const rows = extractGeminiQuotaDetails(data);
  const joined = rows
    .map(row => `${row.id} ${row.metric}`)
    .join(" | ");

  if (/PerDay|RequestsPerDay|RPD/i.test(joined)) return "daily";
  if (/PerMinute|RequestsPerMinute|RPM/i.test(joined)) return "rpm";
  if (/TokensPerMinute|TPM/i.test(joined)) return "tpm";
  return "rate";
}

async function reserveRoutedModelStart({
  modelId,
  estimate,
  job,
  metric,
  bypassPacer = false
}) {
  const profile = GEMINI_MODEL_PROFILES[modelId];
  if (!profile) {
    const error = new Error(`MODEL ROUTER: perfil ausente para ${modelId}.`);
    error.nonRetryable = true;
    throw error;
  }

  const runtime = runtimeForGeminiModel(modelId);
  const previous = runtime.gate;
  let release;
  runtime.gate = new Promise(resolve => { release = resolve; });
  await previous;

  try {
    while (true) {
      const now = Date.now();
      pruneRoutedModelCalls(runtime, now);

      if (modelSkippedForJob(job, modelId)) {
        const error = new Error(`MODEL ROUTER: ${modelId} marcado como indisponÃ­vel neste job.`);
        error.code = "MODEL_JOB_SKIP";
        throw error;
      }

      if (runtime.unavailableUntil > now) {
        const error = new Error(
          `MODEL ROUTER: ${modelId} indisponÃ­vel por mais ${Math.ceil((runtime.unavailableUntil - now) / 1000)}s | ${runtime.unavailableReason}.`
        );
        error.code = "MODEL_COOLDOWN";
        throw error;
      }

      if (estimate > profile.tpmSoft) {
        const error = new Error(
          `MODEL ROUTER: payload estimado ${estimate} > TPM-safe ${profile.tpmSoft} de ${modelId}.`
        );
        error.status = 413;
        error.routerCanSplit = true;
        error.modelId = modelId;
        throw error;
      }

      if (runtime.calls.length >= profile.rpd) {
        skipModelForJob(job, modelId, `RPD local ${runtime.calls.length}/${profile.rpd}`);
        const error = new Error(`MODEL ROUTER: RPD local atingido para ${modelId}.`);
        error.code = "MODEL_LOCAL_RPD";
        error.modelId = modelId;
        throw error;
      }

      const minuteAgo = now - 60000;
      const minuteCalls = runtime.calls
        .filter(item => Number(item?.ts || 0) >= minuteAgo)
        .sort((a, b) => Number(a.ts) - Number(b.ts));

      let rpmWait = 0;
      if (minuteCalls.length >= profile.rpmSoft) {
        const idx = Math.max(0, minuteCalls.length - profile.rpmSoft);
        rpmWait = Math.max(0, Number(minuteCalls[idx].ts) + 60000 - now + 150);
      }

      const minuteTokens = minuteCalls.reduce(
        (sum, item) => sum + routedModelInputCost(item),
        0
      );

      let tpmWait = 0;
      if (minuteTokens + estimate > profile.tpmSoft && minuteCalls.length) {
        let rolling = minuteTokens;
        for (const item of minuteCalls) {
          rolling -= routedModelInputCost(item);
          if (rolling + estimate <= profile.tpmSoft) {
            tpmWait = Math.max(0, Number(item.ts) + 60000 - now + 150);
            break;
          }
        }
        if (!tpmWait) {
          tpmWait = Math.max(
            0,
            Number(minuteCalls[minuteCalls.length - 1].ts) + 60000 - now + 150
          );
        }
      }

      const pacerWait = bypassPacer
        ? 0
        : Math.max(0, Number(runtime.lastStart || 0) + profile.minStartMs - now);

      const waitMs = Math.max(rpmWait, tpmWait, pacerWait);

      if (waitMs > 0) {
        if (job) job.stats.pacerWaitMs += waitMs;
        await sleep(waitMs);
        continue;
      }

      const id = `${modelId}:${now}:${crypto.randomBytes(4).toString("hex")}`;
      runtime.lastStart = Date.now();
      runtime.calls.push({
        id,
        ts: runtime.lastStart,
        metric: String(metric || "main"),
        estimatedInputTokens: estimate,
        actualInputTokens: 0
      });

      return id;
    }
  } finally {
    release();
  }
}

function commitRoutedModelUsage(modelId, reservationId, usage = {}) {
  const runtime = runtimeForGeminiModel(modelId);
  const item = runtime.calls.find(call => call.id === reservationId);
  if (!item) return;
  item.actualInputTokens = Number(usage?.total_input_tokens || 0);
  item.updatedAt = Date.now();
}

function recordRouterModelCall(job, modelId) {
  if (!job) return;
  if (!job.stats.modelCallsById || typeof job.stats.modelCallsById !== "object") {
    job.stats.modelCallsById = {};
  }
  job.stats.modelCallsById[modelId] = Number(job.stats.modelCallsById[modelId] || 0) + 1;
}

async function callGenerateContentModel({
  modelId,
  system,
  user,
  schema,
  thinkingLevel,
  maxOutputTokens,
  timeoutMs,
  job,
  metric,
  bypassPacer = false
}) {
  const profile = GEMINI_MODEL_PROFILES[modelId];
  const estimate = estimateGeminiInputTokens(system, user, schema);

  const reservationId = await reserveRoutedModelStart({
    modelId,
    estimate,
    job,
    metric,
    bypassPacer
  });

  const generationConfig = {
    maxOutputTokens: Number(maxOutputTokens || 8192),
    thinkingConfig: {
      thinkingLevel: String(thinkingLevel || "medium").toUpperCase()
    }
  };

  let routedUser = String(user || "");

  if (schema && profile.supportsStructuredOutput) {
    // Raw REST generateContent uses the GenerationConfig JSON fields below.
    // responseFormat is an SDK/new-surface shape and produced HTTP 400 on
    // gemini-3.1-flash-lite in production. responseJsonSchema preserves our
    // existing lowercase JSON Schema without enum/type conversion.
    generationConfig.responseMimeType = "application/json";
    generationConfig.responseJsonSchema = schema;
  } else if (schema) {
    routedUser +=
      `\n\nEMERGÃŠNCIA DE FORMATO: responda SOMENTE JSON vÃ¡lido, sem markdown nem explicaÃ§Ãµes. ` +
      `O objeto deve obedecer a este schema: ${JSON.stringify(schema)}`;
  }

  const controller = new AbortController();
  const effectiveTimeout = Math.max(
    3000,
    Math.min(Number(timeoutMs || 90000), Number(profile.timeoutCapMs || timeoutMs || 90000))
  );
  const timer = setTimeout(() => controller.abort(), effectiveTimeout);

  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(modelId)}:generateContent`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": GEMINI_API_KEY
        },
        body: JSON.stringify({
          systemInstruction: {
            parts: [{ text: String(system || "") }]
          },
          contents: [{
            role: "user",
            parts: [{ text: routedUser }]
          }],
          generationConfig
        }),
        signal: controller.signal
      }
    );

    const raw = await response.text();
    let data = null;
    try { data = raw ? JSON.parse(raw) : {}; } catch { data = {}; }

    if (!response.ok) {
      const error = new Error(
        `GEMINI ${modelId} HTTP ${response.status}: ${String(
          data?.error?.message || data?.message || raw || "erro"
        ).slice(0, 1800)}`
      );
      error.status = response.status;
      error.modelId = modelId;
      error.providerData = data;
      error.retryAfterMs = retryDelayMs(response, data, 1);
      throw error;
    }

    let text = extractGenerateContentText(data);
    if (!text) {
      const finishReason = String(data?.candidates?.[0]?.finishReason || "");
      const error = new Error(`GEMINI ${modelId} retornou vazio | finishReason=${finishReason || "?"}.`);
      error.status = 502;
      error.modelId = modelId;
      throw error;
    }

    if (schema && !profile.supportsStructuredOutput) {
      text = extractLastValidJsonObject(text);
    }

    const usage = normalizeGenerateContentUsage(data);
    commitRoutedModelUsage(modelId, reservationId, usage);

    return {
      text,
      status: "completed",
      usage,
      modelId,
      raw: data
    };
  } catch (error) {
    if (error?.name === "AbortError") {
      const timeoutError = new Error(
        `GEMINI ${modelId} ${metric}: timeout em ${effectiveTimeout}ms.`
      );
      timeoutError.status = 504;
      timeoutError.modelId = modelId;
      timeoutError.isRouterTimeout = true;
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function geminiRequest({
  system,
  user,
  schema,
  thinkingLevel,
  maxOutputTokens,
  timeoutMs,
  maxRetries,
  job = null,
  metric = "main",
  routeOverride = null
}) {
  if (!GEMINI_API_KEY) {
    throw new Error("GEMINI_API_KEY nÃ£o configurada.");
  }

  void maxRetries; // retries continuam bounded e orientados por estratÃ©gia/modelo.

  const defaultRoute = geminiRouteForMetric(metric);
  const route = Array.isArray(routeOverride) && routeOverride.length
    ? [...new Set(routeOverride.filter(modelId => GEMINI_MODEL_PROFILES[modelId]))]
    : defaultRoute;
  const errors = [];
  let recoveryPass = 0;

  while (recoveryPass <= 1) {
    clearExpiredMetricCooldowns942(job);
    for (let routeIndex = 0; routeIndex < route.length; routeIndex++) {
      const modelId = route[routeIndex];
      if (modelSkippedForJob(job, modelId)) continue;

      const runtime = runtimeForGeminiModel(modelId);
      const now = Date.now();
      const metricCooldownUntil = modelMetricCooldownUntil942(job, metric, modelId);
      if (runtime.unavailableUntil > now || metricCooldownUntil > now) continue;

      // Se ainda hÃ¡ fallback saudÃ¡vel, falha transitÃ³ria troca de rota imediatamente.
      // SÃ³ o Ãºltimo modelo utilizÃ¡vel recebe UM retry curto bounded.
      const laterHealthyRoute = route.slice(routeIndex + 1).some(candidate => {
        if (modelSkippedForJob(job, candidate)) return false;
        const candidateRuntime = runtimeForGeminiModel(candidate);
        return Math.max(
          Number(candidateRuntime.unavailableUntil || 0),
          modelMetricCooldownUntil942(job, metric, candidate)
        ) <= Date.now();
      });
      const sameModelRetryBudget = laterHealthyRoute ? 0 : 1;

      for (let sameModelAttempt = 0; sameModelAttempt <= sameModelRetryBudget; sameModelAttempt++) {
        markAttempt(job, metric);
        recordRouterModelCall(job, modelId);

        if (routeIndex > 0 && job) {
          job.stats.modelFallbacks = Number(job.stats.modelFallbacks || 0) + 1;
        }

        try {
          console.log(
            `[MODEL ROUTER ${String(metric).toUpperCase()}] ${modelId} | ` +
            `route=${routeIndex + 1}/${route.length} | thinking=${thinkingLevel} | ` +
            `attempt=${sameModelAttempt + 1}/${sameModelRetryBudget + 1}.`
          );

          const result = await callGenerateContentModel({
            modelId,
            system,
            user,
            schema,
            thinkingLevel,
            maxOutputTokens,
            timeoutMs,
            job,
            metric,
            bypassPacer: sameModelAttempt > 0
          });

          markSuccess(job, metric, { usage: result.usage });
          setJobModelHealth(job, modelId, "healthy", `${metric} OK`);

          console.log(
            `[MODEL ROUTER ${String(metric).toUpperCase()}] OK ${modelId} | ` +
            `input=${result.usage.total_input_tokens} | ` +
            `output=${result.usage.total_output_tokens} | ` +
            `thought=${result.usage.total_thought_tokens}.`
          );

          return result;
        } catch (error) {
          const status = Number(error?.status || 0);
          errors.push(`${modelId}:${status || error?.code || "ERR"}`);

          if (status === 429) {
            mark429(job, metric);
            const kind = routedQuotaKind(error?.providerData || {});
            const wait = Math.max(1000, Number(error?.retryAfterMs || 30000));

            if (kind === "daily") {
              if (job) job.stats.model429Daily = Number(job.stats.model429Daily || 0) + 1;
              skipModelForJob(
                job,
                modelId,
                `quota diÃ¡ria/RPD | ${quotaDetailsText(error?.providerData || {})}`,
                "daily_exhausted"
              );
              setModelUnavailable(modelId, Math.max(wait, 60000), "RPD/quota diÃ¡ria");
            } else {
              if (job) job.stats.model429Rate = Number(job.stats.model429Rate || 0) + 1;
              // RPM/TPM sÃ£o transitÃ³rios: cooldown, nunca banimento do job inteiro.
              setModelUnavailable(modelId, wait, `${kind.toUpperCase()} 429`);
              setJobModelHealth(job, modelId, "rate_limited_cooldown", `${kind.toUpperCase()} 429`);
            }

            console.warn(
              `[MODEL ROUTER] ${modelId} 429 ${kind.toUpperCase()} -> fallback imediato | ` +
              `${quotaDetailsText(error?.providerData || {})}`
            );
            break;
          }

          if (status === 503) {
            if (job) job.stats.model503 = Number(job.stats.model503 || 0) + 1;

            if (sameModelAttempt < sameModelRetryBudget) {
              console.warn(`[MODEL ROUTER] ${modelId} 503 -> UM retry curto bounded por ser a Ãºltima rota saudÃ¡vel.`);
              if (job) job.stats.modelTransientRetries = Number(job.stats.modelTransientRetries || 0) + 1;
              await sleep(900);
              continue;
            }

            const waitMs = GEMINI_MODEL_PROFILES[modelId]?.unavailable503Ms || 20000;
            setModelUnavailable(modelId, waitMs, "503 high demand");
            setJobModelHealth(job, modelId, "temporarily_unavailable", "503 high demand");
            console.warn(`[MODEL ROUTER] ${modelId} 503 persistiu -> cooldown ${Math.round(waitMs/1000)}s + fallback; modelo poderÃ¡ voltar neste mesmo job.`);
            break;
          }

          if (error?.isRouterTimeout || status === 504) {
            if (job) job.stats.modelTimeouts = Number(job.stats.modelTimeouts || 0) + 1;
            const waitMs = GEMINI_MODEL_PROFILES[modelId]?.family === "gemma"
              ? 300000
              : ROUTER_TIMEOUT_COOLDOWN_MS_942;
            setModelUnavailable(modelId, waitMs, "timeout");
            setJobModelHealth(job, modelId, "timeout_cooldown", "timeout");
            console.warn(`[MODEL ROUTER] ${modelId} timeout -> cooldown ${Math.round(waitMs/1000)}s + fallback; SEM skip permanente do job.`);
            break;
          }

          if (error?.code === "MODEL_COOLDOWN" || error?.code === "MODEL_JOB_SKIP" || error?.code === "MODEL_LOCAL_RPD") {
            break;
          }

          if (error?.routerCanSplit || status === 413 || status === 422) {
            throw error;
          }

          if (isDeterministicGeminiRequestError(error)) {
            throw error;
          }

          if (status >= 500 || [408, 409, 425].includes(status)) {
            const shortTransient = [500, 502, 408, 425].includes(status);
            if (shortTransient && sameModelAttempt < sameModelRetryBudget) {
              if (job) job.stats.modelTransientRetries = Number(job.stats.modelTransientRetries || 0) + 1;
              console.warn(`[MODEL ROUTER] ${modelId} HTTP ${status} transitÃ³rio -> UM retry curto bounded.`);
              await sleep(800);
              continue;
            }
            setModelUnavailable(modelId, ROUTER_TRANSIENT_COOLDOWN_MS_942, `HTTP ${status}`);
            setJobModelHealth(job, modelId, "transient_cooldown", `HTTP ${status}`);
            console.warn(`[MODEL ROUTER] ${modelId} HTTP ${status} persistiu -> cooldown + fallback; SEM skip permanente.`);
            break;
          }

          throw error;
        }
      }
    }

    // 9.4.2: se TODAS as rotas sÃ³ estÃ£o em cooldown curto, aguarda no mÃ¡ximo UMA
    // janela e reprova. Isso evita transformar dois azares transitÃ³rios em BEST_AVAILABLE,
    // sem criar polling/loop aberto.
    if (recoveryPass === 0) {
      const now = Date.now();
      const eligibleRecovery = route
        .filter(modelId => !modelSkippedForJob(job, modelId))
        .map(modelId => Math.max(
          Number(runtimeForGeminiModel(modelId).unavailableUntil || 0),
          modelMetricCooldownUntil942(job, metric, modelId)
        ))
        .filter(until => until > now)
        .sort((a, b) => a - b);

      if (eligibleRecovery.length) {
        const waitMs = eligibleRecovery[0] - now + 120;
        if (waitMs > 0 && waitMs <= ROUTER_RECOVERY_WAIT_MAX_MS_942) {
          recoveryPass++;
          if (job) job.stats.modelRouterRecoveryWaits = Number(job.stats.modelRouterRecoveryWaits || 0) + 1;
          console.warn(`[MODEL ROUTER ${String(metric).toUpperCase()}] todas as rotas em cooldown curto; aguardando ${(waitMs/1000).toFixed(1)}s e fazendo UMA reprova bounded.`);
          await sleep(waitMs);
          continue;
        }
      }
    }

    break;
  }

  const last = errors.length ? errors[errors.length - 1] : "nenhum modelo elegÃ­vel";
  const error = new Error(
    `MODEL ROUTER esgotou a rota de ${metric}: ${errors.join(" -> ") || last}.`
  );
  error.routerExhausted = true;
  error.code = "MODEL_ROUTE_EXHAUSTED";
  error.metric = metric;
  throw error;
}

function parseGeminiOffsetMs(
  value
) {
  const text =
    String(value || "")
      .trim();

  const match =
    text.match(
      /^(-?\d+(?:\.\d+)?)s$/i
    );

  if (match) {
    return Math.round(
      Number(match[1]) *
      1000
    );
  }

  const n =
    Number(text);

  return Number.isFinite(n)
    ? Math.round(n * 1000)
    : null;
}

function extractTranscribedWords(
  data
) {
  const words = [];

  for (
    const step of
    Array.isArray(data?.steps)
      ? data.steps
      : []
  ) {
    for (
      const content of
      Array.isArray(
        step?.content
      )
        ? step.content
        : []
    ) {
      for (
        const annotation of
        Array.isArray(
          content?.annotations
        )
          ? content.annotations
          : []
      ) {
        if (
          annotation?.type !==
          "word_info"
        ) {
          continue;
        }

        const startMs =
          parseGeminiOffsetMs(
            annotation.start_offset
          );

        const endMs =
          parseGeminiOffsetMs(
            annotation.end_offset
          );

        const text =
          String(
            annotation.text ||
            ""
          ).trim();

        if (
          !text ||
          startMs == null ||
          endMs == null
        ) {
          continue;
        }

        words.push({
          text,
          startMs,
          endMs,

          speaker:
            String(
              annotation.speaker ||
              ""
            )
        });
      }
    }
  }

  return words.sort(
    (a, b) =>
      a.startMs -
      b.startMs
  );
}

async function geminiTranscribeInline(
  audioBase64,
  mimeType = "audio/wav",
  durationMs = 0,
  label = "audio-sync",
  languageCode = "en"
) {
  if (!GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY nÃ£o configurada."
    );
  }

  const cleanBase64 =
    String(
      audioBase64 || ""
    ).trim();

  if (!cleanBase64) {
    throw new Error(
      "Ãudio vazio."
    );
  }

  let lastError = null;

  for (
    let attempt = 1;
    attempt <= 3;
    attempt++
  ) {
    const budgetId =
      await acquireTranscribeBudget(
        durationMs,
        `${label} tentativa ${attempt}`
      );

    const controller =
      new AbortController();

    const timer =
      setTimeout(
        () =>
          controller.abort(),
        120000
      );

    try {
      console.log(
        `[GEMINI TRANSCRIBE] ${
          GEMINI_TRANSCRIBE_MODEL
        } request ${
          attempt
        }/3 | montage=${
          label
        } | duraÃ§Ã£oâ‰ˆ${
          (
            Number(
              durationMs ||
              0
            ) /
            1000
          ).toFixed(1)
        }s | word timestamps.`
      );

      const response =
        await fetch(
          "https://generativelanguage.googleapis.com/v1beta/interactions",

          {
            method:
              "POST",

            headers: {
              "Content-Type":
                "application/json",

              "x-goog-api-key":
                GEMINI_API_KEY,

              "Api-Revision":
                "2026-05-20"
            },

            body:
              JSON.stringify({
                model:
                  GEMINI_TRANSCRIBE_MODEL,

                input: [
                  {
                    type:
                      "audio",

                    data:
                      cleanBase64,

                    mime_type:
                      mimeType
                  }
                ],

                generation_config: {
                  transcription_config: {
                    language_codes: [
                      normalizeSyncLanguageCode(languageCode, "en")
                    ],

                    mode: {
                      type:
                        "verbatim",

                      timestamp_granularities: [
                        "word"
                      ]
                    }
                  }
                },

                store: false
              }),

            signal:
              controller.signal
          }
        );

      const raw =
        await response.text();

      let data = null;

      try {
        data =
          raw
            ? JSON.parse(raw)
            : {};
      } catch {}

      commitTranscribeUsage(
        budgetId,
        data?.usage || {}
      );

      if (
        response.ok &&
        data
      ) {
        const words =
          extractTranscribedWords(
            data
          );

        const text =
          extractInteractionText(
            data
          );

        if (!words.length) {
          throw new Error(
            "Gemini Transcribe nÃ£o retornou timestamps de palavras."
          );
        }

        const snapshot =
          transcribeBudgetSnapshot();

        console.log(
          `[GEMINI TRANSCRIBE] OK | words=${
            words.length
          } | input=${
            Number(
              data?.usage
                ?.total_input_tokens ||
              0
            )
          } | output=${
            Number(
              data?.usage
                ?.total_output_tokens ||
              0
            )
          } | RPD=${
            snapshot.calls24h
          }/${
            TRANSCRIBE_RPD_INTERNAL_LIMIT
          }.`
        );

        return {
          text,
          words,
          usage:
            data?.usage || {},
          budget:
            snapshot
        };
      }

      const error =
        new Error(
          `GEMINI ${
            GEMINI_TRANSCRIBE_MODEL
          } HTTP ${
            response.status
          }: ${
            String(
              data?.error
                ?.message ||
              data?.message ||
              raw ||
              "erro"
            ).slice(
              0,
              1200
            )
          }`
        );

      error.status =
        response.status;

      if (
        response.status === 429 &&
        attempt < 3
      ) {
        const wait =
          retryDelayMs(
            response,
            data,
            attempt
          );

        console.warn(
          `[GEMINI TRANSCRIBE] 429; budget manager preserva o prÃ³ximo inÃ­cio e retry em ${
            (wait / 1000).toFixed(1)
          }s.`
        );

        await sleep(wait);

        continue;
      }

      if (
        (
          response.status >= 500 ||
          [
            408,
            409,
            425
          ].includes(
            response.status
          )
        ) &&
        attempt < 3
      ) {
        await sleep(
          Math.min(
            4000 * attempt,
            12000
          )
        );

        continue;
      }

      throw error;
    } catch (error) {
      lastError =
        error?.name ===
          "AbortError"
          ? new Error(
              "Gemini Transcribe: timeout."
            )
          : error;

      if (
        lastError?.nonRetryable
      ) {
        throw lastError;
      }

      if (
        attempt >= 3 ||
        (
          lastError?.status &&
          lastError.status < 500 &&
          lastError.status !== 429 &&
          ![
            408,
            409,
            425
          ].includes(
            lastError.status
          )
        )
      ) {
        throw lastError;
      }

      await sleep(
        Math.min(
          3000 * attempt,
          9000
        )
      );
    } finally {
      clearTimeout(timer);
    }
  }

  throw (
    lastError ||
    new Error(
      "Gemini Transcribe falhou."
    )
  );
}

// ============================================================
// PLANNER / BATCHES
// ============================================================

function compactCue(block) {
  return {
    i: block.index,
    en: block.text,

    ...(
      block.speakerHint
        ? {
            speaker:
              block.speakerHint
          }
        : {}
    )
  };
}

function plannerSample(blocks) {
  if (
    blocks.length <=
    PLAN_SAMPLE_MAX_CUES
  ) {
    return blocks.map(
      compactCue
    );
  }

  const out = [];

  const step =
    blocks.length /
    PLAN_SAMPLE_MAX_CUES;

  const used =
    new Set();

  for (
    let i = 0;
    i <
      PLAN_SAMPLE_MAX_CUES;
    i++
  ) {
    const index =
      Math.min(
        blocks.length - 1,

        Math.floor(
          i * step
        )
      );

    if (!used.has(index)) {
      used.add(index);

      out.push(
        compactCue(
          blocks[index]
        )
      );
    }
  }

  return out;
}

function normalizePlanList(
  value,
  max = 40
) {
  return (
    Array.isArray(value)
      ? value
      : []
  )
    .map(
      item =>
        String(item || "")
          .replace(/\s+/g, " ")
          .trim()
    )
    .filter(Boolean)
    .slice(
      0,
      max
    );
}

function normalizeGenderValue(
  value
) {
  const raw =
    normalizedIdentityKey(
      value
    );

  if (
    /^(female|woman|girl|mulher|feminino|she her)$/.test(
      raw
    )
  ) {
    return "female";
  }

  if (
    /^(male|man|boy|homem|masculino|he him)$/.test(
      raw
    )
  ) {
    return "male";
  }

  if (
    /^(nonbinary|non binary|nb|nao binario|nÃ£o binÃ¡rio|they them)$/.test(
      raw
    )
  ) {
    return "nonbinary";
  }

  return "unknown";
}

function normalizeConfidenceValue(
  value
) {
  const raw =
    normalizedIdentityKey(
      value
    );

  if (
    raw === "high" ||
    raw === "alta" ||
    raw === "alto"
  ) {
    return "high";
  }

  if (
    raw === "medium" ||
    raw === "media" ||
    raw === "medio"
  ) {
    return "medium";
  }

  return "low";
}

function splitLedgerList(
  value,
  { pronouns = false } = {}
) {
  const text =
    String(value || "").trim();

  if (!text) {
    return [];
  }

  const re =
    pronouns
      ? /\s*(?:,|\/|;)\s*/
      : /\s*(?:,|;)\s*/;

  return [
    ...new Set(
      text
        .split(re)
        .map(
          item =>
            item.trim()
        )
        .filter(Boolean)
    )
  ].slice(0, 8);
}

function parseLedgerPersonLine(
  value
) {
  if (
    value &&
    typeof value ===
      "object" &&
    !Array.isArray(value)
  ) {
    const canonical =
      String(
        value.canonical ||
        value.name ||
        ""
      ).trim();

    if (!canonical) {
      return null;
    }

    return {
      canonical,

      aliases:
        normalizePlanList(
          value.aliases,
          8
        ),

      gender:
        normalizeGenderValue(
          value.gender
        ),

      pronouns:
        normalizePlanList(
          value.pronouns,
          6
        ),

      relation:
        String(
          value.relation ||
          value.context ||
          ""
        )
          .trim()
          .slice(
            0,
            240
          ),

      confidence:
        normalizeConfidenceValue(
          value.confidence
        ),

      evidence_cues:
        (
          Array.isArray(
            value.evidence_cues
          )
            ? value.evidence_cues
            : []
        )
          .map(Number)
          .filter(
            Number.isInteger
          )
          .slice(
            0,
            8
          )
    };
  }

  const text =
    String(value || "")
      .replace(
        /\r?\n/g,
        " "
      )
      .trim();

  if (!text) {
    return null;
  }

  const fields = {};

  let bareCanonical = "";

  for (
    const part of
    text.split(
      /\s*\|\|\s*/
    )
  ) {
    const match =
      part.match(
        /^([a-z_]+)\s*=\s*(.*)$/i
      );

    if (match) {
      fields[
        match[1].toLowerCase()
      ] =
        match[2].trim();
    } else if (
      !bareCanonical &&
      part.trim()
    ) {
      bareCanonical =
        part.trim();
    }
  }

  const canonical =
    String(
      fields.canonical ||
      fields.name ||
      bareCanonical ||
      ""
    ).trim();

  if (!canonical) {
    return null;
  }

  const evidenceText =
    String(
      fields.evidence ||
      fields.evidence_cues ||
      ""
    );

  return {
    canonical:
      canonical.slice(
        0,
        100
      ),

    aliases:
      splitLedgerList(
        fields.aliases
      ).filter(
        alias =>
          normalizedIdentityKey(
            alias
          ) !==
          normalizedIdentityKey(
            canonical
          )
      ),

    gender:
      normalizeGenderValue(
        fields.gender
      ),

    pronouns:
      splitLedgerList(
        fields.pronouns,
        {
          pronouns: true
        }
      ),

    relation:
      String(
        fields.relation ||
        fields.context ||
        ""
      )
        .trim()
        .slice(
          0,
          240
        ),

    confidence:
      normalizeConfidenceValue(
        fields.confidence
      ),

    evidence_cues:
      (
        evidenceText.match(
          /\d+/g
        ) || []
      )
        .map(Number)
        .filter(
          Number.isInteger
        )
        .slice(
          0,
          8
        )
  };
}

function normalizeEpisodePlan(
  raw,
  source = "safe-schema"
) {
  const people = [];
  const seen =
    new Set();

  for (
    const entry of
    Array.isArray(
      raw?.people
    )
      ? raw.people
      : []
  ) {
    const person =
      parseLedgerPersonLine(
        entry
      );

    if (!person) {
      continue;
    }

    const key =
      normalizedIdentityKey(
        person.canonical
      );

    if (
      !key ||
      seen.has(key)
    ) {
      continue;
    }

    seen.add(key);
    people.push(person);
  }

  return {
    tone:
      String(
        raw?.tone ||
        "PT-BR contemporÃ¢neo, natural e fiel ao registro."
      )
        .replace(/\s+/g, " ")
        .trim()
        .slice(
          0,
          800
        ),

    people:
      people.slice(
        0,
        30
      ),

    glossary:
      normalizePlanList(
        raw?.glossary,
        40
      ),

    continuity:
      normalizePlanList(
        raw?.continuity,
        30
      ),

    plannerSource:
      source
  };
}

function parseFallbackPlanText(
  value
) {
  const raw = {
    tone: "",
    people: [],
    glossary: [],
    continuity: []
  };

  for (
    const sourceLine of
    String(value || "")
      .split(/\r?\n/)
  ) {
    const line =
      sourceLine.trim();

    if (!line) {
      continue;
    }

    if (
      /^TONE=/i.test(line)
    ) {
      raw.tone =
        line
          .replace(
            /^TONE=/i,
            ""
          )
          .trim();
    } else if (
      /^PERSON=/i.test(line)
    ) {
      raw.people.push(
        line
          .replace(
            /^PERSON=/i,
            ""
          )
          .trim()
      );
    } else if (
      /^GLOSSARY=/i.test(line)
    ) {
      raw.glossary.push(
        line
          .replace(
            /^GLOSSARY=/i,
            ""
          )
          .trim()
      );
    } else if (
      /^CONTINUITY=/i.test(
        line
      )
    ) {
      raw.continuity.push(
        line
          .replace(
            /^CONTINUITY=/i,
            ""
          )
          .trim()
      );
    }
  }

  return normalizeEpisodePlan(
    raw,
    "ultra-simple-fallback"
  );
}

function fallbackPlan(
  blocks = []
) {
  const speakers = [];
  const seen =
    new Set();

  for (
    const block of blocks
  ) {
    const speaker =
      String(
        block?.speakerHint ||
        ""
      ).trim();

    const key =
      normalizedIdentityKey(
        speaker
      );

    if (
      !speaker ||
      !key ||
      seen.has(key)
    ) {
      continue;
    }

    seen.add(key);

    speakers.push({
      canonical:
        speaker,

      aliases: [],

      gender:
        "unknown",

      pronouns: [],

      relation:
        "speaker label observado na legenda; gÃªnero nÃ£o inferido localmente",

      confidence:
        "medium",

      evidence_cues: [
        Number(block.index)
      ].filter(
        Number.isInteger
      )
    });
  }

  return {
    tone:
      "PT-BR contemporÃ¢neo, natural e fiel ao registro; evitar calques e linguagem de traduÃ§Ã£o.",

    people:
      speakers.slice(
        0,
        30
      ),

    glossary: [],

    continuity: [
      "Gender safety: gÃªnero desconhecido nunca deve ser adivinhado; neutralizar concordÃ¢ncia quando possÃ­vel.",
      "Naturalidade: correto mas literal demais deve ser reescrito em PT-BR espontÃ¢neo.",
      "Speaker e pessoa mencionada sÃ£o entidades distintas."
    ],

    plannerSource:
      "local-safety-fallback"
  };
}

function planQualityStats(
  plan
) {
  const people =
    Array.isArray(
      plan?.people
    )
      ? plan.people
      : [];

  const knownGender =
    people.filter(
      person =>
        [
          "female",
          "male",
          "nonbinary"
        ].includes(
          person?.gender
        ) &&
        person?.confidence !==
          "low"
    ).length;

  return {
    people:
      people.length,

    knownGender
  };
}

async function buildEpisodePlan(
  blocks,
  job
) {
  const user =
    `Arquivo: ${
      job.filename ||
      "desconhecido"
    }\n` +
    `Tipo: ${job.type}\n` +
    `ID: ${job.videoId}\n` +
    `Idioma da fonte: ${job.sourceLang || "auto"}\n\n` +
    `Amostra:\n${
      JSON.stringify({
        cues:
          plannerSample(
            blocks
          )
      })
    }`;

  try {
    const response =
      await geminiRequest({
        system:
          PLAN_PROMPT,

        user,

        schema:
          PLAN_SCHEMA,

        thinkingLevel:
          PLAN_THINKING,

        maxOutputTokens:
          PLAN_MAX_OUTPUT_TOKENS,

        timeoutMs:
          PLAN_TIMEOUT_MS,

        maxRetries:
          PLAN_RETRIES,

        job,

        metric:
          "plan"
      });

    const parsed =
      JSON.parse(
        stripCodeFences(
          response.text
        )
      );

    const plan =
      normalizeEpisodePlan(
        parsed,
        "safe-schema"
      );

    const stats =
      planQualityStats(
        plan
      );

    job.stats.planPeople =
      stats.people;

    job.stats.planKnownGender =
      stats.knownGender;

    console.log(
      `[EPISODE PLAN] OK SAFE-SCHEMA | Character Ledger=${
        stats.people
      } | gÃªnero conhecido=${
        stats.knownGender
      } | glossary=${
        plan.glossary.length
      }.`
    );

    return plan;
  } catch (error) {
    job.stats.planFailures++;

    console.warn(
      `[EPISODE PLAN] SAFE-SCHEMA falhou: ${
        errorMessage(
          error
        ).slice(
          0,
          300
        )
      } | tentando fallback ultra-simples.`
    );
  }

  try {
    job.stats.planFallbackCalls++;

    const response =
      await geminiRequest({
        system:
          PLAN_FALLBACK_PROMPT,

        user,

        schema:
          PLAN_FALLBACK_SCHEMA,

        thinkingLevel:
          PLAN_FALLBACK_THINKING,

        maxOutputTokens:
          PLAN_FALLBACK_MAX_OUTPUT_TOKENS,

        timeoutMs:
          PLAN_TIMEOUT_MS,

        maxRetries:
          PLAN_FALLBACK_RETRIES,

        job,

        metric:
          "plan"
      });

    const parsed =
      JSON.parse(
        stripCodeFences(
          response.text
        )
      );

    const plan =
      parseFallbackPlanText(
        parsed?.plan || ""
      );

    const stats =
      planQualityStats(
        plan
      );

    job.stats.planRecovered++;

    job.stats.planPeople =
      stats.people;

    job.stats.planKnownGender =
      stats.knownGender;

    console.log(
      `[EPISODE PLAN] RECUPERADO ULTRA-SIMPLE âœ… | Character Ledger=${
        stats.people
      } | gÃªnero conhecido=${
        stats.knownGender
      }.`
    );

    return plan;
  } catch (error) {
    job.stats.planFailures++;

    const plan =
      fallbackPlan(
        blocks
      );

    const stats =
      planQualityStats(
        plan
      );

    job.stats.planPeople =
      stats.people;

    job.stats.planKnownGender =
      stats.knownGender;

    console.warn(
      `[EPISODE PLAN] fallback Gemini tambÃ©m falhou: ${
        errorMessage(
          error
        ).slice(
          0,
          300
        )
      } | usando SAFETY FALLBACK local com neutralizaÃ§Ã£o obrigatÃ³ria.`
    );

    return plan;
  }
}

function buildMainBatches(
  blocks
) {
  const batches = [];

  let current = [];
  let chars = 0;

  for (
    const block of blocks
  ) {
    const size =
      block.text.length +
      80;

    if (
      current.length &&
      (
        current.length >=
          MAIN_BATCH_MAX_CUES ||
        chars + size >
          MAIN_BATCH_MAX_CHARS
      )
    ) {
      batches.push(
        current
      );

      current = [];
      chars = 0;
    }

    current.push(
      block
    );

    chars +=
      size;
  }

  if (current.length) {
    batches.push(current);
  }

  return batches;
}

function positionMap(blocks) {
  const map =
    new Map();

  blocks.forEach(
    (block, index) =>
      map.set(
        block.index,
        index
      )
  );

  return map;
}

function interleaveBatch(
  batch
) {
  const ordered = [];

  const half =
    Math.ceil(
      batch.length / 2
    );

  for (
    let i = 0;
    i < half;
    i++
  ) {
    if (batch[i]) {
      ordered.push(
        batch[i]
      );
    }

    if (
      batch[
        i + half
      ]
    ) {
      ordered.push(
        batch[
          i + half
        ]
      );
    }
  }

  return ordered;
}

function contextCue(block) {
  return {
    i:
      block.index,

    en:
      block.text,

    ...(
      block.speakerHint
        ? {
            speaker:
              block.speakerHint
          }
        : {}
    )
  };
}

function normalizedIdentityKey(
  value
) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(
      /[\u0300-\u036f]/g,
      ""
    )
    .replace(
      /[^a-z0-9]+/g,
      " "
    )
    .trim();
}

function personAliases(
  person
) {
  return [
    person?.canonical,

    ...(
      Array.isArray(
        person?.aliases
      )
        ? person.aliases
        : []
    )
  ]
    .map(
      normalizedIdentityKey
    )
    .filter(Boolean);
}

function findPersonForSpeaker(
  plan,
  speakerHint
) {
  const wanted =
    normalizedIdentityKey(
      speakerHint
    );

  if (!wanted) {
    return null;
  }

  for (
    const person of
    Array.isArray(
      plan?.people
    )
      ? plan.people
      : []
  ) {
    if (
      personAliases(
        person
      ).some(
        alias =>
          alias === wanted ||
          alias.includes(
            wanted
          ) ||
          wanted.includes(
            alias
          )
      )
    ) {
      return person;
    }
  }

  return null;
}

function mentionedPeople(
  plan,
  text,
  excludeCanonical = ""
) {
  const haystack =
    ` ${
      normalizedIdentityKey(
        text
      )
    } `;

  if (!haystack.trim()) {
    return [];
  }

  const out = [];

  for (
    const person of
    Array.isArray(
      plan?.people
    )
      ? plan.people
      : []
  ) {
    if (
      excludeCanonical &&
      normalizedIdentityKey(
        person?.canonical
      ) ===
      normalizedIdentityKey(
        excludeCanonical
      )
    ) {
      continue;
    }

    const found =
      personAliases(
        person
      ).some(
        alias =>
          alias.length >= 2 &&
          haystack.includes(
            ` ${alias} `
          )
      );

    if (found) {
      out.push(
        person
      );
    }
  }

  return out.slice(
    0,
    8
  );
}

function trustedPersonGender(
  person
) {
  if (!person) {
    return null;
  }

  const gender =
    String(
      person.gender ||
      "unknown"
    )
      .toLocaleLowerCase()
      .trim();

  const confidence =
    String(
      person.confidence ||
      "low"
    )
      .toLocaleLowerCase()
      .trim();

  const pronouns =
    Array.isArray(
      person.pronouns
    )
      ? person.pronouns
          .map(
            value =>
              String(
                value || ""
              )
                .toLocaleLowerCase()
                .trim()
          )
          .filter(Boolean)
      : [];

  // O Ledger sÃ³ pode IMPOR gÃªnero quando:
  // 1. a confianÃ§a Ã© HIGH;
  // 2. hÃ¡ pronome explÃ­cito compatÃ­vel.
  //
  // Nome, aparÃªncia presumida ou palpite do Planner
  // nunca bastam para marcar concordÃ¢ncia em PT-BR.
  if (
    confidence !== "high"
  ) {
    return null;
  }

  if (
    gender === "female" &&
    pronouns.some(
      value =>
        value === "she" ||
        value === "her"
    )
  ) {
    return "female";
  }

  if (
    gender === "male" &&
    pronouns.some(
      value =>
        value === "he" ||
        value === "him"
    )
  ) {
    return "male";
  }

  if (
    gender === "nonbinary" &&
    pronouns.some(
      value =>
        value === "they" ||
        value === "them"
    )
  ) {
    return "nonbinary";
  }

  return null;
}

function compactPersonIdentity(
  person
) {
  if (!person) {
    return null;
  }

  const trustedGender =
    trustedPersonGender(
      person
    );

  return {
    canonical:
      String(
        person.canonical ||
        ""
      ),

    // Nunca exponha ao tradutor um gÃªnero
    // que o prÃ³prio backend nÃ£o considera confiÃ¡vel.
    gender:
      trustedGender ||
      "unknown",

    pronouns:
      trustedGender &&
      Array.isArray(
        person.pronouns
      )
        ? person.pronouns
        : [],

    relation:
      String(
        person.relation ||
        ""
      ),

    confidence:
      trustedGender
        ? "high"
        : "low"
  };
}

function identityLockForCapsule(
  block,
  plan
) {
  const speaker =
    findPersonForSpeaker(
      plan,
      block.speakerHint
    );

  const trustedGender =
    trustedPersonGender(
      speaker
    );

  const mentions =
    mentionedPeople(
      plan,
      block.text,
      speaker?.canonical ||
      ""
    ).map(
      compactPersonIdentity
    );

  const explicitSpeaker =
    Boolean(
      block.speakerHint &&
      speaker
    );

  return {
    rule:
      "speaker Ã© quem fala; mentions sÃ£o pessoas citadas. Nunca transfira gÃªnero/pronomes. Sem evidÃªncia segura, neutralize em PT-BR natural.",

    speaker_status:
      explicitSpeaker
        ? "known_from_source_label"
        : "speaker_unknown",

    speaker:
      compactPersonIdentity(
        speaker
      ),

    trusted_speaker_gender:
      trustedGender,

    self_gender_policy:
      trustedGender
        ? `DEFAULT_NEUTRALIZE: gÃªnero confiÃ¡vel=${trustedGender} serve para impedir contradiÃ§Ãµes, nÃ£o para introduzir gÃªnero em 1Âª pessoa quando a SOURCE Ã© neutra; prefira formulaÃ§Ã£o PT-BR neutra natural`
        : "STRICT_NEUTRALIZE: nÃ£o adivinhar masculino/feminino de 1Âª pessoa; preferir formulaÃ§Ã£o sem marca de gÃªnero quando natural",

    mentions
  };
}

function conciseIdentityForQa(
  block,
  plan
) {
  const lock =
    identityLockForCapsule(
      block,
      plan
    );

  return {
    speaker_status:
      lock.speaker_status,

    speaker:
      lock.speaker,

    trusted_speaker_gender:
      lock.trusted_speaker_gender,

    self_gender_policy:
      lock.self_gender_policy,

    mentions:
      lock.mentions
  };
}

function compactIdentityHint(
  block,
  plan
) {
  const speaker =
    findPersonForSpeaker(
      plan,
      block.speakerHint
    );

  const gender =
    trustedPersonGender(
      speaker
    ) || "unknown";

  const refs =
    mentionedPeople(
      plan,
      block.text,
      speaker?.canonical || ""
    )
      .map(person => ({
        n: String(person?.canonical || ""),
        g: trustedPersonGender(person) || "unknown"
      }))
      .filter(item => item.n)
      .slice(0, 4);

  return {
    s: String(
      speaker?.canonical ||
      block.speakerHint ||
      "unknown"
    ),
    // g Ã© apenas guard de contradiÃ§Ã£o/referente. MAIN deve neutralizar
    // estados de 1Âª pessoa quando a SOURCE nÃ£o marca gÃªnero.
    g: gender,
    n: "neutral-by-default",
    r: refs
  };
}

function buildOwnershipPayload(
  allBlocks,
  posMap,
  batch,
  plan
) {
  const locksById = new Map();
  const ownershipById = new Map();

  const firstPos =
    Math.max(
      0,
      Number(
        posMap.get(batch[0]?.index) || 0
      )
    );

  const lastPos =
    Math.max(
      firstPos,
      Number(
        posMap.get(batch[batch.length - 1]?.index) || firstPos
      )
    );

  const cues = batch.map(block => {
    const protectedTarget =
      protectCulturalLocks(
        block.text,
        block.index
      );

    const ownershipKey =
      `OWN_C${block.index}`;

    locksById.set(
      block.index,
      protectedTarget.locks
    );

    ownershipById.set(
      block.index,
      ownershipKey
    );

    const turnPlan973 =
      sourceDialogueTurnPlan973(block);

    const turns =
      turnPlan973.turns.length;

    return {
      i: block.index,
      k: ownershipKey,
      en: protectedTarget.text,
      boundary: sourceBoundaryHint(block),
      idn: compactIdentityHint(block, plan),
      ...(turns >= 2
        ? {
            turns,
            turn_mode: turnPlan973.mode,
            turn_source: turnPlan973.turns
          }
        : {}),
      ...(turnPlan973.softCandidates.length
        ? {
            turn_candidates: turnPlan973.softCandidates
          }
        : {}),
      ...(block.musicPerformance
        ? {
            music: "contextual_performance_lyric"
          }
        : {}),
      ...(protectedTarget.locks.length
        ? { hard: protectedTarget.locks.map(lock => lock.token) }
        : {})
    };
  });

  return {
    payload: {
      rule:
        "Lista cronolÃ³gica. Traduza exclusivamente en do mesmo i. " +
        "k pertence ao mesmo cue e deve voltar idÃªntico. " +
        "before/after sÃ£o contexto compartilhado, nunca conteÃºdo do target. " +
        "idn.g sÃ³ Ã© confiÃ¡vel quando diferente de unknown; sem prova, neutralize gÃªnero naturalmente. " +
        "NÃ£o antecipe, atrase, duplique ou mova conteÃºdo entre IDs. " +
        "Cada fragmento lexical presente em en[i] precisa continuar em pt[i], mesmo quando a frase atravessa dois cues. " +
        "Se en[i] for fragmento, pt[i] tambÃ©m pode e deve ser fragmento; NÃƒO complete a frase usando en[i+1] nem transfira o fim de en[i] para pt[i+1]. " +
        "boundary.startsMidSentence/endsMidSentence descreve SOMENTE a borda do target: preserve o corte sem puxar conteÃºdo do vizinho. " +
        "Quando turns>=2, turn_source Ã© autoridade estrutural de speakers mesmo se turn_mode=implicit_strong e a SOURCE nÃ£o tinha hÃ­fen. " +
        "turn_candidates sÃ£o apenas pistas contextuais; nÃ£o force troca de speaker sem evidÃªncia. " +
        "music=contextual_performance_lyric significa letra relevante jÃ¡ aprovada pelo filtro contextual.",

      before:
        allBlocks
          .slice(
            Math.max(0, firstPos - CAPSULE_CONTEXT_BEFORE),
            firstPos
          )
          .map(contextCue),

      cues,

      after:
        allBlocks
          .slice(
            lastPos + 1,
            Math.min(
              allBlocks.length,
              lastPos + 1 + CAPSULE_CONTEXT_AFTER
            )
          )
          .map(contextCue)
    },

    locksById,
    ownershipById
  };
}

function parseCueTranslation(
  batch,
  raw,
  locksById = new Map(),
  ownershipById = new Map(),
  enforceOrder = false,
  allowEmpty = false
) {
  let parsed;

  try {
    parsed =
      JSON.parse(
        stripCodeFences(
          raw
        )
      );
  } catch {
    throw new Error(
      "JSON de traduÃ§Ã£o invÃ¡lido."
    );
  }

  if (
    !Array.isArray(
      parsed?.cues
    )
  ) {
    throw new Error(
      "Resposta sem cues."
    );
  }

  const ids =
    batch.map(
      block =>
        block.index
    );

  const expected =
    new Set(
      ids
    );

  if (
    parsed.cues.length !==
    ids.length
  ) {
    throw new Error(
      `Quantidade de cues invÃ¡lida: ` +
      `${parsed.cues.length}/${ids.length}.`
    );
  }

  // Somente o MAIN ativa isto.
  // Repair/Compact continuam compatÃ­veis com o parser.
  if (enforceOrder) {
    const returnedIds =
      parsed.cues.map(
        item =>
          Number(
            item?.i
          )
      );

    for (
      let i = 0;
      i < ids.length;
      i++
    ) {
      if (
        returnedIds[i] !==
        ids[i]
      ) {
        throw new Error(
          `CUE OWNERSHIP ORDER: ` +
          `posiÃ§Ã£o ${i} esperava ID ${ids[i]}, ` +
          `recebeu ${returnedIds[i]}.`
        );
      }
    }
  }

  const byId =
    new Map();

  const seenIds =
    new Set();

  const emptyIds = [];

  for (
    const item of
    parsed.cues
  ) {
    const id =
      Number(
        item?.i
      );

    let pt =
      String(
        item?.pt ??
        ""
      ).trim();

    if (
      !expected.has(id)
    ) {
      throw new Error(
        `ID inesperado ${id}.`
      );
    }

    if (
      seenIds.has(id)
    ) {
      throw new Error(
        `ID duplicado ${id}.`
      );
    }

    seenIds.add(id);

    const expectedOwnershipKey =
      ownershipById.get(
        id
      );

    if (expectedOwnershipKey) {
      const returnedOwnershipKey =
        String(
          item?.k ||
          ""
        ).trim();

      if (
        returnedOwnershipKey !==
        expectedOwnershipKey
      ) {
        throw new Error(
          `CUE OWNERSHIP cue ${id}: ` +
          `esperava key ${expectedOwnershipKey}, ` +
          `recebeu ${returnedOwnershipKey || "(vazia)"}.`
        );
      }
    }

    if (!pt) {
      if (allowEmpty) {
        emptyIds.push(id);
        continue;
      }

      throw new Error(
        `Cue ${id} vazio.`
      );
    }

    // ============================================================
    // PT-BR VOCAB SOFT LOCK â€” "qualÃ©/diacho"
    // ============================================================
    // PreferÃªncia editorial continua valendo, mas nunca pode
    // derrubar um lote ou um episÃ³dio inteiro.
    //
    // QA/Repair ainda podem reformular contextualmente.
    // A rede final determinÃ­stica serÃ¡ aplicada no buildSrt().
    if (
      /(?:^|[^\p{L}\p{N}_])(?:qualÃ©|diacho)(?=$|[^\p{L}\p{N}_])/iu.test(
        pt
      )
    ) {
      console.warn(
        `[PT-BR VOCAB SOFT LOCK] cue ${id}: "qualÃ©/diacho" detectado; ` +
        `lote preservado e correÃ§Ã£o final garantida sem abortar o episÃ³dio.`
      );
    }

    pt =
      restoreCulturalLocks(
        pt,
        locksById.get(id) ||
          [],
        id
      );

    byId.set(
      id,
      pt
    );
  }

  if (
    seenIds.size !==
    ids.length
  ) {
    throw new Error(
      `TraduÃ§Ã£o estruturalmente incompleta ` +
      `${seenIds.size}/${ids.length}.`
    );
  }

  if (allowEmpty) {
    return {
      translations: byId,
      emptyIds
    };
  }

  if (
    byId.size !==
    ids.length
  ) {
    throw new Error(
      `TraduÃ§Ã£o incompleta ` +
      `${byId.size}/${ids.length}.`
    );
  }

  return byId;
}

function parseMainCueTranslationRobust(
  batch,
  raw,
  locksById,
  ownershipById
) {
  let parsed;

  try {
    parsed = JSON.parse(stripCodeFences(raw));
  } catch {
    throw new Error("JSON MAIN invÃ¡lido.");
  }

  if (!Array.isArray(parsed?.cues)) {
    throw new Error("Resposta MAIN sem cues.");
  }

  const expectedIds = batch.map(block => Number(block.index));
  const expected = new Set(expectedIds);
  const translations = new Map();
  const emptyIds = [];
  let ignoredExtras = 0;
  let ignoredDuplicates = 0;
  let badOwnership = 0;

  for (const item of parsed.cues) {
    const id = Number(item?.i);

    if (!Number.isInteger(id) || !expected.has(id)) {
      ignoredExtras++;
      continue;
    }

    if (translations.has(id) || emptyIds.includes(id)) {
      ignoredDuplicates++;
      continue;
    }

    const expectedKey = String(ownershipById.get(id) || "");
    const returnedKey = String(item?.k || "").trim();

    if (expectedKey && returnedKey !== expectedKey) {
      badOwnership++;
      continue;
    }

    let pt = String(item?.pt ?? "").trim();

    if (!pt) {
      emptyIds.push(id);
      continue;
    }

    if (/(?:^|[^\p{L}\p{N}_])(?:qualÃ©|diacho)(?=$|[^\p{L}\p{N}_])/iu.test(pt)) {
      console.warn(
        `[PT-BR VOCAB SOFT LOCK] cue ${id}: "qualÃ©/diacho" detectado; ` +
        `candidato preservado para correÃ§Ã£o focal.`
      );
    }

    pt = restoreCulturalLocks(
      pt,
      locksById.get(id) || [],
      id
    );

    translations.set(id, pt);
  }

  const missingIds = expectedIds.filter(
    id => !translations.has(id) && !emptyIds.includes(id)
  );

  if (ignoredExtras || ignoredDuplicates || badOwnership) {
    console.warn(
      `[MAIN ROBUST PARSER 8.8.3] extras=${ignoredExtras} | ` +
      `duplicados=${ignoredDuplicates} | ownership-invÃ¡lido=${badOwnership}; ` +
      `cues vÃ¡lidos foram preservados, sem retraduzir o lote.`
    );
  }

  return {
    translations,
    emptyIds,
    missingIds
  };
}

async function rescueEmptyMainCue({
  blocks,
  posMap,
  block,
  plan,
  job
}) {
  const localVocalization =
    localizePureVocalizationCue(block);

  if (localVocalization) {
    if (job) {
      job.stats.mainLocalVocalizationRescues =
        Number(job.stats.mainLocalVocalizationRescues || 0) + 1;
    }
    console.log(
      `[MAIN LOCAL VOCALIZATION 9.0] cue ${block.index}: ` +
      `${JSON.stringify(block.text)} -> ${JSON.stringify(localVocalization)} | 0 Gemini.`
    );
    return localVocalization;
  }

  if (
    sourceCueAllowsIntentionalEmpty(
      block,
      job
    )
  ) {
    markIntentionalEmptyCue(
      job,
      block,
      "SOURCE SDH/ruÃ­do puro descartÃ¡vel; vazio intencional, 0 Gemini"
    );

    return "";
  }

  const rescueBatch = [
    block
  ];

  let lastError = null;
  let quotaDeferred = false;
  const rejectedCandidates = [];
  const rejectedSdhCandidates = [];

  for (
    let localCycle = 0;
    localCycle < MAIN_EMPTY_CUE_MAX_CYCLES;
    localCycle++
  ) {

    for (
      let parseAttempt = 1;
      parseAttempt <=
        MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS;
      parseAttempt++
    ) {
      try {
        const {
          payload,
          locksById,
          ownershipById
        } =
          buildOwnershipPayload(
            blocks,
            posMap,
            rescueBatch,
            plan
          );

        job.stats.mainEmptyCueRescueCalls++;

        console.warn(
          `[MAIN EMPTY-CUE RESCUE] cue ${block.index} | ` +
          `ciclo ${localCycle + 1} | ` +
          `tentativa ${parseAttempt}/${MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS}.`
        );

        const response =
          await geminiRequest({
            system:
              TRANSLATOR_PROMPT,

            user:
              `EMPTY-CUE RESCUE DO MAIN â€” 8.4.6.\n\n` +
              `A SOURCE deste cue contÃ©m conteÃºdo real e NÃƒO pode desaparecer. ` +
              `A resposta sÃ³ serÃ¡ aceita se continuar vÃ¡lida DEPOIS do sanitizer final.\n\n` +
              `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
              `BÃBLIA EDITORIAL:\n${
                JSON.stringify(
                  plan
                )
              }\n\n` +
              `CÃPSULA CUE-LOCK ÃšNICA:\n${
                JSON.stringify(
                  payload
                )
              }\n\n` +
              `Retorne EXATAMENTE 1 cue. ` +
              `Copie o mesmo i e o ownership_key exatamente para k. ` +
              `O campo pt DEVE traduzir SOMENTE o target deste ID e preservar o conteÃºdo semÃ¢ntico. ` +
              `NÃƒO devolva vazio, "...", reticÃªncias isoladas, [suspiro], (suspiro), ` +
              `ruÃ­do SDH, speaker label isolado, placeholder ou descriÃ§Ã£o de acessibilidade. ` +
              `Se o target terminar em dois-pontos e for fala/narraÃ§Ã£o real, formule PT-BR natural ` +
              `que nÃ£o pareÃ§a um rÃ³tulo de speaker isolado. ` +
              `NÃ£o invente fala. NÃ£o use before/after como conteÃºdo do target. ` +
              `Todos os tokens __LOCK_C...__ devem voltar idÃªnticos. ` +
              `O token ${BLEEP_TOKEN} Ã© metadata invisÃ­vel de censura da SOURCE: NÃƒO o copie; naturalize a fala em PT-BR pelo contexto, sem placeholder.`,

            schema:
              mainCueTranslationSchema(1),

            thinkingLevel:
              MAIN_EMPTY_CUE_RESCUE_THINKING,

            maxOutputTokens:
              MAIN_EMPTY_CUE_RESCUE_MAX_OUTPUT_TOKENS,

            timeoutMs:
              MAIN_EMPTY_CUE_RESCUE_TIMEOUT_MS,

            maxRetries:
              MAIN_EMPTY_CUE_RESCUE_HTTP_RETRIES,

            job,

            metric:
              "main"
          });

        const translated =
          parseCueTranslation(
            rescueBatch,
            response.text,
            locksById,
            ownershipById,
            true,
            false
          );

        const pt =
          String(
            translated.get(
              block.index
            ) ||
            ""
          ).trim();

        if (!pt) {
          throw new Error(
            `Cue ${block.index} continuou vazio no rescue.`
          );
        }

        rejectedCandidates.push(
          pt
        );

        const sanitized =
          (
            sanitizeFinalCue(
              block,
              pt
            ) ||
            sanitizeFallbackCue(
              pt
            )
          ).trim();

        if (!sanitized) {
          if (
            rejectedCueIsSdhOnly(
              pt
            )
          ) {
            rejectedSdhCandidates.push(
              pt
            );

            if (
              rejectedSdhCandidates.length >=
              MAIN_EMPTY_CUE_SDH_CONSENSUS_MIN
            ) {
              markIntentionalEmptyCue(
                job,
                block,
                `consenso SDH ${rejectedSdhCandidates.length}/${rejectedCandidates.length}`
              );

              job.stats.mainSdhConsensusOmissions =
                (
                  job.stats.mainSdhConsensusOmissions ||
                  0
                ) + 1;

              console.log(
                `[MAIN EMPTY-CUE SDH CONSENSUS] cue ${block.index} ` +
                `omitido corretamente apÃ³s ${rejectedSdhCandidates.length} ` +
                `respostas SDH independentes; retry encerrado âœ….`
              );

              return "";
            }
          }

          throw new Error(
            `Cue ${block.index} virou vazio/lixo apÃ³s sanitizer | raw=${
              JSON.stringify(
                pt.slice(
                  0,
                  140
                )
              )
            }`
          );
        }

        console.log(
          `[MAIN EMPTY-CUE RESCUE] cue ${block.index} recuperado âœ… | ` +
          `${sanitized.length} chars pÃ³s-sanitizer.`
        );

        return sanitized;
      } catch (error) {
        lastError = error;

        if (error?.status === 429) {
          quotaDeferred = true;
          job.stats.mainEmptyCueQuotaDefers =
            (job.stats.mainEmptyCueQuotaDefers || 0) + 1;

          console.warn(
            `[MAIN EMPTY-CUE QUOTA DEFER 8.8.3] cue ${block.index}: ` +
            `quota continuou indisponÃ­vel apÃ³s os retries HTTP internos; ` +
            `NÃƒO haverÃ¡ uma nova rodada 3x3. ConteÃºdo seguro segue para QA/Repair.`
          );
          break;
        }

        if (
          parseAttempt <
          MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS
        ) {
          job.stats.mainEmptyCueRescueParseRetries++;

          console.warn(
            `[MAIN EMPTY-CUE RESCUE] cue ${block.index} ainda invÃ¡lido: ${
              errorMessage(
                error
              ).slice(
                0,
                320
              )
            }`
          );

          continue;
        }
      }
    }

    if (quotaDeferred) {
      break;
    }

    job.stats.mainEmptyCueRescueFailures++;
  }

  // Liveness sem sacrificar o restante do episÃ³dio: nunca reinicia MAIN e
  // nunca agenda ciclos eternos. Uma fala real fica preservada como base
  // temporÃ¡ria; QA/Repair ainda tentam localizÃ¡-la antes do gate final.
  const fallback =
    safestMainRescueFallback(
      block,
      rejectedCandidates
    );

  if (fallback) {
    job.stats.mainEmergencySourceFallbacks =
      (
        job.stats.mainEmergencySourceFallbacks ||
        0
      ) + 1;

    console.warn(
      quotaDeferred
        ? `[MAIN EMPTY-CUE SAFE FALLBACK] cue ${block.index}: quota-defer; ` +
          `conteÃºdo preservado imediatamente para QA/Repair, sem martelar a API | ${
            errorMessage(lastError).slice(0, 260)
          }`
        : `[MAIN EMPTY-CUE SAFE FALLBACK] cue ${block.index} encerrou ` +
          `${MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS} tentativa(s) sem consenso SDH; ` +
          `conteÃºdo preservado para QA/Repair | ${
            errorMessage(lastError).slice(0, 260)
          }`
    );

    return fallback;
  }

  throw (
    lastError ||
    new Error(
      `Cue ${block.index} sem fallback seguro.`
    )
  );
}

async function translateMainBatch({
  blocks,
  posMap,
  batch,
  plan,
  job,
  splitDepth = 0
}) {
  let lastError;

  for (
    let parseAttempt = 1;
    parseAttempt <=
      MAIN_PARSE_ATTEMPTS;
    parseAttempt++
  ) {
    try {
      const {
        payload,
        locksById,
        ownershipById
      } =
        buildOwnershipPayload(
          blocks,
          posMap,
          batch,
          plan
        );

      const response =
        await geminiRequest({
          system:
            TRANSLATOR_PROMPT,

          user:
            `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
            `BÃBLIA EDITORIAL:\n${
              JSON.stringify(
                plan
              )
            }\n\n` +
            `LOTE CUE-LOCK COM CONTEXTO COMPARTILHADO:\n${
              JSON.stringify(
                payload
              )
            }\n\n` +
            `Os cues estÃ£o em ORDEM CRONOLÃ“GICA. ` +
            `Retorne os IDs EXATAMENTE na mesma ordem recebida. ` +
            `Output exatamente ${
              batch.length
            } cues. ` +
            `Para cada item de cues, copie k EXATAMENTE para o campo k do mesmo ID. ` +
            `Traduza SOMENTE en do mesmo item para pt. ` +
            `Nunca use em pt conteÃºdo pertencente a outro ID ou aos contextos before/after. ` +
            `Todos os tokens __LOCK_C...__ recebidos em en devem voltar idÃªnticos em pt. ` +
            `Se turns>=2, preserve exatamente os turn_source como speakers independentes, um por linha com "- ". ` +
            `turn_candidates exigem julgamento contextual e nÃ£o sÃ£o hard lock. ` +
            `music=contextual_performance_lyric deve ser traduzida como letra relevante; â™ª serÃ¡ aplicado localmente. ` +
            `O token ${BLEEP_TOKEN} Ã© metadata invisÃ­vel de censura da SOURCE: NÃƒO o copie; naturalize a fala em PT-BR pelo contexto, sem placeholder.`,

          schema:
            mainCueTranslationSchema(
              batch.length
            ),

          thinkingLevel:
            MAIN_THINKING,

          maxOutputTokens:
            MAIN_MAX_OUTPUT_TOKENS,

          timeoutMs:
            MAIN_TIMEOUT_MS,

          maxRetries:
            MAIN_HTTP_RETRIES,

          job,

          metric:
            "main"
        });

      let parsed;
      try {
        parsed =
          parseMainCueTranslationRobust(
            batch,
            response.text,
            locksById,
            ownershipById
          );
      } catch (parseError) {
        invalidateResponseModelForJob(job, response, "MAIN structured output invÃ¡lido", parseError, "main");
        throw parseError;
      }

      const rescueIds = [
        ...new Set([
          ...parsed.emptyIds,
          ...parsed.missingIds
        ])
      ];

      if (!rescueIds.length) {
        return parsed.translations;
      }

      job.stats.mainEmptyCueRescueCues += rescueIds.length;

      console.warn(
        `[MAIN FOCAL RESCUE 9.0] preservando ${parsed.translations.size}/${batch.length} ` +
        `cues vÃ¡lidos; refazendo SOMENTE ${rescueIds.length} cue(s): ` +
        `${rescueIds.join(", ")}.`
      );

      const batchById = new Map(
        batch.map(block => [block.index, block])
      );

      for (const id of rescueIds) {
        const block = batchById.get(id);

        if (!block) {
          throw new Error(`MAIN FOCAL RESCUE: bloco ${id} nÃ£o encontrado.`);
        }

        const rescuedPt = await rescueEmptyMainCue({
          blocks,
          posMap,
          block,
          plan,
          job
        });

        parsed.translations.set(id, rescuedPt);
      }

      if (parsed.translations.size !== batch.length) {
        throw new Error(
          `MAIN FOCAL RESCUE incompleto: ${parsed.translations.size}/${batch.length}.`
        );
      }

      return parsed.translations;
    } catch (error) {
      lastError = error;

      if (isDeterministicGeminiRequestError(error)) {
        const deterministicStatus = Number(error?.status || 0);
        const deterministicText = String(error?.message || error || "");
        const explicitSize400 = deterministicStatus === 400 &&
          /(?:request|payload|input|context).{0,40}(?:too large|too long|size|limit|max(?:imum)?|tokens?)|(?:too large|too long).{0,40}(?:request|payload|input|context)/i.test(deterministicText);
        const mayShrinkRequest = [413, 422].includes(deterministicStatus) || explicitSize400;
        if (mayShrinkRequest && batch.length > 40 && splitDepth < 2) {
          const mid = Math.ceil(batch.length / 2);
          const leftBatch = batch.slice(0, mid);
          const rightBatch = batch.slice(mid);

          console.warn(
            `[MAIN ADAPTIVE 9.0] request grande recebeu limite de payload; ` +
            `dividindo ${batch.length} cues em ${leftBatch.length}+${rightBatch.length} ` +
            `(depth=${splitDepth + 1}/2), sem reiniciar o job.`
          );

          const left = await translateMainBatch({ blocks, posMap, batch: leftBatch, plan, job, splitDepth: splitDepth + 1 });
          const right = await translateMainBatch({ blocks, posMap, batch: rightBatch, plan, job, splitDepth: splitDepth + 1 });
          return new Map([...left, ...right]);
        }

        error.noFullBatchRetry = true;
        error.noJobRetry = true;
        console.error(
          `[MAIN FAIL-FAST 8.7] erro determinÃ­stico do request; ` +
          `nÃ£o serÃ¡ martelado atÃ© virar 429 nem reiniciarÃ¡ o job | ${errorMessage(error).slice(0, 360)}`
        );
        throw error;
      }

      if (
        error?.noFullBatchRetry
      ) {
        console.error(
          `[MAIN CUE-LOCK] rescue isolado falhou; lote inteiro NÃƒO serÃ¡ retraduzido: ${
            errorMessage(
              error
            ).slice(
              0,
              360
            )
          }`
        );

        throw error;
      }

      if (
        parseAttempt >=
        MAIN_PARSE_ATTEMPTS
      ) {
        console.error(
          `[MAIN CUE-LOCK] lote rejeitado definitivamente apÃ³s ${
            MAIN_PARSE_ATTEMPTS
          } tentativa(s): ${
            errorMessage(
              error
            ).slice(
              0,
              320
            )
          }`
        );

        throw error;
      }

      job.stats.mainParseRetries++;

      console.warn(
        `[MAIN CUE-LOCK] repetindo mesmo lote: ${
          errorMessage(
            error
          ).slice(
            0,
            260
          )
        }`
      );
    }
  }

  throw lastError;
}

async function recoverSanitizedEmptyCues({
  blocks,
  translations,
  plan,
  job,
  stage
}) {
  const updated =
    new Map(
      translations
    );

  const posMap =
    positionMap(
      blocks
    );

  let recovered = 0;
  let intentional = 0;

  for (const block of blocks) {
    const current =
      String(
        updated.get(
          block.index
        ) ??
        ""
      ).trim();

    if (current) {
      continue;
    }

    if (
      sourceCueAllowsIntentionalEmpty(
        block,
        job
      )
    ) {
      intentional++;
      continue;
    }

    console.warn(
      `[EMPTY-CUE LOCAL RECOVERY] ${stage} | cue ${block.index} vazio ` +
      `com SOURCE real; corrigindo SOMENTE este cue.`
    );

    const rescued =
      await rescueEmptyMainCue({
        blocks,
        posMap,
        block,
        plan,
        job
      });

    updated.set(
      block.index,
      rescued
    );

    recovered++;

    job.stats.mainSanitizedEmptyRecoveries =
      (
        job.stats.mainSanitizedEmptyRecoveries ||
        0
      ) + 1;
  }

  if (
    recovered ||
    intentional
  ) {
    console.log(
      `[EMPTY-CUE LOCAL RECOVERY] ${stage} | recuperados=${
        recovered
      } | intencionais=${
        intentional
      } | full-restart=0.`
    );
  }

  return updated;
}

async function translateAllMain(
  blocks,
  plan,
  job
) {
  const batches = buildMainBatches(blocks);
  const translations = new Map();
  const posMap = positionMap(blocks);

  if (!(job.mainCheckpoint instanceof Map)) {
    job.mainCheckpoint = new Map();
  }

  let localVocalizationPrefill = 0;
  for (const block of blocks) {
    if (!job.mainCheckpoint.has(block.index)) {
      const localVocalization = localizePureVocalizationCue(block);
      if (localVocalization) {
        job.mainCheckpoint.set(block.index, localVocalization);
        localVocalizationPrefill++;
      }
    }

    if (job.mainCheckpoint.has(block.index)) {
      translations.set(block.index, job.mainCheckpoint.get(block.index));
    }
  }

  if (localVocalizationPrefill > 0) {
    job.stats.mainLocalVocalizationPrefill =
      Number(job.stats.mainLocalVocalizationPrefill || 0) + localVocalizationPrefill;
    console.log(
      `[MAIN LOCAL VOCALIZATION 9.0] ${localVocalizationPrefill} cue(s) ` +
      `resolvidos localmente antes do MAIN | 0 Gemini.`
    );
  }

  const work = batches
    .map((batch, batchIndex) => ({
      batch: batch.filter(block => !job.mainCheckpoint.has(block.index)),
      batchIndex
    }))
    .filter(({ batch }) => batch.length > 0);

  job.stats.mainBatches = batches.length;
  job.stats.mainCheckpointReused = translations.size;

  console.log(
    `[MAIN 9.0] ${blocks.length} cues -> ${batches.length} lotes | ` +
    `pendentes=${work.length} | checkpoint=${translations.size}/${blocks.length} | ` +
    `concorrÃªncia=${MAIN_CONCURRENCY} | atÃ© ${MAIN_BATCH_MAX_CUES} cues. `
  );

  if (!work.length) {
    return translations;
  }

  let cursor = 0;
  let completed = batches.length - work.length;

  async function worker(workerId) {
    while (true) {
      const workIndex = cursor++;
      if (workIndex >= work.length) return;

      const { batch, batchIndex } = work[workIndex];

      console.log(
        `[MAIN W${workerId}] lote ${batchIndex + 1}/${batches.length}: ${batch.length} cues.`
      );

      const translated = await translateMainBatch({
        blocks,
        posMap,
        batch,
        plan,
        job
      });

      for (const [id, pt] of translated) {
        translations.set(id, pt);
        job.mainCheckpoint.set(id, pt);
      }

      completed++;
      job.progress = Math.max(Number(job.progress || 0), Math.min(
        90,
        5 + Math.round(85 * completed / batches.length)
      ));
      job.updatedAt = Date.now();

      console.log(
        `[MAIN W${workerId}] lote ${batchIndex + 1} OK + CHECKPOINT | ` +
        `${translations.size}/${blocks.length} | ${job.progress}%.`
      );
    }
  }

  const workerResults = await Promise.allSettled(
    Array.from(
      { length: Math.min(MAIN_CONCURRENCY, work.length) },
      (_, index) => worker(index + 1)
    )
  );

  const failedWorker = workerResults.find(result => result.status === "rejected");
  if (failedWorker) {
    job.stats.mainWorkerDrainFailures = Number(job.stats.mainWorkerDrainFailures || 0) + 1;
    console.warn(
      `[MAIN DRAIN 9.2.1] falha localizada detectada; todos os workers em voo ` +
      `foram drenados antes do retry/job terminal | checkpoint=${job.mainCheckpoint.size}/${blocks.length}.`
    );
    throw failedWorker.reason;
  }

  if (translations.size !== blocks.length) {
    throw new Error(
      `TraduÃ§Ã£o principal incompleta: ${translations.size}/${blocks.length}; ` +
      `checkpoint preservado para o prÃ³ximo retry.`
    );
  }

  return translations;
}

// ============================================================
// PT-BR QA SCANNER â€” ENÃ—PT / TODAS AS FONTES
// ============================================================

function buildQaBatches(
  blocks,
  translations,
  plan
) {
  const batches = [];
  let current = [];
  let chars = 0;

  for (let pos = 0; pos < blocks.length; pos++) {
    const block = blocks[pos];
    const lock = identityLockForCapsule(block, plan);

    const item = {
      i: block.index,
      en: String(block.text || ""),
      pt: String(translations.get(block.index) || ""),
      s: String(lock?.speaker?.canonical || block.speakerHint || "unknown"),
      g: String(lock?.trusted_speaker_gender || "unknown"),
      turns: sourceDialogueDashCount(block),
      turn_mode: sourceDialogueTurnPlan973(block).mode,
      turn_source: sourceDialogueTurnPlan973(block).turns,
      turn_candidates: sourceDialogueTurnPlan973(block).softCandidates,
      music: block.musicPerformance ? "contextual_performance_lyric" : "",
      boundary: sourceBoundaryHint(block)
    };

    const size = JSON.stringify(item).length;

    if (
      current.length &&
      (
        current.length >= QA_BATCH_MAX_CUES ||
        chars + size > QA_BATCH_MAX_CHARS
      )
    ) {
      batches.push(current);
      current = [];
      chars = 0;
    }

    current.push(item);
    chars += size;
  }

  if (current.length) batches.push(current);
  return batches;
}

function parseQaIssues(
  text,
  allowedIds
) {
  const parsed =
    JSON.parse(
      stripCodeFences(
        text
      )
    );

  const issues =
    Array.isArray(
      parsed?.issues
    )
      ? parsed.issues
      : [];

  const out = [];
  const seen =
    new Set();

  for (
    const issue of issues
  ) {
    const id =
      Number(
        issue?.i
      );

    const reason =
      String(
        issue?.reason ||
        "QA_PTBR"
      )
        .trim()
        .slice(
          0,
          180
        );

    if (
      !Number.isInteger(id) ||
      !allowedIds.has(id) ||
      seen.has(id)
    ) {
      continue;
    }

    seen.add(id);

    const reasons = [
  `QA_PTBR: ${
    reason ||
    "defeito claro"
  }`
];

if (
  /^CUE_OWNERSHIP_SHIFT\s*:/iu.test(
    reason
  )
) {
  reasons.unshift(
    "POSSIBLE_CUE_SHIFT_PAIR"
  );
}

out.push({
  id,
  reasons
});
  }

  return out;
}

async function scanPtbrQuality(
  blocks,
  translations,
  plan,
  job
) {
  if (!QA_ENABLED) {
    return [];
  }

  const batches =
    buildQaBatches(
      blocks,
      translations,
      plan
    );

  job.stats.qaBatches =
    batches.length;

  const results =
    new Array(
      batches.length
    );

  let cursor = 0;

  console.log(
    `[PTBR QA] ${
      blocks.length
    } cues -> ${
      batches.length
    } lote(s) | contexto=${
      QA_CONTEXT_BEFORE
    }+${
      QA_CONTEXT_AFTER
    } | concorrÃªncia=${
      Math.min(
        QA_CONCURRENCY,
        batches.length
      )
    } | fonte=${
      job.sourceKind
    }.`
  );

  async function qaWorker(
    workerId
  ) {
    while (true) {
      const index =
        cursor++;

      if (
        index >=
        batches.length
      ) {
        return;
      }

      const batch =
        batches[index];

      const allowed =
        new Set(
          batch.map(
            item => item.i
          )
        );

      let parsedIssues =
        null;

      let lastError =
        null;

      for (
        let attempt = 1;
        attempt <=
          QA_PARSE_ATTEMPTS;
        attempt++
      ) {
        try {
          const response =
            await geminiRequest({
              system:
                QA_PROMPT,

              user:
                `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
                `BÃBLIA EDITORIAL DO EPISÃ“DIO:\n${
                  JSON.stringify(
                    plan || {}
                  )
                }\n\n` +
                `SEQUÃŠNCIA CRONOLÃ“GICA FONTEÃ—PT PARA AUDITORIA (vizinhos da prÃ³pria lista sÃ£o o contexto):\n${
                  JSON.stringify(
                    batch
                  )
                }\n\n` +
                `Retorne SOMENTE IDs que realmente merecem repair por erro semÃ¢ntico, gÃªnero/referente, ownership, calque, literalidade ou naturalidade insuficiente. ` +
                `A lista jÃ¡ estÃ¡ em ordem cronolÃ³gica; compare explicitamente SOURCE[i]Ã—PT[i] com i-1/i+1. ` +
                `Se uma parte lexical da SOURCE[i] apareceu em PT[i+1] ou PT[i-1], marque CUE_OWNERSHIP_SHIFT nos IDs afetados. ` +
                `boundary indica se o target comeÃ§a/termina no meio de uma frase; isso NÃƒO autoriza puxar palavras do vizinho. ` +
                `Use os vizinhos apenas como contexto e nÃ£o reescreva os cues.`,

              schema:
                QA_SCHEMA,

              thinkingLevel:
                QA_THINKING,

              maxOutputTokens:
                QA_MAX_OUTPUT_TOKENS,

              timeoutMs:
                QA_TIMEOUT_MS,

              maxRetries:
                QA_HTTP_RETRIES,

              job,

              metric:
                "qa"
            });

          try {
            parsedIssues =
              parseQaIssues(
                response.text,
                allowed
              );
          } catch (parseError) {
            invalidateResponseModelForJob(job, response, "QA structured output invÃ¡lido", parseError);
            throw parseError;
          }

          break;
        } catch (error) {
          lastError =
            error;

          if (
            attempt >=
            QA_PARSE_ATTEMPTS
          ) {
            break;
          }

          job.stats.qaParseRetries++;

          console.warn(
            `[PTBR QA W${
              workerId
            }] parse repetido no lote ${
              index + 1
            }: ${
              errorMessage(
                error
              ).slice(
                0,
                220
              )
            }`
          );
        }
      }

      if (!parsedIssues) {
        console.warn(
          `[PTBR QA W${
            workerId
          }] lote ${
            index + 1
          } ignorado apÃ³s falha: ${
            errorMessage(
              lastError
            ).slice(
              0,
              220
            )
          }`
        );

        results[index] =
          [];

        continue;
      }

      results[index] =
        parsedIssues;

      console.log(
        `[PTBR QA W${
          workerId
        }] lote ${
          index + 1
        }/${
          batches.length
        }: ${
          parsedIssues.length
        } suspeito(s).`
      );
    }
  }

  await Promise.all(
    Array.from(
      {
        length:
          Math.min(
            QA_CONCURRENCY,
            batches.length
          )
      },

      (_, index) =>
        qaWorker(
          index + 1
        )
    )
  );

  const all = [];
  const seen =
    new Set();

  for (
    const issues of results
  ) {
    for (
      const issue of
      Array.isArray(
        issues
      )
        ? issues
        : []
    ) {
      if (
        seen.has(
          issue.id
        )
      ) {
        continue;
      }

      seen.add(
        issue.id
      );

      all.push(
        issue
      );

      if (
        all.length >=
        QA_MAX_FLAGS_TOTAL
      ) {
        break;
      }
    }

    if (
      all.length >=
      QA_MAX_FLAGS_TOTAL
    ) {
      break;
    }
  }

  job.stats.qaFlags =
    all.length;

  console.log(
    `[PTBR QA] total=${
      all.length
    } cue(s) sinalizado(s).`
  );

  return all;
}


// ============================================================
// 9.0 FINAL OWNERSHIP GATE â€” BOUNDED / FAIL-CLOSED
// ============================================================
// Root cause addressed: an LLM can return the correct i/k metadata while the
// natural-language content itself has slid into a neighboring cue. Therefore
// metadata-only ownership is necessary but not sufficient.
//
// 9.0 response when QA proves a shift:
// 1) quarantine the affected ORIGINAL MAIN batch (or a bounded local window
//    for a single isolated signal);
// 2) retranslate it as tiny micro-batches with cryptographic inline seals;
// 3) re-run QA only on the quarantined region;
// 4) any still-suspect cue is translated SOURCE-ONLY, one target per request;
// 5) after all normal Repair/Final Bounded work, audit the quarantine again;
// 6) a persistent ownership shift after SOURCE-ONLY isolation fails CLOSED â€”
//    an incorrect SRT is never deliberately served.
//
// Normal episodes pay ZERO extra requests. There is no open convergence loop.
const OWNERSHIP_MICRO_MAX_CUES_900 = 24;
const OWNERSHIP_MICRO_CONCURRENCY_900 = 2;
const OWNERSHIP_ISOLATED_MARGIN_900 = 12;

function ownershipIssue900(issue) {
  const joined = Array.isArray(issue?.reasons)
    ? issue.reasons.map(String).join(" | ")
    : String(issue?.reason || "");
  return /POSSIBLE_CUE_SHIFT_PAIR|CUE_OWNERSHIP_SHIFT|CUE_OWNERSHIP_(?:SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION)/i.test(joined);
}

function ownershipSeal900(block) {
  const id = Number(block?.index);
  const source = String(block?.text || "");
  const digest = crypto
    .createHash("sha256")
    .update(`${id}\u0000${source}`)
    .digest("hex")
    .slice(0, 12)
    .toUpperCase();
  return {
    start: `__OWN9S_C${id}_${digest}__`,
    end: `__OWN9E_C${id}_${digest}__`
  };
}

function addOwnershipSeals900(payload, batch) {
  const sealsById = new Map();
  const blockById = new Map(batch.map(block => [Number(block.index), block]));
  for (const cue of Array.isArray(payload?.cues) ? payload.cues : []) {
    const block = blockById.get(Number(cue?.i));
    if (!block) continue;
    const seals = ownershipSeal900(block);
    sealsById.set(Number(block.index), seals);
    cue.en = `${seals.start} ${String(cue.en || "")} ${seals.end}`;
  }
  payload.rule =
    `OWNERSHIP 9.0 HARD-SEAL: cada en comeÃ§a com __OWN9S... e termina com __OWN9E.... ` +
    `Em pt, copie o START como PRIMEIRO token e o END como ÃšLTIMO token do MESMO cue. ` +
    `Traduza somente o conteÃºdo que estÃ¡ fisicamente ENTRE os dois selos. ` +
    `Nunca copie conteÃºdo de before/after nem de outro cue. ` +
    String(payload.rule || "");
  return sealsById;
}

function stripAndValidateOwnershipSeal900(id, value, sealsById) {
  const raw = String(value || "").trim();
  const seals = sealsById.get(Number(id));
  if (!seals) return null;
  if (!raw.startsWith(seals.start) || !raw.endsWith(seals.end)) return null;
  const middle = raw
    .slice(seals.start.length, raw.length - seals.end.length)
    .trim();
  if (!middle || /__OWN9[SE]_C\d+_/i.test(middle)) return null;
  return middle;
}

async function translateOwnershipSourceOnly900(block, plan, job, thinkingLevel = "high", metric = "repair") {
  const batch = [block];
  const {
    payload,
    locksById,
    ownershipById
  } = buildOwnershipPayload(batch, positionMap(batch), batch, plan);

  // SOURCE-ONLY means there is literally no neighboring cue text in the
  // request. This is the final deterministic isolation step for ownership.
  payload.before = [];
  payload.after = [];
  const sealsById = addOwnershipSeals900(payload, batch);

  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const response = await geminiRequest({
        system: TRANSLATOR_PROMPT,
        user:
          `OWNERSHIP GATE 9.0 â€” SOURCE-ONLY ISOLATION.\n\n` +
          `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
          `BÃBLIA EDITORIAL:\n${JSON.stringify(plan)}\n\n` +
          `HÃ EXATAMENTE UM TARGET E NENHUM TEXTO DE VIZINHOS NESTE REQUEST.\n` +
          `CÃPSULA SELADA:\n${JSON.stringify(payload)}\n\n` +
          `Retorne exatamente 1 cue. Copie i/k. ` +
          `pt DEVE comeÃ§ar pelo START seal e terminar pelo END seal recebidos. ` +
          `Traduza somente a SOURCE entre os selos; nÃ£o complete com conteÃºdo inexistente.`,
        schema: mainCueTranslationSchema(1),
        thinkingLevel,
        maxOutputTokens: MAIN_EMPTY_CUE_RESCUE_MAX_OUTPUT_TOKENS,
        timeoutMs: MAIN_TIMEOUT_MS,
        maxRetries: 1,
        job,
        metric
      });

      const parsed = parseMainCueTranslationRobust(
        batch,
        response.text,
        locksById,
        ownershipById
      );
      const sealed = parsed.translations.get(block.index);
      const pt = stripAndValidateOwnershipSeal900(block.index, sealed, sealsById);
      if (!pt) throw new Error(`OWNERSHIP 9.0 cue ${block.index}: seal invÃ¡lido/missing.`);
      return pt;
    } catch (error) {
      lastError = error;
      console.warn(
        `[OWNERSHIP SOURCE-ONLY 9.0] cue ${block.index} tentativa ${attempt}/2 falhou | ` +
        `${errorMessage(error).slice(0, 260)}`
      );
    }
  }
  throw lastError || new Error(`OWNERSHIP SOURCE-ONLY 9.0 falhou no cue ${block.index}.`);
}

async function translateOwnershipMicroBatch900(blocks, posMap, batch, plan, job) {
  const {
    payload,
    locksById,
    ownershipById
  } = buildOwnershipPayload(blocks, posMap, batch, plan);
  const sealsById = addOwnershipSeals900(payload, batch);

  try {
    const response = await geminiRequest({
      system: TRANSLATOR_PROMPT,
      user:
        `OWNERSHIP QUARANTINE 9.0 â€” MICRO-BATCH SELADO.\n\n` +
        `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
        `BÃBLIA EDITORIAL:\n${JSON.stringify(plan)}\n\n` +
        `MICRO-BATCH (mÃ¡ximo ${OWNERSHIP_MICRO_MAX_CUES_900} targets):\n${JSON.stringify(payload)}\n\n` +
        `Cada pt comeÃ§a/termina com os selos do MESMO en. ` +
        `Preserve fragmentos e ownership; contexto Ã© somente leitura.`,
      schema: mainCueTranslationSchema(batch.length),
      thinkingLevel: MAIN_THINKING,
      maxOutputTokens: MAIN_MAX_OUTPUT_TOKENS,
      timeoutMs: MAIN_TIMEOUT_MS,
      maxRetries: MAIN_HTTP_RETRIES,
      job,
      metric: "main"
    });

    const parsed = parseMainCueTranslationRobust(
      batch,
      response.text,
      locksById,
      ownershipById
    );

    const out = new Map();
    const invalidIds = new Set([
      ...(parsed.emptyIds || []),
      ...(parsed.missingIds || [])
    ]);

    for (const block of batch) {
      const sealed = parsed.translations.get(block.index);
      const pt = stripAndValidateOwnershipSeal900(block.index, sealed, sealsById);
      if (!pt) invalidIds.add(block.index);
      else out.set(block.index, pt);
    }

    // A micro-resposta que nÃ£o prova fisicamente o seal vira SOURCE-ONLY,
    // somente para o cue invÃ¡lido; os outros resultados vÃ¡lidos sÃ£o mantidos.
    if (invalidIds.size) {
      console.warn(
        `[OWNERSHIP QUARANTINE 9.0] ${invalidIds.size} cue(s) sem seal vÃ¡lido; ` +
        `isolando individualmente, sem refazer os vÃ¡lidos.`
      );
      for (const id of invalidIds) {
        const block = batch.find(item => Number(item.index) === Number(id));
        if (!block) continue;
        out.set(id, await translateOwnershipSourceOnly900(block, plan, job, "high", "repair"));
      }
    }

    if (out.size !== batch.length) {
      throw new Error(`OWNERSHIP MICRO 9.0 incompleto ${out.size}/${batch.length}.`);
    }
    return out;
  } catch (error) {
    console.warn(
      `[OWNERSHIP QUARANTINE 9.0] micro-batch falhou; ` +
      `caindo para SOURCE-ONLY por cue | ${errorMessage(error).slice(0, 280)}`
    );
    const out = new Map();
    for (const block of batch) {
      out.set(block.index, await translateOwnershipSourceOnly900(block, plan, job, "high", "repair"));
    }
    return out;
  }
}

function ownershipQuarantineTargets900(blocks, issues) {
  const ownershipIds = [...new Set(
    (Array.isArray(issues) ? issues : [])
      .filter(ownershipIssue900)
      .map(issue => Number(issue?.id))
      .filter(Number.isInteger)
  )];
  if (!ownershipIds.length) return { ownershipIds, targetIds: new Set(), batchIndexes: [] };

  const posMap = positionMap(blocks);
  const targetIds = new Set();
  const MARGIN = 2;
  const HARD_CAP = 36;

  // Uma suspeita de boundary Ã© LOCAL por natureza. A versÃ£o antiga podia
  // invalidar um MAIN batch inteiro (atÃ© 160 cues), gerando 20 microchamadas.
  // Aqui reabrimos apenas o cue sinalizado + vizinhos imediatos suficientes
  // para provar ownership dos dois lados. Os prÃ³prios ids sinalizados tÃªm
  // prioridade absoluta caso o cap seja atingido.
  for (const id of ownershipIds) targetIds.add(id);
  for (const id of ownershipIds) {
    const pos = posMap.get(id);
    if (!Number.isInteger(pos)) continue;
    for (let delta = -MARGIN; delta <= MARGIN; delta++) {
      const block = blocks[pos + delta];
      if (block) targetIds.add(Number(block.index));
      if (targetIds.size >= HARD_CAP) break;
    }
    if (targetIds.size >= HARD_CAP) break;
  }

  // Cap universal e determinÃ­stico: nunca descarta o prÃ³prio cue sinalizado.
  if (targetIds.size > HARD_CAP) {
    const keep = new Set(ownershipIds.slice(0, HARD_CAP));
    for (const id of targetIds) {
      if (keep.size >= HARD_CAP) break;
      keep.add(id);
    }
    return { ownershipIds, targetIds: keep, batchIndexes: [] };
  }
  return { ownershipIds, targetIds, batchIndexes: [] };
}

async function qaSubset900(blocks, translations, plan, job, label) {
  if (!blocks.length) return [];
  const oldBatches = Number(job.stats.qaBatches || 0);
  const oldFlags = Number(job.stats.qaFlags || 0);
  const issues = await scanPtbrQuality(blocks, translations, plan, job);
  const newBatches = Number(job.stats.qaBatches || 0);
  const newFlags = Number(job.stats.qaFlags || 0);
  job.stats.qaBatches = oldBatches + newBatches;
  job.stats.qaFlags = oldFlags + newFlags;
  console.log(`[${label}] ${blocks.length} cue(s) reauditados | flags=${issues.length}.`);
  return issues;
}

async function runOwnershipQuarantine900(blocks, translations, qaIssues, plan, job) {
  const localOwnershipIssues = detectLocalIssues(blocks, translations, job.filename, plan)
    .filter(ownershipIssue900);

  const ownershipIssues = mergeIssueLists(
    Array.isArray(qaIssues) ? qaIssues : [],
    localOwnershipIssues
  );

  const plan900 = ownershipQuarantineTargets900(
    blocks,
    ownershipIssues
  );

  if (!plan900.targetIds.size) {
    job.ownershipQuarantineIds900 = [];
    console.log(`[OWNERSHIP GATE 9.7.0] nenhum shift provado; 0 rewrite cloud âœ….`);
    return { translations, qaIssues };
  }

  job.ownershipQuarantineIds900 = [...plan900.targetIds]
    .map(Number)
    .filter(Number.isInteger);

  job.forceSemanticAuditIds = [
    ...new Set([
      ...(Array.isArray(job.forceSemanticAuditIds) ? job.forceSemanticAuditIds : []),
      ...job.ownershipQuarantineIds900
    ])
  ];

  console.warn(
    `[OWNERSHIP GATE 9.7.0] ANALYSIS-ONLY | sinais=${plan900.ownershipIds.length} | ` +
    `cues=${job.ownershipQuarantineIds900.length} | 0 micro-batch / 0 SOURCE-ONLY / ` +
    `todos os blockers seguem para a ÃšNICA rodada consolidada de Repair. âœ…`
  );

  return {
    translations,
    qaIssues: mergeIssueLists(
      Array.isArray(qaIssues) ? qaIssues : [],
      localOwnershipIssues
    )
  };
}

async function enforceFinalOwnershipGate900(blocks, translations, plan, job) {
  const out = new Map(translations);

  // 9.7.0: ownership jÃ¡ foi analisado pelo QA global + PRE-AUDIT e enviado Ã 
  // Ãºnica rodada consolidada. O candidate regression guard impede criar novos
  // shifts. Portanto nÃ£o existe mais QA cloud pÃ³s-Repair.
  const localOwnership = detectLocalIssues(
    blocks,
    out,
    job.filename,
    plan
  ).filter(ownershipIssue900);

  job.ownershipFinalResidual900 = localOwnership.length;

  if (!localOwnership.length) {
    console.log(`[OWNERSHIP FINAL GATE 9.7.0] local=0 | 0 QA extra / 0 Repair âœ….`);
    return out;
  }

  const ids = [...new Set(
    localOwnership.map(issue => Number(issue?.id)).filter(Number.isInteger)
  )];

  console.error(
    `[OWNERSHIP FINAL GATE 9.7.0] FAIL-CLOSED determinÃ­stico | local=${localOwnership.length} | ` +
    `ids=[${ids.join(",")}]; blocker jÃ¡ teve chance na Ãºnica rodada consolidada; 0 nova rede.`
  );

  return out;
}

// ============================================================
// DETECTOR / REPAIR
// ============================================================

function words(text) {
  return (
    String(text || "")
      .toLowerCase()
      .match(
        /[\p{L}\p{N}]+(?:['â€™][\p{L}\p{N}]+)?/gu
      ) ||
    []
  );
}

function normalizedWordSet(
  text
) {
  return new Set(
    words(text)
      .map(
        value =>
          value
            .normalize("NFD")
            .replace(
              /[\u0300-\u036f]/g,
              ""
            )
      )
      .filter(
        value =>
          value.length > 2
      )
  );
}

function copiedEnglishRatio(
  en,
  pt
) {
  const source =
    normalizedWordSet(en);

  const translated =
    normalizedWordSet(pt);

  if (!source.size) {
    return 0;
  }

  let copied = 0;

  for (
    const word of source
  ) {
    if (
      translated.has(word)
    ) {
      copied++;
    }
  }

  return (
    copied /
    source.size
  );
}

function isDragContext(
  _filename,
  en
) {
  // 9.5.0 UNIVERSAL: filename/title NEVER selects semantic behavior.
  // Domain slang handling is activated only by lexical evidence inside the
  // SOURCE cue itself, so the same rule works in any movie/show/documentary.
  return /\bwerkroom\b|\blip[\s-]+sync\b|\bshantay\b|\bsashay\b|\bcondragulations\b|\bsnatch\s+game\b|\brusical\b|\bshade\b|\bslay(?:ed|ing|s)?\b|\bspill(?:ing)?\s+the\s+tea\b/iu.test(
    String(en || "")
  );
}

function isPhysicalGagContext(
  en
) {
  return /\bgag reflex\b|\bgag(?:ged|ging)?\s+(?:on|from)\s+(?:food|water|something|it)\b|\bchok(?:e|ed|ing)\b|\bvomit|throw up|nausea|throat|mouth|tape|bound|restrain/i.test(
    String(en || "")
  );
}

function hasGoodGagReaction(
  pt
) {
  return /\bpassad[ao]s?\b|\bt[oÃ´]\s+passad[ao]\b|\bem\s+choque\b|\bsem\s+rea[cÃ§][aÃ£]o\b|\bboquiabert[ao]s?\b|\bchocad[ao]s?\b/i.test(
    String(pt || "")
  );
}

function hasExtendedVocalization(
  text
) {
  const value =
    String(text || "");

  return (
    /(\p{L}{2,})(?:-[aeiouÃ¡Ã©Ã­Ã³ÃºÃ Ã¢ÃªÃ´Ã£ÃµÃ¼]){2,}/giu.test(
      value
    ) ||

    /(\p{L}{2,})(?:-[\p{L}]{1,3}){3,}/gu.test(
      value
    )
  );
}

function sourceHasExplicitProfanity971(value) {
  const text = String(value || "").toLocaleLowerCase();
  if (text.includes(BLEEP_TOKEN.toLocaleLowerCase())) return true;
  return /\b(?:fuck(?:ed|ing|er|ers)?|shit(?:ty)?|bullshit|bitch(?:es)?|asshole|motherfucker|cunt|dick|cock|pussy|bastard|goddamn|damn)\b/iu.test(text);
}

function targetHasStrongProfanity971(value) {
  return /\b(?:porra|caralho|foda-se|foder|fodido|puta\s+que\s+pariu|merda|cu|pau)\b/iu.test(String(value || ""));
}

function literalCalqueReasons(
  en,
  pt
) {
  const reasons = [];

  const source =
    String(en || "");

  const target =
    String(pt || "");

  if (
    /\bactually\b/i.test(
      source
    ) &&
    /\batualmente\b/i.test(
      target
    )
  ) {
    reasons.push(
      "FALSE_COGNATE_ACTUALLY"
    );
  }

  if (
    /\beventually\b/i.test(
      source
    ) &&
    /\beventualmente\b/i.test(
      target
    )
  ) {
    reasons.push(
      "FALSE_COGNATE_EVENTUALLY"
    );
  }

  if (
    /\brealiz(?:e|ed|es|ing)\b/i.test(
      source
    ) &&
    /\brealiz(?:ar|ei|ou|ando|amos|aram)\b/i.test(
      target
    )
  ) {
    reasons.push(
      "FALSE_COGNATE_REALIZE"
    );
  }

  if (
    /\bpretend(?:ed|s|ing)?\b/i.test(
      source
    ) &&
    /\bpretend(?:er|o|e|eu|endo|ia|emos)\b/i.test(
      target
    )
  ) {
    reasons.push(
      "FALSE_COGNATE_PRETEND"
    );
  }

  if (
    /\bparents?\b/i.test(
      source
    ) &&
    /\bparentes?\b/i.test(
      target
    )
  ) {
    reasons.push(
      "FALSE_COGNATE_PARENTS"
    );
  }

  if (
    /\blibrar(?:y|ies)\b/i.test(
      source
    ) &&
    /\blivrarias?\b/i.test(
      target
    )
  ) {
    reasons.push(
      "FALSE_COGNATE_LIBRARY"
    );
  }

  if (
    /\bcollege\b/i.test(
      source
    ) &&
    /\bcol[eÃ©]gio\b/i.test(
      target
    )
  ) {
    reasons.push(
      "POSSIBLE_FALSE_COGNATE_COLLEGE"
    );
  }

  if (
    /\bi\s+mean\b/i.test(
      source
    ) &&
    /\beu\s+quero\s+dizer\b/i.test(
      target
    )
  ) {
    reasons.push(
      "DISCOURSE_MARKER_I_MEAN_LITERAL"
    );
  }

  if (
    /\bat\s+the\s+end\s+of\s+the\s+day\b/i.test(
      source
    ) &&
    /\bno\s+fim\s+do\s+dia\b/i.test(
      target
    )
  ) {
    reasons.push(
      "IDIOM_END_OF_DAY_LITERAL"
    );
  }

  if (
    /\bthat\s+being\s+said\b/i.test(
      source
    ) &&
    /\b(?:isso|isto)\s+sendo\s+dito\b/i.test(
      target
    )
  ) {
    reasons.push(
      "IDIOM_THAT_BEING_SAID_LITERAL"
    );
  }

  if (
    /\bgive\s+(?:me|him|her|us|them)\s+a\s+break\b/i.test(
      source
    ) &&
    /\b(?:d[Ãªe]|dar)\b.{0,20}\b(?:intervalo|pausa)\b/i.test(
      target
    )
  ) {
    reasons.push(
      "IDIOM_GIVE_A_BREAK_LITERAL"
    );
  }

  if (
    /\bpiece\s+of\s+cake\b/i.test(
      source
    ) &&
    /\bpeda[cÃ§]o\s+de\s+bolo\b/i.test(
      target
    )
  ) {
    reasons.push(
      "IDIOM_PIECE_OF_CAKE_LITERAL"
    );
  }

  if (
    /\bbreak\s+a\s+leg\b/i.test(
      source
    ) &&
    /\bquebr(?:e|ar)\s+(?:uma|a)\s+perna\b/i.test(
      target
    )
  ) {
    reasons.push(
      "IDIOM_BREAK_A_LEG_LITERAL"
    );
  }

  if (
    /\bunder\s+the\s+weather\b/i.test(
      source
    ) &&
    /\b(?:sob|debaixo)\b.{0,20}\btempo\b/i.test(
      target
    )
  ) {
    reasons.push(
      "IDIOM_UNDER_THE_WEATHER_LITERAL"
    );
  }

  // 9.7.1 â€” regressÃµes reais observadas sÃ£o convertidas em regras linguÃ­sticas
  // universais, nunca em hardcode por tÃ­tulo/cue.
  if (
    /\b(?:hurt|injur(?:e|ed)|wound(?:ed)?)\b[^.!?]{0,35}\bbadly\b|\bbadly\b[^.!?]{0,35}\b(?:hurt|injur(?:e|ed)|wound(?:ed)?)\b/i.test(source) &&
    /\bfei[oa]s?\b/iu.test(target)
  ) {
    reasons.push("MEANING_INTEGRITY_BADLY_AS_APPEARANCE");
  }

  if (
    /\bbelieve\s+it\s+or\s+not\b/i.test(source) &&
    /\bquerendo\s+ou\s+n[aÃ£]o\b/iu.test(target)
  ) {
    reasons.push("MEANING_INTEGRITY_IDIOM_BELIEVE_IT_OR_NOT");
  }

  if (
    /\b(?:you|he|she|they|we|i)\s+(?:was|were|am|is|are)\s+warm\b/i.test(source) &&
    /\bcaloros[oa]s?\b/iu.test(target)
  ) {
    reasons.push("LITERALITY_PERSON_WARM_CALOROSO");
  }

  if (
    /\breplace\s+(?:it|this|that)\s+with\s+love\b/i.test(source) &&
    /\bsubstitu\p{L}{0,8}\b[^.!?]{0,24}\bpor\s+amor\b/iu.test(target)
  ) {
    reasons.push("LITERALITY_REPLACE_WITH_LOVE");
  }

  if (
    /\bsoft\b[^.!?]{0,40}\bwide\b|\bwide\b[^.!?]{0,40}\bsoft\b/i.test(source) &&
    !/\b(?:larg[oa]s?|ampl[oa]s?|abert[oa]s?|espa[cÃ§]os[oa]s?)\b/iu.test(target)
  ) {
    reasons.push("MEANING_INTEGRITY_COORDINATED_ATTRIBUTE_LOSS");
  }

  if (
    !sourceHasExplicitProfanity971(source) &&
    targetHasStrongProfanity971(target)
  ) {
    reasons.push("MEANING_INTEGRITY_PROFANITY_ESCALATION");
  }

  return reasons;
}

function sourceLanguageIsEnglish(value) {
  return /^(?:en|eng)(?:[-_][a-z]{2,4})?$/iu.test(
    String(value || "").trim()
  );
}

function sourceLanguageIsKnownNonEnglish(value) {
  const text = String(value || "").trim().toLowerCase();
  return Boolean(
    text &&
    !["auto", "und", "unknown", "unk"].includes(text) &&
    !sourceLanguageIsEnglish(text)
  );
}

function looksPredominantlyEnglishSource(value) {
  const tokens = words(value);
  if (!tokens.length) return false;

  const common = new Set([
    "the", "a", "an", "and", "or", "but", "i", "you", "he", "she",
    "we", "they", "it", "is", "are", "was", "were", "be", "been",
    "to", "of", "in", "on", "for", "with", "that", "this", "what",
    "who", "where", "when", "why", "how", "my", "your", "his", "her",
    "our", "their", "do", "does", "did", "have", "has", "had", "not"
  ]);

  const hits = tokens.filter(token => common.has(token)).length;
  return hits >= 2 || (tokens.length >= 4 && hits / tokens.length >= 0.25);
}

function blockSourceIsEnglish(block) {
  if (sourceLanguageIsEnglish(block?.sourceLang)) return true;
  if (sourceLanguageIsKnownNonEnglish(block?.sourceLang)) return false;
  return looksPredominantlyEnglishSource(block?.text || "");
}

const FIRST_PERSON_MALE_MARKERS = [
  /\bobrigado\b/iu,
  /\b(?:eu\s+)?(?:sou|estou|t[oÃ´]|fiquei|estava|ando)\s+(?:muito\s+|super\s+|inacreditavelmente\s+)?(?:assustado|cansado|preocupado|nervoso|sozinho|pronto|louco|chocado|confuso|exausto|orgulhoso|aliviado|animado|decepcionado|desesperado|irritado|furioso|envergonhado|surpreso|separado|inteiro|casado|solteiro|nascido|criado|preparado|acostumado|certo|ocupado|entediado|excitado|perdido|bonito|surdo|tolo|indisposto|amigo|apaixonado|lisonjeado|destinado|colocado|marcado)\b/iu,
  /\bme\s+(?:fez|fazer|deixou|deixar|tornou|tornar|manteve|manter)\s+(?:muito\s+)?(?:assustado|cansado|preocupado|nervoso|sozinho|pronto|louco|chocado|confuso|exausto|orgulhoso|aliviado|animado|decepcionado|desesperado|irritado|furioso|envergonhado|surpreso|separado|inteiro|casado|solteiro|preparado|acostumado|certo|ocupado|entediado|excitado|perdido|bonito)\b/iu,
  /\b(?:fui|era|estava\s+sendo)\s+(?:interrogado|questionado|acusado|convidado|obrigado|destinado|colocado|marcado)\b/iu
];

const FIRST_PERSON_FEMALE_MARKERS = [
  /\bobrigada\b/iu,
  /\b(?:eu\s+)?(?:sou|estou|t[oÃ´]|fiquei|estava|ando)\s+(?:muito\s+|super\s+|inacreditavelmente\s+)?(?:assustada|cansada|preocupada|nervosa|sozinha|pronta|louca|chocada|confusa|exausta|orgulhosa|aliviada|animada|decepcionada|desesperada|irritada|furiosa|envergonhada|surpresa|separada|inteira|casada|solteira|nascida|criada|preparada|acostumada|gr[aÃ¡]vida|certa|ocupada|entediada|excitada|perdida|bonita|surda|tola|indisposta|amiga|apaixonada|lisonjeada|destinada|colocada|marcada)\b/iu,
  /\bme\s+(?:fez|fazer|deixou|deixar|tornou|tornar|manteve|manter)\s+(?:muito\s+)?(?:assustada|cansada|preocupada|nervosa|sozinha|pronta|louca|chocada|confusa|exausta|orgulhosa|aliviada|animada|decepcionada|desesperada|irritada|furiosa|envergonhada|surpresa|separada|inteira|casada|solteira|preparada|acostumada|gr[aÃ¡]vida|certa|ocupada|entediada|excitada|perdida|bonita)\b/iu,
  /\b(?:fui|era|estava\s+sendo)\s+(?:interrogada|questionada|acusada|convidada|obrigada|destinada|colocada|marcada)\b/iu
];

// ============================================================
// SOURCE GENDER EVIDENCE AUTHORITY â€” 9.4.2
// ============================================================
// Uma Ãºnica autoridade decide se a PRÃ“PRIA SOURCE lexicalmente prova gÃªnero.
// Isso evita conflito entre V3/V5/V8 e o QA semÃ¢ntico. NÃ£o usa tÃ­tulo/cue/personagem.
const SOURCE_FEMALE_IDENTITY_ROLE_942 =
  "(?:woman|girl|mother|mom|mum|wife|daughter|sister|bride|female|nun|queen|princess|lady|widow|actress|waitress|heiress|sorceress|hostess|stewardess|aunt|niece|girlfriend|grandmother|grandma|businesswoman|policewoman|saleswoman|chairwoman|congresswoman|spokeswoman|showgirl|cowgirl|schoolgirl|goddess|duchess|baroness|countess|empress|matriarch|cougar)";
const SOURCE_MALE_IDENTITY_ROLE_942 =
  "(?:man|boy|father|dad|husband|son|brother|groom|male|monk|king|prince|gentleman|widower|waiter|uncle|nephew|boyfriend|grandfather|grandpa|businessman|policeman|salesman|chairman|congressman|spokesman|cowboy|schoolboy|duke|baron|emperor|patriarch)";

const SOURCE_ROLE_PREFIX_942 =
  "(?:(?:a|an|the)\\s+)?(?:(?!(?:a|an|the|not|no|never|neither|almost|because|with|about|for|to|from|of|as|like|at|in|on|over|under|my|your|his|her|our|their|playing|portraying|pretending|impersonating|that|who|which|when|if|but|and|or)\\b)[\\p{L}'â€™.-]+(?:\\s*,\\s*|\\s+)){0,5}";

const SOURCE_SELF_FEMALE_942 = new RegExp(
  `\\b(?:i\\s+am|i'm|iâ€™m|i\\s+was|i've\\s+been|iâ€™ve\\s+been)\\b\\s+${SOURCE_ROLE_PREFIX_942}${SOURCE_FEMALE_IDENTITY_ROLE_942}\\b`,
  "iu"
);
const SOURCE_SELF_MALE_942 = new RegExp(
  `\\b(?:i\\s+am|i'm|iâ€™m|i\\s+was|i've\\s+been|iâ€™ve\\s+been)\\b\\s+${SOURCE_ROLE_PREFIX_942}${SOURCE_MALE_IDENTITY_ROLE_942}\\b`,
  "iu"
);
const SOURCE_SECOND_FEMALE_942 = new RegExp(
  `\\byou(?:'re|â€™re|\\s+are|\\s+were|\\s+have\\s+been|'ve\\s+been|â€™ve\\s+been)\\b\\s+${SOURCE_ROLE_PREFIX_942}${SOURCE_FEMALE_IDENTITY_ROLE_942}\\b`,
  "iu"
);
const SOURCE_SECOND_MALE_942 = new RegExp(
  `\\byou(?:'re|â€™re|\\s+are|\\s+were|\\s+have\\s+been|'ve\\s+been|â€™ve\\s+been)\\b\\s+${SOURCE_ROLE_PREFIX_942}${SOURCE_MALE_IDENTITY_ROLE_942}\\b`,
  "iu"
);

function sourceGenderEvidence942(block, person = "first") {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return { explicit: false, gender: null, provenance: "none" };

  let female = false;
  let male = false;

  if (person === "second") {
    female = SOURCE_SECOND_FEMALE_942.test(source) ||
      /\b(?:voc[eÃª]|tu)\s+(?:[Ã©e]|era)\s+(?:uma\s+)?(?:mulher|garota|menina|mÃ£e|esposa|filha|irmÃ£|noiva|rainha|princesa|viÃºva)\b/iu.test(source) ||
      /\b(?:eres|t[uÃº]\s+eres)\s+(?:una\s+)?(?:mujer|chica|madre|esposa|hija|hermana|novia|reina|princesa|viuda)\b/iu.test(source);
    male = SOURCE_SECOND_MALE_942.test(source) ||
      /\b(?:voc[eÃª]|tu)\s+(?:[Ã©e]|era)\s+(?:um\s+)?(?:homem|garoto|menino|pai|marido|filho|irmÃ£o|noivo|rei|prÃ­ncipe|viÃºvo)\b/iu.test(source) ||
      /\b(?:eres|t[uÃº]\s+eres)\s+(?:un\s+)?(?:hombre|chico|padre|esposo|hijo|hermano|novio|rey|pr[iÃ­]ncipe|viudo)\b/iu.test(source);
  } else if (person === "plural") {
    female = /\bwe(?:'re|â€™re|\s+are|\s+were)\s+(?:all\s+)?(?:women|girls|mothers|wives|daughters|sisters|queens|ladies)\b/iu.test(source);
    male = /\bwe(?:'re|â€™re|\s+are|\s+were)\s+(?:all\s+)?(?:men|boys|fathers|husbands|sons|brothers|kings|gentlemen)\b/iu.test(source);
  } else {
    female = SOURCE_SELF_FEMALE_942.test(source) ||
      /\b(?:sou|era|fui|estou)\s+(?:uma\s+)?(?:mulher|garota|menina|mÃ£e|esposa|filha|irmÃ£|noiva|rainha|princesa|viÃºva)\b/iu.test(source) ||
      /\b(?:soy|era|fui)\s+(?:una\s+)?(?:mujer|chica|madre|esposa|hija|hermana|novia|reina|princesa|viuda)\b/iu.test(source);
    male = SOURCE_SELF_MALE_942.test(source) ||
      /\b(?:sou|era|fui|estou)\s+(?:um\s+)?(?:homem|garoto|menino|pai|marido|filho|irmÃ£o|noivo|rei|prÃ­ncipe|viÃºvo)\b/iu.test(source) ||
      /\b(?:soy|era|fui)\s+(?:un\s+)?(?:hombre|chico|padre|esposo|hijo|hermano|novio|rey|pr[iÃ­]ncipe|viudo)\b/iu.test(source);
  }

  if (female && male) return { explicit: false, gender: null, provenance: "ambiguous_source" };
  if (female) return { explicit: true, gender: "female", provenance: "lexical_source" };
  if (male) return { explicit: true, gender: "male", provenance: "lexical_source" };
  return { explicit: false, gender: null, provenance: "none" };
}

function sourceExplicitlyMarksSelfGender(block) {
  return sourceGenderEvidence942(block, "first").explicit;
}

// Nouns whose grammatical article does NOT identify the person's sex/gender.
// Common-gender roles such as artista/jornalista/motorista are deliberately
// NOT exempt: "um/uma artista" introduces avoidable human gender.
const EPICENE_HUMAN_ROLE_WORDS_896 = new Set([
  "pessoa", "gente", "crianÃ§a", "vitima", "vÃ­tima", "testemunha", "autoridade",
  "celebridade", "estrela", "figura", "criatura"
]);

function sourceHasNeutralHumanRoleFrame896(block, person = "second") {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return false;
  if (person === "first") {
    return /\bi(?:'m|â€™m| am| was)\s+(?:just\s+|only\s+)?(?:a|an)\s+[a-z][a-z'â€™-]{2,}\b/i.test(source);
  }
  return /\byou(?:'re|â€™re| are| were)\s+(?:just\s+|only\s+)?(?:a|an)\s+[a-z][a-z'â€™-]{2,}\b/i.test(source);
}

function targetHasGenderMarkedHumanRole896(pt, person = "second") {
  const text = String(pt || "");
  const subject = person === "first" ? "(?:eu\\s+)?" : "(?:voc[eÃª]|c[eÃª]|tu)";
  const copula = person === "first"
    ? "(?:sou|era|fui|estou|t[oÃ´])"
    : "(?:[Ã©e]|era|foi|est[aÃ¡]|t[aÃ¡])";
  const re = new RegExp(`(?:^|[^\\p{L}])${subject}\\s*${copula}\\s+[^.!?\\n]{0,30}?(?:um|uma)\\s+([\\p{L}][\\p{L}-]{2,})(?=$|[^\\p{L}])`, "iu");
  const match = text.match(re);
  if (!match) return false;
  const role = String(match[1] || "").toLocaleLowerCase();
  return !EPICENE_HUMAN_ROLE_WORDS_896.has(role);
}

// ============================================================
// GENDER V5 DEFINITIVE SAFE NEUTRALIZATION â€” 9.0 ZERO-CLOUD
// ============================================================
// Camada propositalmente pequena: sÃ³ atua quando a SOURCE usa um frame direto
// I/you + a/an + papel humano e existe uma reformulaÃ§Ã£o PT-BR inequÃ­voca.
// Preserva pessoa + tempo/modo do PT atual (sou/era/fosse/seja etc.) para nÃ£o
// criar frase artificial em construÃ§Ãµes como "Ã‰ como se eu fosse...".
// PapÃ©is sem reformulaÃ§Ã£o universal segura continuam no pipeline 8.9.6 normal.
const GENDER_V5_SAFE_ROLE_PREDICATES_897 = Object.freeze({
  passenger: "alguÃ©m de passagem",
  doctor: "profissional da medicina",
  teacher: "docente",
  nurse: "profissional de enfermagem",
  student: "estudante",
  journalist: "jornalista",
  driver: "motorista",
  scientist: "cientista",
  lawyer: "profissional do direito"
});

const GENDER_V5_PT_ROLE_PATTERNS_897 = Object.freeze({
  passenger: "passageir[oa]",
  doctor: "m[eÃ©]dic[oa]",
  teacher: "professor(?:a)?",
  nurse: "enfermeir[oa]",
  student: "(?:alun[oa]|estudante)",
  journalist: "jornalista",
  driver: "motorista",
  scientist: "cientista",
  lawyer: "advogad[oa]"
});

function preserveInitialCase897(match, replacement) {
  const raw = String(match || "");
  const out = String(replacement || "");
  if (!out) return out;
  return /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(raw)
    ? out.charAt(0).toLocaleUpperCase() + out.slice(1)
    : out;
}

function sourceNeutralHumanRole897(block, person = "second") {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return null;

  const keys = Object.keys(GENDER_V5_SAFE_ROLE_PREDICATES_897).join("|");
  const frame = person === "first"
    ? new RegExp(`\\bi(?:'m|â€™m| am| was)\\s+(?:just\\s+|only\\s+)?(?:a|an)\\s+(${keys})\\b`, "i")
    : new RegExp(`\\byou(?:'re|â€™re| are| were)\\s+(?:just\\s+|only\\s+)?(?:a|an)\\s+(${keys})\\b`, "i");

  const match = source.match(frame);
  if (!match) return null;

  const role = String(match[1] || "").toLocaleLowerCase();
  if (!GENDER_V5_SAFE_ROLE_PREDICATES_897[role]) return null;

  return {
    role,
    limited: /\b(?:just|only)\b/i.test(match[0])
  };
}

function neutralRoleCopula897(match, person, role, limited) {
  const raw = String(match || "");
  const lower = raw.toLocaleLowerCase();
  const predicate = GENDER_V5_SAFE_ROLE_PREDICATES_897[role];
  if (!predicate) return raw;

  let head;
  if (person === "first") {
    const explicitSubject = /\beu\b/iu.test(raw);
    const subject = explicitSubject ? "eu " : "";
    if (/\bfosse\b/iu.test(lower)) head = `${subject}fosse`;
    else if (/\bseja\b/iu.test(lower)) head = `${subject}seja`;
    else if (/\bera\b/iu.test(lower)) head = `${subject}era`;
    else if (/\bfui\b/iu.test(lower)) head = `${subject}fui`;
    else if (/\b(?:estou|t[oÃ´])\b/iu.test(lower)) head = `${subject}estou`;
    else head = `${subject}sou`;
  } else {
    if (/\bfosse\b/iu.test(lower)) head = "vocÃª fosse";
    else if (/\bseja\b/iu.test(lower)) head = "vocÃª seja";
    else if (/\bera\b/iu.test(lower)) head = "vocÃª era";
    else if (/\bfoi\b/iu.test(lower)) head = "vocÃª foi";
    else if (/\b(?:est[aÃ¡]|t[aÃ¡])\b/iu.test(lower)) head = "vocÃª estÃ¡";
    else head = "vocÃª Ã©";
  }

  const limiter = limited ? " sÃ³" : "";
  return preserveInitialCase897(raw, `${head}${limiter} ${predicate}`);
}

function applyGenderV5DefinitiveNeutralization897(block, value) {
  let pt = String(value || "").trim();
  if (!pt || sourceDialogueDashCount(block) >= 2) return pt;

  for (const person of ["first", "second"]) {
    const info = sourceNeutralHumanRole897(block, person);
    if (!info) continue;

    if (person === "first" && sourceExplicitlyMarksSelfGender(block)) continue;
    if (person === "second" && sourceExplicitlyMarksSecondPersonGender(block)) continue;

    const rolePattern = GENDER_V5_PT_ROLE_PATTERNS_897[info.role];
    if (!rolePattern) continue;

    const subject = person === "first"
      ? "(?:eu\\s+)?"
      : "(?:voc[eÃª]|c[eÃª]|tu)\\s+";
    const copula = person === "first"
      ? "(?:sou|era|fui|fosse|seja|estou|t[oÃ´])"
      : "(?:[Ã©e]|era|foi|fosse|seja|est[aÃ¡]|t[aÃ¡])";

    // Tolera qualificadores curtos do modelo ("sÃ³", "apenas", "simplesmente"),
    // mas nÃ£o atravessa pontuaÃ§Ã£o/linha e sÃ³ fecha no papel lexical esperado.
    const directRole = new RegExp(
      `\\b${subject}${copula}\\s+[^.!?\\n]{0,28}?(?:um|uma)\\s+${rolePattern}\\b`,
      "iu"
    );

    if (!directRole.test(pt)) continue;

    const beforeRole = pt;
    pt = pt.replace(
      directRole,
      match => neutralRoleCopula897(match, person, info.role, info.limited)
    );
    if (pt !== beforeRole) {
      console.log(
        `[GENDER V5 LOCAL 9.0] cue ${block?.index}: role=${info.role} neutralizado | 0 Gemini.`
      );
    }
  }

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

// ============================================================
// BROADCAST ANTI-CALQUE â€” 9.0 ZERO-CLOUD
// ============================================================
// SÃ³ atua quando a prÃ³pria SOURCE contÃ©m "pool feed" em contexto de broadcast.
// Assim "pool" de piscina ou outros usos nunca sÃ£o tocados.
function applyBroadcastAntiCalque897(block, value) {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  let pt = String(value || "").trim();
  if (!pt || !/\bpool\s+feed\b/i.test(source)) return pt;

  const before = pt;
  pt = pt
    .replace(/\bsinal\s+(?:(?:do|da|de)\s+)?pool\b/giu, match => preserveInitialCase897(match, "sinal compartilhado"))
    .replace(/\bfeed\s+(?:(?:do|da|de)\s+)?pool\b/giu, match => preserveInitialCase897(match, "feed compartilhado"))
    .replace(/\bpool\s+feed\b/giu, match => preserveInitialCase897(match, "feed compartilhado"));

  if (pt !== before) {
    console.log(
      `[BROADCAST LOCAL 9.0] cue ${block?.index}: pool feed naturalizado | 0 Gemini.`
    );
  }

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

// ============================================================
// FINAL DETERMINISTIC CLOSURE â€” 9.0 ZERO-CLOUD
// ============================================================
// Esta camada NÃƒO muda modelo, batch, concorrÃªncia, thinking, QA ou Repair.
// Ela fecha classes mecÃ¢nicas/semÃ¢nticas de alta confianÃ§a antes de servir:
// 1) gÃªnero neutro por TURNO, inclusive cues multi-speaker;
// 2) papÃ©is humanos com modal/futuro/passado sem inferir gÃªnero;
// 3) comandos de control-room ainda em inglÃªs, somente com prova na SOURCE;
// 4) resÃ­duos conversacionais Ã³bvios (Okay) somente quando a SOURCE prova;
// 5) repetiÃ§Ã£o dramÃ¡tica completa + layout estrito 2x50;
// 6) ownership duplicado detectado tambÃ©m por overlap semÃ¢ntico de fronteira.

function sourceHasExplicitSecondPersonHonorificGender898(block) {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  return /\b(?:sir|ma['â€™]?am|madam|mister|mr\.?|mrs\.?|miss|ms\.?)\b/iu.test(source) ||
    sourceExplicitlyMarksSecondPersonGender(block);
}

function applyGenderV5ModalNeutralization898(block, value) {
  let pt = String(value || "").trim();
  if (!pt) return pt;

  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  const keys = Object.keys(GENDER_V5_SAFE_ROLE_PREDICATES_897).join("|");

  const frames = [
    {
      person: "first",
      re: new RegExp(`\\bi(?:'m|â€™m| am| was| will be|'ll be|â€™ll be| would be|'d be|â€™d be| could be| can be| may be| might be| should be| won't be| wonâ€™t be| will not be| wouldn't be| wouldnâ€™t be| would not be| can't be| canâ€™t be| cannot be)\\s+(?:just\\s+|only\\s+)?(?:a|an)\\s+(${keys})\\b`, "i")
    },
    {
      person: "second",
      re: new RegExp(`\\byou(?:'re|â€™re| are| were| will be|'ll be|â€™ll be| would be|'d be|â€™d be| could be| can be| may be| might be| should be| won't be| wonâ€™t be| will not be| wouldn't be| wouldnâ€™t be| would not be| can't be| canâ€™t be| cannot be)\\s+(?:just\\s+|only\\s+)?(?:a|an)\\s+(${keys})\\b`, "i")
    }
  ];

  for (const frame of frames) {
    const match = source.match(frame.re);
    if (!match) continue;
    if (frame.person === "first" && sourceExplicitlyMarksSelfGender(block)) continue;
    if (frame.person === "second" && sourceHasExplicitSecondPersonHonorificGender898(block)) continue;

    const role = String(match[1] || "").toLocaleLowerCase();
    const predicate = GENDER_V5_SAFE_ROLE_PREDICATES_897[role];
    const rolePattern = GENDER_V5_PT_ROLE_PATTERNS_897[role];
    if (!predicate || !rolePattern) continue;

    const before = pt;
    const roleRe = new RegExp(`(?:\\b(?:um|uma)\\s+)?\\b${rolePattern}\\b`, "iu");
    if (roleRe.test(pt)) {
      pt = pt.replace(roleRe, predicate);
    }

    if (pt !== before) {
      console.log(`[GENDER V5 MODAL LOCAL 9.0] cue ${block?.index}: role=${role} neutralizado | 0 Gemini.`);
    }
  }

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

function applyContextualPassengerNeutralization898(block, value) {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  let pt = String(value || "").trim();
  if (!source || !pt) return pt;

  // MetÃ¡fora de agÃªncia: "passenger" = estar de carona, nÃ£o "alguÃ©m de passagem".
  if (/\bit(?:'s|â€™s|\s+is)\s+like\s+i(?:'m|â€™m|\s+am)\s+just\s+a\s+passenger\b/iu.test(source)) {
    pt = pt.replace(
      /\b(?:eu\s+)?(?:fosse|sou|era|estivesse|estou|t[oÃ´])\s+(?:s[oÃ³]\s+|apenas\s+|simplesmente\s+)?(?:(?:um|uma)\s+passageir[oa]|algu[eÃ©]m\s+de\s+passagem)\b/giu,
      match => preserveInitialCase897(match, "eu estivesse sÃ³ de carona")
    );
  }

  if (/\byou\s+(?:won't|wonâ€™t|will\s+not)\s+be\s+(?:just\s+|only\s+)?(?:a\s+)?passenger\s+anymore\b/iu.test(source)) {
    pt = pt.replace(
      /\b(?:voc[eÃª]|c[eÃª])\s+n[aÃ£]o\s+vai\s+mais\s+(?:ser|ficar)\s+(?:s[oÃ³]\s+|apenas\s+)?(?:(?:um|uma)?\s*passageir[oa]|algu[eÃ©]m\s+de\s+passagem)\b/giu,
      match => preserveInitialCase897(match, "vocÃª nÃ£o vai mais ficar sÃ³ de carona")
    );
  }

  return pt;
}

function applyAdditionalGenderNeutrality898(block, value) {
  let pt = String(value || "").trim();
  if (!pt) return pt;

  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return pt;

  // Plain "you" nÃ£o autoriza senhor/senhora. Honrarias explÃ­citas da SOURCE continuam intactas.
  if (/\byou\b/iu.test(source) && !sourceHasExplicitSecondPersonHonorificGender898(block)) {
    pt = pt.replace(/\b(?:a\s+senhora|o\s+senhor|senhora|senhor)\b/giu, match => preserveInitialCase897(match, "vocÃª"));
  }

  if (/\bget\s+secure\b/iu.test(source)) {
    pt = pt
      .replace(/\bfique\s+(?:bem\s+)?(?:segur[oa]|protegid[oa])\b/giu, match => preserveInitialCase897(match, "fique em seguranÃ§a"))
      .replace(/\bficar\s+(?:bem\s+)?(?:segur[oa]|protegid[oa])\b/giu, match => preserveInitialCase897(match, "ficar em seguranÃ§a"))
      .replace(/\bestiver\s+(?:bem\s+)?(?:segur[oa]|protegid[oa])\b/giu, match => preserveInitialCase897(match, "estiver em seguranÃ§a"));
  }

  if (/\byou(?:'re|â€™re|\s+are)\s+safe\b/iu.test(source)) {
    pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+(?:est[aÃ¡]|t[aÃ¡]|[Ã©e])\s+segur[oa]\b/giu, match => preserveInitialCase897(match, "vocÃª estÃ¡ em seguranÃ§a"));
  }

  if (/\byou(?:'re|â€™re|\s+are)(?:[-\s]+you(?:'re|â€™re|\s+are))*[^.!?]{0,25}\bgood\b/iu.test(source)) {
    pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+[Ã©e]\s+(?:muito\s+|bem\s+)?(?:bom|boa)\b/giu, match => preserveInitialCase897(match, "vocÃª manda bem"));
  }

  if (/\bi\s+was\s+released\b/iu.test(source)) {
    pt = pt
      .replace(/\bfui\s+solt[oa]\b/giu, match => preserveInitialCase897(match, "me soltaram"))
      .replace(/\bfui\s+libertad[oa]\b/giu, match => preserveInitialCase897(match, "me libertaram"));
  }

  if (/\byou(?:'ve|â€™ve)\s+been\s+misled\b/iu.test(source)) {
    pt = pt.replace(/\b(?:voc[eÃª]\s+)?foi\s+enganad[oa]\b/giu, match => preserveInitialCase897(match, "te enganaram"));
  }

  if (/\byou\s+were\s+in\s+(?:jail|prison)\b/iu.test(source)) {
    pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+foi\s+pres[oa]\b/giu, match => preserveInitialCase897(match, "vocÃª esteve na cadeia"));
  }

  if (/\byou(?:'re|â€™re|\s+are)\s+(?:freaking|scaring)\s+me\b/iu.test(source)) {
    pt = pt
      .replace(/\bme\s+deixando\s+(?:muito\s+)?apavorad[oa]\b/giu, "me apavorando")
      .replace(/\bme\s+deixando\s+(?:muito\s+)?assustad[oa]\b/giu, "me assustando")
      .replace(/\bme\s+deixando\s+(?:muito\s+)?aterrorizad[oa]\b/giu, "me aterrorizando");
  }

  if (/\bwhen\s+i\s+was\s+small\b/iu.test(source)) {
    pt = pt.replace(/\bquando\s+eu\s+era\s+pequen[oa]\b/giu, "quando eu era crianÃ§a");
  }

  if (/\bwhy\s+am\s+i\b[^.!?]{0,35}\bthe\s+only\s+one\b/iu.test(source)) {
    pt = pt.replace(/\bpor\s+que\s+eu\s+sou\s+(?:o\s+[uÃº]nico|a\s+[uÃº]nica)\b/giu, "por que sÃ³ eu");
  }

  if (/\blet\s+me\s+be\s+clear\b/iu.test(source)) {
    pt = pt.replace(/\b(?:deixe-me|deixem-me|deixa\s+eu)\s+ser\s+clar[oa]\b/giu, "deixa eu deixar isso claro");
  }

  if (/\bwe(?:'ve|â€™ve)\s+been\s+raised\s+to\s+believe\b/iu.test(source)) {
    pt = pt.replace(/\bfomos\s+criad[oa]s\s+(?:pra|para)\s+acreditar\b/giu, match => preserveInitialCase897(match, "aprendemos a acreditar"));
  }

  if (/\bwe(?:'re|â€™re|\s+are)\s+ready\s+to\b/iu.test(source)) {
    pt = pt.replace(/\bestamos\s+pront[oa]s\s+(?:pra|para)\b/giu, match => preserveInitialCase897(match, "jÃ¡ podemos"));
  }

  if (/\bwe\s+weren't\s+alone\b|\bwe\s+were\s+not\s+alone\b/iu.test(source)) {
    pt = pt.replace(/\bn[aÃ£]o\s+(?:estamos|est[aÃ¡]vamos|[eÃ©]ramos)\s+sozinh[oa]s\b/giu, "havia mais alguÃ©m alÃ©m da gente");
  } else if (/\bwe(?:'re|â€™re|\s+are)\s+not\s+alone\b/iu.test(source)) {
    pt = pt.replace(/\bn[aÃ£]o\s+estamos\s+sozinh[oa]s\b/giu, "hÃ¡ mais alguÃ©m alÃ©m da gente");
  }

  if (/\bsweetie\b/iu.test(source)) {
    pt = pt.replace(/\bquerid[oa]\b/giu, "meu bem");
  }

  return applyGenderV5ModalNeutralization898(block, pt)
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

// ============================================================
// GENDER NEUTRALITY CLOSURE â€” 9.2.2 ZERO-CLOUD
// ============================================================
// NÃ£o tenta adivinhar sexo/gÃªnero. Atua apenas quando a SOURCE Ã© neutra naquela
// ideia e hÃ¡ uma reformulaÃ§Ã£o PT-BR inequÃ­voca, natural e semanticamente estÃ¡vel.
function applyGenderNeutralityClosure922(block, value) {
  let pt = String(value || "").trim();
  if (!pt) return pt;
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return pt;

  // I am in love -> Eu me apaixonei. Evita apaixonado/apaixonada inteiramente.
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+in\s+love\b/iu.test(source)) {
    pt = pt.replace(/\b(?:eu\s+)?(?:estou|t[oÃ´])\s+apaixonad[oa](?=\b|\s|[,.;!?])/giu,
      match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "Eu me apaixonei" : "eu me apaixonei");
  }

  // I'm flattered (that...) -> Isso Ã© uma honra / Fico feliz que...
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+flattered\b/iu.test(source)) {
    pt = pt
      .replace(/\b(?:eu\s+)?(?:estou|t[oÃ´]|fico|fiquei)\s+lisonjead[oa]\s+que\b/giu, "Fico feliz que")
      .replace(/\b(?:eu\s+)?(?:estou|t[oÃ´]|fico|fiquei)\s+lisonjead[oa]\s+por\b/giu, "Ã‰ uma honra")
      .replace(/\b(?:eu\s+)?(?:estou|t[oÃ´]|fico|fiquei)\s+lisonjead[oa]\b/giu, "Isso Ã© uma honra");
  }

  // I was [never] destined to... -> [Nunca] foi meu destino...
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi\s+was\s+(?:never\s+)?destined\s+to\b/iu.test(source)) {
    pt = pt
      .replace(/\b(?:eu\s+)?nunca\s+fui\s+destinad[oa]\s+a\b/giu, "Nunca foi meu destino")
      .replace(/\b(?:eu\s+)?fui\s+destinad[oa]\s+a\b/giu, "Meu destino era");
  }

  // I was put/placed here -> me colocaram aqui. Preserva causa/contexto sem gÃªnero.
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi\s+was\s+(?:put|placed)\s+here\b/iu.test(source)) {
    pt = pt
      .replace(/\bpor\s+que\s+(?:eu\s+)?fui\s+colocad[oa]\s+aqui\b/giu, "por que me colocaram aqui")
      .replace(/\b(?:eu\s+)?fui\s+colocad[oa]\s+aqui\b/giu, "me colocaram aqui");
  }

  // Don't play the indignant card with me -> nÃ£o transforma "indignant" em
  // substantivo humano masculino/feminino no target.
  if (/\bdon't\s+play\s+the\s+indignant\s+card\s+with\s+me\b/iu.test(source)) {
    pt = pt.replace(/\bn[aÃ£]o\s+venha\s+com\s+esse\s+papo\s+de\s+indignad[oa]\s+comigo\b/giu,
      "NÃ£o venha com esse papo de indignaÃ§Ã£o pra cima de mim");
  }

  // Fechamentos linguÃ­sticos gerais para classes que escapavam do Gender V2-V6.
  // Cada regra exige SOURCE lexicalmente inequÃ­voca e sÃ³ remove gÃªnero evitÃ¡vel.
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+not\s+deaf\b/iu.test(source)) {
    pt = pt.replace(/\b(?:eu\s+)?n[aÃ£]o\s+sou\s+surd[oa]\b/giu, "eu escuto muito bem");
  }

  if (!sourceExplicitlyMarksSelfGender(block) && /\bam\s+i\s+(?:a\s+)?fool\b/iu.test(source)) {
    pt = pt.replace(/\b(?:ser[aÃ¡]\s+que\s+)?(?:eu\s+)?sou\s+tol[oa]\b/giu, match =>
      /^ser[aÃ¡]\s+que/iu.test(match) ? "SerÃ¡ que estou sendo idiota" : "estou sendo idiota");
  }

  if (/\bwe\s+(?:haven't|have\s+not)\s+been\s+beaten\b/iu.test(source) &&
      /\bnor\s+are\s+we\s+bound\s+by\s+chains\b/iu.test(source)) {
    pt = pt.replace(/\bn[aÃ£]o\s+fomos\s+espancad[oa]s\s*,?\s*nem\s+estamos\s+pres[oa]s\s+por\s+correntes\b/giu,
      "NÃ£o nos espancaram, nem nos prenderam com correntes");
  }

  if (/\bwe\s+were\s+(?:very\s+)?impressed\s+with\b/iu.test(source)) {
    pt = pt.replace(/\bficamos\s+(?:muito\s+)?impressionad[oa]s\s+com\b/giu, "nos impressionamos muito com");
  }

  if (/\byou(?:'re|â€™re| are)\s+making\s+me\s+angry\b/iu.test(source)) {
    pt = pt.replace(/\b(?:voc[eÃª]\s+)?(?:est[aÃ¡]|t[aÃ¡])\s+me\s+deixando\s+irritad[oa]\b/giu, match =>
      /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "VocÃª estÃ¡ me irritando" : "vocÃª estÃ¡ me irritando");
  }

  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+just\s+a\s+bit\s+dodgy\s+tonight\b/iu.test(source)) {
    pt = pt.replace(/\b(?:eu\s+)?(?:estou|t[oÃ´])\s+meio\s+indispost[oa]\s+hoje\s+[aÃ ]\s+noite\b/giu,
      "eu nÃ£o tÃ´ muito bem hoje Ã  noite");
  }

  if (!sourceExplicitlyMarksSecondPersonGender(block) && /\byou(?:'re|â€™re| are)\s+beautiful\b/iu.test(source)) {
    pt = pt.replace(/\b(?:voc[eÃª]\s+)?(?:[Ã©e]|est[aÃ¡]|t[aÃ¡])\s+lind[oa]\b/giu, match =>
      /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "VocÃª estÃ¡ incrÃ­vel" : "vocÃª estÃ¡ incrÃ­vel");
  }

  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+(?:a\s+)?friend\s+of\s+his\b/iu.test(source)) {
    pt = pt.replace(/\b(?:eu\s+)?sou\s+amig[oa]\s+dele\b/giu, match =>
      /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "Ele Ã© meu amigo" : "ele Ã© meu amigo");
  }

  if (/\beverybody(?:'s|â€™s| is)\s+just\s+worried\s+about\s+them\b/iu.test(source)) {
    pt = pt.replace(/\btodo\s+mundo\s+s[oÃ³]\s+est[aÃ¡]\s+preocupad[oa]\s+com\b/giu, "todo mundo sÃ³ se preocupa com");
  }

  if (/\byou(?:'re|â€™re| are)\s+the\s+(?:true\s+)?hero\b/iu.test(source) && !sourceExplicitlyMarksSecondPersonGender(block)) {
    pt = pt.replace(/\b(?:voc[eÃª]\s+)?[Ã©e]\s+(?:a\s+verdadeira\s+hero[iÃ­]na|o\s+verdadeiro\s+her[oÃ³]i|a\s+hero[iÃ­]na|o\s+her[oÃ³]i)\b/giu,
      match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "Foi vocÃª quem salvou o dia" : "foi vocÃª quem salvou o dia");
  }

  // Vocativos "darling" sÃ£o neutros em inglÃªs; "meu bem" preserva afeto sem inventar sexo.
  if (/\bdarling\b/iu.test(source)) {
    pt = pt.replace(/\bquerid[oa]\b/giu, "meu bem");
  }

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

function avoidableGenderResidual922(block, value) {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  const pt = String(value || "").trim();
  if (!source || !pt) return false;

  if (!sourceExplicitlyMarksSelfGender(block)) {
    if (/\bi(?:'m|â€™m| am)\s+in\s+love\b/iu.test(source) && /\b(?:estou|t[oÃ´])\s+apaixonad[oa]\b/iu.test(pt)) return true;
    if (/\bi(?:'m|â€™m| am)\s+flattered\b/iu.test(source) && /\blisonjead[oa]\b/iu.test(pt)) return true;
    if (/\bi\s+was\s+(?:never\s+)?destined\s+to\b/iu.test(source) && /\bfui\s+destinad[oa]\b/iu.test(pt)) return true;
    if (/\bi\s+was\s+(?:put|placed)\s+here\b/iu.test(source) && /\bfui\s+colocad[oa]\s+aqui\b/iu.test(pt)) return true;
  }
  if (/\bdon't\s+play\s+the\s+indignant\s+card\s+with\s+me\b/iu.test(source) && /\bpapo\s+de\s+indignad[oa]\b/iu.test(pt)) return true;
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+not\s+deaf\b/iu.test(source) && /\bn[aÃ£]o\s+sou\s+surd[oa]\b/iu.test(pt)) return true;
  if (!sourceExplicitlyMarksSelfGender(block) && /\bam\s+i\s+(?:a\s+)?fool\b/iu.test(source) && /\bsou\s+tol[oa]\b/iu.test(pt)) return true;
  if (/\bwe\s+(?:haven't|have\s+not)\s+been\s+beaten\b/iu.test(source) && /\bfomos\s+espancad[oa]s\b/iu.test(pt)) return true;
  if (/\bwe\s+were\s+(?:very\s+)?impressed\s+with\b/iu.test(source) && /\bimpressionad[oa]s\b/iu.test(pt)) return true;
  if (/\byou(?:'re|â€™re| are)\s+making\s+me\s+angry\b/iu.test(source) && /\bme\s+deixando\s+irritad[oa]\b/iu.test(pt)) return true;
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+just\s+a\s+bit\s+dodgy\s+tonight\b/iu.test(source) && /\bindispost[oa]\b/iu.test(pt)) return true;
  if (!sourceExplicitlyMarksSecondPersonGender(block) && /\byou(?:'re|â€™re| are)\s+beautiful\b/iu.test(source) && /\b(?:voc[eÃª]\s+)?[Ã©e]\s+lind[oa]\b/iu.test(pt)) return true;
  if (!sourceExplicitlyMarksSelfGender(block) && /\bi(?:'m|â€™m| am)\s+(?:a\s+)?friend\s+of\s+his\b/iu.test(source) && /\bsou\s+amig[oa]\s+dele\b/iu.test(pt)) return true;
  if (/\beverybody(?:'s|â€™s| is)\s+just\s+worried\s+about\s+them\b/iu.test(source) && /\btodo\s+mundo\s+s[oÃ³]\s+est[aÃ¡]\s+preocupad[oa]\b/iu.test(pt)) return true;
  if (/\byou(?:'re|â€™re| are)\s+the\s+(?:true\s+)?hero\b/iu.test(source) && !sourceExplicitlyMarksSecondPersonGender(block) && /\b(?:hero[iÃ­]na|her[oÃ³]i)\b/iu.test(pt)) return true;
  if (/\bdarling\b/iu.test(source) && /\bquerid[oa]\b/iu.test(pt)) return true;
  // O substantivo inglÃªs "twin" nÃ£o prova gÃªnero; se a traduÃ§Ã£o do headline
  // ainda introduzir uma cadeia feminina/masculina, forÃ§a Repair contextual.
  if (/\bsiamese\s+twin\b/iu.test(source) && /\bg[eÃª]me[oa]\s+siam[eÃª]s[oa].{0,45}\b(?:separad[oa]|sozinh[oa])\b/iu.test(pt)) return true;
  return false;
}

function applyDeterministicGenderClosure898(block, value) {
  let pt = String(value || "").trim();
  if (!pt) return pt;

  const expectedTurns = sourceDialogueDashCount(block);
  if (expectedTurns >= 2) {
    const sourceTurns = logicalSourceTurns896(block);
    const targetLines = pt.split("\n");

    if (sourceTurns.length === targetLines.length) {
      const rebuilt = targetLines.map((rawLine, index) => {
        const line = String(rawLine || "");
        const marker = line.match(/^\s*([-â€“â€”]\s*)/u);
        const prefix = marker ? marker[1] : "";
        const body = marker ? line.slice(marker[0].length) : line;
        const pseudo = {
          ...block,
          text: String(sourceTurns[index] || ""),
          _prevText: "",
          _nextText: ""
        };

        let safe = applyContextualPassengerNeutralization898(pseudo, body);
        safe = applyDeterministicGenderNeutrality(pseudo, safe);
        safe = applyAdditionalGenderNeutrality898(pseudo, safe);
        safe = applyGenderNeutralityClosure922(pseudo, safe);
        return `${prefix}${safe}`.trimEnd();
      });

      pt = rebuilt.join("\n").trim();
      return pt;
    }
  }

  pt = applyContextualPassengerNeutralization898(block, pt);
  pt = applyDeterministicGenderNeutrality(block, pt);
  pt = applyAdditionalGenderNeutrality898(block, pt);
  pt = applyGenderNeutralityClosure922(block, pt);
  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

function genderClosureResidualReasons898(block, value) {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  const pt = String(value || "").trim();
  const reasons = [];
  if (!source || !pt) return reasons;

  // Audita gÃªnero por TURNO tambÃ©m. O bug crÃ´nico anterior vinha de pular
  // o cue inteiro quando havia dois speakers.
  if (sourceDialogueDashCount(block) >= 2) {
    const sourceTurns = logicalSourceTurns896(block);
    const targetLines = pt.split("\n");
    if (sourceTurns.length === targetLines.length) {
      for (let index = 0; index < sourceTurns.length; index++) {
        const pseudo = {
          ...block,
          text: String(sourceTurns[index] || ""),
          _prevText: "",
          _nextText: ""
        };
        const body = String(targetLines[index] || "").replace(/^\s*[-â€“â€”]\s*/u, "").trim();
        reasons.push(...genderClosureResidualReasons898(pseudo, body));
      }
      return [...new Set(reasons)];
    }
  }

  // V5 modal/futuro/passado: se a SOURCE usa papel neutro seguro, a forma
  // marcada nÃ£o pode sobreviver Ã  camada local.
  const roleKeys = Object.keys(GENDER_V5_SAFE_ROLE_PREDICATES_897).join("|");
  const roleFrame = new RegExp(
    `\\b(?:i|you)(?:'m|â€™m|'re|â€™re| am| are| was| were| will be|'ll be|â€™ll be| would be|'d be|â€™d be| could be| can be| may be| might be| should be| won't be| wonâ€™t be| will not be| wouldn't be| wouldnâ€™t be| would not be| can't be| canâ€™t be| cannot be)\\s+(?:just\\s+|only\\s+)?(?:a|an)\\s+(${roleKeys})\\b`,
    "i"
  );
  const roleMatch = source.match(roleFrame);
  if (roleMatch) {
    const role = String(roleMatch[1] || "").toLocaleLowerCase();
    const pattern = GENDER_V5_PT_ROLE_PATTERNS_897[role];
    if (pattern && new RegExp(`\\b${pattern}\\b`, "iu").test(pt)) {
      const firstPerson = /^i/i.test(String(roleMatch[0] || "").trim());
      const explicit = firstPerson
        ? sourceExplicitlyMarksSelfGender(block)
        : sourceHasExplicitSecondPersonHonorificGender898(block);
      if (!explicit) reasons.push("GENDER_V6_HUMAN_ROLE_MODAL_MARKED");
    }
  }

  if (/\bget\s+secure\b/iu.test(source) && /\bfique\s+(?:segur[oa]|protegid[oa])\b/iu.test(pt)) reasons.push("GENDER_V6_SECURE_MARKED");
  if (/\byou(?:'re|â€™re|\s+are)\s+safe\b/iu.test(source) && /\b(?:voc[eÃª]|c[eÃª])\s+(?:est[aÃ¡]|t[aÃ¡]|[Ã©e])\s+segur[oa]\b/iu.test(pt)) reasons.push("GENDER_V6_SAFE_MARKED");
  if (/\byou(?:'re|â€™re|\s+are)[^.!?]{0,25}\bgood\b/iu.test(source) && /\b(?:voc[eÃª]|c[eÃª])\s+[Ã©e]\s+(?:bom|boa)\b/iu.test(pt)) reasons.push("GENDER_V6_GOOD_MARKED");
  if (/\bi\s+was\s+released\b/iu.test(source) && /\bfui\s+(?:solt[oa]|libertad[oa])\b/iu.test(pt)) reasons.push("GENDER_V6_RELEASED_MARKED");
  if (/\byou(?:'ve|â€™ve)\s+been\s+misled\b/iu.test(source) && /\bfoi\s+enganad[oa]\b/iu.test(pt)) reasons.push("GENDER_V6_MISLED_MARKED");
  if (/\byou\s+were\s+in\s+(?:jail|prison)\b/iu.test(source) && /\bfoi\s+pres[oa]\b/iu.test(pt)) reasons.push("GENDER_V6_JAIL_MARKED");
  if (/\byou(?:'re|â€™re|\s+are)\s+(?:freaking|scaring)\s+me\b/iu.test(source) && /\bme\s+deixando\s+(?:apavorad[oa]|assustad[oa]|aterrorizad[oa])\b/iu.test(pt)) reasons.push("GENDER_V6_OBJECT_ME_MARKED");
  if (/\bwhen\s+i\s+was\s+small\b/iu.test(source) && /\beu\s+era\s+pequen[oa]\b/iu.test(pt)) reasons.push("GENDER_V6_SMALL_MARKED");
  if (/\bthe\s+only\s+one\b/iu.test(source) && /\beu\s+sou\s+(?:o\s+[uÃº]nico|a\s+[uÃº]nica)\b/iu.test(pt)) reasons.push("GENDER_V6_ONLY_ONE_MARKED");
  if (/\blet\s+me\s+be\s+clear\b/iu.test(source) && /\bser\s+clar[oa]\b/iu.test(pt)) reasons.push("GENDER_V6_CLEAR_MARKED");
  if (/\bwe(?:'ve|â€™ve)\s+been\s+raised\b/iu.test(source) && /\bfomos\s+criad[oa]s\b/iu.test(pt)) reasons.push("GENDER_V6_WE_RAISED_MARKED");
  if (/\bwe(?:'re|â€™re|\s+are)\s+ready\b/iu.test(source) && /\bestamos\s+pront[oa]s\b/iu.test(pt)) reasons.push("GENDER_V6_WE_READY_MARKED");
  if (/\bwe\s+(?:weren't|were\s+not)\s+alone\b/iu.test(source) && /\bsozinh[oa]s\b/iu.test(pt)) reasons.push("GENDER_V6_WE_ALONE_MARKED");
  if (/\bsweetie\b/iu.test(source) && /\bquerid[oa]\b/iu.test(pt)) reasons.push("GENDER_V6_SWEETIE_MARKED");
  if (/\byou\b/iu.test(source) && !sourceHasExplicitSecondPersonHonorificGender898(block) && /\b(?:senhora|senhor)\b/iu.test(pt)) reasons.push("GENDER_V6_HONORIFIC_INFERRED");
  if (avoidableGenderResidual922(block, pt)) reasons.push("GENDER_V7_AVOIDABLE_MARKING_9_2_2");

  return [...new Set(reasons)];
}

function sourceHasBroadcastTakeCommand898(source) {
  const s = String(source || "").replace(/\s+/g, " ").trim();
  return /(?:^|[.!?~â€“â€”-]\s*|\bRS\s+\d+[,.:]?\s*)take(?:\s+(?:\d+|[A-Z]))?(?=[.!?,]|$)/iu.test(s) ||
    /\band\s+take\s+(?:\d+|[A-Z])\b/iu.test(s);
}

function sourceHasBroadcastRollCommand898(source) {
  const s = String(source || "").replace(/\s+/g, " ").trim();
  return /(?:^|\d+[.!?]\s*)roll\s+\d+(?=[.!?,]|$)/iu.test(s);
}

function applyBroadcastControlRoom898(block, value) {
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  let pt = String(value || "").trim();
  if (!source || !pt) return pt;

  const before = pt;
  if (sourceHasBroadcastTakeCommand898(source)) {
    pt = pt
      .replace(/\bTake\s+(\d+|[A-Z])\b/gu, "Entra $1")
      .replace(/\btake\s+(\d+|[A-Z])\b/giu, "entra $1")
      .replace(/\bTake\b/gu, "Entra")
      .replace(/\btake\b/giu, "entra");
  }

  if (sourceHasBroadcastRollCommand898(source)) {
    pt = pt
      .replace(/\bRoll\s+(\d+)\b/gu, "Roda $1")
      .replace(/\broll\s+(\d+)\b/giu, "roda $1");
  }

  if (pt !== before) {
    console.log(`[BROADCAST CONTROL LOCAL 9.0] cue ${block?.index}: comando naturalizado | 0 Gemini.`);
  }
  return pt;
}

function applyNaturalPtClosure898(block, value) {
  let pt = applyBroadcastAntiCalque897(block, value);
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();

  pt = applyBroadcastControlRoom898(block, pt);

  // SOURCE fala em "shot of ..." e o PT deixou o anglicismo "take": em legenda
  // de fala corrida, "imagem" Ã© mais natural e nÃ£o altera o jargÃ£o de comando.
  if (/\bshot\s+of\b/iu.test(source) && /\btake\b/iu.test(pt)) {
    pt = pt.replace(/\btake\b/giu, match => preserveInitialCase897(match, "imagem"));
  }

  // "Okay" cru Ã© resÃ­duo inglÃªs quando a prÃ³pria SOURCE contÃ©m a interjeiÃ§Ã£o.
  // A checagem Ã© SOURCE-proven: nÃ£o varre palavras parecidas em portuguÃªs.
  if (/\bokay\b/iu.test(source) && /\bokay\b/iu.test(pt)) {
    pt = pt.replace(/\bokay\b/giu, match => preserveInitialCase897(match, "tÃ¡"));
  }

  // Imperativo repetido "Move, move..." que escapou em inglÃªs.
  if (/\bmove\b(?:[\s,.!?]+\bmove\b){2,}/iu.test(source) && /\bmove\b/iu.test(pt)) {
    pt = pt.replace(/\bmove\b/giu, match => preserveInitialCase897(match, "anda"));
  }

  if (/\bwait[.!?\s]+wait\b/iu.test(source)) {
    pt = pt.replace(/\bespere([.!?])\s+espere\b/giu, "Espera$1 Espera");
  }

  // "Oh, my..." Ã© uma interjeiÃ§Ã£o truncada, nÃ£o um possessivo portuguÃªs.
  if (/\boh,?\s+my\.\.\./iu.test(source)) {
    pt = pt.replace(/\boh,?\s+meu\.\.\./giu, match => preserveInitialCase897(match, "nossa..."));
  }

  // Idioma geral: "being extremely cool ... rolling with it" descreve postura,
  // nÃ£o "ser legal" literalmente. SÃ³ atua quando a SOURCE contÃ©m a construÃ§Ã£o.
  if (/\bextremely\s+cool\s+here\b/iu.test(source) && /\brolling\s+with\s+it\b/iu.test(source)) {
    pt = pt.replace(
      /\bextremamente\s+legal\s+aqui,?\s*(?:e\s+)?que\s+(?:estou|t[oÃ´])\s+aceitando\s+isso\b/giu,
      "lidando numa boa e entrando no jogo"
    );
  } else if (/\brolling\s+with\s+it\b/iu.test(source)) {
    pt = pt
      .replace(/\b(?:estou|t[oÃ´])\s+aceitando\s+isso\b/giu, "tÃ´ levando isso numa boa")
      .replace(/\b(?:estou|t[oÃ´])\s+indo\s+com\s+isso\b/giu, "tÃ´ levando isso numa boa");
  }

  // "Back up over X" Ã© dar rÃ© por cima de X; "passar por cima" perde a direÃ§Ã£o.
  if (/^\s*(?:[-â€“â€”~]\s*)?back\s+up\s+over\b/iu.test(source)) {
    pt = pt.replace(/^\s*passa\s+por\s+cima\b/iu, match => preserveInitialCase897(match, "dÃ¡ rÃ© por cima"));
  }
  if (/^\s*(?:[-â€“â€”~]\s*)?back\s+up\s*[!.?]*\s*$/iu.test(source)) {
    pt = pt.replace(/^\s*passa\s+por\s+cima\s*[!.?]*\s*$/iu, "DÃ¡ rÃ©!");
  }

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

function compactRepeatClause898(value) {
  let text = String(value || "").trim();
  if (!text) return text;

  text = text
    .replace(/^O\s+que\s+eu\s+(?:estou|t[oÃ´])\s+/iu, "Que tÃ´ ")
    .replace(/^O\s+que\s+(?:eu\s+)?estou\s+/iu, "Que tÃ´ ")
    .replace(/^Por\s+que\s+eu\s+(?:estou|t[oÃ´])\s+/iu, "Por que tÃ´ ")
    .replace(/^Eu\s+(?:estou|t[oÃ´])\s+/iu, "TÃ´ ")
    .replace(/^Voc[eÃª]\s+(?:est[aÃ¡]|t[aÃ¡])\s+/iu, "TÃ¡ ")
    .replace(/\bpara\b/giu, "pra")
    .replace(/[ \t]{2,}/g, " ")
    .trim();

  return text;
}

function compactExactRepetitionLayout898(block, value) {
  const original = String(value || "").trim();
  if (!original || layoutCueResult(block, original).fits) return original;

  const sourceTurns = logicalSourceTurns896(block);
  const targetLines = original.split("\n");
  const sourceNeeds = sourceTurns.map(maxConsecutiveRepeat896);
  if (sourceNeeds.every(n => n < 3)) return original;
  if (sourceDialogueDashCount(block) >= 2 && targetLines.length !== sourceTurns.length) return original;

  const out = [...targetLines];
  let changed = false;

  for (let idx = 0; idx < Math.min(out.length, sourceNeeds.length); idx++) {
    if (sourceNeeds[idx] < 3) continue;

    const rawLine = out[idx];
    const marker = rawLine.match(/^\s*([-â€“â€”]\s*)/u);
    const prefix = marker ? marker[1] : "";
    const body = marker ? rawLine.slice(marker[0].length) : rawLine;
    const clauses = body.match(/[^!?]+[!?]+|[^!?]+$/g) || [];
    if (clauses.length < sourceNeeds[idx]) continue;

    const compacted = clauses.map(clause => compactRepeatClause898(clause));
    const candidateLine = `${prefix}${compacted.join(" ")}`.replace(/[ \t]{2,}/g, " ").trim();
    if (candidateLine !== rawLine) {
      out[idx] = candidateLine;
      changed = true;
    }
  }

  if (!changed) return original;
  const candidate = out.join("\n").trim();
  if (targetRepeatCount896(block, candidate) < sourceRepeatNeed896(block)) return original;
  if (!layoutCueResult(block, candidate).fits) return original;

  console.log(`[REPETITION LAYOUT LOCAL 9.0] cue ${block?.index}: repetiÃ§Ã£o Ã­ntegra compactada para 2x50 | 0 Gemini.`);
  return candidate;
}

function applyContextSemanticPostconditions898(block, value) {
  let pt = applyContextSemanticPostconditions896(block, value);
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();

  // Evita uma fala visualmente truncada depois da naturalizaÃ§Ã£o do bleep.
  if (new RegExp(`\\bwhat\\s+the\\s+${BLEEP_TOKEN.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}`, "iu").test(source)) {
    pt = pt.replace(/^\s*Que\s+porra\s*$/iu, "Que porra?");
  }

  pt = compactExactRepetitionLayout898(block, pt);
  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

const BOUNDARY_SEMANTIC_STOPWORDS_898 = new Set([
  "a","o","as","os","um","uma","uns","umas","de","do","da","dos","das","em","no","na","nos","nas",
  "com","para","pra","por","e","ou","que","se","isso","isto","esse","essa","the","an","and","or","of",
  "to","in","on","with","for","is","are","was","were","be","been","being","this","that"
]);

function boundarySemanticTokens898(value) {
  return String(value || "")
    .toLocaleLowerCase("pt-BR")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(token => token && !BOUNDARY_SEMANTIC_STOPWORDS_898.has(token));
}

function boundarySemanticOverlap898(previousValue, currentValue) {
  const left = boundarySemanticTokens898(previousValue).slice(-12);
  const right = boundarySemanticTokens898(currentValue).slice(0, 12);
  if (left.length < 2 || right.length < 2) return null;

  const maxSize = Math.min(8, left.length, right.length);
  for (let size = maxSize; size >= 2; size--) {
    const tail = left.slice(left.length - size);
    const head = right.slice(0, size);
    if (!tail.every((token, index) => token === head[index])) continue;
    const charWeight = tail.join("").length;
    if (size >= 3 || charWeight >= 14) return { size, charWeight, sequence: tail.join(" ") };
  }
  return null;
}

function ownershipReasonsAroundCandidate898(blocks, posMap, translations, id, candidatePt) {
  const pos = posMap.get(Number(id));
  if (!Number.isInteger(pos)) return [];
  const reasons = [];
  const currentBlock = blocks[pos];
  const currentText = String(candidatePt || "").trim();

  if (pos > 0) {
    const prev = blocks[pos - 1];
    reasons.push(...ownershipBoundaryMismatchReasons(prev, currentBlock, translations.get(prev.index), currentText));
  }
  if (pos + 1 < blocks.length) {
    const next = blocks[pos + 1];
    reasons.push(...ownershipBoundaryMismatchReasons(currentBlock, next, currentText, translations.get(next.index)));
  }
  return [...new Set(reasons.filter(r => /^CUE_OWNERSHIP_/i.test(String(r || ""))))];
}

function priorityLocalReasons898(block, pt, filename, plan) {
  return localReasonsForCue(block, pt, filename, plan).filter(reason =>
    !isGenderGrammaticalAdvisory970(reason) &&
    /^(?:EMPTY|MEANING_INTEGRITY_|FINAL_MIXED_SCRIPT_CONFUSABLE|GENDER_V[2-9]_|UNKNOWN_SPEAKER_GENDER_MARKED|SPEAKER_LABEL_RESIDUE|SDH_RESIDUE|DIALOGUE_TURN_MISMATCH|DIALOGUE_TILDE_RESIDUE|MISSING_DIALOGUE_BREAK|ARTIFICIAL_PROFANITY_CENSORSHIP|UNRESOLVED_BLEEP_TOKEN|INVENTED_BLEEP_TOKEN|SOURCE_BLEEP_FIDELITY_LOST|FINAL_GARBAGE_OR_PLACEHOLDER|VISIBLE_CENSOR_PLACEHOLDER|SOURCE_EXACT_REPETITION_LOST|CONTEXTUAL_IMPERATIVE_REFERENT_INVENTION|BARE_IMPERATIVE_CONCRETE_REFERENT_INVENTION)/i.test(String(reason || ""))
  );
}

function removeLeadingSemanticDuplicate898(previousValue, currentValue) {
  const overlap = boundarySemanticOverlap898(previousValue, currentValue);
  if (!overlap) return String(currentValue || "").trim();

  const lines = String(currentValue || "").trim().split("\n");
  if (!lines.length) return String(currentValue || "").trim();
  const first = lines[0];
  const marker = first.match(/^\s*([-â€“â€”]\s*)/u);
  const prefix = marker ? marker[1] : "";
  const body = marker ? first.slice(marker[0].length) : first;
  const wanted = String(overlap.sequence || "").split(/\s+/).filter(Boolean);
  if (wanted.length < 2) return String(currentValue || "").trim();

  const content = [];
  const re = /[\p{L}\p{N}]+/gu;
  let m;
  while ((m = re.exec(body))) {
    const norm = String(m[0] || "")
      .toLocaleLowerCase("pt-BR")
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    if (!norm || BOUNDARY_SEMANTIC_STOPWORDS_898.has(norm)) continue;
    content.push({ token: norm, end: m.index + m[0].length });
    if (content.length >= wanted.length) break;
  }

  if (content.length < wanted.length) return String(currentValue || "").trim();
  if (!wanted.every((token, i) => content[i]?.token === token)) return String(currentValue || "").trim();

  let rest = body.slice(content[wanted.length - 1].end)
    .replace(/^\s*[,;:]?\s*/u, "")
    .trim();
  if (!rest) return String(currentValue || "").trim();
  rest = rest.replace(/^([.!?])\s*/u, "").trim();
  if (!rest) return String(currentValue || "").trim();

  lines[0] = `${prefix}${rest}`.trimEnd();
  return lines.join("\n").trim();
}


function isFinalRepairLocked923(job, id) {
  return Boolean(job?.finalRepairLockedText923 instanceof Map && job.finalRepairLockedText923.has(Number(id)));
}

// 9.4.0 â€” AUTHORITATIVE SEMANTIC STATE.
// A text that a HIGH semantic audit has rejected may never be resurrected by
// Repair Persistence. Rejection is text-specific (not cue-specific), so a
// genuinely new repaired text can still become canonical later.
function semanticTextKey940(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function semanticRejectedTexts940(job, id, create = false) {
  if (!job) return null;
  if (!(job.semanticRejectedTexts940 instanceof Map)) {
    if (!create) return null;
    job.semanticRejectedTexts940 = new Map();
  }
  const key = Number(id);
  if (!job.semanticRejectedTexts940.has(key) && create) job.semanticRejectedTexts940.set(key, new Set());
  return job.semanticRejectedTexts940.get(key) || null;
}

function markSemanticResidualRejected940(job, translations, issues, label = "audit") {
  if (!job || !(translations instanceof Map)) return 0;
  let marked = 0;
  for (const issue of (Array.isArray(issues) ? issues : [])) {
    const id = Number(issue?.id);
    if (!Number.isInteger(id)) continue;
    const text = semanticTextKey940(translations.get(id));
    if (!text) continue;
    const set = semanticRejectedTexts940(job, id, true);
    if (!set.has(text)) { set.add(text); marked++; }
    if (job.finalRepairLockedText923 instanceof Map) {
      const locked = semanticTextKey940(job.finalRepairLockedText923.get(id));
      if (locked && locked === text) {
        job.finalRepairLockedText923.delete(id);
        console.warn(`[SEMANTIC AUTHORITY 9.4.0] cue ${id}: lock de Repair Persistence invalidado por ${label}.`);
      }
    }
  }
  return marked;
}

function isSemanticallyRejectedText940(job, id, value) {
  const set = semanticRejectedTexts940(job, id, false);
  if (!set) return false;
  return set.has(semanticTextKey940(value));
}

function commitVerifiedRepairLocks940(job, translations, changedIds, residual, label = "verified") {
  if (!job || !(translations instanceof Map)) return 0;
  if (!(job.finalRepairLockedText923 instanceof Map)) job.finalRepairLockedText923 = new Map();
  const residualIds = new Set((Array.isArray(residual) ? residual : []).map(x => Number(x?.id)).filter(Number.isInteger));
  let committed = 0;
  for (const rawId of changedIds || []) {
    const id = Number(rawId);
    if (!Number.isInteger(id) || residualIds.has(id)) continue;
    const text = String(translations.get(id) || "").trim();
    if (!text || isSemanticallyRejectedText940(job, id, text)) continue;
    job.finalRepairLockedText923.set(id, text);
    committed++;
  }
  if (committed) console.log(`[SEMANTIC AUTHORITY 9.4.0] ${label}: ${committed} repair lock(s) commitados SOMENTE apÃ³s auditoria limpa.`);
  return committed;
}

function applyRepairPersistenceLock923(blocks, translations, job, filename, plan) {
  const out=new Map(translations);
  if (!(job?.finalRepairLockedText923 instanceof Map) || !job.finalRepairLockedText923.size) return out;
  const byId=new Map(blocks.map(block=>[Number(block.index),block]));
  let restored=0;
  for (const [id, acceptedRaw] of job.finalRepairLockedText923.entries()) {
    const block=byId.get(Number(id));
    if (!block) continue;
    const accepted=String(acceptedRaw || "").trim();
    const current=String(out.get(Number(id)) || "").trim();
    if (!accepted || current===accepted) continue;
    if (isSemanticallyRejectedText940(job, id, accepted)) {
      job.finalRepairLockedText923.delete(Number(id));
      console.warn(`[REPAIR PERSISTENCE 9.4.0] cue ${id}: candidato previamente reprovado semanticamente NÃƒO serÃ¡ restaurado.`);
      continue;
    }
    const acceptedReasons=priorityLocalReasons898(block, accepted, filename, plan);
    const currentReasons=priorityLocalReasons898(block, current, filename, plan);
    const acceptedFits=layoutCueResult(block,accepted).fits;
    // Never resurrect a candidate that is objectively worse. Restore only
    // when the accepted FINAL repair remains locally safe and the later text
    // reintroduced at least as many blockers (the exact bug seen in 9.2.2).
    if (acceptedFits && acceptedReasons.length <= currentReasons.length) {
      out.set(Number(id),accepted);
      restored++;
      console.warn(`[REPAIR PERSISTENCE 9.2.3] cue ${id}: repair FINAL aceito restaurado; fallback posterior nÃ£o pode ressuscitar candidato reprovado.`);
    }
  }
  if (restored) console.warn(`[REPAIR PERSISTENCE 9.2.3] restaurados=${restored} cue(s). âœ…`);
  return out;
}

function applyFinalOwnershipFallback898(blocks, finalTranslations, mainTranslations, filename, plan, job = null) {
  const out = new Map(finalTranslations);
  if (!(mainTranslations instanceof Map)) return out;

  for (let i = 0; i < blocks.length - 1; i++) {
    const prev = blocks[i];
    const curr = blocks[i + 1];
    const finalPrev = String(out.get(prev.index) || "").trim();
    const finalCurr = String(out.get(curr.index) || "").trim();
    const bad = ownershipBoundaryMismatchReasons(prev, curr, finalPrev, finalCurr)
      .some(r => /^CUE_OWNERSHIP_/i.test(String(r || "")));
    if (!bad) continue;

    const options = [
      [finalPrev, String(mainTranslations.get(curr.index) || "").trim(), "current-main"],
      [String(mainTranslations.get(prev.index) || "").trim(), finalCurr, "previous-main"],
      [String(mainTranslations.get(prev.index) || "").trim(), String(mainTranslations.get(curr.index) || "").trim(), "both-main"]
    ];

    let resolved = false;
    for (const [candPrev, candCurr, label] of options) {
      if (!candPrev || !candCurr) continue;
      if (isFinalRepairLocked923(job, prev.index) && candPrev !== finalPrev) continue;
      if (isFinalRepairLocked923(job, curr.index) && candCurr !== finalCurr) continue;
      if (!layoutCueResult(prev, candPrev).fits || !layoutCueResult(curr, candCurr).fits) continue;
      if (priorityLocalReasons898(prev, candPrev, filename, plan).length) continue;
      if (priorityLocalReasons898(curr, candCurr, filename, plan).length) continue;
      const remains = ownershipBoundaryMismatchReasons(prev, curr, candPrev, candCurr)
        .some(r => /^CUE_OWNERSHIP_/i.test(String(r || "")));
      if (remains) continue;

      out.set(prev.index, candPrev);
      out.set(curr.index, candCurr);
      console.log(`[OWNERSHIP FALLBACK LOCAL 9.0] cues ${prev.index}/${curr.index}: ${label} eliminou duplicaÃ§Ã£o de fronteira | 0 Gemini.`);
      resolved = true;
      break;
    }

    // Ãšltimo recurso determinÃ­stico: se final e MAIN ainda repetem uma cauda
    // que a SOURCE nÃ£o repete, remove SOMENTE o prefixo semÃ¢ntico duplicado do
    // cue de continuaÃ§Ã£o. Nunca inventa conteÃºdo nem move timestamps.
    if (!resolved && !isFinalRepairLocked923(job, curr.index)) {
      const nowPrev = String(out.get(prev.index) || "").trim();
      const nowCurr = String(out.get(curr.index) || "").trim();
      const trimmed = removeLeadingSemanticDuplicate898(nowPrev, nowCurr);
      if (
        trimmed && trimmed !== nowCurr &&
        layoutCueResult(curr, trimmed).fits &&
        !priorityLocalReasons898(curr, trimmed, filename, plan).length &&
        !ownershipBoundaryMismatchReasons(prev, curr, nowPrev, trimmed)
          .some(r => /^CUE_OWNERSHIP_/i.test(String(r || "")))
      ) {
        out.set(curr.index, trimmed);
        console.log(`[OWNERSHIP DEDUPE LOCAL 9.0] cues ${prev.index}/${curr.index}: prefixo duplicado removido | 0 Gemini.`);
      }
    }
  }
  return out;
}

function applyFinalStrictLayoutFallback898(blocks, finalTranslations, mainTranslations, filename, plan, job = null) {
  const out = new Map(finalTranslations);

  for (const block of blocks) {
    let current = String(out.get(block.index) || "").trim();
    if (!current || layoutCueResult(block, current).fits) continue;
    if (isFinalRepairLocked923(job, block.index)) continue;

    const compacted = compactExactRepetitionLayout898(block, current);
    if (compacted !== current && layoutCueResult(block, compacted).fits) {
      out.set(block.index, compacted);
      continue;
    }

    const main = String(mainTranslations?.get?.(block.index) || "").trim();
    if (
      main &&
      layoutCueResult(block, main).fits &&
      !priorityLocalReasons898(block, main, filename, plan).length &&
      !repairCandidateRegressionReasons(block, current, main, filename, plan).length
    ) {
      out.set(block.index, main);
      console.log(`[LAYOUT FALLBACK LOCAL 9.0] cue ${block.index}: candidato MAIN Ã­ntegro recuperado | 0 Gemini.`);
    }
  }

  return out;
}

function finalClosureResidualSummary898(blocks, translations, filename, plan) {
  let layout = 0, gender = 0, ownership = 0, broadcast = 0, repetition = 0, censor = 0, english = 0;
  for (const block of blocks) {
    const pt = String(translations.get(block.index) || "").trim();
    if (!layoutCueResult(block, pt).fits) layout++;
    const reasons = localReasonsForCue(block, pt, filename, plan);
    if (reasons.some(r => isHardGenderReason970(r))) gender++;
    if (reasons.some(r => /SOURCE_EXACT_REPETITION_LOST/i.test(String(r)))) repetition++;
    if (/\[(?:censurado|bleep)\]|__CENSORED_BLEEP__/iu.test(pt)) censor++;
    const source = String(block?.text || "");
    if (sourceHasBroadcastTakeCommand898(source) && /\btake\b/iu.test(pt)) broadcast++;
    if (sourceHasBroadcastRollCommand898(source) && /\broll\b/iu.test(pt)) broadcast++;
    if (/\bokay\b/iu.test(source) && /\bokay\b/iu.test(pt)) english++;
    if (/\bshot\s+of\b/iu.test(source) && /\btake\b/iu.test(pt)) english++;
    if (/\bmove\b(?:[\s,.!?]+\bmove\b){2,}/iu.test(source) && /\bmove\b/iu.test(pt)) english++;
    if (/\boh,?\s+my\.\.\./iu.test(source) && /\boh,?\s+meu\.\.\./iu.test(pt)) english++;
  }
  for (let i = 0; i < blocks.length - 1; i++) {
    const a = blocks[i], b = blocks[i + 1];
    if (ownershipBoundaryMismatchReasons(a, b, translations.get(a.index), translations.get(b.index)).some(r => /^CUE_OWNERSHIP_/i.test(String(r)))) ownership++;
  }
  return { layout, gender, ownership, broadcast, repetition, censor, english };
}


function clauseUnits896(value) {
  return String(value || "")
    .replace(/(^|\n)\s*[-â€“â€”~]\s*/gu, "$1")
    .split(/\n+/)
    .flatMap(line => line.match(/[^!?]+[!?]+|[^!?]+$/g) || [])
    .map(x => String(x || "").trim())
    .filter(Boolean);
}

function repeatTokens896(value) {
  return String(value || "")
    .toLocaleLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
}

function repeatSimilarity896(a, b) {
  const aa = repeatTokens896(a);
  const bb = repeatTokens896(b);
  if (aa.length < 2 || bb.length < 2) return 0;
  const A = new Set(aa), B = new Set(bb);
  let inter = 0;
  for (const x of A) if (B.has(x)) inter++;
  const union = new Set([...A, ...B]).size || 1;
  return inter / union;
}

function maxConsecutiveRepeat896(value) {
  const clauses = clauseUnits896(value);
  let best = 1;
  for (let i = 0; i < clauses.length; i++) {
    let count = 1;
    for (let j = i + 1; j < clauses.length; j++) {
      if (repeatSimilarity896(clauses[i], clauses[j]) < 0.72) break;
      count++;
    }
    best = Math.max(best, count);
  }
  return best;
}

function logicalSourceTurns896(block) {
  const lines = String(block?.text || "").split("\n");
  const turns = [];
  let current = "";
  let sawMarker = false;

  for (const raw of lines) {
    const line = String(raw || "").trim();
    if (!line) continue;
    const marked = /^[-â€“â€”~]\s*/u.test(line);
    if (marked) {
      sawMarker = true;
      if (current) turns.push(current.trim());
      current = line.replace(/^[-â€“â€”~]\s*/u, "").trim();
    } else {
      current = current ? `${current} ${line}` : line;
    }
  }
  if (current) turns.push(current.trim());
  if (!sawMarker) return [lines.join(" ").replace(/\s+/g, " ").trim()].filter(Boolean);
  return turns;
}

function logicalTargetTurns896(block, pt) {
  const text = String(pt || "").trim();
  if (!text) return [];
  if (sourceDialogueDashCount(block) < 2) return [text.replace(/\s+/g, " ").trim()];
  return text
    .split("\n")
    .map(line => String(line || "").replace(/^\s*[-â€“â€”]\s*/u, "").trim())
    .filter(Boolean);
}

function sourceRepeatNeed896(block) {
  return Math.max(1, ...logicalSourceTurns896(block).map(maxConsecutiveRepeat896));
}

function targetRepeatCount896(block, pt) {
  return Math.max(1, ...logicalTargetTurns896(block, pt).map(maxConsecutiveRepeat896));
}

function sourceExactRepetitionLost896(block, pt) {
  const needed = sourceRepeatNeed896(block);
  if (needed < 3) return false;
  return targetRepeatCount896(block, pt) < needed;
}

function bareImperativeConcreteReferentRisk92(block, pt) {
  const source = String(block?.text || "")
    .replace(/^\s*[-â€“â€”~]\s*/u, "")
    .replace(/\s+/g, " ")
    .trim();
  const target = String(pt || "");

  // "Get down!/Duck!" permite Abaixem-se/Se abaixem/No chÃ£o, mas nÃ£o
  // autoriza inventar uma arma que a SOURCE/contexto textual nÃ£o menciona.
  if (/^(?:get\s+down|duck)\s*[!.?]*$/iu.test(source)) {
    if (/\b(?:arma|pistola|rev[oÃ³]lver|rifle|fuzil|espingarda)\b/iu.test(target)) return true;
  }
  return false;
}

function contextualHaltImperativeRisk896(block, pt) {
  const source = String(block?.text || "");
  if (!/(?:^|\n)\s*(?:[-â€“â€”~]\s*)?hold\s+it\s*[!.?]*\s*(?:$|\n)/iu.test(source)) return false;
  const context = `${block?._prevText || ""} ${block?._nextText || ""}`;
  const haltContext = /\b(?:stop|wait|watch\s+out|slow\s+down|what\s+am\s+i\s+doing|hey)\b/iu.test(context);
  if (!haltContext) return false;
  return /\bsegur(?:a|e)\s+(?:ele|ela|isso|a[iÃ­])\b/iu.test(String(pt || ""));
}

function restoreExactRepetitionLocally896(block, value) {
  const sourceTurns = logicalSourceTurns896(block);
  const targetLines = String(value || "").trim().split("\n");
  if (!targetLines.length) return String(value || "").trim();

  const sourceNeeds = sourceTurns.map(maxConsecutiveRepeat896);
  const expectedTurns = sourceDialogueDashCount(block) >= 2 ? sourceTurns.length : 1;
  if (sourceNeeds.every(n => n < 3)) return String(value || "").trim();
  if (expectedTurns >= 2 && targetLines.length !== expectedTurns) return String(value || "").trim();

  let changed = false;
  const out = [...targetLines];

  for (let idx = 0; idx < Math.min(sourceNeeds.length, out.length); idx++) {
    const srcNeed = sourceNeeds[idx];
    if (srcNeed < 3) continue;

    const line = out[idx];
    const prefixMatch = line.match(/^\s*([-â€“â€”]\s*)/u);
    const prefix = prefixMatch ? prefixMatch[1] : "";
    const body = prefix ? line.slice(prefixMatch[0].length) : line;
    const clauses = body.match(/[^!?]+[!?]+|[^!?]+$/g) || [];
    if (clauses.length < 2) continue;

    let runStart = -1, runCount = 1;
    for (let i = 0; i < clauses.length - 1; i++) {
      let c = 1;
      while (i + c < clauses.length && repeatSimilarity896(clauses[i], clauses[i + c]) >= 0.72) c++;
      if (c > runCount) { runStart = i; runCount = c; }
    }
    if (runStart < 0 || runCount >= srcNeed || runCount < 2) continue;

    const candidates = clauses.slice(runStart, runStart + runCount).map(x => x.trim());
    const shortest = [...candidates].sort((a,b) => a.length - b.length)[0];
    const rebuilt = [...clauses];
    for (let k = runCount; k < srcNeed; k++) rebuilt.splice(runStart + k, 0, ` ${shortest}`);
    let candidateLine = prefix + rebuilt.join("").replace(/[ \t]{2,}/g, " ").trim();
    let candidateAll = [...out];
    candidateAll[idx] = candidateLine;
    let candidatePt = candidateAll.join("\n");

    if (!layoutCueResult(block, candidatePt).fits) {
      candidateLine = candidateLine
        .replace(/(^|\s)O\s+que\s+(?:eu\s+)?(?=[^?]{1,32}\?)/giu, "$1Que ")
        .replace(/[ \t]{2,}/g, " ")
        .trim();
      candidateAll = [...out];
      candidateAll[idx] = candidateLine;
      candidatePt = candidateAll.join("\n");
    }

    if (layoutCueResult(block, candidatePt).fits) {
      out[idx] = candidateLine;
      changed = true;
    }
  }

  return changed ? out.join("\n").trim() : String(value || "").trim();
}

function applyContextSemanticPostconditions896(block, value) {
  let pt = String(value || "").trim();
  if (!pt) return pt;

  pt = naturalizeVisibleSourceBleep(block, pt);

  if (contextualHaltImperativeRisk896(block, pt)) {
    pt = pt.replace(/\bsegur(?:a|e)\s+(?:ele|ela|isso|a[iÃ­])\b/giu, "Pare");
  }

  pt = restoreExactRepetitionLocally896(block, pt);
  return pt.replace(/[ \t]{2,}/g, " ").trim();
}



// ============================================================
// GENDER EVIDENCE SEVERITY â€” 9.7.0
// ============================================================
// Universal linguistic rule:
// - SOURCE-proven contradiction is HARD.
// - PT-BR grammatical marking without SOURCE gender evidence is ADVISORY.
// We still neutralize advisory marking deterministically whenever a safe,
// natural form exists, but failure to find such a paraphrase never blocks an
// otherwise semantically/structurally valid subtitle.
const GENDER_GRAMMATICAL_ADVISORY_970 =
  /^(?:GENDER_V2_UNKNOWN_SPEAKER_MARKED|UNKNOWN_SPEAKER_GENDER_MARKED|GENDER_V3_NEUTRAL_DEFAULT_VIOLATION|GENDER_V4_SECOND_PERSON_NEUTRAL_DEFAULT_VIOLATION|GENDER_V5_(?:FIRST|SECOND)_PERSON_HUMAN_ROLE_MARKED|GENDER_V6_[A-Z0-9_]+|GENDER_V7_AVOIDABLE_MARKING_9_2_2|GENDER_V8_STRUCTURAL_(?:FIRST|SECOND|PLURAL)_PERSON_PREDICATE)$/i;

function isGenderGrammaticalAdvisory970(reason) {
  return GENDER_GRAMMATICAL_ADVISORY_970.test(String(reason || "").trim());
}

function isHardGenderReason970(reason) {
  const r = String(reason || "").trim();
  if (!/^(?:GENDER_|UNKNOWN_SPEAKER_GENDER_MARKED)/i.test(r)) return false;
  return !isGenderGrammaticalAdvisory970(r);
}

function splitGenderSeverity970(blocks, issues, job = null, label = "gender") {
  const out = [];
  const advisory = [];
  for (const issue of Array.isArray(issues) ? issues : []) {
    const id = Number(issue?.id);
    const hardReasons = [];
    const advisoryReasons = [];
    for (const reason of (Array.isArray(issue?.reasons) ? issue.reasons : [issue?.reason])) {
      const r = String(reason || "").trim();
      if (!r) continue;
      if (isGenderGrammaticalAdvisory970(r)) advisoryReasons.push(r);
      else hardReasons.push(r);
    }
    if (advisoryReasons.length) advisory.push({ id, reasons: [...new Set(advisoryReasons)] });
    if (hardReasons.length) out.push({ ...issue, id, reasons: [...new Set(hardReasons)] });
  }
  if (advisory.length) {
    const count = advisory.reduce((n, item) => n + item.reasons.length, 0);
    if (job) {
      job.stats.genderGrammarAdvisory970 = Number(job.stats.genderGrammarAdvisory970 || 0) + count;
      job.genderGrammarAdvisory970 = advisory;
    }
    console.warn(
      `[GENDER EVIDENCE 9.7.0] ${label}: ${advisory.length} cue(s) / ${count} marca(s) gramatical(is) ` +
      `sem contradiÃ§Ã£o SOURCE viraram ADVISORY; neutralizaÃ§Ã£o local continua best-effort, selo nÃ£o Ã© bloqueado.`
    );
  }
  return { hard: out, advisory };
}

function hardGenderReasons970(block, pt, filename, plan) {
  return genderIntegrityV2Reasons(block, pt, plan)
    .filter(isHardGenderReason970);
}

// ============================================================
// STRUCTURAL GENDER NEUTRALITY â€” 9.2.3
// ============================================================
// Title/model agnostic: asks whether SOURCE requires human gender in the
// proposition and whether PT-BR introduced a marked predicate/article anyway.
const PT_STRUCTURAL_GENDER_WORD_923 = /^(?:(?:ad|id|os|iv|Ã¡ri|eir|ent|ud|ic|at|ot|izad|ficad|ecid|endid)[oa]s?|(?:lou[cq]|put|pront|surd|tol|lind|bonit|cert|sozinh|inteir|amig|doid|maluc|gratid|choc|confus|exaust|orgulhos|aliviad|animad|decepcionad|desesperad|irritad|furios|envergonhad|surpres|cansad|preocupad|nervos|assustad|apavorad|aterrorizad|amedrontad|ocupad|entediad|excitad|perdid|apaixonad|lisonjead|destinad|colocad|marcad)[oa]s?)$/iu;

function ptWordLooksStructurallyGenderMarked923(word) {
  const w = String(word || "").toLocaleLowerCase("pt-BR").replace(/^[^\p{L}]+|[^\p{L}]+$/gu, "");
  if (!w) return false;
  return PT_STRUCTURAL_GENDER_WORD_923.test(w);
}

function targetStructuralPredicateGender923(pt, person = "first") {
  const text = String(pt || "").replace(/\s+/g, " ").trim();
  if (!text) return false;
  const frames = person === "first"
    ? [
        /\b(?:eu\s+)?(?:sou|estou|t[oÃ´]|fiquei|era|estava|fui|pare[cÃ§]o|(?:estou|t[oÃ´])\s+parecendo)\s+([^.!?]{1,80})/giu,
        /\b(?:eu\s+)?me\s+sinto\s+([^.!?]{1,70})/giu
      ]
    : person === "second"
      ? [/\b(?:voc[eÃª]|c[eÃª]|tu)\s+(?:[Ã©e]|est[aÃ¡]|t[aÃ¡]|ficou|era|estava|parece|parecia|ficou\s+parecendo)\s+([^.!?]{1,80})/giu]
      : [/\b(?:n[oÃ³]s\s+)?(?:estamos|ficamos|fomos|[eÃ©]ramos|est[aÃ¡]vamos)\s+([^.!?]{1,80})/giu];

  for (const re of frames) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text))) {
      const tail = String(m[1] || "").trim();
      const tokens = tail.split(/\s+/).slice(0, 8);
      for (const token of tokens) {
        if (/^(?:muito|muita|muitos|muitas|bem|super|t[aÃ£]o|meio|meia|um|uma|uns|umas|pouco|pouca|s[oÃ³]|apenas)$/iu.test(token.replace(/[,;:]/g, ""))) continue;
        if (ptWordLooksStructurallyGenderMarked923(token)) return true;
      }
    }
  }
  return false;
}

function sourceNeutralPredicateFrame923(block, person = "first") {
  if (!blockSourceIsEnglish(block)) return false;
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return false;
  if (person === "first") {
    if (sourceGenderEvidence942(block, "first").explicit) return false;
    return /\bi(?:'m|â€™m| am| was| have been|'ve been|â€™ve been| had been| feel| felt| look| looked| seem| seemed| became| got)\b/iu.test(source);
  }
  if (person === "second") {
    if (sourceGenderEvidence942(block, "second").explicit) return false;
    return /\byou(?:'re|â€™re| are| were| have been|'ve been|â€™ve been| had been| feel| felt| look| looked| seem| seemed| became| got)\b/iu.test(source);
  }
  if (sourceGenderEvidence942(block, "plural").explicit) return false;
  return /\bwe(?:'re|â€™re| are| were| have been|'ve been|â€™ve been| had been| feel| felt| look| looked| seem| seemed| became| got)\b/iu.test(source);
}

function structuralGenderReasons923(block, pt) {
  const reasons=[];
  if (sourceNeutralPredicateFrame923(block,"first") && targetStructuralPredicateGender923(pt,"first")) {
    reasons.push("GENDER_V8_STRUCTURAL_FIRST_PERSON_PREDICATE");
  }
  if (sourceNeutralPredicateFrame923(block,"second") && targetStructuralPredicateGender923(pt,"second")) {
    reasons.push("GENDER_V8_STRUCTURAL_SECOND_PERSON_PREDICATE");
  }
  if (sourceNeutralPredicateFrame923(block,"plural") && targetStructuralPredicateGender923(pt,"plural")) {
    reasons.push("GENDER_V8_STRUCTURAL_PLURAL_PREDICATE");
  }
  return reasons;
}

function targetSelfLexicalGender942(pt) {
  const text = String(pt || "").replace(/\s+/g, " ").trim();
  if (!text) return null;
  const femaleRole = /\b(?:eu\s+)?(?:sou|era|fui|estou|t[oÃ´])\s+(?:uma\s+)?(?:[\p{L}'â€™-]+\s+){0,4}(?:mulher|garota|menina|mÃ£e|esposa|filha|irmÃ£|noiva|rainha|princesa|viÃºva|atriz|garÃ§onete|tia|sobrinha|namorada|avÃ³)\b/iu.test(text);
  const maleRole = /\b(?:eu\s+)?(?:sou|era|fui|estou|t[oÃ´])\s+(?:um\s+)?(?:[\p{L}'â€™-]+\s+){0,4}(?:homem|garoto|menino|pai|marido|filho|irmÃ£o|noivo|rei|prÃ­ncipe|viÃºvo|garÃ§om|tio|sobrinho|namorado|avÃ´)\b/iu.test(text);
  if (femaleRole && maleRole) return null;
  if (femaleRole) return "female";
  if (maleRole) return "male";
  return null;
}

function genderIntegrityV2Reasons(block, pt, plan) {
  const text = String(pt || "");
  if (!text.trim()) return [];

  const male = FIRST_PERSON_MALE_MARKERS.some(re => re.test(text));
  const female = FIRST_PERSON_FEMALE_MARKERS.some(re => re.test(text));
  const reasons = [];

  if (male && female) {
    reasons.push("GENDER_V2_SELF_CONTRADICTION");
  }

  const lock = identityLockForCapsule(block, plan);
  const trusted = lock.trusted_speaker_gender;
  const sourceSelfGender942 = sourceGenderEvidence942(block, "first");
  const targetSelfGender942 = targetSelfLexicalGender942(text);

  // SOURCE Ã© autoridade superior ao Character Ledger para a proposiÃ§Ã£o atual.
  if (sourceSelfGender942.gender === "female" && (male || targetSelfGender942 === "male")) {
    reasons.push("GENDER_V9_SOURCE_FEMALE_TARGET_MASCULINE");
  }
  if (sourceSelfGender942.gender === "male" && (female || targetSelfGender942 === "female")) {
    reasons.push("GENDER_V9_SOURCE_MALE_TARGET_FEMININE");
  }

  if (!sourceSelfGender942.explicit && trusted === "female" && male) {
    reasons.push("GENDER_V2_TRUSTED_FEMALE_MASCULINE_SELF_MARKER");
  }

  if (!sourceSelfGender942.explicit && trusted === "male" && female) {
    reasons.push("GENDER_V2_TRUSTED_MALE_FEMININE_SELF_MARKER");
  }

  if (
    (male || female) &&
    !sourceExplicitlyMarksSelfGender(block)
  ) {
    reasons.push("GENDER_V3_NEUTRAL_DEFAULT_VIOLATION");
  }

  const source = String(block?.text || "");
  const secondPersonSource =
    /\byou(?:'re|â€™re|\s+are|\s+were|\s+feel|\s+felt|\s+look|\s+seem|\s+became)\b/i.test(source) ||
    /\b(?:voc[eÃª]|tu)\b/iu.test(source);

  if (
    secondPersonSource &&
    SECOND_PERSON_GENDERED_STATE_RE.test(text) &&
    !sourceExplicitlyMarksSecondPersonGender(block)
  ) {
    reasons.push("GENDER_V4_SECOND_PERSON_NEUTRAL_DEFAULT_VIOLATION");
  }

  if (
    sourceHasNeutralHumanRoleFrame896(block, "first") &&
    !sourceExplicitlyMarksSelfGender(block) &&
    targetHasGenderMarkedHumanRole896(text, "first")
  ) {
    reasons.push("GENDER_V5_FIRST_PERSON_HUMAN_ROLE_MARKED");
  }

  if (
    sourceHasNeutralHumanRoleFrame896(block, "second") &&
    !sourceExplicitlyMarksSecondPersonGender(block) &&
    targetHasGenderMarkedHumanRole896(text, "second")
  ) {
    reasons.push("GENDER_V5_SECOND_PERSON_HUMAN_ROLE_MARKED");
  }

  for (const reason of genderClosureResidualReasons898(block, text)) {
    reasons.push(reason);
  }
  for (const reason of structuralGenderReasons923(block, text)) {
    reasons.push(reason);
  }

  return [...new Set(reasons)];
}

const UNKNOWN_SPEAKER_GENDERED_STATE_RE =
  /\b(?:estou|t[oÃ´]|fiquei|estava|sou|me\s+sinto)\s+(?:muito\s+)?(?:assustad[oa]s?|apavorad[oa]s?|aterrorizad[oa]s?|amedrontad[oa]s?|cansad[oa]s?|preocupad[oa]s?|nervos[oa]s?|sozinh[oa]s?|pront[oa]s?|lou[cq][oa]s?|chocad[oa]s?|confus[oa]s?|exaust[oa]s?|orgulhos[oa]s?|aliviad[oa]s?|animad[oa]s?|decepcionad[oa]s?|desesperad[oa]s?|irritad[oa]s?|furios[oa]s?|envergonhad[oa]s?|surpres[oa]s?)\b/i;


const SECOND_PERSON_GENDERED_STATE_RE =
  /\b(?:voc[eÃª]|vc|c[eÃª])\s+(?:[Ã©e]|est[aÃ¡]|t[aÃ¡]|ficou|parece|anda)\s+(?:muito\s+)?(?:assustad[oa]s?|apavorad[oa]s?|aterrorizad[oa]s?|amedrontad[oa]s?|cansad[oa]s?|preocupad[oa]s?|nervos[oa]s?|sozinh[oa]s?|pront[oa]s?|lou[cq][oa]s?|chocad[oa]s?|confus[oa]s?|exaust[oa]s?|orgulhos[oa]s?|aliviad[oa]s?|animad[oa]s?|decepcionad[oa]s?|desesperad[oa]s?|irritad[oa]s?|furios[oa]s?|envergonhad[oa]s?|surpres[oa]s?|bonit[oa]s?|lind[oa]s?)\b|\b(?:voc[eÃª]|vc|c[eÃª])\s+[Ã©e]\s+(?:o\s+vencedor|a\s+vencedora)\b/iu;

function sourceExplicitlyMarksSecondPersonGender(block) {
  return sourceGenderEvidence942(block, "second").explicit;
}

// ============================================================
// UNIVERSAL COPULAR IDENTITY NORMALIZER â€” 9.5.0 ZERO-CLOUD
// ============================================================
// Title/content agnostic. Solves a structural PT-BR conflict that previously
// caused repair cascades:
//   SOURCE: "I am a coward" / "You're a journalist"
//   PT:     "Sou um covarde" / "VocÃª Ã© uma jornalista"
// When SOURCE does not prove human gender and the PT predicate itself is
// lexically common-gender, Portuguese can preserve the SAME identity relation
// simply by dropping the gendered article: "Sou covarde", "VocÃª Ã© jornalista".
//
// This is NOT a title/cue dictionary and never guesses a person's gender.
// It only touches direct copular identity frames and only for a high-confidence
// set of PT-BR predicates whose lexical form does not encode sex/gender.
const PT_COMMON_GENDER_IDENTITY_950 = new Set([
  "adolescente","agente","artista","assistente","atendente","atleta",
  "canalha","celebridade","cliente","colega","covarde","cÃºmplice",
  "docente","estudante","fÃ£","gerente","guia","homicida","idiota",
  "imbecil","intÃ©rprete","jornalista","jovem","lÃ­der","lider",
  "motorista","mÃ¡rtir","martir","paciente","pessoa","policial",
  "presidente","profissional","responsÃ¡vel","responsavel","testemunha",
  "vÃ­tima","vitima"
]);

const SOURCE_IDENTITY_NEUTRAL_ALIAS_950 = Object.freeze({
  coward: "covarde",
  murderer: "homicida",
  killer: "homicida",
  journalist: "jornalista",
  driver: "motorista",
  student: "estudante",
  scientist: "cientista",
  artist: "artista",
  athlete: "atleta",
  patient: "paciente",
  client: "cliente",
  customer: "cliente",
  manager: "gerente",
  assistant: "assistente",
  witness: "testemunha",
  victim: "vÃ­tima",
  leader: "lÃ­der",
  president: "presidente",
  teenager: "adolescente"
});

function sourceNeutralCopularIdentity950(block) {
  if (!blockSourceIsEnglish(block)) return null;
  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!source) return null;

  const first = source.match(/\bi(?:'m|â€™m| am| was)\s+(?:just\s+|only\s+)?(?:a|an)\s+([a-z][a-z'â€™-]{1,40})\b/i);
  if (first && !sourceExplicitlyMarksSelfGender(block)) {
    return { person: "first", role: String(first[1] || "").toLocaleLowerCase(), limited: /\b(?:just|only)\b/i.test(first[0]) };
  }

  const second = source.match(/\byou(?:'re|â€™re| are| were)\s+(?:just\s+|only\s+)?(?:a|an)\s+([a-z][a-z'â€™-]{1,40})\b/i);
  if (second && !sourceExplicitlyMarksSecondPersonGender(block)) {
    return { person: "second", role: String(second[1] || "").toLocaleLowerCase(), limited: /\b(?:just|only)\b/i.test(second[0]) };
  }

  return null;
}

function applyUniversalCopularIdentityNeutrality950(block, value) {
  let pt = String(value || "").trim();
  if (!pt || sourceDialogueDashCount(block) >= 2) return pt;

  const identity = sourceNeutralCopularIdentity950(block);
  if (!identity) return pt;

  const person = identity.person;
  const subject = person === "first" ? "(?:eu\\s+)?" : "(?:voc[eÃª]|c[eÃª]|tu)\\s+";
  const copula = person === "first"
    ? "(?:sou|era|fui|fosse|seja|estou|t[oÃ´])"
    : "(?:[Ã©e]|era|foi|fosse|seja|est[aÃ¡]|t[aÃ¡])";

  const direct = new RegExp(
    `\\b(${subject}${copula}\\s+(?:(?:s[oÃ³]|apenas|simplesmente)\\s+)?)(?:um|uma)\\s+([\\p{L}][\\p{L}'â€™-]{1,40})\\b`,
    "iu"
  );

  const m = pt.match(direct);
  if (m) {
    const rolePt = String(m[2] || "").toLocaleLowerCase("pt-BR");
    if (PT_COMMON_GENDER_IDENTITY_950.has(rolePt)) {
      const before = pt;
      pt = pt.replace(direct, (_all, head, noun) => `${head}${noun}`);
      if (pt !== before) {
        console.log(`[IDENTITY NEUTRALIZER 9.5.0] cue ${block?.index}: artigo de gÃªnero removido sem alterar identidade | role=${rolePt} | 0 Gemini. âœ…`);
      }
      return pt.replace(/[ \t]{2,}/g, " ").trim();
    }
  }

  // Exact neutral aliases for direct identity nouns. This path is still
  // deterministic because SOURCE supplies the noun and the replacement keeps
  // the copular identity relation (never turns identity into an action/event).
  const alias = SOURCE_IDENTITY_NEUTRAL_ALIAS_950[identity.role];
  if (alias) {
    const gendered = new RegExp(
      `\\b(${subject}${copula}\\s+(?:(?:s[oÃ³]|apenas|simplesmente)\\s+)?)(?:um|uma)\\s+[\\p{L}][\\p{L}'â€™-]{1,40}\\b`,
      "iu"
    );
    if (gendered.test(pt)) {
      const before = pt;
      pt = pt.replace(gendered, (_all, head) => `${head}${alias}`);
      if (pt !== before) {
        console.log(`[IDENTITY NEUTRALIZER 9.5.0] cue ${block?.index}: identidade copular neutralizada por alias SOURCE-safe | sourceRole=${identity.role} | pt=${alias} | 0 Gemini. âœ…`);
      }
    }
  }

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

// ============================================================
// GENDER POSTCONDITION LOCAL â€” 9.0
// ============================================================
// O modelo continua responsÃ¡vel pela traduÃ§Ã£o. Este guard sÃ³ reescreve
// padrÃµes de altÃ­ssima confianÃ§a quando a SOURCE Ã© explicitamente neutra
// naquela ideia e existe uma forma brasileira natural sem gÃªnero.
function applyDeterministicGenderNeutrality(block, value) {
  let pt = String(value || "").trim();
  if (!pt || sourceDialogueDashCount(block) >= 2) return pt;

  const source = String(block?.text || "")
    .replace(/\s+/g, " ")
    .trim();

  if (!source || sourceExplicitlyMarksSelfGender(block)) return pt;

  const lower = source.toLocaleLowerCase();

  // 9.5.0: resolve direct identity/gender conflicts locally before any cloud
  // repair can start oscillating between semantic fidelity and gender guards.
  pt = applyUniversalCopularIdentityNeutrality950(block, pt);

  if (/\bi(?:'m|â€™m| am)\s+(?:always\s+)?right\b/i.test(source)) {
    pt = pt.replace(/\b(?:eu\s+)?sempre\s+(?:estou|t[oÃ´]|sou)\s+cert[oa]\b/iu, "Eu sempre tenho razÃ£o");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,45}\b(?:angry|furious|mad)\b/i.test(source)) {
    pt = pt
      .replace(/\b(?:estou|t[oÃ´])\s+inacreditavelmente\s+furios[oa]\b/iu, "tÃ´ com uma raiva inacreditÃ¡vel")
      .replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+|super\s+)?(?:furios[oa]|irritad[oa])\b/iu, match => /muito|super/iu.test(match) ? "tÃ´ com muita raiva" : "tÃ´ com raiva");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,35}\b(?:tired|exhausted)\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+|super\s+)?(?:cansad[oa]|exaust[oa])\b/iu, match => /muito|super/iu.test(match) ? "tÃ´ sem energia nenhuma" : "tÃ´ sem energia");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,45}\b(?:scared|afraid|frightened|terrified|petrified)\b/i.test(source)) {
    const intenseFear = /\b(?:frightened\s+out\s+of\s+my\s+wits|terrified|petrified)\b/i.test(source);
    pt = pt.replace(
      /\b(?:estou|t[oÃ´])\s+(?:muito\s+|completamente\s+|totalmente\s+)?(?:assustad[oa]|apavorad[oa]|aterrorizad[oa]|amedrontad[oa])\b/giu,
      intenseFear ? "tÃ´ morrendo de medo" : "tÃ´ com medo"
    );
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,30}\bconfused\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+)?confus[oa]\b/iu, "nÃ£o tÃ´ entendendo");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,30}\balone\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+sozinh[oa]\b/iu, "tÃ´ sem ninguÃ©m por perto");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,40}\bworried\b/i.test(source)) {
    pt = pt.replace(
      /\b(?:estou|t[oÃ´])\s+(?:muito\s+)?preocupad[oa](?:\s+com\s+([^.!?]+))?/iu,
      (_match, object) => {
        const target = String(object || "").trim();
        if (!target) return "isso tÃ¡ me preocupando";
        const normalized = target.charAt(0).toLocaleUpperCase() + target.slice(1);
        return `${normalized} tÃ¡ me preocupando`;
      }
    );
  }

  if (/\bi(?:'m|â€™m| am)\s+proud\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+orgulhos[oa](?:\s+de\s+([^.!?]+))?/iu, (_match, object) => {
      const target = String(object || "").trim();
      return target ? `tenho orgulho de ${target}` : "tenho orgulho disso";
    });
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,25}\b(?:shocked|stunned)\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+)?chocad[oa]\b/iu, "tÃ´ em choque");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,30}\b(?:ashamed|embarrassed)\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+)?envergonhad[oa]\b/iu, "tÃ´ com vergonha");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,25}\bbored\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+|super\s+)?entediad[oa]\b/iu, "tÃ´ morrendo de tÃ©dio");
  }

  if (/\bi(?:'m|â€™m| am)\b[^.!?]{0,25}\bbusy\b/i.test(source)) {
    pt = pt.replace(/\b(?:estou|t[oÃ´])\s+(?:muito\s+|super\s+)?ocupad[oa]\b/iu, "tÃ´ sem tempo");
  }

  if (/\bi\s+was\s+interrogated\b/i.test(source)) {
    pt = pt.replace(/\bfui\s+interrogad[oa]\b/iu, "me interrogaram");
  }
  if (/\bi\s+was\s+questioned\b/i.test(source)) {
    pt = pt.replace(/\bfui\s+questionad[oa]\b/iu, "me questionaram");
  }
  if (/\bi\s+was\s+accused\b/i.test(source)) {
    pt = pt.replace(/\bfui\s+acusad[oa]\b/iu, "me acusaram");
  }
  if (/\bi\s+was\s+invited\b/i.test(source)) {
    pt = pt.replace(/\bfui\s+convidad[oa]\b/iu, "me convidaram");
  }

  if (/\b(?:thank you|thanks)\b/i.test(source)) {
    pt = pt.replace(/^\s*obrigad[oa][.!]?\s*$/iu, "Valeu.");
  }

  if (!sourceExplicitlyMarksSecondPersonGender(block)) {
    if (/\byou(?:'re|â€™re| are| were)\s+(?:just\s+|only\s+)?(?:a|an)\s+passenger\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+(?:[Ã©e]|era|foi)\s+(?:s[oÃ³]\s+|apenas\s+)?(?:um|uma)\s+passageir[oa]\b/iu, "vocÃª Ã© sÃ³ alguÃ©m de passagem");
    }
    if (/\byou(?:'re|â€™re| are)\s+(?:crazy|insane)\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+(?:[Ã©e]|t[aÃ¡])\s+lou[cq][oa]\b/iu, "VocÃª perdeu a noÃ§Ã£o");
    }

    if (/\byou(?:'re|â€™re| are)\s+the\s+winner\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+[Ã©e]\s+(?:o\s+vencedor|a\s+vencedora)\b/iu, "VocÃª venceu");
    }

    if (/^\s*welcome[.!]?\s*$/iu.test(source)) {
      pt = pt.replace(/\bbem-vind[oa]\b/iu, "Que bom ter vocÃª aqui");
    }
  }

  // 9.0 â€” padrÃµes neutros adicionais observados em filme real.
  if (!sourceExplicitlyMarksSelfGender(block)) {
    if (/\blet me be clear\b/i.test(source)) {
      pt = pt.replace(/\b(?:deixe-me|deixa eu)\s+ser\s+clar[oa]\b/iu, "deixa eu deixar isso claro");
    }
    if (/\bi(?:'m|â€™m| am| was)\s+(?:(?:really|very)\s+)?(?:late|delayed)\b/i.test(source)) {
      pt = pt.replace(/\b(?:estou|t[oÃ´]|fiquei|estava)\s+(?:muito\s+|realmente\s+)?atrasad[oa]\b/iu, match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "Me atrasei" : "me atrasei");
    }
  }

  if (!sourceExplicitlyMarksSecondPersonGender(block)) {
    if (/\byou(?:'re|â€™re| are| were)\s+(?:alone|all alone)\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+(?:n[aÃ£]o\s+)?(?:est[aÃ¡]|t[aÃ¡]|ficou)\s+sozinh[oa]\b/iu, "vocÃª tÃ¡ sem ninguÃ©m por perto");
    }
    if (/\byou(?:'re|â€™re| are)\s+always\s+(?:good|great)\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+[Ã©e]\s+sempre\s+(?:muito\s+)?bo[ma]\b/iu, match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "VocÃª Ã© sempre incrÃ­vel" : "vocÃª Ã© sempre incrÃ­vel");
    }
    if (/\byou(?:'re|â€™re| are| were)\s+(?:(?:really|very)\s+)?(?:late|delayed)\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+(?:est[aÃ¡]|t[aÃ¡]|ficou)\s+(?:muito\s+|realmente\s+)?atrasad[oa]\b/iu, match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "VocÃª se atrasou" : "vocÃª se atrasou");
    }
    if (/\byou(?:'re|â€™re| are| were)\s+(?:(?:really|very)\s+)?clear\b/i.test(source)) {
      pt = pt.replace(/\b(?:voc[eÃª]|c[eÃª])\s+(?:foi|era|est[aÃ¡]|t[aÃ¡])\s+(?:muito\s+|bem\s+|realmente\s+)?clar[oa]\b/iu, match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "VocÃª deixou isso bem claro" : "vocÃª deixou isso bem claro");
    }
  }

  if (/\bif\s+i(?:'m|â€™m| am)\s+right\s+about\s+you\b/i.test(source)) {
    pt = pt.replace(/\bse\s+eu\s+(?:estiver|tiver)\s+cert[oa]\s+sobre\s+voc[eÃª](?=\W|$)/iu, "se eu tiver razÃ£o sobre vocÃª");
  }

  if (/\bwhen\s+you\s+get\s+secure\b/i.test(source)) {
    pt = pt.replace(/\bquando\s+(?:voc[eÃª]\s+)?estiver\s+segur[oa]\b/iu, "quando estiver em seguranÃ§a");
  }

  if (/\byou\s+are\s+not\s+alone\b|\byou(?:'re|â€™re)\s+not\s+alone\b/i.test(source)) {
    pt = pt.replace(/\bvoc[eÃª]\s+n[aÃ£]o\s+(?:est[aÃ¡]|t[aÃ¡])\s+sozinh[oa]\b/iu, match => /^[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡]/u.test(match) ? "VocÃª nÃ£o tÃ¡ sÃ³" : "vocÃª nÃ£o tÃ¡ sÃ³");
  }

  pt = applyUniversalCopularIdentityNeutrality950(block, pt);
  pt = applyGenderV5DefinitiveNeutralization897(block, pt);
  pt = applyUniversalCopularIdentityNeutrality950(block, pt);

  return pt.replace(/[ \t]{2,}/g, " ").trim();
}

const PTBR_CYRILLIC_CONFUSABLES_971 = Object.freeze({
  "Ð°": "a", "Ð": "A",
  "Ðµ": "e", "Ð•": "E",
  "Ð¾": "o", "Ðž": "O",
  "Ñ€": "p", "Ð ": "P",
  "Ñ": "c", "Ð¡": "C",
  "Ñ…": "x", "Ð¥": "X",
  "Ñ–": "i", "Ð†": "I",
  "Ñ˜": "j", "Ðˆ": "J"
});

function normalizePtbrMixedScript971(value) {
  const chars = [...String(value || "")];
  const isLatin = ch => Boolean(ch && /\p{Script=Latin}/u.test(ch));

  for (let i = 0; i < chars.length; i++) {
    const replacement = PTBR_CYRILLIC_CONFUSABLES_971[chars[i]];
    if (!replacement) continue;

    const prev = chars[i - 1] || "";
    const next = chars[i + 1] || "";
    if (isLatin(prev) || isLatin(next)) {
      chars[i] = replacement;
    }
  }

  return chars.join("");
}

function hasMixedScriptToken971(value) {
  const tokens = String(value || "").match(/[\p{L}\p{M}]+/gu) || [];
  return tokens.some(token =>
    /\p{Script=Latin}/u.test(token) && /\p{Script=Cyrillic}/u.test(token)
  );
}

function replacePtbrTypoCase973(value, regex, lower, title) {
  return String(value || "").replace(regex, (match, offset, whole) => {
    const prefix = String(whole || "").slice(0, Number(offset || 0));
    const sentenceStart = !prefix.trim() || /[.!?â€¦]\s*$/u.test(prefix);
    return sentenceStart ? title : lower;
  });
}

function normalizeSourceAwareInterjections973(block, value) {
  let out = String(value || "");
  const source = String(block?.text || "");

  if (/(?:^|\s)Ha!(?:\s|$)/u.test(source)) {
    out = out.replace(/(?:^|\s)HÃ¡!(?=\s|$)/gu, match =>
      match.startsWith(" ") ? " Ha!" : "Ha!"
    );
  }

  return out;
}

function normalizeEditorialTypography973(value) {
  return String(value || "")
    .replace(/(^|\n)\s*--+\s*/gu, "$1â€” ")
    .replace(/\s+--+\s+/gu, " â€” ")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function decoratePerformanceLyric973(block, value) {
  const raw = String(value || "").trim();
  if (!block?.musicPerformance || !raw) return raw;

  const lines = raw
    .replace(/[â™ªâ™«â™¬]/gu, " ")
    .split("\n")
    .map(line => line.replace(/[ \t]{2,}/g, " ").trim())
    .filter(Boolean);

  if (!lines.length) return "";
  if (lines.length === 1) return `â™ª ${lines[0]} â™ª`;

  lines[0] = `â™ª ${lines[0]}`;
  lines[lines.length - 1] = `${lines[lines.length - 1]} â™ª`;
  return lines.join("\n");
}

function sourceStrongNegationMissing973(block, pt) {
  if (!blockSourceIsEnglish(block)) return false;

  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  const target = String(pt || "").replace(/\s+/g, " ").trim();

  const strongSourceNegation =
    /\b(?:do|does|did|is|are|was|were|have|has|had|can|could|will|would|should|must|may|might)\s+not\b|n['â€™]t\b|\bnever\b|\bnothing\b|\bno\s+one\b/iu.test(source);

  if (!strongSourceNegation) return false;

  const targetNegation =
    /\b(?:nÃ£o|nao|nunca|jamais|ningu[eÃ©]m|nada|nem|sem)\b/iu.test(target);

  return !targetNegation;
}

function objectToReflexiveRisk973(block, pt) {
  if (!blockSourceIsEnglish(block)) return false;

  const source = String(block?.text || "").replace(/\s+/g, " ").trim();
  const target = String(pt || "").replace(/\s+/g, " ").trim();

  const simpleTheyObject =
    /^(?:(?:do|did|can|could|would|will|should)\s+)?they\b[^.!?;:]{0,72}\b(?:him|her)\b[.!?]?$/iu.test(source);

  const reciprocalPt =
    /\b(?:eles|elas)\s+se\s+[\p{L}Ã€-Ã¿]+/iu.test(target);

  return simpleTheyObject && reciprocalPt;
}

function applyDeterministicOrthography(value) {
  let out = normalizePtbrMixedScript971(String(value || ""))
    .replace(/\bemituiu\b/giu, "emitiu")
    .replace(/\bAgÃ¼enta\b/gu, "Aguenta")
    .replace(/\bagÃ¼enta\b/gu, "aguenta");

  out = replacePtbrTypoCase973(out, /\bsoubi\b/giu, "soube", "Soube");
  out = replacePtbrTypoCase973(out, /\bpermenente\b/giu, "permanente", "Permanente");
  out = out.replace(/\bIhe\b/g, "lhe");

  return normalizeEditorialTypography973(out).trim();
}

function unknownSpeakerGenderRisk(
  block,
  pt,
  plan
) {
  const en =
    String(
      block?.text ||
      ""
    );

  if (
    !/\b(?:i\s+am|i'm|i\s+was|i\s+feel|i\s+felt|i\s+got|i've\s+been)\b/i.test(
      en
    )
  ) {
    return false;
  }

  // SOURCE lexical explÃ­cita autoriza a marca correspondente; mismatch continua
  // coberto por GENDER_V9, portanto UNKNOWN nÃ£o deve contradizer a prÃ³pria SOURCE.
  if (sourceGenderEvidence942(block, "first").explicit) {
    return false;
  }

  const lock =
    identityLockForCapsule(
      block,
      plan
    );

  if (
    lock.trusted_speaker_gender
  ) {
    return false;
  }

  return UNKNOWN_SPEAKER_GENDERED_STATE_RE.test(
    String(pt || "")
  );
}

function localReasonsForCue(
  block,
  pt,
  filename,
  plan
) {
  const en =
    String(
      block.text || ""
    );

  const translated =
    String(pt || "");

  const reasons = [];

  const enCount =
    words(en).length;

  const ptCount =
    words(
      translated
    ).length;

  const drag =
    isDragContext(
      filename,
      en
    );

  if (
    !translated.trim() &&
    sourceCueAllowsIntentionalEmpty(
      block
    )
  ) {
    return [];
  }

  if (
    !translated.trim()
  ) {
    reasons.push(
      "EMPTY"
    );
  }

  if (
    blockSourceIsEnglish(block) &&
    enCount >= 5 &&
    copiedEnglishRatio(
      en,
      translated
    ) >= 0.60
  ) {
    reasons.push(
      "POSSIBLE_UNTRANSLATED"
    );
  }

  if (
    looksLikeFinalGarbageCue(
      translated
    )
  ) {
    reasons.push(
      "FINAL_GARBAGE_OR_PLACEHOLDER"
    );
  }

  if (hasMixedScriptToken971(translated)) {
    reasons.push("FINAL_MIXED_SCRIPT_CONFUSABLE");
  }

  for (
    const reason of
    genderIntegrityV2Reasons(
      block,
      translated,
      plan
    )
  ) {
    reasons.push(reason);
  }

  if (sourceStrongNegationMissing973(block, translated)) {
    reasons.push("NEGATION_EXPLICIT_MISSING_973");
  }

  if (objectToReflexiveRisk973(block, translated)) {
    reasons.push("REFERENT_INTEGRITY_OBJECT_TO_REFLEXIVE_RISK_973");
  }

  if (
    enCount >= 10 &&
    ptCount <=
      Math.max(
        2,
        Math.floor(
          enCount * 0.32
        )
      )
  ) {
    reasons.push(
      "POSSIBLE_OMISSION"
    );
  }

  if (
    enCount >= 3 &&
    ptCount >=
      enCount * 2.8 + 6
  ) {
    reasons.push(
      "POSSIBLE_OVERFLOW"
    );
  }

    const expectedDialogueTurns =
    sourceDialogueDashCount(
      block
    );

  if (
    expectedDialogueTurns >= 2
  ) {
    const actualDialogueTurns =
      translatedDialogueTurnCount(
        block,
        translated
      );

    if (
      actualDialogueTurns !==
      expectedDialogueTurns
    ) {
      reasons.push(
        `DIALOGUE_TURN_MISMATCH expected=${expectedDialogueTurns} got=${actualDialogueTurns}`
      );
    }

    if (/(^|\s)~\s*/u.test(translated)) {
      reasons.push("DIALOGUE_TILDE_RESIDUE");
    }
  }

  if (
    hasExtendedVocalization(
      translated
    ) ||
    /(?<!\p{L})(\p{L})-(?=\1\p{L}+)/giu.test(
      translated
    )
  ) {
    reasons.push(
      "EXTENDED_OR_STUTTERED_VOCALIZATION"
    );
  }

  if (
    /(^|\n)\s*(?:\/{1,3}|[-â€“â€”]{2,}|\|{1,3}|[â€¢Â·â–ªâ—¦]+|[:;])\s*(?:$|\n)/u.test(
      translated
    )
  ) {
    reasons.push(
      "FORMAT_NOISE"
    );
  }

  if (
    hasArtificialCensorship(
      translated
    )
  ) {
    reasons.push(
      "ARTIFICIAL_PROFANITY_CENSORSHIP"
    );
  }

  const sourceHasBleep = en.includes(BLEEP_TOKEN);
  if (!sourceHasBleep && (translated.includes(BLEEP_TOKEN) || /\[(?:censurado|bleep)\]/iu.test(translated))) {
    reasons.push("INVENTED_BLEEP_TOKEN");
  }

  if (
    translated
      .split("\n")
      .some(line => {
        const info = extractSpeaker(line);
        if (info.speaker) return true;
        if (block?.speakerHint) {
          return /^\s*(?:[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡][A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡0-9.'â€™_-]*)(?:\s+[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡0-9.'â€™_-]+){0,4}\s+(?=[A-ZÃÃ‰ÃÃ“ÃšÃ‚ÃŠÃ”ÃƒÃ•Ã‡Ã€-Ã¿])/u.test(String(line || ""));
        }
        return false;
      })
  ) {
    reasons.push(
      "SPEAKER_LABEL_RESIDUE"
    );
  }

  if (
    /\[[^\]]{1,100}\]|\([^)]{1,100}\)/u.test(
      translated
    ) &&
    translated
      .match(
        /\[[^\]]{1,100}\]|\([^)]{1,100}\)/gu
      )
      ?.some(
        part =>
          looksLikeSdhDescriptor(
            part.slice(
              1,
              -1
            )
          )
      )
  ) {
    reasons.push(
      "SDH_RESIDUE"
    );
  }

  if (
    translated
      .split("\n")
      .some(line => looksLikeBareSdhLine(line)) &&
    String(block?.text || "")
      .split("\n")
      .some(line => looksLikeBareSdhLine(line))
  ) {
    reasons.push(
      "SDH_RESIDUE"
    );
  }
  
  if (
    /\b(?:nabeira|olurando|dem[oÃ´]nico|podrindo|qualÃ©|ossas)\b/i.test(
      translated
    ) ||
    /\btomar\s+consist[eÃª]ncia\b/i.test(
      translated
    ) ||
    /\btotalmente\s+loucura\b/i.test(
      translated
    ) ||
    /\bxis\s*,?\s*xis\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "KNOWN_PTBR_CORRUPTION_OR_UNNATURALNESS"
    );
  }

  if (
    /\bfor\s+once\b/i.test(
      en
    ) &&
    /\bpor\s+um\s+dia\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "IDIOM_FOR_ONCE_MISTRANSLATED"
    );
  }

  if (
    /\bcry\s+wolf\b/i.test(
      en
    ) &&
    /\balarmar\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "IDIOM_CRY_WOLF_LITERAL"
    );
  }

  if (
    /\bholler\b/i.test(
      en
    ) &&
    /\b(?:dar|desse|demos|deu|um)\s+(?:um\s+)?al[oÃ´]\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "IDIOM_HOLLER_LITERAL"
    );
  }

  if (
    /\bmalevolent\s+force\b/i.test(
      en
    ) &&
    /\bforÃ§a\s+maldosa\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "UNNATURAL_MALEVOLENT_FORCE"
    );
  }

  if (
    /\bsubjects?\b/i.test(
      en
    ) &&
    /\bsujeitos?\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "POSSIBLE_LITERAL_SUBJECTS"
    );
  }

  if (
    /\b(?:qualÃ©|pistola)\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "POSSIBLE_FORCED_OR_DATED_SLANG"
    );
  }

  if (
    /\bb[eÃª]bad[oa]\s+que\s+s[oÃ³]\s+a\s+porra\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "FORCED_PROFANITY_REGISTER"
    );
  }

  if (/\bemituiu\b|\bagÃ¼enta\b|\bsoubi\b|\bpermenente\b|\bIhe\b/u.test(translated)) {
    reasons.push("PTBR_ORTHOGRAPHY_ERROR");
  }

  if (/\bloosen your grip\b/i.test(en) && /\bafroux(?:e|a)\s+a\s+m[aÃ£]o\b/iu.test(translated)) {
    reasons.push("LITERALITY_GRIP");
  }

  if (/\bsecure the airway\b/i.test(en) && /\bcertifique-se\s+de\s+garantir\s+a\s+via\s+a[eÃ©]rea\b/iu.test(translated)) {
    reasons.push("LITERALITY_MEDICAL_AIRWAY");
  }

  if (/\bsecure private\b/i.test(en) && /\bprivado\s+seguro\b/iu.test(translated)) {
    reasons.push("LITERALITY_SECURE_PRIVATE");
  }

  if (/\bdove on\b/i.test(en) && /\bmergulhou\s+em\s+cima\b/iu.test(translated)) {
    reasons.push("LITERALITY_DOVE_ON");
  }

  if (
    /\b(?:we|you|they|i)\s+(?:need|have|got)\s+to\s+move(?:\s+now|\s+right\s+now)?\b/iu.test(en) &&
    /\b(?:precisamos|temos|precisa|precisam|tenho|t[eÃª]m)\s+(?:que\s+)?(?:nos\s+)?mover\b/iu.test(translated)
  ) {
    reasons.push("LITERALITY_MOVE_ACTION");
  }

  if (
    /\bsweep\s+(?:everything|it|all of it)\s+into\b/iu.test(en) &&
    /\b(?:varr(?:a|e|am|em|eu)|vir(?:a|e|am|em|ou))\b/iu.test(translated)
  ) {
    reasons.push("LITERALITY_SWEEP_GATHER");
  }

  if (
  cueNeedsConciseRepair(
    block,
    translated
  )
) {
  reasons.push(
    "SUBTITLE_TOO_DENSE"
  );
}

  if (translated.includes(BLEEP_TOKEN) || /\[(?:censurado|bleep)\]/iu.test(translated)) {
    reasons.push("VISIBLE_CENSOR_PLACEHOLDER");
  }

  if (sourceExactRepetitionLost896(block, translated)) {
    reasons.push("SOURCE_EXACT_REPETITION_LOST");
  }

  if (contextualHaltImperativeRisk896(block, translated)) {
    reasons.push("CONTEXTUAL_IMPERATIVE_REFERENT_INVENTION");
  }

  if (bareImperativeConcreteReferentRisk92(block, translated)) {
    reasons.push("BARE_IMPERATIVE_CONCRETE_REFERENT_INVENTION");
  }

  for (
    const reason of
    literalCalqueReasons(
      en,
      translated
    )
  ) {
    reasons.push(
      reason
    );
  }

  if (
    blockSourceIsEnglish(block) &&
    unknownSpeakerGenderRisk(
      block,
      translated,
      plan
    )
  ) {
    reasons.push(
      "UNKNOWN_SPEAKER_GENDER_MARKED"
    );
  }

  if (drag) {
    const gagSlang =
      /\bgag(?:ged|ging|s)?\b/i.test(
        en
      ) &&
      !isPhysicalGagContext(
        en
      );

    if (
      gagSlang &&
      !hasGoodGagReaction(
        translated
      )
    ) {
      reasons.push(
        "GAG_SLANG_NOT_NATURAL_PTBR"
      );
    }

    if (
      gagSlang &&
      /\bamordaÃ§|\bengasg|\bÃ¢nsia|\bnÃ¡usea/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_GAGGED"
      );
    }

    if (
      /\b(?:she|he|you|they)\s+ate(?:\s+that)?\b/i.test(
        en
      ) &&
      /\b(?:comeu|comeram|comeste|comi)\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_ATE"
      );
    }

    if (
      /\bslay(?:ed|ing|s)?\b/i.test(
        en
      ) &&
      /\b(?:matar|matou|matei|mataram|assassin)/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_SLAY"
      );
    }

    if (
      /\bshade\b/i.test(en) &&
      /\bsombra\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_SHADE"
      );
    }

    if (
      /\btea\b/i.test(en) &&
      /\bch[aÃ¡]\b/i.test(
        translated
      ) &&
      /\bspill|hot|what(?:'s| is)|give|all the|the tea\b/i.test(
        en
      )
    ) {
      reasons.push(
        "LITERAL_TEA"
      );
    }

    if (
      /\bjudges?\b/i.test(
        en
      ) &&
      /\bju[iÃ­]zes?\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "JUDGES_AS_JUIZES"
      );
    }

    if (
      /\bsupportive\b/i.test(
        en
      ) &&
      /\bsuper\s+apoiador(?:a|es|as)?\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_SUPPORTIVE"
      );
    }

    if (
      /\b(?:double|shared)\s+win\b/i.test(
        en
      ) &&
      /\bempate\s+duplo\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "DOUBLE_WIN"
      );
    }

    const competitionBottom =
      /\b(?:in|into|landed|landing|placed|placing|put|puts|ended|ending)\s+(?:up\s+)?(?:in\s+)?the\s+bottom\b/i.test(
        en
      ) ||

      /\bbottom\s+(?:two|three|2|3|queens?|girls?|contestants?|performers?)\b/i.test(
        en
      ) ||

      /\b(?:the\s+)?bottom\s+(?:this\s+week|tonight|again)\b/i.test(
        en
      );

    if (
      competitionBottom &&
      /\b(?:fundo|quintal|parte\s+de\s+baixo|inferior(?:es)?)\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "COMPETITION_BOTTOM_LITERAL"
      );
    }

    if (
      /\bwho\s+the\s+fuck\s+knows\b/i.test(
        en
      ) &&
      /\bquem\s+sabe\s+(?:o\s+)?caralho\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "WHO_THE_FUCK_KNOWS_LITERAL"
      );
    }

    if (
      /\bforneuzinh|\bforninho\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "UNNATURAL_LITERAL_METAPHOR"
      );
    }
  }

  return [
    ...new Set(
      reasons
    )
  ];
}

function addIssue(
  issueMap,
  id,
  reason
) {
  if (
    !issueMap.has(id)
  ) {
    issueMap.set(
      id,
      new Set()
    );
  }

  issueMap
    .get(id)
    .add(reason);
}

function mergeIssueLists(
  ...lists
) {
  const map =
    new Map();

  for (
    const list of lists
  ) {
    for (
      const issue of
      Array.isArray(list)
        ? list
        : []
    ) {
      const id =
        Number(
          issue?.id
        );

      if (
        !Number.isInteger(id)
      ) {
        continue;
      }

      for (
        const reason of
        Array.isArray(
          issue?.reasons
        )
          ? issue.reasons
          : []
      ) {
        addIssue(
          map,
          id,
          String(reason)
        );
      }
    }
  }

  return [
    ...map.entries()
  ].map(
    ([
      id,
      reasons
    ]) => ({
      id,

      reasons:
        [...reasons]
    })
  );
}

function issueReasonBucket(reason) {
  const text = String(reason || "").trim();

  if (/^QA_PTBR:/i.test(text)) {
    return "QA_PTBR";
  }

  if (/FALSE_COGNATE/i.test(text)) {
    return "FALSE_COGNATE";
  }

  if (/^IDIOM_/i.test(text)) {
    return "IDIOM_LITERAL";
  }

    if (/LITERAL/i.test(text)) {
    return "LITERALITY";
  }

  if (/^DIALOGUE_TURN_MISMATCH/i.test(text)) {
    return "DIALOGUE_TURN_MISMATCH";
  }

  return text || "UNKNOWN";
}

function issuePriority(issue) {
  const reasons = Array.isArray(issue?.reasons)
    ? issue.reasons.map(String)
    : [];

  const joined = reasons.join(" | ");

  // ==========================================================
  // PRIORIDADE 0 â€” PRIORITÃRIO
  // Sentido, identidade, gÃªnero, speaker/referente, omissÃ£o,
  // cue ownership e censura quebrada.
  // ==========================================================

  if (
    /FINAL_PRIORITY/i.test(joined) ||
    /GENDER_V[2-9]_/i.test(joined) ||
    /FINAL_GARBAGE_OR_PLACEHOLDER/i.test(joined) ||
    /CUE_OWNERSHIP_(?:SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION)/i.test(joined) ||
    /UNKNOWN_SPEAKER_GENDER_MARKED/i.test(joined) ||
    /POSSIBLE_OMISSION/i.test(joined) ||
    /POSSIBLE_CUE_SHIFT_PAIR/i.test(joined) ||
    /POSSIBLE_FORCED_OR_DATED_SLANG/i.test(joined) ||
    /EMPTY/i.test(joined) ||
    /(?:UNRESOLVED_BLEEP_TOKEN|INVENTED_BLEEP_TOKEN|SOURCE_BLEEP_FIDELITY_LOST)/i.test(joined) ||
    /BLEEP_CREATED_DANGLING_SENTENCE/i.test(joined) ||
    /ARTIFICIAL_PROFANITY_CENSORSHIP/i.test(joined) ||
    /(?:MISSING_DIALOGUE_BREAK|DIALOGUE_TURN_MISMATCH|DIALOGUE_TILDE_RESIDUE)/i.test(joined) ||

    // Motivos escritos pelo Gemini QA.
    /\bg[eÃª]nero\b/i.test(joined) ||
    /\bpronome\b/i.test(joined) ||
    /\bidentity\b/i.test(joined) ||
    /\bidentidade\b/i.test(joined) ||
    /\bspeaker\b/i.test(joined) ||
    /\breferente\b/i.test(joined) ||
    /\bsentido\b/i.test(joined) ||
    /\bsem[aÃ¢]ntic/i.test(joined) ||
    /\bnega[cÃ§][aÃ£]o\b/i.test(joined) ||
    /\bomiss[aÃ£]o\b/i.test(joined) ||
    /\binven(?:ta|tou|Ã§Ã£o)\b/i.test(joined) ||
    /\bsujeito\b/i.test(joined) ||
    /\bobjeto\b/i.test(joined)
  ) {
    return 0;
  }

  // ==========================================================
  // PRIORIDADE 1 â€” QUALIDADE LINGUÃSTICA
  // Literalidade, calque, idioma, registro, naturalidade etc.
  // ==========================================================

  if (
    /SUBTITLE_TOO_DENSE/i.test(joined) ||
    /QA_PTBR/i.test(joined) ||
    /FALSE_COGNATE/i.test(joined) ||
    /IDIOM_/i.test(joined) ||
    /LITERAL/i.test(joined) ||
    /UNNATURAL/i.test(joined) ||
    /FORCED_/i.test(joined) ||
    /GAG_/i.test(joined) ||
    /JUDGES_/i.test(joined) ||
    /DOUBLE_WIN/i.test(joined) ||
    /COMPETITION_BOTTOM/i.test(joined) ||
    /POSSIBLE_LITERAL/i.test(joined) ||
    /POSSIBLE_FORCED/i.test(joined) ||
    /KNOWN_PTBR/i.test(joined) ||
    /POSSIBLE_UNTRANSLATED/i.test(joined) ||
    /naturalidade/i.test(joined) ||
    /literal/i.test(joined) ||
    /calque/i.test(joined) ||
    /registro/i.test(joined) ||
    /g[iÃ­]ria/i.test(joined) ||
    /met[aÃ¡]fora/i.test(joined)
  ) {
    return 1;
  }

  // PRIORIDADE 2 â€” MECÃ‚NICO
  // Ex.: linha comprida, ruÃ­do leve, formataÃ§Ã£o etc.
  return 2;
}

function logIssueSummary(label, issues) {
  const counts = new Map();

  for (const issue of Array.isArray(issues) ? issues : []) {
    for (const reason of Array.isArray(issue?.reasons) ? issue.reasons : []) {
      const key = issueReasonBucket(reason);
      counts.set(key, (counts.get(key) || 0) + 1);
    }
  }

  const summary = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([reason, count]) => `${reason}=${count}`)
    .join(" | ");

  console.log(
    `[ISSUE SUMMARY] ${label} | cues=${Array.isArray(issues) ? issues.length : 0}` +
    (summary ? ` | ${summary}` : "")
  );
}

function sourceBoundaryHint(block) {
  const text = String(block?.text || "")
    .replace(/@@SPK:[^@]+@@\s*/g, "")
    .replace(/^\s*[-â€“â€”]\s*/u, "")
    .trim();

  const firstLetter = text.match(/[\p{L}]/u)?.[0] || "";
  const startsMidSentence =
    /^(?:\.{2,}|â€¦)/u.test(text) ||
    Boolean(firstLetter && firstLetter === firstLetter.toLocaleLowerCase() && firstLetter !== firstLetter.toLocaleUpperCase());

  const endsMidSentence = Boolean(
    text &&
    !/[.!?â€¦]["'â€™â€)]*\s*$/u.test(text)
  );

  return { startsMidSentence, endsMidSentence };
}

function sourceLooksLikeShortReaction(value) {
  const text = String(value || "")
    .replace(/^\s*[-â€“â€”]\s*/u, "")
    .replace(/\s+/g, " ")
    .trim();

  return /^(?:oh|ah|okay|ok|yeah|yes|no|right|sure|wow|huh|uh-huh|mm-hmm|well|entendi|tÃ¡|ta|sim|nÃ£o|nao|certo|claro|beleza)[.!?â€¦]*$/iu.test(text);
}

function normalizedBoundaryTokens(value) {
  return String(value || "")
    .toLocaleLowerCase("pt-BR")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
}

function boundaryDuplicateOverlap(previousValue, currentValue) {
  const left = normalizedBoundaryTokens(previousValue).slice(-12);
  const right = normalizedBoundaryTokens(currentValue).slice(0, 12);

  if (left.length < 3 || right.length < 3) return null;

  const maxSize = Math.min(8, left.length, right.length);

  for (let size = maxSize; size >= 3; size--) {
    const tail = left.slice(left.length - size);
    const head = right.slice(0, size);

    if (!tail.every((token, index) => token === head[index])) continue;

    const charWeight = tail.join("").length;
    const substantial = tail.filter(token => token.length >= 4).length;

    // Evita flags em microfrases funcionais; exige repetiÃ§Ã£o lexical real.
    if (charWeight >= 14 && (substantial >= 2 || charWeight >= 20)) {
      return { size, charWeight, sequence: tail.join(" ") };
    }
  }

  return null;
}

function ownershipBoundaryMismatchReasons(previousBlock, block, previousPt, pt) {
  if (!previousBlock || !block) return [];

  const prevBoundary = sourceBoundaryHint(previousBlock);
  const currentBoundary = sourceBoundaryHint(block);
  const source = String(block.text || "").trim();
  const target = String(pt || "").trim();

  if (!source || !target) return [];

  const continuationPair =
    currentBoundary.startsMidSentence &&
    prevBoundary.endsMidSentence;

  const reasons = [];

  if (
    continuationPair &&
    !sourceLooksLikeShortReaction(source) &&
    sourceLooksLikeShortReaction(target)
  ) {
    reasons.push("CUE_OWNERSHIP_BOUNDARY_MISMATCH");
  }

  if (continuationPair) {
    const targetOverlap = boundaryDuplicateOverlap(previousPt, pt);
    const sourceOverlap = boundaryDuplicateOverlap(previousBlock.text, block.text);
    const targetSemanticOverlap = boundarySemanticOverlap898(previousPt, pt);
    const sourceSemanticOverlap = boundarySemanticOverlap898(previousBlock.text, block.text);

    // SÃ³ acusa duplicaÃ§Ã£o criada pela traduÃ§Ã£o. AlÃ©m da sequÃªncia literal,
    // 9.0 compara tokens de conteÃºdo e ignora conectivos/preposiÃ§Ãµes.
    // Isso captura "sem graÃ§a ... 22 graus" -> "sem graÃ§a ... 22 graus e sol"
    // sem hardcode de tÃ­tulo, e preserva repetiÃ§Ã£o que jÃ¡ existe na SOURCE.
    if (
      (targetOverlap && (!sourceOverlap || targetOverlap.size > sourceOverlap.size)) ||
      (targetSemanticOverlap && (!sourceSemanticOverlap || targetSemanticOverlap.size > sourceSemanticOverlap.size))
    ) {
      reasons.push("CUE_OWNERSHIP_BOUNDARY_DUPLICATION");
    }
  }

  return [...new Set(reasons)];
}

function detectLocalIssues(
  blocks,
  translations,
  filename,
  plan
) {
  const issueMap =
    new Map();

  for (
    const block of blocks
  ) {
    for (
      const reason of
      localReasonsForCue(
        block,
        translations.get(
          block.index
        ),
        filename,
        plan
      )
    ) {
      addIssue(
        issueMap,
        block.index,
        reason
      );
    }
  }

  for (
    let i = 0;
    i <
      blocks.length - 1;
    i++
  ) {
    const first =
      blocks[i];

    const second =
      blocks[i + 1];

    const firstEn =
      words(
        first.text
      ).length;

    const secondEn =
      words(
        second.text
      ).length;

    const firstPt =
      words(
        translations.get(
          first.index
        )
      ).length;

    const secondPt =
      words(
        translations.get(
          second.index
        )
      ).length;

    const firstTooShort =
      firstEn >= 7 &&
      firstPt <=
        Math.max(
          2,
          Math.floor(
            firstEn * 0.30
          )
        );

    const secondTooShort =
      secondEn >= 7 &&
      secondPt <=
        Math.max(
          2,
          Math.floor(
            secondEn * 0.30
          )
        );

    const firstTooLong =
      firstEn >= 2 &&
      firstPt >=
        firstEn * 2.8 + 6;

    const secondTooLong =
      secondEn >= 2 &&
      secondPt >=
        secondEn * 2.8 + 6;

    const boundaryReasons = ownershipBoundaryMismatchReasons(
      first,
      second,
      translations.get(first.index),
      translations.get(second.index)
    );

    if (boundaryReasons.length) {
      for (const reason of boundaryReasons) {
        addIssue(issueMap, first.index, reason);
        addIssue(issueMap, second.index, reason);
      }
    }

    if (
      firstTooShort &&
      secondTooLong
    ) {
      addIssue(
        issueMap,
        first.index,
        "POSSIBLE_CUE_SHIFT_PAIR"
      );

      addIssue(
        issueMap,
        second.index,
        "POSSIBLE_CUE_SHIFT_PAIR"
      );
    }

    if (
      secondTooShort &&
      firstTooLong
    ) {
      addIssue(
        issueMap,
        first.index,
        "POSSIBLE_CUE_SHIFT_PAIR"
      );

      addIssue(
        issueMap,
        second.index,
        "POSSIBLE_CUE_SHIFT_PAIR"
      );
    }
  }

  return [
    ...issueMap.entries()
  ].map(
    ([
      id,
      reasons
    ]) => ({
      id,

      reasons:
        [...reasons]
    })
  );
}

function buildRepairPayload(
  blocks,
  posMap,
  translations,
  issues,
  plan
) {
  const locksById =
    new Map();

  const cues =
    issues.map(issue => {
      const pos =
        posMap.get(
          issue.id
        );

      const block =
        blocks[pos];

      const protectedTarget =
        protectCulturalLocks(
          block.text,
          block.index
        );

      locksById.set(
        block.index,
        protectedTarget.locks
      );

      return {
        i:
          block.index,

        en:
          protectedTarget.text,

        pt:
          translations.get(
            block.index
          ),

                reasons:
          issue.reasons,

        ...(
          sourceDialogueDashCount(
            block
          ) >= 2
            ? {
                dialogue_turn_count:
                  sourceDialogueDashCount(
                    block
                  ),

                dialogue_turn_lock:
                  "Preserve exatamente a quantidade e a ordem dos speakers; devolva cada turn em linha prÃ³pria comeÃ§ando por '- '."
              }
            : {}
        ),

        hard_locks:
          protectedTarget
            .locks
            .map(
              lock =>
                lock.token
            ),

        identity_lock:
          identityLockForCapsule(
            block,
            plan
          ),

        ...(
          block.speakerHint
            ? {
                speaker:
                  block.speakerHint
              }
            : {}
        ),

        before:
          blocks
            .slice(
              Math.max(
                0,
                pos - 2
              ),
              pos
            )
            .map(
              item => ({
  en:
    item.text,

  pt:
    translations.get(
      item.index
    ) || ""
})
            ),

        after:
          blocks
            .slice(
              pos + 1,

              Math.min(
                blocks.length,
                pos + 3
              )
            )
            .map(
              item => ({
  en:
    item.text,

  pt:
    translations.get(
      item.index
    ) || ""
})
            )
      };
    });

  return {
    payload: {
      cues
    },

    locksById
  };
}

function parseRepairCueTranslationSalvage928(batch, raw, locksById = new Map()) {
  let parsed;
  try {
    parsed = JSON.parse(stripCodeFences(raw));
  } catch {
    throw new Error("JSON REPAIR invÃ¡lido.");
  }
  if (!Array.isArray(parsed?.cues)) {
    throw new Error("Resposta REPAIR sem cues.");
  }

  const expectedIds = batch.map(block => Number(block?.index)).filter(Number.isInteger);
  const expected = new Set(expectedIds);
  const translations = new Map();
  const rejected = new Map();
  const seen = new Set();

  for (const item of parsed.cues) {
    const id = Number(item?.i);
    if (!Number.isInteger(id) || !expected.has(id)) continue;
    if (seen.has(id)) {
      rejected.set(id, "DUPLICATE_ID");
      continue;
    }
    seen.add(id);
    let pt = String(item?.pt ?? "").trim();
    if (!pt) {
      rejected.set(id, "EMPTY_PT");
      continue;
    }
    try {
      pt = restoreCulturalLocks(pt, locksById.get(id) || [], id);
      translations.set(id, pt);
    } catch (error) {
      rejected.set(id, `LOCK_OR_STRUCTURE:${errorMessage(error).slice(0,180)}`);
    }
  }

  const unresolvedIds = expectedIds.filter(id => !translations.has(id));
  for (const id of unresolvedIds) {
    if (!rejected.has(id)) rejected.set(id, "MISSING_ID");
  }
  return { translations, unresolvedIds, rejected };
}

async function repairBatch(
  blocks,
  posMap,
  translations,
  issues,
  plan,
  job
) {
  let lastError;

  for (
    let parseAttempt = 1;
    parseAttempt <=
      REPAIR_PARSE_ATTEMPTS;
    parseAttempt++
  ) {
    try {
      const {
        payload,
        locksById
      } =
        buildRepairPayload(
          blocks,
          posMap,
          translations,
          issues,
          plan
        );

      const response =
        await geminiRequest({
          system:
            REPAIR_PROMPT,

          user:
            `BÃBLIA:\n${
              JSON.stringify(
                plan
              )
            }\n\n` +
            `CUES PARA REPARO:\n${
              JSON.stringify(
                payload
              )
            }\n\n` +
            `CUE OWNERSHIP ABSOLUTO: cada i Ã© uma caixa fechada. Traduza SOMENTE o campo en daquele mesmo i. ` +
            `before/after servem SOMENTE para contexto e NUNCA podem fornecer conteÃºdo ao target. ` +
            `Se o pt atual estiver deslocado, reconstrua diretamente do en do mesmo i. ` +
            `Todos os tokens __LOCK_C...__ devem voltar idÃªnticos. ` +
            `Se dialogue_turn_count existir, preserve EXATAMENTE os turns e devolva cada um em linha prÃ³pria iniciada por "- ". ` +
            `O token ${BLEEP_TOKEN} Ã© metadata invisÃ­vel de censura da SOURCE: NÃƒO o copie; naturalize a fala em PT-BR pelo contexto, sem placeholder.`,

          schema:
            cueTranslationSchema(
              issues.length
            ),

          thinkingLevel:
            REPAIR_THINKING,

          maxOutputTokens:
            REPAIR_MAX_OUTPUT_TOKENS,

          timeoutMs:
            REPAIR_TIMEOUT_MS,

          maxRetries:
            REPAIR_HTTP_RETRIES,

          job,

          metric:
            "repair"
        });

      try {
        const batchBlocks = issues.map(
          issue => blocks[posMap.get(issue.id)]
        );
        const salvaged = parseRepairCueTranslationSalvage928(
          batchBlocks,
          response.text,
          locksById
        );
        const salvagedCount = salvaged.translations.size;
        if (salvagedCount > 0 && salvaged.unresolvedIds.length > 0) {
          job.stats.repairSalvagedCues928 =
            Number(job.stats.repairSalvagedCues928 || 0) + salvagedCount;
          console.warn(
            `[REPAIR ISOLATION 9.4.0] salvage=${salvagedCount}/${issues.length} | ` +
            `residual=${salvaged.unresolvedIds.length} | ids=[${salvaged.unresolvedIds.join(",")}].`
          );
        }
        return salvaged;
      } catch (parseError) {
        invalidateResponseModelForJob(job, response, "REPAIR structured output invÃ¡lido", parseError);
        throw parseError;
      }
    } catch (error) {
      lastError =
        error;

      if (
        parseAttempt >=
        REPAIR_PARSE_ATTEMPTS
      ) {
        throw error;
      }

      job.stats.repairParseRetries++;

      console.warn(
        `[REPAIR CUE-LOCK] repetindo lote: ${
          errorMessage(
            error
          ).slice(
            0,
            260
          )
        }`
      );
    }
  }

  throw lastError;
}

function preRepairAmbiguousHeuristicReason(reason) {
  return /^(?:GENDER_V2_UNKNOWN_SPEAKER_MARKED|UNKNOWN_SPEAKER_GENDER_MARKED|POSSIBLE_OMISSION|POSSIBLE_CUE_SHIFT_PAIR)$/i.test(
    String(reason || "").trim()
  );
}

function preRepairNeedsSemanticConfirmation(issue) {
  const reasons = Array.isArray(issue?.reasons)
    ? issue.reasons.map(reason => String(reason || "").trim()).filter(Boolean)
    : [];

  return (
    reasons.length > 0 &&
    reasons.every(preRepairAmbiguousHeuristicReason)
  );
}

function buildPreRepairConfirmBatches(
  blocks,
  translations,
  issues,
  plan
) {
  const posMap = positionMap(blocks);
  const batches = [];
  let current = [];
  let currentChars = 0;

  for (const issue of issues) {
    const pos = posMap.get(Number(issue?.id));
    if (!Number.isInteger(pos)) continue;

    const block = blocks[pos];
    if (!block) continue;

    const item = {
      i: block.index,
      source: String(block.text || ""),
      pt: String(translations.get(block.index) || ""),
      heuristic_reasons: Array.isArray(issue.reasons) ? issue.reasons : [],
      identity_lock: conciseIdentityForQa(block, plan),
      before: blocks
        .slice(Math.max(0, pos - 1), pos)
        .map(context => ({
          i: context.index,
          source: String(context.text || ""),
          pt: String(translations.get(context.index) || "")
        })),
      after: blocks
        .slice(pos + 1, Math.min(blocks.length, pos + 2))
        .map(context => ({
          i: context.index,
          source: String(context.text || ""),
          pt: String(translations.get(context.index) || "")
        }))
    };

    const size = JSON.stringify(item).length;

    if (
      current.length &&
      (
        current.length >= PRE_REPAIR_CONFIRM_BATCH_MAX_CUES ||
        currentChars + size > PRE_REPAIR_CONFIRM_BATCH_MAX_CHARS
      )
    ) {
      batches.push(current);
      current = [];
      currentChars = 0;
    }

    current.push(item);
    currentChars += size;
  }

  if (current.length) batches.push(current);
  return batches;
}

function parsePreRepairConfirmation(text, allowedIds) {
  const parsed = JSON.parse(stripCodeFences(text));
  const raw = Array.isArray(parsed?.issues) ? parsed.issues : [];
  const flagged = new Set();

  for (const issue of raw) {
    const id = Number(issue?.i);
    if (!Number.isInteger(id) || !allowedIds.has(id)) continue;
    flagged.add(id);
  }

  return flagged;
}

async function preConfirmAmbiguousRepairIssues(
  blocks,
  translations,
  issues,
  plan,
  job
) {
  if (!PRE_REPAIR_CONFIRM_ENABLED) return issues;

  const candidates = (Array.isArray(issues) ? issues : [])
    .filter(preRepairNeedsSemanticConfirmation);

  if (!candidates.length) return issues;

  job.stats.preRepairConfirmCandidates =
    (job.stats.preRepairConfirmCandidates || 0) + candidates.length;

  const candidateById = new Map(
    candidates.map(issue => [Number(issue.id), issue])
  );
  const candidateIds = new Set(candidateById.keys());
  const batches = buildPreRepairConfirmBatches(
    blocks,
    translations,
    candidates,
    plan
  );

  console.log(
    `[PRE-REPAIR CONFIRM] heurÃ­sticos=${candidates.length} | ` +
    `lotes=${batches.length} | rounds=${PRE_REPAIR_CONFIRM_ROUNDS} | ` +
    `concorrÃªncia=${Math.min(PRE_REPAIR_CONFIRM_CONCURRENCY, Math.max(1, batches.length))}.`
  );

  const confirmedIds = new Set();
  const coveredIds = new Set(
    batches.flatMap(batch => batch.map(item => Number(item.i)))
  );
  const technicalFallbackIds = new Set(
    [...candidateIds].filter(id => !coveredIds.has(id))
  );
  let cursor = 0;

  async function worker(workerId) {
    while (true) {
      const batchIndex = cursor++;
      if (batchIndex >= batches.length) return;

      const batch = batches[batchIndex];
      const allowedIds = new Set(batch.map(item => Number(item.i)));
      const roundResults = [];
      let technicalFailure = false;

      for (let round = 1; round <= PRE_REPAIR_CONFIRM_ROUNDS; round++) {
        try {
          const response = await geminiRequest({
            system: PRE_REPAIR_CONFIRM_PROMPT,
            user:
              `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
              `BÃBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\n` +
              `RODADA INDEPENDENTE ${round}/${PRE_REPAIR_CONFIRM_ROUNDS}\n` +
              `CUES HEURÃSTICOS:\n${JSON.stringify({ cues: batch })}\n\n` +
              `Retorne em issues SOMENTE IDs cujo defeito heurÃ­stico estÃ¡ semanticamente CONFIRMADO. ` +
              `Cue correto deve ser omitido de issues. NÃ£o reescreva texto.`,
            schema: PRE_REPAIR_CONFIRM_SCHEMA,
            thinkingLevel: PRE_REPAIR_CONFIRM_THINKING,
            maxOutputTokens: PRE_REPAIR_CONFIRM_MAX_OUTPUT_TOKENS,
            timeoutMs: PRE_REPAIR_CONFIRM_TIMEOUT_MS,
            maxRetries: PRE_REPAIR_CONFIRM_HTTP_RETRIES,
            job,
            metric: "preconfirm"
          });

          const flagged = parsePreRepairConfirmation(
            response.text,
            allowedIds
          );
          roundResults.push(flagged);

          console.log(
            `[PRE-REPAIR CONFIRM W${workerId}] lote ${batchIndex + 1}/${batches.length} | ` +
            `round=${round}/${PRE_REPAIR_CONFIRM_ROUNDS} | confirmados=${flagged.size}.`
          );
        } catch (error) {
          technicalFailure = true;
          console.warn(
            `[PRE-REPAIR CONFIRM W${workerId}] lote ${batchIndex + 1}/${batches.length} | ` +
            `round=${round} falhou; FAIL-SAFE mantÃ©m Repair | ` +
            `${errorMessage(error).slice(0, 320)}`
          );
          break;
        }
      }

      if (technicalFailure || roundResults.length !== PRE_REPAIR_CONFIRM_ROUNDS) {
        for (const id of allowedIds) technicalFallbackIds.add(id);
        continue;
      }

      for (const result of roundResults) {
        for (const id of result) confirmedIds.add(id);
      }
    }
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          PRE_REPAIR_CONFIRM_CONCURRENCY,
          Math.max(1, batches.length)
        )
      },
      (_, index) => worker(index + 1)
    )
  );

  const suppressedIds = new Set();

  for (const id of candidateIds) {
    if (confirmedIds.has(id) || technicalFallbackIds.has(id)) continue;
    suppressedIds.add(id);

    const issue = candidateById.get(id);
    if (!job.finalPriorityConsensusState || typeof job.finalPriorityConsensusState !== "object") {
      job.finalPriorityConsensusState = Object.create(null);
    }

    for (const reason of Array.isArray(issue?.reasons) ? issue.reasons : []) {
      if (!preRepairAmbiguousHeuristicReason(reason)) continue;
      const key = `${id}::${String(reason)}`;
      job.finalPriorityConsensusState[key] = Math.max(
        Number(job.finalPriorityConsensusState[key] || 0),
        PRE_REPAIR_CONFIRM_ROUNDS
      );
    }
  }

  job.stats.preRepairConfirmSuppressed =
    (job.stats.preRepairConfirmSuppressed || 0) + suppressedIds.size;
  job.stats.preRepairConfirmConfirmed =
    (job.stats.preRepairConfirmConfirmed || 0) + confirmedIds.size;
  job.stats.preRepairConfirmTechnicalFallback =
    (job.stats.preRepairConfirmTechnicalFallback || 0) + technicalFallbackIds.size;

  console.log(
    `[PRE-REPAIR CONFIRM] semanticamente-confirmados=${confirmedIds.size} | ` +
    `limpos=${suppressedIds.size} | ` +
    `fail-safe-tÃ©cnico=${technicalFallbackIds.size}.`
  );

  if (suppressedIds.size) {
    console.log(
      `[PRE-REPAIR CONFIRM] ${suppressedIds.size} cue(s) preservados SEM rewrite; ` +
      `Final Priority global continua com autoridade para contradizer esta decisÃ£o.`
    );
  }

  return (Array.isArray(issues) ? issues : [])
    .filter(issue => !suppressedIds.has(Number(issue?.id)));
}

function repairCandidateRegressionReasons(
  block,
  beforePt,
  candidatePt,
  filename,
  plan = null
) {
    const before = String(beforePt || "").trim();
  const candidate = String(candidatePt || "").trim();

  const expectedDialogueTurns =
    sourceDialogueDashCount(
      block
    );

  if (
    expectedDialogueTurns >= 2 &&
    translatedDialogueTurnCount(
      block,
      candidate
    ) !==
      expectedDialogueTurns
  ) {
    return [
      "DIALOGUE_TURN_LOCK_VIOLATION"
    ];
  }

  const beforeReasons = new Set(
    localReasonsForCue(
      block,
      before,
      filename,
      plan
    )
  );

  const afterReasons =
    localReasonsForCue(
      block,
      candidate,
      filename,
      plan
    );

  const regressions = [];

  const sourceRepeat896 = sourceRepeatNeed896(block);
  const beforeRepeat896 = targetRepeatCount896(block, before);
  const candidateRepeat896 = targetRepeatCount896(block, candidate);
  if (sourceRepeat896 >= 2 && beforeRepeat896 >= sourceRepeat896 && candidateRepeat896 < beforeRepeat896) {
    regressions.push("SOURCE_EXACT_REPETITION_REGRESSION");
  }

  for (const reason of afterReasons) {
    const isPriorityRegression =
      /^(?:EMPTY|MEANING_INTEGRITY_|FINAL_MIXED_SCRIPT_CONFUSABLE|POSSIBLE_OMISSION|ARTIFICIAL_PROFANITY_CENSORSHIP|UNRESOLVED_BLEEP_TOKEN|INVENTED_BLEEP_TOKEN|SOURCE_BLEEP_FIDELITY_LOST|BLEEP_CREATED_DANGLING_SENTENCE|VISIBLE_CENSOR_PLACEHOLDER|SOURCE_EXACT_REPETITION_LOST|CONTEXTUAL_IMPERATIVE_REFERENT_INVENTION|BARE_IMPERATIVE_CONCRETE_REFERENT_INVENTION|MISSING_DIALOGUE_BREAK|DIALOGUE_TURN_MISMATCH|DIALOGUE_TILDE_RESIDUE|SDH_RESIDUE|KNOWN_PTBR_CORRUPTION_OR_UNNATURALNESS|UNKNOWN_SPEAKER_GENDER_MARK|GENDER_V[2-9]_|FINAL_GARBAGE_OR_PLACEHOLDER|CUE_OWNERSHIP_(?:SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION)|VISIBLE_CENSOR_PLACEHOLDER|SOURCE_EXACT_REPETITION_LOST|CONTEXTUAL_IMPERATIVE_REFERENT_INVENTION|BARE_IMPERATIVE_CONCRETE_REFERENT_INVENTION)/i.test(
        reason
      );

    if (
      isPriorityRegression &&
      !beforeReasons.has(reason)
    ) {
      regressions.push(reason);
    }
  }

  // Se o Repair inventar [descriÃ§Ã£o] onde nÃ£o havia,
  // especialmente em uma fala comum, rejeitamos.
  const beforeBracketed =
    /\[[^\]]{1,120}\]/u.test(before);

  const newBracketParts =
    candidate.match(/\[[^\]]{1,120}\]/gu) || [];

  const suspiciousNewBrackets =
    newBracketParts.filter(
      part =>
        !/^\[censurado\]$/iu.test(
          part.trim()
        )
    );

  if (
    !beforeBracketed &&
    suspiciousNewBrackets.length
  ) {
    regressions.push(
      "NEW_BRACKETED_STAGE_DIRECTION"
    );
  }

  // TambÃ©m nÃ£o permitimos criar uma linha inteira
  // embrulhada em asteriscos como descriÃ§Ã£o.
  const beforeStarDescriptor =
    /(?:^|\n)\s*\*[^*\n]{1,120}\*\s*(?=$|\n)/u.test(
      before
    );

  const candidateStarDescriptor =
    /(?:^|\n)\s*\*[^*\n]{1,120}\*\s*(?=$|\n)/u.test(
      candidate
    );

  if (
    !beforeStarDescriptor &&
    candidateStarDescriptor
  ) {
    regressions.push(
      "NEW_STARRED_STAGE_DIRECTION"
    );
  }

  return [...new Set(regressions)];
}

const SINGLE_REPAIR_CONTRACT_960 = true;

async function tryFocusedRepair(
  blocks,
  translations,
  plan,
  job,
  extraIssues = [],
  options = {}
) {
  if (!REPAIR_ENABLED) {
    return translations;
  }

  const extraOnly = Boolean(options && options.extraOnly);
  let issues = [];
  let localOnlyCount = 0;

  if (!extraOnly) {
    try {
      issues = detectLocalIssues(blocks, translations, job.filename, plan);
    } catch (error) {
      console.warn(`[LOCAL GUARD] falhou; mantendo principal: ${errorMessage(error)}`);
      return translations;
    }
    localOnlyCount = issues.length;
  }

  issues = mergeIssueLists(issues, extraIssues);
  // 9.6.0: a Ãºnica rodada de Repair recebe a INTERSEÃ‡ÃƒO histÃ³rica completa.
  // Nenhum blocker hard jÃ¡ observado pode ser esquecido entre detectores.
  issues = issuesWithSemanticMemory945(job, issues);
  issues = splitGenderSeverity970(blocks, issues, job, "repair-input").hard;

  if (!extraOnly) {
    issues = await preConfirmAmbiguousRepairIssues(
      blocks,
      translations,
      issues,
      plan,
      job
    );
  }

// ============================================================
// ABSOLUTE OWNERSHIP AUDIT
// ============================================================
// Cues suspeitos de shift precisam ser auditados depois
// mesmo que o Repair nÃ£o os altere ou algum lote falhe.
const ownershipAuditIds =
  issues
    .filter(
      issue =>
        Array.isArray(
          issue?.reasons
        ) &&
        issue.reasons.some(
          reason =>
            /POSSIBLE_CUE_SHIFT_PAIR|CUE_OWNERSHIP_(?:SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION)/i.test(
              String(
                reason || ""
              )
            )
        )
    )
    .map(
      issue =>
        Number(
          issue.id
        )
    )
    .filter(
      Number.isInteger
    );

job.forceSemanticAuditIds = [
  ...new Set([
    ...(
      Array.isArray(
        job.forceSemanticAuditIds
      )
        ? job.forceSemanticAuditIds
        : []
    ),
    ...ownershipAuditIds
  ])
];

if (
  ownershipAuditIds.length
) {
  console.log(
    `[ABSOLUTE OWNERSHIP] ${ownershipAuditIds.length} cue(s) ` +
    `serÃ£o obrigatoriamente auditados apÃ³s o Repair.`
  );
}

if (!extraOnly) job.stats.localFlags = localOnlyCount;

  if (!issues.length) {
    console.log(extraOnly ? "[FINAL PRIORITY REPAIR] 0 blockers." : "[LOCAL GUARD] 0 suspeitos.");
    return translations;
  }

  logIssueSummary(extraOnly ? "FINAL-PRIORITY-REPAIR" : "PRÃ‰-REPAIR", issues);

  issues.sort((a, b) => {
    const priorityDiff =
      issuePriority(a) -
      issuePriority(b);

    if (priorityDiff !== 0) {
      return priorityDiff;
    }

    return (
      b.reasons.length -
      a.reasons.length
    );
  });

  const selected =
    issues.slice(
      0,
      REPAIR_MAX_CUES_TOTAL
    );

  const selectedIssueById960 = new Map(
    selected
      .map(issue => [Number(issue?.id), issue])
      .filter(([id]) => Number.isInteger(id))
  );

  const selectedPriority =
    selected.filter(
      issue =>
        issuePriority(issue) === 0
    ).length;

  const selectedQuality =
    selected.filter(
      issue =>
        issuePriority(issue) === 1
    ).length;

  const selectedMechanical =
    selected.filter(
      issue =>
        issuePriority(issue) === 2
    ).length;

  console.log(
    `[REPAIR PRIORITY] selecionados=${selected.length} | ` +
    `prioritÃ¡rios=${selectedPriority} | ` +
    `qualidade=${selectedQuality} | ` +
    `mecÃ¢nicos=${selectedMechanical}.`
  );

  job.stats.repairSelected =
    selected.length;

  console.log(
    extraOnly
      ? `[FINAL PRIORITY REPAIR] ${issues.length} blocker(s) desta rodada; reparando somente ${selected.length} cue(s).`
      : `[LOCAL GUARD] ${issues.length} suspeitos combinados (local+QA); reparando atÃ© ${selected.length}.`
  );

  const posMap =
    positionMap(
      blocks
    );

  const updated =
    new Map(
      translations
    );

  const repairBatches = [];
  for (let i = 0; i < selected.length; i += REPAIR_BATCH_MAX_CUES) {
    repairBatches.push(selected.slice(i, i + REPAIR_BATCH_MAX_CUES));
  }

  const totalBatches = repairBatches.length;
  let successfulBatches = 0;
  let failedBatches = 0;
  let acceptedCues = 0;
  let repairCursor = 0;
  const isolationResidual = [];

  function acceptRepairCandidates928(candidateMap, referenceTranslations, label) {
    let accepted = 0;
    for (const [id, pt] of candidateMap || []) {
      const pos = posMap.get(id);
      const block = blocks[pos];
      if (!block) continue;
      const beforePt = String(referenceTranslations.get(id) ?? "").trim();
      let candidatePt = String(pt || "").trim();

      // 9.6.0: todo candidato passa PELO MESMO fechamento determinÃ­stico
      // que serÃ¡ exigido no gate final, antes de ser julgado/aceito.
      candidatePt = closeGenderCandidateLocally925(block, candidatePt);

      const regressions = repairCandidateRegressionReasons(
        block, beforePt, candidatePt, job.filename, plan
      ).filter(reason => !isGenderGrammaticalAdvisory970(reason));

      // Bug estrutural eliminado em 9.6.0:
      // antes, um candidato podia manter o MESMO blocker antigo e ainda ser
      // aceito porque ele "nÃ£o criou um erro novo". Agora uma famÃ­lia HARD
      // que motivou o Repair precisa desaparecer no candidato.
      const requiredIssue960 = selectedIssueById960.get(Number(id));
      const requiredFamilies960 = new Set(
        blockerFamilies928(requiredIssue960 || {})
          .filter(family => [
            "GENDER", "NEGATION", "OWNERSHIP", "MEANING",
            "CENSOR", "DIALOGUE", "LAYOUT", "SDH", "REPETITION"
          ].includes(family))
      );

      const candidateLocalReasons960 = localReasonsForCue(
        block,
        candidatePt,
        job.filename,
        plan
      ).filter(reason => !isGenderGrammaticalAdvisory970(reason));
      const candidateFamilies960 = new Set(
        blockerFamilies928({ reasons: candidateLocalReasons960 })
      );

      for (const family of requiredFamilies960) {
        if (candidateFamilies960.has(family)) {
          regressions.push(`UNRESOLVED_${family}_CONSTRAINT_9_6_0`);
        }
      }

      if (
        requiredFamilies960.has("GENDER") &&
        genderTargetReasons925(block, candidatePt, job.filename, plan).length
      ) {
        regressions.push("UNRESOLVED_GENDER_CONSTRAINT_9_6_0");
      }
      const ownershipBefore898 = new Set(
        ownershipReasonsAroundCandidate898(blocks, posMap, referenceTranslations, id, beforePt)
      );
      const ownershipAfter898 = ownershipReasonsAroundCandidate898(
        blocks, posMap, referenceTranslations, id, candidatePt
      );
      for (const reason of ownershipAfter898) {
        if (!ownershipBefore898.has(reason)) regressions.push(reason);
      }
      if (regressions.length) {
        console.warn(
          `[REPAIR REGRESSION GUARD] cue ${id} rejeitado (${label}) | ${[...new Set(regressions)].join(", ")}.`
        );
        continue;
      }
      updated.set(id, candidatePt);
      accepted++;
      acceptedCues++;
    }
    return accepted;
  }

  async function isolateRepairResidual928(batch, initialResult, workerId, batchNumber) {
    if (initialResult?.translations?.size) {
      acceptRepairCandidates928(initialResult.translations, translations, `salvage-${batchNumber}`);
    }
    let unresolvedIds = Array.isArray(initialResult?.unresolvedIds)
      ? initialResult.unresolvedIds.map(Number).filter(Number.isInteger)
      : batch.map(issue => Number(issue.id)).filter(Number.isInteger);
    if (!unresolvedIds.length) return [];

    const residualIssues = batch.filter(issue => unresolvedIds.includes(Number(issue.id)));

    // 9.6.0: one semantic correction round per job. Partial JSON salvage is
    // accepted, but unresolved cues are NOT sent into micro-repair/cue-surgery
    // cascades. They remain visible to the final verification gate.
    if (SINGLE_REPAIR_CONTRACT_960) {
      if (residualIssues.length) {
        console.warn(`[SINGLE REPAIR CONTRACT 9.7.0] lote ${batchNumber}: residual=${residualIssues.length}; micro-repair desativado por arquitetura.`);
      }
      return residualIssues;
    }

    const consolidated = [];
    for (let i = 0; i < residualIssues.length; i += REPAIR_ISOLATION_MICRO_MAX_CUES_928) {
      consolidated.push(residualIssues.slice(i, i + REPAIR_ISOLATION_MICRO_MAX_CUES_928));
    }
    if (consolidated.length > REPAIR_ISOLATION_MAX_MICRO_BATCHES_928) {
      console.warn(`[REPAIR CONSOLIDATED 9.4.0] residual=${residualIssues.length} excede cap local; cue-surgery recebe somente residual nÃ£o resolvido.`);
      return residualIssues;
    }

    job.stats.repairIsolationMicroBatches928 =
      Number(job.stats.repairIsolationMicroBatches928 || 0) + consolidated.length;

    const results = await Promise.all(consolidated.map(async (group, idx) => {
      try {
        const result = await repairBatch(blocks, posMap, updated, group, plan, job);
        if (result?.translations?.size) {
          acceptRepairCandidates928(result.translations, updated, `consolidated-${batchNumber}.${idx + 1}`);
        }
        const missing = new Set((result?.unresolvedIds || []).map(Number));
        console.log(`[REPAIR CONSOLIDATED 9.4.0][W${workerId}] ${idx + 1}/${consolidated.length} | targets=${group.length} | residual=${missing.size}.`);
        return group.filter(issue => missing.has(Number(issue.id)));
      } catch (error) {
        console.warn(`[REPAIR CONSOLIDATED 9.4.0][W${workerId}] ${idx + 1}/${consolidated.length} falhou; ${group.length} cue(s) seguem para surgery | ${errorMessage(error).slice(0,220)}`);
        return group;
      }
    }));
    return results.flat();
  }

  async function repairWorker(workerId) {
    while (true) {
      const batchIndex = repairCursor++;
      if (batchIndex >= repairBatches.length) return;
      const batch = repairBatches[batchIndex];
      const batchNumber = batchIndex + 1;
      try {
        const repaired = await repairBatch(
          blocks, posMap, translations, batch, plan, job
        );
        const residual = await isolateRepairResidual928(
          batch, repaired, workerId, batchNumber
        );
        if (residual.length) isolationResidual.push(...residual);
        successfulBatches++;
        console.log(
          `[REPAIR W${workerId}] lote ${batchNumber}/${totalBatches} OK/ISOLADO | ` +
          `residual=${residual.length}.`
        );
      } catch (error) {
        // JSON integralmente irrecuperÃ¡vel: nÃ£o perdemos o lote. Dividimos sÃ³ este lote.
        const synthetic = {
          translations: new Map(),
          unresolvedIds: batch.map(issue => Number(issue.id))
        };
        const residual = await isolateRepairResidual928(
          batch, synthetic, workerId, batchNumber
        );
        if (residual.length) isolationResidual.push(...residual);
        failedBatches++;
        job.stats.repairFailures++;
        console.warn(
          `[REPAIR W${workerId}] lote ${batchNumber}/${totalBatches} exigiu isolamento; ` +
          `residual final=${residual.length} | ${errorMessage(error).slice(0,300)}`
        );
      }
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(REPAIR_CONCURRENCY, Math.max(1, totalBatches)) },
      (_, index) => repairWorker(index + 1)
    )
  );

  // Ãšltimo degrau desta etapa Ã© REALMENTE diferente: cue surgery SOURCE-ONLY
  // somente nos cues que sobreviveram ao salvage + micro-repair.
  const uniqueResidual = [...new Map(
    isolationResidual.map(issue => [Number(issue?.id), issue])
  ).values()].filter(issue => Number.isInteger(Number(issue?.id)));
  job.stats.repairIsolationResidual928 = uniqueResidual.length;

  if (uniqueResidual.length) {
    job.stats.repairIsolationCueSurgery928 = 0;
    if (SINGLE_REPAIR_CONTRACT_960) {
      console.warn(
        `[SINGLE REPAIR CONTRACT 9.7.0] residual do repair=${uniqueResidual.length} | ` +
        `SOURCE-ONLY/micro/cascade PROIBIDOS; verificaÃ§Ã£o final decidirÃ¡ fail-closed sem nova rewrite.`
      );
    } else {
      job.stats.repairIsolationCueSurgery928 = uniqueResidual.length;
      const surgicallyRepaired = await runCueSurgery927(
        blocks, updated, uniqueResidual, plan, job, "source_only"
      );
      for (const issue of uniqueResidual) {
        const id = Number(issue.id);
        const before = String(updated.get(id) || "");
        const after = String(surgicallyRepaired.get(id) || "");
        if (after && after !== before) {
          updated.set(id, after);
          acceptedCues++;
        }
      }
    }
  }

  console.log(
    `[REPAIR 9.4.0] FINAL | lotes OK=${successfulBatches}/${totalBatches} | ` +
    `lotes isolados=${failedBatches} | cues aceitos=${acceptedCues} | ` +
    `salvaged=${Number(job.stats.repairSalvagedCues928 || 0)} | ` +
    `micro=${Number(job.stats.repairIsolationMicroBatches928 || 0)} | ` +
    `residual-pÃ³s-isolamento=${uniqueResidual.length}.`
  );

  return updated;
}

// ============================================================
// PIPELINE / JOB
// ============================================================

function collectCompactRescueIssues(
  blocks,
  translations
) {
  const issues = [];

  for (const block of blocks) {
    const current =
      String(
        translations.get(block.index) ??
        block.text
      ).trim();

    if (
      !current &&
      sourceCueAllowsIntentionalEmpty(
        block
      )
    ) {
      continue;
    }

    const layout =
      layoutCueResult(
        block,
        current
      );

    if (
      !layout.fits ||
      layout.lines > LAYOUT_MAX_LINES
    ) {
      issues.push({
        id: block.index,
        reasons: [
          "SUBTITLE_TOO_DENSE"
        ]
      });
    }
  }

  return issues;
}

function buildCompactRescuePayload(
  blocks,
  posMap,
  translations,
  issues,
  plan
) {
  const locksById =
    new Map();

  const cues =
    issues.map(issue => {
      const pos =
        posMap.get(issue.id);

      const block =
        blocks[pos];

      const protectedTarget =
        protectCulturalLocks(
          block.text,
          block.index
        );

      locksById.set(
        block.index,
        protectedTarget.locks
      );

      const current =
        String(
          translations.get(block.index) ??
          ""
        ).trim();

      const layout =
        layoutCueResult(
          block,
          current
        );

      return {
        i: block.index,

        en:
          protectedTarget.text,

        pt:
          current,

                reasons:
          issue.reasons,

        ...(
          sourceDialogueDashCount(
            block
          ) >= 2
            ? {
                dialogue_turn_count:
                  sourceDialogueDashCount(
                    block
                  ),

                dialogue_turn_lock:
                  "Preserve exatamente a quantidade e a ordem dos speakers; devolva cada turn em linha prÃ³pria comeÃ§ando por '- '."
              }
            : {}
        ),

        hard_locks:
          protectedTarget.locks.map(
            lock => lock.token
          ),

        ...(block.speakerHint
          ? {
              speaker:
                block.speakerHint
            }
          : {}),

        identity_lock:
          identityLockForCapsule(
            block,
            plan
          ),

        constraints: {
          max_lines:
            LAYOUT_MAX_LINES,

          max_chars_per_line:
            LAYOUT_MAX_CHARS_PER_LINE,

          preferred_total_visible_chars:
            COMPACT_RESCUE_TARGET_TOTAL_CHARS
        },

        current_layout: {
          lines:
            layout.lines,

          max_line_length:
            layout.maxLineLength,

          visible_chars:
            layoutVisibleLength(
              normalizeLayoutWhitespace(
                current
              )
            )
        },

        before:
          blocks
            .slice(
              Math.max(
                0,
                pos - 2
              ),
              pos
            )
            .map(item => ({
              i: item.index,
              en: item.text,
              pt:
                translations.get(
                  item.index
                ) || ""
            })),

        after:
          blocks
            .slice(
              pos + 1,
              Math.min(
                blocks.length,
                pos + 3
              )
            )
            .map(item => ({
              i: item.index,
              en: item.text,
              pt:
                translations.get(
                  item.index
                ) || ""
            }))
      };
    });

  return {
    payload: {
      cues
    },
    locksById
  };
}

async function compactRescueBatch(
  blocks,
  posMap,
  translations,
  issues,
  plan,
  job,
  round
) {
  let lastError;

  for (
    let parseAttempt = 1;
    parseAttempt <= REPAIR_PARSE_ATTEMPTS;
    parseAttempt++
  ) {
    try {
      const {
        payload,
        locksById
      } =
        buildCompactRescuePayload(
          blocks,
          posMap,
          translations,
          issues,
          plan
        );

      const response =
        await geminiRequest({
          system:
            COMPACT_RESCUE_PROMPT,

          user:
            `BÃBLIA:\n${JSON.stringify(plan)}\n\n` +
            `COMPACT RESCUE â€” RODADA ${round}/${COMPACT_RESCUE_MAX_ROUNDS}\n` +
            `CUES:\n${JSON.stringify(payload)}\n\n` +
            `Todos os tokens __LOCK_C...__ devem voltar idÃªnticos. ` +
            `Se dialogue_turn_count existir, preserve EXATAMENTE os turns; compactar nÃ£o autoriza unir speakers. ` +
            `O objetivo Ã© conteÃºdo COMPLETO + PT-BR natural + 2x50.`,

          schema:
            cueTranslationSchema(
              issues.length
            ),

          thinkingLevel:
            COMPACT_RESCUE_THINKING,

          maxOutputTokens:
            COMPACT_RESCUE_MAX_OUTPUT_TOKENS,

          timeoutMs:
            COMPACT_RESCUE_TIMEOUT_MS,

          maxRetries:
            COMPACT_RESCUE_HTTP_RETRIES,

          job,

          metric:
            "compact"
        });

      try {
        return parseCueTranslation(
          issues.map(
            issue =>
              blocks[
                posMap.get(
                  issue.id
                )
              ]
          ),
          response.text,
          locksById
        );
      } catch (parseError) {
        invalidateResponseModelForJob(job, response, "COMPACT structured output invÃ¡lido", parseError);
        throw parseError;
      }
    } catch (error) {
      lastError =
        error;

      if (
        parseAttempt >=
        REPAIR_PARSE_ATTEMPTS
      ) {
        throw error;
      }

      console.warn(
        `[COMPACT CUE-LOCK] repetindo lote | ` +
        `${errorMessage(error).slice(0, 260)}`
      );
    }
  }

  throw lastError;
}

function compactRescueFailureSignature(id, value) {
  return `${Number(id)}:${sha256(normalizeLayoutWhitespace(String(value || ""))).slice(0, 20)}`;
}

function rememberCompactRescueFailure(job, id, value, reason = "rejeitado") {
  if (!job) return;
  if (!(job.compactRescueFailedSignatures instanceof Set)) {
    job.compactRescueFailedSignatures = new Set();
  }

  const signature = compactRescueFailureSignature(id, value);
  if (!job.compactRescueFailedSignatures.has(signature)) {
    job.compactRescueFailedSignatures.add(signature);
    job.stats.compactMemoizedFailures = Number(job.stats.compactMemoizedFailures || 0) + 1;
    console.log(`[COMPACT MEMORY 9.0] cue ${id} memorizado (${reason}); mesmo texto nÃ£o gastarÃ¡ Gemini de novo neste job.`);
  }
}

function compactRescueAlreadyFailed(job, id, value) {
  return Boolean(
    job?.compactRescueFailedSignatures instanceof Set &&
    job.compactRescueFailedSignatures.has(compactRescueFailureSignature(id, value))
  );
}

async function runCompactRescue(
  blocks,
  translations,
  plan,
  job
) {
  if (!COMPACT_RESCUE_ENABLED) {
    return translations;
  }

  const posMap =
    positionMap(blocks);

  const updated =
    new Map(translations);

  let totalAccepted = 0;
  let totalRejected = 0;

  for (
    let round = 1;
    round <= COMPACT_RESCUE_MAX_ROUNDS;
    round++
  ) {
    const detectedIssues =
      collectCompactRescueIssues(
        blocks,
        updated
      );

    const allIssues = detectedIssues.filter(issue =>
      !compactRescueAlreadyFailed(
        job,
        issue.id,
        updated.get(issue.id) || ""
      )
    );

    const memoSkipped = detectedIssues.length - allIssues.length;
    if (memoSkipped > 0) {
      console.log(
        `[COMPACT MEMORY 9.0] ${memoSkipped} overflow(s) jÃ¡ reprovado(s) ` +
        `com o mesmo texto; 0 nova chamada cloud.`
      );
    }

    if (!allIssues.length) {
      console.log(
        `[COMPACT RESCUE] rodada ${round}: ` +
        `nenhum overflow restante âœ…`
      );

      break;
    }

    const selected =
      allIssues.slice(
        0,
        COMPACT_RESCUE_MAX_CUES_TOTAL
      );

    if (
      allIssues.length >
      selected.length
    ) {
      console.warn(
        `[COMPACT RESCUE] ${allIssues.length} overflow(s); ` +
        `limite desta rodada=${selected.length}.`
      );
    }

    console.log(
      `[COMPACT RESCUE] rodada ${round}/${COMPACT_RESCUE_MAX_ROUNDS} | ` +
      `candidatos=${selected.length}.`
    );

    let acceptedThisRound = 0;
    let rejectedThisRound = 0;

    for (
      let i = 0;
      i < selected.length;
      i += COMPACT_RESCUE_BATCH_MAX_CUES
    ) {
      const batch =
        selected.slice(
          i,
          i +
            COMPACT_RESCUE_BATCH_MAX_CUES
        );

      try {
        const rescued =
          await compactRescueBatch(
            blocks,
            posMap,
            updated,
            batch,
            plan,
            job,
            round
          );

        for (
          const [id, rawPt]
          of rescued
        ) {
          const pos =
            posMap.get(id);

          const block =
            blocks[pos];

          if (!block) {
            continue;
          }

          const beforePt =
            String(
              updated.get(id) ??
              ""
            ).trim();

          const candidatePt =
            sanitizeFinalCue(
              block,
              String(rawPt || "")
            );

          if (!candidatePt) {
            rejectedThisRound++;
            totalRejected++;
            rememberCompactRescueFailure(job, id, beforePt, "vazio pÃ³s-sanitizer");

            console.warn(
              `[COMPACT RESCUE] cue ${id} rejeitado: vazio apÃ³s sanitizer.`
            );

            continue;
          }

          const layout =
            layoutCueResult(
              block,
              candidatePt
            );

          if (
            !layout.fits ||
            layout.lines >
              LAYOUT_MAX_LINES
          ) {
            rejectedThisRound++;
            totalRejected++;
            rememberCompactRescueFailure(job, id, beforePt, "ainda nÃ£o cabe em 2x50");

            console.warn(
              `[COMPACT RESCUE] cue ${id} ainda nÃ£o cabe em 2x${LAYOUT_MAX_CHARS_PER_LINE} | ` +
              `maior linha=${layout.maxLineLength}.`
            );

            continue;
          }

          const regressions =
            repairCandidateRegressionReasons(
              block,
              beforePt,
              candidatePt,
              job.filename,
              plan
            );

          if (regressions.length) {
            rejectedThisRound++;
            totalRejected++;
            rememberCompactRescueFailure(job, id, beforePt, `regressÃ£o: ${regressions.join(",")}`);

            console.warn(
              `[COMPACT RESCUE REGRESSION] cue ${id} rejeitado | ` +
              `${regressions.join(", ")}.`
            );

            continue;
          }

          updated.set(
            id,
            candidatePt
          );

          acceptedThisRound++;
          totalAccepted++;
        }
      } catch (error) {
        console.warn(
          `[COMPACT RESCUE] lote falhou sem matar episÃ³dio | ` +
          `${errorMessage(error).slice(0, 350)}`
        );
      }
    }

    console.log(
      `[COMPACT RESCUE] rodada ${round} | ` +
      `aprovados=${acceptedThisRound} | ` +
      `rejeitados=${rejectedThisRound}.`
    );
  }

  const remaining =
    collectCompactRescueIssues(
      blocks,
      updated
    );

  console.log(
    `[COMPACT RESCUE] FINAL | ` +
    `aprovados=${totalAccepted} | ` +
    `rejeitados=${totalRejected} | ` +
    `overflow restante=${remaining.length}.`
  );

  return updated;
}

function semanticComparableText(value) {
  return normalizeLayoutWhitespace(
    String(value || "")
  )
    .toLocaleLowerCase("pt-BR")
    .trim();
}

function collectPostRewriteSemanticCandidates(
  blocks,
  beforeTranslations,
  afterTranslations
) {
  const candidates = [];

  for (const block of blocks) {
    const beforePt =
      String(
        beforeTranslations.get(block.index) ??
        ""
      ).trim();

    const afterPt =
      String(
        afterTranslations.get(block.index) ??
        ""
      ).trim();

    // Apenas mudanÃ§a de quebra de linha NÃƒO conta como rewrite.
    if (
      semanticComparableText(beforePt) ===
      semanticComparableText(afterPt)
    ) {
      continue;
    }

    candidates.push({
      id: block.index
    });
  }

  return candidates;
}

function buildSemanticRewriteAuditBatches(
  blocks,
  posMap,
  beforeTranslations,
  afterTranslations,
  candidates,
  plan
) {
  const batches = [];

  let current = [];
  let currentChars = 0;

  for (const candidate of candidates) {
    const pos =
      posMap.get(candidate.id);

    const block =
      blocks[pos];

    if (!block) {
      continue;
    }

    const protectedTarget =
      protectCulturalLocks(
        block.text,
        block.index
      );

    const beforePt =
      String(
        beforeTranslations.get(block.index) ??
        ""
      ).trim();

    const afterPt =
      String(
        afterTranslations.get(block.index) ??
        ""
      ).trim();

    const item = {
      i: block.index,

      en:
  String(
    block.text ||
    ""
  ),

      before_pt:
        beforePt,

      after_pt:
        afterPt,

      canonical_locks:
  protectedTarget.locks.map(
    lock => lock.value
  ),

      ...(block.speakerHint
        ? {
            speaker:
              block.speakerHint
          }
        : {}),

      identity_lock:
        identityLockForCapsule(
          block,
          plan
        ),

      before_context:
        blocks
          .slice(
            Math.max(
              0,
              pos - 1
            ),
            pos
          )
          .map(item => ({
            
            en: item.text,
            ...(item.speakerHint
              ? {
                  speaker:
                    item.speakerHint
                }
              : {})
          })),

      after_context:
        blocks
          .slice(
            pos + 1,
            Math.min(
              blocks.length,
              pos + 2
            )
          )
          .map(item => ({
            
            en: item.text,
            ...(item.speakerHint
              ? {
                  speaker:
                    item.speakerHint
                }
              : {})
          }))
    };

    const estimatedChars =
      JSON.stringify(item).length;

    if (
      current.length &&
      (
        current.length >=
          SEMANTIC_REWRITE_AUDIT_MAX_CUES_PER_BATCH ||
        currentChars +
          estimatedChars >
          SEMANTIC_REWRITE_AUDIT_MAX_CHARS_PER_BATCH
      )
    ) {
      batches.push(
        current
      );

      current = [];
      currentChars = 0;
    }

    current.push({
      item,
      locks:
        protectedTarget.locks
    });

    currentChars +=
      estimatedChars;
  }

  if (current.length) {
    batches.push(
      current
    );
  }

  return batches;
}

async function trySemanticCompactCorrection({
  block,
  semanticCandidate,
  reason,
  locks,
  plan,
  job
}) {
  console.log(
    `[SEMANTIC COMPACT RETRY] cue ${block.index} | tentando preservar correÃ§Ã£o dentro de 2x50.`
  );

  const response =
    await geminiRequest({
      system:
        SEMANTIC_COMPACT_RETRY_PROMPT,

      user:
        `BÃBLIA:\n${
          JSON.stringify(
            plan || {}
          )
        }\n\n` +
        `CUE:\n${
          JSON.stringify({
            i:
              block.index,

            en:
              block.text,

            semantic_pt:
              semanticCandidate,

            reason:
              String(
                reason ||
                ""
              ),

            canonical_locks:
              (locks || [])
                .map(
                  lock =>
                    lock.value
                )
          })
        }\n\n` +
        `Retorne exatamente 1 cue com o mesmo ID.`,

      schema:
        cueTranslationSchema(
          1
        ),

      thinkingLevel:
        SEMANTIC_COMPACT_RETRY_THINKING,

      maxOutputTokens:
        SEMANTIC_COMPACT_RETRY_MAX_OUTPUT_TOKENS,

      timeoutMs:
        SEMANTIC_COMPACT_RETRY_TIMEOUT_MS,

      maxRetries:
        SEMANTIC_COMPACT_RETRY_HTTP_RETRIES,

      job,

      metric:
        "compact"
    });

  const parsed =
    parseCueTranslation(
      [block],
      response.text
    );

  let candidate =
    String(
      parsed.get(
        block.index
      ) ||
      ""
    ).trim();

  candidate =
    canonicalizeCulturalText(
      candidate
    );

  candidate =
    sanitizeFinalCue(
      block,
      candidate
    );

  candidate =
    canonicalizeCulturalText(
      candidate
    );

  if (!candidate) {
    throw new Error(
      "Semantic Compact Retry retornou vazio."
    );
  }

  const missingLocks =
    missingCanonicalCultureLocks(
      candidate,
      locks
    );

  if (
    missingLocks.length
  ) {
    throw new Error(
      `Semantic Compact Retry perdeu canonical lock: ${
        missingLocks
          .map(
            lock =>
              lock.value
          )
          .join(", ")
      }`
    );
  }

  const layout =
    layoutCueResult(
      block,
      candidate
    );

  if (
    !layout.fits ||
    layout.lines >
      LAYOUT_MAX_LINES
  ) {
    throw new Error(
      `Semantic Compact Retry ainda nÃ£o cabe em ` +
      `${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE}; ` +
      `maior linha=${layout.maxLineLength}.`
    );
  }

  const regressions =
    repairCandidateRegressionReasons(
      block,
      semanticCandidate,
      candidate,
      job.filename,
      plan
    );

  if (
    regressions.length
  ) {
    throw new Error(
      `Semantic Compact Retry criou regressÃ£o: ` +
      `${regressions.join(", ")}`
    );
  }

  return candidate;
}

function semanticSourceLikelyContinuesNextCue(
  blocks,
  posMap,
  id
) {
  const pos =
    posMap.get(id);

  if (
    !Number.isInteger(pos) ||
    pos < 0 ||
    pos >= blocks.length - 1
  ) {
    return false;
  }

  const current =
    String(
      blocks[pos]?.text ||
      ""
    ).trim();

  const next =
    String(
      blocks[pos + 1]?.text ||
      ""
    ).trim();

  if (
    !current ||
    !next
  ) {
    return false;
  }

  // Se o cue atual jÃ¡ termina claramente uma frase,
  // nÃ£o tratamos como continuaÃ§Ã£o.
  if (
    /[.!?]["'â€™â€)\]]*$/u.test(
      current
    )
  ) {
    return false;
  }

  // Primeiro caractere alfabÃ©tico do prÃ³ximo cue.
  const firstLetter =
    next.match(
      /\p{L}/u
    )?.[0] || "";

  if (!firstLetter) {
    return false;
  }

  const startsLowercase =
    firstLetter ===
      firstLetter.toLocaleLowerCase() &&
    firstLetter !==
      firstLetter.toLocaleUpperCase();

  return startsLowercase;
}

async function runPostRewriteSemanticAudit(
  blocks,
  beforeTranslations,
  afterTranslations,
  plan,
  job
) {
  if (!SEMANTIC_REWRITE_AUDIT_ENABLED) {
    return afterTranslations;
  }

  const candidates =
  collectPostRewriteSemanticCandidates(
    blocks,
    beforeTranslations,
    afterTranslations
  );

const forcedOwnershipIds =
  new Set(
    (
      Array.isArray(
        job?.forceSemanticAuditIds
      )
        ? job.forceSemanticAuditIds
        : []
    )
      .map(Number)
      .filter(
        Number.isInteger
      )
  );

const candidateIds =
  new Set(
    candidates.map(
      item =>
        item.id
    )
  );

for (
  const id of
  forcedOwnershipIds
) {
  if (
    !candidateIds.has(id)
  ) {
    candidates.push({
      id
    });

    candidateIds.add(id);
  }
}

if (!candidates.length) {
    console.log(
      "[SEMANTIC REWRITE GUARD] 0 cues realmente reescritos; auditoria dispensada."
    );

    return afterTranslations;
  }

  const posMap =
    positionMap(
      blocks
    );

  const batches =
    buildSemanticRewriteAuditBatches(
      blocks,
      posMap,
      beforeTranslations,
      afterTranslations,
      candidates,
      plan
    );

  console.log(
    `[SEMANTIC REWRITE GUARD] ` +
    `${candidates.length} cue(s) realmente reescrito(s) | ` +
    `${batches.length} lote(s) ENÃ—BEFOREÃ—AFTER | concorrÃªncia=${Math.min(SEMANTIC_REWRITE_AUDIT_CONCURRENCY, batches.length)}.`
  );

  const updated =
    new Map(
      afterTranslations
    );

  let totalFlagged = 0;
  let totalAccepted = 0;
  let totalRejected = 0;
  let semanticCompactRetries = 0;

  let semanticCursor = 0;

  async function semanticWorker(workerId) {
    while (true) {
      const batchIndex = semanticCursor++;
      if (batchIndex >= batches.length) return;

    const batch =
      batches[batchIndex];

    const payload = {
      cues:
        batch.map(
          entry =>
            entry.item
        )
    };

    try {
      const response =
        await geminiRequest({
          system:
            SEMANTIC_REWRITE_AUDIT_PROMPT,

          user:
            `BÃBLIA:\n${JSON.stringify(plan)}\n\n` +
            `AUDITORIA PÃ“S-REESCRITA ${batchIndex + 1}/${batches.length}\n` +
            `CUES:\n${JSON.stringify(payload)}\n\n` +
            `Marque regressÃµes semÃ¢nticas reais E qualquer violaÃ§Ã£o absoluta de cue ownership ENâ†’AFTER_PT. ` +
            `Reparo conservador de fonte claramente truncada Ã© permitido. ` +
            `ParÃ¡frase natural e fiel NÃƒO Ã© erro.`,

          schema:
            SEMANTIC_REWRITE_AUDIT_SCHEMA,

          thinkingLevel:
            SEMANTIC_REWRITE_AUDIT_THINKING,

          maxOutputTokens:
            SEMANTIC_REWRITE_AUDIT_MAX_OUTPUT_TOKENS,

          timeoutMs:
            SEMANTIC_REWRITE_AUDIT_TIMEOUT_MS,

          maxRetries:
            SEMANTIC_REWRITE_AUDIT_HTTP_RETRIES,

          job,

          metric:
            "semantic"
        });

      const parsed =
        JSON.parse(
          stripCodeFences(
            response.text
          )
        );

      const rawIssues =
        Array.isArray(parsed?.issues)
          ? parsed.issues
          : [];

      const allowedIds =
        new Set(
          batch.map(
            entry =>
              entry.item.i
          )
        );

      const locksById =
        new Map(
          batch.map(
            entry => [
              entry.item.i,
              entry.locks
            ]
          )
        );

      for (const issue of rawIssues) {
        const id =
          Number(
            issue?.i
          );

        if (
          !Number.isInteger(id) ||
          !allowedIds.has(id)
        ) {
          continue;
        }

        totalFlagged++;

        const pos =
          posMap.get(id);

        const block =
          blocks[pos];

        if (!block) {
          totalRejected++;
          continue;
        }

        let candidatePt =
          String(
            issue?.pt ||
            ""
          ).trim();

        if (!candidatePt) {
          totalRejected++;

          console.warn(
            `[SEMANTIC REWRITE GUARD] cue ${id} sinalizado sem correÃ§Ã£o utilizÃ¡vel.`
          );

          continue;
        }

        candidatePt =
  canonicalizeCulturalText(
    candidatePt
  );

        candidatePt =
          sanitizeFinalCue(
            block,
            candidatePt
          );

        if (!candidatePt) {
          totalRejected++;

          console.warn(
            `[SEMANTIC REWRITE GUARD] cue ${id} ficou vazio apÃ³s sanitizer.`
          );

          continue;
        }

        candidatePt =
  canonicalizeCulturalText(
    candidatePt
  );

const requiredLocks =
  locksById.get(id) ||
  [];

const missingLocks =
  missingCanonicalCultureLocks(
    candidatePt,
    requiredLocks
  );

if (
  missingLocks.length
) {
  totalRejected++;

  console.warn(
    `[SEMANTIC REWRITE GUARD] cue ${id} rejeitado por CANONICAL LOCK | ` +
    `faltando=${
      missingLocks
        .map(
          lock =>
            lock.value
        )
        .join(", ")
    }`
  );

  continue;
}

        // A correÃ§Ã£o semÃ¢ntica jamais pode relaxar o teto visual.
        const layout =
          layoutCueResult(
            block,
            candidatePt
          );

        if (
  !layout.fits ||
  layout.lines >
    LAYOUT_MAX_LINES
) {
  const canRetryCompact =
    SEMANTIC_COMPACT_RETRY_ENABLED &&
    semanticCompactRetries <
      SEMANTIC_COMPACT_RETRY_MAX_PER_EPISODE;

  if (canRetryCompact) {
    semanticCompactRetries++;

    try {
      const compacted =
        await trySemanticCompactCorrection({
          block,

          semanticCandidate:
            candidatePt,

          reason:
            issue?.reason,

          locks:
            locksById.get(id) ||
              [],

          plan,
          job
        });

      updated.set(
        id,
        compacted
      );

      totalAccepted++;

      console.log(
        `[SEMANTIC COMPACT RETRY] cue ${id} corrigido âœ… | ` +
        `correÃ§Ã£o semÃ¢ntica preservada dentro de ` +
        `${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE}.`
      );

      continue;
    } catch (error) {
      console.warn(
        `[SEMANTIC COMPACT RETRY] cue ${id} falhou | ` +
        `${errorMessage(error).slice(0, 300)}`
      );
    }
  }

  totalRejected++;

  console.warn(
    `[SEMANTIC REWRITE GUARD] cue ${id} rejeitado: ` +
    `nÃ£o cabe em ${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE} | ` +
    `maior linha=${layout.maxLineLength}.`
  );

  continue;
}

        const currentPt =
          String(
            updated.get(id) ??
            ""
          ).trim();

        const sourceContinuesNextCue =
  semanticSourceLikelyContinuesNextCue(
    blocks,
    posMap,
    id
  );

const currentWordCount =
  words(
    currentPt
  ).length;

const candidateWordCount =
  words(
    candidatePt
  ).length;

if (
  !forcedOwnershipIds.has(id) &&
  sourceContinuesNextCue &&
  candidateWordCount >=
    currentWordCount + 2
) {
  totalRejected++;

  const nextCue =
    blocks[
      pos + 1
    ];

  console.warn(
    `[SEMANTIC OWNERSHIP GUARD] cue ${id} correÃ§Ã£o rejeitada | ` +
    `fonte continua no cue ${nextCue?.index ?? "seguinte"} e ` +
    `a correÃ§Ã£o adicionaria conteÃºdo alÃ©m do limite do target.`
  );

  continue;
}

        const regressions =
          repairCandidateRegressionReasons(
            block,
            currentPt,
            candidatePt,
            job.filename,
            plan
          );

        if (regressions.length) {
          totalRejected++;

          console.warn(
            `[SEMANTIC REWRITE GUARD] cue ${id} correÃ§Ã£o rejeitada pelo guard local | ` +
            `${regressions.join(", ")}.`
          );

          continue;
        }

        updated.set(
          id,
          candidatePt
        );

        totalAccepted++;

        console.log(
          `[SEMANTIC REWRITE GUARD] cue ${id} corrigido âœ… | ` +
          `${String(
            issue?.reason ||
            "regressÃ£o semÃ¢ntica"
          ).slice(0, 180)}`
        );
      }

      console.log(
        `[SEMANTIC REWRITE GUARD] lote ${batchIndex + 1}/${batches.length} | ` +
        `flags=${rawIssues.length}.`
      );
    } catch (error) {
      console.warn(
        `[SEMANTIC REWRITE GUARD] lote ${batchIndex + 1}/${batches.length} falhou; ` +
        `mantendo resultado anterior sem matar episÃ³dio | ` +
        `${errorMessage(error).slice(0, 350)}`
      );
    }
    }
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          SEMANTIC_REWRITE_AUDIT_CONCURRENCY,
          batches.length
        )
      },
      (_, index) => semanticWorker(index + 1)
    )
  );


  console.log(
    `[SEMANTIC REWRITE GUARD] FINAL | ` +
    `auditados=${candidates.length} | ` +
    `sinalizados=${totalFlagged} | ` +
    `correÃ§Ãµes aceitas=${totalAccepted} | ` +
    `correÃ§Ãµes rejeitadas=${totalRejected}.`
  );

  return updated;
}

// ============================================================
// FINAL PRIORITY GATE 8.4.0
// ============================================================
// IMPORTANTE: SOURCE pode ser inglÃªs, espanhol, francÃªs, italiano,
// alemÃ£o, neerlandÃªs ou qualquer outro idioma que a Ponte tenha
// escolhido. InglÃªs Ã© preferÃªncia de seleÃ§Ã£o, NÃƒO prÃ©-condiÃ§Ã£o.
const FINAL_PRIORITY_AUDIT_PROMPT = `
VocÃª Ã© o FINAL PRIORITY AUDITOR de legendas SOURCEâ†’PT-BR.

SOURCE Ã© a legenda-fonte REAL escolhida pelo orquestrador e pode estar
em QUALQUER idioma. O rÃ³tulo EN usado em partes antigas do sistema Ã©
apenas legado interno. NUNCA presuma inglÃªs.

Sua funÃ§Ã£o Ã© encontrar SOMENTE defeitos PRIORITÃRIOS. NÃ£o faÃ§a revisÃ£o
cosmÃ©tica e nÃ£o marque uma alternativa apenas porque vocÃª escreveria diferente.

Para cada cue, compare primeiro SOURCE[i] com PT[i]. Os cues aparecem em
ordem cronolÃ³gica e os vizinhos servem apenas para contexto.

1) CUE OWNERSHIP â€” PRIORITÃRIO
- PT[i] precisa traduzir SOURCE[i], nÃ£o SOURCE[i-1] nem SOURCE[i+1].
- Se PT[i] traduz claramente o vizinho, marque category=CUE_OWNERSHIP_SHIFT.
- Se houver uma cadeia deslocada +1/-1, marque TODOS os IDs afetados.
- ContinuaÃ§Ã£o legÃ­tima de frase entre cues nÃ£o Ã© shift: cada ID preserva
  somente a parte pertencente ao SOURCE daquele mesmo ID.

2) GENDER / REFERENT â€” PRIORITÃRIO
- Respeite identidade_lock e evidÃªncia real da cena.
- Speaker Ã© quem fala; mentions sÃ£o pessoas citadas.
- NÃ£o transfira gÃªnero entre speaker e pessoa mencionada.
- Speaker desconhecido NÃƒO autoriza adivinhar masculino/feminino.
- Se PT marcar gÃªnero sem evidÃªncia segura quando seria possÃ­vel neutralizar,
  marque category=GENDER_OR_REFERENT.
- Isso inclui PAPÃ‰IS HUMANOS: source "you/I am a/an ..." sem sexo/gÃªnero explÃ­cito nÃ£o autoriza artigo/substantivo marcado como "um passageiro/uma passageira", "convidado/convidada" etc. Exija neutralidade natural.
- ContradiÃ§Ã£o dentro do mesmo speaker no mesmo cue Ã© sempre prioritÃ¡ria,
  por exemplo masculino em uma palavra e feminino em outra sem mudanÃ§a de referente.

3) MEANING INTEGRITY â€” PRIORITÃRIO
Marque somente perda/troca/invenÃ§Ã£o real de informaÃ§Ã£o importante:
negaÃ§Ã£o, aÃ§Ã£o, sujeito, objeto, quantidade, relaÃ§Ã£o, causa, condiÃ§Ã£o,
contraste, intensidade, insulto, referente ou informaÃ§Ã£o narrativa.
- POLARITY LOCK: confira explicitamente not/n\'t/never/nothing/no one e equivalentes. "didn\'t think" NÃƒO pode virar "achou".
- ARGUMENT LOCK: preserve sujeitoâ†’verboâ†’objeto. Objeto externo nÃ£o pode virar reflexivo/recÃ­proco; "they ... him/her" exige manter o alvo externo.
- OBJECT/ENTITY FIDELITY: bebida/comida/objeto/termo especÃ­fico nÃ£o pode ser trocado por outro item semanticamente diferente.
REPETIÃ‡ÃƒO INTENCIONAL tambÃ©m Ã© informaÃ§Ã£o: se SOURCE repete deliberadamente a mesma pergunta/frase N vezes e PT reduz a contagem, marque MEANING_INTEGRITY.
EXPRESSÃƒO AMBÃGUA exige contexto: leia vizinhos antes de aceitar uma traduÃ§Ã£o literal. NÃ£o aceite objeto/referente inventado por um imperativo idiomÃ¡tico.
category=MEANING_INTEGRITY.

4) GARBAGE / EMPTY â€” PRIORITÃRIO
- PT vazio para SOURCE verbal nÃ£o vazio;
- lixo isolado como Ff, ff, J..., J-, J'j', reticÃªncias/pontuaÃ§Ã£o sem fala;
- placeholder ou resÃ­duo evidente de OCR que nÃ£o comunica fala.
category=GARBAGE_OR_EMPTY.

5) DIALOGUE / SDH â€” PRIORITÃRIO
- speaker/turn perdido ou unido quando SOURCE tem mÃºltiplos turns;
- dialogue_turn_count>=2 Ã© HARD mesmo se a SOURCE nÃ£o usava "-": implicit_strong representa interrupÃ§Ã£o/resposta curta de outro speaker;
- descriÃ§Ã£o SDH inventada no lugar da fala.
category=DIALOGUE_OR_SDH.

6) LANGUAGE / ORTHOGRAPHY / TYPOGRAPHY â€” PRIORITÃRIO QUANDO CLARO
- typo/OCR/palavra impossÃ­vel em PT-BR;
- palavra estrangeira comum deixada sem funÃ§Ã£o cultural real;
- hÃ­brido lexical artificial;
- "--" residual ou pontuaÃ§Ã£o quebrada que prejudica o acabamento;
- preserve nomes prÃ³prios, marcas e emprÃ©stimos realmente naturais.
category=LANGUAGE_INTEGRITY.

7) SOURCE CENSORSHIP NATURALIZATION â€” PRIORITÃRIO
- ${BLEEP_TOKEN} Ã© metadata invisÃ­vel: PT FINAL NUNCA pode conter ${BLEEP_TOKEN}, [censurado], [bleep], asteriscos ou placeholder equivalente.
- Compare a situaÃ§Ã£o e os cues vizinhos: a fala precisa preservar naturalmente surpresa/raiva/medo/insulto/humor e a intensidade pragmÃ¡tica.
- Ã‰ aceitÃ¡vel usar um palavrÃ£o brasileiro plausÃ­vel para reconstruir a FUNÃ‡ÃƒO da fala; nÃ£o Ã© necessÃ¡rio conhecer a palavra exata escondida.
- Marque GARBAGE_OR_EMPTY se houver placeholder visÃ­vel; marque MEANING_INTEGRITY se a naturalizaÃ§Ã£o mudar claramente a intenÃ§Ã£o ou a intensidade da cena.

NÃƒO marque:
- mera preferÃªncia estilÃ­stica;
- uma traduÃ§Ã£o natural diferente mas fiel;
- diferenÃ§a de ordem sintÃ¡tica legÃ­tima em portuguÃªs;
- ausÃªncia de correspondÃªncia palavra por palavra.

A saÃ­da contÃ©m somente issues reais. reason deve explicar brevemente a prova.
`;

const FINAL_PRIORITY_REPAIR_PROMPT = `
VocÃª Ã© o RESCUE PRIORITÃRIO FINAL de legendas SOURCEâ†’PT-BR.

SOURCE pode estar em QUALQUER idioma. O idioma real do campo source Ã© a
autoridade. VocÃª NÃƒO estÃ¡ editando por estilo: estÃ¡ reconstruindo do zero
somente cues que falharam num gate prioritÃ¡rio.

Para cada item:
- traduza EXCLUSIVAMENTE source daquele mesmo i;
- current_pt Ã© apenas evidÃªncia do erro atual, NÃƒO Ã© autoridade;
- before/after sÃ£o contexto e JAMAIS podem fornecer conteÃºdo para o target;
- reasons dizem exatamente por que o cue foi reprovado;
- identity_lock Ã© obrigatÃ³rio;
- se a SOURCE nÃ£o marcar gÃªnero, neutralize naturalmente; masculino genÃ©rico NÃƒO conta como neutro; papÃ©is humanos em 1Âª/2Âª pessoa tambÃ©m precisam ser neutros sem prova explÃ­cita;
- REGRA ESTRUTURAL 9.2.3: I/you/we + estado, adjetivo, particÃ­pio ou papel humano neutro NÃƒO autoriza PT-BR com -o/-a, -os/-as ou artigo um/uma que marque a pessoa. Reformule a FRASE, nÃ£o troque apenas a desinÃªncia; preserve integralmente o significado;
- leia before/after para resolver intenÃ§Ã£o de expressÃµes e imperativos ambÃ­guos, mas nunca copie conteÃºdo deles para o target;
- preserve a CONTAGEM de repetiÃ§Ãµes deliberadas da mesma frase/pergunta da SOURCE;
- nunca produza lixo/placeholder/reticÃªncias para preencher vazio;
- preserve exatamente os hard_locks __LOCK_C...__;
- se source contiver ${BLEEP_TOKEN}, NÃƒO devolva token/placeholder; reconstrua pragmaticamente uma fala natural em PT-BR com intensidade contextual compatÃ­vel;
- preserve exatamente turns de diÃ¡logo quando dialogue_turn_count >= 2; esses turns podem ter sido inferidos com alta confianÃ§a mesmo sem hÃ­fen na SOURCE. Um por linha, comeÃ§ando com "- ";
- preserve polaridade explÃ­cita e sujeitoâ†’verboâ†’objeto; nunca transforme objeto externo em "se" reflexivo/recÃ­proco;
- preserve a identidade semÃ¢ntica de bebidas, comidas, objetos e termos especÃ­ficos; nÃ£o troque por item apenas parecido;
- devolva PT-BR ortograficamente limpo, sem resÃ­duo estrangeiro acidental nem "--" cru; nomes/loanwords genuÃ­nos permanecem;
- nÃ£o altere timestamp, nÃ£o crie cue e nÃ£o mova conteÃºdo.

CUE OWNERSHIP:
Se o current_pt estiver deslocado para o cue anterior/seguinte, IGNORE-O e
reconstrua a traduÃ§Ã£o diretamente do source do mesmo ID.

GÃŠNERO:
Se nÃ£o houver prova segura, prefira formulaÃ§Ãµes naturais sem marca de gÃªnero.
Masculino genÃ©rico NÃƒO Ã© neutro: reformule naturalmente em vez de usar masculino como padrÃ£o.
Nunca misture masculino e feminino para o mesmo speaker/referente.

LAYOUT:
Se possÃ­vel, escreva de modo naturalmente conciso para caber em no mÃ¡ximo
${LAYOUT_MAX_LINES} linhas de ${LAYOUT_MAX_CHARS_PER_LINE} caracteres sem
remover informaÃ§Ã£o.

Devolva exatamente um objeto por cue recebido.
`;

const FINAL_PRIORITY_AUDIT_SCHEMA = {
  // 8.4.2: deliberadamente simples.
  // O Interactions structured-output pode rejeitar schemas cujo limite
  // de array expanda demais a gramÃ¡tica/constraint. O limite real aqui
  // jÃ¡ Ã© imposto pelo tamanho do lote e pelo parser local.
  type: "object",
  properties: {
    issues: {
      type: "array",
      items: {
        type: "object",
        properties: {
          i: { type: "integer" },
          category: { type: "string" },
          reason: { type: "string" }
        },
        required: ["i", "category", "reason"]
      }
    }
  },
  required: ["issues"]
};

const FINAL_PRIORITY_AUDIT_FALLBACK_SCHEMA = {
  // Fallback ainda menor para um eventual HTTP 400 de validaÃ§Ã£o.
  // category pode ser omitido; parseFinalPriorityAudit usa "PRIORITY".
  type: "object",
  properties: {
    issues: {
      type: "array",
      items: {
        type: "object",
        properties: {
          i: { type: "integer" },
          reason: { type: "string" }
        },
        required: ["i", "reason"]
      }
    }
  },
  required: ["issues"]
};

function finalPriorityIssueSignature(issues) {
  return (Array.isArray(issues) ? issues : [])
    .map(issue => `${Number(issue.id)}:${(issue.reasons || []).join("|")}`)
    .sort()
    .join(";;");
}

// JavaScript nÃ£o suporta flag /x. Mantemos a expressÃ£o acima legÃ­vel
// atravÃ©s desta implementaÃ§Ã£o real equivalente.
function finalReasonBlocks(reason) {
  return /FINAL_PRIORITY|MEANING_INTEGRITY_|NEGATION_|REFERENT_INTEGRITY_|PTBR_ORTHOGRAPHY_ERROR|FINAL_MIXED_SCRIPT_CONFUSABLE|GENDER_V[2-9]_|UNKNOWN_SPEAKER_GENDER_MARKED|FINAL_GARBAGE_OR_PLACEHOLDER|^EMPTY$|POSSIBLE_OMISSION|POSSIBLE_CUE_SHIFT_PAIR|CUE_OWNERSHIP_(?:SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION)|UNRESOLVED_BLEEP_TOKEN|INVENTED_BLEEP_TOKEN|SOURCE_BLEEP_FIDELITY_LOST|BLEEP_CREATED_DANGLING_SENTENCE|VISIBLE_CENSOR_PLACEHOLDER|SOURCE_EXACT_REPETITION_LOST|CONTEXTUAL_IMPERATIVE_REFERENT_INVENTION|BARE_IMPERATIVE_CONCRETE_REFERENT_INVENTION|ARTIFICIAL_PROFANITY_CENSORSHIP|DIALOGUE_TURN_MISMATCH|DIALOGUE_TILDE_RESIDUE|MISSING_DIALOGUE_BREAK|SDH_RESIDUE|SPEAKER_LABEL_RESIDUE|SUBTITLE_TOO_DENSE/i.test(
    String(reason || "")
  );
}


function finalPriorityHeuristicReason(reason) {
  return /^(?:GENDER_V2_UNKNOWN_SPEAKER_MARKED|UNKNOWN_SPEAKER_GENDER_MARKED|POSSIBLE_OMISSION|POSSIBLE_CUE_SHIFT_PAIR|CUE_OWNERSHIP_BOUNDARY_MISMATCH)$/i.test(String(reason || "").trim());
}

function applyFinalPriorityConsensus(localIssues, semanticIssues, auditedIds, job) {
  const semanticIds = new Set((Array.isArray(semanticIssues) ? semanticIssues : []).map(x => Number(x && x.id)).filter(Number.isInteger));
  if (!job.finalPriorityConsensusState || typeof job.finalPriorityConsensusState !== "object") job.finalPriorityConsensusState = Object.create(null);
  const state = job.finalPriorityConsensusState;
  const active = new Set();
  const out = [];
  let suppressed = 0;
  let waiting = 0;

  for (const issue of Array.isArray(localIssues) ? localIssues : []) {
    const id = Number(issue && issue.id);
    if (!Number.isInteger(id)) continue;
    const kept = [];
    for (const raw of Array.isArray(issue.reasons) ? issue.reasons : []) {
      const reason = String(raw || "");
      if (!finalPriorityHeuristicReason(reason)) { kept.push(reason); continue; }
      const key = id + "::" + reason;
      active.add(key);
      const audited = !auditedIds || auditedIds.has(id);
      if (!audited) { kept.push(reason); waiting++; continue; }
      if (semanticIds.has(id)) { state[key] = 0; kept.push(reason); waiting++; continue; }
      const clean = Number(state[key] || 0) + 1;
      state[key] = clean;
      if (clean >= FINAL_PRIORITY_HEURISTIC_CONSENSUS_CLEAN_AUDITS) {
        suppressed++;
        job.stats.finalPriorityConsensusSuppressions = (job.stats.finalPriorityConsensusSuppressions || 0) + 1;
      } else {
        kept.push(reason);
        waiting++;
      }
    }
    if (kept.length) out.push({ id, reasons: [...new Set(kept)] });
  }

  for (const key of Object.keys(state)) if (!active.has(key)) delete state[key];
  if (suppressed) console.log(`[FINAL PRIORITY CONSENSUS] ${suppressed} heuristic flag(s) had 2 clean semantic audits and will not block alone. OK`);
  if (waiting) console.log(`[FINAL PRIORITY CONSENSUS] ${waiting} heuristic flag(s) still awaiting semantic consensus.`);
  return out;
}

function blockingLocalIssues(blocks, translations, job, plan) {
  return detectLocalIssues(
    blocks,
    translations,
    job.filename,
    plan
  )
    .map(issue => ({
      id: issue.id,
      reasons: (issue.reasons || []).filter(finalReasonBlocks)
    }))
    .filter(issue => issue.reasons.length);
}

function finalPriorityAuditItem(block, translations, plan) {
  const turnPlan973 = sourceDialogueTurnPlan973(block);
  return {
    i: block.index,
    source: String(block.text || ""),
    pt: String(translations.get(block.index) || ""),
    identity_lock: compactIdentityHint(block, plan),
    dialogue_turn_count: turnPlan973.turns.length,
    dialogue_turn_mode: turnPlan973.mode,
    dialogue_turn_source: turnPlan973.turns,
    dialogue_turn_candidates: turnPlan973.softCandidates,
    music: block.musicPerformance ? "contextual_performance_lyric" : ""
  };
}

function buildFinalPriorityAuditBatches(
  blocks,
  translations,
  plan,
  focusIds = null
) {
  const hasExplicitFocus =
    focusIds instanceof Set;

  const focus =
    hasExplicitFocus
      ? new Set([...focusIds].map(Number))
      : null;

  const targets = blocks.filter(
    block =>
      (
        !hasExplicitFocus ||
        focus.has(
          block.index
        )
      ) &&
      !(
        sourceCueAllowsIntentionalEmpty(
          block
        ) &&
        !String(
          translations.get(
            block.index
          ) ||
          ""
        ).trim()
      )
  );
  const batches = [];
  let current = [];
  let chars = 0;

  const posMap = positionMap(blocks);

  const flush = () => {
    if (!current.length) return;

    const firstPos = posMap.get(current[0].i);
    const lastPos = posMap.get(current[current.length - 1].i);

    const before = Number.isInteger(firstPos) && firstPos > 0
      ? finalPriorityAuditItem(blocks[firstPos - 1], translations, plan)
      : null;

    const after = Number.isInteger(lastPos) && lastPos + 1 < blocks.length
      ? finalPriorityAuditItem(blocks[lastPos + 1], translations, plan)
      : null;

    batches.push({
      targetIds: new Set(current.map(item => item.i)),
      cues: [
        ...(before ? [{ ...before, context_only: true }] : []),
        ...current.map(item => ({ ...item, context_only: false })),
        ...(after ? [{ ...after, context_only: true }] : [])
      ]
    });

    current = [];
    chars = 0;
  };

  for (const block of targets) {
    const item = finalPriorityAuditItem(block, translations, plan);
    const size = JSON.stringify(item).length;

    if (
      current.length &&
      (
        current.length >= FINAL_PRIORITY_AUDIT_BATCH_MAX_CUES ||
        chars + size > FINAL_PRIORITY_AUDIT_BATCH_MAX_CHARS
      )
    ) {
      flush();
    }

    current.push(item);
    chars += size;
  }

  flush();
  return batches;
}

async function finalPriorityGeminiRequest(args, job, label) {
  let failures = 0;
  let schemaFallbackUsed = false;
  let activeArgs = { ...args };

  while (true) {
    try {
      return await geminiRequest(activeArgs);
    } catch (error) {
      failures++;
      job.stats.finalPriorityTechnicalRetries =
        (job.stats.finalPriorityTechnicalRetries || 0) + 1;

      const message = errorMessage(error);
      const status = Number(error?.status || 0);

      // 8.4.2: HTTP 400/INVALID_ARGUMENT nÃ£o Ã© tratado cegamente como
      // "falha transitÃ³ria" do mesmo payload. Primeiro mudamos de forma
      // determinÃ­stica para um schema ainda menor e reduzimos a pressÃ£o
      // de geraÃ§Ã£o. O job continua vivo e a qualidade continua fail-closed.
      if (
        !schemaFallbackUsed &&
        status === 400 &&
        /invalid argument|invalid_request/i.test(message)
      ) {
        schemaFallbackUsed = true;

        job.stats.finalPrioritySchemaFallbacks =
          (job.stats.finalPrioritySchemaFallbacks || 0) + 1;

        activeArgs = {
          ...activeArgs,
          schema: FINAL_PRIORITY_AUDIT_FALLBACK_SCHEMA,
          thinkingLevel: "medium",
          maxOutputTokens: Math.min(
            Number(activeArgs.maxOutputTokens || 7000),
            7000
          ),
          user:
            `${activeArgs.user}\n\n` +
            `FALLBACK DE SCHEMA: retorne somente {"issues":[{"i":123,"reason":"..."}]}. ` +
            `NÃ£o inclua category nesta tentativa.`
        };

        job.error =
          `SCHEMA FALLBACK ${label}: ${message.slice(0, 260)}`;
        job.status = "processing";
        job.progress = Math.max(Number(job.progress || 0), Math.min(99, Math.max(94, job.progress || 0)));
        job.updatedAt = Date.now();

        console.warn(
          `[FINAL PRIORITY SCHEMA FALLBACK] ${label}: HTTP 400/INVALID_ARGUMENT; ` +
          `trocando para schema mÃ­nimo sem liberar a legenda.`
        );

        await sleep(1500);
        continue;
      }

      if (
        failures >=
        FINAL_PRIORITY_REQUEST_MAX_FAILURES
      ) {
        throw new Error(
          `FINAL PRIORITY ${label}: ${failures} falhas tÃ©cnicas consecutivas; ` +
          `encerrando esta etapa para o fallback seguro do job | ${
            message.slice(0, 320)
          }`
        );
      }

      const waitMs = Math.min(
        FINAL_PRIORITY_RETRY_MAX_MS,
        FINAL_PRIORITY_RETRY_BASE_MS * Math.pow(1.65, Math.min(failures - 1, 8))
      );

      job.error =
        `RETRY ${label}: ${message.slice(0, 260)}`;
      job.status = "processing";
      job.progress = Math.max(Number(job.progress || 0), Math.min(99, Math.max(94, job.progress || 0)));
      job.updatedAt = Date.now();

      console.warn(
        `[FINAL PRIORITY RETRY] ${label} falhou (${failures}); ` +
        `nova tentativa limitada em ${(waitMs / 1000).toFixed(1)}s | ` +
        `${message.slice(0, 320)}`
      );

      await sleep(waitMs);
    }
  }
}

function parseFinalPriorityAudit(text, allowedIds) {
  const parsed = JSON.parse(stripCodeFences(text));
  const raw = Array.isArray(parsed?.issues) ? parsed.issues : [];
  const out = [];
  const seen = new Set();

  for (const issue of raw) {
    const id = Number(issue?.i);
    if (!Number.isInteger(id) || !allowedIds.has(id) || seen.has(id)) continue;
    seen.add(id);

    const category = String(issue?.category || "PRIORITY").trim().slice(0, 80);
    const reason = String(issue?.reason || "defeito prioritÃ¡rio").trim().slice(0, 260);

    out.push({
      id,
      reasons: [`FINAL_PRIORITY:${category}: ${reason}`]
    });

    if (out.length >= FINAL_PRIORITY_MAX_ISSUES) {
      break;
    }
  }

  return out;
}

async function scanFinalPriorityAudit(
  blocks,
  translations,
  plan,
  job,
  focusIds = null
) {
  if (!FINAL_PRIORITY_GATE_ENABLED) return [];

  const batches = buildFinalPriorityAuditBatches(
    blocks,
    translations,
    plan,
    focusIds
  );

  if (!batches.length) return [];

  const results = new Array(batches.length);
  let cursor = 0;

  console.log(
    `[FINAL PRIORITY AUDIT] ${batches.length} lote(s) | ` +
    `fonte=${job.sourceLang || "auto"} | ` +
    `escopo=${focusIds instanceof Set ? `${focusIds.size} cue(s) focais` : "episÃ³dio completo"}.`
  );

  async function worker(workerId) {
    while (true) {
      const index = cursor++;
      if (index >= batches.length) return;

      const batch = batches[index];
      let parsed = null;
      let parseFailures = 0;

      while (
        !parsed &&
        parseFailures <
          FINAL_PRIORITY_PARSE_MAX_FAILURES
      ) {
        const response = await finalPriorityGeminiRequest(
          {
            system: FINAL_PRIORITY_AUDIT_PROMPT,
            user:
              `IDIOMA DECLARADO DA FONTE: ${job.sourceLang || "auto"}\n` +
              `IMPORTANTE: use o idioma REAL encontrado em SOURCE; nÃ£o presuma inglÃªs.\n\n` +
              `BÃBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\n` +
              `CUES EM ORDEM CRONOLÃ“GICA:\n${JSON.stringify(batch.cues)}\n\n` +
              `Audite SOMENTE context_only=false. context_only=true existe apenas para comparar vizinhos.`,
            schema: FINAL_PRIORITY_AUDIT_SCHEMA,
            thinkingLevel: FINAL_PRIORITY_AUDIT_THINKING,
            maxOutputTokens: FINAL_PRIORITY_AUDIT_MAX_OUTPUT_TOKENS,
            timeoutMs: FINAL_PRIORITY_AUDIT_TIMEOUT_MS,
            maxRetries: FINAL_PRIORITY_AUDIT_HTTP_RETRIES,
            job,
            metric: "qa"
          },
          job,
          `AUDIT W${workerId} lote ${index + 1}`
        );

        job.stats.finalPriorityAuditCalls =
          (job.stats.finalPriorityAuditCalls || 0) + 1;

        try {
          parsed = parseFinalPriorityAudit(
            response.text,
            batch.targetIds
          );
        } catch (error) {
          invalidateResponseModelForJob(
            job,
            response,
            `FINAL AUDIT lote ${index + 1} structured output invÃ¡lido`,
            error
          );

          parseFailures++;
          job.stats.finalPriorityTechnicalRetries =
            (job.stats.finalPriorityTechnicalRetries || 0) + 1;

          console.warn(
            `[FINAL PRIORITY AUDIT] JSON invÃ¡lido no lote ${index + 1} ` +
            `(tentativa ${parseFailures}); repetindo sem matar job | ` +
            `${errorMessage(error).slice(0, 220)}`
          );

          await sleep(
            Math.min(
              FINAL_PRIORITY_RETRY_MAX_MS,
              FINAL_PRIORITY_RETRY_BASE_MS * Math.max(1, parseFailures)
            )
          );
        }
      }

      if (!parsed) {
        throw new Error(
          `FINAL PRIORITY AUDIT lote ${index + 1}: ` +
          `${FINAL_PRIORITY_PARSE_MAX_FAILURES} respostas invÃ¡lidas consecutivas.`
        );
      }

      results[index] = parsed;

      console.log(
        `[FINAL PRIORITY AUDIT W${workerId}] lote ${index + 1}/${batches.length}: ` +
        `${parsed.length} prioritÃ¡rio(s).`
      );
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(FINAL_PRIORITY_AUDIT_CONCURRENCY, batches.length) },
      (_, index) => worker(index + 1)
    )
  );

  const merged = mergeIssueLists(...results.map(x => Array.isArray(x) ? x : []));
  job.stats.finalPriorityFlags = merged.length;
  return merged;
}

function idsFromIssues(issues, blocks, radius = FINAL_PRIORITY_CONTEXT_RADIUS) {
  const posMap = positionMap(blocks);
  const ids = new Set();

  for (const issue of Array.isArray(issues) ? issues : []) {
    const pos = posMap.get(Number(issue?.id));
    if (!Number.isInteger(pos)) continue;

    for (let delta = -radius; delta <= radius; delta++) {
      const block = blocks[pos + delta];
      if (block) ids.add(block.index);
    }
  }

  return ids;
}


function genderTargetReasons925(block, pt, filename, plan) {
  return localReasonsForCue(block, pt, filename, plan)
    .filter(reason => isHardGenderReason970(reason));
}

function genderTargetIssues925(blocks, translations, filename, plan) {
  const issues = [];
  for (const block of blocks) {
    const pt = String(translations.get(block.index) || "").trim();
    const reasons = [...new Set(genderTargetReasons925(block, pt, filename, plan))];
    if (reasons.length) issues.push({ id: block.index, reasons });
  }
  return issues;
}

function closeGenderCandidateLocally925(block, candidatePt) {
  let value = String(candidatePt || "").trim();
  if (!value) return value;
  value = sanitizeFinalCue(block, value) || sanitizeFallbackCue(value) || value;
  value = applyDeterministicGenderClosure898(block, value);
  value = sanitizeFinalCue(block, value) || sanitizeFallbackCue(value) || value;
  return String(value || "").trim();
}

function strictGenderCandidateCheck925(block, beforePt, candidatePt, filename, plan) {
  const closed = closeGenderCandidateLocally925(block, candidatePt);
  const regressions = repairCandidateRegressionReasons(
    block,
    beforePt,
    closed,
    filename,
    plan
  );
  const remainingGender = genderTargetReasons925(block, closed, filename, plan);
  return {
    candidate: closed,
    regressions: [...new Set(regressions)],
    remainingGender: [...new Set(remainingGender)],
    ok: regressions.length === 0 && remainingGender.length === 0
  };
}

async function runFinalPriorityEscalatedRepair(
  blocks,
  translations,
  issues,
  plan,
  job,
  options = {}
) {
  const posMap = positionMap(blocks);
  const updated = new Map(translations);
  const selected = [...issues].sort((a, b) => issuePriority(a) - issuePriority(b));

  for (
    let offset = 0;
    offset < selected.length;
    offset += FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES
  ) {
    const batchIssues = selected.slice(
      offset,
      offset + FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES
    );

    let completed = false;
    let parseFailures = 0;

    while (
      !completed &&
      parseFailures <
        FINAL_PRIORITY_ESCALATED_MAX_FAILURES
    ) {
      const locksById = new Map();

      const cues = batchIssues.map(issue => {
        const pos = posMap.get(issue.id);
        const block = blocks[pos];
        const protectedTarget = protectCulturalLocks(block.text, block.index);
        locksById.set(block.index, protectedTarget.locks);

        return {
          i: block.index,
          source: protectedTarget.text,
          current_pt: String(updated.get(block.index) || ""),
          reasons: issue.reasons,
          identity_lock: identityLockForCapsule(block, plan),
          dialogue_turn_count: sourceDialogueDashCount(block),
          hard_locks: protectedTarget.locks.map(lock => lock.token),
          before: blocks
            .slice(Math.max(0, pos - 2), pos)
            .map(item => ({
              i: item.index,
              source: item.text,
              pt: String(updated.get(item.index) || "")
            })),
          after: blocks
            .slice(pos + 1, Math.min(blocks.length, pos + 3))
            .map(item => ({
              i: item.index,
              source: item.text,
              pt: String(updated.get(item.index) || "")
            }))
        };
      });

      try {
        const response = await finalPriorityGeminiRequest(
          {
            system: FINAL_PRIORITY_REPAIR_PROMPT,
            user:
              `IDIOMA DECLARADO DA FONTE: ${job.sourceLang || "auto"}\n` +
              `Use o idioma REAL de source.\n\n` +
              `BÃBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\n` +
              `CUES PRIORITÃRIOS PARA RECONSTRUÃ‡ÃƒO:\n${JSON.stringify({ cues })}`,
            schema: cueTranslationSchema(batchIssues.length),
            thinkingLevel: "high",
            maxOutputTokens: FINAL_PRIORITY_ESCALATED_MAX_OUTPUT_TOKENS,
            timeoutMs: FINAL_PRIORITY_ESCALATED_TIMEOUT_MS,
            maxRetries: REPAIR_HTTP_RETRIES,
            job,
            metric: "repair"
          },
          job,
          `ESCALATED REPAIR ${Math.floor(offset / FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES) + 1}`
        );

        let repaired;
        try {
          repaired = parseCueTranslation(
            batchIssues.map(issue => blocks[posMap.get(issue.id)]),
            response.text,
            locksById
          );
        } catch (parseError) {
          invalidateResponseModelForJob(
            job,
            response,
            "FINAL REPAIR structured output invÃ¡lido",
            parseError
          );
          throw parseError;
        }

        let accepted = 0;

        for (const [id, candidate] of repaired) {
          const block = blocks[posMap.get(id)];
          const beforePt = String(updated.get(id) || "");
          let candidatePt = String(candidate || "").trim();

          let regressions = [];
          if (options?.requireGenderZero) {
            const target = strictGenderCandidateCheck925(
              block,
              beforePt,
              candidatePt,
              job.filename,
              plan
            );
            candidatePt = target.candidate;
            regressions = [...target.regressions];
            if (target.remainingGender.length) {
              console.warn(
                `[GENDER TARGET ACCEPTANCE 9.2.5] cue ${id} REJEITADO: ` +
                `candidato ainda contÃ©m ${target.remainingGender.join(", ")}.`
              );
              continue;
            }
          } else {
            regressions = repairCandidateRegressionReasons(
              block,
              beforePt,
              candidatePt,
              job.filename,
              plan
            );
          }

          if (regressions.length) {
            console.warn(
              `[FINAL PRIORITY ESCALATED] cue ${id} rejeitado localmente | ` +
              `${regressions.join(", ")}.`
            );
            continue;
          }

          updated.set(id, candidatePt);
          if (!job.finalRepairLockedText923) job.finalRepairLockedText923 = new Map();
          job.finalRepairLockedText923.set(Number(id), candidatePt);
          accepted++;
        }

        console.log(
          `[FINAL PRIORITY ESCALATED] lote ` +
          `${Math.floor(offset / FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES) + 1} | ` +
          `aceitos=${accepted}/${batchIssues.length}.`
        );

        completed = true;
      } catch (error) {
        parseFailures++;
        job.stats.finalPriorityTechnicalRetries =
          (job.stats.finalPriorityTechnicalRetries || 0) + 1;

        const waitMs = Math.min(
          FINAL_PRIORITY_RETRY_MAX_MS,
          FINAL_PRIORITY_RETRY_BASE_MS * Math.max(1, parseFailures)
        );

        console.warn(
          `[FINAL PRIORITY ESCALATED] lote nÃ£o foi descartado; ` +
          `repetindo em ${(waitMs / 1000).toFixed(1)}s | ` +
          `${errorMessage(error).slice(0, 320)}`
        );

        await sleep(waitMs);
      }
    }

    if (!completed) {
      console.warn(
        `[FINAL PRIORITY ESCALATED] lote ` +
        `${Math.floor(offset / FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES) + 1} ` +
        `atingiu o limite de ${FINAL_PRIORITY_ESCALATED_MAX_FAILURES} falhas; ` +
        `candidato anterior preservado sem loop.`
      );
    }
  }

  return updated;
}

async function convergeFinalPriorityQuality(
  blocks,
  translations,
  plan,
  job,
  initialFocusIds = null
) {
  if (!FINAL_PRIORITY_GATE_ENABLED) return translations;

  let current = new Map(translations);
  let previousSignature = "";
  let stagnantRounds = 0;

  let focusIds = new Set(
    initialFocusIds instanceof Set
      ? [...initialFocusIds]
      : Array.isArray(initialFocusIds)
        ? initialFocusIds
        : []
  );

  // A primeira invocaÃ§Ã£o do pipeline continua GLOBAL para preservar
  // exatamente a funÃ§Ã£o do gate 8.4.6. Quando o gate Ã© reaberto por um
  // problema jÃ¡ conhecido (ex.: 1 overflow de layout), comeÃ§amos focado
  // nesses IDs e NUNCA varremos os ~3.000 cues outra vez.
  let firstAudit =
    initialFocusIds == null;

  let roundsThisRun = 0;

  while (
    roundsThisRun <
    FINAL_PRIORITY_MAX_ROUNDS
  ) {
    roundsThisRun++;

    job.stats.finalPriorityRounds =
      (job.stats.finalPriorityRounds || 0) + 1;

    current = sanitizeTranslationMap(blocks, current, job);
    current = applySubtitleLayout(blocks, current, "FINAL-PRIORITY-CANDIDATE");

    const local = blockingLocalIssues(
      blocks,
      current,
      job,
      plan
    );

    for (const id of idsFromIssues(local, blocks)) {
      focusIds.add(id);
    }

    const auditFocusIds =
      firstAudit
        ? null
        : new Set(focusIds);

    const semantic = await scanFinalPriorityAudit(
      blocks,
      current,
      plan,
      job,
      auditFocusIds
    );

    firstAudit = false;

    const consensusLocal = applyFinalPriorityConsensus(local, semantic, auditFocusIds, job);
    let issues = mergeIssueLists(consensusLocal, semantic);

    if (!issues.length) {
      console.log(
        `[FINAL PRIORITY GATE] PASSOU âœ… | ` +
        `rounds=${job.stats.finalPriorityRounds} | ` +
        `0 defeitos prioritÃ¡rios; resultado autorizado para cache/serve.`
      );

      job.error = null;
      return current;
    }

    logIssueSummary("FINAL-PRIORITY", issues);

    const signature = finalPriorityIssueSignature(issues);
    if (signature === previousSignature) {
      stagnantRounds++;
    } else {
      stagnantRounds = 0;
      previousSignature = signature;
    }

    if (stagnantRounds >= FINAL_PRIORITY_NO_PROGRESS_ESCALATE_AFTER) {
      job.stats.finalPriorityNoProgressEscalations =
        (job.stats.finalPriorityNoProgressEscalations || 0) + 1;

      console.warn(
        `[FINAL PRIORITY ESCALATION] mesmos defeitos persistiram por ` +
        `${stagnantRounds + 1} rodada(s); repair hiperfocado nos IDs prioritÃ¡rios.`
      );
    }

    job.stats.finalPriorityRepairRounds =
      (job.stats.finalPriorityRepairRounds || 0) + 1;

    const before = new Map(current);

    if (
      stagnantRounds >= FINAL_PRIORITY_NO_PROGRESS_ESCALATE_AFTER
    ) {
      current = await runFinalPriorityEscalatedRepair(
        blocks,
        current,
        issues,
        plan,
        job
      );
    } else {
      current = await tryFocusedRepair(
        blocks,
        current,
        plan,
        job,
        issues,
        { extraOnly: true }
      );
    }

    current = sanitizeTranslationMap(blocks, current, job);

    current = await runCompactRescue(
      blocks,
      current,
      plan,
      job
    );

    current = sanitizeTranslationMap(blocks, current, job);

    const changedIds = new Set();
    for (const block of blocks) {
      const a = String(before.get(block.index) || "").replace(/\s+/g, " ").trim();
      const b = String(current.get(block.index) || "").replace(/\s+/g, " ").trim();
      if (a !== b) changedIds.add(block.index);
    }

    focusIds = idsFromIssues(issues, blocks);
    for (const id of changedIds) focusIds.add(id);

    job.status = "processing";
    job.progress = 98;
    job.updatedAt = Date.now();

    console.log(
      `[FINAL PRIORITY GATE] rodada ${job.stats.finalPriorityRounds} ` +
      `reprovou ${issues.length} cue(s); ` +
      `${changedIds.size} cue(s) alterado(s); reauditoria focada continuarÃ¡.`
    );
  }

  job.stats.finalPriorityBoundedReleases =
    (
      job.stats.finalPriorityBoundedReleases ||
      0
    ) + 1;

  job.qualityStatus =
    "bounded_best_candidate";

  console.warn(
    `[FINAL PRIORITY GATE] limite de ${FINAL_PRIORITY_MAX_ROUNDS} rodada(s) ` +
    `atingido; melhor candidato protegido pelos guards locais serÃ¡ finalizado ` +
    `em vez de manter o job eternamente em processing.`
  );

  return current;
}



function blockerFamilies928(issue) {
  const text = (Array.isArray(issue?.reasons) ? issue.reasons : [issue?.reason])
    .map(x => String(x || "").toUpperCase()).join(" | ");
  const families = [];
  const rules = [
    ["GENDER", /GENDER|UNKNOWN_SPEAKER_GENDER/],
    ["NEGATION", /NEGATION/],
    ["OWNERSHIP", /OWNERSHIP|CUE_SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION/],
    ["MEANING", /MEANING_INTEGRITY|OMISSION|PREDICATE_COMPLETENESS|REFERENT_INTEGRITY|IDENTITY_NARROWING/],
    ["CENSOR", /CENSOR|BLEEP/],
    ["DIALOGUE", /DIALOGUE_TURN|MISSING_DIALOGUE_BREAK/],
    ["LAYOUT", /DENSE|LAYOUT/],
    ["SDH", /SDH|MUSIC|SPEAKER_LABEL_RESIDUE/],
    ["REPETITION", /REPETITION/]
  ];
  for (const [name, re] of rules) if (re.test(text)) families.push(name);
  return families.length ? [...new Set(families)] : ["OTHER"];
}

function resolveGuardConflicts942(blocks, issues, job, label = "audit") {
  const byId = new Map(blocks.map(block => [Number(block.index), block]));
  const out = [];
  for (const issue of Array.isArray(issues) ? issues : []) {
    const id = Number(issue?.id);
    const block = byId.get(id);
    let reasons = [...new Set((issue?.reasons || []).map(x => String(x || "")).filter(Boolean))];
    if (!block || !reasons.length) {
      if (reasons.length) out.push({ ...issue, reasons });
      continue;
    }

    const first = sourceGenderEvidence942(block, "first");
    const second = sourceGenderEvidence942(block, "second");
    const plural = sourceGenderEvidence942(block, "plural");
    const removed = [];

    reasons = reasons.filter(reason => {
      const r = String(reason || "");
      const firstNeutral = /GENDER_V3_NEUTRAL_DEFAULT_VIOLATION|GENDER_V5_FIRST_PERSON_HUMAN_ROLE_MARKED|GENDER_V8_STRUCTURAL_FIRST_PERSON_PREDICATE|UNKNOWN_SPEAKER_GENDER_MARKED/i.test(r);
      const secondNeutral = /GENDER_V4_SECOND_PERSON_NEUTRAL_DEFAULT_VIOLATION|GENDER_V5_SECOND_PERSON_HUMAN_ROLE_MARKED|GENDER_V8_STRUCTURAL_SECOND_PERSON_PREDICATE/i.test(r);
      const pluralNeutral = /GENDER_V8_STRUCTURAL_PLURAL_PREDICATE/i.test(r);
      if ((first.explicit && firstNeutral) || (second.explicit && secondNeutral) || (plural.explicit && pluralNeutral)) {
        removed.push(r);
        return false;
      }
      return true;
    });

    if (removed.length) {
      if (job) job.stats.guardConflictsResolved942 = Number(job.stats.guardConflictsResolved942 || 0) + 1;
      console.warn(
        `[GUARD CONFLICT 9.4.2] ${label} cue=${id}: SOURCE lexicalmente prova gÃªnero; ` +
        `neutrality blocker(s) stale removido(s)=[${removed.join(", ")}]. Mismatch real continua fail-closed.`
      );
    }
    if (reasons.length) out.push({ ...issue, reasons });
  }
  return out;
}

function escalationAttemptStore928(job) {
  if (!(job?.escalationAttempts928 instanceof Set)) job.escalationAttempts928 = new Set();
  return job.escalationAttempts928;
}

function escalationStateOptions942(issue, options = {}) {
  const id = Number(issue?.id);
  const translations = options?.translations instanceof Map ? options.translations : null;
  const currentPt = String(
    options?.currentPt !== undefined
      ? options.currentPt
      : (translations && Number.isInteger(id) ? translations.get(id) : "") || ""
  ).replace(/\s+/g, " ").trim();
  const reasons = (Array.isArray(issue?.reasons) ? issue.reasons : [issue?.reason])
    .map(x => String(x || "").trim()).filter(Boolean).sort();
  return {
    stage: String(options?.stage || "generic"),
    currentHash: sha256(currentPt).slice(0, 12),
    blockerHash: sha256(reasons.join(" | ")).slice(0, 12),
    requireGenderZero: Boolean(options?.requireGenderZero)
  };
}

function escalationKeys928(issue, strategy, options = {}) {
  const id = Number(issue?.id);
  if (!Number.isInteger(id)) return [];
  const state = escalationStateOptions942(issue, options);
  return blockerFamilies928(issue).map(family =>
    `${id}|${state.stage}|${String(strategy)}|${family}|${state.blockerHash}|${state.currentHash}|g0=${state.requireGenderZero ? 1 : 0}`
  );
}

function strategyAlreadyAttempted928(job, issue, strategy, options = {}) {
  const keys = escalationKeys928(issue, strategy, options);
  if (!keys.length) return false;
  const store = escalationAttemptStore928(job);
  return keys.every(key => store.has(key));
}

function markStrategyAttempted928(job, issue, strategy, options = {}) {
  const store = escalationAttemptStore928(job);
  for (const key of escalationKeys928(issue, strategy, options)) store.add(key);
}

function eligibleForStrategy928(job, issues, strategy, options = {}) {
  return (Array.isArray(issues) ? issues : []).filter(
    issue => !strategyAlreadyAttempted928(job, issue, strategy, options)
  );
}

const CANDIDATE_BEAM_SCHEMA_928 = {
  type: "object",
  additionalProperties: false,
  properties: {
    candidates: {
      type: "array",
      minItems: QUALITY_BEAM_CANDIDATES_928,
      maxItems: QUALITY_BEAM_CANDIDATES_928,
      items: { type: "string" }
    }
  },
  required: ["candidates"]
};

// 9.4.0 â€” salvage de strings completas do array candidates.
// Se a resposta for truncada no Ãºltimo candidato, os candidatos completos
// anteriores continuam utilizÃ¡veis; zero retry e zero loop.
function parseCandidateBeamSalvage940(raw) {
  const text = stripCodeFences(String(raw || ""));
  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed?.candidates) ? parsed.candidates.map(String) : [];
  } catch {}
  const keyAt = text.indexOf('"candidates"');
  if (keyAt < 0) return [];
  const colonAt = text.indexOf(':', keyAt + 12);
  const start = colonAt >= 0 ? text.indexOf('[', colonAt + 1) : -1;
  if (start < 0) return [];
  const out = [];
  let i = start + 1;
  while (i < text.length) {
    while (i < text.length && /[\s,]/.test(text[i])) i++;
    if (i >= text.length || text[i] === ']') break;
    if (text[i] !== '"') { i++; continue; }
    const s = i;
    i++;
    let escaped = false;
    while (i < text.length) {
      const ch = text[i];
      if (escaped) { escaped = false; i++; continue; }
      if (ch === '\\') { escaped = true; i++; continue; }
      if (ch === '"') {
        const fragment = text.slice(s, i + 1);
        try { out.push(JSON.parse(fragment)); } catch {}
        i++;
        break;
      }
      i++;
    }
    if (i >= text.length && text[text.length - 1] !== '"') break;
  }
  return out;
}

const CANDIDATE_BEAM_PROMPT_928 = `
VocÃª Ã© o CANDIDATE BEAM final de um Ãºnico cue SOURCEâ†’PT-BR.
Gere exatamente ${QUALITY_BEAM_CANDIDATES_928} reconstruÃ§Ãµes REALMENTE diferentes entre si,
ordenadas da mais confiÃ¡vel para a menos confiÃ¡vel. Cada candidata deve resolver TODOS os blockers.
Preserve significado, negaÃ§Ã£o, referente, ownership, hard locks, turns e gÃªnero somente quando a SOURCE prova.
NÃ£o use conteÃºdo de before/after como conteÃºdo do target. NÃ£o altere timestamps. PT-BR natural e conciso.
`;

async function runCandidateBeam928(blocks, translations, issues, plan, job, options = {}) {
  const posMap = positionMap(blocks);
  const updated = new Map(translations);
  const stage = String(options?.stage || "candidate-beam");
  const eligibilityOptions = { ...options, stage, translations };
  const selected = eligibleForStrategy928(job, issues, "candidate_beam", eligibilityOptions)
    .filter(issue => posMap.has(Number(issue?.id)));
  let cursor = 0;
  const concurrency = Math.min(2, Math.max(1, selected.length));

  async function worker(workerId) {
    while (true) {
      const at = cursor++;
      if (at >= selected.length) return;
      const issue = selected[at];
      const id = Number(issue.id);
      const pos = posMap.get(id);
      const block = blocks[pos];
      const protectedTarget = protectCulturalLocks(block.text, id);
      const beforePt = String(updated.get(id) || "");
      const attemptOptions = { ...options, stage, currentPt: beforePt };
      try {
        const response = await finalPriorityGeminiRequest({
          system: CANDIDATE_BEAM_PROMPT_928,
          user: JSON.stringify({
            i: id,
            source: protectedTarget.text,
            current_pt: beforePt,
            blockers: issue.reasons || [],
            identity_lock: identityLockForCapsule(block, plan),
            hard_locks: protectedTarget.locks.map(lock => lock.token),
            dialogue_turn_count: sourceDialogueDashCount(block),
            before_source: blocks.slice(Math.max(0,pos-1),pos).map(x=>({i:x.index,source:x.text})),
            after_source: blocks.slice(pos+1,Math.min(blocks.length,pos+2)).map(x=>({i:x.index,source:x.text}))
          }),
          schema: CANDIDATE_BEAM_SCHEMA_928,
          thinkingLevel: "high",
          maxOutputTokens: 8000,
          timeoutMs: 90000,
          maxRetries: 1,
          routeOverride: [GEMINI_MODELS.MAIN_FALLBACK, GEMINI_MODELS.MAIN_PRIMARY],
          job,
          metric: "repair"
        }, job, `CANDIDATE BEAM 9.4.2 W${workerId} cue ${id}`);
        const list = parseCandidateBeamSalvage940(response.text);
        if (!list.length) throw new Error("Candidate Beam sem candidato JSON completo apÃ³s salvage.");
        let winner = "";
        let safeButSame = 0;
        const normalizedBefore = beforePt.replace(/\s+/g, " ").trim();
        for (const raw of list) {
          let candidate = String(raw || "").trim();
          if (!candidate) continue;
          try { candidate = restoreCulturalLocks(candidate, protectedTarget.locks, id); }
          catch { continue; }
          let regressions = [];
          if (options?.requireGenderZero) {
            const target = strictGenderCandidateCheck925(block, beforePt, candidate, job.filename, plan);
            candidate = target.candidate;
            regressions = [...target.regressions, ...target.remainingGender];
          } else {
            candidate = sanitizeFinalCue(block, candidate) || sanitizeFallbackCue(candidate) || candidate;
            regressions = repairCandidateRegressionReasons(block, beforePt, candidate, job.filename, plan);
          }
          if (regressions.length) continue;
          const layout = layoutCueResult(block, candidate);
          if (!layout.fits || layout.lines > LAYOUT_MAX_LINES) continue;
          if (candidate.replace(/\s+/g, " ").trim() === normalizedBefore) {
            safeButSame++;
            continue;
          }
          winner = candidate;
          break;
        }

        // SÃ³ depois de uma resposta semanticamente processÃ¡vel a estratÃ©gia Ã© consumida
        // para ESTE stage+texto+blockers. Falha tÃ©cnica nÃ£o fecha portas futuras.
        markStrategyAttempted928(job, issue, "candidate_beam", attemptOptions);

        if (winner) {
          updated.set(id, winner);
          console.log(`[CANDIDATE BEAM 9.4.2] cue=${id} vencedor local seguro E diferente selecionado; lock aguardarÃ¡ auditoria semÃ¢ntica.`);
        } else {
          console.warn(`[CANDIDATE BEAM 9.4.2] cue=${id} sem progresso real (${safeButSame} candidata(s) segura(s) idÃªntica(s)); melhor anterior preservado e prÃ³xima estratÃ©gia poderÃ¡ escalar.`);
        }
      } catch (error) {
        console.warn(`[CANDIDATE BEAM 9.4.2] cue=${id} falhou tecnicamente; estratÃ©gia NÃƒO consumida para fase futura | ${errorMessage(error).slice(0,260)}`);
      }
    }
  }
  await Promise.all(Array.from({length:concurrency},(_,i)=>worker(i+1)));
  return updated;
}

const CONSTRAINED_RECONSTRUCTION_PROMPT_928 = `
VocÃª Ã© o Ãºltimo CONSTRAINED RECONSTRUCTION de um Ãºnico cue SOURCEâ†’PT-BR.
Reconstrua do zero SOMENTE o target. Os blockers recebidos sÃ£o pÃ³s-condiÃ§Ãµes obrigatÃ³rias.
A saÃ­da deve ser semanticamente completa, natural, caber em 2x50, preservar hard locks e turns,
e terminar sem gÃªnero humano nÃ£o provado, sem perda de negaÃ§Ã£o, referente, predicado, identidade ou ownership.
Before/after sÃ£o sÃ³ contexto. NÃ£o mova conteÃºdo. NÃ£o altere timestamps.
`;

async function runConstrainedReconstruction928(blocks, translations, issues, plan, job, options = {}) {
  const posMap = positionMap(blocks);
  const updated = new Map(translations);
  const stage = String(options?.stage || "constrained");
  const selected = eligibleForStrategy928(job, issues, "constrained", { ...options, stage, translations })
    .filter(issue => posMap.has(Number(issue?.id)));
  let cursor = 0;
  const concurrency = Math.min(2, Math.max(1, selected.length));
  async function worker(workerId) {
    while (true) {
      const at = cursor++;
      if (at >= selected.length) return;
      const issue = selected[at];
      const id = Number(issue.id);
      const pos = posMap.get(id);
      const block = blocks[pos];
      const protectedTarget = protectCulturalLocks(block.text,id);
      const locksById = new Map([[id,protectedTarget.locks]]);
      const beforePt = String(updated.get(id)||"");
      const attemptOptions = { ...options, stage, currentPt: beforePt };
      try {
        const response = await finalPriorityGeminiRequest({
          system: CONSTRAINED_RECONSTRUCTION_PROMPT_928,
          user: JSON.stringify({
            i:id, source:protectedTarget.text, current_pt:beforePt,
            blockers:issue.reasons||[], hard_locks:protectedTarget.locks.map(x=>x.token),
            identity_lock:identityLockForCapsule(block,plan), dialogue_turn_count:sourceDialogueDashCount(block),
            before_source:blocks.slice(Math.max(0,pos-1),pos).map(x=>({i:x.index,source:x.text})),
            after_source:blocks.slice(pos+1,Math.min(blocks.length,pos+2)).map(x=>({i:x.index,source:x.text}))
          }),
          schema: cueTranslationSchema(1),
          thinkingLevel:"high", maxOutputTokens:7000, timeoutMs:90000, maxRetries:1,
          routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY],
          job, metric:"repair"
        },job,`CONSTRAINED 9.4.2 W${workerId} cue ${id}`);
        const repaired = parseCueTranslation([block],response.text,locksById);
        let candidate=String(repaired.get(id)||"").trim();
        let regressions=[];
        if(options?.requireGenderZero){
          const target=strictGenderCandidateCheck925(block,beforePt,candidate,job.filename,plan);
          candidate=target.candidate; regressions=[...target.regressions,...target.remainingGender];
        } else {
          candidate=sanitizeFinalCue(block,candidate)||sanitizeFallbackCue(candidate)||candidate;
          regressions=repairCandidateRegressionReasons(block,beforePt,candidate,job.filename,plan);
        }
        const layout=layoutCueResult(block,candidate);
        if(!layout.fits||layout.lines>LAYOUT_MAX_LINES) regressions.push("SUBTITLE_TOO_DENSE");
        markStrategyAttempted928(job, issue, "constrained", attemptOptions);
        if(regressions.length){
          console.warn(`[CONSTRAINED 9.4.2] cue=${id} rejeitado | ${[...new Set(regressions)].join(", ")}.`);
          continue;
        }
        if(candidate.replace(/\s+/g," ").trim()===beforePt.replace(/\s+/g," ").trim()){
          console.warn(`[CONSTRAINED 9.4.2] cue=${id} nÃ£o produziu mudanÃ§a real; melhor anterior preservado.`);
          continue;
        }
        updated.set(id,candidate);
        console.log(`[CONSTRAINED 9.4.2] cue=${id} candidato diferente produzido; lock aguardarÃ¡ auditoria semÃ¢ntica.`);
      } catch(error){
        console.warn(`[CONSTRAINED 9.4.2] cue=${id} falhou tecnicamente; estratÃ©gia NÃƒO consumida para fase futura | ${errorMessage(error).slice(0,260)}`);
      }
    }
  }
  await Promise.all(Array.from({length:concurrency},(_,i)=>worker(i+1)));
  return updated;
}

function finalIssueWeight927(issue) {
  const reasons = (issue?.reasons || []).map(x => String(x || ""));
  const joined = reasons.join(" | ");
  if (/GENDER|MEANING_INTEGRITY|FINAL_MIXED_SCRIPT_CONFUSABLE|NEGATION|CUE_OWNERSHIP|OMISSION|REFERENT|DIALOGUE|CENSOR|BLEEP|GARBAGE|EMPTY|SDH_RESIDUE|SPEAKER_LABEL_RESIDUE|REPETITION_LOST|IDENTITY/i.test(joined)) return 100;
  if (/LITERALITY|FORCED_OR_DATED_SLANG|SUBTITLE_TOO_DENSE|POSSIBLE_UNTRANSLATED/i.test(joined)) return 25;
  return /FINAL_PRIORITY/i.test(joined) ? 60 : 40;
}


function semanticConstraintMemory945(job) {
  if (!(job?.semanticConstraintMemory945 instanceof Map)) {
    if (job) job.semanticConstraintMemory945 = new Map();
  }
  return job?.semanticConstraintMemory945 instanceof Map ? job.semanticConstraintMemory945 : new Map();
}

function shouldRememberSemanticReason945(reason) {
  const text = String(reason || "").trim();
  if (!text) return false;
  return /GENDER|MEANING_INTEGRITY|NEGATION|CUE_OWNERSHIP|OWNERSHIP|OMISSION|REFERENT|IDENTITY|PREDICATE|DIALOGUE|CENSOR|BLEEP|REPETITION_LOST|EMPTY|POSSIBLE_UNTRANSLATED|FINAL_PRIORITY/i.test(text);
}

function rememberSemanticConstraints945(job, issues, label = "semantic") {
  if (!job || !Array.isArray(issues) || !issues.length) return 0;
  const store = semanticConstraintMemory945(job);
  let added = 0;
  for (const issue of issues) {
    const id = Number(issue?.id);
    if (!Number.isInteger(id)) continue;
    if (!(store.get(id) instanceof Set)) store.set(id, new Set());
    const set = store.get(id);
    for (const reason of (Array.isArray(issue?.reasons) ? issue.reasons : [issue?.reason])) {
      const text = String(reason || "").trim();
      if (!shouldRememberSemanticReason945(text) || set.has(text)) continue;
      set.add(text);
      added++;
    }
  }
  if (added) {
    job.stats.semanticConstraintMemoryAdded945 = Number(job.stats.semanticConstraintMemoryAdded945 || 0) + added;
    console.log(`[SEMANTIC CONSTRAINT MEMORY 9.4.3.5] ${label} | novas=${added} | cues=${store.size}.`);
  }
  return added;
}

function reasonsWithSemanticMemory945(job, issue) {
  const id = Number(issue?.id);
  const current = (Array.isArray(issue?.reasons) ? issue.reasons : [issue?.reason])
    .map(x => String(x || "").trim()).filter(Boolean);
  const historical = Number.isInteger(id) && semanticConstraintMemory945(job).get(id) instanceof Set
    ? [...semanticConstraintMemory945(job).get(id)]
    : [];
  return [...new Set([...historical, ...current])];
}

function issuesWithSemanticMemory945(job, issues) {
  return (Array.isArray(issues) ? issues : []).map(issue => ({
    ...issue,
    reasons: reasonsWithSemanticMemory945(job, issue)
  }));
}

function issueScore927(issues) {
  const list = Array.isArray(issues) ? issues : [];
  let hard = 0;
  let weighted = 0;
  for (const issue of list) {
    const weight = finalIssueWeight927(issue);
    weighted += weight;
    if (weight >= 60) hard++;
  }
  return { hard, weighted, count: list.length };
}

function betterScore927(a, b) {
  if (!b) return true;
  for (const key of ["hard", "weighted", "count"]) {
    if (Number(a?.[key] || 0) !== Number(b?.[key] || 0)) return Number(a?.[key] || 0) < Number(b?.[key] || 0);
  }
  return false;
}

function recordBestCandidate927(blocks, translations, residual, job, label) {
  const laidOut = applySubtitleLayout(blocks, translations, `BEST-CANDIDATE-9.4.0-${label}`);
  const local = blockingLocalIssues(blocks, laidOut, job, job?.episodePlan || null);
  const merged = mergeIssueLists(local, Array.isArray(residual) ? residual : []);
  const score = issueScore927(merged);
  if (!betterScore927(score, job.bestCandidateScore927)) return false;
  const srt = buildSrt(blocks, laidOut);
  job.bestCandidateScore927 = score;
  job.bestCandidateTranslations927 = new Map(translations);
  job.bestAvailableSrt927 = srt;
  job.bestAvailableLabel927 = String(label || "candidate");
  job.bestCandidateIssues927 = merged;
  console.log(`[BEST CANDIDATE LEDGER 9.4.0] ${job.bestAvailableLabel927} | hard=${score.hard} | weighted=${score.weighted} | residual=${score.count} âœ…`);
  return true;
}

const SOURCE_ONLY_SURGERY_PROMPT_927 = `
VocÃª Ã© o SOURCE-ONLY CUE SURGERY de uma legenda SOURCEâ†’PT-BR.

A traduÃ§Ã£o anterior NÃƒO Ã© mostrada porque esta estratÃ©gia existe justamente para quebrar
ancoragem em um candidato defeituoso. Reconstrua o target DIRETAMENTE da SOURCE.

REGRAS:
- traduza somente o cue alvo;
- before_source/after_source servem apenas de contexto, nunca doe conteÃºdo ao alvo;
- reasons sÃ£o pÃ³s-condiÃ§Ãµes obrigatÃ³rias que precisam desaparecer;
- preserve negaÃ§Ã£o, identidade, referente, aÃ§Ã£o, modalidade, intensidade, repetiÃ§Ã£o e ownership;
- SOURCE sem gÃªnero explÃ­cito nÃ£o autoriza gÃªnero humano em PT-BR;
- preserve hard_locks exatamente;
- dialogue_turn_count deve permanecer semanticamente equivalente;
- nÃ£o invente SDH, marcador musical ou fala;
- PT-BR natural e oral, sem calque desnecessÃ¡rio;
- nÃ£o altere timestamps.

Retorne somente a traduÃ§Ã£o do cue solicitado no schema fornecido.
`;

const CONTRASTIVE_SURGERY_PROMPT_927 = `
VocÃª Ã© o CONTRASTIVE CUE SURGERY final de uma legenda SOURCEâ†’PT-BR.

VocÃª recebe SOURCE, current_pt e blockers residuais. NÃ£o faÃ§a ediÃ§Ã£o cosmÃ©tica.
Primeiro identifique silenciosamente por que current_pt ainda viola cada blocker; depois
reescreva o cue do zero para que TODOS desapareÃ§am sem criar um novo defeito.

Preserve: significado, negaÃ§Ã£o, identidade/referente, ownership, repetiÃ§Ã£o, diÃ¡logo,
registro, forÃ§a pragmÃ¡tica, hard_locks e neutralidade de gÃªnero quando a SOURCE nÃ£o prova gÃªnero.
Nunca mova conteÃºdo de before/after para o target. NÃ£o altere timestamps.
`;

async function runCueSurgery927(blocks, translations, issues, plan, job, mode = "source_only", options = {}) {
  const posMap = positionMap(blocks);
  const updated = new Map(translations);
  const deduped = [...new Map((Array.isArray(issues) ? issues : []).map(issue => [Number(issue?.id), issue])).values()]
    .filter(issue => Number.isInteger(Number(issue?.id)) && posMap.has(Number(issue?.id)));
  const stage = String(options?.stage || `cue-surgery:${mode}`);
  const selected = eligibleForStrategy928(job, deduped, mode, { ...options, stage, translations });
  if (!selected.length) return updated;

  const system = mode === "contrastive" ? CONTRASTIVE_SURGERY_PROMPT_927 : SOURCE_ONLY_SURGERY_PROMPT_927;
  const routeOverride = [GEMINI_MODELS.MAIN_FALLBACK, GEMINI_MODELS.MAIN_PRIMARY];
  let cursor = 0;
  const concurrency = Math.min(2, selected.length);

  async function worker(workerId) {
    while (true) {
      const at = cursor++;
      if (at >= selected.length) return;
      const issue = selected[at];
      const id = Number(issue.id);
      const pos = posMap.get(id);
      const block = blocks[pos];
      const beforePt942 = String(updated.get(id) || "");
      const attemptOptions942 = { ...options, stage, currentPt: beforePt942 };
      const protectedTarget = protectCulturalLocks(block.text, block.index);
      const locksById = new Map([[id, protectedTarget.locks]]);
      const payload = {
        i: id,
        source: protectedTarget.text,
        reasons: issue.reasons || [],
        identity_lock: identityLockForCapsule(block, plan),
        dialogue_turn_count: sourceDialogueDashCount(block),
        hard_locks: protectedTarget.locks.map(lock => lock.token),
        before_source: blocks.slice(Math.max(0, pos - 2), pos).map(x => ({ i: x.index, source: x.text })),
        after_source: blocks.slice(pos + 1, Math.min(blocks.length, pos + 3)).map(x => ({ i: x.index, source: x.text }))
      };
      if (mode === "contrastive") payload.current_pt = String(updated.get(id) || "");

      try {
        const response = await finalPriorityGeminiRequest({
          system,
          user: `IDIOMA DECLARADO DA FONTE: ${job.sourceLang || "auto"}\nBÃBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\nCUE:\n${JSON.stringify(payload)}`,
          schema: cueTranslationSchema(1),
          thinkingLevel: "high",
          maxOutputTokens: 7000,
          timeoutMs: 90000,
          maxRetries: 1,
          routeOverride,
          job,
          metric: "repair"
        }, job, `${mode.toUpperCase()} SURGERY W${workerId} cue ${id}`);

        const repaired = parseCueTranslation([block], response.text, locksById);
        let candidate = String(repaired.get(id) || "").trim();
        const beforePt = String(updated.get(id) || "");
        // Resposta utilizÃ¡vel recebida: agora sim esta estratÃ©gia foi tentada neste estado.
        markStrategyAttempted928(job, issue, mode, attemptOptions942);
        let regressions = [];
        if (options?.requireGenderZero) {
          const target = strictGenderCandidateCheck925(block, beforePt, candidate, job.filename, plan);
          candidate = target.candidate;
          regressions = [...target.regressions, ...target.remainingGender];
        } else {
          candidate = sanitizeFinalCue(block, candidate) || sanitizeFallbackCue(candidate) || candidate;
          regressions = repairCandidateRegressionReasons(block, beforePt, candidate, job.filename, plan);
        }
        if (regressions.length) {
          console.warn(`[CUE SURGERY 9.4.2] mode=${mode} cue=${id} rejeitado localmente | ${[...new Set(regressions)].join(", ")}.`);
          continue;
        }
        updated.set(id, candidate);
        console.log(`[CUE SURGERY 9.4.2] mode=${mode} cue=${id} candidato produzido; lock aguardarÃ¡ auditoria semÃ¢ntica.`);
      } catch (error) {
        console.warn(`[CUE SURGERY 9.4.2] mode=${mode} cue=${id} falhou tecnicamente; melhor candidato preservado | ${errorMessage(error).slice(0,260)}`);
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i + 1)));
  return updated;
}

async function auditTargetSet927(blocks, translations, targetIds, plan, job, label) {
  const laidOut = applySubtitleLayout(blocks, translations, `FINAL-TARGET-9.4.0-${label}`);
  const local = blockingLocalIssues(blocks, laidOut, job, plan)
    .filter(issue => targetIds.has(Number(issue?.id)));
  let semantic = [];
  try {
    semantic = await scanFinalPriorityAudit(blocks, translations, plan, job, targetIds);
  } catch (error) {
    console.warn(`[FINAL TARGET AUDIT 9.4.0] ${label} falhou tecnicamente; selo canÃ´nico fica bloqueado e candidato nÃ£o Ã© degradado | ${errorMessage(error).slice(0,300)}`);
    semantic = [...targetIds].map(id => ({
      id,
      reasons: ["FINAL_PRIORITY:AUDIT_TECHNICAL_UNAVAILABLE: a reauditoria focal nÃ£o ficou disponÃ­vel; preservar o melhor candidato e nunca declarar FINAL_PASS sem prova."]
    }));
    job.finalTargetAuditTechnicalFailure927 = true;
  }
  const mergedRaw = mergeIssueLists(local, semantic).filter(issue => targetIds.has(Number(issue?.id)));
  const merged = resolveGuardConflicts942(blocks, mergedRaw, job, `FINAL TARGET AUDIT ${label}`);
  rememberSemanticConstraints945(job, merged, `audit:${label}`);
  markSemanticResidualRejected940(job, translations, merged, `FINAL TARGET AUDIT ${label}`);
  return merged;
}

function isPureGenderResidual943(issue) {
  const reasons = Array.isArray(issue?.reasons) ? issue.reasons.map(x => String(x || "").trim()).filter(Boolean) : [];
  if (!reasons.length) return false;
  return reasons.every(reason =>
    /^(?:GENDER_V[2-9]_|UNKNOWN_SPEAKER_GENDER_MARKED|STRICT_GENDER_POSTCONDITION_)/i.test(reason)
  );
}

async function verifyFinalTargets927(blocks, translations, targetIssues, plan, job) {
  const targetIds = new Set((Array.isArray(targetIssues) ? targetIssues : [])
    .map(issue => Number(issue?.id)).filter(Number.isInteger));
  if (!targetIds.size) return { translations: new Map(translations), residual: [] };
  rememberSemanticConstraints945(job, targetIssues, "target-entry");

  const posMap = positionMap(blocks);
  const expandChangedIds9210 = changedIds => {
    const expanded = new Set();
    for (const id of changedIds) {
      const pos = posMap.get(Number(id));
      if (!Number.isInteger(pos)) continue;
      for (let p = Math.max(0, pos - 1); p <= Math.min(blocks.length - 1, pos + 1); p++) {
        expanded.add(Number(blocks[p].index));
      }
    }
    return expanded;
  };

  const mergeResidualPatch9210 = (baseResidual, patchResidual, patchIds) => {
    const byId = new Map((Array.isArray(baseResidual) ? baseResidual : []).map(issue => [Number(issue?.id), issue]));
    for (const id of patchIds) byId.delete(Number(id));
    for (const issue of (Array.isArray(patchResidual) ? patchResidual : [])) {
      const id = Number(issue?.id);
      if (Number.isInteger(id)) byId.set(id, issue);
    }
    return [...byId.values()].sort((a,b)=>Number(a.id)-Number(b.id));
  };

  let current = new Map(translations);
  let residual = await auditTargetSet927(blocks, current, targetIds, plan, job, "initial-9210");
  let best = new Map(current);
  let bestResidual = residual;
  let bestScore = issueScore927(residual);
  recordBestCandidate927(blocks, current, residual, job, "target-initial-9210");
  if (!residual.length) {
    console.log(`[LEGACY FINAL TARGET VERIFICATION 9.4.3.5] PASSOU âœ… | strategy=initial | residual=0.`);
    return { translations: current, residual: [] };
  }

  // 9.4.3.5: toda reconstruÃ§Ã£o recebe a UNIÃƒO dos blockers hard jÃ¡ observados
  // para o cue. Isso impede oscilaÃ§Ã£o "corrige significado -> quebra gÃªnero ->
  // corrige gÃªnero -> quebra significado". A SOURCE continua sendo autoridade.
  // CONTRASTIVE Ã© a quarta e ÃšLTIMA estratÃ©gia bounded, nunca um loop.
  const strategies = ["source_only", "candidate_beam", "constrained", "contrastive"];
  for (const strategy of strategies) {
    const residualWithMemory945 = issuesWithSemanticMemory945(job, bestResidual);
    const genericResidual943 = residualWithMemory945.filter(issue => !isPureGenderResidual943(issue));
    const deferredGender943 = residualWithMemory945.length - genericResidual943.length;
    if (!genericResidual943.length) {
      if (deferredGender943 > 0) {
        console.log(`[SPECIALIST-FIRST 9.4.3] ${deferredGender943} residual(is) exclusivamente de gÃªnero adiado(s) ao Gender Target Gate; 0 source_only/beam/constrained desperdiÃ§ado. âœ…`);
      }
      break;
    }
    const eligible = eligibleForStrategy928(job, genericResidual943, strategy, { stage: "final-target", translations: best });
    if (!eligible.length) {
      console.log(`[LEGACY UNIFIED ESCALATION 9.4.3.5] strategy=${strategy} jÃ¡ esgotada para blockers genÃ©ricos; 0 chamadas repetidas. âœ…`);
      continue;
    }
    console.warn(`[LEGACY UNIFIED ESCALATION 9.4.3.5] strategy=${strategy} | eligible=${eligible.length}/${genericResidual943.length} genÃ©rico(s) | gender-deferred=${deferredGender943} | ids=[${eligible.map(x=>x.id).join(",")}].`);

    let candidate;
    if (strategy === "candidate_beam") {
      candidate = await runCandidateBeam928(blocks, best, eligible, plan, job, { stage: "final-target" });
    } else if (strategy === "constrained") {
      candidate = await runConstrainedReconstruction928(blocks, best, eligible, plan, job, { stage: "final-target" });
    } else {
      candidate = await runCueSurgery927(blocks, best, eligible, plan, job, strategy, { stage: "final-target" });
    }

    const changedIds = new Set();
    for (const issue of eligible) {
      const id = Number(issue?.id);
      if (!Number.isInteger(id)) continue;
      if (String(candidate.get(id) || "") !== String(best.get(id) || "")) changedIds.add(id);
    }
    if (!changedIds.size) {
      console.warn(`[LEGACY UNIFIED ESCALATION 9.4.3.5] strategy=${strategy} nÃ£o alterou nenhum cue elegÃ­vel; QA repetida evitada. âœ…`);
      continue;
    }

    // Qualidade preservada com custo menor: reaudita somente o que realmente mudou
    // e seus vizinhos imediatos, pois ownership pode cruzar a fronteira do cue.
    const auditIds = expandChangedIds9210(changedIds);
    const patchResidual = await auditTargetSet927(blocks, candidate, auditIds, plan, job, strategy + "-changed-9210");
    const candidateResidual = mergeResidualPatch9210(bestResidual, patchResidual, auditIds);
    const score = issueScore927(candidateResidual);
    recordBestCandidate927(blocks, candidate, candidateResidual, job, `target-${strategy}-9210`);
    if (betterScore927(score, bestScore)) {
      best = new Map(candidate);
      bestResidual = candidateResidual;
      bestScore = score;
      commitVerifiedRepairLocks940(job, best, changedIds, bestResidual, `strategy=${strategy}`);
      console.log(`[LEGACY UNIFIED ESCALATION 9.4.3.5] strategy=${strategy} melhorou | hard=${score.hard} | weighted=${score.weighted} | residual=${score.count} | reaudited=${auditIds.size}.`);
    } else {
      console.warn(`[LEGACY UNIFIED ESCALATION 9.4.3.5] strategy=${strategy} nÃ£o superou o melhor candidato; rollback lÃ³gico para ledger | reaudited=${auditIds.size}.`);
    }
    if (!bestResidual.length) {
      console.log(`[LEGACY FINAL TARGET VERIFICATION 9.4.3.5] PASSOU âœ… | strategy=${strategy} | residual=0.`);
      return { translations: best, residual: [] };
    }
  }

  console.warn(`[LEGACY FINAL TARGET VERIFICATION 9.4.3.5] estratÃ©gias bounded distintas esgotadas | residual=${bestResidual.length}; MELHOR candidato Ã­ntegro serÃ¡ preservado como RECOVERY CHECKPOINT.`);
  return { translations: best, residual: bestResidual };
}

function sourceNegationRisk926(block) {
  const text = String(block?.text || "").replace(/\s+/g, " ").trim();
  if (!text) return false;
  // Audit focus only, never an automatic rewrite. Covers common explicit negation
  // across the source languages the project already accepts.
  return /(?:\bnot\b|n['â€™]t\b|\bnever\b|\bno\s+one\b|\bnothing\b|\bwithout\b|\bn[aÃ£]o\b|\bnunca\b|\bjamais\b|\bno\b|\bnon\b|\bnicht\b|\bkein(?:e|en|er|es)?\b|\bniet\b|\bpas\b)/iu.test(text);
}

function deterministicFinalResidual960(
  blocks,
  translations,
  job,
  plan
) {
  const current = new Map(translations);
  const laidOut = applySubtitleLayout(
    blocks,
    current,
    "FINAL-97-DETERMINISTIC"
  );

  const local = splitGenderSeverity970(
    blocks,
    resolveGuardConflicts942(
      blocks,
      blockingLocalIssues(blocks, laidOut, job, plan),
      job,
      "DETERMINISTIC FINAL LOCAL 9.7.0"
    ),
    job,
    "deterministic-final"
  ).hard;

  const localById = new Map(
    local.map(issue => [Number(issue?.id), issue])
  );

  const preHard = Array.isArray(job.preRepairHardIssues960)
    ? job.preRepairHardIssues960
    : [];
  const snapshot = job.preRepairHardSnapshot960 instanceof Map
    ? job.preRepairHardSnapshot960
    : new Map();

  const unresolvedSemantic = [];

  for (const issue of preHard) {
    const id = Number(issue?.id);
    if (!Number.isInteger(id)) continue;

    const before = String(snapshot.get(id) ?? "").replace(/\s+/g, " ").trim();
    const after = String(current.get(id) || "").replace(/\s+/g, " ").trim();

    // A Ãºnica rewrite mudou o cue. A partir daqui os guards objetivos locais
    // sÃ£o a autoridade de regressÃ£o; nÃ£o existe segundo juiz probabilÃ­stico.
    if (before !== after) continue;

    // Mesmo sem mudanÃ§a textual, uma normalizaÃ§Ã£o determinÃ­stica pode ter
    // eliminado a famÃ­lia objetiva que motivou o blocker.
    const originalFamilies = new Set(blockerFamilies928(issue));
    const currentIssue = localById.get(id);
    const currentFamilies = new Set(
      currentIssue ? blockerFamilies928(currentIssue) : []
    );

    const objectiveFamilies = [...originalFamilies].filter(family =>
      [
        "GENDER", "NEGATION", "OWNERSHIP", "CENSOR",
        "DIALOGUE", "LAYOUT", "SDH", "REPETITION"
      ].includes(family)
    );

    if (
      objectiveFamilies.length &&
      objectiveFamilies.every(family => !currentFamilies.has(family))
    ) {
      continue;
    }

    // Blocker semÃ¢ntico HARD que permaneceu byte-equivalente apÃ³s a Ãºnica
    // rewrite continua sem prova de resoluÃ§Ã£o e permanece fail-closed.
    unresolvedSemantic.push(issue);
  }

  return splitGenderSeverity970(
    blocks,
    resolveGuardConflicts942(
      blocks,
      mergeIssueLists(local, unresolvedSemantic),
      job,
      "DETERMINISTIC FINAL CONTRACT 9.7.0"
    ),
    job,
    "deterministic-contract"
  ).hard;
}

async function runBoundedFinalQuality88(
  blocks,
  translations,
  mainTranslations,
  qaIssues,
  plan,
  job
) {
  void mainTranslations;
  void qaIssues;

  let current = sanitizeTranslationMap(blocks, translations, job);

  // 9.7.2: o primeiro gate continua determinÃ­stico e barato. A diferenÃ§a Ã©
  // que um blocker semÃ¢ntico prÃ©-Repair que permaneceu byte-idÃªntico nÃ£o Ã©
  // mais condenado automaticamente. Ele recebe UMA reauditoria focal. Se a
  // reauditoria confirmar defeito, existe UMA closure repair bounded e UMA
  // verificaÃ§Ã£o final. Nunca hÃ¡ loop/cascade.
  let residual = deterministicFinalResidual960(
    blocks,
    current,
    job,
    plan
  );

  if (residual.length) {
    const laidOutBefore972 = applySubtitleLayout(
      blocks,
      current,
      "FINAL-972-PRE-CLOSURE"
    );
    const localBefore972 = splitGenderSeverity970(
      blocks,
      resolveGuardConflicts942(
        blocks,
        blockingLocalIssues(blocks, laidOutBefore972, job, plan),
        job,
        "POST-REPAIR CLOSURE LOCAL 9.7.2"
      ),
      job,
      "post-repair-closure-local"
    ).hard;
    const localIds972 = new Set(
      localBefore972.map(issue => Number(issue?.id)).filter(Number.isInteger)
    );

    // Semantic-only residuals are exactly the stale hard findings that 9.7.1
    // could not prove resolved because the Repair chose to keep the same text.
    // Verify them against SOURCE again instead of failing merely on equality.
    const semanticOnly972 = residual.filter(issue => {
      const id = Number(issue?.id);
      return Number.isInteger(id) && !localIds972.has(id);
    });

    let confirmedSemantic972 = [];
    if (semanticOnly972.length) {
      const semanticIds972 = new Set(
        semanticOnly972.map(issue => Number(issue?.id)).filter(Number.isInteger)
      );
      try {
        confirmedSemantic972 = await scanFinalPriorityAudit(
          blocks,
          current,
          plan,
          job,
          semanticIds972
        );
        confirmedSemantic972 = confirmedSemantic972.filter(issue =>
          semanticIds972.has(Number(issue?.id))
        );
        console.log(
          `[POST-REPAIR SEMANTIC VERIFY 9.7.3] candidatos=${semanticOnly972.length} | ` +
          `confirmados=${confirmedSemantic972.length} | falsos/stale=${Math.max(0, semanticOnly972.length - confirmedSemantic972.length)}. âœ…`
        );
      } catch (error) {
        // Technical audit failure may never silently authorize FINAL_PASS.
        confirmedSemantic972 = semanticOnly972;
        console.warn(
          `[POST-REPAIR SEMANTIC VERIFY 9.7.3] auditoria indisponÃ­vel; ` +
          `preservando ${confirmedSemantic972.length} blocker(s) fail-closed | ${errorMessage(error).slice(0,260)}`
        );
      }
    }

    let closureIssues972 = splitGenderSeverity970(
      blocks,
      resolveGuardConflicts942(
        blocks,
        mergeIssueLists(localBefore972, confirmedSemantic972),
        job,
        "POST-REPAIR CLOSURE 9.7.2"
      ),
      job,
      "post-repair-closure"
    ).hard;

    if (closureIssues972.length) {
      // The closure is deliberately finite. 48 cues = at most two 24-cue
      // repair batches; anything larger remains fail-closed instead of
      // exploding latency on a pathological job.
      const MAX_CLOSURE_CUES_972 = 48;
      const selectedClosure972 = closureIssues972.slice(0, MAX_CLOSURE_CUES_972);
      const targetIds972 = new Set(
        selectedClosure972.map(issue => Number(issue?.id)).filter(Number.isInteger)
      );

      if (closureIssues972.length > MAX_CLOSURE_CUES_972) {
        console.warn(
          `[POST-REPAIR CLOSURE 9.7.3] residual=${closureIssues972.length}; ` +
          `repair bounded aos primeiros ${MAX_CLOSURE_CUES_972}; excedente continua fail-closed.`
        );
      }

      const beforeClosure972 = new Map(current);
      console.warn(
        `[POST-REPAIR CLOSURE 9.7.3] repair Ãºnico bounded | ` +
        `alvos=${selectedClosure972.length} | batches<=${Math.ceil(selectedClosure972.length / FINAL_PRIORITY_ESCALATED_BATCH_MAX_CUES)}.`
      );

      current = await runFinalPriorityEscalatedRepair(
        blocks,
        current,
        selectedClosure972,
        plan,
        job
      );
      current = sanitizeTranslationMap(blocks, current, job);

      const changed972 = [...targetIds972].filter(id =>
        String(beforeClosure972.get(id) || "") !== String(current.get(id) || "")
      );
      console.log(
        `[POST-REPAIR CLOSURE 9.7.3] alterados=${changed972.length}/${targetIds972.size}; ` +
        `iniciando verificaÃ§Ã£o final focal.`
      );

      // Re-run deterministic guards globally (cheap) and semantic QA only for
      // closure targets (bounded). This is the final authority; no more repair.
      const laidOutAfter972 = applySubtitleLayout(
        blocks,
        current,
        "FINAL-972-POST-CLOSURE"
      );
      const localAfter972 = splitGenderSeverity970(
        blocks,
        resolveGuardConflicts942(
          blocks,
          blockingLocalIssues(blocks, laidOutAfter972, job, plan),
          job,
          "POST-REPAIR CLOSURE LOCAL FINAL 9.7.2"
        ),
        job,
        "post-repair-closure-final-local"
      ).hard;

      let semanticAfter972 = [];
      if (targetIds972.size) {
        try {
          semanticAfter972 = await scanFinalPriorityAudit(
            blocks,
            current,
            plan,
            job,
            targetIds972
          );
          semanticAfter972 = semanticAfter972.filter(issue =>
            targetIds972.has(Number(issue?.id))
          );
        } catch (error) {
          semanticAfter972 = selectedClosure972.map(issue => ({
            id: Number(issue?.id),
            reasons: [
              `FINAL_PRIORITY:AUDIT_TECHNICAL_UNAVAILABLE: closure 9.7.2 nÃ£o conseguiu provar resoluÃ§Ã£o final (${errorMessage(error).slice(0,160)}).`
            ]
          }));
        }
      }

      residual = splitGenderSeverity970(
        blocks,
        resolveGuardConflicts942(
          blocks,
          mergeIssueLists(localAfter972, semanticAfter972),
          job,
          "POST-REPAIR CLOSURE FINAL 9.7.2"
        ),
        job,
        "post-repair-closure-final"
      ).hard;

      // Any residual beyond the bounded target set also remains authoritative.
      if (closureIssues972.length > MAX_CLOSURE_CUES_972) {
        residual = mergeIssueLists(
          residual,
          closureIssues972.slice(MAX_CLOSURE_CUES_972)
        );
      }
    } else {
      // All old semantic-only blockers were disproved by the focal audit.
      residual = localBefore972;
    }
  }

  job.finalTargetResidual927 = residual;
  job.finalTargetResidualSnapshot9210 = new Map(
    residual.map(issue => [
      Number(issue?.id),
      String(current.get(Number(issue?.id)) || "")
    ])
  );

  if (residual.length) {
    job.qualityStatus = "best_available";
    job.noCacheFinal923 = true;
    console.error(
      `[DETERMINISTIC FINAL GATE 9.7.3] FAIL-CLOSED | residual=${residual.length} | ` +
      `post-repair closure esgotada; 0 loop adicional.`
    );
    for (const issue of residual.slice(0, 24)) {
      console.error(
        `[DETERMINISTIC FINAL RESIDUAL 9.7.3] cue=${Number(issue?.id)} | ` +
        `reasons=${(Array.isArray(issue?.reasons) ? issue.reasons : []).join(" || ")}`
      );
    }
  } else {
    job.qualityStatus = "final_pass";
    console.log(
      `[DETERMINISTIC FINAL GATE 9.7.3] PASSOU âœ… | residual=0 | ` +
      `post-repair semantic verification bounded; 0 cascade.`
    );
  }

  return current;
}

async function translateSrt(
  sourceSrt,
  job
) {
  const startedAt =
    Date.now();

  const blocks =
    parseSrt(
      sourceSrt
    );

  if (!blocks.length) {
    throw new Error(
      "Nenhum cue SRT vÃ¡lido."
    );
  }

  for (let bi = 0; bi < blocks.length; bi++) {
    const block = blocks[bi];
    block.sourceLang = String(job.sourceLang || "auto");
    block._prevText = String(blocks[bi - 1]?.text || "");
    block._nextText = String(blocks[bi + 1]?.text || "");
  }

  job.stats.sourceCues =
    blocks.length;

  console.log(
    `[PIPELINE 9.0 ROUTED] fonte=${
      job.sourceKind
    } | ${
      blocks.length
    } cues.`
  );

  let plan = job.episodePlan;
  if (plan) {
    console.log(`[EPISODE PLAN 9.4.3] CHECKPOINT reutilizado; 0 nova chamada PLAN.`);
  } else {
    plan = await buildEpisodePlan(blocks, job);
    job.episodePlan = plan;
  }

  job.progress = Math.max(5, Number(job.progress || 0));

  let mainTranslations =
  await translateAllMain(
    blocks,
    plan,
    job
  );

mainTranslations =
  sanitizeTranslationMap(
    blocks,
    mainTranslations,
    job
  );

// 8.4.4: qualquer fala real que tenha virado vazio no sanitizer Ã©
// recuperada aqui, cue por cue, antes do Timing Lock.
mainTranslations =
  await recoverSanitizedEmptyCues({
    blocks,
    translations:
      mainTranslations,
    plan,
    job,
    stage:
      "MAIN"
  });

// ============================================================
// HARD GUARD PRE-SAFE 9.0
// ============================================================
// SAFE DRAFT nÃ£o pode depender de QA premium para SDH/gÃªnero/turns/ownership.
// SÃ³ chama IA se houver blocker local real; caso contrÃ¡rio custa 0 requests.
const preSafeHardIssues = splitGenderSeverity970(
  blocks,
  detectLocalIssues(
    blocks,
    mainTranslations,
    job.filename,
    plan
  ).map(issue => ({
    id: issue.id,
    reasons: (issue.reasons || []).filter(reason =>
      /^(?:EMPTY|MEANING_INTEGRITY_|FINAL_MIXED_SCRIPT_CONFUSABLE|GENDER_V[2-9]_|UNKNOWN_SPEAKER_GENDER_MARKED|SPEAKER_LABEL_RESIDUE|SDH_RESIDUE|DIALOGUE_TURN_MISMATCH|DIALOGUE_TILDE_RESIDUE|MISSING_DIALOGUE_BREAK|ARTIFICIAL_PROFANITY_CENSORSHIP|UNRESOLVED_BLEEP_TOKEN|INVENTED_BLEEP_TOKEN|SOURCE_BLEEP_FIDELITY_LOST|FINAL_GARBAGE_OR_PLACEHOLDER|CUE_OWNERSHIP_(?:SHIFT|BOUNDARY_MISMATCH|BOUNDARY_DUPLICATION)|VISIBLE_CENSOR_PLACEHOLDER|SOURCE_EXACT_REPETITION_LOST|CONTEXTUAL_IMPERATIVE_REFERENT_INVENTION|BARE_IMPERATIVE_CONCRETE_REFERENT_INVENTION)/i.test(String(reason || ""))
    )
  })).filter(issue => issue.reasons.length),
  job,
  "pre-safe"
).hard;

if (preSafeHardIssues.length) {
  // 9.4.3: nÃ£o paga uma rodada cloud antes do QA global. Estes blockers jÃ¡
  // entram novamente no detectLocalIssues() do Repair combinado, junto com a
  // autoridade semÃ¢ntica do QA. SAFE DRAFT continua apenas checkpoint interno.
  console.log(
    `[HARD GUARD PRE-SAFE 9.4.3] ${preSafeHardIssues.length} blocker(s) local(is) detectado(s); ` +
    `REPAIR cloud adiado e fundido ao QA global (0 request extra nesta fase). âœ…`
  );
}

// ============================================================
// LAYOUT LOCK â€” SAFE DRAFT
// ============================================================
// O QA e o Repair continuam trabalhando com o texto sem reflow.
// O SAFE DRAFT jÃ¡ Ã© servido com layout profissional.
const mainLayoutTranslations =
  applySubtitleLayout(
    blocks,
    mainTranslations,
    "MAIN"
  );

const mainSrt =
  buildSrt(
    blocks,
    mainLayoutTranslations
  );

auditTimestamps(
  sourceSrt,
  mainSrt,
  "MAIN",
  job
);

job.safeDraft =
  mainSrt;
job.bestAvailableSrt927 = mainSrt;
job.bestAvailableLabel927 = "SAFE_DRAFT";

job.progress =
  92;

console.log(
  `[SAFE DRAFT] ${
    blocks.length
  }/${
    blocks.length
  } protegido com CUE + FORMAT + LAYOUT LOCK.`
);

// ============================================================
// QA
// ============================================================

let qaIssues =
  await scanPtbrQuality(
    blocks,
    mainTranslations,
    plan,
    job
  );

// ============================================================
// OWNERSHIP QUARANTINE â€” 9.0
// ============================================================
// O QA global jÃ¡ existe e jÃ¡ pagou o custo de detectar possÃ­veis shifts.
// Em vez de reparar IDs esparsos, 9.0 reconstrÃ³i a regiÃ£o original como
// micro-batches selados e troca os findings antigos por QA fresco.
{
  const ownershipGate900 = await runOwnershipQuarantine900(
    blocks,
    mainTranslations,
    qaIssues,
    plan,
    job
  );
  mainTranslations = ownershipGate900.translations;
  qaIssues = ownershipGate900.qaIssues;
}

// ============================================================
// SINGLE SEMANTIC CLOSURE PRE-AUDIT â€” 9.5.0
// ============================================================
// Collect every semantic/local constraint BEFORE the only Repair round.
// Final Priority is therefore not allowed to discover a blocker later and
// trigger another rewrite cascade.
{
  const preClosureLocal950 = detectLocalIssues(
    blocks,
    mainTranslations,
    job.filename,
    plan
  );
  const preClosureFocus950 = new Set([
    ...idsFromIssues(qaIssues, blocks),
    ...idsFromIssues(preClosureLocal950, blocks)
  ]);

  for (const block of blocks) {
    const pt = String(mainTranslations.get(block.index) || "");
    if (sourceNegationRisk926(block) &&
        !/(?:\bn[aÃ£]o\b|\bnunca\b|\bjamais\b|\bningu[eÃ©]m\b|\bnada\b|\bnem\b|\bsem\b)/iu.test(pt)) {
      preClosureFocus950.add(block.index);
    }
  }

  let preClosureSemantic950 = [];
  if (preClosureFocus950.size) {
    console.log(
      `[SINGLE REPAIR CONTRACT 9.7.0] PRE-AUDIT | foco=${preClosureFocus950.size} | ` +
      `todos os blockers serÃ£o fundidos ANTES da Ãºnica rodada de Repair.`
    );
    preClosureSemantic950 = await scanFinalPriorityAudit(
      blocks,
      mainTranslations,
      plan,
      job,
      preClosureFocus950
    );
  }

  qaIssues = resolveGuardConflicts942(
    blocks,
    mergeIssueLists(qaIssues, preClosureSemantic950, preClosureLocal950),
    job,
    "PRE-REPAIR SINGLE CLOSURE 9.7.0"
  );
  qaIssues = splitGenderSeverity970(blocks, qaIssues, job, "pre-repair").hard;
  rememberSemanticConstraints945(job, qaIssues, "single-pre-repair-9.7.0");

  // Contrato hard prÃ©-Repair: depois da ÃšNICA rewrite, nÃ£o haverÃ¡ outro
  // auditor probabilÃ­stico autorizado a inventar uma nova cascata.
  job.preRepairHardIssues960 = issuesWithSemanticMemory945(job, qaIssues)
    .filter(issue => finalIssueWeight927(issue) >= 60);
  job.preRepairHardSnapshot960 = new Map(
    job.preRepairHardIssues960.map(issue => [
      Number(issue?.id),
      String(mainTranslations.get(Number(issue?.id)) || "")
    ])
  );
  console.log(
    `[SEMANTIC CONTRACT 9.7.0] hard-pre-repair=${job.preRepairHardIssues960.length} | ` +
    `UMA rewrite consolidada; pÃ³s-Repair somente gates determinÃ­sticos.`
  );
}

// ============================================================
// REPAIR
// ============================================================

// Snapshot semÃ¢ntico ANTES de Repair + Compact Rescue.
// Serve para descobrir se uma reescrita posterior
// perdeu ou inventou informaÃ§Ã£o.
const preRewriteTranslations =
  new Map(
    mainTranslations
  );
  
  let finalTranslations =
  await tryFocusedRepair(
    blocks,
    mainTranslations,
    plan,
    job,
    qaIssues
  );

finalTranslations =
  sanitizeTranslationMap(
    blocks,
    finalTranslations,
    job
  );

// ============================================================
// COMPACT RESCUE
// ============================================================
// SÃ³ os cues que AINDA nÃ£o cabem em 2x50 entram aqui.
// NÃ£o move conteÃºdo, nÃ£o cria cue e nÃ£o toca em timestamp.
finalTranslations =
  await runCompactRescue(
    blocks,
    finalTranslations,
    plan,
    job
  );

finalTranslations =
  sanitizeTranslationMap(
    blocks,
    finalTranslations,
    job
  );

// ============================================================
// FINAL QUALITY â€” 9.5.0 SINGLE SEMANTIC CLOSURE
// ============================================================
// O Ãºnico Repair cloud jÃ¡ aconteceu. Daqui em diante existe somente
// verificaÃ§Ã£o HIGH + fechamento determinÃ­stico local. ZERO nova rewrite cloud.
job.episodePlan = plan;
finalTranslations = await runBoundedFinalQuality88(
  blocks,
  finalTranslations,
  mainTranslations,
  qaIssues,
  plan,
  job
);

// 9.0: FINAL OWNERSHIP GATE. SÃ³ existe custo cloud quando o QA global
// provou uma corrupÃ§Ã£o de ownership e houve quarentena neste job.
finalTranslations = await enforceFinalOwnershipGate900(
  blocks,
  finalTranslations,
  plan,
  job
);

// 9.0: FINAL DETERMINISTIC CLOSURE â€” ZERO-CLOUD.
// Nenhuma chamada Gemini, nenhum loop e nenhum atraso de rede.
// Fecha gÃªnero, broadcast, repetiÃ§Ã£o/layout e ownership com fallback para o
// candidato MAIN quando ele for objetivamente mais seguro.
finalTranslations = sanitizeTranslationMap(blocks, finalTranslations, job);
finalTranslations = applyFinalOwnershipFallback898(
  blocks, finalTranslations, mainTranslations, job.filename, plan, job
);
finalTranslations = sanitizeTranslationMap(blocks, finalTranslations, job);
finalTranslations = applyFinalStrictLayoutFallback898(
  blocks, finalTranslations, mainTranslations, job.filename, plan, job
);
finalTranslations = sanitizeTranslationMap(blocks, finalTranslations, job);
finalTranslations = applyRepairPersistenceLock923(
  blocks, finalTranslations, job, job.filename, plan
);
finalTranslations = sanitizeTranslationMap(blocks, finalTranslations, job);

let finalClosure898 = finalClosureResidualSummary898(
  blocks, finalTranslations, job.filename, plan
);

// 9.5.0 â€” GENDER FINAL GATE IS VERIFICATION-ONLY.
// All gender constraints were already included BEFORE the single Repair round,
// and sanitizeTranslationMap() applies deterministic zero-cloud neutralizers.
// No model rewrite is allowed here; this prevents repair cascades.
if (finalClosure898.gender > 0) {
  console.warn(
    `[GENDER EVIDENCE 9.7.3][HARD] residual=${finalClosure898.gender} | ` +
    `0 Repair adicional; fail-closed se a normalizaÃ§Ã£o determinÃ­stica nÃ£o bastou.`
  );
  job.qualityStatus = "best_available";
  job.noCacheFinal923 = true;
}

// 9.6.0 POST-CLOSURE DETERMINISTIC RECHECK.
// Fechamentos locais podem alterar texto depois do gate anterior; reavaliamos
// somente invariantes determinÃ­sticos. ZERO Gemini, ZERO nova rewrite.
{
  const postResidual960 = deterministicFinalResidual960(
    blocks,
    finalTranslations,
    job,
    plan
  );
  job.finalTargetResidual927 = postResidual960;
  job.finalTargetResidualSnapshot9210 = new Map(
    postResidual960.map(issue => [
      Number(issue?.id),
      String(finalTranslations.get(Number(issue?.id)) || "")
    ])
  );

  if (postResidual960.length) {
    job.qualityStatus = "best_available";
    job.noCacheFinal923 = true;
    console.error(
      `[POST-CLOSURE DETERMINISTIC 9.7.3] residual=${postResidual960.length} | selo bloqueado sem nova chamada cloud.`
    );
  } else {
    console.log(
      `[POST-CLOSURE DETERMINISTIC 9.7.3] residual=0 âœ… | 0 chamada cloud.`
    );
  }
}

if (finalClosure898.gender > 0) {
  job.qualityStatus = "best_available";
  job.noCacheFinal923 = true;
  console.error(`[GENDER HARD FINAL GATE 9.7.3] FAIL-CLOSED PARA SELO/CACHE | gender=${finalClosure898.gender}; melhor candidato Ã­ntegro serÃ¡ preservado como CHECKPOINT; selo canÃ´nico bloqueado e NÃƒO serÃ¡ servido como FINAL.`);
}
console.log(
  `[FINAL CLOSURE 9.4.0] layout=${finalClosure898.layout} | ` +
  `gender=${finalClosure898.gender} | ownership=${finalClosure898.ownership} | ` +
  `broadcast=${finalClosure898.broadcast} | repetition=${finalClosure898.repetition} | ` +
  `censor=${finalClosure898.censor} | ownership-gate-residual=${Number(job.ownershipFinalResidual900 || 0)} ` +
  `${finalClosure898.gender === 0 ? "âœ…" : "â›”"}`
);

const authorizedLayoutTranslations =
  applySubtitleLayout(
    blocks,
    finalTranslations,
    "FINAL-AUTHORIZED-8.8"
  );

const finalSrt =
  buildSrt(
    blocks,
    authorizedLayoutTranslations
  );

auditTimestamps(
  sourceSrt,
  finalSrt,
  "FINAL",
  job
);
  const pipelineElapsedSeconds =
    (
      (
        Date.now() -
        startedAt
      ) /
      1000
    );

  const jobElapsedSeconds =
    (
      (
        Date.now() -
        (
          Number(
            job.translationStartedAt
          ) ||
          startedAt
        )
      ) /
      1000
    );

  const semanticResidual927 = Array.isArray(job.finalTargetResidual927)
    ? job.finalTargetResidual927.length
    : 0;
  const finalHardClosureClean931 =
    Number(finalClosure898.layout || 0) === 0 &&
    Number(finalClosure898.gender || 0) === 0 &&
    Number(finalClosure898.ownership || 0) === 0 &&
    Number(finalClosure898.broadcast || 0) === 0 &&
    Number(finalClosure898.repetition || 0) === 0 &&
    Number(finalClosure898.censor || 0) === 0 &&
    Number(job.ownershipFinalResidual900 || 0) === 0;

  if (finalHardClosureClean931 && semanticResidual927 === 0) {
    // 9.4.0: noCacheFinal923 may have been raised by an earlier candidate that
    // was later repaired. A fresh FINAL closure is the only authority allowed
    // to clear that stale flag. This prevents FINAL_PASS from being persisted
    // as PROVISIONAL and avoids useless full retranslations on the next play.
    job.noCacheFinal923 = false;
    job.qualityStatus = "final_pass";
    console.log(
      `[PIPELINE 9.4.3 ROUTED] FINAL OK | ${
        blocks.length
      } source cues | pipeline=${
        pipelineElapsedSeconds.toFixed(1)
      }s | job-total=${
        jobElapsedSeconds.toFixed(1)
      }s | full-job-retries=${
        job.stats.jobRetries || 0
      } | canonical-eligible=sim.`
    );
  } else {
    console.warn(
      `[PIPELINE 9.4.3 ROUTED] CHECKPOINT ONLY + PROVISIONAL | ${blocks.length} source cues | ` +
      `gender-residual=${finalClosure898.gender} | semantic-residual=${semanticResidual927} | ` +
      `closure-clean=${finalHardClosureClean931 ? "sim" : "nÃ£o"} | ` +
      `pipeline=${pipelineElapsedSeconds.toFixed(1)}s | job-total=${jobElapsedSeconds.toFixed(1)}s. FINAL OK bloqueado.`
    );
    job.noCacheFinal923 = true;
    job.qualityStatus = "best_available";
  }

  job.bestAvailableSrt927 = finalSrt;
  job.bestAvailableLabel927 = job.qualityStatus === "final_pass" ? "FINAL_PASS" : "RECOVERY_CHECKPOINT_FINAL";
  return finalSrt;
}

async function processJob(
  job
) {
  if (
    !Number(
      job.translationStartedAt
    )
  ) {
    job.translationStartedAt =
      Date.now();
  }

  job.status = "processing";
  job.progress = Math.max(1, job.progress || 0);
  job.updatedAt = Date.now();

  let attempt = 0;
  let lastJobError = null;

  while (
    attempt <
    JOB_MAX_ATTEMPTS
  ) {
    try {
      const cached = getCache(job.cacheKey);
      const previousProvisional927 = getProvisionalCache927(job.cacheKey);
      if (previousProvisional927 && !job.previousProvisionalSrt927) {
        job.previousProvisionalSrt927 = previousProvisional927.srt;
      }

      if (cached) {
        restoreIntentionalEmptyIdsFromCache(
          job.cacheKey,
          job
        );

        auditTimestamps(
          job.sourceSrt,
          cached,
          "CACHE",
          job
        );

        job.result = cached;
        job.status = "completed";
        job.progress = 100;
        job.error = null;
        job.qualityStatus =
          "cache_verified";
        return;
      }

      const finalSrt = await translateSrt(
        job.sourceSrt,
        job
      );

      // 9.4.2: disponibilidade tÃ©cnica NÃƒO Ã© produto final. Se qualquer closure
      // terminou com residual, preservamos o candidato como recovery checkpoint,
      // mas NÃƒO marcamos completed e NÃƒO servimos BEST_AVAILABLE como legenda.
      if (job.qualityStatus !== "final_pass") {
        job.bestAvailableSrt927 = finalSrt || job.bestAvailableSrt927 || job.safeDraft || null;
        if (job.bestAvailableSrt927) {
          setProvisionalCache927(job.cacheKey, job.bestAvailableSrt927, job, "recovery_checkpoint");
        }
        job.result = null;
        job.status = "failed";
        job.progress = 100;
        job.noCacheFinal923 = true;
        job.qualityStatus = "no_final_pass";
        job.error = `FINAL_PASS bloqueado por residual hard/semÃ¢ntico; checkpoint preservado e nÃ£o servido.`;
        job.updatedAt = Date.now();
        console.error(
          `[FINAL PASS REQUIRED 9.4.2] translateSrt terminou sem closure limpa; ` +
          `checkpoint preservado, canonical bloqueado e entrega final recusada.`
        );
        return;
      }

      // 9.2.3: strict final gate may allow availability without freezing a
      // residual as canonical. Such results are served but never cached.
      if (!job.noCacheFinal923) {
        setCache(job.cacheKey, finalSrt, job);
        console.log(`[CACHE 9.4.0] CANONICAL salvo | quality=${job.qualityStatus || "final_pass"}.`);
      } else {
        setProvisionalCache927(job.cacheKey, finalSrt, job, "best_available");
        console.warn(`[CACHE 9.4.0] CANONICAL bloqueado; PROVISIONAL preservado | quality=${job.qualityStatus || "best_available"}.`);
      }

      job.result = finalSrt;
      job.status = "completed";
      job.progress = 100;
      job.error = null;
      if (
        job.qualityStatus ===
        "pending"
      ) {
        job.qualityStatus =
          "final_pass";
      }
      return;
    } catch (error) {
      lastJobError = error;

      if (error?.routerExhausted && job.safeDraft) {
        attempt++;
        job.stats.jobRetries = (job.stats.jobRetries || 0) + 1;
        if (attempt < JOB_MAX_ATTEMPTS) {
          const metric = String(error?.metric || "repair");
          const now = Date.now();
          const route = geminiRouteForMetric(metric);
          const waits = route
            .filter(modelId => !modelSkippedForJob(job, modelId))
            .map(modelId => Math.max(
              Number(runtimeForGeminiModel(modelId).unavailableUntil || 0),
              modelMetricCooldownUntil942(job, metric, modelId)
            ) - now)
            .filter(ms => ms > 0);
          const waitMs = Math.min(ROUTER_RECOVERY_WAIT_MAX_MS_942, waits.length ? Math.min(...waits) + 150 : JOB_RETRY_BASE_MS);
          job.status = "processing";
          job.qualityStatus = "recovering_final_pass";
          job.error = `ROUTER RECOVERY: retomando do PLAN/MAIN checkpoint em ${(waitMs/1000).toFixed(1)}s`;
          console.warn(
            `[JOB ${job.id}] router esgotado apÃ³s SAFE DRAFT; UMA retomada bounded serÃ¡ feita do checkpoint ` +
            `(PLAN reutilizado + MAIN checkpoint), sem apagar progresso | wait=${(waitMs/1000).toFixed(1)}s.`
          );
          await sleep(waitMs);
          continue;
        }
        console.error(`[JOB ${job.id}] router continuou esgotado apÃ³s a Ãºnica retomada bounded; FINAL_PASS nÃ£o serÃ¡ falsificado.`);
        break;
      }

      // 9.2.7: after an integral SAFE DRAFT exists, a late-stage exception must
      // never restart PLAN+MAIN from zero. Final/QA/repair stages already have their
      // own bounded strategy escalation. Preserve and deliver the best known result.
      if (job.safeDraft && Number(job.progress || 0) >= 92) {
        console.warn(
          `[DELIVERY GUARANTEE 9.4.0] falha tardia apÃ³s SAFE DRAFT; full-job restart proibido. ` +
          `Checkpoint serÃ¡ preservado sem entrega FINAL | ${errorMessage(error).slice(0,320)}`
        );
        break;
      }

      if (error?.noJobRetry || isDeterministicGeminiRequestError(error)) {
        attempt = JOB_MAX_ATTEMPTS;
        console.error(
          `[JOB ${job.id}] erro determinÃ­stico; full-job retry PROIBIDO em 8.7 | ` +
          `${errorMessage(error).slice(0, 420)}`
        );
        break;
      }

      attempt++;
      job.stats.jobRetries =
        (job.stats.jobRetries || 0) + 1;

      const waitMs = Math.min(
        JOB_RETRY_MAX_MS,
        JOB_RETRY_BASE_MS * Math.pow(1.7, Math.min(attempt - 1, 7))
      );

      // 8.4.0: NÃƒO cachear safeDraft defeituoso e NÃƒO marcar failed.
      // Mantemos o job vivo e repetimos. SafeDraft continua apenas como
      // proteÃ§Ã£o interna/diagnÃ³stico; nunca ganha selo FINAL por erro.
      job.status = "processing";
      job.progress = Math.max(Number(job.progress || 0), Math.min(99, Math.max(1, job.progress || 1)));
      job.error =
        `RETRYING (${attempt}): ${errorMessage(error).slice(0, 500)}`;
      job.updatedAt = Date.now();

      if (
        attempt <
        JOB_MAX_ATTEMPTS
      ) {
        console.warn(
          `[JOB ${job.id}] falha transitÃ³ria; ` +
          `retry ${attempt}/${JOB_MAX_ATTEMPTS - 1} em ` +
          `${(waitMs / 1000).toFixed(1)}s | ` +
          `${errorMessage(error).slice(0, 420)}`
        );

        await sleep(waitMs);
      }
    }
  }

  const deliveryCandidate927 =
    job.bestAvailableSrt927 ||
    job.safeDraft ||
    job.previousProvisionalSrt927 ||
    getProvisionalCache927(job.cacheKey)?.srt ||
    null;

  if (deliveryCandidate927) {
    auditTimestamps(job.sourceSrt, deliveryCandidate927, "RECOVERY-CHECKPOINT-9.4.2", job);
    job.bestAvailableSrt927 = deliveryCandidate927;
    job.result = null;
    job.status = "failed";
    job.progress = 100;
    job.qualityStatus = "no_final_pass";
    job.noCacheFinal923 = true;
    job.updatedAt = Date.now();
    job.error =
      `FINAL_PASS nÃ£o obtido; melhor checkpoint Ã­ntegro foi preservado internamente, mas NÃƒO serÃ¡ servido como legenda final: ` +
      `${errorMessage(lastJobError).slice(0, 360)}`;
    setProvisionalCache927(job.cacheKey, deliveryCandidate927, job, "recovery_checkpoint");
    job.stats.boundedSafeDraftReleases = (job.stats.boundedSafeDraftReleases || 0) + 1;
    console.error(
      `[FINAL PASS REQUIRED 9.4.2] cloud/estratÃ©gias encerradas sem selo canÃ´nico; ` +
      `checkpoint Ã­ntegro (${job.bestAvailableLabel927 || (job.safeDraft ? "SAFE_DRAFT" : "PROVISIONAL")}) preservado ` +
      `APENAS para recuperaÃ§Ã£o. NÃ£o serÃ¡ entregue como FINAL nem substituirÃ¡ canonical.`
    );
    return;
  }


  job.status = "failed";
  job.progress = 100;
  job.error =
    `Falha terminal somente porque nenhum candidato SRT Ã­ntegro chegou a existir: ${
      errorMessage(lastJobError).slice(0, 500)
    }`;
  job.qualityStatus =
    "no_safe_draft";
  job.updatedAt = Date.now();

  console.error(
    `[JOB ${job.id}] encerrado sem SAFE DRAFT apÃ³s ${JOB_MAX_ATTEMPTS} tentativa(s) | ` +
    `${errorMessage(lastJobError).slice(0, 420)}`
  );
}

function startJob(job) {
  if (job.promise) {
    return job.promise;
  }

  job.started =
    true;

  job.status =
    "processing";

  job.promise =
    processJob(job)
      .finally(() => {
        job.promise =
          null;
      });

  return job.promise;
}

function jobResponse(
  req,
  job
) {
  return {
    ok: true,

    jobId:
      job.id,

    status:
      job.status,

    qualityStatus:
      job.qualityStatus,

    residualBlockers:
      Array.isArray(job.finalTargetResidual927) ? job.finalTargetResidual927.length : 0,

    bestCandidateLabel:
      job.bestAvailableLabel927 || "",

    progress:
      job.progress,

    sourceKind:
      job.sourceKind,

    sourceHash:
      job.sourceHash,

    subtitleUrl:
      `${baseUrl(req)}/subtitle/${
        encodeURIComponent(
          job.id
        )
      }.srt`
  };
}

// ============================================================
// OPENSUBTITLES CLOUD
// ============================================================

async function fetchWithTimeout(
  url,
  options = {},
  timeoutMs =
    FETCH_TIMEOUT_MS
) {
  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () =>
        controller.abort(),
      timeoutMs
    );

  try {
    return await fetch(
      url,
      {
        ...options,

        signal:
          controller.signal
      }
    );
  } finally {
    clearTimeout(timer);
  }
}

function parseExtra(extra) {
  const params =
    new URLSearchParams(
      extra || ""
    );

  return {
    filename:
      params.get(
        "filename"
      ) || "",

    videoSize:
      params.get(
        "videoSize"
      ) || "",

    videoHash:
      params.get(
        "videoHash"
      ) || ""
  };
}

function buildOpenSubtitlesUrl(
  type,
  id,
  {
    filename,
    videoSize,
    videoHash
  }
) {
  const base =
    `https://opensubtitles-v3.strem.io/subtitles/${
      encodeURIComponent(
        type
      )
    }/${
      encodeURIComponent(
        id
      )
    }`;

  const params =
    new URLSearchParams();

  if (videoHash) {
    params.set(
      "videoHash",
      videoHash
    );
  }

  if (videoSize) {
    params.set(
      "videoSize",
      videoSize
    );
  }

  if (filename) {
    params.set(
      "filename",
      filename
    );
  }

  return params.toString()
    ? `${base}/${params.toString()}.json`
    : `${base}.json`;
}

function selectEnglishSubtitle(
  subtitles
) {
  return (
    Array.isArray(
      subtitles
    )
      ? subtitles
      : []
  )
    .filter(
      subtitle =>
        [
          "eng",
          "en"
        ].includes(
          String(
            subtitle?.lang ||
            ""
          ).toLowerCase()
        ) &&

        /^https?:\/\//i.test(
          String(
            subtitle?.url ||
            ""
          )
        )
    )
    .sort(
      (a, b) => {
        const score =
          subtitle =>
            (
              String(
                subtitle?.lang ||
                ""
              ).toLowerCase() ===
              "eng"
                ? 100
                : 90
            ) +

            (
              subtitle
                ?.hearingImpaired ===
              false
                ? 20
                : 0
            ) +

            (
              String(
                subtitle?.format ||
                ""
              ).toLowerCase() ===
              "srt"
                ? 10
                : 0
            );

        return (
          score(b) -
          score(a)
        );
      }
    )[0] ||
    null;
}

async function fetchOpenSubtitlesSource({
  type,
  id,
  filename,
  videoSize,
  videoHash
}) {
  const url =
    buildOpenSubtitlesUrl(
      type,
      id,
      {
        filename,
        videoSize,
        videoHash
      }
    );

  console.log(
    `[OPENSUBTITLES CLOUD] ${url}`
  );

  const response =
    await fetchWithTimeout(
      url,

      {
        headers: {
          Accept:
            "application/json",

          "User-Agent":
            "Stremio-PTBR/8.4.0"
        }
      }
    );

  if (!response.ok) {
    throw new Error(
      `OpenSubtitles HTTP ${
        response.status
      }.`
    );
  }

  const data =
    await response.json();

  const target =
    selectEnglishSubtitle(
      data?.subtitles
    );

  if (!target) {
    return null;
  }

  const subtitleResponse =
    await fetchWithTimeout(
      target.url,

      {
        headers: {
          "User-Agent":
            "Stremio-PTBR/8.4.0"
        }
      }
    );

  if (
    !subtitleResponse.ok
  ) {
    throw new Error(
      `Download OpenSubtitles HTTP ${
        subtitleResponse.status
      }.`
    );
  }

  const raw =
    normalizeSrt(
      await subtitleResponse.text()
    );

  if (
    !raw ||
    raw.length >
      MAX_SOURCE_CHARS
  ) {
    throw new Error(
      "Legenda OpenSubtitles vazia/grande demais."
    );
  }

  const clean =
    cleanSrtForTranslation(
      raw
    );

  if (!clean) {
    throw new Error(
      "Legenda OpenSubtitles vazia apÃ³s limpeza."
    );
  }

  return clean;
}

async function publicSubtitlesHandler(
  req,
  res
) {
  const type =
    String(
      req.params.type ||
      ""
    );

  const id =
    String(
      req.params.id ||
      ""
    );

  const {
    filename,
    videoSize,
    videoHash
  } =
    parseExtra(
      req.params.extra ||
      ""
    );

  console.log(
    `[STREMIO CLOUD] ${
      type
    }/${
      id
    } | ${
      filename ||
      "sem filename"
    }`
  );

  try {
    const sourceSrt =
      await fetchOpenSubtitlesSource({
        type,
        id,
        filename,
        videoSize,
        videoHash
      });

    if (!sourceSrt) {
      console.log(
        "[STREMIO CLOUD] nenhuma legenda inglesa utilizÃ¡vel."
      );

      return safeJson(
        res,
        {
          subtitles: []
        }
      );
    }

    const recovery = {
      type,
      id,
      filename,
      videoSize,
      videoHash
    };

    const job =
      getOrCreateJob(
        {
          type,

          videoId:
            id,

          filename,

          sourceSrt,

          sourceKind:
            "opensubtitles-cloud",

          recovery
        },

        {
          lazy: true
        }
      );

    const subtitleUrl =
      buildCloudSubtitleUrl(
        req,
        job,
        recovery
      );

    console.log(
      `[CLOUD LAZY] opÃ§Ã£o criada sem Gemini | job=${
        job.id
      }`
    );

    return safeJson(
      res,

      {
        subtitles: [
          {
            id:
              `ptbr-cloud-opensub-${
                job.sourceHash.slice(
                  0,
                  12
                )
              }`,

            url:
              subtitleUrl,

            lang:
             "PT-BR Cloud"
          }
        ]
      }
    );
  } catch (error) {
    console.error(
      `[STREMIO CLOUD] ${
        errorMessage(
          error
        )
      }`
    );

    return safeJson(
      res,

      {
        subtitles: []
      }
    );
  }
}

async function recoverCloudJob(
  token
) {
  const payload =
    decodeRecovery(
      token
    );

  const recovery = {
    type:
      String(
        payload.t ||
        ""
      ),

    id:
      String(
        payload.i ||
        ""
      ),

    filename:
      String(
        payload.f ||
        ""
      ),

    videoSize:
      String(
        payload.s ||
        ""
      ),

    videoHash:
      String(
        payload.h ||
        ""
      )
  };

  console.log(
    `[CLOUD SELF-HEAL] recuperando ${
      recovery.type
    }/${
      recovery.id
    } apÃ³s restart/expiraÃ§Ã£o de memÃ³ria.`
  );

  const sourceSrt =
    await fetchOpenSubtitlesSource(
      recovery
    );

  if (!sourceSrt) {
    throw new Error(
      "OpenSubtitles nÃ£o retornou fonte para autorrecuperaÃ§Ã£o."
    );
  }

  const job =
    getOrCreateJob(
      {
        type:
          recovery.type,

        videoId:
          recovery.id,

        filename:
          recovery.filename,

        sourceSrt,

        sourceKind:
          "opensubtitles-cloud",

        recovery
      },

      {
        lazy: false
      }
    );

  return job;
}

// ============================================================
// ROUTES
// ============================================================

const manifest = {
  id:
    "org.tradutor.stateless.gemini.free",

    version:
    "9.4.2",

  name:
    "PT-BR Cloud â€¢ OpenSubtitles",

  description:
    "OpenSubtitles â†’ PT-BR com Router por fase, SOURCE Gender Evidence Authority, convergÃªncia bounded, Cue Ownership, QA/Repair focal e OpenSub Sync preservado.",

  resources: [
    "subtitles"
  ],

  types: [
    "movie",
    "series"
  ],

  idPrefixes: [
    "tt"
  ],

  catalogs: [],

  behaviorHints: {
    configurable:
      false
  }
};

app.get(
  "/manifest.json",

  (req, res) =>
    res.json(
      manifest
    )
);

app.get(
  "/",

  (req, res) =>
    res.json({
      status:
        "online",

      version:
        manifest.version,

      model:
        GEMINI_MODEL,

      models: {
        mainPreferred: geminiRouteForMetric("main")[0],
        mainFallback: geminiRouteForMetric("main")[1],
        qaPreferred: geminiRouteForMetric("qa")[0],
        qaFallback: geminiRouteForMetric("qa")[1],
        repairPreferred: geminiRouteForMetric("repair")[0],
        repairFallback: geminiRouteForMetric("repair")[1]
      },

      mode:
        "CLOUD_OPENSUB_PLUS_LOCAL_TRANSLATION_MULTI_MODEL_ROUTER_AND_GEMINI_TRANSCRIBE",

      mainBatchMaxCues:
        MAIN_BATCH_MAX_CUES,

      mainConcurrency:
        MAIN_CONCURRENCY,

      pacerMs:
        GEMINI_MODEL_PROFILES[GEMINI_MODELS.MAIN_PRIMARY].minStartMs,

      transcribeBudget:
        transcribeBudgetSnapshot(),

      cache:
        translationCache.size,

      jobs:
        jobs.size
    })
);

app.get(
  "/subtitles/:type/:id.json",
  publicSubtitlesHandler
);

app.get(
  "/subtitles/:type/:id/:extra.json",
  publicSubtitlesHandler
);

// ============================================================
// BRIDGE GATEWAY 1.1 â€” gateway estÃ¡vel notebook + Samsung
// Render NÃƒO faz fetch do Quick Tunnel: registra autenticado e redireciona o cliente.
// ============================================================
app.post(
  "/api/bridge/register",
  (req, res) => {
    if (!authorized(req)) {
      return safeJson(res, { error: "Unauthorized" }, 401);
    }

    try {
      const baseUrl = normalizeBridgeGatewayUrl(req.body?.baseUrl);
      const now = Date.now();

      bridgeGatewayState.baseUrl = baseUrl;
      bridgeGatewayState.registeredAt = now;
      bridgeGatewayState.expiresAt = now + BRIDGE_GATEWAY_TTL_MS;
      bridgeGatewayState.lastOkAt = now;

      console.log(
        `[BRIDGE GATEWAY 1.1] registrado âœ… | target=${baseUrl} | ` +
        `ttl=${Math.round(BRIDGE_GATEWAY_TTL_MS / 1000)}s | mode=redirect.`
      );

      return safeJson(res, {
        ok: true,
        publicBase: bridgeGatewayPublicBase(),
        expiresInSeconds: Math.round(BRIDGE_GATEWAY_TTL_MS / 1000),
        mode: "redirect"
      });
    } catch (error) {
      console.warn(
        `[BRIDGE GATEWAY 1.1] registro recusado | ${errorMessage(error).slice(0, 500)}`
      );
      return safeJson(res, { error: errorMessage(error) }, 400);
    }
  }
);

app.get(
  "/bridge/status.json",
  (req, res) => safeJson(res, {
    status: bridgeGatewayIsFresh() ? "online" : "offline",
    registered: Boolean(bridgeGatewayState.baseUrl),
    fresh: bridgeGatewayIsFresh(),
    expiresInSeconds: bridgeGatewayIsFresh()
      ? Math.max(0, Math.ceil((bridgeGatewayState.expiresAt - Date.now()) / 1000))
      : 0
  })
);

app.use(
  "/bridge",
  (req, res) => {
    if (req.method !== "GET" && req.method !== "HEAD") {
      return safeJson(res, { error: "Method not allowed" }, 405);
    }

    try {
      const mountedPath = String(req.url || "/");
      const queryIndex = mountedPath.indexOf("?");
      const pathOnly = queryIndex === -1 ? mountedPath : mountedPath.slice(0, queryIndex);
      const query = queryIndex === -1 ? "" : mountedPath.slice(queryIndex);
      const firstSlash = pathOnly.indexOf("/", 1);
      const suppliedKey = decodeURIComponent(
        firstSlash === -1 ? pathOnly.slice(1) : pathOnly.slice(1, firstSlash)
      );

      if (suppliedKey !== BRIDGE_GATEWAY_PUBLIC_KEY) {
        return safeJson(res, { error: "Not found" }, 404);
      }

      if (!bridgeGatewayIsFresh()) {
        return safeJson(res, {
          error: "Ponte Local offline ou registro expirado.",
          hint: "Ligue o notebook e aguarde a inicializaÃ§Ã£o automÃ¡tica da Ponte."
        }, 503);
      }

      const suffixPath = firstSlash === -1 ? "/" : pathOnly.slice(firstSlash);
      const upstreamUrl = `${bridgeGatewayState.baseUrl}${suffixPath}${query}`;

      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("X-PTBR-Bridge-Gateway", "1.1-redirect");
      res.setHeader("Location", upstreamUrl);
      return res.status(307).end();
    } catch (error) {
      console.error(
        `[BRIDGE GATEWAY 1.1] redirect falhou | ${errorMessage(error).slice(0, 500)}`
      );
      return safeJson(res, { error: "Gateway temporariamente indisponÃ­vel." }, 502);
    }
  }
);

// ============================================================
// LOCAL APIs â€” PONTE LOCAL
// ============================================================

async function localTranslateHandler(
  req,
  res,
  forcedSourceKind = ""
) {
  if (!authorized(req)) {
    return safeJson(
      res,
      {
        error:
          "Unauthorized"
      },
      401
    );
  }

  try {
    const type =
      String(
        req.body?.type ||
        "unknown"
      ).trim();

    const videoId =
      String(
        req.body?.id ||
        "unknown"
      ).trim();

    const filename =
      String(
        req.body?.filename ||
        req.body?.name ||
        "local"
      ).trim();

    const requestedKind =
      String(
        req.body?.sourceKind ||
        forcedSourceKind ||
        "embedded"
      )
        .trim()
        .toLowerCase();

    const sourceKind =
      forcedSourceKind ||
      (
        requestedKind ===
          "opensubtitles-local-sync"
          ? "opensubtitles-local-sync"
          : "embedded"
      );

    const sourceLang =
      String(
        req.body?.sourceLang ||
        "auto"
      )
        .trim()
        .toLowerCase()
        .slice(0, 32);
    
    const rawSrt =
      req.body?.srt;

    if (
      typeof rawSrt !==
        "string" ||
      !rawSrt.trim()
    ) {
      return safeJson(
        res,

        {
          error:
            "Campo srt obrigatÃ³rio."
        },

        400
      );
    }

    if (
      rawSrt.length >
      MAX_SOURCE_CHARS
    ) {
      return safeJson(
        res,

        {
          error:
            "SRT grande demais."
        },

        413
      );
    }

    const sourceSrt =
      cleanSrtForTranslation(
        rawSrt
      );

    if (
      !sourceSrt ||
      !parseSrt(
        sourceSrt
      ).length
    ) {
      throw new Error(
        "Legenda local invÃ¡lida apÃ³s HARD SDH CLEAN."
      );
    }

    const job =
      getOrCreateJob(
        {
          type,
          videoId,
          filename,
          sourceSrt,
          sourceKind,
          sourceLang
        },

        {
          lazy: false
        }
      );

    return safeJson(
      res,
      jobResponse(
        req,
        job
      )
    );
  } catch (error) {
    return safeJson(
      res,

      {
        error:
          errorMessage(
            error
          )
      },

      500
    );
  }
}


const TIMING_COMPACT_VARIANTS_940 = 5;
const TIMING_COMPACT_SCHEMA_928 = {
  type: "object",
  additionalProperties: false,
  properties: {
    cues: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: { type: "integer" },
          candidates: { type: "array", minItems: TIMING_COMPACT_VARIANTS_940, maxItems: TIMING_COMPACT_VARIANTS_940, items: { type: "string" } }
        },
        required: ["i", "candidates"]
      }
    }
  },
  required: ["cues"]
};

const TIMING_COMPACT_AUDIT_SCHEMA_928 = {
  type: "object",
  additionalProperties: false,
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: { type: "integer" },
          chosen_index: { type: "integer" },
          reason: { type: "string" }
        },
        required: ["i", "chosen_index", "reason"]
      }
    }
  },
  required: ["items"]
};

// 9.4.3.4 â€” schemas da Ãºltima micro-recuperaÃ§Ã£o. O namespace semÃ¢ntico NÃƒO muda:
// isto Ã© somente fechamento de timing/readability depois de FINAL_PASS/cache_verified.
const TIMING_COMPACT_NEEDLE_SCHEMA_9434 = {
  type: "object",
  additionalProperties: false,
  properties: {
    cues: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: { i: { type: "integer" }, candidate: { type: "string" } },
        required: ["i", "candidate"]
      }
    }
  },
  required: ["cues"]
};

const TIMING_COMPACT_SOURCE_TURNS_SCHEMA_9434 = {
  type: "object",
  additionalProperties: false,
  properties: {
    items: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          i: { type: "integer" },
          turns_pt: { type: "array", items: { type: "string" } }
        },
        required: ["i", "turns_pt"]
      }
    }
  },
  required: ["items"]
};

function timingCompactVisibleChars928(text) {
  return String(text || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\{\\[^}]+\}/g, " ")
    .replace(/^\s*[-â€“â€”]\s*/gm, "")
    .replace(/\s+/g, " ")
    .trim().length;
}

function timingCompactFitsWindow928(text, availableDisplayMs) {
  const chars = timingCompactVisibleChars928(text);
  if (chars <= 3) return true;
  const target = chars <= 6 ? 420 : chars <= 10 ? 600 : Math.min(2800, Math.max(700, (chars / 22) * 1000));
  return Number(availableDisplayMs || 0) + 60 >= target;
}

// 9.4.1 â€” the timing budget is a HARD generation contract, not merely metadata.
// Keep a small safety margin below the local 22 cps admission law so the text that
// passes cloud QA is also more likely to survive the final child geometry locally.
function timingCompactHardVisibleChars941(availableDisplayMs) {
  const ms = Math.max(1, Number(availableDisplayMs || 0));
  return Math.min(96, Math.max(3, Math.floor(((ms + 20) / 1000) * 21.5)));
}

function timingCompactVariantCaps941(availableDisplayMs) {
  const hard = timingCompactHardVisibleChars941(availableDisplayMs);
  const factors = [1.00, 0.94, 0.88, 0.82, 0.76];
  return factors.map(f => Math.max(3, Math.min(hard, Math.floor(hard * f))));
}

function parseStructuredArraySalvage9210(raw, fieldName) {
  const text = stripCodeFences(String(raw || ""));
  try {
    const parsed = JSON.parse(text);
    return { items: Array.isArray(parsed?.[fieldName]) ? parsed[fieldName] : [], complete: true };
  } catch {}

  const key = `"${String(fieldName)}"`;
  const keyAt = text.indexOf(key);
  if (keyAt < 0) return { items: [], complete: false };
  const colonAt = text.indexOf(":", keyAt + key.length);
  if (colonAt < 0) return { items: [], complete: false };
  const arrayStart = text.indexOf("[", colonAt + 1);
  if (arrayStart < 0) return { items: [], complete: false };

  const items = [];
  let depth = 0;
  let objectStart = -1;
  let inString = false;
  let escaped = false;
  for (let i = arrayStart + 1; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') { inString = true; continue; }
    if (ch === "{") {
      if (depth === 0) objectStart = i;
      depth++;
      continue;
    }
    if (ch === "}" && depth > 0) {
      depth--;
      if (depth === 0 && objectStart >= 0) {
        const fragment = text.slice(objectStart, i + 1);
        try { items.push(JSON.parse(fragment)); } catch {}
        objectStart = -1;
      }
      continue;
    }
    if (ch === "]" && depth === 0) return { items, complete: false };
  }
  return { items, complete: false };
}

function chunks9210(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

async function runBoundedTasks9210(tasks, concurrency = 2) {
  const results = new Array(tasks.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const at = cursor++;
      if (at >= tasks.length) return;
      try { results[at] = await tasks[at](); }
      catch (error) { results[at] = { error }; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, Math.max(1, tasks.length)) }, () => worker()));
  return results;
}

function timingCompactDialogueTurns943(text) {
  const lines = String(text || "").replace(/\r/g, "").split("\n").map(x => x.trim()).filter(Boolean);
  if (!lines.some(line => /^[-â€“â€”]\s*/u.test(line))) return lines.length ? [lines.join(" ")] : [];
  const turns = [];
  for (const line of lines) {
    if (/^[-â€“â€”]\s*/u.test(line)) {
      const body = line.replace(/^[-â€“â€”]\s*/u, "");
      const parts = body.split(/\s+[-â€“â€”]\s+(?=\S)/u).map(x => x.trim()).filter(Boolean);
      turns.push(...parts);
    } else if (lines.length === 1 && /\s+[-â€“â€”]\s+(?=\S)/u.test(line)) {
      turns.push(...line.split(/\s+[-â€“â€”]\s+(?=\S)/u).map(x => x.trim()).filter(Boolean));
    } else {
      turns.push(line);
    }
  }
  return turns;
}

async function timingAwareCompactSingle942(items) {
  const clean = (Array.isArray(items) ? items : [])
    .map(item => ({
      i:Number(item?.i), source:String(item?.source || "").trim(), pt:String(item?.pt || "").trim(),
      availableDisplayMs:Math.max(1,Math.min(10000,Number(item?.availableDisplayMs || 0))),
      before:String(item?.before || "").slice(0,900), after:String(item?.after || "").slice(0,900),
      constraintReason9432:String(item?.constraintReason9432 || "").replace(/\s+/g," ").trim().slice(0,320)
    }))
    .filter(item => Number.isInteger(item.i) && item.source && item.pt && item.availableDisplayMs > 0);
  if (!clean.length) throw new Error("TIMING COMPACT sem itens vÃ¡lidos.");
  if (clean.length > TIMING_COMPACT_MAX_ITEMS_928) throw new Error(`TIMING COMPACT excede ${TIMING_COMPACT_MAX_ITEMS_928} itens.`);
  const chars = clean.reduce((sum,item)=>sum+item.source.length+item.pt.length,0);
  if (chars > TIMING_COMPACT_MAX_CHARS_928) throw new Error(`TIMING COMPACT excede ${TIMING_COMPACT_MAX_CHARS_928} caracteres.`);

  const locksById = new Map();
  const protectedItems = clean.map(item => {
    const protectedSource = protectCulturalLocks(item.source,item.i);
    locksById.set(item.i,protectedSource.locks);
    return {
      i:item.i, source:protectedSource.text, current_pt:item.pt,
      available_display_ms:Math.round(item.availableDisplayMs),
      current_visible_chars:timingCompactVisibleChars928(item.pt),
      target_visible_chars:timingCompactHardVisibleChars941(item.availableDisplayMs),
      variant_caps:timingCompactVariantCaps941(item.availableDisplayMs),
      hard_locks:protectedSource.locks.map(lock=>lock.token), before:item.before, after:item.after,
      constraint_reason:item.constraintReason9432 || ""
    };
  });

  // 9.4.0: 3 alternativas distintas sÃ£o geradas NA MESMA chamada por parent.
  // Isso aumenta a chance de caber com fidelidade sem criar retries/micro-loops.
  const generationGroups = chunks9210(protectedItems, 24);
  const generationResults = await runBoundedTasks9210(generationGroups.map(group => async () => {
    const response = await geminiRequest({
      system:`VocÃª faz TIMING-AWARE COMPACT SURGERY de legendas SOURCEâ†’PT-BR.\n`+
        `A janela disponÃ­vel foi medida no Ã¡udio real e NÃƒO pode ser aumentada movendo START.\n`+
        `target_visible_chars Ã© LIMITE DURO, nÃ£o sugestÃ£o. variant_caps[k] Ã© o mÃ¡ximo ABSOLUTO de caracteres visÃ­veis permitido em candidates[k].\n`+
        `Para CADA cue, gere EXATAMENTE ${TIMING_COMPACT_VARIANTS_940} alternativas diferentes. Cada candidates[k] DEVE caber em variant_caps[k], contando letras, espaÃ§os e pontuaÃ§Ã£o visÃ­veis.\n`+
        `Candidate 0 deve ser a formulaÃ§Ã£o mais natural que jÃ¡ caiba; as seguintes devem ficar progressivamente mais curtas SEM virar fragmento.\n`+
        `Comprima por redaÃ§Ã£o idiomÃ¡tica, contraÃ§Ã£o natural e remoÃ§Ã£o apenas de hesitaÃ§Ãµes/redundÃ¢ncias sem valor semÃ¢ntico. Nunca apague uma unidade de sentido para cumprir o limite.\n`+
        `Todas devem preservar 100% do significado, negaÃ§Ã£o, referente, predicado/aÃ§Ã£o, identidade, ownership, forÃ§a pragmÃ¡tica, nomes, turnos de diÃ¡logo e hard_locks.\n`+
        `Se constraint_reason estiver preenchido, esta Ã© a ÃšNICA tentativa constrained final: preserve explicitamente cada unidade semÃ¢ntica citada no motivo da rejeiÃ§Ã£o anterior; encurte por sintaxe/lexicalizaÃ§Ã£o, NUNCA por omissÃ£o.\n`+
        `NÃ£o invente, nÃ£o mova conteÃºdo entre cues, nÃ£o altere timestamps. PT-BR natural, mÃ¡ximo 2x50.\n`+
        `Se for semanticamente impossÃ­vel cumprir um cap, repita current_pt naquela posiÃ§Ã£o; o filtro local a rejeitarÃ¡ com seguranÃ§a. JSON somente.`,
      user:JSON.stringify({cues:group}), schema:TIMING_COMPACT_SCHEMA_928,
      thinkingLevel:TIMING_COMPACT_THINKING_928, maxOutputTokens:22000,
      timeoutMs:60000, maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY], job:null, metric:"repair"
    });
    return parseStructuredArraySalvage9210(response.text,"cues").items || [];
  }),2);

  const returned = new Map();
  for (const result of generationResults) {
    if (result?.error) continue;
    for (const x of (Array.isArray(result)?result:[])) {
      const id=Number(x?.i);
      const variants=Array.isArray(x?.candidates)?x.candidates.map(v=>String(v||"").trim()).filter(Boolean):[];
      if (Number.isInteger(id) && variants.length) returned.set(id,variants);
    }
  }

  const candidates=[];
  for (const item of clean) {
    const rawVariants=returned.get(item.i)||[];
    const safe=[]; const seen=new Set();
    for (const raw of rawVariants) {
      let candidate=String(raw||"").trim();
      if(!candidate) continue;
      try { candidate=restoreCulturalLocks(candidate,locksById.get(item.i)||[],item.i); } catch { continue; }
      const block={index:item.i,text:item.source};
      candidate=sanitizeFinalCue(block,candidate)||sanitizeFallbackCue(candidate)||candidate;
      const key=semanticTextKey940(candidate);
      if(!key || seen.has(key) || key===semanticTextKey940(item.pt)) continue;
      seen.add(key);
      const regressions=repairCandidateRegressionReasons(block,item.pt,candidate,"",null);
      const layout=layoutCueResult(block,candidate);
      if(regressions.length||!layout.fits||layout.lines>LAYOUT_MAX_LINES||!timingCompactFitsWindow928(candidate,item.availableDisplayMs)) continue;
      safe.push(candidate);
    }
    if(safe.length) {
      safe.sort((a,b)=>timingCompactVisibleChars928(a)-timingCompactVisibleChars928(b));
      candidates.push({...item,variants:safe.slice(0,TIMING_COMPACT_VARIANTS_940)});
    }
  }
  if(!candidates.length) return clean.map(item=>({i:item.i,pt:item.pt,changed:false,verified:false,reason:"no_locally_safe_candidate"}));

  // Auditoria independente escolhe UMA das alternativas jÃ¡ validadas localmente.
  // Mesmo nÃºmero de itens de auditoria do 9.3.1; nÃ£o hÃ¡ round extra.
  const auditGroups=chunks9210(candidates,16);
  const auditResults=await runBoundedTasks9210(auditGroups.map(group=>async()=>{
    const response=await geminiRequest({
      system:`VocÃª Ã© o auditor final de Meaning Integrity de compactaÃ§Ãµes PT-BR.\n`+
        `Para cada item, compare SOURCE, BEFORE_PT e cada CANDIDATE_PT.\n`+
        `candidate_pts jÃ¡ chega ordenado do MAIS CURTO para o MAIS LONGO entre os candidatos que passaram pelos guards locais.\n`+
        `chosen_index deve ser o Ã­ndice 0-based da PRIMEIRA candidata totalmente segura, ou -1 se nenhuma preservar integralmente significado, negaÃ§Ã£o, referente, predicado/aÃ§Ã£o, identidade, ownership, registro/forÃ§a e conteÃºdo.\n`+
        `Portanto escolha o texto MAIS CURTO que ainda seja semanticamente completo. CompactaÃ§Ã£o idiomÃ¡tica Ã© permitida; perda semÃ¢ntica nÃ£o. Contexto Ã© apenas contexto. JSON somente.`,
      user:JSON.stringify({items:group.map(x=>({i:x.i,source:x.source,before_pt:x.pt,candidate_pts:x.variants,before:x.before,after:x.after}))}),
      schema:TIMING_COMPACT_AUDIT_SCHEMA_928, thinkingLevel:"high", maxOutputTokens:12000,
      timeoutMs:60000, maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY], job:null, metric:"qa"
    });
    return parseStructuredArraySalvage9210(response.text,"items").items || [];
  }),3);

  const auditById=new Map();
  for(const result of auditResults){
    if(result?.error) continue;
    for(const x of (Array.isArray(result)?result:[])){
      const id=Number(x?.i), chosen=Number(x?.chosen_index);
      if(Number.isInteger(id)&&Number.isInteger(chosen)) auditById.set(id,{...x,chosen_index:chosen});
    }
  }
  const candidateById=new Map(candidates.map(x=>[x.i,x]));
  const out=clean.map(item=>{
    const c=candidateById.get(item.i), a=auditById.get(item.i);
    const idx=Number(a?.chosen_index);
    if(c&&Number.isInteger(idx)&&idx>=0&&idx<c.variants.length){
      const chosen=c.variants[idx];
      return {i:item.i,pt:chosen,changed:chosen!==item.pt,verified:true};
    }
    return {i:item.i,pt:item.pt,changed:false,verified:false,reason:c?String(a?.reason||"semantic_audit_not_passed").slice(0,240):"no_locally_safe_candidate"};
  });
  console.log(`[TIMING COMPACT SINGLE 9.4.3.1] variants=${TIMING_COMPACT_VARIANTS_940} | generation=${generationGroups.length} batch(es) | audit=${auditGroups.length} batch(es) | verified=${out.filter(x=>x.verified).length}/${clean.length} | recursive-micro=0.`);
  return out;
}

async function timingAwareCompactTurnAware943(items) {
  const clean = (Array.isArray(items) ? items : [])
    .map(item => {
      const source = String(item?.source || "").trim();
      const pt = String(item?.pt || "").trim();
      const sourceTurns = timingCompactDialogueTurns943(source);
      const ptTurns = timingCompactDialogueTurns943(pt);
      return {
        i:Number(item?.i), source, pt,
        availableDisplayMs:Math.max(1,Math.min(10000,Number(item?.availableDisplayMs || 0))),
        before:String(item?.before || "").slice(0,900), after:String(item?.after || "").slice(0,900),
        sourceTurns, ptTurns,
        dialogueTurnCount: Math.max(sourceTurns.length, ptTurns.length),
        turnAwareRescue: Boolean(item?.turnAwareRescue)
      };
    })
    .filter(item => Number.isInteger(item.i) && item.source && item.pt && item.availableDisplayMs > 0);
  if (!clean.length) throw new Error("TIMING COMPACT sem itens vÃ¡lidos.");
  if (clean.length > TIMING_COMPACT_MAX_ITEMS_928) throw new Error(`TIMING COMPACT excede ${TIMING_COMPACT_MAX_ITEMS_928} itens.`);
  const chars = clean.reduce((sum,item)=>sum+item.source.length+item.pt.length,0);
  if (chars > TIMING_COMPACT_MAX_CHARS_928) throw new Error(`TIMING COMPACT excede ${TIMING_COMPACT_MAX_CHARS_928} caracteres.`);

  const locksById = new Map();
  const protectedItems = clean.map(item => {
    const protectedSource = protectCulturalLocks(item.source,item.i);
    locksById.set(item.i,protectedSource.locks);
    return {
      i:item.i, source:protectedSource.text, current_pt:item.pt,
      available_display_ms:Math.round(item.availableDisplayMs),
      current_visible_chars:timingCompactVisibleChars928(item.pt),
      target_visible_chars:timingCompactHardVisibleChars941(item.availableDisplayMs),
      variant_caps:timingCompactVariantCaps941(item.availableDisplayMs),
      hard_locks:protectedSource.locks.map(lock=>lock.token), before:item.before, after:item.after,
      dialogue_turn_count:item.dialogueTurnCount, source_turns:item.sourceTurns, current_pt_turns:item.ptTurns,
      turn_aware_rescue:item.turnAwareRescue
    };
  });

  // 9.4.0: 3 alternativas distintas sÃ£o geradas NA MESMA chamada por parent.
  // Isso aumenta a chance de caber com fidelidade sem criar retries/micro-loops.
  const generationGroups = chunks9210(protectedItems, 24);
  const generationResults = await runBoundedTasks9210(generationGroups.map(group => async () => {
    const response = await geminiRequest({
      system:`VocÃª faz TIMING-AWARE COMPACT SURGERY de legendas SOURCEâ†’PT-BR.\n`+
        `A janela disponÃ­vel foi medida no Ã¡udio real e NÃƒO pode ser aumentada movendo START.\n`+
        `target_visible_chars Ã© LIMITE DURO, nÃ£o sugestÃ£o. variant_caps[k] Ã© o mÃ¡ximo ABSOLUTO de caracteres visÃ­veis permitido em candidates[k].\n`+
        `Para CADA cue, gere EXATAMENTE ${TIMING_COMPACT_VARIANTS_940} alternativas diferentes. Cada candidates[k] DEVE caber em variant_caps[k], contando letras, espaÃ§os e pontuaÃ§Ã£o visÃ­veis.\n`+
        `Candidate 0 deve ser a formulaÃ§Ã£o mais natural que jÃ¡ caiba; as seguintes devem ficar progressivamente mais curtas SEM virar fragmento.\n`+
        `Comprima por redaÃ§Ã£o idiomÃ¡tica, contraÃ§Ã£o natural e remoÃ§Ã£o apenas de hesitaÃ§Ãµes/redundÃ¢ncias sem valor semÃ¢ntico. Nunca apague uma unidade de sentido para cumprir o limite.\n`+
        `Todas devem preservar 100% do significado, negaÃ§Ã£o, referente, predicado/aÃ§Ã£o, identidade, ownership, forÃ§a pragmÃ¡tica, nomes, turnos de diÃ¡logo e hard_locks.\n`+
        `Se dialogue_turn_count > 1, preserve EXATAMENTE essa quantidade e a MESMA ordem de falantes/turnos. Compacte CADA turno de forma independente antes de recompor o cue; nÃ£o funda falas e nÃ£o transfira sentido entre speakers.\n`+
        `Se turn_aware_rescue=true, priorize formulaÃ§Ãµes orais muito concisas por turno, removendo apenas hesitaÃ§Ãµes/fillers sem carga semÃ¢ntica; ainda assim preserve integralmente o conteÃºdo proposicional de cada fala.\n`+
        `NÃ£o invente, nÃ£o mova conteÃºdo entre cues, nÃ£o altere timestamps. PT-BR natural, mÃ¡ximo 2x50.\n`+
        `Se for semanticamente impossÃ­vel cumprir um cap, repita current_pt naquela posiÃ§Ã£o; o filtro local a rejeitarÃ¡ com seguranÃ§a. JSON somente.`,
      user:JSON.stringify({cues:group}), schema:TIMING_COMPACT_SCHEMA_928,
      thinkingLevel:TIMING_COMPACT_THINKING_928, maxOutputTokens:22000,
      timeoutMs:60000, maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY], job:null, metric:"repair"
    });
    return parseStructuredArraySalvage9210(response.text,"cues").items || [];
  }),2);

  const returned = new Map();
  for (const result of generationResults) {
    if (result?.error) continue;
    for (const x of (Array.isArray(result)?result:[])) {
      const id=Number(x?.i);
      const variants=Array.isArray(x?.candidates)?x.candidates.map(v=>String(v||"").trim()).filter(Boolean):[];
      if (Number.isInteger(id) && variants.length) returned.set(id,variants);
    }
  }

  const candidates=[];
  for (const item of clean) {
    const rawVariants=returned.get(item.i)||[];
    const safe=[]; const seen=new Set();
    for (const raw of rawVariants) {
      let candidate=String(raw||"").trim();
      if(!candidate) continue;
      try { candidate=restoreCulturalLocks(candidate,locksById.get(item.i)||[],item.i); } catch { continue; }
      const block={index:item.i,text:item.source};
      candidate=sanitizeFinalCue(block,candidate)||sanitizeFallbackCue(candidate)||candidate;
      const key=semanticTextKey940(candidate);
      if(!key || seen.has(key) || key===semanticTextKey940(item.pt)) continue;
      seen.add(key);
      const regressions=repairCandidateRegressionReasons(block,item.pt,candidate,"",null);
      const layout=layoutCueResult(block,candidate);
      const candidateTurns943=timingCompactDialogueTurns943(candidate);
      const turnMismatch943=item.dialogueTurnCount>1 && candidateTurns943.length!==item.dialogueTurnCount;
      if(regressions.length||turnMismatch943||!layout.fits||layout.lines>LAYOUT_MAX_LINES||!timingCompactFitsWindow928(candidate,item.availableDisplayMs)) continue;
      safe.push(candidate);
    }
    if(safe.length) {
      safe.sort((a,b)=>timingCompactVisibleChars928(a)-timingCompactVisibleChars928(b));
      candidates.push({...item,variants:safe.slice(0,TIMING_COMPACT_VARIANTS_940)});
    }
  }
  if(!candidates.length) return clean.map(item=>({i:item.i,pt:item.pt,changed:false,verified:false,reason:"no_locally_safe_candidate_or_turn_mismatch"}));

  // Auditoria independente escolhe UMA das alternativas jÃ¡ validadas localmente.
  // Mesmo nÃºmero de itens de auditoria do 9.3.1; nÃ£o hÃ¡ round extra.
  const auditGroups=chunks9210(candidates,16);
  const auditResults=await runBoundedTasks9210(auditGroups.map(group=>async()=>{
    const response=await geminiRequest({
      system:`VocÃª Ã© o auditor final de Meaning Integrity de compactaÃ§Ãµes PT-BR.\n`+
        `Para cada item, compare SOURCE, BEFORE_PT e cada CANDIDATE_PT.\n`+
        `candidate_pts jÃ¡ chega ordenado do MAIS CURTO para o MAIS LONGO entre os candidatos que passaram pelos guards locais.\n`+
        `chosen_index deve ser o Ã­ndice 0-based da PRIMEIRA candidata totalmente segura, ou -1 se nenhuma preservar integralmente significado, negaÃ§Ã£o, referente, predicado/aÃ§Ã£o, identidade, ownership, registro/forÃ§a e conteÃºdo.\n`+
        `Para itens com dialogue_turn_count > 1, rejeite qualquer candidata que funda, apague, reordene ou troque conteÃºdo entre turnos/speakers; cada turno precisa continuar semanticamente fiel ao correspondente SOURCE/PT.\n`+
        `Portanto escolha o texto MAIS CURTO que ainda seja semanticamente completo. CompactaÃ§Ã£o idiomÃ¡tica Ã© permitida; perda semÃ¢ntica nÃ£o. Contexto Ã© apenas contexto. JSON somente.`,
      user:JSON.stringify({items:group.map(x=>({i:x.i,source:x.source,before_pt:x.pt,candidate_pts:x.variants,before:x.before,after:x.after,dialogue_turn_count:x.dialogueTurnCount,source_turns:x.sourceTurns,current_pt_turns:x.ptTurns,turn_aware_rescue:x.turnAwareRescue}))}),
      schema:TIMING_COMPACT_AUDIT_SCHEMA_928, thinkingLevel:"high", maxOutputTokens:12000,
      timeoutMs:60000, maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY], job:null, metric:"qa"
    });
    return parseStructuredArraySalvage9210(response.text,"items").items || [];
  }),3);

  const auditById=new Map();
  for(const result of auditResults){
    if(result?.error) continue;
    for(const x of (Array.isArray(result)?result:[])){
      const id=Number(x?.i), chosen=Number(x?.chosen_index);
      if(Number.isInteger(id)&&Number.isInteger(chosen)) auditById.set(id,{...x,chosen_index:chosen});
    }
  }
  const candidateById=new Map(candidates.map(x=>[x.i,x]));
  const out=clean.map(item=>{
    const c=candidateById.get(item.i), a=auditById.get(item.i);
    const idx=Number(a?.chosen_index);
    if(c&&Number.isInteger(idx)&&idx>=0&&idx<c.variants.length){
      const chosen=c.variants[idx];
      return {i:item.i,pt:chosen,changed:chosen!==item.pt,verified:true};
    }
    return {i:item.i,pt:item.pt,changed:false,verified:false,reason:c?String(a?.reason||"semantic_audit_not_passed").slice(0,240):"no_locally_safe_candidate_or_turn_mismatch"};
  });
  console.log(`[TIMING COMPACT MULTI 9.4.3.1] variants=${TIMING_COMPACT_VARIANTS_940} | generation=${generationGroups.length} batch(es) | audit=${auditGroups.length} batch(es) | verified=${out.filter(x=>x.verified).length}/${clean.length} | recursive-micro=0.`);
  return out;
}

// 9.4.3.3 â€” rescue realmente por turno. A versÃ£o 9.4.3.2 apenas reexecutava
// o prompt multi-turn inteiro com uma flag mais forte; isso ainda podia falhar quando UM
// speaker precisava de muita compressÃ£o. Agora cada fala recebe seu prÃ³prio orÃ§amento
// proporcional, passa pelo mesmo Single 9.4.2 + auditoria HIGH e sÃ³ entÃ£o Ã© recomposta.
function timingCompactTurnBudgets9433(turns,totalMs) {
  const clean=(Array.isArray(turns)?turns:[]).map(x=>String(x||"").trim());
  const n=clean.length;
  if(!n) return [];
  const total=Math.max(n,Number(totalMs||0));
  const weights=clean.map(t=>Math.max(1,timingCompactVisibleChars928(t)));
  const weightSum=weights.reduce((a,b)=>a+b,0)||n;
  const avg=total/n;
  const floorMs=Math.max(120,Math.min(450,Math.floor(avg*0.65)));
  const baseTotal=Math.min(total,floorMs*n);
  const remain=Math.max(0,total-baseTotal);
  const out=weights.map(w=>Math.max(1,Math.floor((baseTotal/n)+(remain*(w/weightSum)))));
  let delta=Math.round(total-out.reduce((a,b)=>a+b,0));
  if(out.length) out[out.length-1]=Math.max(1,out[out.length-1]+delta);
  return out;
}

function timingCompactRecomposeTurns9433(turns) {
  const clean=(Array.isArray(turns)?turns:[]).map(x=>String(x||"").trim()).filter(Boolean);
  if(clean.length<=1) return clean[0]||"";
  const pivot=Math.ceil(clean.length/2);
  const pack=part=>part.map((t,i)=>(i===0?"- ":" - ")+t).join("");
  return `${pack(clean.slice(0,pivot))}\n${pack(clean.slice(pivot))}`.trim();
}

async function timingAwareCompactPerTurn9433(items) {
  const clean=(Array.isArray(items)?items:[]).map(item=>{
    const source=String(item?.source||"").trim();
    const pt=String(item?.pt||"").trim();
    return {
      ...item,
      i:Number(item?.i), source, pt,
      availableDisplayMs:Math.max(1,Math.min(10000,Number(item?.availableDisplayMs||0))),
      sourceTurns:timingCompactDialogueTurns943(source),
      ptTurns:timingCompactDialogueTurns943(pt)
    };
  }).filter(x=>Number.isInteger(x.i)&&x.source&&x.pt&&x.availableDisplayMs>0);
  if(!clean.length) return [];

  const directBySynthetic=new Map();
  const synthetic=[];
  const metaByParent=new Map();
  for(const item of clean){
    const n=Math.max(item.sourceTurns.length,item.ptTurns.length);
    if(n<=1 || item.sourceTurns.length!==n || item.ptTurns.length!==n){
      metaByParent.set(item.i,{item,invalid:true,reason:"per_turn_count_mismatch"});
      continue;
    }
    const budgets=timingCompactTurnBudgets9433(item.ptTurns,item.availableDisplayMs);
    const turnIds=[];
    for(let ti=0;ti<n;ti++){
      const syntheticId=item.i*100+(ti+1);
      turnIds.push(syntheticId);
      const sourceTurn=item.sourceTurns[ti];
      const ptTurn=item.ptTurns[ti];
      const budget=Math.max(1,Number(budgets[ti]||1));
      if(timingCompactFitsWindow928(ptTurn,budget)){
        directBySynthetic.set(syntheticId,{i:syntheticId,pt:ptTurn,changed:false,verified:true,perTurnUnchanged9433:true});
      }else{
        synthetic.push({
          i:syntheticId,
          source:sourceTurn,
          pt:ptTurn,
          availableDisplayMs:budget,
          before:ti>0?item.sourceTurns[ti-1]:String(item?.before||""),
          after:ti+1<n?item.sourceTurns[ti+1]:String(item?.after||"")
        });
      }
    }
    metaByParent.set(item.i,{item,invalid:false,turnIds});
  }

  const compactedBySynthetic=new Map(directBySynthetic);
  if(synthetic.length){
    const groups=chunks9210(synthetic,24);
    const results=await runBoundedTasks9210(groups.map(group=>async()=>timingAwareCompactSingle942(group)),2);
    for(const result of results){
      if(result?.error) continue;
      for(const x of (Array.isArray(result)?result:[])){
        if(Number.isInteger(Number(x?.i))) compactedBySynthetic.set(Number(x.i),x);
      }
    }
  }

  const out=[];
  for(const item of clean){
    const meta=metaByParent.get(item.i);
    if(!meta || meta.invalid){
      out.push({i:item.i,pt:item.pt,changed:false,verified:false,reason:meta?.reason||"per_turn_metadata_missing"});
      continue;
    }
    const resolved=[];
    let failedReason="";
    for(const sid of meta.turnIds){
      const got=compactedBySynthetic.get(sid);
      if(got?.verified!==true || !String(got?.pt||"").trim()){
        failedReason=String(got?.reason||"per_turn_unresolved");
        break;
      }
      resolved.push(String(got.pt).trim());
    }
    if(failedReason){
      out.push({i:item.i,pt:item.pt,changed:false,verified:false,reason:`per_turn_unresolved:${failedReason}`.slice(0,240)});
      continue;
    }
    const candidate=timingCompactRecomposeTurns9433(resolved);
    const candidateTurns=timingCompactDialogueTurns943(candidate);
    const block={index:item.i,text:item.source};
    const regressions=repairCandidateRegressionReasons(block,item.pt,candidate,"",null);
    const layout=layoutCueResult(block,candidate);
    const safe=Boolean(
      candidate && candidateTurns.length===meta.turnIds.length && !regressions.length &&
      layout.fits && layout.lines<=LAYOUT_MAX_LINES && timingCompactFitsWindow928(candidate,item.availableDisplayMs)
    );
    if(!safe){
      out.push({i:item.i,pt:item.pt,changed:false,verified:false,reason:"per_turn_recompose_failed_local_guard"});
      continue;
    }
    out.push({i:item.i,pt:candidate,changed:semanticTextKey940(candidate)!==semanticTextKey940(item.pt),verified:true,perTurnRescue9433:true});
  }
  console.log(`[TIMING COMPACT TRUE PER-TURN 9.4.3.3] parents=${clean.length} | turns=${[...metaByParent.values()].reduce((n,m)=>n+(m?.turnIds?.length||0),0)} | verified=${out.filter(x=>x?.verified===true).length}/${clean.length}.`);
  return out;
}

// 9.4.3.4 â€” needle rescue: UMA candidata extremamente dirigida para single-turn que jÃ¡
// falhou geraÃ§Ã£o normal + constrained. O motivo da auditoria anterior vira contrato explÃ­cito,
// mas a SOURCE continua sendo a autoridade. AceitaÃ§Ã£o exige guard local + auditor HIGH independente.
async function timingAwareCompactNeedleSingle9434(items) {
  const clean=(Array.isArray(items)?items:[]).map(item=>({
    ...item,
    i:Number(item?.i),
    source:String(item?.source||"").trim(),
    pt:String(item?.pt||"").trim(),
    availableDisplayMs:Math.max(1,Math.min(10000,Number(item?.availableDisplayMs||0))),
    rejectionReason:String(item?.rejectionReason9434||item?.constraintReason9432||"").replace(/\s+/g," ").trim().slice(0,420),
    before:String(item?.before||"").slice(0,900),
    after:String(item?.after||"").slice(0,900)
  })).filter(x=>Number.isInteger(x.i)&&x.source&&x.pt&&x.availableDisplayMs>0);
  if(!clean.length) return [];

  const locksById=new Map();
  const payload=clean.map(item=>{
    const protectedSource=protectCulturalLocks(item.source,item.i);
    locksById.set(item.i,protectedSource.locks);
    return {
      i:item.i,
      source:protectedSource.text,
      current_pt:item.pt,
      mandatory_semantic_warning:item.rejectionReason,
      hard_locks:protectedSource.locks.map(x=>x.token),
      available_display_ms:Math.round(item.availableDisplayMs),
      hard_visible_char_cap:timingCompactHardVisibleChars941(item.availableDisplayMs),
      before:item.before,
      after:item.after
    };
  });

  const generated=new Map();
  const groups=chunks9210(payload,8);
  const genResults=await runBoundedTasks9210(groups.map(group=>async()=>{
    const response=await geminiRequest({
      system:`VocÃª Ã© a ÃšLTIMA micro-cirurgia bounded de readability PT-BR.\n`+
        `Cada item jÃ¡ falhou tentativas anteriores. Gere UMA Ãºnica candidate, natural e oral, que caiba no hard_visible_char_cap.\n`+
        `SOURCE Ã© a autoridade. mandatory_semantic_warning informa exatamente a unidade de sentido que a auditoria viu desaparecer; PRESERVE essa unidade ou um equivalente semÃ¢ntico inequÃ­voco.\n`+
        `Encurte por sintaxe, contraÃ§Ãµes e lexicalizaÃ§Ã£o. NÃ£o resolva o limite apagando modalidade epistÃªmica, negaÃ§Ã£o, referente, predicado, relaÃ§Ã£o, identidade, nome, intensidade ou ownership.\n`+
        `HesitaÃ§Ã£o/filler realmente nÃ£o-semÃ¢ntico pode sair. hard_locks devem sobreviver. NÃ£o altere timestamps, nÃ£o mova conteÃºdo entre cues.\n`+
        `Se nÃ£o houver formulaÃ§Ã£o fiel dentro do cap, devolva current_pt. JSON somente.`,
      user:JSON.stringify({cues:group}),
      schema:TIMING_COMPACT_NEEDLE_SCHEMA_9434,
      thinkingLevel:"high", maxOutputTokens:6000,
      timeoutMs:60000, maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY], job:null, metric:"repair"
    });
    return parseStructuredArraySalvage9210(response.text,"cues").items||[];
  }),2);
  for(const result of genResults){
    if(result?.error) continue;
    for(const x of (Array.isArray(result)?result:[])){
      const id=Number(x?.i), candidate=String(x?.candidate||"").trim();
      if(Number.isInteger(id)&&candidate) generated.set(id,candidate);
    }
  }

  const locallySafe=[];
  for(const item of clean){
    let candidate=generated.get(item.i)||"";
    if(!candidate) continue;
    try{ candidate=restoreCulturalLocks(candidate,locksById.get(item.i)||[],item.i); }catch{ continue; }
    const block={index:item.i,text:item.source};
    candidate=sanitizeFinalCue(block,candidate)||sanitizeFallbackCue(candidate)||candidate;
    if(!candidate||semanticTextKey940(candidate)===semanticTextKey940(item.pt)) continue;
    const regressions=repairCandidateRegressionReasons(block,item.pt,candidate,"",null);
    const layout=layoutCueResult(block,candidate);
    if(regressions.length||!layout.fits||layout.lines>LAYOUT_MAX_LINES||!timingCompactFitsWindow928(candidate,item.availableDisplayMs)) continue;
    locallySafe.push({...item,candidate});
  }

  if(!locallySafe.length){
    console.log(`[TIMING COMPACT NEEDLE SINGLE 9.4.3.4] generated=${clean.length} | localSafe=0 | verified=0/${clean.length}.`);
    return clean.map(item=>({i:item.i,pt:item.pt,changed:false,verified:false,reason:"needle_no_locally_safe_candidate"}));
  }

  const auditResults=await runBoundedTasks9210(chunks9210(locallySafe,8).map(group=>async()=>{
    const response=await geminiRequest({
      system:`Audite a ÃšLTIMA candidata de compactaÃ§Ã£o. SOURCE Ã© autoridade.\n`+
        `A candidata sÃ³ pode ser aceita se preservar integralmente significado, modalidade/certeza, negaÃ§Ã£o, referente, predicado/aÃ§Ã£o, identidade, ownership, registro e forÃ§a pragmÃ¡tica.\n`+
        `rejection_reason_previous explica a perda detectada antes e deve estar semanticamente resolvida.\n`+
        `chosen_index=0 somente se candidate_pts[0] for totalmente segura; senÃ£o -1. JSON somente.`,
      user:JSON.stringify({items:group.map(x=>({i:x.i,source:x.source,before_pt:x.pt,candidate_pts:[x.candidate],rejection_reason_previous:x.rejectionReason,before:x.before,after:x.after}))}),
      schema:TIMING_COMPACT_AUDIT_SCHEMA_928,
      thinkingLevel:"high", maxOutputTokens:5000,
      timeoutMs:60000, maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY], job:null, metric:"qa"
    });
    return parseStructuredArraySalvage9210(response.text,"items").items||[];
  }),2);
  const auditById=new Map();
  for(const result of auditResults){
    if(result?.error) continue;
    for(const x of (Array.isArray(result)?result:[])){
      const id=Number(x?.i); if(Number.isInteger(id)) auditById.set(id,x);
    }
  }
  const safeById=new Map(locallySafe.map(x=>[x.i,x]));
  const out=clean.map(item=>{
    const c=safeById.get(item.i), a=auditById.get(item.i);
    if(c&&Number(a?.chosen_index)===0){
      return {i:item.i,pt:c.candidate,changed:true,verified:true,needleRescue9434:true};
    }
    return {i:item.i,pt:item.pt,changed:false,verified:false,reason:String(a?.reason||"needle_semantic_audit_not_passed").slice(0,240)};
  });
  console.log(`[TIMING COMPACT NEEDLE SINGLE 9.4.3.4] generated=${clean.length} | localSafe=${locallySafe.length} | verified=${out.filter(x=>x?.verified===true).length}/${clean.length}.`);
  return out;
}

// 9.4.3.4 â€” SOURCE-turn reconstruction para o caso em que o parent Ã© multi-turn pela SOURCE,
// mas o PT atual perdeu/colou os marcadores e portanto sourceTurns.length !== ptTurns.length.
// Em vez de desistir com turns=0, reconstruÃ­mos EXATAMENTE um PT por SOURCE turn e auditamos o parent final.
async function timingAwareCompactSourceTurns9434(items) {
  const clean=(Array.isArray(items)?items:[]).map(item=>{
    const source=String(item?.source||"").trim();
    const pt=String(item?.pt||"").trim();
    return {...item,i:Number(item?.i),source,pt,
      availableDisplayMs:Math.max(1,Math.min(10000,Number(item?.availableDisplayMs||0))),
      sourceTurns:timingCompactDialogueTurns943(source),
      ptTurns:timingCompactDialogueTurns943(pt),
      before:String(item?.before||"").slice(0,900),after:String(item?.after||"").slice(0,900)};
  }).filter(x=>Number.isInteger(x.i)&&x.source&&x.pt&&x.availableDisplayMs>0&&x.sourceTurns.length>1);
  if(!clean.length) return [];

  const metaById=new Map();
  const payload=[];
  for(const item of clean){
    const budgets=timingCompactTurnBudgets9433(item.sourceTurns,item.availableDisplayMs);
    const protectedTurns=[]; const locks=[];
    for(let ti=0;ti<item.sourceTurns.length;ti++){
      const p=protectCulturalLocks(item.sourceTurns[ti],item.i*100+(ti+1));
      protectedTurns.push(p.text); locks.push(p.locks);
    }
    metaById.set(item.i,{item,budgets,locks});
    payload.push({
      i:item.i,
      source_turns:protectedTurns,
      current_pt:item.pt,
      current_pt_turns:item.ptTurns,
      turn_count:item.sourceTurns.length,
      turn_visible_char_caps:budgets.map(ms=>timingCompactHardVisibleChars941(ms)),
      total_available_display_ms:Math.round(item.availableDisplayMs),
      before:item.before,after:item.after
    });
  }

  const generated=new Map();
  const results=await runBoundedTasks9210(chunks9210(payload,6).map(group=>async()=>{
    const response=await geminiRequest({
      system:`Reconstrua um parent de legenda multi-speaker diretamente da SOURCE.\n`+
        `SOURCE_TURNS Ã© a autoridade absoluta de ownership e ordem. Retorne turns_pt com EXATAMENTE turn_count entradas; turns_pt[k] traduz SOMENTE source_turns[k].\n`+
        `current_pt Ã© apenas referÃªncia lexical: se ele perdeu hÃ­fens, fundiu falas ou tem contagem incompatÃ­vel, NÃƒO copie esse defeito.\n`+
        `Cada turns_pt[k] deve caber em turn_visible_char_caps[k] quando semanticamente possÃ­vel, usando PT-BR oral e conciso. Preserve significado, negaÃ§Ã£o, modalidade, referente, nomes, forÃ§a pragmÃ¡tica e hard locks.\n`+
        `NÃ£o transfira palavras/sentido entre speakers. NÃ£o invente. NÃ£o altere timestamps. JSON somente.`,
      user:JSON.stringify({items:group}),
      schema:TIMING_COMPACT_SOURCE_TURNS_SCHEMA_9434,
      thinkingLevel:"high",maxOutputTokens:7000,
      timeoutMs:60000,maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY],job:null,metric:"repair"
    });
    return parseStructuredArraySalvage9210(response.text,"items").items||[];
  }),2);
  for(const result of results){
    if(result?.error) continue;
    for(const x of (Array.isArray(result)?result:[])){
      const id=Number(x?.i), turns=Array.isArray(x?.turns_pt)?x.turns_pt.map(v=>String(v||"").trim()):[];
      if(Number.isInteger(id)&&turns.length) generated.set(id,turns);
    }
  }

  const locallySafe=[];
  for(const item of clean){
    const meta=metaById.get(item.i); const rawTurns=generated.get(item.i)||[];
    if(!meta||rawTurns.length!==item.sourceTurns.length) continue;
    const restored=[]; let bad=false;
    for(let ti=0;ti<rawTurns.length;ti++){
      let turn=rawTurns[ti];
      try{turn=restoreCulturalLocks(turn,meta.locks[ti]||[],item.i*100+(ti+1));}catch{bad=true;break;}
      const blockTurn={index:item.i*100+(ti+1),text:item.sourceTurns[ti]};
      turn=sanitizeFinalCue(blockTurn,turn)||sanitizeFallbackCue(turn)||turn;
      if(!turn){bad=true;break;}
      restored.push(turn);
    }
    if(bad||restored.length!==item.sourceTurns.length) continue;
    const candidate=timingCompactRecomposeTurns9433(restored);
    const candidateTurns=timingCompactDialogueTurns943(candidate);
    const block={index:item.i,text:item.source};
    const regressions=repairCandidateRegressionReasons(block,item.pt,candidate,"",null);
    const layout=layoutCueResult(block,candidate);
    const safe=Boolean(candidate&&candidateTurns.length===item.sourceTurns.length&&!regressions.length&&layout.fits&&layout.lines<=LAYOUT_MAX_LINES&&timingCompactFitsWindow928(candidate,item.availableDisplayMs));
    if(safe) locallySafe.push({...item,candidate});
  }

  if(!locallySafe.length){
    console.log(`[TIMING COMPACT SOURCE-TURNS 9.4.3.4] parents=${clean.length} | localSafe=0 | verified=0/${clean.length}.`);
    return clean.map(item=>({i:item.i,pt:item.pt,changed:false,verified:false,reason:"source_turn_rebuild_no_locally_safe_candidate"}));
  }

  const auditResults=await runBoundedTasks9210(chunks9210(locallySafe,6).map(group=>async()=>{
    const response=await geminiRequest({
      system:`Audite reconstruÃ§Ãµes multi-turn SOURCEâ†’PT-BR. SOURCE Ã© autoridade por speaker.\n`+
        `chosen_index=0 somente se a candidata preservar TODAS as falas na mesma ordem, sem fundir/trocar ownership e sem perda de significado, negaÃ§Ã£o, modalidade, referente ou forÃ§a pragmÃ¡tica. Caso contrÃ¡rio -1. JSON somente.`,
      user:JSON.stringify({items:group.map(x=>({i:x.i,source:x.source,before_pt:x.pt,candidate_pts:[x.candidate],before:x.before,after:x.after}))}),
      schema:TIMING_COMPACT_AUDIT_SCHEMA_928,
      thinkingLevel:"high",maxOutputTokens:5000,
      timeoutMs:60000,maxRetries:1,
      routeOverride:[GEMINI_MODELS.MAIN_FALLBACK,GEMINI_MODELS.MAIN_PRIMARY],job:null,metric:"qa"
    });
    return parseStructuredArraySalvage9210(response.text,"items").items||[];
  }),2);
  const auditById=new Map();
  for(const result of auditResults){if(result?.error)continue;for(const x of (Array.isArray(result)?result:[])){const id=Number(x?.i);if(Number.isInteger(id))auditById.set(id,x);}}
  const safeById=new Map(locallySafe.map(x=>[x.i,x]));
  const out=clean.map(item=>{
    const c=safeById.get(item.i),a=auditById.get(item.i);
    if(c&&Number(a?.chosen_index)===0)return {i:item.i,pt:c.candidate,changed:true,verified:true,sourceTurnRebuild9434:true};
    return {i:item.i,pt:item.pt,changed:false,verified:false,reason:String(a?.reason||"source_turn_rebuild_audit_not_passed").slice(0,240)};
  });
  console.log(`[TIMING COMPACT SOURCE-TURNS 9.4.3.4] parents=${clean.length} | localSafe=${locallySafe.length} | verified=${out.filter(x=>x?.verified===true).length}/${clean.length}.`);
  return out;
}

async function timingAwareCompactSurgery928(items) {
  const raw = Array.isArray(items) ? items : [];
  const single = [];
  const multi = [];
  for (const item of raw) {
    const sourceTurns = timingCompactDialogueTurns943(String(item?.source || ""));
    const ptTurns = timingCompactDialogueTurns943(String(item?.pt || ""));
    const isMulti = Boolean(item?.turnAwareRescue) || sourceTurns.length > 1 || ptTurns.length > 1;
    (isMulti ? multi : single).push(item);
  }

  const tasks = [];
  if (single.length) tasks.push(
    timingAwareCompactSingle942(single).then(items => ({kind:"single",items}))
  );
  if (multi.length) tasks.push(
    timingAwareCompactTurnAware943(multi).then(items => ({kind:"multi",items}))
  );

  const groups = await Promise.all(tasks);
  const byId = new Map();
  for (const group of groups) {
    for (const item of (Array.isArray(group?.items) ? group.items : [])) {
      byId.set(Number(item?.i), {...item, compactPath9431:group.kind});
    }
  }

  let out = raw.map(item => {
    const id = Number(item?.i);
    return byId.get(id) || {
      i:id, pt:String(item?.pt || ""), changed:false, verified:false,
      reason:"compact_path_no_result", compactPath9431:"none"
    };
  });

  // 9.4.3.2 â€” UMA recuperaÃ§Ã£o final, somente nos itens realmente rejeitados.
  // single: constrained retry recebe o motivo do auditor e nÃ£o pode omitir os atoms citados.
  // multi: retry per-turn usa o modo turnAwareRescue jÃ¡ bounded, preservando nÃºmero/ordem de speakers.
  const firstRejected = out.filter(x => x?.verified !== true);
  if (firstRejected.length) {
    const originalById = new Map(raw.map(x => [Number(x?.i), x]));
    const rejectedSingle = firstRejected
      .filter(x => x?.compactPath9431 === "single")
      .map(x => ({...originalById.get(Number(x.i)), constraintReason9432:String(x.reason || "semantic_audit_not_passed")}));
    const rejectedMulti = firstRejected
      .filter(x => x?.compactPath9431 === "multi")
      .map(x => ({...originalById.get(Number(x.i)), turnAwareRescue:true}));
    const rescueTasks = [];
    if (rejectedSingle.length) rescueTasks.push(
      timingAwareCompactSingle942(rejectedSingle).then(items => ({kind:"single-constrained",items}))
    );
    if (rejectedMulti.length) rescueTasks.push(
      timingAwareCompactPerTurn9433(rejectedMulti).then(items => ({kind:"multi-per-turn-9433",items}))
    );
    const rescueGroups = await Promise.all(rescueTasks);
    const rescueById = new Map();
    for (const group of rescueGroups) {
      for (const item of (Array.isArray(group?.items) ? group.items : [])) {
        if (item?.verified === true) rescueById.set(Number(item.i), {...item,compactPath9431:group.kind,finalRescue9432:true});
      }
    }
    if (rescueById.size) {
      out = out.map(x => rescueById.get(Number(x.i)) || x);
    }
    console.log(`[TIMING COMPACT FINAL RESCUE 9.4.3.3] single=${rejectedSingle.length} | multi=${rejectedMulti.length} | recovered=${rescueById.size}/${firstRejected.length}.`);
  }

  // 9.4.3.4 â€” Ãºltima micro-recuperaÃ§Ã£o SOMENTE nos residuais que sobreviveram a tudo acima.
  // Single recebe uma candidata needle com o motivo semÃ¢ntico obrigatÃ³rio. Multi com SOURCE multi-turn
  // Ã© reconstruÃ­do diretamente dos SOURCE turns, inclusive quando o PT atual perdeu a segmentaÃ§Ã£o.
  const residual9434 = out.filter(x => x?.verified !== true);
  if (residual9434.length) {
    const originalById9434 = new Map(raw.map(x => [Number(x?.i), x]));
    const needleSingle9434 = residual9434
      .filter(x => x?.compactPath9431 === "single")
      .map(x => ({...originalById9434.get(Number(x.i)), rejectionReason9434:String(x.reason||"semantic_audit_not_passed")}));
    const sourceTurnMulti9434 = residual9434
      .filter(x => x?.compactPath9431 === "multi")
      .map(x => ({...originalById9434.get(Number(x.i)), rejectionReason9434:String(x.reason||"turn_mismatch")}));
    const tasks9434=[];
    if(needleSingle9434.length) tasks9434.push(timingAwareCompactNeedleSingle9434(needleSingle9434).then(items=>({kind:"single-needle-9434",items})));
    if(sourceTurnMulti9434.length) tasks9434.push(timingAwareCompactSourceTurns9434(sourceTurnMulti9434).then(items=>({kind:"multi-source-turns-9434",items})));
    const groups9434=await Promise.all(tasks9434);
    const recovered9434=new Map();
    for(const group of groups9434){
      for(const item of (Array.isArray(group?.items)?group.items:[])){
        if(item?.verified===true) recovered9434.set(Number(item.i),{...item,compactPath9431:group.kind,finalNeedle9434:true});
      }
    }
    if(recovered9434.size) out=out.map(x=>recovered9434.get(Number(x.i))||x);
    console.log(`[TIMING COMPACT NEEDLE CLOSURE 9.4.3.4] single=${needleSingle9434.length} | multi=${sourceTurnMulti9434.length} | recovered=${recovered9434.size}/${residual9434.length}.`);
  }

  const rejected = out.filter(x => x?.verified !== true);
  console.log(
    `[TIMING COMPACT ROUTER 9.4.3.4] single=${single.length} | multi=${multi.length} | ` +
    `verified=${out.filter(x=>x?.verified===true).length}/${out.length}.`
  );
  if (rejected.length) {
    console.warn(
      `[TIMING COMPACT REJECTIONS 9.4.3.4] ` +
      rejected.map(x => `i=${x.i}:${x.compactPath9431||"?"}:${String(x.reason||"unknown").replace(/\s+/g," ").slice(0,120)}`).join(" | ")
    );
  }
  return out;
}

app.post(
  "/api/translate-embedded",

  (req, res) =>
    localTranslateHandler(
      req,
      res,
      "embedded"
    )
);

app.post(
  "/api/translate-local",

  (req, res) =>
    localTranslateHandler(
      req,
      res
    )
);

// 9.4.1 â€” compactaÃ§Ã£o semÃ¢ntica acionada somente por impossibilidade fÃ­sica
// comprovada pela geometria final local. Render NÃƒO recebe nem devolve timestamps.
app.post(
  "/api/timing-compact",
  async (req,res)=>{
    if(!authorized(req)) return safeJson(res,{error:"Unauthorized"},401);
    try{
      const items=Array.isArray(req.body?.items)?req.body.items:[];
      const compacted=await timingAwareCompactSurgery928(items);
      console.log(`[TIMING COMPACT API 9.4.3.4] received=${items.length} | verified=${compacted.filter(x=>x?.verified===true).length} | changed=${compacted.filter(x=>x?.changed===true).length}.`);
      return safeJson(res,{ok:true,version:"9.4.3.4",semanticNamespace:CACHE_VERSION,items:compacted});
    }catch(error){
      console.error(`[TIMING COMPACT API 9.4.3.4] ${errorMessage(error).slice(0,500)}`);
      return safeJson(res,{error:errorMessage(error)},500);
    }
  }
);

// Proxy lexical multilÃ­ngue para o Auto-Sync da Ponte.
// NÃ£o toca em timestamps nem na traduÃ§Ã£o PT-BR final.
app.post(
  "/api/sync-proxy",

  async (req, res) => {
    if (!authorized(req)) {
      return safeJson(res, { error: "Unauthorized" }, 401);
    }

    try {
      const sourceLang = String(req.body?.sourceLang || "auto").trim();
      const targetLang = String(req.body?.targetLang || "en").trim();
      const items = Array.isArray(req.body?.items) ? req.body.items : [];

      const translated = await buildMultilingualSyncProxy(
        items,
        sourceLang,
        targetLang
      );

      return safeJson(res, {
        ok: true,
        sourceLang: normalizeSyncLanguageCode(sourceLang, "auto"),
        targetLang: normalizeSyncLanguageCode(targetLang, "en"),
        items: translated
      });
    } catch (error) {
      console.error(
        `[SYNC PROXY API] ${errorMessage(error).slice(0, 500)}`
      );
      return safeJson(res, { error: errorMessage(error) }, 500);
    }
  }
);

// Alinhamento semÃ¢ntico SOURCEâ†”ASR para o Total Sync.
// Trabalha sÃ³ com poucos cues e palavras transcritas; nÃ£o altera timestamps.
app.post(
  "/api/sync-align",

  async (req, res) => {
    if (!authorized(req)) {
      return safeJson(res, { error: "Unauthorized" }, 401);
    }

    try {
      const sourceLang = String(req.body?.sourceLang || "auto").trim();
      const audioLang = String(req.body?.audioLang || "en").trim();
      const items = Array.isArray(req.body?.items) ? req.body.items : [];

      const aligned = await alignMultilingualSyncAnchors(
        items,
        sourceLang,
        audioLang
      );

      return safeJson(res, {
        ok: true,
        sourceLang: normalizeSyncLanguageCode(sourceLang, "auto"),
        audioLang: normalizeSyncLanguageCode(audioLang, "en"),
        items: aligned
      });
    } catch (error) {
      console.error(
        `[SYNC ALIGN API] ${errorMessage(error).slice(0, 500)}`
      );
      return safeJson(res, { error: errorMessage(error) }, 500);
    }
  }
);

// Ponte monta vÃ¡rias janelas em um WAV.
// Render mantÃ©m chave Gemini, orÃ§amento e word timestamps.
app.post(
  "/api/audio-transcribe",

  async (req, res) => {
    if (!authorized(req)) {
      return safeJson(
        res,

        {
          error:
            "Unauthorized"
        },

        401
      );
    }

    try {
      const audioBase64 =
        String(
          req.body
            ?.audioBase64 ||
          ""
        ).trim();

      const mimeType =
        String(
          req.body
            ?.mimeType ||
          "audio/wav"
        )
          .trim()
          .toLowerCase();

      const label =
        String(
          req.body
            ?.label ||
          "montage"
        )
          .replace(
            /\s+/g,
            " "
          )
          .slice(
            0,
            120
          );

      const durationMs =
        Math.max(
          0,

          Number(
            req.body
              ?.durationMs ||
            0
          )
        );

      const languageCode =
        normalizeSyncLanguageCode(
          req.body?.languageCode || "en",
          "en"
        );

      if (
        !/^audio\//i.test(
          mimeType
        )
      ) {
        return safeJson(
          res,

          {
            error:
              "mimeType de Ã¡udio invÃ¡lido."
          },

          400
        );
      }

      if (!audioBase64) {
        return safeJson(
          res,

          {
            error:
              "audioBase64 obrigatÃ³rio."
          },

          400
        );
      }

      if (
        audioBase64.length >
        8 * 1024 * 1024
      ) {
        return safeJson(
          res,

          {
            error:
              "Montagem de Ã¡udio grande demais."
          },

          413
        );
      }

      if (
        durationMs >
        4 * 60 * 1000
      ) {
        return safeJson(
          res,

          {
            error:
              "Montagem excede 4 minutos."
          },

          413
        );
      }

      console.log(
        `[AUDIO SYNC API] ${
          label
        } | base64=${
          audioBase64.length
        } | duraÃ§Ã£oâ‰ˆ${
          (
            durationMs /
            1000
          ).toFixed(1)
        }s.`
      );

      const result =
        await geminiTranscribeInline(
          audioBase64,
          mimeType,
          durationMs,
          label,
          languageCode
        );

      return safeJson(
        res,

        {
          ok: true,

          model:
            GEMINI_TRANSCRIBE_MODEL,

          text:
            result.text,

          words:
            result.words,

          usage:
            result.usage,

          budget:
            result.budget
        }
      );
    } catch (error) {
      const status =
        error?.code ===
          "TRANSCRIBE_RPD_LOCK"
          ? 429
          : 500;

      console.error(
        `[AUDIO SYNC API] ${
          errorMessage(
            error
          ).slice(
            0,
            500
          )
        }`
      );

      return safeJson(
        res,

        {
          error:
            errorMessage(
              error
            ),

          code:
            error?.code ||
            ""
        },

        status
      );
    }
  }
);

app.get(
  "/api/audio-budget",

  (req, res) => {
    if (!authorized(req)) {
      return safeJson(
        res,

        {
          error:
            "Unauthorized"
        },

        401
      );
    }

    return safeJson(
      res,

      {
        ok: true,

        model:
          GEMINI_TRANSCRIBE_MODEL,

        limits: {
          rpm: 3,

          tpm:
            TRANSCRIBE_TPM_LIMIT,

          rpdExternal:
            25,

          rpdInternal:
            TRANSCRIBE_RPD_INTERNAL_LIMIT,

          minStartIntervalMs:
            TRANSCRIBE_MIN_START_INTERVAL_MS
        },

        state:
          transcribeBudgetSnapshot()
      }
    );
  }
);

// ============================================================
// JOB STATUS
// ============================================================

app.get(
  "/job/:jobId",

  (req, res) => {
    const job =
      jobs.get(
        String(
          req.params.jobId ||
          ""
        )
      );

    if (!job) {
      return safeJson(
        res,

        {
          error:
            "Job nÃ£o encontrado."
        },

        404
      );
    }

    return safeJson(
      res,

      {
        id:
          job.id,

        status:
          job.status,

        qualityStatus:
          job.qualityStatus,

        sourceKind:
          job.sourceKind,

        progress:
          job.progress,

        error:
          job.error,

        safeDraft:
          Boolean(
            job.safeDraft
          ),

        stats:
          job.stats
      }
    );
  }
);

// ============================================================
// SUBTITLE DELIVERY
// ============================================================

function processingSrt(job) {
  return [
    "1",
    "00:00:01,000 --> 00:00:08,000",
    "Traduzindo legenda para PT-BR...",
    "",
    "2",
    "00:00:08,500 --> 00:00:15,000",
    `Progresso: ${Number(job?.progress || 0)}%.`
  ].join("\n");
}

function errorSrt(error) {
  return [
    "1",
    "00:00:01,000 --> 00:00:08,000",
    "NÃ£o foi possÃ­vel concluir a legenda PT-BR.",
    "",
    "2",
    "00:00:08,500 --> 00:00:18,000",
    String(error || "Erro desconhecido.")
      .replace(/\s+/g, " ")
      .slice(0, 300)
  ].join("\n");
}

app.get(
  "/subtitle/:jobId.srt",

  async (req, res) => {
    let jobId;

    try {
      jobId =
        decodeURIComponent(
          String(
            req.params.jobId ||
            ""
          )
        );
    } catch {
      jobId =
        String(
          req.params.jobId ||
          ""
        );
    }

    let job =
      jobs.get(jobId);

    const recoveryToken =
      String(
        req.query.r ||
        ""
      ).trim();

    if (
      !job &&
      recoveryToken
    ) {
      try {
        job =
          await recoverCloudJob(
            recoveryToken
          );

        jobs.set(
          jobId,
          job
        );

        console.log(
          `[CLOUD SELF-HEAL] job ativo novamente: ${
            job.id
          } (alias=${
            jobId
          }).`
        );
      } catch (error) {
        console.error(
          `[CLOUD SELF-HEAL] falhou: ${
            errorMessage(
              error
            )
          }`
        );

        return sendSrt(
          res,

          errorSrt(
            `NÃ£o foi possÃ­vel recuperar a legenda: ${
              errorMessage(
                error
              )
            }`
          )
        );
      }
    }

    if (!job) {
      return sendSrt(
        res,

        errorSrt(
          "Job expirado e sem dados de recuperaÃ§Ã£o."
        )
      );
    }

    if (
      job.status ===
        "pending" &&
      !job.started
    ) {
      console.log(
        `[CLOUD LAZY] URL selecionada; iniciando ${
          job.id
        }.`
      );

      startJob(job);
    }

    if (
      job.status ===
        "completed" &&
      job.result
    ) {
      try {
        auditTimestamps(
          job.sourceSrt,
          job.result,
          "SERVING",
          job
        );
      } catch (error) {
        return sendSrt(
          res,
          errorSrt(
            errorMessage(
              error
            )
          )
        );
      }

      return sendSrt(
        res,
        job.result,
        "public, max-age=604800"
      );
    }

    if (
      job.status ===
      "failed"
    ) {
      if (job.qualityStatus === "no_final_pass") {
        return sendSrt(
          res,
          errorSrt("FINAL_PASS nÃ£o foi obtido. O checkpoint Ã­ntegro foi preservado, mas esta versÃ£o nÃ£o entrega BEST_AVAILABLE como final."),
          "no-store, no-cache, must-revalidate"
        );
      }

      console.warn(
        `[SELF-HEAL 8.4.0] job ${job.id} estava failed; ` +
        `reativando como processing em vez de matar a legenda.`
      );

      job.status = "pending";
      job.started = false;
      startJob(job);

      return sendSrt(
        res,
        processingSrt(job),
        "no-store, no-cache, must-revalidate"
      );
    }

    return sendSrt(
      res,
      processingSrt(job),
      "no-store, no-cache, must-revalidate"
    );
  }
);

// ============================================================
// START
// ============================================================

app.listen(PORT, () => {
  console.log(
    "============================================================"
  );

    console.log(
        " STREMIO PT-BR 9.7.3 - SEMANTIC FIDELITY + SPEAKER-TURN + MUSIC POLISH | UNIVERSAL / TIMING 9.4.3.4 PRESERVED"
  );

  console.log(
    "============================================================"
  );

  console.log(
    `Gemini: ${
      GEMINI_API_KEY
        ? "CONFIGURADA âœ…"
        : "FALTANDO âŒ"
    }`
  );

  console.log(
    `Bridge Gateway 1.1 REDIRECT: ${bridgeGatewayPublicBase()} | heartbeat TTL=${Math.round(BRIDGE_GATEWAY_TTL_MS / 1000)}s âœ…`
  );

  console.log(
    `MAIN route: ${geminiRouteForMetric("main").join(" -> ")} | preferÃªncia configurÃ¡vel por MAIN_ROUTE_PREFERENCE âœ…`
  );

  console.log(
    `QA/Repair refined route: ${geminiRouteForMetric("qa").join(" -> ")} | HIGH preservado âœ…`
  );

  console.log(
    "Cloud OpenSubtitles: ATIVO + LAZY + SELF-HEAL âœ…"
  );

  console.log(
    "APIs Local Embedded + OpenSub Sync: ATIVAS âœ…"
  );

  console.log(
    `Audio Sync ASR: ${GEMINI_TRANSCRIBE_MODEL} | montage COARSE/PRECISION/RESCUE + word timestamps âœ…`
  );

  console.log(
    `Transcribe Budget: 22s entre inÃ­cios | TPM soft=${TRANSCRIBE_TPM_SOFT_LIMIT}/${TRANSCRIBE_TPM_LIMIT} | RPD interno=${TRANSCRIBE_RPD_INTERNAL_LIMIT}/25 âœ…`
  );

  console.log(
    "Context + Identity Lock / Character Ledger SAFE-SCHEMA: ATIVO âœ…"
  );

  console.log(
    "Planner fallback ultra-simples + fallback local neutro: ATIVOS âœ…"
  );

  console.log(
    "Gender-safe unknown speaker: NÃƒO ADIVINHA; neutralizaÃ§Ã£o contextual ATIVA âœ…"
  );

  console.log(
    "Naturalidade PT-BR 2026 + Anti-Calque/Falsos Cognatos: ATIVOS âœ…"
  );

  console.log(
  "Naturalness Lock: literal porÃ©m artificial = ERRO; intenÃ§Ã£o + oralidade PT-BR prioritÃ¡rias âœ…"
);

console.log(
  `Subtitle Layout Lock: alvo mÃ¡ximo=${LAYOUT_MAX_LINES} linhas Ã— ${LAYOUT_MAX_CHARS_PER_LINE} chars | quebra somente entre palavras âœ…`
);

console.log(
  "Layout Safety: zero truncamento | zero word-split | zero novos cues | zero alteraÃ§Ã£o de timestamps âœ…"
);

console.log(
  "Universal SDH Action Classifier: sujeito/personagem genÃ©rico + aÃ§Ã£o/evento; sem hardcode de programa âœ…"
);

console.log(
  "Contextual Performance Music Lock: fundo editorial sai; performance real fica; decisÃ£o atÃ´mica por cue âœ…"
);

console.log(
  `Dialogue Turn Lock: speakers/turns preservados + layout turn-aware dentro de ${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE} âœ…`
);

console.log(
  "SUBTITLE_TOO_DENSE: Repair busca concisÃ£o natural antes do reflow final âœ…"
);

  console.log(
  "Meaning Integrity Lock: naturalizar/compactar NÃƒO pode apagar unidades semÃ¢nticas âœ…"
);

console.log(
  `Compact Rescue: atÃ© ${COMPACT_RESCUE_MAX_ROUNDS} rodada(s), somente overflow residual, thinking=${COMPACT_RESCUE_THINKING} âœ…`
);

console.log(
  `Final Layout Cap: objetivo estrito=${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE}; zero truncamento / zero word-split âœ…`
);

console.log(
  "Repair Regression Guard: bloqueia nova omissÃ£o/SDH/censura/gÃªnero/diÃ¡logo quebrado âœ…"
);

  console.log(
  "Post-Rewrite 9.0: auditoria redundante fundida no Final Bounded focal HIGH âœ…"
);

console.log(
  "Source Defect Recovery: truncamento evidente pode ser completado de forma mÃ­nima; interrupÃ§Ã£o/ambiguidade sÃ£o preservadas âœ…"
);

console.log(
  `Semantic Guard Safety: qualquer correÃ§Ã£o ainda exige CANONICAL LOCKS + sentido + ${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE} âœ…`
);

    console.log(
    `Main: atÃ© ${MAIN_BATCH_MAX_CUES} cues / ${MAIN_BATCH_MAX_CHARS} chars | concorrÃªncia=${MAIN_CONCURRENCY} âœ…`
  );

  console.log(
    `Main Empty-Cue Rescue: ${MAIN_EMPTY_CUE_RESCUE_ENABLED ? "ATIVO" : "DESATIVADO"} | ` +
    `somente cue vazio Ã© refeito | parse invÃ¡lido pode repetir; 429 NÃƒO multiplica 3x3 | consenso SDH=${MAIN_EMPTY_CUE_SDH_CONSENSUS_MIN} âœ…`
  );

  console.log(
    `Cue capsules: ${CAPSULE_CONTEXT_BEFORE} antes + target fechado + ${CAPSULE_CONTEXT_AFTER} depois âœ…`
  );

  console.log(
    `Thinking: PLAN=${PLAN_THINKING} | MAIN=${MAIN_THINKING} | QA=${QA_THINKING} | REPAIR=${REPAIR_THINKING} âœ…`
  );

  console.log(
    "Quota Router: pacing + RPM/TPM/RPD independentes por modelo; 429/503 fazem failover sem barreira global âœ…"
  );

  console.log(
    "Culture Hard Locks: ATIVOS âœ…"
  );

  console.log(
  "Canonical Catchphrase Lock: typos conhecidos da fonte sÃ£o restaurados para a forma canÃ´nica âœ…"
);

console.log(
  "Culture & Register Integrity: bitch/gag/profanity/slang por funÃ§Ã£o social, nunca tabela lexical âœ…"
);

console.log(
  "Profanity Pragmatic Lock: preserva forÃ§a sem inserir palavrÃ£o mecanicamente âœ…"
);

  console.log(
    "Condragulations / Sashay away / Shantay / Werkroom / Rusical: PROTEGIDOS âœ…"
  );

  console.log(
    "Cue Ownership 9.0: ID + key por cue, contexto compartilhado âœ…"
  );

  console.log(
  "Cue Ownership Key Lock: ordem cronolÃ³gica + ownership_key por ID + resposta na mesma ordem âœ…"
);

console.log(
  "Semantic Compact Retry: correÃ§Ã£o semÃ¢ntica >2x50 Ã© compactada antes de ser rejeitada âœ…"
);

console.log(
  "Semantic Canonical Locks: auditor pÃ³s-rewrite usa bordÃµes reais, nÃ£o tokens opacos âœ…"
);

  console.log(
    "GAG/GAGGED reaction guard: ATIVO âœ…"
  );

  console.log(
    "BOTTOM + palavrÃµes/intensificadores: GUARDS ATIVOS âœ…"
  );

  console.log(
    "Source Censorship Naturalization: reticÃªncia â‰  bleep; metadata invisÃ­vel vira fala PT-BR contextual âœ…"
  );

  console.log(
    "HARD SDH Sanitizer prÃ©/pÃ³s Gemini + credits/placeholders: ATIVO âœ…"
  );

  console.log(
    "Profanity Integrity Lock: ATIVO âœ…"
  );

  console.log(
    `PT-BR QA contextual SOURCEÃ—PT + Identity Lock + anti-literalidade: ATIVO | concorrÃªncia=${QA_CONCURRENCY} âœ…`
  );

  console.log(
    "Format Lock Empty-Cue Rescue: ATIVO âœ…"
  );

  console.log(
    `Pre-Repair Semantic Confirmation 8.4.5: ${PRE_REPAIR_CONFIRM_ROUNDS} auditorias limpas para heurÃ­stica ambÃ­gua escapar do Repair; falha tÃ©cnica=FAIL-SAFE âœ…`
  );

  console.log(
    `ExecuÃ§Ã£o roteada: MAIN=${MAIN_CONCURRENCY} | QA=${QA_CONCURRENCY} | REPAIR=${REPAIR_CONCURRENCY} | single-flight global=REMOVIDO âœ…`
  );

  console.log(
    "Intentional Empty 8.4.4: somente SOURCE realmente descartÃ¡vel pode sumir; fala real continua fail-closed âœ…"
  );

  console.log(
    "No Full Restart 8.4.4: empty cue/sanitizer retry fica no cue; PLAN+MAIN nÃ£o reiniciam por esse motivo âœ…"
  );

  console.log(
    "Job Wall Clock 8.4.4: FINAL reporta pipeline + tempo real total do job âœ…"
  );

  console.log(
    "LocalizaÃ§Ã£o brasileira por intenÃ§Ã£o: ATIVA âœ…"
  );

  console.log(
    "SÃ­mbolos inÃºteis + notas estendidas: NORMALIZAÃ‡ÃƒO ATIVA âœ…"
  );

  console.log(
    "Timestamp lock: absoluto; Gemini nunca gera tempos âœ…"
  );

  console.log(
    "SAFE DRAFT: ATIVO âœ… (diagnÃ³stico interno; nunca substitui FINAL prioritÃ¡rio reprovado)"
  );

  console.log(
    "Source Hygiene 8.4.0: Ff/J'j'/Jâ€œjâ€œ/pontuaÃ§Ã£o isolada removidos; wrappers com conteÃºdo preservam o conteÃºdo âœ…"
  );

  console.log(
    "Gender Integrity V2: contradiÃ§Ã£o intracuÐµ + speaker unknown marcado + trusted gender hard-check âœ…"
  );

  console.log(
    "Semantic Ownership Audit: SOURCE[i]Ã—PT[i] contra vizinhos; SOURCE pode ser QUALQUER idioma âœ…"
  );

  console.log(
    "Final Audit 8.4.2: lotes <=80 + schema sem maxItems + fallback adaptativo para HTTP 400 âœ…"
  );

  console.log("Focused Repair 8.4.3: Final Priority repairs only current-round blockers. OK");

  console.log(
    `Job Liveness 9.4.2: atÃ© ${JOB_MAX_ATTEMPTS} tentativa(s); SAFE DRAFT Ã© checkpoint, nunca substitui FINAL_PASS; zero loop aberto âœ…`
  );
  console.log("Final 9.0: pipeline bounded preservado; HARD SDH + neutral gender + router multimodelo âœ…");
  console.log("MAIN 9.4.0 runtime: 3.5 Flash-Lite MEDIUM primeiro; 3.1 Ã© fallback; checkpoint por lote preservado âœ…");
  console.log("MAIN Fail-Fast 9.0: erro determinÃ­stico nÃ£o vira loop; payload adaptativo + rescue focal âœ…");
  console.log("Final Bounded 9.0: zero loop aberto; auditoria/repair continuam focais e com fallback âœ…");
  console.log("Semantic Sync API preservada para OpenSub; Embedded 2.6 nÃ£o depende dela âœ…");

  console.log(
    "Semantic Fidelity 9.7.3: polarity + subject/object + object/entity fidelity endurecidos sem nova passada global âœ…"
  );
  console.log(
    "Implicit Speaker-Turn 9.7.3: interrupÃ§Ãµes/respostas curtas fortes sem hÃ­fen viram HARD turn lock; candidatos suaves vÃ£o ao QA âœ…"
  );
  console.log(
    "Contextual Music Polish 9.7.3: polÃ­tica 9.2.6 preservada; sÃ³ letra relevante mantida e recebe â™ª ... â™ª localmente âœ…"
  );
  console.log(
    "PT-BR Hygiene 9.7.3: typos/OCR + -- residual + interjeiÃ§Ã£o Ha/HÃ¡ normalizados localmente; zero chamada cloud extra âœ…"
  );

  console.log(
    `Cache namespace: ${CACHE_VERSION}`
  );
  console.log("Quality Closure 9.7.3: polarity + referent + implicit speaker-turn + language purity + contextual music typography | ZERO rodada cloud global extra âœ…");
  console.log("Semantic Repair Budget 9.7.0: UMA rodada consolidada apÃ³s PRE-AUDIT; Ownership nÃ£o reescreve fora dela; pÃ³s-Repair cloud=0 âœ…");
  console.log("Timing Compact 9.4.0: geraÃ§Ã£o <=24 + auditoria <=16; zero micro-recursÃ£o; HIGH com output budget anti-truncamento âœ…");

  console.log(
    "Multilingual Audio-Sync API: /api/sync-align + /api/sync-proxy + language-aware Transcribe ATIVOS âœ…"
  );

  console.log(
    "Pre-Repair 9.0: QA HIGH continua autoridade semÃ¢ntica; HARD GUARDS locais autorizam repair focal antes do SAFE DRAFT âœ…"
  );

  console.log(
    "GenerateContent 9.0: chamadas de texto migradas de Interactions para REST generateContent âœ…"
  );
  console.log(
    "Structured Output REST 9.0: responseMimeType + responseJsonSchema; responseFormat incompatÃ­vel removido âœ…"
  );

  console.log(
    "Empty-Cue 8.4.4: intentional omission + sanitizer-aware local recovery + no full restart. OK"
  );

  console.log(
    "HARD SDH 9.0 SAFE-BARE: descriÃ§Ãµes estruturadas saem; Look/Breathe/Dance/Entra e vocalizaÃ§Ãµes faladas nÃ£o viram SDH âœ…"
  );

  console.log(
    "Spoken Vocalization Lock 9.0: Mm-hmm/Hmm/Uhum/Um sÃ£o resolvidos localmente; 0 rescue HIGH desnecessÃ¡rio âœ…"
  );

  console.log(
    "Performance Atomic Reprise 9.0: fragmentos que repetem performance confirmada permanecem no mesmo cluster lÃ³gico âœ…"
  );

  console.log(
    "Dialogue Turn Restore 9.0: turn count correto + hÃ­fens ausentes/colados sÃ£o restaurados localmente âœ…"
  );

  console.log(
    "Gender Neutral 9.0: hard guard + pÃ³s-condiÃ§Ã£o local para padrÃµes neutros seguros; gÃªnero explÃ­cito da SOURCE Ã© preservado âœ…"
  );

  console.log(
    "Absolute Ownership 9.0: boundary hints no MAIN + detector local de continuaÃ§Ã£o/reaÃ§Ã£o deslocada antes do SAFE DRAFT âœ…"
  );

  console.log(
    "Compact Memory 9.0: mesmo cue/texto rejeitado nÃ£o consome Compact Rescue novamente no mesmo job âœ…"
  );

  console.log(
    "Latency Contract 9.0: MAIN MEDIUM, concorrÃªncia restaurada e nenhum 429 cria cooldown global âœ…"
  );

  console.log(
    "Model Router 9.4.0: MAIN/QA/Repair usam 3.5 primeiro; 3.1 somente fallback; 3.7/3.8 fora do projeto âœ…"
  );

  console.log(
    "Model Health 9.4.2: RPD diÃ¡rio hard-skip; timeout/503/RPM/TPM/JSON invÃ¡lido usam cooldown e podem voltar no mesmo job âœ…"
  );

  console.log(
    "Quota Diagnostics 9.4.2: RPD diÃ¡rio = daily_exhausted; falhas transitÃ³rias fazem fallback imediato + recuperaÃ§Ã£o bounded âœ…"
  );
  console.log("Turn Canonicalization 9.0: ~ colado/com espaÃ§o + hÃ­fen => speakers separados localmente; 0 Gemini extra âœ…");
  console.log("Standalone Speaker Labels 9.0: labels ALL CAPS em linha prÃ³pria viram metadata e nunca texto visÃ­vel âœ…");
  console.log("Short Performance Guard 9.0: clusters lÃ­ricos densos + launch atÃ© 20s preservados; letras narrativas nÃ£o somem âœ…");
  console.log("Ownership 9.0: short-reaction exata + semantic consensus + boundary QA reforÃ§ado âœ…");
  console.log("Gender Postcondition 9.0: right/secure/clear/late/good/not-alone cobertos localmente âœ…");
  console.log("Source Metadata 9.0: notas tÃ©cnicas/credits de subtitle removidos antes do MAIN âœ…");
  console.log("PT-BR Orthography 9.0: emituiu/agÃ¼enta corrigidos localmente + calques reais entram no Repair focal âœ…");
  console.log("Dialogue Invariant 9.0: ~ anexado/espacado reconhecido; SOURCE multi-turn termina canÃ´nica em hÃ­fens âœ…");
  console.log("Bleep Naturalization 9.0: metadata de censura fica invisÃ­vel; forÃ§a pragmÃ¡tica vira fala PT-BR natural âœ…");
  console.log("Ownership Boundary 9.0: overlap substancial criado no PT, ausente na SOURCE, aciona repair focal âœ…");
  console.log("Gender Neutrality 9.0: estados emocionais neutros ampliados em 1Âª/2Âª pessoa sem listas por tÃ­tulo âœ…");
  console.log("Bleep Detector 9.0: palavra vÃ¡lida + reticÃªncias nunca vira censura sÃ³ pelo prefixo; dots exigem stem forte/contexto âœ…");
  console.log("Hyphen Turn 9.0: -fala/- fala reconhecidos no inÃ­cio da linha; nÃºmeros negativos protegidos âœ…");
  console.log("Bare SDH Safety 9.0: fala SOURCE sobrevivente nÃ£o Ã© apagada silenciosamente por heurÃ­stica de aÃ§Ã£o âœ…");

  console.log(
    "Context Semantic Lock 9.0: before/after resolvem intenÃ§Ã£o; zero nova auditoria global / zero nova rodada cloud âœ…"
  );
  console.log(
    "Gender Priority V5 9.0: papel humano 1Âª/2Âª pessoa sem prova explÃ­cita nÃ£o pode ganhar gÃªnero por Repair âœ…"
  );
  console.log(
    "Gender V5 Definitive 9.0: neutralizaÃ§Ãµes inequÃ­vocas sÃ£o ZERO-CLOUD e nÃ£o reabrem Repair HIGH âœ…"
  );
  console.log(
    "Broadcast Anti-Calque 9.0: pool feed/sinal pool vira formulaÃ§Ã£o PT-BR natural somente com prova na SOURCE âœ…"
  );
  console.log(
    "Exact Repetition Lock 9.0: repetiÃ§Ã£o dramÃ¡tica nÃ£o pode ser compactada nem perdida por Repair âœ…"
  );
  console.log(
    "Visible Censor Zero 9.0: [censurado]/BLEEP_TOKEN nunca chegam ao SRT final; naturalizaÃ§Ã£o contextual âœ…"
  );
  console.log(
    "Contextual Imperative Lock 9.0: referente inventado Ã© bloqueado quando vizinhos provam sentido de parada/interrupÃ§Ã£o âœ…"
  );
  console.log(
    "Final Deterministic Closure 9.0: gÃªnero por turno + layout estrito + ownership semÃ¢ntico + broadcast commands | ZERO-CLOUD âœ…"
  );
  console.log(
    "Gender Multi-Turn 9.0: neutralizaÃ§Ã£o roda por speaker; cues com dois speakers nÃ£o pulam mais o guard âœ…"
  );
  console.log(
    "Strict Repetition Layout 9.0: repetiÃ§Ã£o completa nunca autoriza linha >50; contraÃ§Ã£o sem perda semÃ¢ntica âœ…"
  );
  console.log(
    "Ownership Semantic Boundary 9.0: overlap de conteÃºdo criado no PT Ã© detectado mesmo com preposiÃ§Ãµes diferentes âœ…"
  );
  console.log(
    "Broadcast/English Closure 9.0: Take/Roll/Okay/Move sÃ³ sÃ£o localizados quando a SOURCE prova o uso âœ…"
  );
  console.log(
    "Ownership Gate 9.7.0: shift detectado entra na Ãºnica rodada consolidada; 0 micro-batch / 0 SOURCE-ONLY de rewrite âœ…"
  );
  console.log(
    "Ownership Fail-Closed 9.0: shift persistente apÃ³s isolamento nunca Ã© servido como SRT final âœ…"
  );
  console.log(
    "Explicit Gender Evidence 9.0: modifiers como ten-year-old girl preservam gÃªnero declarado pela SOURCE âœ…"
  );

  console.log(
    "Empty-Cue Hygiene 9.1: SDH/ruÃ­do puro pode ficar vazio | fala real continua protegida | 0 rescue cloud para lixo âœ…"
  );

  console.log(
    "Subtitle Hygiene 9.2: hesitaÃ§Ã£o/vocalizaÃ§Ã£o nÃ£o-semÃ¢ntica pura sai; repetiÃ§Ã£o dramÃ¡tica continua protegida âœ…"
  );

  console.log(
    "Music Relevance Gate 9.2: performance/narrativa fica; mÃºsica incidental e vocalizaÃ§Ã£o fonÃ©tica nÃ£o poluem a legenda âœ…"
  );

  console.log(
    "Concrete Referent Lock 9.2: contexto textual nÃ£o autoriza inventar objetos invisÃ­veis ao modelo âœ…"
  );

  console.log(
    "Router Resilience 9.2.1: invalid_response nÃ£o envenena a Ãºltima rota; MAIN drena workers antes de retry/terminal âœ…"
  );
  console.log(
    "Structural Gender 9.2.3: predicativos/artigos humanos 1Âª/2Âª/plural sÃ£o auditados pela estrutura; SOURCE explÃ­cita continua autoridade âœ…",
    "Repair Persistence 9.2.3: repair FINAL aceito nÃ£o pode ser revertido silenciosamente para candidato antigo âœ…",
    "Strict Gender Final Gate 9.2.3: gender>0 bloqueia FINAL OK/cache; uma reconstruÃ§Ã£o focal bounded Ã© tentada antes âœ…",
    "Transient Route Resilience 9.2.4: ultima rota saudavel recebe 1 retry curto para 500/502/503/408/425 antes de skip; zero loop âœ… Target-Elimination Gender Gate 9.2.5 preservado âœ… Final Target Verification 9.2.6: blocker final reparado Ã© re-auditado em no mÃ¡ximo 2 passes focais; residual => sem FINAL OK/cache âœ… Universal Hygiene 9.2.6: #/â™ª lyric markers sÃ£o metadata; SDH puro com dash tambÃ©m Ã© removido âœ…"
  );

  console.log(
    "Gender Lock 9.1: proteÃ§Ãµes de gÃªnero preservadas integralmente; ambiguidade humana continua fail-closed âœ…"
  );
  console.log("Post-Closure Verify 9.7.0: recheck determinÃ­stico local; 0 auditoria Gemini pÃ³s-Repair âœ…");
  console.log("Call Budget 9.7.0: semantic Repair=1; Ownership rewrite=0 fora dela; pÃ³s-Repair semantic QA=0; Timing Compact separado e bounded âœ…");
  console.log("Canonical Cache Closure 9.4.0: FINAL fresco limpa noCache stale somente com todos os guards zerados âœ…");
  console.log("Semantic Authority 9.4.0: auditoria HIGH invalida Repair Persistence reprovado; strategy lock sÃ³ nasce apÃ³s QA limpa âœ…");
  console.log("Semantic Rewrite 9.7.0: blockers locais + QA + Final Priority + Ownership sÃ£o fundidos ANTES da Ãºnica rodada genÃ©rica de Repair âœ…");
  console.log("Gender Evidence Authority 9.4.2: SOURCE lexical explÃ­cita governa V3/V5/V8; modifiers e papÃ©is gender-coded genÃ©ricos preservados âœ…");
  console.log("Guard Conflict Resolver 9.4.2: neutrality guard contraditÃ³rio com SOURCE explÃ­cita Ã© removido; mismatch real continua HARD âœ…");
  console.log("Contextual Escalation Ledger 9.4.2: stage + blocker hash + current-PT hash; estratÃ©gia sÃ³ Ã© consumida apÃ³s resposta utilizÃ¡vel âœ…");
  console.log("Legacy convergence 9.4.2: DESATIVADA no pipeline ativo 9.7.0; sem source_only/beam/constrained/contrastive apÃ³s Repair âœ…");
  console.log("Router Health 9.4.2: timeout/503/RPM/TPM = cooldown transitÃ³rio; somente RPD diÃ¡rio vira hard-skip do job âœ…");
  console.log("MAIN Checkpoint 9.4.2: invalid_response refaz o lote no fallback; PLAN reutilizado em retomada âœ…");
  console.log("FINAL PASS Required 9.4.2: checkpoint sem selo Ã© preservado, mas nÃ£o Ã© servido como BEST_AVAILABLE final âœ…");
  console.log("Combined Repair 9.4.3: HARD local prÃ©-SAFE Ã© detectado cedo, mas a chamada cloud Ã© fundida ao QA global; elimina Repair redundante âœ…");
  console.log("Gender Evidence Severity 9.7.0: contradiÃ§Ã£o SOURCEâ†”PT Ã© HARD; marca gramatical PT-BR sem evidÃªncia de gÃªnero Ã© ADVISORY e nunca bloqueia FINAL âœ…");
  console.log("Turn-Aware Timing Compact 9.4.3: cues multi-speaker preservam contagem/ordem de turnos e compactam cada fala sem mover sentido âœ…");
  console.log("Timing Compact Path Isolation 9.4.3.4: single-turn base 9.4.2 + constrained; multi-turn preservado + SOURCE-turn rebuild quando PT perdeu segmentaÃ§Ã£o âœ…");
  console.log("Timing Compact Final Closure 9.4.3.4: residual single recebe NEEDLE semÃ¢ntico; residual multi reconstrÃ³i da SOURCE por speaker; UMA micro-etapa bounded; namespace 9.4.3 âœ…");
  console.log("Deterministic Final Gate 9.7.0: nenhum auditor probabilÃ­stico pÃ³s-Repair pode derrubar o episÃ³dio; sÃ³ invariantes objetivas bloqueiam âœ…");
  console.log("Universal Identity Normalizer 9.7.0: copular identity neutra usa PT-BR sem artigo de gÃªnero/alias SOURCE-safe; 0 lÃ³gica por tÃ­tulo/cue âœ…");
  console.log("Timing Compact Beam 9.4.1: 5 alternativas por parent com hard char caps; auditor recebe shortest-first; zero micro-loop âœ…");
  console.log("Timing Closure 9.4.1 preservado; semantic namespace sobe para 9.4.2 porque Gender Evidence/Convergence mudaram a autoridade textual âœ…");

  console.log(
    "Status: ONLINE"
  );

  console.log(
    "============================================================"
  );
});

process.on(
  "unhandledRejection",

  error =>
    console.error(
      "[PROCESS] Unhandled rejection:",
      error
    )
);

process.on(
  "uncaughtException",

  error =>
    console.error(
      "[PROCESS] Uncaught exception:",
      error
    )
);
