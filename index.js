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
// STREMIO PT-BR 8.8.1 - HARD SDH + NEUTRAL GENDER
// SAME BOUNDED PIPELINE; ZERO EXTRA GEMINI STAGES; SDH EARLY-DROP + GENDER-NEUTRAL DEFAULT
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();
const GEMINI_MODEL = "gemini-3.5-flash-lite";
const GEMINI_TRANSCRIBE_MODEL = "gemini-3.5-transcribe";

const CACHE_VERSION =
  "8.8.1-hard-sdh-neutral-gender-v1";
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_SOURCE_CHARS = 800000;
const FETCH_TIMEOUT_MS = 25000;

const GEMINI_MIN_START_INTERVAL_MS = 4300;

// Multilingual Audio-Sync Adapter.
// A Ponte usa este endpoint SOMENTE para construir texto-proxy lexical
// no idioma real do áudio. Não gera timestamps e não altera a tradução final.
const SYNC_PROXY_MAX_ITEMS = 260;
const SYNC_PROXY_MAX_CHARS = 36000;
const SYNC_PROXY_THINKING = "low";
const SYNC_PROXY_MAX_OUTPUT_TOKENS = 12000;
const SYNC_PROXY_TIMEOUT_MS = 90000;
const SYNC_PROXY_HTTP_RETRIES = 3;

// Cross-language semantic anchor alignment.
// Recebe SOMENTE poucos cues + palavras já transcritas; nunca traduz a legenda inteira.
const SYNC_ALIGN_MAX_ITEMS = 20;
const SYNC_ALIGN_MAX_WORDS_PER_ITEM = 120;
const SYNC_ALIGN_MAX_CHARS = 30000;
const SYNC_ALIGN_THINKING = "low";
const SYNC_ALIGN_MAX_OUTPUT_TOKENS = 5000;
const SYNC_ALIGN_TIMEOUT_MS = 60000;
const SYNC_ALIGN_HTTP_RETRIES = 3;

// Gemini Transcribe free-tier guard: 3 RPM / 10k TPM / 25 RPD.
// O projeto usa 22s entre inícios, teto interno de 24 chamadas/24h e
// uma margem de TPM para reduzir 429 antes que aconteçam.
const TRANSCRIBE_MIN_START_INTERVAL_MS = 22000;
const TRANSCRIBE_TPM_LIMIT = 10000;
const TRANSCRIBE_TPM_SOFT_LIMIT = 9500;
const TRANSCRIBE_RPD_INTERNAL_LIMIT = 24;
const TRANSCRIBE_TOKEN_ESTIMATE_PER_SECOND = 32;
const TRANSCRIBE_OUTPUT_TOKEN_RESERVE = 320;

// INTENCIONALMENTE continua 8.3.5:
// não podemos trocar o nome do ledger e esquecer chamadas Transcribe
// já consumidas nas últimas 24h durante o deploy do 8.3.16.
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
// Se o SAFE-SCHEMA vier INCOMPLETE, não repetimos a mesma estratégia.
const PLAN_FALLBACK_THINKING = "low";
const PLAN_FALLBACK_MAX_OUTPUT_TOKENS = 5000;
const PLAN_FALLBACK_RETRIES = 1;

const MAIN_BATCH_MAX_CUES = 160;
const MAIN_BATCH_MAX_CHARS = 36000;
const MAIN_CONCURRENCY = 4;
const CAPSULE_CONTEXT_BEFORE = 2;
const CAPSULE_CONTEXT_AFTER = 2;
const MAIN_THINKING = "high";
const MAIN_MAX_OUTPUT_TOKENS = 18000;
const MAIN_TIMEOUT_MS = 120000;
const MAIN_HTTP_RETRIES = 4;
const MAIN_PARSE_ATTEMPTS = 2;

// MAIN EMPTY-CUE RESCUE
// Se uma resposta estruturalmente válida trouxer pt vazio para um target
// não vazio, preservamos os demais cues do lote e refazemos SOMENTE o cue vazio.
const MAIN_EMPTY_CUE_RESCUE_ENABLED = true;
const MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS = 3;
const MAIN_EMPTY_CUE_RESCUE_THINKING = "high";
const MAIN_EMPTY_CUE_RESCUE_MAX_OUTPUT_TOKENS = 2400;
const MAIN_EMPTY_CUE_RESCUE_TIMEOUT_MS = 90000;
const MAIN_EMPTY_CUE_RESCUE_HTTP_RETRIES = 3;

// 8.4.6: nenhuma cue pode manter o job eternamente em processing.
// Duas respostas independentes classificadas como SDH confirmam omissão;
// caso contrário, após um ciclo finito preservamos uma base segura para QA.
const MAIN_EMPTY_CUE_MAX_CYCLES = 1;
const MAIN_EMPTY_CUE_SDH_CONSENSUS_MIN = 2;

const REPAIR_ENABLED = true;
const REPAIR_MAX_CUES_TOTAL = 120;
const REPAIR_BATCH_MAX_CUES = 32;
const REPAIR_THINKING = "high";
const REPAIR_MAX_OUTPUT_TOKENS = 10000;
const REPAIR_TIMEOUT_MS = 90000;
const REPAIR_HTTP_RETRIES = 3;
const REPAIR_PARSE_ATTEMPTS = 2;
const REPAIR_CONCURRENCY = 3;

// QA semântico SOURCE×PT para TODAS as fontes.
// Não reescreve diretamente: aponta cues problemáticos para Repair.
const QA_ENABLED = true;
const QA_BATCH_MAX_CUES = 420;
const QA_BATCH_MAX_CHARS = 90000;
const QA_THINKING = "high";
const QA_MAX_OUTPUT_TOKENS = 9000;
const QA_TIMEOUT_MS = 120000;
const QA_HTTP_RETRIES = 3;
const QA_PARSE_ATTEMPTS = 2;
const QA_MAX_FLAGS_TOTAL = 120;
const QA_CONCURRENCY = 4;
const QA_CONTEXT_BEFORE = 1;
const QA_CONTEXT_AFTER = 1;

// ============================================================
// PRE-REPAIR SEMANTIC CONFIRMATION — 8.4.5
// ============================================================
// Heurísticas ambíguas não ganham autoridade para reescrever texto sozinhas.
// Duas auditorias semânticas independentes precisam concordar que o cue está
// limpo para dispensar Repair. Qualquer flag OU falha técnica mantém Repair.
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
// FINAL CRITICAL CONVERGENCE — 8.4.2 SCHEMA-SAFE
// ============================================================
// Um problema de QUALIDADE não encerra o job. O gate audita a legenda
// que seria realmente servida e corrige somente os cues reprovados
// até que não reste defeito crítico. Falhas transitórias de Gemini
// também entram em retry; não viram "failed" por conveniência.
const FINAL_CRITICAL_GATE_ENABLED = true;
const FINAL_CRITICAL_AUDIT_BATCH_MAX_CUES = 200;
const FINAL_CRITICAL_AUDIT_BATCH_MAX_CHARS = 56000;
const FINAL_CRITICAL_AUDIT_CONCURRENCY = 3;
const FINAL_CRITICAL_AUDIT_THINKING = "high";
const FINAL_CRITICAL_AUDIT_MAX_OUTPUT_TOKENS = 10000;
const FINAL_CRITICAL_AUDIT_TIMEOUT_MS = 120000;
const FINAL_CRITICAL_AUDIT_HTTP_RETRIES = 4;
const FINAL_CRITICAL_MAX_ISSUES = 80;
const FINAL_CRITICAL_RETRY_BASE_MS = 4300;
const FINAL_CRITICAL_RETRY_MAX_MS = 60000;
const FINAL_CRITICAL_CONTEXT_RADIUS = 1;
const FINAL_CRITICAL_NO_PROGRESS_ESCALATE_AFTER = 2;
const FINAL_CRITICAL_HEURISTIC_CONSENSUS_CLEAN_AUDITS = 2;
const FINAL_CRITICAL_ESCALATED_BATCH_MAX_CUES = 24;
const FINAL_CRITICAL_ESCALATED_MAX_OUTPUT_TOKENS = 7000;
const FINAL_CRITICAL_ESCALATED_TIMEOUT_MS = 120000;
const FINAL_CRITICAL_REQUEST_MAX_FAILURES = 3;
const FINAL_CRITICAL_PARSE_MAX_FAILURES = 3;
const FINAL_CRITICAL_ESCALATED_MAX_FAILURES = 3;
const FINAL_CRITICAL_MAX_ROUNDS = 2;
const JOB_RETRY_BASE_MS = 5000;
const JOB_RETRY_MAX_MS = 60000;
const JOB_MAX_ATTEMPTS = 2;

// ============================================================
// SUBTITLE LAYOUT LOCK
// ============================================================

// Alvo audiovisual: no máximo 2 linhas, até 50 caracteres por linha.
// IMPORTANTE: estes limites NUNCA autorizam cortar palavras, truncar
// conteúdo, criar cues ou alterar timestamps.
const LAYOUT_MAX_LINES = 2;
const LAYOUT_MAX_CHARS_PER_LINE = 50;

// Quanto mais perto deste valor, mais equilibradas as duas linhas ficam.
const LAYOUT_IDEAL_CHARS_PER_LINE = 44;

// ============================================================
// COMPACT RESCUE — HARD 2x50
// ============================================================

// Só entra aqui quem continuou grande DEMAIS mesmo após o Repair normal.
// Em episódios como o teste do RuPaul, esperamos pouquíssimos cues.
const COMPACT_RESCUE_ENABLED = true;
const COMPACT_RESCUE_MAX_CUES_TOTAL = 120;
const COMPACT_RESCUE_BATCH_MAX_CUES = 24;
const COMPACT_RESCUE_MAX_ROUNDS = 1;

const COMPACT_RESCUE_THINKING = "high";
const COMPACT_RESCUE_MAX_OUTPUT_TOKENS = 7000;
const COMPACT_RESCUE_TIMEOUT_MS = 90000;
const COMPACT_RESCUE_HTTP_RETRIES = 3;

// 96 dá folga para o JavaScript encontrar uma quebra <= 50/50.
// Não é truncamento; é objetivo editorial para o Gemini.
const COMPACT_RESCUE_TARGET_TOTAL_CHARS = 96;

// ============================================================
// POST-REWRITE SEMANTIC GUARD
// ============================================================

// Audita somente cues cujo TEXTO foi realmente reescrito
// depois do MAIN. Mudança apenas de quebra de linha não conta.
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
const jobs = new Map();

let lastGeminiRequestStart = 0;
let geminiGate = Promise.resolve();

let transcribeGate = Promise.resolve();
let lastTranscribeRequestStart = 0;
let transcribeLedger = { calls: [] };

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

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
      `[TRANSCRIBE BUDGET] ledger não pôde ser lido: ${
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
      `[TRANSCRIBE BUDGET] ledger não pôde ser salvo: ${
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
            }/24h atingido; próxima vaga em ~${
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
    error: null,
    qualityStatus: "pending",

    // IDs removidos por regra SDH multilíngue ou por consenso de duas
    // respostas independentes. O Set nunca é exposto diretamente na API.
    intentionalEmptyCueIds: new Set(),

    started: false,
    promise: null,

    createdAt: now,
    updatedAt: now,

    // Relógio real da tradução: não reinicia em retry técnico.
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

      finalCriticalRounds: 0,
      finalCriticalAuditCalls: 0,
      finalCriticalFlags: 0,
      finalCriticalRepairRounds: 0,
      finalCriticalBoundedReleases: 0,
      boundedSafeDraftReleases: 0,
      finalCriticalTechnicalRetries: 0,
      finalCriticalNoProgressEscalations: 0,
      jobRetries: 0,

      pacerWaitMs: 0,

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
      "Token de recuperação inválido."
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
      "Assinatura de recuperação inválida."
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
      "Dados de recuperação incompletos."
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
  /(?:laugh|laughing|laughter|chuckle|chuckling|giggle|giggling|sigh|sighing|gasp|gasping|pant|panting|breath|breathing|breathes|inhale|inhaling|exhale|exhaling|whimper|whimpering|cry|crying|sob|sobbing|music|musical|song|singing|sings|chant|chanting|humming|hums|applause|cheer|cheering|clap|clapping|door|knock|knocking|bang|banging|slam|slamming|phone|ring|ringing|buzz|buzzing|beep|beeping|static|groan|groaning|grunt|grunting|scream|screaming|yell|yelling|shout|shouting|whisper|whispering|murmur|murmuring|talk|talking|quietly|inaudible|indistinct|foreign language|clears? throat|sniff|sniffing|cough|coughing|footstep|footsteps|steps|walking|running|rustle|rustling|leaves|branch|twig|floorboard|creak|creaking|crack|cracking|snap|snapping|glass|shatter|shattering|smash|horn|honking|tire|tires|engine|car|vehicle|wind|thunder|rain|storm|fire|crackle|crackling|growl|growling|roar|roaring|howl|howling|cricket|crickets|bird|birds|dog|dogs|cat|cats|moan|moaning|distorted|echo|echoing|voice|voices|distant|offscreen|off-screen|background|continues|speaking|calling|calls|narrating|voice-over|muffled|thud|impact|squish|squishing|squelch|squelching|scrape|scraping|metal|click|clicking|lock|unlock|faint|softly|loudly|tv|radio|siren|alarm|gunshot|gunshots|explosion|heartbeat|wheez|wheezing|whistl|whistling|snoring|screech|squeal|squealing|approaching|receding|door closes|door opens|footsteps approaching|breathing heavily|song playing|music playing|risos?|rindo|risadinhas?|gargalhada|gargalhando|suspira|suspiro|ofegante|ofegando|respira(?:ção|ndo)?|respiração|chora|chorando|soluça|soluçando|música|canção|cantando|canto|tarareando|aplausos?|palmas|gritos?|gritando|sussurra|sussurrando|murmura|murmurando|chamando|narração|narrando|falando baixo|continua falando|inaudível|indistinto|estática|passos?|pisada|pisadas|correndo|folhas?|farfalhando|galhos?|quebrando|assoalho|rangendo|rangido|vidro|estilhaça|estilhaçando|buzina|pneus?|motor|marcha lenta|vento|trovão|chuva|tempestade|fogo|estalando|uivo|grilos?|rosnado|rosnando|grunhido|grunhidos|guincho|guinchos|distorcido|distorcida|eco|voz ao longe|ao longe|ao fundo|em voz baixa|voz baixa|voz de|baque|impacto|raspando|metal|clique|clicando|tranca a porta|porta fechando|porta abrindo|sirene|alarme|tiro|tiros|explosão|menina rindo|som abafado)/i;

const CENSOR_CLUSTER_RE =
  /[!@#$%^&*()_+=~`¤£€¥¢]{3,}/gu;

const STANDALONE_SYMBOL_CLUSTER_RE =
  /(^|\s)[!@#$%^&*()_+=~`¤£€¥¢]{3,}(?=\s|$)/gu;

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
    /^(?:okay|ok|well|look|listen|so|now|then|actually|basically|because|but|and|or|yes|no|right|wait|hey|wow|girl|bitch|previously|meanwhile|later|earlier|tonight|today|tomorrow|atenção|cuidado|olha|escuta|então|agora|sim|não)$/i.test(
      speaker
    )
  ) {
    return false;
  }

  const letters =
    speaker.replace(
      /[^A-Za-zÀ-ÿ]/g,
      ""
    );

  const allUpper =
    Boolean(letters) &&
    letters ===
      letters.toUpperCase();

  const titleLike =
    parts.every(
      part =>
        /^[A-ZÀ-Ý][A-Za-zÀ-ÿ'’.-]*$/u.test(
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
        /^\s*[-–—]\s*/u.test(
          clean
        )
    };
  }

  const bracket =
    original.match(
      /^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*:?[ \t]*/u
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
          /^\s*[-–—]\s*/u.test(
            original
          )
      };
    }
  }

  const colon =
    original.match(
      /^\s*([-–—]\s*)?([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 #.'-]{0,45})(?:\s*\(([^)]{1,45})\))?\s*:\s*(.*)$/u
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
      /^\s*[-–—]\s*/u.test(
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
      /\s*\(([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 #.'-]{0,45})\)\s*$/u,
      (match, inside) =>
        looksLikeSpeakerLabel(
          inside
        )
          ? ""
          : match
    );

  text =
    text.replace(
      /\s*\[([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 #.'-]{0,45})\]\s*$/u,
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
        /[.,!?…]+/g,
        " "
      )
      .replace(/\s+/g, " ")
      .trim();

  return /^(?:ah|ha|heh|uh|um|hm|hmm)(?:\s+(?:ah|ha|heh|uh|um|hm|hmm)){1,8}$/.test(
    value
  );
}

// ============================================================
// UNIVERSAL SDH ACTION / ACCESSIBILITY CLASSIFIER
// ============================================================
//
// Objetivo:
// - remover descrições de acessibilidade por ESTRUTURA, não por nome/personagem;
// - reconhecer "RUPAUL CACKLING", "NAYA CLEARS THROAT",
//   "THE CROWD CHEERS", "DOORBELL RINGS" etc. sem hardcode de pessoas;
// - continuar conservador com fala real.
//
// A lista abaixo descreve AÇÕES/ESTADOS de acessibilidade, nunca identidades.
const SDH_ACTION_CORE_RE =
  /(?:bursts?\s+into\s+(?:laughter|applause|cheers?)|erupts?\s+(?:in|into)\s+(?:laughter|applause|cheers?)|breaks?\s+into\s+(?:laughter|applause|cheers?)|falls?\s+silent|goes?\s+quiet|goes?\s+wild|music\s+(?:plays?|playing|swells?|swelling|fades?|fading|continues?|continuing|starts?|starting|stops?|stopping)|song\s+(?:plays?|playing|continues?|continuing|starts?|starting|stops?|stopping)|smiles?|smiling|grins?|grinning|nods?|nodding|shrugs?|shrugging|waves?|waving|points?|pointing|stares?|staring|looks?|looking|rolls?\s+(?:(?:his|her|their)\s+)?eyes|gestures?|gesturing|enters?|entering|exits?|exiting|walks?|walking|runs?|running|dances?|dancing|turns?|turning|sorri|sorrindo|acena|acenando|assente|assentindo|encolhe\s+os\s+ombros|aponta|apontando|encara|encarando|olha|olhando|revira\s+os\s+olhos|gesticula|gesticulando|entra|entrando|sai|saindo|caminha|caminhando|corre|correndo|dança|dançando|vira|virando|laughs?|laughing|cackles?|cackling|chuckles?|chuckling|giggles?|giggling|snickers?|snickering|sighs?|sighing|gasps?|gasping|pants?|panting|breathes?|breathing|inhales?|inhaling|exhales?|exhaling|whimpers?|whimpering|cries?|crying|sobs?|sobbing|sniffs?|sniffing|coughs?|coughing|sneezes?|sneezing|clears?\s+(?:(?:his|her|their|the)\s+)?throat|hums?|humming|whistles?|whistling|chants?|chanting|cheers?|cheering|applauds?|applauding|claps?|clapping|groans?|groaning|grunts?|grunting|screams?|screaming|yells?|yelling|shouts?|shouting|whispers?|whispering|murmurs?|murmuring|moans?|moaning|wheezes?|wheezing|snore?s?|snoring|growls?|growling|roars?|roaring|howls?|howling|barks?|barking|meows?|meowing|rings?|ringing|buzzes?|buzzing|beeps?|beeping|dings?|dinging|chimes?|chiming|knocks?|knocking|bangs?|banging|slams?|slamming|creaks?|creaking|cracks?|cracking|snaps?|snapping|shatters?|shattering|smashes?|smashing|honks?|honking|screeches?|screeching|rustles?|rustling|clicks?|clicking|thuds?|thudding|rattles?|rattling|approaches?|approaching|recedes?|receding|continues?\s+(?:laughing|crying|sobbing|singing|cheering|applauding)|(?:begins?|starts?)\s+(?:laughing|crying|sobbing|singing|cheering|applauding)|risos?|ri|rindo|gargalha|gargalhando|cai\s+na\s+risada|suspira|suspirando|ofega|ofegando|respira|respirando|chora|chorando|soluça|soluçando|fungando|tosse|tossindo|espirra|espirrando|limpa\s+(?:a\s+)?garganta|pigarreia|pigarreando|cantarola|cantarolando|assobia|assobiando|canta|cantando|grita|gritando|berrando|sussurra|sussurrando|murmura|murmurando|geme|gemendo|rosna|rosnando|uiva|uivando|late|latindo|mia|miando|toca|tocando|vibra|vibrando|bipa|bipando|tilinta|tilintando|bate|batendo|fecha|fechando|abre|abrindo|range|rangendo|quebra|quebrando|estilhaça|estilhaçando|buzina|buzinando|chia|chiando|farfalha|farfalhando|clica|clicando|se\s+aproxima|se\s+afasta)/iu;

const SDH_EVENT_NOUN_RE =
  /(?:laughter|applause|cheers?|cheering|whooping|whinnies|whinnying|chatter|chattering|babble|babbling|crowd\s+noise|audience\s+noise|giggles?|chuckles?|cackling|sighs?|gasps?|panting|heavy\s+breathing|crying|sobbing|sniffing|coughing|sneezing|humming|whistling|chanting|groans?|grunts?|screams?|yells?|shouts?|whispers?|murmuring|footsteps?|steps?|knocking|banging|ringing|buzzing|beeping|dinging|chimes?|static|thunder|rain|storm|wind|fire\s+crackling|glass\s+(?:breaking|shattering)|engine\s+(?:starting|idling)|horn|tires?\s+screeching|rustling|clicking|thuds?|impact|heartbeat|siren|alarm|gunshots?|explosion|risos?|gargalhadas?|aplausos?|palmas|gritos?|gritaria|suspiros?|ofegos?|respiração|choro|soluços?|tosse|espirros?|pigarro|canto|cantoria|assobios?|murmúrios?|gemidos?|rosnados?|uivos?|latidos?|miados?|passos?|batidas?|campainha|toque|toques|bipes?|estática|trovão|chuva|tempestade|vento|fogo\s+estalando|vidro\s+(?:quebrando|estilhaçando)|motor|buzina|pneus?\s+cantando|farfalhar|cliques?|baques?|impacto|batimentos?|sirene|alarme|tiros?|explosão)/iu;

const SDH_ACTION_TAIL_RE =
  /^(?:(?:loudly|softly|quietly|wildly|nervously|awkwardly|hysterically|together|again|offscreen|off-screen|onstage|on-stage|offstage|off-stage|away|back|in\s+background|in\s+the\s+background|in\s+distance|in\s+the\s+distance|faintly|briefly|continuously|heavily|rapidly|twice|once|three\s+times|a\s+lot|all\s+together|at\s+(?:him|her|them|camera|the\s+camera)|toward(?:s)?\s+\w+|to\s+camera|ao\s+fundo|ao\s+longe|baixinho|alto|altamente|forte|fortemente|nervosamente|sem\s+graça|histericamente|juntos?|juntas?|novamente|de\s+novo|duas\s+vezes|uma\s+vez|brevemente|continuamente|muito|bastante|para\s+(?:ele|ela|eles|elas|a\s+câmera)|em\s+direção\s+a\s+\w+|para\s+trás|embora)(?:\s+|$))*$/iu;

function normalizeSdhCandidate(
  value
) {
  return stripMarkup(
    String(value || "")
  )
    .replace(
      /^\s*[-–—]\s*/u,
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
      /[.:;!?…]+$/gu,
      ""
    )
    .replace(
      /\s+/g,
      " "
    )
    .trim();
}

// A ponte pode escolher uma SOURCE embutida em qualquer idioma. A 8.4.5
// reconhecia ações SDH em inglês/português, mas deixava descritores franceses
// como "Angela rit" e "musique tendue" chegarem ao tradutor como se fossem
// diálogo. O Gemini então devolvia corretamente "Angela ri"/"Risos" e o
// sanitizer PT-BR apagava a resposta, criando um ciclo impossível.
const FRENCH_SDH_EVENT_RE =
  /^(?:rires?|éclats? de rire|gloussements?|ricanements?|sourires?|soupirs?|halètements?|respiration(?: forte| lourde)?|pleurs?|sanglots?|reniflements?|toux|éternuements?|raclement de gorge|fredonnement|sifflements?|chants?|applaudissements?|acclamations?|cris?|chuchotements?|murmures?|gémissements?|grognements?|hurlements?|aboiements?|miaulements?|sonnerie|bips?|grincements?|claquements?|coups?|tonnerre|pluie|vent|orage|musique(?: [\p{L}'’-]+){0,5}|chanson(?: [\p{L}'’-]+){0,5}|bruit(?:s)?(?: [\p{L}'’-]+){0,6}|pas(?: [\p{L}'’-]+){0,5})$/iu;

const FRENCH_SDH_ACTION_RE =
  /(?:éclate(?:nt)? de rire|se met(?:tent)? à rire|rit|rient|rigole|rigolent|glousse|gloussent|ricane|ricanent|sourit|sourient|soupire|soupirent|halète|halètent|respire|respirent|pleure|pleurent|sanglote|sanglotent|renifle|reniflent|tousse|toussent|éternue|éternuent|se racle(?:nt)? la gorge|fredonne|fredonnent|siffle|sifflent|chante|chantent|applaudit|applaudissent|crie|crient|chuchote|chuchotent|murmure|murmurent|gémit|gémissent|grogne|grognent|hurle|hurlent|aboie|aboient|miaule|miaulent|sonne|sonnent|vibre|vibrent|grince|grincent|claque|claquent|frappe|frappent|s'ouvre|s'ouvrent|se ferme|se ferment)$/iu;

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
        /^(?:fort|fortement|doucement|nerveusement|ensemble|encore|au loin|en arrière-plan|hors champ|brièvement)$/iu,
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
        /[.:;!?…]+$/gu,
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
    /[!?…]/u.test(
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

  // Evita apagar respostas/interjeições gritadas comuns.
  if (
    /^(?:OK|OKAY|YES|NO|YEAH|YEP|NOPE|HI|HELLO|HEY|BYE|GOODBYE|THANKS|THANK YOU|PLEASE|SORRY|RIGHT|WRONG|WOW|AMAZING|BRILLIANT|HELP|STOP|WAIT|COME ON|LET'S GO|LETS GO|GO|READY|CHEERS)$/iu.test(
      text
    )
  ) {
    return false;
  }

  // Nomes de trilha/descrição musical sem ♪.
  if (
    words.length >= 2 &&
    /^(?:MUSIC|SONG|SCORE|THEME|INSTRUMENTAL|MÚSICA|CANÇÃO|TRILHA)$/iu.test(
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
    /^(?:SPEAKS?|SPEAKING|TALKS?|TALKING)\s+[A-ZÀ-Ý][A-ZÀ-Ý'’-]*(?:\s+[A-ZÀ-Ý][A-ZÀ-Ý'’-]*)?$/u.test(
      text
    )
  ) {
    return true;
  }

  const subjectAction =
    text.match(
      /^(.{1,80}?)\s+([A-ZÀ-Ý][A-ZÀ-Ý'’-]*)$/u
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
      /^(?:HE|SHE|THEY|IT|EVERYONE|EVERYBODY|SOMEONE|SOMEBODY|AUDIENCE|CROWD|CAST|GROUP|PEOPLE|MAN|WOMAN|BOY|GIRL|MEN|WOMEN|KIDS|CHILDREN|DOG|CAT|HORSE|PHONE|DOOR|BELL|ENGINE|CAR|VEHICLE|ELE|ELA|ELES|ELAS|TODOS|TODAS|PÚBLICO|PLATEIA|GRUPO)$/iu.test(
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
      !/^(?:I|WE|YOU|HE|SHE|THEY|IT|EU|NÓS|NOS|VOCÊ|VOCÊS|ELE|ELA|ELES|ELAS)$/iu.test(
        firstSubjectWord
      ) &&
      subjectWords.length <= 3 &&
      subjectWords.every(
        word =>
          /^[A-ZÀ-Ý0-9][A-ZÀ-Ý0-9'’.-]*$/u.test(
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
      /^(?:KNOWS?|THINKS?|WANTS?|NEEDS?|LOVES?|HATES?|LIKES?|HAS|HAVE|IS|ARE|WAS|WERE|CAN|COULD|WILL|WOULD|SHOULD|DOES?|DID|SAYS?|MEANS?|GETS?|GOES?|COMES?|SEES?|FEELS?|SABE|SABEM|PENSA|PENSAM|QUER|QUEREM|PRECISA|PRECISAM|AMA|AMAM|ODEIA|ODEIAM|GOSTA|GOSTAM|TEM|TÊM|É|SÃO|PODE|PODEM|VAI|VÃO|DIZ|DIZEM)$/iu.test(
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
      /^(?:THEY|WE|HE|SHE|EVERYONE|EVERYBODY|AUDIENCE|CROWD|CAST|GROUP|PEOPLE|TODOS|TODAS|ELES|ELAS)(?:\s+(?:ALL|BOTH|ENTIRE|WHOLE|JUNTOS|JUNTAS))?\s+([A-ZÀ-Ý][A-ZÀ-Ý'’-]*)$/u
    );

  if (
    groupAction
  ) {
    const action =
      groupAction[1];

    if (
      !/^(?:KNOW|THINK|WANT|NEED|LOVE|HATE|LIKE|HAVE|ARE|CAN|WILL|DO|SAY|MEAN|GET|GO|COME|SEE|FEEL|SABEM|PENSAM|QUEREM|PRECISAM|AMAM|ODEIAM|GOSTAM|TÊM|SÃO|PODEM|VÃO|DIZEM)$/iu.test(
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

  return words.every(
    word =>
      /^[\p{Lu}\d][\p{L}\p{N}'’.-]*$/u.test(
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
    /[♪♫♬]/u.test(
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
// HARD SDH STRUCTURE 8.8.1
// ============================================================
// Bracket/parenthetical captions are accessibility metadata far more often
// than dialogue. The older lexical classifier missed perfectly normal CC
// descriptions such as [takes off shoes], [typing], [melody ends],
// [gun drops to floor] and [singers vocalizing].
//
// This classifier is intentionally used ONLY for accessibility-shaped
// segments / bare caption lines. It does not rewrite ordinary dialogue.
const STRUCTURED_SDH_EVENT_RE =
  /(?:fanfare|melody|score|instrumental|chatter|typing|keyboard|keys?|liquid|water|shower|gun|wood|bag|zipper|insects?|singers?|vocali[sz](?:e|es|ing|ation)|retch(?:es|ing)|gag(?:s|ging)|scoffs?|mumbles?|stammers?|yawns?|yawning|whispers?|squeaks?|squeaking|trickl(?:e|es|ing)|trill(?:s|ing)|jingl(?:e|es|ing)|tapping|footsteps?|melodia|fanfarra|trilha|conversa|burburinho|cochichos?|teclado|teclando|digitando|chaves?|líquido|agua|água|chuveiro|arma|madeira|bolsa|zíper|insetos?|cantores?|vocaliza(?:ção|ndo)|ânsia|engasga(?:ndo)?|gagueja(?:ndo)?|sussurr(?:a|ando)|rangido|chiado)/iu;

const STRUCTURED_SDH_ACTION_RE =
  /(?:plays?|playing|fades?|fading|ends?|ending|stops?|stopping|starts?|starting|continues?|continuing|drops?|dropping|falls?|falling|opens?|opening|closes?|closing|unzips?|unzipping|zips?|zipping|taps?|tapping|types?|typing|trickles?|trickling|trills?|trilling|vocali[sz](?:es?|ing)|retches?|retching|gags?|gagging|scoffs?|scoffing|mumbles?|mumbling|stammers?|stammering|yawns?|yawning|whispers?|whispering|squeaks?|squeaking|jingl(?:e|es|ing)|snaps?|snapping|takes?\s+off|puts?\s+on|picks?\s+up|sets?\s+down|tocando|termina|terminando|para|parando|começa|começando|continua|continuando|cai|caindo|abre|abrindo|fecha|fechando|digita|digitando|tecla|teclando|goteja|gotejando|vocaliza|vocalizando|engasga|engasgando|gagueja|gaguejando|sussurra|sussurrando|tilinta|tilintando|estala|estalando)/iu;

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
    /^(?:["“][^"”]{1,100}["”]\s+by\s+.{1,80}\s+playing|tocando\s+["“][^"”]{1,100}["”](?:\s*,?\s*(?:by|de|do|da)\s+.{1,80})?)$/iu.test(text)
  ) {
    return true;
  }

  const hasFirstSecondPerson =
    /\b(?:i|i'm|i’m|i am|me|my|mine|we|us|our|ours|you|your|yours|eu|meu|minha|nós|nosso|nossa|você|vocês|seu|sua)\b/iu.test(text);

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

  if (
    /^[-–—]\s+/u.test(
      original
    )
  ) {
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
    /[!?…]\s*$/u.test(
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

  return /^(?:sound of |sounds of )?(?:static|laughter|applause|music(?: playing)?|song(?: playing)?|footsteps?(?: approaching| receding)?|door (?:opens|closes|slams|creaks)|phone (?:rings|buzzes)|wind (?:blows|howls)|thunder|rain(?: falling)?|fire crackling|glass (?:breaks|shatters)|engine (?:starts|idles)|car horn|tires? screeching|branch (?:breaks|snaps)|leaves rustling|heavy breathing|panting|gasping|sobbing|crying|humming|whistling|growling|roaring|howling|muffled voices?|distant voices?|estática|risos?|aplausos?|música|passos?(?: se aproximando| ao longe)?|porta (?:abrindo|fechando|batendo|rangendo)|telefone (?:tocando|vibrando)|vento (?:soprando|uivando)|trovão|chuva|fogo estalando|vidro (?:quebrando|estilhaçando)|motor (?:ligando|em marcha lenta)|buzina|pneus? cantando|galho (?:quebrando|estalando)|folhas farfalhando|respiração (?:forte|ofegante)|ofegante|ofegando|chorando|soluçando|tarareando|assobiando|rosnado|uivo|vozes? abafadas?|vozes? ao longe|som (?:abafado )?(?:de )?(?:passos|pisadas|esmagamento|algo sendo esmagado)|esmagando|som pastoso)$/iu.test(
    text
  );
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
      /(\p{L}{2,})(?:-[aeiouáéíóúàâêôãõü]){2,}/giu,
      "$1"
    )
    .replace(
      /(\p{L}{2,})(?:-[\p{L}]{1,3}){3,}/gu,
      "$1"
    )
    .replace(
      /([aeiouáéíóúàâêôãõü])\1{3,}/giu,
      "$1$1"
    );
}

function looksLikeMaskedProfanityToken(
  token
) {
  const raw =
    String(token || "");

  if (!raw) {
    return false;
  }

  const symbolMask =
    /[*#@%&$]/u.test(raw);

  const dotMask =
    /(?:\.{2,}|…)/u.test(raw);

  if (
    !symbolMask &&
    !dotMask
  ) {
    return false;
  }

  const maskIndex =
    raw.search(
      /[*#@%&$]|\.{2,}|…/u
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

  const dotPrefixes =
    new Set([
      "f",
      "fu",
      "fuc",
      "fuck",
      "motherf",

      "sh",
      "shi",

      "b",
      "bi",
      "bit",

      "c",
      "cu",
      "cun",
      "car",
      "cara",
      "caral",

      "p",
      "pu",
      "put",
      "por",
      "porr",
      "pus",
      "puss",

      "ass",
      "assh",

      "d",
      "di",
      "dic",
      "dick",

      "fo",
      "fod",
      "fud",

      "me",
      "mer",
      "merd"
    ]);

  if (
    dotMask &&
    !symbolMask
  ) {
    return dotPrefixes.has(
      visiblePrefix
    );
  }

  const symbolPrefixes =
    new Set([
      ...dotPrefixes,
      "s",
      "m"
    ]);

  return symbolPrefixes.has(
    visiblePrefix
  );
}

function replaceMaskedProfanity(
  value
) {
  const decoded =
    decodeBasicEntities(
      value
    );

  return decoded.replace(
    /[\p{L}][\p{L}0-9*#@%&$!._~…’'-]{1,28}/gu,

    token =>
      looksLikeMaskedProfanityToken(
        token
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
      /[\p{L}][\p{L}0-9*#@%&$!._~…’'-]{1,28}/gu
    ) || [];

  if (
    tokens.some(
      looksLikeMaskedProfanityToken
    )
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
      /^\s*[-–—]{2,}\s*/u,
      "- "
    )
    .replace(
      /^\s*[:;]+\s*$/u,
      ""
    )
    .replace(
      /^\s*[•·▪◦]+\s*/u,
      ""
    )
    .replace(
      /\s+[\/\\|]{1,3}\s+/gu,
      " "
    )
    .replace(
      /\s*[-–—]{2,}\s*/gu,
      "… "
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

    /\bmedia access group\b.*\bwgbh\b/i.test(
      text
    )
  );
}

// ============================================================
// SOURCE HYGIENE 8.4.0
// ============================================================
// Alguns SRTs/OCRs convertem símbolos musicais em lixo textual, por
// exemplo: Ff, ff, J'j', J“j“ ou wrappers J“ ... j“.
//
// Regras conservadoras:
// - wrapper com CONTEÚDO real: remove só o wrapper e preserva a fala/letra;
// - marcador isolado sem conteúdo: remove o cue antes do Gemini;
// - pontuação isolada (..., …, -- etc.) não vira legenda inventada.
function stripPseudoMusicOcrWrappers(value) {
  let text = String(value || "").trim();

  // J“ in the land ... j“  -> in the land ...
  // j" I was an angel j"   -> I was an angel
  text = text
    .replace(/^\s*[Jj]\s*[“”"'‘’`´]+\s*/u, "")
    .replace(/\s*[Jj]\s*[“”"'‘’`´]+\s*$/u, "")
    .trim();

  return text;
}

function looksLikeSourceGarbageLine(value) {
  const text = String(value || "")
    .replace(/<[^>]+>/g, "")
    .replace(/\{\\[^}]+\}/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) return true;

  if (/^(?:f{2,4})$/iu.test(text)) return true;

  if (
    /^(?:[Jj]\s*[“”"'‘’`´]+\s*[Jj]\s*[“”"'‘’`´]*|[Jj]\s*(?:\.{2,}|…+|-+))$/u.test(text)
  ) {
    return true;
  }

  if (/^[.·•…,:;!?_~*#@\-–—/\\|\s]+$/u.test(text)) {
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
    /^(?:[Jj]\s*[“”"'‘’`´]+\s*[Jj]\s*[“”"'‘’`´]*|[Jj]\s*(?:\.{2,}|…+|-+))$/u.test(text) ||
    /^[.·•…,:;!?_~*#@\-–—/\\|\s]+$/u.test(text)
  );
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
        /^(\s*[-–—]\s*)[:;]+\s*/u,
        "$1"
      )
      .replace(
        /[♪♫♬★☆✦✧]/gu,
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
    /^[-–—/\\|:;\s]*$/u.test(
      text
    ) ||
    isEmptyVocalization(
      text
    )
  ) {
    return "";
  }

  return text;
}

// ============================================================
// INTENTIONAL EMPTY CUES — 8.4.4
// ============================================================
// Autoriza ausência visual SOMENTE se a própria SOURCE, reavaliada pelas
// mesmas regras conservadoras de Source Hygiene, não contiver conteúdo
// semântico. Qualquer fala, palavra, número ou bleep continua fail-closed.
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
// Princípio:
// - música editorial/de fundo continua sendo removida;
// - apresentação real continua sendo legendada;
// - decisão musical é por CUE/BLOCO, nunca por linha isolada;
// - uma letra nunca pode ser mutilada ("linha com ♪ some / continuação fica").
//
// Âncoras longas preservam a regra que funcionou no The Voice.
// Clusters curtos só entram quando o contexto imediato prova lançamento
// de performance ou quando ficam ENTRE dois clusters já confirmados.

const MUSIC_CLUSTER_GAP_MS =
  12000;

const MUSIC_STRONG_MIN_CUES =
  4;

const MUSIC_STRONG_MIN_SPAN_MS =
  25000;

const MUSIC_SHOWCASE_REGION_GAP_MS =
  45000;

const MUSIC_LAUNCH_CONTEXT_MAX_GAP_MS =
  6500;

const PERFORMANCE_LAUNCH_RE =
  /(?:\bhit it\b|\btake it away\b|\bgive it up\b|\blet'?s hear it\b|\blet'?s hear (?:it )?for\b|\bstart the music\b|\bmusic[, ]+maestro\b|\bplay it\b|\bshowtime\b|\b(?:now|next)[, ]+(?:performing|singing)\b|\bperforming live\b|\bsinging live\b|\bon stage now\b|\bmanda ver\b|\bsolta o som\b|\bcomeça a música\b|\bvamos ouvir\b|\bvalendo\b|\bagora[, ]+(?:cantando|se apresentando)\b)/iu;

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
        /[♪♫♬]/u.test(
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
      /[♪♫♬]/u.test(
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

    // Cue misto: fala real depois de linha musical não pode sumir.
    if (
      /^\s*[-–—]\s+/u.test(
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

    // Continuação de uma linha que começou com ♪.
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
            item.visible
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
          !/[♪♫♬]/u.test(
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

  // CAMADA 1 — regra segura original:
  // 4+ cues distribuídos por 25+ segundos.
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

    if (
      current.length >=
        MUSIC_STRONG_MIN_CUES &&
      spanMs >=
        MUSIC_STRONG_MIN_SPAN_MS &&
      !terminalOutro
    ) {
      confirmedClusters.add(
        clusterIndex
      );
    }
  }

  // CAMADA 2 — performance curta com lançamento explícito.
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
      )
    ) {
      confirmedClusters.add(
        clusterIndex
      );
    }
  }

  // CAMADA 3 — região de showcase.
  // Clusters curtos ENTRE duas performances confirmadas
  // pertencem à mesma sequência de apresentações.
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
    `[MUSIC CONTEXT] cues com ♪=${musicIndexes.length} | ` +
    `clusters=${clusters.length} | âncoras fortes=${strongCount} | ` +
    `clusters confirmados=${confirmedClusters.size} | ` +
    `cues de performance mantidos=${keep.size}.`
  );

  return {
    info,
    keep,
    clusters,
    confirmedClusters
  };
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

    for (
      const classified of
      classifiedLines
    ) {
      const sourceLine =
        classified.raw;

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
        classified.kind ===
        "lyric"
      ) {
        performanceLyricLinesKept++;
      }

      const info =
        extractSpeaker(
          sourceLine
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

      if (
        info.hadDialogueDash &&
        !/^\s*[-–—]\s*/u.test(
          cleaned
        )
      ) {
        cleaned =
          `- ${cleaned}`;
      } else {
        cleaned =
          cleaned.replace(
            /^\s*[-–—]\s*/u,
            "- "
          );
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

    const explicitDialogueTurns =
      dialogue.filter(
        line => /^\s*[-–—]\s+/u.test(line)
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
      // 8.8.1: a label de um turno NÃO vira identidade do cue inteiro.
      // Isso impede "-I'm tired / -SARAH: ..." de atribuir Sarah ao 1º turno.
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
    }; música-fundo-linhas=${
      backgroundLyricLinesRemoved
    }; letra-performance-linhas=${
      performanceLyricLinesKept
    }; speakerHints=${
      speakerHints
    }; speakerHints-multiturno-suprimidos=${
      speakerHintsSuppressedMultiTurn
    }; bleepCues=${
      bleepCues
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

    if (textLines.length) {
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

      speakerHint
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
  // CONDRAGULATIONS — CANONICALIZAÇÃO DETERMINÍSTICA
  // ==========================================================
  // Algumas legendas-fonte trazem typos como:
  // Condragtulations / Condraglulations.
  // Todas representam o mesmo bordão e devem voltar
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

  // Nome da franquia — aceita inclusive fonte sem apóstrofo.
  {
    regex:
      /\bRuPaul(?:'|’)?s\s+Drag\s+Race\b/giu,
    value:
      "RuPaul's Drag Race"
  },

  // Deve ficar por último para não capturar antes
  // os dois bordões completos "Lip Sync for..."
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
        } não voltou.`
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
// A fonte é a autoridade para a quantidade/ordem de speakers dentro do cue.
// O Gemini pode escolher palavras e concisão; não pode apagar fronteiras.
// O layout final pode agrupar turns na mesma linha, mas nunca quebrar um turn
// de forma que pareça pertencer ao speaker seguinte.

function sourceDialogueTurns(
  block
) {
  return String(
    block?.text ||
    ""
  )
    .split("\n")
    .map(
      line =>
        String(
          line || ""
        ).trim()
    )
    .filter(
      line =>
        /^\s*[-–—]\s+/u.test(
          line
        )
    )
    .map(
      line =>
        line.replace(
          /^\s*[-–—]\s+/u,
          ""
        ).trim()
    )
    .filter(Boolean);
}

function sourceDialogueDashCount(
  block
) {
  return sourceDialogueTurns(
    block
  ).length;
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

  const pieces =
    flattened
      .split(
        /(?:^|\s)[-–—]\s+/u
      )
      .map(
        part =>
          part
            .replace(
              /^[ \t]+|[ \t]+$/g,
              ""
            )
      )
      .filter(Boolean);

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
    .map(
      turn =>
        `- ${turn}`
    )
    .join("\n");
}

function stripOutputAccessibilityLine(
  line
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
        /[♪♫♬★☆✦✧]/gu,
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
      /^\s*[-–—]\s*/u.test(text);

    text =
      `${hadDash ? "- " : ""}${info.text}`.trim();
  }

  // Fallback 8.8.1: speaker labels must never be visible in the final SRT.
  // Conservative: removes only a prefix that itself passes looksLikeSpeakerLabel.
  text = text.replace(
    /^(\s*[-–—]\s*)?([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 #.'’_-]{0,45})\s*:\s*(?=\S)/u,
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

  if (
    looksLikeBareSdhLine(
      text
    )
  ) {
    return "";
  }

  if (
    /^[A-ZÁÉÍÓÚÂÊÔÃÕÇ][A-ZÁÉÍÓÚÂÊÔÃÕÇ .'-]{1,45}:\s*$/u.test(
      text
    )
  ) {
    return "";
  }

  return text;
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
        /[♪♫♬]/gu,
        " "
      )
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

  text =
    text.replace(
      new RegExp(
        BLEEP_TOKEN,
        "g"
      ),
      "[censurado]"
    );

  const expectedDialogueTurns =
    sourceDialogueDashCount(
      block
    );

  let lines =
    text
      .replace(/\r/g, "")
      .split("\n")
      .map(
        stripOutputAccessibilityLine
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
                /^\s*[•·▪◦]+\s*/u,
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
                /^\s*[-–—]+\s*/u,
                ""
              );
          } else {
            cleaned =
              cleaned.replace(
                /^\s*[-–—]+\s*/u,
                "- "
              );
          }

          cleaned =
            cleaned
              .replace(
                /([^\s])\s*[-–—]{2,}\s*([^\s])/gu,
                "$1… $2"
              )
              .replace(
                /\s+[-–—]{2,}\s+/gu,
                " "
              )
              .replace(
                /\s+([,.;:!?])/g,
                "$1"
              )
              .replace(
                /\[censurado\]\s*([,.;:!?])/gi,
                "[censurado]$1"
              )
              .replace(
                /[ \t]{2,}/g,
                " "
              )
              .trim();

          if (
            /^[-–—/\\|.:;·•_*~…\s]+$/u.test(
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
        /[♪♫♬]/gu,
        " "
      )
      .replace(
        STANDALONE_SYMBOL_CLUSTER_RE,
        "$1 "
      )
      .replace(
        new RegExp(
          BLEEP_TOKEN,
          "g"
        ),
        "[censurado]"
      );

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
              /^\s*[•·▪◦]+\s*/u,
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
            "SOURCE sem conteúdo semântico utilizável"
          );
        } else {
          needsLocalRecovery++;

          console.warn(
            `[FORMAT LOCK] cue ${
              block.index
            } ficou vazio após sanitização, mas a SOURCE contém conteúdo real; ` +
            `rescue LOCAL será obrigatório | raw=${
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
    `vazios pós-sanitizer=${
      emptiedAfterSanitizer
    }; intencionais=${
      intentionalEmpty
    }; rescue-local=${
      needsLocalRecovery
    }; SDH/ruído/alongamentos controlados.`
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

  // Uma única palavra enorme jamais será cortada.
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

    // Pequena preferência por uma quebra linguisticamente agradável.
    const punctuationBonus =
      /[,.;:!?…]$/.test(first)
        ? 10
        : 0;

    // Preferimos:
    // 1. caber em 50;
    // 2. ficar equilibrado;
    // 3. aproximar-se visualmente de ~44;
    // 4. quebrar perto de pontuação quando possível.
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
  // testamos todas as divisões contíguas entre APENAS duas linhas.
  // Nenhum speaker é partido ao meio para encaixar o seguinte.
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

  // Se JavaScript consegue diagramar em <= 2x50, NÃO desperdiçamos Gemini.
  if (
    result.fits &&
    result.lines <= LAYOUT_MAX_LINES
  ) {
    return false;
  }

  // Não significa que o texto será cortado.
  // Significa apenas: peça ao Repair para tentar uma versão
  // semanticamente equivalente, porém mais concisa e natural.
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

    // Segurança absoluta:
    // layout nunca pode apagar conteúdo.
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
      /qualé/giu,
      match =>
        /^[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛ]/u.test(match)
          ? "Qual é"
          : "qual é"
    )
    .replace(
      /diacho/giu,
      match =>
        /^[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛ]/u.test(match)
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
      normalizeEditorialVocab(
        String(
          translations.get(
            block.index
          ) ??
          block.text
        )
      ).trim();

    // Vazio autorizado não vira "..." nem bloco visual artificial.
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
        } ausente sem autorização de vazio intencional.`
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
    }: PASSOU — ${
      source.length
    } source / ${
      final.length
    } visíveis; ${
      intentionalOmitted
    } vazio(s) intencional(is) omitido(s); 0 alterações de timestamp.`
  );
}

// ============================================================
// STYLE / CONTEXTO
// ============================================================

const STYLE_PACK = `
PORTUGUÊS BRASILEIRO NATURAL — GUIA EDITORIAL 8.4.0

IDIOMA DA FONTE
- A fonte normalmente é inglês, mas pode ser espanhol ou outro idioma.
- Detecte e respeite o idioma realmente presente no target.
- Traduza diretamente para PT-BR natural; nunca passe por tradução literal intermediária.
- Regras específicas de inglês abaixo só se aplicam quando a fonte realmente estiver em inglês.
- Todas as regras de identidade, ownership, significado, registro, layout e timestamps continuam valendo para qualquer idioma.
- Campos internos chamados en/EN significam "texto-fonte" por compatibilidade e podem conter espanhol ou outro idioma.

PRIORIDADE ABSOLUTA

PRIORIDADE ABSOLUTA
1. sentido/contexto correto;
2. identidade/gênero/referentes corretos;
3. ownership do cue e sincronização semântica;
4. naturalidade PT-BR contemporânea;
5. cultura/registro corretos;
6. velocidade.

PRINCÍPIO CENTRAL: PRESERVAR IDENTIDADE, LOCALIZAR INTENÇÃO
- Preserve nomes, marcas, bordões consagrados e termos cuja identidade cultural importa.
- Localize para o Brasil humor, insulto, gíria, metáfora, intenção social e expressão idiomática quando uma tradução literal esconderia o sentido.
- Não "abrasileire" nomes/bordões que soariam falsos traduzidos.
- Não deixe inglês estrutural dentro de português só porque as palavras foram traduzidas.
- A legenda deve fazer um brasileiro entender o que a fala QUER DIZER e como ela SOA socialmente.

NAMED / CULTURAL ENTITY INTEGRITY — REGRA ABSOLUTA
- Nomes e identidades culturais NÃO são matéria de adaptação livre.
- Preserve a identidade de pessoas reais, personagens, figuras mitológicas,
  lendas/folclore, celebridades, marcas, empresas, programas, filmes, séries,
  músicas, obras, instituições, eventos, lugares e demais entidades nomeadas.
- NUNCA substitua uma entidade estrangeira por uma entidade brasileira
  "equivalente", parecida ou culturalmente análoga.
- Localizar a INTENÇÃO não autoriza trocar a IDENTIDADE.
- Uma forma canônica consagrada em PT-BR para A MESMA entidade é permitida:
  exônimos estabelecidos, títulos oficiais localizados e nomes oficialmente
  usados em português continuam representando a mesma identidade.
- Se houver dúvida entre preservar o nome original ou inventar/adaptar
  culturalmente, PRESERVE O ORIGINAL.
- Traduza a explicação em volta da entidade, não transforme a entidade em outra.
- Se a mesma entidade reaparecer no episódio, mantenha sua identidade consistente.
- Exemplo de erro de identidade:
  "Bloody Mary" -> "Loira do Banheiro" é PROIBIDO.
  São lendas culturalmente análogas, mas NÃO são a mesma entidade.
- Isso é diferente de uma tradução canônica da MESMA entidade.

CONTEXT + IDENTITY LOCK — REGRA INVIOLÁVEL
- A BÍBLIA EDITORIAL contém um Character Ledger. Use identidades confirmadas para impedir contradições de referente; NÃO use o Ledger como licença para introduzir gênero que a própria SOURCE não expressou.
- Speaker é QUEM ESTÁ FALANDO; pessoa citada/mentioned é DE QUEM SE FALA. speaker ≠ pessoa mencionada.
- Nunca transfira gênero, pronome, relação ou identidade entre turnos, entre speaker e pessoa mencionada, nem entre cues vizinhos.
- Em cue com duas falas, cada turno é independente. Um label explícito no segundo turno JAMAIS identifica o primeiro.
- Um nome/pronome no target deve ser resolvido com before/after + Character Ledger; se ainda houver ambiguidade, preserve a ambiguidade de forma natural.
- Não invente parentesco, identidade, pronome, título ou nome ausente da evidência.

GENDER-NEUTRAL DEFAULT 8.8.1 — REGRA ABSOLUTA
- Se a SOURCE não expressa gênero naquela ideia, o PT-BR NÃO deve introduzir gênero desnecessariamente, MESMO quando a identidade do speaker é conhecida.
- O Character Ledger protege contra contradição; ele NÃO obriga "cansado/cansada", "sozinho/sozinha", "confuso/confusa" etc. quando existe formulação neutra natural.
- Prefira SEMPRE a formulação naturalmente neutra: "I'm scared" -> "Tô com medo"; "I'm confused" -> "Não tô entendendo"; "I'm alone" -> "Tô sem ninguém por perto"; "I'm worried" -> "Isso tá me preocupando".
- Não use linguagem artificial como "cansade"/"confuse". Neutralidade aqui significa REESCREVER em PT-BR natural.
- Só marque gênero quando ele for semanticamente necessário ou explicitamente sustentado pela SOURCE naquele referente: she/her, he/him, woman/man, daughter/son, mother/father etc.
- Se before/after não provarem inequivocamente o referente, preserve a incerteza. Não deduza gênero só porque uma pessoa conhecida aparece na cena.
- Gênero conhecido de pessoa mencionada aplica-se SOMENTE à pessoa mencionada, nunca automaticamente ao speaker.

NATURALIDADE PT-BR 2026 — REGRA DE ACEITAÇÃO
- CORRETO MAS LITERAL DEMAIS NÃO É SUFICIENTE. Tradução com cara de tradução é defeito editorial.
- Antes de devolver cada cue, faça o teste: "um brasileiro falaria isso espontaneamente hoje, nessa situação e com essa personalidade?".
- Preserve intenção, pragmática e temperatura emocional antes da ordem das palavras do inglês.
- Reestruture sujeito, verbo, intensificador, metáfora e ordem sintática quando o português pedir.
- Evite português de dublagem antigo, formalidade artificial, calques e falsos cognatos.
- Use PT-BR contemporâneo SEMPRE; use Gen Z/Alpha/fandom SOMENTE quando idade, personalidade, comunidade e situação pedirem.
- Uma fala adulta em drama/horror deve soar atual e humana, não necessariamente como internet/TikTok.
- Não use "sequer", "de fato", "eu suponho", "eu aprecio isso", "isso sendo dito" ou outras formas engessadas por reflexo do inglês quando uma forma brasileira simples for mais natural no registro da cena.

NATURALNESS LOCK — REGRA INVIOLÁVEL
- TRADUÇÃO LITERAL QUE SOA TRADUZIDA É TRADUÇÃO ERRADA, mesmo quando a gramática e o significado básico estiverem corretos.
- Fidelidade NÃO significa preservar sintaxe, ordem de palavras, verbo, substantivo ou metáfora do inglês.
- Fidelidade significa preservar o que a pessoa QUER DIZER, o efeito social da fala, a emoção e a personalidade.
- Antes de devolver o pt, imagine que a pessoa da cena é brasileira e está dizendo espontaneamente a mesma coisa. Escreva essa fala.
- Depois de entender o EN, pare de usá-lo como molde sintático.
- Prefira equivalência pragmática e idiomática a equivalência palavra por palavra.
- Se a tradução permitir enxergar facilmente a frase inglesa por baixo dela, revise em busca de calque.
- Frase compreensível mas artificial NÃO está pronta.
- Frase gramaticalmente correta mas que ninguém diria naturalmente NÃO está pronta.
- Frase que parece Google Translate, legenda estudantil ou português de dublagem antiga NÃO está pronta.
- Use contrações brasileiras naturais como "tô", "tá", "pra", "né" quando o personagem e a cena pedirem.
- Não use informalidade artificial apenas para parecer moderno.

TESTE DO BRASILEIRO NATIVO
Antes de cada cue, pergunte silenciosamente:
1. Eu ouviria um brasileiro real dizer isso?
2. Essa pessoa específica diria isso?
3. A ordem da frase nasceu em português ou foi copiada do inglês?
4. Existe uma forma igualmente fiel, porém mais curta e natural?
Se qualquer resposta indicar artificialidade, REESCREVA antes de responder.

EXEMPLOS DE DEFEITO DE NATURALIDADE
- RUIM: "Eu estava esperando mais picos e vales."
  MELHOR: "Eu esperava mais altos e baixos." / "Eu esperava mais variação.", conforme contexto.
- RUIM: "Eu aprecio isso."
  MELHOR: "Valeu.", "Agradeço.", "Fico feliz." etc., conforme personagem e situação.
- RUIM: "Isso sendo dito..."
  MELHOR: reconstrua a transição naturalmente: "Dito isso...", "Mas...", "Só que..." etc.
- RUIM: "O Maxi Desafio desta semana é uma reviravolta no Snatch Game chamada..."
  MELHOR: prefira uma formulação enxuta e brasileira como "O Maxi Desafio desta semana é uma versão do Snatch Game..." quando esse for o sentido.
- Os exemplos ensinam o TIPO de correção; não os copie mecanicamente em contextos diferentes.

CONCISÃO AUDIOVISUAL
- Legenda não é transcrição palavra por palavra.
- Preserve TODA a informação relevante, mas elimine redundância sintática que o português não precisa.
- Se duas formulações forem semanticamente equivalentes, prefira a mais curta, natural e rápida de ler.
- Não acrescente sujeitos, pronomes, conectivos ou explicações que o português possa omitir naturalmente.
- Não resuma informação; compacte a FORMA, não o conteúdo.

MEANING INTEGRITY LOCK — REGRA INVIOLÁVEL
- NATURALIZAR NÃO É RESUMIR.
- ENCURTAR NÃO É APAGAR.
- Uma tradução mais elegante que perde uma unidade real de significado é INCORRETA.
- Antes de compactar, faça silenciosamente um inventário semântico do EN.

Toda unidade independente deve sobreviver quando existir no original:
* quem fez/sentiu/disse;
* ação ou estado;
* objeto/alvo da ação;
* negação;
* causa e consequência;
* condição;
* quantidade;
* comparação;
* relação familiar/social;
* tempo relevante;
* contraste;
* intensidade;
* palavrão ou força emocional relevante;
* insulto e seu grau de agressividade;
* piada, shade ou duplo sentido;
* informação narrativa nova.

- Se o EN trouxer duas informações ligadas por "and", não apague automaticamente a segunda só para encurtar.
- Não transforme duas ideias diferentes em uma ideia genérica.
- Não suavize insulto, vulgaridade, raiva ou intensidade apenas para economizar caracteres.
- Não transforme FALA em descrição SDH, stage direction ou efeito sonoro.
- Uma instrução falada continua sendo fala.
- Preserve a função comunicativa do cue.

CONCISÃO SEGURA
- Economize caracteres mudando a ESTRUTURA do português:
  * elimine sujeito redundante;
  * use verbo mais direto;
  * retire repetição sintática;
  * prefira expressão idiomática curta;
  * evite nominalizações e construções burocráticas;
  * use elipse natural do português quando o sentido continuar completo.
- NÃO economize caracteres removendo fatos ou relações.
- O objetivo visual é caber em 2 linhas de até 50 caracteres.
- Se não houver forma segura no MAIN, preserve o sentido completo. O sistema possui uma etapa especializada posterior para compactação.

CUE OWNERSHIP — REGRA INVIOLÁVEL
- Cada cápsula é independente.
- Traduza SOMENTE o campo target daquela cápsula.
- before/after existem exclusivamente para compreensão.
- NUNCA complete o target com palavras que pertencem ao after.
- NUNCA empurre o final do target para outro id.
- NUNCA puxe o final do before para o target.
- Se uma frase estiver cortada entre cues, respeite exatamente o corte original.
- Cada id é uma caixa fechada de conteúdo.

HARD LOCKS
- Tokens no formato __LOCK_C...__ são texto protegido.
- Copie cada token EXATAMENTE, caractere por caractere, no ponto correspondente da tradução.
- Nunca traduza, reformule, remova, pluralize ou pontue dentro do token.

CENSURA / BLEEP
- O token ${BLEEP_TOKEN} representa uma palavra ou expressão censurada por bleep/símbolos na fonte.
- NUNCA devolva ${BLEEP_TOKEN} nem sequências como @#$%&*() na legenda final.
- Use a gramática, o sentimento, a cena e os cues vizinhos para reconstruir a INTENÇÃO em PT-BR natural.
- Se o sentido geral estiver claro, escolha uma formulação brasileira coerente com o tom, sem fingir certeza sobre a palavra inglesa exata.
- Se a palavra exata for impossível de inferir com segurança, reescreva a frase para continuar completa e natural; [censurado] é último recurso.
- NUNCA deixe buracos como "é um vestido bem .".

LGBTQIAPN+ / DRAG / BALLROOM / REALITY / FANDOM — CULTURE & REGISTER INTEGRITY LOCK
- Tenha letramento real de cultura LGBTQIAPN+, drag, ballroom, camp, shade, stan culture, internet culture e reality competition.
- Preserve humor, sexualidade, irreverência, shade, camp, afeto, orgulho, deboche e agressividade conforme a cena.
- Não suavize a personalidade de queens, jurados, participantes ou personagens.
- Não force gíria em pessoas cujo registro não pede isso.
- NÃO trate vocabulário cultural como tabela fixa EN→PT.
- Antes de traduzir gíria, palavrão, insulto ou vocativo, determine silenciosamente sua FUNÇÃO SOCIAL na fala.
- A tradução correta é a que preserva intenção, relação entre as pessoas, intensidade, humor e efeito social — não necessariamente a palavra de dicionário.

BORDÕES / FRASES CANÔNICAS / IDENTIDADE DE PROGRAMA
- Bordões, nomes de desafios, nomes de segmentos, marcas e expressões reconhecidamente canônicas exigem cuidado máximo.
- Tokens __LOCK_C...__ são autoridade absoluta e devem voltar IDÊNTICOS.
- Nunca "corrija criativamente", traduza ou abrasileire um HARD LOCK.
- Se a legenda-fonte contiver um typo reconhecível de um bordão que o HARD LOCK canonicalizou, use a forma CANÔNICA restaurada.
- Para frases conhecidas que NÃO são HARD LOCK, preserve a estrutura retórica, a piada, a intensidade e as unidades importantes do bordão.
- Não transforme uma frase icônica em uma paráfrase genérica só para ficar curta.
- Familiaridade com o programa/personagem é contexto editorial, não licença para inventar.

BITCH — PROIBIDA TRADUÇÃO AUTOMÁTICA
- "bitch" NÃO possui equivalente PT-BR fixo.
- NUNCA converta mecanicamente toda ocorrência para "puta".
- NUNCA converta mecanicamente toda ocorrência para "vadia", "bicha", "gata" ou qualquer outra palavra.
- Primeiro identifique a função da ocorrência.

Possíveis funções de "bitch":
* insulto hostil;
* provocação/briga;
* vocativo afetuoso entre amigas/queens;
* cumplicidade camp;
* exclamação;
* admiração;
* autoelogio;
* orgulho/empoderamento;
* descrição de personalidade;
* termo sexual, quando o contexto realmente for sexual.

- Em uso amigável/camp, possibilidades naturais incluem "bicha", "gata", "amiga", "menina" ou até omissão do vocativo.
- Em insulto real, possibilidades incluem "vadia", "escrota", "desgraçada" ou outra formulação compatível com a intensidade e a personagem.
- "puta" só é apropriado quando o sentido/contexto realmente justificar; não é tradução-padrão de "bitch".
- Em autoafirmação como "I'm a bad bitch", preserve orgulho, poder e atitude.
- "I'm a bad bitch" NÃO significa "sou uma puta ruim".
- Dependendo da personagem/cena, pode equivaler pragmaticamente a algo como "eu sou foda", "sou poderosa", "sou aquela gata" ou outra formulação brasileira natural.
- Não transforme autoelogio em autodepreciação nem afeto em agressão.

PALAVRÕES / PROFANITY — PRESERVAR FORÇA, NÃO CONTAGEM
- Preserve a força pragmática dos palavrões e intensificadores relevantes.
- NÃO censure artificialmente.
- NÃO suavize palavrão apenas para economizar caracteres.
- NÃO insira palavrão aleatoriamente só porque existe um palavrão no EN.
- NÃO tente manter uma correspondência de 1 palavrão EN = 1 palavrão PT.
- "fuck", "fucking", "shit", "damn", "hell", "ass", "motherfucker" etc. dependem da função na frase.
- Um palavrão pode funcionar como insulto, raiva, surpresa, intensidade, humor, admiração, sexualidade ou ritmo de fala.
- Escolha a solução PT-BR que preserve ESSA função.
- Se uma construção brasileira natural expressa a mesma intensidade sem tradução lexical do palavrão, isso pode ser correto.
- Se retirar o palavrão destruir a força, personalidade ou piada da fala, preserve essa força em PT-BR.
- Se acrescentar "porra", "caralho", "puta", "merda" etc. tornar a fala artificialmente mais agressiva que o original, NÃO acrescente.
- Profanidade deve soar como algo que aquela pessoa realmente diria em português naquela situação.

PHRASAL PROFANITY / EXPRESSÕES FIXAS
- Não traduza palavrão dentro de expressão idiomática palavra por palavra.
- "don't fuck it up" pede preservação da intenção: "não estrague tudo",
  "não faça merda", "não cague tudo" etc. conforme personagem e intensidade.
- Evite calques artificiais como "não fode tudo".
- Expressões como "shake shit up" significam causar impacto,
  virar o jogo, bagunçar estruturas ou causar; não exigem automaticamente
  inserir "foder" em PT-BR.
- Preserve a força pragmática, mas NÃO aumente a vulgaridade só para
  demonstrar que percebeu o palavrão inglês.

GAG / GAGGED — DISTINGUIR SENTIDOS
- Em drag/fandom/reaction slang, "I'm gagged", "she gagged me", "I was gagged" normalmente expressam choque, impacto ou ficar sem reação.
- Nesses casos, prefira conforme o registro: "tô passada", "fiquei passada", "tô em choque", "fiquei sem reação", "me deixou passada" etc.
- NÃO traduza reaction "gag/gagged" como engasgar, amordaçar ou ter ânsia.
- Uso físico de gag/engasgar só vale quando o contexto realmente envolve garganta, comida, vômito, sufocamento, mordaça ou ação física semelhante.
- "the gag is..." pode significar "o babado é...", "a questão é...", "o detalhe é..." ou outra construção conforme a intenção.
- "gag" também pode significar piada/bit/recurso cômico; determine pelo contexto.

DRAG / INTERNET / REALITY — TRADUZIR POR SENTIDO
- "ate / ate that" em elogio = arrasou, entregou tudo, serviu etc.; NUNCA "comeu isso" nesse sentido.
- "no crumbs" = não deixou nada pra ninguém / entregou tudo; não traduza literalmente migalhas quando for elogio.
- "slay" = arrasar, entregar, servir etc. quando for elogio; não "matar" salvo sentido literal.
- "shade" = shade, alfinetada, indireta, veneno etc. conforme contexto; não "sombra".
- "tea" como fofoca/informação = babado, fofoca, novidade etc.; não "chá".
- "read / reading" em contexto drag = ler/alfinetar/desmontar/colocar no lugar conforme a fala; não aplicar tradução lexical cegamente.
- "serve / serving" pode significar entregar visual, atitude, energia ou performance; traduza a intenção.
- "bottom" em competição = bottom, piores, berlinda/zona de risco conforme o formato; não "fundo".
- Diferencie completamente "bottom" competitivo de uso sexual.
- "mother" como título/elogio cultural não significa automaticamente "mãe" literal.
- "girl" pode ser vocativo social ("amiga", "gata", "mulher", "menina", omissão etc.) e não descrição literal de gênero.
- Preserve duplo sentido sexual quando ele fizer parte da piada.
- Não explique a piada dentro da legenda.

REGRA DE OURO DE REGISTRO
- Duas frases podem ter o mesmo significado factual e ainda assim NÃO serem equivalentes socialmente.
- Preserve também: afeto, hostilidade, intimidade, poder, sarcasmo, vulgaridade, camp, orgulho, ironia e intensidade.
- Uma tradução semanticamente correta mas socialmente errada deve ser tratada como ERRO.

GAG / GAGGED / GAGGING EM SENTIDO DE REAÇÃO
- Em reação, surpresa, impacto ou admiração, prefira: "passada", "tô passada", "fiquei passada", "em choque", "sem reação".
- Em Drag Race/reality queer, "I'm gagged" normalmente deve soar como "tô passada".
- NUNCA use "amordaçada" ou "engasgada" nesse sentido.
- Só use sentido físico quando a cena realmente falar de boca, engasgo, reflexo de vômito, sufocamento etc.

BOTTOM EM COMPETIÇÕES / REALITY
- Em Drag Race/reality, bottom frequentemente significa colocação ruim ou risco de eliminação.
- "in the bottom" -> "no bottom" ou "entre as piores".
- "bottom queens" -> "queens do bottom" ou "as piores da semana".
- "bottom two" -> "bottom 2" ou "as duas piores".
- "bottom three" -> "bottom 3" ou "as três piores".
- NUNCA traduza bottom competitivo como "fundo", "quintal", "parte de baixo" ou "inferior".
- Diferencie bottom competitivo de bottom sexual pelo contexto.

ELIMINATION / UP FOR ELIMINATION — DRAG RACE
- Quando a fonte disser explicitamente "elimination", use "eliminação".
- NUNCA substitua "elimination" por "berlinda", "zona de risco" ou outro eufemismo.
- "I'm sorry, my dear, but you are up for elimination." =
  "Sinto muito, querida, mas você está na eliminação."
- "You are both up for elimination." =
  "Vocês duas estão na eliminação."
- "You're going straight to elimination." =
  "Você vai direto para a eliminação."
- Adapte singular/plural e a construção naturalmente, mas preserve o termo "eliminação".
- Esta regra NÃO altera o uso de "bottom" quando a fonte realmente disser "bottom".

PALAVRÕES E INTENSIFICADORES
- "fuck", "fucking" e "the fuck" muitas vezes funcionam como intensidade, não como substantivos literais.
- Preserve agressividade, humor e personalidade, mas reconstrua a frase em PT-BR natural.
- "Who the fuck knows?" -> "Quem caralhos sabe?", "Quem é que sabe, porra?" ou "Sei lá, porra.".
- NUNCA "Quem sabe o caralho?".
- "What the fuck is that?" -> "Que porra é essa?".
- "Where the fuck is she?" -> "Onde caralhos ela tá?" ou equivalente natural.
- "Why the fuck would I do that?" -> "Por que caralhos eu faria isso?".
- Não preserve mecanicamente a posição sintática de "fuck" do inglês.
- Não transforme automaticamente todo "fucking" em "do caralho".

OUTRAS GÍRIAS IMPORTANTES
- she ate / you ate / they ate, quando elogio: "arrasou", "entregou tudo", "serviu". Nunca "comeu".
- no crumbs: "não deixou nada pra ninguém" ou equivalente natural.
- slay/slayed/slaying como elogio: arrasar, entregar, servir. Não "matar".
- shade social: shade, alfinetada, indireta, veneno, conforme contexto. Não "sombra".
- tea em fofoca/fandom: babado ou equivalente; nunca "chá" literal por reflexo.
- read/reading em drag: dar um read, acabar com alguém, ler alguém, conforme contexto; não tradução escolar automática.
- serving em fashion/drag: servindo/entregando um look, entregando conceito etc., conforme a fala.
- bitch como vocativo amigável: bicha, gata, amiga, menina ou omitir. Nunca "puta" automaticamente.
- judges em competição/reality: jurados.
- supportive: "me apoiou muito", "esteve do meu lado". Evite "super apoiador".
- double/shared win: vitória dupla / as duas ganharam. Não "empate duplo" sem empate.

GEN Z / GEN ALPHA / INTERNET
- Entenda memes, fandom, stan culture, cringe, delulu, iconic, mother, serve, clocked, gag, ate, shade e linguagem de internet pelo SENTIDO.
- Use equivalentes brasileiros atuais quando naturais.
- Não transforme toda fala jovem em caricatura de TikTok.
- Não injete gíria só para modernizar. "Qualé", "pistola" e equivalentes não são atalhos automáticos para informalidade.
- Uma queen jovem no Werkroom pode pedir linguagem de fandom/Gen Z; uma personagem adulta em drama/horror pode pedir fala simples contemporânea sem gíria de internet.
- Preserve idade, personalidade, classe, formalidade, época da obra e situação social do falante.

METÁFORAS E NATURALIDADE
- Traduza metáforas pela imagem/intenção que um brasileiro entenderia naturalmente.
- Evite calques estranhos e diminutivos artificiais que ninguém diria em PT-BR.
- Se o inglês usa fire/spark/heat para dizer que algo despertou uma emoção, prefira uma expressão natural como "acendeu uma chama em mim", "despertou algo em mim" etc., conforme contexto; não invente objetos literais como "forneuzinha" sem motivo real.
- Referências com tradução brasileira consolidada podem ser localizadas: Death Star -> Estrela da Morte, por exemplo.
- CONTINUIDADE AUDIOVISUAL: se uma palavra inglesa estiver sendo soletrada, formada por iniciais, escrita na tela ou usada como pista visual, preserve a relação com as letras/imagem. Não traduza de modo que a pista deixe de fazer sentido. Quando necessário, mantenha a palavra visual em inglês e deixe o sentido claro sem quebrar o cue.
- Em fala casual: "tô", "tá", "pra", "né" podem ser usados quando combinarem com a pessoa.
- Não use lusitanismos ou linguagem burocrática.
- Não traduza expressão idiomática palavra por palavra.
- Não censure palavrões; preserve intensidade de forma brasileira natural.

ANTI-CALQUE / FALSOS COGNATOS
- actually normalmente é "na verdade", "aliás", "pior que" etc., não "atualmente".
- eventually normalmente é "no fim", "uma hora", "acabou acontecendo" etc., não "eventualmente" por reflexo.
- realize é "perceber/se dar conta", não "realizar" quando significa entender.
- pretend é "fingir", não "pretender" quando significa fazer de conta.
- parents são "pais", não "parentes"; college raramente é "colégio"; library é "biblioteca", não "livraria".
- "I mean" em conversa geralmente é "quer dizer", "tipo", "digo" ou pode ser omitido; evite "eu quero dizer" mecânico.
- "at the end of the day" idiomático tende a "no fim das contas", não "no fim do dia" literal.
- "that being said" não deve virar "isso sendo dito"; reconstrua a transição naturalmente.
- Idiomas como give me a break, piece of cake, break a leg, under the weather, on the same page exigem intenção contextual, não palavra por palavra.

INTEGRIDADE DE PALAVRÃO — REGRA INVIOLÁVEL
- Se a intenção da fala exige palavrão, escreva o palavrão por extenso em PT-BR natural.
- NUNCA devolva autocensura gráfica criada por você: "f...", "f****", "fu&#", "p***", "c*****" etc.
- Quando a FONTE estiver censurada e vier como ${BLEEP_TOKEN}, reconstrua a intenção naturalmente; não replique os símbolos.
- "no fucking way" pode ser "nem fudendo" quando o registro pedir essa intensidade.
- Não suavize palavrões por pudor e não aumente a agressividade sem base na cena.

REVISÃO ORTOGRÁFICA E NATURALIDADE — ANTES DE DEVOLVER CADA CUE
- Faça uma microrevisão silenciosa do campo pt antes de responder.
- PT-BR precisa sair ortograficamente correto, salvo erro proposital que faça parte da fala/personagem.
- NUNCA produza palavras corrompidas como "nabeira" ou "Olurando".
- Prefira formas brasileiras naturais como "demoníaco" e "apodrecendo" quando esse for o sentido; evite formações estranhas como "demônico" ou "podrindo" por reflexo do inglês.
- Não invente gíria envelhecida/artificial para parecer informal. Evite "qualé" como escolha automática; use o registro natural daquela pessoa.
- Evite calques sem sentido como "tomar consistência" quando um brasileiro diria a ideia de outro modo.
- Leia a frase PT-BR inteira mentalmente: se parecer tradução mecânica, REESCREVA preservando sentido, identidade e cue.

ACESSIBILIDADE / SDH
- O texto recebido já passou por limpeza, mas se escapar qualquer descrição de som, ação, voz ou speaker label, NÃO a reproduza.
- Não devolva NOME:, [NOME], (ofegante), [porta fechando], (ao longe), descrição sonora, indicação de voz ou comentário de acessibilidade.
- Preserve somente o que é fala/diálogo verbal relevante.

CANTO / NOTAS ESTENDIDAS
- Traduza o conteúdo verbal, NÃO a duração vocal da nota.
- "I love you-u-u-u-u" -> "Eu te amo", nunca "Eu te amo-o-o-o-o".
- Não reproduza vogais ou sílabas repetidas apenas porque a pessoa sustentou uma nota.

FORMATAÇÃO
- Não adicione símbolos decorativos.
- Não devolva linhas com "/", "//", "---", "--", pipes ou sequências de traços como decoração.
- Não invente bullets, asteriscos ou notas musicais.
- Não adicione nomes de speaker, [NOME], NOME:, SDH ou comentários.
- Use hífen de diálogo apenas quando o próprio cue tiver DUAS OU MAIS falas/turnos separados.
- DIALOGUE TURN LOCK: cada linha-fonte iniciada por hífen representa um turno/speaker independente.
- Preserve EXATAMENTE a quantidade e a ordem desses turnos.
- No pt bruto, devolva cada turno em sua própria linha começando por "- ".
- Nunca transforme quebra visual dentro de uma fala em novo speaker.
- Nunca una dois speakers apagando a fronteira entre eles.
- O JavaScript fará a composição visual final em no máximo 2x50 sem perder os turnos.

FIDELIDADE E SINCRONIZAÇÃO
- Não resuma.
- Não invente fatos.
- Não omita finais de frase.
- Não mova conteúdo de um cue para outro.
- Não antecipe fala do cue seguinte.
- Cada id recebido deve voltar exatamente uma vez.
- O Gemini NÃO cria timestamps.
- Os timestamps são responsabilidade exclusiva do JavaScript.
`;

const PLAN_PROMPT = `
Você é editor de continuidade FONTE→PT-BR e responsável pelo CONTEXT + IDENTITY LOCK.
Leia a amostra do episódio e produza uma bíblia editorial CURTA e um Character Ledger confiável.

IMPORTANTE: o schema de saída é deliberadamente simples para máxima compatibilidade.
O campo people é um ARRAY DE STRINGS. Cada pessoa deve usar EXATAMENTE este formato textual:
canonical=NOME || aliases=ALIAS1, ALIAS2 || gender=female|male|nonbinary|unknown || pronouns=she/her ou he/him ou they/them ou vazio || relation=RELAÇÃO/CONTEXTO CURTO || confidence=high|medium|low || evidence=12,45,90

REGRAS DO CHARACTER LEDGER:
- Registre apenas pessoas realmente sustentadas pela amostra.
- canonical é o nome/identificador mais estável; aliases apenas variações realmente vistas.
- gender só pode ser female, male, nonbinary ou unknown. Use unknown se a evidência não for segura.
- pronouns refletem somente evidência clara; se não houver, deixe pronouns= e gender=unknown.
- relation descreve relações apenas quando claras.
- confidence mede a confiança na identidade/gênero/relação; não invente certeza.
- evidence recebe IDs de cues que sustentam a entrada, separados por vírgula, até 8 IDs.
- speaker é quem fala; pessoa mencionada é de quem se fala. Nunca transfira gênero entre elas.

TONE:
- Resuma registro, época, gênero da obra, faixa etária/estilo social dominante e o nível adequado de informalidade PT-BR contemporânea.
- Indique explicitamente se Gen Z/Alpha/fandom é central, ocasional ou inadequado para a maior parte da obra.

GLOSSARY/CONTINUITY:
- Extraia termos recorrentes, referências culturais, fandom, relações, bordões e escolhas de consistência.
- Inclua alertas contra literalidade/calques específicos que a amostra sugerir.
- Reconheça especialmente reality, drag, LGBTQIAPN+, Gen Z/Alpha, terror/drama, música, competições e linguagem censurada por bleep.

Não traduza o episódio. Não invente fatos. Não proponha tradução para tokens HARD LOCK.
`;

const PLAN_FALLBACK_PROMPT = `
Você é editor de continuidade FONTE→PT-BR. A saída estruturada principal não pôde ser usada.
Produza TODO o plano dentro do único campo string "plan", uma linha por registro, usando SOMENTE estes prefixos:
TONE=texto
PERSON=canonical=NOME || aliases=A1, A2 || gender=female|male|nonbinary|unknown || pronouns=she/her ou he/him ou they/them ou vazio || relation=texto || confidence=high|medium|low || evidence=1,2,3
GLOSSARY=texto
CONTINUITY=texto

Pode haver várias linhas PERSON/GLOSSARY/CONTINUITY.
Se gênero não estiver seguro, gender=unknown e pronouns=.
Speaker e pessoa mencionada são entidades diferentes. Não invente identidade, parentesco ou gênero.
Registre também o nível correto de PT-BR contemporâneo e se Gen Z/Alpha/fandom é central, ocasional ou inadequado.
`;

const TRANSLATOR_PROMPT = `
Você é o tradutor principal de legendas FONTE→PT-BR.

${STYLE_PACK}

Você receberá uma lista de CÁPSULAS.
Cada cápsula contém before, target, after e identity_lock.
Traduza SOMENTE target.
As cápsulas estão SEMPRE em ordem cronológica. Preserve rigorosamente essa ordem e nunca redistribua conteúdo entre IDs.

CHECKLIST SILENCIOSO OBRIGATÓRIO ANTES DE CADA pt:
1. Quem fala está realmente provado? Se não, não marque gênero de 1ª pessoa sem necessidade.
2. Há pessoa mencionada? Não transfira identidade do speaker para ela ou vice-versa.
3. A frase preserva a intenção e não a sintaxe do inglês?
4. Um brasileiro falaria isso espontaneamente em 2026, nesse registro?
5. A gíria é apropriada à pessoa/contexto, e não uma tentativa artificial de parecer jovem?
6. Todo conteúdo pertence somente a este target?
7. Se eu escondesse o inglês e lesse somente o PT, isso pareceria escrito originalmente em português brasileiro?
8. Existe alguma expressão, verbo ou ordem sintática que estou preservando apenas porque aparece assim em inglês?
9. Consigo dizer exatamente a mesma coisa de forma mais espontânea e/ou mais curta sem perder informação?
10. A frase cabe naturalmente como LEGENDA, e não como tradução acadêmica da sentença?
11. Se o resultado estiver correto porém literal, NÃO devolva ainda: reescreva.

Devolva exatamente um objeto por target, mantendo o mesmo id em i.
`;

const REPAIR_PROMPT = `
Você é editor final FONTE→PT-BR.

${STYLE_PACK}

Você receberá somente cues sinalizados por detectores locais e/ou pelo QA PT-BR.

NATURALNESS REPAIR
- Se o motivo incluir QA_PTBR, LITERAL, FALSE_COGNATE, IDIOM, UNNATURAL ou SUBTITLE_TOO_DENSE, não faça uma correção superficial.
- Releia EN + contexto + Character Ledger e reconstrua a fala em PT-BR espontâneo.
- NÃO preserve a sintaxe inglesa só porque a primeira tradução estava compreensível.
- O resultado reparado deve soar melhor que o MAIN, não apenas diferente.

SUBTITLE_TOO_DENSE
- Significa que a tradução atual não consegue ser diagramada confortavelmente em no máximo 2 linhas de 50 caracteres.
- Torne a frase MAIS CONCISA e MAIS NATURAL sem remover fatos, intenção, piada, shade, emoção, negação, referente ou informação importante.
- Remova redundância causada pela tradução, não conteúdo da fala.
- NÃO corte palavra.
- NÃO trunque a frase.
- NÃO mova conteúdo para outro cue.
- NÃO invente outro cue.
- NÃO crie nem altere timestamps.
- Se não houver forma segura de reduzir, preserve o conteúdo completo. Integridade vem antes do limite visual.

MEANING INTEGRITY DURANTE O REPAIR
- Antes de reescrever, identifique silenciosamente todas as unidades semânticas do EN.
- Sua nova versão só é válida se TODAS continuarem representadas.
- Compacte sintaxe, não significado.
- NÃO transforme duas informações em uma só mais genérica.
- NÃO apague relação familiar/social.
- NÃO apague causa, contraste, condição ou consequência.
- NÃO neutralize insulto ou intensidade apenas para diminuir caracteres.
- NÃO converta fala em [descrição], (descrição), *descrição* ou SDH.
- Se o PT atual estiver semanticamente mais completo que sua proposta, NÃO piore o cue.

Corrija defeitos reais de cultura, literalidade/calque, censura/bleep, gênero/referente, ortografia, palavra corrompida, naturalidade, SDH residual, omissão, overflow, formatação ou ownership.
- GENDER-NEUTRAL DEFAULT: se a SOURCE não marca gênero naquela ideia, reescreva a 1ª pessoa de forma naturalmente neutra mesmo quando o speaker for conhecido; o Ledger serve para evitar contradição, não para forçar marcação desnecessária.
- "Gramaticalmente correto" não basta se soar traduzido, antiquado ou pouco espontâneo em PT-BR contemporâneo.
- Se houver palavrão autocensurado graficamente, reconstrua a fala natural por extenso; nunca preserve f..., fu&#, *** ou equivalentes.
Preserve o que já estiver bom.
Não redistribua conteúdo entre ids.
`;

const COMPACT_RESCUE_PROMPT = `
Você é o editor audiovisual FINAL de legendas FONTE→PT-BR.

${STYLE_PACK}

Sua tarefa é MUITO específica:

Você receberá somente cues que, mesmo depois do MAIN + QA + REPAIR,
ainda NÃO conseguem ser diagramados em no máximo:

- ${LAYOUT_MAX_LINES} linhas;
- ${LAYOUT_MAX_CHARS_PER_LINE} caracteres por linha.

OBJETIVO
Reescreva SOMENTE o PT do mesmo cue para que fique:
1. semanticamente completo;
2. natural em PT-BR;
3. conciso;
4. diagramável em 2x50.

MEANING INTEGRITY LOCK
- Faça silenciosamente um inventário de TODAS as unidades de significado do EN.
- Nenhuma delas pode desaparecer apenas para atingir o limite.
- Preserve fatos, referentes, relações, negação, causa, contraste,
  condição, quantidade, intensidade, emoção, insulto, palavrão relevante,
  humor, shade e informação narrativa.
- Compacte a FORMA, nunca o CONTEÚDO.

COMO ECONOMIZAR
- reorganize completamente a sintaxe inglesa;
- use português mais direto;
- elimine sujeito/pronome redundante;
- elimine repetição puramente estrutural;
- prefira verbo curto a construção nominal longa;
- prefira expressão brasileira idiomática e curta;
- use contrações naturais quando combinarem com a personagem;
- procure ficar preferencialmente em até ${COMPACT_RESCUE_TARGET_TOTAL_CHARS}
  caracteres visíveis totais para dar margem ao reflow.

PROIBIDO
- cortar palavra;
- truncar frase;
- remover informação;
- mover palavras para outro cue;
- criar outro cue;
- criar ou alterar timestamp;
- transformar fala em SDH/stage direction;
- criar [som...], (som...), *som...* ou equivalentes;
- suavizar insulto/intensidade por conveniência;
- inventar sinônimo que altere a força social da fala.

DIÁLOGO
Se o original contém duas falas no mesmo cue, preserve as duas falas
e o formato de diálogo. Não una speakers diferentes.

HARD LOCK
Todos os tokens __LOCK_C...__ devem voltar IDÊNTICOS.

A resposta deve conter exatamente um objeto por cue recebido.
`;

const QA_PROMPT = `
Você é o revisor semântico e linguístico FINAL de legendas FONTE→PT-BR.

IMPORTANTE SOBRE O IDIOMA DA FONTE:
- "EN" neste prompt e nos campos internos é um rótulo legado para TEXTO-FONTE.
- O texto-fonte pode estar em inglês, espanhol ou outro idioma.
- Compare o PT com o idioma que realmente estiver presente no campo EN.
- Regras e exemplos lexicalmente específicos do inglês só se aplicam quando a fonte realmente estiver em inglês.

Você recebe EN e PT do MESMO cue, contexto curto e identity_lock. NÃO reescreva aqui: apenas sinalize IDs que devem ir para a única passada de repair.

SEJA EXIGENTE. CORRETO MAS LITERAL DEMAIS = DEFEITO.

NATURALIDADE É UM CRITÉRIO SEMÂNTICO, NÃO COSMÉTICO.
Uma frase deve ser sinalizada mesmo que não contenha "erro" tradicional se um brasileiro nativo perceber imediatamente que foi traduzida do inglês.

TESTE DE CALQUE:
- Tente mentalmente reconstruir o inglês olhando apenas o PT.
- Se a estrutura, metáfora, colocação ou ordem das ideias denunciar demais a frase inglesa, sinalize.
- Não exija equivalência lexical quando a intenção pede localização.

TESTE DE ORALIDADE:
- Leia mentalmente a frase em voz alta.
- Se parecer texto escrito/traduzido em vez da fala espontânea daquela pessoa, sinalize.
- Reality, confessional, conversa, discussão, piada e shade devem soar FALADOS.

TESTE DE CONCISÃO:
- Se o PT ficou muito maior ou mais burocrático que o necessário por seguir a estrutura inglesa, sinalize.
- Uma versão mais curta só é melhor quando preserva toda a informação e intenção.

EXEMPLOS:
- "picos e vales" para variação de performance pode ser calque; avalie "altos e baixos", "variação" etc.
- "eu aprecio isso" frequentemente é artificial em fala casual.
- "isso sendo dito" é calque.
- construções como "para ela não estar mais aqui" podem exigir reorganização conforme o contexto para soar realmente brasileira.

Para cada cue, pergunte silenciosamente:
1. Um brasileiro falaria isso espontaneamente em 2026 nessa situação?
2. A frase preserva a INTENÇÃO ou apenas copia estrutura/ordem do inglês?
3. Existe calque, falso cognato, metáfora literal, colocação estranha ou português de tradução/dublagem antiga?
4. O registro corresponde à idade, personalidade, classe, época, gênero da obra e comunidade do falante?
5. Identidade, gênero, pronome e referente estão realmente sustentados pelo Character Ledger/contexto?

MARQUE quando houver:
- sentido errado: pessoa verbal, sujeito, objeto, negação, tempo, intensidade ou referente;
- omissão, invenção ou conteúdo pertencente a outro cue;
- gênero incorreto de pessoa conhecida;
- concordância de 1ª pessoa masculina/feminina INTRODUZIDA pelo PT quando a SOURCE daquela ideia é neutra e uma formulação PT-BR natural sem gênero resolveria — mesmo se o speaker for conhecido;
- speaker desconhecido com concordância de 1ª pessoa desnecessariamente masculina/feminina quando uma forma neutra natural resolveria;
- confusão speaker ≠ pessoa mencionada;
- expressão idiomática/calque/falso cognato;
- tradução tecnicamente compreensível mas pouco natural, engessada, antiquada ou com sintaxe de inglês;
- formalidade sem motivo: "sequer", "de fato", "eu suponho", "eu aprecio isso" etc. quando o registro pede fala simples;
- Gen Z/Alpha/fandom ausente quando o contexto claramente pede OU injetado artificialmente quando não pede;
- ortografia, digitação, concordância, palavra inventada/corrompida;
- palavrão censurado/suavizado sem motivo ou intensidade aumentada sem base;
- speaker labels, SDH/CC, descrição sonora, créditos, símbolos, placeholders, gagueira gráfica/alongamento;
- quebra de continuidade audiovisual, palavras/letras exibidas na tela.

NAMED / CULTURAL ENTITY INTEGRITY — CRÍTICO

Compare as entidades nomeadas do EN com o PT.

SINALIZE se:
- uma pessoa, personagem, lenda, figura cultural, marca, obra, programa,
  música, instituição, lugar ou outra entidade foi trocada por OUTRA entidade;
- o PT abrasileirou uma identidade usando um equivalente cultural local;
- um nome desapareceu e foi substituído por uma explicação que muda sua identidade;
- a mesma entidade recebe identidades diferentes em cues próximos.

É permitido usar a forma canônica consagrada em PT-BR da MESMA entidade.
Não sinalize "Nova York" por "New York", por exemplo.

Na dúvida sobre existir uma forma canônica PT-BR,
preservar a entidade original é a escolha segura.

Quando houver substituição real de identidade,
reason deve começar com:

ENTITY_IDENTITY_SUBSTITUTION: identidade cultural alterada.

CUE OWNERSHIP / SEMANTIC SYNC — CRÍTICO

Para CADA cue, compare exclusivamente o EN daquele ID
com o PT daquele MESMO ID.

É ERRO CRÍTICO se:
- o PT traduz claramente o EN do cue anterior;
- o PT traduz claramente o EN do cue seguinte;
- o PT repete a tradução do cue anterior enquanto o EN mudou;
- uma sequência de PT parece deslocada em +1 ou -1 cue;
- informação do target desapareceu e reapareceu no ID vizinho;
- o PT contém conteúdo principal pertencente a before/after;
- um cue ficou com a fala pertencente a outro timestamp.

Quando detectar isso, reason DEVE começar exatamente com:

CUE_OWNERSHIP_SHIFT:

Depois explique brevemente.

Não confunda continuação legítima de uma frase entre cues
com deslocamento. Cada ID só pode conter a parte que pertence
ao EN desse mesmo ID.

PADRÕES OBJETIVOS A EVITAR:
- "Why do you let them hurt me?" não pode virar algo com "te machucarem";
- "They alerted..." não pode virar "Alertei...";
- "for once" não é "por um dia";
- "Shh, shh" não é "Xis, xis";
- "have you even been to sleep yet?" não é automaticamente "você sequer dorme?";
- "totally crazy" não pode virar "totalmente loucura";
- "my child" deve respeitar gênero conhecido, mas se o gênero NÃO estiver conhecido não invente;
- "subjects" não deve virar automaticamente "sujeitos" se significar pessoas pesquisadas/entrevistadas;
- cry wolf / give them a holler / at the end of the day / that being said exigem intenção idiomática;
- actually≠atualmente, eventually≠eventualmente por reflexo, realize≠realizar no sentido de perceber, pretend≠pretender no sentido de fingir;
- "malevolent force" não deve virar português infantil/artificial como "força maldosa";
- não use "qualé", "pistola" ou construções como "bêbada que só a porra" por automatismo estilístico.

MEANING INTEGRITY AUDIT — OBRIGATÓRIO
Antes de decidir que um cue está correto, compare EN×PT por UNIDADES DE SIGNIFICADO.

Pergunte silenciosamente:
1. Cada fato do EN ainda existe no PT?
2. Toda relação relevante ainda existe?
3. Alguma informação depois de "and", "but", "because", "if", "while" etc. desapareceu?
4. A intensidade emocional foi preservada?
5. Um insulto virou palavra neutra?
6. Um palavrão/intensificador desapareceu de maneira que mudou o tom?
7. Uma fala virou descrição de som/SDH?
8. O tradutor compactou tanto que virou resumo?

MARQUE para Repair quando houver perda real, mesmo que o PT final:
- esteja gramaticalmente correto;
- soe natural;
- esteja curto;
- caiba perfeitamente na tela.

Naturalidade SEM fidelidade não passa.

CASOS-TESTE DO TIPO DE ERRO:
- "relationships and family connections": ambas as ideias precisam sobreviver; "relações" sozinho pode apagar os laços familiares.
- "how in the hell": não precisa de tradução lexical, mas a ênfase/força da fala não pode simplesmente desaparecer.
- "fire crotch": não pode ser domesticado automaticamente para algo neutro como "ruivinha"; preserve a função de insulto vulgar/cômico conforme o contexto.
- "Insert rattlesnakes.": se for uma instrução falada, deve continuar sendo fala; NÃO transformar em "[som de cascavel]".
Os exemplos definem o TIPO de falha. Não os copie mecanicamente.

NÃO marque uma escolha apenas diferente se ela for realmente correta, espontânea e adequada ao registro.
Tô/tá/pra/né e palavrões por extenso podem ser ótimos quando combinarem com a personagem.
Se houver duas boas traduções naturais, NÃO marque.
Se a opção atual soar como tradução mesmo estando entendível, MARQUE.
`;

const PRE_REPAIR_CONFIRM_PROMPT = `
Você é o AUDITOR SEMÂNTICO PRÉ-REPAIR de legendas SOURCE→PT-BR.

Sua função é CONFIRMAR OU DESCARTAR SOMENTE suspeitas heurísticas ambíguas.
Você NÃO reescreve tradução. Você NÃO melhora estilo. Você NÃO marca uma
alternativa apenas porque faria diferente.

Para cada target:
- SOURCE do mesmo i é a autoridade absoluta de conteúdo e ownership;
- PT do mesmo i é a tradução atual;
- before/after existem SOMENTE para contexto;
- nunca puxe conteúdo de vizinhos para o target.

POSSIBLE_CUE_SHIFT_PAIR:
Marque SOMENTE se PT[i] traduz conteúdo pertencente claramente a SOURCE
de outro cue, ou se informação pertencente a SOURCE[i] está deslocada para
um vizinho. Diferença natural de tamanho, ordem sintática ou uma frase que
continua legitimamente entre cues NÃO é shift.

POSSIBLE_OMISSION:
Marque SOMENTE quando uma unidade de significado real de SOURCE[i] estiver
ausente em PT[i]. Conciliação, contração e tradução não literal fiel não são
omissão.

GENDER_V2_UNKNOWN_SPEAKER_MARKED / UNKNOWN_SPEAKER_GENDER_MARKED:
Marque SOMENTE se PT atribui gênero a speaker/referente sem evidência segura
da SOURCE, identity_lock ou contexto fornecido. Não marque gênero que esteja
claramente sustentado pela fala/cena.

REGRA FAIL-SAFE DO AUDITOR:
- se houver defeito real, inclua o ID em issues e explique a prova;
- se a suspeita heurística for falso positivo, NÃO inclua o ID;
- não proponha pt novo; não faça revisão cosmética.

Cada rodada é um julgamento independente.
`;

const SEMANTIC_REWRITE_AUDIT_PROMPT = `
Você é o AUDITOR SEMÂNTICO PÓS-REESCRITA de legendas FONTE→PT-BR.

IMPORTANTE: o campo EN é um nome legado para a legenda-fonte e pode conter inglês, espanhol ou outro idioma. Julgue sempre o idioma realmente presente nesse campo.

Sua função NÃO é melhorar estilo por preferência.
Sua função NÃO é retraduzir tudo.
Sua função NÃO é deixar a legenda mais longa.

Você receberá somente cues que sofreram REESCRITA
depois da tradução principal.

Para cada cue, compare rigorosamente:

1. EN = legenda-fonte;
2. BEFORE_PT = tradução antes do Repair/Compact Rescue;
3. AFTER_PT = resultado candidato a final;
4. BEFORE_CONTEXT / AFTER_CONTEXT = contexto para entender a cena,
   nunca autorização automática para mover conteúdo entre cues.

============================================================
ABSOLUTE CUE OWNERSHIP — PRIORIDADE MÁXIMA
============================================================

ANTES de comparar BEFORE_PT com AFTER_PT, faça este teste:

AFTER_PT realmente traduz o EN deste MESMO i?

O EN do target é a autoridade absoluta de ownership.

- BEFORE_CONTEXT e AFTER_CONTEXT servem SOMENTE para entender a cena.
- Nunca use conteúdo dos cues vizinhos como conteúdo do target.
- Se AFTER_PT traduz o EN do cue anterior ou seguinte, sinalize e CORRIJA.
- Se o EN atual perdeu informação porque a tradução ficou deslocada,
  sinalize e CORRIJA.
- Se BEFORE_PT já estava deslocado e AFTER_PT manteve o mesmo erro,
  ISSO CONTINUA SENDO ERRO.
- Não é necessário existir piora de BEFORE_PT para AFTER_PT.
- Cada correção deve traduzir SOMENTE o EN pertencente ao mesmo i.
- Evidência explícita do EN atual sobre gênero/pronomes vence qualquer
  Character Ledger conflitante.
- Se gênero não estiver seguro, prefira PT-BR naturalmente neutro.

============================================================
PRINCÍPIO CENTRAL
============================================================

Preserve a FALA/INTENÇÃO PROVÁVEL da cena.

NÃO seja escravo de um erro evidente da legenda-fonte,
mas também NÃO invente conteúdo apenas porque ele parece plausível.

NAMED / CULTURAL ENTITY INTEGRITY

Uma reescrita NÃO pode mudar a identidade de uma entidade nomeada.

- Preserve pessoas, personagens, lendas, figuras culturais, marcas,
  obras, programas, músicas, instituições, lugares e demais entidades.
- Forma canônica PT-BR da MESMA entidade é válida.
- Entidade brasileira/culturalmente análoga NÃO é a mesma entidade.
- Nunca aceite adaptação cultural que substitua uma identidade por outra.
- Se BEFORE_PT preservava corretamente a entidade e AFTER_PT a substituiu,
  isso é REGRESSÃO SEMÂNTICA.
- Se EN contém a entidade e AFTER_PT a trocou por outra, CORRIJA.
- Na dúvida, preserve o nome original.

============================================================
CULTURE & REGISTER INTEGRITY
============================================================

Mudança de FUNÇÃO SOCIAL também é regressão semântica.

Audite cuidadosamente gírias, palavrões, insultos, vocativos,
bordões, referências culturais e linguagem de fandom.

NÃO use equivalências lexicais automáticas.

BITCH
- "bitch" é altamente contextual.
- NÃO exija "puta", "vadia", "bicha" ou qualquer tradução fixa.
- Determine se é ataque, afeto, camp, cumplicidade, admiração,
  autoelogio, orgulho, provocação ou outro uso.
- Se BEFORE/AFTER transformar afeto em insulto, insulto em carinho,
  autoafirmação em ofensa ou vice-versa, marque como regressão.
- "I'm a bad bitch" é tipicamente autoafirmação/empoderamento,
  não "sou uma puta ruim".

PALAVRÕES
- Compare a FORÇA pragmática, não a contagem de palavrões.
- Marque se AFTER_PT suavizar injustificadamente raiva, vulgaridade,
  insulto, humor ou intensidade importante.
- Marque também se AFTER_PT acrescentar palavrão/agressividade
  que o EN não sustenta.
- O palavrão PT deve parecer organicamente pertencente àquela fala.

GAG / DRAG / INTERNET SLANG
- Diferencie reaction "gag/gagged" de sentido físico.
- Verifique ate, slay, shade, tea, read, serving, bottom e termos
  semelhantes pelo sentido cultural/contextual, nunca pelo dicionário.
- Não permita calque literal que destrua o sentido social.

BORDÕES / FRASES CONHECIDAS
- O campo canonical_locks contém formas canônicas literais.
- Cada canonical_lock deve sobreviver EXATAMENTE.
- Nesta auditoria NÃO devolva tokens __LOCK_C...__.
- Trabalhe diretamente com a forma canônica real.
- Uma frase conhecida não protegida por token ainda deve preservar
  seus elementos distintivos, intensidade, humor e estrutura retórica.
- Não aceite simplificação que transforme um bordão reconhecível
  em frase genérica.

Uma tradução pode preservar os fatos e ainda assim estar ERRADA
se destruir a personalidade, o registro ou o efeito social da fala.

============================================================
REGRESSÃO SEMÂNTICA
============================================================

Marque quando AFTER_PT perder, distorcer ou trocar informação
que está claramente presente no EN.

Audite especialmente:

- ação/verbo principal;
- sujeito;
- objeto/alvo;
- negação;
- fato ou estado;
- causa e consequência;
- condição;
- contraste;
- relação familiar/social;
- quantidade;
- números;
- unidades de medida ou tempo;
- duração;
- enumerações;
- intensidade;
- palavrão/intensificador semanticamente relevante;
- insulto e força social;
- humor/shade;
- referente;
- informação depois de "and", "but", "because", "if" etc.;
- informação narrativa nova.

Exemplos do TIPO de regressão:
- uma fala sobre pessoas se beijando não pode perder a ação de beijar;
- "eight minutes and three seconds" não pode perder "seconds";
- "relationships and family connections" não pode apagar os laços familiares;
- um insulto forte não pode virar termo neutro só para economizar espaço.

============================================================
SOURCE DEFECT RECOVERY — REPARO CONSERVADOR DA FONTE
============================================================

Uma frase EN aparentemente incompleta NÃO deve ser tratada
automaticamente como conteúdo proibido de completar.

Primeiro classifique silenciosamente o caso:

A) CONTINUA NO PRÓXIMO CUE
- A estrutura, gramática ou sentido mostram claramente que a frase
  continua no AFTER_CONTEXT.
- Nesse caso, NÃO antecipe nem puxe a continuação.
- CUE OWNERSHIP é absoluto.

B) INTERRUPÇÃO REAL DA FALA
Sinais possíveis:
- reticências expressivas;
- travessão/corte;
- outra pessoa interrompe;
- mudança abrupta de speaker;
- a interrupção faz sentido dramático;
- a frase foi propositalmente abandonada.
Nesse caso, PRESERVE a interrupção.

C) PROVÁVEL DEFEITO/TRUNCAMENTO DA LEGENDA-FONTE
Pode completar SOMENTE quando houver evidência forte de que:
- o EN termina de forma gramatical ou semanticamente quebrada;
- não há sinal razoável de interrupção real;
- o próximo cue começa outra fala ou outra ideia;
- o contexto torna a intenção praticamente inequívoca;
- existe uma conclusão mínima e genérica que recupera a intenção
  sem inventar fato específico.

Exemplo permitido em princípio:
"If I can get through Snatch Game, I can get through..."
→ algo equivalente a
"Se passo pelo Snatch Game, passo por qualquer coisa."
SE o contexto sustentar claramente essa conclusão.

Exemplo NÃO permitido:
→ "Se passo pelo Snatch Game, vou ganhar a competição."
Isso inventa informação específica não sustentada pela fonte.

D) AMBÍGUO
Se houver duas ou mais continuações plausíveis,
ou dúvida real entre truncamento da legenda e interrupção da fala:
NÃO COMPLETE.
Preserve a ambiguidade/incompletude.

============================================================
COMO JULGAR AFTER_PT
============================================================

Se AFTER_PT fez um SOURCE DEFECT RECOVERY conservador e bem sustentado:
- NÃO marque como regressão;
- não exija que volte a ficar incompleto.

Se AFTER_PT inventou uma conclusão específica sem evidência:
- marque.

Se AFTER_PT perdeu informação claramente presente no EN:
- marque.

Se AFTER_PT apenas reformulou livremente,
mas todo o sentido e efeito social sobreviveram:
- NÃO marque.

Não exija correspondência palavra por palavra.
Equivalência idiomática e pragmática é correta.

============================================================
CORREÇÃO
============================================================

Se AFTER_PT estiver semanticamente correto:
NÃO devolva o cue em issues.

Se houver regressão real:
- devolva o id;
- explique resumidamente em reason;
- forneça em pt uma correção natural, completa e concisa.

A correção DEVE:
- permanecer no MESMO cue;
- preservar exatamente todos os canonical_locks;
- preservar Meaning Integrity;
- respeitar SOURCE DEFECT RECOVERY;
- não criar SDH;
- não inventar informação;
- não mover conteúdo entre cues;
- não alterar timestamp;
- ser diagramável em no máximo ${LAYOUT_MAX_LINES} linhas
  de ${LAYOUT_MAX_CHARS_PER_LINE} caracteres;
- preferencialmente ficar em até ${COMPACT_RESCUE_TARGET_TOTAL_CHARS}
  caracteres visíveis totais.

Meta final:
SIGNIFICADO / INTENÇÃO COMPLETOS
+ PT-BR NATURAL
+ 2x50.
`;

const SEMANTIC_COMPACT_RETRY_PROMPT = `
Você é o COMPACTADOR SEMÂNTICO FINAL de um único cue FONTE→PT-BR.

A tradução recebida já foi identificada como semanticamente necessária.

Sua única tarefa é reescrever a FORMA para caber em:

- no máximo ${LAYOUT_MAX_LINES} linhas;
- no máximo ${LAYOUT_MAX_CHARS_PER_LINE} caracteres por linha.

REGRA ABSOLUTA:
NÃO remova nenhuma unidade de significado da correção recebida.

Preserve:
- ação;
- sujeito;
- objeto;
- números;
- unidades;
- relações;
- negação;
- intensidade;
- insulto;
- humor;
- palavrão relevante;
- informação narrativa.

Pode:
- mudar completamente a sintaxe;
- usar contrações naturais;
- eliminar sujeito redundante;
- escolher formulação PT-BR mais curta;
- usar números em algarismos quando natural.

Não pode:
- resumir conteúdo;
- apagar informação;
- inventar;
- mover conteúdo;
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
    throw new Error("SYNC PROXY sem itens válidos.");
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
    throw new Error("SYNC ALIGN sem itens válidos.");
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
    `[SYNC ALIGN] ${out.filter(item => item.matched).length}/${out.length} âncora(s) semanticamente alinhadas | ` +
    `source=${src} | audio=${aud}.`
  );

  return out;
}

// Deliberadamente simples.
// O 8.3.5 estava enviando objetos aninhados em people e o PLAN
// recebia HTTP 400. Agora Gemini devolve strings simples e o
// JavaScript reconstrói o Character Ledger rico.
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
// não deixa o Character Ledger simplesmente desaparecer.
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
  // 8.7: NÃO codificamos o tamanho do lote no JSON Schema.
  // O parser local abaixo já exige contagem exata, ordem exata, IDs e ownership_key.
  // Isso evita rejeição do request pelo provedor quando lotes grandes (ex.: 140)
  // são expressos como minItems/maxItems rígidos, sem relaxar nenhuma validação.
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

async function acquireGeminiSlot(
  job
) {
  let release;

  const previous =
    geminiGate;

  geminiGate =
    new Promise(resolve => {
      release = resolve;
    });

  await previous;

  try {
    const wait =
      Math.max(
        0,

        GEMINI_MIN_START_INTERVAL_MS -
        (
          Date.now() -
          lastGeminiRequestStart
        )
      );

    if (wait > 0) {
      if (job) {
        job.stats.pacerWaitMs +=
          wait;
      }

      await sleep(wait);
    }

    lastGeminiRequestStart =
      Date.now();
  } finally {
    release();
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

async function geminiRequest({
  system,
  user,
  schema,
  thinkingLevel,
  maxOutputTokens,
  timeoutMs,
  maxRetries,
  job = null,
  metric = "main"
}) {
  if (!GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY não configurada."
    );
  }

  let lastError = null;

  for (
    let attempt = 1;
    attempt <= maxRetries;
    attempt++
  ) {
    markAttempt(
      job,
      metric
    );

    await acquireGeminiSlot(
      job
    );

    const controller =
      new AbortController();

    const timer =
      setTimeout(
        () =>
          controller.abort(),
        timeoutMs
      );

    try {
      console.log(
        `[GEMINI ${
          metric.toUpperCase()
        }] ${
          GEMINI_MODEL
        } request ${
          attempt
        }/${
          maxRetries
        } | thinking=${
          thinkingLevel
        }.`
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
                  GEMINI_MODEL,

                input:
                  user,

                system_instruction:
                  system,

                response_format: {
                  type:
                    "text",

                  mime_type:
                    "application/json",

                  schema
                },

                generation_config: {
                  max_output_tokens:
                    maxOutputTokens,

                  thinking_level:
                    thinkingLevel
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

      if (
        response.ok &&
        data
      ) {
        const status =
          String(
            data?.status ||
            "completed"
          ).toLowerCase();

        const text =
          extractInteractionText(
            data
          );

        if (
          [
            "failed",
            "cancelled",
            "budget_exceeded"
          ].includes(status)
        ) {
          const error =
            new Error(
              `Gemini ${
                metric
              } status=${
                status
              }: ${
                String(
                  data?.error
                    ?.message ||
                  data?.message ||
                  "sem detalhe"
                ).slice(
                  0,
                  1200
                )
              }`
            );

          error.nonRetryable =
            status ===
            "budget_exceeded";

          throw error;
        }

        if (
  status === "incomplete" ||
  !text
) {
  const incompleteDetail =
    data?.incomplete_details ??
    data?.incompleteDetails ??
    data?.finish_reason ??
    data?.finishReason ??
    data?.response?.finish_reason ??
    data?.response?.finishReason ??
    null;

  let detailText = "sem detalhe adicional da API";

  if (incompleteDetail != null) {
    try {
      detailText = JSON.stringify(incompleteDetail).slice(0, 900);
    } catch {
      detailText = String(incompleteDetail).slice(0, 900);
    }
  }

  throw new Error(
    status === "incomplete"
      ? `Gemini ${metric} retornou INCOMPLETE | detalhe=${detailText}`
      : `Gemini ${metric} retornou vazio.`
  );
}

        markSuccess(
          job,
          metric,
          data
        );

        console.log(
          `[GEMINI ${
            metric.toUpperCase()
          }] OK | input=${
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
          } | thought=${
            Number(
              data?.usage
                ?.total_thought_tokens ||
              0
            )
          }.`
        );

        return {
          text,
          status,
          usage:
            data?.usage || {}
        };
      }

      const error =
        new Error(
          `GEMINI ${
            GEMINI_MODEL
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
              1600
            )
          }`
        );

      error.status =
        response.status;

      if (
        response.status === 429
      ) {
        mark429(
          job,
          metric
        );

        if (
          attempt ===
          maxRetries
        ) {
          throw error;
        }

        const wait =
          retryDelayMs(
            response,
            data,
            attempt
          );

        console.warn(
          `[GEMINI ${
            metric.toUpperCase()
          }] 429; mesmo lote em ${
            (wait / 1000).toFixed(1)
          }s.`
        );

        await sleep(wait);

        continue;
      }

      const retryable =
        [
          408,
          409,
          425
        ].includes(
          response.status
        ) ||
        response.status >= 500;

      if (
        !retryable ||
        attempt === maxRetries
      ) {
        throw error;
      }

      await sleep(
        Math.min(
          4000 * attempt,
          20000
        )
      );
    } catch (error) {
      lastError =
        error?.name ===
          "AbortError"
          ? new Error(
              `Gemini ${metric}: timeout.`
            )
          : error;

      if (
        lastError?.nonRetryable
      ) {
        throw lastError;
      }

      if (
        lastError?.status === 429
      ) {
        if (
          attempt === maxRetries
        ) {
          throw lastError;
        }

        continue;
      }

      if (
        lastError?.status &&
        lastError.status < 500 &&
        ![
          408,
          409,
          425
        ].includes(
          lastError.status
        )
      ) {
        throw lastError;
      }

      if (
        attempt === maxRetries
      ) {
        throw lastError;
      }

      await sleep(
        Math.min(
          4000 * attempt,
          20000
        )
      );
    } finally {
      clearTimeout(timer);
    }
  }

  throw (
    lastError ||
    new Error(
      `Gemini ${metric} falhou.`
    )
  );
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
      "GEMINI_API_KEY não configurada."
    );
  }

  const cleanBase64 =
    String(
      audioBase64 || ""
    ).trim();

  if (!cleanBase64) {
    throw new Error(
      "Áudio vazio."
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
        } | duração≈${
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
            "Gemini Transcribe não retornou timestamps de palavras."
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
          `[GEMINI TRANSCRIBE] 429; budget manager preserva o próximo início e retry em ${
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
    /^(nonbinary|non binary|nb|nao binario|não binário|they them)$/.test(
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
        "PT-BR contemporâneo, natural e fiel ao registro."
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
        "speaker label observado na legenda; gênero não inferido localmente",

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
      "PT-BR contemporâneo, natural e fiel ao registro; evitar calques e linguagem de tradução.",

    people:
      speakers.slice(
        0,
        30
      ),

    glossary: [],

    continuity: [
      "Gender safety: gênero desconhecido nunca deve ser adivinhado; neutralizar concordância quando possível.",
      "Naturalidade: correto mas literal demais deve ser reescrito em PT-BR espontâneo.",
      "Speaker e pessoa mencionada são entidades distintas."
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
      } | gênero conhecido=${
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
      `[EPISODE PLAN] RECUPERADO ULTRA-SIMPLE ✅ | Character Ledger=${
        stats.people
      } | gênero conhecido=${
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
      `[EPISODE PLAN] fallback Gemini também falhou: ${
        errorMessage(
          error
        ).slice(
          0,
          300
        )
      } | usando SAFETY FALLBACK local com neutralização obrigatória.`
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

  // O Ledger só pode IMPOR gênero quando:
  // 1. a confiança é HIGH;
  // 2. há pronome explícito compatível.
  //
  // Nome, aparência presumida ou palpite do Planner
  // nunca bastam para marcar concordância em PT-BR.
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

    // Nunca exponha ao tradutor um gênero
    // que o próprio backend não considera confiável.
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
      "speaker é quem fala; mentions são pessoas citadas. Nunca transfira gênero/pronomes. Sem evidência segura, neutralize em PT-BR natural.",

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
        ? `DEFAULT_NEUTRALIZE: gênero confiável=${trustedGender} serve para impedir contradições, não para introduzir gênero em 1ª pessoa quando a SOURCE é neutra; prefira formulação PT-BR neutra natural`
        : "STRICT_NEUTRALIZE: não adivinhar masculino/feminino de 1ª pessoa; preferir formulação sem marca de gênero quando natural",

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
    // g é apenas guard de contradição/referente. MAIN deve neutralizar
    // estados de 1ª pessoa quando a SOURCE não marca gênero.
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

    const turns =
      sourceDialogueDashCount(block);

    return {
      i: block.index,
      k: ownershipKey,
      en: protectedTarget.text,
      idn: compactIdentityHint(block, plan),
      ...(turns >= 2 ? { turns } : {}),
      ...(protectedTarget.locks.length
        ? { hard: protectedTarget.locks.map(lock => lock.token) }
        : {})
    };
  });

  return {
    payload: {
      rule:
        "Lista cronológica. Traduza exclusivamente en do mesmo i. " +
        "k pertence ao mesmo cue e deve voltar idêntico. " +
        "before/after são contexto compartilhado, nunca conteúdo do target. " +
        "idn.g só é confiável quando diferente de unknown; sem prova, neutralize gênero naturalmente. " +
        "Não antecipe, atrase, duplique ou mova conteúdo entre IDs.",

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
      "JSON de tradução inválido."
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
      `Quantidade de cues inválida: ` +
      `${parsed.cues.length}/${ids.length}.`
    );
  }

  // Somente o MAIN ativa isto.
  // Repair/Compact continuam compatíveis com o parser.
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
          `posição ${i} esperava ID ${ids[i]}, ` +
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
    // PT-BR VOCAB SOFT LOCK — "qualé/diacho"
    // ============================================================
    // Preferência editorial continua valendo, mas nunca pode
    // derrubar um lote ou um episódio inteiro.
    //
    // QA/Repair ainda podem reformular contextualmente.
    // A rede final determinística será aplicada no buildSrt().
    if (
      /(?:^|[^\p{L}\p{N}_])(?:qualé|diacho)(?=$|[^\p{L}\p{N}_])/iu.test(
        pt
      )
    ) {
      console.warn(
        `[PT-BR VOCAB SOFT LOCK] cue ${id}: "qualé/diacho" detectado; ` +
        `lote preservado e correção final garantida sem abortar o episódio.`
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
      `Tradução estruturalmente incompleta ` +
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
      `Tradução incompleta ` +
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
    throw new Error("JSON MAIN inválido.");
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

    if (/(?:^|[^\p{L}\p{N}_])(?:qualé|diacho)(?=$|[^\p{L}\p{N}_])/iu.test(pt)) {
      console.warn(
        `[PT-BR VOCAB SOFT LOCK] cue ${id}: "qualé/diacho" detectado; ` +
        `candidato preservado para correção focal.`
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
      `[MAIN ROBUST PARSER 8.8.1] extras=${ignoredExtras} | ` +
      `duplicados=${ignoredDuplicates} | ownership-inválido=${badOwnership}; ` +
      `cues válidos foram preservados, sem retraduzir o lote.`
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
  if (
    sourceCueAllowsIntentionalEmpty(
      block,
      job
    )
  ) {
    markIntentionalEmptyCue(
      job,
      block,
      "SOURCE descartável; nenhuma chamada Gemini extra"
    );

    return "";
  }

  const rescueBatch = [
    block
  ];

  let lastError = null;
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
              `EMPTY-CUE RESCUE DO MAIN — 8.4.6.\n\n` +
              `A SOURCE deste cue contém conteúdo real e NÃO pode desaparecer. ` +
              `A resposta só será aceita se continuar válida DEPOIS do sanitizer final.\n\n` +
              `IDIOMA DA FONTE: ${job.sourceLang || "auto"}\n\n` +
              `BÍBLIA EDITORIAL:\n${
                JSON.stringify(
                  plan
                )
              }\n\n` +
              `CÁPSULA CUE-LOCK ÚNICA:\n${
                JSON.stringify(
                  payload
                )
              }\n\n` +
              `Retorne EXATAMENTE 1 cue. ` +
              `Copie o mesmo i e o ownership_key exatamente para k. ` +
              `O campo pt DEVE traduzir SOMENTE o target deste ID e preservar o conteúdo semântico. ` +
              `NÃO devolva vazio, "...", reticências isoladas, [suspiro], (suspiro), ` +
              `ruído SDH, speaker label isolado, placeholder ou descrição de acessibilidade. ` +
              `Se o target terminar em dois-pontos e for fala/narração real, formule PT-BR natural ` +
              `que não pareça um rótulo de speaker isolado. ` +
              `Não invente fala. Não use before/after como conteúdo do target. ` +
              `Todos os tokens __LOCK_C...__ devem voltar idênticos. ` +
              `O token ${BLEEP_TOKEN} deve ser resolvido em linguagem natural, nunca copiado.`,

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
                `omitido corretamente após ${rejectedSdhCandidates.length} ` +
                `respostas SDH independentes; retry encerrado ✅.`
              );

              return "";
            }
          }

          throw new Error(
            `Cue ${block.index} virou vazio/lixo após sanitizer | raw=${
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
          `[MAIN EMPTY-CUE RESCUE] cue ${block.index} recuperado ✅ | ` +
          `${sanitized.length} chars pós-sanitizer.`
        );

        return sanitized;
      } catch (error) {
        lastError = error;

        if (
          parseAttempt <
          MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS
        ) {
          job.stats.mainEmptyCueRescueParseRetries++;

          console.warn(
            `[MAIN EMPTY-CUE RESCUE] cue ${block.index} ainda inválido: ${
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

    job.stats.mainEmptyCueRescueFailures++;
  }

  // Liveness sem sacrificar o restante do episódio: nunca reinicia MAIN e
  // nunca agenda ciclos eternos. Uma fala real fica preservada como base
  // temporária; QA/Repair ainda tentam localizá-la antes do gate final.
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
      `[MAIN EMPTY-CUE SAFE FALLBACK] cue ${block.index} encerrou ` +
      `${MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS} tentativa(s) sem consenso SDH; ` +
      `conteúdo preservado para QA/Repair | ${
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
            `BÍBLIA EDITORIAL:\n${
              JSON.stringify(
                plan
              )
            }\n\n` +
            `LOTE CUE-LOCK COM CONTEXTO COMPARTILHADO:\n${
              JSON.stringify(
                payload
              )
            }\n\n` +
            `Os cues estão em ORDEM CRONOLÓGICA. ` +
            `Retorne os IDs EXATAMENTE na mesma ordem recebida. ` +
            `Output exatamente ${
              batch.length
            } cues. ` +
            `Para cada item de cues, copie k EXATAMENTE para o campo k do mesmo ID. ` +
            `Traduza SOMENTE en do mesmo item para pt. ` +
            `Nunca use em pt conteúdo pertencente a outro ID ou aos contextos before/after. ` +
            `Todos os tokens __LOCK_C...__ recebidos em en devem voltar idênticos em pt. ` +
            `O token ${BLEEP_TOKEN} deve ser resolvido em linguagem natural, nunca copiado.`,

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

      const parsed =
        parseMainCueTranslationRobust(
          batch,
          response.text,
          locksById,
          ownershipById
        );

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
        `[MAIN FOCAL RESCUE 8.8.1] preservando ${parsed.translations.size}/${batch.length} ` +
        `cues válidos; refazendo SOMENTE ${rescueIds.length} cue(s): ` +
        `${rescueIds.join(", ")}.`
      );

      const batchById = new Map(
        batch.map(block => [block.index, block])
      );

      for (const id of rescueIds) {
        const block = batchById.get(id);

        if (!block) {
          throw new Error(`MAIN FOCAL RESCUE: bloco ${id} não encontrado.`);
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
        if (batch.length > 90 && splitDepth < 1) {
          const mid = Math.ceil(batch.length / 2);
          const leftBatch = batch.slice(0, mid);
          const rightBatch = batch.slice(mid);

          console.warn(
            `[MAIN ADAPTIVE 8.7] request grande recebeu erro determinístico; ` +
            `dividindo ${batch.length} cues uma única vez em ${leftBatch.length}+${rightBatch.length}, ` +
            `sem repetir o mesmo request e sem reiniciar o job.`
          );

          const [left, right] = await Promise.all([
            translateMainBatch({ blocks, posMap, batch: leftBatch, plan, job, splitDepth: splitDepth + 1 }),
            translateMainBatch({ blocks, posMap, batch: rightBatch, plan, job, splitDepth: splitDepth + 1 })
          ]);
          return new Map([...left, ...right]);
        }

        error.noFullBatchRetry = true;
        error.noJobRetry = true;
        console.error(
          `[MAIN FAIL-FAST 8.7] erro determinístico do request; ` +
          `não será martelado até virar 429 nem reiniciará o job | ${errorMessage(error).slice(0, 360)}`
        );
        throw error;
      }

      if (
        error?.noFullBatchRetry
      ) {
        console.error(
          `[MAIN CUE-LOCK] rescue isolado falhou; lote inteiro NÃO será retraduzido: ${
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
          `[MAIN CUE-LOCK] lote rejeitado definitivamente após ${
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
  const batches =
    buildMainBatches(
      blocks
    );

  const translations =
    new Map();

  const posMap =
    positionMap(
      blocks
    );

  job.stats.mainBatches =
    batches.length;

  console.log(
    `[MAIN] ${
      blocks.length
    } cues -> ${
      batches.length
    } lotes | concorrência=${
      MAIN_CONCURRENCY
    } | até ${
      MAIN_BATCH_MAX_CUES
    } cues.`
  );

  let cursor = 0;
  let completed = 0;

  async function worker(
    workerId
  ) {
    while (true) {
      const batchIndex =
        cursor++;

      if (
        batchIndex >=
        batches.length
      ) {
        return;
      }

      const batch =
        batches[
          batchIndex
        ];

      console.log(
        `[MAIN W${
          workerId
        }] lote ${
          batchIndex + 1
        }/${
          batches.length
        }: ${
          batch.length
        } cues.`
      );

      const translated =
        await translateMainBatch({
          blocks,
          posMap,
          batch,
          plan,
          job
        });

      for (
        const [
          id,
          pt
        ] of translated
      ) {
        translations.set(
          id,
          pt
        );
      }

      completed++;

      job.progress =
        Math.min(
          90,

          5 +
          Math.round(
            85 *
            completed /
            batches.length
          )
        );

      job.updatedAt =
        Date.now();

      console.log(
        `[MAIN W${
          workerId
        }] lote ${
          batchIndex + 1
        } OK | ${
          translations.size
        }/${
          blocks.length
        } | ${
          job.progress
        }%.`
      );
    }
  }

  await Promise.all(
    Array.from(
      {
        length:
          Math.min(
            MAIN_CONCURRENCY,
            batches.length
          )
      },

      (_, index) =>
        worker(index + 1)
    )
  );

  if (
    translations.size !==
    blocks.length
  ) {
    throw new Error(
      `Tradução principal incompleta: ${
        translations.size
      }/${
        blocks.length
      }.`
    );
  }

  return translations;
}

// ============================================================
// PT-BR QA SCANNER — EN×PT / TODAS AS FONTES
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
      turns: sourceDialogueDashCount(block)
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
    } | concorrência=${
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
                `BÍBLIA EDITORIAL DO EPISÓDIO:\n${
                  JSON.stringify(
                    plan || {}
                  )
                }\n\n` +
                `SEQUÊNCIA CRONOLÓGICA FONTE×PT PARA AUDITORIA (vizinhos da própria lista são o contexto):\n${
                  JSON.stringify(
                    batch
                  )
                }\n\n` +
                `Retorne SOMENTE IDs que realmente merecem repair por erro semântico, gênero/referente, ownership, calque, literalidade ou naturalidade insuficiente. A lista já está em ordem cronológica; use os vizinhos como contexto sem mover conteúdo entre IDs. Não reescreva os cues.`,

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

          parsedIssues =
            parseQaIssues(
              response.text,
              allowed
            );

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
          } ignorado após falha: ${
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
// DETECTOR / REPAIR
// ============================================================

function words(text) {
  return (
    String(text || "")
      .toLowerCase()
      .match(
        /[\p{L}\p{N}]+(?:['’][\p{L}\p{N}]+)?/gu
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
  filename,
  en
) {
  return (
    /rupaul|drag[ ._-]*race|dragula|queen of the universe/i.test(
      String(
        filename ||
        ""
      )
    ) ||

    /\bwerkroom\b|\blip sync\b|\bshantay\b|\bsashay\b|\bcondragulations\b|\bsnatch game\b|\brusical\b/i.test(
      String(en || "")
    )
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
  return /\bpassad[ao]s?\b|\bt[oô]\s+passad[ao]\b|\bem\s+choque\b|\bsem\s+rea[cç][aã]o\b|\bboquiabert[ao]s?\b|\bchocad[ao]s?\b/i.test(
    String(pt || "")
  );
}

function hasExtendedVocalization(
  text
) {
  const value =
    String(text || "");

  return (
    /(\p{L}{2,})(?:-[aeiouáéíóúàâêôãõü]){2,}/giu.test(
      value
    ) ||

    /(\p{L}{2,})(?:-[\p{L}]{1,3}){3,}/gu.test(
      value
    )
  );
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
    /\bcol[eé]gio\b/i.test(
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
    /\b(?:d[êe]|dar)\b.{0,20}\b(?:intervalo|pausa)\b/i.test(
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
    /\bpeda[cç]o\s+de\s+bolo\b/i.test(
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
  /\b(?:eu\s+)?(?:sou|estou|t[oô]|fiquei|estava|ando)\s+(?:muito\s+)?(?:assustado|cansado|preocupado|nervoso|sozinho|pronto|louco|chocado|confuso|exausto|orgulhoso|aliviado|animado|decepcionado|desesperado|irritado|furioso|envergonhado|surpreso|separado|inteiro|casado|solteiro|nascido|criado|preparado|acostumado)\b/iu,
  /\bme\s+(?:fez|fazer|deixou|deixar|tornou|tornar|manteve|manter)\s+(?:muito\s+)?(?:assustado|cansado|preocupado|nervoso|sozinho|pronto|louco|chocado|confuso|exausto|orgulhoso|aliviado|animado|decepcionado|desesperado|irritado|furioso|envergonhado|surpreso|separado|inteiro|casado|solteiro|preparado|acostumado)\b/iu
];

const FIRST_PERSON_FEMALE_MARKERS = [
  /\bobrigada\b/iu,
  /\b(?:eu\s+)?(?:sou|estou|t[oô]|fiquei|estava|ando)\s+(?:muito\s+)?(?:assustada|cansada|preocupada|nervosa|sozinha|pronta|louca|chocada|confusa|exausta|orgulhosa|aliviada|animada|decepcionada|desesperada|irritada|furiosa|envergonhada|surpresa|separada|inteira|casada|solteira|nascida|criada|preparada|acostumada|gr[aá]vida)\b/iu,
  /\bme\s+(?:fez|fazer|deixou|deixar|tornou|tornar|manteve|manter)\s+(?:muito\s+)?(?:assustada|cansada|preocupada|nervosa|sozinha|pronta|louca|chocada|confusa|exausta|orgulhosa|aliviada|animada|decepcionada|desesperada|irritada|furiosa|envergonhada|surpresa|separada|inteira|casada|solteira|preparada|acostumada|gr[aá]vida)\b/iu
];

function sourceExplicitlyMarksSelfGender(block) {
  const source = String(block?.text || "")
    .toLocaleLowerCase()
    .replace(/\s+/g, " ")
    .trim();

  if (!source) return false;

  return /\b(?:i\s+am|i'm|i’m|i\s+was|i've\s+been|i’ve\s+been|as)\s+(?:(?:a|an)\s+)?(?:woman|man|girl|boy|mother|father|mom|mum|dad|wife|husband|daughter|son|sister|brother|bride|groom|female|male)\b/i.test(source) ||
    /\b(?:sou|era|como)\s+(?:uma?\s+)?(?:mulher|homem|garota|garoto|menina|menino|mãe|pai|esposa|marido|filha|filho|irmã|irmão|noiva|noivo)\b/iu.test(source) ||
    /\b(?:soy|era|como)\s+(?:una?\s+)?(?:mujer|hombre|chica|chico|madre|padre|esposa|esposo|hija|hijo|hermana|hermano|novia|novio)\b/iu.test(source);
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

  if (trusted === "female" && male) {
    reasons.push("GENDER_V2_TRUSTED_FEMALE_MASCULINE_SELF_MARKER");
  }

  if (trusted === "male" && female) {
    reasons.push("GENDER_V2_TRUSTED_MALE_FEMININE_SELF_MARKER");
  }

  if (
    (male || female) &&
    !sourceExplicitlyMarksSelfGender(block)
  ) {
    reasons.push("GENDER_V3_NEUTRAL_DEFAULT_VIOLATION");
  }

  return [...new Set(reasons)];
}

const UNKNOWN_SPEAKER_GENDERED_STATE_RE =
  /\b(?:estou|t[oô]|fiquei|estava|sou|me\s+sinto)\s+(?:muito\s+)?(?:assustad[oa]s?|cansad[oa]s?|preocupad[oa]s?|nervos[oa]s?|sozinh[oa]s?|pront[oa]s?|lou[cq][oa]s?|chocad[oa]s?|confus[oa]s?|exaust[oa]s?|orgulhos[oa]s?|aliviad[oa]s?|animad[oa]s?|decepcionad[oa]s?|desesperad[oa]s?|irritad[oa]s?|furios[oa]s?|envergonhad[oa]s?|surpres[oa]s?)\b/i;

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
    /(^|\n)\s*(?:\/{1,3}|[-–—]{2,}|\|{1,3}|[•·▪◦]+|[:;])\s*(?:$|\n)/u.test(
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

  if (
    translated.includes(
      BLEEP_TOKEN
    )
  ) {
    reasons.push(
      "UNRESOLVED_BLEEP_TOKEN"
    );
  }

  if (
    translated
      .split("\n")
      .some(line => {
        const info =
          extractSpeaker(
            line
          );

        return Boolean(
          info.speaker
        );
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
      .some(
        line =>
          looksLikeBareSdhLine(
            line
          )
      )
  ) {
    reasons.push(
      "SDH_RESIDUE"
    );
  }
  
  if (
    /\b(?:nabeira|olurando|dem[oô]nico|podrindo|qualé|ossas)\b/i.test(
      translated
    ) ||
    /\btomar\s+consist[eê]ncia\b/i.test(
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
    /\b(?:dar|desse|demos|deu|um)\s+(?:um\s+)?al[oô]\b/i.test(
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
    /\bforça\s+maldosa\b/i.test(
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
    /\b(?:qualé|pistola)\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "POSSIBLE_FORCED_OR_DATED_SLANG"
    );
  }

  if (
    /\bb[eê]bad[oa]\s+que\s+s[oó]\s+a\s+porra\b/i.test(
      translated
    )
  ) {
    reasons.push(
      "FORCED_PROFANITY_REGISTER"
    );
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

  if (
    en.includes(
      BLEEP_TOKEN
    ) &&
    /\b(?:bem|muito|super)\s*[.!?,;:]?\s*$/i.test(
      translated.trim()
    )
  ) {
    reasons.push(
      "BLEEP_CREATED_DANGLING_SENTENCE"
    );
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
      /\bamordaç|\bengasg|\bânsia|\bnáusea/i.test(
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
      /\bch[aá]\b/i.test(
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
      /\bju[ií]zes?\b/i.test(
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
  // PRIORIDADE 0 — CRÍTICO
  // Sentido, identidade, gênero, speaker/referente, omissão,
  // cue ownership e censura quebrada.
  // ==========================================================

  if (
    /FINAL_CRITICAL/i.test(joined) ||
    /GENDER_V[23]_/i.test(joined) ||
    /FINAL_GARBAGE_OR_PLACEHOLDER/i.test(joined) ||
    /CUE_OWNERSHIP_SHIFT/i.test(joined) ||
    /UNKNOWN_SPEAKER_GENDER_MARKED/i.test(joined) ||
    /POSSIBLE_OMISSION/i.test(joined) ||
    /POSSIBLE_CUE_SHIFT_PAIR/i.test(joined) ||
    /POSSIBLE_FORCED_OR_DATED_SLANG/i.test(joined) ||
    /EMPTY/i.test(joined) ||
    /UNRESOLVED_BLEEP_TOKEN/i.test(joined) ||
    /BLEEP_CREATED_DANGLING_SENTENCE/i.test(joined) ||
    /ARTIFICIAL_PROFANITY_CENSORSHIP/i.test(joined) ||
    /(?:MISSING_DIALOGUE_BREAK|DIALOGUE_TURN_MISMATCH)/i.test(joined) ||

    // Motivos escritos pelo Gemini QA.
    /\bg[eê]nero\b/i.test(joined) ||
    /\bpronome\b/i.test(joined) ||
    /\bidentity\b/i.test(joined) ||
    /\bidentidade\b/i.test(joined) ||
    /\bspeaker\b/i.test(joined) ||
    /\breferente\b/i.test(joined) ||
    /\bsentido\b/i.test(joined) ||
    /\bsem[aâ]ntic/i.test(joined) ||
    /\bnega[cç][aã]o\b/i.test(joined) ||
    /\bomiss[aã]o\b/i.test(joined) ||
    /\binven(?:ta|tou|ção)\b/i.test(joined) ||
    /\bsujeito\b/i.test(joined) ||
    /\bobjeto\b/i.test(joined)
  ) {
    return 0;
  }

  // ==========================================================
  // PRIORIDADE 1 — QUALIDADE LINGUÍSTICA
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
    /g[ií]ria/i.test(joined) ||
    /met[aá]fora/i.test(joined)
  ) {
    return 1;
  }

  // PRIORIDADE 2 — MECÂNICO
  // Ex.: linha comprida, ruído leve, formatação etc.
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
                  "Preserve exatamente a quantidade e a ordem dos speakers; devolva cada turn em linha própria começando por '- '."
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
            `BÍBLIA:\n${
              JSON.stringify(
                plan
              )
            }\n\n` +
            `CUES PARA REPARO:\n${
              JSON.stringify(
                payload
              )
            }\n\n` +
            `CUE OWNERSHIP ABSOLUTO: cada i é uma caixa fechada. Traduza SOMENTE o campo en daquele mesmo i. ` +
            `before/after servem SOMENTE para contexto e NUNCA podem fornecer conteúdo ao target. ` +
            `Se o pt atual estiver deslocado, reconstrua diretamente do en do mesmo i. ` +
            `Todos os tokens __LOCK_C...__ devem voltar idênticos. ` +
            `Se dialogue_turn_count existir, preserve EXATAMENTE os turns e devolva cada um em linha própria iniciada por "- ". ` +
            `O token ${BLEEP_TOKEN} deve ser resolvido naturalmente e nunca copiado.`,

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
    `[PRE-REPAIR CONFIRM] heurísticos=${candidates.length} | ` +
    `lotes=${batches.length} | rounds=${PRE_REPAIR_CONFIRM_ROUNDS} | ` +
    `concorrência=${Math.min(PRE_REPAIR_CONFIRM_CONCURRENCY, Math.max(1, batches.length))}.`
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
              `BÍBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\n` +
              `RODADA INDEPENDENTE ${round}/${PRE_REPAIR_CONFIRM_ROUNDS}\n` +
              `CUES HEURÍSTICOS:\n${JSON.stringify({ cues: batch })}\n\n` +
              `Retorne em issues SOMENTE IDs cujo defeito heurístico está semanticamente CONFIRMADO. ` +
              `Cue correto deve ser omitido de issues. Não reescreva texto.`,
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
            `round=${round} falhou; FAIL-SAFE mantém Repair | ` +
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
    if (!job.finalCriticalConsensusState || typeof job.finalCriticalConsensusState !== "object") {
      job.finalCriticalConsensusState = Object.create(null);
    }

    for (const reason of Array.isArray(issue?.reasons) ? issue.reasons : []) {
      if (!preRepairAmbiguousHeuristicReason(reason)) continue;
      const key = `${id}::${String(reason)}`;
      job.finalCriticalConsensusState[key] = Math.max(
        Number(job.finalCriticalConsensusState[key] || 0),
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
    `fail-safe-técnico=${technicalFallbackIds.size}.`
  );

  if (suppressedIds.size) {
    console.log(
      `[PRE-REPAIR CONFIRM] ${suppressedIds.size} cue(s) preservados SEM rewrite; ` +
      `Final Critical global continua com autoridade para contradizer esta decisão.`
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

  for (const reason of afterReasons) {
    const isCriticalRegression =
      /^(?:EMPTY|POSSIBLE_OMISSION|ARTIFICIAL_PROFANITY_CENSORSHIP|UNRESOLVED_BLEEP_TOKEN|BLEEP_CREATED_DANGLING_SENTENCE|MISSING_DIALOGUE_BREAK|DIALOGUE_TURN_MISMATCH|SDH_RESIDUE|KNOWN_PTBR_CORRUPTION_OR_UNNATURALNESS|UNKNOWN_SPEAKER_GENDER_MARK|GENDER_V[23]_|FINAL_GARBAGE_OR_PLACEHOLDER|CUE_OWNERSHIP_SHIFT)/i.test(
        reason
      );

    if (
      isCriticalRegression &&
      !beforeReasons.has(reason)
    ) {
      regressions.push(reason);
    }
  }

  // Se o Repair inventar [descrição] onde não havia,
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

  // Também não permitimos criar uma linha inteira
  // embrulhada em asteriscos como descrição.
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
// mesmo que o Repair não os altere ou algum lote falhe.
const ownershipAuditIds =
  issues
    .filter(
      issue =>
        Array.isArray(
          issue?.reasons
        ) &&
        issue.reasons.some(
          reason =>
            /POSSIBLE_CUE_SHIFT_PAIR/i.test(
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
    `serão obrigatoriamente auditados após o Repair.`
  );
}

if (!extraOnly) job.stats.localFlags = localOnlyCount;

  if (!issues.length) {
    console.log(extraOnly ? "[FINAL CRITICAL REPAIR] 0 blockers." : "[LOCAL GUARD] 0 suspeitos.");
    return translations;
  }

  logIssueSummary(extraOnly ? "FINAL-CRITICAL-REPAIR" : "PRÉ-REPAIR", issues);

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

  const selectedCritical =
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
    `críticos=${selectedCritical} | ` +
    `qualidade=${selectedQuality} | ` +
    `mecânicos=${selectedMechanical}.`
  );

  job.stats.repairSelected =
    selected.length;

  console.log(
    extraOnly
      ? `[FINAL CRITICAL REPAIR] ${issues.length} blocker(s) desta rodada; reparando somente ${selected.length} cue(s).`
      : `[LOCAL GUARD] ${issues.length} suspeitos combinados (local+QA); reparando até ${selected.length}.`
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
  for (
    let i = 0;
    i < selected.length;
    i += REPAIR_BATCH_MAX_CUES
  ) {
    repairBatches.push(
      selected.slice(i, i + REPAIR_BATCH_MAX_CUES)
    );
  }

  const totalBatches = repairBatches.length;
  let successfulBatches = 0;
  let failedBatches = 0;
  let acceptedCues = 0;
  let repairCursor = 0;

  async function repairWorker(workerId) {
    while (true) {
      const batchIndex = repairCursor++;
      if (batchIndex >= repairBatches.length) return;

      const batch = repairBatches[batchIndex];
      const batchNumber = batchIndex + 1;

      try {
        const repaired = await repairBatch(
          blocks,
          posMap,
          translations,
          batch,
          plan,
          job
        );

        let acceptedThisBatch = 0;

        for (const [id, pt] of repaired) {
          const pos = posMap.get(id);
          const block = blocks[pos];
          if (!block) continue;

          const beforePt = String(
            translations.get(id) ?? ""
          ).trim();
          const candidatePt = String(pt || "").trim();

          const regressions = repairCandidateRegressionReasons(
            block,
            beforePt,
            candidatePt,
            job.filename,
            plan
          );

          if (regressions.length) {
            console.warn(
              `[REPAIR REGRESSION GUARD] cue ${id} rejeitado | ${regressions.join(", ")}.`
            );
            continue;
          }

          updated.set(id, candidatePt);
          acceptedThisBatch++;
          acceptedCues++;
        }

        successfulBatches++;
        console.log(
          `[REPAIR W${workerId}] lote ${batchNumber}/${totalBatches} OK | aceitos=${acceptedThisBatch}.`
        );
      } catch (error) {
        failedBatches++;
        job.stats.repairFailures++;
        console.warn(
          `[REPAIR W${workerId}] lote ${batchNumber}/${totalBatches} falhou; ` +
          `candidato anterior preservado | ${errorMessage(error).slice(0, 350)}`
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

  console.log(
    `[REPAIR] FINAL | ` +
    `lotes OK=${successfulBatches}/${totalBatches} | ` +
    `lotes falhos=${failedBatches} | ` +
    `cues aceitos=${acceptedCues}.`
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
                  "Preserve exatamente a quantidade e a ordem dos speakers; devolva cada turn em linha própria começando por '- '."
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
            `BÍBLIA:\n${JSON.stringify(plan)}\n\n` +
            `COMPACT RESCUE — RODADA ${round}/${COMPACT_RESCUE_MAX_ROUNDS}\n` +
            `CUES:\n${JSON.stringify(payload)}\n\n` +
            `Todos os tokens __LOCK_C...__ devem voltar idênticos. ` +
            `Se dialogue_turn_count existir, preserve EXATAMENTE os turns; compactar não autoriza unir speakers. ` +
            `O objetivo é conteúdo COMPLETO + PT-BR natural + 2x50.`,

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
    const allIssues =
      collectCompactRescueIssues(
        blocks,
        updated
      );

    if (!allIssues.length) {
      console.log(
        `[COMPACT RESCUE] rodada ${round}: ` +
        `nenhum overflow restante ✅`
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

            console.warn(
              `[COMPACT RESCUE] cue ${id} rejeitado: vazio após sanitizer.`
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

            console.warn(
              `[COMPACT RESCUE] cue ${id} ainda não cabe em 2x${LAYOUT_MAX_CHARS_PER_LINE} | ` +
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
          `[COMPACT RESCUE] lote falhou sem matar episódio | ` +
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

    // Apenas mudança de quebra de linha NÃO conta como rewrite.
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
    `[SEMANTIC COMPACT RETRY] cue ${block.index} | tentando preservar correção dentro de 2x50.`
  );

  const response =
    await geminiRequest({
      system:
        SEMANTIC_COMPACT_RETRY_PROMPT,

      user:
        `BÍBLIA:\n${
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
      `Semantic Compact Retry ainda não cabe em ` +
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
      `Semantic Compact Retry criou regressão: ` +
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

  // Se o cue atual já termina claramente uma frase,
  // não tratamos como continuação.
  if (
    /[.!?]["'’”)\]]*$/u.test(
      current
    )
  ) {
    return false;
  }

  // Primeiro caractere alfabético do próximo cue.
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
    `${batches.length} lote(s) EN×BEFORE×AFTER | concorrência=${Math.min(SEMANTIC_REWRITE_AUDIT_CONCURRENCY, batches.length)}.`
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
            `BÍBLIA:\n${JSON.stringify(plan)}\n\n` +
            `AUDITORIA PÓS-REESCRITA ${batchIndex + 1}/${batches.length}\n` +
            `CUES:\n${JSON.stringify(payload)}\n\n` +
            `Marque regressões semânticas reais E qualquer violação absoluta de cue ownership EN→AFTER_PT. ` +
            `Reparo conservador de fonte claramente truncada é permitido. ` +
            `Paráfrase natural e fiel NÃO é erro.`,

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
            `[SEMANTIC REWRITE GUARD] cue ${id} sinalizado sem correção utilizável.`
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
            `[SEMANTIC REWRITE GUARD] cue ${id} ficou vazio após sanitizer.`
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

        // A correção semântica jamais pode relaxar o teto visual.
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
        `[SEMANTIC COMPACT RETRY] cue ${id} corrigido ✅ | ` +
        `correção semântica preservada dentro de ` +
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
    `não cabe em ${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE} | ` +
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
    `[SEMANTIC OWNERSHIP GUARD] cue ${id} correção rejeitada | ` +
    `fonte continua no cue ${nextCue?.index ?? "seguinte"} e ` +
    `a correção adicionaria conteúdo além do limite do target.`
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
            `[SEMANTIC REWRITE GUARD] cue ${id} correção rejeitada pelo guard local | ` +
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
          `[SEMANTIC REWRITE GUARD] cue ${id} corrigido ✅ | ` +
          `${String(
            issue?.reason ||
            "regressão semântica"
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
        `mantendo resultado anterior sem matar episódio | ` +
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
    `correções aceitas=${totalAccepted} | ` +
    `correções rejeitadas=${totalRejected}.`
  );

  return updated;
}

// ============================================================
// FINAL CRITICAL GATE 8.4.0
// ============================================================
// IMPORTANTE: SOURCE pode ser inglês, espanhol, francês, italiano,
// alemão, neerlandês ou qualquer outro idioma que a Ponte tenha
// escolhido. Inglês é preferência de seleção, NÃO pré-condição.
const FINAL_CRITICAL_AUDIT_PROMPT = `
Você é o FINAL CRITICAL AUDITOR de legendas SOURCE→PT-BR.

SOURCE é a legenda-fonte REAL escolhida pelo orquestrador e pode estar
em QUALQUER idioma. O rótulo EN usado em partes antigas do sistema é
apenas legado interno. NUNCA presuma inglês.

Sua função é encontrar SOMENTE defeitos CRÍTICOS. Não faça revisão
cosmética e não marque uma alternativa apenas porque você escreveria diferente.

Para cada cue, compare primeiro SOURCE[i] com PT[i]. Os cues aparecem em
ordem cronológica e os vizinhos servem apenas para contexto.

1) CUE OWNERSHIP — CRÍTICO
- PT[i] precisa traduzir SOURCE[i], não SOURCE[i-1] nem SOURCE[i+1].
- Se PT[i] traduz claramente o vizinho, marque category=CUE_OWNERSHIP_SHIFT.
- Se houver uma cadeia deslocada +1/-1, marque TODOS os IDs afetados.
- Continuação legítima de frase entre cues não é shift: cada ID preserva
  somente a parte pertencente ao SOURCE daquele mesmo ID.

2) GENDER / REFERENT — CRÍTICO
- Respeite identidade_lock e evidência real da cena.
- Speaker é quem fala; mentions são pessoas citadas.
- Não transfira gênero entre speaker e pessoa mencionada.
- Speaker desconhecido NÃO autoriza adivinhar masculino/feminino.
- Se PT marcar gênero sem evidência segura quando seria possível neutralizar,
  marque category=GENDER_OR_REFERENT.
- Contradição dentro do mesmo speaker no mesmo cue é sempre crítica,
  por exemplo masculino em uma palavra e feminino em outra sem mudança de referente.

3) MEANING INTEGRITY — CRÍTICO
Marque somente perda/troca/invenção real de informação importante:
negação, ação, sujeito, objeto, quantidade, relação, causa, condição,
contraste, intensidade, insulto, referente ou informação narrativa.
category=MEANING_INTEGRITY.

4) GARBAGE / EMPTY — CRÍTICO
- PT vazio para SOURCE verbal não vazio;
- lixo isolado como Ff, ff, J..., J-, J'j', reticências/pontuação sem fala;
- placeholder ou resíduo evidente de OCR que não comunica fala.
category=GARBAGE_OR_EMPTY.

5) DIALOGUE / SDH — CRÍTICO
- speaker/turn perdido ou unido quando SOURCE tem múltiplos turns;
- descrição SDH inventada no lugar da fala.
category=DIALOGUE_OR_SDH.

NÃO marque:
- mera preferência estilística;
- uma tradução natural diferente mas fiel;
- diferença de ordem sintática legítima em português;
- ausência de correspondência palavra por palavra.

A saída contém somente issues reais. reason deve explicar brevemente a prova.
`;

const FINAL_CRITICAL_REPAIR_PROMPT = `
Você é o RESCUE CRÍTICO FINAL de legendas SOURCE→PT-BR.

SOURCE pode estar em QUALQUER idioma. O idioma real do campo source é a
autoridade. Você NÃO está editando por estilo: está reconstruindo do zero
somente cues que falharam num gate crítico.

Para cada item:
- traduza EXCLUSIVAMENTE source daquele mesmo i;
- current_pt é apenas evidência do erro atual, NÃO é autoridade;
- before/after são contexto e JAMAIS podem fornecer conteúdo para o target;
- reasons dizem exatamente por que o cue foi reprovado;
- identity_lock é obrigatório;
- se speaker for desconhecido, neutralize gênero de 1ª pessoa naturalmente;
- nunca produza lixo/placeholder/reticências para preencher vazio;
- preserve exatamente os hard_locks __LOCK_C...__;
- preserve exatamente turns de diálogo quando dialogue_turn_count >= 2;
- não altere timestamp, não crie cue e não mova conteúdo.

CUE OWNERSHIP:
Se o current_pt estiver deslocado para o cue anterior/seguinte, IGNORE-O e
reconstrua a tradução diretamente do source do mesmo ID.

GÊNERO:
Se não houver prova segura, prefira formulações naturais sem marca de gênero.
Nunca misture masculino e feminino para o mesmo speaker/referente.

LAYOUT:
Se possível, escreva de modo naturalmente conciso para caber em no máximo
${LAYOUT_MAX_LINES} linhas de ${LAYOUT_MAX_CHARS_PER_LINE} caracteres sem
remover informação.

Devolva exatamente um objeto por cue recebido.
`;

const FINAL_CRITICAL_AUDIT_SCHEMA = {
  // 8.4.2: deliberadamente simples.
  // O Interactions structured-output pode rejeitar schemas cujo limite
  // de array expanda demais a gramática/constraint. O limite real aqui
  // já é imposto pelo tamanho do lote e pelo parser local.
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

const FINAL_CRITICAL_AUDIT_FALLBACK_SCHEMA = {
  // Fallback ainda menor para um eventual HTTP 400 de validação.
  // category pode ser omitido; parseFinalCriticalAudit usa "CRITICAL".
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

function finalCriticalIssueSignature(issues) {
  return (Array.isArray(issues) ? issues : [])
    .map(issue => `${Number(issue.id)}:${(issue.reasons || []).join("|")}`)
    .sort()
    .join(";;");
}

// JavaScript não suporta flag /x. Mantemos a expressão acima legível
// através desta implementação real equivalente.
function finalReasonBlocks(reason) {
  return /FINAL_CRITICAL|GENDER_V[23]_|UNKNOWN_SPEAKER_GENDER_MARKED|FINAL_GARBAGE_OR_PLACEHOLDER|^EMPTY$|POSSIBLE_OMISSION|POSSIBLE_CUE_SHIFT_PAIR|CUE_OWNERSHIP_SHIFT|UNRESOLVED_BLEEP_TOKEN|BLEEP_CREATED_DANGLING_SENTENCE|ARTIFICIAL_PROFANITY_CENSORSHIP|DIALOGUE_TURN_MISMATCH|MISSING_DIALOGUE_BREAK|SDH_RESIDUE|SPEAKER_LABEL_RESIDUE|SUBTITLE_TOO_DENSE/i.test(
    String(reason || "")
  );
}


function finalCriticalHeuristicReason(reason) {
  return /^(?:GENDER_V2_UNKNOWN_SPEAKER_MARKED|UNKNOWN_SPEAKER_GENDER_MARKED|POSSIBLE_OMISSION|POSSIBLE_CUE_SHIFT_PAIR)$/i.test(String(reason || "").trim());
}

function applyFinalCriticalConsensus(localIssues, semanticIssues, auditedIds, job) {
  const semanticIds = new Set((Array.isArray(semanticIssues) ? semanticIssues : []).map(x => Number(x && x.id)).filter(Number.isInteger));
  if (!job.finalCriticalConsensusState || typeof job.finalCriticalConsensusState !== "object") job.finalCriticalConsensusState = Object.create(null);
  const state = job.finalCriticalConsensusState;
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
      if (!finalCriticalHeuristicReason(reason)) { kept.push(reason); continue; }
      const key = id + "::" + reason;
      active.add(key);
      const audited = !auditedIds || auditedIds.has(id);
      if (!audited) { kept.push(reason); waiting++; continue; }
      if (semanticIds.has(id)) { state[key] = 0; kept.push(reason); waiting++; continue; }
      const clean = Number(state[key] || 0) + 1;
      state[key] = clean;
      if (clean >= FINAL_CRITICAL_HEURISTIC_CONSENSUS_CLEAN_AUDITS) {
        suppressed++;
        job.stats.finalCriticalConsensusSuppressions = (job.stats.finalCriticalConsensusSuppressions || 0) + 1;
      } else {
        kept.push(reason);
        waiting++;
      }
    }
    if (kept.length) out.push({ id, reasons: [...new Set(kept)] });
  }

  for (const key of Object.keys(state)) if (!active.has(key)) delete state[key];
  if (suppressed) console.log(`[FINAL CRITICAL CONSENSUS] ${suppressed} heuristic flag(s) had 2 clean semantic audits and will not block alone. OK`);
  if (waiting) console.log(`[FINAL CRITICAL CONSENSUS] ${waiting} heuristic flag(s) still awaiting semantic consensus.`);
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

function finalCriticalAuditItem(block, translations, plan) {
  return {
    i: block.index,
    source: String(block.text || ""),
    pt: String(translations.get(block.index) || ""),
    identity_lock: compactIdentityHint(block, plan),
    dialogue_turn_count: sourceDialogueDashCount(block)
  };
}

function buildFinalCriticalAuditBatches(
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
      ? finalCriticalAuditItem(blocks[firstPos - 1], translations, plan)
      : null;

    const after = Number.isInteger(lastPos) && lastPos + 1 < blocks.length
      ? finalCriticalAuditItem(blocks[lastPos + 1], translations, plan)
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
    const item = finalCriticalAuditItem(block, translations, plan);
    const size = JSON.stringify(item).length;

    if (
      current.length &&
      (
        current.length >= FINAL_CRITICAL_AUDIT_BATCH_MAX_CUES ||
        chars + size > FINAL_CRITICAL_AUDIT_BATCH_MAX_CHARS
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

async function finalCriticalGeminiRequest(args, job, label) {
  let failures = 0;
  let schemaFallbackUsed = false;
  let activeArgs = { ...args };

  while (true) {
    try {
      return await geminiRequest(activeArgs);
    } catch (error) {
      failures++;
      job.stats.finalCriticalTechnicalRetries =
        (job.stats.finalCriticalTechnicalRetries || 0) + 1;

      const message = errorMessage(error);
      const status = Number(error?.status || 0);

      // 8.4.2: HTTP 400/INVALID_ARGUMENT não é tratado cegamente como
      // "falha transitória" do mesmo payload. Primeiro mudamos de forma
      // determinística para um schema ainda menor e reduzimos a pressão
      // de geração. O job continua vivo e a qualidade continua fail-closed.
      if (
        !schemaFallbackUsed &&
        status === 400 &&
        /invalid argument|invalid_request/i.test(message)
      ) {
        schemaFallbackUsed = true;

        job.stats.finalCriticalSchemaFallbacks =
          (job.stats.finalCriticalSchemaFallbacks || 0) + 1;

        activeArgs = {
          ...activeArgs,
          schema: FINAL_CRITICAL_AUDIT_FALLBACK_SCHEMA,
          thinkingLevel: "medium",
          maxOutputTokens: Math.min(
            Number(activeArgs.maxOutputTokens || 7000),
            7000
          ),
          user:
            `${activeArgs.user}\n\n` +
            `FALLBACK DE SCHEMA: retorne somente {"issues":[{"i":123,"reason":"..."}]}. ` +
            `Não inclua category nesta tentativa.`
        };

        job.error =
          `SCHEMA FALLBACK ${label}: ${message.slice(0, 260)}`;
        job.status = "processing";
        job.progress = Math.min(99, Math.max(94, job.progress || 0));
        job.updatedAt = Date.now();

        console.warn(
          `[FINAL CRITICAL SCHEMA FALLBACK] ${label}: HTTP 400/INVALID_ARGUMENT; ` +
          `trocando para schema mínimo sem liberar a legenda.`
        );

        await sleep(1500);
        continue;
      }

      if (
        failures >=
        FINAL_CRITICAL_REQUEST_MAX_FAILURES
      ) {
        throw new Error(
          `FINAL CRITICAL ${label}: ${failures} falhas técnicas consecutivas; ` +
          `encerrando esta etapa para o fallback seguro do job | ${
            message.slice(0, 320)
          }`
        );
      }

      const waitMs = Math.min(
        FINAL_CRITICAL_RETRY_MAX_MS,
        FINAL_CRITICAL_RETRY_BASE_MS * Math.pow(1.65, Math.min(failures - 1, 8))
      );

      job.error =
        `RETRY ${label}: ${message.slice(0, 260)}`;
      job.status = "processing";
      job.progress = Math.min(99, Math.max(94, job.progress || 0));
      job.updatedAt = Date.now();

      console.warn(
        `[FINAL CRITICAL RETRY] ${label} falhou (${failures}); ` +
        `nova tentativa limitada em ${(waitMs / 1000).toFixed(1)}s | ` +
        `${message.slice(0, 320)}`
      );

      await sleep(waitMs);
    }
  }
}

function parseFinalCriticalAudit(text, allowedIds) {
  const parsed = JSON.parse(stripCodeFences(text));
  const raw = Array.isArray(parsed?.issues) ? parsed.issues : [];
  const out = [];
  const seen = new Set();

  for (const issue of raw) {
    const id = Number(issue?.i);
    if (!Number.isInteger(id) || !allowedIds.has(id) || seen.has(id)) continue;
    seen.add(id);

    const category = String(issue?.category || "CRITICAL").trim().slice(0, 80);
    const reason = String(issue?.reason || "defeito crítico").trim().slice(0, 260);

    out.push({
      id,
      reasons: [`FINAL_CRITICAL:${category}: ${reason}`]
    });

    if (out.length >= FINAL_CRITICAL_MAX_ISSUES) {
      break;
    }
  }

  return out;
}

async function scanFinalCriticalAudit(
  blocks,
  translations,
  plan,
  job,
  focusIds = null
) {
  if (!FINAL_CRITICAL_GATE_ENABLED) return [];

  const batches = buildFinalCriticalAuditBatches(
    blocks,
    translations,
    plan,
    focusIds
  );

  if (!batches.length) return [];

  const results = new Array(batches.length);
  let cursor = 0;

  console.log(
    `[FINAL CRITICAL AUDIT] ${batches.length} lote(s) | ` +
    `fonte=${job.sourceLang || "auto"} | ` +
    `escopo=${focusIds instanceof Set ? `${focusIds.size} cue(s) focais` : "episódio completo"}.`
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
          FINAL_CRITICAL_PARSE_MAX_FAILURES
      ) {
        const response = await finalCriticalGeminiRequest(
          {
            system: FINAL_CRITICAL_AUDIT_PROMPT,
            user:
              `IDIOMA DECLARADO DA FONTE: ${job.sourceLang || "auto"}\n` +
              `IMPORTANTE: use o idioma REAL encontrado em SOURCE; não presuma inglês.\n\n` +
              `BÍBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\n` +
              `CUES EM ORDEM CRONOLÓGICA:\n${JSON.stringify(batch.cues)}\n\n` +
              `Audite SOMENTE context_only=false. context_only=true existe apenas para comparar vizinhos.`,
            schema: FINAL_CRITICAL_AUDIT_SCHEMA,
            thinkingLevel: FINAL_CRITICAL_AUDIT_THINKING,
            maxOutputTokens: FINAL_CRITICAL_AUDIT_MAX_OUTPUT_TOKENS,
            timeoutMs: FINAL_CRITICAL_AUDIT_TIMEOUT_MS,
            maxRetries: FINAL_CRITICAL_AUDIT_HTTP_RETRIES,
            job,
            metric: "qa"
          },
          job,
          `AUDIT W${workerId} lote ${index + 1}`
        );

        job.stats.finalCriticalAuditCalls =
          (job.stats.finalCriticalAuditCalls || 0) + 1;

        try {
          parsed = parseFinalCriticalAudit(
            response.text,
            batch.targetIds
          );
        } catch (error) {
          parseFailures++;
          job.stats.finalCriticalTechnicalRetries =
            (job.stats.finalCriticalTechnicalRetries || 0) + 1;

          console.warn(
            `[FINAL CRITICAL AUDIT] JSON inválido no lote ${index + 1} ` +
            `(tentativa ${parseFailures}); repetindo sem matar job | ` +
            `${errorMessage(error).slice(0, 220)}`
          );

          await sleep(
            Math.min(
              FINAL_CRITICAL_RETRY_MAX_MS,
              FINAL_CRITICAL_RETRY_BASE_MS * Math.max(1, parseFailures)
            )
          );
        }
      }

      if (!parsed) {
        throw new Error(
          `FINAL CRITICAL AUDIT lote ${index + 1}: ` +
          `${FINAL_CRITICAL_PARSE_MAX_FAILURES} respostas inválidas consecutivas.`
        );
      }

      results[index] = parsed;

      console.log(
        `[FINAL CRITICAL AUDIT W${workerId}] lote ${index + 1}/${batches.length}: ` +
        `${parsed.length} crítico(s).`
      );
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(FINAL_CRITICAL_AUDIT_CONCURRENCY, batches.length) },
      (_, index) => worker(index + 1)
    )
  );

  const merged = mergeIssueLists(...results.map(x => Array.isArray(x) ? x : []));
  job.stats.finalCriticalFlags = merged.length;
  return merged;
}

function idsFromIssues(issues, blocks, radius = FINAL_CRITICAL_CONTEXT_RADIUS) {
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

async function runFinalCriticalEscalatedRepair(
  blocks,
  translations,
  issues,
  plan,
  job
) {
  const posMap = positionMap(blocks);
  const updated = new Map(translations);
  const selected = [...issues].sort((a, b) => issuePriority(a) - issuePriority(b));

  for (
    let offset = 0;
    offset < selected.length;
    offset += FINAL_CRITICAL_ESCALATED_BATCH_MAX_CUES
  ) {
    const batchIssues = selected.slice(
      offset,
      offset + FINAL_CRITICAL_ESCALATED_BATCH_MAX_CUES
    );

    let completed = false;
    let parseFailures = 0;

    while (
      !completed &&
      parseFailures <
        FINAL_CRITICAL_ESCALATED_MAX_FAILURES
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
        const response = await finalCriticalGeminiRequest(
          {
            system: FINAL_CRITICAL_REPAIR_PROMPT,
            user:
              `IDIOMA DECLARADO DA FONTE: ${job.sourceLang || "auto"}\n` +
              `Use o idioma REAL de source.\n\n` +
              `BÍBLIA EDITORIAL:\n${JSON.stringify(plan || {})}\n\n` +
              `CUES CRÍTICOS PARA RECONSTRUÇÃO:\n${JSON.stringify({ cues })}`,
            schema: cueTranslationSchema(batchIssues.length),
            thinkingLevel: "high",
            maxOutputTokens: FINAL_CRITICAL_ESCALATED_MAX_OUTPUT_TOKENS,
            timeoutMs: FINAL_CRITICAL_ESCALATED_TIMEOUT_MS,
            maxRetries: REPAIR_HTTP_RETRIES,
            job,
            metric: "repair"
          },
          job,
          `ESCALATED REPAIR ${Math.floor(offset / FINAL_CRITICAL_ESCALATED_BATCH_MAX_CUES) + 1}`
        );

        const repaired = parseCueTranslation(
          batchIssues.map(issue => blocks[posMap.get(issue.id)]),
          response.text,
          locksById
        );

        let accepted = 0;

        for (const [id, candidate] of repaired) {
          const block = blocks[posMap.get(id)];
          const beforePt = String(updated.get(id) || "");
          const candidatePt = String(candidate || "").trim();

          const regressions = repairCandidateRegressionReasons(
            block,
            beforePt,
            candidatePt,
            job.filename,
            plan
          );

          if (regressions.length) {
            console.warn(
              `[FINAL CRITICAL ESCALATED] cue ${id} rejeitado localmente | ` +
              `${regressions.join(", ")}.`
            );
            continue;
          }

          updated.set(id, candidatePt);
          accepted++;
        }

        console.log(
          `[FINAL CRITICAL ESCALATED] lote ` +
          `${Math.floor(offset / FINAL_CRITICAL_ESCALATED_BATCH_MAX_CUES) + 1} | ` +
          `aceitos=${accepted}/${batchIssues.length}.`
        );

        completed = true;
      } catch (error) {
        parseFailures++;
        job.stats.finalCriticalTechnicalRetries =
          (job.stats.finalCriticalTechnicalRetries || 0) + 1;

        const waitMs = Math.min(
          FINAL_CRITICAL_RETRY_MAX_MS,
          FINAL_CRITICAL_RETRY_BASE_MS * Math.max(1, parseFailures)
        );

        console.warn(
          `[FINAL CRITICAL ESCALATED] lote não foi descartado; ` +
          `repetindo em ${(waitMs / 1000).toFixed(1)}s | ` +
          `${errorMessage(error).slice(0, 320)}`
        );

        await sleep(waitMs);
      }
    }

    if (!completed) {
      console.warn(
        `[FINAL CRITICAL ESCALATED] lote ` +
        `${Math.floor(offset / FINAL_CRITICAL_ESCALATED_BATCH_MAX_CUES) + 1} ` +
        `atingiu o limite de ${FINAL_CRITICAL_ESCALATED_MAX_FAILURES} falhas; ` +
        `candidato anterior preservado sem loop.`
      );
    }
  }

  return updated;
}

async function convergeFinalCriticalQuality(
  blocks,
  translations,
  plan,
  job,
  initialFocusIds = null
) {
  if (!FINAL_CRITICAL_GATE_ENABLED) return translations;

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

  // A primeira invocação do pipeline continua GLOBAL para preservar
  // exatamente a função do gate 8.4.6. Quando o gate é reaberto por um
  // problema já conhecido (ex.: 1 overflow de layout), começamos focado
  // nesses IDs e NUNCA varremos os ~3.000 cues outra vez.
  let firstAudit =
    initialFocusIds == null;

  let roundsThisRun = 0;

  while (
    roundsThisRun <
    FINAL_CRITICAL_MAX_ROUNDS
  ) {
    roundsThisRun++;

    job.stats.finalCriticalRounds =
      (job.stats.finalCriticalRounds || 0) + 1;

    current = sanitizeTranslationMap(blocks, current, job);
    current = applySubtitleLayout(blocks, current, "FINAL-CRITICAL-CANDIDATE");

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

    const semantic = await scanFinalCriticalAudit(
      blocks,
      current,
      plan,
      job,
      auditFocusIds
    );

    firstAudit = false;

    const consensusLocal = applyFinalCriticalConsensus(local, semantic, auditFocusIds, job);
    let issues = mergeIssueLists(consensusLocal, semantic);

    if (!issues.length) {
      console.log(
        `[FINAL CRITICAL GATE] PASSOU ✅ | ` +
        `rounds=${job.stats.finalCriticalRounds} | ` +
        `0 defeitos críticos; resultado autorizado para cache/serve.`
      );

      job.error = null;
      return current;
    }

    logIssueSummary("FINAL-CRITICAL", issues);

    const signature = finalCriticalIssueSignature(issues);
    if (signature === previousSignature) {
      stagnantRounds++;
    } else {
      stagnantRounds = 0;
      previousSignature = signature;
    }

    if (stagnantRounds >= FINAL_CRITICAL_NO_PROGRESS_ESCALATE_AFTER) {
      job.stats.finalCriticalNoProgressEscalations =
        (job.stats.finalCriticalNoProgressEscalations || 0) + 1;

      console.warn(
        `[FINAL CRITICAL ESCALATION] mesmos defeitos persistiram por ` +
        `${stagnantRounds + 1} rodada(s); repair hiperfocado nos IDs críticos.`
      );
    }

    job.stats.finalCriticalRepairRounds =
      (job.stats.finalCriticalRepairRounds || 0) + 1;

    const before = new Map(current);

    if (
      stagnantRounds >= FINAL_CRITICAL_NO_PROGRESS_ESCALATE_AFTER
    ) {
      current = await runFinalCriticalEscalatedRepair(
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
      `[FINAL CRITICAL GATE] rodada ${job.stats.finalCriticalRounds} ` +
      `reprovou ${issues.length} cue(s); ` +
      `${changedIds.size} cue(s) alterado(s); reauditoria focada continuará.`
    );
  }

  job.stats.finalCriticalBoundedReleases =
    (
      job.stats.finalCriticalBoundedReleases ||
      0
    ) + 1;

  job.qualityStatus =
    "bounded_best_candidate";

  console.warn(
    `[FINAL CRITICAL GATE] limite de ${FINAL_CRITICAL_MAX_ROUNDS} rodada(s) ` +
    `atingido; melhor candidato protegido pelos guards locais será finalizado ` +
    `em vez de manter o job eternamente em processing.`
  );

  return current;
}

async function runBoundedFinalQuality88(
  blocks,
  translations,
  mainTranslations,
  qaIssues,
  plan,
  job
) {
  let current = sanitizeTranslationMap(
    blocks,
    translations,
    job
  );

  const initialFocus = idsFromIssues(qaIssues, blocks);

  for (const block of blocks) {
    const before = String(mainTranslations.get(block.index) || "")
      .replace(/\s+/g, " ").trim();
    const after = String(current.get(block.index) || "")
      .replace(/\s+/g, " ").trim();
    if (before !== after) initialFocus.add(block.index);
  }

  const localBefore = blockingLocalIssues(
    blocks,
    applySubtitleLayout(blocks, current, "FINAL-88-CANDIDATE"),
    job,
    plan
  );
  for (const id of idsFromIssues(localBefore, blocks)) initialFocus.add(id);

  console.log(
    `[FINAL BOUNDED 8.8.1] auditoria HIGH única inicial | foco=${initialFocus.size} cue(s); ` +
    `zero convergência aberta.`
  );

  const semantic1 = await scanFinalCriticalAudit(
    blocks,
    current,
    plan,
    job,
    initialFocus
  );

  let issues1 = mergeIssueLists(localBefore, semantic1);
  let changedAfterFinalRepair = new Set();

  if (issues1.length) {
    logIssueSummary("FINAL-88-REPAIR", issues1);
    const beforeRepair = new Map(current);

    current = await runFinalCriticalEscalatedRepair(
      blocks,
      current,
      issues1,
      plan,
      job
    );

    current = sanitizeTranslationMap(blocks, current, job);
    current = await runCompactRescue(blocks, current, plan, job);
    current = sanitizeTranslationMap(blocks, current, job);

    for (const block of blocks) {
      const a = String(beforeRepair.get(block.index) || "").replace(/\s+/g, " ").trim();
      const b = String(current.get(block.index) || "").replace(/\s+/g, " ").trim();
      if (a !== b) changedAfterFinalRepair.add(block.index);
    }
  }

  const verifyFocus = new Set([
    ...idsFromIssues(issues1, blocks),
    ...changedAfterFinalRepair
  ]);

  if (verifyFocus.size) {
    console.log(
      `[FINAL BOUNDED 8.8.1] verificação HIGH final | foco=${verifyFocus.size} cue(s); ` +
      `esta é a última auditoria Gemini do job.`
    );

    const laidOut = applySubtitleLayout(
      blocks,
      current,
      "FINAL-88-VERIFY"
    );

    const local2 = blockingLocalIssues(
      blocks,
      laidOut,
      job,
      plan
    ).filter(issue => verifyFocus.has(Number(issue.id)));

    const semantic2 = await scanFinalCriticalAudit(
      blocks,
      current,
      plan,
      job,
      verifyFocus
    );

    const issues2 = mergeIssueLists(local2, semantic2);

    if (issues2.length) {
      logIssueSummary("FINAL-88-LAST-REPAIR", issues2);
      console.warn(
        `[FINAL BOUNDED 8.8.1] ${issues2.length} blocker(s) residuais; ` +
        `executando UMA reconstrução final focal. Não haverá nova auditoria em loop.`
      );

      current = await runFinalCriticalEscalatedRepair(
        blocks,
        current,
        issues2,
        plan,
        job
      );
      current = sanitizeTranslationMap(blocks, current, job);
      current = await runCompactRescue(blocks, current, plan, job);
      current = sanitizeTranslationMap(blocks, current, job);
    }
  }

  const finalLocal = blockingLocalIssues(
    blocks,
    applySubtitleLayout(blocks, current, "FINAL-88-LOCAL"),
    job,
    plan
  );

  if (finalLocal.length) {
    console.warn(
      `[FINAL BOUNDED 8.8.1] ${finalLocal.length} guard(s) local(is) residual(is) ` +
      `após o pipeline fechado; sem loop cloud. Melhor candidato íntegro será servido.`
    );
    job.qualityStatus = "bounded_best_candidate";
  } else {
    job.qualityStatus = "final_pass";
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
      "Nenhum cue SRT válido."
    );
  }

  for (const block of blocks) {
    block.sourceLang = String(job.sourceLang || "auto");
  }

  job.stats.sourceCues =
    blocks.length;

  console.log(
    `[PIPELINE 8.8.1 BOUNDED] fonte=${
      job.sourceKind
    } | ${
      blocks.length
    } cues.`
  );

  const plan =
    await buildEpisodePlan(
      blocks,
      job
    );

  job.progress = 5;

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

// 8.4.4: qualquer fala real que tenha virado vazio no sanitizer é
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
// LAYOUT LOCK — SAFE DRAFT
// ============================================================
// O QA e o Repair continuam trabalhando com o texto sem reflow.
// O SAFE DRAFT já é servido com layout profissional.
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

const qaIssues =
  await scanPtbrQuality(
    blocks,
    mainTranslations,
    plan,
    job
  );

// ============================================================
// REPAIR
// ============================================================

// Snapshot semântico ANTES de Repair + Compact Rescue.
// Serve para descobrir se uma reescrita posterior
// perdeu ou inventou informação.
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
// Só os cues que AINDA não cabem em 2x50 entram aqui.
// Não move conteúdo, não cria cue e não toca em timestamp.
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
// FINAL QUALITY — 8.8 BOUNDED
// ============================================================
// O QA global HIGH já cobriu o episódio inteiro. Depois dele existe somente:
// 1) repair focal;
// 2) uma auditoria HIGH focal;
// 3) no máximo uma reconstrução focal + uma verificação HIGH final.
// Não existe convergência aberta, reabertura por layout nem ciclo de rounds.
finalTranslations = await runBoundedFinalQuality88(
  blocks,
  finalTranslations,
  mainTranslations,
  qaIssues,
  plan,
  job
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

  console.log(
    `[PIPELINE 8.8.1 BOUNDED] FINAL OK | ${
      blocks.length
    } source cues | pipeline=${
      pipelineElapsedSeconds.toFixed(1)
    }s | job-total=${
      jobElapsedSeconds.toFixed(1)
    }s | full-job-retries=${
      job.stats.jobRetries || 0
    }.`
  );

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

      // Só chega aqui depois do FINAL CRITICAL GATE PASSAR.
      setCache(
        job.cacheKey,
        finalSrt,
        job
      );

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

      if (error?.noJobRetry || isDeterministicGeminiRequestError(error)) {
        attempt = JOB_MAX_ATTEMPTS;
        console.error(
          `[JOB ${job.id}] erro determinístico; full-job retry PROIBIDO em 8.7 | ` +
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

      // 8.4.0: NÃO cachear safeDraft defeituoso e NÃO marcar failed.
      // Mantemos o job vivo e repetimos. SafeDraft continua apenas como
      // proteção interna/diagnóstico; nunca ganha selo FINAL por erro.
      job.status = "processing";
      job.progress = Math.min(99, Math.max(1, job.progress || 1));
      job.error =
        `RETRYING (${attempt}): ${errorMessage(error).slice(0, 500)}`;
      job.updatedAt = Date.now();

      if (
        attempt <
        JOB_MAX_ATTEMPTS
      ) {
        console.warn(
          `[JOB ${job.id}] falha transitória; ` +
          `retry ${attempt}/${JOB_MAX_ATTEMPTS - 1} em ` +
          `${(waitMs / 1000).toFixed(1)}s | ` +
          `${errorMessage(error).slice(0, 420)}`
        );

        await sleep(waitMs);
      }
    }
  }

  if (job.safeDraft) {
    auditTimestamps(
      job.sourceSrt,
      job.safeDraft,
      "BOUNDED-SAFE-DRAFT",
      job
    );

    job.result =
      job.safeDraft;
    job.status =
      "completed";
    job.progress = 100;
    job.error = null;
    job.qualityStatus =
      "bounded_safe_draft";
    job.stats.boundedSafeDraftReleases =
      (
        job.stats.boundedSafeDraftReleases ||
        0
      ) + 1;
    job.updatedAt = Date.now();

    console.warn(
      `[JOB ${job.id}] ${JOB_MAX_ATTEMPTS} tentativa(s) encerradas; ` +
      `SAFE DRAFT íntegro liberado sem cache e sem loop ✅ | ${
        errorMessage(lastJobError).slice(0, 360)
      }`
    );

    return;
  }

  job.status = "failed";
  job.progress = 100;
  job.error =
    `Falha terminal antes da criação do SAFE DRAFT: ${
      errorMessage(lastJobError).slice(0, 500)
    }`;
  job.qualityStatus =
    "no_safe_draft";
  job.updatedAt = Date.now();

  console.error(
    `[JOB ${job.id}] encerrado sem SAFE DRAFT após ${JOB_MAX_ATTEMPTS} tentativa(s) | ` +
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
      "Legenda OpenSubtitles vazia após limpeza."
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
        "[STREMIO CLOUD] nenhuma legenda inglesa utilizável."
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
      `[CLOUD LAZY] opção criada sem Gemini | job=${
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
    } após restart/expiração de memória.`
  );

  const sourceSrt =
    await fetchOpenSubtitlesSource(
      recovery
    );

  if (!sourceSrt) {
    throw new Error(
      "OpenSubtitles não retornou fonte para autorrecuperação."
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
    "8.4.1",

  name:
    "PT-BR Cloud • OpenSubtitles",

  description:
    "OpenSubtitles inglês → PT-BR com Context + Identity Lock, Character Ledger, Cue Ownership, HARD SDH, QA SOURCE×PT e suporte ao OpenSub Sync V5 por Gemini Transcribe, Character Ledger compatível e QA anti-literalidade.",

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

      mode:
        "CLOUD_OPENSUB_PLUS_LOCAL_TRANSLATION_AND_GEMINI_TRANSCRIBE_MONTAGE_BUDGET",

      mainBatchMaxCues:
        MAIN_BATCH_MAX_CUES,

      mainConcurrency:
        MAIN_CONCURRENCY,

      pacerMs:
        GEMINI_MIN_START_INTERVAL_MS,

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
// LOCAL APIs — PONTE LOCAL
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
            "Campo srt obrigatório."
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
        "Legenda local inválida após HARD SDH CLEAN."
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

// Proxy lexical multilíngue para o Auto-Sync da Ponte.
// Não toca em timestamps nem na tradução PT-BR final.
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

// Alinhamento semântico SOURCE↔ASR para o Total Sync.
// Trabalha só com poucos cues e palavras transcritas; não altera timestamps.
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

// Ponte monta várias janelas em um WAV.
// Render mantém chave Gemini, orçamento e word timestamps.
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
              "mimeType de áudio inválido."
          },

          400
        );
      }

      if (!audioBase64) {
        return safeJson(
          res,

          {
            error:
              "audioBase64 obrigatório."
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
              "Montagem de áudio grande demais."
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
        } | duração≈${
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
            "Job não encontrado."
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
    "Não foi possível concluir a legenda PT-BR.",
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
            `Não foi possível recuperar a legenda: ${
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
          "Job expirado e sem dados de recuperação."
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
        " STREMIO PT-BR 8.8.1 - HARD SDH + NEUTRAL GENDER"
  );

  console.log(
    "============================================================"
  );

  console.log(
    `Gemini: ${
      GEMINI_API_KEY
        ? "CONFIGURADA ✅"
        : "FALTANDO ❌"
    }`
  );

  console.log(
    `Modelo: ${GEMINI_MODEL} ✅`
  );

  console.log(
    "Cloud OpenSubtitles: ATIVO + LAZY + SELF-HEAL ✅"
  );

  console.log(
    "APIs Local Embedded + OpenSub Sync: ATIVAS ✅"
  );

  console.log(
    `Audio Sync ASR: ${GEMINI_TRANSCRIBE_MODEL} | montage COARSE/PRECISION/RESCUE + word timestamps ✅`
  );

  console.log(
    `Transcribe Budget: 22s entre inícios | TPM soft=${TRANSCRIBE_TPM_SOFT_LIMIT}/${TRANSCRIBE_TPM_LIMIT} | RPD interno=${TRANSCRIBE_RPD_INTERNAL_LIMIT}/25 ✅`
  );

  console.log(
    "Context + Identity Lock / Character Ledger SAFE-SCHEMA: ATIVO ✅"
  );

  console.log(
    "Planner fallback ultra-simples + fallback local neutro: ATIVOS ✅"
  );

  console.log(
    "Gender-safe unknown speaker: NÃO ADIVINHA; neutralização contextual ATIVA ✅"
  );

  console.log(
    "Naturalidade PT-BR 2026 + Anti-Calque/Falsos Cognatos: ATIVOS ✅"
  );

  console.log(
  "Naturalness Lock: literal porém artificial = ERRO; intenção + oralidade PT-BR prioritárias ✅"
);

console.log(
  `Subtitle Layout Lock: alvo máximo=${LAYOUT_MAX_LINES} linhas × ${LAYOUT_MAX_CHARS_PER_LINE} chars | quebra somente entre palavras ✅`
);

console.log(
  "Layout Safety: zero truncamento | zero word-split | zero novos cues | zero alteração de timestamps ✅"
);

console.log(
  "Universal SDH Action Classifier: sujeito/personagem genérico + ação/evento; sem hardcode de programa ✅"
);

console.log(
  "Contextual Performance Music Lock: fundo editorial sai; performance real fica; decisão atômica por cue ✅"
);

console.log(
  `Dialogue Turn Lock: speakers/turns preservados + layout turn-aware dentro de ${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE} ✅`
);

console.log(
  "SUBTITLE_TOO_DENSE: Repair busca concisão natural antes do reflow final ✅"
);

  console.log(
  "Meaning Integrity Lock: naturalizar/compactar NÃO pode apagar unidades semânticas ✅"
);

console.log(
  `Compact Rescue: até ${COMPACT_RESCUE_MAX_ROUNDS} rodada(s), somente overflow residual, thinking=${COMPACT_RESCUE_THINKING} ✅`
);

console.log(
  `Final Layout Cap: objetivo estrito=${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE}; zero truncamento / zero word-split ✅`
);

console.log(
  "Repair Regression Guard: bloqueia nova omissão/SDH/censura/gênero/diálogo quebrado ✅"
);

  console.log(
  "Post-Rewrite 8.8.1: auditoria redundante fundida no Final Bounded focal HIGH ✅"
);

console.log(
  "Source Defect Recovery: truncamento evidente pode ser completado de forma mínima; interrupção/ambiguidade são preservadas ✅"
);

console.log(
  `Semantic Guard Safety: qualquer correção ainda exige CANONICAL LOCKS + sentido + ${LAYOUT_MAX_LINES}x${LAYOUT_MAX_CHARS_PER_LINE} ✅`
);

    console.log(
    `Main: até ${MAIN_BATCH_MAX_CUES} cues / ${MAIN_BATCH_MAX_CHARS} chars | concorrência=${MAIN_CONCURRENCY} ✅`
  );

  console.log(
    `Main Empty-Cue Rescue: ${MAIN_EMPTY_CUE_RESCUE_ENABLED ? "ATIVO" : "DESATIVADO"} | ` +
    `somente cue vazio é refeito | ${MAIN_EMPTY_CUE_RESCUE_PARSE_ATTEMPTS} tentativa(s) | consenso SDH=${MAIN_EMPTY_CUE_SDH_CONSENSUS_MIN} | zero loop ✅`
  );

  console.log(
    `Cue capsules: ${CAPSULE_CONTEXT_BEFORE} antes + target fechado + ${CAPSULE_CONTEXT_AFTER} depois ✅`
  );

  console.log(
    `Thinking: PLAN=${PLAN_THINKING} | MAIN=${MAIN_THINKING} | QA=${QA_THINKING} | REPAIR=${REPAIR_THINKING} ✅`
  );

  console.log(
    `Gate global: ${GEMINI_MIN_START_INTERVAL_MS}ms entre inícios ✅`
  );

  console.log(
    "Culture Hard Locks: ATIVOS ✅"
  );

  console.log(
  "Canonical Catchphrase Lock: typos conhecidos da fonte são restaurados para a forma canônica ✅"
);

console.log(
  "Culture & Register Integrity: bitch/gag/profanity/slang por função social, nunca tabela lexical ✅"
);

console.log(
  "Profanity Pragmatic Lock: preserva força sem inserir palavrão mecanicamente ✅"
);

  console.log(
    "Condragulations / Sashay away / Shantay / Werkroom / Rusical: PROTEGIDOS ✅"
  );

  console.log(
    "Cue Ownership 8.8.1: ID + key por cue, contexto compartilhado ✅"
  );

  console.log(
  "Cue Ownership Key Lock: ordem cronológica + ownership_key por ID + resposta na mesma ordem ✅"
);

console.log(
  "Semantic Compact Retry: correção semântica >2x50 é compactada antes de ser rejeitada ✅"
);

console.log(
  "Semantic Canonical Locks: auditor pós-rewrite usa bordões reais, não tokens opacos ✅"
);

  console.log(
    "GAG/GAGGED reaction guard: ATIVO ✅"
  );

  console.log(
    "BOTTOM + palavrões/intensificadores: GUARDS ATIVOS ✅"
  );

  console.log(
    "Censored Bleep Reconstruction: ATIVO ✅"
  );

  console.log(
    "HARD SDH Sanitizer pré/pós Gemini + credits/placeholders: ATIVO ✅"
  );

  console.log(
    "Profanity Integrity Lock: ATIVO ✅"
  );

  console.log(
    `PT-BR QA contextual SOURCE×PT + Identity Lock + anti-literalidade: ATIVO | concorrência=${QA_CONCURRENCY} ✅`
  );

  console.log(
    "Format Lock Empty-Cue Rescue: ATIVO ✅"
  );

  console.log(
    `Pre-Repair Semantic Confirmation 8.4.5: ${PRE_REPAIR_CONFIRM_ROUNDS} auditorias limpas para heurística ambígua escapar do Repair; falha técnica=FAIL-SAFE ✅`
  );

  console.log(
    `Concurrency Efficiency 8.4.5: MAIN=${MAIN_CONCURRENCY} | QA=${QA_CONCURRENCY} | Semantic=${SEMANTIC_REWRITE_AUDIT_CONCURRENCY} | gate global=${GEMINI_MIN_START_INTERVAL_MS}ms INALTERADO ✅`
  );

  console.log(
    "Intentional Empty 8.4.4: somente SOURCE realmente descartável pode sumir; fala real continua fail-closed ✅"
  );

  console.log(
    "No Full Restart 8.4.4: empty cue/sanitizer retry fica no cue; PLAN+MAIN não reiniciam por esse motivo ✅"
  );

  console.log(
    "Job Wall Clock 8.4.4: FINAL reporta pipeline + tempo real total do job ✅"
  );

  console.log(
    "Localização brasileira por intenção: ATIVA ✅"
  );

  console.log(
    "Símbolos inúteis + notas estendidas: NORMALIZAÇÃO ATIVA ✅"
  );

  console.log(
    "Timestamp lock: absoluto; Gemini nunca gera tempos ✅"
  );

  console.log(
    "SAFE DRAFT: ATIVO ✅ (diagnóstico interno; nunca substitui FINAL crítico reprovado)"
  );

  console.log(
    "Source Hygiene 8.4.0: Ff/J'j'/J“j“/pontuação isolada removidos; wrappers com conteúdo preservam o conteúdo ✅"
  );

  console.log(
    "Gender Integrity V2: contradição intracuе + speaker unknown marcado + trusted gender hard-check ✅"
  );

  console.log(
    "Semantic Ownership Audit: SOURCE[i]×PT[i] contra vizinhos; SOURCE pode ser QUALQUER idioma ✅"
  );

  console.log(
    "Final Audit 8.4.2: lotes <=80 + schema sem maxItems + fallback adaptativo para HTTP 400 ✅"
  );

  console.log("Focused Repair 8.4.3: Final Critical repairs only current-round blockers. OK");

  console.log(
    `Job Liveness 8.4.6: até ${JOB_MAX_ATTEMPTS} tentativa(s); SAFE DRAFT íntegro encerra falha tardia; zero processing eterno ✅`
  );
  console.log("Final 8.8.1: mesmo pipeline bounded 8.8; HARD SDH early-drop + neutral gender default; zero estágio Gemini novo ✅");
  console.log("MAIN Robust 8.8.1: extras/duplicatas não derrubam lote; somente cues ausentes são refeitos ✅");
  console.log("MAIN Fail-Fast 8.8.1: erro determinístico não vira loop; payload compacto + rescue focal ✅");
  console.log("Final Bounded 8.8.1: zero loop de convergência; no máximo 2 auditorias focais + repairs focais ✅");
  console.log("Semantic Sync API preservada para OpenSub; Embedded 2.6 não depende dela ✅");

  console.log(
    `Cache namespace: ${CACHE_VERSION}`
  );

  console.log(
    "Multilingual Audio-Sync API: /api/sync-align + /api/sync-proxy + language-aware Transcribe ATIVOS ✅"
  );

  console.log(
    "Pre-Repair 8.8.1: rodada Gemini redundante removida; QA HIGH global é a autoridade semântica ✅"
  );

  console.log(
    "Concurrency 8.4.5: more in-flight work without increasing Gemini start rate. OK"
  );

  console.log(
    "Empty-Cue 8.4.4: intentional omission + sanitizer-aware local recovery + no full restart. OK"
  );

  console.log(
    "HARD SDH 8.8.1: caption/action-only é eliminado ANTES do MAIN; SDH-only não pode acionar Gemini rescue ✅"
  );

  console.log(
    "Gender Neutral Default 8.8.1: SOURCE neutra => PT-BR neutro natural; speaker label nunca atravessa turno ✅"
  );

  console.log(
    "Latency Contract 8.8.1: zero etapa Gemini nova; mesmos MAIN/QA/REPAIR/FINAL bounded; SDH early-drop reduz trabalho ✅"
  );

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
