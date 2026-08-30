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
// STREMIO PT-BR 8.3.7 — TRANSLATION QUALITY + CONTEXT + IDENTITY LOCK + GEMINI TRANSCRIBE BUDGET/MONTAGE
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();
const GEMINI_MODEL = "gemini-3.5-flash-lite";
const GEMINI_TRANSCRIBE_MODEL = "gemini-3.5-transcribe";

const CACHE_VERSION = "8.3.7-plan-fast-priority-qa-transcribe-budget";
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_SOURCE_CHARS = 800000;
const FETCH_TIMEOUT_MS = 25000;

const GEMINI_MIN_START_INTERVAL_MS = 4300;

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
// já consumidas nas últimas 24h durante o deploy do 8.3.7.
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

const MAIN_BATCH_MAX_CUES = 60;
const MAIN_BATCH_MAX_CHARS = 15000;
const MAIN_CONCURRENCY = 2;
const CAPSULE_CONTEXT_BEFORE = 2;
const CAPSULE_CONTEXT_AFTER = 2;
const MAIN_THINKING = "high";
const MAIN_MAX_OUTPUT_TOKENS = 18000;
const MAIN_TIMEOUT_MS = 120000;
const MAIN_HTTP_RETRIES = 4;
const MAIN_PARSE_ATTEMPTS = 2;

const REPAIR_ENABLED = true;
const REPAIR_MAX_CUES_TOTAL = 120;
const REPAIR_BATCH_MAX_CUES = 24;
const REPAIR_THINKING = "high";
const REPAIR_MAX_OUTPUT_TOKENS = 10000;
const REPAIR_TIMEOUT_MS = 90000;
const REPAIR_HTTP_RETRIES = 3;
const REPAIR_PARSE_ATTEMPTS = 2;

// QA semântico EN×PT para TODAS as fontes.
// Não reescreve diretamente: aponta cues problemáticos para Repair.
const QA_ENABLED = true;
const QA_BATCH_MAX_CUES = 160;
const QA_BATCH_MAX_CHARS = 36000;
const QA_THINKING = "high";
const QA_MAX_OUTPUT_TOKENS = 9000;
const QA_TIMEOUT_MS = 120000;
const QA_HTTP_RETRIES = 3;
const QA_PARSE_ATTEMPTS = 2;
const QA_MAX_FLAGS_TOTAL = 120;
const QA_CONCURRENCY = 2;
const QA_CONTEXT_BEFORE = 1;
const QA_CONTEXT_AFTER = 1;

const BLEEP_TOKEN = "__CENSORED_BLEEP__";
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

function setCache(key, srt) {
  translationCache.set(
    key,
    {
      srt,
      expiresAt:
        Date.now() +
        CACHE_TTL_MS
    }
  );
}

function createJob({
  type,
  videoId,
  filename,
  sourceSrt,
  sourceKind,
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

    started: false,
    promise: null,

    createdAt: now,
    updatedAt: now,

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

    job.status =
      "completed";

    job.progress =
      100;

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
    SDH_WORDS.test(speaker) ||
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
      ) &&
      !SDH_WORDS.test(
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

function looksLikeSdhDescriptor(
  value
) {
  const inside =
    stripMarkup(value)
      .replace(
        /[.:;!?]+$/g,
        ""
      )
      .replace(/\s+/g, " ")
      .trim();

  if (
    !inside ||
    inside.length > 140
  ) {
    return false;
  }

  if (
    looksLikeSpeakerLabel(
      inside
    )
  ) {
    return false;
  }

  return SDH_WORDS.test(
    inside
  );
}

function looksLikeBareSdhLine(
  value
) {
  const text =
    stripMarkup(value)
      .replace(
        /[.:;!?]+$/g,
        ""
      )
      .replace(/\s+/g, " ")
      .trim();

  if (
    !text ||
    text.length > 100 ||
    looksLikeSpeakerLabel(
      text
    )
  ) {
    return false;
  }

  return /^(?:sound of |sounds of )?(?:static|laughter|applause|music(?: playing)?|song(?: playing)?|footsteps?(?: approaching| receding)?|door (?:opens|closes|slams|creaks)|phone (?:rings|buzzes)|wind (?:blows|howls)|thunder|rain(?: falling)?|fire crackling|glass (?:breaks|shatters)|engine (?:starts|idles)|car horn|tires? screeching|branch (?:breaks|snaps)|leaves rustling|heavy breathing|panting|gasping|sobbing|crying|humming|whistling|growling|roaring|howling|muffled voices?|distant voices?|estática|risos?|aplausos?|música|passos?(?: se aproximando| ao longe)?|porta (?:abrindo|fechando|batendo|rangendo)|telefone (?:tocando|vibrando)|vento (?:soprando|uivando)|trovão|chuva|fogo estalando|vidro (?:quebrando|estilhaçando)|motor (?:ligando|em marcha lenta)|buzina|pneus? cantando|galho (?:quebrando|estalando)|folhas farfalhando|respiração (?:forte|ofegante)|ofegante|ofegando|chorando|soluçando|tarareando|assobiando|rosnado|uivo|vozes? abafadas?|vozes? ao longe|som (?:abafado )?(?:de )?(?:passos|pisadas|esmagamento|algo sendo esmagado)|esmagando|som pastoso)$/i.test(
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
    stripTrailingSpeakerLabel(
      text
    );

  text =
    removeSdhSegments(
      text
    ).replace(
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

function cleanSrtForTranslation(
  srt
) {
  const normalized =
    normalizeSrt(srt);

  if (!normalized) {
    return "";
  }

  const rawBlocks =
    normalized
      .split(/\n{2,}/)
      .filter(Boolean);

  const out = [];

  let removed = 0;
  let speakerHints = 0;
  let bleepCues = 0;

  for (const raw of rawBlocks) {
    const lines =
      raw
        .trim()
        .split("\n");

    const timingIndex =
      lines.findIndex(
        line =>
          /-->/.test(line)
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

    for (
      const sourceLine of
      cleanedBlockText.split(
        "\n"
      )
    ) {
      const info =
        extractSpeaker(
          sourceLine
        );

      if (info.speaker) {
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

    if (
      speakers.size === 1
    ) {
      const speaker =
        [...speakers][0];

      dialogue[0] =
        `@@SPK:${
          encodeURIComponent(
            speaker
          )
        }@@ ${dialogue[0]}`;

      speakerHints++;
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
    }; speakerHints=${
      speakerHints
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
        (block, index) =>
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
      /\bLip\s+Sync\s+for\s+Your\s+Life\b/giu,
    value:
      "Lip Sync for Your Life"
  },
  {
    regex:
      /\bLip\s+Sync\s+for\s+the\s+Crown\b/giu,
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
      /\bSashay\s+away\b/giu,
    value:
      "Sashay away"
  },
  {
    regex:
      /\bYou\s+betta\s+werk\b/giu,
    value:
      "You betta werk"
  },
  {
    regex:
      /\bCondragulations\b/giu,
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
  {
    regex:
      /\blip\s+sync\b/giu,
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

// ============================================================
// FINAL FORMAT LOCK
// ============================================================

function sourceDialogueDashCount(
  block
) {
  return String(
    block?.text || ""
  )
    .split("\n")
    .filter(
      line =>
        /^\s*[-–—]\s+/u.test(
          line
        )
    )
    .length;
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
    text =
      info.text;
  }

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

  const preserveDialogueDashes =
    sourceDialogueDashCount(
      block
    ) >= 2;

  let lines =
    text
      .replace(/\r/g, "")
      .split("\n")
      .map(
        stripOutputAccessibilityLine
      )
      .map(
        line => line.trim()
      )
      .filter(Boolean)
      .map(line => {
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
          !preserveDialogueDashes
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
      })
      .filter(Boolean);

  if (!lines.length) {
    return "";
  }

  if (
    preserveDialogueDashes &&
    lines.length >= 2
  ) {
    lines =
      lines.map(
        line =>
          line.startsWith("- ")
            ? line
            : `- ${line}`
      );
  }

  return lines
    .join("\n")
    .trim();
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
  let rescuedEmpty = 0;

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
        after = "…";
      }

      rescuedEmpty++;

      console.warn(
        `[FORMAT LOCK] cue ${
          block.index
        } ficaria vazio; fallback conservador aplicado | raw=${
          JSON.stringify(
            before.slice(
              0,
              140
            )
          )
        }`
      );
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
    } cue(s) normalizado(s); ${
      rescuedEmpty
    } cue(s) resgatado(s) de vazio; SDH/ruído/alongamentos controlados.`
  );

  return out;
}

function buildSrt(
  blocks,
  translations
) {
  return (
    blocks
      .map(
        block =>
          [
            block.index,
            block.timing,

            String(
              translations.get(
                block.index
              ) ??
              block.text
            ).trim()
          ].join("\n")
      )
      .join("\n\n")
      .trim() +
    "\n"
  );
}

function auditTimestamps(
  sourceSrt,
  finalSrt,
  label
) {
  const source =
    parseSrt(sourceSrt);

  const final =
    parseSrt(finalSrt);

  if (
    source.length !==
    final.length
  ) {
    throw new Error(
      `TIMING LOCK ${
        label
      }: ${
        source.length
      }/${
        final.length
      }.`
    );
  }

  for (
    let i = 0;
    i < source.length;
    i++
  ) {
    if (
      source[i].index !==
        final[i].index ||
      source[i].timing !==
        final[i].timing
    ) {
      throw new Error(
        `TIMING LOCK ${
          label
        }: cue ${
          source[i].index
        }.`
      );
    }
  }

  console.log(
    `[TIMING LOCK] ${
      label
    }: PASSOU — ${
      source.length
    }/${
      source.length
    }; 0 alterações.`
  );
}

// ============================================================
// STYLE / CONTEXTO
// ============================================================

const STYLE_PACK = `
PORTUGUÊS BRASILEIRO NATURAL — GUIA EDITORIAL 8.3.7

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

CONTEXT + IDENTITY LOCK — REGRA INVIOLÁVEL
- A BÍBLIA EDITORIAL contém um Character Ledger. Trate identidades confirmadas como estado de continuidade do episódio.
- Quando gênero/pronomes estiverem marcados como conhecidos/seguros, respeite-os na concordância e nos referentes em PT-BR.
- Quando gênero for unknown/incerto, NÃO adivinhe. Reformule naturalmente para evitar marcar gênero sem necessidade.
- Speaker é QUEM ESTÁ FALANDO; pessoa citada/mentioned é DE QUEM SE FALA. speaker ≠ pessoa mencionada.
- Nunca transfira gênero, pronome, relação ou identidade do speaker para a pessoa mencionada, nem o contrário.
- Um nome/pronome no target deve ser resolvido com before/after + Character Ledger; se ainda houver ambiguidade, preserve a ambiguidade de forma natural.
- Não invente parentesco, identidade, pronome, título ou nome ausente da evidência.

GENDER-SAFE LANGUAGE — QUANDO O SPEAKER NÃO ESTÁ PROVADO
- Ausência de speaker NÃO autoriza masculino genérico nem feminino por palpite.
- Se identity_lock disser speaker_unknown/neutralize, evite concordância de 1ª pessoa que escolha gênero sem evidência.
- Prefira reformulações naturais: "I'm scared" -> "Tô com medo"; "I'm exhausted" -> "Não aguento mais" / "Tô sem energia" quando o gênero do speaker não estiver provado.
- Se before/after não provarem inequivocamente quem fala, preserve a incerteza. Não deduza gênero só porque uma pessoa conhecida aparece na cena ou é mencionada no target.
- Se a pessoa mencionada tiver gênero conhecido, aplique esse gênero SOMENTE ao referente mencionado, nunca automaticamente ao speaker.

NATURALIDADE PT-BR 2026 — REGRA DE ACEITAÇÃO
- CORRETO MAS LITERAL DEMAIS NÃO É SUFICIENTE. Tradução com cara de tradução é defeito editorial.
- Antes de devolver cada cue, faça o teste: "um brasileiro falaria isso espontaneamente hoje, nessa situação e com essa personalidade?".
- Preserve intenção, pragmática e temperatura emocional antes da ordem das palavras do inglês.
- Reestruture sujeito, verbo, intensificador, metáfora e ordem sintática quando o português pedir.
- Evite português de dublagem antigo, formalidade artificial, calques e falsos cognatos.
- Use PT-BR contemporâneo SEMPRE; use Gen Z/Alpha/fandom SOMENTE quando idade, personalidade, comunidade e situação pedirem.
- Uma fala adulta em drama/horror deve soar atual e humana, não necessariamente como internet/TikTok.
- Não use "sequer", "de fato", "eu suponho", "eu aprecio isso", "isso sendo dito" ou outras formas engessadas por reflexo do inglês quando uma forma brasileira simples for mais natural no registro da cena.

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

LGBTQIAPN+ / DRAG / BALLROOM / REALITY / FANDOM
- Tenha letramento real de cultura LGBTQIAPN+, drag, ballroom, camp, shade, stan culture e reality competition.
- Preserve humor, sexualidade, irreverência, shade, camp, afeto e agressividade conforme a cena.
- Não suavize a personalidade de queens, jurados ou participantes.
- Não force gíria em pessoas cujo registro não pede isso.

GAG / GAGGED / GAGGING EM SENTIDO DE REAÇÃO
- Em reação, surpresa, impacto ou admiração, prefira: "passada", "tô passada", "fiquei passada", "em choque", "sem reação".
- Em Drag Race/reality queer, "I'm gagged" normalmente deve soar como "tô passada".
- NUNCA use "amordaçada" ou "engasgada" nesse sentido.
- Só use sentido físico quando a cena realmente falar de boca, engasgo, reflexo de vômito, sufocamento etc.

BOTTOM EM COMPETIÇÕES / REALITY
- Em Drag Race/reality, bottom frequentemente significa colocação ruim ou risco de eliminação.
- "in the bottom" -> "no bottom", "entre as piores" ou "na berlinda".
- "bottom queens" -> "queens do bottom" ou "as piores da semana".
- "bottom two" -> "bottom 2" ou "as duas piores".
- "bottom three" -> "bottom 3" ou "as três piores".
- NUNCA traduza bottom competitivo como "fundo", "quintal", "parte de baixo" ou "inferior".
- Diferencie bottom competitivo de bottom sexual pelo contexto.

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
- Use hífen de diálogo apenas quando o próprio cue tiver duas falas separadas.
- Preserve quebra de linha quando ela separar duas falas no mesmo cue.

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
Você é editor de continuidade EN→PT-BR e responsável pelo CONTEXT + IDENTITY LOCK.
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
Você é editor de continuidade EN→PT-BR. A saída estruturada principal não pôde ser usada.
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
Você é o tradutor principal de legendas EN→PT-BR.

${STYLE_PACK}

Você receberá uma lista de CÁPSULAS.
Cada cápsula contém before, target, after e identity_lock.
Traduza SOMENTE target.
As cápsulas podem estar propositalmente fora de ordem para impedir redistribuição de conteúdo. Isso é intencional.

CHECKLIST SILENCIOSO OBRIGATÓRIO ANTES DE CADA pt:
1. Quem fala está realmente provado? Se não, não marque gênero de 1ª pessoa sem necessidade.
2. Há pessoa mencionada? Não transfira identidade do speaker para ela ou vice-versa.
3. A frase preserva a intenção e não a sintaxe do inglês?
4. Um brasileiro falaria isso espontaneamente em 2026, nesse registro?
5. A gíria é apropriada à pessoa/contexto, e não uma tentativa artificial de parecer jovem?
6. Todo conteúdo pertence somente a este target?

Devolva exatamente um objeto por target, mantendo o mesmo id em i.
`;

const REPAIR_PROMPT = `
Você é editor final EN→PT-BR.

${STYLE_PACK}

Você receberá somente cues sinalizados por detectores locais e/ou pelo QA PT-BR.
Corrija defeitos reais de cultura, literalidade/calque, censura/bleep, gênero/referente, ortografia, palavra corrompida, naturalidade, SDH residual, omissão, overflow, formatação ou ownership.
- Se identity_lock disser que o speaker é desconhecido/incerto, neutralize concordância de 1ª pessoa quando não houver evidência segura.
- "Gramaticalmente correto" não basta se soar traduzido, antiquado ou pouco espontâneo em PT-BR contemporâneo.
- Se houver palavrão autocensurado graficamente, reconstrua a fala natural por extenso; nunca preserve f..., fu&#, *** ou equivalentes.
Preserve o que já estiver bom.
Não redistribua conteúdo entre ids.
`;

const QA_PROMPT = `
Você é o revisor semântico e linguístico FINAL de legendas EN→PT-BR.
Você recebe EN e PT do MESMO cue, contexto curto e identity_lock. NÃO reescreva aqui: apenas sinalize IDs que devem ir para a única passada de repair.

SEJA EXIGENTE. CORRETO MAS LITERAL DEMAIS = DEFEITO.

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

NÃO marque uma escolha apenas diferente se ela for realmente correta, espontânea e adequada ao registro.
Tô/tá/pra/né e palavrões por extenso podem ser ótimos quando combinarem com a personagem.
Se houver duas boas traduções naturais, NÃO marque.
Se a opção atual soar como tradução mesmo estando entendível, MARQUE.
`;

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
  label = "audio-sync"
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
                      "en"
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
    `ID: ${job.videoId}\n\n` +
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

function compactPersonIdentity(
  person
) {
  if (!person) {
    return null;
  }

  return {
    canonical:
      String(
        person.canonical ||
        ""
      ),

    gender:
      String(
        person.gender ||
        "unknown"
      ),

    pronouns:
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
      String(
        person.confidence ||
        "low"
      )
  };
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
    );

  const confidence =
    String(
      person.confidence ||
      "low"
    );

  if (
    ![
      "female",
      "male",
      "nonbinary"
    ].includes(gender) ||
    confidence === "low"
  ) {
    return null;
  }

  return gender;
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
        ? `speaker com gênero confiável=${trustedGender}; respeitar concordância quando ela realmente se referir ao speaker`
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

function buildOwnershipPayload(
  allBlocks,
  posMap,
  batch,
  plan
) {
  const locksById =
    new Map();

  const capsules = [];

  for (
    const block of
    interleaveBatch(
      batch
    )
  ) {
    const pos =
      posMap.get(
        block.index
      );

    const protectedTarget =
      protectCulturalLocks(
        block.text,
        block.index
      );

    locksById.set(
      block.index,
      protectedTarget.locks
    );

    capsules.push({
      i:
        block.index,

      before:
        allBlocks
          .slice(
            Math.max(
              0,
              pos -
              CAPSULE_CONTEXT_BEFORE
            ),
            pos
          )
          .map(
            contextCue
          ),

      target: {
        i:
          block.index,

        en:
          protectedTarget.text,

        ...(
          block.speakerHint
            ? {
                speaker:
                  block.speakerHint
              }
            : {}
        )
      },

      after:
        allBlocks
          .slice(
            pos + 1,

            Math.min(
              allBlocks.length,

              pos +
              1 +
              CAPSULE_CONTEXT_AFTER
            )
          )
          .map(
            contextCue
          ),

      identity_lock:
        identityLockForCapsule(
          block,
          plan
        ),

      hard_locks:
        protectedTarget
          .locks
          .map(
            lock =>
              lock.token
          )
    });
  }

  return {
    payload: {
      ownership_rule:
        "Cada cápsula é independente. Traduza somente target; before/after são leitura contextual e nunca fornecem conteúdo ao output. Se speaker for unknown, não adivinhe gênero. Correto mas literal demais deve ser reescrito em PT-BR espontâneo.",

      capsules
    },

    locksById
  };
}

function parseCueTranslation(
  batch,
  raw,
  locksById = new Map()
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
    new Set(ids);

  const byId =
    new Map();

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
      byId.has(id)
    ) {
      throw new Error(
        `ID duplicado ${id}.`
      );
    }

    if (!pt) {
      throw new Error(
        `Cue ${id} vazio.`
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
    byId.size !==
    ids.length
  ) {
    throw new Error(
      `Tradução incompleta ${
        byId.size
      }/${
        ids.length
      }.`
    );
  }

  return byId;
}

async function translateMainBatch({
  blocks,
  posMap,
  batch,
  plan,
  job
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
        locksById
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
            `BÍBLIA EDITORIAL:\n${
              JSON.stringify(
                plan
              )
            }\n\n` +
            `CÁPSULAS CUE-LOCK:\n${
              JSON.stringify(
                payload
              )
            }\n\n` +
            `Traduza somente cada target. ` +
            `Output exatamente ${
              batch.length
            } cues. ` +
            `Todos os tokens __LOCK_C...__ recebidos no target devem voltar idênticos em pt. ` +
            `O token ${BLEEP_TOKEN} deve ser resolvido em linguagem natural, nunca copiado.`,

          schema:
            cueTranslationSchema(
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

      return parseCueTranslation(
        batch,
        response.text,
        locksById
      );
    } catch (error) {
      lastError =
        error;

      if (
        parseAttempt >=
        MAIN_PARSE_ATTEMPTS
      ) {
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

  for (
    let pos = 0;
    pos < blocks.length;
    pos++
  ) {
    const block =
      blocks[pos];

    const item = {
      i:
        block.index,

      en:
        String(
          block.text ||
          ""
        ),

      pt:
        String(
          translations.get(
            block.index
          ) ||
          ""
        ),

      ...(
        block.speakerHint
          ? {
              speaker:
                block.speakerHint
            }
          : {}
      ),

      identity_lock:
        conciseIdentityForQa(
          block,
          plan
        ),

      before:
        blocks
          .slice(
            Math.max(
              0,
              pos -
              QA_CONTEXT_BEFORE
            ),

            pos
          )
          .map(
            contextCue
          ),

      after:
        blocks
          .slice(
            pos + 1,

            Math.min(
              blocks.length,
              pos +
              1 +
              QA_CONTEXT_AFTER
            )
          )
          .map(
            contextCue
          )
    };

    const size =
      JSON.stringify(
        item
      ).length;

    if (
      current.length &&
      (
        current.length >=
          QA_BATCH_MAX_CUES ||
        chars + size >
          QA_BATCH_MAX_CHARS
      )
    ) {
      batches.push(
        current
      );

      current = [];
      chars = 0;
    }

    current.push(
      item
    );

    chars +=
      size;
  }

  if (current.length) {
    batches.push(
      current
    );
  }

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

    out.push({
      id,

      reasons: [
        `QA_PTBR: ${
          reason ||
          "defeito claro"
        }`
      ]
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
                `BÍBLIA EDITORIAL DO EPISÓDIO:\n${
                  JSON.stringify(
                    plan || {}
                  )
                }\n\n` +
                `CUES EN×PT PARA AUDITORIA:\n${
                  JSON.stringify(
                    batch
                  )
                }\n\n` +
                `Retorne IDs que merecem repair por erro semântico, gênero/referente, calque, literalidade ou naturalidade insuficiente. Não reescreva os cues.`,

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
    !translated.trim()
  ) {
    reasons.push(
      "EMPTY"
    );
  }

  if (
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

  if (
    sourceDialogueDashCount(
      block
    ) >= 2 &&
    translated
      .split("\n")
      .filter(Boolean)
      .length < 2
  ) {
    reasons.push(
      "MISSING_DIALOGUE_BREAK"
    );
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
    translated
      .split("\n")
      .some(
        line =>
          [...line].length >
          52
      )
  ) {
    reasons.push(
      "OVERLONG_SUBTITLE_LINE"
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
    /UNKNOWN_SPEAKER_GENDER_MARKED/i.test(joined) ||
    /POSSIBLE_OMISSION/i.test(joined) ||
    /POSSIBLE_CUE_SHIFT_PAIR/i.test(joined) ||
    /EMPTY/i.test(joined) ||
    /UNRESOLVED_BLEEP_TOKEN/i.test(joined) ||
    /BLEEP_CREATED_DANGLING_SENTENCE/i.test(joined) ||
    /ARTIFICIAL_PROFANITY_CENSORSHIP/i.test(joined) ||
    /MISSING_DIALOGUE_BREAK/i.test(joined) ||

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
                i:
                  item.index,

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
                i:
                  item.index,

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
            `Todos os tokens __LOCK_C...__ devem voltar idênticos. ` +
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

async function tryFocusedRepair(
  blocks,
  translations,
  plan,
  job,
  extraIssues = []
) {
  if (!REPAIR_ENABLED) {
    return translations;
  }

  let issues;

  try {
    issues =
      detectLocalIssues(
        blocks,
        translations,
        job.filename,
        plan
      );
  } catch (error) {
    console.warn(
      `[LOCAL GUARD] falhou; mantendo principal: ${
        errorMessage(error)
      }`
    );

    return translations;
  }

  const localOnlyCount =
    issues.length;

  issues =
    mergeIssueLists(
      issues,
      extraIssues
    );

  job.stats.localFlags =
    localOnlyCount;

  if (!issues.length) {
    console.log(
      "[LOCAL GUARD] 0 suspeitos."
    );

    return translations;
  }

  logIssueSummary(
  "PRÉ-REPAIR",
  issues
);

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
    `[LOCAL GUARD] ${
      issues.length
    } suspeitos combinados (local+QA); reparando até ${
      selected.length
    }.`
  );

  const posMap =
    positionMap(
      blocks
    );

  const updated =
    new Map(
      translations
    );

  try {
    for (
      let i = 0;
      i < selected.length;
      i +=
        REPAIR_BATCH_MAX_CUES
    ) {
      const batch =
        selected.slice(
          i,
          i +
          REPAIR_BATCH_MAX_CUES
        );

      const repaired =
        await repairBatch(
          blocks,
          posMap,
          updated,
          batch,
          plan,
          job
        );

      for (
        const [
          id,
          pt
        ] of repaired
      ) {
        updated.set(
          id,
          pt
        );
      }
    }

    return updated;
  } catch (error) {
    job.stats.repairFailures++;

    console.warn(
      `[REPAIR] falhou sem matar episódio: ${
        errorMessage(
          error
        ).slice(
          0,
          350
        )
      }`
    );

    return translations;
  }
}

// ============================================================
// PIPELINE / JOB
// ============================================================

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

  job.stats.sourceCues =
    blocks.length;

  console.log(
    `[PIPELINE 8.3.7] fonte=${
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

  const mainSrt =
    buildSrt(
      blocks,
      mainTranslations
    );

  auditTimestamps(
    sourceSrt,
    mainSrt,
    "MAIN"
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
    } protegido com CUE + FORMAT LOCK.`
  );

  const qaIssues =
    await scanPtbrQuality(
      blocks,
      mainTranslations,
      plan,
      job
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

  const remaining =
    detectLocalIssues(
      blocks,
      finalTranslations,
      job.filename,
      plan
    );

  if (remaining.length) {
  logIssueSummary(
    "PÓS-REPAIR",
    remaining
  );

  console.log(
    `[POST-REPAIR GUARD] ${
      remaining.length
    } cue(s) ainda sinalizado(s) após o repair; mantendo o primeiro repair para evitar retradução repetitiva e perda de velocidade.`
  );
}

  const finalSrt =
    buildSrt(
      blocks,
      finalTranslations
    );

  auditTimestamps(
    sourceSrt,
    finalSrt,
    "FINAL"
  );

  console.log(
    `[PIPELINE 8.3.7] FINAL OK | ${
      blocks.length
    } cues | ${
      (
        (
          Date.now() -
          startedAt
        ) /
        1000
      ).toFixed(1)
    }s.`
  );

  return finalSrt;
}

async function processJob(
  job
) {
  job.status =
    "processing";

  job.progress =
    Math.max(
      1,
      job.progress || 0
    );

  job.updatedAt =
    Date.now();

  try {
    const cached =
      getCache(
        job.cacheKey
      );

    if (cached) {
      auditTimestamps(
        job.sourceSrt,
        cached,
        "CACHE"
      );

      job.result =
        cached;

      job.status =
        "completed";

      job.progress =
        100;

      return;
    }

    const finalSrt =
      await translateSrt(
        job.sourceSrt,
        job
      );

    setCache(
      job.cacheKey,
      finalSrt
    );

    job.result =
      finalSrt;

    job.status =
      "completed";

    job.progress =
      100;
  } catch (error) {
    if (job.safeDraft) {
      try {
        auditTimestamps(
          job.sourceSrt,
          job.safeDraft,
          "SAFE-DRAFT-FALLBACK"
        );

        setCache(
          job.cacheKey,
          job.safeDraft
        );

        job.result =
          job.safeDraft;

        job.status =
          "completed";

        job.progress =
          100;

        job.stats.usedSafeDraftFallback =
          true;

        console.warn(
          `[JOB ${
            job.id
          }] entregando SAFE DRAFT após erro opcional: ${
            errorMessage(
              error
            ).slice(
              0,
              300
            )
          }`
        );

        return;
      } catch {}
    }

    job.status =
      "failed";

    job.error =
      errorMessage(
        error
      );

    console.error(
      `[JOB ${
        job.id
      }] Falhou: ${
        job.error
      }`
    );
  } finally {
    job.updatedAt =
      Date.now();
  }
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
            "Stremio-PTBR/8.3.7"
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
            "Stremio-PTBR/8.3.7"
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
    "8.3.7",

  name:
    "PT-BR Cloud • OpenSubtitles",

  description:
    "OpenSubtitles inglês → PT-BR com Context + Identity Lock, Character Ledger, Cue Ownership, HARD SDH, QA EN×PT e suporte ao OpenSub Sync V5 por Gemini Transcribe, Character Ledger compatível e QA anti-literalidade.",

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
          sourceKind
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
          label
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
          "SERVING"
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
      return sendSrt(
        res,
        errorSrt(
          job.error
        )
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
    " STREMIO PT-BR 8.3.7 — MAX TRANSLATION QUALITY + IDENTITY SAFE + ANTI-LITERAL + TRANSCRIBE BUDGET/MONTAGE"
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
    `Main: até ${MAIN_BATCH_MAX_CUES} cues / ${MAIN_BATCH_MAX_CHARS} chars | concorrência=${MAIN_CONCURRENCY} ✅`
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
    "Condragulations / Sashay away / Shantay / Werkroom / Rusical: PROTEGIDOS ✅"
  );

  console.log(
    "Cue Ownership capsules: ATIVAS ✅"
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
    `PT-BR QA contextual EN×PT + Identity Lock + anti-literalidade: ATIVO | concorrência=${QA_CONCURRENCY} ✅`
  );

  console.log(
    "Format Lock Empty-Cue Rescue: ATIVO ✅"
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
    "Repair: UMA passada cirúrgica; sem segunda retradução ✅"
  );

  console.log(
    "SAFE DRAFT: ATIVO ✅"
  );

  console.log(
    `Cache namespace: ${CACHE_VERSION}`
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
