const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// STREMIO PT-BR BACKEND 7.0 FINAL — GEMINI 3.5 FLASH-LITE ONLY
// ============================================================
// Objetivo: qualidade + coerência + sincronização + velocidade.
// Modelo único: gemini-3.5-flash-lite.
//
// Limites observados pelo usuário no Free Tier:
//   15 RPM | 250K TPM | 500 RPD
//
// Arquitetura por episódio:
//   1 chamada curta de CONTEXTO GLOBAL
//   4 lotes principais em PARALELO
//   até 8 lotes de AUDITORIA ampla
//   1 micro-auditoria PROFUNDA dos grupos de maior risco
//   reparo SOMENTE do que foi sinalizado
//   rechecagem SOMENTE do que foi reparado
//
// O gate de RPM NÃO é um pacer arbitrário: ele só impede ultrapassar
// o limite real de 15 requisições nos últimos 60 segundos.
// Não existe teto global do episódio.
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();

// Deliberadamente fixo para impedir que uma variável antiga volte ao 3.6.
const GEMINI_MODEL = "gemini-3.5-flash-lite";

const CACHE_VERSION = "7.0.0-flash-lite-final";
const MAX_SOURCE_CHARS = 800000;
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS = 25000;

// Limites reais fornecidos pelo usuário.
const GEMINI_RPM_LIMIT = 15;
const GEMINI_TPM_LIMIT = 250000;
const GEMINI_RPD_LIMIT = 500;

// 4 lotes principais: boa combinação de paralelismo + margem de RPM.
const MAIN_PARALLEL_BATCHES = 4;
const MAIN_CONTEXT_OVERLAP_GROUPS = 8;
const MAIN_THINKING = "low";
const MAIN_MAX_OUTPUT_TOKENS = 18000;
const MAIN_TIMEOUT_MS = 150000;
const MAIN_RETRIES = 4;

// Planner curto: extrai apenas contexto útil para manter coerência.
const PLAN_THINKING = "minimal";
const PLAN_MAX_OUTPUT_TOKENS = 2600;
const PLAN_TIMEOUT_MS = 45000;

// Auditoria ampla: 1150 groups normalmente viram 8 lotes (~144 cada).
const AUDIT_BATCH_GROUPS = 150;
const AUDIT_BATCH_CHARS = 34000;
const AUDIT_CONCURRENCY = 4;
const AUDIT_THINKING = "minimal";
const AUDIT_MAX_OUTPUT_TOKENS = 7000;
const AUDIT_TIMEOUT_MS = 70000;
const AUDIT_RETRIES = 3;

// Uma micro-auditoria extra para os 40 trechos mais arriscados.
const DEEP_AUDIT_MAX_GROUPS = 40;
const DEEP_AUDIT_THINKING = "low";
const DEEP_AUDIT_MAX_OUTPUT_TOKENS = 5000;

// Reparo focado: poucos groups, mais raciocínio.
const REPAIR_BATCH_GROUPS = 100;
const REPAIR_BATCH_CHARS = 32000;
const REPAIR_THINKING = "medium";
const REPAIR_MAX_OUTPUT_TOKENS = 14000;
const REPAIR_TIMEOUT_MS = 100000;
const REPAIR_RETRIES = 4;

// Última tentativa só para residual realmente conhecido.
const EMERGENCY_THINKING = "high";

const translationCache = new Map();
const jobs = new Map();
const queue = [];
let queueRunning = false;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ============================================================
// HELPERS
// ============================================================

function sha256(value) {
  return crypto.createHash("sha256").update(String(value), "utf8").digest("hex");
}

function randomId(bytes = 6) {
  return crypto.randomBytes(bytes).toString("hex");
}

function errorMessage(error) {
  return String(error?.message || error || "Erro desconhecido.");
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
    .replace(/^\s*```(?:json|text|plaintext|srt)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

function baseUrl(req) {
  if (PUBLIC_URL) return PUBLIC_URL;

  const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "https")
    .split(",")[0]
    .trim();

  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "")
    .split(",")[0]
    .trim();

  return `${proto}://${host}`.replace(/\/+$/, "");
}

function safeJson(res, payload, status = 200) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate");
  return res.status(status).json(payload);
}

function sendSrt(res, srt, cacheControl = "no-store") {
  res.status(200);
  res.set("Content-Type", "application/x-subrip; charset=utf-8");
  res.set("Cache-Control", cacheControl);
  res.send(String(srt || ""));
}

function authorized(req) {
  if (!LOCAL_BRIDGE_SECRET) return false;

  const a = Buffer.from(String(req.headers.authorization || "").trim());
  const b = Buffer.from(`Bearer ${LOCAL_BRIDGE_SECRET}`);

  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ============================================================
// CACHE / JOBS
// ============================================================

function makeCacheKey(type, videoId, sourceSrt) {
  return `${CACHE_VERSION}:${type}:${videoId}:${sha256(sourceSrt)}`;
}

function getCache(key) {
  const item = translationCache.get(key);
  if (!item) return null;

  if (item.expiresAt <= Date.now()) {
    translationCache.delete(key);
    return null;
  }

  return item.srt;
}

function setCache(key, srt) {
  translationCache.set(key, {
    srt,
    expiresAt: Date.now() + CACHE_TTL_MS
  });
}

function createJob({ type, videoId, filename, sourceSrt, sourceKind }) {
  const now = Date.now();
  const sourceHash = sha256(sourceSrt);

  const job = {
    id: `job-${sourceHash.slice(0, 24)}-${randomId()}`,
    type,
    videoId,
    filename,
    sourceSrt,
    sourceKind,
    sourceHash,
    cacheKey: makeCacheKey(type, videoId, sourceSrt),

    status: "processing",
    progress: 1,
    result: null,
    error: null,

    createdAt: now,
    updatedAt: now,
    expiresAt: now + JOB_TTL_MS,

    stats: {
      planCalls: 0,

      mainCalls: 0,
      mainAttempts: 0,
      main429: 0,
      mainSplits: 0,
      mainRescueGroups: 0,

      auditCalls: 0,
      auditAttempts: 0,
      audit429: 0,
      deepAuditCalls: 0,
      auditPrimaryGroups: 0,
      auditRecheckGroups: 0,
      auditFlagged: 0,

      repairCalls: 0,
      repairAttempts: 0,
      repair429: 0,
      repairedGroups: 0,

      secondPassGroups: 0,
      emergencyRepairGroups: 0,

      localStyleFlags: 0,
      omissionFlags: 0,

      rpmWaitMs: 0,

      inputTokens: 0,
      outputTokens: 0,
      thoughtTokens: 0
    }
  };

  jobs.set(job.id, job);
  return job;
}

function findJobByCache(key, statuses) {
  for (const job of jobs.values()) {
    if (job.cacheKey === key && statuses.includes(job.status)) {
      return job;
    }
  }

  return null;
}

function getOrCreateJob(args) {
  const key = makeCacheKey(args.type, args.videoId, args.sourceSrt);

  const cached = getCache(key);

  if (cached) {
    let job = findJobByCache(key, ["completed"]);

    if (!job) {
      job = createJob(args);
      job.status = "completed";
      job.progress = 100;
      job.result = cached;
    }

    return job;
  }

  const active = findJobByCache(key, ["processing"]);
  if (active) return active;

  const done = findJobByCache(key, ["completed"]);
  if (done) return done;

  const job = createJob(args);
  enqueue(job);

  return job;
}

setInterval(() => {
  const now = Date.now();

  for (const [key, item] of translationCache.entries()) {
    if (item.expiresAt <= now) {
      translationCache.delete(key);
    }
  }

  for (const [id, job] of jobs.entries()) {
    if (
      job.expiresAt <= now &&
      job.status !== "processing"
    ) {
      jobs.delete(id);
    }
  }
}, 10 * 60 * 1000).unref();

// ============================================================
// SRT CLEAN / PARSE
// ============================================================

const TIMING_RE =
  /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
  /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
  /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

function normalizeSpeaker(value) {
  const speaker = String(value || "")
    .replace(/<[^>]+>/g, " ")
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

function extractSpeaker(line) {
  const original = String(line || "");

  const hidden = original.match(SPEAKER_RE);

  if (hidden) {
    let speaker = "";

    try {
      speaker = normalizeSpeaker(
        decodeURIComponent(hidden[1])
      );
    } catch {}

    return {
      speaker,
      text: original.replace(SPEAKER_RE, "")
    };
  }

  const bracket = original.match(
    /^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*:?[ \t]*/u
  );

  if (bracket) {
    const speaker = normalizeSpeaker(bracket[1]);

    if (speaker) {
      return {
        speaker,
        text: original.slice(bracket[0].length)
      };
    }
  }

  const colon = original.match(
    /^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50})\s*:\s+(?=\S)/u
  );

  if (colon) {
    const speaker = normalizeSpeaker(colon[1]);

    if (speaker) {
      return {
        speaker,
        text: original.slice(colon[0].length)
      };
    }
  }

  return {
    speaker: "",
    text: original
  };
}

function normalizeElongations(text) {
  return String(text || "")
    .replace(
      /([A-Za-zÀ-ÖØ-öø-ÿ]+?)([-–—])([A-Za-zÀ-ÖØ-öø-ÿ])(?:\2\3){2,}/gu,
      "$1"
    )
    .replace(
      /([A-Za-zÀ-ÖØ-öø-ÿ])(?:[-–—]\1){2,}[-–—]?/giu,
      "$1"
    )
    .replace(
      /([aeiouáéíóúãõâêô])\1{3,}/giu,
      "$1"
    );
}

// uh-huh / uh-uh NÃO são removidos: podem significar sim/não.
function isEmptyVocalization(text) {
  const value = String(text || "")
    .toLowerCase()
    .replace(/[.,!?…]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return /^(?:ah|ha|heh)(?:\s+(?:ah|ha|heh)){1,5}$/.test(value);
}

function cleanSourceLine(line) {
  let text = String(line || "").trim();

  if (!text) return "";

  text = text.replace(
    /\s*\[[^\]]+\]\s*/gu,
    " "
  );

  text = text.replace(
    /\s*\(([^)]*)\)\s*/gu,
    (match, inside) =>
      SDH_WORDS.test(String(inside || ""))
        ? " "
        : match
  );

  text = normalizeElongations(
    text.replace(/[♪♫♬]/gu, " ")
  )
    .replace(/[ \t]{2,}/g, " ")
    .trim();

  if (
    !text ||
    /^[-–—/\s]*$/u.test(text) ||
    isEmptyVocalization(text)
  ) {
    return "";
  }

  return text;
}

function cleanSrtForTranslation(srt) {
  const normalized = normalizeSrt(srt);

  if (!normalized) return "";

  const rawBlocks =
    normalized
      .split(/\n{2,}/)
      .filter(Boolean);

  const out = [];

  let removed = 0;
  let speakerHints = 0;
  let vocalizations = 0;

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
      lines[timingIndex].trim();

    if (!TIMING_RE.test(timing)) {
      continue;
    }

    const dialogue = [];
    const speakers = new Set();

    for (
      const sourceLine
      of lines.slice(timingIndex + 1)
    ) {
      const info =
        extractSpeaker(sourceLine);

      if (info.speaker) {
        speakers.add(info.speaker);
      }

      const before =
        String(info.text || "").trim();

      const cleaned =
        cleanSourceLine(before);

      if (
        !cleaned &&
        isEmptyVocalization(before)
      ) {
        vocalizations++;
      }

      if (cleaned) {
        dialogue.push(cleaned);
      }
    }

    if (!dialogue.length) {
      removed++;
      continue;
    }

    if (speakers.size === 1) {
      const speaker =
        [...speakers][0];

      dialogue[0] =
        `@@SPK:${encodeURIComponent(speaker)}@@ ${dialogue[0]}`;

      speakerHints++;
    }

    out.push({
      timing,
      dialogue
    });
  }

  console.log(
    `[CLEAN] ${rawBlocks.length} -> ${out.length}; ` +
    `removidos=${removed}; ` +
    `speakerHints=${speakerHints}; ` +
    `vocalizações=${vocalizations}.`
  );

  if (!out.length) return "";

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
    const raw
    of normalized.split(/\n{2,}/)
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

    let speakerHint = null;

    if (textLines.length) {
      const match =
        textLines[0].match(SPEAKER_RE);

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

function buildSrt(
  blocks,
  texts
) {
  return (
    blocks
      .map(
        (block, index) =>
          [
            block.index,
            block.timing,
            texts[index] ??
              block.text
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
      `TIMING LOCK ${label}: ` +
      `${source.length}/${final.length}.`
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
        `TIMING LOCK ${label}: cue ${source[i].index}.`
      );
    }
  }

  console.log(
    `[AUDIT TIMESTAMP] ${label}: PASSOU — ` +
    `${source.length}/${source.length}; 0 alterações.`
  );
}

// ============================================================
// SENTENCE GROUPS
// ============================================================

function parseTimeSeconds(value) {
  const match =
    String(value || "").match(
      /^(\d{2}):(\d{2}):(\d{2})[,.](\d{3})$/
    );

  if (!match) {
    return NaN;
  }

  return (
    Number(match[1]) * 3600 +
    Number(match[2]) * 60 +
    Number(match[3]) +
    Number(match[4]) / 1000
  );
}

function timingParts(timing) {
  const match =
    String(timing || "").match(
      /^(\d{2}:\d{2}:\d{2}[,.]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[,.]\d{3})/
    );

  return match
    ? {
        start:
          parseTimeSeconds(
            match[1]
          ),

        end:
          parseTimeSeconds(
            match[2]
          )
      }
    : {
        start: NaN,
        end: NaN
      };
}

function groupingText(text) {
  return String(text || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\{\\[^}]+\}/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isMultiSpeaker(text) {
  const lines =
    String(text || "")
      .split("\n")
      .filter(
        line =>
          line.trim()
      );

  return (
    lines.length >= 2 &&
    lines.filter(
      line =>
        /^\s*[-–—]\s*\S/u.test(line)
    ).length >= 2
  );
}

function shouldMerge(
  group,
  next
) {
  if (
    !group.length ||
    group.length >= 4
  ) {
    return false;
  }

  const previous =
    group[group.length - 1];

  if (
    isMultiSpeaker(
      previous.text
    ) ||
    isMultiSpeaker(
      next.text
    )
  ) {
    return false;
  }

  if (
    previous.speakerHint &&
    next.speakerHint &&
    normalizeSpeaker(
      previous.speakerHint
    ).toLowerCase() !==
      normalizeSpeaker(
        next.speakerHint
      ).toLowerCase()
  ) {
    return false;
  }

  const a =
    timingParts(previous.timing);

  const b =
    timingParts(next.timing);

  if (
    Number.isFinite(a.end) &&
    Number.isFinite(b.start) &&
    b.start - a.end > 0.9
  ) {
    return false;
  }

  const nextText =
    groupingText(next.text)
      .replace(
        /^[-–—]\s*/u,
        ""
      )
      .replace(
        /^["'“‘(\[]+/u,
        ""
      );

  if (
    /^[a-zà-öø-ÿ]/u.test(
      nextText
    )
  ) {
    return true;
  }

  const previousText =
    groupingText(previous.text);

  if (
    /[,;:]$/u.test(
      previousText
    )
  ) {
    return true;
  }

  return (
    !/[.!?…]["'”’)\]}]*$/u.test(
      previousText
    ) &&
    /\b(?:the|to|of|or|with|for|in|at|from|that|who|which|about|into|as|than|while)\s*$/iu.test(
      previousText
    )
  );
}

function buildSentenceGroups(
  blocks
) {
  const groups = [];

  let current = [];

  const flush = () => {
    if (!current.length) {
      return;
    }

    groups.push({
      groupId:
        groups.length + 1,

      cues:
        current,

      multiSpeaker:
        current.some(
          cue =>
            isMultiSpeaker(
              cue.text
            )
        )
    });

    current = [];
  };

  for (const block of blocks) {
    if (
      !current.length ||
      shouldMerge(
        current,
        block
      )
    ) {
      current.push(block);
    } else {
      flush();
      current.push(block);
    }
  }

  flush();

  return groups;
}

function compactGroup(group) {
  return {
    g:
      group.groupId,

    cues:
      group.cues.map(
        cue => ({
          i:
            cue.index,

          en:
            cue.text,

          ...(
            cue.speakerHint
              ? {
                  speaker:
                    cue.speakerHint
                }
              : {}
          )
        })
      )
  };
}

function countCues(groups) {
  return groups.reduce(
    (sum, group) =>
      sum +
      group.cues.length,

    0
  );
}

function splitEvenly(
  items,
  parts
) {
  const out = [];

  let start = 0;

  for (
    let p = 0;
    p < parts;
    p++
  ) {
    const remaining =
      items.length - start;

    const remainingParts =
      parts - p;

    const size =
      Math.ceil(
        remaining /
        remainingParts
      );

    out.push(
      items.slice(
        start,
        start + size
      )
    );

    start += size;
  }

  return out.filter(
    batch =>
      batch.length
  );
}

function splitByBudget(
  items,
  maxChars,
  maxItems,
  builder
) {
  const batches = [];

  let current = [];
  let chars = 0;

  for (const item of items) {
    const size =
      JSON.stringify(
        builder(item)
      ).length +
      8;

    if (
      current.length &&
      (
        current.length >=
          maxItems ||
        chars + size >
          maxChars
      )
    ) {
      batches.push(current);

      current = [];
      chars = 0;
    }

    current.push(item);
    chars += size;
  }

  if (current.length) {
    batches.push(current);
  }

  return batches;
}

function withOverlap(
  allGroups,
  targetBatch
) {
  const firstId =
    targetBatch[0].groupId;

  const lastId =
    targetBatch[
      targetBatch.length - 1
    ].groupId;

  const firstIndex =
    allGroups.findIndex(
      group =>
        group.groupId ===
        firstId
    );

  const lastIndex =
    allGroups.findIndex(
      group =>
        group.groupId ===
        lastId
    );

  return {
    before:
      allGroups.slice(
        Math.max(
          0,
          firstIndex -
            MAIN_CONTEXT_OVERLAP_GROUPS
        ),

        firstIndex
      ).map(compactGroup),

    target:
      targetBatch.map(
        compactGroup
      ),

    after:
      allGroups.slice(
        lastIndex + 1,

        Math.min(
          allGroups.length,
          lastIndex +
            1 +
            MAIN_CONTEXT_OVERLAP_GROUPS
        )
      ).map(compactGroup)
  };
}

// ============================================================
// PROMPTS / STYLE PACK
// ============================================================

const CORE_STYLE = `
PORTUGUÊS BRASILEIRO 2026 — REGRAS EDITORIAIS OBRIGATÓRIAS

Objetivo: legenda natural, oral, atual, fiel, coerente e curta o suficiente para leitura.
Nunca traduza como "inglês vestido de português".
Evite literalidade, rigidez, lusitanismo, linguagem burocrática, gíria de tiozão ou internetês forçado.
Use Gen Z/Alpha apenas quando personagem, fandom e contexto pedirem.

DRAG / REALITY / LGBTQIA+ / POP / MODA / MÚSICA:

- bitch como vocativo amigável:
  bicha, gata, amiga, menina ou omitir.
  Nunca "puta" automaticamente.

- I'm gagged / gagged como reação:
  Tô passada / Tô muito passada / Tô em choque / Tô sem reação.
  Nunca "amordaçada".

- she ate:
  arrasou / entregou tudo / serviu demais,
  conforme contexto.

- no crumbs:
  não deixou nada pra ninguém,
  quando couber.

- fucking/motherfucking:
  preserve intensidade de forma brasileira natural.

  NUNCA:
  competição da porra,
  competição do caralho,
  lip sync da porra,
  lip sync do caralho,
  cheque da porra,
  cheque do caralho.

- fucking lip sync:
  um puta lip sync /
  um lip sync foda /
  um lip sync absurdo,
  conforme contexto.

- supportive:
  "sempre me apoiou muito",
  "sempre esteve do meu lado" etc.
  Evite "super apoiador".

- judges em Drag Race:
  jurados,
  não juízes.

- the judgers are now the judgees:
  "agora quem julgava vai ser julgado"
  ou
  "agora os jurados é que vão ser julgados".

- plucking pussy hairs:
  preserve corpo + vulgaridade;
  exemplo "catar pelo de xereca".
  Nunca "fio de bigode".

- double win/shared win:
  vitória dupla /
  as duas ganharam.
  Não "empate duplo" sem empate.

- week one:
  primeira semana.

- off the top sobre dinheiro:
  comissão/corte/porcentagem.

- closing ranks:
  grupo se protegendo/panelinha.

- Carry the two em conta:
  vai dois.

- evite "apoiante".

- The talent performers
  não é
  "As artistas do talento".

Preserve quando culturalmente reconhecíveis:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

PALAVRÃO:
não censure.
Preserve intensidade no lugar em que brasileiro realmente fala.

GÊNERO:
use speaker apenas como contexto oculto.
Se não houver segurança, reformule.

Nunca escreva:
empolgado(a)
animado(a)
ele/ela
ela/ele

FORMATO:
sem speaker labels,
[NOME],
NOME:,
barras como separador,
hífen/travessão decorativo,
SDH/CC
ou alongamentos gráficos.

CUE LOCK ABSOLUTO:

Cada cue tem um id "i".
Devolva exatamente um texto PT por id-alvo.

Não resuma.
Não omita fim de frase/raciocínio.
Não antecipe o cue seguinte.
Não empurre conteúdo para cue posterior.

Sentence Groups e contexto vizinho servem para compreender a frase,
NÃO para mover texto entre timestamps.
`;

const PLAN_PROMPT = `
Você é um editor de continuidade de legendas EN→PT-BR.

Leia o material do episódio e crie uma BÍBLIA EDITORIAL CURTA
para outra instância do mesmo modelo traduzir o episódio em lotes paralelos.

Extraia somente informações úteis para consistência:

- nomes;
- relações;
- gênero quando claro;
- termos recorrentes;
- catchphrases;
- referências de fandom;
- piadas/expressões repetidas;
- tom geral.

Não traduza o episódio.
Não invente informações.
`;

const TRANSLATOR_PROMPT = `
Você é o tradutor principal EN→PT-BR de legendas de entretenimento.

${CORE_STYLE}

Você receberá:

1) uma bíblia editorial global do episódio;
2) alguns groups ANTERIORES apenas como contexto;
3) os groups ALVO que precisam ser traduzidos;
4) alguns groups POSTERIORES apenas como contexto.

TRADUZA SOMENTE os cues dos groups ALVO.

Não devolva os cues de contexto.
Respeite exatamente os ids.
Use a bíblia global para manter coerência entre lotes paralelos.
`;

const AUDITOR_PROMPT = `
Você é auditor editorial EN→PT-BR.

${CORE_STYLE}

Compare EN × PT cue por cue.

Não marque erro por gosto pessoal.
Marque somente problema REAL.

Categorias:

SEMANTIC
OMISSION
CUE_SYNC
LITERAL
REGISTER
CULTURE
PROFANITY
GENDER
FORMAT
UNTRANSLATED

Verifique especialmente:

- fim de raciocínio omitido;
- conteúdo que migrou para cue vizinho;
- termos de Drag/reality traduzidos mecanicamente;
- bitch-vocativo → puta;
- competição/lip sync/cheque da porra/do caralho;
- supportive → super apoiador;
- judges → juízes em Drag Race;
- judgers/judgees literal;
- plucking pussy hairs → fio de bigode;
- shared/double win → empate duplo;
- gagged → amordaçada;
- português duro, velho ou artificial.

Para cada group:

v="ok"
ou
v="fix".

Em fix:
reasons curtos + hint objetivo.

Em ok:
reasons=[]
hint=""
`;

const DEEP_AUDITOR_PROMPT = `
Você é o REVISOR SÊNIOR de uma legenda EN→PT-BR.

Estes são os trechos de MAIOR RISCO do episódio.

${CORE_STYLE}

Faça comparação minuciosa EN × PT, cue por cue.

Priorize:

- sentido;
- naturalidade brasileira;
- referência cultural;
- vulgaridade correta;
- gênero;
- CUE_SYNC.

Se houver qualquer defeito concreto,
marque fix e explique exatamente o que deve ser corrigido.

Não aprove uma tradução só porque é gramatical:
ela precisa soar humana e adequada ao universo do programa.
`;

const REPAIR_PROMPT = `
Você é o editor final EN→PT-BR.

${CORE_STYLE}

Recebe apenas groups sinalizados com:

- EN original;
- PT atual;
- reasons;
- hint.

Corrija somente o necessário,
mas reescreva livremente quando a frase atual estiver literal/engessada.

Exatamente um pt para cada id recebido.
Mesmo id.
Sem omissão.
Sem migração entre cues.
`;

// ============================================================
// JSON SCHEMAS
// ============================================================

const PLAN_SCHEMA = {
  type: "object",

  additionalProperties:
    false,

  properties: {
    tone: {
      type:
        "string"
    },

    people: {
      type:
        "array",

      items: {
        type:
          "string"
      },

      maxItems:
        30
    },

    glossary: {
      type:
        "array",

      items: {
        type:
          "string"
      },

      maxItems:
        40
    },

    continuity: {
      type:
        "array",

      items: {
        type:
          "string"
      },

      maxItems:
        30
    },

    warnings: {
      type:
        "array",

      items: {
        type:
          "string"
      },

      maxItems:
        30
    }
  },

  required: [
    "tone",
    "people",
    "glossary",
    "continuity",
    "warnings"
  ]
};

function cueTranslationSchema(
  expectedCount
) {
  return {
    type:
      "object",

    additionalProperties:
      false,

    properties: {
      cues: {
        type:
          "array",

        minItems:
          expectedCount,

        maxItems:
          expectedCount,

        items: {
          type:
            "object",

          additionalProperties:
            false,

          properties: {
            i: {
              type:
                "integer"
            },

            pt: {
              type:
                "string"
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

function auditSchema(
  expectedCount
) {
  return {
    type:
      "object",

    additionalProperties:
      false,

    properties: {
      items: {
        type:
          "array",

        minItems:
          expectedCount,

        maxItems:
          expectedCount,

        items: {
          type:
            "object",

          additionalProperties:
            false,

          properties: {
            g: {
              type:
                "integer"
            },

            v: {
              type:
                "string",

              enum: [
                "ok",
                "fix"
              ]
            },

            reasons: {
              type:
                "array",

              items: {
                type:
                  "string"
              },

              maxItems:
                8
            },

            hint: {
              type:
                "string"
            }
          },

          required: [
            "g",
            "v",
            "reasons",
            "hint"
          ]
        }
      }
    },

    required: [
      "items"
    ]
  };
}

// ============================================================
// REAL RPM GATE
// somente o limite real de 15 RPM
// ============================================================

const requestStarts = [];

let rpmMutex =
  Promise.resolve();

async function acquireGeminiRpmSlot(
  job
) {
  let release;

  const previous =
    rpmMutex;

  rpmMutex =
    new Promise(
      resolve => {
        release =
          resolve;
      }
    );

  await previous;

  try {
    while (true) {
      const now =
        Date.now();

      while (
        requestStarts.length &&
        now -
          requestStarts[0] >=
          60000
      ) {
        requestStarts.shift();
      }

      if (
        requestStarts.length <
        GEMINI_RPM_LIMIT
      ) {
        requestStarts.push(
          Date.now()
        );

        return;
      }

      const wait =
        Math.max(
          50,

          60000 -
          (
            now -
            requestStarts[0]
          ) +
          50
        );

      if (job) {
        job.stats.rpmWaitMs +=
          wait;
      }

      console.log(
        `[GEMINI QUOTA] 15 RPM reais atingidos; ` +
        `aguardando ${(wait / 1000).toFixed(1)}s ` +
        `até abrir o próximo slot da própria API.`
      );

      await sleep(wait);
    }
  } finally {
    release();
  }
}

// ============================================================
// GEMINI INTERACTIONS API
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

  if (
    Number.isFinite(num) &&
    num > 0
  ) {
    return Math.max(
      1000,
      num * 1000
    );
  }

  return null;
}

function retryDelayMs(
  response,
  data,
  attempt
) {
  const header =
    parseDurationMs(
      response
        ?.headers
        ?.get(
          "retry-after"
        )
    );

  if (header) {
    return Math.min(
      90000,
      header + 250
    );
  }

  const details =
    Array.isArray(
      data?.error?.details
    )
      ? data.error.details
      : [];

  for (
    const detail
    of details
  ) {
    const parsed =
      parseDurationMs(
        detail?.retryDelay ||
        detail?.retry_delay ||
        detail
          ?.metadata
          ?.retryDelay ||
        detail
          ?.metadata
          ?.retry_delay
      );

    if (parsed) {
      return Math.min(
        90000,
        parsed + 250
      );
    }
  }

  return Math.min(
    5000 * attempt,
    30000
  );
}

function extractInteractionText(data) {
  const steps =
    Array.isArray(
      data?.steps
    )
      ? data.steps
      : [];

  let out = "";

  for (
    const step
    of steps
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

    const text =
      step.content
        .filter(
          part =>
            part?.type ===
              "text" &&
            typeof part?.text ===
              "string"
        )
        .map(
          part =>
            part.text
        )
        .join("");

    if (text) {
      out += text;
    }
  }

  return out.trim();
}

function markAttempt(
  job,
  metric
) {
  if (!job) return;

  if (
    metric ===
    "main"
  ) {
    job.stats.mainAttempts++;
  }

  if (
    metric ===
      "audit" ||
    metric ===
      "deep"
  ) {
    job.stats.auditAttempts++;
  }

  if (
    metric ===
    "repair"
  ) {
    job.stats.repairAttempts++;
  }
}

function mark429(
  job,
  metric
) {
  if (!job) return;

  if (
    metric ===
    "main"
  ) {
    job.stats.main429++;
  }

  if (
    metric ===
      "audit" ||
    metric ===
      "deep"
  ) {
    job.stats.audit429++;
  }

  if (
    metric ===
    "repair"
  ) {
    job.stats.repair429++;
  }
}

function markSuccess(
  job,
  metric,
  data
) {
  if (!job) return;

  if (
    metric ===
    "plan"
  ) {
    job.stats.planCalls++;
  }

  if (
    metric ===
    "main"
  ) {
    job.stats.mainCalls++;
  }

  if (
    metric ===
    "audit"
  ) {
    job.stats.auditCalls++;
  }

  if (
    metric ===
    "deep"
  ) {
    job.stats.auditCalls++;
    job.stats.deepAuditCalls++;
  }

  if (
    metric ===
    "repair"
  ) {
    job.stats.repairCalls++;
  }

  job.stats.inputTokens +=
    Number(
      data
        ?.usage
        ?.total_input_tokens ||
      0
    );

  job.stats.outputTokens +=
    Number(
      data
        ?.usage
        ?.total_output_tokens ||
      0
    );

  job.stats.thoughtTokens +=
    Number(
      data
        ?.usage
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

    await acquireGeminiRpmSlot(
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
        `[GEMINI ${metric.toUpperCase()}] ` +
        `${GEMINI_MODEL} request ` +
        `${attempt}/${maxRetries} | ` +
        `thinking=${thinkingLevel} | ` +
        `maxOutput=${maxOutputTokens}.`
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

                store:
                  false
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
            "unknown"
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
          const inner =
            data?.error?.message ||
            data?.message ||
            "interaction sem detalhe";

          const e =
            new Error(
              `Gemini ${metric} ` +
              `status=${status}: ` +
              `${String(inner).slice(0, 1200)}`
            );

          e.nonRetryable =
            status ===
            "budget_exceeded";

          throw e;
        }

        if (!text) {
          const e =
            new Error(
              `Gemini ${metric} retornou resposta vazia; ` +
              `status=${status}.`
            );

          e.interactionStatus =
            status;

          throw e;
        }

        markSuccess(
          job,
          metric,
          data
        );

        console.log(
          `[GEMINI ${metric.toUpperCase()}] OK | ` +
          `status=${status.toUpperCase()} | ` +
          `input=${Number(
            data
              ?.usage
              ?.total_input_tokens ||
            0
          )} | ` +
          `output=${Number(
            data
              ?.usage
              ?.total_output_tokens ||
            0
          )} | ` +
          `thought=${Number(
            data
              ?.usage
              ?.total_thought_tokens ||
            0
          )} | ` +
          `chars=${text.length}.`
        );

        return {
          text,
          status,
          usage:
            data?.usage ||
            {}
        };
      }

      const message =
        data
          ?.error
          ?.message ||
        data
          ?.message ||
        raw ||
        `HTTP ${response.status}`;

      const error =
        new Error(
          `GEMINI ${GEMINI_MODEL} ` +
          `HTTP ${response.status}: ` +
          `${String(message).slice(0, 1800)}`
        );

      error.status =
        response.status;

      if (
        response.status ===
        429
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
          `[GEMINI ${metric.toUpperCase()}] ` +
          `429; retry em ${(wait / 1000).toFixed(1)}s.`
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
        response.status >=
          500;

      if (
        !retryable ||
        attempt ===
          maxRetries
      ) {
        throw error;
      }

      const wait =
        Math.min(
          2000 * attempt,
          10000
        );

      console.warn(
        `[GEMINI ${metric.toUpperCase()}] ` +
        `HTTP ${response.status}; ` +
        `retry em ${(wait / 1000).toFixed(1)}s.`
      );

      await sleep(wait);
    } catch (error) {
      lastError =
        error?.name ===
          "AbortError"
          ? new Error(
              `GEMINI ${GEMINI_MODEL} ${metric}: ` +
              `timeout desta request.`
            )
          : error;

      if (
        lastError?.status ===
        429
      ) {
        if (
          attempt ===
          maxRetries
        ) {
          throw lastError;
        }

        continue;
      }

      if (
        lastError?.nonRetryable
      ) {
        throw lastError;
      }

      if (
        lastError?.status &&
        lastError.status <
          500 &&
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
        attempt ===
        maxRetries
      ) {
        throw lastError;
      }

      const wait =
        Math.min(
          2000 * attempt,
          10000
        );

      console.warn(
        `[GEMINI ${metric.toUpperCase()}] ` +
        `${errorMessage(lastError).slice(0, 220)}; ` +
        `retry em ${(wait / 1000).toFixed(1)}s.`
      );

      await sleep(wait);
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

// ============================================================
// CONTEXTO GLOBAL
// ============================================================

async function buildEpisodePlan(
  groups,
  job
) {
  const compact =
    groups.map(
      compactGroup
    );

  try {
    const response =
      await geminiRequest({
        system:
          PLAN_PROMPT,

        user:
          `Arquivo: ${job.filename || "desconhecido"}\n\n` +
          `Crie uma bíblia editorial CURTA para este episódio.\n\n` +
          JSON.stringify({
            groups:
              compact
          }),

        schema:
          PLAN_SCHEMA,

        thinkingLevel:
          PLAN_THINKING,

        maxOutputTokens:
          PLAN_MAX_OUTPUT_TOKENS,

        timeoutMs:
          PLAN_TIMEOUT_MS,

        maxRetries:
          2,

        job,

        metric:
          "plan"
      });

    const plan =
      JSON.parse(
        stripCodeFences(
          response.text
        )
      );

    console.log(
      `[EPISODE PLAN] OK | ` +
      `people=${plan.people?.length || 0} | ` +
      `glossary=${plan.glossary?.length || 0} | ` +
      `continuity=${plan.continuity?.length || 0}.`
    );

    return plan;
  } catch (error) {
    console.warn(
      `[EPISODE PLAN] Falhou, seguindo sem bloquear: ` +
      `${errorMessage(error).slice(0, 220)}`
    );

    return {
      tone:
        "Natural spoken Brazilian Portuguese; preserve show/fandom register.",

      people:
        [],

      glossary:
        [],

      continuity:
        [],

      warnings:
        []
    };
  }
}

// ============================================================
// MAIN TRANSLATION
// ============================================================

function parseCueTranslation(
  groups,
  raw
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
    return {
      groupMap:
        new Map(),

      invalidGroups:
        groups.slice(),

      issue:
        "JSON_INVALID"
    };
  }

  const items =
    Array.isArray(
      parsed?.cues
    )
      ? parsed.cues
      : [];

  const expectedIds =
    new Set(
      groups.flatMap(
        group =>
          group.cues.map(
            cue =>
              cue.index
          )
      )
    );

  const byId =
    new Map();

  const duplicates =
    new Set();

  for (
    const item
    of items
  ) {
    const id =
      Number(
        item?.i
      );

    const pt =
      String(
        item?.pt ||
        ""
      ).trim();

    if (
      !expectedIds.has(id) ||
      !pt
    ) {
      continue;
    }

    if (
      byId.has(id)
    ) {
      duplicates.add(id);

      continue;
    }

    byId.set(
      id,
      pt
    );
  }

  const groupMap =
    new Map();

  const invalidGroups =
    [];

  for (
    const group
    of groups
  ) {
    const segments =
      [];

    let valid =
      true;

    for (
      const cue
      of group.cues
    ) {
      if (
        !byId.has(
          cue.index
        ) ||
        duplicates.has(
          cue.index
        )
      ) {
        valid =
          false;

        break;
      }

      segments.push(
        byId.get(
          cue.index
        )
      );
    }

    if (valid) {
      groupMap.set(
        group.groupId,
        segments
      );
    } else {
      invalidGroups.push(
        group
      );
    }
  }

  return {
    groupMap,

    invalidGroups,

    issue:
      invalidGroups.length
        ? `INCOMPLETE:${groupMap.size}/${groups.length}`
        : null
  };
}

async function translateBatchResilient({
  allGroups,
  targetGroups,
  plan,
  job,
  depth = 0
}) {
  const expectedCues =
    countCues(
      targetGroups
    );

  const contextual =
    withOverlap(
      allGroups,
      targetGroups
    );

  let response;

  try {
    response =
      await geminiRequest({
        system:
          TRANSLATOR_PROMPT,

        user:
          `BÍBLIA GLOBAL:\n${JSON.stringify(plan)}\n\n` +
          `CONTEXTO E ALVOS:\n${JSON.stringify(contextual)}\n\n` +
          `Traduza SOMENTE target. ` +
          `O JSON deve conter exatamente ${expectedCues} cues-alvo.`,

        schema:
          cueTranslationSchema(
            expectedCues
          ),

        thinkingLevel:
          MAIN_THINKING,

        maxOutputTokens:
          MAIN_MAX_OUTPUT_TOKENS,

        timeoutMs:
          MAIN_TIMEOUT_MS,

        maxRetries:
          MAIN_RETRIES,

        job,

        metric:
          "main"
      });
  } catch (error) {
    const msg =
      errorMessage(error)
        .toLowerCase();

    const splittable =
      targetGroups.length >
        40 &&
      depth <
        5 &&
      (
        /timeout|response vazia|invalid argument|schema|too large|token|incomplete/.test(
          msg
        ) ||
        error?.status ===
          400 ||
        error?.status >=
          500
      );

    if (!splittable) {
      throw error;
    }

    job.stats.mainSplits++;

    const middle =
      Math.ceil(
        targetGroups.length /
        2
      );

    console.warn(
      `[GEMINI MAIN SPLIT] ` +
      `${targetGroups.length} -> ` +
      `${middle}+${targetGroups.length - middle}.`
    );

    const [
      left,
      right
    ] =
      await Promise.all([
        translateBatchResilient({
          allGroups,

          targetGroups:
            targetGroups.slice(
              0,
              middle
            ),

          plan,
          job,

          depth:
            depth + 1
        }),

        translateBatchResilient({
          allGroups,

          targetGroups:
            targetGroups.slice(
              middle
            ),

          plan,
          job,

          depth:
            depth + 1
        })
      ]);

    return new Map([
      ...left,
      ...right
    ]);
  }

  const parsed =
    parseCueTranslation(
      targetGroups,
      response.text
    );

  const incomplete =
    response.status ===
    "incomplete";

  if (
    !parsed.invalidGroups.length &&
    !incomplete
  ) {
    return parsed.groupMap;
  }

  if (
    (
      incomplete ||
      parsed.issue ===
        "JSON_INVALID"
    ) &&
    targetGroups.length >
      40 &&
    depth <
      5
  ) {
    job.stats.mainSplits++;

    const middle =
      Math.ceil(
        targetGroups.length /
        2
      );

    console.warn(
      `[GEMINI MAIN SPLIT] ` +
      `${targetGroups.length} -> ` +
      `${middle}+${targetGroups.length - middle}; ` +
      `motivo=${incomplete ? "INCOMPLETE" : parsed.issue}.`
    );

    const [
      left,
      right
    ] =
      await Promise.all([
        translateBatchResilient({
          allGroups,

          targetGroups:
            targetGroups.slice(
              0,
              middle
            ),

          plan,
          job,

          depth:
            depth + 1
        }),

        translateBatchResilient({
          allGroups,

          targetGroups:
            targetGroups.slice(
              middle
            ),

          plan,
          job,

          depth:
            depth + 1
        })
      ]);

    return new Map([
      ...left,
      ...right
    ]);
  }

  if (
    parsed.invalidGroups.length
  ) {
    if (
      depth >=
      6
    ) {
      throw new Error(
        `Gemini não preservou estrutura após resgates: ` +
        `${parsed.invalidGroups.length} group(s).`
      );
    }

    job.stats.mainRescueGroups +=
      parsed.invalidGroups.length;

    console.warn(
      `[GEMINI MAIN RESCUE] ` +
      `válidos=${parsed.groupMap.size}/${targetGroups.length}; ` +
      `refazendo somente ${parsed.invalidGroups.length} group(s).`
    );

    const rescued =
      await translateBatchResilient({
        allGroups,

        targetGroups:
          parsed.invalidGroups,

        plan,
        job,

        depth:
          depth + 1
      });

    return new Map([
      ...parsed.groupMap,
      ...rescued
    ]);
  }

  return parsed.groupMap;
}

async function translateMainParallel(
  groups,
  plan,
  job
) {
  const batches =
    splitEvenly(
      groups,

      Math.min(
        MAIN_PARALLEL_BATCHES,
        groups.length
      )
    );

  console.log(
    `[GEMINI MAIN] ${groups.length} groups -> ` +
    `${batches.length} lote(s) PARALELOS ` +
    `(~${batches.map(batch => batch.length).join("/")} groups).`
  );

  const results =
    await Promise.all(
      batches.map(
        (
          batch,
          index
        ) => {
          console.log(
            `[GEMINI MAIN] Iniciando lote ` +
            `${index + 1}/${batches.length}: ` +
            `${batch.length} group(s).`
          );

          return translateBatchResilient({
            allGroups:
              groups,

            targetGroups:
              batch,

            plan,
            job
          });
        }
      )
    );

  const merged =
    new Map(
      results.flatMap(
        map =>
          [...map.entries()]
      )
    );

  if (
    merged.size !==
    groups.length
  ) {
    throw new Error(
      `Tradução principal incompleta ` +
      `${merged.size}/${groups.length}.`
    );
  }

  return merged;
}

// ============================================================
// CLEAN / LOCAL GUARDS
// ============================================================

function cleanFinalText(text) {
  let value =
    normalizeElongations(
      String(text || "")
        .replace(
          /\r\n/g,
          "\n"
        )
        .replace(
          /\r/g,
          "\n"
        )
    );

  value =
    value.replace(
      /\s+\/\s+(?=\S)/g,
      "\n"
    );

  return value
    .split("\n")
    .map(
      line =>
        String(line || "")
          .replace(
            /^\s*\[[^\]]{1,60}\]\s*:?[ \t]*/u,
            ""
          )
          .replace(
            /^\s*[A-ZÀ-Ý][\p{L}0-9.'’_-]*(?:\s+[A-ZÀ-Ý][\p{L}0-9.'’_-]*){0,3}\s*:\s+(?=\S)/u,
            ""
          )
          .replace(
            /^\s*[-–—/]+\s*(?=\S)/u,
            ""
          )
          .replace(
            /\s*--+\s*$/u,
            "…"
          )
          .replace(
            /\s*--+\s*/gu,
            "… "
          )
          .replace(
            /[♪♫♬]+/gu,
            ""
          )
          .replace(
            /[ \t]{2,}/g,
            " "
          )
          .trim()
    )
    .filter(Boolean)
    .join("\n")
    .trim();
}

function applySourceAwareHardFix(
  source,
  target,
  filename = ""
) {
  const en =
    String(source || "");

  let pt =
    String(target || "");

  const dragContext =
    /rupaul|drag[ ._-]*race/i.test(
      filename
    ) ||
    /\bwerkroom\b|\blip sync\b|\bshantay\b|\bsashay\b/i.test(
      en
    );

  pt =
    pt
      .replace(
        /\buma alçapão\b/gi,
        "um alçapão"
      )
      .replace(
        /\bcabina de votação\b/gi,
        "cabine de votação"
      )
      .replace(
        /\bse eu manter\b/gi,
        "se eu mantiver"
      )
      .replace(
        /\bbanheiro das (?:moças|damas)\b/gi,
        "banheiro feminino"
      );

  if (
    /\bWerkroom\b/i.test(en)
  ) {
    pt =
      pt
        .replace(
          /\bworkroom\b/gi,
          "Werkroom"
        )
        .replace(
          /\bwerkroom\b/g,
          "Werkroom"
        );
  }

  if (
    /\bCondragulations\b/i.test(
      en
    )
  ) {
    pt =
      pt.replace(
        /\bcondragulations\b/gi,
        "Condragulations"
      );
  }

  if (
    /^\s*bitch[,!]/i.test(en) &&
    /^\s*puta[,!]/i.test(pt)
  ) {
    pt =
      pt.replace(
        /^\s*puta([,!])/i,
        "Bicha$1"
      );
  }

  if (
    /\bgagged\b/i.test(en) &&
    /amordaçad/i.test(pt)
  ) {
    pt =
      pt.replace(
        /(?:t[oô]\s+)?amordaçad[oa]s?/gi,
        "tô passada"
      );
  }

  if (
    /\b(?:double|shared)\s+win\b/i.test(
      en
    ) &&
    /\bempate duplo\b/i.test(
      pt
    )
  ) {
    pt =
      pt.replace(
        /\bempate duplo\b/gi,
        "vitória dupla"
      );
  }

  if (
    /\bplucking\s+pussy\s+hairs?\b|\bpussy\s+hairs?\b/i.test(
      en
    ) &&
    /\bfio(?:s)? de bigode\b/i.test(
      pt
    )
  ) {
    pt =
      pt.replace(
        /\bfio(?:s)? de bigode\b/gi,
        "pelo de xereca"
      );
  }

  if (
    /\bthe judgers are now the judgees\b/i.test(
      en
    )
  ) {
    if (
      /ju[ií]zes?.*julgad/i.test(
        pt
      )
    ) {
      pt =
        "Agora quem julgava vai ser julgado.";
    }
  }

  if (
    dragContext &&
    /\bjudges?\b/i.test(en)
  ) {
    pt =
      pt
        .replace(
          /\bjuízes\b/gi,
          "jurados"
        )
        .replace(
          /\bjuiz\b/gi,
          "jurado"
        );
  }

  if (
    /\blip sync\b/i.test(en)
  ) {
    pt =
      pt
        .replace(
          /\bum lip sync da porra\b/gi,
          "um puta lip sync"
        )
        .replace(
          /\blip sync da porra\b/gi,
          "lip sync foda"
        )
        .replace(
          /\bum lip sync do caralho\b/gi,
          "um puta lip sync"
        )
        .replace(
          /\blip sync do caralho\b/gi,
          "lip sync foda"
        );
  }

  if (
    /\bgame[- ]playing girls\b/i.test(
      en
    ) &&
    /\bmotherfucking competition\b/i.test(
      en
    )
  ) {
    if (
      /competição (?:da porra|do caralho)/i.test(
        pt
      )
    ) {
      pt =
        "Tem umas meninas jogando sujo pra caralho nessa competição.";
    }
  }

  return pt;
}

function wordCount(text) {
  return (
    String(text || "")
      .match(
        /[\p{L}\p{N}]+(?:['’][\p{L}\p{N}]+)?/gu
      ) ||
    []
  ).length;
}

function normalizedWordSet(text) {
  return new Set(
    (
      String(text || "")
        .toLowerCase()
        .match(
          /[a-zà-ÿ0-9]+/g
        ) ||
      []
    ).filter(
      word =>
        word.length > 2
    )
  );
}

function copiedEnglishRatio(
  en,
  pt
) {
  const enWords =
    normalizedWordSet(en);

  const ptWords =
    [
      ...normalizedWordSet(pt)
    ];

  if (
    ptWords.length < 6 ||
    enWords.size < 6
  ) {
    return 0;
  }

  const copied =
    ptWords.filter(
      word =>
        enWords.has(word)
    ).length;

  return (
    copied /
    ptWords.length
  );
}

function knownIssuesForGroup(
  group,
  segments,
  filename = ""
) {
  const english =
    group.cues
      .map(
        cue =>
          cue.text
      )
      .join(" ");

  const pt =
    segments.join(" ");

  const reasons =
    new Set();

  const dragContext =
    /rupaul|drag[ ._-]*race/i.test(
      filename
    ) ||
    /\bwerkroom\b|\blip sync\b|\bshantay\b|\bsashay\b/i.test(
      english
    );

  if (
    /\b(?:competição|competicao) (?:da porra|do caralho)\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "LITERAL_PROFANITY"
    );
  }

  if (
    /\blip sync (?:da porra|do caralho)\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "LITERAL_PROFANITY"
    );
  }

  if (
    /\bcheque (?:da porra|do caralho)\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "LITERAL_PROFANITY"
    );
  }

  if (
    /\bsuper apoiador(?:a)?\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "SUPPORTIVE_LITERAL"
    );
  }

  if (
    /\bapoiante\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "PORTUGALISM"
    );
  }

  if (
    /\bju[ií]zes?\s+viraram\s+os\s+julgados\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "JUDGERS_LITERAL"
    );
  }

  if (
    /\bfio(?:s)? de bigode\b/i.test(
      pt
    ) &&
    /pussy\s+hairs?|pubic\s+hairs?/i.test(
      english
    )
  ) {
    reasons.add(
      "SEMANTIC_BODY_PART"
    );
  }

  if (
    /\bempate duplo\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "DOUBLE_TIE"
    );
  }

  if (
    /^\s*puta[,!]/i.test(
      pt
    ) &&
    /^\s*bitch[,!]/i.test(
      english
    )
  ) {
    reasons.add(
      "BITCH_VOCATIVE"
    );
  }

  if (
    /\bamordaçad/i.test(
      pt
    ) &&
    /\bgagged\b/i.test(
      english
    )
  ) {
    reasons.add(
      "GAG_LITERAL"
    );
  }

  if (
    /\bas artistas do talento\b/i.test(
      pt
    ) &&
    /talent performers?/i.test(
      english
    )
  ) {
    reasons.add(
      "TALENT_LITERAL"
    );
  }

  if (
    /\bsemana um\b/i.test(
      pt
    ) &&
    /\bweek one\b/i.test(
      english
    )
  ) {
    reasons.add(
      "WEEK_ONE_LITERAL"
    );
  }

  if (
    /\bfechando fileiras\b/i.test(
      pt
    ) &&
    /\bclosing ranks\b/i.test(
      english
    )
  ) {
    reasons.add(
      "CLOSING_RANKS_LITERAL"
    );
  }

  if (
    /\s\/\s/u.test(pt) ||
    /--+/u.test(pt) ||
    /^\s*[-–—/]\s*\S/mu.test(
      pt
    )
  ) {
    reasons.add(
      "FORMAT"
    );
  }

  if (
    /empolgado\(a\)|empolgada\(o\)|animado\(a\)|animada\(o\)|\bele\/ela\b|\bela\/ele\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "GENDER_FORMAT"
    );
  }

  if (
    dragContext &&
    /\bjudges?\b/i.test(
      english
    ) &&
    /\bju[ií]zes?\b/i.test(
      pt
    )
  ) {
    reasons.add(
      "DRAG_JUDGES"
    );
  }

  const enWords =
    wordCount(
      english
    );

  const ptWords =
    wordCount(
      pt
    );

  if (
    enWords >= 12 &&
    ptWords <=
      Math.max(
        2,
        Math.floor(
          enWords *
          0.30
        )
      )
  ) {
    reasons.add(
      "POSSIBLE_OMISSION"
    );
  }

  if (
    english.length >= 80 &&
    pt.length <=
      Math.floor(
        english.length *
        0.27
      )
  ) {
    reasons.add(
      "POSSIBLE_OMISSION"
    );
  }

  if (
    copiedEnglishRatio(
      english,
      pt
    ) >=
    0.68
  ) {
    reasons.add(
      "POSSIBLE_UNTRANSLATED"
    );
  }

  return [
    ...reasons
  ];
}

function riskScore(
  group,
  translations,
  filename = ""
) {
  const en =
    group.cues
      .map(
        cue =>
          cue.text
      )
      .join(" ");

  const pt =
    (
      translations.get(
        group.groupId
      ) ||
      []
    ).join(" ");

  let score = 0;

  const hot =
    /\bbitch\b|\bgagged\b|\bshe ate\b|no crumbs|motherfucking|\bfucking\b|lip sync|\bjudges?\b|judgers|judgees|supportive|pussy hairs?|double win|shared win|Werkroom|Condragulations|Shantay|Sashay|closing ranks|carry the two|off the top/i;

  if (
    hot.test(en)
  ) {
    score += 8;
  }

  if (
    group.cues.length >= 3
  ) {
    score += 3;
  }

  if (
    en.length > 180
  ) {
    score += 3;
  }

  if (
    group.multiSpeaker
  ) {
    score += 3;
  }

  const issues =
    knownIssuesForGroup(
      group,

      translations.get(
        group.groupId
      ),

      filename
    );

  score +=
    issues.length * 10;

  if (
    copiedEnglishRatio(
      en,
      pt
    ) >
    0.45
  ) {
    score += 5;
  }

  return score;
}

function addIssue(
  issueMap,
  groupId,
  reasons,
  hint = ""
) {
  if (
    !reasons?.length
  ) {
    return;
  }

  const current =
    issueMap.get(
      groupId
    ) ||
    {
      reasons:
        new Set(),

      hints:
        []
    };

  for (
    const reason
    of reasons
  ) {
    current
      .reasons
      .add(
        String(reason)
      );
  }

  if (hint) {
    current
      .hints
      .push(
        String(hint)
      );
  }

  issueMap.set(
    groupId,
    current
  );
}

function deterministicIssueMap(
  groups,
  translations,
  job
) {
  const out =
    new Map();

  for (
    const group
    of groups
  ) {
    const reasons =
      knownIssuesForGroup(
        group,

        translations.get(
          group.groupId
        ),

        job.filename
      );

    if (
      !reasons.length
    ) {
      continue;
    }

    job.stats.localStyleFlags +=
      reasons.length;

    if (
      reasons.includes(
        "POSSIBLE_OMISSION"
      )
    ) {
      job.stats.omissionFlags++;
    }

    addIssue(
      out,

      group.groupId,

      reasons,

      "Corrija o defeito sem perder naturalidade, conteúdo nem alinhamento cue a cue."
    );
  }

  return out;
}

function mergeIssueMaps(
  target,
  source
) {
  for (
    const [
      groupId,
      item
    ]
    of source
  ) {
    addIssue(
      target,

      groupId,

      item.reasons instanceof
        Set
        ? [
            ...item.reasons
          ]
        : item.reasons,

      item.hint ||
      (
        item.hints ||
        []
      ).join(" | ")
    );
  }

  return target;
}

function flattenTranslations(
  blocks,
  groups,
  translations
) {
  const positions =
    new Map(
      blocks.map(
        (
          block,
          index
        ) => [
          block.index,
          index
        ]
      )
    );

  const texts =
    new Array(
      blocks.length
    );

  for (
    const group
    of groups
  ) {
    const segments =
      translations.get(
        group.groupId
      );

    if (
      !segments ||
      segments.length !==
        group.cues.length
    ) {
      throw new Error(
        `Flatten inválido g=${group.groupId}.`
      );
    }

    group.cues.forEach(
      (
        cue,
        index
      ) => {
        texts[
          positions.get(
            cue.index
          )
        ] =
          segments[index];
      }
    );
  }

  if (
    texts.some(
      value =>
        typeof value !==
          "string" ||
        !value.trim()
    )
  ) {
    throw new Error(
      "Cue ausente/vazio no flatten."
    );
  }

  return texts;
}

function cleanAll(
  blocks,
  texts,
  filename
) {
  return texts.map(
    (
      text,
      index
    ) =>
      applySourceAwareHardFix(
        blocks[index].text,

        cleanFinalText(
          text
        ),

        filename
      )
  );
}

function writeCleanBack(
  blocks,
  groups,
  translations,
  cleanedTexts
) {
  const positions =
    new Map(
      blocks.map(
        (
          block,
          index
        ) => [
          block.index,
          index
        ]
      )
    );

  for (
    const group
    of groups
  ) {
    translations.set(
      group.groupId,

      group.cues.map(
        cue =>
          cleanedTexts[
            positions.get(
              cue.index
            )
          ]
      )
    );
  }
}

// ============================================================
// AUDIT
// ============================================================

function reviewPayload(
  group,
  translations
) {
  const pt =
    translations.get(
      group.groupId
    );

  return {
    g:
      group.groupId,

    cues:
      group.cues.map(
        (
          cue,
          index
        ) => ({
          i:
            cue.index,

          en:
            cue.text,

          pt:
            pt[index],

          ...(
            cue.speakerHint
              ? {
                  speaker:
                    cue.speakerHint
                }
              : {}
          )
        })
      )
  };
}

function parseAudit(
  groups,
  raw
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
      "Audit JSON inválido."
    );
  }

  if (
    !Array.isArray(
      parsed?.items
    )
  ) {
    throw new Error(
      "Audit sem items."
    );
  }

  const expected =
    new Set(
      groups.map(
        group =>
          group.groupId
      )
    );

  const seen =
    new Set();

  const flags =
    new Map();

  for (
    const item
    of parsed.items
  ) {
    const groupId =
      Number(
        item?.g
      );

    if (
      !expected.has(
        groupId
      ) ||
      seen.has(
        groupId
      )
    ) {
      continue;
    }

    const verdict =
      String(
        item?.v ||
        ""
      ).toLowerCase();

    if (
      ![
        "ok",
        "fix"
      ].includes(
        verdict
      )
    ) {
      continue;
    }

    seen.add(
      groupId
    );

    if (
      verdict ===
      "fix"
    ) {
      flags.set(
        groupId,

        {
          reasons:
            Array.isArray(
              item?.reasons
            )
              ? item.reasons
                  .map(
                    value =>
                      String(
                        value
                      ).toUpperCase()
                  )
                  .slice(
                    0,
                    8
                  )
              : [
                  "REVIEW"
                ],

          hint:
            String(
              item?.hint ||
              ""
            ).slice(
              0,
              450
            )
        }
      );
    }
  }

  if (
    seen.size !==
    groups.length
  ) {
    throw new Error(
      `Audit incompleto ` +
      `${seen.size}/${groups.length}.`
    );
  }

  return flags;
}

async function auditBatchResilient(
  groups,
  translations,
  plan,
  job,
  deep = false,
  depth = 0
) {
  try {
    const response =
      await geminiRequest({
        system:
          deep
            ? DEEP_AUDITOR_PROMPT
            : AUDITOR_PROMPT,

        user:
          `BÍBLIA GLOBAL:\n${JSON.stringify(plan)}\n\n` +
          `Audite exatamente ${groups.length} groups ` +
          `e devolva um item por group.\n\n` +
          JSON.stringify({
            groups:
              groups.map(
                group =>
                  reviewPayload(
                    group,
                    translations
                  )
              )
          }),

        schema:
          auditSchema(
            groups.length
          ),

        thinkingLevel:
          deep
            ? DEEP_AUDIT_THINKING
            : AUDIT_THINKING,

        maxOutputTokens:
          deep
            ? DEEP_AUDIT_MAX_OUTPUT_TOKENS
            : AUDIT_MAX_OUTPUT_TOKENS,

        timeoutMs:
          AUDIT_TIMEOUT_MS,

        maxRetries:
          AUDIT_RETRIES,

        job,

        metric:
          deep
            ? "deep"
            : "audit"
      });

    return parseAudit(
      groups,
      response.text
    );
  } catch (error) {
    if (
      groups.length > 30 &&
      depth < 3
    ) {
      const middle =
        Math.ceil(
          groups.length /
          2
        );

      console.warn(
        `[GEMINI AUDIT SPLIT] ` +
        `${groups.length} -> ` +
        `${middle}+${groups.length - middle}.`
      );

      const [
        left,
        right
      ] =
        await Promise.all([
          auditBatchResilient(
            groups.slice(
              0,
              middle
            ),

            translations,
            plan,
            job,
            deep,
            depth + 1
          ),

          auditBatchResilient(
            groups.slice(
              middle
            ),

            translations,
            plan,
            job,
            deep,
            depth + 1
          )
        ]);

      return new Map([
        ...left,
        ...right
      ]);
    }

    throw error;
  }
}

async function mapWithConcurrency(
  items,
  concurrency,
  worker
) {
  const results =
    new Array(
      items.length
    );

  let cursor = 0;

  async function runner() {
    while (true) {
      const index =
        cursor++;

      if (
        index >=
        items.length
      ) {
        return;
      }

      results[index] =
        await worker(
          items[index],
          index
        );
    }
  }

  await Promise.all(
    Array.from(
      {
        length:
          Math.min(
            concurrency,
            items.length
          )
      },

      () =>
        runner()
    )
  );

  return results;
}

async function auditAllGroups(
  groups,
  translations,
  plan,
  job
) {
  const batches =
    splitByBudget(
      groups,

      AUDIT_BATCH_CHARS,

      AUDIT_BATCH_GROUPS,

      group =>
        reviewPayload(
          group,
          translations
        )
    );

  console.log(
    `[GEMINI AUDIT] ${groups.length} groups -> ` +
    `${batches.length} lote(s), ` +
    `concorrência=${AUDIT_CONCURRENCY}.`
  );

  const maps =
    await mapWithConcurrency(
      batches,

      AUDIT_CONCURRENCY,

      async batch => {
        const flags =
          await auditBatchResilient(
            batch,
            translations,
            plan,
            job,
            false
          );

        job.stats.auditPrimaryGroups +=
          batch.length;

        job.stats.auditFlagged +=
          flags.size;

        console.log(
          `[GEMINI AUDIT] ${batch.length} revisados; ` +
          `${flags.size} marcado(s).`
        );

        return flags;
      }
    );

  return new Map(
    maps.flatMap(
      map =>
        [...map.entries()]
    )
  );
}

async function deepAuditHighRisk(
  groups,
  translations,
  plan,
  job
) {
  const ranked =
    groups
      .map(
        group => ({
          group,

          score:
            riskScore(
              group,
              translations,
              job.filename
            )
        })
      )
      .filter(
        item =>
          item.score > 0
      )
      .sort(
        (
          a,
          b
        ) =>
          b.score -
          a.score
      )
      .slice(
        0,
        DEEP_AUDIT_MAX_GROUPS
      )
      .map(
        item =>
          item.group
      );

  if (
    !ranked.length
  ) {
    return new Map();
  }

  console.log(
    `[GEMINI DEEP AUDIT] ` +
    `${ranked.length} group(s) de maior risco.`
  );

  const flags =
    await auditBatchResilient(
      ranked,
      translations,
      plan,
      job,
      true
    );

  job.stats.auditFlagged +=
    flags.size;

  console.log(
    `[GEMINI DEEP AUDIT] ` +
    `${flags.size} marcado(s).`
  );

  return flags;
}

// ============================================================
// REPAIR
// ============================================================

function repairPayload(
  group,
  translations,
  issue
) {
  return {
    g:
      group.groupId,

    reasons:
      [
        ...issue.reasons
      ],

    hint:
      issue
        .hints
        .join(" | ")
        .slice(
          0,
          800
        ),

    cues:
      group.cues.map(
        (
          cue,
          index
        ) => ({
          i:
            cue.index,

          en:
            cue.text,

          pt:
            translations
              .get(
                group.groupId
              )[index],

          ...(
            cue.speakerHint
              ? {
                  speaker:
                    cue.speakerHint
                }
              : {}
          )
        })
      )
  };
}

async function repairBatch(
  groups,
  translations,
  issueMap,
  plan,
  job,
  thinkingLevel,
  depth = 0
) {
  const expectedCues =
    countCues(
      groups
    );

  try {
    const response =
      await geminiRequest({
        system:
          REPAIR_PROMPT,

        user:
          `BÍBLIA GLOBAL:\n${JSON.stringify(plan)}\n\n` +
          `Repare estes ${groups.length} groups. ` +
          `Exatamente ${expectedCues} cues no output.\n\n` +
          JSON.stringify({
            groups:
              groups.map(
                group =>
                  repairPayload(
                    group,
                    translations,

                    issueMap.get(
                      group.groupId
                    )
                  )
              )
          }),

        schema:
          cueTranslationSchema(
            expectedCues
          ),

        thinkingLevel,

        maxOutputTokens:
          REPAIR_MAX_OUTPUT_TOKENS,

        timeoutMs:
          REPAIR_TIMEOUT_MS,

        maxRetries:
          REPAIR_RETRIES,

        job,

        metric:
          "repair"
      });

    const parsed =
      parseCueTranslation(
        groups,
        response.text
      );

    if (
      !parsed.invalidGroups.length &&
      response.status !==
        "incomplete"
    ) {
      return parsed.groupMap;
    }

    throw new Error(
      `Reparo incompleto: ` +
      `${parsed.invalidGroups.length} invalid group(s), ` +
      `status=${response.status}.`
    );
  } catch (error) {
    if (
      groups.length > 1 &&
      depth < 4
    ) {
      const middle =
        Math.ceil(
          groups.length /
          2
        );

      console.warn(
        `[GEMINI REPAIR SPLIT] ` +
        `${groups.length} -> ` +
        `${middle}+${groups.length - middle}.`
      );

      const [
        left,
        right
      ] =
        await Promise.all([
          repairBatch(
            groups.slice(
              0,
              middle
            ),

            translations,
            issueMap,
            plan,
            job,
            thinkingLevel,
            depth + 1
          ),

          repairBatch(
            groups.slice(
              middle
            ),

            translations,
            issueMap,
            plan,
            job,
            thinkingLevel,
            depth + 1
          )
        ]);

      return new Map([
        ...left,
        ...right
      ]);
    }

    throw error;
  }
}

async function repairIssueMap(
  allGroups,
  translations,
  issueMap,
  plan,
  job,
  thinkingLevel =
    REPAIR_THINKING
) {
  const selected =
    allGroups.filter(
      group =>
        issueMap.has(
          group.groupId
        )
    );

  if (
    !selected.length
  ) {
    return;
  }

  const batches =
    splitByBudget(
      selected,

      REPAIR_BATCH_CHARS,

      REPAIR_BATCH_GROUPS,

      group =>
        repairPayload(
          group,
          translations,

          issueMap.get(
            group.groupId
          )
        )
    );

  console.log(
    `[GEMINI REPAIR] ` +
    `${selected.length} group(s) -> ` +
    `${batches.length} lote(s) | ` +
    `thinking=${thinkingLevel}.`
  );

  for (
    const batch
    of batches
  ) {
    const repaired =
      await repairBatch(
        batch,
        translations,
        issueMap,
        plan,
        job,
        thinkingLevel
      );

    for (
      const [
        groupId,
        segments
      ]
      of repaired
    ) {
      translations.set(
        groupId,
        segments
      );

      job.stats.repairedGroups++;
    }
  }
}

async function recheckRepaired(
  allGroups,
  translations,
  plan,
  issueMap,
  job
) {
  const repairedGroups =
    allGroups.filter(
      group =>
        issueMap.has(
          group.groupId
        )
    );

  if (
    !repairedGroups.length
  ) {
    return new Map();
  }

  const batches =
    splitByBudget(
      repairedGroups,

      26000,

      100,

      group =>
        reviewPayload(
          group,
          translations
        )
    );

  const out =
    new Map();

  for (
    const batch
    of batches
  ) {
    const flags =
      await auditBatchResilient(
        batch,
        translations,
        plan,
        job,
        true
      );

    job.stats.auditRecheckGroups +=
      batch.length;

    job.stats.auditFlagged +=
      flags.size;

    for (
      const [
        id,
        issue
      ]
      of flags
    ) {
      out.set(
        id,
        issue
      );
    }
  }

  return out;
}

// ============================================================
// PIPELINE
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

  if (
    !blocks.length
  ) {
    throw new Error(
      "Nenhum cue SRT válido."
    );
  }

  const groups =
    buildSentenceGroups(
      blocks
    );

  console.log(
    `[PIPELINE 7.0] ` +
    `fonte=${job.sourceKind} | ` +
    `${blocks.length} cues -> ` +
    `${groups.length} Sentence Groups.`
  );

  console.log(
    `[PIPELINE 7.0] Limites conhecidos: ` +
    `${GEMINI_RPM_LIMIT} RPM | ` +
    `${GEMINI_TPM_LIMIT} TPM | ` +
    `${GEMINI_RPD_LIMIT} RPD.`
  );

  // 1) CONTEXTO GLOBAL
  const plan =
    await buildEpisodePlan(
      groups,
      job
    );

  job.progress =
    8;

  // 2) TRADUÇÃO PRINCIPAL
  // 4 lotes em paralelo
  const translations =
    await translateMainParallel(
      groups,
      plan,
      job
    );

  let texts =
    cleanAll(
      blocks,

      flattenTranslations(
        blocks,
        groups,
        translations
      ),

      job.filename
    );

  writeCleanBack(
    blocks,
    groups,
    translations,
    texts
  );

  auditTimestamps(
    sourceSrt,

    buildSrt(
      blocks,
      texts
    ),

    "CHECKPOINT FLASH-LITE"
  );

  job.progress =
    64;

  job.updatedAt =
    Date.now();

  // 3) AUDITORIA AMPLA
  const issues =
    await auditAllGroups(
      groups,
      translations,
      plan,
      job
    );

  if (
    job.stats.auditPrimaryGroups !==
    groups.length
  ) {
    throw new Error(
      `Auditoria ampla incompleta ` +
      `${job.stats.auditPrimaryGroups}/${groups.length}.`
    );
  }

  // 4) MICRO-AUDITORIA DOS TRECHOS MAIS ARRISCADOS
  const deepIssues =
    await deepAuditHighRisk(
      groups,
      translations,
      plan,
      job
    );

  mergeIssueMaps(
    issues,
    deepIssues
  );

  // 5) GUARDS LOCAIS
  mergeIssueMaps(
    issues,

    deterministicIssueMap(
      groups,
      translations,
      job
    )
  );

  console.log(
    `[QUALITY MAP] ` +
    `${issues.size} group(s) para reparo dirigido.`
  );

  job.progress =
    80;

  // 6) REPARO SOMENTE DO QUE FOI SINALIZADO
  if (
    issues.size
  ) {
    await repairIssueMap(
      groups,
      translations,
      issues,
      plan,
      job,
      REPAIR_THINKING
    );

    texts =
      cleanAll(
        blocks,

        flattenTranslations(
          blocks,
          groups,
          translations
        ),

        job.filename
      );

    writeCleanBack(
      blocks,
      groups,
      translations,
      texts
    );

    // 7) RECHECK SOMENTE DOS REPARADOS
    const secondIssues =
      await recheckRepaired(
        groups,
        translations,
        plan,
        issues,
        job
      );

    mergeIssueMaps(
      secondIssues,

      deterministicIssueMap(
        groups.filter(
          group =>
            issues.has(
              group.groupId
            )
        ),

        translations,
        job
      )
    );

    if (
      secondIssues.size
    ) {
      job.stats.secondPassGroups =
        secondIssues.size;

      console.log(
        `[SECOND PASS] ` +
        `${secondIssues.size} group(s) ainda suspeito(s); ` +
        `reparo final HIGH.`
      );

      await repairIssueMap(
        groups,
        translations,
        secondIssues,
        plan,
        job,
        EMERGENCY_THINKING
      );
    }
  }

  // 8) LIMPEZA + HARD FIXES SOURCE-AWARE
  texts =
    cleanAll(
      blocks,

      flattenTranslations(
        blocks,
        groups,
        translations
      ),

      job.filename
    );

  writeCleanBack(
    blocks,
    groups,
    translations,
    texts
  );

  let residual =
    deterministicIssueMap(
      groups,
      translations,
      job
    );

  if (
    residual.size
  ) {
    const residualGroups =
      groups.filter(
        group =>
          residual.has(
            group.groupId
          )
      );

    job.stats.emergencyRepairGroups =
      residualGroups.length;

    console.warn(
      `[QUALITY EMERGENCY] ` +
      `${residualGroups.length} group(s) residual(is); ` +
      `última correção HIGH focada.`
    );

    try {
      await repairIssueMap(
        groups,
        translations,
        residual,
        plan,
        job,
        EMERGENCY_THINKING
      );

      texts =
        cleanAll(
          blocks,

          flattenTranslations(
            blocks,
            groups,
            translations
          ),

          job.filename
        );

      writeCleanBack(
        blocks,
        groups,
        translations,
        texts
      );

      residual =
        deterministicIssueMap(
          groups,
          translations,
          job
        );
    } catch (error) {
      console.warn(
        `[QUALITY EMERGENCY] Falhou sem matar o episódio: ` +
        `${errorMessage(error).slice(0, 220)}`
      );
    }
  }

  if (
    residual.size
  ) {
    console.warn(
      `[QUALITY GUARD] AVISO — ` +
      `${residual.size} group(s) residual(is). ` +
      `SRT será entregue para não travar o Stremio.`
    );
  } else {
    console.log(
      "[QUALITY GUARD] PASSOU — 0 padrão conhecido restante."
    );
  }

  const finalSrt =
    buildSrt(
      blocks,
      texts
    );

  auditTimestamps(
    sourceSrt,
    finalSrt,
    "FINAL 7.0"
  );

  const elapsed =
    (
      Date.now() -
      startedAt
    ) /
    1000;

  console.log(
    `[PIPELINE 7.0] OK em ${elapsed.toFixed(1)}s | ` +

    `Plan=${job.stats.planCalls} | ` +

    `MainCalls=${job.stats.mainCalls} | ` +
    `MainAttempts=${job.stats.mainAttempts} | ` +
    `Main429=${job.stats.main429} | ` +
    `MainSplits=${job.stats.mainSplits} | ` +
    `MainRescueGroups=${job.stats.mainRescueGroups} | ` +

    `AuditCalls=${job.stats.auditCalls} | ` +
    `DeepAudit=${job.stats.deepAuditCalls} | ` +
    `Audit429=${job.stats.audit429} | ` +
    `AuditPrimary=${job.stats.auditPrimaryGroups}/${groups.length} | ` +
    `AuditRecheck=${job.stats.auditRecheckGroups} | ` +
    `Flagged=${job.stats.auditFlagged} | ` +

    `RepairCalls=${job.stats.repairCalls} | ` +
    `Repair429=${job.stats.repair429} | ` +
    `Repaired=${job.stats.repairedGroups} | ` +

    `SecondPass=${job.stats.secondPassGroups} | ` +
    `Emergency=${job.stats.emergencyRepairGroups} | ` +
    `Residual=${residual.size} | ` +

    `Tokens=${job.stats.inputTokens}+${job.stats.outputTokens}+` +
    `thought:${job.stats.thoughtTokens} | ` +

    `RPMWait=${(job.stats.rpmWaitMs / 1000).toFixed(1)}s.`
  );

  return finalSrt;
}

// ============================================================
// JOB QUEUE
// ============================================================

async function processJob(job) {
  job.status =
    "processing";

  job.updatedAt =
    Date.now();

  console.log(
    `[JOB ${job.id}] ` +
    `Iniciando fonte=${job.sourceKind}. ` +
    `SEM teto global.`
  );

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

    console.log(
      `[JOB ${job.id}] Concluído.`
    );
  } catch (error) {
    job.status =
      "failed";

    job.error =
      errorMessage(
        error
      );

    console.error(
      `[JOB ${job.id}] Falhou: ` +
      `${job.error}`
    );
  }

  job.updatedAt =
    Date.now();
}

function enqueue(job) {
  if (
    queue.some(
      item =>
        item.id ===
        job.id
    )
  ) {
    return;
  }

  queue.push(job);

  console.log(
    `[JOB QUEUE] ${job.id} entrou; ` +
    `aguardando=${queue.length}.`
  );

  runQueue();
}

async function runQueue() {
  if (queueRunning) {
    return;
  }

  queueRunning =
    true;

  try {
    while (
      queue.length
    ) {
      const job =
        queue.shift();

      if (
        job &&
        job.status ===
          "processing"
      ) {
        await processJob(job);
      }
    }
  } finally {
    queueRunning =
      false;

    if (
      queue.length
    ) {
      runQueue();
    }
  }
}

// ============================================================
// OPENSUBTITLES FALLBACK
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

function buildOpenSubtitlesUrl(
  type,
  id,
  extra
) {
  const base =
    `https://opensubtitles-v3.strem.io/` +
    `subtitles/` +
    `${encodeURIComponent(type)}/` +
    `${encodeURIComponent(id)}`;

  const params =
    new URLSearchParams();

  if (
    extra.videoHash
  ) {
    params.set(
      "videoHash",
      extra.videoHash
    );
  }

  if (
    extra.videoSize
  ) {
    params.set(
      "videoSize",
      extra.videoSize
    );
  }

  if (
    extra.filename
  ) {
    params.set(
      "filename",
      extra.filename
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
      (
        a,
        b
      ) => {
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
              subtitle?.hearingImpaired ===
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

async function fetchFallbackSrt({
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
    `[OPENSUBTITLES FALLBACK] ${url}`
  );

  const response =
    await fetchWithTimeout(
      url,

      {
        headers: {
          Accept:
            "application/json",

          "User-Agent":
            "Stremio-PTBR/7.0"
        }
      }
    );

  if (!response.ok) {
    throw new Error(
      `OpenSubtitles HTTP ${response.status}.`
    );
  }

  const data =
    await response.json();

  const target =
    selectEnglishSubtitle(
      data?.subtitles
    );

  if (!target) {
    throw new Error(
      "OpenSubtitles não encontrou inglês utilizável."
    );
  }

  const subtitleResponse =
    await fetchWithTimeout(
      target.url,

      {
        headers: {
          "User-Agent":
            "Stremio-PTBR/7.0"
        }
      }
    );

  if (
    !subtitleResponse.ok
  ) {
    throw new Error(
      `Download OpenSubtitles HTTP ` +
      `${subtitleResponse.status}.`
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
      "Legenda fallback vazia/grande demais."
    );
  }

  const clean =
    cleanSrtForTranslation(
      raw
    );

  if (!clean) {
    throw new Error(
      "Legenda fallback vazia após limpeza."
    );
  }

  return clean;
}

// ============================================================
// ROUTES
// ============================================================

function jobResponse(
  req,
  job
) {
  return {
    ok:
      true,

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
      `${baseUrl(req)}/` +
      `subtitle/` +
      `${encodeURIComponent(job.id)}` +
      `.srt`
  };
}

const manifest = {
  id:
    "org.tradutor.stateless.gemini.free",

  version:
    "7.0.0",

  name:
    "Tradutor PT-BR Backend",

  description:
    "Backend-only: Gemini 3.5 Flash-Lite unificado, rápido e rate-aware.",

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

  (
    req,
    res
  ) =>
    res.json(
      manifest
    )
);

app.get(
  "/",

  (
    req,
    res
  ) =>
    res.json({
      status:
        "online",

      version:
        manifest.version,

      role:
        "BACKEND_ONLY",

      cacheVersion:
        CACHE_VERSION,

      model:
        GEMINI_MODEL,

      api:
        "INTERACTIONS",

      limits: {
        rpm:
          GEMINI_RPM_LIMIT,

        tpm:
          GEMINI_TPM_LIMIT,

        rpd:
          GEMINI_RPD_LIMIT
      },

      mainParallelBatches:
        MAIN_PARALLEL_BATCHES,

      auditBatchGroups:
        AUDIT_BATCH_GROUPS,

      deepAuditMaxGroups:
        DEEP_AUDIT_MAX_GROUPS,

      queue:
        queue.length,

      processing:
        queueRunning
    })
);

function backendOnlySubtitles(
  req,
  res
) {
  console.log(
    "[STREMIO PUBLIC] " +
    "Backend-only: 0 legendas; use Ponte Local."
  );

  return safeJson(
    res,

    {
      subtitles: []
    }
  );
}

app.get(
  "/subtitles/:type/:id.json",
  backendOnlySubtitles
);

app.get(
  "/subtitles/:type/:id/:extra.json",
  backendOnlySubtitles
);

// ============================================================
// EMBEDDED API
// ============================================================

app.post(
  "/api/translate-embedded",

  async (
    req,
    res
  ) => {
    if (
      !authorized(req)
    ) {
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
          "embedded"
        ).trim();

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

      const cueCount =
        parseSrt(
          sourceSrt
        ).length;

      if (
        !sourceSrt ||
        !cueCount
      ) {
        throw new Error(
          "Embedded vazia/inválida após limpeza."
        );
      }

      console.log(
        `[EMBEDDED API] ` +
        `${type}/${videoId} | ` +
        `${filename} | ` +
        `${cueCount} cues.`
      );

      const job =
        getOrCreateJob({
          type,

          videoId,

          filename,

          sourceSrt,

          sourceKind:
            "embedded"
        });

      return safeJson(
        res,

        jobResponse(
          req,
          job
        )
      );
    } catch (error) {
      console.error(
        `[EMBEDDED API] ` +
        `${errorMessage(error)}`
      );

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
);

// ============================================================
// FALLBACK API
// ============================================================

app.post(
  "/api/translate-fallback",

  async (
    req,
    res
  ) => {
    if (
      !authorized(req)
    ) {
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
          ""
        ).trim();

      const videoSize =
        String(
          req.body?.videoSize ||
          ""
        ).trim();

      const videoHash =
        String(
          req.body?.videoHash ||
          ""
        ).trim();

      const sourceSrt =
        await fetchFallbackSrt({
          type,

          id:
            videoId,

          filename,

          videoSize,

          videoHash
        });

      console.log(
        `[FALLBACK API] ` +
        `${parseSrt(sourceSrt).length} cues.`
      );

      const job =
        getOrCreateJob({
          type,

          videoId,

          filename,

          sourceSrt,

          sourceKind:
            "opensubtitles-fallback"
        });

      return safeJson(
        res,

        jobResponse(
          req,
          job
        )
      );
    } catch (error) {
      console.error(
        `[FALLBACK API] ` +
        `${errorMessage(error)}`
      );

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
);

// ============================================================
// JOB STATUS
// ============================================================

app.get(
  "/job/:jobId",

  (
    req,
    res
  ) => {
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

        stats:
          job.stats
      }
    );
  }
);

// ============================================================
// SRT DELIVERY
// ============================================================

function processingSrt(job) {
  return [
    "1",

    "00:00:01,000 --> 00:00:08,000",

    "Traduzindo e revisando legenda...",

    "",

    "2",

    "00:00:08,500 --> 00:00:15,000",

    `Progresso: ` +
    `${Number(
      job?.progress ||
      0
    )}%.`
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

    String(
      error ||
      "Erro desconhecido."
    )
      .replace(
        /\s+/g,
        " "
      )
      .slice(
        0,
        300
      )
  ].join("\n");
}

app.get(
  "/subtitle/:jobId.srt",

  (
    req,
    res
  ) => {
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

    const job =
      jobs.get(
        jobId
      );

    if (!job) {
      return sendSrt(
        res,

        errorSrt(
          "Job expirado."
        )
      );
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
            errorMessage(error)
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

      processingSrt(
        job
      ),

      "no-store, no-cache, must-revalidate"
    );
  }
);

// ============================================================
// START
// ============================================================

app.listen(
  PORT,

  () => {
    console.log(
      "============================================================"
    );

    console.log(
      " STREMIO PT-BR BACKEND 7.0 FINAL — FLASH-LITE ONLY"
    );

    console.log(
      "============================================================"
    );

    console.log(
      `Gemini API: ` +
      `${
        GEMINI_API_KEY
          ? "CONFIGURADA ✅"
          : "FALTANDO ❌"
      }`
    );

    console.log(
      `Modelo único: ${GEMINI_MODEL} ✅`
    );

    console.log(
      "Gemini 3.6 Flash: NÃO USADO ✅"
    );

    console.log(
      "Mistral: FORA DO PIPELINE ✅"
    );

    console.log(
      "Groq: FORA DO PIPELINE ✅"
    );

    console.log(
      "API: Interactions + Structured Output ✅"
    );

    console.log(
      "temperature/top_p/top_k: NÃO ENVIADOS ✅"
    );

    console.log(
      "safety_settings: NÃO ENVIADO ✅"
    );

    console.log(
      `Limite observado: ` +
      `${GEMINI_RPM_LIMIT} RPM | ` +
      `${GEMINI_TPM_LIMIT} TPM | ` +
      `${GEMINI_RPD_LIMIT} RPD ✅`
    );

    console.log(
      "Gate de RPM: SOMENTE quando os 15 RPM reais seriam ultrapassados ✅"
    );

    console.log(
      "Pacer artificial/TPM inventado: NÃO EXISTE ✅"
    );

    console.log(
      "Contexto global do episódio: 1 chamada curta ✅"
    );

    console.log(
      `Tradução principal: ` +
      `${MAIN_PARALLEL_BATCHES} lotes PARALELOS, ` +
      `thinking=${MAIN_THINKING} ✅`
    );

    console.log(
      `Auditoria ampla: até ` +
      `${AUDIT_BATCH_GROUPS} groups/lote, ` +
      `concorrência=${AUDIT_CONCURRENCY} ✅`
    );

    console.log(
      `Micro-auditoria profunda: ` +
      `top ${DEEP_AUDIT_MAX_GROUPS} groups de risco ✅`
    );

    console.log(
      `Reparo: SOMENTE sinalizados, ` +
      `thinking=${REPAIR_THINKING} ✅`
    );

    console.log(
      "Style Pack Drag/Reality/Gen Z/Alpha 2026: ATIVO ✅"
    );

    console.log(
      "bitch vocativo → bicha/gata/amiga; puta automática PROIBIDA ✅"
    );

    console.log(
      "competição/lip sync/cheque 'da porra': GUARD ATIVO ✅"
    );

    console.log(
      "supportive→super apoiador: GUARD ATIVO ✅"
    );

    console.log(
      "judges Drag→jurados + judgers/judgees literal: GUARD ATIVO ✅"
    );

    console.log(
      "plucking pussy hairs→fio de bigode: GUARD ATIVO ✅"
    );

    console.log(
      "empate duplo indevido: GUARD ATIVO ✅"
    );

    console.log(
      "gagged→amordaçada: GUARD ATIVO ✅"
    );

    console.log(
      "uh-huh/uh-uh semânticos: PRESERVADOS ✅"
    );

    console.log(
      "Cue-ID lock + timestamps embedded imutáveis: ATIVOS ✅"
    );

    console.log(
      "OpenSubtitles: somente fallback solicitado pela Ponte ✅"
    );

    console.log(
      "Render no menu do Stremio: BACKEND ONLY ✅"
    );

    console.log(
      "Teto global do episódio: NÃO EXISTE ✅"
    );

    console.log(
      `Namespace de cache: ${CACHE_VERSION}`
    );

    console.log(
      "Status: ONLINE"
    );

    console.log(
      "============================================================"
    );
  }
);

process.on(
  "unhandledRejection",

  error => {
    console.error(
      "[PROCESS] Unhandled rejection:",
      error
    );
  }
);

process.on(
  "uncaughtException",

  error => {
    console.error(
      "[PROCESS] Uncaught exception:",
      error
    );
  }
);
