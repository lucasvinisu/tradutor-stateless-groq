const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// STREMIO PT-BR BACKEND 6.6.2 — GEMINI INTERACTIONS FIX
// ============================================================
// Tradutor: Gemini 3.6 Flash
// Auditor: Gemini 3.5 Flash-Lite
// API: Gemini Interactions API
//
// IMPORTANTE:
// - NÃO envia safety_settings: a Gemini Developer API rejeitou esse
//   parâmetro no POST /interactions com API key.
// - NÃO envia temperature, top_p ou top_k.
// - NÃO usa Mistral nem Groq.
// - NÃO existe teto global do episódio.
// - Ponte Local 4.1 permanece compatível.
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();

const GEMINI_TRANSLATOR_MODEL = String(
  process.env.GEMINI_TRANSLATOR_MODEL || "gemini-3.6-flash"
).trim();

const GEMINI_AUDITOR_MODEL = String(
  process.env.GEMINI_MODEL || "gemini-3.5-flash-lite"
).trim();

const CACHE_VERSION = "6.6.2-interactions-fix";
const MAX_SOURCE_CHARS = 800000;
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS = 25000;

// Timeouts são por REQUEST, nunca do episódio inteiro.
const MAIN_REQUEST_TIMEOUT_MS = Number(
  process.env.GEMINI_MAIN_TIMEOUT_MS || 180000
);

const AUDIT_REQUEST_TIMEOUT_MS = Number(
  process.env.GEMINI_AUDIT_TIMEOUT_MS || 90000
);

const REPAIR_REQUEST_TIMEOUT_MS = Number(
  process.env.GEMINI_REPAIR_TIMEOUT_MS || 120000
);

const MAIN_MAX_RETRIES = Number(
  process.env.GEMINI_MAIN_RETRIES || 5
);

const AUDIT_MAX_RETRIES = Number(
  process.env.GEMINI_AUDIT_RETRIES || 4
);

const REPAIR_MAX_RETRIES = Number(
  process.env.GEMINI_REPAIR_RETRIES || 5
);

const MAIN_MAX_OUTPUT_TOKENS = Number(
  process.env.GEMINI_MAIN_MAX_OUTPUT_TOKENS || 50000
);

const AUDIT_MAX_OUTPUT_TOKENS = Number(
  process.env.GEMINI_AUDIT_MAX_OUTPUT_TOKENS || 12000
);

const REPAIR_MAX_OUTPUT_TOKENS = Number(
  process.env.GEMINI_REPAIR_MAX_OUTPUT_TOKENS || 24000
);

const MAIN_THINKING_LEVEL = String(
  process.env.GEMINI_TRANSLATOR_THINKING || "medium"
)
  .trim()
  .toLowerCase();

const AUDIT_THINKING_LEVEL = "minimal";
const REPAIR_THINKING_LEVEL = "high";

const AUDIT_BATCH_GROUPS = Number(
  process.env.GEMINI_AUDIT_GROUPS || 80
);

const AUDIT_BATCH_CHARS = Number(
  process.env.GEMINI_AUDIT_CHARS || 22000
);

const AUDIT_CONCURRENCY = Math.max(
  1,
  Math.min(
    3,
    Number(
      process.env.GEMINI_AUDIT_CONCURRENCY || 3
    )
  )
);

const REPAIR_BATCH_GROUPS = Number(
  process.env.GEMINI_REPAIR_GROUPS || 100
);

const REPAIR_BATCH_CHARS = Number(
  process.env.GEMINI_REPAIR_CHARS || 28000
);

const translationCache = new Map();
const jobs = new Map();
const queue = [];

let queueRunning = false;

const sleep = ms =>
  new Promise(resolve =>
    setTimeout(resolve, ms)
  );

// ============================================================
// HELPERS
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

  const proto = String(
    req.headers["x-forwarded-proto"] ||
    req.protocol ||
    "https"
  )
    .split(",")[0]
    .trim();

  const host = String(
    req.headers["x-forwarded-host"] ||
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

  const a = Buffer.from(
    String(
      req.headers.authorization ||
      ""
    ).trim()
  );

  const b = Buffer.from(
    `Bearer ${LOCAL_BRIDGE_SECRET}`
  );

  return (
    a.length === b.length &&
    crypto.timingSafeEqual(
      a,
      b
    )
  );
}

// ============================================================
// CACHE / JOBS
// ============================================================

function makeCacheKey(
  type,
  videoId,
  sourceSrt
) {
  return (
    `${CACHE_VERSION}:` +
    `${type}:` +
    `${videoId}:` +
    `${sha256(sourceSrt)}`
  );
}

function getCache(key) {
  const item =
    translationCache.get(key);

  if (!item) {
    return null;
  }

  if (
    item.expiresAt <=
    Date.now()
  ) {
    translationCache.delete(key);
    return null;
  }

  return item.srt;
}

function setCache(
  key,
  srt
) {
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
  sourceKind
}) {
  const now =
    Date.now();

  const sourceHash =
    sha256(sourceSrt);

  const job = {
    id:
      `job-` +
      `${sourceHash.slice(
        0,
        24
      )}-` +
      `${randomId()}`,

    type,

    videoId,

    filename,

    sourceSrt,

    sourceKind,

    sourceHash,

    cacheKey:
      makeCacheKey(
        type,
        videoId,
        sourceSrt
      ),

    status:
      "processing",

    progress:
      1,

    result:
      null,

    error:
      null,

    createdAt:
      now,

    updatedAt:
      now,

    expiresAt:
      now +
      JOB_TTL_MS,

    stats: {
      mainCalls: 0,
      mainAttempts: 0,
      main429: 0,
      mainSplits: 0,
      mainRescueGroups: 0,

      mainInputTokens: 0,
      mainOutputTokens: 0,
      mainThoughtTokens: 0,

      auditCalls: 0,
      auditAttempts: 0,
      audit429: 0,
      auditFallbackCalls: 0,

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
      omissionFlags: 0
    }
  };

  jobs.set(
    job.id,
    job
  );

  return job;
}

function findJobByCache(
  key,
  statuses
) {
  for (
    const job
    of jobs.values()
  ) {
    if (
      job.cacheKey === key &&
      statuses.includes(
        job.status
      )
    ) {
      return job;
    }
  }

  return null;
}

function getOrCreateJob(
  args
) {
  const key =
    makeCacheKey(
      args.type,
      args.videoId,
      args.sourceSrt
    );

  const cached =
    getCache(key);

  if (cached) {
    let job =
      findJobByCache(
        key,
        ["completed"]
      );

    if (!job) {
      job =
        createJob(args);

      job.status =
        "completed";

      job.progress =
        100;

      job.result =
        cached;
    }

    return job;
  }

  const active =
    findJobByCache(
      key,
      ["processing"]
    );

  if (active) {
    return active;
  }

  const done =
    findJobByCache(
      key,
      ["completed"]
    );

  if (done) {
    return done;
  }

  const job =
    createJob(args);

  enqueue(job);

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
      ]
      of translationCache.entries()
    ) {
      if (
        item.expiresAt <=
        now
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
      ]
      of jobs.entries()
    ) {
      if (
        job.expiresAt <=
          now &&
        job.status !==
          "processing"
      ) {
        jobs.delete(
          id
        );
      }
    }
  },

  10 *
  60 *
  1000
).unref();

// ============================================================
// SRT CLEAN / PARSE
// ============================================================

const TIMING_RE =
  /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
  /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
  /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

function normalizeSpeaker(
  value
) {
  const speaker =
    String(
      value ||
      ""
    )
      .replace(
        /<[^>]+>/g,
        " "
      )
      .replace(
        /\s+/g,
        " "
      )
      .trim();

  if (
    !speaker ||
    speaker.length >
      60 ||
    SDH_WORDS.test(
      speaker
    ) ||
    /[!?;]/u.test(
      speaker
    )
  ) {
    return "";
  }

  return speaker;
}

function extractSpeaker(
  line
) {
  const original =
    String(
      line ||
      ""
    );

  const hidden =
    original.match(
      SPEAKER_RE
    );

  if (hidden) {
    let speaker =
      "";

    try {
      speaker =
        normalizeSpeaker(
          decodeURIComponent(
            hidden[1]
          )
        );
    }
    catch {}

    return {
      speaker,

      text:
        original.replace(
          SPEAKER_RE,
          ""
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

    if (speaker) {
      return {
        speaker,

        text:
          original.slice(
            bracket[0].length
          )
      };
    }
  }

  const colon =
    original.match(
      /^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50})\s*:\s+(?=\S)/u
    );

  if (colon) {
    const speaker =
      normalizeSpeaker(
        colon[1]
      );

    if (speaker) {
      return {
        speaker,

        text:
          original.slice(
            colon[0].length
          )
      };
    }
  }

  return {
    speaker:
      "",

    text:
      original
  };
}

function normalizeElongations(
  text
) {
  return String(
    text ||
    ""
  )
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

// NÃO remove uh-huh / uh-uh:
// eles podem carregar sentido de sim/não.
function isEmptyVocalization(
  text
) {
  const value =
    String(
      text ||
      ""
    )
      .toLowerCase()
      .replace(
        /[.,!?…]+/g,
        " "
      )
      .replace(
        /\s+/g,
        " "
      )
      .trim();

  return /^(?:ah|ha|heh)(?:\s+(?:ah|ha|heh)){1,5}$/.test(
    value
  );
}

function cleanSourceLine(
  line
) {
  let text =
    String(
      line ||
      ""
    ).trim();

  if (!text) {
    return "";
  }

  text =
    text.replace(
      /\s*\[[^\]]+\]\s*/gu,
      " "
    );

  text =
    text.replace(
      /\s*\(([^)]*)\)\s*/gu,
      (
        match,
        inside
      ) =>
        SDH_WORDS.test(
          String(
            inside ||
            ""
          )
        )
          ? " "
          : match
    );

  text =
    normalizeElongations(
      text.replace(
        /[♪♫♬]/gu,
        " "
      )
    )
      .replace(
        /[ \t]{2,}/g,
        " "
      )
      .trim();

  if (
    !text ||
    /^[-–—/\s]*$/u.test(
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
      .filter(
        Boolean
      );

  const out =
    [];

  let removed =
    0;

  let speakerHints =
    0;

  let vocalizations =
    0;

  for (
    const raw
    of rawBlocks
  ) {
    const lines =
      raw
        .trim()
        .split(
          "\n"
        );

    const timingIndex =
      lines.findIndex(
        line =>
          /-->/.test(
            line
          )
      );

    if (
      timingIndex <
      0
    ) {
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

    const dialogue =
      [];

    const speakers =
      new Set();

    for (
      const sourceLine
      of lines.slice(
        timingIndex +
        1
      )
    ) {
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

      const before =
        String(
          info.text ||
          ""
        ).trim();

      const cleaned =
        cleanSourceLine(
          before
        );

      if (
        !cleaned &&
        isEmptyVocalization(
          before
        )
      ) {
        vocalizations++;
      }

      if (
        cleaned
      ) {
        dialogue.push(
          cleaned
        );
      }
    }

    if (
      !dialogue.length
    ) {
      removed++;
      continue;
    }

    if (
      speakers.size ===
      1
    ) {
      const speaker =
        [
          ...speakers
        ][0];

      dialogue[0] =
        `@@SPK:` +
        `${encodeURIComponent(
          speaker
        )}` +
        `@@ ${dialogue[0]}`;

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

  if (
    !out.length
  ) {
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
            index +
            1,

            block.timing,

            ...block.dialogue
          ].join(
            "\n"
          )
      )
      .join(
        "\n\n"
      )
      .trim() +
    "\n"
  );
}

function parseSrt(
  srt
) {
  const normalized =
    normalizeSrt(
      srt
    );

  if (!normalized) {
    return [];
  }

  const result =
    [];

  for (
    const raw
    of normalized.split(
      /\n{2,}/
    )
  ) {
    const lines =
      raw
        .trim()
        .split(
          "\n"
        );

    if (
      lines.length <
        3 ||
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
      lines.slice(
        2
      );

    let speakerHint =
      null;

    if (
      textLines.length
    ) {
      const match =
        textLines[0]
          .match(
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
        }
        catch {}

        textLines[0] =
          textLines[0]
            .replace(
              SPEAKER_RE,
              ""
            );
      }
    }

    result.push({
      index:
        Number(
          lines[0]
            .trim()
        ),

      timing:
        lines[1]
          .trim(),

      text:
        textLines
          .join(
            "\n"
          )
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
        (
          block,
          index
        ) =>
          [
            block.index,

            block.timing,

            texts[index] ??
            block.text
          ].join(
            "\n"
          )
      )
      .join(
        "\n\n"
      )
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
    parseSrt(
      sourceSrt
    );

  const final =
    parseSrt(
      finalSrt
    );

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
    i <
    source.length;
    i++
  ) {
    if (
      source[i].index !==
        final[i].index ||
      source[i].timing !==
        final[i].timing
    ) {
      throw new Error(
        `TIMING LOCK ${label}: ` +
        `cue ${source[i].index}.`
      );
    }
  }

  console.log(
    `[AUDIT TIMESTAMP] ${label}: PASSOU — ` +
    `${source.length}/${source.length}; ` +
    `0 alterações.`
  );
}

// ============================================================
// SENTENCE GROUPS
// ============================================================

function parseTimeSeconds(
  value
) {
  const match =
    String(
      value ||
      ""
    ).match(
      /^(\d{2}):(\d{2}):(\d{2})[,.](\d{3})$/
    );

  if (
    !match
  ) {
    return NaN;
  }

  return (
    Number(
      match[1]
    ) *
      3600 +

    Number(
      match[2]
    ) *
      60 +

    Number(
      match[3]
    ) +

    Number(
      match[4]
    ) /
      1000
  );
}

function timingParts(
  timing
) {
  const match =
    String(
      timing ||
      ""
    ).match(
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
        start:
          NaN,

        end:
          NaN
      };
}

function groupingText(
  text
) {
  return String(
    text ||
    ""
  )
    .replace(
      /<[^>]+>/g,
      " "
    )
    .replace(
      /\{\\[^}]+\}/g,
      " "
    )
    .replace(
      /\s+/g,
      " "
    )
    .trim();
}

function isMultiSpeaker(
  text
) {
  const lines =
    String(
      text ||
      ""
    )
      .split(
        "\n"
      )
      .filter(
        line =>
          line.trim()
      );

  return (
    lines.length >=
      2 &&
    lines.filter(
      line =>
        /^\s*[-–—]\s*\S/u.test(
          line
        )
    ).length >=
      2
  );
}

function shouldMerge(
  group,
  next
) {
  if (
    !group.length ||
    group.length >=
      4
  ) {
    return false;
  }

  const previous =
    group[
      group.length -
      1
    ];

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
    timingParts(
      previous.timing
    );

  const b =
    timingParts(
      next.timing
    );

  if (
    Number.isFinite(
      a.end
    ) &&
    Number.isFinite(
      b.start
    ) &&
    b.start -
      a.end >
      0.9
  ) {
    return false;
  }

  const nextText =
    groupingText(
      next.text
    )
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
    groupingText(
      previous.text
    );

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
  const groups =
    [];

  let current =
    [];

  const flush =
    () => {
      if (
        !current.length
      ) {
        return;
      }

      groups.push({
        groupId:
          groups.length +
          1,

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

      current =
        [];
    };

  for (
    const block
    of blocks
  ) {
    if (
      !current.length ||
      shouldMerge(
        current,
        block
      )
    ) {
      current.push(
        block
      );
    }
    else {
      flush();

      current.push(
        block
      );
    }
  }

  flush();

  return groups;
}

function compactGroup(
  group
) {
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

function splitByBudget(
  items,
  maxChars,
  maxItems,
  builder
) {
  const batches =
    [];

  let current =
    [];

  let chars =
    0;

  for (
    const item
    of items
  ) {
    const size =
      JSON.stringify(
        builder(
          item
        )
      ).length +
      8;

    if (
      current.length &&
      (
        current.length >=
          maxItems ||
        chars +
          size >
          maxChars
      )
    ) {
      batches.push(
        current
      );

      current =
        [];

      chars =
        0;
    }

    current.push(
      item
    );

    chars +=
      size;
  }

  if (
    current.length
  ) {
    batches.push(
      current
    );
  }

  return batches;
}

function countCues(
  groups
) {
  return groups.reduce(
    (
      sum,
      group
    ) =>
      sum +
      group.cues.length,

    0
  );
}

// ============================================================
// STYLE PACK 2026
// ============================================================

const TRANSLATOR_PROMPT = `
Você é o tradutor principal EN→PT-BR de legendas de entretenimento em 2026.

PRIORIDADES ABSOLUTAS:
- português brasileiro natural, oral, atual, coerente e fiel;
- sincronização semântica cue a cue;
- adaptação cultural inteligente sem caricatura;
- concisão de legenda sem perder informação.

Não soe literal, engessado, lusitano, burocrático, antiquado, "tiozão" ou com internetês forçado.
Use linguagem Gen Z/Alpha apenas quando personagem, contexto e tom pedirem.

DRAG / REALITY / LGBTQIA+ / POP / MODA / MÚSICA:

- "bitch" como vocativo amigável:
  bicha, gata, amiga, menina ou omitir.
  NÃO use "puta" automaticamente.

- "I'm gagged" / "gagged" como reação:
  "Tô passada",
  "Tô muito passada",
  "Tô em choque",
  "Tô sem reação".
  Nunca "amordaçada".

- "she ate":
  "arrasou",
  "entregou tudo",
  "serviu demais",
  quando for gíria.

- "no crumbs":
  "não deixou nada pra ninguém",
  quando couber.

- fucking / motherfucking:
  são intensificadores.
  Preserve força em posição natural brasileira.

  Nunca por literalidade:
  "competição da porra",
  "competição do caralho",
  "lip sync da porra",
  "lip sync do caralho",
  "cheque da porra",
  "cheque do caralho".

- "fucking lip sync":
  pode ser
  "um puta lip sync",
  "um lip sync foda",
  "um lip sync absurdo",
  conforme contexto.

- supportive:
  prefira
  "sempre me apoiou",
  "sempre esteve do meu lado".
  Evite "super apoiador".

- judges em Drag Race:
  jurados,
  não juízes.

- "the judgers are now the judgees":
  "agora quem julgava vai ser julgado"
  ou
  "agora os jurados é que vão ser julgados".

  Nunca:
  "os juízes viraram os julgados".

- "plucking pussy hairs":
  preserve corpo + vulgaridade;
  ex. "catar pelo de xereca".

  Nunca:
  "fio de bigode".

- double win / shared win:
  "vitória dupla",
  "as duas ganharam"
  ou equivalente.

  Não invente "empate duplo"
  quando não há empate.

- week one:
  primeira semana.

- off the top sobre dinheiro:
  comissão/corte/porcentagem.

- closing ranks:
  grupo se protegendo/panelinha.

- Carry the two em conta:
  vai dois.

- evite "apoiante".

- "The talent performers"
  não é
  "As artistas do talento".

Preserve quando presentes e reconhecíveis:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

PALAVRÃO:
não censure.
Preserve intensidade de maneira que brasileiro realmente fala.

GÊNERO:
"speaker" é contexto oculto.
Use quando seguro.
Se não for, reformule naturalmente.

Nunca escreva:
empolgado(a)
animado(a)
ele/ela
ela/ele

FORMATAÇÃO:
sem speaker labels,
[NOME],
NOME:,
barras como separador de diálogo,
hífens/travessões decorativos,
SDH/CC
ou alongamentos gráficos.

CUE LOCK ABSOLUTO:

Sentence Groups servem SOMENTE para contexto.
Cada cue tem id "i".

Devolva exatamente UM "pt"
para CADA "i",
com o MESMO id.

Não resuma.
Não omita fim de frase/raciocínio.
Não antecipe o cue seguinte.
Não empurre conteúdo para cue posterior.

Mantenha cada pedaço no timestamp
em que é falado.

Você está fazendo transformação/tradução
de diálogo ficcional/reality já fornecido.
Preserve o registro do original,
inclusive vulgaridade quando necessária à fidelidade.

Responda somente pelo JSON Schema fornecido pela API.
`;

const AUDITOR_PROMPT = `
Você é auditor editorial independente EN→PT-BR em 2026.

Compare EN × PT cue por cue.
Não reescreva por gosto.

Para cada group devolva:
"ok"
ou
"fix".

Marque fix somente por problema real:

SEMANTIC
OMISSION
CUE_SYNC
LITERAL
REGISTER
CULTURE
PROFANITY
GENDER
FORMAT

Cheque especialmente:

- bitch-vocativo não vira puta automaticamente;
- intensificadores não viram competição/lip sync/cheque da porra/do caralho por tradução mecânica;
- supportive não vira super apoiador;
- judges em Drag Race = jurados;
- judgers/judgees não vira juízes viraram os julgados;
- plucking pussy hairs não vira fio de bigode;
- shared/double win não vira empate duplo se não há empate;
- gagged como reação deve soar natural;
- nenhuma frase/raciocínio pode ficar incompleta;
- conteúdo não pode migrar entre cues.

Para fix:
reasons + hint curto e concreto.

Para ok:
reasons=[]
hint=""

Responda somente pelo JSON Schema fornecido.
`;

const REPAIR_PROMPT = `
Você é o editor final EN→PT-BR.

Recebe apenas groups com problema concreto.
Corrija somente o necessário.

Mantenha:
naturalidade,
contemporaneidade,
fidelidade,
cultura,
vulgaridade
e sincronização cue a cue.

Regras:

- bitch-vocativo =
  bicha/gata/amiga/menina
  ou omitir;
  não puta automática.

- fucking/motherfucking =
  intensificador natural;
  nunca competição/lip sync/cheque da porra
  por literalidade.

- supportive =
  sempre me apoiou/
  esteve do meu lado.

- judges =
  jurados.

- judgers/judgees =
  quem julgava vai ser julgado
  ou equivalente natural.

- plucking pussy hairs =
  preserve pelo de xereca
  ou equivalente vulgar;
  nunca fio de bigode.

- shared/double win =
  vitória dupla/
  as duas ganharam;
  não empate duplo
  sem empate.

- gagged =
  Tô passada/
  Tô muito passada/
  Tô em choque
  conforme contexto.

- preserve catchphrases reconhecíveis de Drag Race.

CUE LOCK:
exatamente um pt por cue i;
mesmo i;
sem omissão
ou migração.

speaker é contexto oculto.

Sem labels,
barras,
marcadores
ou alongamentos.

Responda somente pelo JSON Schema fornecido.
`;

// ============================================================
// JSON SCHEMAS
// ============================================================

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
              }
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
// GEMINI INTERACTIONS API
// ============================================================

function parseDurationMs(
  value
) {
  const text =
    String(
      value ||
      ""
    )
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
      Number(
        ms[1]
      )
    );
  }

  const sec =
    text.match(
      /^(\d+(?:\.\d+)?)s$/
    );

  if (sec) {
    return Math.max(
      1000,
      Number(
        sec[1]
      ) *
        1000
    );
  }

  const num =
    Number(
      text
    );

  if (
    Number.isFinite(
      num
    ) &&
    num >
      0
  ) {
    return Math.max(
      1000,
      num *
        1000
    );
  }

  return null;
}

function retryDelayMs(
  response,
  data,
  attempt
) {
  const fromHeader =
    parseDurationMs(
      response
        ?.headers
        ?.get(
          "retry-after"
        )
    );

  if (
    fromHeader
  ) {
    return Math.min(
      90000,
      fromHeader +
      250
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

    if (
      parsed
    ) {
      return Math.min(
        90000,
        parsed +
        250
      );
    }
  }

  return Math.min(
    10000 *
    attempt,
    60000
  );
}

function extractInteractionText(
  data
) {
  const steps =
    Array.isArray(
      data?.steps
    )
      ? data.steps
      : [];

  let last =
    "";

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
        .join(
          ""
        );

    if (
      text
    ) {
      last =
        text;
    }
  }

  return last.trim();
}

function markAttempt(
  job,
  metric
) {
  if (!job) {
    return;
  }

  if (
    metric ===
    "main"
  ) {
    job.stats.mainAttempts++;
  }

  if (
    metric ===
    "audit"
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
  if (!job) {
    return;
  }

  if (
    metric ===
    "main"
  ) {
    job.stats.main429++;
  }

  if (
    metric ===
    "audit"
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
  if (!job) {
    return;
  }

  if (
    metric ===
    "main"
  ) {
    job.stats.mainCalls++;

    job.stats.mainInputTokens +=
      Number(
        data
          ?.usage
          ?.total_input_tokens ||
        0
      );

    job.stats.mainOutputTokens +=
      Number(
        data
          ?.usage
          ?.total_output_tokens ||
        0
      );

    job.stats.mainThoughtTokens +=
      Number(
        data
          ?.usage
          ?.total_thought_tokens ||
        0
      );
  }

  if (
    metric ===
    "audit"
  ) {
    job.stats.auditCalls++;
  }

  if (
    metric ===
    "repair"
  ) {
    job.stats.repairCalls++;
  }
}

async function geminiRequest({
  model,
  system,
  user,
  schema,
  thinkingLevel,
  maxOutputTokens,
  timeoutMs,
  maxRetries,
  job = null,
  metric = "probe"
}) {
  if (
    !GEMINI_API_KEY
  ) {
    throw new Error(
      "GEMINI_API_KEY não configurada."
    );
  }

  let lastError =
    null;

  for (
    let attempt = 1;
    attempt <=
    maxRetries;
    attempt++
  ) {
    markAttempt(
      job,
      metric
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
        `${model} request ` +
        `${attempt}/${maxRetries} | ` +
        `interactions | ` +
        `thinking=${thinkingLevel} | ` +
        `maxOutput=${maxOutputTokens}.`
      );

      // NÃO adicionar safety_settings aqui.
      const body = {
        model,

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
      };

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
                GEMINI_API_KEY
            },

            body:
              JSON.stringify(
                body
              ),

            signal:
              controller.signal
          }
        );

      const raw =
        await response.text();

      let data =
        null;

      try {
        data =
          raw
            ? JSON.parse(
                raw
              )
            : {};
      }
      catch {}

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
          ].includes(
            status
          )
        ) {
          const inner =
            data?.error?.message ||
            data?.message ||
            "interaction sem mensagem de erro";

          const e =
            new Error(
              `Gemini ${metric} ` +
              `status=${status}: ` +
              `${String(
                inner
              ).slice(
                0,
                1200
              )}`
            );

          e.nonRetryable =
            status ===
            "budget_exceeded";

          throw e;
        }

        if (
          !text
        ) {
          const e =
            new Error(
              `Gemini ${metric} ` +
              `retornou resposta vazia; ` +
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
          `GEMINI ${model} ` +
          `HTTP ${response.status}: ` +
          `${String(
            message
          ).slice(
            0,
            1800
          )}`
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
          `429; aguardando ` +
          `${(
            wait /
            1000
          ).toFixed(
            1
          )}s.`
        );

        await sleep(
          wait
        );

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
          2500 *
          attempt,
          15000
        );

      console.warn(
        `[GEMINI ${metric.toUpperCase()}] ` +
        `HTTP ${response.status}; ` +
        `retry em ` +
        `${(
          wait /
          1000
        ).toFixed(
          1
        )}s.`
      );

      await sleep(
        wait
      );
    }
    catch (
      error
    ) {
      lastError =
        error?.name ===
          "AbortError"
          ? new Error(
              `GEMINI ${model} ${metric}: ` +
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
        lastError
          ?.nonRetryable
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
          2500 *
          attempt,
          15000
        );

      console.warn(
        `[GEMINI ${metric.toUpperCase()}] ` +
        `${errorMessage(
          lastError
        ).slice(
          0,
          220
        )}; ` +
        `retry em ` +
        `${(
          wait /
          1000
        ).toFixed(
          1
        )}s.`
      );

      await sleep(
        wait
      );
    }
    finally {
      clearTimeout(
        timer
      );
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
// STARTUP SELF-TEST
// ============================================================

const SELFTEST_SCHEMA = {
  type:
    "object",

  additionalProperties:
    false,

  properties: {
    ok: {
      type:
        "string",

      enum: [
        "ok"
      ]
    }
  },

  required: [
    "ok"
  ]
};

async function probeModel(
  model,
  thinkingLevel,
  label
) {
  const result =
    await geminiRequest({
      model,

      system:
        "Responda estritamente pelo JSON Schema fornecido.",

      user:
        'Retorne {"ok":"ok"}.',

      schema:
        SELFTEST_SCHEMA,

      thinkingLevel,

      maxOutputTokens:
        128,

      timeoutMs:
        30000,

      maxRetries:
        1,

      metric:
        "selftest"
    });

  const parsed =
    JSON.parse(
      stripCodeFences(
        result.text
      )
    );

  if (
    parsed?.ok !==
    "ok"
  ) {
    throw new Error(
      `Self-test ${label}: JSON inesperado.`
    );
  }

  console.log(
    `[GEMINI SELFTEST] ${label}: ` +
    `PASSOU ✅ ` +
    `(${model}, thinking=${thinkingLevel})`
  );
}

async function runStartupSelfTests() {
  if (
    !GEMINI_API_KEY
  ) {
    console.error(
      "[GEMINI SELFTEST] NÃO EXECUTADO: GEMINI_API_KEY ausente ❌"
    );

    return;
  }

  console.log(
    "[GEMINI SELFTEST] Iniciando testes mínimos da API..."
  );

  try {
    await probeModel(
      GEMINI_TRANSLATOR_MODEL,
      MAIN_THINKING_LEVEL,
      "Tradutor 3.6"
    );

    await probeModel(
      GEMINI_AUDITOR_MODEL,
      "minimal",
      "Auditor Flash-Lite"
    );

    console.log(
      "[GEMINI SELFTEST] TUDO PASSOU — pode testar o episódio ✅"
    );
  }
  catch (
    error
  ) {
    console.error(
      `[GEMINI SELFTEST] FALHOU ❌: ` +
      `${errorMessage(
        error
      )}`
    );

    console.error(
      "[GEMINI SELFTEST] NÃO teste o episódio enquanto esta linha não passar."
    );
  }
}

// ============================================================
// TRANSLATION PARSER / RESILIENCE
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
  }
  catch {
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
      !expectedIds.has(
        id
      ) ||
      !pt
    ) {
      continue;
    }

    if (
      byId.has(
        id
      )
    ) {
      duplicates.add(
        id
      );

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
    let valid =
      true;

    const segments =
      [];

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

    if (
      valid
    ) {
      groupMap.set(
        group.groupId,
        segments
      );
    }
    else {
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
        ? `INCOMPLETE:` +
          `${groupMap.size}/` +
          `${groups.length}`
        : null
  };
}

function translatorInput(
  groups
) {
  return JSON.stringify({
    context:
      "Subtitle Sentence Groups in chronological episode order.",

    groups:
      groups.map(
        compactGroup
      )
  });
}

async function translateGroupsResilient(
  groups,
  job,
  depth = 0,
  mode = "main"
) {
  const expectedCues =
    countCues(
      groups
    );

  const isMain =
    mode ===
    "main";

  let response;

  try {
    response =
      await geminiRequest({
        model:
          GEMINI_TRANSLATOR_MODEL,

        system:
          TRANSLATOR_PROMPT,

        user:
          `Traduza TODOS os cues abaixo. ` +
          `O output deve conter exatamente ` +
          `${expectedCues} objetos de cue, ` +
          `um por id recebido.\n\n` +
          `${translatorInput(
            groups
          )}`,

        schema:
          cueTranslationSchema(
            expectedCues
          ),

        thinkingLevel:
          isMain
            ? MAIN_THINKING_LEVEL
            : "high",

        maxOutputTokens:
          isMain
            ? MAIN_MAX_OUTPUT_TOKENS
            : REPAIR_MAX_OUTPUT_TOKENS,

        timeoutMs:
          isMain
            ? MAIN_REQUEST_TIMEOUT_MS
            : REPAIR_REQUEST_TIMEOUT_MS,

        maxRetries:
          isMain
            ? MAIN_MAX_RETRIES
            : REPAIR_MAX_RETRIES,

        job,

        metric:
          isMain
            ? "main"
            : "repair"
      });
  }
  catch (
    error
  ) {
    const msg =
      errorMessage(
        error
      ).toLowerCase();

    const splittable400 =
      error?.status ===
        400 &&
      /schema|too large|size|token|request contains an invalid argument/.test(
        msg
      );

    if (
      splittable400 &&
      groups.length >
        80 &&
      depth <
        5
    ) {
      job.stats.mainSplits++;

      const middle =
        Math.ceil(
          groups.length /
          2
        );

      console.warn(
        `[GEMINI MAIN SPLIT] ` +
        `HTTP 400 no lote grande; ` +
        `${groups.length} -> ` +
        `${middle}+` +
        `${groups.length - middle}.`
      );

      const left =
        await translateGroupsResilient(
          groups.slice(
            0,
            middle
          ),

          job,

          depth +
          1,

          mode
        );

      const right =
        await translateGroupsResilient(
          groups.slice(
            middle
          ),

          job,

          depth +
          1,

          mode
        );

      return new Map([
        ...left,
        ...right
      ]);
    }

    throw error;
  }

  const parsed =
    parseCueTranslation(
      groups,
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
    groups.length >
      50 &&
    depth <
      5
  ) {
    job.stats.mainSplits++;

    const middle =
      Math.ceil(
        groups.length /
        2
      );

    console.warn(
      `[GEMINI MAIN SPLIT] ` +
      `${groups.length} groups -> ` +
      `${middle}+` +
      `${groups.length - middle}; ` +
      `motivo=` +
      `${
        incomplete
          ? "INCOMPLETE"
          : parsed.issue
      }.`
    );

    const left =
      await translateGroupsResilient(
        groups.slice(
          0,
          middle
        ),

        job,

        depth +
        1,

        mode
      );

    const right =
      await translateGroupsResilient(
        groups.slice(
          middle
        ),

        job,

        depth +
        1,

        mode
      );

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
        `Gemini não preservou estrutura ` +
        `após resgates: ` +
        `${parsed.invalidGroups.length} group(s).`
      );
    }

    job.stats.mainRescueGroups +=
      parsed.invalidGroups.length;

    console.warn(
      `[GEMINI MAIN RESCUE] ` +
      `válidos=${parsed.groupMap.size}/` +
      `${groups.length}; ` +
      `refazendo ` +
      `${parsed.invalidGroups.length} group(s).`
    );

    const rescued =
      await translateGroupsResilient(
        parsed.invalidGroups,

        job,

        depth +
        1,

        "rescue"
      );

    return new Map([
      ...parsed.groupMap,
      ...rescued
    ]);
  }

  return parsed.groupMap;
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
  }
  catch {
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
              350
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
      `${seen.size}/` +
      `${groups.length}.`
    );
  }

  return flags;
}

async function auditBatchWithModel(
  groups,
  translations,
  job,
  model,
  fallback
) {
  const response =
    await geminiRequest({
      model,

      system:
        AUDITOR_PROMPT,

      user:
        `Audite exatamente estes ` +
        `${groups.length} groups. ` +
        `Devolva um item por group.\n\n` +
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
        fallback
          ? "low"
          : AUDIT_THINKING_LEVEL,

      maxOutputTokens:
        AUDIT_MAX_OUTPUT_TOKENS,

      timeoutMs:
        AUDIT_REQUEST_TIMEOUT_MS,

      maxRetries:
        fallback
          ? 2
          : AUDIT_MAX_RETRIES,

      job,

      metric:
        "audit"
    });

  if (
    fallback
  ) {
    job.stats.auditFallbackCalls++;
  }

  return parseAudit(
    groups,
    response.text
  );
}

async function auditBatchResilient(
  groups,
  translations,
  job,
  depth = 0
) {
  try {
    return await auditBatchWithModel(
      groups,
      translations,
      job,
      GEMINI_AUDITOR_MODEL,
      false
    );
  }
  catch (
    error
  ) {
    if (
      groups.length >
        20 &&
      depth <
        3
    ) {
      const middle =
        Math.ceil(
          groups.length /
          2
        );

      console.warn(
        `[GEMINI AUDIT SPLIT] ` +
        `${groups.length} -> ` +
        `${middle}+` +
        `${groups.length - middle}; ` +
        `${errorMessage(
          error
        ).slice(
          0,
          120
        )}`
      );

      const left =
        await auditBatchResilient(
          groups.slice(
            0,
            middle
          ),

          translations,

          job,

          depth +
          1
        );

      const right =
        await auditBatchResilient(
          groups.slice(
            middle
          ),

          translations,

          job,

          depth +
          1
        );

      return new Map([
        ...left,
        ...right
      ]);
    }

    console.warn(
      `[GEMINI AUDIT FALLBACK] ` +
      `${GEMINI_AUDITOR_MODEL} falhou; ` +
      `${GEMINI_TRANSLATOR_MODEL} assume ` +
      `${groups.length} group(s).`
    );

    return auditBatchWithModel(
      groups,
      translations,
      job,
      GEMINI_TRANSLATOR_MODEL,
      true
    );
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

  let cursor =
    0;

  async function runner() {
    while (
      true
    ) {
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

async function auditGroups(
  groups,
  translations,
  job,
  phase = "primary"
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
    `[GEMINI AUDIT ${phase.toUpperCase()}] ` +
    `${groups.length} groups -> ` +
    `${batches.length} lote(s), ` +
    `concorrência=${AUDIT_CONCURRENCY}.`
  );

  const results =
    await mapWithConcurrency(
      batches,

      AUDIT_CONCURRENCY,

      async batch => {
        const flags =
          await auditBatchResilient(
            batch,
            translations,
            job
          );

        if (
          phase ===
          "primary"
        ) {
          job.stats.auditPrimaryGroups +=
            batch.length;
        }
        else {
          job.stats.auditRecheckGroups +=
            batch.length;
        }

        job.stats.auditFlagged +=
          flags.size;

        console.log(
          `[GEMINI AUDIT ${phase.toUpperCase()}] ` +
          `${batch.length} revisados; ` +
          `${flags.size} marcado(s).`
        );

        return flags;
      }
    );

  return new Map(
    results.flatMap(
      map =>
        [
          ...map.entries()
        ]
    )
  );
}

// ============================================================
// FINAL CLEAN / QUALITY GUARD
// ============================================================

function cleanFinalText(
  text
) {
  let value =
    normalizeElongations(
      String(
        text ||
        ""
      )
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
    .split(
      "\n"
    )
    .map(
      line =>
        String(
          line ||
          ""
        )
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
    .filter(
      Boolean
    )
    .join(
      "\n"
    )
    .trim();
}

function applySafeFixes(
  source,
  target
) {
  let text =
    String(
      target ||
      ""
    )
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

  const english =
    String(
      source ||
      ""
    );

  if (
    /\bWerkroom\b/i.test(
      english
    )
  ) {
    text =
      text
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
      english
    )
  ) {
    text =
      text.replace(
        /\bcondragulations\b/gi,
        "Condragulations"
      );
  }

  return text;
}

function wordCount(
  text
) {
  return (
    String(
      text ||
      ""
    ).match(
      /[\p{L}\p{N}]+(?:['’][\p{L}\p{N}]+)?/gu
    ) ||
    []
  ).length;
}

function knownIssuesForGroup(
  group,
  segments
) {
  const english =
    group.cues
      .map(
        cue =>
          cue.text
      )
      .join(
        " "
      );

  const pt =
    segments.join(
      " "
    );

  const reasons =
    new Set();

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
    /\s\/\s/u.test(
      pt
    ) ||
    /--+/u.test(
      pt
    ) ||
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

  const enWords =
    wordCount(
      english
    );

  const ptWords =
    wordCount(
      pt
    );

  if (
    enWords >=
      12 &&
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
    english.length >=
      80 &&
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

  return [
    ...reasons
  ];
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
        String(
          reason
        )
      );
  }

  if (
    hint
  ) {
    current
      .hints
      .push(
        String(
          hint
        )
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
        )
      );

    if (
      reasons.length
    ) {
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

        "Corrija sem perder sentido, naturalidade nem alinhamento por cue."
      );
    }
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
      ).join(
        " | "
      )
    );
  }

  return target;
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
        .join(
          " | "
        )
        .slice(
          0,
          700
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
              )[
                index
              ],

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
  job,
  thinkingLevel
) {
  const expectedCues =
    countCues(
      groups
    );

  const response =
    await geminiRequest({
      model:
        GEMINI_TRANSLATOR_MODEL,

      system:
        REPAIR_PROMPT,

      user:
        `Repare estes ` +
        `${groups.length} groups. ` +
        `O output deve conter exatamente ` +
        `${expectedCues} cues.\n\n` +
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
        REPAIR_REQUEST_TIMEOUT_MS,

      maxRetries:
        REPAIR_MAX_RETRIES,

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

  if (
    groups.length >
    1
  ) {
    const middle =
      Math.ceil(
        groups.length /
        2
      );

    console.warn(
      `[GEMINI REPAIR SPLIT] ` +
      `${groups.length} -> ` +
      `${middle}+` +
      `${groups.length - middle}.`
    );

    const left =
      await repairBatch(
        groups.slice(
          0,
          middle
        ),

        translations,

        issueMap,

        job,

        thinkingLevel
      );

    const right =
      await repairBatch(
        groups.slice(
          middle
        ),

        translations,

        issueMap,

        job,

        thinkingLevel
      );

    return new Map([
      ...left,
      ...right
    ]);
  }

  throw new Error(
    `Reparo estruturado falhou ` +
    `no group ${groups[0].groupId}.`
  );
}

async function repairIssueMap(
  allGroups,
  translations,
  issueMap,
  job,
  thinkingLevel =
    REPAIR_THINKING_LEVEL
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

// ============================================================
// FLATTEN
// ============================================================

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
        `Flatten inválido ` +
        `g=${group.groupId}.`
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
  texts
) {
  return texts.map(
    (
      text,
      index
    ) =>
      applySafeFixes(
        blocks[index].text,

        cleanFinalText(
          text
        )
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
// MAIN PIPELINE
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
    `[PIPELINE 6.6.2] ` +
    `fonte=${job.sourceKind} | ` +
    `${blocks.length} cues -> ` +
    `${groups.length} Sentence Groups.`
  );

  console.log(
    `[GEMINI MAIN] ` +
    `Tentativa inicial: ` +
    `EPISÓDIO INTEIRO em uma chamada ` +
    `(${groups.length} groups / ` +
    `${blocks.length} cues).`
  );

  const translations =
    await translateGroupsResilient(
      groups,
      job
    );

  if (
    translations.size !==
    groups.length
  ) {
    throw new Error(
      `Tradução principal incompleta ` +
      `${translations.size}/` +
      `${groups.length}.`
    );
  }

  let texts =
    cleanAll(
      blocks,

      flattenTranslations(
        blocks,
        groups,
        translations
      )
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

    "CHECKPOINT GEMINI 3.6"
  );

  job.progress =
    72;

  job.updatedAt =
    Date.now();

  const auditIssues =
    await auditGroups(
      groups,
      translations,
      job,
      "primary"
    );

  if (
    job.stats.auditPrimaryGroups !==
    groups.length
  ) {
    throw new Error(
      `Auditoria primária incompleta ` +
      `${job.stats.auditPrimaryGroups}/` +
      `${groups.length}.`
    );
  }

  mergeIssueMaps(
    auditIssues,

    deterministicIssueMap(
      groups,
      translations,
      job
    )
  );

  console.log(
    `[QUALITY MAP] ` +
    `auditor+guard => ` +
    `${auditIssues.size} group(s) ` +
    `para reparo dirigido.`
  );

  if (
    auditIssues.size
  ) {
    await repairIssueMap(
      groups,
      translations,
      auditIssues,
      job,
      REPAIR_THINKING_LEVEL
    );

    const repairedGroups =
      groups.filter(
        group =>
          auditIssues.has(
            group.groupId
          )
      );

    job.progress =
      90;

    job.updatedAt =
      Date.now();

    const secondIssues =
      await auditGroups(
        repairedGroups,
        translations,
        job,
        "recheck"
      );

    mergeIssueMaps(
      secondIssues,

      deterministicIssueMap(
        repairedGroups,
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
        `${secondIssues.size} group(s) ` +
        `ainda suspeito(s); ` +
        `reparo HIGH focado.`
      );

      await repairIssueMap(
        groups,
        translations,
        secondIssues,
        job,
        "high"
      );
    }
  }

  texts =
    cleanAll(
      blocks,

      flattenTranslations(
        blocks,
        groups,
        translations
      )
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
      `${residualGroups.length} group(s) ` +
      `ainda sinalizado(s); ` +
      `última correção dirigida ` +
      `sem travar por timer.`
    );

    await repairIssueMap(
      groups,
      translations,
      residual,
      job,
      "high"
    );

    texts =
      cleanAll(
        blocks,

        flattenTranslations(
          blocks,
          groups,
          translations
        )
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
  }

  if (
    residual.size
  ) {
    console.warn(
      `[QUALITY GUARD] AVISO — ` +
      `${residual.size} group(s) residual(is); ` +
      `SRT será entregue para não bloquear o sistema.`
    );
  }
  else {
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
    "FINAL 6.6.2"
  );

  const elapsed =
    (
      Date.now() -
      startedAt
    ) /
    1000;

  console.log(
    `[PIPELINE 6.6.2] OK em ` +
    `${elapsed.toFixed(1)}s | ` +

    `MainCalls=` +
    `${job.stats.mainCalls} | ` +

    `MainAttempts=` +
    `${job.stats.mainAttempts} | ` +

    `Main429=` +
    `${job.stats.main429} | ` +

    `MainSplits=` +
    `${job.stats.mainSplits} | ` +

    `MainRescueGroups=` +
    `${job.stats.mainRescueGroups} | ` +

    `MainTokens=` +
    `${job.stats.mainInputTokens}+` +
    `${job.stats.mainOutputTokens} | ` +

    `Thought=` +
    `${job.stats.mainThoughtTokens} | ` +

    `AuditCalls=` +
    `${job.stats.auditCalls} | ` +

    `Audit429=` +
    `${job.stats.audit429} | ` +

    `AuditFallback=` +
    `${job.stats.auditFallbackCalls} | ` +

    `AuditPrimary=` +
    `${job.stats.auditPrimaryGroups}/` +
    `${groups.length} | ` +

    `AuditRecheck=` +
    `${job.stats.auditRecheckGroups} | ` +

    `Flagged=` +
    `${job.stats.auditFlagged} | ` +

    `RepairCalls=` +
    `${job.stats.repairCalls} | ` +

    `Repair429=` +
    `${job.stats.repair429} | ` +

    `Repaired=` +
    `${job.stats.repairedGroups} | ` +

    `SecondPass=` +
    `${job.stats.secondPassGroups} | ` +

    `Emergency=` +
    `${job.stats.emergencyRepairGroups} | ` +

    `Residual=` +
    `${residual.size}.`
  );

  return finalSrt;
}

// ============================================================
// JOB QUEUE
// ============================================================

async function processJob(
  job
) {
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

    if (
      cached
    ) {
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
  }
  catch (
    error
  ) {
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

function enqueue(
  job
) {
  if (
    queue.some(
      item =>
        item.id ===
        job.id
    )
  ) {
    return;
  }

  queue.push(
    job
  );

  console.log(
    `[JOB QUEUE] ${job.id} entrou; ` +
    `aguardando=${queue.length}.`
  );

  runQueue();
}

async function runQueue() {
  if (
    queueRunning
  ) {
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
        await processJob(
          job
        );
      }
    }
  }
  finally {
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
  }
  finally {
    clearTimeout(
      timer
    );
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
          score(
            b
          ) -
          score(
            a
          )
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
            "Stremio-PTBR/6.6.2"
        }
      }
    );

  if (
    !response.ok
  ) {
    throw new Error(
      `OpenSubtitles HTTP ` +
      `${response.status}.`
    );
  }

  const data =
    await response.json();

  const target =
    selectEnglishSubtitle(
      data?.subtitles
    );

  if (
    !target
  ) {
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
            "Stremio-PTBR/6.6.2"
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

  if (
    !clean
  ) {
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
    "6.6.2",

  name:
    "Tradutor PT-BR Backend",

  description:
    "Backend-only: Gemini 3.6 Flash + Gemini 3.5 Flash-Lite via Interactions API.",

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

      translator:
        GEMINI_TRANSLATOR_MODEL,

      auditor:
        GEMINI_AUDITOR_MODEL,

      api:
        "INTERACTIONS",

      safetySettingsSent:
        false,

      mainThinking:
        MAIN_THINKING_LEVEL,

      strategy:
        "WHOLE_EPISODE_FIRST_TARGETED_REPAIR",

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
      !authorized(
        req
      )
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
    }
    catch (
      error
    ) {
      console.error(
        `[EMBEDDED API] ` +
        `${errorMessage(
          error
        )}`
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
      !authorized(
        req
      )
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
        `${parseSrt(
          sourceSrt
        ).length} cues.`
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
    }
    catch (
      error
    ) {
      console.error(
        `[FALLBACK API] ` +
        `${errorMessage(
          error
        )}`
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

    if (
      !job
    ) {
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

function processingSrt(
  job
) {
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
  ].join(
    "\n"
  );
}

function errorSrt(
  error
) {
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
  ].join(
    "\n"
  );
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
    }
    catch {
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

    if (
      !job
    ) {
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
      }
      catch (
        error
      ) {
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
      " STREMIO PT-BR BACKEND 6.6.2 GEMINI INTERACTIONS FIX"
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
      `Tradutor principal: ` +
      `${GEMINI_TRANSLATOR_MODEL} ✅`
    );

    console.log(
      `Auditor independente: ` +
      `${GEMINI_AUDITOR_MODEL} ✅`
    );

    console.log(
      "API Gemini: INTERACTIONS ✅"
    );

    console.log(
      "safety_settings no POST /interactions: REMOVIDO ✅"
    );

    console.log(
      "temperature/top_p/top_k: NÃO ENVIADOS ✅"
    );

    console.log(
      "Structured Output: response_format + JSON Schema ✅"
    );

    console.log(
      "Startup self-test de tradutor + auditor: ATIVO ✅"
    );

    console.log(
      "Mistral: FORA DO PIPELINE ✅"
    );

    console.log(
      "Groq: FORA DO PIPELINE ✅"
    );

    console.log(
      "Ponte Local 4.1: COMPATÍVEL; NÃO ALTERAR ✅"
    );

    console.log(
      "Tradução inicial: episódio inteiro em 1 chamada quando couber ✅"
    );

    console.log(
      `Thinking principal: ` +
      `${MAIN_THINKING_LEVEL} ✅`
    );

    console.log(
      `Auditoria: até ` +
      `${AUDIT_BATCH_GROUPS} groups, ` +
      `concorrência ${AUDIT_CONCURRENCY} ✅`
    );

    console.log(
      "Reparo: somente groups sinalizados ✅"
    );

    console.log(
      "Style Pack Drag/Reality/Gen Z/Alpha 2026: ATIVO ✅"
    );

    console.log(
      "uh-huh/uh-uh semânticos: NÃO são removidos ✅"
    );

    console.log(
      "Cue-ID lock + timestamps embedded imutáveis: ATIVOS ✅"
    );

    console.log(
      "Quality residual: tenta reparo final, mas NÃO trava por timer ✅"
    );

    console.log(
      "Pacer artificial: NÃO EXISTE ✅"
    );

    console.log(
      "Teto global do episódio: NÃO EXISTE ✅"
    );

    console.log(
      `Namespace de cache: ` +
      `${CACHE_VERSION}`
    );

    console.log(
      "Status: ONLINE"
    );

    console.log(
      "============================================================"
    );

    runStartupSelfTests();
  }
);

// ============================================================
// PROCESS SAFETY
// ============================================================

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
