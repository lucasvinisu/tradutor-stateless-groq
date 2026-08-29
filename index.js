const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

// ============================================================
// STREMIO PT-BR BACKEND 8.0 STABLE
// Gemini 3.5 Flash-Lite ONLY
// ============================================================
//
// Filosofia desta versão:
// - integração simples com a Ponte Local
// - 1 planner opcional e não-bloqueante
// - tradução principal em lotes pequenos e SEQUENCIAIS
// - timestamps nunca são gerados pelo Gemini
// - nenhuma auditoria ampla por IA
// - nenhum deep audit
// - nenhum split recursivo por 429
// - reparo opcional SOMENTE de cues objetivamente suspeitos
// - se o reparo falhar, entrega a tradução principal validada
//
// ============================================================

const app = express();

app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// CONFIG
// ============================================================

const PORT = Number(process.env.PORT || 10000);

const PUBLIC_URL = String(
  process.env.PUBLIC_URL || ""
).replace(/\/+$/, "");

const LOCAL_BRIDGE_SECRET = String(
  process.env.LOCAL_BRIDGE_SECRET || ""
).trim();

const GEMINI_API_KEY = String(
  process.env.GEMINI_API_KEY || ""
).trim();

// Fixo de propósito.
// Uma variável antiga no Render não consegue trocar o modelo.
const GEMINI_MODEL = "gemini-3.5-flash-lite";

const CACHE_VERSION = "8.0.0-stable";

const MAX_SOURCE_CHARS = 800000;

const CACHE_TTL_MS =
  7 * 24 * 60 * 60 * 1000;

const JOB_TTL_MS =
  24 * 60 * 60 * 1000;

const FETCH_TIMEOUT_MS = 25000;

// No máximo ~12 inícios de request por minuto.
// Ignora MIN_REQUEST_INTERVAL_MS antigo do Render.
const GEMINI_MIN_START_INTERVAL_MS = 5000;

// --------------------
// Planner
// --------------------

const PLAN_THINKING = "minimal";
const PLAN_MAX_OUTPUT_TOKENS = 2200;
const PLAN_TIMEOUT_MS = 60000;
const PLAN_RETRIES = 2;
const PLAN_SAMPLE_MAX_CUES = 800;

// --------------------
// Tradução principal
// --------------------

const MAIN_BATCH_MAX_CUES = 60;
const MAIN_BATCH_MAX_CHARS = 12000;

const CONTEXT_CUES_BEFORE = 6;
const CONTEXT_CUES_AFTER = 6;

const MAIN_THINKING = "low";
const MAIN_MAX_OUTPUT_TOKENS = 16000;
const MAIN_TIMEOUT_MS = 120000;

const MAIN_HTTP_RETRIES = 4;

// Se o Gemini respondeu, mas o JSON ficou estruturalmente
// inválido, repetimos exatamente o mesmo lote uma vez.
const MAIN_PARSE_ATTEMPTS = 2;

// --------------------
// Reparo opcional
// --------------------

const REPAIR_ENABLED = true;

const REPAIR_MAX_CUES_TOTAL = 50;
const REPAIR_BATCH_MAX_CUES = 25;

const REPAIR_THINKING = "medium";
const REPAIR_MAX_OUTPUT_TOKENS = 9000;
const REPAIR_TIMEOUT_MS = 90000;

const REPAIR_HTTP_RETRIES = 3;
const REPAIR_PARSE_ATTEMPTS = 2;

// ============================================================
// MEMORY
// ============================================================

const translationCache = new Map();

const jobs = new Map();

const queue = [];

let queueRunning = false;

let lastGeminiRequestStart = 0;

// ============================================================
// GENERIC HELPERS
// ============================================================

const sleep = ms =>
  new Promise(
    resolve =>
      setTimeout(resolve, ms)
  );

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

  const provided = Buffer.from(
    String(
      req.headers.authorization ||
      ""
    ).trim()
  );

  const expected = Buffer.from(
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
      `${sourceHash.slice(0, 24)}-` +
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

    // Quando a tradução principal terminar,
    // uma cópia segura ficará aqui.
    safeDraft:
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
      sourceCues:
        0,

      planCalls:
        0,

      planFailures:
        0,

      mainBatches:
        0,

      mainCalls:
        0,

      mainAttempts:
        0,

      main429:
        0,

      mainParseRetries:
        0,

      localFlags:
        0,

      repairSelected:
        0,

      repairBatches:
        0,

      repairCalls:
        0,

      repairAttempts:
        0,

      repair429:
        0,

      repairParseRetries:
        0,

      repairFailures:
        0,

      pacerWaitMs:
        0,

      inputTokens:
        0,

      outputTokens:
        0,

      thoughtTokens:
        0,

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

function getOrCreateJob(args) {
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
        [
          "completed"
        ]
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
      [
        "processing"
      ]
    );

  if (active) {
    return active;
  }

  const done =
    findJobByCache(
      key,
      [
        "completed"
      ]
    );

  if (done) {
    return done;
  }

  const job =
    createJob(args);

  enqueue(job);

  return job;
}

// Limpeza periódica.
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
      ]
      of jobs.entries()
    ) {
      if (
        job.expiresAt <= now &&
        job.status !==
          "processing"
      ) {
        jobs.delete(id);
      }
    }
  },

  10 * 60 * 1000
).unref();

// ============================================================
// SRT CLEAN / PARSE
// ============================================================

const TIMING_RE =
  /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
  /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
  /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough|speaking indistinctly|speaks? indistinctly/i;

function normalizeSpeaker(value) {
  const speaker =
    String(value || "")
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
    speaker.length > 60 ||
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

function looksLikeSpeakerLabel(
  value
) {
  const speaker =
    normalizeSpeaker(
      value
    );

  if (!speaker) {
    return false;
  }

  const parts =
    speaker
      .split(/\s+/)
      .filter(Boolean);

  if (
    !parts.length ||
    parts.length > 4
  ) {
    return false;
  }

  // Evita interpretar:
  // "Okay: let's go"
  // como se "Okay" fosse nome de speaker.
  if (
    /^(?:okay|ok|well|look|listen|so|now|then|actually|basically|because|but|and|or|yes|no|right|wait|hey|wow|girl|bitch|previously|meanwhile|later|earlier|tonight|today|tomorrow)$/i.test(
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
        ) ||
        /^[A-ZÀ-Ý][a-zà-ÿ]+[A-Z][A-Za-zÀ-ÿ]*$/u.test(
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
    String(line || "");

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
    } catch {}

    const clean =
      original.replace(
        SPEAKER_RE,
        ""
      );

    return {
      speaker,

      text:
        clean,

      hadDialogueDash:
        /^\s*[-–—]\s*/u.test(
          clean
        )
    };
  }

  // [RUPAUL] fala
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

  // RUPAUL: fala
  const colon =
    original.match(
      /^\s*([-–—]\s*)?([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,45})\s*:\s+(?=\S)/u
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
          original.slice(
            colon[0].length
          ),

        hadDialogueDash:
          Boolean(
            colon[1]
          )
      };
    }
  }

  return {
    speaker:
      "",

    text:
      original,

    hadDialogueDash:
      /^\s*[-–—]\s*/u.test(
        original
      )
  };
}

function normalizeElongations(
  text
) {
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
      /([aeiouáéíóúãõâêô])\1{4,}/giu,
      "$1"
    );
}

// uh-huh / uh-uh não são removidos,
// pois podem ter sentido de sim/não.
function isEmptyVocalization(
  text
) {
  const value =
    String(text || "")
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

  return /^(?:ah|ha|heh)(?:\s+(?:ah|ha|heh)){1,6}$/.test(
    value
  );
}

function removeSdhSegments(
  text
) {
  return String(text || "")
    .replace(
      /\[([^\]]+)\]/gu,

      (
        match,
        inside
      ) =>
        SDH_WORDS.test(
          String(
            inside || ""
          )
        )
          ? " "
          : match
    )
    .replace(
      /\(([^)]+)\)/gu,

      (
        match,
        inside
      ) =>
        SDH_WORDS.test(
          String(
            inside || ""
          )
        )
          ? " "
          : match
    );
}

function cleanSourceLine(line) {
  let text =
    String(line || "")
      .trim();

  if (!text) {
    return "";
  }

  text =
    removeSdhSegments(
      text
    );

  // Remove tags de formatação.
  text =
    text
      .replace(
        /<[^>]+>/g,
        ""
      )
      .replace(
        /\{\\[^}]+\}/g,
        " "
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
    normalizeSrt(srt);

  if (!normalized) {
    return "";
  }

  const rawBlocks =
    normalized
      .split(
        /\n{2,}/
      )
      .filter(Boolean);

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
        .split("\n");

    const timingIndex =
      lines.findIndex(
        line =>
          /-->/.test(
            line
          )
      );

    if (
      timingIndex < 0
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
        timingIndex + 1
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
          info.text || ""
        ).trim();

      let cleaned =
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

      if (!cleaned) {
        continue;
      }

      // Se era fala marcada por hífen,
      // mantém a distinção de speaker.
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

    if (
      !dialogue.length
    ) {
      removed++;

      continue;
    }

    // Speaker único vira apenas pista oculta.
    // Ele nunca será mostrado na legenda final.
    if (
      speakers.size === 1
    ) {
      const speaker =
        [
          ...speakers
        ][0];

      dialogue[0] =
        `@@SPK:${encodeURIComponent(
          speaker
        )}@@ ${dialogue[0]}`;

      speakerHints++;
    }

    out.push({
      timing,
      dialogue
    });
  }

  console.log(
    `[CLEAN] ` +
    `${rawBlocks.length} -> ${out.length}; ` +
    `removidos=${removed}; ` +
    `speakerHints=${speakerHints}; ` +
    `vocalizações=${vocalizations}.`
  );

  if (!out.length) {
    return "";
  }

  // Renumera apenas os cues válidos.
  // Os timestamps continuam exatamente iguais.
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

    if (
      textLines.length
    ) {
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

function buildSrt(
  blocks,
  translations
) {
  return (
    blocks
      .map(
        block => {
          const text =
            translations instanceof
              Map
              ? translations.get(
                  block.index
                )
              : undefined;

          return [
            block.index,

            block.timing,

            String(
              text ??
              block.text
            ).trim()
          ].join("\n");
        }
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
        `TIMING LOCK ${label}: ` +
        `cue ${source[i].index}.`
      );
    }
  }

  console.log(
    `[TIMING LOCK] ${label}: PASSOU — ` +
    `${source.length}/${source.length}; ` +
    `0 alterações.`
  );
}

// ============================================================
// PROMPTS
// ============================================================

const STYLE_PACK = `
PORTUGUÊS BRASILEIRO NATURAL — REGRAS EDITORIAIS

OBJETIVO

Traduza como uma legenda profissional brasileira de entretenimento.

Priorize:
- sentido;
- naturalidade oral;
- personalidade;
- coerência;
- concisão.

Nunca faça "inglês vestido de português".

NATURALIDADE

- Em fala casual, use português brasileiro realmente falado.
- "tô", "tá", "pra", "né" podem ser usados quando combinarem com o personagem.
- Em fala formal, preserve formalidade.
- Não force gíria jovem em quem não fala assim.
- Não use lusitanismos.
- Não use linguagem burocrática ou artificial.
- Não traduza expressão idiomática palavra por palavra.
- Preserve humor, ironia, sarcasmo, flerte, shade e intensidade.

FIDELIDADE

- Não resuma.
- Não invente informação.
- Não omita o final de uma frase.
- Não mova conteúdo de um cue para outro.
- Cada id recebido precisa de exatamente uma tradução correspondente.

PALAVRÃO

- Não censure.
- Preserve a intensidade de modo brasileiro natural.
- Não transforme automaticamente todo "fucking" em "da porra" ou "do caralho".
- Escolha a posição em que um brasileiro realmente colocaria a ênfase.

DRAG / REALITY / POP / MODA
SOMENTE QUANDO O CONTEXTO PEDIR

- bitch como vocativo amigável:
  bicha, gata, amiga, menina ou omitir.
  Não "puta" automaticamente.

- gagged como reação:
  tô passada,
  tô em choque,
  tô sem reação.
  "amordaçada" apenas quando o sentido for físico.

- she ate / you ate como gíria:
  arrasou,
  entregou tudo,
  serviu.
  Nunca "comeu" literalmente nesse sentido.

- no crumbs:
  não deixou nada pra ninguém,
  quando couber.

- judges em competição/reality:
  jurados.

- supportive:
  "me apoiou muito",
  "esteve do meu lado".
  Evite "super apoiador".

- fucking lip sync:
  "um puta lip sync",
  "um lip sync foda"
  ou equivalente natural.

- shared/double win:
  vitória dupla /
  as duas ganharam.
  Não "empate duplo" sem empate.

- week one:
  primeira semana.

Preserve quando forem termos ou catchphrases reconhecíveis:

- Werkroom
- Condragulations
- Shantay, you stay
- Sashay away
- You betta werk

GÊNERO

- Use speaker apenas como pista contextual.
- Se gênero não estiver claro, prefira reformular naturalmente em vez de inventar.
- Nunca escreva:
  ele/ela
  ela/ele
  animado(a)
  empolgado(a)

FORMATO

- Não crie timestamps.
- Não crie ids diferentes.
- Não adicione nome de speaker.
- Não adicione [NOME].
- Não adicione "NOME:".
- Não adicione SDH/CC.
- Preserve quebra de linha quando ela separar duas falas dentro do mesmo cue.
- Use "- " apenas quando o próprio cue claramente contiver duas falas.
`;

const PLAN_PROMPT = `
Você é editor de continuidade de legendas EN→PT-BR.

Leia uma amostra ampla do episódio e produza uma BÍBLIA EDITORIAL CURTA.

Ela será usada por outra instância do mesmo modelo para manter coerência.

Extraia SOMENTE o que ajudar a tradução:

- tom geral;
- nomes e relações quando claras;
- gênero quando realmente claro;
- termos recorrentes;
- referências de fandom/cultura;
- catchphrases;
- escolhas de tradução que precisam permanecer consistentes.

Não traduza o episódio.

Não invente fatos.

Se algo não estiver claro, não inclua.

Seja curto.
`;

const TRANSLATOR_PROMPT = `
Você é o tradutor principal de legendas EN→PT-BR.

${STYLE_PACK}

Você receberá:

1) uma bíblia editorial curta;
2) contexto anterior;
3) cues ALVO;
4) contexto posterior.

TRADUZA SOMENTE os cues ALVO.

Contexto anterior/posterior existe apenas para compreender continuidade.

NUNCA devolva cues de contexto.

NUNCA use conteúdo do cue seguinte para completar artificialmente o cue atual.

NUNCA antecipe fala futura.

Para cada cue alvo, devolva exatamente:

- o mesmo id em "i";
- a tradução em "pt".

Se um cue tiver duas falas em linhas separadas, preserve a separação dentro de "pt".
`;

const REPAIR_PROMPT = `
Você é o editor final de uma legenda EN→PT-BR.

${STYLE_PACK}

Você receberá SOMENTE cues que um detector local marcou por sinais objetivos.

Cada item contém:
- EN;
- PT atual;
- motivo(s);
- contexto curto.

Corrija apenas defeitos reais.

Se a tradução atual já estiver correta, preserve o sentido e faça apenas o mínimo necessário.

Não redistribua conteúdo entre ids.

Devolva exatamente um "pt" para cada "i" recebido.
`;

// ============================================================
// JSON SCHEMAS
// ============================================================

const PLAN_SCHEMA = {
  type:
    "object",

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
        25
    },

    glossary: {
      type:
        "array",

      items: {
        type:
          "string"
      },

      maxItems:
        35
    },

    continuity: {
      type:
        "array",

      items: {
        type:
          "string"
      },

      maxItems:
        25
    }
  },

  required: [
    "tone",
    "people",
    "glossary",
    "continuity"
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

// ============================================================
// GEMINI INTERACTIONS API
// ============================================================

function parseDurationMs(
  value
) {
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
    Number(text);

  if (
    Number.isFinite(num) &&
    num > 0
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
      120000,

      header +
      500
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
        detail?.metadata?.retryDelay ||
        detail?.metadata?.retry_delay
      );

    if (parsed) {
      return Math.min(
        120000,

        parsed +
        500
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
  // SDKs usam output_text.
  // REST normalmente retorna steps.
  // Suportamos ambos.
  if (
    typeof data?.output_text ===
      "string" &&
    data.output_text.trim()
  ) {
    return data.output_text.trim();
  }

  const steps =
    Array.isArray(
      data?.steps
    )
      ? data.steps
      : [];

  let out =
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
        .join("");

    if (text) {
      out += text;
    }
  }

  return out.trim();
}

async function waitGeminiPacer(
  job
) {
  const elapsed =
    Date.now() -
    lastGeminiRequestStart;

  const wait =
    Math.max(
      0,

      GEMINI_MIN_START_INTERVAL_MS -
      elapsed
    );

  if (
    wait > 0
  ) {
    if (job) {
      job.stats.pacerWaitMs +=
        wait;
    }

    console.log(
      `[GEMINI PACER] ` +
      `aguardando ` +
      `${(wait / 1000).toFixed(1)}s.`
    );

    await sleep(
      wait
    );
  }

  lastGeminiRequestStart =
    Date.now();
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
    attempt <= maxRetries;
    attempt++
  ) {
    markAttempt(
      job,
      metric
    );

    await waitGeminiPacer(
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
                GEMINI_API_KEY
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

      let data =
        null;

      try {
        data =
          raw
            ? JSON.parse(
                raw
              )
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
          )
            .toLowerCase();

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
            data
              ?.error
              ?.message ||
            data?.message ||
            "interaction sem detalhe";

          const error =
            new Error(
              `Gemini ${metric} ` +
              `status=${status}: ` +
              `${String(inner).slice(0, 1200)}`
            );

          error.nonRetryable =
            status ===
            "budget_exceeded";

          throw error;
        }

        if (
          status ===
            "incomplete" ||
          !text
        ) {
          const error =
            new Error(
              status ===
                "incomplete"
                ? `Gemini ${metric} retornou INCOMPLETE.`
                : `Gemini ${metric} retornou resposta vazia.`
            );

          error.retryable =
            true;

          throw error;
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
        data?.message ||
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

      // -----------------------------------------
      // 429
      // -----------------------------------------
      //
      // NUNCA divide o lote.
      // Aguarda e repete exatamente a mesma coisa.
      //
      // -----------------------------------------

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
          `429; repetindo O MESMO lote em ` +
          `${(wait / 1000).toFixed(1)}s.`
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
          4000 *
          attempt,

          20000
        );

      console.warn(
        `[GEMINI ${metric.toUpperCase()}] ` +
        `HTTP ${response.status}; ` +
        `repetindo O MESMO lote em ` +
        `${(wait / 1000).toFixed(1)}s.`
      );

      await sleep(
        wait
      );
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
        lastError
          ?.nonRetryable
      ) {
        throw lastError;
      }

      // Erro 4xx real não é motivo para
      // ficar criando lotes menores.
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
          4000 *
          attempt,

          20000
        );

      console.warn(
        `[GEMINI ${metric.toUpperCase()}] ` +
        `${errorMessage(lastError).slice(0, 220)}; ` +
        `repetindo O MESMO lote em ` +
        `${(wait / 1000).toFixed(1)}s.`
      );

      await sleep(
        wait
      );
    } finally {
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
// PLANNER
// ============================================================

function compactCue(
  block
) {
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

function plannerSample(
  blocks
) {
  if (
    blocks.length <=
    PLAN_SAMPLE_MAX_CUES
  ) {
    return blocks.map(
      compactCue
    );
  }

  const out =
    [];

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
          i *
          step
        )
      );

    if (
      used.has(
        index
      )
    ) {
      continue;
    }

    used.add(
      index
    );

    out.push(
      compactCue(
        blocks[index]
      )
    );
  }

  return out;
}

function fallbackPlan() {
  return {
    tone:
      "Português brasileiro natural; preserve o registro e a personalidade do conteúdo.",

    people:
      [],

    glossary:
      [],

    continuity:
      []
  };
}

async function buildEpisodePlan(
  blocks,
  job
) {
  try {
    const response =
      await geminiRequest({
        system:
          PLAN_PROMPT,

        user:
          `Arquivo: ${job.filename || "desconhecido"}\n` +
          `Tipo: ${job.type}\n` +
          `ID: ${job.videoId}\n\n` +
          `Crie uma bíblia editorial CURTA a partir desta amostra do episódio:\n\n` +
          JSON.stringify({
            cues:
              plannerSample(
                blocks
              )
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
          PLAN_RETRIES,

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
    job.stats.planFailures++;

    console.warn(
      `[EPISODE PLAN] ` +
      `Falhou sem bloquear o episódio: ` +
      `${errorMessage(error).slice(0, 300)}`
    );

    return fallbackPlan();
  }
}

// ============================================================
// MAIN BATCHING
// ============================================================

function buildMainBatches(
  blocks
) {
  const batches =
    [];

  let current =
    [];

  let chars =
    0;

  for (
    const block
    of blocks
  ) {
    const size =
      block.text.length +
      80;

    if (
      current.length &&
      (
        current.length >=
          MAIN_BATCH_MAX_CUES ||
        chars +
          size >
          MAIN_BATCH_MAX_CHARS
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
      block
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

function findBlockPositionMap(
  blocks
) {
  const map =
    new Map();

  blocks.forEach(
    (
      block,
      index
    ) =>
      map.set(
        block.index,
        index
      )
  );

  return map;
}

function contextPayload(
  allBlocks,
  positionMap,
  targetBatch,
  translations
) {
  const firstPos =
    positionMap.get(
      targetBatch[0].index
    );

  const lastPos =
    positionMap.get(
      targetBatch[
        targetBatch.length -
        1
      ].index
    );

  const before =
    allBlocks
      .slice(
        Math.max(
          0,

          firstPos -
          CONTEXT_CUES_BEFORE
        ),

        firstPos
      )
      .map(
        block => ({
          ...compactCue(
            block
          ),

          ...(
            translations.has(
              block.index
            )
              ? {
                  // Contexto anterior já traduzido
                  // também ajuda na coerência.
                  pt:
                    translations.get(
                      block.index
                    )
                }
              : {}
          )
        })
      );

  const target =
    targetBatch.map(
      compactCue
    );

  const after =
    allBlocks
      .slice(
        lastPos +
        1,

        Math.min(
          allBlocks.length,

          lastPos +
          1 +
          CONTEXT_CUES_AFTER
        )
      )
      .map(
        compactCue
      );

  return {
    before,
    target,
    after
  };
}

// ============================================================
// MAIN RESPONSE VALIDATION
// ============================================================

function parseCueTranslation(
  targetBatch,
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
      "JSON de tradução inválido."
    );
  }

  if (
    !Array.isArray(
      parsed?.cues
    )
  ) {
    throw new Error(
      "Resposta de tradução sem cues."
    );
  }

  const expectedIds =
    targetBatch.map(
      block =>
        block.index
    );

  const expectedSet =
    new Set(
      expectedIds
    );

  const byId =
    new Map();

  for (
    const item
    of parsed.cues
  ) {
    const id =
      Number(
        item?.i
      );

    const pt =
      String(
        item?.pt ??
        ""
      ).trim();

    if (
      !expectedSet.has(
        id
      )
    ) {
      throw new Error(
        `Tradução devolveu id inesperado: ${id}.`
      );
    }

    if (
      byId.has(
        id
      )
    ) {
      throw new Error(
        `Tradução duplicou cue ${id}.`
      );
    }

    if (!pt) {
      throw new Error(
        `Tradução vazia no cue ${id}.`
      );
    }

    byId.set(
      id,
      pt
    );
  }

  if (
    byId.size !==
    expectedIds.length
  ) {
    const missing =
      expectedIds.filter(
        id =>
          !byId.has(
            id
          )
      );

    throw new Error(
      `Tradução incompleta: ` +
      `${byId.size}/${expectedIds.length}; ` +
      `faltando=${missing.slice(0, 12).join(",")}.`
    );
  }

  return byId;
}

// ============================================================
// MAIN TRANSLATION
// ============================================================

async function translateMainBatch({
  allBlocks,
  positionMap,
  targetBatch,
  translations,
  plan,
  job
}) {
  let lastError =
    null;

  for (
    let parseAttempt = 1;
    parseAttempt <=
      MAIN_PARSE_ATTEMPTS;
    parseAttempt++
  ) {
    try {
      const payload =
        contextPayload(
          allBlocks,
          positionMap,
          targetBatch,
          translations
        );

      const response =
        await geminiRequest({
          system:
            TRANSLATOR_PROMPT,

          user:
            `BÍBLIA EDITORIAL:\n` +
            `${JSON.stringify(plan)}\n\n` +

            `CONTEXTO E CUES:\n` +
            `${JSON.stringify(payload)}\n\n` +

            `Traduza SOMENTE target.\n` +
            `O output deve conter exatamente ` +
            `${targetBatch.length} cues.`,

          schema:
            cueTranslationSchema(
              targetBatch.length
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
        targetBatch,
        response.text
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
        `[MAIN VALIDATION] ` +
        `lote inválido; ` +
        `repetindo O MESMO lote ` +
        `(${parseAttempt + 1}/${MAIN_PARSE_ATTEMPTS}): ` +
        `${errorMessage(error).slice(0, 260)}`
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

  const positionMap =
    findBlockPositionMap(
      blocks
    );

  job.stats.mainBatches =
    batches.length;

  console.log(
    `[MAIN] ${blocks.length} cues -> ` +
    `${batches.length} lote(s) SEQUENCIAIS | ` +
    `máx=${MAIN_BATCH_MAX_CUES} cues/lote | ` +
    `pacer=${GEMINI_MIN_START_INTERVAL_MS}ms.`
  );

  let translatedExpected =
    0;

  for (
    let i = 0;
    i < batches.length;
    i++
  ) {
    const batch =
      batches[i];

    console.log(
      `[MAIN] lote ${i + 1}/${batches.length}: ` +
      `${batch.length} cue(s).`
    );

    const translated =
      await translateMainBatch({
        allBlocks:
          blocks,

        positionMap,

        targetBatch:
          batch,

        translations,

        plan,

        job
      });

    for (
      const [
        id,
        pt
      ]
      of translated
    ) {
      translations.set(
        id,
        pt
      );
    }

    translatedExpected +=
      batch.length;

    if (
      translations.size !==
      translatedExpected
    ) {
      throw new Error(
        `Contagem interna de tradução inconsistente: ` +
        `${translations.size}/${translatedExpected}.`
      );
    }

    job.progress =
      Math.min(
        90,

        5 +
        Math.round(
          85 *
          (
            (i + 1) /
            batches.length
          )
        )
      );

    job.updatedAt =
      Date.now();

    console.log(
      `[MAIN] lote ${i + 1}/${batches.length} OK | ` +
      `traduzidos=${translations.size}/${blocks.length} | ` +
      `progresso=${job.progress}%.`
    );
  }

  if (
    translations.size !==
    blocks.length
  ) {
    throw new Error(
      `Tradução principal incompleta: ` +
      `${translations.size}/${blocks.length}.`
    );
  }

  return translations;
}

// ============================================================
// LOCAL QUALITY GUARDS
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
            .normalize(
              "NFD"
            )
            .replace(
              /[\u0300-\u036f]/g,
              ""
            )
      )
      .filter(
        value =>
          value.length >
          2
      )
  );
}

function copiedEnglishRatio(
  en,
  pt
) {
  const enWords =
    normalizedWordSet(
      en
    );

  const ptWords =
    normalizedWordSet(
      pt
    );

  if (
    !enWords.size
  ) {
    return 0;
  }

  let copied =
    0;

  for (
    const word
    of enWords
  ) {
    if (
      ptWords.has(
        word
      )
    ) {
      copied++;
    }
  }

  return (
    copied /
    enWords.size
  );
}

function isDragContext(
  filename,
  en
) {
  return (
    /rupaul|drag[ ._-]*race|dragula/i.test(
      String(
        filename || ""
      )
    ) ||
    /\bwerkroom\b|\blip sync\b|\bshantay\b|\bsashay\b|\bcondragulations\b/i.test(
      String(
        en || ""
      )
    )
  );
}

function localReasonsForCue(
  block,
  pt,
  filename
) {
  const en =
    String(
      block.text ||
      ""
    );

  const translated =
    String(
      pt ||
      ""
    );

  const reasons =
    [];

  const enWordCount =
    words(
      en
    ).length;

  const ptWordCount =
    words(
      translated
    ).length;

  // Vazio.
  if (
    !translated.trim()
  ) {
    reasons.push(
      "EMPTY"
    );
  }

  // Grande chance de trecho não traduzido.
  if (
    enWordCount >= 5 &&
    copiedEnglishRatio(
      en,
      translated
    ) >=
      0.60
  ) {
    reasons.push(
      "POSSIBLE_UNTRANSLATED"
    );
  }

  // Tradução extremamente menor
  // que um EN relativamente longo.
  if (
    enWordCount >= 12 &&
    ptWordCount <=
      Math.max(
        2,

        Math.floor(
          enWordCount *
          0.28
        )
      )
  ) {
    reasons.push(
      "POSSIBLE_OMISSION"
    );
  }

  const drag =
    isDragContext(
      filename,
      en
    );

  if (drag) {
    // gagged literal
    if (
      /\bgagged\b/i.test(
        en
      ) &&
      /\bamordaçad[oa]s?\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_GAGGED"
      );
    }

    // she ate literal
    if (
      /\b(?:she|he|you|they)\s+ate(?:\s+that)?\b/i.test(
        en
      ) &&
      /\b(?:comeu|comeram|comeste|comeram isso)\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "LITERAL_ATE"
      );
    }

    // judges -> juízes
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

    // supportive -> super apoiador
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

    // palavrão mecanicamente encaixado.
    if (
      /\b(?:competição|competicao|lip sync|cheque)\s+(?:da porra|do caralho)\b/i.test(
        translated
      )
    ) {
      reasons.push(
        "AWKWARD_PROFANITY"
      );
    }

    // double/shared win
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

    // Caso específico já observado.
    if (
      /plucking\s+pussy\s+hairs?/i.test(
        en
      ) &&
      /fio\s+de\s+bigode/i.test(
        translated
      )
    ) {
      reasons.push(
        "CULTURE_BODY_MISS"
      );
    }
  }

  return [
    ...new Set(
      reasons
    )
  ];
}

function detectLocalIssues(
  blocks,
  translations,
  filename
) {
  const issues =
    [];

  for (
    const block
    of blocks
  ) {
    const pt =
      translations.get(
        block.index
      );

    const reasons =
      localReasonsForCue(
        block,
        pt,
        filename
      );

    if (
      !reasons.length
    ) {
      continue;
    }

    issues.push({
      id:
        block.index,

      reasons
    });
  }

  return issues;
}

// ============================================================
// OPTIONAL FOCUSED REPAIR
// ============================================================

function repairItemPayload(
  blocks,
  positionMap,
  translations,
  issue
) {
  const pos =
    positionMap.get(
      issue.id
    );

  const block =
    blocks[pos];

  const before =
    blocks
      .slice(
        Math.max(
          0,

          pos -
          2
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
            ) ||
            ""
        })
      );

  const after =
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
            ) ||
            ""
        })
      );

  return {
    i:
      block.index,

    en:
      block.text,

    pt:
      translations.get(
        block.index
      ),

    reasons:
      issue.reasons,

    ...(
      block.speakerHint
        ? {
            speaker:
              block.speakerHint
          }
        : {}
    ),

    before,

    after
  };
}

async function repairOneBatch({
  blocks,
  positionMap,
  translations,
  issues,
  plan,
  job
}) {
  let lastError =
    null;

  for (
    let parseAttempt = 1;
    parseAttempt <=
      REPAIR_PARSE_ATTEMPTS;
    parseAttempt++
  ) {
    try {
      const response =
        await geminiRequest({
          system:
            REPAIR_PROMPT,

          user:
            `BÍBLIA EDITORIAL:\n` +
            `${JSON.stringify(plan)}\n\n` +

            `Revise somente estes cues sinalizados:\n\n` +

            JSON.stringify({
              cues:
                issues.map(
                  issue =>
                    repairItemPayload(
                      blocks,
                      positionMap,
                      translations,
                      issue
                    )
                )
            }),

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

      const targetBlocks =
        issues.map(
          issue =>
            blocks[
              positionMap.get(
                issue.id
              )
            ]
        );

      return parseCueTranslation(
        targetBlocks,
        response.text
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
        `[REPAIR VALIDATION] ` +
        `resposta inválida; ` +
        `repetindo O MESMO lote: ` +
        `${errorMessage(error).slice(0, 260)}`
      );
    }
  }

  throw lastError;
}

async function tryFocusedRepair(
  blocks,
  translations,
  plan,
  job
) {
  if (
    !REPAIR_ENABLED
  ) {
    return translations;
  }

  let issues;

  try {
    issues =
      detectLocalIssues(
        blocks,
        translations,
        job.filename
      );
  } catch (error) {
    console.warn(
      `[LOCAL GUARD] ` +
      `Falhou; mantendo tradução principal: ` +
      `${errorMessage(error).slice(0, 260)}`
    );

    return translations;
  }

  job.stats.localFlags =
    issues.length;

  if (
    !issues.length
  ) {
    console.log(
      "[LOCAL GUARD] 0 cues suspeitos. Nenhum reparo necessário."
    );

    return translations;
  }

  // Os que têm mais de um sinal objetivo
  // vêm primeiro.
  issues.sort(
    (
      a,
      b
    ) =>
      b.reasons.length -
      a.reasons.length
  );

  const selected =
    issues.slice(
      0,

      REPAIR_MAX_CUES_TOTAL
    );

  job.stats.repairSelected =
    selected.length;

  console.log(
    `[LOCAL GUARD] ` +
    `${issues.length} cue(s) suspeito(s); ` +
    `${selected.length} selecionado(s) ` +
    `para reparo opcional.`
  );

  const batches =
    [];

  for (
    let i = 0;
    i < selected.length;
    i +=
      REPAIR_BATCH_MAX_CUES
  ) {
    batches.push(
      selected.slice(
        i,

        i +
        REPAIR_BATCH_MAX_CUES
      )
    );
  }

  job.stats.repairBatches =
    batches.length;

  const positionMap =
    findBlockPositionMap(
      blocks
    );

  // Trabalha numa cópia.
  // A tradução principal continua intacta.
  const updated =
    new Map(
      translations
    );

  try {
    for (
      let i = 0;
      i < batches.length;
      i++
    ) {
      const batch =
        batches[i];

      console.log(
        `[REPAIR] lote ${i + 1}/${batches.length}: ` +
        `${batch.length} cue(s).`
      );

      const repaired =
        await repairOneBatch({
          blocks,

          positionMap,

          translations:
            updated,

          issues:
            batch,

          plan,

          job
        });

      for (
        const [
          id,
          pt
        ]
        of repaired
      ) {
        updated.set(
          id,
          pt
        );
      }
    }

    console.log(
      `[REPAIR] concluído; ` +
      `${selected.length} cue(s) revisado(s).`
    );

    return updated;
  } catch (error) {
    job.stats.repairFailures++;

    console.warn(
      `[REPAIR] Falhou sem matar o episódio. ` +
      `Mantendo tradução principal: ` +
      `${errorMessage(error).slice(0, 400)}`
    );

    // FUNDAMENTAL:
    // não joga fora a tradução principal.
    return translations;
  }
}

// ============================================================
// TRANSLATION PIPELINE
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

  job.stats.sourceCues =
    blocks.length;

  console.log(
    `[PIPELINE 8.0] ` +
    `fonte=${job.sourceKind} | ` +
    `${blocks.length} cues.`
  );

  // ========================================================
  // 1) PLANNER
  //
  // Opcional.
  // Se falhar, buildEpisodePlan devolve fallbackPlan.
  // ========================================================

  const plan =
    await buildEpisodePlan(
      blocks,
      job
    );

  job.progress =
    5;

  // ========================================================
  // 2) TRADUÇÃO PRINCIPAL
  //
  // Esta é a única fase obrigatória de IA.
  // ========================================================

  const mainTranslations =
    await translateAllMain(
      blocks,
      plan,
      job
    );

  const mainSrt =
    buildSrt(
      blocks,
      mainTranslations
    );

  // Timestamp lock ANTES de qualquer reparo.
  auditTimestamps(
    sourceSrt,
    mainSrt,
    "MAIN"
  );

  // ========================================================
  // SAFE DRAFT
  //
  // A partir daqui existe uma legenda completa,
  // traduzida e sincronizada.
  //
  // Nenhuma etapa opcional posterior poderá destruir isso.
  // ========================================================

  job.safeDraft =
    mainSrt;

  job.progress =
    92;

  job.updatedAt =
    Date.now();

  console.log(
    `[SAFE DRAFT] ` +
    `tradução principal validada e protegida | ` +
    `${blocks.length}/${blocks.length} cues.`
  );

  // ========================================================
  // 3) REPARO LOCAL FOCADO
  //
  // Opcional.
  // Só cues objetivamente suspeitos.
  // ========================================================

  const finalTranslations =
    await tryFocusedRepair(
      blocks,
      mainTranslations,
      plan,
      job
    );

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

  job.progress =
    99;

  console.log(
    `[PIPELINE 8.0] FINAL OK | ` +
    `${blocks.length} cues | ` +
    `${(
      (
        Date.now() -
        startedAt
      ) /
      1000
    ).toFixed(1)}s.`
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
    `Iniciando fonte=${job.sourceKind}.`
  );

  try {
    // -----------------------
    // CACHE
    // -----------------------

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

      console.log(
        `[JOB ${job.id}] Cache válido.`
      );

      return;
    }

    // -----------------------
    // PIPELINE
    // -----------------------

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
    // ======================================================
    // ÚLTIMO CINTO DE SEGURANÇA
    //
    // Se a tradução principal terminou,
    // nenhum erro posterior mata o episódio.
    // ======================================================

    if (
      job.safeDraft
    ) {
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

        job.error =
          null;

        job
          .stats
          .usedSafeDraftFallback =
          true;

        console.warn(
          `[JOB ${job.id}] ` +
          `Etapa opcional falhou, ` +
          `mas SAFE DRAFT foi entregue: ` +
          `${errorMessage(error).slice(0, 400)}`
        );

        return;
      } catch (
        draftError
      ) {
        console.error(
          `[JOB ${job.id}] ` +
          `SAFE DRAFT também falhou validação: ` +
          `${errorMessage(draftError)}`
        );
      }
    }

    // Só chega aqui se a própria tradução
    // principal não conseguiu terminar.
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
  } finally {
    job.updatedAt =
      Date.now();
  }
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
            "Stremio-PTBR/8.0"
        }
      }
    );

  if (
    !response.ok
  ) {
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
            "Stremio-PTBR/8.0"
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
// ROUTE HELPERS
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
    "8.0.0",

  name:
    "Tradutor PT-BR Backend",

  description:
    "Backend-only estável: Gemini 3.5 Flash-Lite, lotes sequenciais e timestamps imutáveis.",

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

  catalogs:
    [],

  behaviorHints: {
    configurable:
      false
  }
};

// ============================================================
// PUBLIC / STATUS ROUTES
// ============================================================

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

      architecture:
        "SEQUENTIAL_STABLE",

      mainBatchMaxCues:
        MAIN_BATCH_MAX_CUES,

      mainBatchMaxChars:
        MAIN_BATCH_MAX_CHARS,

      contextBefore:
        CONTEXT_CUES_BEFORE,

      contextAfter:
        CONTEXT_CUES_AFTER,

      pacerMs:
        GEMINI_MIN_START_INTERVAL_MS,

      repairEnabled:
        REPAIR_ENABLED,

      queue:
        queue.length,

      processing:
        queueRunning
    })
);

app.get(
  "/stats.json",

  (
    req,
    res
  ) => {
    const processing =
      [
        ...jobs.values()
      ].filter(
        job =>
          job.status ===
          "processing"
      ).length;

    const completed =
      [
        ...jobs.values()
      ].filter(
        job =>
          job.status ===
          "completed"
      ).length;

    const failed =
      [
        ...jobs.values()
      ].filter(
        job =>
          job.status ===
          "failed"
      ).length;

    return safeJson(
      res,

      {
        status:
          "online",

        version:
          manifest.version,

        model:
          GEMINI_MODEL,

        cache:
          translationCache.size,

        jobs: {
          processing,
          completed,
          failed,

          queued:
            queue.length
        },

        queueRunning
      }
    );
  }
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
      subtitles:
        []
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
// SRT DELIVERY
// ============================================================

function processingSrt(
  job
) {
  return [
    "1",

    "00:00:01,000 --> 00:00:08,000",

    "Traduzindo legenda para PT-BR...",

    "",

    "2",

    "00:00:08,500 --> 00:00:15,000",

    `Progresso: ${Number(
      job?.progress ||
      0
    )}%.`
  ].join("\n");
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
      " STREMIO PT-BR BACKEND 8.0 STABLE — FLASH-LITE ONLY"
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
      `Local Bridge Secret: ` +
      `${
        LOCAL_BRIDGE_SECRET
          ? "CONFIGURADA ✅"
          : "FALTANDO ❌"
      }`
    );

    console.log(
      `Modelo único: ${GEMINI_MODEL} ✅`
    );

    console.log(
      "API: Interactions + Structured Output ✅"
    );

    console.log(
      `Pacer: 1 início de request a cada ` +
      `${GEMINI_MIN_START_INTERVAL_MS}ms ✅`
    );

    console.log(
      `Tradução: lotes SEQUENCIAIS de até ` +
      `${MAIN_BATCH_MAX_CUES} cues / ` +
      `${MAIN_BATCH_MAX_CHARS} chars ✅`
    );

    console.log(
      `Contexto: ${CONTEXT_CUES_BEFORE} cues antes + ` +
      `${CONTEXT_CUES_AFTER} depois ✅`
    );

    console.log(
      `Planner: opcional, ` +
      `thinking=${PLAN_THINKING} ✅`
    );

    console.log(
      `Tradução principal: ` +
      `thinking=${MAIN_THINKING} ✅`
    );

    console.log(
      `Reparo focado: ` +
      `${REPAIR_ENABLED ? "ATIVO" : "DESATIVADO"} | ` +
      `thinking=${REPAIR_THINKING} | ` +
      `máx=${REPAIR_MAX_CUES_TOTAL} cues ✅`
    );

    console.log(
      "Auditoria ampla por IA: REMOVIDA ✅"
    );

    console.log(
      "Deep Audit: REMOVIDA ✅"
    );

    console.log(
      "Recheck em cascata: REMOVIDO ✅"
    );

    console.log(
      "Split por 429: PROIBIDO ✅"
    );

    console.log(
      "429: espera + retry DO MESMO lote ✅"
    );

    console.log(
      "SAFE DRAFT após tradução principal: ATIVO ✅"
    );

    console.log(
      "Timestamps: sempre copiados do SRT original ✅"
    );

    console.log(
      "OpenSubtitles: somente fallback solicitado pela Ponte ✅"
    );

    console.log(
      "Render no menu do Stremio: BACKEND ONLY ✅"
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
