const express = require('express');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.disable('x-powered-by');
app.use(express.json({ limit: '1mb' }));

/* ========================================================================== */
/* 6.5 RESILIENT-FAST / QUALITY-FIRST                                         */
/* ========================================================================== */

const PORT = Number(process.env.PORT || 10000);
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || '').trim();
const GEMINI_MODEL = String(process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite').trim();
const PUBLIC_URL = String(process.env.PUBLIC_URL || '').replace(/\/+$/, '');
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || '').trim();

const SOURCE_FETCH_TIMEOUT_MS = 30000;
const PERFORMANCE_TARGET_MS = 90000; // meta; NÃO é kill switch

const configuredMaxBatchBlocks = Number(process.env.MAX_BATCH_BLOCKS || 160);
const configuredMaxBatchChars = Number(process.env.MAX_BATCH_CHARS || 10000);
const MAX_BATCH_BLOCKS = Number.isFinite(configuredMaxBatchBlocks) && configuredMaxBatchBlocks > 0
  ? Math.min(Math.floor(configuredMaxBatchBlocks), 160)
  : 160;
const MAX_BATCH_CHARS = Number.isFinite(configuredMaxBatchChars) && configuredMaxBatchChars > 0
  ? Math.min(Math.floor(configuredMaxBatchChars), 10000)
  : 10000;

/*
 * LIMITAÇÃO REAL: janela móvel por minuto.
 * Não existe orçamento máximo por legenda/filme.
 */
const GEMINI_CONCURRENCY = 3;
const SAFE_RPM = 14;
const RPM_WINDOW_MS = 60000;
const MIN_REQUEST_INTERVAL_MS = Math.max(Number(process.env.MIN_REQUEST_INTERVAL_MS || 3000), 4300);

/*
 * Timeouts são margens de saúde da conexão, NÃO metas de performance.
 * Timeout é tratado como erro transitório e recebe retry/backoff localizado.
 */
const TRANSLATION_REQUEST_TIMEOUT_MS = 45000;
const STRUCTURE_RETRY_TIMEOUT_MS = 45000;
const RESCUE_TRANSLATION_REQUEST_TIMEOUT_MS = 35000;
const REPAIR_TRANSLATION_REQUEST_TIMEOUT_MS = 30000;
const SINGLE_TRANSLATION_REQUEST_TIMEOUT_MS = 30000;
const SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS = 25000;

/* Retry somente para erros transitórios da chamada atual. Nunca por tamanho do job. */
const MAX_TRANSIENT_RETRIES = 4;
const TRANSIENT_BACKOFF_BASE_MS = 1000;
const TRANSIENT_BACKOFF_MAX_MS = 15000;

const RESCUE_BATCH_BLOCKS = 80;
const RESCUE_BATCH_CHARS = 4500;
const MAX_RESCUE_SPLIT_DEPTH = 2;

const AUDIT_CHUNK_SIZE = 24;
const RECHECK_GROUP_SIZE = 6;
const MAX_REPAIR_WINDOW_BLOCKS = 4;

/* SAFE proporcional: ~3,5% dos blocos, mínimo 72. */
const SAFE_AUDIT_RATIO = 0.035;
const SAFE_AUDIT_MIN_RECORDS = 72;
const EMBEDDED_CANARY_MIN = 24;
const OPENSUB_CANARY_MIN = 18;
const EMBEDDED_CANARY_EVERY = 80;
const OPENSUB_CANARY_EVERY = 100;

const MAX_OUTPUT_TOKENS = Number(process.env.MAX_OUTPUT_TOKENS || 16000);
const AUDIT_MAX_OUTPUT_TOKENS = 4096;
const MAX_RATE_LIMIT_COOLDOWN_MS = 180000;
const LAZY_OPENSUB_START_GRACE_MS = 1500;

const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const PARTIAL_CHECKPOINT_TTL_MS = 6 * 60 * 60 * 1000;
const MAX_CACHE_ENTRIES = 200;
const MAX_PARTIAL_CHECKPOINTS = 40;
const MAX_JOBS = 300;
const MAX_SOURCE_CHARS = 1200000;

/* Mantido para compatibilidade com a Ponte Local 2.5.1. */
const TRANSLATION_CACHE_VERSION = '5.8';
const BLOCK_LOCK_VERSION = '5.8';
const FAST_CHECKPOINT_VERSION = '6.5';
const RENDER_ENGINE_VERSION = '6.5-resilient-fast-quality-first';

const translationCache = new Map();
const partialTranslationCache = new Map();
const jobs = new Map();

const translationJobQueue = [];
let translationJobWorkerRunning = false;

const geminiQueue = [];
let activeGeminiWorkers = 0;
let rateSlotChain = Promise.resolve();
const geminiRequestStarts = [];
let geminiCooldownUntil = 0;
let lastGeminiRequestAt = 0;
let lifetimeGeminiRequests = 0;

/* ========================================================================== */
/* HELPERS                                                                    */
/* ========================================================================== */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function sha256(text) {
  return crypto.createHash('sha256').update(String(text), 'utf8').digest('hex');
}

function randomId(length = 8) {
  return crypto.randomBytes(length).toString('hex');
}

function getErrorMessage(error) {
  if (!error) return 'Erro desconhecido.';
  if (typeof error === 'string') return error;
  return error.message || error.statusText || 'Erro desconhecido.';
}

function makeError(code, message, extra = {}) {
  const error = new Error(message);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

function badModelOutputError(message) {
  return makeError('BAD_MODEL_OUTPUT', message);
}

function badAuditOutputError(message) {
  return makeError('BAD_AUDIT_OUTPUT', message);
}

function timestampIntegrityError(message) {
  return makeError('TIMESTAMP_INTEGRITY_ERROR', message);
}

function geminiRequestTimeoutError(kind) {
  return makeError(
    'GEMINI_REQUEST_TIMEOUT',
    `Timeout da chamada Gemini (${kind || 'request'}).`,
    { requestKind: kind || 'request' }
  );
}

function jobCancelledError(reason = 'Job cancelado.') {
  return makeError('JOB_CANCELLED', String(reason || 'Job cancelado.'));
}

function assertJobActive(job) {
  if (!job) return;
  if (job.cancelled || job.abortController?.signal?.aborted) {
    throw jobCancelledError(job.cancelReason || 'Job cancelado.');
  }
}

function cleanBaseUrl(req) {
  if (PUBLIC_URL) return PUBLIC_URL;
  const protocol = req.headers['x-forwarded-proto'] || req.protocol || 'https';
  const host = req.headers['x-forwarded-host'] || req.get('host');
  return `${protocol}://${host}`;
}

function safeJson(res, data, status = 200) {
  res.status(status);
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
  return res.json(data);
}

function jitterMs(base) {
  const b = Math.max(1, Math.floor(base));
  return Math.floor(b * (0.8 + Math.random() * 0.4));
}

function transientBackoffMs(attempt) {
  return Math.min(
    TRANSIENT_BACKOFF_MAX_MS,
    jitterMs(TRANSIENT_BACKOFF_BASE_MS * Math.pow(2, Math.max(0, attempt - 1)))
  );
}

/* ========================================================================== */
/* AUTENTICAÇÃO PONTE LOCAL                                                   */
/* ========================================================================== */

function isAuthorizedLocalBridge(req) {
  if (!LOCAL_BRIDGE_SECRET) return false;
  const auth = String(req.headers.authorization || '').trim();
  if (!auth) return false;
  const expected = `Bearer ${LOCAL_BRIDGE_SECRET}`;
  const a = Buffer.from(auth);
  const b = Buffer.from(expected);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/* ========================================================================== */
/* FETCH COM TIMEOUT                                                          */
/* ========================================================================== */

async function fetchWithTimeout(url, options = {}, timeoutMs = SOURCE_FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

/* ========================================================================== */
/* MEMÓRIA / CACHE / CHECKPOINT                                               */
/* ========================================================================== */

function cleanupMemory() {
  const now = Date.now();

  for (const [key, item] of translationCache.entries()) {
    if (item.expiresAt <= now) {
      translationCache.delete(key);
    }
  }

  for (const [key, item] of partialTranslationCache.entries()) {
    if (item.expiresAt <= now) {
      partialTranslationCache.delete(key);
    }
  }

  for (const [key, job] of jobs.entries()) {
    if (
      job.expiresAt <= now &&
      !['processing', 'pending'].includes(job.status)
    ) {
      jobs.delete(key);
    }
  }

  while (translationCache.size > MAX_CACHE_ENTRIES) {
    const key = translationCache.keys().next().value;
    if (key === undefined) break;
    translationCache.delete(key);
  }

  while (partialTranslationCache.size > MAX_PARTIAL_CHECKPOINTS) {
    const key = partialTranslationCache.keys().next().value;
    if (key === undefined) break;
    partialTranslationCache.delete(key);
  }

  while (jobs.size > MAX_JOBS) {
    const key = jobs.keys().next().value;
    if (key === undefined) break;

    const job = jobs.get(key);

    if (
      job &&
      ['processing', 'pending'].includes(job.status)
    ) {
      break;
    }

    jobs.delete(key);
  }
}

setInterval(cleanupMemory, 5 * 60 * 1000).unref();

function setTranslationCache(key, srt) {
  const now = Date.now();

  translationCache.set(key, {
    srt,
    version: TRANSLATION_CACHE_VERSION,
    contentAuditPassed: true,
    createdAt: now,
    expiresAt: now + CACHE_TTL_MS
  });

  partialTranslationCache.delete(
    `${FAST_CHECKPOINT_VERSION}:${key}`
  );

  cleanupMemory();
}

function getTranslationCache(key) {
  const item = translationCache.get(key);

  if (!item) {
    return null;
  }

  if (
    item.version !== TRANSLATION_CACHE_VERSION ||
    item.contentAuditPassed !== true ||
    item.expiresAt <= Date.now()
  ) {
    translationCache.delete(key);
    return null;
  }

  return item.srt;
}

function partialCheckpointKey(cacheKey) {
  return `${FAST_CHECKPOINT_VERSION}:${cacheKey}`;
}

function getOrCreatePartialCheckpoint(
  cacheKey,
  blockCount,
  sourceSignature
) {
  const key = partialCheckpointKey(cacheKey);

  let item = partialTranslationCache.get(key);

  if (
    !item ||
    item.blockCount !== blockCount ||
    item.sourceSignature !== sourceSignature ||
    item.expiresAt <= Date.now()
  ) {
    item = {
      version: FAST_CHECKPOINT_VERSION,
      blockCount,
      sourceSignature,
      texts: new Array(blockCount),
      done: new Array(blockCount).fill(false),
      createdAt: Date.now(),
      updatedAt: Date.now(),
      expiresAt: Date.now() + PARTIAL_CHECKPOINT_TTL_MS
    };

    partialTranslationCache.set(
      key,
      item
    );
  }

  return item;
}

function writeCheckpointRange(
  checkpoint,
  start,
  texts
) {
  for (
    let i = 0;
    i < texts.length;
    i++
  ) {
    checkpoint.texts[start + i] =
      texts[i];

    checkpoint.done[start + i] =
      true;
  }

  checkpoint.updatedAt =
    Date.now();

  checkpoint.expiresAt =
    Date.now() +
    PARTIAL_CHECKPOINT_TTL_MS;
}

function checkpointRangeComplete(
  checkpoint,
  start,
  length
) {
  for (
    let i = 0;
    i < length;
    i++
  ) {
    if (
      !checkpoint.done[start + i] ||
      typeof checkpoint.texts[start + i] !== 'string'
    ) {
      return false;
    }
  }

  return true;
}

/* ========================================================================== */
/* SRT / LIMPEZA                                                              */
/* ========================================================================== */

function normalizeSrt(text) {
  return String(text || '')
    .replace(/^\uFEFF/, '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim();
}

function stripCodeFences(text) {
  return String(text || '')
    .replace(
      /^```(?:json|srt|text|plaintext)?\s*/i,
      ''
    )
    .replace(/\s*```$/i, '')
    .trim();
}

function createCharacterSanitizationStats() {
  return {
    controlChars: 0,
    invisibleChars: 0,
    normalizedSpaces: 0,
    replacementChars: 0,
    assTags: 0,
    nfcChanges: 0
  };
}

function addCharacterSanitizationStats(
  target,
  source
) {
  for (const key of Object.keys(target)) {
    target[key] +=
      Number(
        source?.[key] ||
        0
      );
  }

  return target;
}

function sanitizeSubtitleText(value) {
  const stats =
    createCharacterSanitizationStats();

  let text =
    String(
      value ??
      ''
    );

  const nfc =
    text.normalize(
      'NFC'
    );

  if (nfc !== text) {
    stats.nfcChanges++;
    text = nfc;
  }

  text =
    text.replace(
      /[\u00A0\u202F]/gu,
      () => {
        stats.normalizedSpaces++;
        return ' ';
      }
    );

  text =
    text.replace(
      /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/gu,
      () => {
        stats.controlChars++;
        return '';
      }
    );

  text =
    text.replace(
      /[\u00AD\u200B\u2060\uFEFF\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu,
      () => {
        stats.invisibleChars++;
        return '';
      }
    );

  text =
    text.replace(
      /\uFFFD/gu,
      () => {
        stats.replacementChars++;
        return '';
      }
    );

  text =
    text.replace(
      /\{\\[^}\r\n]*\}/gu,
      () => {
        stats.assTags++;
        return '';
      }
    );

  text =
    text
      .split('\n')
      .map(
        line =>
          line
            .replace(
              /[ \t]{2,}/g,
              ' '
            )
            .trimEnd()
      )
      .join('\n')
      .trim();

  return {
    text,
    stats
  };
}

function characterSanitizationTotal(stats) {
  return (
    stats.controlChars +
    stats.invisibleChars +
    stats.normalizedSpaces +
    stats.replacementChars +
    stats.assTags +
    stats.nfcChanges
  );
}

function logCharacterSanitization(
  stage,
  stats
) {
  console.log(
    `[CLEAN CHAR] ${stage}: ` +
    `${characterSanitizationTotal(stats)} ajuste(s); ` +
    `controles=${stats.controlChars}, ` +
    `invisíveis=${stats.invisibleChars}, ` +
    `espaços=${stats.normalizedSpaces}, ` +
    `replacement=${stats.replacementChars}, ` +
    `ASS=${stats.assTags}, ` +
    `NFC=${stats.nfcChanges}.`
  );
}

const SDH_CUE_WORDS =
  /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

const SPEAKER_HINT_MARKER_REGEX =
  /^@@SPK:([^@]+)@@\s*/u;

function normalizeSpeakerHint(value) {
  const speaker =
    String(
      value ||
      ''
    )
      .replace(
        /<[^>]+>/g,
        ' '
      )
      .replace(
        /\s+/g,
        ' '
      )
      .trim();

  if (
    !speaker ||
    speaker.length > 60 ||
    SDH_CUE_WORDS.test(speaker) ||
    /[!?;]/u.test(speaker)
  ) {
    return '';
  }

  return speaker;
}

function encodeSpeakerHint(speaker) {
  return encodeURIComponent(
    String(
      speaker ||
      ''
    )
  );
}

function decodeSpeakerHint(encoded) {
  try {
    return normalizeSpeakerHint(
      decodeURIComponent(
        String(
          encoded ||
          ''
        )
      )
    );
  } catch {
    return '';
  }
}

function extractSpeakerHint(line) {
  const original =
    String(
      line ||
      ''
    );

  const hidden =
    original.match(
      SPEAKER_HINT_MARKER_REGEX
    );

  if (hidden) {
    return {
      speaker:
        decodeSpeakerHint(
          hidden[1]
        ),

      lineForCleaning:
        original.replace(
          SPEAKER_HINT_MARKER_REGEX,
          ''
        )
    };
  }

  const bracket =
    original.match(
      /^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*/u
    );

  if (bracket) {
    const speaker =
      normalizeSpeakerHint(
        bracket[1]
      );

    if (speaker) {
      return {
        speaker,
        lineForCleaning:
          original
      };
    }
  }

  const colon =
    original.match(
      /^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50}):\s+(?=\S)/u
    );

  if (colon) {
    const speaker =
      normalizeSpeakerHint(
        colon[1]
      );

    if (speaker) {
      return {
        speaker,
        lineForCleaning:
          original
      };
    }
  }

  return {
    speaker: '',
    lineForCleaning:
      original
  };
}

function normalizeHyphenatedVocalElongations(text) {
  return String(
    text ??
    ''
  ).replace(
    /([A-Za-zÀ-ÖØ-öø-ÿ])(?:[-–—]\1){2,}[-–—]?/giu,
    '$1'
  );
}

function normalizeTranslatedVocalElongations(text) {
  let result =
    normalizeHyphenatedVocalElongations(
      text
    );

  result =
    result.replace(
      /([AEIOUÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜaeiouáàâãäéèêëíìîïóòôõöúùûü])\1{3,}/gu,
      '$1'
    );

  return result;
}

function cleanDialogueLine(line) {
  let text =
    String(
      line ||
      ''
    ).trim();

  if (!text) {
    return '';
  }

  text =
    text.replace(
      /\s*\[[^\]]+\]\s*/gu,
      ' '
    );

  text =
    text.replace(
      /\s*\(([^)]*)\)\s*/gu,
      (
        match,
        inside
      ) =>
        SDH_CUE_WORDS.test(
          String(
            inside ||
            ''
          )
        )
          ? ' '
          : match
    );

  text =
    text.replace(
      /^\s*[-–—]?\s*[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ .'-]{0,30}:\s+(?=\S)/u,
      ''
    );

  text =
    text
      .replace(
        /[ \t]{2,}/g,
        ' '
      )
      .trim();

  if (
    /^[-–—♪♫♬\s]*$/u.test(
      text
    )
  ) {
    return '';
  }

  return text;
}

const TIMING_LINE_REGEX =
  /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

function extractTimingLines(srt) {
  return String(
    srt ||
    ''
  )
    .replace(
      /\r\n/g,
      '\n'
    )
    .replace(
      /\r/g,
      '\n'
    )
    .split('\n')
    .map(
      line =>
        line.trim()
    )
    .filter(
      line =>
        TIMING_LINE_REGEX.test(
          line
        )
    );
}

function timingSignature(timings) {
  return sha256(
    JSON.stringify(
      timings
    )
  ).slice(
    0,
    16
  );
}

function auditCleaningTimestamps(
  rawSrt,
  cleanedSrt,
  label = 'SOURCE'
) {
  const raw =
    extractTimingLines(
      rawSrt
    );

  const clean =
    extractTimingLines(
      cleanedSrt
    );

  let cursor =
    0;

  for (
    let i = 0;
    i < clean.length;
    i++
  ) {
    while (
      cursor < raw.length &&
      raw[cursor] !== clean[i]
    ) {
      cursor++;
    }

    if (
      cursor >=
      raw.length
    ) {
      throw timestampIntegrityError(
        `Auditoria de timestamps falhou na limpeza (${label}) ` +
        `no bloco preservado ${i + 1}.`
      );
    }

    cursor++;
  }

  console.log(
    `[AUDIT TIMESTAMP] ${label} RAW->CLEAN: OK — ` +
    `${clean.length}/${raw.length} timing(s) preservado(s) exatamente; ` +
    `removidos=${raw.length - clean.length}; ` +
    `assinatura=${timingSignature(clean)}.`
  );

  return true;
}

function parseSrt(srt) {
  const normalized =
    normalizeSrt(
      srt
    );

  if (!normalized) {
    return [];
  }

  const rawBlocks =
    normalized
      .split(
        /\n{2,}/
      )
      .map(
        block =>
          block.trim()
      )
      .filter(Boolean);

  const result =
    [];

  for (
    const rawBlock
    of rawBlocks
  ) {
    const lines =
      rawBlock.split(
        '\n'
      );

    if (
      lines.length < 3
    ) {
      continue;
    }

    const indexLine =
      lines[0].trim();

    const timingLine =
      lines[1].trim();

    if (
      !/^\d+$/.test(
        indexLine
      ) ||
      !TIMING_LINE_REGEX.test(
        timingLine
      )
    ) {
      continue;
    }

    const textLines =
      lines.slice(
        2
      );

    let speakerHint =
      '';

    if (
      textLines.length
    ) {
      const match =
        textLines[0].match(
          SPEAKER_HINT_MARKER_REGEX
        );

      if (match) {
        speakerHint =
          decodeSpeakerHint(
            match[1]
          );

        textLines[0] =
          textLines[0].replace(
            SPEAKER_HINT_MARKER_REGEX,
            ''
          );
      }
    }

    result.push({
      index:
        Number(
          indexLine
        ),

      timing:
        timingLine,

      text:
        textLines.join(
          '\n'
        ),

      speakerHint:
        speakerHint ||
        null
    });
  }

  return result;
}

function auditFinalTimestamps(
  sourceSrt,
  finalSrt,
  label = 'FINAL'
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
    throw timestampIntegrityError(
      `Auditoria de timestamps falhou (${label}): ` +
      `fonte=${source.length}, final=${final.length}.`
    );
  }

  for (
    let i = 0;
    i < source.length;
    i++
  ) {
    if (
      source[i].index !== final[i].index ||
      source[i].timing !== final[i].timing
    ) {
      throw timestampIntegrityError(
        `Auditoria de timestamps falhou (${label}) ` +
        `no bloco ${i + 1}: índice/timing divergente.`
      );
    }
  }

  const signature =
    timingSignature(
      source.map(
        block => [
          block.index,
          block.timing
        ]
      )
    );

  console.log(
    `[AUDIT TIMESTAMP] ${label}: OK — ` +
    `${source.length}/${source.length} bloco(s), ` +
    `0 alteração(ões), assinatura=${signature}.`
  );

  return true;
}

function cleanSrtForTranslation(srt) {
  const normalized =
    normalizeSrt(
      srt
    );

  if (!normalized) {
    return '';
  }

  const rawBlocks =
    normalized.split(
      /\n{2,}/
    );

  const cleanedBlocks =
    [];

  let removedBlocks =
    0;

  let changedLines =
    0;

  let speakerHintBlocks =
    0;

  let elongatedLines =
    0;

  const characterStats =
    createCharacterSanitizationStats();

  for (
    const rawBlock
    of rawBlocks
  ) {
    const lines =
      rawBlock
        .trim()
        .split('\n');

    const timingIndex =
      lines.findIndex(
        line =>
          /-->/.test(
            line
          )
      );

    if (
      timingIndex === -1
    ) {
      continue;
    }

    const timing =
      lines[
        timingIndex
      ].trim();

    const cleanedDialogue =
      [];

    const speakerHints =
      new Set();

    for (
      const line
      of lines.slice(
        timingIndex + 1
      )
    ) {
      const speakerInfo =
        extractSpeakerHint(
          line
        );

      if (
        speakerInfo.speaker
      ) {
        speakerHints.add(
          speakerInfo.speaker
        );
      }

      const beforeChars =
        cleanDialogueLine(
          speakerInfo.lineForCleaning
        );

      const sanitized =
        sanitizeSubtitleText(
          beforeChars
        );

      addCharacterSanitizationStats(
        characterStats,
        sanitized.stats
      );

      const cleaned =
        normalizeHyphenatedVocalElongations(
          sanitized.text
        );

      if (
        cleaned !==
        sanitized.text
      ) {
        elongatedLines++;
      }

      if (
        cleaned !==
        line.trim()
      ) {
        changedLines++;
      }

      if (cleaned) {
        cleanedDialogue.push(
          cleaned
        );
      }
    }

    if (
      !cleanedDialogue.length
    ) {
      removedBlocks++;
      continue;
    }

    if (
      speakerHints.size === 1
    ) {
      const speaker =
        Array.from(
          speakerHints
        )[0];

      cleanedDialogue[0] =
        `@@SPK:${encodeSpeakerHint(speaker)}@@ ` +
        cleanedDialogue[0];

      speakerHintBlocks++;
    }

    cleanedBlocks.push({
      timing,
      dialogue:
        cleanedDialogue
    });
  }

  const result =
    cleanedBlocks
      .map(
        (
          block,
          index
        ) =>
          [
            index + 1,
            block.timing,
            ...block.dialogue
          ].join(
            '\n'
          )
      )
      .join('\n\n')
      .trim();

  console.log(
    `[CLEAN] SDH/CC: ` +
    `${rawBlocks.length} -> ${cleanedBlocks.length} blocos; ` +
    `${removedBlocks} removidos; ` +
    `${changedLines} linha(s) alterada(s).`
  );

  console.log(
    `[CLEAN] Contexto de falante: ` +
    `${speakerHintBlocks} bloco(s) preservado(s).`
  );

  console.log(
    `[CLEAN] Alongamentos vocais na fonte: ` +
    `${elongatedLines} linha(s) normalizada(s).`
  );

  logCharacterSanitization(
    'FONTE',
    characterStats
  );

  const finalResult =
    result
      ? result + '\n'
      : '';

  if (finalResult) {
    auditCleaningTimestamps(
      normalized,
      finalResult,
      'FONTE'
    );
  }

  return finalResult;
}

function cleanTranslatedDialogueMarkers(text) {
  const normalized =
    String(
      text ??
      ''
    )
      .replace(
        /\r\n/g,
        '\n'
      )
      .replace(
        /\r/g,
        '\n'
      );

  const lines =
    normalized.split(
      '\n'
    );

  const markerRegex =
    /^\s*[-–—/]+\s+(?=\S)/u;

  let marked =
    0;

  let nonEmpty =
    0;

  for (
    const line
    of lines
  ) {
    if (
      line.trim()
    ) {
      nonEmpty++;
    }

    if (
      markerRegex.test(
        line
      )
    ) {
      marked++;
    }
  }

  if (
    nonEmpty >= 2 &&
    marked >= 2
  ) {
    return normalized;
  }

  return lines
    .map(
      line =>
        line.replace(
          markerRegex,
          ''
        )
    )
    .join('\n');
}

function finalizeTranslatedText(text) {
  const sanitized =
    sanitizeSubtitleText(
      String(
        text ??
        ''
      )
    );

  let result =
    normalizeTranslatedVocalElongations(
      sanitized.text
    );

  result =
    cleanTranslatedDialogueMarkers(
      result
    );

  return sanitizeSubtitleText(
    result
  ).text;
}

function finalizeAllTranslatedTexts(
  translatedTexts
) {
  const stats =
    createCharacterSanitizationStats();

  let charChanged =
    0;

  let vocalChanged =
    0;

  let markerChanged =
    0;

  const result =
    translatedTexts.map(
      text => {
        const original =
          String(
            text ??
            ''
          );

        const sanitized =
          sanitizeSubtitleText(
            original
          );

        addCharacterSanitizationStats(
          stats,
          sanitized.stats
        );

        if (
          sanitized.text !==
          original
        ) {
          charChanged++;
        }

        const vocal =
          normalizeTranslatedVocalElongations(
            sanitized.text
          );

        if (
          vocal !==
          sanitized.text
        ) {
          vocalChanged++;
        }

        const marker =
          cleanTranslatedDialogueMarkers(
            vocal
          );

        if (
          marker !==
          vocal
        ) {
          markerChanged++;
        }

        return sanitizeSubtitleText(
          marker
        ).text;
      }
    );

  logCharacterSanitization(
    `TRADUÇÃO (${charChanged} bloco(s) alterado(s))`,
    stats
  );

  console.log(
    `[CLEAN] Alongamentos vocais traduzidos: ` +
    `${vocalChanged} bloco(s) ajustado(s).`
  );

  console.log(
    `[CLEAN] Marcadores de diálogo: ` +
    `${markerChanged} bloco(s) ajustado(s).`
  );

  return result;
}

function buildSrt(
  blocks,
  translatedTexts
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
            translatedTexts[index] ??
            block.text
          ].join(
            '\n'
          )
      )
      .join('\n\n')
      .trim() +
    '\n'
  );
}

/* ========================================================================== */
/* LOTES                                                                      */
/* ========================================================================== */

function splitIntoBatches(blocks) {
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
      String(
        block.text ||
        ''
      ).length +
      24;

    if (
      current.length &&
      (
        current.length >=
        MAX_BATCH_BLOCKS ||
        chars + size >
        MAX_BATCH_CHARS
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

/* ========================================================================== */
/* CANCELAMENTO                                                               */
/* ========================================================================== */

function cancelJobOperations(
  job,
  reason
) {
  if (
    !job ||
    job.cancelled
  ) {
    return;
  }

  job.cancelled =
    true;

  job.cancelReason =
    getErrorMessage(
      reason ||
      'Job cancelado.'
    );

  try {
    job.abortController?.abort();
  } catch {}

  let removed =
    0;

  for (
    let i =
      geminiQueue.length -
      1;
    i >= 0;
    i--
  ) {
    const item =
      geminiQueue[i];

    if (
      item?.job ===
      job
    ) {
      geminiQueue.splice(
        i,
        1
      );

      removed++;

      try {
        item.reject(
          jobCancelledError(
            job.cancelReason
          )
        );
      } catch {}
    }
  }

  if (
    removed > 0
  ) {
    console.warn(
      `[CANCEL] Job ${job.id}: ` +
      `${removed} request(s) pendente(s) removida(s) da fila Gemini.`
    );
  }
}

/* ========================================================================== */
/* RATE LIMIT GLOBAL                                                          */
/* ========================================================================== */

function getCooldownRemaining() {
  return Math.max(
    0,
    geminiCooldownUntil -
    Date.now()
  );
}

function setGeminiCooldown(ms) {
  const safeMs =
    Math.min(
      Math.max(
        Number(ms) ||
        30000,
        1000
      ),
      MAX_RATE_LIMIT_COOLDOWN_MS
    );

  geminiCooldownUntil =
    Math.max(
      geminiCooldownUntil,
      Date.now() +
      safeMs
    );

  console.log(
    `[GEMINI] RATE LIMIT/capacidade transitória. ` +
    `Cooldown global de ${Math.ceil(safeMs / 1000)}s.`
  );
}

function pruneRequestStarts(
  now = Date.now()
) {
  while (
    geminiRequestStarts.length &&
    now -
    geminiRequestStarts[0] >=
    RPM_WINDOW_MS
  ) {
    geminiRequestStarts.shift();
  }
}

async function sleepAbortable(
  ms,
  job
) {
  assertJobActive(
    job
  );

  const signal =
    job
      ?.abortController
      ?.signal;

  await new Promise(
    (
      resolve,
      reject
    ) => {
      let settled =
        false;

      const finish =
        (
          fn,
          value
        ) => {
          if (settled) {
            return;
          }

          settled =
            true;

          clearTimeout(
            timer
          );

          try {
            signal
              ?.removeEventListener(
                'abort',
                onAbort
              );
          } catch {}

          fn(
            value
          );
        };

      const onAbort =
        () =>
          finish(
            reject,
            jobCancelledError(
              job
                ?.cancelReason ||
              'Job cancelado.'
            )
          );

      const timer =
        setTimeout(
          () =>
            finish(
              resolve
            ),
          Math.max(
            0,
            Number(ms) ||
            0
          )
        );

      if (signal) {
        if (
          signal.aborted
        ) {
          onAbort();
        } else {
          signal.addEventListener(
            'abort',
            onAbort,
            {
              once:
                true
            }
          );
        }
      }
    }
  );

  assertJobActive(
    job
  );
}

async function acquireGeminiRateSlot(job) {
  let resolveOuter;
  let rejectOuter;

  const outer =
    new Promise(
      (
        resolve,
        reject
      ) => {
        resolveOuter =
          resolve;

        rejectOuter =
          reject;
      }
    );

  rateSlotChain =
    rateSlotChain
      .catch(
        () => {}
      )
      .then(
        async () => {
          try {
            for (;;) {
              assertJobActive(
                job
              );

              const now =
                Date.now();

              pruneRequestStarts(
                now
              );

              const cooldown =
                getCooldownRemaining();

              if (
                cooldown > 0
              ) {
                console.log(
                  `[GEMINI] Fila aguardando cooldown de ` +
                  `${Math.ceil(cooldown / 1000)}s.`
                );

                await sleepAbortable(
                  cooldown,
                  job
                );

                continue;
              }

              let waitMs =
                0;

              if (
                lastGeminiRequestAt >
                0
              ) {
                waitMs =
                  Math.max(
                    waitMs,
                    MIN_REQUEST_INTERVAL_MS -
                    (
                      now -
                      lastGeminiRequestAt
                    )
                  );
              }

              if (
                geminiRequestStarts.length >=
                SAFE_RPM
              ) {
                waitMs =
                  Math.max(
                    waitMs,
                    RPM_WINDOW_MS -
                    (
                      now -
                      geminiRequestStarts[0]
                    ) +
                    75
                  );
              }

              if (
                waitMs > 0
              ) {
                await sleepAbortable(
                  waitMs,
                  job
                );

                continue;
              }

              const reservedAt =
                Date.now();

              geminiRequestStarts.push(
                reservedAt
              );

              lastGeminiRequestAt =
                reservedAt;

              resolveOuter();

              return;
            }
          } catch (error) {
            rejectOuter(
              error
            );
          }
        }
      );

  return outer;
}

function getRetryAfterMs(
  response,
  errorData
) {
  const header =
    response
      ?.headers
      ?.get(
        'retry-after'
      );

  if (header) {
    const seconds =
      Number(
        header
      );

    if (
      Number.isFinite(
        seconds
      ) &&
      seconds > 0
    ) {
      return Math.min(
        seconds *
        1000,
        MAX_RATE_LIMIT_COOLDOWN_MS
      );
    }
  }

  const message =
    String(
      errorData
        ?.error
        ?.message ||
      ''
    );

  let match =
    message.match(
      /retry in\s+([\d.]+)s/i
    );

  if (match) {
    return Math.min(
      (
        Number(
          match[1]
        ) +
        1
      ) *
      1000,
      MAX_RATE_LIMIT_COOLDOWN_MS
    );
  }

  match =
    message.match(
      /retry in\s+(\d+)m\s*(\d+(?:\.\d+)?)?s?/i
    );

  if (match) {
    return Math.min(
      (
        Number(
          match[1]
        ) *
        60 +
        Number(
          match[2] ||
          0
        ) +
        1
      ) *
      1000,
      MAX_RATE_LIMIT_COOLDOWN_MS
    );
  }

  return 30000;
}

function isRateLimitError(
  status,
  message
) {
  return (
    status === 429 ||
    /quota|rate.?limit|resource.?exhausted|too many requests/i.test(
      String(
        message ||
        ''
      )
    )
  );
}

function isDailyQuotaError(message) {
  return /per day|daily|requests per day|RPD|quota.*day|day.*quota/i.test(
    String(
      message ||
      ''
    )
  );
}

function isTransientHttpStatus(status) {
  return (
    status === 408 ||
    status === 409 ||
    status === 425 ||
    status === 429 ||
    status === 500 ||
    status === 502 ||
    status === 503 ||
    status === 504
  );
}

function isTransientGeminiError(error) {
  if (!error) {
    return false;
  }

  if (
    error.code ===
    'GEMINI_REQUEST_TIMEOUT'
  ) {
    return true;
  }

  if (
    error.name ===
    'AbortError'
  ) {
    return true;
  }

  if (
    error.transient ===
    true
  ) {
    return true;
  }

  if (
    isTransientHttpStatus(
      Number(
        error.status
      )
    )
  ) {
    return true;
  }

  return /timeout|timed out|network|fetch failed|socket|ECONNRESET|ETIMEDOUT|EAI_AGAIN|temporar|unavailable|deadline/i.test(
    getErrorMessage(
      error
    )
  );
}

/* ========================================================================== */
/* PROMPTS                                                                    */
/* ========================================================================== */

function translationSystemPrompt() {
  return (
    'Você é um tradutor e localizador profissional de legendas para Português do Brasil, padrão premium de streaming. ' +
    'Leia TODO o lote antes de responder e use as falas próximas apenas para compreender situação, relação entre pessoas, humor, época, intenção e escolha lexical. ' +
    'Depois traduza cada ID como unidade ATÔMICA: o texto de um ID jamais pode receber conteúdo que pertence ao ID anterior ou seguinte. ' +
    'Prioridade: integridade por ID → sentido/intenção → coerência contextual → voz da pessoa → naturalidade brasileira atual → humor/ritmo → literalidade. ' +
    'A tradução precisa fazer sentido NESTA situação específica. Antes de escolher uma palavra, verifique se ela combina com quem fala, com quem escuta, com o assunto, com o tom e com a época. ' +
    'Evite tradução palavra-por-palavra, tradução com cara de dublagem antiga, falsos cognatos, termos envelhecidos ou expressões brasileiras deslocadas no contexto atual. ' +
    'Ao mesmo tempo, não force memes ou gírias modernas onde elas não cabem. Linguagem contemporânea significa naturalidade, não caricatura. ' +
    'Quando a cena/personagem pedir, use repertório atual de geração Z/Alpha, cultura digital e oralidade jovem de forma orgânica. ' +
    'Em contexto LGBTQIAPN+, queer, drag, ballroom, camp, shade e cultura pop, preserve identidade, pronome, gênero, ironia, afeto, provocação, termos ressignificados e intensidade; não heteronormativize nem higienize. ' +
    'Vocativos como girl, bitch, honey, sis, queen, baby e babe são contextuais; não traduza automaticamente girl como garota nem bitch como vadia. ' +
    'Adapte expressões idiomáticas, piadas, trocadilhos, shade e referências quando houver solução brasileira natural que preserve o efeito. Se a adaptação piorar, preserve a referência. ' +
    'Não censure palavrões ou intensidade emocional. Preserve nomes, marcas, títulos e termos técnicos. Preserve bordões consagrados quando apropriado. ' +
    'speaker é contexto oculto: nunca copie o nome para t. Se o gênero não estiver claro, não chute; prefira construção natural sem marcação desnecessária. ' +
    'Não acrescente SDH/CC, sons, nomes de falantes ou explicações. Preserve formatação útil. ' +
    'Antes de enviar, faça uma revisão silenciosa de cada ID: ' +
    '(1) conteúdo pertence somente a ele; ' +
    '(2) a frase faz sentido no contexto; ' +
    '(3) o vocabulário soa atual e natural; ' +
    '(4) não há termo deslocado ou sem nexo. ' +
    'Entrada compacta: cada tupla de b é [i,l,x,s?]. ' +
    'Retorne somente JSON exatamente na ordem recebida, usando i=id, l=lock e t=tradução. Preserve i e l sem alteração.'
  );
}

function auditSystemPrompt() {
  return (
    'Você é um revisor bilíngue independente de legendas EN→PT-BR, rigoroso com SINCRONIZAÇÃO SEMÂNTICA e QUALIDADE CONTEXTUAL. NÃO reescreva na resposta. ' +
    'Cada registro usa i=id, l=lock, s=source, t=translation, p=[id,source,pt] anterior e n=[id,source,pt] seguinte. ' +
    'Os registros do array podem ser esparsos: nunca trate outro elemento do array como vizinho. Use somente p/n do próprio registro. ' +
    'Primeiro verifique fronteira: t precisa corresponder exclusivamente a s. Se conteúdo claramente pertence ao vizinho anterior/seguinte, m deve ser o ID daquele vizinho e f=false. ' +
    'Depois avalie o próprio ID no contexto: sentido, intenção, humor, registro, relação entre pessoas, gênero/pronomes, referências culturais, atualidade lexical e naturalidade do PT-BR. ' +
    'Se a tradução é fiel mas usa termo sem nexo naquela situação, expressão deslocada da época, tradução literal estranha, escolha lexical artificial/datada ou tom incompatível, use m=i e f=false. ' +
    'Aceite paráfrase, localização, gíria, humor, drag/camp/shade e linguagem LGBTQIAPN+ quando naturais e fiéis à intenção. Não penalize apenas por não ser literal. ' +
    'Fragmentos podem continuar fragmentários se a fonte também for fragmentária. ' +
    'm só pode ser i, p[0] ou n[0]. ' +
    'Se tudo estiver fiel, contextual, atual e natural, retorne m=i e f=true. ' +
    'Preserve i/l exatamente. Retorne somente JSON.'
  );
}

/* ========================================================================== */
/* LOCK / CONTEXTO                                                            */
/* ========================================================================== */

function blockTranslationLock(block) {
  return sha256(
    JSON.stringify(
      [
        BLOCK_LOCK_VERSION,
        Number(
          block?.index
        ),
        String(
          block?.timing ??
          ''
        ),
        String(
          block?.text ??
          ''
        ),
        String(
          block?.speakerHint ??
          ''
        )
      ]
    )
  ).slice(
    0,
    12
  );
}

function contextBlockPayload(block) {
  return block
    ? [
        block.index,
        block.text
      ]
    : null;
}

function buildTranslationPrompt(
  blocks,
  context = {},
  options = {}
) {
  const items =
    blocks.map(
      block => {
        const tuple =
          [
            block.index,
            blockTranslationLock(
              block
            ),
            block.text
          ];

        if (
          block.speakerHint
        ) {
          tuple.push(
            block.speakerHint
          );
        }

        return tuple;
      }
    );

  let mode =
    'TRADUÇÃO PREMIUM. Leia o lote inteiro para contexto, mas mantenha cada ID semanticamente isolado.';

  if (
    options.strictStructure
  ) {
    mode =
      'REPETIÇÃO ESTRUTURAL. A resposta anterior quebrou o contrato. Traduza todos os itens exatamente uma vez; preserve ordem/i/l; não omita nem duplique. Continue aplicando a qualidade premium.';
  }

  if (
    options.repairMode
  ) {
    mode =
      'REPARO CIRÚRGICO. Releia p/b/n; traduza SOMENTE b. Corrija sentido, naturalidade e fronteira sem absorver conteúdo de p/n. Preserve fragmentos quando a frase continuar fora da janela.';
  }

  return (
    `${mode}\n` +
    'p/n são contexto externo. ' +
    'Tupla b=[i,l,x,s?]. ' +
    'SAÍDA EXATA: [{"i":123,"l":"abc","t":"..."}]\n' +
    JSON.stringify(
      {
        p:
          contextBlockPayload(
            context.before
          ),

        b:
          items,

        n:
          contextBlockPayload(
            context.after
          )
      }
    )
  );
}

/* ========================================================================== */
/* GEMINI RAW                                                                 */
/* ========================================================================== */

function requestTimeoutForKind(kind) {
  if (
    kind ===
    'semantic-audit'
  ) {
    return SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS;
  }

  if (
    kind ===
    'translation-structure-retry'
  ) {
    return STRUCTURE_RETRY_TIMEOUT_MS;
  }

  if (
    kind ===
    'translation-rescue'
  ) {
    return RESCUE_TRANSLATION_REQUEST_TIMEOUT_MS;
  }

  if (
    kind ===
    'translation-repair'
  ) {
    return REPAIR_TRANSLATION_REQUEST_TIMEOUT_MS;
  }

  if (
    kind ===
    'translation-single'
  ) {
    return SINGLE_TRANSLATION_REQUEST_TIMEOUT_MS;
  }

  return TRANSLATION_REQUEST_TIMEOUT_MS;
}

async function rawGeminiRequest(
  prompt,
  requestKind,
  job
) {
  if (!GEMINI_API_KEY) {
    throw new Error(
      'GEMINI_API_KEY não configurada.'
    );
  }

  assertJobActive(
    job
  );

  const endpoint =
    `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      GEMINI_MODEL
    )}:generateContent`;

  const isAudit =
    requestKind ===
    'semantic-audit';

  const generationConfig =
    isAudit
      ? {
          thinkingConfig: {
            thinkingLevel:
              'MINIMAL'
          },

          responseMimeType:
            'application/json',

          responseSchema: {
            type:
              'ARRAY',

            items: {
              type:
                'OBJECT',

              properties: {
                i: {
                  type:
                    'INTEGER'
                },

                l: {
                  type:
                    'STRING'
                },

                m: {
                  type:
                    'INTEGER'
                },

                f: {
                  type:
                    'BOOLEAN'
                }
              },

              required: [
                'i',
                'l',
                'm',
                'f'
              ]
            },

            minItems:
              1
          },

          maxOutputTokens:
            AUDIT_MAX_OUTPUT_TOKENS
        }
      : {
          thinkingConfig: {
            thinkingLevel:
              'MINIMAL'
          },

          responseMimeType:
            'application/json',

          responseSchema: {
            type:
              'ARRAY',

            items: {
              type:
                'OBJECT',

              properties: {
                i: {
                  type:
                    'INTEGER'
                },

                l: {
                  type:
                    'STRING'
                },

                t: {
                  type:
                    'STRING'
                }
              },

              required: [
                'i',
                'l',
                't'
              ]
            },

            minItems:
              1
          },

          maxOutputTokens:
            MAX_OUTPUT_TOKENS
        };

  const body = {
    systemInstruction: {
      parts: [
        {
          text:
            isAudit
              ? auditSystemPrompt()
              : translationSystemPrompt()
        }
      ]
    },

    contents: [
      {
        role:
          'user',

        parts: [
          {
            text:
              prompt
          }
        ]
      }
    ],

    generationConfig
  };

  const controller =
    new AbortController();

  let requestTimedOut =
    false;

  let jobAborted =
    false;

  const onJobAbort =
    () => {
      jobAborted =
        true;

      controller.abort();
    };

  const signal =
    job
      ?.abortController
      ?.signal;

  if (signal) {
    if (
      signal.aborted
    ) {
      onJobAbort();
    } else {
      signal.addEventListener(
        'abort',
        onJobAbort,
        {
          once:
            true
        }
      );
    }
  }

  const timeoutMs =
    requestTimeoutForKind(
      requestKind
    );

  const timer =
    setTimeout(
      () => {
        requestTimedOut =
          true;

        controller.abort();
      },
      timeoutMs
    );

  let response;
  let rawText;

  try {
    response =
      await fetch(
        endpoint,
        {
          method:
            'POST',

          headers: {
            'Content-Type':
              'application/json',

            'x-goog-api-key':
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

    rawText =
      await response.text();
  } catch (error) {
    if (
      jobAborted ||
      job?.cancelled
    ) {
      throw jobCancelledError(
        job
          ?.cancelReason ||
        'Job cancelado.'
      );
    }

    if (
      requestTimedOut
    ) {
      throw geminiRequestTimeoutError(
        requestKind
      );
    }

    const wrapped =
      new Error(
        getErrorMessage(
          error
        )
      );

    wrapped.transient =
      true;

    wrapped.cause =
      error;

    throw wrapped;
  } finally {
    clearTimeout(
      timer
    );

    try {
      signal
        ?.removeEventListener(
          'abort',
          onJobAbort
        );
    } catch {}
  }

  assertJobActive(
    job
  );

  let data;

  try {
    data =
      JSON.parse(
        rawText
      );
  } catch {
    const error =
      new Error(
        `Resposta não-JSON do Gemini. HTTP ${response.status}.`
      );

    error.status =
      response.status;

    error.transient =
      isTransientHttpStatus(
        response.status
      );

    throw error;
  }

  if (
    !response.ok
  ) {
    const message =
      data
        ?.error
        ?.message ||
      `HTTP ${response.status}`;

    const error =
      new Error(
        message
      );

    error.status =
      response.status;

    error.rateLimit =
      isRateLimitError(
        response.status,
        message
      );

    error.dailyQuota =
      error.rateLimit &&
      isDailyQuotaError(
        message
      );

    error.retryAfterMs =
      error.rateLimit
        ? getRetryAfterMs(
            response,
            data
          )
        : 0;

    error.transient =
      isTransientHttpStatus(
        response.status
      ) &&
      !error.dailyQuota;

    throw error;
  }

  const text =
    data
      ?.candidates
      ?.[0]
      ?.content
      ?.parts
      ?.map(
        part =>
          part?.text ||
          ''
      )
      .join('')
      .trim();

  if (!text) {
    const error =
      new Error(
        'Gemini não retornou conteúdo.'
      );

    error.transient =
      true;

    throw error;
  }

  return text;
}

/* ========================================================================== */
/* FILA GEMINI RESILIENTE                                                     */
/* ========================================================================== */

function enqueueGemini(
  prompt,
  requestKind,
  job
) {
  return new Promise(
    (
      resolve,
      reject
    ) => {
      try {
        assertJobActive(
          job
        );
      } catch (error) {
        reject(
          error
        );

        return;
      }

      geminiQueue.push({
        prompt,
        requestKind,
        job,
        resolve,
        reject
      });

      pumpGeminiQueue();
    }
  );
}

function pumpGeminiQueue() {
  while (
    activeGeminiWorkers <
    GEMINI_CONCURRENCY &&
    geminiQueue.length > 0
  ) {
    const item =
      geminiQueue.shift();

    activeGeminiWorkers++;

    runGeminiItem(
      item
    )
      .catch(
        () => {}
      )
      .finally(
        () => {
          activeGeminiWorkers--;

          pumpGeminiQueue();
        }
      );
  }
}

async function runGeminiItem(item) {
  let transientAttempt =
    0;

  try {
    for (;;) {
      try {
        assertJobActive(
          item.job
        );

        await acquireGeminiRateSlot(
          item.job
        );

        assertJobActive(
          item.job
        );

        item.job.geminiRequestsUsed =
          Number(
            item.job.geminiRequestsUsed ||
            0
          ) +
          1;

        lifetimeGeminiRequests++;

        console.log(
          `[GEMINI] Request ` +
          `job=${item.job.geminiRequestsUsed} ` +
          `total=${lifetimeGeminiRequests} ` +
          `(${item.requestKind || 'translation'}) | ` +
          `ativos=${activeGeminiWorkers}/${GEMINI_CONCURRENCY} | ` +
          `janela=${geminiRequestStarts.length}/${SAFE_RPM}`
        );

        const result =
          await rawGeminiRequest(
            item.prompt,
            item.requestKind ||
            'translation',
            item.job
          );

        item.resolve(
          result
        );

        return;
      } catch (error) {
        if (
          error?.code ===
          'JOB_CANCELLED'
        ) {
          item.reject(
            error
          );

          return;
        }

        if (
          error?.rateLimit
        ) {
          if (
            error?.dailyQuota
          ) {
            item.reject(
              makeError(
                'DAILY_QUOTA_EXCEEDED',
                `Cota diária real do Gemini atingida: ${getErrorMessage(error)}`
              )
            );

            return;
          }

          setGeminiCooldown(
            error.retryAfterMs ||
            30000
          );

          transientAttempt++;

          console.warn(
            `[GEMINI] 429 transitório; ` +
            `aguardando a janela e mantendo o job vivo. ` +
            `ciclo=${transientAttempt}.`
          );

          continue;
        }

        if (
          isTransientGeminiError(
            error
          ) &&
          transientAttempt <
          MAX_TRANSIENT_RETRIES
        ) {
          transientAttempt++;

          const wait =
            transientBackoffMs(
              transientAttempt
            );

          console.warn(
            `[GEMINI] Erro transitório ` +
            `(${item.requestKind || 'translation'}): ` +
            `${getErrorMessage(error)} | ` +
            `retry ${transientAttempt}/${MAX_TRANSIENT_RETRIES} ` +
            `em ${(wait / 1000).toFixed(1)}s.`
          );

          await sleepAbortable(
            wait,
            item.job
          );

          continue;
        }

        item.reject(
          error
        );

        return;
      }
    }
  } catch (error) {
    item.reject(
      error
    );
  }
}

/* ========================================================================== */
/* TRADUÇÃO COMPACTA                                                         */
/* ========================================================================== */

async function translateBatchOnce(
  blocks,
  context,
  job,
  options = {}
) {
  const raw =
    await enqueueGemini(
      buildTranslationPrompt(
        blocks,
        context,
        options
      ),
      options.requestKind ||
      'translation',
      job
    );

  let parsed;

  try {
    parsed =
      JSON.parse(
        stripCodeFences(
          raw
        )
      );
  } catch {
    throw badModelOutputError(
      'Gemini retornou JSON inválido.'
    );
  }

  if (
    !Array.isArray(
      parsed
    ) ||
    parsed.length !==
    blocks.length
  ) {
    throw badModelOutputError(
      `Quantidade incorreta de blocos: ` +
      `esperado=${blocks.length}, ` +
      `recebido=${Array.isArray(parsed) ? parsed.length : 0}.`
    );
  }

  const seen =
    new Set();

  const texts =
    [];

  for (
    let i = 0;
    i < blocks.length;
    i++
  ) {
    const expected =
      blocks[i];

    const lock =
      blockTranslationLock(
        expected
      );

    const item =
      parsed[i];

    if (
      !item ||
      !Number.isInteger(
        item.i
      ) ||
      item.i !==
      expected.index ||
      seen.has(
        item.i
      ) ||
      item.l !==
      lock ||
      typeof item.t !==
      'string' ||
      !item.t.trim()
    ) {
      throw badModelOutputError(
        `Contrato ID/lock inválido na posição ${i + 1}; ` +
        `esperado ID ${expected.index}.`
      );
    }

    seen.add(
      item.i
    );

    texts.push(
      item.t
    );
  }

  console.log(
    `[AUDIT ID] OK — ` +
    `${blocks.length}/${blocks.length} bloco(s); ` +
    `ordem, IDs e locks preservados exatamente.`
  );

  return texts;
}

function contextForLocalSlice(
  blocks,
  start,
  endExclusive,
  outerContext
) {
  return {
    before:
      start > 0
        ? blocks[
            start - 1
          ]
        : outerContext
            ?.before ||
          null,

    after:
      endExclusive <
      blocks.length
        ? blocks[
            endExclusive
          ]
        : outerContext
            ?.after ||
          null
  };
}

function contextForGlobalWindow(
  blocks,
  start,
  endInclusive
) {
  return {
    before:
      start > 0
        ? blocks[
            start - 1
          ]
        : null,

    after:
      endInclusive <
      blocks.length - 1
        ? blocks[
            endInclusive + 1
          ]
        : null
  };
}

/* ========================================================================== */
/* RESCUE                                                                     */
/* ========================================================================== */

function splitFixedRescueBatches(blocks) {
  const result =
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
      String(
        block.text ||
        ''
      ).length +
      20;

    if (
      current.length &&
      (
        current.length >=
        RESCUE_BATCH_BLOCKS ||
        chars + size >
        RESCUE_BATCH_CHARS
      )
    ) {
      result.push(
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
    result.push(
      current
    );
  }

  return result;
}

async function translateSingleLastChance(
  block,
  context,
  job,
  repairMode = false
) {
  return translateBatchOnce(
    [
      block
    ],
    context,
    job,
    {
      requestKind:
        repairMode
          ? 'translation-repair'
          : 'translation-single',

      strictStructure:
        true,

      repairMode
    }
  );
}

async function translateRescueChunk(
  blocks,
  context,
  job,
  depth = 0
) {
  try {
    return await translateBatchOnce(
      blocks,
      context,
      job,
      {
        requestKind:
          'translation-rescue',

        strictStructure:
          true
      }
    );
  } catch (error) {
    const reducible =
      error?.code ===
      'BAD_MODEL_OUTPUT' ||
      isTransientGeminiError(
        error
      );

    if (!reducible) {
      throw error;
    }

    if (
      blocks.length === 1
    ) {
      return translateSingleLastChance(
        blocks[0],
        context,
        job,
        false
      );
    }

    if (
      depth >=
      MAX_RESCUE_SPLIT_DEPTH
    ) {
      throw error;
    }

    const middle =
      Math.ceil(
        blocks.length /
        2
      );

    const left =
      blocks.slice(
        0,
        middle
      );

    const right =
      blocks.slice(
        middle
      );

    console.warn(
      `[RESCUE 6.5] ${blocks.length} -> ` +
      `${left.length}+${right.length}.`
    );

    const [
      leftTexts,
      rightTexts
    ] =
      await Promise.all([
        translateRescueChunk(
          left,
          contextForLocalSlice(
            blocks,
            0,
            middle,
            context
          ),
          job,
          depth + 1
        ),

        translateRescueChunk(
          right,
          contextForLocalSlice(
            blocks,
            middle,
            blocks.length,
            context
          ),
          job,
          depth + 1
        )
      ]);

    return [
      ...leftTexts,
      ...rightTexts
    ];
  }
}

async function translateBatchRescue(
  blocks,
  context,
  job
) {
  const chunks =
    splitFixedRescueBatches(
      blocks
    );

  console.warn(
    `[RESCUE 6.5] ${blocks.length} bloco(s) -> ` +
    `${chunks.length} micro-lote(s), ` +
    `máx ${RESCUE_BATCH_BLOCKS}/${RESCUE_BATCH_CHARS}.`
  );

  let cursor =
    0;

  const tasks =
    chunks.map(
      chunk => {
        const start =
          cursor;

        cursor +=
          chunk.length;

        return translateRescueChunk(
          chunk,
          contextForLocalSlice(
            blocks,
            start,
            start +
            chunk.length,
            context
          ),
          job,
          0
        );
      }
    );

  return (
    await Promise.all(
      tasks
    )
  ).flat();
}

async function translateMainBatch(
  blocks,
  context,
  job
) {
  try {
    return await translateBatchOnce(
      blocks,
      context,
      job,
      {
        requestKind:
          'translation'
      }
    );
  } catch (error) {
    if (
      error?.code ===
      'BAD_MODEL_OUTPUT'
    ) {
      console.warn(
        `[FAST 6.5] Contrato inválido em lote ${blocks.length}; ` +
        `UMA repetição estrutural do mesmo lote.`
      );

      try {
        return await translateBatchOnce(
          blocks,
          context,
          job,
          {
            requestKind:
              'translation-structure-retry',

            strictStructure:
              true
          }
        );
      } catch (retryError) {
        if (
          retryError?.code !==
          'BAD_MODEL_OUTPUT'
        ) {
          throw retryError;
        }

        console.warn(
          `[FAST 6.5] Repetição estrutural ainda inválida; ` +
          `RESCUE somente deste lote.`
        );

        return translateBatchRescue(
          blocks,
          context,
          job
        );
      }
    }

    /*
     * Se mesmo depois dos retries da chamada
     * restar um problema transitório, diminui
     * somente este lote.
     */
    if (
      isTransientGeminiError(
        error
      )
    ) {
      console.warn(
        `[FAST 6.5] Lote ${blocks.length} permaneceu lento/indisponível ` +
        `após retries; RESCUE menor sem perder os demais lotes.`
      );

      return translateBatchRescue(
        blocks,
        context,
        job
      );
    }

    throw error;
  }
}

/* ========================================================================== */
/* SAFE PROPORCIONAL                                                          */
/* ========================================================================== */

function normalizedRiskText(value) {
  return String(
    value ||
    ''
  )
    .replace(
      /<[^>]+>|\{[^}]+\}/g,
      ' '
    )
    .replace(
      /\s+/g,
      ' '
    )
    .trim();
}

function strongSentenceEnd(text) {
  return /[.!?…]["'”’\)\]]*$/u.test(
    normalizedRiskText(
      text
    )
  );
}

function strictFragmentTail(text) {
  const value =
    normalizedRiskText(
      text
    );

  return (
    /[,;:–—-]\s*$/u.test(
      value
    ) ||
    /\b(?:and|but|or|because|so|to|of|for|with|that|which|who|when|if|than|as)\s*$/iu.test(
      value
    )
  );
}

function startsLikeContinuation(text) {
  const value =
    normalizedRiskText(
      text
    );

  return (
    /^[a-z]/u.test(
      value
    ) ||
    /^(?:and|but|or|so|because|then|that|which|who|when|if|to|of|for|with)\b/iu.test(
      value
    )
  );
}

function strongBoundaryRisk(
  left,
  right
) {
  if (
    !left ||
    !right
  ) {
    return false;
  }

  const l =
    normalizedRiskText(
      left.text
    );

  const r =
    normalizedRiskText(
      right.text
    );

  if (
    !l ||
    !r
  ) {
    return false;
  }

  if (
    strictFragmentTail(
      l
    )
  ) {
    return true;
  }

  return (
    !strongSentenceEnd(
      l
    ) &&
    l.length <=
    44 &&
    startsLikeContinuation(
      r
    )
  );
}

function translationLengthRisk(
  source,
  translated
) {
  const sourceLength =
    normalizedRiskText(
      source
    ).length;

  const translatedLength =
    normalizedRiskText(
      translated
    ).length;

  if (
    sourceLength < 24 ||
    translatedLength === 0
  ) {
    return false;
  }

  const ratio =
    translatedLength /
    sourceLength;

  return (
    ratio < 0.24 ||
    ratio > 4.2
  );
}

function suspiciousNeighborDuplicate(
  translatedTexts,
  index,
  blocks
) {
  const current =
    normalizedRiskText(
      translatedTexts[index]
    ).toLowerCase();

  if (
    current.length < 16
  ) {
    return false;
  }

  for (
    const neighborIndex
    of [
      index - 1,
      index + 1
    ]
  ) {
    if (
      neighborIndex < 0 ||
      neighborIndex >=
      translatedTexts.length
    ) {
      continue;
    }

    const neighbor =
      normalizedRiskText(
        translatedTexts[
          neighborIndex
        ]
      ).toLowerCase();

    const source =
      normalizedRiskText(
        blocks[index]?.text
      ).toLowerCase();

    const neighborSource =
      normalizedRiskText(
        blocks[
          neighborIndex
        ]?.text
      ).toLowerCase();

    if (
      current === neighbor &&
      source !== neighborSource
    ) {
      return true;
    }
  }

  return false;
}

function evenlySpacedIndices(
  length,
  target
) {
  if (
    length <= 0 ||
    target <= 0
  ) {
    return [];
  }

  if (
    target >= length
  ) {
    return Array.from(
      {
        length
      },
      (
        _,
        i
      ) =>
        i
    );
  }

  const result =
    new Set();

  if (
    target === 1
  ) {
    return [
      Math.floor(
        (
          length - 1
        ) /
        2
      )
    ];
  }

  for (
    let i = 0;
    i < target;
    i++
  ) {
    result.add(
      Math.round(
        (
          i *
          (
            length - 1
          )
        ) /
        (
          target - 1
        )
      )
    );
  }

  return Array.from(
    result
  ).sort(
    (
      a,
      b
    ) =>
      a - b
  );
}

function spreadPick(
  values,
  target
) {
  const unique =
    Array.from(
      new Set(
        values
      )
    ).sort(
      (
        a,
        b
      ) =>
        a - b
    );

  if (
    unique.length <=
    target
  ) {
    return unique;
  }

  return evenlySpacedIndices(
    unique.length,
    target
  ).map(
    position =>
      unique[position]
  );
}

function safeAuditTarget(blockCount) {
  return Math.min(
    blockCount,
    Math.max(
      SAFE_AUDIT_MIN_RECORDS,
      Math.ceil(
        blockCount *
        SAFE_AUDIT_RATIO
      )
    )
  );
}

function canaryTargetFor(
  blockCount,
  jobKind
) {
  if (
    jobKind ===
    'embedded'
  ) {
    return Math.min(
      blockCount,
      Math.max(
        EMBEDDED_CANARY_MIN,
        Math.ceil(
          blockCount /
          EMBEDDED_CANARY_EVERY
        )
      )
    );
  }

  return Math.min(
    blockCount,
    Math.max(
      OPENSUB_CANARY_MIN,
      Math.ceil(
        blockCount /
        OPENSUB_CANARY_EVERY
      )
    )
  );
}

function selectFinalAuditIndices(
  blocks,
  translatedTexts,
  batchBoundaryStarts,
  jobKind
) {
  const target =
    safeAuditTarget(
      blocks.length
    );

  const canaryTarget =
    Math.min(
      target,
      canaryTargetFor(
        blocks.length,
        jobKind
      )
    );

  const selected =
    new Set();

  const reasons =
    new Map();

  function add(
    index,
    reason
  ) {
    if (
      !Number.isInteger(
        index
      ) ||
      index < 0 ||
      index >=
      blocks.length
    ) {
      return false;
    }

    if (
      selected.size >=
      target &&
      !selected.has(
        index
      )
    ) {
      return false;
    }

    if (
      !selected.has(
        index
      )
    ) {
      selected.add(
        index
      );

      reasons.set(
        index,
        reason
      );

      return true;
    }

    return false;
  }

  /*
   * 1) CANÁRIOS RESERVADOS.
   */
  for (
    const index
    of evenlySpacedIndices(
      blocks.length,
      canaryTarget
    )
  ) {
    add(
      index,
      'canary'
    );
  }

  /*
   * 2) Fronteiras de lote.
   */
  for (
    const start
    of [
      ...batchBoundaryStarts
    ].sort(
      (
        a,
        b
      ) =>
        a - b
    )
  ) {
    add(
      start - 1,
      'batch-boundary'
    );

    add(
      start,
      'batch-boundary'
    );
  }

  /*
   * 3) Anomalias determinísticas.
   */
  const anomalies =
    [];

  for (
    let i = 0;
    i < blocks.length;
    i++
  ) {
    if (
      translationLengthRisk(
        blocks[i]?.text,
        translatedTexts[i]
      ) ||
      suspiciousNeighborDuplicate(
        translatedTexts,
        i,
        blocks
      )
    ) {
      anomalies.push(
        i
      );
    }
  }

  for (
    const index
    of anomalies
  ) {
    add(
      index,
      'anomaly'
    );
  }

  /*
   * 4) Fronteiras sintáticas fortes
   * espalhadas pelo episódio/filme.
   */
  const strongCandidates =
    [];

  for (
    let i = 0;
    i <
    blocks.length - 1;
    i++
  ) {
    if (
      strongBoundaryRisk(
        blocks[i],
        blocks[i + 1]
      )
    ) {
      strongCandidates.push(
        i,
        i + 1
      );
    }
  }

  const remaining =
    target -
    selected.size;

  if (
    remaining > 0
  ) {
    for (
      const index
      of spreadPick(
        strongCandidates.filter(
          i =>
            !selected.has(
              i
            )
        ),
        remaining
      )
    ) {
      add(
        index,
        'strong-boundary'
      );
    }
  }

  const indices =
    Array.from(
      selected
    ).sort(
      (
        a,
        b
      ) =>
        a - b
    );

  const counts =
    {};

  for (
    const index
    of indices
  ) {
    const reason =
      reasons.get(
        index
      ) ||
      'unknown';

    counts[reason] =
      (
        counts[reason] ||
        0
      ) +
      1;
  }

  return {
    indices,
    counts,
    target,
    canaryTarget,

    strongCandidates:
      new Set(
        strongCandidates
      ).size,

    anomalyCandidates:
      anomalies.length
  };
}

function buildAuditRecord(
  blocks,
  translatedTexts,
  index
) {
  const block =
    blocks[index];

  const previous =
    index > 0
      ? blocks[
          index - 1
        ]
      : null;

  const next =
    index <
    blocks.length - 1
      ? blocks[
          index + 1
        ]
      : null;

  return {
    id:
      block.index,

    lock:
      blockTranslationLock(
        block
      ),

    source:
      block.text,

    translation:
      translatedTexts[index],

    speaker:
      block.speakerHint ||
      null,

    prev:
      previous
        ? {
            id:
              previous.index,

            source:
              previous.text,

            translation:
              translatedTexts[
                index - 1
              ]
          }
        : null,

    next:
      next
        ? {
            id:
              next.index,

            source:
              next.text,

            translation:
              translatedTexts[
                index + 1
              ]
          }
        : null,

    _index:
      index
  };
}

function auditRecordForModel(record) {
  const item = {
    i:
      record.id,

    l:
      record.lock,

    s:
      record.source,

    t:
      record.translation
  };

  if (
    record.speaker
  ) {
    item.h =
      record.speaker;
  }

  if (
    record.prev
  ) {
    item.p =
      [
        record.prev.id,
        record.prev.source,
        record.prev.translation
      ];
  }

  if (
    record.next
  ) {
    item.n =
      [
        record.next.id,
        record.next.source,
        record.next.translation
      ];
  }

  return item;
}

function buildSemanticAuditPrompt(records) {
  return (
    'AUDITORIA FINAL CIRÚRGICA. ' +
    'Avalie fronteira EN↔PT e se o termo escolhido faz sentido NESTA situação, ' +
    'soa natural/atual e combina com p/n. ' +
    'Use SOMENTE p/n do próprio registro. ' +
    'm só pode ser i, p[0] ou n[0]. ' +
    'SAÍDA EXATA: [{"i":123,"l":"abc","m":123,"f":true}]\n' +
    JSON.stringify(
      records.map(
        auditRecordForModel
      )
    )
  );
}

function allowedMatchedIds(record) {
  const allowed =
    new Set(
      [
        record.id
      ]
    );

  if (
    record.prev
  ) {
    allowed.add(
      record.prev.id
    );
  }

  if (
    record.next
  ) {
    allowed.add(
      record.next.id
    );
  }

  return allowed;
}

/* ========================================================================== */
/* AUDITORIA RESILIENTE                                                       */
/* ========================================================================== */

async function auditRecordsOnce(
  records,
  job
) {
  const raw =
    await enqueueGemini(
      buildSemanticAuditPrompt(
        records
      ),
      'semantic-audit',
      job
    );

  let parsed;

  try {
    parsed =
      JSON.parse(
        stripCodeFences(
          raw
        )
      );
  } catch {
    throw badAuditOutputError(
      'Auditoria retornou JSON inválido.'
    );
  }

  if (
    !Array.isArray(
      parsed
    ) ||
    parsed.length !==
    records.length
  ) {
    throw badAuditOutputError(
      `Auditoria incompleta: ` +
      `esperado=${records.length}, ` +
      `recebido=${Array.isArray(parsed) ? parsed.length : 0}.`
    );
  }

  const failures =
    [];

  const invalid =
    [];

  for (
    let i = 0;
    i < records.length;
    i++
  ) {
    const expected =
      records[i];

    const item =
      parsed[i];

    if (
      !item ||
      item.i !==
      expected.id ||
      item.l !==
      expected.lock ||
      !Number.isInteger(
        item.m
      ) ||
      typeof item.f !==
      'boolean'
    ) {
      invalid.push(
        expected
      );

      continue;
    }

    if (
      !allowedMatchedIds(
        expected
      ).has(
        item.m
      )
    ) {
      console.warn(
        `[AUDIT 6.5] matchedSourceId inválido ` +
        `${expected.id}->${item.m}; ` +
        `será rechecado, nunca tratado como migração distante.`
      );

      invalid.push(
        expected
      );

      continue;
    }

    if (
      item.f !== true ||
      item.m !==
      expected.id
    ) {
      failures.push({
        id:
          expected.id,

        matchedSourceId:
          item.m,

        faithful:
          item.f,

        _index:
          expected._index
      });
    }
  }

  const sample =
    failures
      .slice(
        0,
        8
      )
      .map(
        item =>
          `${item.id}->${item.matchedSourceId}`
      )
      .join(
        ', '
      );

  if (
    failures.length
  ) {
    console.warn(
      `[AUDIT SAFE] ${failures.length}/${records.length} suspeito(s)` +
      `${sample ? `: ${sample}` : ''}.`
    );
  } else {
    console.log(
      `[AUDIT SAFE] OK — ` +
      `${records.length}/${records.length} bloco(s).`
    );
  }

  return {
    failures,
    invalid
  };
}

/*
 * TIMEOUT/indisponibilidade:
 * - a fila já fez retry/backoff;
 * - se o chunk ainda estiver pesado, reduz o chunk.
 *
 * Saída estrutural ruim:
 * - reduz o chunk.
 *
 * Nunca existe árvore baseada em timeout global do job.
 */
async function auditRecordsResilient(
  records,
  job,
  singleRecoveryUsed = false
) {
  try {
    return await auditRecordsOnce(
      records,
      job
    );
  } catch (error) {
    const reducible =
      error?.code ===
      'BAD_AUDIT_OUTPUT' ||
      isTransientGeminiError(
        error
      );

    if (!reducible) {
      throw error;
    }

    if (
      records.length === 1
    ) {
      if (
        singleRecoveryUsed
      ) {
        throw error;
      }

      console.warn(
        `[AUDIT 6.5] Auditoria de 1 ID permaneceu transitória; ` +
        `pausa curta e novo ciclo resiliente.`
      );

      await sleepAbortable(
        3000,
        job
      );

      return auditRecordsResilient(
        records,
        job,
        true
      );
    }

    const middle =
      Math.ceil(
        records.length /
        2
      );

    console.warn(
      `[AUDIT 6.5] Chunk ${records.length} lento/inválido; ` +
      `reduzindo adaptativamente para ` +
      `${middle}+${records.length - middle}.`
    );

    const [
      left,
      right
    ] =
      await Promise.all([
        auditRecordsResilient(
          records.slice(
            0,
            middle
          ),
          job,
          false
        ),

        auditRecordsResilient(
          records.slice(
            middle
          ),
          job,
          false
        )
      ]);

    return {
      failures: [
        ...left.failures,
        ...right.failures
      ],

      invalid: [
        ...left.invalid,
        ...right.invalid
      ]
    };
  }
}

async function runAuditChunks(
  records,
  job,
  chunkSize =
    AUDIT_CHUNK_SIZE
) {
  if (
    !records.length
  ) {
    return {
      failures: [],
      invalid: []
    };
  }

  const chunks =
    [];

  for (
    let i = 0;
    i < records.length;
    i += chunkSize
  ) {
    chunks.push(
      records.slice(
        i,
        i + chunkSize
      )
    );
  }

  const results =
    await Promise.all(
      chunks.map(
        chunk =>
          auditRecordsResilient(
            chunk,
            job
          )
      )
    );

  return {
    failures:
      results.flatMap(
        result =>
          result.failures
      ),

    invalid:
      results.flatMap(
        result =>
          result.invalid
      )
  };
}

async function recheckRecordsInGroups(
  records,
  job,
  label
) {
  if (
    !records.length
  ) {
    return [];
  }

  const chunks =
    [];

  for (
    let i = 0;
    i < records.length;
    i += RECHECK_GROUP_SIZE
  ) {
    chunks.push(
      records.slice(
        i,
        i + RECHECK_GROUP_SIZE
      )
    );
  }

  console.log(
    `[AUDIT SAFE] ${label}: ` +
    `rechecando ${records.length} registro(s) ` +
    `em ${chunks.length} grupo(s).`
  );

  const results =
    await Promise.all(
      chunks.map(
        chunk =>
          auditRecordsResilient(
            chunk,
            job
          )
      )
    );

  const failures =
    [];

  for (
    const result
    of results
  ) {
    failures.push(
      ...result.failures
    );

    for (
      const record
      of result.invalid
    ) {
      failures.push({
        id:
          record.id,

        matchedSourceId:
          record.id,

        faithful:
          false,

        _index:
          record._index
      });
    }
  }

  return failures;
}

/* ========================================================================== */
/* REPARO + VERIFY POR JANELA                                                 */
/* ========================================================================== */

function buildRepairWindows(
  failures,
  blocks
) {
  const idToIndex =
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

  const suspectIndices =
    new Set();

  for (
    const failure
    of failures
  ) {
    const own =
      idToIndex.get(
        failure.id
      );

    if (
      Number.isInteger(
        own
      )
    ) {
      suspectIndices.add(
        own
      );
    }

    const matched =
      idToIndex.get(
        failure.matchedSourceId
      );

    if (
      Number.isInteger(
        own
      ) &&
      Number.isInteger(
        matched
      ) &&
      Math.abs(
        own -
        matched
      ) <= 1
    ) {
      suspectIndices.add(
        matched
      );
    }
  }

  const sorted =
    Array.from(
      suspectIndices
    ).sort(
      (
        a,
        b
      ) =>
        a - b
    );

  const windows =
    [];

  let current =
    [];

  for (
    const index
    of sorted
  ) {
    if (
      current.length === 0 ||
      (
        index <=
        current[
          current.length - 1
        ] +
        1 &&
        current.length <
        MAX_REPAIR_WINDOW_BLOCKS
      )
    ) {
      current.push(
        index
      );
    } else {
      windows.push(
        current
      );

      current =
        [
          index
        ];
    }
  }

  if (
    current.length
  ) {
    windows.push(
      current
    );
  }

  return windows;
}

function logRepairDetails(
  blocks,
  translatedTexts,
  indices,
  stage
) {
  for (
    const index
    of indices
  ) {
    const block =
      blocks[index];

    console.log(
      `[REPAIR DETAIL] ${stage} | ` +
      `ID=${block.index} | ` +
      `${block.timing} | ` +
      `EN=${JSON.stringify(block.text)} | ` +
      `PT=${JSON.stringify(translatedTexts[index])}`
    );
  }
}

async function repairWindow(
  blocks,
  translatedTexts,
  indices,
  job
) {
  const repairBlocks =
    indices.map(
      index =>
        blocks[index]
    );

  const context =
    contextForGlobalWindow(
      blocks,
      indices[0],
      indices[
        indices.length - 1
      ]
    );

  console.warn(
    `[REPAIR 6.5] IDs ` +
    `${repairBlocks.map(block => block.index).join(',')} ` +
    `(${repairBlocks.length}).`
  );

  logRepairDetails(
    blocks,
    translatedTexts,
    indices,
    'ANTES'
  );

  let repaired;

  try {
    repaired =
      await translateBatchOnce(
        repairBlocks,
        context,
        job,
        {
          requestKind:
            'translation-repair',

          strictStructure:
            true,

          repairMode:
            true
        }
      );
  } catch (error) {
    if (
      error?.code !==
      'BAD_MODEL_OUTPUT' &&
      !isTransientGeminiError(
        error
      )
    ) {
      throw error;
    }

    console.warn(
      `[REPAIR 6.5] Janela grande permaneceu lenta/inválida; ` +
      `reparando por ID sem afetar o restante do job.`
    );

    repaired =
      [];

    for (
      let i = 0;
      i < repairBlocks.length;
      i++
    ) {
      const globalIndex =
        indices[i];

      const one =
        await translateSingleLastChance(
          repairBlocks[i],
          contextForGlobalWindow(
            blocks,
            globalIndex,
            globalIndex
          ),
          job,
          true
        );

      repaired.push(
        one[0]
      );
    }
  }

  for (
    let i = 0;
    i < indices.length;
    i++
  ) {
    translatedTexts[
      indices[i]
    ] =
      finalizeTranslatedText(
        repaired[i]
      );
  }

  logRepairDetails(
    blocks,
    translatedTexts,
    indices,
    'DEPOIS'
  );
}

async function verifyRepairWindow(
  blocks,
  translatedTexts,
  indices,
  job
) {
  const records =
    indices.map(
      index =>
        buildAuditRecord(
          blocks,
          translatedTexts,
          index
        )
    );

  console.log(
    `[REPAIR VERIFY] Reauditando janela ` +
    `${records.map(record => record.id).join(',')} ` +
    `(${records.length}).`
  );

  const result =
    await auditRecordsResilient(
      records,
      job
    );

  if (
    result.invalid.length ||
    result.failures.length
  ) {
    const ids =
      [
        ...result.failures.map(
          item =>
            item.id
        ),

        ...result.invalid.map(
          item =>
            item.id
        )
      ];

    return {
      ok:
        false,

      ids:
        Array.from(
          new Set(
            ids
          )
        )
    };
  }

  console.log(
    `[REPAIR VERIFY] OK — ` +
    `${records.length}/${records.length} ID(s) da janela aprovados.`
  );

  return {
    ok:
      true,

    ids:
      []
  };
}

/*
 * Se o primeiro reparo ainda não passar,
 * fazemos UMA nova TRADUÇÃO da mesma janela.
 *
 * Isso é diferente de matar o job por timeout.
 * Todo FAST já concluído continua intacto.
 */
async function repairAndVerifyWindow(
  blocks,
  translatedTexts,
  indices,
  job
) {
  await repairWindow(
    blocks,
    translatedTexts,
    indices,
    job
  );

  let verified =
    await verifyRepairWindow(
      blocks,
      translatedTexts,
      indices,
      job
    );

  if (
    verified.ok
  ) {
    return;
  }

  console.warn(
    `[REPAIR 6.5] Janela ${verified.ids.join(',')} ainda reprovada; ` +
    `segunda tradução cirúrgica localizada.`
  );

  await repairWindow(
    blocks,
    translatedTexts,
    indices,
    job
  );

  verified =
    await verifyRepairWindow(
      blocks,
      translatedTexts,
      indices,
      job
    );

  if (
    !verified.ok
  ) {
    throw badAuditOutputError(
      `Reparo não aprovado após segunda tradução localizada. ` +
      `IDs: ${verified.ids.join(', ')}.`
    );
  }
}

/* ========================================================================== */
/* TRADUÇÃO COMPLETA                                                          */
/* ========================================================================== */

async function translateSrt(
  originalSrt,
  job
) {
  const blocks =
    parseSrt(
      originalSrt
    );

  if (
    !blocks.length
  ) {
    throw new Error(
      'Nenhum bloco SRT válido.'
    );
  }

  const batches =
    splitIntoBatches(
      blocks
    );

  const startedAt =
    Date.now();

  job.startedAt =
    startedAt;

  job.updatedAt =
    startedAt;

  job.geminiRequestsUsed =
    0;

  job.completedBatches =
    0;

  job.totalBatches =
    batches.length;

  console.log(
    `[TRANSLATE] ${blocks.length} blocos.`
  );

  console.log(
    `[TRANSLATE] ${batches.length} lote(s).`
  );

  console.log(
    `[FAST 6.5] ` +
    `${MAX_BATCH_BLOCKS} blocos/${MAX_BATCH_CHARS} chars; ` +
    `concorrência=${GEMINI_CONCURRENCY}; ` +
    `rate=${SAFE_RPM} RPM; ` +
    `SEM CAP por job; SEM timeout global.`
  );

  const translatedTexts =
    new Array(
      blocks.length
    );

  const originalPositions =
    new Map();

  blocks.forEach(
    (
      block,
      index
    ) =>
      originalPositions.set(
        block,
        index
      )
  );

  const batchBoundaryStarts =
    [];

  /*
   * CHECKPOINT FAST
   */
  const sourceSignature =
    sha256(
      originalSrt
    ).slice(
      0,
      24
    );

  const checkpoint =
    getOrCreatePartialCheckpoint(
      job.cacheKey,
      blocks.length,
      sourceSignature
    );

  let reusedBlocks =
    0;

  for (
    let i = 0;
    i < blocks.length;
    i++
  ) {
    if (
      checkpoint.done[i] &&
      typeof checkpoint.texts[i] ===
      'string'
    ) {
      translatedTexts[i] =
        checkpoint.texts[i];

      reusedBlocks++;
    }
  }

  if (
    reusedBlocks
  ) {
    console.log(
      `[CHECKPOINT] Reutilizando ` +
      `${reusedBlocks}/${blocks.length} bloco(s) FAST ` +
      `já validados de tentativa anterior.`
    );
  }

  console.log(
    `[PHASE FAST] Pipeline 3x iniciado; ` +
    `cada lote aprovado vira checkpoint reutilizável.`
  );

  const tasks =
    batches.map(
      async (
        batch,
        batchIndex
      ) => {
        assertJobActive(
          job
        );

        const batchStart =
          originalPositions.get(
            batch[0]
          );

        const batchEnd =
          originalPositions.get(
            batch[
              batch.length - 1
            ]
          );

        if (
          batchIndex > 0 &&
          Number.isInteger(
            batchStart
          )
        ) {
          batchBoundaryStarts.push(
            batchStart
          );
        }

        /*
         * Reutilização de lote inteiro.
         */
        if (
          checkpointRangeComplete(
            checkpoint,
            batchStart,
            batch.length
          )
        ) {
          for (
            let i = 0;
            i < batch.length;
            i++
          ) {
            translatedTexts[
              batchStart + i
            ] =
              checkpoint.texts[
                batchStart + i
              ];
          }

          job.completedBatches++;

          console.log(
            `[CHECKPOINT] Lote ${batchIndex + 1}/${batches.length} ` +
            `reutilizado — ${batch.length} blocos; 0 request Gemini.`
          );

          return;
        }

        const context = {
          before:
            Number.isInteger(
              batchStart
            ) &&
            batchStart > 0
              ? blocks[
                  batchStart - 1
                ]
              : null,

          after:
            Number.isInteger(
              batchEnd
            ) &&
            batchEnd <
            blocks.length - 1
              ? blocks[
                  batchEnd + 1
                ]
              : null
        };

        const chars =
          batch.reduce(
            (
              sum,
              block
            ) =>
              sum +
              String(
                block.text ||
                ''
              ).length,
            0
          );

        const batchStartedAt =
          Date.now();

        console.log(
          `[PHASE FAST] Lote ` +
          `${batchIndex + 1}/${batches.length} ` +
          `enfileirado - ` +
          `${batch.length} blocos/${chars} chars.`
        );

        const translated =
          await translateMainBatch(
            batch,
            context,
            job
          );

        assertJobActive(
          job
        );

        for (
          let i = 0;
          i < batch.length;
          i++
        ) {
          translatedTexts[
            batchStart + i
          ] =
            translated[i];
        }

        /*
         * Assim que ID/lock daquele lote passou,
         * checkpoint é salvo.
         */
        writeCheckpointRange(
          checkpoint,
          batchStart,
          translated
        );

        job.completedBatches++;

        job.progress =
          Math.round(
            (
              job.completedBatches /
              batches.length
            ) *
            82
          );

        job.updatedAt =
          Date.now();

        console.log(
          `[CHECKPOINT] Lote ` +
          `${batchIndex + 1}/${batches.length} salvo.`
        );

        console.log(
          `[PHASE FAST] Lote ` +
          `${batchIndex + 1}/${batches.length} OK em ` +
          `${((Date.now() - batchStartedAt) / 1000).toFixed(1)}s.`
        );
      }
    );

  try {
    await Promise.all(
      tasks
    );
  } catch (error) {
    /*
     * Só chega aqui após:
     * - retries transitórios;
     * - redução/rescue localizada.
     *
     * Se for realmente irrecuperável,
     * cancelamos chamadas irmãs,
     * MAS CHECKPOINTS permanecem.
     */
    cancelJobOperations(
      job,
      error
    );

    console.warn(
      `[CHECKPOINT] Progresso FAST já aprovado foi preservado por ` +
      `${Math.round(PARTIAL_CHECKPOINT_TTL_MS / 3600000)}h ` +
      `para uma nova tentativa do mesmo SRT.`
    );

    throw error;
  }

  if (
    translatedTexts.some(
      text =>
        typeof text !==
        'string'
    )
  ) {
    throw new Error(
      'A fase FAST terminou com blocos ausentes.'
    );
  }

  const fastElapsed =
    Date.now() -
    startedAt;

  console.log(
    `[PHASE FAST] Tradução completa em ` +
    `${(fastElapsed / 1000).toFixed(1)}s; ` +
    `requests-do-job=${job.geminiRequestsUsed}; ` +
    `checkpoint=${blocks.length}/${blocks.length}.`
  );

  /*
   * SAFE lê exatamente o texto final.
   */
  const finalTexts =
    finalizeAllTranslatedTexts(
      translatedTexts
    );

  assertJobActive(
    job
  );

  const selection =
    selectFinalAuditIndices(
      blocks,
      finalTexts,
      batchBoundaryStarts,
      job.jobKind
    );

  console.log(
    `[PHASE SAFE] ` +
    `${selection.indices.length}/${blocks.length} selecionados | ` +
    `alvo-proporcional=${selection.target} | ` +
    `tipo=${job.jobKind} | ` +
    `canários=${selection.canaryTarget} | ` +
    `candidatos-fronteira=${selection.strongCandidates} | ` +
    `anomalias=${selection.anomalyCandidates} | ` +
    `composição=${JSON.stringify(selection.counts)}.`
  );

  const records =
    selection.indices.map(
      index =>
        buildAuditRecord(
          blocks,
          finalTexts,
          index
        )
    );

  let failures =
    [];

  try {
    if (
      records.length
    ) {
      const baseAudit =
        await runAuditChunks(
          records,
          job
        );

      failures.push(
        ...baseAudit.failures
      );

      if (
        baseAudit.invalid.length
      ) {
        failures.push(
          ...await recheckRecordsInGroups(
            baseAudit.invalid,
            job,
            'matchedSourceId/contrato inválido'
          )
        );
      }

      const ownFailures =
        failures.filter(
          item =>
            item.matchedSourceId ===
            item.id
        );

      const migrationFailures =
        failures.filter(
          item =>
            item.matchedSourceId !==
            item.id
        );

      if (
        ownFailures.length
      ) {
        const ownRecords =
          ownFailures.map(
            item =>
              buildAuditRecord(
                blocks,
                finalTexts,
                item._index
              )
          );

        const confirmedOwn =
          await recheckRecordsInGroups(
            ownRecords,
            job,
            'f=false/m=i (qualidade/contexto)'
          );

        failures =
          [
            ...migrationFailures,
            ...confirmedOwn
          ];
      } else {
        failures =
          migrationFailures;
      }
    }

    /*
     * Dedup diagnóstico.
     */
    const uniqueFailures =
      new Map();

    for (
      const failure
      of failures
    ) {
      uniqueFailures.set(
        `${failure.id}:${failure.matchedSourceId}`,
        failure
      );
    }

    failures =
      Array.from(
        uniqueFailures.values()
      );

    if (
      failures.length
    ) {
      console.warn(
        `[PHASE SAFE] ${failures.length} falha(s) confirmada(s); ` +
        `reparo somente das janelas afetadas.`
      );

      const windows =
        buildRepairWindows(
          failures,
          blocks
        );

      /*
       * Cada janela é:
       * reparar -> auditar -> se necessário reparar de novo.
       *
       * Janelas distantes não são agrupadas.
       */
      for (
        const window
        of windows
      ) {
        await repairAndVerifyWindow(
          blocks,
          finalTexts,
          window,
          job
        );
      }
    }
  } catch (error) {
    cancelJobOperations(
      job,
      error
    );

    console.warn(
      `[CHECKPOINT] FAST completo permanece preservado; ` +
      `uma nova tentativa não precisa começar do zero.`
    );

    throw error;
  }

  assertJobActive(
    job
  );

  const finalSrt =
    buildSrt(
      blocks,
      finalTexts
    );

  /*
   * Sincronização física absoluta.
   */
  auditFinalTimestamps(
    originalSrt,
    finalSrt,
    'TRADUÇÃO FINAL'
  );

  job.timestampAuditPassed =
    true;

  job.contentAuditPassed =
    true;

  job.progress =
    100;

  const elapsedMs =
    Date.now() -
    startedAt;

  console.log(
    `[TRANSLATE] Finalizada em ` +
    `${(elapsedMs / 1000).toFixed(1)}s.`
  );

  console.log(
    `[PERF] ` +
    `FAST=${(fastElapsed / 1000).toFixed(1)}s | ` +
    `SAFE+REPAIR=${((elapsedMs - fastElapsed) / 1000).toFixed(1)}s | ` +
    `TOTAL=${(elapsedMs / 1000).toFixed(1)}s | ` +
    `requests-do-job=${job.geminiRequestsUsed} | ` +
    `janela-RPM=${geminiRequestStarts.length}/${SAFE_RPM}.`
  );

  if (
    elapsedMs <=
    PERFORMANCE_TARGET_MS
  ) {
    console.log(
      '[PERF] META de velocidade ≤90s: ATINGIDA ✅'
    );
  } else {
    console.log(
      `[PERF] Acima da meta de 90s, mas job NÃO foi abortado: ` +
      `${(elapsedMs / 1000).toFixed(1)}s.`
    );
  }

  console.log(
    '[AUDIT CONTENT] TRADUÇÃO FINAL: OK — ' +
    'contexto, atualidade lexical, ID/lock, SAFE proporcional, ' +
    'reparos revalidados e timestamps íntegros.'
  );

  /*
   * Só apaga checkpoint quando existe produto FINAL aprovado.
   */
  partialTranslationCache.delete(
    partialCheckpointKey(
      job.cacheKey
    )
  );

  return finalSrt;
}

/* ========================================================================== */
/* JOBS                                                                       */
/* ========================================================================== */

function createJob({
  jobId,
  cacheKey,
  type,
  videoId,
  sourceHash,
  sourceSrt
}) {
  const now =
    Date.now();

  const job = {
    id:
      jobId,

    cacheKey,
    type,
    videoId,
    sourceHash,
    sourceSrt,

    status:
      'processing',

    result:
      null,

    error:
      null,

    progress:
      0,

    completedBatches:
      0,

    totalBatches:
      0,

    createdAt:
      now,

    updatedAt:
      now,

    startedAt:
      null,

    queuedAt:
      null,

    priority:
      50,

    jobKind:
      'generic',

    lazyStartScheduled:
      false,

    expiresAt:
      now +
      JOB_TTL_MS,

    timestampAuditPassed:
      false,

    contentAuditPassed:
      false,

    geminiRequestsUsed:
      0,

    cancelled:
      false,

    cancelReason:
      null,

    abortController:
      new AbortController(),

    promise:
      null
  };

  jobs.set(
    jobId,
    job
  );

  cleanupMemory();

  return job;
}

function getJob(jobId) {
  const job =
    jobs.get(
      jobId
    );

  if (!job) {
    return null;
  }

  if (
    job.expiresAt <=
    Date.now() &&
    ![
      'processing',
      'pending'
    ].includes(
      job.status
    )
  ) {
    jobs.delete(
      jobId
    );

    return null;
  }

  return job;
}

async function processJob(job) {
  console.log(
    `[JOB ${job.id}] Iniciando.`
  );

  try {
    const cached =
      getTranslationCache(
        job.cacheKey
      );

    if (cached) {
      auditFinalTimestamps(
        job.sourceSrt,
        cached,
        'CACHE'
      );

      job.timestampAuditPassed =
        true;

      job.contentAuditPassed =
        true;

      job.status =
        'completed';

      job.result =
        cached;

      job.progress =
        100;

      job.updatedAt =
        Date.now();

      console.log(
        `[JOB ${job.id}] Cache utilizado.`
      );

      return;
    }

    const translated =
      await translateSrt(
        job.sourceSrt,
        job
      );

    setTranslationCache(
      job.cacheKey,
      translated
    );

    job.result =
      translated;

    job.status =
      'completed';

    job.progress =
      100;

    job.updatedAt =
      Date.now();

    console.log(
      `[JOB ${job.id}] Concluído.`
    );
  } catch (error) {
    cancelJobOperations(
      job,
      error
    );

    job.status =
      'failed';

    job.error =
      getErrorMessage(
        error
      );

    job.updatedAt =
      Date.now();

    console.error(
      `[JOB ${job.id}] Falhou: ${job.error}`
    );
  }
}

function enqueueTranslationJob(job) {
  return new Promise(
    (
      resolve,
      reject
    ) => {
      if (
        job.status !==
        'processing'
      ) {
        resolve();
        return;
      }

      job.queuedAt =
        Date.now();

      const item = {
        job,
        resolve,
        reject
      };

      const insertAt =
        translationJobQueue
          .findIndex(
            queued =>
              Number(
                queued
                  ?.job
                  ?.priority ||
                0
              ) <
              Number(
                job.priority ||
                0
              )
          );

      if (
        insertAt === -1
      ) {
        translationJobQueue.push(
          item
        );
      } else {
        translationJobQueue.splice(
          insertAt,
          0,
          item
        );
      }

      console.log(
        `[JOB QUEUE] ${job.id} entrou na fila. ` +
        `Prioridade=${job.priority}; ` +
        `aguardando=${translationJobQueue.length}.`
      );

      processTranslationJobQueue();
    }
  );
}

async function processTranslationJobQueue() {
  if (
    translationJobWorkerRunning
  ) {
    return;
  }

  translationJobWorkerRunning =
    true;

  try {
    while (
      translationJobQueue.length
    ) {
      const item =
        translationJobQueue.shift();

      if (!item) {
        continue;
      }

      try {
        if (
          item.job.status !==
          'processing'
        ) {
          item.resolve();
          continue;
        }

        const wait =
          Number.isFinite(
            item.job.queuedAt
          )
            ? Date.now() -
              item.job.queuedAt
            : 0;

        console.log(
          `[JOB QUEUE] Iniciando ${item.job.id}; ` +
          `prioridade=${item.job.priority}; ` +
          `espera=${(wait / 1000).toFixed(1)}s.`
        );

        await processJob(
          item.job
        );

        item.resolve();
      } catch (error) {
        item.reject(
          error
        );
      }
    }
  } finally {
    translationJobWorkerRunning =
      false;

    if (
      translationJobQueue.length
    ) {
      processTranslationJobQueue();
    }
  }
}

function startTranslationJob(
  job,
  label = 'JOB'
) {
  if (!job) {
    return null;
  }

  if (
    [
      'completed',
      'failed'
    ].includes(
      job.status
    )
  ) {
    return job.promise;
  }

  if (
    job.promise
  ) {
    return job.promise;
  }

  if (
    job.status ===
    'pending'
  ) {
    job.status =
      'processing';

    job.updatedAt =
      Date.now();
  }

  job.promise =
    enqueueTranslationJob(
      job
    ).catch(
      error => {
        cancelJobOperations(
          job,
          error
        );

        console.error(
          `[${label} ${job.id}] Erro inesperado:`,
          error
        );

        job.status =
          'failed';

        job.error =
          getErrorMessage(
            error
          );

        job.updatedAt =
          Date.now();
      }
    );

  return job.promise;
}

function findProcessingJob(cacheKey) {
  for (
    const job
    of jobs.values()
  ) {
    if (
      job.cacheKey ===
      cacheKey &&
      [
        'processing',
        'pending'
      ].includes(
        job.status
      )
    ) {
      return job;
    }
  }

  return null;
}

/* ========================================================================== */
/* SRT TEMPORÁRIO / ERRO                                                      */
/* ========================================================================== */

function buildProcessingSrt(job) {
  const progress =
    Number.isFinite(
      job?.progress
    )
      ? job.progress
      : 0;

  return [
    '1',
    '00:00:01,000 --> 05:59:59,000',
    `⏳ Traduzindo legenda PT-BR... ${progress}%`
  ].join(
    '\n'
  );
}

function buildErrorSrt(message) {
  const safe =
    String(
      message ||
      'Erro desconhecido.'
    )
      .replace(
        /\s+/g,
        ' '
      )
      .slice(
        0,
        350
      );

  return [
    '1',
    '00:00:01,000 --> 05:59:59,000',
    `Não foi possível traduzir esta legenda. ${safe}`
  ].join(
    '\n'
  );
}

function sendSubtitleResponse(
  res,
  srt,
  cacheControl = 'no-store'
) {
  res.status(
    200
  );

  res.set({
    'Content-Type':
      'text/plain; charset=utf-8',

    'Cache-Control':
      cacheControl,

    'Access-Control-Allow-Origin':
      '*'
  });

  return res.send(
    srt
  );
}

async function waitForJob(
  job,
  timeoutMs
) {
  if (
    job.status ===
    'completed'
  ) {
    return true;
  }

  if (
    job.status ===
    'failed'
  ) {
    return false;
  }

  const start =
    Date.now();

  while (
    job.status ===
    'processing' &&
    Date.now() -
    start <
    timeoutMs
  ) {
    await sleep(
      500
    );
  }

  return (
    job.status ===
    'completed'
  );
}

/* ========================================================================== */
/* OPENSUBTITLES                                                              */
/* ========================================================================== */

function scoreSubtitle(subtitle) {
  let score =
    0;

  const lang =
    String(
      subtitle?.lang ||
      ''
    ).toLowerCase();

  if (
    lang ===
    'eng'
  ) {
    score +=
      100;
  } else if (
    lang ===
    'en'
  ) {
    score +=
      90;
  }

  if (
    subtitle
      ?.hearingImpaired ===
    false
  ) {
    score +=
      20;
  }

  if (
    String(
      subtitle?.format ||
      ''
    ).toLowerCase() ===
    'srt'
  ) {
    score +=
      20;
  }

  if (
    /english/i.test(
      String(
        subtitle?.name ||
        ''
      )
    )
  ) {
    score +=
      10;
  }

  return score;
}

function isUsableEnglishSubtitle(subtitle) {
  const lang =
    String(
      subtitle?.lang ||
      ''
    ).toLowerCase();

  return (
    (
      lang ===
      'eng' ||
      lang ===
      'en'
    ) &&
    typeof subtitle?.url ===
    'string' &&
    /^https?:\/\//i.test(
      subtitle.url
    )
  );
}

function selectBestSubtitle(
  subtitles,
  {
    releaseAware =
      false
  } = {}
) {
  if (
    !Array.isArray(
      subtitles
    )
  ) {
    return null;
  }

  const usable =
    subtitles.filter(
      isUsableEnglishSubtitle
    );

  if (
    !usable.length
  ) {
    return null;
  }

  if (
    releaseAware
  ) {
    return usable[0];
  }

  return (
    usable
      .sort(
        (
          a,
          b
        ) =>
          scoreSubtitle(b) -
          scoreSubtitle(a)
      )[0] ||
    null
  );
}

function rawSubtitleExtraSegment(req) {
  const pathname =
    String(
      req.originalUrl ||
      req.url ||
      ''
    )
      .split('?')[0];

  const match =
    pathname.match(
      /^\/subtitles\/[^/]+\/[^/]+\/(.+)\.json$/
    );

  return (
    match?.[1] ||
    String(
      req.params.extra ||
      ''
    ).trim()
  );
}

function parseStremioSubtitleExtra(req) {
  const rawExtra =
    rawSubtitleExtraSegment(
      req
    );

  if (!rawExtra) {
    return {
      rawExtra: '',
      videoHash: '',
      videoSize: '',
      filename: ''
    };
  }

  const params =
    new URLSearchParams(
      rawExtra
    );

  const rawVideoHash =
    String(
      params.get(
        'videoHash'
      ) ||
      ''
    ).trim();

  const rawVideoSize =
    String(
      params.get(
        'videoSize'
      ) ||
      ''
    ).trim();

  const filename =
    String(
      params.get(
        'filename'
      ) ||
      ''
    )
      .replace(
        /[\u0000-\u001F\u007F]/g,
        ''
      )
      .trim()
      .slice(
        0,
        1000
      );

  const videoHash =
    /^[a-f0-9]{16}$/i.test(
      rawVideoHash
    )
      ? rawVideoHash.toLowerCase()
      : '';

  const videoSizeNumber =
    Number(
      rawVideoSize
    );

  const videoSize =
    Number.isSafeInteger(
      videoSizeNumber
    ) &&
    videoSizeNumber >
    0
      ? String(
          videoSizeNumber
        )
      : '';

  return {
    rawExtra,
    videoHash,
    videoSize,
    filename
  };
}

function buildOpenSubtitlesSearchUrl(
  type,
  id,
  {
    videoHash = '',
    videoSize = '',
    filename = ''
  } = {}
) {
  const base =
    `https://opensubtitles-v3.strem.io/subtitles/` +
    `${encodeURIComponent(type)}/` +
    `${encodeURIComponent(id)}`;

  const extra =
    new URLSearchParams();

  if (
    videoHash
  ) {
    extra.set(
      'videoHash',
      videoHash
    );
  }

  if (
    videoSize
  ) {
    extra.set(
      'videoSize',
      videoSize
    );
  }

  if (
    filename
  ) {
    extra.set(
      'filename',
      filename
    );
  }

  const encoded =
    extra.toString();

  return encoded
    ? `${base}/${encoded}.json`
    : `${base}.json`;
}

function releaseIdentityDescription(extra) {
  return (
    [
      extra.videoHash &&
      'videoHash',

      extra.videoSize &&
      'videoSize',

      extra.filename &&
      'filename'
    ]
      .filter(Boolean)
      .join(' + ') ||
    'nenhuma'
  );
}

async function findEnglishSubtitle(
  type,
  id,
  extra = {}
) {
  const releaseAware =
    Boolean(
      extra.videoHash ||
      extra.videoSize ||
      extra.filename
    );

  const searchUrl =
    buildOpenSubtitlesSearchUrl(
      type,
      id,
      extra
    );

  console.log(
    `[OPENSUBTITLES MATCH] Modo: ` +
    `${releaseAware ? 'RELEASE-AWARE' : 'GENÉRICO'}; ` +
    `identidade: ${releaseIdentityDescription(extra)}.`
  );

  console.log(
    `[STREMIO] Procurando legenda: ${searchUrl}`
  );

  const response =
    await fetchWithTimeout(
      searchUrl,
      {
        headers: {
          Accept:
            'application/json',

          'User-Agent':
            'Stremio-Gemini-Subtitle-Translator/6.5'
        }
      },
      SOURCE_FETCH_TIMEOUT_MS
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

  const subtitles =
    Array.isArray(
      data?.subtitles
    )
      ? data.subtitles
      : [];

  const target =
    selectBestSubtitle(
      subtitles,
      {
        releaseAware
      }
    );

  if (target) {
    const position =
      subtitles.indexOf(
        target
      );

    console.log(
      `[OPENSUBTITLES MATCH] Selecionada posição upstream ` +
      `${position >= 0 ? position + 1 : '?'}/${subtitles.length}; ` +
      `id=${String(target.id || '(sem id)')}; ` +
      `nome=${String(target.name || '(sem nome)')}.`
    );
  } else {
    console.log(
      `[OPENSUBTITLES MATCH] Nenhuma legenda inglesa utilizável ` +
      `(${subtitles.length} resultado(s)).`
    );
  }

  return target;
}

async function downloadSubtitle(url) {
  console.log(
    `[SOURCE] Baixando legenda: ${url}`
  );

  const response =
    await fetchWithTimeout(
      url,
      {
        headers: {
          'User-Agent':
            'Stremio-Gemini-Subtitle-Translator/6.5'
        }
      },
      SOURCE_FETCH_TIMEOUT_MS
    );

  if (
    !response.ok
  ) {
    throw new Error(
      `Falha ao baixar legenda: HTTP ${response.status}.`
    );
  }

  const rawText =
    normalizeSrt(
      await response.text()
    );

  if (!rawText) {
    throw new Error(
      'Legenda vazia.'
    );
  }

  if (
    rawText.length >
    MAX_SOURCE_CHARS
  ) {
    throw new Error(
      `Legenda muito grande: ${rawText.length} caracteres.`
    );
  }

  const text =
    cleanSrtForTranslation(
      rawText
    );

  if (!text) {
    throw new Error(
      'A legenda ficou vazia após a limpeza SDH/CC.'
    );
  }

  return text;
}

/* ========================================================================== */
/* EMBEDDED                                                                   */
/* ========================================================================== */

async function createEmbeddedTranslationJob({
  type,
  videoId,
  sourceSrt,
  sourceName = 'embedded'
}) {
  const rawNormalizedSrt =
    normalizeSrt(
      sourceSrt
    );

  if (
    !rawNormalizedSrt
  ) {
    throw new Error(
      'A legenda embutida está vazia.'
    );
  }

  if (
    rawNormalizedSrt.length >
    MAX_SOURCE_CHARS
  ) {
    throw new Error(
      `Legenda embutida muito grande: ` +
      `${rawNormalizedSrt.length} caracteres.`
    );
  }

  const normalizedSrt =
    cleanSrtForTranslation(
      rawNormalizedSrt
    );

  if (
    !normalizedSrt
  ) {
    throw new Error(
      'A legenda embutida ficou vazia após a limpeza SDH/CC.'
    );
  }

  const blocks =
    parseSrt(
      normalizedSrt
    );

  if (
    !blocks.length
  ) {
    throw new Error(
      'A legenda embutida não possui blocos SRT válidos.'
    );
  }

  const sourceHash =
    sha256(
      normalizedSrt
    );

  const cacheKey =
    `${TRANSLATION_CACHE_VERSION}:embedded:${sourceHash}`;

  const cached =
    getTranslationCache(
      cacheKey
    );

  if (cached) {
    auditFinalTimestamps(
      normalizedSrt,
      cached,
      'EMBEDDED CACHE'
    );

    const cachedJobId =
      `embedded-cached-${sourceHash.slice(0, 24)}`;

    let cachedJob =
      getJob(
        cachedJobId
      );

    if (!cachedJob) {
      cachedJob =
        createJob({
          jobId:
            cachedJobId,

          cacheKey,
          type,
          videoId,
          sourceHash,

          sourceSrt:
            normalizedSrt
        });

      cachedJob.jobKind =
        'embedded';

      cachedJob.status =
        'completed';

      cachedJob.result =
        cached;

      cachedJob.progress =
        100;

      cachedJob.timestampAuditPassed =
        true;

      cachedJob.contentAuditPassed =
        true;
    }

    console.log(
      `[EMBEDDED] Cache utilizado para ${sourceName}.`
    );

    return cachedJob;
  }

  const existing =
    findProcessingJob(
      cacheKey
    );

  if (existing) {
    console.log(
      `[EMBEDDED] Job existente reutilizado: ${existing.id}`
    );

    return existing;
  }

  const jobId =
    `embedded-${sourceHash.slice(0, 24)}-${randomId(8)}`;

  const job =
    createJob({
      jobId,
      cacheKey,
      type,
      videoId,
      sourceHash,

      sourceSrt:
        normalizedSrt
    });

  job.source =
    sourceName;

  job.priority =
    100;

  job.jobKind =
    'embedded';

  job.totalBatches =
    splitIntoBatches(
      blocks
    ).length;

  startTranslationJob(
    job,
    'EMBEDDED JOB'
  );

  console.log(
    `[EMBEDDED] Novo job ${job.id} criado; ` +
    `${blocks.length} blocos / ${job.totalBatches} lote(s).`
  );

  return job;
}

/* ========================================================================== */
/* STREMIO SUBTITLES                                                          */
/* ========================================================================== */

async function subtitlesHandler(
  req,
  res
) {
  const type =
    String(
      req.params.type ||
      ''
    ).trim();

  const id =
    String(
      req.params.id ||
      ''
    ).trim();

  console.log(
    `[STREMIO] Pedido: ${type}/${id}`
  );

  const extra =
    parseStremioSubtitleExtra(
      req
    );

  console.log(
    `[STREMIO EXTRA] filename: ${extra.filename || '(não enviado)'}`
  );

  console.log(
    `[STREMIO EXTRA] videoSize: ${extra.videoSize || '(não enviado)'}`
  );

  console.log(
    `[STREMIO EXTRA] videoHash: ${extra.videoHash || '(não enviado)'}`
  );

  if (
    !type ||
    !id
  ) {
    return safeJson(
      res,
      {
        subtitles: []
      }
    );
  }

  try {
    const target =
      await findEnglishSubtitle(
        type,
        id,
        extra
      );

    if (!target) {
      return safeJson(
        res,
        {
          subtitles: []
        }
      );
    }

    const sourceSrt =
      await downloadSubtitle(
        target.url
      );

    const blocks =
      parseSrt(
        sourceSrt
      );

    if (
      !blocks.length
    ) {
      return safeJson(
        res,
        {
          subtitles: []
        }
      );
    }

    const sourceHash =
      sha256(
        sourceSrt
      );

    const cacheKey =
      `${TRANSLATION_CACHE_VERSION}:${type}:${id}:${sourceHash}`;

    const baseUrl =
      cleanBaseUrl(
        req
      );

    const cached =
      getTranslationCache(
        cacheKey
      );

    if (cached) {
      auditFinalTimestamps(
        sourceSrt,
        cached,
        'OPENSUBTITLES CACHE'
      );

      const jobId =
        `cached-${sourceHash.slice(0, 24)}`;

      let job =
        getJob(
          jobId
        );

      if (!job) {
        job =
          createJob({
            jobId,
            cacheKey,
            type,

            videoId:
              id,

            sourceHash,
            sourceSrt
          });

        job.jobKind =
          'opensubtitles';

        job.status =
          'completed';

        job.result =
          cached;

        job.progress =
          100;

        job.timestampAuditPassed =
          true;

        job.contentAuditPassed =
          true;
      }

      return safeJson(
        res,
        {
          subtitles: [
            {
              id:
                `${id}-gemini-${sourceHash.slice(0, 12)}`,

              url:
                `${baseUrl}/subtitle/${encodeURIComponent(jobId)}.srt`,

              lang:
                'por'
            }
          ]
        }
      );
    }

    let job =
      findProcessingJob(
        cacheKey
      );

    if (!job) {
      const jobId =
        `job-${sourceHash.slice(0, 24)}-${randomId(8)}`;

      job =
        createJob({
          jobId,
          cacheKey,
          type,

          videoId:
            id,

          sourceHash,
          sourceSrt
        });

      job.totalBatches =
        splitIntoBatches(
          blocks
        ).length;

      job.priority =
        10;

      job.jobKind =
        'opensubtitles';

      job.status =
        'pending';

      job.lazyStart =
        true;

      console.log(
        `[LAZY] Job OpenSubtitles ${job.id} criado sem consumir Gemini.`
      );
    }

    const subtitleUrl =
      `${baseUrl}/subtitle/${encodeURIComponent(job.id)}.srt`;

    console.log(
      `[STREMIO] Subtitle URL: ${subtitleUrl}`
    );

    return safeJson(
      res,
      {
        subtitles: [
          {
            id:
              `${id}-gemini-${sourceHash.slice(0, 12)}`,

            url:
              subtitleUrl,

            lang:
              'por'
          }
        ]
      }
    );
  } catch (error) {
    console.error(
      `[STREMIO] Erro: ${getErrorMessage(error)}`
    );

    return safeJson(
      res,
      {
        subtitles: []
      }
    );
  }
}

app.get(
  '/subtitles/:type/:id.json',
  subtitlesHandler
);

app.get(
  '/subtitles/:type/:id/:extra.json',
  subtitlesHandler
);

/* ========================================================================== */
/* API PONTE LOCAL                                                            */
/* ========================================================================== */

app.post(
  '/api/translate-embedded',
  async (
    req,
    res
  ) => {
    if (
      !isAuthorizedLocalBridge(
        req
      )
    ) {
      console.warn(
        '[EMBEDDED API] Tentativa não autorizada.'
      );

      return safeJson(
        res,
        {
          error:
            'Unauthorized'
        },
        401
      );
    }

    try {
      const {
        type,
        id,
        srt,
        name
      } =
        req.body ||
        {};

      if (
        !srt ||
        typeof srt !==
        'string'
      ) {
        return safeJson(
          res,
          {
            error:
              "Campo 'srt' é obrigatório."
          },
          400
        );
      }

      if (
        srt.length >
        MAX_SOURCE_CHARS
      ) {
        return safeJson(
          res,
          {
            error:
              `SRT muito grande. Limite: ${MAX_SOURCE_CHARS} caracteres.`
          },
          413
        );
      }

      const mediaType =
        String(
          type ||
          'unknown'
        ).trim();

      const videoId =
        String(
          id ||
          'unknown'
        ).trim();

      const sourceName =
        String(
          name ||
          'embedded'
        ).trim();

      console.log(
        `[EMBEDDED API] Recebido SRT de ${sourceName} ` +
        `para ${mediaType}/${videoId}; ` +
        `${srt.length} caracteres.`
      );

      const job =
        await createEmbeddedTranslationJob({
          type:
            mediaType,

          videoId,

          sourceSrt:
            srt,

          sourceName
        });

      const baseUrl =
        cleanBaseUrl(
          req
        );

      return safeJson(
        res,
        {
          ok:
            true,

          jobId:
            job.id,

          status:
            job.status,

          progress:
            job.progress,

          subtitleUrl:
            `${baseUrl}/subtitle/${encodeURIComponent(job.id)}.srt`
        }
      );
    } catch (error) {
      console.error(
        '[EMBEDDED API] Erro:',
        error
      );

      return safeJson(
        res,
        {
          error:
            getErrorMessage(
              error
            )
        },
        500
      );
    }
  }
);

/* ========================================================================== */
/* RESULTADO                                                                  */
/* ========================================================================== */

async function subtitleResultHandler(
  req,
  res
) {
  let jobId;

  try {
    jobId =
      decodeURIComponent(
        String(
          req.params.jobId ||
          ''
        ).trim()
      );
  } catch {
    jobId =
      String(
        req.params.jobId ||
        ''
      ).trim();
  }

  if (!jobId) {
    return sendSubtitleResponse(
      res,
      buildErrorSrt(
        'Job inválido.'
      )
    );
  }

  const job =
    getJob(
      jobId
    );

  if (!job) {
    return sendSubtitleResponse(
      res,
      buildErrorSrt(
        'Esta tradução expirou. Recarregue as legendas.'
      )
    );
  }

  if (
    job.status ===
    'pending' &&
    !job.lazyStartScheduled
  ) {
    job.lazyStartScheduled =
      true;

    console.log(
      `[LAZY] URL ${job.id} requisitada; ` +
      `grace ${LAZY_OPENSUB_START_GRACE_MS}ms.`
    );

    const timer =
      setTimeout(
        () => {
          job.lazyStartScheduled =
            false;

          if (
            job.status ===
            'pending'
          ) {
            startTranslationJob(
              job,
              'JOB'
            );
          }
        },
        LAZY_OPENSUB_START_GRACE_MS
      );

    if (
      typeof timer.unref ===
      'function'
    ) {
      timer.unref();
    }
  }

  async function serveCompleted() {
    if (
      !job.timestampAuditPassed
    ) {
      try {
        auditFinalTimestamps(
          job.sourceSrt,
          job.result,
          'SERVING'
        );

        job.timestampAuditPassed =
          true;
      } catch (error) {
        console.error(
          `[AUDIT TIMESTAMP] Bloqueando ${job.id}: ` +
          `${getErrorMessage(error)}`
        );

        return sendSubtitleResponse(
          res,
          buildErrorSrt(
            'A auditoria de timestamps bloqueou esta legenda.'
          ),
          'no-store'
        );
      }
    }

    if (
      !job.contentAuditPassed
    ) {
      console.error(
        `[AUDIT CONTENT] Bloqueando ${job.id}: ` +
        `conteúdo não aprovado.`
      );

      return sendSubtitleResponse(
        res,
        buildErrorSrt(
          'A auditoria semântica/contextual não confirmou esta legenda.'
        ),
        'no-store'
      );
    }

    return sendSubtitleResponse(
      res,
      job.result,
      'public, max-age=604800'
    );
  }

  if (
    job.status ===
    'completed' &&
    job.result
  ) {
    return serveCompleted();
  }

  if (
    job.status ===
    'failed'
  ) {
    return sendSubtitleResponse(
      res,
      buildErrorSrt(
        job.error
      ),
      'no-store'
    );
  }

  const completed =
    await waitForJob(
      job,
      15000
    );

  if (
    completed &&
    job.status ===
    'completed' &&
    job.result
  ) {
    return serveCompleted();
  }

  if (
    job.status ===
    'failed'
  ) {
    return sendSubtitleResponse(
      res,
      buildErrorSrt(
        job.error
      ),
      'no-store'
    );
  }

  return sendSubtitleResponse(
    res,
    buildProcessingSrt(
      job
    ),
    'no-store, no-cache, must-revalidate'
  );
}

app.get(
  '/subtitle/:jobId.srt',
  subtitleResultHandler
);

/* ========================================================================== */
/* MANIFEST / HEALTH                                                          */
/* ========================================================================== */

const manifest = {
  id:
    'org.tradutor.stateless.gemini.free',

  version:
    '6.5.0',

  name:
    'Tradutor Gemini PT-BR',

  description:
    'Traduz automaticamente legendas para Português do Brasil usando Gemini.',

  logo:
    '',

  resources: [
    'subtitles'
  ],

  types: [
    'movie',
    'series'
  ],

  idPrefixes: [
    'tt'
  ],

  catalogs:
    [],

  behaviorHints: {
    configurable:
      false,

    adult:
      false
  }
};

app.get(
  '/manifest.json',
  (
    req,
    res
  ) =>
    res.json(
      manifest
    )
);

function statusPayload() {
  pruneRequestStarts();

  return {
    name:
      manifest.name,

    version:
      manifest.version,

    status:
      'online',

    model:
      GEMINI_MODEL,

    renderEngineVersion:
      RENDER_ENGINE_VERSION,

    performanceMode:
      'resilient-fast-quality-first',

    thinkingLevel:
      'MINIMAL',

    translationCacheVersion:
      TRANSLATION_CACHE_VERSION,

    bridgeCompatibleCacheVersion:
      '5.8',

    batchMaxBlocks:
      MAX_BATCH_BLOCKS,

    batchMaxChars:
      MAX_BATCH_CHARS,

    geminiConcurrency:
      GEMINI_CONCURRENCY,

    safeRpm:
      SAFE_RPM,

    requestIntervalMs:
      MIN_REQUEST_INTERVAL_MS,

    requestsInRollingMinute:
      geminiRequestStarts.length,

    hardRequestCapPerJob:
      null,

    globalJobTimeoutMs:
      null,

    transientRetriesPerRequest:
      MAX_TRANSIENT_RETRIES,

    translationRequestTimeoutMs:
      TRANSLATION_REQUEST_TIMEOUT_MS,

    semanticAuditTimeoutMs:
      SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS,

    safeAuditRatio:
      SAFE_AUDIT_RATIO,

    safeAuditMinRecords:
      SAFE_AUDIT_MIN_RECORDS,

    partialCheckpointHours:
      PARTIAL_CHECKPOINT_TTL_MS /
      3600000,

    repairedWindowsAreReaudited:
      true,

    contextualLexicalAudit:
      true,

    realJobCancellation:
      true,

    timestampAudit:
      true,

    structuralIdLock:
      true,

    releaseAwareOpenSubtitles:
      true,

    lazyOpenSubtitles:
      true,

    translationJobQueue:
      translationJobQueue.length,

    geminiQueue:
      geminiQueue.length,

    activeGeminiWorkers,

    cooldownSeconds:
      Math.ceil(
        getCooldownRemaining() /
        1000
      ),

    partialCheckpoints:
      partialTranslationCache.size
  };
}

app.get(
  '/',
  (
    req,
    res
  ) =>
    res.json(
      statusPayload()
    )
);

app.get(
  '/health',
  (
    req,
    res
  ) =>
    res.json({
      ...statusPayload(),

      uptime:
        process.uptime(),

      jobs:
        jobs.size,

      cache:
        translationCache.size
    })
);

/* ========================================================================== */
/* START                                                                      */
/* ========================================================================== */

app.listen(
  PORT,
  () => {
    console.log(
      '=============================================='
    );

    console.log(
      ' STREMIO GEMINI SUBTITLE TRANSLATOR 6.5 RESILIENT-FAST / QUALITY-FIRST'
    );

    console.log(
      '=============================================='
    );

    console.log(
      `Porta: ${PORT}`
    );

    console.log(
      `Modelo Gemini: ${GEMINI_MODEL}`
    );

    console.log(
      'Thinking Gemini: MINIMAL ✅'
    );

    console.log(
      'Sampling temperature/top_p/top_k: REMOVIDOS ✅'
    );

    console.log(
      `PUBLIC_URL: ${PUBLIC_URL || '(automático)'}`
    );

    console.log(
      `Batch FAST: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} chars ✅`
    );

    console.log(
      `Concorrência Gemini: ${GEMINI_CONCURRENCY} ✅`
    );

    console.log(
      `Limitador global: ${SAFE_RPM} RPM / ` +
      `intervalo mínimo ${MIN_REQUEST_INTERVAL_MS}ms ✅`
    );

    console.log(
      'Limite máximo de requests por job: NÃO EXISTE ✅'
    );

    console.log(
      'Timeout global da legenda/filme: NÃO EXISTE ✅'
    );

    console.log(
      `Timeout por request: ` +
      `tradução=${TRANSLATION_REQUEST_TIMEOUT_MS}ms | ` +
      `audit=${SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS}ms ✅`
    );

    console.log(
      `Retry transitório localizado: até ${MAX_TRANSIENT_RETRIES} ` +
      `com backoff+jitter ✅`
    );

    console.log(
      `Checkpoint FAST parcial: ` +
      `${Math.round(PARTIAL_CHECKPOINT_TTL_MS / 3600000)}h ✅`
    );

    console.log(
      `SAFE proporcional: ` +
      `${Math.round(SAFE_AUDIT_RATIO * 1000) / 10}% dos blocos / ` +
      `mínimo ${SAFE_AUDIT_MIN_RECORDS} ✅`
    );

    console.log(
      `Canários Embedded: mínimo ${EMBEDDED_CANARY_MIN}, ` +
      `~1 a cada ${EMBEDDED_CANARY_EVERY} blocos ✅`
    );

    console.log(
      `Canários OpenSubtitles: mínimo ${OPENSUB_CANARY_MIN}, ` +
      `~1 a cada ${OPENSUB_CANARY_EVERY} blocos ✅`
    );

    console.log(
      'Auditoria contextual: sentido + naturalidade + atualidade lexical + fronteira EN↔PT ✅'
    );

    console.log(
      'Reparo: janela afetada + verificação independente por janela + segunda tradução localizada se necessário ✅'
    );

    console.log(
      'matchedSourceId: somente próprio ID ou vizinho imediato ✅'
    );

    console.log(
      'Erros transitórios/429/timeouts: aguardam e recuperam; não matam o job por tamanho ✅'
    );

    console.log(
      'ID + lock: 100% DOS BLOCOS ✅'
    );

    console.log(
      'Auditoria absoluta de timestamps: ATIVA ✅'
    );

    console.log(
      'PT-BR premium contemporâneo + contexto + Gen Z/Alpha contextual + LGBTQIAPN+/drag/camp/shade: ATIVO ✅'
    );

    console.log(
      'Ponte Local 2.5.1: COMPATÍVEL + prioridade alta ✅'
    );

    console.log(
      `Namespace de cache: ${TRANSLATION_CACHE_VERSION}`
    );

    console.log(
      'Status: ONLINE'
    );

    console.log(
      '=============================================='
    );
  }
);

process.on(
  'unhandledRejection',
  error =>
    console.error(
      '[PROCESS] Unhandled rejection:',
      error
    )
);

process.on(
  'uncaughtException',
  error =>
    console.error(
      '[PROCESS] Uncaught exception:',
      error
    )
);
