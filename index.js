const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

/* --------------------------------------------------------------------------
 * RENDER 6.4 FAST-LEAN / SAFE-SURGICAL
 * Objetivo: reduzir tempo de parede sem abandonar ID/lock, timestamps e uma
 * auditoria semântica independente cirúrgica.
 * -------------------------------------------------------------------------- */

const PORT = Number(process.env.PORT || 10000);
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();
const GEMINI_MODEL = String(process.env.GEMINI_MODEL || "gemini-3.5-flash-lite").trim();
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();

const SOURCE_FETCH_TIMEOUT_MS = 20000;
const MAX_TRANSLATION_TIME_MS = Number(process.env.MAX_TRANSLATION_TIME_MS || 480000);
const PERFORMANCE_TARGET_MS = 90000;

/* FAST: poucos lotes + até três chamadas em voo, com RPM rigidamente limitado. */
const configuredMaxBatchBlocks = Number(process.env.MAX_BATCH_BLOCKS || 200);
const configuredMaxBatchChars = Number(process.env.MAX_BATCH_CHARS || 13000);
const MAX_BATCH_BLOCKS = Number.isFinite(configuredMaxBatchBlocks) && configuredMaxBatchBlocks > 0
    ? Math.min(Math.floor(configuredMaxBatchBlocks), 200)
    : 200;
const MAX_BATCH_CHARS = Number.isFinite(configuredMaxBatchChars) && configuredMaxBatchChars > 0
    ? Math.min(Math.floor(configuredMaxBatchChars), 13000)
    : 13000;

const GEMINI_CONCURRENCY = 3;
const SAFE_RPM = 14;
const RPM_WINDOW_MS = 60000;

const MIN_REQUEST_INTERVAL_MS = Math.max(
    Number(process.env.MIN_REQUEST_INTERVAL_MS || 3000),
    4300
);

const MAX_JOB_GEMINI_REQUESTS = 14;

const TRANSLATION_REQUEST_TIMEOUT_MS = 30000;
const STRUCTURE_RETRY_TIMEOUT_MS = 28000;
const RESCUE_TRANSLATION_REQUEST_TIMEOUT_MS = 18000;
const REPAIR_TRANSLATION_REQUEST_TIMEOUT_MS = 15000;
const SINGLE_TRANSLATION_REQUEST_TIMEOUT_MS = 12000;
const SINGLE_LAST_TRANSLATION_REQUEST_TIMEOUT_MS = 18000;
const SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS = 9000;

const RESCUE_BATCH_BLOCKS = 90;
const RESCUE_BATCH_CHARS = 4800;
const MAX_RESCUE_SPLIT_DEPTH = 1;

const MAX_FINAL_AUDIT_RECORDS = 72;
const MAX_INDEPENDENT_AUDIT_BLOCKS = 24;
const AUDIT_CANARY_STRIDE = 30;
const MAX_REPAIR_WINDOW_BLOCKS = 4;

const MAX_OUTPUT_TOKENS = Number(process.env.MAX_OUTPUT_TOKENS || 16000);
const AUDIT_MAX_OUTPUT_TOKENS = 4096;

const MAX_NORMAL_RETRIES = 1;
const MAX_SINGLE_BLOCK_AUDIT_RETRIES = 1;
const MAX_RATE_LIMIT_COOLDOWN_MS = 120000;
const LAZY_OPENSUB_START_GRACE_MS = 1500;

const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;

const MAX_CACHE_ENTRIES = 200;
const MAX_JOBS = 300;
const MAX_SOURCE_CHARS = 800000;

/* Mantido para compatibilidade da Ponte Local 2.5.1. */
const TRANSLATION_CACHE_VERSION = "5.8";
const BLOCK_LOCK_VERSION = "5.8";
const RENDER_ENGINE_VERSION = "6.4-fast-lean-safe-surgical";

const translationCache = new Map();
const jobs = new Map();

const translationJobQueue = [];
let translationJobWorkerRunning = false;

const geminiQueue = [];
let activeGeminiWorkers = 0;

let rateSlotChain = Promise.resolve();

const geminiRequestStarts = [];

let geminiCooldownUntil = 0;
let lastGeminiRequestAt = 0;

/* --------------------------------------------------------------------------
 * HELPERS
 * -------------------------------------------------------------------------- */

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function translationTimeoutError() {
    const error = new Error(
        "Tempo máximo de tradução atingido."
    );

    error.code = "TRANSLATION_TIMEOUT";

    return error;
}

function geminiRequestTimeoutError(kind) {
    const error = new Error(
        `Timeout da chamada Gemini (${kind || "request"}).`
    );

    error.code = "GEMINI_REQUEST_TIMEOUT";
    error.requestKind = kind || "request";

    return error;
}

function requestBudgetError() {
    const error = new Error(
        `Orçamento FAST-SAFE de ${MAX_JOB_GEMINI_REQUESTS} chamadas Gemini atingido; job interrompido para evitar espera longa/quota.`
    );

    error.code = "JOB_REQUEST_BUDGET_EXCEEDED";

    return error;
}

function timestampIntegrityError(message) {
    const error = new Error(message);

    error.code = "TIMESTAMP_INTEGRITY_ERROR";

    return error;
}

function badModelOutputError(message) {
    const error = new Error(message);

    error.code = "BAD_MODEL_OUTPUT";

    return error;
}

function badAuditOutputError(message) {
    const error = new Error(message);

    error.code = "BAD_AUDIT_OUTPUT";

    return error;
}

function assertBeforeDeadline(deadlineAt) {
    if (
        Number.isFinite(deadlineAt) &&
        Date.now() >= deadlineAt
    ) {
        throw translationTimeoutError();
    }
}

function remainingBeforeDeadline(deadlineAt) {
    if (!Number.isFinite(deadlineAt)) {
        return Infinity;
    }

    return Math.max(
        0,
        deadlineAt - Date.now()
    );
}

async function sleepWithDeadline(
    ms,
    deadlineAt
) {
    const safeMs = Math.max(
        0,
        Number(ms) || 0
    );

    const remaining = remainingBeforeDeadline(
        deadlineAt
    );

    if (
        Number.isFinite(remaining) &&
        remaining <= safeMs
    ) {
        if (remaining > 0) {
            await sleep(remaining);
        }

        throw translationTimeoutError();
    }

    if (safeMs > 0) {
        await sleep(safeMs);
    }

    assertBeforeDeadline(deadlineAt);
}

function sha256(text) {
    return crypto
        .createHash("sha256")
        .update(
            String(text),
            "utf8"
        )
        .digest("hex");
}

function randomId(length = 8) {
    return crypto
        .randomBytes(length)
        .toString("hex");
}

function getErrorMessage(error) {
    if (!error) {
        return "Erro desconhecido.";
    }

    if (typeof error === "string") {
        return error;
    }

    return (
        error.message ||
        error.statusText ||
        "Erro desconhecido."
    );
}

function cleanBaseUrl(req) {
    if (PUBLIC_URL) {
        return PUBLIC_URL;
    }

    const protocol =
        req.headers["x-forwarded-proto"] ||
        req.protocol ||
        "https";

    const host =
        req.headers["x-forwarded-host"] ||
        req.get("host");

    return `${protocol}://${host}`;
}

function safeJson(
    res,
    data,
    status = 200
) {
    res.status(status);

    res.set(
        "Cache-Control",
        "no-store, no-cache, must-revalidate"
    );

    return res.json(data);
}

function isAuthorizedLocalBridge(req) {
    if (!LOCAL_BRIDGE_SECRET) {
        return false;
    }

    const auth = String(
        req.headers.authorization || ""
    ).trim();

    if (!auth) {
        return false;
    }

    const expected =
        `Bearer ${LOCAL_BRIDGE_SECRET}`;

    const authBuffer =
        Buffer.from(auth);

    const expectedBuffer =
        Buffer.from(expected);

    return (
        authBuffer.length ===
            expectedBuffer.length &&
        crypto.timingSafeEqual(
            authBuffer,
            expectedBuffer
        )
    );
}

async function fetchWithTimeout(
    url,
    options = {},
    timeoutMs = 20000
) {
    const controller =
        new AbortController();

    const timer =
        setTimeout(
            () => controller.abort(),
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

/* --------------------------------------------------------------------------
 * LIMPEZA DE MEMÓRIA
 * -------------------------------------------------------------------------- */

function cleanupMemory() {
    const now = Date.now();

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
            key,
            job
        ] of jobs.entries()
    ) {
        if (
            job.expiresAt <= now &&
            job.status !== "processing"
        ) {
            jobs.delete(key);
        }
    }

    while (
        translationCache.size >
        MAX_CACHE_ENTRIES
    ) {
        const key =
            translationCache
                .keys()
                .next()
                .value;

        if (key === undefined) {
            break;
        }

        translationCache.delete(key);
    }

    while (
        jobs.size >
        MAX_JOBS
    ) {
        const key =
            jobs
                .keys()
                .next()
                .value;

        if (key === undefined) {
            break;
        }

        const job =
            jobs.get(key);

        if (
            job &&
            job.status === "processing"
        ) {
            break;
        }

        jobs.delete(key);
    }
}

setInterval(
    cleanupMemory,
    5 * 60 * 1000
).unref();

/* --------------------------------------------------------------------------
 * SRT / LIMPEZA
 * -------------------------------------------------------------------------- */

function normalizeSrt(text) {
    return String(text || "")
        .replace(/^\uFEFF/, "")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n")
        .trim();
}

function stripCodeFences(text) {
    return String(text || "")
        .replace(
            /^```(?:json|srt|text|plaintext)?\s*/i,
            ""
        )
        .replace(
            /\s*```$/i,
            ""
        )
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
    for (
        const key
        of Object.keys(target)
    ) {
        target[key] +=
            Number(
                source?.[key] || 0
            );
    }

    return target;
}

function sanitizeSubtitleText(value) {
    const stats =
        createCharacterSanitizationStats();

    let text =
        String(value ?? "");

    const normalizedNfc =
        text.normalize("NFC");

    if (
        normalizedNfc !== text
    ) {
        stats.nfcChanges++;
        text = normalizedNfc;
    }

    text = text.replace(
        /[\u00A0\u202F]/gu,
        () => {
            stats.normalizedSpaces++;
            return " ";
        }
    );

    text = text.replace(
        /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/gu,
        () => {
            stats.controlChars++;
            return "";
        }
    );

    text = text.replace(
        /[\u00AD\u200B\u2060\uFEFF\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu,
        () => {
            stats.invisibleChars++;
            return "";
        }
    );

    text = text.replace(
        /\uFFFD/gu,
        () => {
            stats.replacementChars++;
            return "";
        }
    );

    text = text.replace(
        /\{\\[^}\r\n]*\}/gu,
        () => {
            stats.assTags++;
            return "";
        }
    );

    text = text
        .split("\n")
        .map(
            line =>
                line
                    .replace(
                        /[ \t]{2,}/g,
                        " "
                    )
                    .trimEnd()
        )
        .join("\n")
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
        `[CLEAN CHAR] ${stage}: ${characterSanitizationTotal(stats)} ajuste(s); controles=${stats.controlChars}, invisíveis=${stats.invisibleChars}, espaços=${stats.normalizedSpaces}, replacement=${stats.replacementChars}, ASS=${stats.assTags}, NFC=${stats.nfcChanges}.`
    );
}

const SDH_CUE_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

const SPEAKER_HINT_MARKER_REGEX =
    /^@@SPK:([^@]+)@@\s*/u;

function normalizeSpeakerHint(value) {
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
        SDH_CUE_WORDS.test(speaker) ||
        /[!?;]/u.test(speaker)
    ) {
        return "";
    }

    return speaker;
}

function encodeSpeakerHint(speaker) {
    return encodeURIComponent(
        String(speaker || "")
    );
}

function decodeSpeakerHint(encoded) {
    try {
        return normalizeSpeakerHint(
            decodeURIComponent(
                String(encoded || "")
            )
        );

    } catch {
        return "";
    }
}

function extractSpeakerHint(line) {
    const original =
        String(line || "");

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
                    ""
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
        speaker: "",
        lineForCleaning:
            original
    };
}

function normalizeHyphenatedVocalElongations(text) {
    return String(text ?? "")
        .replace(
            /([A-Za-zÀ-ÖØ-öø-ÿ])(?:[-–—]\1){2,}[-–—]?/giu,
            "$1"
        );
}

function normalizeTranslatedVocalElongations(text) {
    let result =
        normalizeHyphenatedVocalElongations(
            text
        );

    result = result.replace(
        /([AEIOUÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜaeiouáàâãäéèêëíìîïóòôõöúùûü])\1{3,}/gu,
        "$1"
    );

    return result;
}

function cleanDialogueLine(line) {
    let text =
        String(line || "")
            .trim();

    if (!text) {
        return "";
    }

    text = text.replace(
        /\s*\[[^\]]+\]\s*/gu,
        " "
    );

    text = text.replace(
        /\s*\(([^)]*)\)\s*/gu,
        (
            match,
            inside
        ) =>
            SDH_CUE_WORDS.test(
                String(inside || "")
            )
                ? " "
                : match
    );

    text = text.replace(
        /^\s*[-–—]?\s*[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ .'-]{0,30}:\s+(?=\S)/u,
        ""
    );

    text = text
        .replace(
            /[ \t]{2,}/g,
            " "
        )
        .trim();

    if (
        /^[-–—♪♫♬\s]*$/u.test(text)
    ) {
        return "";
    }

    return text;
}

const TIMING_LINE_REGEX =
    /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

function extractTimingLines(srt) {
    return String(srt || "")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n")
        .split("\n")
        .map(
            line => line.trim()
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
        JSON.stringify(timings)
    ).slice(
        0,
        16
    );
}

function auditCleaningTimestamps(
    rawSrt,
    cleanedSrt,
    label = "SOURCE"
) {
    const raw =
        extractTimingLines(rawSrt);

    const clean =
        extractTimingLines(cleanedSrt);

    let cursor = 0;

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
            cursor >= raw.length
        ) {
            throw timestampIntegrityError(
                `Auditoria de timestamps falhou na limpeza (${label}) no bloco preservado ${i + 1}.`
            );
        }

        cursor++;
    }

    console.log(
        `[AUDIT TIMESTAMP] ${label} RAW->CLEAN: OK — ${clean.length}/${raw.length} timing(s) preservado(s) exatamente; removidos=${raw.length - clean.length}; assinatura=${timingSignature(clean)}.`
    );

    return true;
}

function parseSrt(srt) {
    const normalized =
        normalizeSrt(srt);

    if (!normalized) {
        return [];
    }

    const blocks =
        normalized
            .split(/\n{2,}/)
            .map(
                block =>
                    block.trim()
            )
            .filter(Boolean);

    const result = [];

    for (const block of blocks) {
        const lines =
            block.split("\n");

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
            !/^\d+$/.test(indexLine) ||
            !TIMING_LINE_REGEX.test(
                timingLine
            )
        ) {
            continue;
        }

        const textLines =
            lines.slice(2);

        let speakerHint = "";

        if (textLines.length) {
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
                        ""
                    );
            }
        }

        result.push({
            index:
                Number(indexLine),

            timing:
                timingLine,

            text:
                textLines.join("\n"),

            speakerHint:
                speakerHint || null
        });
    }

    return result;
}

function auditFinalTimestamps(
    sourceSrt,
    finalSrt,
    label = "FINAL"
) {
    const source =
        parseSrt(sourceSrt);

    const final =
        parseSrt(finalSrt);

    if (
        source.length !==
        final.length
    ) {
        throw timestampIntegrityError(
            `Auditoria de timestamps falhou (${label}): fonte=${source.length}, final=${final.length}.`
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
            throw timestampIntegrityError(
                `Auditoria de timestamps falhou (${label}) no bloco ${i + 1}.`
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
        `[AUDIT TIMESTAMP] ${label}: OK — ${source.length}/${source.length} bloco(s), 0 alteração(ões), assinatura=${signature}.`
    );

    return true;
}

function cleanSrtForTranslation(srt) {
    const normalized =
        normalizeSrt(srt);

    if (!normalized) {
        return "";
    }

    const rawBlocks =
        normalized.split(/\n{2,}/);

    const cleanedBlocks = [];

    let removedBlocks = 0;
    let changedLines = 0;
    let speakerHintBlocks = 0;
    let elongatedLines = 0;

    const characterStats =
        createCharacterSanitizationStats();

    for (const rawBlock of rawBlocks) {
        const lines =
            rawBlock
                .trim()
                .split("\n");

        const timingIndex =
            lines.findIndex(
                line =>
                    /-->/.test(line)
            );

        if (
            timingIndex === -1
        ) {
            continue;
        }

        const timing =
            lines[timingIndex]
                .trim();

        const cleanedDialogue = [];

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
                    speakerInfo
                        .lineForCleaning
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
                `@@SPK:${encodeSpeakerHint(
                    speaker
                )}@@ ${cleanedDialogue[0]}`;

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
                    ].join("\n")
            )
            .join("\n\n")
            .trim();

    console.log(
        `[CLEAN] SDH/CC: ${rawBlocks.length} -> ${cleanedBlocks.length} blocos; ${removedBlocks} removidos; ${changedLines} linha(s) alterada(s).`
    );

    console.log(
        `[CLEAN] Contexto de falante: ${speakerHintBlocks} bloco(s) preservado(s).`
    );

    console.log(
        `[CLEAN] Alongamentos vocais na fonte: ${elongatedLines} linha(s) normalizada(s).`
    );

    logCharacterSanitization(
        "FONTE",
        characterStats
    );

    const finalResult =
        result
            ? result + "\n"
            : "";

    if (finalResult) {
        auditCleaningTimestamps(
            normalized,
            finalResult,
            "FONTE"
        );
    }

    return finalResult;
}

function cleanAllTranslatedCharacters(
    translatedTexts
) {
    const stats =
        createCharacterSanitizationStats();

    let changed = 0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(
                        text ?? ""
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
                    changed++;
                }

                return sanitized.text;
            }
        );

    logCharacterSanitization(
        `TRADUÇÃO (${changed} bloco(s) alterado(s))`,
        stats
    );

    return cleaned;
}

function cleanTranslatedDialogueMarkers(text) {
    const normalized =
        String(text ?? "")
            .replace(/\r\n/g, "\n")
            .replace(/\r/g, "\n");

    const lines =
        normalized.split("\n");

    const markerRegex =
        /^\s*[-–—/]+\s+(?=\S)/u;

    let marked = 0;
    let nonEmpty = 0;

    for (const line of lines) {
        if (line.trim()) {
            nonEmpty++;
        }

        if (
            markerRegex.test(line)
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
                    ""
                )
        )
        .join("\n");
}

function cleanAllTranslatedDialogueMarkers(
    translatedTexts
) {
    let changed = 0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(
                        text ?? ""
                    );

                const result =
                    cleanTranslatedDialogueMarkers(
                        original
                    );

                if (
                    result !==
                    original
                ) {
                    changed++;
                }

                return result;
            }
        );

    console.log(
        `[CLEAN] Marcadores de diálogo: ${changed} bloco(s) ajustado(s).`
    );

    return cleaned;
}

function cleanAllTranslatedVocalElongations(
    translatedTexts
) {
    let changed = 0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(
                        text ?? ""
                    );

                const result =
                    normalizeTranslatedVocalElongations(
                        original
                    );

                if (
                    result !==
                    original
                ) {
                    changed++;
                }

                return result;
            }
        );

    console.log(
        `[CLEAN] Alongamentos vocais traduzidos: ${changed} bloco(s) ajustado(s).`
    );

    return cleaned;
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
                    ].join("\n")
            )
            .join("\n\n")
            .trim() +
        "\n"
    );
}

/* --------------------------------------------------------------------------
 * CACHE
 * -------------------------------------------------------------------------- */

function setTranslationCache(
    key,
    srt
) {
    const now =
        Date.now();

    translationCache.set(
        key,
        {
            srt,

            version:
                TRANSLATION_CACHE_VERSION,

            contentAuditPassed:
                true,

            createdAt:
                now,

            expiresAt:
                now +
                CACHE_TTL_MS
        }
    );

    cleanupMemory();
}

function getTranslationCache(key) {
    const item =
        translationCache.get(
            key
        );

    if (!item) {
        return null;
    }

    if (
        item.version !==
            TRANSLATION_CACHE_VERSION ||
        item.contentAuditPassed !== true ||
        item.expiresAt <=
            Date.now()
    ) {
        translationCache.delete(
            key
        );

        return null;
    }

    return item.srt;
}

/* --------------------------------------------------------------------------
 * LOTES
 * -------------------------------------------------------------------------- */

function splitIntoBatches(blocks) {
    const batches = [];

    let current = [];
    let chars = 0;

    for (const block of blocks) {
        const size =
            String(
                block.text || ""
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

            current = [];
            chars = 0;
        }

        current.push(block);

        chars += size;
    }

    if (current.length) {
        batches.push(
            current
        );
    }

    return batches;
}

/* --------------------------------------------------------------------------
 * RATE LIMIT GLOBAL
 * -------------------------------------------------------------------------- */

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
        `[GEMINI] RATE LIMIT. Cooldown global de ${Math.ceil(
            safeMs / 1000
        )}s.`
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

async function acquireGeminiRateSlot(
    deadlineAt
) {
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
                            assertBeforeDeadline(
                                deadlineAt
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
                                    `[GEMINI] Fila aguardando cooldown de ${Math.ceil(
                                        cooldown /
                                        1000
                                    )}s.`
                                );

                                await sleepWithDeadline(
                                    cooldown,
                                    deadlineAt
                                );

                                continue;
                            }

                            let waitMs = 0;

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
                                        50
                                    );
                            }

                            if (
                                waitMs > 0
                            ) {
                                await sleepWithDeadline(
                                    waitMs,
                                    deadlineAt
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
        response?.headers?.get(
            "retry-after"
        );

    if (header) {
        const seconds =
            Number(header);

        if (
            Number.isFinite(
                seconds
            ) &&
            seconds > 0
        ) {
            return Math.min(
                seconds * 1000,
                MAX_RATE_LIMIT_COOLDOWN_MS
            );
        }
    }

    const message =
        String(
            errorData?.error?.message ||
            ""
        );

    let match =
        message.match(
            /retry in\s+([\d.]+)s/i
        );

    if (match) {
        return Math.min(
            (
                Number(match[1]) +
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
                    match[2] || 0
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
                message || ""
            )
        )
    );
}

function consumeJobBudget(
    budget
) {
    if (!budget) {
        return;
    }

    if (
        budget.used >=
        budget.max
    ) {
        throw requestBudgetError();
    }

    budget.used++;
}

/* --------------------------------------------------------------------------
 * GEMINI
 * -------------------------------------------------------------------------- */

function translationSystemPrompt() {
    return (
        "Você é um tradutor, localizador e adaptador profissional de legendas para Português do Brasil, padrão premium de streaming. " +
        "Prioridade: INTEGRIDADE POR ID → sentido/intenção → voz do personagem → naturalidade contemporânea → humor/ritmo → literalidade. " +
        "Cada ID é atômico: traduza SOMENTE o texto daquele ID. Nunca antecipe, atrase, complete ou transfira conteúdo entre IDs, mesmo quando a frase continua no próximo bloco. Vizinhos são só contexto. " +
        "Escreva PT-BR vivo, oral e atual. Use repertório Gen Z/Alpha, cultura digital e memes apenas quando cena/personagem pedirem, sem forçar gíria. " +
        "Em contexto LGBTQIAPN+, queer, drag, ballroom, camp, shade e cultura pop, preserve identidade, pronome, gênero, ironia, afeto, provocação, termos ressignificados e intensidade; não heteronormativize nem higienize. " +
        "Vocativos girl, bitch, honey, sis, queen, baby e babe são contextuais; não traduza automaticamente girl=garota ou bitch=vadia. " +
        "Adapte expressões, piadas, trocadilhos, shade e referências quando houver solução brasileira natural. Preserve Condragulations quando aplicável. Não censure palavrões. " +
        "Preserve nomes, marcas, títulos e termos técnicos. speaker é contexto oculto e nunca deve aparecer em t. Se gênero não estiver claro, não chute. " +
        "Não acrescente SDH/CC, sons, nomes de falantes ou explicações. Preserve formatação útil. " +
        "A entrada usa i=id, l=lock, x=texto e s=speaker opcional. Retorne somente JSON exatamente na ordem recebida, usando i, l e t; preserve i/l sem alteração."
    );
}

function auditSystemPrompt() {
    return (
        "Você é um auditor bilíngue rigoroso de integridade EN→PT-BR. NÃO traduza nem reescreva. " +
        "Cada registro usa i=id, l=lock, s=source, t=translation, p=vizinho anterior e n=vizinho seguinte. " +
        "Os registros do array podem ser esparsos; NUNCA use outro item do array como vizinho. Use somente p/n do próprio registro. " +
        "Se t corresponde exclusivamente a s, retorne m=i e f=true. Se conteúdo migrou do vizinho anterior ou seguinte, retorne m igual ao ID desse vizinho e f=false. " +
        "Se há erro semântico relevante mas t continua pertencendo ao próprio i, retorne m=i e f=false. " +
        "m só pode ser i, p.id ou n.id. Aceite paráfrase natural, localização, humor, gíria e linguagem LGBTQIAPN+/drag/camp/shade quando o significado do próprio ID estiver preservado. " +
        "Preserve i/l exatamente e retorne somente JSON."
    );
}

async function rawGeminiRequest(
    prompt,
    deadlineAt,
    requestKind = "translation"
) {
    if (!GEMINI_API_KEY) {
        throw new Error(
            "GEMINI_API_KEY não configurada."
        );
    }

    assertBeforeDeadline(
        deadlineAt
    );

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            GEMINI_MODEL
        )}:generateContent`;

    const isAudit =
        requestKind ===
        "semantic-audit";

    const generationConfig =
        isAudit
            ? {
                  thinkingConfig: {
                      thinkingLevel:
                          "MINIMAL"
                  },

                  responseMimeType:
                      "application/json",

                  responseSchema: {
                      type:
                          "ARRAY",

                      items: {
                          type:
                              "OBJECT",

                          properties: {
                              i: {
                                  type:
                                      "INTEGER"
                              },

                              l: {
                                  type:
                                      "STRING"
                              },

                              m: {
                                  type:
                                      "INTEGER"
                              },

                              f: {
                                  type:
                                      "BOOLEAN"
                              }
                          },

                          required: [
                              "i",
                              "l",
                              "m",
                              "f"
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
                          "MINIMAL"
                  },

                  responseMimeType:
                      "application/json",

                  responseSchema: {
                      type:
                          "ARRAY",

                      items: {
                          type:
                              "OBJECT",

                          properties: {
                              i: {
                                  type:
                                      "INTEGER"
                              },

                              l: {
                                  type:
                                      "STRING"
                              },

                              t: {
                                  type:
                                      "STRING"
                              }
                          },

                          required: [
                              "i",
                              "l",
                              "t"
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
                    "user",

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

    let timeoutMs =
        TRANSLATION_REQUEST_TIMEOUT_MS;

    if (
        requestKind ===
        "semantic-audit"
    ) {
        timeoutMs =
            SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-structure-retry"
    ) {
        timeoutMs =
            STRUCTURE_RETRY_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-rescue"
    ) {
        timeoutMs =
            RESCUE_TRANSLATION_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-repair"
    ) {
        timeoutMs =
            REPAIR_TRANSLATION_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-single"
    ) {
        timeoutMs =
            SINGLE_TRANSLATION_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-single-last"
    ) {
        timeoutMs =
            SINGLE_LAST_TRANSLATION_REQUEST_TIMEOUT_MS;
    }

    timeoutMs =
        Math.max(
            1,

            Math.min(
                timeoutMs,

                remainingBeforeDeadline(
                    deadlineAt
                )
            )
        );

    const controller =
        new AbortController();

    let requestTimedOut =
        false;

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

        rawText =
            await response.text();

    } catch (error) {
        if (
            Number.isFinite(
                deadlineAt
            ) &&
            Date.now() >=
                deadlineAt
        ) {
            throw translationTimeoutError();
        }

        if (requestTimedOut) {
            throw geminiRequestTimeoutError(
                requestKind
            );
        }

        throw error;

    } finally {
        clearTimeout(timer);
    }

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

        throw error;
    }

    if (!response.ok) {
        const message =
            data?.error?.message ||
            `HTTP ${response.status}`;

        const error =
            new Error(message);

        error.status =
            response.status;

        error.rateLimit =
            isRateLimitError(
                response.status,
                message
            );

        error.retryAfterMs =
            error.rateLimit
                ? getRetryAfterMs(
                    response,
                    data
                )
                : 0;

        throw error;
    }

    const text =
        data?.candidates?.[0]
            ?.content?.parts
            ?.map(
                part =>
                    part?.text || ""
            )
            .join("")
            .trim();

    if (!text) {
        throw new Error(
            "Gemini não retornou conteúdo."
        );
    }

    return text;
}

function enqueueGemini(
    prompt,
    deadlineAt,
    requestKind,
    budget
) {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            if (
                Number.isFinite(
                    deadlineAt
                ) &&
                Date.now() >=
                    deadlineAt
            ) {
                reject(
                    translationTimeoutError()
                );

                return;
            }

            geminiQueue.push({
                prompt,
                deadlineAt,
                requestKind,
                budget,
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

        runGeminiItem(item)
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
    let normalAttempt = 1;

    try {
        for (;;) {
            try {
                assertBeforeDeadline(
                    item.deadlineAt
                );

                await acquireGeminiRateSlot(
                    item.deadlineAt
                );

                consumeJobBudget(
                    item.budget
                );

                console.log(
                    `[GEMINI] Request ${item.budget ? item.budget.used : "?"}/${item.budget ? item.budget.max : "?"} (${item.requestKind || "translation"}) | ativos=${activeGeminiWorkers}/${GEMINI_CONCURRENCY}`
                );

                const result =
                    await rawGeminiRequest(
                        item.prompt,
                        item.deadlineAt,
                        item.requestKind ||
                            "translation"
                    );

                item.resolve(
                    result
                );

                return;

            } catch (error) {
                if (
                    error?.code ===
                        "TRANSLATION_TIMEOUT" ||
                    error?.code ===
                        "JOB_REQUEST_BUDGET_EXCEEDED"
                ) {
                    item.reject(
                        error
                    );

                    return;
                }

                if (
                    error?.code ===
                    "GEMINI_REQUEST_TIMEOUT"
                ) {
                    console.warn(
                        `[GEMINI] Timeout curto (${item.requestKind || "translation"}).`
                    );

                    item.reject(
                        error
                    );

                    return;
                }

                const message =
                    getErrorMessage(
                        error
                    );

                console.error(
                    `[GEMINI] Erro: ${message}`
                );

                if (
                    error?.rateLimit
                ) {
                    setGeminiCooldown(
                        Math.min(
                            error.retryAfterMs ||
                            30000,

                            MAX_RATE_LIMIT_COOLDOWN_MS
                        )
                    );

                    continue;
                }

                if (
                    normalAttempt <=
                    MAX_NORMAL_RETRIES
                ) {
                    const wait =
                        1000 *
                        normalAttempt;

                    normalAttempt++;

                    console.log(
                        `[GEMINI] Retry técnico em ${Math.ceil(
                            wait /
                            1000
                        )}s.`
                    );

                    await sleepWithDeadline(
                        wait,
                        item.deadlineAt
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

/* --------------------------------------------------------------------------
 * TRADUÇÃO COMPACTA
 * -------------------------------------------------------------------------- */

function blockTranslationLock(block) {
    return sha256(
        JSON.stringify([
            BLOCK_LOCK_VERSION,
            Number(
                block?.index
            ),
            String(
                block?.timing ??
                ""
            ),
            String(
                block?.text ??
                ""
            ),
            String(
                block?.speakerHint ??
                ""
            )
        ])
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
                const tuple = [
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

    let instruction =
        "TRADUZA os itens para PT-BR premium. Cada tupla é [i,l,x,s?].";

    if (
        options.strictStructure
    ) {
        instruction =
            "REPETIÇÃO ESTRUTURAL: traduza todos os itens exatamente uma vez e preserve i/l sem qualquer alteração. Tupla=[i,l,x,s?].";
    }

    if (
        options.repairMode
    ) {
        instruction =
            "REPARO CIRÚRGICO: traduza apenas os itens de b; não absorva conteúdo de p/n. Tupla=[i,l,x,s?].";
    }

    return `${instruction}
p/n são SOMENTE contexto. SAÍDA EXATA: [{"i":123,"l":"abc","t":"..."}]
${JSON.stringify({
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
    })}`;
}

async function translateBatchOnce(
    blocks,
    deadlineAt,
    context,
    budget,
    options = {}
) {
    const raw =
        await enqueueGemini(
            buildTranslationPrompt(
                blocks,
                context,
                options
            ),

            deadlineAt,

            options.requestKind ||
            "translation",

            budget
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
            "Gemini retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(parsed) ||
        parsed.length !==
            blocks.length
    ) {
        throw badModelOutputError(
            `Quantidade incorreta de blocos: esperado=${blocks.length}, recebido=${Array.isArray(parsed) ? parsed.length : 0}.`
        );
    }

    const texts = [];
    const seen =
        new Set();

    for (
        let i = 0;
        i < blocks.length;
        i++
    ) {
        const expected =
            blocks[i];

        const expectedLock =
            blockTranslationLock(
                expected
            );

        const item =
            parsed[i];

        if (
            !item ||
            item.i !==
                expected.index ||
            seen.has(
                item.i
            ) ||
            item.l !==
                expectedLock ||
            typeof item.t !==
                "string" ||
            !item.t.trim()
        ) {
            throw badModelOutputError(
                `Contrato ID/lock inválido no ID ${expected.index}.`
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
        `[AUDIT ID] OK — ${blocks.length}/${blocks.length} bloco(s); ordem, IDs e locks preservados exatamente.`
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
                : outerContext?.before ||
                  null,

        after:
            endExclusive <
            blocks.length
                ? blocks[
                      endExclusive
                  ]
                : outerContext?.after ||
                  null
    };
}

function splitFixedRescueBatches(blocks) {
    const result = [];

    let current = [];
    let chars = 0;

    for (const block of blocks) {
        const size =
            String(
                block.text || ""
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
        result.push(
            current
        );
    }

    return result;
}

async function translateSingleLastChance(
    block,
    deadlineAt,
    context,
    budget,
    repairMode = false
) {
    try {
        return await translateBatchOnce(
            [
                block
            ],

            deadlineAt,

            context,

            budget,

            {
                requestKind:
                    "translation-single",

                strictStructure:
                    true,

                repairMode
            }
        );

    } catch (error) {
        if (
            ![
                "BAD_MODEL_OUTPUT",
                "GEMINI_REQUEST_TIMEOUT"
            ].includes(
                error?.code
            )
        ) {
            throw error;
        }

        return translateBatchOnce(
            [
                block
            ],

            deadlineAt,

            context,

            budget,

            {
                requestKind:
                    "translation-single-last",

                strictStructure:
                    true,

                repairMode
            }
        );
    }
}

async function translateRescueChunk(
    blocks,
    deadlineAt,
    context,
    budget,
    depth = 0
) {
    try {
        return await translateBatchOnce(
            blocks,

            deadlineAt,

            context,

            budget,

            {
                requestKind:
                    "translation-rescue",

                strictStructure:
                    true
            }
        );

    } catch (error) {
        if (
            ![
                "BAD_MODEL_OUTPUT",
                "GEMINI_REQUEST_TIMEOUT"
            ].includes(
                error?.code
            )
        ) {
            throw error;
        }

        if (
            blocks.length === 1
        ) {
            return translateSingleLastChance(
                blocks[0],
                deadlineAt,
                context,
                budget,
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
            `[RESCUE 6.4] ${blocks.length} -> ${left.length}+${right.length} em paralelo.`
        );

        const [
            translatedLeft,
            translatedRight
        ] =
            await Promise.all([
                translateRescueChunk(
                    left,

                    deadlineAt,

                    contextForLocalSlice(
                        blocks,
                        0,
                        middle,
                        context
                    ),

                    budget,

                    depth + 1
                ),

                translateRescueChunk(
                    right,

                    deadlineAt,

                    contextForLocalSlice(
                        blocks,
                        middle,
                        blocks.length,
                        context
                    ),

                    budget,

                    depth + 1
                )
            ]);

        return [
            ...translatedLeft,
            ...translatedRight
        ];
    }
}

async function translateBatchRescue(
    blocks,
    deadlineAt,
    context,
    budget
) {
    const chunks =
        splitFixedRescueBatches(
            blocks
        );

    console.warn(
        `[RESCUE 6.4] ${blocks.length} bloco(s) -> ${chunks.length} micro-lote(s) grandes.`
    );

    const starts = [];

    let cursor = 0;

    for (
        const chunk
        of chunks
    ) {
        starts.push(
            cursor
        );

        cursor +=
            chunk.length;
    }

    const results =
        await Promise.all(
            chunks.map(
                (
                    chunk,
                    index
                ) => {
                    const start =
                        starts[index];

                    return translateRescueChunk(
                        chunk,

                        deadlineAt,

                        contextForLocalSlice(
                            blocks,
                            start,
                            start +
                                chunk.length,
                            context
                        ),

                        budget,

                        0
                    );
                }
            )
        );

    return results.flat();
}

async function translateMainBatch(
    blocks,
    deadlineAt,
    context,
    budget
) {
    try {
        return await translateBatchOnce(
            blocks,

            deadlineAt,

            context,

            budget,

            {
                requestKind:
                    "translation"
            }
        );

    } catch (error) {
        if (
            error?.code ===
            "BAD_MODEL_OUTPUT"
        ) {
            console.warn(
                `[FAST 6.4] Contrato inválido em lote ${blocks.length}; UMA repetição do mesmo lote.`
            );

            try {
                return await translateBatchOnce(
                    blocks,

                    deadlineAt,

                    context,

                    budget,

                    {
                        requestKind:
                            "translation-structure-retry",

                        strictStructure:
                            true
                    }
                );

            } catch (
                retryError
            ) {
                if (
                    ![
                        "BAD_MODEL_OUTPUT",
                        "GEMINI_REQUEST_TIMEOUT"
                    ].includes(
                        retryError?.code
                    )
                ) {
                    throw retryError;
                }

                return translateBatchRescue(
                    blocks,
                    deadlineAt,
                    context,
                    budget
                );
            }
        }

        if (
            error?.code ===
            "GEMINI_REQUEST_TIMEOUT"
        ) {
            console.warn(
                `[FAST 6.4] Timeout em lote ${blocks.length}; RESCUE grande direto.`
            );

            return translateBatchRescue(
                blocks,
                deadlineAt,
                context,
                budget
            );
        }

        throw error;
    }
}

/* --------------------------------------------------------------------------
 * SAFE-SURGICAL
 * -------------------------------------------------------------------------- */

function normalizedRiskText(value) {
    return String(value || "")
        .replace(
            /<[^>]+>|\{[^}]+\}/g,
            " "
        )
        .replace(
            /\s+/g,
            " "
        )
        .trim();
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

    const leftText =
        normalizedRiskText(
            left.text
        );

    const rightText =
        normalizedRiskText(
            right.text
        );

    if (
        !leftText ||
        !rightText
    ) {
        return false;
    }

    return (
        strictFragmentTail(
            leftText
        ) ||
        (
            leftText.length <=
                44 &&
            !/[.!?…]["'”’\)\]]*$/u.test(
                leftText
            ) &&
            startsLikeContinuation(
                rightText
            )
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
        !translatedLength
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
            translatedTexts[
                index
            ]
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

        if (
            current ===
                neighbor &&
            normalizedRiskText(
                blocks[
                    index
                ].text
            ).toLowerCase() !==
                normalizedRiskText(
                    blocks[
                        neighborIndex
                    ].text
                ).toLowerCase()
        ) {
            return true;
        }
    }

    return false;
}

function addAuditCandidate(
    map,
    index,
    priority,
    reason
) {
    if (
        index < 0
    ) {
        return;
    }

    const existing =
        map.get(index);

    if (
        !existing ||
        priority <
            existing.priority
    ) {
        map.set(
            index,
            {
                index,
                priority,
                reason
            }
        );
    }
}

function selectFinalAuditIndices(
    blocks,
    translatedTexts,
    batchBoundaryStarts
) {
    const candidates =
        new Map();

    for (
        let i = 0;
        i < blocks.length;
        i++
    ) {
        if (
            translationLengthRisk(
                blocks[i].text,
                translatedTexts[i]
            ) ||
            suspiciousNeighborDuplicate(
                translatedTexts,
                i,
                blocks
            )
        ) {
            addAuditCandidate(
                candidates,
                i,
                0,
                "anomalia"
            );
        }
    }

    for (
        const start
        of batchBoundaryStarts
    ) {
        addAuditCandidate(
            candidates,
            start - 1,
            1,
            "fronteira-lote"
        );

        addAuditCandidate(
            candidates,
            start,
            1,
            "fronteira-lote"
        );
    }

    let strongCount = 0;

    for (
        let i = 0;
        i < blocks.length - 1;
        i++
    ) {
        if (
            strongBoundaryRisk(
                blocks[i],
                blocks[i + 1]
            )
        ) {
            if (
                strongCount %
                2 ===
                0
            ) {
                addAuditCandidate(
                    candidates,
                    i,
                    2,
                    "fronteira-forte"
                );

                addAuditCandidate(
                    candidates,
                    i + 1,
                    2,
                    "fronteira-forte"
                );
            }

            strongCount++;
        }
    }

    addAuditCandidate(
        candidates,
        0,
        3,
        "canario"
    );

    addAuditCandidate(
        candidates,
        blocks.length - 1,
        3,
        "canario"
    );

    for (
        let i =
            AUDIT_CANARY_STRIDE;
        i <
        blocks.length - 1;
        i +=
            AUDIT_CANARY_STRIDE
    ) {
        addAuditCandidate(
            candidates,
            i,
            3,
            "canario"
        );
    }

    const ordered =
        Array.from(
            candidates.values()
        ).sort(
            (
                a,
                b
            ) =>
                a.priority -
                    b.priority ||
                a.index -
                    b.index
        );

    const selected =
        ordered
            .slice(
                0,
                MAX_FINAL_AUDIT_RECORDS
            )
            .map(
                item =>
                    item.index
            )
            .sort(
                (
                    a,
                    b
                ) =>
                    a -
                    b
            );

    const counts = {
        anomaly: 0,
        batch: 0,
        strong: 0,
        canary: 0
    };

    for (
        const index
        of selected
    ) {
        const reason =
            candidates.get(
                index
            )?.reason;

        if (
            reason ===
            "anomalia"
        ) {
            counts.anomaly++;

        } else if (
            reason ===
            "fronteira-lote"
        ) {
            counts.batch++;

        } else if (
            reason ===
            "fronteira-forte"
        ) {
            counts.strong++;

        } else {
            counts.canary++;
        }
    }

    return {
        indices:
            selected,

        counts,

        candidatesTotal:
            ordered.length
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
            translatedTexts[
                index
            ],

        prev:
            previous
                ? {
                      id:
                          previous.index,

                      source:
                          previous.text
                  }
                : null,

        next:
            next
                ? {
                      id:
                          next.index,

                      source:
                          next.text
                  }
                : null,

        _index:
            index
    };
}

function auditRecordForModel(record) {
    const result = {
        i:
            record.id,

        l:
            record.lock,

        s:
            record.source,

        t:
            record.translation
    };

    if (record.prev) {
        result.p = [
            record.prev.id,
            record.prev.source
        ];
    }

    if (record.next) {
        result.n = [
            record.next.id,
            record.next.source
        ];
    }

    return result;
}

function buildSemanticAuditPrompt(records) {
    return `AUDITORIA CIRÚRGICA. Use SOMENTE p/n do próprio registro. m só pode ser i, p[0] ou n[0].
SAÍDA EXATA: [{"i":123,"l":"abc","m":123,"f":true}]
${JSON.stringify(
        records.map(
            auditRecordForModel
        )
    )}`;
}

function allowedMatchedIds(record) {
    const allowed =
        new Set([
            record.id
        ]);

    if (record.prev) {
        allowed.add(
            record.prev.id
        );
    }

    if (record.next) {
        allowed.add(
            record.next.id
        );
    }

    return allowed;
}

async function auditRecordsOnce(
    records,
    deadlineAt,
    budget
) {
    const raw =
        await enqueueGemini(
            buildSemanticAuditPrompt(
                records
            ),

            deadlineAt,

            "semantic-audit",

            budget
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
            "Auditoria retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(parsed) ||
        parsed.length !==
            records.length
    ) {
        throw badAuditOutputError(
            `Auditoria incompleta: esperado=${records.length}, recebido=${Array.isArray(parsed) ? parsed.length : 0}.`
        );
    }

    const failures = [];
    const invalid = [];

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
                "boolean"
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
                `[AUDIT 6.4] m inválido ignorado para reparo: ${expected.id}->${item.m}; será rechecado isoladamente.`
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

    return {
        failures,
        invalid
    };
}

async function auditRecords(
    records,
    deadlineAt,
    budget
) {
    try {
        return await auditRecordsOnce(
            records,
            deadlineAt,
            budget
        );

    } catch (error) {
        if (
            ![
                "BAD_AUDIT_OUTPUT",
                "GEMINI_REQUEST_TIMEOUT"
            ].includes(
                error?.code
            )
        ) {
            throw error;
        }

        if (
            records.length <= 6
        ) {
            throw error;
        }

        const middle =
            Math.ceil(
                records.length /
                2
            );

        console.warn(
            `[AUDIT 6.4] Chunk ${records.length} lento/inválido; split único ${middle}+${records.length - middle}.`
        );

        const [
            leftResult,
            rightResult
        ] =
            await Promise.all([
                auditRecordsOnce(
                    records.slice(
                        0,
                        middle
                    ),

                    deadlineAt,

                    budget
                ),

                auditRecordsOnce(
                    records.slice(
                        middle
                    ),

                    deadlineAt,

                    budget
                )
            ]);

        return {
            failures: [
                ...leftResult.failures,
                ...rightResult.failures
            ],

            invalid: [
                ...leftResult.invalid,
                ...rightResult.invalid
            ]
        };
    }
}

async function runBaseAudit(
    records,
    deadlineAt,
    budget
) {
    const chunks = [];

    for (
        let i = 0;
        i < records.length;
        i +=
            MAX_INDEPENDENT_AUDIT_BLOCKS
    ) {
        chunks.push(
            records.slice(
                i,
                i +
                MAX_INDEPENDENT_AUDIT_BLOCKS
            )
        );
    }

    const results =
        await Promise.all(
            chunks.map(
                chunk =>
                    auditRecords(
                        chunk,
                        deadlineAt,
                        budget
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
            ),

        baseRequests:
            chunks.length
    };
}

function contextForGlobalWindow(
    blocks,
    start,
    end
) {
    return {
        before:
            start > 0
                ? blocks[
                      start - 1
                  ]
                : null,

        after:
            end <
            blocks.length - 1
                ? blocks[
                      end + 1
                  ]
                : null
    };
}

async function recheckSmall(
    records,
    deadlineAt,
    budget
) {
    if (
        !records.length
    ) {
        return [];
    }

    const compact =
        records.slice(
            0,
            6
        );

    try {
        const result =
            await auditRecordsOnce(
                compact,
                deadlineAt,
                budget
            );

        return [
            ...result.failures,

            ...result.invalid.map(
                record => ({
                    id:
                        record.id,

                    matchedSourceId:
                        record.id,

                    faithful:
                        false,

                    _index:
                        record._index
                })
            )
        ];

    } catch (error) {
        console.warn(
            `[AUDIT 6.4] Rechecagem curta falhou: ${getErrorMessage(error)}.`
        );

        return compact.map(
            record => ({
                id:
                    record.id,

                matchedSourceId:
                    record.id,

                faithful:
                    false,

                _index:
                    record._index
            })
        );
    }
}

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

    const suspects =
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
            suspects.add(
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
            suspects.add(
                matched
            );
        }
    }

    const sorted =
        Array.from(
            suspects
        ).sort(
            (
                a,
                b
            ) =>
                a -
                b
        );

    const windows = [];

    let current = [];

    for (
        const index
        of sorted
    ) {
        if (
            !current.length ||
            (
                index <=
                    current[
                        current.length -
                        1
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

            current = [
                index
            ];
        }
    }

    if (current.length) {
        windows.push(
            current
        );
    }

    return windows;
}

async function repairWindow(
    blocks,
    translatedTexts,
    indices,
    deadlineAt,
    budget
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
                indices.length -
                1
            ]
        );

    console.warn(
        `[REPAIR 6.4] IDs ${repairBlocks.map(
            block =>
                block.index
        ).join(",")} (${repairBlocks.length}).`
    );

    let repaired;

    try {
        repaired =
            await translateBatchOnce(
                repairBlocks,

                deadlineAt,

                context,

                budget,

                {
                    requestKind:
                        "translation-repair",

                    strictStructure:
                        true,

                    repairMode:
                        true
                }
            );

    } catch (error) {
        if (
            ![
                "BAD_MODEL_OUTPUT",
                "GEMINI_REQUEST_TIMEOUT"
            ].includes(
                error?.code
            )
        ) {
            throw error;
        }

        repaired = [];

        for (
            let i = 0;
            i <
            repairBlocks.length;
            i++
        ) {
            const one =
                await translateSingleLastChance(
                    repairBlocks[i],

                    deadlineAt,

                    contextForGlobalWindow(
                        blocks,
                        indices[i],
                        indices[i]
                    ),

                    budget,

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
            repaired[i];
    }
}

/* --------------------------------------------------------------------------
 * TRANSLATE SRT
 * -------------------------------------------------------------------------- */

async function translateSrt(
    originalSrt,
    job
) {
    const blocks =
        parseSrt(
            originalSrt
        );

    if (!blocks.length) {
        throw new Error(
            "Nenhum bloco SRT válido."
        );
    }

    const batches =
        splitIntoBatches(
            blocks
        );

    const startedAt =
        Date.now();

    const deadlineAt =
        startedAt +
        MAX_TRANSLATION_TIME_MS;

    const budget = {
        used:
            0,

        max:
            MAX_JOB_GEMINI_REQUESTS
    };

    job.startedAt =
        startedAt;

    job.deadlineAt =
        deadlineAt;

    job.updatedAt =
        startedAt;

    job.requestBudget =
        budget;

    console.log(
        `[TRANSLATE] ${blocks.length} blocos.`
    );

    console.log(
        `[TRANSLATE] ${batches.length} lote(s).`
    );

    console.log(
        `[FAST 6.4] ${MAX_BATCH_BLOCKS} blocos/${MAX_BATCH_CHARS} chars; concorrência=${GEMINI_CONCURRENCY}; orçamento=${MAX_JOB_GEMINI_REQUESTS} requests.`
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

    const batchBoundaryStarts = [];

    console.log(
        "[PHASE FAST] Disparando lotes com pipeline concorrente 3x."
    );

    const tasks =
        batches.map(
            async (
                batch,
                batchIndex
            ) => {
                const batchStart =
                    originalPositions.get(
                        batch[0]
                    );

                const batchEnd =
                    originalPositions.get(
                        batch[
                            batch.length -
                            1
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

                const context = {
                    before:
                        Number.isInteger(
                            batchStart
                        ) &&
                        batchStart > 0
                            ? blocks[
                                  batchStart -
                                  1
                              ]
                            : null,

                    after:
                        Number.isInteger(
                            batchEnd
                        ) &&
                        batchEnd <
                            blocks.length -
                            1
                            ? blocks[
                                  batchEnd +
                                  1
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
                                ""
                            ).length,

                        0
                    );

                const started =
                    Date.now();

                console.log(
                    `[PHASE FAST] Lote ${batchIndex + 1}/${batches.length} enfileirado - ${batch.length} blocos/${chars} chars.`
                );

                const translated =
                    await translateMainBatch(
                        batch,
                        deadlineAt,
                        context,
                        budget
                    );

                for (
                    let i = 0;
                    i <
                    batch.length;
                    i++
                ) {
                    translatedTexts[
                        originalPositions.get(
                            batch[i]
                        )
                    ] =
                        translated[i];
                }

                job.completedBatches =
                    (
                        job.completedBatches ||
                        0
                    ) +
                    1;

                job.totalBatches =
                    batches.length;

                job.progress =
                    Math.round(
                        (
                            job.completedBatches /
                            batches.length
                        ) *
                        84
                    );

                job.updatedAt =
                    Date.now();

                console.log(
                    `[PHASE FAST] Lote ${batchIndex + 1}/${batches.length} OK em ${((Date.now() - started) / 1000).toFixed(1)}s.`
                );
            }
        );

    await Promise.all(
        tasks
    );

    if (
        translatedTexts.some(
            text =>
                typeof text !==
                "string"
        )
    ) {
        throw new Error(
            "A fase FAST terminou com blocos ausentes."
        );
    }

    const fastElapsed =
        Date.now() -
        startedAt;

    console.log(
        `[PHASE FAST] Tradução completa em ${(fastElapsed / 1000).toFixed(1)}s; requests usados=${budget.used}/${budget.max}.`
    );

    const selection =
        selectFinalAuditIndices(
            blocks,
            translatedTexts,
            batchBoundaryStarts
        );

    console.log(
        `[PHASE SAFE] ${selection.indices.length}/${blocks.length} selecionados (candidatos=${selection.candidatesTotal}; anomalia=${selection.counts.anomaly}, lote=${selection.counts.batch}, forte=${selection.counts.strong}, canário=${selection.counts.canary}).`
    );

    const records =
        selection.indices.map(
            index =>
                buildAuditRecord(
                    blocks,
                    translatedTexts,
                    index
                )
        );

    let failures = [];

    if (records.length) {
        const audit =
            await runBaseAudit(
                records,
                deadlineAt,
                budget
            );

        failures.push(
            ...audit.failures
        );

        if (
            audit.invalid.length
        ) {
            console.log(
                `[PHASE SAFE] ${audit.invalid.length} resposta(s) inválida(s) serão rechecadas em grupo pequeno.`
            );

            failures.push(
                ...await recheckSmall(
                    audit.invalid,
                    deadlineAt,
                    budget
                )
            );
        }

        const ownFailures =
            failures.filter(
                failure =>
                    failure
                        .matchedSourceId ===
                    failure.id
            );

        const migrations =
            failures.filter(
                failure =>
                    failure
                        .matchedSourceId !==
                    failure.id
            );

        if (
            ownFailures.length &&
            budget.used <
                budget.max
        ) {
            const ownRecords =
                ownFailures
                    .slice(
                        0,
                        6
                    )
                    .map(
                        failure =>
                            buildAuditRecord(
                                blocks,
                                translatedTexts,
                                failure._index
                            )
                    );

            const confirmedOwn =
                await recheckSmall(
                    ownRecords,
                    deadlineAt,
                    budget
                );

            failures = [
                ...migrations,
                ...confirmedOwn
            ];

        } else {
            failures = [
                ...migrations,
                ...ownFailures
            ];
        }
    }

    if (failures.length) {
        const unique =
            new Map();

        for (
            const failure
            of failures
        ) {
            unique.set(
                `${failure.id}:${failure.matchedSourceId}`,
                failure
            );
        }

        failures =
            Array.from(
                unique.values()
            );

        console.warn(
            `[PHASE SAFE] ${failures.length} falha(s) confirmada(s); reparo cirúrgico.`
        );

        const windows =
            buildRepairWindows(
                failures,
                blocks
            );

        for (
            const window
            of windows
        ) {
            if (
                budget.used >=
                budget.max
            ) {
                throw requestBudgetError();
            }

            await repairWindow(
                blocks,
                translatedTexts,
                window,
                deadlineAt,
                budget
            );
        }
    }

    const elapsedMs =
        Date.now() -
        startedAt;

    console.log(
        `[TRANSLATE] Finalizada em ${(elapsedMs / 1000).toFixed(1)}s.`
    );

    console.log(
        `[PERF] FAST=${(fastElapsed / 1000).toFixed(1)}s | SAFE+REPAIR=${((elapsedMs - fastElapsed) / 1000).toFixed(1)}s | TOTAL=${(elapsedMs / 1000).toFixed(1)}s | requests=${budget.used}/${budget.max}.`
    );

    if (
        elapsedMs <=
        PERFORMANCE_TARGET_MS
    ) {
        console.log(
            "[PERF] META ~1 MIN / até 90s: ATINGIDA ✅"
        );

    } else {
        console.log(
            `[PERF] Acima de 90s: ${(elapsedMs / 1000).toFixed(1)}s.`
        );
    }

    const charsClean =
        cleanAllTranslatedCharacters(
            translatedTexts
        );

    const vocalsClean =
        cleanAllTranslatedVocalElongations(
            charsClean
        );

    const markersClean =
        cleanAllTranslatedDialogueMarkers(
            vocalsClean
        );

    const finalSrt =
        buildSrt(
            blocks,
            markersClean
        );

    auditFinalTimestamps(
        originalSrt,
        finalSrt,
        "TRADUÇÃO FINAL"
    );

    job.timestampAuditPassed =
        true;

    job.contentAuditPassed =
        true;

    job.progress =
        100;

    console.log(
        "[AUDIT CONTENT] TRADUÇÃO FINAL: OK — ID/lock 100%; SAFE-SURGICAL concluído; timestamps íntegros."
    );

    return finalSrt;
}

/* --------------------------------------------------------------------------
 * JOBS
 * -------------------------------------------------------------------------- */

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
            "processing",

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

        deadlineAt:
            null,

        queuedAt:
            null,

        priority:
            50,

        jobKind:
            "generic",

        lazyStartScheduled:
            false,

        expiresAt:
            now +
            JOB_TTL_MS,

        timestampAuditPassed:
            false,

        contentAuditPassed:
            false,

        requestBudget:
            null,

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
        job.status !==
            "processing"
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
                "CACHE"
            );

            job.timestampAuditPassed =
                true;

            job.contentAuditPassed =
                true;

            job.status =
                "completed";

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
            "completed";

        job.progress =
            100;

        job.updatedAt =
            Date.now();

        console.log(
            `[JOB ${job.id}] Concluído.`
        );

    } catch (error) {
        job.status =
            "failed";

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
                "processing"
            ) {
                return resolve();
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
                        item =>
                            Number(
                                item?.job?.priority ||
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
                `[JOB QUEUE] ${job.id} entrou na fila. Prioridade=${job.priority}; aguardando=${translationJobQueue.length}.`
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
                    "processing"
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
                    `[JOB QUEUE] Iniciando ${item.job.id}; espera=${(wait / 1000).toFixed(1)}s.`
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
    label = "JOB"
) {
    if (
        !job ||
        job.status === "completed" ||
        job.status === "failed" ||
        job.promise
    ) {
        return (
            job?.promise ||
            null
        );
    }

    if (
        job.status === "pending"
    ) {
        job.status =
            "processing";

        job.updatedAt =
            Date.now();
    }

    job.promise =
        enqueueTranslationJob(
            job
        ).catch(
            error => {
                console.error(
                    `[${label} ${job.id}] Erro inesperado:`,
                    error
                );

                job.status =
                    "failed";

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
            (
                job.status ===
                    "processing" ||
                job.status ===
                    "pending"
            )
        ) {
            return job;
        }
    }

    return null;
}

function buildProcessingSrt(job) {
    const progress =
        Number.isFinite(
            job?.progress
        )
            ? job.progress
            : 0;

    return [
        "1",
        "00:00:01,000 --> 00:00:06,000",
        "⏳ Traduzindo legenda...",
        "",
        "2",
        "00:00:06,500 --> 00:00:12,000",
        `Progresso: ${progress}%.`
    ].join("\n");
}

function buildErrorSrt(message) {
    const safe =
        String(
            message ||
            "Erro desconhecido."
        )
            .replace(
                /\s+/g,
                " "
            )
            .slice(
                0,
                300
            );

    return [
        "1",
        "00:00:01,000 --> 00:00:08,000",
        "Não foi possível traduzir esta legenda.",
        "",
        "2",
        "00:00:08,500 --> 00:00:18,000",
        safe
    ].join("\n");
}

function sendSubtitleResponse(
    res,
    srt,
    cacheControl = "no-store"
) {
    res.status(200);

    res.set({
        "Content-Type":
            "text/plain; charset=utf-8",

        "Cache-Control":
            cacheControl,

        "Access-Control-Allow-Origin":
            "*"
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
        "completed"
    ) {
        return true;
    }

    if (
        job.status ===
        "failed"
    ) {
        return false;
    }

    const start =
        Date.now();

    while (
        job.status ===
            "processing" &&
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
        "completed"
    );
}

/* --------------------------------------------------------------------------
 * OPENSUBTITLES
 * -------------------------------------------------------------------------- */

function scoreSubtitle(subtitle) {
    let score = 0;

    const lang =
        String(
            subtitle?.lang ||
            ""
        ).toLowerCase();

    if (
        lang === "eng"
    ) {
        score += 100;

    } else if (
        lang === "en"
    ) {
        score += 90;
    }

    if (
        subtitle?.hearingImpaired ===
        false
    ) {
        score += 20;
    }

    if (
        String(
            subtitle?.format ||
            ""
        ).toLowerCase() ===
        "srt"
    ) {
        score += 20;
    }

    if (
        /english/i.test(
            String(
                subtitle?.name ||
                ""
            )
        )
    ) {
        score += 10;
    }

    return score;
}

function isUsableEnglishSubtitle(subtitle) {
    const lang =
        String(
            subtitle?.lang ||
            ""
        ).toLowerCase();

    return (
        (
            lang === "eng" ||
            lang === "en"
        ) &&
        typeof subtitle?.url ===
            "string" &&
        /^https?:\/\//i.test(
            subtitle.url
        )
    );
}

function selectBestSubtitle(
    subtitles,
    {
        releaseAware = false
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

    if (!usable.length) {
        return null;
    }

    if (releaseAware) {
        return usable[0];
    }

    return (
        usable.sort(
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
            ""
        ).split("?")[0];

    const match =
        pathname.match(
            /^\/subtitles\/[^/]+\/[^/]+\/(.+)\.json$/
        );

    return (
        match?.[1] ||
        String(
            req.params.extra ||
            ""
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
            rawExtra:
                "",

            videoHash:
                "",

            videoSize:
                "",

            filename:
                ""
        };
    }

    const params =
        new URLSearchParams(
            rawExtra
        );

    const rawVideoHash =
        String(
            params.get(
                "videoHash"
            ) ||
            ""
        ).trim();

    const rawVideoSize =
        String(
            params.get(
                "videoSize"
            ) ||
            ""
        ).trim();

    const filename =
        String(
            params.get(
                "filename"
            ) ||
            ""
        )
            .replace(
                /[\u0000-\u001F\u007F]/g,
                ""
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
            ? rawVideoHash
                  .toLowerCase()
            : "";

    const videoSizeNumber =
        Number(
            rawVideoSize
        );

    const videoSize =
        Number.isSafeInteger(
            videoSizeNumber
        ) &&
        videoSizeNumber > 0
            ? String(
                  videoSizeNumber
              )
            : "";

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
        videoHash = "",
        videoSize = "",
        filename = ""
    } = {}
) {
    const base =
        `https://opensubtitles-v3.strem.io/subtitles/${encodeURIComponent(
            type
        )}/${encodeURIComponent(
            id
        )}`;

    const extra =
        new URLSearchParams();

    if (videoHash) {
        extra.set(
            "videoHash",
            videoHash
        );
    }

    if (videoSize) {
        extra.set(
            "videoSize",
            videoSize
        );
    }

    if (filename) {
        extra.set(
            "filename",
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
    return [
        extra.videoHash &&
            "videoHash",

        extra.videoSize &&
            "videoSize",

        extra.filename &&
            "filename"
    ]
        .filter(Boolean)
        .join(" + ") ||
        "nenhuma";
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
        `[OPENSUBTITLES MATCH] Modo: ${releaseAware ? "RELEASE-AWARE" : "GENÉRICO"}; identidade: ${releaseIdentityDescription(extra)}.`
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
                        "application/json",

                    "User-Agent":
                        "Stremio-Gemini-Subtitle-Translator/6.4"
                }
            },

            SOURCE_FETCH_TIMEOUT_MS
        );

    if (!response.ok) {
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
            `[OPENSUBTITLES MATCH] Selecionada posição upstream ${position >= 0 ? position + 1 : "?"}/${subtitles.length}; id=${String(target.id || "(sem id)")}; nome=${String(target.name || "(sem nome)")}.`
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
                    "User-Agent":
                        "Stremio-Gemini-Subtitle-Translator/6.4"
                }
            },

            SOURCE_FETCH_TIMEOUT_MS
        );

    if (!response.ok) {
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
            "Legenda vazia."
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
            "A legenda ficou vazia após a limpeza SDH/CC."
        );
    }

    return text;
}

/* --------------------------------------------------------------------------
 * EMBEDDED
 * -------------------------------------------------------------------------- */

async function createEmbeddedTranslationJob({
    type,
    videoId,
    sourceSrt,
    sourceName = "embedded"
}) {
    const rawNormalizedSrt =
        normalizeSrt(
            sourceSrt
        );

    if (!rawNormalizedSrt) {
        throw new Error(
            "A legenda embutida está vazia."
        );
    }

    if (
        rawNormalizedSrt.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda embutida muito grande: ${rawNormalizedSrt.length} caracteres.`
        );
    }

    const normalizedSrt =
        cleanSrtForTranslation(
            rawNormalizedSrt
        );

    if (!normalizedSrt) {
        throw new Error(
            "A legenda embutida ficou vazia após a limpeza SDH/CC."
        );
    }

    const blocks =
        parseSrt(
            normalizedSrt
        );

    if (!blocks.length) {
        throw new Error(
            "A legenda embutida não possui blocos SRT válidos."
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
            "EMBEDDED CACHE"
        );

        const cachedJobId =
            `embedded-cached-${sourceHash.slice(
                0,
                24
            )}`;

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

            cachedJob.status =
                "completed";

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
        return existing;
    }

    const jobId =
        `embedded-${sourceHash.slice(
            0,
            24
        )}-${randomId(8)}`;

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
        "embedded";

    job.totalBatches =
        splitIntoBatches(
            blocks
        ).length;

    startTranslationJob(
        job,
        "EMBEDDED JOB"
    );

    console.log(
        `[EMBEDDED] Novo job ${job.id} criado.`
    );

    return job;
}

/* --------------------------------------------------------------------------
 * STREMIO
 * -------------------------------------------------------------------------- */

async function subtitlesHandler(
    req,
    res
) {
    const type =
        String(
            req.params.type ||
            ""
        ).trim();

    const id =
        String(
            req.params.id ||
            ""
        ).trim();

    console.log(
        `[STREMIO] Pedido: ${type}/${id}`
    );

    const extra =
        parseStremioSubtitleExtra(
            req
        );

    console.log(
        `[STREMIO EXTRA] filename: ${extra.filename || "(não enviado)"}`
    );

    console.log(
        `[STREMIO EXTRA] videoSize: ${extra.videoSize || "(não enviado)"}`
    );

    console.log(
        `[STREMIO EXTRA] videoHash: ${extra.videoHash || "(não enviado)"}`
    );

    if (!type || !id) {
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

        if (!blocks.length) {
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
                "OPENSUBTITLES CACHE"
            );

            const jobId =
                `cached-${sourceHash.slice(
                    0,
                    24
                )}`;

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

                job.status =
                    "completed";

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
                                `${id}-gemini-${sourceHash.slice(
                                    0,
                                    12
                                )}`,

                            url:
                                `${baseUrl}/subtitle/${encodeURIComponent(
                                    jobId
                                )}.srt`,

                            lang:
                                "por"
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
                `job-${sourceHash.slice(
                    0,
                    24
                )}-${randomId(8)}`;

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
                "opensubtitles";

            job.status =
                "pending";

            job.lazyStart =
                true;

            console.log(
                `[LAZY] Job OpenSubtitles ${job.id} criado sem consumir Gemini.`
            );
        }

        const subtitleUrl =
            `${baseUrl}/subtitle/${encodeURIComponent(
                job.id
            )}.srt`;

        console.log(
            `[STREMIO] Subtitle URL: ${subtitleUrl}`
        );

        return safeJson(
            res,
            {
                subtitles: [
                    {
                        id:
                            `${id}-gemini-${sourceHash.slice(
                                0,
                                12
                            )}`,

                        url:
                            subtitleUrl,

                        lang:
                            "por"
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
    "/subtitles/:type/:id.json",
    subtitlesHandler
);

app.get(
    "/subtitles/:type/:id/:extra.json",
    subtitlesHandler
);

/* --------------------------------------------------------------------------
 * API EMBEDDED
 * -------------------------------------------------------------------------- */

app.post(
    "/api/translate-embedded",

    async (
        req,
        res
    ) => {
        if (
            !isAuthorizedLocalBridge(
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
                    "string"
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
                            `SRT muito grande. Limite: ${MAX_SOURCE_CHARS}.`
                    },

                    413
                );
            }

            const job =
                await createEmbeddedTranslationJob({
                    type:
                        String(
                            type ||
                            "unknown"
                        ).trim(),

                    videoId:
                        String(
                            id ||
                            "unknown"
                        ).trim(),

                    sourceSrt:
                        srt,

                    sourceName:
                        String(
                            name ||
                            "embedded"
                        ).trim()
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
                        `${baseUrl}/subtitle/${encodeURIComponent(
                            job.id
                        )}.srt`
                }
            );

        } catch (error) {
            console.error(
                "[EMBEDDED API] Erro:",
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

/* --------------------------------------------------------------------------
 * RESULTADO
 * -------------------------------------------------------------------------- */

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
                    ""
                ).trim()
            );

    } catch {
        jobId =
            String(
                req.params.jobId ||
                ""
            ).trim();
    }

    if (!jobId) {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(
                "Job inválido."
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
                "Esta tradução expirou. Recarregue as legendas."
            )
        );
    }

    if (
        job.status ===
            "pending" &&
        !job.lazyStartScheduled
    ) {
        job.lazyStartScheduled =
            true;

        console.log(
            `[LAZY] URL ${job.id} requisitada; grace ${LAZY_OPENSUB_START_GRACE_MS}ms.`
        );

        const timer =
            setTimeout(
                () => {
                    job.lazyStartScheduled =
                        false;

                    if (
                        job.status ===
                        "pending"
                    ) {
                        startTranslationJob(
                            job,
                            "JOB"
                        );
                    }
                },

                LAZY_OPENSUB_START_GRACE_MS
            );

        if (
            typeof timer.unref ===
            "function"
        ) {
            timer.unref();
        }
    }

    if (
        job.status ===
            "completed" &&
        job.result
    ) {
        if (
            !job.timestampAuditPassed
        ) {
            try {
                auditFinalTimestamps(
                    job.sourceSrt,
                    job.result,
                    "SERVING"
                );

                job.timestampAuditPassed =
                    true;

            } catch {
                return sendSubtitleResponse(
                    res,
                    buildErrorSrt(
                        "Auditoria de timestamps bloqueou esta legenda."
                    )
                );
            }
        }

        if (
            !job.contentAuditPassed
        ) {
            return sendSubtitleResponse(
                res,
                buildErrorSrt(
                    "Auditoria semântica não confirmou esta legenda."
                )
            );
        }

        return sendSubtitleResponse(
            res,
            job.result,
            "public, max-age=604800"
        );
    }

    if (
        job.status ===
        "failed"
    ) {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(
                job.error
            )
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
            "completed" &&
        job.result
    ) {
        if (
            !job.timestampAuditPassed
        ) {
            try {
                auditFinalTimestamps(
                    job.sourceSrt,
                    job.result,
                    "SERVING"
                );

                job.timestampAuditPassed =
                    true;

            } catch {
                return sendSubtitleResponse(
                    res,
                    buildErrorSrt(
                        "Auditoria de timestamps bloqueou esta legenda."
                    )
                );
            }
        }

        if (
            !job.contentAuditPassed
        ) {
            return sendSubtitleResponse(
                res,
                buildErrorSrt(
                    "Auditoria semântica não confirmou esta legenda."
                )
            );
        }

        return sendSubtitleResponse(
            res,
            job.result,
            "public, max-age=604800"
        );
    }

    if (
        job.status ===
        "failed"
    ) {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(
                job.error
            )
        );
    }

    return sendSubtitleResponse(
        res,
        buildProcessingSrt(
            job
        ),
        "no-store, no-cache, must-revalidate"
    );
}

app.get(
    "/subtitle/:jobId.srt",
    subtitleResultHandler
);

/* --------------------------------------------------------------------------
 * MANIFEST / HEALTH
 * -------------------------------------------------------------------------- */

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "6.4.0",

    name:
        "Tradutor Gemini PT-BR",

    description:
        "Traduz automaticamente legendas para Português do Brasil usando Gemini.",

    logo:
        "",

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
            false,

        adult:
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

function statusPayload() {
    pruneRequestStarts();

    return {
        name:
            manifest.name,

        version:
            manifest.version,

        status:
            "online",

        model:
            GEMINI_MODEL,

        renderEngineVersion:
            RENDER_ENGINE_VERSION,

        performanceMode:
            "fast-lean-safe-surgical",

        thinkingLevel:
            "MINIMAL",

        translationCacheVersion:
            TRANSLATION_CACHE_VERSION,

        bridgeCompatibleCacheVersion:
            "5.8",

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

        maxJobGeminiRequests:
            MAX_JOB_GEMINI_REQUESTS,

        finalAuditMaxRecords:
            MAX_FINAL_AUDIT_RECORDS,

        finalAuditChunk:
            MAX_INDEPENDENT_AUDIT_BLOCKS,

        auditCanaryStride:
            AUDIT_CANARY_STRIDE,

        timestampAudit:
            true,

        structuralIdLock:
            true,

        semanticAuditMode:
            "surgical-final-max-72",

        releaseAwareOpenSubtitles:
            true,

        lazyOpenSubtitles:
            true,

        queue:
            geminiQueue.length,

        activeGeminiWorkers,

        cooldownSeconds:
            Math.ceil(
                getCooldownRemaining() /
                1000
            )
    };
}

app.get(
    "/",

    (
        req,
        res
    ) =>
        res.json(
            statusPayload()
        )
);

app.get(
    "/health",

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

/* --------------------------------------------------------------------------
 * START
 * -------------------------------------------------------------------------- */

app.listen(
    PORT,

    () => {
        console.log(
            "=============================================="
        );

        console.log(
            " STREMIO GEMINI SUBTITLE TRANSLATOR 6.4 FAST-LEAN / SAFE-SURGICAL"
        );

        console.log(
            "=============================================="
        );

        console.log(
            `Porta: ${PORT}`
        );

        console.log(
            `Modelo Gemini: ${GEMINI_MODEL}`
        );

        console.log(
            "Thinking Gemini: MINIMAL ✅"
        );

        console.log(
            "Sampling temperature/top_p/top_k: REMOVIDOS ✅"
        );

        console.log(
            `PUBLIC_URL: ${PUBLIC_URL || "(automático)"}`
        );

        console.log(
            `Batch FAST: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} chars`
        );

        console.log(
            `Concorrência Gemini: ${GEMINI_CONCURRENCY} ✅`
        );

        console.log(
            `Limitador deslizante: ${SAFE_RPM} RPM / intervalo mínimo ${MIN_REQUEST_INTERVAL_MS}ms ✅`
        );

        console.log(
            `Orçamento por job: máx ${MAX_JOB_GEMINI_REQUESTS} requests ✅`
        );

        console.log(
            `Timeout tradução: ${TRANSLATION_REQUEST_TIMEOUT_MS}ms`
        );

        console.log(
            `SAFE-SURGICAL: máx ${MAX_FINAL_AUDIT_RECORDS} registros / ${MAX_INDEPENDENT_AUDIT_BLOCKS} por request / canário a cada ${AUDIT_CANARY_STRIDE} ✅`
        );

        console.log(
            "FAST: PIPELINE 3x / LOTES GRANDES ✅"
        );

        console.log(
            "SAFE: SOMENTE FINAL / CIRÚRGICO / SEM AUDITAR 40% DA LEGENDA ✅"
        );

        console.log(
            "matchedSourceId: SOMENTE próprio ID ou vizinho imediato ✅"
        );

        console.log(
            "Falha semântica: REPARO SOMENTE DA JANELA AFETADA ✅"
        );

        console.log(
            "ID + lock: 100% DOS BLOCOS ✅"
        );

        console.log(
            "Auditoria absoluta de timestamps: ATIVA ✅"
        );

        console.log(
            "PT-BR premium + Gen Z/Alpha contextual + LGBTQIAPN+/drag/camp/shade: ATIVO ✅"
        );

        console.log(
            "Ponte Local 2.5.1: COMPATÍVEL + PRIORIDADE ALTA ✅"
        );

        console.log(
            `Namespace de cache: ${TRANSLATION_CACHE_VERSION}`
        );

        console.log(
            "Status: ONLINE"
        );

        console.log(
            "=============================================="
        );
    }
);

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
