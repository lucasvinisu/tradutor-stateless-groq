const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({
    limit: "1mb"
}));

/*
|--------------------------------------------------------------------------
| CONFIGURAÇÃO
|--------------------------------------------------------------------------
*/

const PORT = Number(process.env.PORT || 10000);
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();
const GEMINI_MODEL = String(process.env.GEMINI_MODEL ||
    "gemini-3.5-flash-lite").trim();
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const SOURCE_FETCH_TIMEOUT_MS = 20000;

/*
 * 6.3 FAST-FIRST / SAFE-FINAL:
 * - traduz TODOS os lotes primeiro;
 * - protocolo compacto i/l/t;
 * - ID + lock + ordem + quantidade validados em 100%;
 * - auditoria Gemini somente ao final, em chunks curtos de 24;
 * - detector local seletivo: fronteiras sintáticas fortes, fronteiras de lote,
 *   proporção anormal, duplicação suspeita e canários;
 * - f=false/m=i é rechecado antes de qualquer reparo;
 * - migração m!=i repara apenas a janela afetada;
 * - erro estrutural recebe uma única repetição do mesmo lote antes de RESCUE;
 * - timeout entra em RESCUE fixo, sem árvore longa;
 * - Ponte Local 2.5.1 permanece compatível.
 */

const TRANSLATION_REQUEST_TIMEOUT_MS = 30000;
const STRUCTURE_RETRY_TIMEOUT_MS = 30000;
const RESCUE_TRANSLATION_REQUEST_TIMEOUT_MS = 15000;
const REPAIR_TRANSLATION_REQUEST_TIMEOUT_MS = 15000;
const SINGLE_TRANSLATION_REQUEST_TIMEOUT_MS = 12000;
const SINGLE_LAST_TRANSLATION_REQUEST_TIMEOUT_MS = 18000;
const SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS = 8000;

const MAX_INDEPENDENT_AUDIT_BLOCKS = 24;
const AUDIT_CANARY_STRIDE = 9;
const HIGH_RISK_AUDIT_SCORE = 3;

const RESCUE_BATCH_BLOCKS = 24;
const RESCUE_BATCH_CHARS = 2400;
const MAX_RESCUE_SPLIT_DEPTH = 1;
const MAX_REPAIR_WINDOW_BLOCKS = 6;

const PERFORMANCE_TARGET_MS = 180000;
const LAZY_OPENSUB_START_GRACE_MS = 1500;

const MAX_TRANSLATION_TIME_MS = Number(
    process.env.MAX_TRANSLATION_TIME_MS ||
    480000
);

const configuredMaxBatchChars = Number(
    process.env.MAX_BATCH_CHARS ||
    7000
);

const MAX_BATCH_CHARS =
    Number.isFinite(configuredMaxBatchChars) &&
    configuredMaxBatchChars > 0
        ? Math.min(configuredMaxBatchChars, 7000)
        : 7000;

const configuredMaxBatchBlocks = Number(
    process.env.MAX_BATCH_BLOCKS ||
    96
);

const MAX_BATCH_BLOCKS =
    Number.isFinite(configuredMaxBatchBlocks) &&
    configuredMaxBatchBlocks > 0
        ? Math.min(
              Math.floor(configuredMaxBatchBlocks),
              96
          )
        : 96;

const GEMINI_CONCURRENCY = 1;

const MIN_REQUEST_INTERVAL_MS = Number(
    process.env.MIN_REQUEST_INTERVAL_MS ||
    3000
);

const MAX_OUTPUT_TOKENS = Number(
    process.env.MAX_OUTPUT_TOKENS ||
    16000
);

const AUDIT_MAX_OUTPUT_TOKENS = 4096;

const MAX_NORMAL_RETRIES = 1;
const MAX_AUDIT_SPLIT_DEPTH = 2;
const MAX_SINGLE_BLOCK_OUTPUT_RETRIES = 1;
const MAX_SINGLE_BLOCK_AUDIT_RETRIES = 1;
const MAX_RATE_LIMIT_COOLDOWN_MS = 120000;

const CACHE_TTL_MS =
    7 * 24 * 60 * 60 * 1000;

const JOB_TTL_MS =
    24 * 60 * 60 * 1000;

const MAX_CACHE_ENTRIES = 200;
const MAX_JOBS = 300;
const MAX_SOURCE_CHARS = 800000;

/*
 * Cache/lock continuam em 5.8 para compatibilidade direta com a
 * Ponte Local 2.5.1. O protocolo da Ponte não muda.
 */

const TRANSLATION_CACHE_VERSION = "5.8";
const BLOCK_LOCK_VERSION = "5.8";
const SEMANTIC_AUDIT_ENABLED = true;
const RENDER_ENGINE_VERSION = "6.3-fast-first-safe-final";

/*
|--------------------------------------------------------------------------
| MEMÓRIA
|--------------------------------------------------------------------------
*/

const translationCache = new Map();
const jobs = new Map();

const translationJobQueue = [];
let translationJobWorkerRunning = false;

const geminiQueue = [];
let geminiWorkerRunning = false;

let lastGeminiRequestAt = 0;
let geminiCooldownUntil = 0;

/*
|--------------------------------------------------------------------------
| HELPERS
|--------------------------------------------------------------------------
*/

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function translationTimeoutError() {
    const error = new Error(
        "Tempo máximo de tradução atingido."
    );

    error.code =
        "TRANSLATION_TIMEOUT";

    return error;
}

function geminiRequestTimeoutError(requestKind) {
    const error = new Error(
        `Timeout da chamada Gemini (${requestKind || "request"}).`
    );

    error.code =
        "GEMINI_REQUEST_TIMEOUT";

    error.requestKind =
        requestKind ||
        "request";

    return error;
}

function timestampIntegrityError(message) {
    const error = new Error(message);

    error.code =
        "TIMESTAMP_INTEGRITY_ERROR";

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
    const safeMs =
        Math.max(
            0,
            Number(ms) || 0
        );

    if (safeMs === 0) {
        assertBeforeDeadline(deadlineAt);
        return;
    }

    const remaining =
        remainingBeforeDeadline(
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

    await sleep(safeMs);
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

/*
|--------------------------------------------------------------------------
| AUTENTICAÇÃO DA PONTE LOCAL
|--------------------------------------------------------------------------
*/

function isAuthorizedLocalBridge(req) {
    if (!LOCAL_BRIDGE_SECRET) {
        return false;
    }

    const auth =
        String(
            req.headers.authorization ||
            ""
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

    if (
        authBuffer.length !==
        expectedBuffer.length
    ) {
        return false;
    }

    return crypto.timingSafeEqual(
        authBuffer,
        expectedBuffer
    );
}

/*
|--------------------------------------------------------------------------
| FETCH COM TIMEOUT
|--------------------------------------------------------------------------
*/

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

/*
|--------------------------------------------------------------------------
| LIMPEZA DE MEMÓRIA
|--------------------------------------------------------------------------
*/

function cleanupMemory() {
    const now = Date.now();

    for (
        const [
            key,
            item
        ] of translationCache.entries()
    ) {
        if (item.expiresAt <= now) {
            translationCache.delete(key);
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
            translationCache.keys()
                .next()
                .value;

        if (key === undefined) {
            break;
        }

        translationCache.delete(key);
    }

    while (jobs.size > MAX_JOBS) {
        const key =
            jobs.keys()
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

/*
|--------------------------------------------------------------------------
| SRT
|--------------------------------------------------------------------------
*/

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

/*
|--------------------------------------------------------------------------
| HIGIENIZAÇÃO CONSERVADORA DE CARACTERES
|--------------------------------------------------------------------------
*/

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
        const key of Object.keys(target)
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

    if (normalizedNfc !== text) {
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
        /[\u00AD\u200B\u2060\uFEFF]/gu,
        () => {
            stats.invisibleChars++;
            return "";
        }
    );

    text = text.replace(
        /[\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu,
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

    text =
        text
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
    const total =
        characterSanitizationTotal(stats);

    console.log(
        `[CLEAN CHAR] ${stage}: ${total} ajuste(s); controles=${stats.controlChars}, invisíveis=${stats.invisibleChars}, espaços=${stats.normalizedSpaces}, replacement=${stats.replacementChars}, ASS=${stats.assTags}, NFC=${stats.nfcChanges}.`
    );
}

/*
|--------------------------------------------------------------------------
| LIMPEZA SDH / CC
|--------------------------------------------------------------------------
*/

const SDH_CUE_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

/*
|--------------------------------------------------------------------------
| CONTEXTO OCULTO DE FALANTE
|--------------------------------------------------------------------------
*/

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
        speaker.length > 60
    ) {
        return "";
    }

    if (SDH_CUE_WORDS.test(speaker)) {
        return "";
    }

    if (/[!?;]/u.test(speaker)) {
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

    const hiddenMatch =
        original.match(
            SPEAKER_HINT_MARKER_REGEX
        );

    if (hiddenMatch) {
        return {
            speaker:
                decodeSpeakerHint(
                    hiddenMatch[1]
                ),

            lineForCleaning:
                original.replace(
                    SPEAKER_HINT_MARKER_REGEX,
                    ""
                )
        };
    }

    const bracketMatch =
        original.match(
            /^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*/u
        );

    if (bracketMatch) {
        const speaker =
            normalizeSpeakerHint(
                bracketMatch[1]
            );

        if (speaker) {
            return {
                speaker,
                lineForCleaning:
                    original
            };
        }
    }

    const colonMatch =
        original.match(
            /^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50}):\s+(?=\S)/u
        );

    if (colonMatch) {
        const speaker =
            normalizeSpeakerHint(
                colonMatch[1]
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

/*
|--------------------------------------------------------------------------
| NORMALIZAÇÃO DE ALONGAMENTOS VOCAIS
|--------------------------------------------------------------------------
*/

function normalizeHyphenatedVocalElongations(text) {
    return String(text ?? "").replace(
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
        String(line || "").trim();

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

    text =
        text
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

/*
|--------------------------------------------------------------------------
| AUDITORIA ABSOLUTA DE TIMESTAMPS
|--------------------------------------------------------------------------
*/

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
                TIMING_LINE_REGEX.test(line)
        );
}

function timingSignature(timings) {
    return sha256(
        JSON.stringify(timings)
    ).slice(0, 16);
}

function auditCleaningTimestamps(
    rawSrt,
    cleanedSrt,
    label = "SOURCE"
) {
    const rawTimings =
        extractTimingLines(rawSrt);

    const cleanedTimings =
        extractTimingLines(cleanedSrt);

    let cursor = 0;

    for (
        let i = 0;
        i < cleanedTimings.length;
        i++
    ) {
        const wanted =
            cleanedTimings[i];

        while (
            cursor < rawTimings.length &&
            rawTimings[cursor] !== wanted
        ) {
            cursor++;
        }

        if (
            cursor >= rawTimings.length
        ) {
            throw timestampIntegrityError(
                `Auditoria de timestamps falhou na limpeza (${label}) no bloco preservado ${i + 1}.`
            );
        }

        cursor++;
    }

    console.log(
        `[AUDIT TIMESTAMP] ${label} RAW->CLEAN: OK — ${cleanedTimings.length}/${rawTimings.length} timing(s) preservado(s) exatamente; removidos=${rawTimings.length - cleanedTimings.length}; assinatura=${timingSignature(cleanedTimings)}.`
    );

    return true;
}

function auditFinalTimestamps(
    sourceSrt,
    finalSrt,
    label = "FINAL"
) {
    const sourceBlocks =
        parseSrt(sourceSrt);

    const finalBlocks =
        parseSrt(finalSrt);

    if (
        sourceBlocks.length !==
        finalBlocks.length
    ) {
        throw timestampIntegrityError(
            `Auditoria de timestamps falhou (${label}): fonte=${sourceBlocks.length} blocos, final=${finalBlocks.length} blocos.`
        );
    }

    for (
        let i = 0;
        i < sourceBlocks.length;
        i++
    ) {
        const source =
            sourceBlocks[i];

        const final =
            finalBlocks[i];

        if (
            source.index !==
                final.index ||
            source.timing !==
                final.timing
        ) {
            throw timestampIntegrityError(
                `Auditoria de timestamps falhou (${label}) no bloco ${i + 1}: índice/timing divergente.`
            );
        }
    }

    const signature =
        timingSignature(
            sourceBlocks.map(
                block => [
                    block.index,
                    block.timing
                ]
            )
        );

    console.log(
        `[AUDIT TIMESTAMP] ${label}: OK — ${sourceBlocks.length}/${sourceBlocks.length} bloco(s), 0 alteração(ões), assinatura=${signature}.`
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

        if (timingIndex === -1) {
            continue;
        }

        const timing =
            lines[timingIndex].trim();

        const cleanedDialogue = [];
        const speakerHints =
            new Set();

        for (
            const line
            of lines.slice(timingIndex + 1)
        ) {
            const speakerInfo =
                extractSpeakerHint(line);

            if (speakerInfo.speaker) {
                speakerHints.add(
                    speakerInfo.speaker
                );
            }

            const cleanedBeforeCharacters =
                cleanDialogueLine(
                    speakerInfo.lineForCleaning
                );

            const sanitized =
                sanitizeSubtitleText(
                    cleanedBeforeCharacters
                );

            addCharacterSanitizationStats(
                characterStats,
                sanitized.stats
            );

            const cleanedBeforeElongation =
                sanitized.text;

            const cleaned =
                normalizeHyphenatedVocalElongations(
                    cleanedBeforeElongation
                );

            if (
                cleaned !==
                cleanedBeforeElongation
            ) {
                elongatedLines++;
            }

            if (
                cleaned !== line.trim()
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
            cleanedDialogue.length === 0
        ) {
            removedBlocks++;
            continue;
        }

        if (speakerHints.size === 1) {
            const speakerHint =
                Array.from(
                    speakerHints
                )[0];

            cleanedDialogue[0] =
                `@@SPK:${encodeSpeakerHint(
                    speakerHint
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
                ) => [
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

/*
|--------------------------------------------------------------------------
| PARSER SRT
|--------------------------------------------------------------------------
*/

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

        if (lines.length < 3) {
            continue;
        }

        const indexLine =
            lines[0].trim();

        const timingLine =
            lines[1].trim();

        if (!/^\d+$/.test(indexLine)) {
            continue;
        }

        if (
            !TIMING_LINE_REGEX.test(
                timingLine
            )
        ) {
            continue;
        }

        const textLines =
            lines.slice(2);

        let speakerHint = "";

        if (textLines.length > 0) {
            const markerMatch =
                textLines[0].match(
                    SPEAKER_HINT_MARKER_REGEX
                );

            if (markerMatch) {
                speakerHint =
                    decodeSpeakerHint(
                        markerMatch[1]
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

/*
|--------------------------------------------------------------------------
| LIMPEZA DE CARACTERES APÓS TRADUÇÃO
|--------------------------------------------------------------------------
*/

function cleanAllTranslatedCharacters(
    translatedTexts
) {
    const stats =
        createCharacterSanitizationStats();

    let changedBlocks = 0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(text ?? "");

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
                    changedBlocks++;
                }

                return sanitized.text;
            }
        );

    logCharacterSanitization(
        `TRADUÇÃO (${changedBlocks} bloco(s) alterado(s))`,
        stats
    );

    return cleaned;
}

/*
|--------------------------------------------------------------------------
| LIMPEZA DE MARCADORES DE DIÁLOGO TRADUZIDOS
|--------------------------------------------------------------------------
*/

function cleanTranslatedDialogueMarkers(text) {
    const normalized =
        String(text ?? "")
            .replace(/\r\n/g, "\n")
            .replace(/\r/g, "\n");

    const lines =
        normalized.split("\n");

    const markerRegex =
        /^\s*[-–—/]+\s+(?=\S)/u;

    let markedDialogueLines = 0;
    let nonEmptyLines = 0;

    for (const line of lines) {
        if (line.trim()) {
            nonEmptyLines++;
        }

        if (markerRegex.test(line)) {
            markedDialogueLines++;
        }
    }

    const isRealMultiSpeakerBlock =
        nonEmptyLines >= 2 &&
        markedDialogueLines >= 2;

    if (isRealMultiSpeakerBlock) {
        return normalized;
    }

    return lines
        .map(
            line =>
                line.replace(
                    /^\s*[-–—/]+\s+(?=\S)/u,
                    ""
                )
        )
        .join("\n");
}

function cleanAllTranslatedDialogueMarkers(
    translatedTexts
) {
    let changedBlocks = 0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(text ?? "");

                const result =
                    cleanTranslatedDialogueMarkers(
                        original
                    );

                if (result !== original) {
                    changedBlocks++;
                }

                return result;
            }
        );

    console.log(
        `[CLEAN] Marcadores de diálogo: ${changedBlocks} bloco(s) ajustado(s).`
    );

    return cleaned;
}

function cleanAllTranslatedVocalElongations(
    translatedTexts
) {
    let changedBlocks = 0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(text ?? "");

                const result =
                    normalizeTranslatedVocalElongations(
                        original
                    );

                if (result !== original) {
                    changedBlocks++;
                }

                return result;
            }
        );

    console.log(
        `[CLEAN] Alongamentos vocais traduzidos: ${changedBlocks} bloco(s) ajustado(s).`
    );

    return cleaned;
}

/*
|--------------------------------------------------------------------------
| CONSTRUTOR SRT
|--------------------------------------------------------------------------
*/

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
                ) => {
                    const translated =
                        translatedTexts[index] ??
                        block.text;

                    return [
                        block.index,
                        block.timing,
                        translated
                    ].join("\n");
                }
            )
            .join("\n\n")
            .trim() +
        "\n"
    );
}

/*
|--------------------------------------------------------------------------
| CACHE
|--------------------------------------------------------------------------
*/

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
                now + CACHE_TTL_MS
        }
    );

    cleanupMemory();
}

function getTranslationCache(key) {
    const item =
        translationCache.get(key);

    if (!item) {
        return null;
    }

    if (
        item.version !==
            TRANSLATION_CACHE_VERSION ||
        item.contentAuditPassed !== true
    ) {
        translationCache.delete(key);
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

/*
|--------------------------------------------------------------------------
| DIVISÃO INTELIGENTE DE LOTES
|--------------------------------------------------------------------------
*/

function splitIntoBatches(blocks) {
    const batches = [];

    let current = [];
    let currentChars = 0;

    for (const block of blocks) {
        const text =
            String(block.text || "");

        const blockChars =
            text.length + 28;

        const wouldExceedChars =
            current.length > 0 &&
            currentChars +
                blockChars >
                MAX_BATCH_CHARS;

        const wouldExceedBlocks =
            current.length >=
            MAX_BATCH_BLOCKS;

        if (
            wouldExceedChars ||
            wouldExceedBlocks
        ) {
            batches.push(current);

            current = [];
            currentChars = 0;
        }

        current.push(block);

        currentChars +=
            blockChars;
    }

    if (current.length > 0) {
        batches.push(current);
    }

    return batches;
}

/*
|--------------------------------------------------------------------------
| COOLDOWN / RETRY-AFTER / RATE LIMIT
|--------------------------------------------------------------------------
*/

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
                Number(ms) || 30000,
                1000
            ),
            MAX_RATE_LIMIT_COOLDOWN_MS
        );

    const until =
        Date.now() + safeMs;

    if (until > geminiCooldownUntil) {
        geminiCooldownUntil = until;
    }

    console.log(
        `[GEMINI] RATE LIMIT. Cooldown global de ${Math.ceil(
            safeMs / 1000
        )}s.`
    );
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
            Number.isFinite(seconds) &&
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

    const secondsMatch =
        message.match(
            /retry in\s+([\d.]+)s/i
        );

    if (secondsMatch) {
        const seconds =
            Number(secondsMatch[1]);

        if (Number.isFinite(seconds)) {
            return Math.min(
                (seconds + 1) * 1000,
                MAX_RATE_LIMIT_COOLDOWN_MS
            );
        }
    }

    const minuteSecondMatch =
        message.match(
            /retry in\s+(\d+)m\s*(\d+(?:\.\d+)?)?s?/i
        );

    if (minuteSecondMatch) {
        const minutes =
            Number(
                minuteSecondMatch[1]
            );

        const seconds =
            Number(
                minuteSecondMatch[2] || 0
            );

        return Math.min(
            (
                minutes * 60 +
                seconds +
                1
            ) * 1000,
            MAX_RATE_LIMIT_COOLDOWN_MS
        );
    }

    const minuteMatch =
        message.match(
            /retry in\s+(\d+)m/i
        );

    if (minuteMatch) {
        return Math.min(
            (
                Number(
                    minuteMatch[1]
                ) * 60 +
                1
            ) * 1000,
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
            String(message || "")
        )
    );
}/*
|--------------------------------------------------------------------------
| REQUEST GEMINI
|--------------------------------------------------------------------------
*/

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

    assertBeforeDeadline(deadlineAt);

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            GEMINI_MODEL
        )}:generateContent`;

    const isAudit =
        requestKind === "semantic-audit";

    const body = isAudit
        ? {
              systemInstruction: {
                  parts: [
                      {
                          text:
                              "Você é um auditor bilíngue rigoroso de integridade de legendas EN→PT-BR. " +
                              "NÃO traduza nem reescreva. Cada registro usa chaves compactas: i=id, l=lock, s=source, t=translation, h=speaker, p=source anterior, n=source seguinte. " +
                              "Os registros podem ser esparsos: NUNCA use a posição no array como vizinhança; use somente p/n do próprio registro. " +
                              "t deve corresponder exclusivamente a s do mesmo i. Conteúdo antecipado de n ou atrasado de p é falha. " +
                              "Aceite paráfrase natural, localização idiomática, humor, gíria contemporânea e linguagem LGBTQIAPN+/drag/camp/shade quando preservarem o sentido do próprio ID. " +
                              "Fragmentos podem permanecer fragmentários. Se t corresponde ao próprio s e está semanticamente correta, m=i e f=true. " +
                              "Se t pertence inequivocamente a outro ID, m aponta para esse ID e f=false. Se t permanece no próprio ID mas contém erro semântico relevante, m=i e f=false. " +
                              "Preserve i e l exatamente. Retorne somente JSON compacto."
                      }
                  ]
              },

              contents: [
                  {
                      role: "user",

                      parts: [
                          {
                              text: prompt
                          }
                      ]
                  }
              ],

              generationConfig: {
                  thinkingConfig: {
                      thinkingLevel:
                          "MINIMAL"
                  },

                  responseMimeType:
                      "application/json",

                  responseSchema: {
                      type: "ARRAY",

                      items: {
                          type: "OBJECT",

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

                      minItems: 1
                  },

                  maxOutputTokens:
                      AUDIT_MAX_OUTPUT_TOKENS
              }
          }
        : {
              systemInstruction: {
                  parts: [
                      {
                          text:
                              "Você é um tradutor, localizador e adaptador profissional de legendas para Português do Brasil, com padrão premium de streaming. " +
                              "A prioridade é: INTEGRIDADE DO CONTEÚDO POR ID → sentido/intenção → voz do personagem → naturalidade contemporânea → humor/ritmo → literalidade. " +
                              "Cada ID é uma unidade atômica e imutável: traduza SOMENTE o texto daquele ID. Nunca antecipe, atrase, complete ou transfira conteúdo entre IDs, mesmo quando uma frase continuar no bloco seguinte. Vizinhos existem só para contexto. " +
                              "O PT-BR deve soar vivo, oral, atual e brasileiro de verdade. Prefira vocabulário contemporâneo compatível com 2026 e, quando idade/cena/personagem pedirem, use naturalmente repertório de geração Z e Alpha, cultura digital, memes e oralidade jovem — sem transformar todo mundo em adolescente nem forçar gíria. " +
                              "Evite vocabulário artificialmente datado. Modernidade vem de ritmo, escolhas de palavras e registro. " +
                              "Em contextos LGBTQIAPN+, queer, drag, ballroom, camp, shade e cultura pop, traduza com letramento cultural e carinho: preserve identidade, nome escolhido, pronome, gênero, ironia, afeto, provocação, termos ressignificados e intensidade. Não heteronormativize nem higienize linguagem queer. " +
                              "girl, bitch, honey, sis, queen, baby e babe podem ser vocativos afetivos/camp; nunca traduza automaticamente girl como garota nem bitch como vadia. Escolha gata, amiga, mana, bicha, querida, amor, mona ou omissão somente quando o contexto sustentar. 'vadia' apenas quando houver insulto real ou uso ressignificado inequivocamente equivalente. " +
                              "Adapte expressões idiomáticas, piadas, trocadilhos, shade e referências culturais quando houver solução brasileira natural que preserve intenção e graça. Se adaptar piorar, preserve o original. Preserve bordões consagrados como Condragulations. " +
                              "Não censure palavrões nem intensidade emocional. Preserve nomes próprios, marcas, títulos e termos técnicos. Traduza falas ou letras efetivamente transcritas em outros idiomas para PT-BR; não invente letra para marcações de música. " +
                              "speaker é contexto oculto: nunca copie o nome para a tradução. Se identificar claramente a pessoa, respeite gênero/pronomes; se não estiver claro, NÃO chute gênero — prefira construção brasileira natural que evite marcação. " +
                              "Não acrescente SDH/CC, sons, nomes de falantes ou explicações. Não reproduza alongamentos gráficos desnecessários. Preserve formatação útil. " +
                              "Antes de responder, confira silenciosamente cada ID para garantir que nenhum conteúdo do vizinho migrou para ele. " +
                              "A entrada usa chaves compactas: i=id, l=lock, x=texto e s=speaker opcional; p/n são contexto externo. " +
                              "Retorne somente JSON compacto, exatamente na ordem recebida, usando i=id, l=lock e t=tradução. Preserve i e l sem qualquer alteração."
                      }
                  ]
              },

              contents: [
                  {
                      role: "user",

                      parts: [
                          {
                              text: prompt
                          }
                      ]
                  }
              ],

              generationConfig: {
                  thinkingConfig: {
                      thinkingLevel:
                          "MINIMAL"
                  },

                  responseMimeType:
                      "application/json",

                  responseSchema: {
                      type: "ARRAY",

                      items: {
                          type: "OBJECT",

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

                      minItems: 1
                  },

                  maxOutputTokens:
                      MAX_OUTPUT_TOKENS
              }
          };

    const remaining =
        remainingBeforeDeadline(
            deadlineAt
        );

    if (remaining <= 0) {
        throw translationTimeoutError();
    }

    let kindTimeoutMs =
        TRANSLATION_REQUEST_TIMEOUT_MS;

    if (
        requestKind ===
        "semantic-audit"
    ) {
        kindTimeoutMs =
            SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-structure-retry"
    ) {
        kindTimeoutMs =
            STRUCTURE_RETRY_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-rescue"
    ) {
        kindTimeoutMs =
            RESCUE_TRANSLATION_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-repair"
    ) {
        kindTimeoutMs =
            REPAIR_TRANSLATION_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-single"
    ) {
        kindTimeoutMs =
            SINGLE_TRANSLATION_REQUEST_TIMEOUT_MS;

    } else if (
        requestKind ===
        "translation-single-last"
    ) {
        kindTimeoutMs =
            SINGLE_LAST_TRANSLATION_REQUEST_TIMEOUT_MS;
    }

    const requestTimeoutMs =
        Math.max(
            1,

            Math.min(
                kindTimeoutMs,

                Number.isFinite(remaining)
                    ? remaining
                    : kindTimeoutMs
            )
        );

    const controller =
        new AbortController();

    let requestTimedOut = false;

    const timer =
        setTimeout(
            () => {
                requestTimedOut = true;
                controller.abort();
            },

            requestTimeoutMs
        );

    let response;
    let rawText;

    try {
        response =
            await fetch(
                endpoint,

                {
                    method: "POST",

                    headers: {
                        "Content-Type":
                            "application/json",

                        "x-goog-api-key":
                            GEMINI_API_KEY
                    },

                    body:
                        JSON.stringify(body),

                    signal:
                        controller.signal
                }
            );

        rawText =
            await response.text();

    } catch (error) {
        if (
            Number.isFinite(deadlineAt) &&
            Date.now() >= deadlineAt
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

    assertBeforeDeadline(deadlineAt);

    let data;

    try {
        data =
            JSON.parse(rawText);

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

/*
|--------------------------------------------------------------------------
| FILA GEMINI
|--------------------------------------------------------------------------
*/

function enqueueGemini(
    prompt,
    deadlineAt,
    requestKind = "translation"
) {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            if (
                Number.isFinite(deadlineAt) &&
                Date.now() >= deadlineAt
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
                resolve,
                reject
            });

            processGeminiQueue();
        }
    );
}

async function processGeminiQueue() {
    if (geminiWorkerRunning) {
        return;
    }

    geminiWorkerRunning = true;

    try {
        while (
            geminiQueue.length > 0
        ) {
            const item =
                geminiQueue.shift();

            if (!item) {
                continue;
            }

            if (
                Number.isFinite(
                    item.deadlineAt
                ) &&
                Date.now() >=
                    item.deadlineAt
            ) {
                item.reject(
                    translationTimeoutError()
                );

                continue;
            }

            let normalAttempt = 1;
            let finished = false;

            while (!finished) {
                try {
                    assertBeforeDeadline(
                        item.deadlineAt
                    );

                    const cooldown =
                        getCooldownRemaining();

                    if (cooldown > 0) {
                        console.log(
                            `[GEMINI] Fila aguardando cooldown de ${Math.ceil(
                                cooldown / 1000
                            )}s.`
                        );

                        await sleepWithDeadline(
                            cooldown,
                            item.deadlineAt
                        );
                    }

                    const sinceLast =
                        Date.now() -
                        lastGeminiRequestAt;

                    if (
                        lastGeminiRequestAt > 0 &&
                        sinceLast <
                            MIN_REQUEST_INTERVAL_MS
                    ) {
                        const wait =
                            MIN_REQUEST_INTERVAL_MS -
                            sinceLast;

                        await sleepWithDeadline(
                            wait,
                            item.deadlineAt
                        );
                    }

                    assertBeforeDeadline(
                        item.deadlineAt
                    );

                    console.log(
                        `[GEMINI] Request ${normalAttempt}/${MAX_NORMAL_RETRIES + 1} (${item.requestKind || "translation"})`
                    );

                    lastGeminiRequestAt =
                        Date.now();

                    const result =
                        await rawGeminiRequest(
                            item.prompt,
                            item.deadlineAt,
                            item.requestKind ||
                            "translation"
                        );

                    item.resolve(result);
                    finished = true;

                } catch (error) {
                    if (
                        error?.code ===
                            "TRANSLATION_TIMEOUT" ||
                        (
                            Number.isFinite(
                                item.deadlineAt
                            ) &&
                            Date.now() >=
                                item.deadlineAt
                        )
                    ) {
                        item.reject(
                            translationTimeoutError()
                        );

                        finished = true;
                        continue;
                    }

                    if (
                        error?.code ===
                        "GEMINI_REQUEST_TIMEOUT"
                    ) {
                        console.warn(
                            `[GEMINI] Timeout curto (${item.requestKind || "translation"}); request será tratado pelo fluxo FAST/RESCUE 6.3.`
                        );

                        item.reject(error);

                        finished = true;
                        continue;
                    }

                    const message =
                        getErrorMessage(error);

                    console.error(
                        `[GEMINI] Erro: ${message}`
                    );

                    if (error?.rateLimit) {
                        const cooldownMs =
                            Math.min(
                                error.retryAfterMs ||
                                30000,

                                MAX_RATE_LIMIT_COOLDOWN_MS
                            );

                        setGeminiCooldown(
                            cooldownMs
                        );

                        continue;
                    }

                    if (
                        normalAttempt <=
                        MAX_NORMAL_RETRIES
                    ) {
                        const wait =
                            1500 *
                            normalAttempt;

                        console.log(
                            `[GEMINI] Retry normal em ${Math.ceil(
                                wait / 1000
                            )}s.`
                        );

                        normalAttempt++;

                        try {
                            await sleepWithDeadline(
                                wait,
                                item.deadlineAt
                            );

                        } catch (
                            timeoutError
                        ) {
                            item.reject(
                                timeoutError
                            );

                            finished = true;
                        }

                        continue;
                    }

                    item.reject(error);
                    finished = true;
                }
            }
        }

    } finally {
        geminiWorkerRunning = false;

        if (geminiQueue.length > 0) {
            processGeminiQueue();
        }
    }
}

/*
|--------------------------------------------------------------------------
| PROMPT DE TRADUÇÃO
|--------------------------------------------------------------------------
*/

function blockTranslationLock(block) {
    return sha256(
        JSON.stringify([
            BLOCK_LOCK_VERSION,
            Number(block?.index),
            String(block?.timing ?? ""),
            String(block?.text ?? ""),
            String(block?.speakerHint ?? "")
        ])
    ).slice(0, 20);
}

function contextBlockPayload(block) {
    if (!block) {
        return null;
    }

    return {
        i: block.index,
        x: block.text
    };
}

function buildTranslationPrompt(
    blocks,
    context = {},
    options = {}
) {
    const payload =
        blocks.map(
            block => {
                const item = {
                    i: block.index,

                    l:
                        blockTranslationLock(
                            block
                        ),

                    x: block.text
                };

                if (block.speakerHint) {
                    item.s =
                        block.speakerHint;
                }

                return item;
            }
        );

    let mode =
        "TRADUÇÃO NORMAL. Seja rigoroso com a separação semântica entre IDs.";

    if (options.strictStructure) {
        mode =
            "REPETIÇÃO ESTRUTURAL. A resposta anterior quebrou o contrato. Não omita, duplique, reordene ou altere nenhum i/l. Traduza todos os itens exatamente uma vez.";
    }

    if (options.repairMode) {
        mode =
            "REPARO SEMÂNTICO CIRÚRGICO. Traduza APENAS os IDs listados em b. Cada ID deve conter exclusivamente o sentido do próprio x. Não absorva fala anterior/posterior. Preserve fragmentos quando a frase continua fora da janela.";
    }

    return `${mode}
Siga integralmente as regras premium do sistema.
p e n são SOMENTE contexto externo e nunca devem ser absorvidos em b.
SAÍDA EXATA: [{"i":123,"l":"abc","t":"tradução"}]
ENTRADA: ${JSON.stringify({
        p:
            contextBlockPayload(
                context.before
            ),

        b:
            payload,

        n:
            contextBlockPayload(
                context.after
            )
    })}`;
}

/*
|--------------------------------------------------------------------------
| 6.3 FAST-FIRST / SAFE-FINAL
|--------------------------------------------------------------------------
*/

function badModelOutputError(message) {
    const error =
        new Error(message);

    error.code =
        "BAD_MODEL_OUTPUT";

    return error;
}

function badAuditOutputError(message) {
    const error =
        new Error(message);

    error.code =
        "BAD_AUDIT_OUTPUT";

    return error;
}

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

function strongSentenceEnd(text) {
    return /[.!?…]["'”’\)\]]*$/u.test(
        normalizedRiskText(text)
    );
}

function strictFragmentTail(text) {
    const value =
        normalizedRiskText(text);

    return (
        /[,;:–—-]\s*$/u.test(value) ||
        /\b(?:and|but|or|because|so|to|of|for|with|that|which|who|when|if|than|as)\s*$/iu.test(
            value
        )
    );
}

function startsLikeContinuation(text) {
    const value =
        normalizedRiskText(text);

    return (
        /^[a-z]/u.test(value) ||
        /^(?:and|but|or|so|because|then|that|which|who|when|if|to|of|for|with)\b/iu.test(
            value
        )
    );
}

function strongBoundaryRisk(
    leftBlock,
    rightBlock
) {
    if (!leftBlock || !rightBlock) {
        return false;
    }

    const left =
        normalizedRiskText(
            leftBlock.text
        );

    const right =
        normalizedRiskText(
            rightBlock.text
        );

    if (!left || !right) {
        return false;
    }

    if (strictFragmentTail(left)) {
        return true;
    }

    if (
        !strongSentenceEnd(left) &&
        left.length <= 45 &&
        startsLikeContinuation(right)
    ) {
        return true;
    }

    return false;
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
        sourceLength < 18 ||
        translatedLength === 0
    ) {
        return false;
    }

    const ratio =
        translatedLength /
        sourceLength;

    return (
        ratio < 0.28 ||
        ratio > 3.8
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

    if (current.length < 14) {
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

function selectFinalAuditIndices(
    blocks,
    translatedTexts,
    batchBoundaryStarts
) {
    const selected =
        new Set();

    const risk =
        new Set();

    const boundary =
        new Set();

    const canary =
        new Set();

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
            selected.add(i);
            selected.add(i + 1);

            risk.add(i);
            risk.add(i + 1);
        }
    }

    for (
        const startIndex
        of batchBoundaryStarts
    ) {
        if (startIndex > 0) {
            selected.add(
                startIndex - 1
            );

            boundary.add(
                startIndex - 1
            );
        }

        if (
            startIndex <
            blocks.length
        ) {
            selected.add(startIndex);
            boundary.add(startIndex);
        }
    }

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
            selected.add(i);
            risk.add(i);
        }
    }

    for (
        let i = 0;
        i < blocks.length;
        i++
    ) {
        if (
            i === 0 ||
            i === blocks.length - 1 ||
            i % AUDIT_CANARY_STRIDE === 0
        ) {
            selected.add(i);
            canary.add(i);
        }
    }

    return {
        indices:
            Array.from(selected)
                .sort(
                    (a, b) =>
                        a - b
                ),

        riskCount:
            risk.size,

        boundaryCount:
            boundary.size,

        canaryCount:
            canary.size
    };
}

function buildIndependentAuditRecord(
    blocks,
    translatedTexts,
    index
) {
    const block =
        blocks[index];

    const previous =
        index > 0
            ? blocks[index - 1]
            : null;

    const next =
        index <
        blocks.length - 1
            ? blocks[index + 1]
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

    if (record.speaker) {
        item.h =
            record.speaker;
    }

    if (record.prev) {
        item.p = [
            record.prev.id,
            record.prev.source
        ];
    }

    if (record.next) {
        item.n = [
            record.next.id,
            record.next.source
        ];
    }

    return item;
}

function buildSemanticAuditPrompt(records) {
    return `AUDITORIA FINAL FAST-SAFE.
Os registros podem ser esparsos. Use SOMENTE p/n do próprio registro como vizinhos.
Falha de migração = t contém sentido pertencente a p/n. Falha semântica própria = t permanece no próprio i, mas não preserva adequadamente s.
Aceite localização natural, humor, gíria e linguagem LGBTQIAPN+/drag/camp/shade quando o sentido do próprio i estiver preservado.
SAÍDA EXATA: [{"i":123,"l":"abc","m":123,"f":true}]
REGISTROS: ${JSON.stringify(
        records.map(
            auditRecordForModel
        )
    )}`;
}

async function auditIndependentRecordsOnce(
    records,
    deadlineAt
) {
    const raw =
        await enqueueGemini(
            buildSemanticAuditPrompt(
                records
            ),

            deadlineAt,

            "semantic-audit"
        );

    let parsed;

    try {
        parsed =
            JSON.parse(
                stripCodeFences(raw)
            );

    } catch {
        throw badAuditOutputError(
            "Auditoria independente retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(parsed) ||
        parsed.length !==
            records.length
    ) {
        throw badAuditOutputError(
            `Auditoria independente incompleta: esperado=${records.length}, recebido=${Array.isArray(parsed) ? parsed.length : 0}.`
        );
    }

    const failures = [];

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
            item.i !== expected.id ||
            item.l !== expected.lock ||
            !Number.isInteger(
                item.m
            ) ||
            typeof item.f !==
                "boolean"
        ) {
            throw badAuditOutputError(
                `Auditoria independente quebrou o contrato no ID ${expected.id}.`
            );
        }

        if (
            item.f !== true ||
            item.m !== expected.id
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

    if (
        failures.length === 0
    ) {
        console.log(
            `[AUDIT FINAL] OK — ${records.length}/${records.length} bloco(s).`
        );

    } else {
        const sample =
            failures
                .slice(0, 8)
                .map(
                    item =>
                        `${item.id}->${item.matchedSourceId}`
                )
                .join(", ");

        console.warn(
            `[AUDIT FINAL] ${failures.length}/${records.length} suspeito(s): ${sample}.`
        );
    }

    return failures;
}async function auditIndependentRecords(
    records,
    deadlineAt,
    splitDepth = 0,
    singleBlockRetry = 0
) {
    assertBeforeDeadline(deadlineAt);

    try {
        return await auditIndependentRecordsOnce(
            records,
            deadlineAt
        );

    } catch (error) {
        const recoverable =
            error?.code ===
                "BAD_AUDIT_OUTPUT" ||
            error?.code ===
                "GEMINI_REQUEST_TIMEOUT";

        if (!recoverable) {
            throw error;
        }

        assertBeforeDeadline(deadlineAt);

        if (records.length === 1) {
            if (
                singleBlockRetry <
                MAX_SINGLE_BLOCK_AUDIT_RETRIES
            ) {
                console.warn(
                    "[AUDIT FINAL] Retry único de auditoria atômica."
                );

                return auditIndependentRecords(
                    records,
                    deadlineAt,
                    splitDepth,
                    singleBlockRetry + 1
                );
            }

            throw error;
        }

        if (
            splitDepth >=
            MAX_AUDIT_SPLIT_DEPTH
        ) {
            throw error;
        }

        const middle =
            Math.ceil(
                records.length / 2
            );

        const left =
            records.slice(
                0,
                middle
            );

        const right =
            records.slice(
                middle
            );

        console.warn(
            `[AUDIT FINAL] Request lenta/inválida (${records.length}); split curto ${left.length} + ${right.length}.`
        );

        const leftFailures =
            await auditIndependentRecords(
                left,
                deadlineAt,
                splitDepth + 1,
                0
            );

        const rightFailures =
            await auditIndependentRecords(
                right,
                deadlineAt,
                splitDepth + 1,
                0
            );

        return [
            ...leftFailures,
            ...rightFailures
        ];
    }
}

async function runFinalAudit(
    records,
    deadlineAt
) {
    const failures = [];
    let requests = 0;

    for (
        let offset = 0;
        offset < records.length;
        offset +=
            MAX_INDEPENDENT_AUDIT_BLOCKS
    ) {
        const chunk =
            records.slice(
                offset,
                offset +
                    MAX_INDEPENDENT_AUDIT_BLOCKS
            );

        requests++;

        const chunkFailures =
            await auditIndependentRecords(
                chunk,
                deadlineAt
            );

        failures.push(
            ...chunkFailures
        );
    }

    return {
        failures,
        requests
    };
}

function contextForGlobalWindow(
    blocks,
    startIndex,
    endIndexInclusive
) {
    return {
        before:
            startIndex > 0
                ? blocks[startIndex - 1]
                : null,

        after:
            endIndexInclusive <
            blocks.length - 1
                ? blocks[
                      endIndexInclusive + 1
                  ]
                : null
    };
}

async function translateBatchOnce(
    blocks,
    deadlineAt,
    context = {},
    options = {}
) {
    assertBeforeDeadline(deadlineAt);

    const raw =
        await enqueueGemini(
            buildTranslationPrompt(
                blocks,
                context,
                options
            ),

            deadlineAt,

            options.requestKind ||
            "translation"
        );

    let parsed;

    try {
        parsed =
            JSON.parse(
                stripCodeFences(raw)
            );

    } catch {
        throw badModelOutputError(
            "Gemini retornou JSON inválido."
        );
    }

    if (!Array.isArray(parsed)) {
        throw badModelOutputError(
            "Gemini não retornou uma lista."
        );
    }

    if (
        parsed.length !==
        blocks.length
    ) {
        throw badModelOutputError(
            `Quantidade incorreta de blocos: esperado=${blocks.length}, recebido=${parsed.length}. Resposta inteira descartada.`
        );
    }

    const texts = [];
    const seenIds =
        new Set();

    for (
        let i = 0;
        i < blocks.length;
        i++
    ) {
        const block =
            blocks[i];

        const item =
            parsed[i];

        const expectedLock =
            blockTranslationLock(
                block
            );

        if (
            !item ||
            !Number.isInteger(item.i) ||
            item.i !== block.index ||
            seenIds.has(item.i) ||
            typeof item.l !==
                "string" ||
            item.l !== expectedLock ||
            typeof item.t !==
                "string" ||
            item.t.trim().length === 0
        ) {
            throw badModelOutputError(
                `Contrato compacto ID/lock inválido na posição ${i + 1}; esperado id=${block.index}, lock=${expectedLock}.`
            );
        }

        seenIds.add(item.i);
        texts.push(item.t);
    }

    console.log(
        `[AUDIT ID] OK — ${blocks.length}/${blocks.length} bloco(s); ordem, IDs e locks preservados exatamente.`
    );

    return texts;
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
            current.length > 0 &&
            (
                current.length >=
                    RESCUE_BATCH_BLOCKS ||
                chars + size >
                    RESCUE_BATCH_CHARS
            )
        ) {
            result.push(current);

            current = [];
            chars = 0;
        }

        current.push(block);
        chars += size;
    }

    if (current.length) {
        result.push(current);
    }

    return result;
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
                ? blocks[start - 1]
                : outerContext?.before ||
                  null,

        after:
            endExclusive <
            blocks.length
                ? blocks[endExclusive]
                : outerContext?.after ||
                  null
    };
}

async function translateSingleLastChance(
    block,
    deadlineAt,
    context,
    repairMode = false
) {
    try {
        return await translateBatchOnce(
            [block],

            deadlineAt,

            context,

            {
                requestKind:
                    "translation-single",

                repairMode
            }
        );

    } catch (error) {
        const recoverable =
            error?.code ===
                "BAD_MODEL_OUTPUT" ||
            error?.code ===
                "GEMINI_REQUEST_TIMEOUT";

        if (!recoverable) {
            throw error;
        }

        assertBeforeDeadline(deadlineAt);

        console.warn(
            `[RESCUE 6.3] ID ${block.index}: última tentativa atômica.`
        );

        return translateBatchOnce(
            [block],

            deadlineAt,

            context,

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
    splitDepth = 0
) {
    try {
        return await translateBatchOnce(
            blocks,

            deadlineAt,

            context,

            {
                requestKind:
                    "translation-rescue",

                strictStructure:
                    true
            }
        );

    } catch (error) {
        const recoverable =
            error?.code ===
                "BAD_MODEL_OUTPUT" ||
            error?.code ===
                "GEMINI_REQUEST_TIMEOUT";

        if (!recoverable) {
            throw error;
        }

        assertBeforeDeadline(deadlineAt);

        if (blocks.length === 1) {
            return translateSingleLastChance(
                blocks[0],
                deadlineAt,
                context,
                false
            );
        }

        if (
            splitDepth >=
            MAX_RESCUE_SPLIT_DEPTH
        ) {
            console.warn(
                `[RESCUE 6.3] Micro-lote ${blocks.length} ainda falhou; indo para IDs atômicos sem nova árvore.`
            );

            const texts = [];

            for (
                let i = 0;
                i < blocks.length;
                i++
            ) {
                const singleContext =
                    contextForLocalSlice(
                        blocks,
                        i,
                        i + 1,
                        context
                    );

                const single =
                    await translateSingleLastChance(
                        blocks[i],
                        deadlineAt,
                        singleContext,
                        false
                    );

                texts.push(...single);
            }

            return texts;
        }

        const middle =
            Math.ceil(
                blocks.length / 2
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
            `[RESCUE 6.3] Micro-lote ${blocks.length} falhou; split único ${left.length} + ${right.length}.`
        );

        const leftTexts =
            await translateRescueChunk(
                left,

                deadlineAt,

                contextForLocalSlice(
                    blocks,
                    0,
                    middle,
                    context
                ),

                splitDepth + 1
            );

        const rightTexts =
            await translateRescueChunk(
                right,

                deadlineAt,

                contextForLocalSlice(
                    blocks,
                    middle,
                    blocks.length,
                    context
                ),

                splitDepth + 1
            );

        return [
            ...leftTexts,
            ...rightTexts
        ];
    }
}

async function translateBatchRescue(
    blocks,
    deadlineAt,
    context
) {
    const rescueBatches =
        splitFixedRescueBatches(
            blocks
        );

    console.warn(
        `[RESCUE 6.3] ${blocks.length} bloco(s) -> ${rescueBatches.length} micro-lote(s), máx ${RESCUE_BATCH_BLOCKS}/${RESCUE_BATCH_CHARS}.`
    );

    const texts = [];
    let cursor = 0;

    for (
        const chunk
        of rescueBatches
    ) {
        const start =
            cursor;

        const end =
            start + chunk.length;

        const chunkContext =
            contextForLocalSlice(
                blocks,
                start,
                end,
                context
            );

        const result =
            await translateRescueChunk(
                chunk,
                deadlineAt,
                chunkContext,
                0
            );

        texts.push(...result);

        cursor = end;
    }

    return texts;
}

async function translateMainBatch(
    blocks,
    deadlineAt,
    context
) {
    try {
        return await translateBatchOnce(
            blocks,

            deadlineAt,

            context,

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
                `[TRANSLATE] Lote ${blocks.length}: contrato inválido. Repetindo UMA vez o mesmo lote com contrato reforçado.`
            );

            try {
                return await translateBatchOnce(
                    blocks,

                    deadlineAt,

                    context,

                    {
                        requestKind:
                            "translation-structure-retry",

                        strictStructure:
                            true
                    }
                );

            } catch (retryError) {
                const recoverable =
                    retryError?.code ===
                        "BAD_MODEL_OUTPUT" ||
                    retryError?.code ===
                        "GEMINI_REQUEST_TIMEOUT";

                if (!recoverable) {
                    throw retryError;
                }

                console.warn(
                    "[TRANSLATE] Repetição estrutural falhou; entrando no RESCUE fixo 6.3."
                );

                return translateBatchRescue(
                    blocks,
                    deadlineAt,
                    context
                );
            }
        }

        if (
            error?.code ===
            "GEMINI_REQUEST_TIMEOUT"
        ) {
            console.warn(
                `[TRANSLATE] Lote ${blocks.length}: timeout normal. Indo direto ao RESCUE fixo 6.3.`
            );

            return translateBatchRescue(
                blocks,
                deadlineAt,
                context
            );
        }

        throw error;
    }
}

function writeTranslatedBatch(
    batch,
    translated,
    translatedTexts,
    originalPositions
) {
    for (
        let i = 0;
        i < batch.length;
        i++
    ) {
        const originalIndex =
            originalPositions.get(
                batch[i]
            );

        if (
            originalIndex !==
            undefined
        ) {
            translatedTexts[
                originalIndex
            ] =
                translated[i];
        }
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

    const suspectIndices =
        new Set();

    for (
        const failure
        of failures
    ) {
        const ownIndex =
            idToIndex.get(
                failure.id
            );

        if (
            Number.isInteger(
                ownIndex
            )
        ) {
            suspectIndices.add(
                ownIndex
            );
        }

        if (
            failure.matchedSourceId !==
            failure.id
        ) {
            const matchedIndex =
                idToIndex.get(
                    failure.matchedSourceId
                );

            if (
                Number.isInteger(
                    matchedIndex
                ) &&
                Number.isInteger(
                    ownIndex
                ) &&
                Math.abs(
                    matchedIndex -
                    ownIndex
                ) <= 2
            ) {
                suspectIndices.add(
                    matchedIndex
                );
            }
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

    const windows = [];
    let current = [];

    for (
        const index
        of sorted
    ) {
        if (
            current.length === 0 ||
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
            current.push(index);

        } else {
            windows.push(current);
            current = [index];
        }
    }

    if (current.length) {
        windows.push(current);
    }

    return windows;
}

async function auditSpecificIndices(
    blocks,
    translatedTexts,
    indices,
    deadlineAt
) {
    const records =
        indices.map(
            index =>
                buildIndependentAuditRecord(
                    blocks,
                    translatedTexts,
                    index
                )
        );

    return runFinalAudit(
        records,
        deadlineAt
    );
}

async function repairWindow(
    blocks,
    translatedTexts,
    indices,
    deadlineAt
) {
    const start =
        indices[0];

    const end =
        indices[
            indices.length - 1
        ];

    const repairBlocks =
        indices.map(
            index =>
                blocks[index]
        );

    const context =
        contextForGlobalWindow(
            blocks,
            start,
            end
        );

    console.warn(
        `[REPAIR 6.3] Janela IDs ${repairBlocks.map(
            block => block.index
        ).join(",")} (${repairBlocks.length} bloco(s)).`
    );

    let repaired;

    try {
        repaired =
            await translateBatchOnce(
                repairBlocks,

                deadlineAt,

                context,

                {
                    requestKind:
                        "translation-repair",

                    repairMode:
                        true,

                    strictStructure:
                        true
                }
            );

    } catch (error) {
        const recoverable =
            error?.code ===
                "BAD_MODEL_OUTPUT" ||
            error?.code ===
                "GEMINI_REQUEST_TIMEOUT";

        if (!recoverable) {
            throw error;
        }

        repaired = [];

        for (
            let i = 0;
            i < repairBlocks.length;
            i++
        ) {
            const globalIndex =
                indices[i];

            const singleContext =
                contextForGlobalWindow(
                    blocks,
                    globalIndex,
                    globalIndex
                );

            const single =
                await translateSingleLastChance(
                    repairBlocks[i],
                    deadlineAt,
                    singleContext,
                    true
                );

            repaired.push(
                ...single
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

    const check =
        await auditSpecificIndices(
            blocks,
            translatedTexts,
            indices,
            deadlineAt
        );

    if (
        check.failures.length === 0
    ) {
        console.log(
            "[REPAIR 6.3] Janela aprovada após reparo."
        );

        return;
    }

    console.warn(
        `[REPAIR 6.3] ${check.failures.length} ID(s) ainda suspeito(s); última correção atômica.`
    );

    for (
        const failure
        of check.failures
    ) {
        const index =
            blocks.findIndex(
                block =>
                    block.index ===
                    failure.id
            );

        if (index < 0) {
            throw badModelOutputError(
                `ID ${failure.id} não encontrado no reparo.`
            );
        }

        const singleContext =
            contextForGlobalWindow(
                blocks,
                index,
                index
            );

        const single =
            await translateSingleLastChance(
                blocks[index],
                deadlineAt,
                singleContext,
                true
            );

        translatedTexts[index] =
            single[0];

        const finalCheck =
            await auditSpecificIndices(
                blocks,
                translatedTexts,
                [index],
                deadlineAt
            );

        if (
            finalCheck.failures.length >
            0
        ) {
            throw badModelOutputError(
                `Reparo semântico fail-closed não conseguiu aprovar o ID ${failure.id}.`
            );
        }
    }
}

async function confirmOwnIdFailures(
    failures,
    blocks,
    translatedTexts,
    deadlineAt
) {
    const migrationFailures =
        failures.filter(
            item =>
                item.matchedSourceId !==
                item.id
        );

    const ownFailures =
        failures.filter(
            item =>
                item.matchedSourceId ===
                item.id
        );

    if (
        ownFailures.length === 0
    ) {
        return migrationFailures;
    }

    const ownIndices =
        ownFailures
            .map(
                item =>
                    blocks.findIndex(
                        block =>
                            block.index ===
                            item.id
                    )
            )
            .filter(
                index =>
                    index >= 0
            );

    console.log(
        `[AUDIT FINAL] ${ownIndices.length} suspeita(s) f=false/m=i serão rechecadas antes de qualquer reparo.`
    );

    const recheck =
        await auditSpecificIndices(
            blocks,
            translatedTexts,
            ownIndices,
            deadlineAt
        );

    return [
        ...migrationFailures,
        ...recheck.failures
    ];
}

async function translateSrt(
    originalSrt,
    job
) {
    const blocks =
        parseSrt(
            originalSrt
        );

    if (blocks.length === 0) {
        throw new Error(
            "Nenhum bloco SRT válido."
        );
    }

    const batches =
        splitIntoBatches(
            blocks
        );

    console.log(
        `[TRANSLATE] ${blocks.length} blocos.`
    );

    console.log(
        `[TRANSLATE] ${batches.length} lote(s).`
    );

    console.log(
        `[TRANSLATE] 6.3 FAST-FIRST: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} caracteres; SAFE-FINAL só depois da tradução completa.`
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
        ) => {
            originalPositions.set(
                block,
                index
            );
        }
    );

    const startedAt =
        Date.now();

    const deadlineAt =
        startedAt +
        MAX_TRANSLATION_TIME_MS;

    job.startedAt =
        startedAt;

    job.deadlineAt =
        deadlineAt;

    job.updatedAt =
        startedAt;

    console.log(
        `[JOB ${job.id}] Cronômetro iniciado: teto ${MAX_TRANSLATION_TIME_MS}ms; fila anterior não contabilizada.`
    );

    const batchBoundaryStarts = [];

    console.log(
        "[PHASE FAST] Iniciando tradução de todos os lotes; nenhuma auditoria Gemini interromperá esta fase."
    );

    for (
        let batchIndex = 0;
        batchIndex < batches.length;
        batchIndex++
    ) {
        assertBeforeDeadline(
            deadlineAt
        );

        const batch =
            batches[batchIndex];

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

        const batchChars =
            batch.reduce(
                (
                    total,
                    block
                ) =>
                    total +
                    String(
                        block.text ||
                        ""
                    ).length,

                0
            );

        const batchStartedAt =
            Date.now();

        console.log(
            `[PHASE FAST] Lote ${batchIndex + 1}/${batches.length} - ${batch.length} blocos / ${batchChars} caracteres.`
        );

        const translated =
            await translateMainBatch(
                batch,
                deadlineAt,
                context
            );

        writeTranslatedBatch(
            batch,
            translated,
            translatedTexts,
            originalPositions
        );

        job.progress =
            Math.round(
                (
                    (
                        batchIndex + 1
                    ) /
                    batches.length
                ) *
                82
            );

        job.completedBatches =
            batchIndex + 1;

        job.totalBatches =
            batches.length;

        job.updatedAt =
            Date.now();

        console.log(
            `[PHASE FAST] Lote ${batchIndex + 1}/${batches.length} OK em ${((Date.now() - batchStartedAt) / 1000).toFixed(1)}s.`
        );
    }

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
        `[PHASE FAST] Tradução completa em ${(fastElapsed / 1000).toFixed(1)}s. Iniciando SAFE-FINAL.`
    );

    assertBeforeDeadline(
        deadlineAt
    );

    const selection =
        selectFinalAuditIndices(
            blocks,
            translatedTexts,
            batchBoundaryStarts
        );

    console.log(
        `[PHASE SAFE] Selecionados ${selection.indices.length}/${blocks.length} bloco(s): risco=${selection.riskCount}, fronteiras-de-lote=${selection.boundaryCount}, canários=${selection.canaryCount}; chunks máx=${MAX_INDEPENDENT_AUDIT_BLOCKS}.`
    );

    const auditRecords =
        selection.indices.map(
            index =>
                buildIndependentAuditRecord(
                    blocks,
                    translatedTexts,
                    index
                )
        );

    const initialAudit =
        await runFinalAudit(
            auditRecords,
            deadlineAt
        );

    let confirmedFailures =
        await confirmOwnIdFailures(
            initialAudit.failures,
            blocks,
            translatedTexts,
            deadlineAt
        );

    if (
        confirmedFailures.length > 0
    ) {
        console.warn(
            `[PHASE SAFE] ${confirmedFailures.length} falha(s) confirmada(s); reparo cirúrgico, sem retraduzir lotes inteiros.`
        );

        const windows =
            buildRepairWindows(
                confirmedFailures,
                blocks
            );

        for (
            const windowIndices
            of windows
        ) {
            await repairWindow(
                blocks,
                translatedTexts,
                windowIndices,
                deadlineAt
            );
        }
    }

    assertBeforeDeadline(
        deadlineAt
    );

    const elapsedMs =
        Date.now() -
        startedAt;

    console.log(
        `[TRANSLATE] Finalizada em ${(elapsedMs / 1000).toFixed(1)}s.`
    );

    console.log(
        `[PERF] FAST=${(fastElapsed / 1000).toFixed(1)}s | TOTAL=${(elapsedMs / 1000).toFixed(1)}s | auditorias-base=${initialAudit.requests} request(s) | reparos=${confirmedFailures.length}.`
    );

    if (
        elapsedMs <=
        PERFORMANCE_TARGET_MS
    ) {
        console.log(
            `[PERF] META 1–3 MIN: ATINGIDA ✅ (${(elapsedMs / 1000).toFixed(1)}s)`
        );

    } else {
        console.log(
            `[PERF] Acima da meta de 180s: ${(elapsedMs / 1000).toFixed(1)}s.`
        );
    }

    const sanitizedTranslatedTexts =
        cleanAllTranslatedCharacters(
            translatedTexts
        );

    const withoutVocalElongations =
        cleanAllTranslatedVocalElongations(
            sanitizedTranslatedTexts
        );

    const cleanedTranslatedTexts =
        cleanAllTranslatedDialogueMarkers(
            withoutVocalElongations
        );

    const finalSrt =
        buildSrt(
            blocks,
            cleanedTranslatedTexts
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

    job.progress = 100;

    console.log(
        "[AUDIT CONTENT] TRADUÇÃO FINAL: OK — ID/lock 100%; SAFE-FINAL seletivo concluído; falhas confirmadas foram reparadas e reauditaram fail-closed."
    );

    return finalSrt;
}

/*
|--------------------------------------------------------------------------
| JOBS
|--------------------------------------------------------------------------
*/

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
        jobs.get(jobId);

    if (!job) {
        return null;
    }

    if (
        job.expiresAt <=
            Date.now() &&
        job.status !==
            "processing"
    ) {
        jobs.delete(jobId);
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

        assertBeforeDeadline(
            job.deadlineAt
        );

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
            getErrorMessage(error);

        job.updatedAt =
            Date.now();

        console.error(
            `[JOB ${job.id}] Falhou: ${job.error}`
        );
    }
}/*
|--------------------------------------------------------------------------
| FILA DE JOBS COMPLETOS
|--------------------------------------------------------------------------
*/

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
                resolve();
                return;
            }

            job.queuedAt =
                Date.now();

            const queueItem = {
                job,
                resolve,
                reject
            };

            const insertAt =
                translationJobQueue.findIndex(
                    item =>
                        Number(
                            item?.job?.priority ||
                            0
                        ) <
                        Number(
                            job.priority || 0
                        )
                );

            if (insertAt === -1) {
                translationJobQueue.push(
                    queueItem
                );

            } else {
                translationJobQueue.splice(
                    insertAt,
                    0,
                    queueItem
                );
            }

            console.log(
                `[JOB QUEUE] ${job.id} entrou na fila. Prioridade=${job.priority || 0}; aguardando=${translationJobQueue.length}.`
            );

            processTranslationJobQueue();
        }
    );
}

async function processTranslationJobQueue() {
    if (translationJobWorkerRunning) {
        return;
    }

    translationJobWorkerRunning =
        true;

    try {
        while (
            translationJobQueue.length >
            0
        ) {
            const item =
                translationJobQueue.shift();

            if (!item) {
                continue;
            }

            const {
                job,
                resolve,
                reject
            } = item;

            try {
                if (
                    job.status !==
                    "processing"
                ) {
                    resolve();
                    continue;
                }

                const queueWaitMs =
                    Number.isFinite(
                        job.queuedAt
                    )
                        ? Math.max(
                              0,
                              Date.now() -
                              job.queuedAt
                          )
                        : 0;

                console.log(
                    `[JOB QUEUE] Iniciando job completo ${job.id}. Prioridade=${job.priority || 0}; restantes=${translationJobQueue.length}. Espera=${(queueWaitMs / 1000).toFixed(1)}s — fila não consumiu o teto.`
                );

                await processJob(job);

                resolve();

            } catch (error) {
                reject(error);
            }
        }

    } finally {
        translationJobWorkerRunning =
            false;

        if (
            translationJobQueue.length >
            0
        ) {
            processTranslationJobQueue();
        }
    }
}

function startTranslationJob(
    job,
    label = "JOB"
) {
    if (!job) {
        return null;
    }

    if (
        job.status ===
            "completed" ||
        job.status ===
            "failed"
    ) {
        return job.promise;
    }

    if (job.promise) {
        return job.promise;
    }

    if (
        job.status ===
        "pending"
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
        `Progresso: ${progress}%. Aguarde alguns instantes.`
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

    return res.send(srt);
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
        await sleep(500);
    }

    return (
        job.status ===
        "completed"
    );
}

/*
|--------------------------------------------------------------------------
| MANIFEST STREMIO
|--------------------------------------------------------------------------
*/

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "6.3.0",

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
    ) => {
        res.json(manifest);
    }
);

app.get(
    "/",

    (
        req,
        res
    ) => {
        res.json({
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

            thinkingLevel:
                "MINIMAL",

            performanceMode:
                "fast-first-safe-final",

            releaseAwareOpenSubtitles:
                true,

            timestampAudit:
                true,

            conservativeCharacterSanitization:
                true,

            translationCacheVersion:
                TRANSLATION_CACHE_VERSION,

            structuralIdLock:
                true,

            semanticContentAudit:
                SEMANTIC_AUDIT_ENABLED,

            semanticAuditMode:
                "safe-final-selective-boundary-risk-canary",

            independentAuditMaxBlocks:
                MAX_INDEPENDENT_AUDIT_BLOCKS,

            auditCanaryStride:
                AUDIT_CANARY_STRIDE,

            lazyOpenSubtitles:
                true,

            bridgeCompatibleCacheVersion:
                "5.8",

            translationJobQueue:
                translationJobQueue.length,

            activeTranslationJob:
                translationJobWorkerRunning,

            queue:
                geminiQueue.length,

            cooldownSeconds:
                Math.ceil(
                    getCooldownRemaining() /
                    1000
                )
        });
    }
);

app.get(
    "/health",

    (
        req,
        res
    ) => {
        res.json({
            status:
                "ok",

            uptime:
                process.uptime(),

            model:
                GEMINI_MODEL,

            jobs:
                jobs.size,

            cache:
                translationCache.size,

            renderEngineVersion:
                RENDER_ENGINE_VERSION,

            thinkingLevel:
                "MINIMAL",

            performanceMode:
                "fast-first-safe-final",

            releaseAwareOpenSubtitles:
                true,

            timestampAudit:
                true,

            conservativeCharacterSanitization:
                true,

            translationCacheVersion:
                TRANSLATION_CACHE_VERSION,

            structuralIdLock:
                true,

            semanticContentAudit:
                SEMANTIC_AUDIT_ENABLED,

            semanticAuditMode:
                "safe-final-selective-boundary-risk-canary",

            independentAuditMaxBlocks:
                MAX_INDEPENDENT_AUDIT_BLOCKS,

            auditCanaryStride:
                AUDIT_CANARY_STRIDE,

            lazyOpenSubtitles:
                true,

            bridgeCompatibleCacheVersion:
                "5.8",

            translationJobQueue:
                translationJobQueue.length,

            translationJobWorkerRunning,

            geminiQueue:
                geminiQueue.length,

            geminiCooldownSeconds:
                Math.ceil(
                    getCooldownRemaining() /
                    1000
                ),

            batchMaxBlocks:
                MAX_BATCH_BLOCKS,

            batchMaxChars:
                MAX_BATCH_CHARS,

            requestIntervalMs:
                MIN_REQUEST_INTERVAL_MS,

            translationRequestTimeoutMs:
                TRANSLATION_REQUEST_TIMEOUT_MS,

            semanticAuditRequestTimeoutMs:
                SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS,

            maxOutputTokens:
                MAX_OUTPUT_TOKENS,

            translationTimeoutMs:
                MAX_TRANSLATION_TIME_MS,

            maxTranslationTimeMs:
                MAX_TRANSLATION_TIME_MS
        });
    }
);

/*
|--------------------------------------------------------------------------
| OPEN SUBTITLES - RELEASE AWARE
|--------------------------------------------------------------------------
*/

function scoreSubtitle(subtitle) {
    let score = 0;

    const lang =
        String(
            subtitle?.lang || ""
        ).toLowerCase();

    if (lang === "eng") {
        score += 100;

    } else if (lang === "en") {
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
            subtitle?.format || ""
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
            subtitle?.lang || ""
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
    if (!Array.isArray(subtitles)) {
        return null;
    }

    const usable =
        subtitles.filter(
            isUsableEnglishSubtitle
        );

    if (usable.length === 0) {
        return null;
    }

    if (releaseAware) {
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
    const originalUrl =
        String(
            req.originalUrl ||
            req.url ||
            ""
        );

    const pathname =
        originalUrl.split("?")[0];

    const match =
        pathname.match(
            /^\/subtitles\/[^/]+\/[^/]+\/(.+)\.json$/
        );

    if (match?.[1]) {
        return match[1];
    }

    return String(
        req.params.extra ||
        ""
    ).trim();
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
            ? rawVideoHash.toLowerCase()
            : "";

    const videoSizeNumber =
        Number(rawVideoSize);

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

    const encodedExtra =
        extra.toString();

    if (!encodedExtra) {
        return `${base}.json`;
    }

    return `${base}/${encodedExtra}.json`;
}

function releaseIdentityDescription(extra) {
    const parts = [];

    if (extra.videoHash) {
        parts.push(
            "videoHash"
        );
    }

    if (extra.videoSize) {
        parts.push(
            "videoSize"
        );
    }

    if (extra.filename) {
        parts.push(
            "filename"
        );
    }

    return parts.length
        ? parts.join(" + ")
        : "nenhuma";
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
                        "Stremio-Gemini-Subtitle-Translator/6.3"
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
        const upstreamIndex =
            subtitles.indexOf(
                target
            );

        console.log(
            `[OPENSUBTITLES MATCH] Selecionada posição upstream ${upstreamIndex >= 0 ? upstreamIndex + 1 : "?"}/${subtitles.length}; id=${String(target.id || "(sem id)")}; nome=${String(target.name || "(sem nome)")}.`
        );

    } else {
        console.log(
            `[OPENSUBTITLES MATCH] Nenhuma legenda inglesa utilizável retornada (${subtitles.length} resultado(s) upstream).`
        );
    }

    return target;
}

/*
|--------------------------------------------------------------------------
| DOWNLOAD SRT
|--------------------------------------------------------------------------
*/

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
                        "Stremio-Gemini-Subtitle-Translator/6.3"
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

/*
|--------------------------------------------------------------------------
| JOB DE LEGENDA EMBUTIDA
|--------------------------------------------------------------------------
*/

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

    if (blocks.length === 0) {
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

            cachedJob.updatedAt =
                Date.now();
        }

        console.log(
            `[EMBEDDED] Cache utilizado para ${sourceName}.`
        );

        return cachedJob;
    }

    const existingJob =
        findProcessingJob(
            cacheKey
        );

    if (existingJob) {
        console.log(
            `[EMBEDDED] Job existente reutilizado: ${existingJob.id}`
        );

        return existingJob;
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

/*
|--------------------------------------------------------------------------
| ENDPOINT SUBTITLES
|--------------------------------------------------------------------------
*/

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
            console.log(
                "[STREMIO] Nenhuma legenda inglesa encontrada."
            );

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

        if (blocks.length === 0) {
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

            console.log(
                "[CACHE] Tradução pronta."
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

            job.updatedAt =
                Date.now();

            console.log(
                `[LAZY] Job OpenSubtitles ${job.id} criado sem consumir Gemini; aguardando acesso à URL da legenda.`
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
            `[STREMIO] Erro: ${getErrorMessage(
                error
            )}`
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
);/*
|--------------------------------------------------------------------------
| API DA PONTE LOCAL - LEGENDA EMBUTIDA
|--------------------------------------------------------------------------
*/

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
            console.warn(
                "[EMBEDDED API] Tentativa não autorizada."
            );

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

            const mediaType =
                String(
                    type ||
                    "unknown"
                ).trim();

            const videoId =
                String(
                    id ||
                    "unknown"
                ).trim();

            const sourceName =
                String(
                    name ||
                    "embedded"
                ).trim();

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
                            `SRT muito grande. Limite: ${MAX_SOURCE_CHARS} caracteres.`
                    },

                    413
                );
            }

            console.log(
                `[EMBEDDED API] Recebido SRT de ${sourceName} para ${mediaType}/${videoId}.`
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

            const subtitleUrl =
                `${baseUrl}/subtitle/${encodeURIComponent(
                    job.id
                )}.srt`;

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

                    subtitleUrl
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

/*
|--------------------------------------------------------------------------
| RESULTADO DA LEGENDA
|--------------------------------------------------------------------------
*/

async function subtitleResultHandler(
    req,
    res
) {
    const raw =
        String(
            req.params.jobId ||
            ""
        ).trim();

    let jobId;

    try {
        jobId =
            decodeURIComponent(
                raw
            );

    } catch {
        jobId =
            raw;
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
        "pending"
    ) {
        if (
            !job.lazyStartScheduled
        ) {
            job.lazyStartScheduled =
                true;

            job.updatedAt =
                Date.now();

            console.log(
                `[LAZY] URL OpenSubtitles ${job.id} requisitada; grace de ${LAZY_OPENSUB_START_GRACE_MS}ms para priorizar Embedded, se ele chegar.`
            );

            const lazyTimer =
                setTimeout(
                    () => {
                        job.lazyStartScheduled =
                            false;

                        if (
                            job.status ===
                            "pending"
                        ) {
                            job.lazyStart =
                                false;

                            startTranslationJob(
                                job,
                                "JOB"
                            );
                        }
                    },

                    LAZY_OPENSUB_START_GRACE_MS
                );

            if (
                typeof lazyTimer.unref ===
                "function"
            ) {
                lazyTimer.unref();
            }
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

            } catch (error) {
                console.error(
                    `[AUDIT TIMESTAMP] Bloqueando legenda ${job.id}: ${getErrorMessage(error)}`
                );

                return sendSubtitleResponse(
                    res,

                    buildErrorSrt(
                        "A auditoria de timestamps detectou uma divergência e bloqueou esta legenda."
                    ),

                    "no-store"
                );
            }
        }

        if (
            !job.contentAuditPassed
        ) {
            console.error(
                `[AUDIT CONTENT] Bloqueando legenda ${job.id}: auditoria semântica não confirmada.`
            );

            return sendSubtitleResponse(
                res,

                buildErrorSrt(
                    "A auditoria semântica não confirmou a correspondência EN↔PT-BR desta legenda."
                ),

                "no-store"
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
            ),

            "no-store"
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

            } catch (error) {
                console.error(
                    `[AUDIT TIMESTAMP] Bloqueando legenda ${job.id}: ${getErrorMessage(error)}`
                );

                return sendSubtitleResponse(
                    res,

                    buildErrorSrt(
                        "A auditoria de timestamps detectou uma divergência e bloqueou esta legenda."
                    ),

                    "no-store"
                );
            }
        }

        if (
            !job.contentAuditPassed
        ) {
            console.error(
                `[AUDIT CONTENT] Bloqueando legenda ${job.id}: auditoria semântica não confirmada.`
            );

            return sendSubtitleResponse(
                res,

                buildErrorSrt(
                    "A auditoria semântica não confirmou a correspondência EN↔PT-BR desta legenda."
                ),

                "no-store"
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
            ),

            "no-store"
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

/*
|--------------------------------------------------------------------------
| START
|--------------------------------------------------------------------------
*/

app.listen(
    PORT,

    () => {
        console.log(
            "=============================================="
        );

        console.log(
            " STREMIO GEMINI SUBTITLE TRANSLATOR 6.3 FAST-FIRST / SAFE-FINAL"
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
            "Thinking Gemini: MINIMAL (latência) ✅"
        );

        console.log(
            "Sampling temperature/top_p/top_k: REMOVIDOS ✅"
        );

        console.log(
            `PUBLIC_URL: ${PUBLIC_URL || "(automático)"}`
        );

        console.log(
            `Batch FAST máximo: ${MAX_BATCH_BLOCKS} blocos`
        );

        console.log(
            `Batch FAST máximo: ${MAX_BATCH_CHARS} caracteres`
        );

        console.log(
            `Intervalo Gemini: ${MIN_REQUEST_INTERVAL_MS}ms`
        );

        console.log(
            `Saída máxima Gemini: ${MAX_OUTPUT_TOKENS} tokens`
        );

        console.log(
            `Concorrência Gemini: ${GEMINI_CONCURRENCY}`
        );

        console.log(
            `Timeout tradução normal: ${TRANSLATION_REQUEST_TIMEOUT_MS}ms`
        );

        console.log(
            `Timeout auditoria SAFE-FINAL: ${SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS}ms`
        );

        console.log(
            `Auditoria SAFE-FINAL: máx ${MAX_INDEPENDENT_AUDIT_BLOCKS} blocos/request`
        );

        console.log(
            `Canário determinístico: 1 a cada ${AUDIT_CANARY_STRIDE} blocos + extremidades`
        );

        console.log(
            `Teto total tradução: ${MAX_TRANSLATION_TIME_MS}ms`
        );

        console.log(
            "Relógio do teto: INICIA AO COMEÇAR O PROCESSAMENTO (fila não conta)"
        );

        console.log(
            `Cache TTL: ${CACHE_TTL_MS / 3600000}h`
        );

        console.log(
            "FASE FAST: TRADUZ TUDO PRIMEIRO / SEM AUDITORIA GEMINI ENTRE LOTES ✅"
        );

        console.log(
            "FASE SAFE-FINAL: FRONTEIRAS FORTES + FRONTEIRAS DE LOTE + RISCO + CANÁRIOS ✅"
        );

        console.log(
            "f=false + m=i: RECHECAGEM ANTES DE REPARAR ✅"
        );

        console.log(
            "m!=i: REPARO CIRÚRGICO DO PAR/JANELA, NÃO DO LOTE INTEIRO ✅"
        );

        console.log(
            "Erro estrutural: 1 REPETIÇÃO DO MESMO LOTE antes do RESCUE ✅"
        );

        console.log(
            "Timeout: RESCUE FIXO / SEM ÁRVORE LONGA ✅"
        );

        console.log(
            "Trava estrutural ID + lock: 100% DOS BLOCOS ✅"
        );

        console.log(
            "Limpeza SDH/CC: ATIVA"
        );

        console.log(
            "Adaptação audiovisual: ATIVA"
        );

        console.log(
            "Localização contemporânea Gen Z/Alpha contextual: ATIVA"
        );

        console.log(
            "Letramento LGBTQIAPN+/drag/camp/shade: ATIVO"
        );

        console.log(
            "Contexto oculto de falante: ATIVO"
        );

        console.log(
            "Proteção contra chute de gênero: ATIVA"
        );

        console.log(
            "Normalização de alongamentos vocais: ATIVA"
        );

        console.log(
            "Limpeza de traços soltos: ATIVA"
        );

        console.log(
            "Recuperação parcial de IDs: DESATIVADA (fail-closed)"
        );

        console.log(
            `OpenSubtitles: LAZY + grace ${LAZY_OPENSUB_START_GRACE_MS}ms + prioridade baixa ✅`
        );

        console.log(
            "Ponte Local 2.5.1: COMPATÍVEL + PRIORIDADE ALTA ✅"
        );

        console.log(
            `Namespace de cache da tradução: ${TRANSLATION_CACHE_VERSION}`
        );

        console.log(
            "Matching OpenSubtitles por release: ATIVO"
        );

        console.log(
            "Auditoria absoluta de timestamps: ATIVA"
        );

        console.log(
            "Higienização conservadora de caracteres: ATIVA"
        );

        console.log(
            "Fila de jobs completos: SEQUENCIAL (1 por vez)"
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
