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
 * 5.9 PERFORMANCE:
 * chamadas individuais mais curtas. Se uma chamada de tradução estourar
 * o limite, o lote é dividido imediatamente; auditoria lenta é dividida
 * sem retraduzir blocos que já passaram pelo contrato estrutural.
 */
const TRANSLATION_REQUEST_TIMEOUT_MS = 45000;
const SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS = 35000;
const MAX_TRANSLATION_TIME_MS = Number(process.env.MAX_TRANSLATION_TIME_MS ||
    480000);
const configuredMaxBatchChars = Number(process.env.MAX_BATCH_CHARS ||
    7000);
const MAX_BATCH_CHARS = Number.isFinite(configuredMaxBatchChars) &&
    configuredMaxBatchChars > 0
    ? Math.min(configuredMaxBatchChars, 7000)
    : 7000;
const configuredMaxBatchBlocks = Number(process.env.MAX_BATCH_BLOCKS ||
    96);
const MAX_BATCH_BLOCKS = Number.isFinite(configuredMaxBatchBlocks) &&
    configuredMaxBatchBlocks > 0
    ? Math.min(Math.floor(configuredMaxBatchBlocks), 96)
    : 96;
const GEMINI_CONCURRENCY = 1;
const MIN_REQUEST_INTERVAL_MS = Number(process.env.MIN_REQUEST_INTERVAL_MS ||
    3000);
const MAX_OUTPUT_TOKENS = Number(process.env.MAX_OUTPUT_TOKENS ||
    16000);
const MAX_NORMAL_RETRIES = 2;
const MAX_BAD_OUTPUT_SPLIT_DEPTH = 8;
const MAX_AUDIT_SPLIT_DEPTH = 8;
const MAX_SINGLE_BLOCK_OUTPUT_RETRIES = 1;
const MAX_SINGLE_BLOCK_AUDIT_RETRIES = 1;
const MAX_RATE_LIMIT_COOLDOWN_MS = 120000;
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_CACHE_ENTRIES = 200;
const MAX_JOBS = 300;
const MAX_SOURCE_CHARS = 800000;
/*
 * 5.9 mantém o contrato/cache 5.8 para compatibilidade direta com
 * a Ponte Local 2.5.1. A otimização muda execução, não a identidade
 * semântica do cache local.
 */
const TRANSLATION_CACHE_VERSION = "5.8";
const BLOCK_LOCK_VERSION = "5.8";
const SEMANTIC_AUDIT_ENABLED = true;
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
    const error = new Error("Tempo máximo de tradução atingido.");
    error.code =
        "TRANSLATION_TIMEOUT";
    return error;
}
function geminiRequestTimeoutError(requestKind) {
    const error = new Error(`Timeout da chamada Gemini (${requestKind || "request"}).`);
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
    if (Number.isFinite(deadlineAt) &&
        Date.now() >= deadlineAt) {
        throw translationTimeoutError();
    }
}
function remainingBeforeDeadline(deadlineAt) {
    if (!Number.isFinite(deadlineAt)) {
        return Infinity;
    }
    return Math.max(0, deadlineAt - Date.now());
}
async function sleepWithDeadline(ms, deadlineAt) {
    const safeMs = Math.max(0, Number(ms) || 0);
    if (safeMs === 0) {
        assertBeforeDeadline(deadlineAt);
        return;
    }
    const remaining = remainingBeforeDeadline(deadlineAt);
    if (Number.isFinite(remaining) &&
        remaining <= safeMs) {
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
        .update(String(text), "utf8")
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
    if (typeof error ===
        "string") {
        return error;
    }
    return (error.message ||
        error.statusText ||
        "Erro desconhecido.");
}
function cleanBaseUrl(req) {
    if (PUBLIC_URL) {
        return PUBLIC_URL;
    }
    const protocol = req.headers["x-forwarded-proto"] ||
        req.protocol ||
        "https";
    const host = req.headers["x-forwarded-host"] ||
        req.get("host");
    return `${protocol}://${host}`;
}
function safeJson(res, data, status = 200) {
    res.status(status);
    res.set("Cache-Control", "no-store, no-cache, must-revalidate");
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
    const auth = String(req.headers.authorization ||
        "").trim();
    if (!auth) {
        return false;
    }
    const expected = `Bearer ${LOCAL_BRIDGE_SECRET}`;
    const authBuffer = Buffer.from(auth);
    const expectedBuffer = Buffer.from(expected);
    if (authBuffer.length !==
        expectedBuffer.length) {
        return false;
    }
    return crypto.timingSafeEqual(authBuffer, expectedBuffer);
}
/*
|--------------------------------------------------------------------------
| FETCH COM TIMEOUT
|--------------------------------------------------------------------------
*/
async function fetchWithTimeout(url, options = {}, timeoutMs = 20000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, {
            ...options,
            signal: controller.signal
        });
    }
    finally {
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
    for (const [key, item] of translationCache.entries()) {
        if (item.expiresAt <= now) {
            translationCache.delete(key);
        }
    }
    for (const [key, job] of jobs.entries()) {
        if (job.expiresAt <= now &&
            job.status !==
                "processing") {
            jobs.delete(key);
        }
    }
    while (translationCache.size >
        MAX_CACHE_ENTRIES) {
        const key = translationCache.keys()
            .next()
            .value;
        if (key === undefined) {
            break;
        }
        translationCache.delete(key);
    }
    while (jobs.size > MAX_JOBS) {
        const key = jobs.keys()
            .next()
            .value;
        if (key === undefined) {
            break;
        }
        const job = jobs.get(key);
        if (job &&
            job.status ===
                "processing") {
            break;
        }
        jobs.delete(key);
    }
}
setInterval(cleanupMemory, 5 * 60 * 1000).unref();
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
        .replace(/^```(?:json|srt|text|plaintext)?\s*/i, "")
        .replace(/\s*```$/i, "")
        .trim();
}
/*
|--------------------------------------------------------------------------
| HIGIENIZAÇÃO CONSERVADORA DE CARACTERES - 5.7
|--------------------------------------------------------------------------
|
| Regras deliberadamente estreitas:
|
| - mantém acentos, pontuação, emojis e símbolos musicais;
| - mantém tags SRT comuns como <i>, <b> e <u>;
| - converte espaços não separáveis em espaço normal;
| - remove controles C0/C1 que não deveriam aparecer em diálogo;
| - remove invisíveis conhecidos que costumam vazar de fontes externas;
| - remove controles bidirecionais invisíveis;
| - remove U+FFFD (replacement character), que indica texto corrompido;
| - remove overrides ASS do tipo {\an8}, {\i1}, etc., pois podem ser
|   exibidos literalmente quando o destino final é SRT.
|
| Não tentamos “consertar” mojibake visível de forma agressiva.
|
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
function addCharacterSanitizationStats(target, source) {
    for (const key of Object.keys(target)) {
        target[key] +=
            Number(source?.[key] || 0);
    }
    return target;
}
function sanitizeSubtitleText(value) {
    const stats = createCharacterSanitizationStats();
    let text = String(value ?? "");
    const normalizedNfc = text.normalize("NFC");
    if (normalizedNfc !== text) {
        stats.nfcChanges++;
        text = normalizedNfc;
    }
    text =
        text.replace(/[\u00A0\u202F]/gu, () => {
            stats.normalizedSpaces++;
            return " ";
        });
    text =
        text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/gu, () => {
            stats.controlChars++;
            return "";
        });
    text =
        text.replace(/[\u00AD\u200B\u2060\uFEFF]/gu, () => {
            stats.invisibleChars++;
            return "";
        });
    text =
        text.replace(/[\u200E\u200F\u202A-\u202E\u2066-\u2069]/gu, () => {
            stats.invisibleChars++;
            return "";
        });
    text =
        text.replace(/\uFFFD/gu, () => {
            stats.replacementChars++;
            return "";
        });
    text =
        text.replace(/\{\\[^}\r\n]*\}/gu, () => {
            stats.assTags++;
            return "";
        });
    text =
        text
            .split("\n")
            .map(line => line
            .replace(/[ \t]{2,}/g, " ")
            .trimEnd())
            .join("\n")
            .trim();
    return {
        text,
        stats
    };
}
function characterSanitizationTotal(stats) {
    return (stats.controlChars +
        stats.invisibleChars +
        stats.normalizedSpaces +
        stats.replacementChars +
        stats.assTags +
        stats.nfcChanges);
}
function logCharacterSanitization(stage, stats) {
    const total = characterSanitizationTotal(stats);
    console.log(`[CLEAN CHAR] ${stage}: ${total} ajuste(s); controles=${stats.controlChars}, invisíveis=${stats.invisibleChars}, espaços=${stats.normalizedSpaces}, replacement=${stats.replacementChars}, ASS=${stats.assTags}, NFC=${stats.nfcChanges}.`);
}
/*
|--------------------------------------------------------------------------
| LIMPEZA SDH / CC
|--------------------------------------------------------------------------
*/
const SDH_CUE_WORDS = /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;
/*
|--------------------------------------------------------------------------
| CONTEXTO OCULTO DE FALANTE - 5.6
|--------------------------------------------------------------------------
*/
const SPEAKER_HINT_MARKER_REGEX = /^@@SPK:([^@]+)@@\s*/u;
function normalizeSpeakerHint(value) {
    const speaker = String(value || "")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim();
    if (!speaker ||
        speaker.length > 60) {
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
    return encodeURIComponent(String(speaker || ""));
}
function decodeSpeakerHint(encoded) {
    try {
        return normalizeSpeakerHint(decodeURIComponent(String(encoded || "")));
    }
    catch {
        return "";
    }
}
function extractSpeakerHint(line) {
    const original = String(line || "");
    const hiddenMatch = original.match(SPEAKER_HINT_MARKER_REGEX);
    if (hiddenMatch) {
        return {
            speaker: decodeSpeakerHint(hiddenMatch[1]),
            lineForCleaning: original.replace(SPEAKER_HINT_MARKER_REGEX, "")
        };
    }
    const bracketMatch = original.match(/^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*/u);
    if (bracketMatch) {
        const speaker = normalizeSpeakerHint(bracketMatch[1]);
        if (speaker) {
            return {
                speaker,
                lineForCleaning: original
            };
        }
    }
    const colonMatch = original.match(/^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50}):\s+(?=\S)/u);
    if (colonMatch) {
        const speaker = normalizeSpeakerHint(colonMatch[1]);
        if (speaker) {
            return {
                speaker,
                lineForCleaning: original
            };
        }
    }
    return {
        speaker: "",
        lineForCleaning: original
    };
}
/*
|--------------------------------------------------------------------------
| NORMALIZAÇÃO DE ALONGAMENTOS VOCAIS - 5.6
|--------------------------------------------------------------------------
*/
function normalizeHyphenatedVocalElongations(text) {
    return String(text ?? "").replace(/([A-Za-zÀ-ÖØ-öø-ÿ])(?:[-–—]\1){2,}[-–—]?/giu, "$1");
}
function normalizeTranslatedVocalElongations(text) {
    let result = normalizeHyphenatedVocalElongations(text);
    result =
        result.replace(/([AEIOUÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜaeiouáàâãäéèêëíìîïóòôõöúùûü])\1{3,}/gu, "$1");
    return result;
}
function cleanDialogueLine(line) {
    let text = String(line || "").trim();
    if (!text) {
        return "";
    }
    text =
        text.replace(/\s*\[[^\]]+\]\s*/gu, " ");
    text =
        text.replace(/\s*\(([^)]*)\)\s*/gu, (match, inside) => SDH_CUE_WORDS.test(String(inside || ""))
            ? " "
            : match);
    text =
        text.replace(/^\s*[-–—]?\s*[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ .'-]{0,30}:\s+(?=\S)/u, "");
    text =
        text
            .replace(/[ \t]{2,}/g, " ")
            .trim();
    if (/^[-–—♪♫♬\s]*$/u.test(text)) {
        return "";
    }
    return text;
}
/*
|--------------------------------------------------------------------------
| AUDITORIA ABSOLUTA DE TIMESTAMPS - 5.7
|--------------------------------------------------------------------------
|
| Existem duas barreiras:
|
| 1. RAW -> CLEAN:
|    cada timing que sobrevive à limpeza precisa existir EXATAMENTE,
|    byte por byte depois do trim, na fonte original e na mesma ordem.
|
| 2. SOURCE -> FINAL:
|    quantidade de blocos, índices e linhas de timing precisam ser
|    exatamente iguais. Qualquer divergência faz o job falhar em vez de
|    servir uma legenda com tempo alterado.
|
*/
const TIMING_LINE_REGEX = /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;
function extractTimingLines(srt) {
    return String(srt || "")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n")
        .split("\n")
        .map(line => line.trim())
        .filter(line => TIMING_LINE_REGEX.test(line));
}
function timingSignature(timings) {
    return sha256(JSON.stringify(timings)).slice(0, 16);
}
function auditCleaningTimestamps(rawSrt, cleanedSrt, label = "SOURCE") {
    const rawTimings = extractTimingLines(rawSrt);
    const cleanedTimings = extractTimingLines(cleanedSrt);
    let cursor = 0;
    for (let i = 0; i < cleanedTimings.length; i++) {
        const wanted = cleanedTimings[i];
        while (cursor < rawTimings.length &&
            rawTimings[cursor] !== wanted) {
            cursor++;
        }
        if (cursor >= rawTimings.length) {
            throw timestampIntegrityError(`Auditoria de timestamps falhou na limpeza (${label}) no bloco preservado ${i + 1}.`);
        }
        cursor++;
    }
    console.log(`[AUDIT TIMESTAMP] ${label} RAW->CLEAN: OK — ${cleanedTimings.length}/${rawTimings.length} timing(s) preservado(s) exatamente; removidos=${rawTimings.length - cleanedTimings.length}; assinatura=${timingSignature(cleanedTimings)}.`);
    return true;
}
function auditFinalTimestamps(sourceSrt, finalSrt, label = "FINAL") {
    const sourceBlocks = parseSrt(sourceSrt);
    const finalBlocks = parseSrt(finalSrt);
    if (sourceBlocks.length !==
        finalBlocks.length) {
        throw timestampIntegrityError(`Auditoria de timestamps falhou (${label}): fonte=${sourceBlocks.length} blocos, final=${finalBlocks.length} blocos.`);
    }
    for (let i = 0; i < sourceBlocks.length; i++) {
        const source = sourceBlocks[i];
        const final = finalBlocks[i];
        if (source.index !==
            final.index ||
            source.timing !==
                final.timing) {
            throw timestampIntegrityError(`Auditoria de timestamps falhou (${label}) no bloco ${i + 1}: índice/timing divergente.`);
        }
    }
    const signature = timingSignature(sourceBlocks.map(block => [
        block.index,
        block.timing
    ]));
    console.log(`[AUDIT TIMESTAMP] ${label}: OK — ${sourceBlocks.length}/${sourceBlocks.length} bloco(s), 0 alteração(ões), assinatura=${signature}.`);
    return true;
}
function cleanSrtForTranslation(srt) {
    const normalized = normalizeSrt(srt);
    if (!normalized) {
        return "";
    }
    const rawBlocks = normalized.split(/\n{2,}/);
    const cleanedBlocks = [];
    let removedBlocks = 0;
    let changedLines = 0;
    let speakerHintBlocks = 0;
    let elongatedLines = 0;
    const characterStats = createCharacterSanitizationStats();
    for (const rawBlock of rawBlocks) {
        const lines = rawBlock
            .trim()
            .split("\n");
        const timingIndex = lines.findIndex(line => /-->/.test(line));
        if (timingIndex === -1) {
            continue;
        }
        const timing = lines[timingIndex].trim();
        const cleanedDialogue = [];
        const speakerHints = new Set();
        for (const line of lines.slice(timingIndex + 1)) {
            const speakerInfo = extractSpeakerHint(line);
            if (speakerInfo.speaker) {
                speakerHints.add(speakerInfo.speaker);
            }
            const cleanedBeforeCharacters = cleanDialogueLine(speakerInfo.lineForCleaning);
            const sanitized = sanitizeSubtitleText(cleanedBeforeCharacters);
            addCharacterSanitizationStats(characterStats, sanitized.stats);
            const cleanedBeforeElongation = sanitized.text;
            const cleaned = normalizeHyphenatedVocalElongations(cleanedBeforeElongation);
            if (cleaned !==
                cleanedBeforeElongation) {
                elongatedLines++;
            }
            if (cleaned !==
                line.trim()) {
                changedLines++;
            }
            if (cleaned) {
                cleanedDialogue.push(cleaned);
            }
        }
        if (cleanedDialogue.length === 0) {
            removedBlocks++;
            continue;
        }
        if (speakerHints.size === 1) {
            const speakerHint = Array.from(speakerHints)[0];
            cleanedDialogue[0] =
                `@@SPK:${encodeSpeakerHint(speakerHint)}@@ ${cleanedDialogue[0]}`;
            speakerHintBlocks++;
        }
        cleanedBlocks.push({
            timing,
            dialogue: cleanedDialogue
        });
    }
    const result = cleanedBlocks
        .map((block, index) => [
        index + 1,
        block.timing,
        ...block.dialogue
    ].join("\n"))
        .join("\n\n")
        .trim();
    console.log(`[CLEAN] SDH/CC: ${rawBlocks.length} -> ${cleanedBlocks.length} blocos; ${removedBlocks} removidos; ${changedLines} linha(s) alterada(s).`);
    console.log(`[CLEAN] Contexto de falante: ${speakerHintBlocks} bloco(s) preservado(s).`);
    console.log(`[CLEAN] Alongamentos vocais na fonte: ${elongatedLines} linha(s) normalizada(s).`);
    logCharacterSanitization("FONTE", characterStats);
    const finalResult = result
        ? result + "\n"
        : "";
    if (finalResult) {
        auditCleaningTimestamps(normalized, finalResult, "FONTE");
    }
    return finalResult;
}
/*
|--------------------------------------------------------------------------
| PARSER SRT
|--------------------------------------------------------------------------
*/
function parseSrt(srt) {
    const normalized = normalizeSrt(srt);
    if (!normalized) {
        return [];
    }
    const blocks = normalized
        .split(/\n{2,}/)
        .map(block => block.trim())
        .filter(Boolean);
    const result = [];
    for (const block of blocks) {
        const lines = block.split("\n");
        if (lines.length < 3) {
            continue;
        }
        const indexLine = lines[0].trim();
        const timingLine = lines[1].trim();
        if (!/^\d+$/.test(indexLine)) {
            continue;
        }
        if (!TIMING_LINE_REGEX.test(timingLine)) {
            continue;
        }
        const textLines = lines.slice(2);
        let speakerHint = "";
        if (textLines.length > 0) {
            const markerMatch = textLines[0].match(SPEAKER_HINT_MARKER_REGEX);
            if (markerMatch) {
                speakerHint =
                    decodeSpeakerHint(markerMatch[1]);
                textLines[0] =
                    textLines[0].replace(SPEAKER_HINT_MARKER_REGEX, "");
            }
        }
        result.push({
            index: Number(indexLine),
            timing: timingLine,
            text: textLines.join("\n"),
            speakerHint: speakerHint || null
        });
    }
    return result;
}
/*
|--------------------------------------------------------------------------
| LIMPEZA DE CARACTERES APÓS TRADUÇÃO - 5.7
|--------------------------------------------------------------------------
*/
function cleanAllTranslatedCharacters(translatedTexts) {
    const stats = createCharacterSanitizationStats();
    let changedBlocks = 0;
    const cleaned = translatedTexts.map(text => {
        const original = String(text ?? "");
        const sanitized = sanitizeSubtitleText(original);
        addCharacterSanitizationStats(stats, sanitized.stats);
        if (sanitized.text !==
            original) {
            changedBlocks++;
        }
        return sanitized.text;
    });
    logCharacterSanitization(`TRADUÇÃO (${changedBlocks} bloco(s) alterado(s))`, stats);
    return cleaned;
}
/*
|--------------------------------------------------------------------------
| LIMPEZA DE MARCADORES DE DIÁLOGO TRADUZIDOS
|--------------------------------------------------------------------------
*/
function cleanTranslatedDialogueMarkers(text) {
    const normalized = String(text ?? "")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n");
    const lines = normalized.split("\n");
    const markerRegex = /^\s*[-–—/]+\s+(?=\S)/u;
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
    const isRealMultiSpeakerBlock = nonEmptyLines >= 2 &&
        markedDialogueLines >= 2;
    if (isRealMultiSpeakerBlock) {
        return normalized;
    }
    return lines
        .map(line => line.replace(/^\s*[-–—/]+\s+(?=\S)/u, ""))
        .join("\n");
}
function cleanAllTranslatedDialogueMarkers(translatedTexts) {
    let changedBlocks = 0;
    const cleaned = translatedTexts.map(text => {
        const original = String(text ?? "");
        const result = cleanTranslatedDialogueMarkers(original);
        if (result !== original) {
            changedBlocks++;
        }
        return result;
    });
    console.log(`[CLEAN] Marcadores de diálogo: ${changedBlocks} bloco(s) ajustado(s).`);
    return cleaned;
}
function cleanAllTranslatedVocalElongations(translatedTexts) {
    let changedBlocks = 0;
    const cleaned = translatedTexts.map(text => {
        const original = String(text ?? "");
        const result = normalizeTranslatedVocalElongations(original);
        if (result !== original) {
            changedBlocks++;
        }
        return result;
    });
    console.log(`[CLEAN] Alongamentos vocais traduzidos: ${changedBlocks} bloco(s) ajustado(s).`);
    return cleaned;
}
/*
|--------------------------------------------------------------------------
| CONSTRUTOR SRT
|--------------------------------------------------------------------------
*/
function buildSrt(blocks, translatedTexts) {
    return (blocks
        .map((block, index) => {
        const translated = translatedTexts[index] ??
            block.text;
        return [
            block.index,
            block.timing,
            translated
        ].join("\n");
    })
        .join("\n\n")
        .trim() +
        "\n");
}
/*
|--------------------------------------------------------------------------
| CACHE
|--------------------------------------------------------------------------
*/
function setTranslationCache(key, srt) {
    const now = Date.now();
    translationCache.set(key, {
        srt,
        version: TRANSLATION_CACHE_VERSION,
        contentAuditPassed: true,
        createdAt: now,
        expiresAt: now + CACHE_TTL_MS
    });
    cleanupMemory();
}
function getTranslationCache(key) {
    const item = translationCache.get(key);
    if (!item) {
        return null;
    }
    if (item.version !==
        TRANSLATION_CACHE_VERSION ||
        item.contentAuditPassed !==
            true) {
        translationCache.delete(key);
        return null;
    }
    if (item.expiresAt <=
        Date.now()) {
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
        const text = String(block.text || "");
        const blockChars = text.length + 50;
        const wouldExceedChars = current.length > 0 &&
            currentChars +
                blockChars >
                MAX_BATCH_CHARS;
        const wouldExceedBlocks = current.length >=
            MAX_BATCH_BLOCKS;
        if (wouldExceedChars ||
            wouldExceedBlocks) {
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
    return Math.max(0, geminiCooldownUntil -
        Date.now());
}
function setGeminiCooldown(ms) {
    const safeMs = Math.min(Math.max(Number(ms) || 30000, 1000), MAX_RATE_LIMIT_COOLDOWN_MS);
    const until = Date.now() + safeMs;
    if (until > geminiCooldownUntil) {
        geminiCooldownUntil = until;
    }
    console.log(`[GEMINI] RATE LIMIT. Cooldown global de ${Math.ceil(safeMs / 1000)}s.`);
}
function getRetryAfterMs(response, errorData) {
    const header = response?.headers?.get("retry-after");
    if (header) {
        const seconds = Number(header);
        if (Number.isFinite(seconds) &&
            seconds > 0) {
            return Math.min(seconds * 1000, MAX_RATE_LIMIT_COOLDOWN_MS);
        }
    }
    const message = String(errorData?.error?.message ||
        "");
    const secondsMatch = message.match(/retry in\s+([\d.]+)s/i);
    if (secondsMatch) {
        const seconds = Number(secondsMatch[1]);
        if (Number.isFinite(seconds)) {
            return Math.min((seconds + 1) * 1000, MAX_RATE_LIMIT_COOLDOWN_MS);
        }
    }
    const minuteSecondMatch = message.match(/retry in\s+(\d+)m\s*(\d+(?:\.\d+)?)?s?/i);
    if (minuteSecondMatch) {
        const minutes = Number(minuteSecondMatch[1]);
        const seconds = Number(minuteSecondMatch[2] || 0);
        return Math.min((minutes * 60 +
            seconds +
            1) * 1000, MAX_RATE_LIMIT_COOLDOWN_MS);
    }
    const minuteMatch = message.match(/retry in\s+(\d+)m/i);
    if (minuteMatch) {
        return Math.min((Number(minuteMatch[1]) * 60 +
            1) * 1000, MAX_RATE_LIMIT_COOLDOWN_MS);
    }
    return 30000;
}
function isRateLimitError(status, message) {
    return (status === 429 ||
        /quota|rate.?limit|resource.?exhausted|too many requests/i.test(String(message || "")));
}
/*
|--------------------------------------------------------------------------
| REQUEST GEMINI
|--------------------------------------------------------------------------
*/
async function rawGeminiRequest(prompt, deadlineAt, requestKind = "translation") {
    if (!GEMINI_API_KEY) {
        throw new Error("GEMINI_API_KEY não configurada.");
    }
    assertBeforeDeadline(deadlineAt);
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`;
    const body = requestKind ===
        "semantic-audit"
        ? {
            systemInstruction: {
                parts: [
                    {
                        text: "Você é um auditor bilíngue de integridade de legendas EN→PT-BR. " +
                            "NÃO traduza nem reescreva. Compare cada tradução com a fonte do MESMO ID. " +
                            "Cada ID é uma unidade atômica: uma tradução pode ser um fragmento se a fonte daquele ID também for fragmentária. " +
                            "Não penalize paráfrase natural, gíria, adaptação idiomática ou mudança de estrutura gramatical quando o significado daquele próprio ID for preservado. " +
                            "Use vizinhos somente para desambiguar e detectar migração. Se a tradução de um ID contiver claramente conteúdo de outro ID, marque faithful=false e indique o outro ID em matchedSourceId. " +
                            "Se a tradução corresponder ao próprio ID, inclusive em falas curtas ou repetidas, prefira matchedSourceId igual ao próprio id. " +
                            "Se não houver correspondência confiável, use matchedSourceId=-1 e faithful=false. " +
                            "Preserve exatamente id e lock e retorne somente o JSON solicitado."
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
                responseMimeType: "application/json",
                responseSchema: {
                    type: "ARRAY",
                    items: {
                        type: "OBJECT",
                        properties: {
                            id: {
                                type: "INTEGER"
                            },
                            lock: {
                                type: "STRING"
                            },
                            matchedSourceId: {
                                type: "INTEGER"
                            },
                            faithful: {
                                type: "BOOLEAN"
                            }
                        },
                        required: [
                            "id",
                            "lock",
                            "matchedSourceId",
                            "faithful"
                        ]
                    },
                    minItems: 1
                },
                maxOutputTokens: MAX_OUTPUT_TOKENS
            }
        }
        : {
            systemInstruction: {
                parts: [
                    {
                        text: "Você é um tradutor, localizador e adaptador profissional de legendas para Português do Brasil, especializado em diálogo audiovisual contemporâneo. " +
                            "Seu objetivo não é produzir uma tradução de dicionário: é fazer a fala soar como uma pessoa brasileira realmente falaria naquela situação, preservando intenção, humor, personalidade, ironia, shade, camp, provocação, carinho e ritmo cômico. " +
                            "A entrada será majoritariamente em inglês, mas pode conter trechos em italiano, espanhol, francês ou outros idiomas; traduza também esses trechos para PT-BR quando forem conteúdo falado ou cantado relevante. " +
                            "Em reality shows, competição, conversa informal, cultura pop e cenas descontraídas, prefira PT-BR oral, espontâneo e natural. Use gírias brasileiras somente quando combinarem de verdade com o contexto; não force caricaturas nem gírias aleatórias. " +
                            "O português deve soar contemporâneo e atual para o público brasileiro de hoje, com sensação de legenda profissional de streaming. Evite escolhas lexicais datadas, antiquadas ou com cara de dublagem/tradução antiga. " +
                            "Evite especialmente, salvo se o próprio personagem ou contexto exigir de forma clara, expressões como balacobaco, qualé, mó, broto, supimpa, jóia como gíria, é o bicho, da hora usado artificialmente e outras fórmulas que hoje possam soar envelhecidas ou deslocadas. Entre uma gíria antiga e uma formulação simples, natural e atual, prefira a formulação atual. " +
                            "Pode usar linguagem contemporânea associada à cultura LGBTQIA+, drag, camp, shade e cultura pop quando isso combinar de verdade com quem fala e com a cena. Termos como gata, bicha, amiga, mana, serviu, arrasou, entregou ou amassou podem funcionar em contextos específicos, mas nunca devem ser inseridos mecanicamente só para deixar a tradução mais jovem ou queer. " +
                            "Não transforme todo personagem em alguém da geração Z ou Alpha. A modernidade deve aparecer principalmente no ritmo, nas escolhas de palavras e na naturalidade, e não numa enxurrada de gírias. Preserve idade, personalidade, região, formalidade e contexto social do falante sempre que essas informações estiverem disponíveis. " +
                            "REGRA DE INTEGRIDADE PRIORITÁRIA: cada ID é uma unidade atômica e imutável. O texto devolvido em um ID deve traduzir SOMENTE o campo text daquele próprio ID. Se a fonte daquele ID terminar no meio de uma frase, a tradução desse ID também deve conter somente a parte semanticamente presente nele; jamais antecipe palavras, ideias ou a conclusão do ID seguinte. " +
                            "Antes de traduzir literalmente uma expressão curta ou ambígua, interprete a intenção usando os blocos vizinhos. Pontuação, quebras de linha e segmentação podem ser imperfeitas; trate os vizinhos SOMENTE como contexto para interpretar o próprio ID, nunca como autorização para redistribuir, completar, antecipar ou atrasar conteúdo. " +
                            "Reconheça vocativos coloquiais. Palavras como girl, bitch, honey, sis, queen, baby e babe nem sempre são substantivos literais. Dependendo do tom, podem equivaler a gata, amiga, mana, bicha, querida, amor ou até ser omitidas quando isso soar mais natural. " +
                            "Nunca traduza automaticamente girl como garota nem bitch como vadia. Use vadia apenas quando houver intenção real de insulto. Em fala camp, afetiva, debochada ou entre queens, escolha a solução brasileira que preserve o humor e o tom. " +
                            "Exemplo de interpretação: 'Kenya got you, girl' em tom de shade significa algo como 'Mas a Kenya te pegou, gata', e não 'Kenya pegou a sua garota'. " +
                            "Exemplo de registro: 'Vita is quiet, but this bitch is a silent killer' em contexto camp pode soar como 'A Vita é calada, mas essa bicha é uma assassina silenciosa', em vez de traduzir bitch mecanicamente como vadia. " +
                            "Adapte expressões idiomáticas, piadas e trocadilhos quando existir uma solução natural em PT-BR que preserve a intenção e a graça. Se a adaptação ficar forçada, confusa ou perder um bordão reconhecível, preserve o termo original. " +
                            "Exemplo: preserve 'Condragulations' como 'Condragulations'; não invente neologismos como 'Parabravas'. " +
                            "Quando houver letra de música realmente transcrita na legenda, traduza seu conteúdo para PT-BR, mesmo que esteja em um idioma diferente do inglês. Não invente letras quando houver apenas marcações como [music], símbolos musicais ou descrições de som. " +
                            "Preserve nomes próprios, marcas, títulos, termos técnicos, palavrões, intensidade emocional e intenção. Não censure. Não resuma. Não explique. " +
                            "Não acrescente nomes de falantes, descrições de sons, rubricas SDH/CC ou observações que não existam no texto recebido. Traduza somente o campo text e mantenha exatamente os IDs e locks recebidos, na mesma ordem. O lock é um selo opaco: copie-o sem alteração. " +
                            "Algumas entradas podem trazer um campo opcional speaker. Esse campo é somente contexto oculto de quem fala: nunca o copie para o campo text e nunca acrescente o nome do falante à legenda final. Quando speaker identificar claramente uma pessoa, respeite o gênero gramatical dessa pessoa em adjetivos, particípios e construções em primeira pessoa. " +
                            "Quando não houver speaker ou a identidade do falante não estiver clara, NÃO chute masculino ou feminino. Prefira uma formulação brasileira natural sem marca de gênero sempre que isso evitar uma escolha incerta. Por exemplo, em vez de adivinhar entre 'estou empolgado' e 'estou empolgada', uma solução como 'Mal posso esperar', 'Que empolgação' ou outra construção neutra pode ser melhor conforme o contexto. Nunca use formas artificiais como empolgado(a), empolgade ou barras de gênero. " +
                            "Não reproduza graficamente notas ou sílabas sustentadas. Alongamentos como home-e-e-e, ce-e-e-e-rto, sooooo ou nããããão representam duração da voz e devem virar a palavra normal adequada ao sentido. O áudio já transmite a duração. Preserve apenas vocalizações que tenham valor real de fala, como 'ah', 'oh' ou 'hmm', sem multiplicar letras desnecessariamente. " +
                            "Não acrescente traços, travessões, barras ou marcadores de diálogo que não sejam necessários. Preserve marcadores somente quando forem realmente necessários para distinguir dois ou mais falantes no mesmo bloco. " +
                            "Preserve tags de formatação como <i>, </i>, <b>, </b>, {\\i1}, {\\i0} e similares."
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
                responseMimeType: "application/json",
                responseSchema: {
                    type: "ARRAY",
                    items: {
                        type: "OBJECT",
                        properties: {
                            id: {
                                type: "INTEGER"
                            },
                            lock: {
                                type: "STRING"
                            },
                            text: {
                                type: "STRING"
                            }
                        },
                        required: [
                            "id",
                            "lock",
                            "text"
                        ]
                    },
                    minItems: 1
                },
                maxOutputTokens: MAX_OUTPUT_TOKENS
            }
        };
    const remaining = remainingBeforeDeadline(deadlineAt);
    if (remaining <= 0) {
        throw translationTimeoutError();
    }
    const kindTimeoutMs = requestKind ===
        "semantic-audit"
        ? SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS
        : TRANSLATION_REQUEST_TIMEOUT_MS;
    const requestTimeoutMs = Math.max(1, Math.min(kindTimeoutMs, Number.isFinite(remaining)
        ? remaining
        : kindTimeoutMs));
    const controller = new AbortController();
    let requestTimedOut = false;
    const timer = setTimeout(() => {
        requestTimedOut =
            true;
        controller.abort();
    }, requestTimeoutMs);
    let response;
    let rawText;
    try {
        response =
            await fetch(endpoint, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "x-goog-api-key": GEMINI_API_KEY
                },
                body: JSON.stringify(body),
                signal: controller.signal
            });
        rawText =
            await response.text();
    }
    catch (error) {
        if (Number.isFinite(deadlineAt) &&
            Date.now() >= deadlineAt) {
            throw translationTimeoutError();
        }
        if (requestTimedOut) {
            throw geminiRequestTimeoutError(requestKind);
        }
        throw error;
    }
    finally {
        clearTimeout(timer);
    }
    assertBeforeDeadline(deadlineAt);
    let data;
    try {
        data =
            JSON.parse(rawText);
    }
    catch {
        const error = new Error(`Resposta não-JSON do Gemini. HTTP ${response.status}.`);
        error.status =
            response.status;
        throw error;
    }
    if (!response.ok) {
        const message = data?.error?.message ||
            `HTTP ${response.status}`;
        const error = new Error(message);
        error.status =
            response.status;
        error.rateLimit =
            isRateLimitError(response.status, message);
        error.retryAfterMs =
            error.rateLimit
                ? getRetryAfterMs(response, data)
                : 0;
        throw error;
    }
    const text = data?.candidates?.[0]
        ?.content?.parts
        ?.map(part => part?.text ||
        "")
        .join("")
        .trim();
    if (!text) {
        throw new Error("Gemini não retornou conteúdo.");
    }
    return text;
}
/*
|--------------------------------------------------------------------------
| FILA GEMINI
|--------------------------------------------------------------------------
*/
function enqueueGemini(prompt, deadlineAt, requestKind = "translation") {
    return new Promise((resolve, reject) => {
        if (Number.isFinite(deadlineAt) &&
            Date.now() >=
                deadlineAt) {
            reject(translationTimeoutError());
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
    });
}
async function processGeminiQueue() {
    if (geminiWorkerRunning) {
        return;
    }
    geminiWorkerRunning = true;
    try {
        while (geminiQueue.length > 0) {
            const item = geminiQueue.shift();
            if (!item) {
                continue;
            }
            if (Number.isFinite(item.deadlineAt) &&
                Date.now() >=
                    item.deadlineAt) {
                item.reject(translationTimeoutError());
                continue;
            }
            let normalAttempt = 1;
            let finished = false;
            while (!finished) {
                try {
                    assertBeforeDeadline(item.deadlineAt);
                    const cooldown = getCooldownRemaining();
                    if (cooldown > 0) {
                        console.log(`[GEMINI] Fila aguardando cooldown de ${Math.ceil(cooldown / 1000)}s.`);
                        await sleepWithDeadline(cooldown, item.deadlineAt);
                    }
                    const sinceLast = Date.now() -
                        lastGeminiRequestAt;
                    if (lastGeminiRequestAt > 0 &&
                        sinceLast <
                            MIN_REQUEST_INTERVAL_MS) {
                        const wait = MIN_REQUEST_INTERVAL_MS -
                            sinceLast;
                        await sleepWithDeadline(wait, item.deadlineAt);
                    }
                    assertBeforeDeadline(item.deadlineAt);
                    console.log(`[GEMINI] Request ${normalAttempt}/${MAX_NORMAL_RETRIES + 1} (${item.requestKind || "translation"})`);
                    lastGeminiRequestAt =
                        Date.now();
                    const result = await rawGeminiRequest(item.prompt, item.deadlineAt, item.requestKind ||
                        "translation");
                    item.resolve(result);
                    finished = true;
                }
                catch (error) {
                    if (error?.code ===
                        "TRANSLATION_TIMEOUT" ||
                        (Number.isFinite(item.deadlineAt) &&
                            Date.now() >=
                                item.deadlineAt)) {
                        item.reject(translationTimeoutError());
                        finished = true;
                        continue;
                    }
                    if (error?.code ===
                        "GEMINI_REQUEST_TIMEOUT") {
                        console.warn(`[GEMINI] Timeout curto (${item.requestKind || "translation"}); lote será tratado pelo split inteligente.`);
                        item.reject(error);
                        finished = true;
                        continue;
                    }
                    const message = getErrorMessage(error);
                    console.error(`[GEMINI] Erro: ${message}`);
                    if (error?.rateLimit) {
                        const cooldownMs = Math.min(error.retryAfterMs ||
                            30000, MAX_RATE_LIMIT_COOLDOWN_MS);
                        setGeminiCooldown(cooldownMs);
                        continue;
                    }
                    if (normalAttempt <=
                        MAX_NORMAL_RETRIES) {
                        const wait = 1500 *
                            normalAttempt;
                        console.log(`[GEMINI] Retry normal em ${Math.ceil(wait / 1000)}s.`);
                        normalAttempt++;
                        try {
                            await sleepWithDeadline(wait, item.deadlineAt);
                        }
                        catch (timeoutError) {
                            item.reject(timeoutError);
                            finished = true;
                        }
                        continue;
                    }
                    item.reject(error);
                    finished = true;
                }
            }
        }
    }
    finally {
        geminiWorkerRunning =
            false;
        if (geminiQueue.length > 0) {
            processGeminiQueue();
        }
    }
}
/*
|--------------------------------------------------------------------------
| PROMPT - 5.9 / INTEGRIDADE ATÔMICA + PERFORMANCE
|--------------------------------------------------------------------------
*/
function blockTranslationLock(block) {
    return sha256(JSON.stringify([
        BLOCK_LOCK_VERSION,
        Number(block?.index),
        String(block?.timing ?? ""),
        String(block?.text ?? ""),
        String(block?.speakerHint ??
            "")
    ])).slice(0, 20);
}
function buildTranslationPrompt(blocks) {
    const payload = blocks.map(block => {
        const item = {
            id: block.index,
            lock: blockTranslationLock(block),
            timing: block.timing,
            text: block.text
        };
        if (block.speakerHint) {
            item.speaker =
                block.speakerHint;
        }
        return item;
    });
    return `
TAREFA: traduza/localize cada "text" para PT-BR seguindo integralmente as regras do sistema.
Cada item é atômico: preserve id+lock+ordem e devolva somente conteúdo do próprio ID. timing e speaker são apenas contexto.
SAÍDA EXATA: [{"id":123,"lock":"abc123","text":"tradução"}]
ENTRADAS:
${JSON.stringify(payload)}
`;
}
/*
|--------------------------------------------------------------------------
| TRADUZIR LOTE
|--------------------------------------------------------------------------
*/
function badModelOutputError(message) {
    const error = new Error(message);
    error.code =
        "BAD_MODEL_OUTPUT";
    return error;
}
function badAuditOutputError(message) {
    const error = new Error(message);
    error.code =
        "BAD_AUDIT_OUTPUT";
    return error;
}
function buildSemanticAuditPrompt(blocks, translatedTexts) {
    const payload = blocks.map((block, index) => {
        const item = {
            id: block.index,
            lock: blockTranslationLock(block),
            timing: block.timing,
            source: block.text,
            translation: translatedTexts[index]
        };
        if (block.speakerHint) {
            item.speaker =
                block.speakerHint;
        }
        return item;
    });
    return `
AUDITE 100% dos itens conforme as regras do sistema. Não traduza nem reescreva.
Para cada item, confirme se translation corresponde semanticamente ao source do MESMO id; vizinhos servem apenas para detectar migração.
SAÍDA EXATA: [{"id":123,"lock":"abc123","matchedSourceId":123,"faithful":true}]
BLOCOS:
${JSON.stringify(payload)}
`;
}
async function auditBatchSemanticsOnce(blocks, translatedTexts, deadlineAt) {
    const raw = await enqueueGemini(buildSemanticAuditPrompt(blocks, translatedTexts), deadlineAt, "semantic-audit");
    let parsed;
    try {
        parsed =
            JSON.parse(stripCodeFences(raw));
    }
    catch {
        throw badAuditOutputError("Auditoria semântica retornou JSON inválido.");
    }
    if (!Array.isArray(parsed) ||
        parsed.length !==
            blocks.length) {
        throw badAuditOutputError(`Auditoria semântica incompleta: esperado=${blocks.length}, recebido=${Array.isArray(parsed) ? parsed.length : 0}.`);
    }
    const failures = [];
    for (let i = 0; i < blocks.length; i++) {
        const block = blocks[i];
        const item = parsed[i];
        const expectedLock = blockTranslationLock(block);
        if (!item ||
            item.id !==
                block.index ||
            item.lock !==
                expectedLock ||
            !Number.isInteger(item.matchedSourceId) ||
            typeof item.faithful !==
                "boolean") {
            throw badAuditOutputError(`Auditoria semântica quebrou o contrato no bloco ${block.index}.`);
        }
        if (item.faithful !==
            true ||
            item.matchedSourceId !==
                block.index) {
            failures.push({
                id: block.index,
                matchedSourceId: item.matchedSourceId
            });
        }
    }
    if (failures.length > 0) {
        const sample = failures
            .slice(0, 8)
            .map(item => `${item.id}->${item.matchedSourceId}`)
            .join(", ");
        console.warn(`[AUDIT CONTENT] FALHA — ${failures.length}/${blocks.length} bloco(s) suspeito(s); ${sample}. Tradução desse lote será descartada.`);
        throw badModelOutputError(`Auditoria semântica detectou ${failures.length} bloco(s) com conteúdo associado ao ID errado.`);
    }
    const signature = sha256(JSON.stringify(blocks.map(block => [
        block.index,
        blockTranslationLock(block)
    ]))).slice(0, 16);
    console.log(`[AUDIT CONTENT] OK — ${blocks.length}/${blocks.length} bloco(s); ID/lock/significado correspondentes; assinatura=${signature}.`);
    return true;
}
async function auditBatchSemantics(blocks, translatedTexts, deadlineAt, splitDepth = 0, singleBlockRetry = 0) {
    if (!SEMANTIC_AUDIT_ENABLED) {
        return true;
    }
    assertBeforeDeadline(deadlineAt);
    if (blocks.length !==
        translatedTexts.length) {
        throw badModelOutputError(`Auditoria semântica recebeu tamanhos divergentes: fonte=${blocks.length}, tradução=${translatedTexts.length}.`);
    }
    try {
        return await auditBatchSemanticsOnce(blocks, translatedTexts, deadlineAt);
    }
    catch (error) {
        if (error?.code ===
            "BAD_MODEL_OUTPUT") {
            /*
             * Falha semântica verdadeira: a tradução é suspeita.
             * Propagamos para translateBatch, que descarta/retraduz.
             */
            throw error;
        }
        const auditRecoverable = error?.code ===
            "BAD_AUDIT_OUTPUT" ||
            error?.code ===
                "GEMINI_REQUEST_TIMEOUT";
        if (!auditRecoverable) {
            throw error;
        }
        assertBeforeDeadline(deadlineAt);
        if (blocks.length === 1) {
            if (singleBlockRetry <
                MAX_SINGLE_BLOCK_AUDIT_RETRIES) {
                console.warn(`[AUDIT CONTENT] Auditoria de 1 bloco falhou/estourou timeout. Nova tentativa ${singleBlockRetry + 1}/${MAX_SINGLE_BLOCK_AUDIT_RETRIES}.`);
                return auditBatchSemantics(blocks, translatedTexts, deadlineAt, splitDepth, singleBlockRetry + 1);
            }
            throw error;
        }
        if (splitDepth >=
            MAX_AUDIT_SPLIT_DEPTH) {
            throw error;
        }
        const middle = Math.ceil(blocks.length /
            2);
        const leftBlocks = blocks.slice(0, middle);
        const rightBlocks = blocks.slice(middle);
        const leftTranslations = translatedTexts.slice(0, middle);
        const rightTranslations = translatedTexts.slice(middle);
        console.warn(`[AUDIT CONTENT] Auditoria de ${blocks.length} bloco(s) lenta/inválida; dividindo SOMENTE a auditoria em ${leftBlocks.length} + ${rightBlocks.length}. Tradução já aprovada por ID/lock será preservada enquanto ambos os lados forem auditados.`);
        await auditBatchSemantics(leftBlocks, leftTranslations, deadlineAt, splitDepth + 1, 0);
        await auditBatchSemantics(rightBlocks, rightTranslations, deadlineAt, splitDepth + 1, 0);
        return true;
    }
}
async function translateBatchOnce(blocks, deadlineAt) {
    assertBeforeDeadline(deadlineAt);
    const raw = await enqueueGemini(buildTranslationPrompt(blocks), deadlineAt, "translation");
    let parsed;
    try {
        parsed =
            JSON.parse(stripCodeFences(raw));
    }
    catch {
        throw badModelOutputError("Gemini retornou JSON inválido.");
    }
    if (!Array.isArray(parsed)) {
        throw badModelOutputError("Gemini não retornou uma lista.");
    }
    if (parsed.length !==
        blocks.length) {
        throw badModelOutputError(`Quantidade incorreta de blocos: esperado=${blocks.length}, recebido=${parsed.length}. Resposta inteira descartada.`);
    }
    const translatedTexts = [];
    const seenIds = new Set();
    for (let i = 0; i < blocks.length; i++) {
        const block = blocks[i];
        const item = parsed[i];
        const expectedLock = blockTranslationLock(block);
        if (!item ||
            !Number.isInteger(item.id) ||
            item.id !==
                block.index ||
            seenIds.has(item.id) ||
            typeof item.lock !==
                "string" ||
            item.lock !==
                expectedLock ||
            typeof item.text !==
                "string" ||
            item.text.trim()
                .length === 0) {
            throw badModelOutputError(`Contrato ID/lock inválido na posição ${i + 1}; esperado id=${block.index}, lock=${expectedLock}. Resposta inteira descartada.`);
        }
        seenIds.add(item.id);
        translatedTexts.push(item.text);
    }
    console.log(`[AUDIT ID] OK — ${blocks.length}/${blocks.length} bloco(s); ordem, IDs e locks preservados exatamente.`);
    await auditBatchSemantics(blocks, translatedTexts, deadlineAt);
    return translatedTexts;
}
async function translateBatch(blocks, deadlineAt, splitDepth = 0, singleBlockRetry = 0) {
    assertBeforeDeadline(deadlineAt);
    try {
        return await translateBatchOnce(blocks, deadlineAt);
    }
    catch (error) {
        const splitRecoverable = error?.code ===
            "BAD_MODEL_OUTPUT" ||
            error?.code ===
                "GEMINI_REQUEST_TIMEOUT";
        if (!splitRecoverable) {
            throw error;
        }
        assertBeforeDeadline(deadlineAt);
        if (blocks.length === 1 &&
            singleBlockRetry <
                MAX_SINGLE_BLOCK_OUTPUT_RETRIES) {
            console.warn(`[TRANSLATE] Bloco atômico rejeitado pelo contrato/auditoria. Nova tentativa ${singleBlockRetry + 1}/${MAX_SINGLE_BLOCK_OUTPUT_RETRIES}.`);
            return translateBatch(blocks, deadlineAt, splitDepth, singleBlockRetry +
                1);
        }
        const canSplit = blocks.length > 1 &&
            splitDepth <
                MAX_BAD_OUTPUT_SPLIT_DEPTH;
        if (!canSplit) {
            throw error;
        }
        const middle = Math.ceil(blocks.length /
            2);
        const left = blocks.slice(0, middle);
        const right = blocks.slice(middle);
        const splitReason = error?.code ===
            "GEMINI_REQUEST_TIMEOUT"
            ? "timeout curto"
            : "contrato/auditoria";
        console.warn(`[TRANSLATE] Lote de ${blocks.length} bloco(s) rejeitado integralmente por ${splitReason}. Nenhum resultado parcial será preservado. Dividindo em ${left.length} + ${right.length}.`);
        const translatedLeft = await translateBatch(left, deadlineAt, splitDepth + 1, 0);
        const translatedRight = await translateBatch(right, deadlineAt, splitDepth + 1, 0);
        return [
            ...translatedLeft,
            ...translatedRight
        ];
    }
}
/*
|--------------------------------------------------------------------------
| TRADUÇÃO COMPLETA
|--------------------------------------------------------------------------
*/
async function translateSrt(originalSrt, job) {
    const blocks = parseSrt(originalSrt);
    if (blocks.length === 0) {
        throw new Error("Nenhum bloco SRT válido.");
    }
    console.log(`[TRANSLATE] ${blocks.length} blocos.`);
    const batches = splitIntoBatches(blocks);
    console.log(`[TRANSLATE] ${batches.length} lote(s).`);
    console.log(`[TRANSLATE] Limite: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} caracteres.`);
    const translatedTexts = new Array(blocks.length);
    const originalPositions = new Map();
    blocks.forEach((block, index) => {
        originalPositions.set(block, index);
    });
    const startedAt = Date.now();
    /*
     * 5.9 mantém a correção da 5.8.1:
     * O teto de tradução começa SOMENTE quando este job realmente
     * entra em translateSrt. Tempo aguardando translationJobQueue
     * não consome mais os 8 minutos de processamento.
     */
    const deadlineAt = startedAt +
        MAX_TRANSLATION_TIME_MS;
    job.startedAt =
        startedAt;
    job.deadlineAt =
        deadlineAt;
    job.updatedAt =
        startedAt;
    console.log(`[JOB ${job.id}] Cronômetro de tradução iniciado agora: ${MAX_TRANSLATION_TIME_MS}ms disponíveis; espera anterior na fila não contabilizada.`);
    assertBeforeDeadline(deadlineAt);
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        assertBeforeDeadline(deadlineAt);
        const batch = batches[batchIndex];
        const batchChars = batch.reduce((total, block) => total +
            String(block.text ||
                "").length, 0);
        console.log(`[TRANSLATE] Lote ${batchIndex + 1}/${batches.length} - ${batch.length} blocos / ${batchChars} caracteres.`);
        const translated = await translateBatch(batch, deadlineAt);
        assertBeforeDeadline(deadlineAt);
        for (let i = 0; i < batch.length; i++) {
            const originalIndex = originalPositions.get(batch[i]);
            if (originalIndex !==
                undefined) {
                translatedTexts[originalIndex] =
                    translated[i];
            }
        }
        job.progress =
            Math.round(((batchIndex +
                1) /
                batches.length) *
                100);
        job.completedBatches =
            batchIndex + 1;
        job.totalBatches =
            batches.length;
        job.updatedAt =
            Date.now();
        console.log(`[TRANSLATE] Lote ${batchIndex + 1}/${batches.length} concluído.`);
    }
    if (translatedTexts.some(text => typeof text !==
        "string")) {
        throw new Error("A tradução terminou com blocos ausentes.");
    }
    const elapsedMs = Date.now() -
        startedAt;
    console.log(`[TRANSLATE] Finalizada em ${(elapsedMs / 1000).toFixed(1)}s.`);
    const sanitizedTranslatedTexts = cleanAllTranslatedCharacters(translatedTexts);
    const withoutVocalElongations = cleanAllTranslatedVocalElongations(sanitizedTranslatedTexts);
    const cleanedTranslatedTexts = cleanAllTranslatedDialogueMarkers(withoutVocalElongations);
    const finalSrt = buildSrt(blocks, cleanedTranslatedTexts);
    auditFinalTimestamps(originalSrt, finalSrt, "TRADUÇÃO FINAL");
    job.timestampAuditPassed =
        true;
    job.contentAuditPassed =
        true;
    console.log("[AUDIT CONTENT] TRADUÇÃO FINAL: OK — todos os lotes passaram pela auditoria semântica EN↔PT-BR.");
    return finalSrt;
}
/*
|--------------------------------------------------------------------------
| JOBS
|--------------------------------------------------------------------------
*/
function createJob({ jobId, cacheKey, type, videoId, sourceHash, sourceSrt }) {
    const now = Date.now();
    const job = {
        id: jobId,
        cacheKey,
        type,
        videoId,
        sourceHash,
        sourceSrt,
        status: "processing",
        result: null,
        error: null,
        progress: 0,
        completedBatches: 0,
        totalBatches: 0,
        createdAt: now,
        updatedAt: now,
        deadlineAt: null,
        queuedAt: null,
        expiresAt: now +
            JOB_TTL_MS,
        timestampAuditPassed: false,
        contentAuditPassed: false,
        promise: null
    };
    jobs.set(jobId, job);
    cleanupMemory();
    return job;
}
function getJob(jobId) {
    const job = jobs.get(jobId);
    if (!job) {
        return null;
    }
    if (job.expiresAt <=
        Date.now() &&
        job.status !==
            "processing") {
        jobs.delete(jobId);
        return null;
    }
    return job;
}
async function processJob(job) {
    console.log(`[JOB ${job.id}] Iniciando.`);
    try {
        const cached = getTranslationCache(job.cacheKey);
        if (cached) {
            auditFinalTimestamps(job.sourceSrt, cached, "CACHE");
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
            console.log(`[JOB ${job.id}] Cache utilizado.`);
            return;
        }
        assertBeforeDeadline(job.deadlineAt);
        const translated = await translateSrt(job.sourceSrt, job);
        setTranslationCache(job.cacheKey, translated);
        job.result =
            translated;
        job.status =
            "completed";
        job.progress =
            100;
        job.updatedAt =
            Date.now();
        console.log(`[JOB ${job.id}] Concluído.`);
    }
    catch (error) {
        job.status =
            "failed";
        job.error =
            getErrorMessage(error);
        job.updatedAt =
            Date.now();
        console.error(`[JOB ${job.id}] Falhou: ${job.error}`);
    }
}
/*
|--------------------------------------------------------------------------
| FILA DE JOBS COMPLETOS
|--------------------------------------------------------------------------
*/
function enqueueTranslationJob(job) {
    return new Promise((resolve, reject) => {
        if (job.status !==
            "processing") {
            resolve();
            return;
        }
        job.queuedAt =
            Date.now();
        translationJobQueue.push({
            job,
            resolve,
            reject
        });
        console.log(`[JOB QUEUE] ${job.id} entrou na fila. Aguardando: ${translationJobQueue.length}.`);
        processTranslationJobQueue();
    });
}
async function processTranslationJobQueue() {
    if (translationJobWorkerRunning) {
        return;
    }
    translationJobWorkerRunning =
        true;
    try {
        while (translationJobQueue.length >
            0) {
            const item = translationJobQueue.shift();
            if (!item) {
                continue;
            }
            const { job, resolve, reject } = item;
            try {
                if (job.status !==
                    "processing") {
                    resolve();
                    continue;
                }
                const queueWaitMs = Number.isFinite(job.queuedAt)
                    ? Math.max(0, Date.now() -
                        job.queuedAt)
                    : 0;
                console.log(`[JOB QUEUE] Iniciando job completo ${job.id}. Restantes na fila: ${translationJobQueue.length}. Espera na fila: ${(queueWaitMs / 1000).toFixed(1)}s — não consumiu o teto de tradução.`);
                await processJob(job);
                resolve();
            }
            catch (error) {
                reject(error);
            }
        }
    }
    finally {
        translationJobWorkerRunning =
            false;
        if (translationJobQueue.length >
            0) {
            processTranslationJobQueue();
        }
    }
}
function startTranslationJob(job, label = "JOB") {
    if (!job) {
        return null;
    }
    if (job.status ===
        "completed" ||
        job.status ===
            "failed") {
        return job.promise;
    }
    if (job.promise) {
        return job.promise;
    }
    if (job.status ===
        "pending") {
        job.status =
            "processing";
        job.updatedAt =
            Date.now();
    }
    job.promise =
        enqueueTranslationJob(job).catch(error => {
            console.error(`[${label} ${job.id}] Erro inesperado:`, error);
            job.status =
                "failed";
            job.error =
                getErrorMessage(error);
            job.updatedAt =
                Date.now();
        });
    return job.promise;
}
function findProcessingJob(cacheKey) {
    for (const job of jobs.values()) {
        if (job.cacheKey ===
            cacheKey &&
            (job.status ===
                "processing" ||
                job.status ===
                    "pending")) {
            return job;
        }
    }
    return null;
}
function buildProcessingSrt(job) {
    const progress = Number.isFinite(job?.progress)
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
    const safe = String(message ||
        "Erro desconhecido.")
        .replace(/\s+/g, " ")
        .slice(0, 300);
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
function sendSubtitleResponse(res, srt, cacheControl = "no-store") {
    res.status(200);
    res.set({
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": cacheControl,
        "Access-Control-Allow-Origin": "*"
    });
    return res.send(srt);
}
async function waitForJob(job, timeoutMs) {
    if (job.status ===
        "completed") {
        return true;
    }
    if (job.status ===
        "failed") {
        return false;
    }
    const start = Date.now();
    while (job.status ===
        "processing" &&
        Date.now() -
            start <
            timeoutMs) {
        await sleep(500);
    }
    return (job.status ===
        "completed");
}
/*
|--------------------------------------------------------------------------
| MANIFEST STREMIO
|--------------------------------------------------------------------------
*/
const manifest = {
    id: "org.tradutor.stateless.gemini.free",
    version: "5.9.0",
    name: "Tradutor Gemini PT-BR",
    description: "Traduz automaticamente legendas para Português do Brasil usando Gemini.",
    logo: "",
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
        configurable: false,
        adult: false
    }
};
app.get("/manifest.json", (req, res) => {
    res.json(manifest);
});
app.get("/", (req, res) => {
    res.json({
        name: manifest.name,
        version: manifest.version,
        status: "online",
        model: GEMINI_MODEL,
        releaseAwareOpenSubtitles: true,
        timestampAudit: true,
        conservativeCharacterSanitization: true,
        translationCacheVersion: TRANSLATION_CACHE_VERSION,
        structuralIdLock: true,
        semanticContentAudit: SEMANTIC_AUDIT_ENABLED,
        performanceMode: "5.9-quality-fast",
        lazyOpenSubtitles: true,
        bridgeCompatibleCacheVersion: "5.8",
        translationJobQueue: translationJobQueue.length,
        activeTranslationJob: translationJobWorkerRunning,
        queue: geminiQueue.length,
        cooldownSeconds: Math.ceil(getCooldownRemaining() /
            1000)
    });
});
app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        uptime: process.uptime(),
        model: GEMINI_MODEL,
        jobs: jobs.size,
        cache: translationCache.size,
        releaseAwareOpenSubtitles: true,
        timestampAudit: true,
        conservativeCharacterSanitization: true,
        translationCacheVersion: TRANSLATION_CACHE_VERSION,
        structuralIdLock: true,
        semanticContentAudit: SEMANTIC_AUDIT_ENABLED,
        performanceMode: "5.9-quality-fast",
        lazyOpenSubtitles: true,
        bridgeCompatibleCacheVersion: "5.8",
        translationJobQueue: translationJobQueue.length,
        translationJobWorkerRunning,
        geminiQueue: geminiQueue.length,
        geminiCooldownSeconds: Math.ceil(getCooldownRemaining() /
            1000),
        batchMaxBlocks: MAX_BATCH_BLOCKS,
        batchMaxChars: MAX_BATCH_CHARS,
        requestIntervalMs: MIN_REQUEST_INTERVAL_MS,
        translationRequestTimeoutMs: TRANSLATION_REQUEST_TIMEOUT_MS,
        semanticAuditRequestTimeoutMs: SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS,
        maxOutputTokens: MAX_OUTPUT_TOKENS,
        translationTimeoutMs: MAX_TRANSLATION_TIME_MS,
        maxTranslationTimeMs: MAX_TRANSLATION_TIME_MS
    });
});
/*
|--------------------------------------------------------------------------
| OPEN SUBTITLES - RELEASE AWARE 5.7
|--------------------------------------------------------------------------
*/
function scoreSubtitle(subtitle) {
    let score = 0;
    const lang = String(subtitle?.lang || "").toLowerCase();
    if (lang === "eng") {
        score +=
            100;
    }
    else if (lang === "en") {
        score +=
            90;
    }
    if (subtitle?.hearingImpaired ===
        false) {
        score +=
            20;
    }
    if (String(subtitle?.format || "").toLowerCase() ===
        "srt") {
        score +=
            20;
    }
    if (/english/i.test(String(subtitle?.name || ""))) {
        score +=
            10;
    }
    return score;
}
function isUsableEnglishSubtitle(subtitle) {
    const lang = String(subtitle?.lang || "").toLowerCase();
    return ((lang === "eng" ||
        lang === "en") &&
        typeof subtitle?.url ===
            "string" &&
        /^https?:\/\//i.test(subtitle.url));
}
function selectBestSubtitle(subtitles, { releaseAware = false } = {}) {
    if (!Array.isArray(subtitles)) {
        return null;
    }
    const usable = subtitles.filter(isUsableEnglishSubtitle);
    if (usable.length === 0) {
        return null;
    }
    /*
     * Quando o upstream recebeu hash/tamanho/filename,
     * preservamos a ordem dele. Essa ordem carrega a relevância
     * específica do release; não deixamos a preferência por SRT/SDH
     * reordenar uma correspondência de arquivo mais precisa.
     */
    if (releaseAware) {
        return usable[0];
    }
    return usable
        .sort((a, b) => scoreSubtitle(b) -
        scoreSubtitle(a))[0] ||
        null;
}
function rawSubtitleExtraSegment(req) {
    const originalUrl = String(req.originalUrl ||
        req.url ||
        "");
    const pathname = originalUrl.split("?")[0];
    const match = pathname.match(/^\/subtitles\/[^/]+\/[^/]+\/(.+)\.json$/);
    if (match?.[1]) {
        return match[1];
    }
    return String(req.params.extra ||
        "").trim();
}
function parseStremioSubtitleExtra(req) {
    const rawExtra = rawSubtitleExtraSegment(req);
    if (!rawExtra) {
        return {
            rawExtra: "",
            videoHash: "",
            videoSize: "",
            filename: ""
        };
    }
    const params = new URLSearchParams(rawExtra);
    const rawVideoHash = String(params.get("videoHash") ||
        "").trim();
    const rawVideoSize = String(params.get("videoSize") ||
        "").trim();
    const filename = String(params.get("filename") ||
        "")
        .replace(/[\u0000-\u001F\u007F]/g, "")
        .trim()
        .slice(0, 1000);
    const videoHash = /^[a-f0-9]{16}$/i.test(rawVideoHash)
        ? rawVideoHash.toLowerCase()
        : "";
    const videoSizeNumber = Number(rawVideoSize);
    const videoSize = Number.isSafeInteger(videoSizeNumber) &&
        videoSizeNumber > 0
        ? String(videoSizeNumber)
        : "";
    return {
        rawExtra,
        videoHash,
        videoSize,
        filename
    };
}
function buildOpenSubtitlesSearchUrl(type, id, { videoHash = "", videoSize = "", filename = "" } = {}) {
    const base = `https://opensubtitles-v3.strem.io/subtitles/${encodeURIComponent(type)}/${encodeURIComponent(id)}`;
    const extra = new URLSearchParams();
    if (videoHash) {
        extra.set("videoHash", videoHash);
    }
    if (videoSize) {
        extra.set("videoSize", videoSize);
    }
    if (filename) {
        extra.set("filename", filename);
    }
    const encodedExtra = extra.toString();
    if (!encodedExtra) {
        return `${base}.json`;
    }
    return `${base}/${encodedExtra}.json`;
}
function releaseIdentityDescription(extra) {
    const parts = [];
    if (extra.videoHash) {
        parts.push("videoHash");
    }
    if (extra.videoSize) {
        parts.push("videoSize");
    }
    if (extra.filename) {
        parts.push("filename");
    }
    return parts.length
        ? parts.join(" + ")
        : "nenhuma";
}
async function findEnglishSubtitle(type, id, extra = {}) {
    const releaseAware = Boolean(extra.videoHash ||
        extra.videoSize ||
        extra.filename);
    const searchUrl = buildOpenSubtitlesSearchUrl(type, id, extra);
    console.log(`[OPENSUBTITLES MATCH] Modo: ${releaseAware ? "RELEASE-AWARE" : "GENÉRICO"}; identidade: ${releaseIdentityDescription(extra)}.`);
    console.log(`[STREMIO] Procurando legenda: ${searchUrl}`);
    const response = await fetchWithTimeout(searchUrl, {
        headers: {
            Accept: "application/json",
            "User-Agent": "Stremio-Gemini-Subtitle-Translator/5.9"
        }
    }, SOURCE_FETCH_TIMEOUT_MS);
    if (!response.ok) {
        throw new Error(`OpenSubtitles HTTP ${response.status}.`);
    }
    const data = await response.json();
    const subtitles = Array.isArray(data?.subtitles)
        ? data.subtitles
        : [];
    const target = selectBestSubtitle(subtitles, {
        releaseAware
    });
    if (target) {
        const upstreamIndex = subtitles.indexOf(target);
        console.log(`[OPENSUBTITLES MATCH] Selecionada posição upstream ${upstreamIndex >= 0 ? upstreamIndex + 1 : "?"}/${subtitles.length}; id=${String(target.id || "(sem id)")}; nome=${String(target.name || "(sem nome)")}.`);
    }
    else {
        console.log(`[OPENSUBTITLES MATCH] Nenhuma legenda inglesa utilizável retornada (${subtitles.length} resultado(s) upstream).`);
    }
    return target;
}
/*
|--------------------------------------------------------------------------
| DOWNLOAD SRT
|--------------------------------------------------------------------------
*/
async function downloadSubtitle(url) {
    console.log(`[SOURCE] Baixando legenda: ${url}`);
    const response = await fetchWithTimeout(url, {
        headers: {
            "User-Agent": "Stremio-Gemini-Subtitle-Translator/5.9"
        }
    }, SOURCE_FETCH_TIMEOUT_MS);
    if (!response.ok) {
        throw new Error(`Falha ao baixar legenda: HTTP ${response.status}.`);
    }
    const rawText = normalizeSrt(await response.text());
    if (!rawText) {
        throw new Error("Legenda vazia.");
    }
    if (rawText.length >
        MAX_SOURCE_CHARS) {
        throw new Error(`Legenda muito grande: ${rawText.length} caracteres.`);
    }
    const text = cleanSrtForTranslation(rawText);
    if (!text) {
        throw new Error("A legenda ficou vazia após a limpeza SDH/CC.");
    }
    return text;
}
/*
|--------------------------------------------------------------------------
| JOB DE LEGENDA EMBUTIDA
|--------------------------------------------------------------------------
*/
async function createEmbeddedTranslationJob({ type, videoId, sourceSrt, sourceName = "embedded" }) {
    const rawNormalizedSrt = normalizeSrt(sourceSrt);
    if (!rawNormalizedSrt) {
        throw new Error("A legenda embutida está vazia.");
    }
    if (rawNormalizedSrt.length >
        MAX_SOURCE_CHARS) {
        throw new Error(`Legenda embutida muito grande: ${rawNormalizedSrt.length} caracteres.`);
    }
    const normalizedSrt = cleanSrtForTranslation(rawNormalizedSrt);
    if (!normalizedSrt) {
        throw new Error("A legenda embutida ficou vazia após a limpeza SDH/CC.");
    }
    const blocks = parseSrt(normalizedSrt);
    if (blocks.length === 0) {
        throw new Error("A legenda embutida não possui blocos SRT válidos.");
    }
    const sourceHash = sha256(normalizedSrt);
    const cacheKey = `${TRANSLATION_CACHE_VERSION}:embedded:${sourceHash}`;
    const cached = getTranslationCache(cacheKey);
    if (cached) {
        auditFinalTimestamps(normalizedSrt, cached, "EMBEDDED CACHE");
        const cachedJobId = `embedded-cached-${sourceHash.slice(0, 24)}`;
        let cachedJob = getJob(cachedJobId);
        if (!cachedJob) {
            cachedJob =
                createJob({
                    jobId: cachedJobId,
                    cacheKey,
                    type,
                    videoId,
                    sourceHash,
                    sourceSrt: normalizedSrt
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
        console.log(`[EMBEDDED] Cache utilizado para ${sourceName}.`);
        return cachedJob;
    }
    const existingJob = findProcessingJob(cacheKey);
    if (existingJob) {
        console.log(`[EMBEDDED] Job existente reutilizado: ${existingJob.id}`);
        return existingJob;
    }
    const jobId = `embedded-${sourceHash.slice(0, 24)}-${randomId(8)}`;
    const job = createJob({
        jobId,
        cacheKey,
        type,
        videoId,
        sourceHash,
        sourceSrt: normalizedSrt
    });
    job.source =
        sourceName;
    job.totalBatches =
        splitIntoBatches(blocks).length;
    startTranslationJob(job, "EMBEDDED JOB");
    console.log(`[EMBEDDED] Novo job ${job.id} criado.`);
    return job;
}
/*
|--------------------------------------------------------------------------
| ENDPOINT SUBTITLES
|--------------------------------------------------------------------------
*/
async function subtitlesHandler(req, res) {
    const type = String(req.params.type ||
        "").trim();
    const id = String(req.params.id ||
        "").trim();
    console.log(`[STREMIO] Pedido: ${type}/${id}`);
    const extra = parseStremioSubtitleExtra(req);
    console.log(`[STREMIO EXTRA] filename: ${extra.filename || "(não enviado)"}`);
    console.log(`[STREMIO EXTRA] videoSize: ${extra.videoSize || "(não enviado)"}`);
    console.log(`[STREMIO EXTRA] videoHash: ${extra.videoHash || "(não enviado)"}`);
    if (!type ||
        !id) {
        return safeJson(res, {
            subtitles: []
        });
    }
    try {
        const target = await findEnglishSubtitle(type, id, extra);
        if (!target) {
            console.log("[STREMIO] Nenhuma legenda inglesa encontrada.");
            return safeJson(res, {
                subtitles: []
            });
        }
        const sourceSrt = await downloadSubtitle(target.url);
        const blocks = parseSrt(sourceSrt);
        if (blocks.length ===
            0) {
            return safeJson(res, {
                subtitles: []
            });
        }
        const sourceHash = sha256(sourceSrt);
        const cacheKey = `${TRANSLATION_CACHE_VERSION}:${type}:${id}:${sourceHash}`;
        const baseUrl = cleanBaseUrl(req);
        const cached = getTranslationCache(cacheKey);
        if (cached) {
            auditFinalTimestamps(sourceSrt, cached, "OPENSUBTITLES CACHE");
            const jobId = `cached-${sourceHash.slice(0, 24)}`;
            let job = getJob(jobId);
            if (!job) {
                job =
                    createJob({
                        jobId,
                        cacheKey,
                        type,
                        videoId: id,
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
            console.log("[CACHE] Tradução pronta.");
            return safeJson(res, {
                subtitles: [
                    {
                        id: `${id}-gemini-${sourceHash.slice(0, 12)}`,
                        url: `${baseUrl}/subtitle/${encodeURIComponent(jobId)}.srt`,
                        lang: "por"
                    }
                ]
            });
        }
        let job = findProcessingJob(cacheKey);
        if (!job) {
            const jobId = `job-${sourceHash.slice(0, 24)}-${randomId(8)}`;
            job =
                createJob({
                    jobId,
                    cacheKey,
                    type,
                    videoId: id,
                    sourceHash,
                    sourceSrt
                });
            job.totalBatches =
                splitIntoBatches(blocks).length;
            /*
             * 5.9 LAZY OPENSUBTITLES:
             * listar a legenda não gasta Gemini. O job só entra na fila
             * quando /subtitle/:jobId.srt for realmente requisitado.
             * Se o cliente fizer prefetch da URL, o comportamento volta
             * naturalmente ao modo anterior, sem quebrar compatibilidade.
             */
            job.status =
                "pending";
            job.lazyStart =
                true;
            job.updatedAt =
                Date.now();
            console.log(`[LAZY] Job OpenSubtitles ${job.id} criado sem consumir Gemini; aguardando acesso à URL da legenda.`);
        }
        const subtitleUrl = `${baseUrl}/subtitle/${encodeURIComponent(job.id)}.srt`;
        console.log(`[STREMIO] Subtitle URL: ${subtitleUrl}`);
        return safeJson(res, {
            subtitles: [
                {
                    id: `${id}-gemini-${sourceHash.slice(0, 12)}`,
                    url: subtitleUrl,
                    lang: "por"
                }
            ]
        });
    }
    catch (error) {
        console.error(`[STREMIO] Erro: ${getErrorMessage(error)}`);
        return safeJson(res, {
            subtitles: []
        });
    }
}
app.get("/subtitles/:type/:id.json", subtitlesHandler);
app.get("/subtitles/:type/:id/:extra.json", subtitlesHandler);
/*
|--------------------------------------------------------------------------
| API DA PONTE LOCAL - LEGENDA EMBUTIDA
|--------------------------------------------------------------------------
*/
app.post("/api/translate-embedded", async (req, res) => {
    if (!isAuthorizedLocalBridge(req)) {
        console.warn("[EMBEDDED API] Tentativa não autorizada.");
        return safeJson(res, {
            error: "Unauthorized"
        }, 401);
    }
    try {
        const { type, id, srt, name } = req.body ||
            {};
        const mediaType = String(type ||
            "unknown").trim();
        const videoId = String(id ||
            "unknown").trim();
        const sourceName = String(name ||
            "embedded").trim();
        if (!srt ||
            typeof srt !==
                "string") {
            return safeJson(res, {
                error: "Campo 'srt' é obrigatório."
            }, 400);
        }
        if (srt.length >
            MAX_SOURCE_CHARS) {
            return safeJson(res, {
                error: `SRT muito grande. Limite: ${MAX_SOURCE_CHARS} caracteres.`
            }, 413);
        }
        console.log(`[EMBEDDED API] Recebido SRT de ${sourceName} para ${mediaType}/${videoId}.`);
        const job = await createEmbeddedTranslationJob({
            type: mediaType,
            videoId,
            sourceSrt: srt,
            sourceName
        });
        const baseUrl = cleanBaseUrl(req);
        const subtitleUrl = `${baseUrl}/subtitle/${encodeURIComponent(job.id)}.srt`;
        return safeJson(res, {
            ok: true,
            jobId: job.id,
            status: job.status,
            progress: job.progress,
            subtitleUrl
        });
    }
    catch (error) {
        console.error("[EMBEDDED API] Erro:", error);
        return safeJson(res, {
            error: getErrorMessage(error)
        }, 500);
    }
});
/*
|--------------------------------------------------------------------------
| RESULTADO DA LEGENDA
|--------------------------------------------------------------------------
*/
async function subtitleResultHandler(req, res) {
    const raw = String(req.params.jobId ||
        "").trim();
    let jobId;
    try {
        jobId =
            decodeURIComponent(raw);
    }
    catch {
        jobId =
            raw;
    }
    if (!jobId) {
        return sendSubtitleResponse(res, buildErrorSrt("Job inválido."));
    }
    const job = getJob(jobId);
    if (!job) {
        return sendSubtitleResponse(res, buildErrorSrt("Esta tradução expirou. Recarregue as legendas."));
    }
    if (job.status ===
        "pending") {
        console.log(`[LAZY] URL da legenda ${job.id} requisitada; iniciando tradução agora.`);
        job.lazyStart =
            false;
        startTranslationJob(job, "JOB");
    }
    if (job.status ===
        "completed" &&
        job.result) {
        if (!job.timestampAuditPassed) {
            try {
                auditFinalTimestamps(job.sourceSrt, job.result, "SERVING");
                job.timestampAuditPassed =
                    true;
            }
            catch (error) {
                console.error(`[AUDIT TIMESTAMP] Bloqueando legenda ${job.id}: ${getErrorMessage(error)}`);
                return sendSubtitleResponse(res, buildErrorSrt("A auditoria de timestamps detectou uma divergência e bloqueou esta legenda."), "no-store");
            }
        }
        if (!job.contentAuditPassed) {
            console.error(`[AUDIT CONTENT] Bloqueando legenda ${job.id}: auditoria semântica não confirmada.`);
            return sendSubtitleResponse(res, buildErrorSrt("A auditoria semântica não confirmou a correspondência EN↔PT-BR desta legenda."), "no-store");
        }
        return sendSubtitleResponse(res, job.result, "public, max-age=604800");
    }
    if (job.status ===
        "failed") {
        return sendSubtitleResponse(res, buildErrorSrt(job.error), "no-store");
    }
    const completed = await waitForJob(job, 15000);
    if (completed &&
        job.status ===
            "completed" &&
        job.result) {
        if (!job.timestampAuditPassed) {
            try {
                auditFinalTimestamps(job.sourceSrt, job.result, "SERVING");
                job.timestampAuditPassed =
                    true;
            }
            catch (error) {
                console.error(`[AUDIT TIMESTAMP] Bloqueando legenda ${job.id}: ${getErrorMessage(error)}`);
                return sendSubtitleResponse(res, buildErrorSrt("A auditoria de timestamps detectou uma divergência e bloqueou esta legenda."), "no-store");
            }
        }
        if (!job.contentAuditPassed) {
            console.error(`[AUDIT CONTENT] Bloqueando legenda ${job.id}: auditoria semântica não confirmada.`);
            return sendSubtitleResponse(res, buildErrorSrt("A auditoria semântica não confirmou a correspondência EN↔PT-BR desta legenda."), "no-store");
        }
        return sendSubtitleResponse(res, job.result, "public, max-age=604800");
    }
    if (job.status ===
        "failed") {
        return sendSubtitleResponse(res, buildErrorSrt(job.error), "no-store");
    }
    return sendSubtitleResponse(res, buildProcessingSrt(job), "no-store, no-cache, must-revalidate");
}
app.get("/subtitle/:jobId.srt", subtitleResultHandler);
/*
|--------------------------------------------------------------------------
| START
|--------------------------------------------------------------------------
*/
app.listen(PORT, () => {
    console.log("==============================================");
    console.log(" STREMIO GEMINI SUBTITLE TRANSLATOR 5.9");
    console.log("==============================================");
    console.log(`Porta: ${PORT}`);
    console.log(`Modelo Gemini: ${GEMINI_MODEL}`);
    console.log(`PUBLIC_URL: ${PUBLIC_URL || "(automático)"}`);
    console.log(`Batch rápido máximo: ${MAX_BATCH_BLOCKS} blocos`);
    console.log(`Batch rápido máximo: ${MAX_BATCH_CHARS} caracteres`);
    console.log(`Intervalo Gemini: ${MIN_REQUEST_INTERVAL_MS}ms`);
    console.log(`Saída máxima Gemini: ${MAX_OUTPUT_TOKENS} tokens`);
    console.log(`Concorrência Gemini: ${GEMINI_CONCURRENCY}`);
    console.log(`Timeout request tradução: ${TRANSLATION_REQUEST_TIMEOUT_MS}ms`);
    console.log(`Timeout request auditoria: ${SEMANTIC_AUDIT_REQUEST_TIMEOUT_MS}ms`);
    console.log(`Teto total tradução: ${MAX_TRANSLATION_TIME_MS}ms`);
    console.log("Relógio do teto: INICIA AO COMEÇAR O PROCESSAMENTO (fila não conta)");
    console.log(`Cache TTL: ${CACHE_TTL_MS / 3600000}h`);
    console.log("Limpeza SDH/CC: ATIVA");
    console.log("Adaptação audiovisual 5.6: ATIVA");
    console.log("Localização coloquial/contextual: ATIVA");
    console.log("Contexto oculto de falante: ATIVO");
    console.log("Proteção contra chute de gênero: ATIVA");
    console.log("Normalização de alongamentos vocais: ATIVA");
    console.log("Limpeza de traços soltos: ATIVA");
    console.log("Recuperação parcial de IDs: DESATIVADA (fail-closed)");
    console.log("Trava estrutural ID + lock: ATIVA");
    console.log("Auditoria semântica EN↔PT-BR por lote: ATIVA (100% dos blocos)");
    console.log("Auditoria lenta: SPLIT SOMENTE DA AUDITORIA ✅");
    console.log("Timeout de lote: SPLIT IMEDIATO ✅");
    console.log("OpenSubtitles: LAZY / SOB DEMANDA POR URL ✅");
    console.log("Compatibilidade Ponte Local 2.5.1: SIM ✅");
    console.log(`Namespace de cache da tradução: ${TRANSLATION_CACHE_VERSION}`);
    console.log("Matching OpenSubtitles por release: ATIVO");
    console.log("Auditoria absoluta de timestamps: ATIVA");
    console.log("Higienização conservadora de caracteres: ATIVA");
    console.log("Fila de jobs completos: SEQUENCIAL (1 por vez)");
    console.log("Status: ONLINE");
    console.log("==============================================");
});
process.on("unhandledRejection", error => {
    console.error("[PROCESS] Unhandled rejection:", error);
});
process.on("uncaughtException", error => {
    console.error("[PROCESS] Uncaught exception:", error);
});
