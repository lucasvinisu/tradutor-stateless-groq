const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// CONFIG
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();

const MISTRAL_API_KEY = String(process.env.MISTRAL_API_KEY || "").trim();
const MISTRAL_MODEL = String(
    process.env.MISTRAL_MODEL ||
    "mistral-medium-3-5"
).trim();

const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();

const GEMINI_MODEL = String(
    process.env.GEMINI_MODEL ||
    "gemini-3.5-flash-lite"
).trim();

// A chave Groq pode continuar no Render,
// mas o Groq NÃO participa do pipeline 6.2.
const GROQ_API_KEY_PRESENT =
    Boolean(
        String(
            process.env.GROQ_API_KEY ||
            ""
        ).trim()
    );

const TRANSLATION_CACHE_VERSION =
    "6.2.0-final";

const MAX_SOURCE_CHARS =
    800000;

const SOURCE_FETCH_TIMEOUT_MS =
    20000;

const CACHE_TTL_MS =
    7 *
    24 *
    60 *
    60 *
    1000;

const JOB_TTL_MS =
    24 *
    60 *
    60 *
    1000;

const MAX_CACHE_ENTRIES =
    200;

const MAX_JOBS =
    300;

// ============================================================
// TIMEOUTS
// ============================================================
//
// NÃO existe teto global do episódio.
//
// Estes timeouts são somente
// de cada request de rede/API.
//

const MISTRAL_TIMEOUT_MS =
    Number(
        process.env.MISTRAL_TIMEOUT_MS ||
        150000
    );

const GEMINI_TIMEOUT_MS =
    Number(
        process.env.GEMINI_TIMEOUT_MS ||
        90000
    );

const MISTRAL_MAX_RETRIES =
    Number(
        process.env.MISTRAL_MAX_RETRIES ||
        8
    );

const GEMINI_MAX_RETRIES =
    Number(
        process.env.GEMINI_MAX_RETRIES ||
        8
    );

// ============================================================
// MISTRAL - TRADUÇÃO PRINCIPAL
// ============================================================

const MISTRAL_BATCH_CHARS =
    Number(
        process.env.MISTRAL_BATCH_CHARS ||
        18000
    );

const MISTRAL_BATCH_GROUPS =
    Number(
        process.env.MISTRAL_BATCH_GROUPS ||
        320
    );

const MISTRAL_MAX_OUTPUT_TOKENS =
    Number(
        process.env.MISTRAL_MAX_OUTPUT_TOKENS ||
        16000
    );

// Concorrência adaptativa.
// Nunca passa de 2.
const MISTRAL_MAX_CONCURRENCY =
    Math.max(
        1,
        Math.min(
            Number(
                process.env.MISTRAL_CONCURRENCY ||
                2
            ),
            2
        )
    );

// ============================================================
// RESGATE ESTRUTURAL
// ============================================================

const MISTRAL_RESCUE_GROUPS =
    Number(
        process.env.MISTRAL_RESCUE_GROUPS ||
        24
    );

const MISTRAL_RESCUE_CHARS =
    Number(
        process.env.MISTRAL_RESCUE_CHARS ||
        8000
    );

// ============================================================
// GEMINI REVIEW
// ============================================================
//
// Gemini revisa TODOS os groups.
//
// A revisão começa assim que
// cada lote Mistral termina.
//
// Portanto Gemini trabalha
// enquanto o Mistral traduz
// os lotes seguintes.
//

const GEMINI_REVIEW_MAX_OUTPUT_TOKENS =
    Number(
        process.env.GEMINI_REVIEW_MAX_OUTPUT_TOKENS ||
        10000
    );

// ============================================================
// ARBITRAGEM MISTRAL
// ============================================================

const ARBITER_BATCH_GROUPS =
    30;

const ARBITER_BATCH_CHARS =
    7000;

// ============================================================
// ESTADO
// ============================================================

const translationCache =
    new Map();

const jobs =
    new Map();

const translationJobQueue =
    [];

let translationJobWorkerRunning =
    false;

// ============================================================
// HELPERS
// ============================================================

const sleep =
    ms =>
        new Promise(
            resolve =>
                setTimeout(
                    resolve,
                    ms
                )
        );

function sha256(
    value
) {
    return crypto
        .createHash(
            "sha256"
        )
        .update(
            String(
                value
            ),
            "utf8"
        )
        .digest(
            "hex"
        );
}

function randomId(
    bytes = 8
) {
    return crypto
        .randomBytes(
            bytes
        )
        .toString(
            "hex"
        );
}

function getErrorMessage(
    error
) {
    return String(
        error?.message ||
        error ||
        "Erro desconhecido."
    );
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
        .status(
            status
        )
        .json(
            payload
        );
}

function cleanBaseUrl(
    req
) {
    if (
        PUBLIC_URL
    ) {
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
            .split(
                ","
            )[0]
            .trim();

    const host =
        String(
            req.headers[
                "x-forwarded-host"
            ] ||
            req.headers.host ||
            ""
        )
            .split(
                ","
            )[0]
            .trim();

    return `${proto}://${host}`
        .replace(
            /\/+$/,
            ""
        );
}

function normalizeSrt(
    value
) {
    return String(
        value ||
        ""
    )
        .replace(
            /^\uFEFF/,
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
        .trim();
}

function stripCodeFences(
    value
) {
    return String(
        value ||
        ""
    )
        .replace(
            /^\s*```(?:json|text|plaintext)?\s*/i,
            ""
        )
        .replace(
            /\s*```\s*$/i,
            ""
        )
        .trim();
}

function formatSeconds(
    seconds
) {
    if (
        !Number.isFinite(
            seconds
        ) ||
        seconds <
            0
    ) {
        return "?";
    }

    const total =
        Math.round(
            seconds
        );

    const min =
        Math.floor(
            total /
            60
        );

    const sec =
        total %
        60;

    return min
        ? `${min}m${String(
            sec
        ).padStart(
            2,
            "0"
        )}s`
        : `${sec}s`;
}

// ============================================================
// GOVERNADOR ADAPTATIVO DO MISTRAL
// ============================================================
//
// Objetivo:
//
// - começa permitindo 2 requests;
// - se vier 429, entra em cooldown;
// - reduz temporariamente para 1;
// - depois volta automaticamente a 2.
//
// Isso tenta ganhar velocidade
// sem transformar a API numa
// tempestade de 429.
//

class AdaptiveGovernor {

    constructor(
        maxConcurrency
    ) {
        this.maxConcurrency =
            maxConcurrency;

        this.active =
            0;

        this.cooldownUntil =
            0;

        this.serialUntil =
            0;

        this.waiters =
            [];
    }

    effectiveLimit() {
        return Date.now() <
            this.serialUntil
            ? 1
            : this.maxConcurrency;
    }

    async acquire() {

        while (
            true
        ) {
            const now =
                Date.now();

            if (
                now <
                this.cooldownUntil
            ) {
                await sleep(
                    this.cooldownUntil -
                    now
                );

                continue;
            }

            if (
                this.active <
                this.effectiveLimit()
            ) {
                this.active++;

                return;
            }

            await new Promise(
                resolve =>
                    this.waiters.push(
                        resolve
                    )
            );
        }
    }

    release() {

        this.active =
            Math.max(
                0,
                this.active -
                1
            );

        const waiters =
            this.waiters.splice(
                0
            );

        for (
            const resolve
            of waiters
        ) {
            resolve();
        }
    }

    noteRateLimit(
        waitMs
    ) {
        const now =
            Date.now();

        this.cooldownUntil =
            Math.max(
                this.cooldownUntil,
                now +
                    Math.max(
                        1000,
                        waitMs
                    )
            );

        /*
         * TPM costuma trabalhar
         * em janela de ~1 minuto.
         *
         * Depois de 429,
         * serializamos temporariamente.
         */
        this.serialUntil =
            Math.max(
                this.serialUntil,
                now +
                    60000
            );

        const waiters =
            this.waiters.splice(
                0
            );

        for (
            const resolve
            of waiters
        ) {
            resolve();
        }
    }

    status() {
        return {
            configuredMax:
                this.maxConcurrency,

            effective:
                this.effectiveLimit(),

            active:
                this.active,

            cooldownMs:
                Math.max(
                    0,
                    this.cooldownUntil -
                    Date.now()
                ),

            serializedMs:
                Math.max(
                    0,
                    this.serialUntil -
                    Date.now()
                )
        };
    }
}

const mistralGovernor =
    new AdaptiveGovernor(
        MISTRAL_MAX_CONCURRENCY
    );

// ============================================================
// PROJEÇÃO DE EFICIÊNCIA
// ============================================================
//
// É SOMENTE informativa.
//
// Nunca cancela o job.
//

function updateProjection(
    job,
    phase,
    completed,
    total,
    phaseStartedAt
) {
    if (
        !completed ||
        !total ||
        completed >
            total
    ) {
        return;
    }

    const elapsed =
        (
            Date.now() -
            phaseStartedAt
        ) /
        1000;

    const avg =
        elapsed /
        completed;

    const remaining =
        Math.max(
            0,
            avg *
                (
                    total -
                    completed
                )
        );

    const jobElapsed =
        job.processingStartedAt
            ? (
                Date.now() -
                job.processingStartedAt
            ) /
                1000
            : elapsed;

    job.projection = {
        phase,

        completed,

        total,

        averageSecondsPerBatch:
            Number(
                avg.toFixed(
                    2
                )
            ),

        estimatedRemainingSeconds:
            Math.round(
                remaining
            ),

        projectedElapsedAtPhaseEndSeconds:
            Math.round(
                jobElapsed +
                remaining
            ),

        updatedAt:
            Date.now()
    };

    console.log(
        `[EFICIÊNCIA] ${phase} ${completed}/${total} | ` +
        `média=${avg.toFixed(
            1
        )}s/lote | ` +
        `restante~${formatSeconds(
            remaining
        )} | ` +
        `projeção~${formatSeconds(
            jobElapsed +
            remaining
        )}.`
    );
}

// ============================================================
// CACHE / JOBS
// ============================================================

function cleanupMemory() {

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
            key,
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
                key
            );
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

        if (
            key ===
            undefined
        ) {
            break;
        }

        translationCache.delete(
            key
        );
    }

    if (
        jobs.size >
        MAX_JOBS
    ) {
        for (
            const [
                key,
                job
            ]
            of jobs.entries()
        ) {
            if (
                jobs.size <=
                MAX_JOBS
            ) {
                break;
            }

            if (
                job.status !==
                "processing"
            ) {
                jobs.delete(
                    key
                );
            }
        }
    }
}

setInterval(
    cleanupMemory,
    10 *
    60 *
    1000
).unref();

function getTranslationCache(
    key
) {
    const item =
        translationCache.get(
            key
        );

    if (
        !item
    ) {
        return null;
    }

    if (
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

function setTranslationCache(
    key,
    srt
) {
    translationCache.set(
        key,
        {
            srt,

            createdAt:
                Date.now(),

            expiresAt:
                Date.now() +
                CACHE_TTL_MS
        }
    );

    cleanupMemory();
}

function createJob({
    jobId,
    cacheKey,
    type,
    videoId,
    sourceHash,
    sourceSrt,
    sourceName = ""
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

        sourceName,

        status:
            "processing",

        progress:
            0,

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

        queuedAt:
            now,

        processingStartedAt:
            null,

        queueWaitSeconds:
            0,

        projection:
            null,

        timestampAuditPassed:
            false,

        contentAuditPassed:
            false,

        mistralCalls:
            0,

        mistralAttempts:
            0,

        mistral429s:
            0,

        geminiReviewCalls:
            0,

        geminiReviewAttempts:
            0,

        gemini429s:
            0,

        geminiReviewedGroups:
            0,

        geminiProposalCount:
            0,

        geminiFallbackGroups:
            0,

        arbiterCalls:
            0,

        arbiterAccepted:
            0,

        salvagedGroups:
            0,

        rescueBatchCalls:
            0,

        atomicRescues:
            0,

        highReasoningRescues:
            0,

        perCueRescues:
            0,

        qualityRepairCalls:
            0,

        qualityGuardRisks:
            0,

        resumes:
            0,

        /*
         * Checkpoint da tradução
         * Mistral por Sentence Group.
         */
        translationCheckpoint:
            new Map(),

        /*
         * SRT completo do Mistral
         * antes de propostas secundárias.
         */
        primaryCheckpoint:
            null,

        primaryCheckpointAt:
            null,

        providerUsage:
            {}
    };

    jobs.set(
        job.id,
        job
    );

    cleanupMemory();

    return job;
}

function getJob(
    id
) {
    const job =
        jobs.get(
            id
        );

    if (
        !job
    ) {
        return null;
    }

    if (
        job.expiresAt <=
            Date.now() &&
        job.status !==
            "processing"
    ) {
        jobs.delete(
            id
        );

        return null;
    }

    return job;
}

function findProcessingJob(
    cacheKey
) {
    for (
        const job
        of jobs.values()
    ) {
        if (
            job.cacheKey ===
                cacheKey &&
            job.status ===
                "processing"
        ) {
            return job;
        }
    }

    return null;
}

function findReusableFailedJob(
    cacheKey
) {
    let best =
        null;

    for (
        const job
        of jobs.values()
    ) {
        if (
            job.cacheKey !==
                cacheKey ||
            job.status !==
                "failed"
        ) {
            continue;
        }

        if (
            !best ||
            job.updatedAt >
            best.updatedAt
        ) {
            best =
                job;
        }
    }

    return best;
}

// ============================================================
// PONTE LOCAL AUTH
// ============================================================

function isAuthorizedLocalBridge(
    req
) {
    if (
        !LOCAL_BRIDGE_SECRET
    ) {
        return false;
    }

    const auth =
        String(
            req.headers
                .authorization ||
            ""
        ).trim();

    const expected =
        `Bearer ${LOCAL_BRIDGE_SECRET}`;

    if (
        !auth
    ) {
        return false;
    }

    const a =
        Buffer.from(
            auth
        );

    const b =
        Buffer.from(
            expected
        );

    return (
        a.length ===
            b.length &&
        crypto.timingSafeEqual(
            a,
            b
        )
    );
}

// ============================================================
// SRT / LIMPEZA DA FONTE
// ============================================================

const TIMING_LINE_REGEX =
    /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_HINT_MARKER_REGEX =
    /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

function normalizeSpeakerHint(
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

function encodeSpeakerHint(
    value
) {
    return encodeURIComponent(
        String(
            value ||
            ""
        )
    );
}

function decodeSpeakerHint(
    value
) {
    try {
        return normalizeSpeakerHint(
            decodeURIComponent(
                String(
                    value ||
                    ""
                )
            )
        );
    }
    catch {
        return "";
    }
}

function extractSpeakerHint(
    line
) {
    const original =
        String(
            line ||
            ""
        );

    const hidden =
        original.match(
            SPEAKER_HINT_MARKER_REGEX
        );

    if (
        hidden
    ) {
        return {
            speaker:
                decodeSpeakerHint(
                    hidden[1]
                ),

            text:
                original.replace(
                    SPEAKER_HINT_MARKER_REGEX,
                    ""
                )
        };
    }

    const bracket =
        original.match(
            /^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*:?[ \t]*/u
        );

    if (
        bracket
    ) {
        const speaker =
            normalizeSpeakerHint(
                bracket[1]
            );

        if (
            speaker
        ) {
            return {
                speaker,

                text:
                    original.slice(
                        bracket[0]
                            .length
                    )
            };
        }
    }

    const colon =
        original.match(
            /^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50})\s*:\s+(?=\S)/u
        );

    if (
        colon
    ) {
        const speaker =
            normalizeSpeakerHint(
                colon[1]
            );

        if (
            speaker
        ) {
            return {
                speaker,

                text:
                    original.slice(
                        colon[0]
                            .length
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

// ============================================================
// ALONGAMENTOS VOCAIS
// ============================================================

function normalizeVocalElongations(
    text
) {
    let value =
        String(
            text ||
            ""
        );

    /*
     * você-e-e-e-e
     * home-e-e-e-e
     */
    value =
        value.replace(
            /([A-Za-zÀ-ÖØ-öø-ÿ]+?)([-–—])([A-Za-zÀ-ÖØ-öø-ÿ])(?:\2\3){2,}/gu,
            (
                match,
                word
            ) =>
                word
        );

    /*
     * a-a-a-a
     */
    value =
        value.replace(
            /([A-Za-zÀ-ÖØ-öø-ÿ])(?:[-–—]\1){2,}[-–—]?/giu,
            "$1"
        );

    /*
     * sooooo
     * nããããão
     */
    value =
        value.replace(
            /([aeiouáéíóúãõâêô])\1{3,}/giu,
            "$1"
        );

    return value;
}

function cleanSourceLine(
    line
) {
    let text =
        String(
            line ||
            ""
        ).trim();

    if (
        !text
    ) {
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
        text.replace(
            /♪|♫|♬/gu,
            " "
        );

    text =
        normalizeVocalElongations(
            text
        );

    text =
        text
            .replace(
                /[ \t]{2,}/g,
                " "
            )
            .trim();

    if (
        !text ||
        /^[-–—/\s]*$/u.test(
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

    if (
        !normalized
    ) {
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

    let speakerBlocks =
        0;

    let elongationChanges =
        0;

    for (
        const rawBlock
        of rawBlocks
    ) {
        const lines =
            rawBlock
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
            !TIMING_LINE_REGEX.test(
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
                extractSpeakerHint(
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
                );

            const cleaned =
                cleanSourceLine(
                    before
                );

            if (
                cleaned !==
                    before.trim() &&
                /(?:[-–—][A-Za-zÀ-ÿ]){2,}|([aeiouáéíóúãõâêô])\1{3,}/iu.test(
                    before
                )
            ) {
                elongationChanges++;
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
                `@@SPK:${encodeSpeakerHint(
                    speaker
                )}@@ ${dialogue[0]}`;

            speakerBlocks++;
        }

        out.push({
            timing,
            dialogue
        });
    }

    console.log(
        `[CLEAN] SDH/CC: ${rawBlocks.length} -> ${out.length}; ` +
        `removidos=${removed}; ` +
        `speakerHints=${speakerBlocks}; ` +
        `alongamentos=${elongationChanges}.`
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

    if (
        !normalized
    ) {
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
                lines[0]
                    .trim()
            ) ||
            !TIMING_LINE_REGEX.test(
                lines[1]
                    .trim()
            )
        ) {
            continue;
        }

        const textLines =
            lines.slice(
                2
            );

        let speakerHint =
            "";

        if (
            textLines.length
        ) {
            const match =
                textLines[0]
                    .match(
                        SPEAKER_HINT_MARKER_REGEX
                    );

            if (
                match
            ) {
                speakerHint =
                    decodeSpeakerHint(
                        match[1]
                    );

                textLines[0] =
                    textLines[0]
                        .replace(
                            SPEAKER_HINT_MARKER_REGEX,
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

            speakerHint:
                speakerHint ||
                null
        });
    }

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
                    i
                ) =>
                    [
                        block.index,

                        block.timing,

                        translatedTexts[
                            i
                        ] ??
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

function auditFinalTimestamps(
    sourceSrt,
    finalSrt,
    label = "FINAL"
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
            `TIMING LOCK ${label}: quantidade diferente ${source.length}/${final.length}.`
        );
    }

    for (
        let i = 0;
        i <
        source.length;
        i++
    ) {
        if (
            source[i]
                .index !==
                final[i]
                    .index ||
            source[i]
                .timing !==
                final[i]
                    .timing
        ) {
            throw new Error(
                `TIMING LOCK ${label}: divergência no cue ${source[i].index}.`
            );
        }
    }

    console.log(
        `[AUDIT TIMESTAMP] ${label}: PASSOU — ` +
        `${source.length}/${source.length}; 0 alterações.`
    );

    return true;
}

// ============================================================
// SENTENCE GROUPS
// ============================================================

function parseTimeSeconds(
    value
) {
    const m =
        String(
            value ||
            ""
        ).match(
            /^(\d{2}):(\d{2}):(\d{2})[,.](\d{3})$/
        );

    if (
        !m
    ) {
        return NaN;
    }

    return (
        Number(
            m[1]
        ) *
            3600 +
        Number(
            m[2]
        ) *
            60 +
        Number(
            m[3]
        ) +
        Number(
            m[4]
        ) /
            1000
    );
}

function timingParts(
    timing
) {
    const m =
        String(
            timing ||
            ""
        ).match(
            /^(\d{2}:\d{2}:\d{2}[,.]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[,.]\d{3})/
        );

    return m
        ? {
            start:
                parseTimeSeconds(
                    m[1]
                ),

            end:
                parseTimeSeconds(
                    m[2]
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

function isMultiSpeakerSource(
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

    const marked =
        lines.filter(
            line =>
                /^\s*[-–—]\s*\S/u.test(
                    line
                )
        ).length;

    return (
        lines.length >=
            2 &&
        marked >=
            2
    );
}

function endsStrongSentence(
    text
) {
    return /[.!?…]["'”’\)\]\}]*$/u.test(
        groupingText(
            text
        )
    );
}

function startsLowercaseContinuation(
    text
) {
    const value =
        groupingText(
            text
        )
            .replace(
                /^[-–—]\s*/u,
                ""
            )
            .replace(
                /^["'“‘\(\[]+/u,
                ""
            )
            .trim();

    return /^[a-zà-öø-ÿ]/u.test(
        value
    );
}

function shouldMergeSentenceCue(
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

    const prev =
        group[
            group.length -
            1
        ];

    if (
        isMultiSpeakerSource(
            prev.text
        ) ||
        isMultiSpeakerSource(
            next.text
        )
    ) {
        return false;
    }

    if (
        prev.speakerHint &&
        next.speakerHint &&
        normalizeSpeakerHint(
            prev.speakerHint
        ).toLowerCase() !==
            normalizeSpeakerHint(
                next.speakerHint
            ).toLowerCase()
    ) {
        return false;
    }

    const a =
        timingParts(
            prev.timing
        );

    const b =
        timingParts(
            next.timing
        );

    const gap =
        b.start -
        a.end;

    if (
        Number.isFinite(
            gap
        ) &&
        gap >
            0.9
    ) {
        return false;
    }

    if (
        startsLowercaseContinuation(
            next.text
        )
    ) {
        return true;
    }

    const previous =
        groupingText(
            prev.text
        );

    if (
        /[,;:]$/u.test(
            previous
        )
    ) {
        return true;
    }

    if (
        !endsStrongSentence(
            prev.text
        ) &&
        /\b(?:the|to|of|or|with|for|in|at|from|that|who|which|about|into|as|than|while)\s*$/iu.test(
            previous
        )
    ) {
        return true;
    }

    return false;
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
                            isMultiSpeakerSource(
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
            !current.length
        ) {
            current.push(
                block
            );
        }
        else if (
            shouldMergeSentenceCue(
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

function compactTranslationGroup(
    group
) {
    return {
        g:
            group.groupId,

        n:
            group.cues.length,

        c:
            group.cues.map(
                cue => {

                    const item = {
                        i:
                            cue.index,

                        t:
                            cue.text
                    };

                    if (
                        cue.speakerHint
                    ) {
                        item.speaker =
                            cue.speakerHint;
                    }

                    return item;
                }
            ),

        m:
            group.multiSpeaker
                ? 1
                : 0
    };
}

function splitByBudget(
    items,
    maxChars,
    maxItems,
    itemBuilder
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
        const built =
            itemBuilder(
                item
            );

        const size =
            JSON.stringify(
                built
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

// ============================================================
// PROMPT - TRADUTOR
// ============================================================

const TRANSLATOR_SYSTEM_PROMPT = `
Você é o TRADUTOR PRINCIPAL de legendas EN→PT-BR de um pipeline de produção.

Traduza como uma excelente legenda brasileira de streaming: natural, contemporânea, oral, fiel e contextual.

Evite português literal, engessado, lusitano, datado ou com cara de tradução automática.

CONTRATO TEMPORAL ABSOLUTO:

Cada Sentence Group contém um ou mais cues consecutivos.

Traduza o grupo holisticamente para entender a frase, MAS devolva exatamente um segmento PT para cada cue EN, na mesma ordem.

O campo n informa quantos segmentos devem existir.

Cada segmento PT deve conter somente o conteúdo semanticamente pronunciado naquele cue.

Não antecipe conteúdo do próximo cue e não atrase conteúdo para outro cue.

Preserve humor, ironia, shade, camp, personalidade, vulgaridade e intensidade.

Não censure.

Adapte idioms, gírias e memes de reality, drag, LGBTQIA+, música, moda e cultura pop quando houver equivalente brasileiro natural, sem forçar internetês.

"I'm gagged" não é "estou amordaçada".

"She ate" não é comer literalmente quando for gíria.

"off the top" monetário é comissão/corte/porcentagem.

"closing ranks" pode significar grupo se protegendo/panelinha.

"Carry the two" em conta matemática é "vai dois".

Preserve nomes, marcas, títulos e catchphrases reconhecidas.

Preserve exatamente quando aparecerem:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

O campo opcional speaker é CONTEXTO OCULTO.

NUNCA escreva [NOME], NOME:, rótulo de personagem ou nome do falante.

Quando speaker/contexto identificar claramente uma pessoa, use o gênero gramatical correto.

Se o gênero não for seguro, NÃO chute masculino/feminino.

Reescreva naturalmente de forma neutra.

Nunca use:

empolgado(a)
animado(a)
ele/ela
ela/ele
barras de gênero

Não desenhe notas sustentadas.

Nada de:

você-e-e-e
amo-o-o-o
sooooo
nãããão

O áudio já carrega a duração.

Não acrescente hífen, travessão, meia-risca, barra ou marcador decorativo de diálogo.

Em cue com mais de um falante, use linhas limpas sem prefixos.

Não acrescente SDH/CC, sons, explicações ou markdown.

Traduza letras de música somente quando a letra real estiver transcrita.

Responda SOMENTE JSON:

{"items":[{"g":1,"s":["segmento cue 1","segmento cue 2"]}]}

g deve repetir exatamente o group id.

s deve ter EXATAMENTE n elementos.

Não omita groups.

Não invente groups.
`;

// ============================================================
// PROMPT - RESGATE
// ============================================================

const TRANSLATOR_RESCUE_SYSTEM_PROMPT = `
Você está fazendo RESGATE ESTRUTURAL de uma tradução EN→PT-BR.

A qualidade continua importante, mas o contrato estrutural é absoluto.

Para cada group:

g deve ser exatamente o mesmo.

n é a quantidade EXATA de strings em s.

c contém os cues na ordem temporal.

Traduza holisticamente, mas mantenha o conteúdo semanticamente no cue correspondente.

speaker é contexto oculto e nunca aparece.

Não use nomes de falante.

Não use hífens, travessões ou barras decorativas.

Não use alongamentos vocais gráficos.

Use PT-BR natural, atual, fiel e não literal.

Responda SOMENTE:

{"items":[{"g":123,"s":["...","..."]}]}
`;

// ============================================================
// PROMPT - GEMINI REVIEWER
// ============================================================

const GEMINI_REVIEWER_SYSTEM_PROMPT = `
Você é a SEGUNDA IA independente de revisão de legendas EN→PT-BR.

Você NÃO é o tradutor principal.

O PT foi produzido por Mistral Medium 3.5.

Revise TODOS os groups recebidos, comparando EN e PT cue por cue.

Proponha mudança SOMENTE quando houver erro real ou melhoria claramente necessária e de alta confiança.

Não faça mudanças cosméticas.

Não reescreva frases que já estão boas.

Procure especialmente:

1. sentido errado, omissão, antecipação ou atraso semântico;
2. conteúdo colocado no cue temporal errado;
3. português literal, engessado, lusitano ou datado;
4. idioms, gírias, memes, reality, drag, LGBTQIA+, camp, shade, música, moda e cultura pop culturalmente deslocados;
5. palavrão censurado ou reposicionado de modo artificial;
6. concordância ou gênero errado;
7. speaker vazado como [Kelly]:, Kelly: ou personagem:;
8. alongamento gráfico como você-e-e-e ou sooooo;
9. hífen, travessão ou barra decorativa de diálogo;
10. catchphrase ou termo protegido alterado.

Termos protegidos:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

speaker é pista OCULTA.

Nunca apareça na proposta.

Se speaker/contexto identifica claramente homem ou mulher, preserve o gênero correto.

Se não houver segurança, prefira frase natural sem marca de gênero.

Cada proposta deve manter EXATAMENTE a mesma quantidade de segmentos do group.

Nunca mova conteúdo entre cues.

Você deve revisar 100% dos groups do lote, mas retornar SOMENTE propostas de correção.

Responda SOMENTE JSON:

{"reviewed":123,"proposals":[{"g":12,"s":["..."],"why":"motivo curto","confidence":0.98}]}

reviewed deve ser EXATAMENTE a quantidade de groups recebidos.

Se tudo estiver bom:

{"reviewed":123,"proposals":[]}
`;

// ============================================================
// PROMPT - ÁRBITRO
// ============================================================

const ARBITER_SYSTEM_PROMPT = `
Você é o ÁRBITRO FINAL Mistral de propostas feitas por uma segunda IA.

A tradução ORIGINAL foi feita pelo próprio Mistral Medium 3.5.

O Gemini apenas sugeriu correções.

Compare para cada item:

- EN original;
- PT original Mistral;
- proposta Gemini;
- speaker oculto, quando houver;
- motivo e confiança da proposta.

REGRA PRINCIPAL:

Só aceite a proposta se ela for claramente mais correta, fiel, natural e apropriada em PT-BR.

Se houver dúvida, mantenha o PT original.

Não aceite mudança meramente cosmética.

Não deixe a proposta alterar o conteúdo temporal de um cue para outro.

Preserve exatamente a quantidade de segmentos.

Nunca exponha speaker.

Nunca introduza rótulo de falante.

Nunca introduza hífen, travessão ou barra decorativa.

Nunca introduza alongamento vocal gráfico.

Nunca chute gênero.

Responda SOMENTE JSON:

{"accepted":[{"g":123,"s":["..."],"why":"motivo curto"}]}

Retorne apenas as propostas ACEITAS.

Se nenhuma for melhor:

{"accepted":[]}
`;

// ============================================================
// PROMPT - QUALITY REPAIR
// ============================================================

const QUALITY_REPAIR_SYSTEM_PROMPT = `
Você é o reparador final de uma legenda EN→PT-BR.

Receberá somente groups onde o Quality Guard detectou risco concreto.

Use:

- EN original;
- PT atual;
- speaker oculto;
- reasons;

para corrigir SOMENTE o necessário.

Preserve exatamente g.

Preserve exatamente o número de segmentos s.

Não mova conteúdo entre cues.

PT-BR natural, atual, fiel e conciso.

Nunca exponha speaker.

Nunca use rótulo de falante.

Nunca use hífen, travessão ou barra decorativa.

Nunca use alongamento vocal gráfico.

Nunca use gênero artificial.

Responda SOMENTE:

{"items":[{"g":123,"s":["..."]}]}
`;

// ============================================================
// RETRY-AFTER
// ============================================================

function retryAfterMs(
    response,
    fallbackMs
) {
    const header =
        response
            ?.headers
            ?.get(
                "retry-after"
            );

    if (
        header
    ) {
        const seconds =
            Number(
                header
            );

        if (
            Number.isFinite(
                seconds
            ) &&
            seconds >
                0
        ) {
            return Math.min(
                Math.ceil(
                    seconds *
                    1000
                ),
                120000
            );
        }

        const date =
            Date.parse(
                header
            );

        if (
            Number.isFinite(
                date
            )
        ) {
            return Math.min(
                Math.max(
                    1000,
                    date -
                        Date.now()
                ),
                120000
            );
        }
    }

    for (
        const name
        of [
            "x-ratelimit-reset-tokens",
            "x-ratelimit-reset-requests"
        ]
    ) {
        const value =
            response
                ?.headers
                ?.get(
                    name
                );

        if (
            !value
        ) {
            continue;
        }

        const parsed =
            parseFloat(
                String(
                    value
                )
            );

        if (
            Number.isFinite(
                parsed
            ) &&
            parsed >
                0
        ) {
            return Math.min(
                Math.max(
                    1000,
                    parsed *
                        1000
                ),
                120000
            );
        }
    }

    return Math.min(
        Math.max(
            Number(
                fallbackMs
            ) ||
            5000,
            1000
        ),
        120000
    );
}

// ============================================================
// USAGE
// ============================================================

function recordUsage(
    job,
    provider,
    usage
) {
    if (
        !job ||
        !usage
    ) {
        return;
    }

    const prev =
        job.providerUsage[
            provider
        ] ||
        {
            promptTokens:
                0,

            completionTokens:
                0,

            totalTokens:
                0,

            cachedTokens:
                0
        };

    if (
        provider ===
        "GEMINI_REVIEW"
    ) {
        prev.promptTokens +=
            Number(
                usage.promptTokenCount ||
                0
            );

        prev.completionTokens +=
            Number(
                usage.candidatesTokenCount ||
                0
            );

        prev.totalTokens +=
            Number(
                usage.totalTokenCount ||
                0
            );
    }
    else {
        prev.promptTokens +=
            Number(
                usage.prompt_tokens ||
                usage.input_tokens ||
                0
            );

        prev.completionTokens +=
            Number(
                usage.completion_tokens ||
                usage.output_tokens ||
                0
            );

        prev.totalTokens +=
            Number(
                usage.total_tokens ||
                (
                    Number(
                        usage.prompt_tokens ||
                        0
                    ) +
                    Number(
                        usage.completion_tokens ||
                        0
                    )
                )
            );

        prev.cachedTokens +=
            Number(
                usage
                    ?.prompt_tokens_details
                    ?.cached_tokens ||
                0
            );
    }

    job.providerUsage[
        provider
    ] =
        prev;
}

// ============================================================
// POST JSON + RETRIES
// ============================================================

async function postJsonWithRetry({
    url,
    headers,
    body,
    timeoutMs,
    provider,
    maxRetries,
    job,
    attemptCounter,
    successCounter,
    rateCounter,
    governor = null
}) {
    let lastError =
        null;

    for (
        let attempt = 1;
        attempt <=
        maxRetries;
        attempt++
    ) {
        if (
            job &&
            attemptCounter
        ) {
            job[
                attemptCounter
            ] =
                Number(
                    job[
                        attemptCounter
                    ] ||
                    0
                ) +
                1;
        }

        let slotHeld =
            false;

        const releaseSlot =
            () => {

                if (
                    governor &&
                    slotHeld
                ) {
                    slotHeld =
                        false;

                    governor.release();
                }
            };

        if (
            governor
        ) {
            await governor.acquire();

            slotHeld =
                true;
        }

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
                `[${provider}] Request ${attempt}/${maxRetries}.`
            );

            const response =
                await fetch(
                    url,
                    {
                        method:
                            "POST",

                        headers,

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
            catch {
                data =
                    null;
            }

            if (
                response.ok &&
                data
            ) {
                if (
                    job &&
                    successCounter
                ) {
                    job[
                        successCounter
                    ] =
                        Number(
                            job[
                                successCounter
                            ] ||
                            0
                        ) +
                        1;
                }

                return data;
            }

            const message =
                data
                    ?.error
                    ?.message ||
                data
                    ?.message ||
                raw ||
                `${provider} HTTP ${response.status}`;

            const error =
                new Error(
                    `${provider} HTTP ${response.status}: ${String(
                        message
                    ).slice(
                        0,
                        1600
                    )}`
                );

            error.status =
                response.status;

            const retryable =
                response.status ===
                    408 ||
                response.status ===
                    409 ||
                response.status ===
                    425 ||
                response.status ===
                    429 ||
                response.status >=
                    500;

            if (
                response.status ===
                429
            ) {
                if (
                    job &&
                    rateCounter
                ) {
                    job[
                        rateCounter
                    ] =
                        Number(
                            job[
                                rateCounter
                            ] ||
                            0
                        ) +
                        1;
                }

                const wait =
                    retryAfterMs(
                        response,
                        Math.min(
                            4000 *
                                attempt,
                            30000
                        )
                    );

                if (
                    governor
                ) {
                    governor.noteRateLimit(
                        wait
                    );
                }

                if (
                    attempt ===
                    maxRetries
                ) {
                    throw error;
                }

                console.warn(
                    `[${provider}] HTTP 429; aguardando ${Math.ceil(
                        wait /
                        1000
                    )}s.`
                );

                releaseSlot();

                clearTimeout(
                    timer
                );

                await sleep(
                    wait
                );

                continue;
            }

            if (
                !retryable ||
                attempt ===
                    maxRetries
            ) {
                throw error;
            }

            const wait =
                retryAfterMs(
                    response,
                    Math.min(
                        3000 *
                            attempt,
                        20000
                    )
                );

            console.warn(
                `[${provider}] HTTP ${response.status}; aguardando ${Math.ceil(
                    wait /
                    1000
                )}s.`
            );

            releaseSlot();

            clearTimeout(
                timer
            );

            await sleep(
                wait
            );

            continue;
        }
        catch (
            error
        ) {
            lastError =
                error?.name ===
                    "AbortError"
                    ? new Error(
                        `${provider}: timeout por request.`
                    )
                    : error;

            if (
                lastError?.status &&
                ![
                    408,
                    409,
                    425,
                    429
                ].includes(
                    lastError.status
                ) &&
                lastError.status <
                    500
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
                    20000
                );

            console.warn(
                `[${provider}] ${getErrorMessage(
                    lastError
                ).slice(
                    0,
                    250
                )}; ` +
                `retry em ${(wait / 1000).toFixed(
                    1
                )}s.`
            );

            releaseSlot();

            clearTimeout(
                timer
            );

            await sleep(
                wait
            );

            continue;
        }
        finally {
            clearTimeout(
                timer
            );

            releaseSlot();
        }
    }

    throw (
        lastError ||
        new Error(
            `${provider}: falha desconhecida.`
        )
    );
}

// ============================================================
// MISTRAL
// ============================================================

function extractMistralText(
    content
) {
    if (
        typeof content ===
        "string"
    ) {
        return content;
    }

    if (
        Array.isArray(
            content
        )
    ) {
        return content
            .filter(
                item =>
                    item?.type ===
                        "text" &&
                    typeof item.text ===
                        "string"
            )
            .map(
                item =>
                    item.text
            )
            .join(
                ""
            );
    }

    return "";
}

async function mistralChat({
    systemPrompt,
    userPrompt,
    job,
    reasoningEffort =
        "none",
    temperature =
        0.1,
    maxTokens =
        MISTRAL_MAX_OUTPUT_TOKENS,
    purpose =
        "translation"
}) {
    if (
        !MISTRAL_API_KEY
    ) {
        throw new Error(
            "MISTRAL_API_KEY não configurada."
        );
    }

    const providerName =
        purpose ===
        "arbiter"
            ? "MISTRAL_ARBITER"
            : purpose ===
            "repair"
                ? "MISTRAL_REPAIR"
                : "MISTRAL";

    const data =
        await postJsonWithRetry({
            url:
                "https://api.mistral.ai/v1/chat/completions",

            headers: {
                "Content-Type":
                    "application/json",

                Authorization:
                    `Bearer ${MISTRAL_API_KEY}`
            },

            body: {
                model:
                    MISTRAL_MODEL,

                messages: [
                    {
                        role:
                            "system",

                        content:
                            systemPrompt
                    },

                    {
                        role:
                            "user",

                        content:
                            userPrompt
                    }
                ],

                response_format: {
                    type:
                        "json_object"
                },

                reasoning_effort:
                    reasoningEffort,

                temperature,

                max_tokens:
                    maxTokens,

                prompt_cache_key:
                    purpose ===
                    "translation"
                        ? "stremio-ptbr-6-2-translator"
                        : "stremio-ptbr-6-2-editor"
            },

            timeoutMs:
                MISTRAL_TIMEOUT_MS,

            provider:
                providerName,

            maxRetries:
                MISTRAL_MAX_RETRIES,

            job,

            attemptCounter:
                "mistralAttempts",

            successCounter:
                purpose ===
                "arbiter"
                    ? "arbiterCalls"
                    : "mistralCalls",

            rateCounter:
                "mistral429s",

            governor:
                mistralGovernor
        });

    recordUsage(
        job,
        purpose ===
            "arbiter"
            ? "MISTRAL_ARBITER"
            : "MISTRAL",
        data.usage ||
        {}
    );

    const text =
        extractMistralText(
            data
                ?.choices
                ?.[0]
                ?.message
                ?.content
        );

    if (
        !text
    ) {
        throw new Error(
            "Mistral retornou resposta vazia."
        );
    }

    return text;
}

async function mistralTranslateGroups(
    groups,
    job,
    rescue = false
) {
    const payload =
        groups.map(
            compactTranslationGroup
        );

    return mistralChat({
        systemPrompt:
            rescue
                ? TRANSLATOR_RESCUE_SYSTEM_PROMPT
                : TRANSLATOR_SYSTEM_PROMPT,

        userPrompt:
            `${rescue
                ? "Resgate somente estes groups."
                : "Traduza este lote."
            }\n` +
            `JSON de entrada:\n${JSON.stringify(
                {
                    groups:
                        payload
                }
            )}`,

        job,

        reasoningEffort:
            "none",

        temperature:
            rescue
                ? 0
                : 0.1,

        purpose:
            "translation"
    });
}

function parseTranslationResponse(
    groups,
    raw
) {
    const expected =
        new Map(
            groups.map(
                group => [
                    group.groupId,
                    group
                ]
            )
        );

    const valid =
        new Map();

    const issues =
        [];

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
            valid,

            invalidGroups:
                groups.slice(),

            issues: [
                "JSON_INVALID"
            ]
        };
    }

    let items =
        [];

    if (
        Array.isArray(
            parsed
        )
    ) {
        items =
            parsed;
    }
    else if (
        Array.isArray(
            parsed?.items
        )
    ) {
        items =
            parsed.items;
    }
    else if (
        parsed &&
        typeof parsed ===
            "object" &&
        parsed.g !=
            null
    ) {
        items = [
            parsed
        ];
    }
    else {
        return {
            valid,

            invalidGroups:
                groups.slice(),

            issues: [
                "ITEMS_MISSING"
            ]
        };
    }

    for (
        const item
        of items
    ) {
        const groupId =
            Number(
                item?.g ??
                item?.groupId ??
                item?.id
            );

        const group =
            expected.get(
                groupId
            );

        if (
            !group ||
            valid.has(
                groupId
            )
        ) {
            continue;
        }

        let segments =
            item?.s ??
            item?.segments;

        if (
            !Array.isArray(
                segments
            ) &&
            group.cues.length ===
                1 &&
            typeof (
                item?.text ??
                item?.translation
            ) ===
                "string"
        ) {
            segments = [
                item.text ??
                item.translation
            ];
        }

        if (
            !Array.isArray(
                segments
            )
        ) {
            issues.push(
                `g${groupId}:sem-array`
            );

            continue;
        }

        if (
            segments.length !==
            group.cues.length
        ) {
            issues.push(
                `g${groupId}:esperava=${group.cues.length},recebeu=${segments.length}`
            );

            continue;
        }

        if (
            segments.some(
                text =>
                    typeof text !==
                        "string" ||
                    !text.trim()
            )
        ) {
            issues.push(
                `g${groupId}:segmento-vazio`
            );

            continue;
        }

        valid.set(
            groupId,

            segments.map(
                text =>
                    String(
                        text
                    ).trim()
            )
        );
    }

    const invalidGroups =
        groups.filter(
            group =>
                !valid.has(
                    group.groupId
                )
        );

    if (
        invalidGroups.length
    ) {
        issues.push(
            `faltando=${invalidGroups
                .map(
                    group =>
                        group.groupId
                )
                .join(
                    ","
                )}`
        );
    }

    return {
        valid,
        invalidGroups,
        issues
    };
}

async function mistralAtomicGroup(
    group,
    job,
    reasoningEffort =
        "none"
) {
    const raw =
        await mistralChat({
            systemPrompt:
                TRANSLATOR_RESCUE_SYSTEM_PROMPT,

            userPrompt:
                `TRADUZA EXATAMENTE UM GROUP.\n` +
                `group=${group.groupId}; ` +
                `cueIds=${JSON.stringify(
                    group.cues.map(
                        cue =>
                            cue.index
                    )
                )}; ` +
                `n=${group.cues.length}.\n` +
                `A saída deve conter exatamente ${group.cues.length} string(s) em s.\n` +
                `Entrada:\n${JSON.stringify(
                    compactTranslationGroup(
                        group
                    )
                )}`,

            job,

            reasoningEffort,

            temperature:
                0,

            maxTokens:
                4000,

            purpose:
                "translation"
        });

    return parseTranslationResponse(
        [
            group
        ],
        raw
    );
}

async function mistralSingleCueRescue(
    group,
    cuePosition,
    job
) {
    const cue =
        group.cues[
            cuePosition
        ];

    const context =
        group.cues.map(
            (
                item,
                index
            ) => ({
                pos:
                    index +
                    1,

                id:
                    item.index,

                text:
                    item.text,

                speaker:
                    item.speakerHint ||
                    undefined,

                target:
                    index ===
                    cuePosition
            })
        );

    const raw =
        await mistralChat({
            systemPrompt: `
Você está fazendo o último RESGATE de um único cue de legenda EN→PT-BR.

Use todo o grupo apenas como contexto, mas traduza SOMENTE o cue target=true.

Não antecipe nem atrase conteúdo de outros cues.

Use PT-BR natural, contemporâneo e fiel.

speaker é contexto oculto e nunca aparece.

Não use rótulo de falante.

Não use hífen, travessão ou barra decorativa.

Não use alongamento vocal gráfico.

Responda SOMENTE JSON:

{"text":"tradução do cue target"}
`,

            userPrompt:
                `group=${group.groupId}; ` +
                `targetCue=${cue.index}.\n` +
                `Contexto:\n${JSON.stringify(
                    context
                )}`,

            job,

            reasoningEffort:
                "high",

            temperature:
                0,

            maxTokens:
                1800,

            purpose:
                "translation"
        });

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
            `Resgate por cue retornou JSON inválido no group ${group.groupId}, cue ${cue.index}.`
        );
    }

    const text =
        parsed?.text ??
        parsed?.translation ??
        parsed?.s?.[0];

    if (
        typeof text !==
            "string" ||
        !text.trim()
    ) {
        throw new Error(
            `Resgate por cue vazio no group ${group.groupId}, cue ${cue.index}.`
        );
    }

    job.perCueRescues++;

    return String(
        text
    ).trim();
}

async function rescueSingleGroup(
    group,
    job
) {
    job.atomicRescues++;

    console.warn(
        `[MISTRAL RESCUE] Group ${group.groupId} isolado; ` +
        `cues=${group.cues
            .map(
                cue =>
                    cue.index
            )
            .join(
                ","
            )}.`
    );

    let parsed =
        await mistralAtomicGroup(
            group,
            job,
            "none"
        );

    if (
        parsed.valid.has(
            group.groupId
        )
    ) {
        return parsed.valid.get(
            group.groupId
        );
    }

    console.warn(
        `[MISTRAL RESCUE] Group ${group.groupId} ainda inválido; reasoning=high.`
    );

    job.highReasoningRescues++;

    parsed =
        await mistralAtomicGroup(
            group,
            job,
            "high"
        );

    if (
        parsed.valid.has(
            group.groupId
        )
    ) {
        return parsed.valid.get(
            group.groupId
        );
    }

    console.warn(
        `[MISTRAL RESCUE] Group ${group.groupId} persistente; resgate por cue.`
    );

    const segments =
        [];

    for (
        let i = 0;
        i <
        group.cues.length;
        i++
    ) {
        segments.push(
            await mistralSingleCueRescue(
                group,
                i,
                job
            )
        );
    }

    return segments;
}

async function translateGroupBatchResilient(
    groups,
    job
) {
    const result =
        new Map();

    const raw =
        await mistralTranslateGroups(
            groups,
            job,
            false
        );

    const parsed =
        parseTranslationResponse(
            groups,
            raw
        );

    for (
        const [
            groupId,
            segments
        ]
        of parsed.valid
    ) {
        result.set(
            groupId,
            segments
        );
    }

    const pending =
        parsed.invalidGroups;

    if (
        !pending.length
    ) {
        return result;
    }

    job.salvagedGroups +=
        parsed.valid.size;

    console.warn(
        `[MISTRAL SALVAGE] Lote parcial: ` +
        `válidos=${parsed.valid.size}/${groups.length}; ` +
        `resgatar=${pending.length}; ` +
        `${parsed.issues
            .slice(
                0,
                8
            )
            .join(
                " | "
            )}`
    );

    const rescueBatches =
        splitByBudget(
            pending,
            MISTRAL_RESCUE_CHARS,
            MISTRAL_RESCUE_GROUPS,
            compactTranslationGroup
        );

    const stillInvalid =
        [];

    for (
        const rescueBatch
        of rescueBatches
    ) {
        job.rescueBatchCalls++;

        if (
            rescueBatch.length ===
            1
        ) {
            const group =
                rescueBatch[0];

            result.set(
                group.groupId,

                await rescueSingleGroup(
                    group,
                    job
                )
            );

            continue;
        }

        console.warn(
            `[MISTRAL RESCUE] Lote pequeno com ${rescueBatch.length} group(s).`
        );

        const rescueRaw =
            await mistralTranslateGroups(
                rescueBatch,
                job,
                true
            );

        const rescueParsed =
            parseTranslationResponse(
                rescueBatch,
                rescueRaw
            );

        for (
            const [
                groupId,
                segments
            ]
            of rescueParsed.valid
        ) {
            result.set(
                groupId,
                segments
            );
        }

        stillInvalid.push(
            ...rescueParsed
                .invalidGroups
        );
    }

    for (
        const group
        of stillInvalid
    ) {
        if (
            !result.has(
                group.groupId
            )
        ) {
            result.set(
                group.groupId,

                await rescueSingleGroup(
                    group,
                    job
                )
            );
        }
    }

    const missing =
        groups.filter(
            group =>
                !result.has(
                    group.groupId
                )
        );

    if (
        missing.length
    ) {
        throw new Error(
            `Resgate estrutural incompleto: groups ${missing
                .map(
                    group =>
                        group.groupId
                )
                .join(
                    ","
                )}.`
        );
    }

    return result;
}

// ============================================================
// COMPACT REVIEW GROUP
// ============================================================

function compactReviewGroup(
    group,
    segments
) {
    const item = {
        g:
            group.groupId,

        en:
            group.cues.map(
                cue =>
                    cue.text
            ),

        pt:
            segments
    };

    const speakers =
        group.cues.map(
            cue =>
                cue.speakerHint ||
                null
        );

    if (
        speakers.some(
            Boolean
        )
    ) {
        item.speaker =
            speakers;
    }

    return item;
}

// ============================================================
// GEMINI
// ============================================================

function extractGeminiText(
    data
) {
    return (
        data
            ?.candidates
            ?.[0]
            ?.content
            ?.parts ||
        []
    )
        .map(
            part =>
                typeof part?.text ===
                    "string"
                    ? part.text
                    : ""
        )
        .join(
            ""
        )
        .trim();
}

async function geminiReviewRequest(
    entries,
    job
) {
    if (
        !GEMINI_API_KEY
    ) {
        throw new Error(
            "GEMINI_API_KEY não configurada."
        );
    }

    const payload =
        entries.map(
            entry =>
                compactReviewGroup(
                    entry.group,
                    entry.segments
                )
        );

    const data =
        await postJsonWithRetry({
            url:
                `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
                    GEMINI_MODEL
                )}:generateContent`,

            headers: {
                "Content-Type":
                    "application/json",

                "x-goog-api-key":
                    GEMINI_API_KEY
            },

            body: {
                systemInstruction: {
                    parts: [
                        {
                            text:
                                GEMINI_REVIEWER_SYSTEM_PROMPT
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
                                    `Revise os ${entries.length} groups abaixo.\n` +
                                    `JSON de entrada:\n${JSON.stringify(
                                        {
                                            groups:
                                                payload
                                        }
                                    )}`
                            }
                        ]
                    }
                ],

                /*
                 * Legenda de reality
                 * pode conter palavrão,
                 * sexualidade, shade etc.
                 *
                 * Isso é tradução/revisão,
                 * não geração nociva.
                 */
                safetySettings: [
                    {
                        category:
                            "HARM_CATEGORY_HARASSMENT",

                        threshold:
                            "BLOCK_NONE"
                    },

                    {
                        category:
                            "HARM_CATEGORY_HATE_SPEECH",

                        threshold:
                            "BLOCK_NONE"
                    },

                    {
                        category:
                            "HARM_CATEGORY_SEXUALLY_EXPLICIT",

                        threshold:
                            "BLOCK_NONE"
                    },

                    {
                        category:
                            "HARM_CATEGORY_DANGEROUS_CONTENT",

                        threshold:
                            "BLOCK_NONE"
                    }
                ],

                generationConfig: {
                    temperature:
                        0,

                    responseMimeType:
                        "application/json",

                    maxOutputTokens:
                        GEMINI_REVIEW_MAX_OUTPUT_TOKENS
                }
            },

            timeoutMs:
                GEMINI_TIMEOUT_MS,

            provider:
                "GEMINI_REVIEW",

            maxRetries:
                GEMINI_MAX_RETRIES,

            job,

            attemptCounter:
                "geminiReviewAttempts",

            successCounter:
                "geminiReviewCalls",

            rateCounter:
                "gemini429s"
        });

    recordUsage(
        job,
        "GEMINI_REVIEW",
        data.usageMetadata ||
        {}
    );

    const text =
        extractGeminiText(
            data
        );

    if (
        !text
    ) {
        throw new Error(
            "Gemini reviewer retornou resposta vazia."
        );
    }

    return text;
}

function parseGeminiReview(
    entries,
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
        const error =
            new Error(
                "Gemini reviewer retornou JSON inválido."
            );

        error.code =
            "BAD_GEMINI_OUTPUT";

        throw error;
    }

    if (
        Number(
            parsed?.reviewed
        ) !==
            entries.length ||
        !Array.isArray(
            parsed?.proposals
        )
    ) {
        const error =
            new Error(
                `Gemini reviewer não confirmou revisão completa: ` +
                `reviewed=${parsed?.reviewed}, esperado=${entries.length}.`
            );

        error.code =
            "BAD_GEMINI_OUTPUT";

        throw error;
    }

    const expected =
        new Map(
            entries.map(
                entry => [
                    entry
                        .group
                        .groupId,
                    entry
                ]
            )
        );

    const seen =
        new Set();

    const proposals =
        [];

    for (
        const proposal
        of parsed.proposals
    ) {
        const groupId =
            Number(
                proposal?.g ??
                proposal?.groupId
            );

        const entry =
            expected.get(
                groupId
            );

        if (
            !entry ||
            seen.has(
                groupId
            )
        ) {
            continue;
        }

        let segments =
            proposal?.s ??
            proposal?.segments;

        if (
            !Array.isArray(
                segments
            ) &&
            entry.group.cues.length ===
                1 &&
            typeof (
                proposal?.text ??
                proposal?.translation
            ) ===
                "string"
        ) {
            segments = [
                proposal.text ??
                proposal.translation
            ];
        }

        if (
            !Array.isArray(
                segments
            ) ||
            segments.length !==
                entry.group.cues.length ||
            segments.some(
                text =>
                    typeof text !==
                        "string" ||
                    !text.trim()
            )
        ) {
            continue;
        }

        seen.add(
            groupId
        );

        const cleaned =
            segments.map(
                text =>
                    String(
                        text
                    ).trim()
            );

        if (
            JSON.stringify(
                cleaned
            ) ===
            JSON.stringify(
                entry.segments
            )
        ) {
            continue;
        }

        proposals.push({
            g:
                groupId,

            s:
                cleaned,

            why:
                String(
                    proposal?.why ||
                    "correção sugerida"
                ).slice(
                    0,
                    240
                ),

            confidence:
                Math.max(
                    0,
                    Math.min(
                        1,
                        Number(
                            proposal?.confidence ??
                            0.9
                        )
                    )
                ),

            group:
                entry.group,

            original:
                entry.segments
        });
    }

    return proposals;
}

// ============================================================
// CONTINGÊNCIA DE REVIEW
// ============================================================
//
// Caso Gemini realmente fique
// indisponível depois de todos
// os retries:
//
// nenhum group fica "sem review".
//
// Mistral faz contingência,
// em lotes pequenos.
//
// Normalmente esperamos:
//
// GeminiFallbackGroups = 0
//

async function mistralFallbackReview(
    entries,
    job
) {
    job.geminiFallbackGroups +=
        entries.length;

    const batches =
        splitByBudget(
            entries,
            7000,
            30,
            entry =>
                compactReviewGroup(
                    entry.group,
                    entry.segments
                )
        );

    const all =
        [];

    for (
        const batch
        of batches
    ) {
        const payload =
            batch.map(
                entry =>
                    compactReviewGroup(
                        entry.group,
                        entry.segments
                    )
            );

        const raw =
            await mistralChat({
                systemPrompt: `
Você é um revisor de contingência de legendas EN→PT-BR.

Revise TODOS os groups recebidos.

A tradução já foi feita pelo Mistral.

Não reescreva o que está bom.

Proponha somente correções reais e de alta confiança:

- sentido;
- temporalidade;
- naturalidade;
- gênero;
- gíria/cultura;
- palavrão;
- speaker vazado;
- alongamento;
- marcadores de diálogo.

Preserve exatamente o número de segmentos de cada group.

speaker é contexto oculto.

Responda SOMENTE JSON:

{"reviewed":123,"proposals":[{"g":12,"s":["..."],"why":"motivo","confidence":0.95}]}
`,

                userPrompt:
                    `Revise os ${batch.length} groups:\n` +
                    `${JSON.stringify(
                        {
                            groups:
                                payload
                        }
                    )}`,

                job,

                reasoningEffort:
                    "none",

                temperature:
                    0,

                maxTokens:
                    5000,

                purpose:
                    "repair"
            });

        all.push(
            ...parseGeminiReview(
                batch,
                raw
            )
        );
    }

    return all;
}

async function geminiReviewBatchResilient(
    entries,
    job,
    depth = 0
) {
    try {
        const raw =
            await geminiReviewRequest(
                entries,
                job
            );

        const proposals =
            parseGeminiReview(
                entries,
                raw
            );

        job.geminiReviewedGroups +=
            entries.length;

        return proposals;
    }
    catch (
        error
    ) {
        const message =
            getErrorMessage(
                error
            );

        const splittable =
            error?.code ===
                "BAD_GEMINI_OUTPUT" ||
            error?.status ===
                400 ||
            error?.status ===
                413 ||
            /JSON inválido|revisão completa|response too large|too large/i.test(
                message
            );

        if (
            entries.length >
                1 &&
            depth <
                7 &&
            splittable
        ) {
            const middle =
                Math.ceil(
                    entries.length /
                    2
                );

            console.warn(
                `[GEMINI REVIEW] Split estrutural ${entries.length} -> ` +
                `${middle}+${entries.length - middle}.`
            );

            const left =
                await geminiReviewBatchResilient(
                    entries.slice(
                        0,
                        middle
                    ),
                    job,
                    depth +
                        1
                );

            const right =
                await geminiReviewBatchResilient(
                    entries.slice(
                        middle
                    ),
                    job,
                    depth +
                        1
                );

            return [
                ...left,
                ...right
            ];
        }

        console.warn(
            `[GEMINI REVIEW] Falha persistente em ${entries.length} group(s); ` +
            `revisão de contingência Mistral. ` +
            `${message.slice(
                0,
                220
            )}`
        );

        const proposals =
            await mistralFallbackReview(
                entries,
                job
            );

        job.geminiReviewedGroups +=
            entries.length;

        return proposals;
    }
}

// ============================================================
// ARBITRAGEM MISTRAL
// ============================================================

function proposalPayload(
    proposal
) {
    const speakers =
        proposal
            .group
            .cues
            .map(
                cue =>
                    cue.speakerHint ||
                    null
            );

    const item = {
        g:
            proposal.g,

        en:
            proposal
                .group
                .cues
                .map(
                    cue =>
                        cue.text
                ),

        original:
            proposal.original,

        proposed:
            proposal.s,

        why:
            proposal.why,

        confidence:
            proposal.confidence
    };

    if (
        speakers.some(
            Boolean
        )
    ) {
        item.speaker =
            speakers;
    }

    return item;
}

function parseArbiterResponse(
    batch,
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
        const error =
            new Error(
                "Mistral árbitro retornou JSON inválido."
            );

        error.code =
            "BAD_ARBITER_OUTPUT";

        throw error;
    }

    if (
        !Array.isArray(
            parsed?.accepted
        )
    ) {
        const error =
            new Error(
                "Mistral árbitro não retornou accepted[]."
            );

        error.code =
            "BAD_ARBITER_OUTPUT";

        throw error;
    }

    const expected =
        new Map(
            batch.map(
                proposal => [
                    proposal.g,
                    proposal
                ]
            )
        );

    const accepted =
        [];

    const seen =
        new Set();

    for (
        const item
        of parsed.accepted
    ) {
        const groupId =
            Number(
                item?.g ??
                item?.groupId
            );

        const proposal =
            expected.get(
                groupId
            );

        if (
            !proposal ||
            seen.has(
                groupId
            )
        ) {
            continue;
        }

        let segments =
            item?.s ??
            item?.segments;

        if (
            !Array.isArray(
                segments
            ) &&
            proposal.group.cues.length ===
                1 &&
            typeof (
                item?.text ??
                item?.translation
            ) ===
                "string"
        ) {
            segments = [
                item.text ??
                item.translation
            ];
        }

        if (
            !Array.isArray(
                segments
            ) ||
            segments.length !==
                proposal.group.cues.length ||
            segments.some(
                text =>
                    typeof text !==
                        "string" ||
                    !text.trim()
            )
        ) {
            continue;
        }

        seen.add(
            groupId
        );

        accepted.push({
            g:
                groupId,

            s:
                segments.map(
                    text =>
                        String(
                            text
                        ).trim()
                ),

            why:
                String(
                    item?.why ||
                    "aceita pelo árbitro"
                ).slice(
                    0,
                    240
                )
        });
    }

    return accepted;
}

async function arbitrateBatchResilient(
    batch,
    job,
    depth = 0
) {
    try {
        const raw =
            await mistralChat({
                systemPrompt:
                    ARBITER_SYSTEM_PROMPT,

                userPrompt:
                    `Avalie estas ${batch.length} propostas:\n` +
                    `${JSON.stringify(
                        {
                            proposals:
                                batch.map(
                                    proposalPayload
                                )
                        }
                    )}`,

                job,

                reasoningEffort:
                    "none",

                temperature:
                    0,

                maxTokens:
                    5000,

                purpose:
                    "arbiter"
            });

        return parseArbiterResponse(
            batch,
            raw
        );
    }
    catch (
        error
    ) {
        if (
            batch.length >
                1 &&
            depth <
                7
        ) {
            const middle =
                Math.ceil(
                    batch.length /
                    2
                );

            console.warn(
                `[MISTRAL ARBITER] Split ${batch.length} -> ` +
                `${middle}+${batch.length - middle} por ` +
                `${getErrorMessage(
                    error
                ).slice(
                    0,
                    120
                )}.`
            );

            const left =
                await arbitrateBatchResilient(
                    batch.slice(
                        0,
                        middle
                    ),
                    job,
                    depth +
                        1
                );

            const right =
                await arbitrateBatchResilient(
                    batch.slice(
                        middle
                    ),
                    job,
                    depth +
                        1
                );

            return [
                ...left,
                ...right
            ];
        }

        /*
         * Segurança editorial:
         *
         * se não conseguimos provar
         * que a proposta é melhor,
         * o Mistral ORIGINAL vence.
         */
        console.warn(
            `[MISTRAL ARBITER] Proposta g=${batch[0]?.g || "?"} ` +
            `mantida como ORIGINAL por falha de arbitragem: ` +
            `${getErrorMessage(
                error
            ).slice(
                0,
                200
            )}`
        );

        return [];
    }
}

async function arbitrateAllProposals(
    proposals,
    translations,
    job
) {
    if (
        !proposals.length
    ) {
        console.log(
            "[MISTRAL ARBITER] Gemini propôs 0 alterações."
        );

        return [];
    }

    const batches =
        splitByBudget(
            proposals,
            ARBITER_BATCH_CHARS,
            ARBITER_BATCH_GROUPS,
            proposalPayload
        );

    console.log(
        `[MISTRAL ARBITER] ${proposals.length} proposta(s) Gemini -> ` +
        `${batches.length} lote(s) de arbitragem.`
    );

    const accepted =
        [];

    for (
        let i = 0;
        i <
        batches.length;
        i++
    ) {
        const result =
            await arbitrateBatchResilient(
                batches[i],
                job
            );

        for (
            const item
            of result
        ) {
            translations.set(
                item.g,
                item.s
            );

            accepted.push(
                item
            );
        }

        console.log(
            `[MISTRAL ARBITER] ${i + 1}/${batches.length}: ` +
            `${result.length} aceita(s).`
        );
    }

    job.arbiterAccepted =
        accepted.length;

    return accepted;
}

// ============================================================
// FINAL TEXT CLEANING
// ============================================================

function escapeRegExp(
    value
) {
    return String(
        value ||
        ""
    ).replace(
        /[.*+?^${}()|[\]\\]/g,
        "\\$&"
    );
}

function cleanFinalText(
    text,
    speakerHint
) {
    let value =
        normalizeVocalElongations(
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

    const speaker =
        normalizeSpeakerHint(
            speakerHint ||
            ""
        );

    const speakerRegex =
        speaker
            ? new RegExp(
                `^\\s*(?:\\[\\s*)?${escapeRegExp(
                    speaker
                )}(?:\\s*\\])?\\s*:\\s*`,
                "iu"
            )
            : null;

    value =
        value
            .split(
                "\n"
            )
            .map(
                line => {

                    let clean =
                        String(
                            line ||
                            ""
                        );

                    clean =
                        clean.replace(
                            /^\s*\[[^\]]{1,60}\]\s*:?[ \t]*/u,
                            ""
                        );

                    if (
                        speakerRegex
                    ) {
                        clean =
                            clean.replace(
                                speakerRegex,
                                ""
                            );
                    }

                    clean =
                        clean.replace(
                            /^\s*[A-ZÀ-Ý][\p{L}0-9.'’_-]*(?:\s+[A-ZÀ-Ý][\p{L}0-9.'’_-]*){0,3}\s*:\s+(?=\S)/u,
                            ""
                        );

                    clean =
                        clean.replace(
                            /^\s*[-–—/]+\s*(?=\S)/u,
                            ""
                        );

                    return clean
                        .replace(
                            /[♪♫♬]+/gu,
                            ""
                        )
                        .replace(
                            /[ \t]{2,}/g,
                            " "
                        )
                        .trim();
                }
            )
            .filter(
                Boolean
            )
            .join(
                "\n"
            );

    value =
        value.replace(
            /\s+\/\s+(?=\S)/g,
            "\n"
        );

    return value.trim();
}

function applyProtectedRules(
    source,
    target
) {
    let text =
        String(
            target ||
            ""
        );

    const en =
        String(
            source ||
            ""
        );

    if (
        /\bWerkroom\b/i.test(
            en
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
            en
        )
    ) {
        text =
            text.replace(
                /\bcondragulations\b/gi,
                "Condragulations"
            );
    }

    if (
        /Shantay,? you stay/i.test(
            en
        )
    ) {
        text =
            text.replace(
                /shantay,?\s+you\s+stay/gi,
                "Shantay, you stay"
            );
    }

    if (
        /Sashay away/i.test(
            en
        )
    ) {
        text =
            text.replace(
                /sashay\s+away/gi,
                "Sashay away"
            );
    }

    if (
        /You betta werk/i.test(
            en
        )
    ) {
        text =
            text.replace(
                /you\s+betta\s+werk/gi,
                "You betta werk"
            );
    }

    if (
        /Racers,? start your engines/i.test(
            en
        )
    ) {
        text =
            text.replace(
                /racers,?\s+start\s+your\s+engines/gi,
                "Racers, start your engines"
            );
    }

    if (
        /\blip[ -]?sync\b/i.test(
            en
        )
    ) {
        text =
            text
                .replace(
                    /\blipsync\b/gi,
                    "lip sync"
                )
                .replace(
                    /\bdublagem\b/gi,
                    "lip sync"
                )
                .replace(
                    /\bdublar\b/gi,
                    "fazer lip sync"
                );
    }

    return text;
}

function deterministicNeutralizeArtificialGender(
    text
) {
    return String(
        text ||
        ""
    )
        .replace(
            /\b(?:estou|tô)\s+empolgado\(a\)\b/gi,
            "Mal posso esperar"
        )
        .replace(
            /\b(?:estou|tô)\s+animado\(a\)\b/gi,
            "Mal posso esperar"
        )
        .replace(
            /\bempolgado\(a\)\b/gi,
            "com muita empolgação"
        )
        .replace(
            /\banimado\(a\)\b/gi,
            "com muita animação"
        );
}

function cleanAllFinal(
    blocks,
    texts
) {
    return texts.map(
        (
            text,
            i
        ) =>
            deterministicNeutralizeArtificialGender(
                applyProtectedRules(
                    blocks[i]
                        .text,

                    cleanFinalText(
                        text,
                        blocks[i]
                            .speakerHint
                    )
                )
            )
    );
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
                `Flatten inválido no group ${group.groupId}.`
            );
        }

        group.cues.forEach(
            (
                cue,
                i
            ) => {

                texts[
                    positions.get(
                        cue.index
                    )
                ] =
                    segments[i];
            }
        );
    }

    if (
        texts.some(
            text =>
                typeof text !==
                    "string" ||
                !text.trim()
        )
    ) {
        throw new Error(
            "Cue ausente/vazio na tradução."
        );
    }

    return texts;
}

// ============================================================
// QUALITY GUARD
// ============================================================

function finalRiskScan(
    blocks,
    texts
) {
    const issues =
        [];

    for (
        let i = 0;
        i <
        texts.length;
        i++
    ) {
        const text =
            String(
                texts[i] ||
                ""
            );

        const source =
            String(
                blocks[i]
                    ?.text ||
                ""
            );

        const reasons =
            [];

        if (
            /^\s*\[[^\]]{1,60}\]\s*:|^\s*[A-ZÀ-Ý][\p{L}0-9.'’_-]*(?:\s+[A-ZÀ-Ý][\p{L}0-9.'’_-]*){0,3}\s*:\s+/mu.test(
                text
            )
        ) {
            reasons.push(
                "SPEAKER_LABEL"
            );
        }

        if (
            /^\s*[-–—/]\s*\S/mu.test(
                text
            )
        ) {
            reasons.push(
                "DIALOGUE_MARKER"
            );
        }

        if (
            /(?:[A-Za-zÀ-ÖØ-öø-ÿ][-–—]){3,}[A-Za-zÀ-ÖØ-öø-ÿ]|([aeiouáéíóúãõâêô])\1{3,}/iu.test(
                text
            )
        ) {
            reasons.push(
                "VOCAL_ELONGATION"
            );
        }

        if (
            /empolgado\(a\)|empolgada\(o\)|animado\(a\)|animada\(o\)|\bele\/ela\b|\bela\/ele\b/i.test(
                text
            )
        ) {
            reasons.push(
                "ARTIFICIAL_GENDER"
            );
        }

        if (
            /\buma alçapão\b/i.test(
                text
            )
        ) {
            reasons.push(
                "GRAMMAR_ALCAPAO"
            );
        }

        if (
            /\bcabina de votação\b/i.test(
                text
            )
        ) {
            reasons.push(
                "CABINA_VOTACAO"
            );
        }

        if (
            /\bse eu manter\b/i.test(
                text
            )
        ) {
            reasons.push(
                "SE_EU_MANTER"
            );
        }

        if (
            /\bv(?:ão|ao)\b[^.!?\n]{0,80}\be serem\b/i.test(
                text
            )
        ) {
            reasons.push(
                "VAO_E_SEREM"
            );
        }

        if (
            /\bforam pedidas para\b/i.test(
                text
            )
        ) {
            reasons.push(
                "FORAM_PEDIDAS"
            );
        }

        if (
            /\bapoiante\b/i.test(
                text
            )
        ) {
            reasons.push(
                "APOIANTE"
            );
        }

        if (
            /\bbanheiro das (?:moças|damas)\b/i.test(
                text
            )
        ) {
            reasons.push(
                "BANHEIRO_DATADO"
            );
        }

        if (
            /\bcheque (?:da porra|do caralho)\b/i.test(
                text
            )
        ) {
            reasons.push(
                "CHEQUE_PROFANITY_PLACEMENT"
            );
        }

        if (
            /\bcompetição (?:da porra|do caralho)\b/i.test(
                text
            )
        ) {
            reasons.push(
                "COMPETICAO_PROFANITY_PLACEMENT"
            );
        }

        if (
            /\bgagged\b/i.test(
                source
            ) &&
            /amordaçad/i.test(
                text
            )
        ) {
            reasons.push(
                "GAG_LITERAL"
            );
        }

        if (
            /\bslay\w*\b/i.test(
                source
            ) &&
            /\bmat(?:ar|a|ou|aram|ando)\b/i.test(
                text
            )
        ) {
            reasons.push(
                "SLAY_LITERAL"
            );
        }

        if (
            /\bate\b/i.test(
                source
            ) &&
            /\b(?:comeu|comi|comemos|comeram)\b/i.test(
                text
            )
        ) {
            reasons.push(
                "ATE_LITERAL"
            );
        }

        if (
            /\blip[ -]?sync\b/i.test(
                source
            ) &&
            /\bdubl(?:agem|ar|ou|ando)\b/i.test(
                text
            )
        ) {
            reasons.push(
                "LIPSYNC_LITERAL"
            );
        }

        if (
            /\bWerkroom\b/i.test(
                source
            ) &&
            !/\bWerkroom\b/.test(
                text
            )
        ) {
            reasons.push(
                "WERKROOM"
            );
        }

        if (
            /\bCondragulations\b/i.test(
                source
            ) &&
            !/\bCondragulations\b/.test(
                text
            )
        ) {
            reasons.push(
                "CONDRAGULATIONS"
            );
        }

        if (
            reasons.length
        ) {
            issues.push({
                id:
                    blocks[i]
                        .index,

                reasons:
                    [
                        ...new Set(
                            reasons
                        )
                    ],

                source,

                text
            });
        }
    }

    return issues;
}

function mapIssuesToGroups(
    groups,
    issues
) {
    const cueToGroup =
        new Map();

    for (
        const group
        of groups
    ) {
        for (
            const cue
            of group.cues
        ) {
            cueToGroup.set(
                cue.index,
                group.groupId
            );
        }
    }

    const reasonsByGroup =
        new Map();

    for (
        const issue
        of issues
    ) {
        const groupId =
            cueToGroup.get(
                issue.id
            );

        if (
            !groupId
        ) {
            continue;
        }

        const set =
            reasonsByGroup.get(
                groupId
            ) ||
            new Set();

        for (
            const reason
            of issue.reasons
        ) {
            set.add(
                reason
            );
        }

        reasonsByGroup.set(
            groupId,
            set
        );
    }

    return reasonsByGroup;
}

// ============================================================
// QUALITY REPAIR
// ============================================================

async function mistralRepairRiskGroups(
    groups,
    translations,
    issues,
    job
) {
    if (
        !issues.length
    ) {
        return [];
    }

    const reasonsByGroup =
        mapIssuesToGroups(
            groups,
            issues
        );

    const riskyGroups =
        groups.filter(
            group =>
                reasonsByGroup.has(
                    group.groupId
                )
        );

    if (
        !riskyGroups.length
    ) {
        return [];
    }

    console.warn(
        `[QUALITY REPAIR] ${issues.length} risco(s) em ` +
        `${riskyGroups.length} group(s); reparo localizado.`
    );

    const entries =
        riskyGroups.map(
            group => ({
                group,

                reasons:
                    [
                        ...reasonsByGroup.get(
                            group.groupId
                        )
                    ]
            })
        );

    const batches =
        splitByBudget(
            entries,
            5000,
            12,
            entry => ({
                ...compactReviewGroup(
                    entry.group,
                    translations.get(
                        entry
                            .group
                            .groupId
                    )
                ),

                reasons:
                    entry.reasons
            })
        );

    const changed =
        [];

    for (
        const batch
        of batches
    ) {
        job.qualityRepairCalls++;

        const payload =
            batch.map(
                entry => ({
                    ...compactReviewGroup(
                        entry.group,
                        translations.get(
                            entry
                                .group
                                .groupId
                        )
                    ),

                    reasons:
                        entry.reasons
                })
            );

        try {
            const raw =
                await mistralChat({
                    systemPrompt:
                        QUALITY_REPAIR_SYSTEM_PROMPT,

                    userPrompt:
                        `Repare estes groups:\n` +
                        `${JSON.stringify(
                            {
                                groups:
                                    payload
                            }
                        )}`,

                    job,

                    reasoningEffort:
                        "none",

                    temperature:
                        0,

                    maxTokens:
                        5000,

                    purpose:
                        "repair"
                });

            const pseudoGroups =
                batch.map(
                    entry =>
                        entry.group
                );

            const parsed =
                parseTranslationResponse(
                    pseudoGroups,
                    raw
                );

            for (
                const [
                    groupId,
                    segments
                ]
                of parsed.valid
            ) {
                const before =
                    translations.get(
                        groupId
                    );

                if (
                    JSON.stringify(
                        before
                    ) !==
                    JSON.stringify(
                        segments
                    )
                ) {
                    translations.set(
                        groupId,
                        segments
                    );

                    changed.push(
                        groupId
                    );
                }
            }

            for (
                const group
                of parsed.invalidGroups
            ) {
                translations.set(
                    group.groupId,

                    await rescueSingleGroup(
                        group,
                        job
                    )
                );

                changed.push(
                    group.groupId
                );
            }
        }
        catch (
            error
        ) {
            console.warn(
                `[QUALITY REPAIR] Erro transitório: ` +
                `${getErrorMessage(
                    error
                ).slice(
                    0,
                    220
                )}.`
            );

            /*
             * Qualidade vence velocidade.
             *
             * Não ignoramos o risco.
             */
            for (
                const entry
                of batch
            ) {
                translations.set(
                    entry
                        .group
                        .groupId,

                    await rescueSingleGroup(
                        entry.group,
                        job
                    )
                );

                changed.push(
                    entry
                        .group
                        .groupId
                );
            }
        }
    }

    return [
        ...new Set(
            changed
        )
    ];
}

// ============================================================
// PIPELINE 6.2
// ============================================================

async function translateSrt(
    sourceSrt,
    job
) {
    /*
     * Em uma retomada:
     *
     * reaproveitamos a tradução Mistral,
     * mas refazemos a revisão secundária
     * para não somar contadores antigos.
     */
    job.geminiReviewedGroups =
        0;

    job.geminiProposalCount =
        0;

    job.geminiFallbackGroups =
        0;

    job.arbiterAccepted =
        0;

    job.qualityGuardRisks =
        0;

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

    if (
        !MISTRAL_API_KEY
    ) {
        throw new Error(
            "MISTRAL_API_KEY não configurada no Render."
        );
    }

    if (
        !GEMINI_API_KEY
    ) {
        throw new Error(
            "GEMINI_API_KEY não configurada no Render."
        );
    }

    const groups =
        buildSentenceGroups(
            blocks
        );

    const batches =
        splitByBudget(
            groups,
            MISTRAL_BATCH_CHARS,
            MISTRAL_BATCH_GROUPS,
            compactTranslationGroup
        );

    const translations =
        job.translationCheckpoint
            instanceof Map
            ? new Map(
                job.translationCheckpoint
            )
            : new Map();

    job.sentenceGroups =
        groups.length;

    job.totalBatches =
        batches.length;

    job.progress =
        Math.max(
            job.progress ||
                0,
            1
        );

    const startedAt =
        Date.now();

    const mistralStartedAt =
        Date.now();

    let completedBatchCount =
        0;

    let nextBatchIndex =
        0;

    // ========================================================
    // FILA GEMINI EM PARALELO TEMPORAL
    // ========================================================

    const geminiProposals =
        [];

    const reviewScheduled =
        new Set();

    let geminiReviewChain =
        Promise.resolve();

    function scheduleGeminiReview(
        batch
    ) {
        const key =
            batch
                .map(
                    group =>
                        group.groupId
                )
                .join(
                    ","
                );

        if (
            reviewScheduled.has(
                key
            )
        ) {
            return;
        }

        reviewScheduled.add(
            key
        );

        const entries =
            batch.map(
                group => ({
                    group,

                    segments:
                        translations.get(
                            group.groupId
                        )
                })
            );

        geminiReviewChain =
            geminiReviewChain.then(
                async () => {

                    console.log(
                        `[GEMINI REVIEW] Revisando ${entries.length} group(s) ` +
                        `enquanto Mistral continua.`
                    );

                    const proposals =
                        await geminiReviewBatchResilient(
                            entries,
                            job
                        );

                    geminiProposals.push(
                        ...proposals
                    );

                    job.geminiProposalCount =
                        geminiProposals.length;

                    console.log(
                        `[GEMINI REVIEW] Lote confirmado: ` +
                        `${entries.length}/${entries.length} revisados; ` +
                        `${proposals.length} proposta(s). ` +
                        `Total revisado=${job.geminiReviewedGroups}/${groups.length}.`
                    );
                }
            );
    }

    console.log(
        `[PIPELINE 6.2] ${blocks.length} cues -> ` +
        `${groups.length} Sentence Groups -> ` +
        `${batches.length} lote(s) Mistral | ` +
        `concorrência adaptativa até ${MISTRAL_MAX_CONCURRENCY}.`
    );

    if (
        translations.size
    ) {
        console.log(
            `[CHECKPOINT] Retomando ${translations.size}/${groups.length} group(s) já aprovados.`
        );
    }

    /*
     * Em retomada:
     *
     * se um batch já estava
     * inteiramente traduzido,
     * ele já pode ir ao Gemini.
     */
    for (
        const batch
        of batches
    ) {
        if (
            batch.every(
                group =>
                    translations.has(
                        group.groupId
                    )
            )
        ) {
            scheduleGeminiReview(
                batch
            );
        }
    }

    // ========================================================
    // WORKERS MISTRAL
    // ========================================================

    async function worker(
        workerId
    ) {
        while (
            true
        ) {
            const index =
                nextBatchIndex++;

            if (
                index >=
                batches.length
            ) {
                return;
            }

            const originalBatch =
                batches[
                    index
                ];

            const pending =
                originalBatch.filter(
                    group =>
                        !translations.has(
                            group.groupId
                        )
                );

            if (
                !pending.length
            ) {
                completedBatchCount++;

                console.log(
                    `[MISTRAL W${workerId}] Lote ${index + 1}/${batches.length}: checkpoint.`
                );

                updateProjection(
                    job,
                    "Mistral adaptativo",
                    completedBatchCount,
                    batches.length,
                    mistralStartedAt
                );

                continue;
            }

            console.log(
                `[MISTRAL W${workerId}] Lote ${index + 1}/${batches.length}: ` +
                `${pending.length} group(s) pendente(s).`
            );

            const result =
                await translateGroupBatchResilient(
                    pending,
                    job
                );

            for (
                const [
                    groupId,
                    segments
                ]
                of result
            ) {
                translations.set(
                    groupId,
                    segments
                );
            }

            /*
             * Checkpoint após
             * CADA lote concluído.
             */
            job.translationCheckpoint =
                new Map(
                    translations
                );

            job.updatedAt =
                Date.now();

            completedBatchCount++;

            job.completedBatches =
                completedBatchCount;

            job.progress =
                Math.max(
                    job.progress,

                    Math.round(
                        (
                            completedBatchCount /
                            batches.length
                        ) *
                        72
                    )
                );

            console.log(
                `[MISTRAL W${workerId}] Lote ${index + 1}/${batches.length} aprovado; ` +
                `total=${translations.size}/${groups.length}.`
            );

            updateProjection(
                job,
                "Mistral adaptativo",
                completedBatchCount,
                batches.length,
                mistralStartedAt
            );

            /*
             * Assim que o batch
             * terminou, Gemini já
             * pode revisar enquanto
             * o próximo Mistral roda.
             */
            if (
                originalBatch.every(
                    group =>
                        translations.has(
                            group.groupId
                        )
                )
            ) {
                scheduleGeminiReview(
                    originalBatch
                );
            }
        }
    }

    const workers =
        [];

    for (
        let i = 0;
        i <
        MISTRAL_MAX_CONCURRENCY;
        i++
    ) {
        workers.push(
            worker(
                i +
                1
            )
        );
    }

    await Promise.all(
        workers
    );

    // ========================================================
    // TRADUÇÃO COMPLETA?
    // ========================================================

    if (
        translations.size !==
        groups.length
    ) {
        const missing =
            groups
                .filter(
                    group =>
                        !translations.has(
                            group.groupId
                        )
                )
                .map(
                    group =>
                        group.groupId
                );

        throw new Error(
            `Tradução incompleta: ` +
            `${translations.size}/${groups.length}; ` +
            `faltando=${missing.join(
                ","
            )}.`
        );
    }

    /*
     * Garante review de qualquer
     * batch que por alguma retomada
     * ainda não tenha sido agendado.
     */
    for (
        const batch
        of batches
    ) {
        scheduleGeminiReview(
            batch
        );
    }

    // ========================================================
    // CHECKPOINT MISTRAL COMPLETO
    // ========================================================

    let primaryTexts =
        cleanAllFinal(
            blocks,

            flattenTranslations(
                blocks,
                groups,
                translations
            )
        );

    const primarySrt =
        buildSrt(
            blocks,
            primaryTexts
        );

    auditFinalTimestamps(
        sourceSrt,
        primarySrt,
        "CHECKPOINT MISTRAL"
    );

    job.primaryCheckpoint =
        primarySrt;

    job.primaryCheckpointAt =
        Date.now();

    job.translationCheckpoint =
        new Map(
            translations
        );

    job.progress =
        Math.max(
            job.progress,
            74
        );

    job.updatedAt =
        Date.now();

    console.log(
        `[CHECKPOINT] Mistral completo preservado: ` +
        `${groups.length}/${groups.length} groups. ` +
        `A revisão secundária nunca apaga esse resultado.`
    );

    // ========================================================
    // ESPERA SOMENTE CAUDA DO GEMINI
    // ========================================================

    console.log(
        "[GEMINI REVIEW] Aguardando apenas a cauda da revisão 100% independente..."
    );

    await geminiReviewChain;

    if (
        job.geminiReviewedGroups !==
        groups.length
    ) {
        throw new Error(
            `Revisão secundária incompleta: ` +
            `${job.geminiReviewedGroups}/${groups.length} groups.`
        );
    }

    console.log(
        `[GEMINI REVIEW] COMPLETA: ` +
        `${job.geminiReviewedGroups}/${groups.length} groups; ` +
        `${geminiProposals.length} proposta(s).`
    );

    job.progress =
        Math.max(
            job.progress,
            84
        );

    // ========================================================
    // MISTRAL ARBITRA GEMINI
    // ========================================================

    await arbitrateAllProposals(
        geminiProposals,
        translations,
        job
    );

    job.progress =
        Math.max(
            job.progress,
            93
        );

    // ========================================================
    // QUALITY GUARD
    // ========================================================

    let texts =
        cleanAllFinal(
            blocks,

            flattenTranslations(
                blocks,
                groups,
                translations
            )
        );

    let risks =
        finalRiskScan(
            blocks,
            texts
        );

    if (
        risks.length
    ) {
        await mistralRepairRiskGroups(
            groups,
            translations,
            risks,
            job
        );

        texts =
            cleanAllFinal(
                blocks,

                flattenTranslations(
                    blocks,
                    groups,
                    translations
                )
            );

        risks =
            finalRiskScan(
                blocks,
                texts
            );
    }

    /*
     * Última limpeza
     * determinística.
     */
    texts =
        cleanAllFinal(
            blocks,
            texts
        );

    risks =
        finalRiskScan(
            blocks,
            texts
        );

    job.qualityGuardRisks =
        risks.length;

    if (
        risks.length
    ) {
        const summary =
            risks
                .slice(
                    0,
                    20
                )
                .map(
                    item =>
                        `${item.id}:${item.reasons.join(
                            "+"
                        )}`
                )
                .join(
                    ", "
                );

        /*
         * Aqui NÃO fingimos
         * que qualidade passou.
         */
        throw new Error(
            `Quality Guard ainda encontrou ${risks.length} risco(s): ${summary}`
        );
    }

    console.log(
        "[QUALITY GUARD] PASSOU — 0 padrão(s) conhecido(s) restante(s)."
    );

    // ========================================================
    // FINAL SRT
    // ========================================================

    const finalSrt =
        buildSrt(
            blocks,
            texts
        );

    auditFinalTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL 6.2"
    );

    job.timestampAuditPassed =
        true;

    job.contentAuditPassed =
        true;

    job.progress =
        100;

    job.updatedAt =
        Date.now();

    const elapsed =
        (
            Date.now() -
            startedAt
        ) /
        1000;

    console.log(
        `[PIPELINE 6.2] OK em ${elapsed.toFixed(
            1
        )}s | ` +
        `MistralCalls=${job.mistralCalls} | ` +
        `MistralAttempts=${job.mistralAttempts} | ` +
        `Mistral429=${job.mistral429s} | ` +
        `salvaged=${job.salvagedGroups} | ` +
        `rescueBatch=${job.rescueBatchCalls} | ` +
        `atomic=${job.atomicRescues} | ` +
        `GeminiReviewed=${job.geminiReviewedGroups}/${groups.length} | ` +
        `GeminiCalls=${job.geminiReviewCalls} | ` +
        `Gemini429=${job.gemini429s} | ` +
        `GeminiPropostas=${job.geminiProposalCount} | ` +
        `GeminiFallbackGroups=${job.geminiFallbackGroups} | ` +
        `ArbiterCalls=${job.arbiterCalls} | ` +
        `ArbiterAccepted=${job.arbiterAccepted} | ` +
        `QualityRepairCalls=${job.qualityRepairCalls} | ` +
        `riscos finais=0.`
    );

    return finalSrt;
}

// ============================================================
// JOB PROCESSING
// ============================================================

async function processJob(
    job
) {
    job.processingStartedAt =
        Date.now();

    job.queueWaitSeconds =
        Math.max(
            0,

            (
                job.processingStartedAt -
                job.queuedAt
            ) /
            1000
        );

    job.status =
        "processing";

    job.error =
        null;

    job.updatedAt =
        Date.now();

    console.log(
        `[JOB ${job.id}] Iniciando após ` +
        `${job.queueWaitSeconds.toFixed(
            1
        )}s na fila. SEM teto global.`
    );

    try {
        const cached =
            getTranslationCache(
                job.cacheKey
            );

        if (
            cached
        ) {
            auditFinalTimestamps(
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

            job.timestampAuditPassed =
                true;

            job.contentAuditPassed =
                true;

            job.updatedAt =
                Date.now();

            return;
        }

        const finalSrt =
            await translateSrt(
                job.sourceSrt,
                job
            );

        setTranslationCache(
            job.cacheKey,
            finalSrt
        );

        job.result =
            finalSrt;

        job.status =
            "completed";

        job.progress =
            100;

        job.updatedAt =
            Date.now();

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
            getErrorMessage(
                error
            );

        job.updatedAt =
            Date.now();

        console.error(
            `[JOB ${job.id}] Falhou: ${job.error}`
        );

        if (
            job.translationCheckpoint
                instanceof Map &&
            job.translationCheckpoint
                .size
        ) {
            console.error(
                `[CHECKPOINT] ` +
                `${job.translationCheckpoint.size} group(s) Mistral ` +
                `preservados em memória para retomada.`
            );
        }
    }
}

// ============================================================
// JOB QUEUE
// ============================================================

function enqueueTranslationJob(
    job
) {
    return new Promise(
        resolve => {

            translationJobQueue.push({
                job,
                resolve
            });

            console.log(
                `[JOB QUEUE] ${job.id} entrou na fila; ` +
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

            if (
                !item
            ) {
                continue;
            }

            if (
                item.job.status ===
                "processing"
            ) {
                await processJob(
                    item.job
                );
            }

            item.resolve();
        }
    }
    finally {
        translationJobWorkerRunning =
            false;

        if (
            translationJobQueue.length
        ) {
            processTranslationJobQueue();
        }
    }
}

function getOrCreateTranslationJob({
    type,
    videoId,
    cleanedSrt,
    sourceName = ""
}) {
    const sourceHash =
        sha256(
            cleanedSrt
        );

    const cacheKey =
        `${TRANSLATION_CACHE_VERSION}:${type}:${videoId}:${sourceHash}`;

    // ========================================================
    // CACHE FINAL
    // ========================================================

    const cached =
        getTranslationCache(
            cacheKey
        );

    if (
        cached
    ) {
        const jobId =
            `cached-${sourceHash.slice(
                0,
                24
            )}`;

        let job =
            getJob(
                jobId
            );

        if (
            !job
        ) {
            job =
                createJob({
                    jobId,

                    cacheKey,

                    type,

                    videoId,

                    sourceHash,

                    sourceSrt:
                        cleanedSrt,

                    sourceName
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

        return job;
    }

    // ========================================================
    // JÁ PROCESSANDO
    // ========================================================

    const processing =
        findProcessingJob(
            cacheKey
        );

    if (
        processing
    ) {
        return processing;
    }

    // ========================================================
    // RETOMADA
    // ========================================================

    const reusable =
        findReusableFailedJob(
            cacheKey
        );

    if (
        reusable
    ) {
        reusable.status =
            "processing";

        reusable.error =
            null;

        reusable.progress =
            Math.max(
                1,
                reusable.progress ||
                    0
            );

        reusable.updatedAt =
            Date.now();

        reusable.queuedAt =
            Date.now();

        reusable.resumes =
            Number(
                reusable.resumes ||
                0
            ) +
            1;

        console.log(
            `[CHECKPOINT] Retomando job ${reusable.id}; ` +
            `${reusable.translationCheckpoint?.size || 0} group(s) preservados.`
        );

        reusable.promise =
            enqueueTranslationJob(
                reusable
            )
                .catch(
                    error => {

                        reusable.status =
                            "failed";

                        reusable.error =
                            getErrorMessage(
                                error
                            );

                        reusable.updatedAt =
                            Date.now();
                    }
                );

        return reusable;
    }

    // ========================================================
    // NOVO
    // ========================================================

    const jobId =
        `job-${sourceHash.slice(
            0,
            24
        )}-${randomId(
            6
        )}`;

    const job =
        createJob({
            jobId,

            cacheKey,

            type,

            videoId,

            sourceHash,

            sourceSrt:
                cleanedSrt,

            sourceName
        });

    job.promise =
        enqueueTranslationJob(
            job
        )
            .catch(
                error => {

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

    return job;
}

// ============================================================
// NETWORK FETCH
// ============================================================

async function fetchWithTimeout(
    url,
    options = {},
    timeoutMs =
        SOURCE_FETCH_TIMEOUT_MS
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

// ============================================================
// OPENSUBTITLES
// ============================================================

function buildOpenSubtitlesSearchUrl(
    type,
    id,
    extra = {}
) {
    const base =
        `https://opensubtitles-v3.strem.io/subtitles/${encodeURIComponent(
            type
        )}/${encodeURIComponent(
            id
        )}`;

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

    const suffix =
        params.toString();

    return suffix
        ? `${base}/${suffix}.json`
        : `${base}.json`;
}

function scoreSubtitle(
    subtitle
) {
    let score =
        0;

    const lang =
        String(
            subtitle?.lang ||
            ""
        ).toLowerCase();

    if (
        lang ===
        "eng"
    ) {
        score +=
            100;
    }
    else if (
        lang ===
        "en"
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
            ""
        ).toLowerCase() ===
        "srt"
    ) {
        score +=
            20;
    }

    if (
        /english/i.test(
            String(
                subtitle?.name ||
                ""
            )
        )
    ) {
        score +=
            10;
    }

    return score;
}

function selectBestSubtitle(
    subtitles
) {
    if (
        !Array.isArray(
            subtitles
        )
    ) {
        return null;
    }

    return (
        subtitles
            .filter(
                subtitle => {

                    const lang =
                        String(
                            subtitle?.lang ||
                            ""
                        ).toLowerCase();

                    return (
                        (
                            lang ===
                                "eng" ||
                            lang ===
                                "en"
                        ) &&
                        typeof subtitle?.url ===
                            "string" &&
                        /^https?:\/\//i.test(
                            subtitle.url
                        )
                    );
                }
            )
            .sort(
                (
                    a,
                    b
                ) =>
                    scoreSubtitle(
                        b
                    ) -
                    scoreSubtitle(
                        a
                    )
            )[0] ||
        null
    );
}

async function findEnglishSubtitle(
    type,
    id,
    extra
) {
    const url =
        buildOpenSubtitlesSearchUrl(
            type,
            id,
            extra
        );

    console.log(
        `[OPENSUBTITLES] ${url}`
    );

    const response =
        await fetchWithTimeout(
            url,
            {
                headers: {
                    Accept:
                        "application/json",

                    "User-Agent":
                        "Stremio-PTBR-DualAI/6.2"
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

    const subtitles =
        Array.isArray(
            data?.subtitles
        )
            ? data.subtitles
            : [];

    const target =
        selectBestSubtitle(
            subtitles
        );

    console.log(
        target
            ? `[OPENSUBTITLES] Inglês selecionado; resultados=${subtitles.length}.`
            : `[OPENSUBTITLES] Nenhuma legenda inglesa utilizável; resultados=${subtitles.length}.`
    );

    return target;
}

async function downloadAndCleanSubtitle(
    url
) {
    const response =
        await fetchWithTimeout(
            url,
            {
                headers: {
                    "User-Agent":
                        "Stremio-PTBR-DualAI/6.2"
                }
            }
        );

    if (
        !response.ok
    ) {
        throw new Error(
            `Falha ao baixar legenda: HTTP ${response.status}.`
        );
    }

    const raw =
        normalizeSrt(
            await response.text()
        );

    if (
        !raw
    ) {
        throw new Error(
            "Legenda vazia."
        );
    }

    if (
        raw.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda muito grande: ${raw.length} caracteres.`
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
            "Legenda vazia após limpeza."
        );
    }

    return clean;
}

function parseExtra(
    req
) {
    const raw =
        String(
            req.params
                .extra ||
            ""
        ).trim();

    const params =
        new URLSearchParams(
            raw
        );

    return {
        filename:
            String(
                params.get(
                    "filename"
                ) ||
                req.query
                    .filename ||
                ""
            ).trim(),

        videoSize:
            String(
                params.get(
                    "videoSize"
                ) ||
                req.query
                    .videoSize ||
                ""
            ).trim(),

        videoHash:
            String(
                params.get(
                    "videoHash"
                ) ||
                req.query
                    .videoHash ||
                ""
            ).trim()
    };
}

// ============================================================
// STATUS SRT
// ============================================================

function sendSubtitleResponse(
    res,
    srt,
    cacheControl =
        "no-store"
) {
    res.status(
        200
    );

    res.set(
        "Content-Type",
        "application/x-subrip; charset=utf-8"
    );

    res.set(
        "Cache-Control",
        cacheControl
    );

    res.send(
        String(
            srt ||
            ""
        )
    );
}

function buildProcessingSrt(
    job
) {
    const projection =
        job
            ?.projection
            ?.estimatedRemainingSeconds;

    const extra =
        Number.isFinite(
            projection
        )
            ? ` Estimativa atual: ~${formatSeconds(
                projection
            )} restantes.`
            : "";

    return [
        "1",

        "00:00:01,000 --> 00:00:06,000",

        "Traduzindo e revisando legenda...",

        "",

        "2",

        "00:00:06,500 --> 00:00:14,000",

        `Progresso: ${Number(
            job?.progress ||
            0
        )}%.${extra}`
    ].join(
        "\n"
    );
}

function buildErrorSrt(
    message
) {
    return [
        "1",

        "00:00:01,000 --> 00:00:08,000",

        "Não foi possível concluir a legenda PT-BR.",

        "",

        "2",

        "00:00:08,500 --> 00:00:18,000",

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
            )
    ].join(
        "\n"
    );
}

// ============================================================
// MANIFEST
// ============================================================

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "6.2.0",

    name:
        "Tradutor PT-BR Premium",

    description:
        "Mistral Medium 3.5 + revisão integral Gemini 3.5 Flash-Lite + arbitragem Mistral + timestamps imutáveis.",

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
            false,

        adult:
            false
    }
};

// ============================================================
// ROOT / HEALTH
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
    ) => {

        res.json({
            status:
                "online",

            name:
                manifest.name,

            version:
                manifest.version,

            translator:
                MISTRAL_MODEL,

            independentReviewer:
                GEMINI_MODEL,

            arbiter:
                MISTRAL_MODEL,

            groqInPipeline:
                false,

            groqKeyCanRemain:
                GROQ_API_KEY_PRESENT,

            translationCacheVersion:
                TRANSLATION_CACHE_VERSION,

            embeddedBridge:
                Boolean(
                    LOCAL_BRIDGE_SECRET
                ),

            mistralConfigured:
                Boolean(
                    MISTRAL_API_KEY
                ),

            geminiConfigured:
                Boolean(
                    GEMINI_API_KEY
                ),

            queue:
                translationJobQueue.length,

            processing:
                translationJobWorkerRunning,

            cache:
                translationCache.size,

            jobs:
                jobs.size,

            mistralGovernor:
                mistralGovernor.status(),

            rules: {
                globalTimeCeiling:
                    false,

                mistralAdaptiveConcurrencyUpTo2:
                    true,

                checkpointPerMistralBatch:
                    true,

                geminiReviews100Percent:
                    true,

                geminiRunsWhileMistralTranslates:
                    true,

                secondaryAiCannotEditDirectly:
                    true,

                mistralArbitratesAllGeminiProposals:
                    true,

                groqRemovedFromCriticalPath:
                    true,

                timestampsImmutable:
                    true,

                hiddenSpeakerContext:
                    true,

                speakerLabelsForbidden:
                    true,

                genderGuessForbidden:
                    true,

                vocalElongationNormalization:
                    true,

                decorativeDialogueMarkersForbidden:
                    true
            }
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

            translator:
                MISTRAL_MODEL,

            reviewer:
                GEMINI_MODEL,

            cacheVersion:
                TRANSLATION_CACHE_VERSION,

            queue:
                translationJobQueue.length,

            processing:
                translationJobWorkerRunning,

            mistralGovernor:
                mistralGovernor.status()
        });
    }
);

// ============================================================
// STREMIO SUBTITLES
// ============================================================

async function subtitlesHandler(
    req,
    res
) {
    try {
        const type =
            String(
                req.params
                    .type ||
                ""
            ).trim();

        const id =
            String(
                req.params
                    .id ||
                ""
            ).trim();

        const extra =
            parseExtra(
                req
            );

        console.log(
            `[STREMIO] Pedido: ${type}/${id}`
        );

        console.log(
            `[STREMIO] filename=${extra.filename || "(não enviado)"}; ` +
            `videoSize=${extra.videoSize || "(não enviado)"}; ` +
            `videoHash=${extra.videoHash || "(não enviado)"}`
        );

        const target =
            await findEnglishSubtitle(
                type,
                id,
                extra
            );

        if (
            !target
        ) {
            return safeJson(
                res,
                {
                    subtitles: []
                }
            );
        }

        const sourceSrt =
            await downloadAndCleanSubtitle(
                target.url
            );

        const job =
            getOrCreateTranslationJob({
                type,

                videoId:
                    id,

                cleanedSrt:
                    sourceSrt,

                sourceName:
                    target.name ||
                    "OpenSubtitles"
            });

        const baseUrl =
            cleanBaseUrl(
                req
            );

        return safeJson(
            res,
            {
                subtitles: [
                    {
                        id:
                            `${id}-ptbr-${job.sourceHash.slice(
                                0,
                                12
                            )}`,

                        url:
                            `${baseUrl}/subtitle/${encodeURIComponent(
                                job.id
                            )}.srt`,

                        lang:
                            "por"
                    }
                ]
            }
        );
    }
    catch (
        error
    ) {
        console.error(
            `[STREMIO] ${getErrorMessage(
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
);

// ============================================================
// API PONTE LOCAL
// ============================================================

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
            const type =
                String(
                    req.body
                        ?.type ||
                    "unknown"
                ).trim();

            const videoId =
                String(
                    req.body
                        ?.id ||
                    "unknown"
                ).trim();

            const sourceName =
                String(
                    req.body
                        ?.name ||
                    "embedded"
                ).trim();

            const rawSrt =
                req.body
                    ?.srt;

            if (
                typeof rawSrt !==
                    "string" ||
                !rawSrt.trim()
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
                rawSrt.length >
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

            const cleanedSrt =
                cleanSrtForTranslation(
                    rawSrt
                );

            const cueCount =
                parseSrt(
                    cleanedSrt
                ).length;

            if (
                !cleanedSrt ||
                !cueCount
            ) {
                throw new Error(
                    "Legenda embutida vazia/inválida após limpeza."
                );
            }

            console.log(
                `[EMBEDDED API] ${type}/${videoId} | ` +
                `${sourceName} | ${cueCount} cues.`
            );

            const job =
                getOrCreateTranslationJob({
                    type,

                    videoId,

                    cleanedSrt,

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
                        `${baseUrl}/subtitle/${encodeURIComponent(
                            job.id
                        )}.srt`
                }
            );
        }
        catch (
            error
        ) {
            console.error(
                `[EMBEDDED API] ${getErrorMessage(
                    error
                )}`
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
            getJob(
                String(
                    req.params
                        .jobId ||
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
                        "Job não encontrado/expirado."
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

                progress:
                    job.progress,

                error:
                    job.error,

                queueWaitSeconds:
                    job.queueWaitSeconds,

                projection:
                    job.projection,

                sentenceGroups:
                    job.sentenceGroups ||
                    0,

                checkpointGroups:
                    job.translationCheckpoint
                        ?.size ||
                    0,

                primaryCheckpointReady:
                    Boolean(
                        job.primaryCheckpoint
                    ),

                resumes:
                    job.resumes ||
                    0,

                mistralCalls:
                    job.mistralCalls ||
                    0,

                mistralAttempts:
                    job.mistralAttempts ||
                    0,

                mistral429s:
                    job.mistral429s ||
                    0,

                geminiReviewCalls:
                    job.geminiReviewCalls ||
                    0,

                geminiReviewAttempts:
                    job.geminiReviewAttempts ||
                    0,

                gemini429s:
                    job.gemini429s ||
                    0,

                geminiReviewedGroups:
                    job.geminiReviewedGroups ||
                    0,

                geminiProposalCount:
                    job.geminiProposalCount ||
                    0,

                geminiFallbackGroups:
                    job.geminiFallbackGroups ||
                    0,

                arbiterCalls:
                    job.arbiterCalls ||
                    0,

                arbiterAccepted:
                    job.arbiterAccepted ||
                    0,

                salvagedGroups:
                    job.salvagedGroups ||
                    0,

                rescueBatchCalls:
                    job.rescueBatchCalls ||
                    0,

                atomicRescues:
                    job.atomicRescues ||
                    0,

                highReasoningRescues:
                    job.highReasoningRescues ||
                    0,

                perCueRescues:
                    job.perCueRescues ||
                    0,

                qualityRepairCalls:
                    job.qualityRepairCalls ||
                    0,

                qualityGuardRisks:
                    job.qualityGuardRisks ||
                    0,

                mistralGovernor:
                    mistralGovernor.status(),

                providerUsage:
                    job.providerUsage ||
                    {}
            }
        );
    }
);

// ============================================================
// RESULTADO SRT
// ============================================================

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
                        req.params
                            .jobId ||
                        ""
                    )
                );
        }
        catch {
            jobId =
                String(
                    req.params
                        .jobId ||
                    ""
                );
        }

        const job =
            getJob(
                jobId
            );

        if (
            !job
        ) {
            return sendSubtitleResponse(
                res,

                buildErrorSrt(
                    "Esta tradução expirou. Recarregue as legendas."
                )
            );
        }

        if (
            job.status ===
                "completed" &&
            job.result
        ) {
            try {
                auditFinalTimestamps(
                    job.sourceSrt,
                    job.result,
                    "SERVING"
                );
            }
            catch {
                return sendSubtitleResponse(
                    res,

                    buildErrorSrt(
                        "A auditoria de timestamps bloqueou esta legenda."
                    )
                );
            }

            if (
                !job.contentAuditPassed
            ) {
                return sendSubtitleResponse(
                    res,

                    buildErrorSrt(
                        "A revisão de conteúdo não foi concluída."
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
            " STREMIO PT-BR DUAL AI TRANSLATOR 6.2 FINAL"
        );

        console.log(
            "============================================================"
        );

        console.log(
            `Porta: ${PORT}`
        );

        console.log(
            `Tradutor principal: ${MISTRAL_MODEL}`
        );

        console.log(
            `Revisor independente 100%: ${GEMINI_MODEL}`
        );

        console.log(
            `Árbitro final das propostas: ${MISTRAL_MODEL}`
        );

        console.log(
            `Mistral: ${
                MISTRAL_API_KEY
                    ? "CONFIGURADO ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            `Gemini: ${
                GEMINI_API_KEY
                    ? "CONFIGURADO ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            `Groq: FORA DO PIPELINE 6.2 ${
                GROQ_API_KEY_PRESENT
                    ? "(chave pode permanecer) ✅"
                    : "✅"
            }`
        );

        console.log(
            `Ponte Local: ${
                LOCAL_BRIDGE_SECRET
                    ? "COMPATÍVEL ✅"
                    : "SECRET FALTANDO ❌"
            }`
        );

        console.log(
            `Mistral batch: ${MISTRAL_BATCH_GROUPS} groups / ${MISTRAL_BATCH_CHARS} chars`
        );

        console.log(
            `Mistral concorrência adaptativa: ATÉ ${MISTRAL_MAX_CONCURRENCY} ✅`
        );

        console.log(
            "429 Mistral reduz temporariamente concorrência para 1: ATIVO ✅"
        );

        console.log(
            "Gemini revisa 100% enquanto Mistral continua traduzindo: ATIVO ✅"
        );

        console.log(
            "Gemini NÃO altera legenda diretamente: GARANTIDO ✅"
        );

        console.log(
            "Mistral arbitra TODAS as propostas Gemini: ATIVO ✅"
        );

        console.log(
            "Checkpoint após cada lote Mistral: ATIVO ✅"
        );

        console.log(
            "Checkpoint Mistral antes da revisão final: ATIVO ✅"
        );

        console.log(
            "Teto global de tradução: NÃO EXISTE ✅"
        );

        console.log(
            "Timeout somente por request + retry: ATIVO ✅"
        );

        console.log(
            "Quality Guard: BLOQUEIA risco conhecido restante ✅"
        );

        console.log(
            "Speaker labels/nomes na saída: PROIBIDOS ✅"
        );

        console.log(
            "Gênero chutado: PROIBIDO ✅"
        );

        console.log(
            "Alongamentos gráficos: PROIBIDOS ✅"
        );

        console.log(
            "Hífens/travessões decorativos: PROIBIDOS ✅"
        );

        console.log(
            "Timestamps: IMUTÁVEIS ✅"
        );

        console.log(
            `Namespace de cache: ${TRANSLATION_CACHE_VERSION}`
        );

        console.log(
            "Fila de episódios: SEQUENCIAL (1 por vez)"
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
