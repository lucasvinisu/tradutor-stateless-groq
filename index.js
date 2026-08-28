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

const GROQ_API_KEY = String(process.env.GROQ_API_KEY || "").trim();
const GROQ_REVIEW_MODEL = String(
    process.env.GROQ_REVIEW_MODEL ||
    "groq/compound-mini"
).trim();

// Rollback manual apenas. Não participa do pipeline normal.
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();

const GEMINI_MODEL = String(
    process.env.GEMINI_MODEL ||
    "gemini-3.5-flash-lite"
).trim();

const TRANSLATION_CACHE_VERSION = "6.1.0-final";

const SOURCE_FETCH_TIMEOUT_MS = 20_000;

const MISTRAL_TIMEOUT_MS = Number(
    process.env.MISTRAL_TIMEOUT_MS ||
    150_000
);

const GROQ_REVIEW_TIMEOUT_MS = Number(
    process.env.GROQ_REVIEW_TIMEOUT_MS ||
    90_000
);

const MAX_SOURCE_CHARS = 800_000;

const CACHE_TTL_MS =
    7 * 24 * 60 * 60 * 1000;

const JOB_TTL_MS =
    24 * 60 * 60 * 1000;

const MAX_CACHE_ENTRIES = 200;
const MAX_JOBS = 300;

// Sem teto global de duração.
// Existem apenas timeouts por REQUEST + retries.
const MISTRAL_MAX_RETRIES = Number(
    process.env.MISTRAL_MAX_RETRIES ||
    6
);

const GROQ_MAX_RETRIES = Number(
    process.env.GROQ_MAX_RETRIES ||
    4
);

// ------------------------------------------------------------
// MISTRAL - TRADUÇÃO PRINCIPAL
// ------------------------------------------------------------

const MISTRAL_BATCH_CHARS = Number(
    process.env.MISTRAL_BATCH_CHARS ||
    18_000
);

const MISTRAL_BATCH_GROUPS = Number(
    process.env.MISTRAL_BATCH_GROUPS ||
    320
);

const MISTRAL_MAX_OUTPUT_TOKENS = Number(
    process.env.MISTRAL_MAX_OUTPUT_TOKENS ||
    16_000
);

// ------------------------------------------------------------
// MISTRAL - RESGATE ESTRUTURAL
// ------------------------------------------------------------

const MISTRAL_RESCUE_GROUPS = Number(
    process.env.MISTRAL_RESCUE_GROUPS ||
    24
);

const MISTRAL_RESCUE_CHARS = Number(
    process.env.MISTRAL_RESCUE_CHARS ||
    8_000
);

// ------------------------------------------------------------
// GROQ - REVISÃO SELETIVA
// ------------------------------------------------------------
//
// Mesmo que as variáveis antigas no Render estejam:
//
// REVIEW_BATCH_GROUPS=160
// REVIEW_BATCH_CHARS=16000
//
// esta versão aplica um CAP INTERNO seguro.
//
// Isso evita repetir o HTTP 413.
//

const REVIEW_BATCH_GROUPS = Math.min(
    Number(
        process.env.REVIEW_BATCH_GROUPS ||
        30
    ),
    30
);

const REVIEW_BATCH_CHARS = Math.min(
    Number(
        process.env.REVIEW_BATCH_CHARS ||
        6_000
    ),
    6_000
);

const GROQ_REVIEW_MAX_OUTPUT_TOKENS = Math.min(
    Number(
        process.env.GROQ_REVIEW_MAX_OUTPUT_TOKENS ||
        4_000
    ),
    4_000
);

// ============================================================
// ESTADO
// ============================================================

const translationCache = new Map();

const jobs = new Map();

const translationJobQueue = [];

let translationJobWorkerRunning = false;

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
            /^\s*```(?:json|srt|text|plaintext)?\s*/i,
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
        seconds < 0
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

    return min > 0
        ? `${min}m${String(
            sec
        ).padStart(
            2,
            "0"
        )}s`
        : `${sec}s`;
}

// ============================================================
// PROJEÇÃO DINÂMICA DE EFICIÊNCIA
// ============================================================
//
// IMPORTANTE:
//
// Isto NÃO cancela o job.
//
// Serve somente para:
// - medir a velocidade real;
// - estimar o restante;
// - mostrar projeção;
// - diagnosticar lentidão.
//
// Não existe mais "8 minutos e morre".
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

    const phaseElapsed =
        (
            Date.now() -
            phaseStartedAt
        ) /
        1000;

    const avg =
        phaseElapsed /
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
            : phaseElapsed;

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
    10 * 60 * 1000
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
    const now =
        Date.now();

    translationCache.set(
        key,
        {
            srt,

            createdAt:
                now,

            expiresAt:
                now +
                CACHE_TTL_MS
        }
    );

    cleanupMemory();
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

        groqReviewCalls:
            0,

        reviewChanges:
            0,

        reviewCandidateGroups:
            0,

        reviewSkippedGroups:
            0,

        reviewerSplitRescues:
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

        providerUsage:
            {},

        /*
         * Checkpoint dos groups Mistral.
         *
         * Se um job falhar depois,
         * ele pode continuar daqui.
         */
        translationCheckpoint:
            new Map(),

        /*
         * SRT primário fechado após
         * 100% do Mistral.
         *
         * A revisão posterior não
         * destrói esse resultado.
         */
        primaryCheckpoint:
            null,

        primaryCheckpointAt:
            null,

        reviewDegraded:
            false
    };

    jobs.set(
        job.id,
        job
    );

    cleanupMemory();

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
// AUTORIZAÇÃO PONTE LOCAL
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

    if (
        a.length !==
        b.length
    ) {
        return false;
    }

    return crypto
        .timingSafeEqual(
            a,
            b
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

    /*
     * Marker interno já existente:
     *
     * @@SPK:Kelly@@ ...
     */
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

    /*
     * [Kelly] texto
     * [Kelly]&#58; texto
     */
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

    /*
     * KELLY: texto
     * Kelly Clarkson: texto
     */
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
// NORMALIZAÇÃO DE ALONGAMENTOS VOCAIS
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
     * Exemplos:
     *
     * você-e-e-e-e
     * home-e-e-e-e
     *
     * A parte antes da sequência
     * já representa a palavra-base.
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

    /*
     * Neste ponto o speaker inicial
     * já foi extraído e preservado.
     *
     * O restante em [] tende a ser
     * rubrica SDH/CC.
     */
    text =
        text.replace(
            /\s*\[[^\]]+\]\s*/gu,
            " "
        );

    /*
     * Remove parenteses somente quando
     * claramente são SDH/CC.
     */
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
        text.replace(
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

    const out = [];

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

        const dialogue = [];

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

        /*
         * Cue que virou apenas SDH/CC.
         */
        if (
            !dialogue.length
        ) {
            removed++;

            continue;
        }

        /*
         * Só guarda um speakerHint
         * quando o cue inteiro tem
         * um único speaker claramente
         * identificado.
         */
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

    const result = [];

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
        `[AUDIT TIMESTAMP] ${label}: PASSOU — ${source.length}/${source.length}; 0 alterações.`
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

    /*
     * Speakers diferentes:
     * não junta.
     */
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
    const groups = [];

    let current = [];

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

            current = [];
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
    groups,
    maxChars,
    maxGroups,
    itemBuilder
) {
    const batches = [];

    let current = [];

    let chars = 0;

    for (
        const group
        of groups
    ) {
        const item =
            itemBuilder(
                group
            );

        const size =
            JSON.stringify(
                item
            ).length +
            8;

        if (
            current.length &&
            (
                current.length >=
                    maxGroups ||
                chars +
                    size >
                    maxChars
            )
        ) {
            batches.push(
                current
            );

            current = [];

            chars = 0;
        }

        current.push(
            group
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
// PROMPTS
// ============================================================

const TRANSLATOR_SYSTEM_PROMPT = `
Você é o TRADUTOR PRINCIPAL de legendas EN→PT-BR de um pipeline de produção.

Traduza como uma ótima legenda brasileira de streaming: natural, contemporânea, oral, fiel e contextual. Evite português literal, engessado, lusitano, datado ou com cara de tradução automática.

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
empolgada(o)
animado(a)
animada(o)
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

Responda SOMENTE JSON neste formato:

{"items":[{"g":1,"s":["segmento cue 1","segmento cue 2"]}]}

g deve repetir exatamente o group id.

s deve ter EXATAMENTE n elementos.

Não omita groups.

Não invente groups.
`;

const TRANSLATOR_RESCUE_SYSTEM_PROMPT = `
Você está fazendo RESGATE ESTRUTURAL de uma tradução EN→PT-BR.

A qualidade continua importante, mas o contrato estrutural é absoluto.

Para cada group recebido:

- g deve ser exatamente o mesmo.
- n informa a quantidade EXATA de strings em s.
- c contém os cues na ordem temporal.
- traduza o grupo holisticamente, mas mantenha o conteúdo semanticamente no cue correspondente.
- speaker é contexto oculto; nunca apareça na saída.
- não use nomes de falante.
- não use hífens, travessões ou barras decorativas.
- não use alongamentos vocais gráficos.
- use PT-BR natural, atual, fiel e não literal.

Responda SOMENTE:

{"items":[{"g":123,"s":["...","..."]}]}
`;

const REVIEWER_SYSTEM_PROMPT = `
Você é a SEGUNDA IA, editora independente de legendas EN→PT-BR.

Você receberá SOMENTE grupos selecionados por risco.

Corrija apenas erros reais e de alta confiança.

Não retraduza o que já está bom.

Não faça mudanças cosméticas.

Procure principalmente:

1. sentido errado, omissão ou antecipação;
2. conteúdo no cue temporal errado;
3. português literal, engessado, lusitano ou datado;
4. idiom, gíria, meme, reality, drag, LGBTQIA+, camp, shade, música, moda ou cultura pop culturalmente deslocados;
5. palavrão censurado ou colocado artificialmente;
6. concordância ou gênero errado;
7. speaker vazado como [Kelly]: ou Kelly:;
8. alongamento gráfico como você-e-e-e ou sooooo;
9. hífen, travessão ou barra decorativa de diálogo;
10. catchphrase ou termo protegido alterado.

Termos/catchphrases protegidos:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

GÊNERO:

Se speaker/contexto identifica homem, não use feminino.

Se speaker/contexto identifica mulher, não use masculino.

Se não há segurança, prefira frase natural sem marca de gênero.

speaker é pista oculta e NUNCA aparece na saída.

CONTRATO TEMPORAL:

EN e PT são arrays por cue.

Qualquer correção mantém exatamente o mesmo número de segmentos.

Cada segmento continua preso ao mesmo cue EN.

Nunca mova conteúdo entre cues.

Responda SOMENTE:

{"corrections":[{"g":123,"s":["segmento corrigido"],"why":"motivo curto"}]}

Se não houver correção:

{"corrections":[]}
`;

const QUALITY_REPAIR_SYSTEM_PROMPT = `
Você é o reparador final de uma legenda EN→PT-BR.

Receberá somente groups em que o Quality Guard detectou um risco concreto.

Use:

- EN original;
- PT atual;
- speaker oculto;
- reasons;

para corrigir SOMENTE o necessário.

Preserve exatamente g.

Preserve exatamente o número de segmentos s.

Não mova conteúdo entre cues.

Use PT-BR natural, atual, fiel e conciso.

Nunca exponha speaker.

Nunca use rótulo de falante.

Nunca use hífen, travessão ou barra decorativa.

Nunca represente alongamento vocal graficamente.

Nunca use gênero artificial como empolgado(a).

Preserve catchphrases e termos culturais quando apropriado.

Responda SOMENTE:

{"items":[{"g":123,"s":["..."]}]}
`;

// ============================================================
// HTTP PARA PROVIDERS
// ============================================================

function extractProviderText(
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
                120_000
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
                120_000
            );
        }
    }

    /*
     * Mistral pode informar reset
     * específico dos tokens.
     *
     * Se vier um valor parseável,
     * aproveitamos.
     */
    const resetTokens =
        response
            ?.headers
            ?.get(
                "x-ratelimit-reset-tokens"
            );

    if (
        resetTokens
    ) {
        const parsed =
            parseFloat(
                String(
                    resetTokens
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
                120_000
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
        120_000
    );
}

async function providerFetchJson({
    url,
    headers,
    body,
    timeoutMs,
    provider,
    job,
    counter,
    maxRetries
}) {
    let lastError =
        null;

    for (
        let attempt = 1;
        attempt <=
        maxRetries;
        attempt++
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
                    job
                ) {
                    job[
                        counter
                    ] =
                        Number(
                            job[
                                counter
                            ] ||
                            0
                        ) +
                        1;

                    const usage =
                        data.usage ||
                        {};

                    const prev =
                        job
                            .providerUsage[
                                provider
                            ] ||
                        {
                            promptTokens:
                                0,

                            completionTokens:
                                0,

                            totalTokens:
                                0
                        };

                    prev.promptTokens +=
                        Number(
                            usage.prompt_tokens ??
                            usage.input_tokens ??
                            0
                        );

                    prev.completionTokens +=
                        Number(
                            usage.completion_tokens ??
                            usage.output_tokens ??
                            0
                        );

                    prev.totalTokens +=
                        Number(
                            usage.total_tokens ??
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

                    job.providerUsage[
                        provider
                    ] =
                        prev;
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
                        1500
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
                        4000 *
                            attempt,
                        30_000
                    )
                );

            console.warn(
                `[${provider}] HTTP ${response.status}; aguardando ${Math.ceil(
                    wait /
                    1000
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
                        `${provider}: timeout por request.`
                    )
                    : error;

            /*
             * 4xx não-retryable:
             * sobe imediatamente.
             *
             * Exemplo:
             * 413 do Groq será tratado
             * pela divisão do batch.
             */
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
                    20_000
                );

            console.warn(
                `[${provider}] ${getErrorMessage(
                    lastError
                ).slice(
                    0,
                    250
                )}; retry em ${(wait / 1000).toFixed(
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
            `${provider}: falha desconhecida.`
        )
    );
}

// ============================================================
// MISTRAL
// ============================================================

async function mistralChat({
    systemPrompt,
    userPrompt,
    job,
    reasoningEffort =
        "none",
    temperature =
        0.1,
    maxTokens =
        MISTRAL_MAX_OUTPUT_TOKENS
}) {
    if (
        !MISTRAL_API_KEY
    ) {
        throw new Error(
            "MISTRAL_API_KEY não configurada."
        );
    }

    const data =
        await providerFetchJson({
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
                    "stremio-ptbr-6-1-final"
            },

            timeoutMs:
                MISTRAL_TIMEOUT_MS,

            provider:
                "MISTRAL",

            job,

            counter:
                "mistralCalls",

            maxRetries:
                MISTRAL_MAX_RETRIES
        });

    const text =
        extractProviderText(
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
            `${
                rescue
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
                : 0.1
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

    const issues = [];

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

    let items = [];

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
            parsed
                ?.items
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
    const payload =
        compactTranslationGroup(
            group
        );

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
                    payload
                )}`,

            job,

            reasoningEffort,

            temperature:
                0,

            maxTokens:
                4000
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

speaker é contexto oculto e nunca aparece na saída.

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
                1800
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

    /*
     * Primeira tentativa:
     * reasoning none.
     */
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

    /*
     * Segunda tentativa:
     * reasoning high.
     */
    console.warn(
        `[MISTRAL RESCUE] Group ${group.groupId} ainda inválido; tentando reasoning=high.`
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

    /*
     * Último recurso:
     *
     * cue por cue,
     * MAS com o Sentence Group inteiro
     * como contexto.
     */
    console.warn(
        `[MISTRAL RESCUE] Group ${group.groupId} persistente; resgate por cue.`
    );

    const segments = [];

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

    /*
     * Tudo que veio válido fica
     * imediatamente preservado.
     */
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

    /*
     * SOMENTE os inválidos
     * viram pequenos lotes.
     */
    const rescueBatches =
        splitByBudget(
            pending,
            MISTRAL_RESCUE_CHARS,
            MISTRAL_RESCUE_GROUPS,
            compactTranslationGroup
        );

    const stillInvalid = [];

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

        if (
            rescueParsed
                .invalidGroups
                .length
        ) {
            console.warn(
                `[MISTRAL RESCUE] ${rescueParsed.invalidGroups.length} ainda inválido(s): ` +
                rescueParsed
                    .invalidGroups
                    .map(
                        group =>
                            group.groupId
                    )
                    .join(
                        ","
                    )
            );

            stillInvalid.push(
                ...rescueParsed
                    .invalidGroups
            );
        }
    }

    /*
     * Apenas persistentes
     * chegam ao resgate atômico.
     */
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
// GROQ - REVISÃO SELETIVA
// ============================================================

async function groqChat({
    systemPrompt,
    userPrompt,
    job,
    maxTokens =
        GROQ_REVIEW_MAX_OUTPUT_TOKENS
}) {
    if (
        !GROQ_API_KEY
    ) {
        throw new Error(
            "GROQ_API_KEY não configurada."
        );
    }

    const data =
        await providerFetchJson({
            url:
                "https://api.groq.com/openai/v1/chat/completions",

            headers: {
                "Content-Type":
                    "application/json",

                Authorization:
                    `Bearer ${GROQ_API_KEY}`
            },

            body: {
                model:
                    GROQ_REVIEW_MODEL,

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

                temperature:
                    0,

                max_completion_tokens:
                    maxTokens,

                /*
                 * Compound Mini:
                 * nenhuma ferramenta externa
                 * é necessária para revisão.
                 */
                tool_choice:
                    "none",

                citation_options:
                    "disabled"
            },

            timeoutMs:
                GROQ_REVIEW_TIMEOUT_MS,

            provider:
                "GROQ_REVIEW",

            job,

            counter:
                "groqReviewCalls",

            maxRetries:
                GROQ_MAX_RETRIES
        });

    const text =
        extractProviderText(
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
            "Groq reviewer retornou resposta vazia."
        );
    }

    return text;
}

function compactReviewGroup(
    group,
    segments,
    reasons = []
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
            segments,

        reasons
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

function parseReviewerCorrections(
    raw,
    batch,
    translations
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
                "Groq reviewer retornou JSON inválido."
            );

        error.code =
            "BAD_REVIEW_OUTPUT";

        throw error;
    }

    let corrections =
        Array.isArray(
            parsed
        )
            ? parsed
            : parsed
                ?.corrections;

    if (
        !Array.isArray(
            corrections
        )
    ) {
        const error =
            new Error(
                "Groq reviewer não retornou corrections[]."
            );

        error.code =
            "BAD_REVIEW_OUTPUT";

        throw error;
    }

    const allowed =
        new Map(
            batch.map(
                entry => [
                    entry
                        .group
                        .groupId,

                    entry.group
                ]
            )
        );

    const seen =
        new Set();

    const accepted = [];

    for (
        const correction
        of corrections
    ) {
        const groupId =
            Number(
                correction?.g ??
                correction?.groupId
            );

        const group =
            allowed.get(
                groupId
            );

        if (
            !group ||
            seen.has(
                groupId
            )
        ) {
            continue;
        }

        let segments =
            correction?.s ??
            correction?.segments;

        if (
            !Array.isArray(
                segments
            ) &&
            group.cues.length ===
                1 &&
            typeof (
                correction?.text ??
                correction?.translation
            ) ===
                "string"
        ) {
            segments = [
                correction.text ??
                correction.translation
            ];
        }

        if (
            !Array.isArray(
                segments
            ) ||
            segments.length !==
                group.cues.length ||
            segments.some(
                text =>
                    typeof text !==
                        "string" ||
                    !text.trim()
            )
        ) {
            console.warn(
                `[GROQ REVIEW] Correção inválida ignorada g=${groupId}.`
            );

            continue;
        }

        seen.add(
            groupId
        );

        const before =
            translations.get(
                groupId
            );

        const after =
            segments.map(
                text =>
                    String(
                        text
                    ).trim()
            );

        if (
            JSON.stringify(
                before
            ) ===
            JSON.stringify(
                after
            )
        ) {
            continue;
        }

        accepted.push({
            groupId,

            before,

            after,

            why:
                String(
                    correction?.why ||
                    "correção editorial"
                ).slice(
                    0,
                    240
                )
        });
    }

    return accepted;
}

// ============================================================
// SELEÇÃO DOS GROUPS DE RISCO
// ============================================================
//
// Em vez de mandar os 1.500+ groups
// novamente para o Groq,
// escolhemos apenas os que merecem
// segunda opinião.
//
// Critérios:
// - grupos multicue;
// - múltiplos speakers;
// - speakerHint conhecido;
// - gíria/cultura/reality/drag;
// - palavrão;
// - padrões historicamente problemáticos;
// - gênero artificial;
// - sinais estruturais suspeitos.
//

function riskReasonsForGroup(
    group,
    segments
) {
    const en =
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

    const reasons = [];

    /*
     * Multi-cue:
     *
     * merece revisão porque envolve
     * distribuição temporal interna.
     */
    if (
        group.cues.length >
        1
    ) {
        reasons.push(
            "MULTI_CUE"
        );
    }

    if (
        group.multiSpeaker
    ) {
        reasons.push(
            "MULTI_SPEAKER"
        );
    }

    if (
        group.cues.some(
            cue =>
                cue.speakerHint
        )
    ) {
        reasons.push(
            "SPEAKER_CONTEXT"
        );
    }

    /*
     * Cultura / slang / reality.
     */
    if (
        /\b(?:gagged|gaggy|ate|slay|slayed|slaying|serve|served|serving|shade|mother|motherfucking|bitch|werk|queen|queens|lip[ -]?sync|no crumbs|off the top|closing ranks|game playing|tea|read her|read him|reading her|reading him|camp|fierce)\b/i.test(
            en
        )
    ) {
        reasons.push(
            "CULTURAL_IDIOM"
        );
    }

    /*
     * Palavrões.
     */
    if (
        /\b(?:fuck|fucking|fucked|shit|bitch|ass|pussy|damn|goddamn|motherfucker|motherfucking)\b/i.test(
            en
        )
    ) {
        reasons.push(
            "PROFANITY"
        );
    }

    /*
     * Padrões que já vimos falhar
     * durante o desenvolvimento.
     */
    if (
        /\b(?:amordaçad|apoiante|banheiro das moças|banheiro das damas|cabina de votação|cheque da porra|cheque do caralho|competição da porra|competição do caralho|se eu manter|e serem|uma alçapão|dublar pela sua vida|dublagem pela sua vida)\b/i.test(
            pt
        )
    ) {
        reasons.push(
            "KNOWN_BAD_PATTERN"
        );
    }

    if (
        /empolgado\(a\)|empolgada\(o\)|animado\(a\)|animada\(o\)|\bele\/ela\b|\bela\/ele\b/i.test(
            pt
        )
    ) {
        reasons.push(
            "ARTIFICIAL_GENDER"
        );
    }

    if (
        /^\s*\[[^\]]{1,60}\]\s*:|^\s*[A-ZÀ-Ý][\p{L}0-9.'’_-]*(?:\s+[A-ZÀ-Ý][\p{L}0-9.'’_-]*){0,3}\s*:\s+/mu.test(
            pt
        )
    ) {
        reasons.push(
            "SPEAKER_LABEL"
        );
    }

    if (
        /^\s*[-–—/]\s*\S/mu.test(
            pt
        )
    ) {
        reasons.push(
            "DIALOGUE_MARKER"
        );
    }

    if (
        /(?:[A-Za-zÀ-ÖØ-öø-ÿ][-–—]){3,}[A-Za-zÀ-ÖØ-öø-ÿ]|([aeiouáéíóúãõâêô])\1{3,}/iu.test(
            pt
        )
    ) {
        reasons.push(
            "VOCAL_ELONGATION"
        );
    }

    /*
     * Traduções literais clássicas.
     */
    if (
        /\bgagged\b/i.test(
            en
        ) &&
        /amordaçad/i.test(
            pt
        )
    ) {
        reasons.push(
            "GAG_LITERAL"
        );
    }

    if (
        /\bslay\w*\b/i.test(
            en
        ) &&
        /\bmat(?:ar|a|ou|aram|ando)\b/i.test(
            pt
        )
    ) {
        reasons.push(
            "SLAY_LITERAL"
        );
    }

    if (
        /\bate\b/i.test(
            en
        ) &&
        /\b(?:comeu|comi|comemos|comeram)\b/i.test(
            pt
        )
    ) {
        reasons.push(
            "ATE_LITERAL"
        );
    }

    if (
        /\blip[ -]?sync\b/i.test(
            en
        ) &&
        /\bdubl(?:agem|ar|ou|ando)\b/i.test(
            pt
        )
    ) {
        reasons.push(
            "LIPSYNC_LITERAL"
        );
    }

    return [
        ...new Set(
            reasons
        )
    ];
}

function selectReviewCandidates(
    groups,
    translations
) {
    const candidates = [];

    for (
        const group
        of groups
    ) {
        const segments =
            translations.get(
                group.groupId
            );

        const reasons =
            riskReasonsForGroup(
                group,
                segments
            );

        if (
            reasons.length
        ) {
            candidates.push({
                group,
                reasons
            });
        }
    }

    return candidates;
}

// ============================================================
// GROQ - BATCH RESILIENTE
// ============================================================
//
// 413:
// divide SOMENTE esse batch.
//
// JSON ruim:
// divide SOMENTE esse batch.
//
// Falha final de revisão:
// preserva Mistral.
//
// Nunca apaga o episódio inteiro.
//

async function reviewBatchResilient(
    batch,
    translations,
    job,
    depth = 0
) {
    const payload =
        batch.map(
            entry =>
                compactReviewGroup(
                    entry.group,

                    translations.get(
                        entry
                            .group
                            .groupId
                    ),

                    entry.reasons
                )
        );

    try {
        const raw =
            await groqChat({
                systemPrompt:
                    REVIEWER_SYSTEM_PROMPT,

                userPrompt:
                    `Revise SOMENTE estes grupos selecionados por risco.\n` +
                    `JSON de entrada:\n${JSON.stringify(
                        {
                            groups:
                                payload
                        }
                    )}`,

                job
            });

        return parseReviewerCorrections(
            raw,
            batch,
            translations
        );
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
                "BAD_REVIEW_OUTPUT" ||
            error?.status ===
                413 ||
            /HTTP 413|Request Entity Too Large/i.test(
                message
            );

        if (
            splittable &&
            batch.length >
                1 &&
            depth <
                8
        ) {
            const middle =
                Math.ceil(
                    batch.length /
                    2
                );

            const left =
                batch.slice(
                    0,
                    middle
                );

            const right =
                batch.slice(
                    middle
                );

            job.reviewerSplitRescues++;

            console.warn(
                `[GROQ REVIEW] ${message.slice(
                    0,
                    120
                )}; ` +
                `split ${batch.length} -> ${left.length}+${right.length}.`
            );

            const a =
                await reviewBatchResilient(
                    left,
                    translations,
                    job,
                    depth +
                        1
                );

            const b =
                await reviewBatchResilient(
                    right,
                    translations,
                    job,
                    depth +
                        1
                );

            return [
                ...a,
                ...b
            ];
        }

        /*
         * A revisão é uma camada
         * de melhoria.
         *
         * NÃO destrói uma tradução
         * Mistral válida por falha
         * de transporte/API.
         */
        job.reviewSkippedGroups +=
            batch.length;

        job.reviewDegraded =
            true;

        console.warn(
            `[GROQ REVIEW] ${batch.length} group(s) preservado(s) sem revisão por erro: ` +
            `${message.slice(
                0,
                220
            )}`
        );

        return [];
    }
}

async function reviewSelective(
    groups,
    translations,
    job
) {
    const candidates =
        selectReviewCandidates(
            groups,
            translations
        );

    job.reviewCandidateGroups =
        candidates.length;

    if (
        !candidates.length
    ) {
        console.log(
            "[GROQ REVIEW] Revisão seletiva: 0 grupos de risco."
        );

        return [];
    }

    /*
     * Batch baseado no payload
     * REAL EN + PT + reasons.
     */
    const batches = [];

    let current = [];

    let chars = 0;

    for (
        const entry
        of candidates
    ) {
        const item =
            compactReviewGroup(
                entry.group,

                translations.get(
                    entry
                        .group
                        .groupId
                ),

                entry.reasons
            );

        const size =
            JSON.stringify(
                item
            ).length +
            8;

        if (
            current.length &&
            (
                current.length >=
                    REVIEW_BATCH_GROUPS ||
                chars +
                    size >
                    REVIEW_BATCH_CHARS
            )
        ) {
            batches.push(
                current
            );

            current = [];

            chars = 0;
        }

        current.push(
            entry
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

    console.log(
        `[GROQ REVIEW] Revisão SELETIVA: ` +
        `${candidates.length}/${groups.length} group(s) de risco ` +
        `-> ${batches.length} lote(s).`
    );

    const startedAt =
        Date.now();

    const changes = [];

    for (
        let i = 0;
        i <
        batches.length;
        i++
    ) {
        const accepted =
            await reviewBatchResilient(
                batches[i],
                translations,
                job
            );

        for (
            const change
            of accepted
        ) {
            translations.set(
                change.groupId,
                change.after
            );

            changes.push(
                change
            );
        }

        console.log(
            `[GROQ REVIEW] ${i + 1}/${batches.length}: ${accepted.length} correção(ões).`
        );

        updateProjection(
            job,
            "Groq seletivo",
            i +
                1,
            batches.length,
            startedAt
        );

        job.progress =
            Math.max(
                job.progress,

                78 +
                Math.round(
                    (
                        (
                            i +
                            1
                        ) /
                        batches.length
                    ) *
                    15
                )
            );

        job.updatedAt =
            Date.now();
    }

    return changes;
}

// ============================================================
// LIMPEZA FINAL / PROTEÇÕES
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

                    /*
                     * [Kelly]&#58;                      */
                    clean =
                        clean.replace(
                            /^\s*\[[^\]]{1,60}\]\s*:?[ \t]*/u,
                            ""
                        );

                    /*
                     * Speaker conhecido.
                     */
                    if (
                        speakerRegex
                    ) {
                        clean =
                            clean.replace(
                                speakerRegex,
                                ""
                            );
                    }

                    /*
                     * KELLY:
                     * Kelly Clarkson:
                     */
                    clean =
                        clean.replace(
                            /^\s*[A-ZÀ-Ý][\p{L}0-9.'’_-]*(?:\s+[A-ZÀ-Ý][\p{L}0-9.'’_-]*){0,3}\s*:\s+(?=\S)/u,
                            ""
                        );

                    /*
                     * Sem:
                     *
                     * - fala
                     * — fala
                     * / fala
                     */
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

    /*
     * Barra artificial entre
     * dois falantes vira quebra
     * limpa de linha.
     */
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

    /*
     * Se o original diz "lip sync",
     * não queremos "dublagem".
     */
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

// ============================================================
// QUALITY GUARD
// ============================================================

function finalRiskScan(
    blocks,
    texts
) {
    const issues = [];

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

        const reasons = [];

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

        /*
         * Idioms culturais.
         */
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

        /*
         * Termos protegidos.
         */
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
// QUALITY REPAIR LOCALIZADO - MISTRAL
// ============================================================
//
// Só entra aqui se, mesmo após Groq,
// ainda existir um padrão concreto
// detectado pelo Quality Guard.
//
// Não refaz o episódio.
//

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
        `[QUALITY REPAIR] ${issues.length} risco(s) em ${riskyGroups.length} group(s); ` +
        `reparo Mistral localizado.`
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

    /*
     * Pequenos batches.
     */
    const batches = [];

    let current = [];

    let chars = 0;

    for (
        const entry
        of entries
    ) {
        const item =
            compactReviewGroup(
                entry.group,

                translations.get(
                    entry
                        .group
                        .groupId
                ),

                entry.reasons
            );

        const size =
            JSON.stringify(
                item
            ).length +
            8;

        if (
            current.length &&
            (
                current.length >=
                    12 ||
                chars +
                    size >
                    5_000
            )
        ) {
            batches.push(
                current
            );

            current = [];

            chars = 0;
        }

        current.push(
            entry
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

    const changed = [];

    for (
        const batch
        of batches
    ) {
        const payload =
            batch.map(
                entry =>
                    compactReviewGroup(
                        entry.group,

                        translations.get(
                            entry
                                .group
                                .groupId
                        ),

                        entry.reasons
                    )
            );

        try {
            job.qualityRepairCalls++;

            const raw =
                await mistralChat({
                    systemPrompt:
                        QUALITY_REPAIR_SYSTEM_PROMPT,

                    userPrompt:
                        `Repare estes groups:\n${JSON.stringify(
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
                        5000
                });

            /*
             * O formato da resposta é
             * compatível com o parser
             * estrutural do tradutor.
             */
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

            /*
             * Se o reparador voltou
             * estruturalmente errado,
             * resgata APENAS aquele group.
             */
            for (
                const group
                of parsed.invalidGroups
            ) {
                const repaired =
                    await rescueSingleGroup(
                        group,
                        job
                    );

                translations.set(
                    group.groupId,
                    repaired
                );

                changed.push(
                    group.groupId
                );
            }
        }
        catch (
            error
        ) {
            /*
             * Uma falha de polimento
             * NÃO deve destruir o
             * resultado primário.
             */
            job.reviewDegraded =
                true;

            console.warn(
                `[QUALITY REPAIR] Lote preservado sem reparo por erro: ` +
                `${getErrorMessage(
                    error
                ).slice(
                    0,
                    240
                )}`
            );
        }
    }

    return [
        ...new Set(
            changed
        )
    ];
}

// ============================================================
// PIPELINE FINAL
// ============================================================

async function translateSrt(
    sourceSrt,
    job
) {
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
        !GROQ_API_KEY
    ) {
        throw new Error(
            "GROQ_API_KEY não configurada no Render."
        );
    }

    const groups =
        buildSentenceGroups(
            blocks
        );

    const allBatches =
        splitByBudget(
            groups,
            MISTRAL_BATCH_CHARS,
            MISTRAL_BATCH_GROUPS,
            compactTranslationGroup
        );

    /*
     * Se estamos retomando um job
     * que falhou por problema transitório,
     * carregamos os groups já aprovados.
     */
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
        allBatches.length;

    job.progress =
        Math.max(
            job.progress ||
                0,
            1
        );

    const startedAt =
        Date.now();

    const mistralPhaseStartedAt =
        Date.now();

    console.log(
        `[PIPELINE 6.1] ${blocks.length} cues -> ` +
        `${groups.length} Sentence Groups -> ` +
        `${allBatches.length} lote(s) Mistral.`
    );

    if (
        translations.size
    ) {
        console.log(
            `[CHECKPOINT] Retomando ${translations.size}/${groups.length} group(s) Mistral já aprovados.`
        );
    }

    for (
        let i = 0;
        i <
        allBatches.length;
        i++
    ) {
        const originalBatch =
            allBatches[i];

        /*
         * Em retomada:
         * traduz somente o que ainda
         * não está no checkpoint.
         */
        const pendingBatch =
            originalBatch.filter(
                group =>
                    !translations.has(
                        group.groupId
                    )
            );

        if (
            !pendingBatch.length
        ) {
            console.log(
                `[MISTRAL] Lote ${i + 1}/${allBatches.length}: já estava em checkpoint.`
            );
        }
        else {
            console.log(
                `[MISTRAL] Lote ${i + 1}/${allBatches.length}: ` +
                `${pendingBatch.length} group(s) pendente(s).`
            );

            const result =
                await translateGroupBatchResilient(
                    pendingBatch,
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
             * CHECKPOINT APÓS CADA
             * LOTE APROVADO.
             *
             * Isso evita que uma falha
             * futura faça a tradução
             * voltar ao zero.
             */
            job.translationCheckpoint =
                new Map(
                    translations
                );

            job.updatedAt =
                Date.now();
        }

        job.completedBatches =
            i +
            1;

        job.progress =
            Math.max(
                job.progress,

                Math.round(
                    (
                        (
                            i +
                            1
                        ) /
                        allBatches.length
                    ) *
                    74
                )
            );

        console.log(
            `[MISTRAL] Lote ${i + 1}/${allBatches.length} aprovado; ` +
            `total=${translations.size}/${groups.length} groups.`
        );

        updateProjection(
            job,
            "Mistral",
            i +
                1,
            allBatches.length,
            mistralPhaseStartedAt
        );
    }

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

    // ========================================================
    // CHECKPOINT PRIMÁRIO MISTRAL
    // ========================================================
    //
    // Agora os 100% groups já existem.
    //
    // Antes de chamar Groq:
    // - limpa;
    // - constrói SRT;
    // - audita timestamps;
    // - guarda checkpoint.
    //
    // Falha posterior NÃO apaga isso.
    //

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
            76
        );

    job.updatedAt =
        Date.now();

    console.log(
        `[CHECKPOINT] Mistral completo preservado: ` +
        `${groups.length}/${groups.length} groups. ` +
        `Falha posterior de revisão NÃO apaga este resultado.`
    );

    // ========================================================
    // GROQ SELETIVO
    // ========================================================

    const reviewChanges =
        await reviewSelective(
            groups,
            translations,
            job
        );

    job.reviewChanges +=
        reviewChanges.length;

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

    /*
     * Restou risco concreto?
     *
     * Não manda episódio inteiro.
     * Repara só os groups envolvidos.
     */
    if (
        risks.length
    ) {
        const repairedGroups =
            await mistralRepairRiskGroups(
                groups,
                translations,
                risks,
                job
            );

        if (
            repairedGroups.length
        ) {
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
    }

    /*
     * Última limpeza determinística.
     *
     * Nenhum lixo visual precisa
     * bloquear o episódio inteiro.
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
        console.warn(
            `[QUALITY GUARD] ${risks.length} alerta(s) restante(s), ` +
            `SEM quebrar o episódio: ` +
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
                )
        );
    }
    else {
        console.log(
            "[QUALITY GUARD] PASSOU — 0 padrão(s) conhecido(s) restante(s)."
        );
    }

    // ========================================================
    // SRT FINAL + TIMESTAMP LOCK
    // ========================================================

    const finalSrt =
        buildSrt(
            blocks,
            texts
        );

    auditFinalTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL 6.1"
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
        `[PIPELINE 6.1] OK em ${elapsed.toFixed(
            1
        )}s | ` +
        `Mistral=${job.mistralCalls} | ` +
        `salvaged=${job.salvagedGroups} | ` +
        `rescueBatch=${job.rescueBatchCalls} | ` +
        `atomic=${job.atomicRescues} | ` +
        `high=${job.highReasoningRescues} | ` +
        `perCue=${job.perCueRescues} | ` +
        `GroqCandidates=${job.reviewCandidateGroups} | ` +
        `GroqCalls=${job.groqReviewCalls} | ` +
        `GroqCorreções=${job.reviewChanges} | ` +
        `reviewSkipped=${job.reviewSkippedGroups} | ` +
        `qualityRepairCalls=${job.qualityRepairCalls} | ` +
        `riscos finais=${risks.length}.`
    );

    return finalSrt;
}

// ============================================================
// FILA DE JOBS
// ============================================================

async function processJob(
    job
) {
    /*
     * IMPORTANTE:
     *
     * O relógio de processamento
     * começa AQUI.
     *
     * Mas ele serve apenas para
     * estatística/projeção.
     *
     * NÃO existe deadline.
     */
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
        `[JOB ${job.id}] Iniciando processamento após ` +
        `${job.queueWaitSeconds.toFixed(
            1
        )}s na fila. ` +
        `SEM teto global de tempo.`
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

        /*
         * Somente o final completo
         * entra no cache final.
         */
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

        /*
         * Se já existem groups Mistral
         * válidos, eles ficam preservados
         * enquanto este processo Render
         * continuar vivo.
         *
         * Nova tentativa do mesmo source
         * pode retomar.
         */
        if (
            job.translationCheckpoint
                instanceof Map &&
            job.translationCheckpoint
                .size
        ) {
            console.error(
                `[CHECKPOINT] ${job.translationCheckpoint.size} group(s) ` +
                `preservados em memória para retomada.`
            );
        }
    }
}

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
    // MESMO JOB JÁ RODANDO
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
    // RETOMADA DE JOB FALHO
    // ========================================================
    //
    // Se foi o mesmo source e ainda
    // estamos no mesmo processo Render,
    // NÃO joga fora os batches Mistral.
    //

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
            `${reusable.translationCheckpoint?.size || 0} group(s) já preservados.`
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
    // JOB NOVO
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
// OPENSUBTITLES - RELEASE AWARE
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
                        "Stremio-PTBR-DualAI/6.1"
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
                        "Stremio-PTBR-DualAI/6.1"
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
// SRT DE STATUS
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
// STREMIO
// ============================================================

const manifest = {
    /*
     * Mantém o mesmo ID do addon
     * Render anterior para não
     * duplicar instalação.
     */
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "6.1.0",

    name:
        "Tradutor PT-BR Premium",

    description:
        "Mistral Medium 3.5 + revisão seletiva Groq Compound Mini + checkpoints + resgate estrutural + timestamps imutáveis.",

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

            reviewer:
                GROQ_REVIEW_MODEL,

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

            groqConfigured:
                Boolean(
                    GROQ_API_KEY
                ),

            geminiRollbackConfigured:
                Boolean(
                    GEMINI_API_KEY
                ),

            geminiRollbackModel:
                GEMINI_MODEL,

            queue:
                translationJobQueue.length,

            processing:
                translationJobWorkerRunning,

            cache:
                translationCache.size,

            jobs:
                jobs.size,

            rules: {
                globalTimeCeiling:
                    false,

                dynamicEfficiencyProjection:
                    true,

                sentenceGroups:
                    true,

                resilientPartialSalvage:
                    true,

                checkpointPerMistralBatch:
                    true,

                resumeFailedJobFromCheckpoint:
                    true,

                primaryCheckpointBeforeReview:
                    true,

                selectiveSecondAiReview:
                    true,

                reviewerNeverInvalidatesPrimaryOnTransportError:
                    true,

                targetedQualityRepair:
                    true,

                timestampsImmutable:
                    true,

                speakerLabelsForbidden:
                    true,

                hiddenSpeakerContext:
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
                GROQ_REVIEW_MODEL,

            cacheVersion:
                TRANSLATION_CACHE_VERSION,

            queue:
                translationJobQueue.length,

            processing:
                translationJobWorkerRunning
        });
    }
);

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
// API DA PONTE LOCAL
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
// STATUS DO JOB
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

                mistralCalls:
                    job.mistralCalls ||
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

                reviewCandidateGroups:
                    job.reviewCandidateGroups ||
                    0,

                groqReviewCalls:
                    job.groqReviewCalls ||
                    0,

                reviewerSplitRescues:
                    job.reviewerSplitRescues ||
                    0,

                reviewerSkippedGroups:
                    job.reviewSkippedGroups ||
                    0,

                reviewChanges:
                    job.reviewChanges ||
                    0,

                reviewDegraded:
                    Boolean(
                        job.reviewDegraded
                    ),

                qualityRepairCalls:
                    job.qualityRepairCalls ||
                    0,

                qualityGuardRisks:
                    job.qualityGuardRisks ||
                    0,

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
            " STREMIO PT-BR DUAL AI TRANSLATOR 6.1 FINAL"
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
            `Revisor seletivo: ${GROQ_REVIEW_MODEL}`
        );

        console.log(
            `Mistral: ${
                MISTRAL_API_KEY
                    ? "CONFIGURADO ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            `Groq: ${
                GROQ_API_KEY
                    ? "CONFIGURADO ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            `Gemini rollback: ${
                GEMINI_API_KEY
                    ? `DISPONÍVEL (${GEMINI_MODEL}) ✅`
                    : "não configurado"
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
            `Resgate inválidos: ${MISTRAL_RESCUE_GROUPS} groups / ${MISTRAL_RESCUE_CHARS} chars`
        );

        console.log(
            `Groq SELETIVO batch efetivo: ${REVIEW_BATCH_GROUPS} groups / ${REVIEW_BATCH_CHARS} chars`
        );

        console.log(
            "Teto global de tradução: REMOVIDO ✅"
        );

        console.log(
            "Timeout apenas por request + retries: ATIVO ✅"
        );

        console.log(
            "Projeção dinâmica por eficiência: ATIVA, NÃO INTERROMPE JOB ✅"
        );

        console.log(
            "Checkpoint após cada lote Mistral: ATIVO ✅"
        );

        console.log(
            "Retomada de job falho pelo checkpoint: ATIVA ✅"
        );

        console.log(
            "Checkpoint Mistral antes do Groq: ATIVO ✅"
        );

        console.log(
            "Groq revisa SOMENTE grupos de risco: ATIVO ✅"
        );

        console.log(
            "Falha do Groq NÃO apaga Mistral: ATIVO ✅"
        );

        console.log(
            "Quality Repair localizado: ATIVO ✅"
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
