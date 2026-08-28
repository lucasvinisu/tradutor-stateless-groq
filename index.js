const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// STREMIO PT-BR BACKEND 6.4 FINAL
// ============================================================
// Ponte Local = única interface do Stremio.
// Render = backend de tradução/revisão.
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

const CACHE_VERSION = "6.4.0-final";

const MAX_SOURCE_CHARS = 800000;
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const SOURCE_FETCH_TIMEOUT_MS = 20000;

const MISTRAL_TIMEOUT_MS = Number(
    process.env.MISTRAL_TIMEOUT_MS ||
    150000
);

const GEMINI_TIMEOUT_MS = Number(
    process.env.GEMINI_TIMEOUT_MS ||
    90000
);

const MISTRAL_MAX_RETRIES = Number(
    process.env.MISTRAL_MAX_RETRIES ||
    8
);

const GEMINI_MAX_RETRIES = Number(
    process.env.GEMINI_MAX_RETRIES ||
    8
);

const MISTRAL_BATCH_CHARS = Number(
    process.env.MISTRAL_BATCH_CHARS ||
    18000
);

const MISTRAL_BATCH_GROUPS = Number(
    process.env.MISTRAL_BATCH_GROUPS ||
    320
);

const MISTRAL_RESCUE_CHARS = Number(
    process.env.MISTRAL_RESCUE_CHARS ||
    8000
);

const MISTRAL_RESCUE_GROUPS = Number(
    process.env.MISTRAL_RESCUE_GROUPS ||
    24
);

const MISTRAL_CONCURRENCY = Math.max(
    1,
    Math.min(
        Number(
            process.env.MISTRAL_CONCURRENCY ||
            2
        ),
        2
    )
);

/*
 * Controle preditivo para evitar 429 antes do envio.
 *
 * Nos testes desta conta o Medium mostrou throughput próximo
 * de 25K TPM. Usamos margem interna.
 *
 * Se o limite da conta mudar no futuro:
 * MISTRAL_TARGET_TPM
 *
 * Isto NÃO é um teto de duração.
 */
const MISTRAL_TARGET_TPM = Math.max(
    8000,
    Number(
        process.env.MISTRAL_TARGET_TPM ||
        23500
    )
);

const MISTRAL_TPM_WINDOW_MS = 60000;

const GEMINI_MAX_OUTPUT_TOKENS = Number(
    process.env.GEMINI_REVIEW_MAX_OUTPUT_TOKENS ||
    10000
);

const ARBITER_BATCH_GROUPS = 30;
const ARBITER_BATCH_CHARS = 7000;

// ============================================================
// STATE
// ============================================================

const translationCache = new Map();
const jobs = new Map();
const queue = [];

let queueRunning = false;

// ============================================================
// HELPERS
// ============================================================

const sleep = ms =>
    new Promise(
        resolve =>
            setTimeout(
                resolve,
                ms
            )
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

    const proto =
        String(
            req.headers["x-forwarded-proto"] ||
            req.protocol ||
            "https"
        )
            .split(",")[0]
            .trim();

    const host =
        String(
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

    const a =
        Buffer.from(
            String(
                req.headers.authorization ||
                ""
            ).trim()
        );

    const b =
        Buffer.from(
            `Bearer ${LOCAL_BRIDGE_SECRET}`
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
// CACHE / JOBS
// ============================================================

function cacheKey(
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
        translationCache.delete(
            key
        );

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
            `job-${sourceHash.slice(
                0,
                24
            )}-${randomId()}`,

        type,
        videoId,
        filename,
        sourceSrt,
        sourceKind,
        sourceHash,

        cacheKey:
            cacheKey(
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

        processingStartedAt:
            null,

        expiresAt:
            now +
            JOB_TTL_MS,

        translationCheckpoint:
            new Map(),

        primaryCheckpoint:
            null,

        stats: {
            mistralCalls:
                0,

            mistralAttempts:
                0,

            mistral429:
                0,

            predictiveWaits:
                0,

            predictiveWaitMs:
                0,

            deterministicSingleCueJoins:
                0,

            geminiCalls:
                0,

            geminiAttempts:
                0,

            gemini429:
                0,

            geminiReviewed:
                0,

            geminiProposals:
                0,

            arbiterCalls:
                0,

            arbiterAccepted:
                0,

            salvageGroups:
                0,

            rescueBatches:
                0,

            atomicRescues:
                0,

            perCueRescues:
                0,

            qualityRepairs:
                0,

            hardFixes:
                0,

            omissionRisks:
                0
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

function getOrCreateJob({
    type,
    videoId,
    filename,
    sourceSrt,
    sourceKind
}) {
    const key =
        cacheKey(
            type,
            videoId,
            sourceSrt
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
                createJob({
                    type,
                    videoId,
                    filename,
                    sourceSrt,
                    sourceKind
                });

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
        createJob({
            type,
            videoId,
            filename,
            sourceSrt,
            sourceKind
        });

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

    10 * 60 * 1000
).unref();

// ============================================================
// SRT SOURCE CLEANING
// ============================================================

const TIMING_RE =
    /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
    /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

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

function isEmptyVocalization(text) {
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

    /*
     * Só remove vocalização repetida
     * realmente vazia:
     *
     * ah ah
     * ha ha
     * heh heh
     *
     * Não remove:
     * uh-huh
     * uh-uh
     * hmm
     * Oh!
     */
    return /^(?:ah|ha|heh)(?:\s+(?:ah|ha|heh)){1,5}$/.test(
        value
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
            /[♪♫♬]/gu,
            " "
        );

    text =
        normalizeElongations(
            text
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

function cleanSrtForTranslation(srt) {
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

            if (cleaned) {
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
        `[CLEAN] ${rawBlocks.length} -> ${out.length}; ` +
        `removidos=${removed}; ` +
        `speakerHints=${speakerHints}; ` +
        `vocalizações=${vocalizations}.`
    );

    if (!out.length) {
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
                (
                    block,
                    index
                ) =>
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
        let index = 0;
        index < source.length;
        index++
    ) {
        if (
            source[index].index !==
                final[index].index ||
            source[index].timing !==
                final[index].timing
        ) {
            throw new Error(
                `TIMING LOCK ${label}: ` +
                `cue ${source[index].index}.`
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

function parseTimeSeconds(value) {
    const match =
        String(value || "")
            .match(
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
        String(timing || "")
            .match(
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

function groupingText(text) {
    return String(text || "")
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
                /^\s*[-–—]\s*\S/u.test(
                    line
                )
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
        group[
            group.length - 1
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

function buildSentenceGroups(blocks) {
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

function compactGroup(group) {
    return {
        g:
            group.groupId,

        n:
            group.cues.length,

        c:
            group.cues.map(
                cue => ({
                    i:
                        cue.index,

                    t:
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

// ============================================================
// EDITORIAL PROMPTS
// ============================================================

const TRANSLATOR_PROMPT = `
Você é o TRADUTOR PRINCIPAL de legendas EN→PT-BR.

Produza uma excelente legenda brasileira:
natural, contemporânea, oral, fiel, contextual e concisa.

Nunca soe:
literal,
engessado,
antiquado,
lusitano,
artificial
ou com gíria de tiozão.

Também não force gíria jovem onde ela não cabe.

Reality TV, drag, LGBTQIA+, música, moda, memes e cultura pop:
entenda a referência e adapte culturalmente quando necessário.

Use linguagem Gen Z/Alpha somente quando combinar de verdade com:
personagem,
época,
tom,
fandom,
humor
e referência.

Preserve personalidade, humor, ironia, shade, camp,
intensidade, vulgaridade e palavrões.
Não censure.

"I'm gagged" NÃO é "estou amordaçada".
Pode ser "Tô passada", "Tô em choque", "Tô sem reação",
conforme contexto.

"She ate" como gíria NÃO é comer literalmente.
Pode ser "Ela arrasou", "Ela entregou tudo", "Ela serviu demais".

"no crumbs" pode ser "não deixou nada pra ninguém".

"off the top" em dinheiro =
comissão/corte/porcentagem.

"closing ranks" =
grupo se protegendo/panelinha.

"Carry the two" em conta =
"vai dois".

"week one" =
"primeira semana".

Evite "apoiante".

Preserve quando presentes:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

CONTRATO TEMPORAL ABSOLUTO:

Cada Sentence Group contém n cues consecutivos.

Entenda o group holisticamente,
mas devolva EXATAMENTE n strings em s,
uma por cue
e na mesma ordem.

Cada string deve conter TODO o conteúdo
semanticamente falado naquele cue.

Não omita o fim de uma frase/raciocínio.

Não antecipe conteúdo do cue seguinte.

Não atrase conteúdo para outro cue.

Se uma frase atravessar vários cues,
traduza com o contexto completo
mas preserve a distribuição temporal do inglês.

speaker é CONTEXTO OCULTO.

Nunca escreva:

[NOME]
NOME:
rótulo de falante
nome de personagem

Use speaker apenas para referência e gênero.

Se gênero não for seguro,
reformule naturalmente de forma neutra.

Nunca use:

empolgado(a)
animado(a)
ele/ela
ela/ele
barras de gênero

Não acrescente:

hífen de diálogo
travessão de diálogo
barra "/" como separador
labels
SDH/CC
notas

Não represente alongamentos vocais graficamente.

Responda SOMENTE JSON:

{"items":[{"g":1,"s":["segmento cue 1","segmento cue 2"]}]}
`;

const RESCUE_PROMPT = `
Faça RESGATE ESTRUTURAL EN→PT-BR
mantendo a mesma qualidade editorial.

g permanece igual.

s contém EXATAMENTE n strings,
uma por cue,
na ordem original.

Preserve TODO o conteúdo falado.

Nunca mova conteúdo entre cues.

speaker é oculto.

Sem nomes.
Sem labels.
Sem barra decorativa.
Sem hífen/travessão decorativo.
Sem alongamento gráfico.

Use PT-BR natural,
contemporâneo,
fiel
e não literal.

Responda SOMENTE:

{"items":[{"g":1,"s":["..."]}]}
`;

const GEMINI_REVIEW_PROMPT = `
Você é a SEGUNDA IA independente
de revisão de legendas EN→PT-BR.

O PT foi produzido por Mistral Medium 3.5.

Revise 100% dos groups recebidos,
cue por cue.

Não reescreva o que já está bom.

Não faça mudança cosmética.

Proponha mudança somente por problema real
e de alta confiança.

Cheque especialmente:

1. sentido incorreto ou referência mal entendida;

2. COMPLETUDE:
parte do EN sumiu,
o fim da frase/raciocínio foi perdido,
ou o PT resumiu demais;

3. SINCRONIA SEMÂNTICA:
conteúdo antecipado
ou atrasado entre cues;

4. português literal,
engessado,
antiquado,
lusitano
ou artificial;

5. reality,
drag,
LGBTQIA+,
Gen Z/Alpha,
memes,
música,
moda,
shade,
camp
e cultura pop;

6. gíria de tiozão,
gíria jovem forçada,
palavrão censurado
ou mal posicionado;

7. concordância/gênero errado;

8. speaker vazado,
[NOME],
NOME:,
barra "/",
"--" cru,
hífen/travessão decorativo
ou alongamento gráfico.

Preserve:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

speaker é contexto oculto
e nunca aparece.

Cada proposta mantém EXATAMENTE
o mesmo número de segmentos.

Nunca mova conteúdo entre cues.

Responda SOMENTE:

{"reviewed":123,"proposals":[{"g":12,"s":["..."],"why":"motivo curto","confidence":0.98}]}

Se tudo estiver bom:

{"reviewed":123,"proposals":[]}
`;

const ARBITER_PROMPT = `
Você é o ÁRBITRO FINAL Mistral.

Compare:

EN
PT original Mistral
proposta Gemini

Aceite somente se a proposta
for claramente mais:

correta,
completa,
fiel,
natural,
contemporânea
ou melhor sincronizada semanticamente por cue.

Na dúvida,
mantenha o original.

Não aceite mudança cosmética.

Nunca altere número de segmentos.

Nunca mova conteúdo entre cues.

Nunca exponha speaker.

Nunca introduza label.

Nunca introduza barra decorativa.

Nunca introduza hífen/travessão decorativo.

Nunca introduza alongamento gráfico.

Nunca chute gênero.

Responda SOMENTE:

{"accepted":[{"g":123,"s":["..."],"why":"motivo"}]}
`;

const REPAIR_PROMPT = `
Você é o reparador final EN→PT-BR.

Recebe somente groups com risco concreto.

Use:

EN original
PT atual
speaker oculto
reasons

Corrija SOMENTE o problema indicado.

Mantenha PT-BR:

natural,
contemporâneo,
oral,
fiel,
culturalmente correto.

Se reasons incluir POSSIBLE_OMISSION:
restaure qualquer conteúdo do EN
que tenha sido perdido,
sem mover fala para outro cue.

Preserve:

g
quantidade exata de segmentos
ordem
temporalidade

Nunca exponha speaker.

Nunca use labels.

Nunca use barra decorativa.

Nunca use hífen/travessão decorativo.

Nunca use alongamentos.

Responda SOMENTE:

{"items":[{"g":123,"s":["..."]}]}
`;

// ============================================================
// PREDICTIVE MISTRAL PACER
// ============================================================

class PredictiveTokenLimiter {

    constructor(
        targetTokens,
        windowMs
    ) {
        this.targetTokens =
            targetTokens;

        this.windowMs =
            windowMs;

        this.events =
            [];

        this.sequence =
            0;

        this.cooldownUntil =
            0;
    }

    prune() {
        const cutoff =
            Date.now() -
            this.windowMs;

        this.events =
            this.events.filter(
                event =>
                    event.ts >
                    cutoff
            );
    }

    used() {
        this.prune();

        return this.events.reduce(
            (
                sum,
                event
            ) =>
                sum +
                event.tokens,
            0
        );
    }

    async acquire(
        estimatedTokens,
        job
    ) {
        const estimate =
            Math.max(
                500,
                Math.min(
                    Math.ceil(
                        estimatedTokens
                    ),
                    this.targetTokens
                )
            );

        while (true) {
            const now =
                Date.now();

            if (
                now <
                this.cooldownUntil
            ) {
                const wait =
                    this.cooldownUntil -
                    now;

                if (job) {
                    job.stats.predictiveWaits++;

                    job.stats.predictiveWaitMs +=
                        wait;
                }

                console.log(
                    `[MISTRAL PACER] cooldown ` +
                    `${(wait / 1000).toFixed(1)}s.`
                );

                await sleep(
                    wait
                );

                continue;
            }

            this.prune();

            const used =
                this.used();

            if (
                used +
                    estimate <=
                this.targetTokens
            ) {
                const ticket = {
                    id:
                        ++this.sequence,

                    ts:
                        Date.now(),

                    tokens:
                        estimate
                };

                this.events.push(
                    ticket
                );

                console.log(
                    `[MISTRAL PACER] reserva~${estimate} | ` +
                    `janela~${used + estimate}/${this.targetTokens} tokens.`
                );

                return ticket;
            }

            const oldest =
                this.events.reduce(
                    (
                        a,
                        b
                    ) =>
                        a.ts <=
                        b.ts
                            ? a
                            : b
                );

            const wait =
                Math.max(
                    300,
                    oldest.ts +
                        this.windowMs -
                        Date.now() +
                        150
                );

            if (job) {
                job.stats.predictiveWaits++;

                job.stats.predictiveWaitMs +=
                    wait;
            }

            console.log(
                `[MISTRAL PACER] evitando 429: ` +
                `janela~${used}/${this.targetTokens}; ` +
                `espera ${(wait / 1000).toFixed(1)}s.`
            );

            await sleep(
                wait
            );
        }
    }

    reconcile(
        ticket,
        actualTokens
    ) {
        const actual =
            Number(
                actualTokens ||
                0
            );

        if (
            !ticket ||
            !Number.isFinite(
                actual
            ) ||
            actual <= 0
        ) {
            return;
        }

        const item =
            this.events.find(
                event =>
                    event.id ===
                    ticket.id
            );

        if (item) {
            item.tokens =
                actual;
        }
    }

    cancel(ticket) {
        if (!ticket) {
            return;
        }

        this.events =
            this.events.filter(
                event =>
                    event.id !==
                    ticket.id
            );
    }

    note429(waitMs) {
        this.cooldownUntil =
            Math.max(
                this.cooldownUntil,
                Date.now() +
                    Math.max(
                        1000,
                        waitMs
                    )
            );
    }

    status() {
        return {
            targetTPM:
                this.targetTokens,

            estimatedUsed:
                this.used(),

            cooldownMs:
                Math.max(
                    0,
                    this.cooldownUntil -
                    Date.now()
                )
        };
    }
}

class ConcurrencyGate {

    constructor(limit) {
        this.limit =
            limit;

        this.active =
            0;

        this.waiters =
            [];
    }

    async acquire() {
        while (
            this.active >=
            this.limit
        ) {
            await new Promise(
                resolve =>
                    this.waiters.push(
                        resolve
                    )
            );
        }

        this.active++;
    }

    release() {
        this.active =
            Math.max(
                0,
                this.active -
                1
            );

        const waiter =
            this.waiters.shift();

        if (waiter) {
            waiter();
        }
    }
}

const mistralLimiter =
    new PredictiveTokenLimiter(
        MISTRAL_TARGET_TPM,
        MISTRAL_TPM_WINDOW_MS
    );

const mistralConcurrencyGate =
    new ConcurrencyGate(
        MISTRAL_CONCURRENCY
    );

function estimateMistralTokens(
    system,
    user,
    purpose
) {
    const input =
        (
            String(
                system ||
                ""
            ).length +
            String(
                user ||
                ""
            ).length
        ) /
        3.6;

    const output =
        String(
            user ||
            ""
        ).length /
        (
            purpose ===
            "translation"
                ? 4.0
                : purpose ===
                    "arbiter"
                    ? 7.0
                    : 5.5
        );

    return Math.max(
        1000,
        Math.min(
            12000,
            Math.ceil(
                input +
                output
            )
        )
    );
}

function retryAfterMs(
    response,
    fallback
) {
    const header =
        response
            ?.headers
            ?.get(
                "retry-after"
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
        const raw =
            response
                ?.headers
                ?.get(
                    name
                );

        if (!raw) {
            continue;
        }

        const seconds =
            parseFloat(
                String(raw)
            );

        if (
            Number.isFinite(
                seconds
            ) &&
            seconds >
                0
        ) {
            return Math.min(
                Math.max(
                    1000,
                    seconds *
                        1000
                ),
                120000
            );
        }
    }

    return Math.min(
        Math.max(
            Number(
                fallback
            ) ||
            4000,
            1000
        ),
        120000
    );
}

function extractMistralText(content) {
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
            .join("");
    }

    return "";
}

async function mistralChat({
    system,
    user,
    job,
    reasoning =
        "none",
    temperature =
        0.1,
    maxTokens =
        16000,
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

    const provider =
        purpose ===
            "arbiter"
            ? "MISTRAL_ARBITER"
            : purpose ===
                "repair"
                ? "MISTRAL_REPAIR"
                : "MISTRAL";

    let lastError =
        null;

    for (
        let attempt = 1;
        attempt <=
        MISTRAL_MAX_RETRIES;
        attempt++
    ) {
        job.stats.mistralAttempts++;

        const ticket =
            await mistralLimiter.acquire(
                estimateMistralTokens(
                    system,
                    user,
                    purpose
                ),
                job
            );

        await mistralConcurrencyGate.acquire();

        let gateHeld =
            true;

        const releaseGate =
            () => {
                if (
                    gateHeld
                ) {
                    gateHeld =
                        false;

                    mistralConcurrencyGate.release();
                }
            };

        const controller =
            new AbortController();

        const timer =
            setTimeout(
                () =>
                    controller.abort(),
                MISTRAL_TIMEOUT_MS
            );

        try {
            console.log(
                `[${provider}] Request ` +
                `${attempt}/${MISTRAL_MAX_RETRIES}.`
            );

            const response =
                await fetch(
                    "https://api.mistral.ai/v1/chat/completions",
                    {
                        method:
                            "POST",

                        headers: {
                            "Content-Type":
                                "application/json",

                            Authorization:
                                `Bearer ${MISTRAL_API_KEY}`
                        },

                        body:
                            JSON.stringify({
                                model:
                                    MISTRAL_MODEL,

                                messages: [
                                    {
                                        role:
                                            "system",

                                        content:
                                            system
                                    },

                                    {
                                        role:
                                            "user",

                                        content:
                                            user
                                    }
                                ],

                                response_format: {
                                    type:
                                        "json_object"
                                },

                                reasoning_effort:
                                    reasoning,

                                temperature,

                                max_tokens:
                                    maxTokens,

                                prompt_cache_key:
                                    purpose ===
                                    "translation"
                                        ? "stremio-ptbr-6-4-translator"
                                        : "stremio-ptbr-6-4-editor"
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
            }
            catch {}

            if (
                response.ok &&
                data
            ) {
                mistralLimiter.reconcile(
                    ticket,
                    Number(
                        data
                            ?.usage
                            ?.total_tokens ||
                        0
                    )
                );

                if (
                    purpose ===
                    "arbiter"
                ) {
                    job.stats.arbiterCalls++;
                }
                else {
                    job.stats.mistralCalls++;
                }

                const text =
                    extractMistralText(
                        data
                            ?.choices
                            ?.[0]
                            ?.message
                            ?.content
                    );

                if (!text) {
                    throw new Error(
                        `${provider} retornou resposta vazia.`
                    );
                }

                return text;
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
                    `${provider} HTTP ${response.status}: ` +
                    `${String(
                        message
                    ).slice(
                        0,
                        1400
                    )}`
                );

            error.status =
                response.status;

            if (
                response.status ===
                429
            ) {
                job.stats.mistral429++;

                mistralLimiter.cancel(
                    ticket
                );

                const wait =
                    retryAfterMs(
                        response,
                        Math.min(
                            4000 *
                                attempt,
                            30000
                        )
                    );

                mistralLimiter.note429(
                    wait
                );

                if (
                    attempt ===
                    MISTRAL_MAX_RETRIES
                ) {
                    throw error;
                }

                console.warn(
                    `[${provider}] HTTP 429 residual; ` +
                    `aguardando ${Math.ceil(
                        wait /
                        1000
                    )}s.`
                );

                releaseGate();

                clearTimeout(
                    timer
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

            mistralLimiter.cancel(
                ticket
            );

            if (
                !retryable ||
                attempt ===
                    MISTRAL_MAX_RETRIES
            ) {
                throw error;
            }

            const wait =
                Math.min(
                    2500 *
                        attempt,
                    20000
                );

            releaseGate();

            clearTimeout(
                timer
            );

            await sleep(
                wait
            );
        }
        catch (error) {
            lastError =
                error?.name ===
                    "AbortError"
                    ? new Error(
                        `${provider}: timeout por request.`
                    )
                    : error;

            mistralLimiter.cancel(
                ticket
            );

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
                MISTRAL_MAX_RETRIES
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
                `[${provider}] ` +
                `${errorMessage(
                    lastError
                ).slice(
                    0,
                    220
                )}; ` +
                `retry em ${(wait / 1000).toFixed(
                    1
                )}s.`
            );

            releaseGate();

            clearTimeout(
                timer
            );

            await sleep(
                wait
            );
        }
        finally {
            clearTimeout(
                timer
            );

            releaseGate();
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
// TRANSLATION PARSER / RESCUE
// ============================================================

function parseTranslationResponse(
    groups,
    raw,
    job
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

    const items =
        Array.isArray(
            parsed
        )
            ? parsed
            : Array.isArray(
                parsed?.items
            )
                ? parsed.items
                : parsed &&
                    typeof parsed ===
                        "object" &&
                    parsed.g !=
                        null
                    ? [
                        parsed
                    ]
                    : [];

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

        /*
         * Otimização segura:
         *
         * 1 cue + modelo devolveu vários pedaços.
         *
         * Todos pertencem ao MESMO timestamp.
         * Em vez de chamar Mistral novamente,
         * juntamos no mesmo cue.
         */
        if (
            group.cues.length ===
                1 &&
            Array.isArray(
                segments
            ) &&
            segments.length >
                1 &&
            segments.length <=
                4 &&
            segments.every(
                value =>
                    typeof value ===
                        "string" &&
                    value.trim()
            )
        ) {
            const originalCount =
                segments.length;

            segments = [
                segments
                    .map(
                        value =>
                            value.trim()
                    )
                    .join("\n")
            ];

            job
                .stats
                .deterministicSingleCueJoins++;

            console.log(
                `[STRUCTURAL FIX] g${groupId}: ` +
                `oversplit ${originalCount}->1 unido no mesmo cue.`
            );
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
                `g${groupId}:esperava=${group.cues.length},` +
                `recebeu=${segments.length}`
            );

            continue;
        }

        if (
            segments.some(
                value =>
                    typeof value !==
                        "string" ||
                    !value.trim()
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
                value =>
                    value.trim()
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

async function translateBatch(
    groups,
    job,
    rescue = false
) {
    const raw =
        await mistralChat({
            system:
                rescue
                    ? RESCUE_PROMPT
                    : TRANSLATOR_PROMPT,

            user:
                `${rescue
                    ? "Resgate"
                    : "Traduza"
                } estes groups:\n` +
                `${JSON.stringify({
                    groups:
                        groups.map(
                            compactGroup
                        )
                })}`,

            job,

            temperature:
                rescue
                    ? 0
                    : 0.1,

            purpose:
                "translation"
        });

    return parseTranslationResponse(
        groups,
        raw,
        job
    );
}

async function rescueSingleGroup(
    group,
    job
) {
    job.stats.atomicRescues++;

    console.warn(
        `[MISTRAL RESCUE] ` +
        `Group ${group.groupId} isolado.`
    );

    for (
        const reasoning
        of [
            "none",
            "high"
        ]
    ) {
        const raw =
            await mistralChat({
                system:
                    RESCUE_PROMPT,

                user:
                    `Traduza este group com EXATAMENTE ` +
                    `${group.cues.length} string(s):\n` +
                    `${JSON.stringify(
                        compactGroup(
                            group
                        )
                    )}`,

                job,

                reasoning,

                temperature:
                    0,

                maxTokens:
                    4000,

                purpose:
                    "translation"
            });

        const parsed =
            parseTranslationResponse(
                [
                    group
                ],
                raw,
                job
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
    }

    const output =
        [];

    for (
        let index = 0;
        index <
        group.cues.length;
        index++
    ) {
        const raw =
            await mistralChat({
                system:
                    `Traduza SOMENTE o cue target=true ` +
                    `para PT-BR natural, contemporâneo e COMPLETO. ` +
                    `Use os demais apenas como contexto. ` +
                    `speaker é oculto. ` +
                    `Não mova conteúdo temporalmente. ` +
                    `Responda {"text":"..."}.`,

                user:
                    JSON.stringify({
                        group:
                            group.groupId,

                        cues:
                            group.cues.map(
                                (
                                    cue,
                                    cueIndex
                                ) => ({
                                    id:
                                        cue.index,

                                    text:
                                        cue.text,

                                    speaker:
                                        cue.speakerHint ||
                                        undefined,

                                    target:
                                        cueIndex ===
                                        index
                                })
                            )
                    }),

                job,

                reasoning:
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
            parsed =
                null;
        }

        const text =
            parsed?.text ??
            parsed?.translation;

        if (
            typeof text !==
                "string" ||
            !text.trim()
        ) {
            throw new Error(
                `Resgate por cue falhou ` +
                `no group ${group.groupId}.`
            );
        }

        output.push(
            text.trim()
        );

        job.stats.perCueRescues++;
    }

    return output;
}

async function translateBatchResilient(
    groups,
    job
) {
    const first =
        await translateBatch(
            groups,
            job,
            false
        );

    const result =
        new Map(
            first.valid
        );

    if (
        !first
            .invalidGroups
            .length
    ) {
        return result;
    }

    job.stats.salvageGroups +=
        first.valid.size;

    console.warn(
        `[MISTRAL SALVAGE] ` +
        `válidos=${first.valid.size}/${groups.length}; ` +
        `resgatar=${first.invalidGroups.length}; ` +
        `${first.issues
            .slice(
                0,
                6
            )
            .join(
                " | "
            )}`
    );

    const rescueBatches =
        splitByBudget(
            first.invalidGroups,
            MISTRAL_RESCUE_CHARS,
            MISTRAL_RESCUE_GROUPS,
            compactGroup
        );

    for (
        const batch
        of rescueBatches
    ) {
        job.stats.rescueBatches++;

        if (
            batch.length === 1
        ) {
            const group =
                batch[0];

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
            `[MISTRAL RESCUE] ` +
            `Lote pequeno com ${batch.length} group(s).`
        );

        const parsed =
            await translateBatch(
                batch,
                job,
                true
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

        for (
            const group
            of parsed.invalidGroups
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
            `Resgate estrutural incompleto: ` +
            `${missing
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
// GEMINI REVIEW
// ============================================================

function compactReviewGroup(
    group,
    segments
) {
    const speakers =
        group.cues.map(
            cue =>
                cue.speakerHint ||
                null
        );

    return {
        g:
            group.groupId,

        en:
            group.cues.map(
                cue =>
                    cue.text
            ),

        pt:
            segments,

        ...(
            speakers.some(
                Boolean
            )
                ? {
                    speaker:
                        speakers
                }
                : {}
        )
    };
}

function extractGeminiText(data) {
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
        .join("")
        .trim();
}

async function geminiRequest(
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

    let lastError =
        null;

    for (
        let attempt = 1;
        attempt <=
        GEMINI_MAX_RETRIES;
        attempt++
    ) {
        job.stats.geminiAttempts++;

        const controller =
            new AbortController();

        const timer =
            setTimeout(
                () =>
                    controller.abort(),
                GEMINI_TIMEOUT_MS
            );

        try {
            console.log(
                `[GEMINI_REVIEW] Request ` +
                `${attempt}/${GEMINI_MAX_RETRIES}.`
            );

            const response =
                await fetch(
                    `https://generativelanguage.googleapis.com/` +
                    `v1beta/models/${encodeURIComponent(
                        GEMINI_MODEL
                    )}:generateContent`,
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
                                systemInstruction: {
                                    parts: [
                                        {
                                            text:
                                                GEMINI_REVIEW_PROMPT
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
                                                    `Revise ${entries.length} groups:\n` +
                                                    `${JSON.stringify({
                                                        groups:
                                                            entries.map(
                                                                entry =>
                                                                    compactReviewGroup(
                                                                        entry.group,
                                                                        entry.segments
                                                                    )
                                                            )
                                                    })}`
                                            }
                                        ]
                                    }
                                ],

                                generationConfig: {
                                    temperature:
                                        0,

                                    responseMimeType:
                                        "application/json",

                                    maxOutputTokens:
                                        GEMINI_MAX_OUTPUT_TOKENS
                                }
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
            }
            catch {}

            if (
                response.ok &&
                data
            ) {
                job.stats.geminiCalls++;

                const text =
                    extractGeminiText(
                        data
                    );

                if (!text) {
                    throw new Error(
                        "Gemini reviewer retornou resposta vazia."
                    );
                }

                return text;
            }

            const message =
                data
                    ?.error
                    ?.message ||
                data
                    ?.message ||
                raw ||
                `Gemini HTTP ${response.status}`;

            const error =
                new Error(
                    `GEMINI_REVIEW HTTP ${response.status}: ` +
                    `${String(
                        message
                    ).slice(
                        0,
                        1400
                    )}`
                );

            error.status =
                response.status;

            if (
                response.status ===
                429
            ) {
                job.stats.gemini429++;

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
                    attempt ===
                    GEMINI_MAX_RETRIES
                ) {
                    throw error;
                }

                console.warn(
                    `[GEMINI_REVIEW] HTTP 429; ` +
                    `aguardando ${Math.ceil(
                        wait /
                        1000
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
                    GEMINI_MAX_RETRIES
            ) {
                throw error;
            }

            await sleep(
                Math.min(
                    2500 *
                        attempt,
                    20000
                )
            );
        }
        catch (error) {
            lastError =
                error?.name ===
                    "AbortError"
                    ? new Error(
                        "GEMINI_REVIEW: timeout por request."
                    )
                    : error;

            if (
                attempt ===
                GEMINI_MAX_RETRIES
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
                `[GEMINI_REVIEW] ` +
                `${errorMessage(
                    lastError
                ).slice(
                    0,
                    220
                )}; ` +
                `retry em ${(wait / 1000).toFixed(
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
            "Gemini reviewer falhou."
        )
    );
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
                `Gemini não confirmou revisão integral: ` +
                `${parsed?.reviewed}/${entries.length}.`
            );

        error.code =
            "BAD_GEMINI_OUTPUT";

        throw error;
    }

    const allowed =
        new Map(
            entries.map(
                entry => [
                    entry.group.groupId,
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
            allowed.get(
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
                value =>
                    typeof value !==
                        "string" ||
                    !value.trim()
            )
        ) {
            continue;
        }

        seen.add(
            groupId
        );

        const clean =
            segments.map(
                value =>
                    value.trim()
            );

        if (
            JSON.stringify(
                clean
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
                clean,

            why:
                String(
                    proposal?.why ||
                    "correção"
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

async function geminiReviewResilient(
    entries,
    job,
    depth = 0
) {
    try {
        const raw =
            await geminiRequest(
                entries,
                job
            );

        const proposals =
            parseGeminiReview(
                entries,
                raw
            );

        job.stats.geminiReviewed +=
            entries.length;

        job.stats.geminiProposals +=
            proposals.length;

        return proposals;
    }
    catch (error) {
        if (
            entries.length >
                1 &&
            depth < 7
        ) {
            const middle =
                Math.ceil(
                    entries.length /
                    2
                );

            console.warn(
                `[GEMINI REVIEW] Split ` +
                `${entries.length} -> ` +
                `${middle}+${entries.length - middle} por ` +
                `${errorMessage(
                    error
                ).slice(
                    0,
                    120
                )}.`
            );

            const left =
                await geminiReviewResilient(
                    entries.slice(
                        0,
                        middle
                    ),
                    job,
                    depth + 1
                );

            const right =
                await geminiReviewResilient(
                    entries.slice(
                        middle
                    ),
                    job,
                    depth + 1
                );

            return [
                ...left,
                ...right
            ];
        }

        throw error;
    }
}

// ============================================================
// MISTRAL ARBITER
// ============================================================

function proposalPayload(proposal) {
    const speakers =
        proposal
            .group
            .cues
            .map(
                cue =>
                    cue.speakerHint ||
                    null
            );

    return {
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
            proposal.confidence,

        ...(
            speakers.some(
                Boolean
            )
                ? {
                    speaker:
                        speakers
                }
                : {}
        )
    };
}

function parseArbiter(
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
        return [];
    }

    if (
        !Array.isArray(
            parsed?.accepted
        )
    ) {
        return [];
    }

    const allowed =
        new Map(
            batch.map(
                proposal => [
                    proposal.g,
                    proposal
                ]
            )
        );

    const seen =
        new Set();

    const accepted =
        [];

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
            allowed.get(
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
                value =>
                    typeof value !==
                        "string" ||
                    !value.trim()
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
                    value =>
                        value.trim()
                )
        });
    }

    return accepted;
}

async function arbitrateAll(
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

        return;
    }

    const batches =
        splitByBudget(
            proposals,
            ARBITER_BATCH_CHARS,
            ARBITER_BATCH_GROUPS,
            proposalPayload
        );

    console.log(
        `[MISTRAL ARBITER] ` +
        `${proposals.length} proposta(s) -> ` +
        `${batches.length} lote(s).`
    );

    for (
        let index = 0;
        index <
        batches.length;
        index++
    ) {
        const batch =
            batches[index];

        const raw =
            await mistralChat({
                system:
                    ARBITER_PROMPT,

                user:
                    JSON.stringify({
                        proposals:
                            batch.map(
                                proposalPayload
                            )
                    }),

                job,

                temperature:
                    0,

                maxTokens:
                    5000,

                purpose:
                    "arbiter"
            });

        const accepted =
            parseArbiter(
                batch,
                raw
            );

        for (
            const item
            of accepted
        ) {
            translations.set(
                item.g,
                item.s
            );
        }

        job.stats.arbiterAccepted +=
            accepted.length;

        console.log(
            `[MISTRAL ARBITER] ` +
            `${index + 1}/${batches.length}: ` +
            `${accepted.length} aceita(s).`
        );
    }
}

// ============================================================
// FINAL FORMATTING
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

    /*
     * Corrige:
     * "Artistas talentosas. / Meninas."
     *
     * -> duas linhas limpas.
     */
    value =
        value.replace(
            /\s+\/\s+(?=\S)/g,
            "\n"
        );

    value =
        value
            .split("\n")
            .map(
                line => {
                    let clean =
                        String(line || "");

                    clean =
                        clean.replace(
                            /^\s*\[[^\]]{1,60}\]\s*:?[ \t]*/u,
                            ""
                        );

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

                    /*
                     * "A conta não--"
                     * ->
                     * "A conta não…"
                     */
                    clean =
                        clean.replace(
                            /\s*--+\s*$/u,
                            "…"
                        );

                    clean =
                        clean.replace(
                            /\s*--+\s*/gu,
                            "… "
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
            .filter(Boolean)
            .join("\n")
            .trim();

    return value;
}

function applyProtectedRules(
    source,
    target
) {
    let text =
        String(target || "");

    const en =
        String(source || "");

    if (
        /\bWerkroom\b/i.test(en)
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
        /\bCondragulations\b/i.test(en)
    ) {
        text =
            text.replace(
                /\bcondragulations\b/gi,
                "Condragulations"
            );
    }

    if (
        /Shantay,? you stay/i.test(en)
    ) {
        text =
            text.replace(
                /shantay,?\s+you\s+stay/gi,
                "Shantay, you stay"
            );
    }

    if (
        /Sashay away/i.test(en)
    ) {
        text =
            text.replace(
                /sashay\s+away/gi,
                "Sashay away"
            );
    }

    if (
        /You betta werk/i.test(en)
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

function applyHardFixes(
    source,
    target
) {
    let text =
        String(target || "");

    const before =
        text;

    text =
        text
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
            )
            .replace(
                /\bcheque da porra\b/gi,
                "cheque, porra"
            )
            .replace(
                /\bcheque do caralho\b/gi,
                "cheque, caralho"
            )
            .replace(
                /\bapoiante\b/gi,
                "apoiador"
            );

    if (
        /\bgoddamn check\b/i.test(
            String(
                source ||
                ""
            )
        )
    ) {
        text =
            text.replace(
                /\bcheque,\s*caralho\b/gi,
                "cheque, porra"
            );
    }

    return {
        text,

        changed:
            text !==
            before
    };
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
                    segments[
                        index
                    ];
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

function cleanAllFinal(
    blocks,
    texts,
    job
) {
    return texts.map(
        (
            text,
            index
        ) => {
            let output =
                cleanFinalText(
                    text
                );

            output =
                applyProtectedRules(
                    blocks[index].text,
                    output
                );

            const fixed =
                applyHardFixes(
                    blocks[index].text,
                    output
                );

            if (
                fixed.changed
            ) {
                job.stats.hardFixes++;
            }

            return fixed.text;
        }
    );
}

// ============================================================
// QUALITY + COMPLETENESS
// ============================================================

function wordCount(text) {
    return (
        String(text || "")
            .match(
                /[\p{L}\p{N}]+(?:['’][\p{L}\p{N}]+)?/gu
            ) ||
        []
    ).length;
}

function coverageRiskReasons(
    group,
    segments
) {
    const en =
        group.cues
            .map(
                cue =>
                    cue.text
            )
            .join(" ");

    const pt =
        segments.join(" ");

    const enWords =
        wordCount(en);

    const ptWords =
        wordCount(pt);

    const reasons =
        [];

    /*
     * Só dispara em redução extrema.
     * É um detector de provável omissão,
     * não um medidor de estilo.
     */
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
        reasons.push(
            "POSSIBLE_OMISSION"
        );
    }

    if (
        en.length >= 80 &&
        pt.length <=
            Math.floor(
                en.length *
                0.27
            )
    ) {
        reasons.push(
            "POSSIBLE_OMISSION"
        );
    }

    return [
        ...new Set(
            reasons
        )
    ];
}

function finalRiskScan(
    blocks,
    groups,
    finalTexts,
    job
) {
    const issues =
        [];

    const position =
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
        const segments =
            group.cues.map(
                cue =>
                    finalTexts[
                        position.get(
                            cue.index
                        )
                    ]
            );

        const groupReasons =
            coverageRiskReasons(
                group,
                segments
            );

        if (
            groupReasons.includes(
                "POSSIBLE_OMISSION"
            )
        ) {
            job.stats.omissionRisks++;
        }

        for (
            let index = 0;
            index <
            group.cues.length;
            index++
        ) {
            const cue =
                group.cues[index];

            const pt =
                String(
                    segments[index] ||
                    ""
                );

            const en =
                String(
                    cue.text ||
                    ""
                );

            const reasons = [
                ...groupReasons
            ];

            if (
                /^\s*\[[^\]]{1,60}\]\s*:/mu.test(
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
                /\s\/\s/u.test(
                    pt
                )
            ) {
                reasons.push(
                    "SLASH_SEPARATOR"
                );
            }

            if (
                /--+/u.test(
                    pt
                )
            ) {
                reasons.push(
                    "RAW_DOUBLE_HYPHEN"
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
                /\bcabina de votação\b/i.test(
                    pt
                )
            ) {
                reasons.push(
                    "CABINA_VOTACAO"
                );
            }

            if (
                /\bcheque (?:da porra|do caralho)\b/i.test(
                    pt
                )
            ) {
                reasons.push(
                    "CHEQUE_PROFANITY_PLACEMENT"
                );
            }

            if (
                /\bapoiante\b/i.test(
                    pt
                )
            ) {
                reasons.push(
                    "APOIANTE"
                );
            }

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

            const unique = [
                ...new Set(
                    reasons
                )
            ];

            if (
                unique.length
            ) {
                issues.push({
                    groupId:
                        group.groupId,

                    cueId:
                        cue.index,

                    reasons:
                        unique,

                    source:
                        en,

                    text:
                        pt
                });
            }
        }
    }

    return issues;
}

async function repairRisks(
    groups,
    translations,
    issues,
    job
) {
    if (
        !issues.length
    ) {
        return;
    }

    const reasonMap =
        new Map();

    for (
        const issue
        of issues
    ) {
        const set =
            reasonMap.get(
                issue.groupId
            ) ||
            new Set();

        issue.reasons.forEach(
            reason =>
                set.add(
                    reason
                )
        );

        reasonMap.set(
            issue.groupId,
            set
        );
    }

    const riskyGroups =
        groups.filter(
            group =>
                reasonMap.has(
                    group.groupId
                )
        );

    console.warn(
        `[QUALITY REPAIR] ` +
        `${issues.length} alerta(s) em ` +
        `${riskyGroups.length} group(s).`
    );

    const batches =
        splitByBudget(
            riskyGroups,
            5000,
            12,
            group => ({
                ...compactReviewGroup(
                    group,
                    translations.get(
                        group.groupId
                    )
                ),

                reasons:
                    [
                        ...reasonMap.get(
                            group.groupId
                        )
                    ]
            })
        );

    for (
        const batch
        of batches
    ) {
        job.stats.qualityRepairs++;

        const raw =
            await mistralChat({
                system:
                    REPAIR_PROMPT,

                user:
                    JSON.stringify({
                        groups:
                            batch.map(
                                group => ({
                                    ...compactReviewGroup(
                                        group,
                                        translations.get(
                                            group.groupId
                                        )
                                    ),

                                    reasons:
                                        [
                                            ...reasonMap.get(
                                                group.groupId
                                            )
                                        ]
                                })
                            )
                    }),

                job,

                temperature:
                    0,

                maxTokens:
                    5000,

                purpose:
                    "repair"
            });

        const parsed =
            parseTranslationResponse(
                batch,
                raw,
                job
            );

        for (
            const [
                groupId,
                segments
            ]
            of parsed.valid
        ) {
            translations.set(
                groupId,
                segments
            );
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
        }
    }
}

// ============================================================
// MAIN PIPELINE
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

    const groups =
        buildSentenceGroups(
            blocks
        );

    const batches =
        splitByBudget(
            groups,
            MISTRAL_BATCH_CHARS,
            MISTRAL_BATCH_GROUPS,
            compactGroup
        );

    const translations =
        new Map();

    const proposals =
        [];

    const reviewScheduled =
        new Set();

    let reviewChain =
        Promise.resolve();

    let nextBatch =
        0;

    let completedBatches =
        0;

    const startedAt =
        Date.now();

    console.log(
        `[PIPELINE 6.4] fonte=${job.sourceKind} | ` +
        `${blocks.length} cues -> ` +
        `${groups.length} Sentence Groups -> ` +
        `${batches.length} lote(s).`
    );

    function scheduleReview(
        batch
    ) {
        const key =
            batch
                .map(
                    group =>
                        group.groupId
                )
                .join(",");

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

        reviewChain =
            reviewChain.then(
                async () => {
                    console.log(
                        `[GEMINI REVIEW] ` +
                        `Revisando ${entries.length} group(s) ` +
                        `enquanto Mistral continua.`
                    );

                    const batchProposals =
                        await geminiReviewResilient(
                            entries,
                            job
                        );

                    proposals.push(
                        ...batchProposals
                    );

                    console.log(
                        `[GEMINI REVIEW] ` +
                        `confirmado=${entries.length}; ` +
                        `propostas=${batchProposals.length}; ` +
                        `total=${job.stats.geminiReviewed}/${groups.length}.`
                    );
                }
            );
    }

    async function worker(
        workerId
    ) {
        while (true) {
            const batchIndex =
                nextBatch++;

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
                `[MISTRAL W${workerId}] ` +
                `Lote ${batchIndex + 1}/${batches.length}: ` +
                `${batch.length} group(s).`
            );

            const result =
                await translateBatchResilient(
                    batch,
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

            job.translationCheckpoint =
                new Map(
                    translations
                );

            completedBatches++;

            job.progress =
                Math.max(
                    job.progress,
                    Math.round(
                        (
                            completedBatches /
                            batches.length
                        ) *
                        72
                    )
                );

            job.updatedAt =
                Date.now();

            console.log(
                `[MISTRAL W${workerId}] ` +
                `Lote ${batchIndex + 1}/${batches.length} aprovado; ` +
                `total=${translations.size}/${groups.length}.`
            );

            scheduleReview(
                batch
            );
        }
    }

    await Promise.all(
        Array.from(
            {
                length:
                    MISTRAL_CONCURRENCY
            },

            (
                _,
                index
            ) =>
                worker(
                    index + 1
                )
        )
    );

    if (
        translations.size !==
        groups.length
    ) {
        throw new Error(
            `Tradução incompleta: ` +
            `${translations.size}/${groups.length}.`
        );
    }

    let primaryTexts =
        cleanAllFinal(
            blocks,

            flattenTranslations(
                blocks,
                groups,
                translations
            ),

            job
        );

    const primarySrt =
        buildSrt(
            blocks,
            primaryTexts
        );

    auditTimestamps(
        sourceSrt,
        primarySrt,
        "CHECKPOINT MISTRAL"
    );

    job.primaryCheckpoint =
        primarySrt;

    console.log(
        `[CHECKPOINT] Mistral completo preservado: ` +
        `${groups.length}/${groups.length}.`
    );

    await reviewChain;

    if (
        job.stats.geminiReviewed !==
        groups.length
    ) {
        throw new Error(
            `Gemini revisou ` +
            `${job.stats.geminiReviewed}/${groups.length}.`
        );
    }

    console.log(
        `[GEMINI REVIEW] COMPLETA: ` +
        `${job.stats.geminiReviewed}/${groups.length}; ` +
        `propostas=${proposals.length}.`
    );

    await arbitrateAll(
        proposals,
        translations,
        job
    );

    let finalTexts =
        cleanAllFinal(
            blocks,

            flattenTranslations(
                blocks,
                groups,
                translations
            ),

            job
        );

    let risks =
        finalRiskScan(
            blocks,
            groups,
            finalTexts,
            job
        );

    if (
        risks.length
    ) {
        await repairRisks(
            groups,
            translations,
            risks,
            job
        );

        finalTexts =
            cleanAllFinal(
                blocks,

                flattenTranslations(
                    blocks,
                    groups,
                    translations
                ),

                job
            );

        risks =
            finalRiskScan(
                blocks,
                groups,
                finalTexts,
                job
            );
    }

    finalTexts =
        cleanAllFinal(
            blocks,
            finalTexts,
            job
        );

    risks =
        finalRiskScan(
            blocks,
            groups,
            finalTexts,
            job
        );

    if (
        risks.length
    ) {
        throw new Error(
            `Quality Guard ainda encontrou ` +
            `${risks.length} risco(s): ` +
            `${risks
                .slice(
                    0,
                    12
                )
                .map(
                    issue =>
                        `g${issue.groupId}/` +
                        `cue${issue.cueId}:` +
                        `${issue.reasons.join("+")}`
                )
                .join(
                    ", "
                )}`
        );
    }

    console.log(
        "[QUALITY GUARD] PASSOU — 0 risco conhecido restante."
    );

    const finalSrt =
        buildSrt(
            blocks,
            finalTexts
        );

    auditTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL 6.4"
    );

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
        `[PIPELINE 6.4] OK em ${elapsed.toFixed(1)}s | ` +
        `fonte=${job.sourceKind} | ` +
        `MistralCalls=${job.stats.mistralCalls} | ` +
        `Attempts=${job.stats.mistralAttempts} | ` +
        `429=${job.stats.mistral429} | ` +
        `PredictiveWaits=${job.stats.predictiveWaits} | ` +
        `PredictiveWait=${(
            job.stats.predictiveWaitMs /
            1000
        ).toFixed(1)}s | ` +
        `SingleCueJoin=${job.stats.deterministicSingleCueJoins} | ` +
        `Gemini=${job.stats.geminiReviewed}/${groups.length} | ` +
        `Propostas=${job.stats.geminiProposals} | ` +
        `ArbiterAccepted=${job.stats.arbiterAccepted} | ` +
        `OmissionRisks=${job.stats.omissionRisks} | ` +
        `HardFixes=${job.stats.hardFixes}.`
    );

    return finalSrt;
}

// ============================================================
// JOB QUEUE
// ============================================================

async function processJob(job) {
    job.processingStartedAt =
        Date.now();

    job.status =
        "processing";

    job.error =
        null;

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
    }
    catch (error) {
        job.status =
            "failed";

        job.error =
            errorMessage(
                error
            );

        console.error(
            `[JOB ${job.id}] Falhou: ${job.error}`
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

function buildOpenSubtitlesUrl(
    type,
    id,
    extra
) {
    const base =
        `https://opensubtitles-v3.strem.io/subtitles/` +
        `${encodeURIComponent(
            type
        )}/` +
        `${encodeURIComponent(
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

    return params.toString()
        ? `${base}/${params.toString()}.json`
        : `${base}.json`;
}

function scoreOpenSubtitle(subtitle) {
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
        subtitle?.hearingImpaired ===
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

    return score;
}

function selectEnglishSubtitle(subtitles) {
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
            ) =>
                scoreOpenSubtitle(
                    b
                ) -
                scoreOpenSubtitle(
                    a
                )
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
                        "Stremio-PTBR-Backend/6.4"
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

    if (!target) {
        throw new Error(
            "OpenSubtitles não encontrou legenda inglesa utilizável."
        );
    }

    const subtitleResponse =
        await fetchWithTimeout(
            target.url,
            {
                headers: {
                    "User-Agent":
                        "Stremio-PTBR-Backend/6.4"
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
            "Legenda OpenSubtitles vazia ou grande demais."
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
            `${baseUrl(
                req
            )}/subtitle/${encodeURIComponent(
                job.id
            )}.srt`
    };
}

// ============================================================
// ROUTES
// ============================================================

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "6.4.0",

    name:
        "Tradutor PT-BR Backend",

    description:
        "Backend da Ponte Local: embedded-first, Mistral Medium 3.5 + Gemini review + Mistral arbiter.",

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

            publicStremioSubtitles:
                false,

            translator:
                MISTRAL_MODEL,

            reviewer:
                GEMINI_MODEL,

            cacheVersion:
                CACHE_VERSION,

            sourcePolicy:
                "PONTE_LOCAL_DECIDES_EMBEDDED_FIRST",

            mistralConcurrency:
                MISTRAL_CONCURRENCY,

            mistralPacer:
                mistralLimiter.status(),

            queue:
                queue.length,

            processing:
                queueRunning
        })
);

/*
 * O Render não oferece mais
 * uma segunda legenda concorrente
 * no menu do Stremio.
 *
 * Só a Ponte Local oferece PT-BR.
 */
async function backendOnlySubtitles(
    req,
    res
) {
    console.log(
        "[STREMIO PUBLIC] Backend-only: " +
        "0 legendas; use Ponte Local."
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
// API EMBEDDED
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
        catch (error) {
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
// API FALLBACK
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

            console.log(
                `[FALLBACK API] ` +
                `${type}/${videoId} | ` +
                `${filename || "(sem filename)"}.`
            );

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
                `OpenSubtitles escolhido: ` +
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
        catch (error) {
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

                sourceHash:
                    job.sourceHash,

                checkpointGroups:
                    job
                        .translationCheckpoint
                        ?.size ||
                    0,

                primaryCheckpointReady:
                    Boolean(
                        job.primaryCheckpoint
                    ),

                stats:
                    job.stats,

                mistralPacer:
                    mistralLimiter.status()
            }
        );
    }
);

// ============================================================
// SRT RESULT
// ============================================================

function processingSrt(job) {
    return [
        "1",

        "00:00:01,000 --> 00:00:08,000",

        "Traduzindo e revisando legenda...",

        "",

        "2",

        "00:00:08,500 --> 00:00:15,000",

        `Progresso: ${Number(
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
            }
            catch (error) {
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
            " STREMIO PT-BR BACKEND 6.4 FINAL"
        );

        console.log(
            "============================================================"
        );

        console.log(
            `Mistral: ${
                MISTRAL_API_KEY
                    ? "CONFIGURADO ✅"
                    : "FALTANDO ❌"
            } (${MISTRAL_MODEL})`
        );

        console.log(
            `Gemini: ${
                GEMINI_API_KEY
                    ? "CONFIGURADO ✅"
                    : "FALTANDO ❌"
            } (${GEMINI_MODEL})`
        );

        console.log(
            `Ponte Local: ${
                LOCAL_BRIDGE_SECRET
                    ? "CONFIGURADA ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            "Render no menu do Stremio: " +
            "DESATIVADO — BACKEND ONLY ✅"
        );

        console.log(
            "Fonte embedded: decidida pela Ponte " +
            "ANTES de chamar Render ✅"
        );

        console.log(
            "OpenSubtitles: SOMENTE fallback " +
            "solicitado pela Ponte ✅"
        );

        console.log(
            "Tradução duplicada OpenSubtitles + embedded: " +
            "ELIMINADA ✅"
        );

        console.log(
            `Mistral concorrência máxima: ` +
            `${MISTRAL_CONCURRENCY} ✅`
        );

        console.log(
            `Mistral pacing preditivo: ` +
            `${MISTRAL_TARGET_TPM} TPM alvo ✅`
        );

        console.log(
            "429: prevenção preditiva + retry residual ✅"
        );

        console.log(
            "Oversplit de 1 cue: " +
            "corrigido localmente sem nova IA ✅"
        );

        console.log(
            "Gemini revisa 100% + " +
            "checa omissões/sincronia semântica ✅"
        );

        console.log(
            "Mistral arbitra propostas Gemini ✅"
        );

        console.log(
            'Vocalização isolada repetida tipo "ah ah": ' +
            "FILTRADA ✅"
        );

        console.log(
            'Separador " / ": PROIBIDO no SRT final ✅'
        );

        console.log(
            'Double hyphen "--" cru: NORMALIZADO ✅'
        );

        console.log(
            "Quality Guard + detector de possível omissão: " +
            "ATIVOS ✅"
        );

        console.log(
            "Teto global do episódio: NÃO EXISTE ✅"
        );

        console.log(
            "Timestamps da fonte escolhida: IMUTÁVEIS ✅"
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
