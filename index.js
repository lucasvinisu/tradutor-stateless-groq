const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();

app.use(cors());
app.disable("x-powered-by");
app.use(
    express.json({
        limit: "2mb"
    })
);

// ============================================================
// STREMIO PT-BR BACKEND 6.5.1 — HOTFIX REAL
// ============================================================
//
// Ponte Local 4.1 = NÃO MUDA.
//
// Objetivos:
//
// - embedded continua sendo autoridade temporal;
// - Mistral principal continua serial;
// - sem pacer arbitrário;
// - sem teto global de episódio;
// - quota usa headers REAIS da Mistral;
// - top_p=1 explícito corrige greedy sampling;
// - lote truncado/JSON inválido é DIVIDIDO;
// - nunca transformar 200 groups em dezenas de rescues;
// - Gemini micro-audita EN × PT em paralelo;
// - somente groups problemáticos voltam ao Mistral;
// - Style Pack 2026 preservado;
// - cue IDs e timestamps são imutáveis.
//
// ============================================================

const PORT =
    Number(
        process.env.PORT ||
        10000
    );

const PUBLIC_URL =
    String(
        process.env.PUBLIC_URL ||
        ""
    ).replace(
        /\/+$/,
        ""
    );

const LOCAL_BRIDGE_SECRET =
    String(
        process.env.LOCAL_BRIDGE_SECRET ||
        ""
    ).trim();

const MISTRAL_API_KEY =
    String(
        process.env.MISTRAL_API_KEY ||
        ""
    ).trim();

const MISTRAL_MODEL =
    String(
        process.env.MISTRAL_MODEL ||
        "mistral-medium-3-5"
    ).trim();

const GEMINI_API_KEY =
    String(
        process.env.GEMINI_API_KEY ||
        ""
    ).trim();

const GEMINI_MODEL =
    String(
        process.env.GEMINI_MODEL ||
        "gemini-3.5-flash-lite"
    ).trim();

const CACHE_VERSION =
    "6.5.1-final";

const MAX_SOURCE_CHARS =
    800000;

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

const FETCH_TIMEOUT_MS =
    25000;

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
        7
    );

const GEMINI_MAX_RETRIES =
    Number(
        process.env.GEMINI_MAX_RETRIES ||
        6
    );

// ============================================================
// LOTE PRINCIPAL
//
// Menor do que 6.5.
// Mais espaço de saída por group.
//
// O objetivo é ficar abaixo do custo que monopoliza
// a janela de 25K TPM, mas sem truncar o JSON.
// ============================================================

const MAIN_BATCH_CHARS =
    Number(
        process.env.MISTRAL_BATCH_CHARS ||
        12500
    );

const MAIN_BATCH_GROUPS =
    Number(
        process.env.MISTRAL_BATCH_GROUPS ||
        145
    );

const MAIN_MAX_TOKENS =
    Number(
        process.env.MISTRAL_MAIN_MAX_TOKENS ||
        6000
    );

// ============================================================
// RESCUE
// ============================================================

const RESCUE_BATCH_CHARS =
    5200;

const RESCUE_BATCH_GROUPS =
    10;

const RESCUE_MAX_TOKENS =
    3600;

// ============================================================
// REPAIR
// ============================================================

const REPAIR_BATCH_CHARS =
    7500;

const REPAIR_BATCH_GROUPS =
    24;

const REPAIR_MAX_TOKENS =
    4800;

// ============================================================
// GEMINI MICRO-AUDIT
// ============================================================

const GEMINI_AUDIT_GROUPS =
    Number(
        process.env.GEMINI_AUDIT_GROUPS ||
        28
    );

const GEMINI_AUDIT_CHARS =
    Number(
        process.env.GEMINI_AUDIT_CHARS ||
        9000
    );

// ============================================================
// STATE
// ============================================================

const translationCache =
    new Map();

const jobs =
    new Map();

const queue =
    [];

let queueRunning =
    false;

let mistralLane =
    Promise.resolve();

const sleep =
    ms =>
        new Promise(
            resolve =>
                setTimeout(
                    resolve,
                    ms
                )
        );

// ============================================================
// BASIC HELPERS
// ============================================================

function sha256(value) {
    return crypto
        .createHash(
            "sha256"
        )
        .update(
            String(value),
            "utf8"
        )
        .digest(
            "hex"
        );
}

function randomId(
    bytes = 6
) {
    return crypto
        .randomBytes(
            bytes
        )
        .toString(
            "hex"
        );
}

function errorMessage(
    error
) {
    return String(
        error?.message ||
        error ||
        "Erro desconhecido."
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
            /^\s*```(?:json|text|plaintext|srt)?\s*/i,
            ""
        )
        .replace(
            /\s*```\s*$/i,
            ""
        )
        .trim();
}

function baseUrl(
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
            .split(",")[0]
            .trim();

    const host =
        String(
            req.headers[
                "x-forwarded-host"
            ] ||
            req.headers.host ||
            ""
        )
            .split(",")[0]
            .trim();

    return (
        `${proto}://${host}`
            .replace(
                /\/+$/,
                ""
            )
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

function sendSrt(
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

function authorized(
    req
) {
    if (
        !LOCAL_BRIDGE_SECRET
    ) {
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
// CACHE / JOB
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
        `${sha256(
            sourceSrt
        )}`
    );
}

function getCache(
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
        sha256(
            sourceSrt
        );

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

        expiresAt:
            now +
            JOB_TTL_MS,

        stats: {
            mistralCalls:
                0,

            mistralAttempts:
                0,

            mistral429:
                0,

            quotaWaitMs:
                0,

            mainSplits:
                0,

            jsonInvalidSplits:
                0,

            outputTruncations:
                0,

            salvageGroups:
                0,

            rescueCalls:
                0,

            singleCueJoins:
                0,

            geminiCalls:
                0,

            geminiAttempts:
                0,

            gemini429:
                0,

            geminiReviewed:
                0,

            geminiFlagged:
                0,

            repairCalls:
                0,

            repairedGroups:
                0,

            secondPassGroups:
                0,

            localStyleFlags:
                0,

            omissionFlags:
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
            job.cacheKey ===
                key &&
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
        cacheKey(
            args.type,
            args.videoId,
            args.sourceSrt
        );

    const cached =
        getCache(
            key
        );

    if (
        cached
    ) {
        let job =
            findJobByCache(
                key,
                [
                    "completed"
                ]
            );

        if (
            !job
        ) {
            job =
                createJob(
                    args
                );

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

    if (
        active
    ) {
        return active;
    }

    const done =
        findJobByCache(
            key,
            [
                "completed"
            ]
        );

    if (
        done
    ) {
        return done;
    }

    const job =
        createJob(
            args
        );

    enqueue(
        job
    );

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

    if (
        hidden
    ) {
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

    if (
        bracket
    ) {
        const speaker =
            normalizeSpeaker(
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
            normalizeSpeaker(
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
            !TIMING_RE.test(
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
            null;

        if (
            textLines.length
        ) {
            const match =
                textLines[0]
                    .match(
                        SPEAKER_RE
                    );

            if (
                match
            ) {
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
        let index = 0;
        index <
        source.length;
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

// ============================================================
// STYLE PACK 2026
// ============================================================

const TRANSLATOR_PROMPT = `
Você traduz legendas EN→PT-BR em 2026.

Entregue português brasileiro:
natural,
oral,
atual,
conciso,
fiel,
contextual
e culturalmente inteligente.

Nunca soe:
literal,
engessado,
antiquado,
lusitano,
burocrático
ou com gíria de tiozão.

Não force internetês ou Gen Z.
Use linguagem jovem apenas quando personagem,
tom e contexto realmente pedirem.

REALITY / DRAG / LGBTQIA+ / POP:

- "bitch" como VOCATIVO amigável:
  bicha,
  gata,
  amiga,
  menina
  ou omitir.
  NÃO use "puta" automaticamente.
  Como insulto real,
  adapte conforme contexto.

- "I'm gagged" / "gagged" em reação:
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

- fucking / motherfucking =
  INTENSIFICADOR.
  Preserve força em posição natural brasileira.

  Nunca:
  "competição da porra",
  "competição do caralho",
  "lip sync da porra",
  "lip sync do caralho",
  "cheque da porra",
  "cheque do caralho".

- supportive:
  prefira:
  "sempre me apoiou",
  "sempre esteve do meu lado".

  Evite:
  "super apoiador".

- judges em Drag Race =
  "jurados",
  não "juízes".

- "the judgers are now the judgees":
  "agora quem julgava vai ser julgado"
  ou
  "agora os jurados é que vão ser julgados".

  Nunca:
  "os juízes viraram os julgados".

- "plucking pussy hairs":
  preserve corpo + vulgaridade.

  Pode ser:
  "catar pelo de xereca",
  "arrancar pelo de xereca",
  conforme contexto.

  Nunca:
  "fio de bigode".

- double win / shared win:
  "vitória dupla",
  "as duas ganharam"
  ou equivalente.

  Não use "empate duplo"
  quando não há empate.

- "off the top" sobre dinheiro:
  comissão,
  corte,
  porcentagem.

- "closing ranks":
  grupo se protegendo,
  panelinha.

- "Carry the two":
  "vai dois".

- "week one":
  "primeira semana".

- evite "apoiante".

Preserve quando presentes:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

PALAVRÃO:

Não censure.

Preserve intensidade,
mas posicione como um brasileiro realmente fala.

GÊNERO:

speaker é contexto oculto.

Use apenas quando for seguro.

Se não for seguro,
reformule naturalmente.

Nunca use:

empolgado(a)
animado(a)
ele/ela
ela/ele

FORMATAÇÃO:

Sem speaker labels.
Sem [NOME].
Sem NOME:.
Sem barra "/" decorativa.
Sem hífen/travessão decorativo.
Sem SDH/CC.
Sem alongamento gráfico.

CUE LOCK ABSOLUTO:

Cada cue possui um id "i".

Devolva exatamente UM "pt"
para CADA "i" recebido,
com o MESMO "i".

Não resuma.

Não omita o fim do raciocínio.

Não antecipe conteúdo do cue seguinte.

Não atrase conteúdo para outro cue.

Use Sentence Group apenas para entender contexto.

Timestamps nunca são alterados.

Responda SOMENTE JSON válido:

{
  "groups": [
    {
      "g": 1,
      "c": [
        {
          "i": 10,
          "pt": "..."
        }
      ]
    }
  ]
}
`;

const RESCUE_PROMPT = `
Corrija a estrutura de uma tradução EN→PT-BR.

Mantenha qualidade editorial alta:

natural,
contemporânea,
completa,
fiel
e não literal.

Para cada group,
devolva exatamente um "pt"
para cada cue "i" recebido.

Preserve o mesmo "i".

Não omita conteúdo.

Não mova conteúdo entre cues.

speaker é contexto oculto.

Sem labels.
Sem barras.
Sem marcadores.
Sem alongamentos.

Responda SOMENTE JSON válido:

{
  "groups": [
    {
      "g": 1,
      "c": [
        {
          "i": 10,
          "pt": "..."
        }
      ]
    }
  ]
}
`;

const GEMINI_AUDIT_PROMPT = `
Você é auditor editorial independente
EN→PT-BR em 2026.

Compare EN x PT
CUE POR CUE.

Para CADA group,
devolva obrigatoriamente:

v="ok"

ou

v="fix".

Não reescreva por gosto.

Marque "fix" somente por problema real.

Motivos possíveis:

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

- bitch como vocativo amigável
  não vira "puta" automaticamente.

- motherfucking/fucking
  não vira:
  "competição da porra",
  "lip sync da porra",
  "cheque da porra".

- supportive
  não vira
  "super apoiador".

- judges no Drag Race =
  jurados.

- judgers/judgees
  não vira:
  "juízes viraram os julgados".

- plucking pussy hairs
  não vira:
  "fio de bigode".

- double/shared win
  não vira:
  "empate duplo"
  quando não existe empate.

- gagged em reação
  deve soar naturalmente:
  "Tô passada",
  "Tô em choque",
  etc.

Cheque também:

- fim de frase/raciocínio perdido;
- conteúdo antecipado;
- conteúdo atrasado;
- conteúdo migrado para cue vizinho.

Para "fix",
inclua:

reasons

e

hint CURTO.

Responda SOMENTE JSON
e inclua EXATAMENTE
um item por group:

{
  "items": [
    {
      "g": 1,
      "v": "ok"
    },
    {
      "g": 2,
      "v": "fix",
      "reasons": [
        "LITERAL"
      ],
      "hint": "..."
    }
  ]
}
`;

const REPAIR_PROMPT = `
Você é editor final EN→PT-BR.

Recebe SOMENTE groups
com problema concreto.

Use:

EN original
PT atual
reasons
hint

Retraduza apenas o necessário.

Mantenha:

naturalidade
contemporaneidade
fidelidade
cultura
vulgaridade
sincronização semântica por cue

REGRAS OBRIGATÓRIAS:

- bitch vocativo amigável:
  bicha/gata/amiga/menina
  ou omitir conforme tom.
  Não "puta" automaticamente.

- fucking/motherfucking:
  intensificador natural brasileiro.

  Nunca:
  competição da porra/do caralho,
  lip sync da porra/do caralho,
  cheque da porra/do caralho.

- supportive:
  sempre me apoiou,
  esteve do meu lado,
  etc.

- judges no Drag Race:
  jurados.

- judgers/judgees:
  quem julgava vai ser julgado
  ou equivalente natural.

- plucking pussy hairs:
  preserve pelo de xereca
  ou equivalente vulgar.

- double/shared win:
  vitória dupla,
  as duas ganharam,
  ou equivalente.

  Não "empate duplo"
  quando não há empate.

- gagged:
  Tô passada,
  Tô muito passada,
  Tô em choque,
  etc.

Preserve catchphrases de Drag Race.

CUE LOCK:

Exatamente um "pt"
para cada cue "i".

Mesmo "i".

Sem omissão.

Sem mover conteúdo entre cues.

speaker é oculto.

Sem labels.
Sem barras.
Sem marcadores.
Sem alongamentos.

Responda SOMENTE JSON:

{
  "groups": [
    {
      "g": 1,
      "c": [
        {
          "i": 10,
          "pt": "..."
        }
      ]
    }
  ]
}
`;

// ============================================================
// MISTRAL RATE-AWARE LANE
//
// Usa:
// x-ratelimit-limit-tokens-minute
// x-ratelimit-remaining-tokens-minute
// x-ratelimit-tokens-query-cost
//
// Não inventa 23.500.
// Não possui teto global.
// ============================================================

const rateState = {
    limitTokens:
        25000,

    remainingTokens:
        null,

    events:
        [],

    lastCostByPurpose:
        new Map()
};

function withMistralLane(
    fn
) {
    const run =
        mistralLane.then(
            fn,
            fn
        );

    mistralLane =
        run.catch(
            () => {}
        );

    return run;
}

function numberHeader(
    response,
    name
) {
    const raw =
        response
            ?.headers
            ?.get(
                name
            );

    if (
        raw == null ||
        raw ===
            ""
    ) {
        return null;
    }

    const number =
        Number(
            raw
        );

    return Number.isFinite(
        number
    )
        ? number
        : null;
}

function pruneRateEvents() {
    const cutoff =
        Date.now() -
        60000;

    rateState.events =
        rateState.events.filter(
            event =>
                event.ts >
                cutoff
        );
}

function updateRateState(
    response,
    purpose,
    payloadChars,
    eventTs =
        Date.now()
) {
    const limit =
        numberHeader(
            response,
            "x-ratelimit-limit-tokens-minute"
        );

    const remaining =
        numberHeader(
            response,
            "x-ratelimit-remaining-tokens-minute"
        );

    const cost =
        numberHeader(
            response,
            "x-ratelimit-tokens-query-cost"
        );

    if (
        limit &&
        limit >
            0
    ) {
        rateState.limitTokens =
            limit;
    }

    if (
        remaining !=
        null
    ) {
        rateState.remainingTokens =
            remaining;
    }

    if (
        cost &&
        cost >
            0
    ) {
        rateState.events.push({
            ts:
                eventTs,

            cost
        });

        rateState
            .lastCostByPurpose
            .set(
                purpose,
                {
                    cost,

                    payloadChars
                }
            );
    }

    pruneRateEvents();

    const info =
        [];

    try {
        for (
            const [
                key,
                value
            ]
            of response.headers.entries()
        ) {
            const lower =
                key.toLowerCase();

            if (
                lower.startsWith(
                    "x-ratelimit"
                ) ||
                lower ===
                    "retry-after"
            ) {
                info.push(
                    `${key}=${value}`
                );
            }
        }
    }
    catch {}

    if (
        info.length
    ) {
        console.log(
            `[MISTRAL RATE] ${info.join(
                " | "
            )}`
        );
    }
}

function estimateQueryCost(
    purpose,
    payloadChars,
    maxTokens,
    systemChars
) {
    const last =
        rateState
            .lastCostByPurpose
            .get(
                purpose
            );

    if (
        last &&
        last.payloadChars >
            0
    ) {
        const ratio =
            Math.max(
                0.45,

                Math.min(
                    1.6,

                    payloadChars /
                    last.payloadChars
                )
            );

        return Math.ceil(
            last.cost *
            (
                0.58 +
                0.42 *
                ratio
            )
        );
    }

    return Math.ceil(
        maxTokens +
        (
            payloadChars +
            systemChars
        ) /
        3.4 +
        350
    );
}

async function waitForRealQuota(
    estimate,
    job
) {
    while (
        true
    ) {
        pruneRateEvents();

        const used =
            rateState
                .events
                .reduce(
                    (
                        sum,
                        event
                    ) =>
                        sum +
                        event.cost,

                    0
                );

        const limit =
            rateState.limitTokens ||
            25000;

        const syntheticRemaining =
            Math.max(
                0,
                limit -
                used
            );

        const serverRemaining =
            rateState.remainingTokens ==
            null
                ? syntheticRemaining
                : rateState.remainingTokens;

        const effectiveRemaining =
            rateState.remainingTokens ==
            null
                ? syntheticRemaining
                : Math.min(
                    serverRemaining,
                    syntheticRemaining
                );

        if (
            estimate <=
            Math.max(
                0,

                effectiveRemaining -
                250
            )
        ) {
            return;
        }

        if (
            !rateState.events.length
        ) {
            return;
        }

        let freed =
            0;

        let releaseAt =
            Date.now() +
            1000;

        for (
            const event
            of [
                ...rateState.events
            ].sort(
                (
                    a,
                    b
                ) =>
                    a.ts -
                    b.ts
            )
        ) {
            freed +=
                event.cost;

            releaseAt =
                event.ts +
                60000 +
                350;

            if (
                effectiveRemaining +
                freed >=
                estimate +
                250
            ) {
                break;
            }
        }

        const wait =
            Math.max(
                250,

                releaseAt -
                Date.now()
            );

        job.stats.quotaWaitMs +=
            wait;

        console.log(
            `[MISTRAL QUOTA] ` +
            `custo estimado~${estimate}; ` +
            `restante~${Math.round(
                effectiveRemaining
            )}; ` +
            `aguardando ${(wait / 1000).toFixed(
                1
            )}s pela própria janela de ` +
            `${limit} TPM.`
        );

        await sleep(
            wait
        );

        // O header anterior agora ficou velho.
        // Os eventos reais locais passam a determinar a janela.
        rateState.remainingTokens =
            null;
    }
}

function parseRetryAfter(
    response
) {
    const raw =
        response
            ?.headers
            ?.get(
                "retry-after"
            );

    if (
        raw
    ) {
        const number =
            Number(
                raw
            );

        if (
            Number.isFinite(
                number
            ) &&
            number >
                0
        ) {
            return Math.min(
                90000,

                Math.max(
                    1000,

                    number *
                    1000
                )
            );
        }

        const date =
            Date.parse(
                raw
            );

        if (
            Number.isFinite(
                date
            )
        ) {
            return Math.min(
                90000,

                Math.max(
                    1000,

                    date -
                    Date.now()
                )
            );
        }
    }

    pruneRateEvents();

    if (
        rateState.events.length
    ) {
        return Math.max(
            1000,

            Math.min(
                90000,

                Math.min(
                    ...rateState
                        .events
                        .map(
                            event =>
                                event.ts +
                                60000 -
                                Date.now() +
                                500
                        )
                )
            )
        );
    }

    return 12000;
}

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
                    "text"
            )
            .map(
                item =>
                    item.text ||
                    ""
            )
            .join(
                ""
            );
    }

    return "";
}

async function mistralChat({
    system,
    user,
    job,
    purpose =
        "translation",
    reasoning =
        "none",
    temperature =
        0.1,
    maxTokens
}) {
    if (
        !MISTRAL_API_KEY
    ) {
        throw new Error(
            "MISTRAL_API_KEY não configurada."
        );
    }

    return withMistralLane(
        async () => {
            let lastError =
                null;

            for (
                let attempt = 1;
                attempt <=
                MISTRAL_MAX_RETRIES;
                attempt++
            ) {
                job.stats.mistralAttempts++;

                const estimate =
                    estimateQueryCost(
                        purpose,

                        user.length,

                        maxTokens,

                        system.length
                    );

                await waitForRealQuota(
                    estimate,
                    job
                );

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
                        `[MISTRAL ${purpose.toUpperCase()}] ` +
                        `Request ${attempt}/${MISTRAL_MAX_RETRIES} | ` +
                        `max_tokens=${maxTokens} | ` +
                        `custo~${estimate}.`
                    );

                    const requestStartedAt =
                        Date.now();

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

                                        // IMPORTANTE:
                                        // temperature 0 entra em greedy.
                                        // A API exigiu top_p=1.
                                        top_p:
                                            1,

                                        max_tokens:
                                            maxTokens,

                                        prompt_cache_key:
                                            purpose ===
                                            "translation"
                                                ? "stremio-ptbr-651-main"
                                                : "stremio-ptbr-651-editor"
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

                    updateRateState(
                        response,

                        purpose,

                        user.length,

                        requestStartedAt
                    );

                    if (
                        response.ok &&
                        data
                    ) {
                        job.stats.mistralCalls++;

                        if (
                            purpose ===
                            "repair"
                        ) {
                            job.stats.repairCalls++;
                        }

                        if (
                            purpose ===
                            "rescue"
                        ) {
                            job.stats.rescueCalls++;
                        }

                        const text =
                            extractMistralText(
                                data
                                    ?.choices
                                    ?.[0]
                                    ?.message
                                    ?.content
                            );

                        const finishReason =
                            String(
                                data
                                    ?.choices
                                    ?.[0]
                                    ?.finish_reason ||
                                ""
                            );

                        if (
                            !text
                        ) {
                            throw new Error(
                                "Mistral retornou resposta vazia."
                            );
                        }

                        console.log(
                            `[MISTRAL ${purpose.toUpperCase()}] ` +
                            `finish_reason=${finishReason || "UNKNOWN"} | ` +
                            `chars=${text.length}.`
                        );

                        return {
                            text,

                            finishReason,

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
                            `MISTRAL HTTP ${response.status}: ` +
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

                        const wait =
                            parseRetryAfter(
                                response
                            );

                        if (
                            attempt ===
                            MISTRAL_MAX_RETRIES
                        ) {
                            throw error;
                        }

                        console.warn(
                            `[MISTRAL] 429 residual; ` +
                            `aguardando ${(wait / 1000).toFixed(
                                1
                            )}s.`
                        );

                        await sleep(
                            wait
                        );

                        rateState.remainingTokens =
                            null;

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
                            MISTRAL_MAX_RETRIES
                    ) {
                        throw error;
                    }

                    await sleep(
                        Math.min(
                            2500 *
                            attempt,

                            15000
                        )
                    );
                }
                catch (
                    error
                ) {
                    lastError =
                        error?.name ===
                        "AbortError"
                            ? new Error(
                                `MISTRAL ${purpose}: ` +
                                `timeout desta request.`
                            )
                            : error;

                    if (
                        lastError?.status ===
                        429
                    ) {
                        if (
                            attempt ===
                            MISTRAL_MAX_RETRIES
                        ) {
                            throw lastError;
                        }

                        continue;
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
                        MISTRAL_MAX_RETRIES
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
                        `[MISTRAL] ` +
                        `${errorMessage(
                            lastError
                        ).slice(
                            0,
                            180
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
                    "Mistral falhou."
                )
            );
        }
    );
}

// ============================================================
// PARSER POR CUE ID
// ============================================================

function parseGroupResponse(
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
            parsed?.groups
        )
            ? parsed.groups
            : Array.isArray(
                parsed?.items
            )
                ? parsed.items
                : [];

    for (
        const item
        of items
    ) {
        const groupId =
            Number(
                item?.g ??
                item?.groupId
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

        let cueItems =
            Array.isArray(
                item?.c
            )
                ? item.c
                : null;

        if (
            !cueItems &&
            Array.isArray(
                item?.s
            )
        ) {
            cueItems =
                item.s.map(
                    (
                        pt,
                        index
                    ) => ({
                        i:
                            group
                                .cues[
                                index
                            ]
                                ?.index,

                        pt
                    })
                );
        }

        if (
            group.cues.length ===
                1 &&
            Array.isArray(
                cueItems
            ) &&
            cueItems.length >
                1 &&
            cueItems.every(
                value =>
                    typeof (
                        value?.pt ??
                        value?.text
                    ) ===
                    "string"
            )
        ) {
            const joined =
                cueItems
                    .map(
                        value =>
                            String(
                                value.pt ??
                                value.text
                            ).trim()
                    )
                    .filter(
                        Boolean
                    )
                    .join(
                        "\n"
                    );

            if (
                joined
            ) {
                cueItems = [
                    {
                        i:
                            group
                                .cues[0]
                                .index,

                        pt:
                            joined
                    }
                ];

                job.stats.singleCueJoins++;

                console.log(
                    `[STRUCTURAL FIX] ` +
                    `g${groupId}: ` +
                    `oversplit unido no mesmo cue ` +
                    `${group.cues[0].index}.`
                );
            }
        }

        if (
            !Array.isArray(
                cueItems
            ) ||
            cueItems.length !==
                group.cues.length
        ) {
            issues.push(
                `g${groupId}:count=` +
                `${cueItems?.length ?? 0}/` +
                `${group.cues.length}`
            );

            continue;
        }

        const byId =
            new Map();

        let bad =
            false;

        for (
            const cueItem
            of cueItems
        ) {
            const id =
                Number(
                    cueItem?.i ??
                    cueItem?.id
                );

            const pt =
                String(
                    cueItem?.pt ??
                    cueItem?.text ??
                    ""
                ).trim();

            if (
                !id ||
                !pt ||
                byId.has(
                    id
                )
            ) {
                bad =
                    true;

                break;
            }

            byId.set(
                id,
                pt
            );
        }

        if (
            bad
        ) {
            issues.push(
                `g${groupId}:invalid-cue`
            );

            continue;
        }

        const segments =
            [];

        for (
            const cue
            of group.cues
        ) {
            const pt =
                byId.get(
                    cue.index
                );

            if (
                !pt
            ) {
                bad =
                    true;

                break;
            }

            segments.push(
                pt
            );
        }

        if (
            bad
        ) {
            issues.push(
                `g${groupId}:missing-id`
            );

            continue;
        }

        valid.set(
            groupId,
            segments
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
            `faltando=` +
            `${invalidGroups
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

// ============================================================
// TRANSLATE / RESCUE
// ============================================================

async function translateBatch(
    groups,
    job,
    {
        rescue =
            false,
        high =
            false
    } = {}
) {
    const system =
        rescue
            ? RESCUE_PROMPT
            : TRANSLATOR_PROMPT;

    const purpose =
        rescue
            ? "rescue"
            : "translation";

    const maxTokens =
        rescue
            ? RESCUE_MAX_TOKENS
            : MAIN_MAX_TOKENS;

    const user =
        rescue
            ?
            `Corrija a estrutura destes groups:\n` +
            `${JSON.stringify({
                groups:
                    groups.map(
                        compactGroup
                    )
            })}`
            :
            `Traduza estes Sentence Groups:\n` +
            `${JSON.stringify({
                groups:
                    groups.map(
                        compactGroup
                    )
            })}`;

    const response =
        await mistralChat({
            system,
            user,
            job,
            purpose,

            reasoning:
                high
                    ? "high"
                    : "none",

            temperature:
                rescue
                    ? 0
                    : 0.12,

            maxTokens
        });

    if (
        response.finishReason ===
            "length" ||
        response.finishReason ===
            "max_tokens"
    ) {
        const error =
            new Error(
                `OUTPUT_TRUNCATED:${groups.length}`
            );

        error.code =
            "OUTPUT_TRUNCATED";

        throw error;
    }

    return parseGroupResponse(
        groups,
        response.text,
        job
    );
}

async function rescueGroups(
    groups,
    job
) {
    const result =
        new Map();

    const batches =
        splitByBudget(
            groups,

            RESCUE_BATCH_CHARS,

            RESCUE_BATCH_GROUPS,

            compactGroup
        );

    for (
        const batch
        of batches
    ) {
        const parsed =
            await translateBatch(
                batch,
                job,
                {
                    rescue:
                        true
                }
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
            const response =
                await mistralChat({
                    system:
                        RESCUE_PROMPT,

                    user:
                        `Corrija SOMENTE este group ` +
                        `e preserve cada cue i:\n` +
                        `${JSON.stringify(
                            compactGroup(
                                group
                            )
                        )}`,

                    job,

                    purpose:
                        "rescue",

                    reasoning:
                        "high",

                    temperature:
                        0,

                    maxTokens:
                        2600
                });

            if (
                response.finishReason ===
                    "length" ||
                response.finishReason ===
                    "max_tokens"
            ) {
                throw new Error(
                    `Resgate truncado ` +
                    `g=${group.groupId}.`
                );
            }

            const one =
                parseGroupResponse(
                    [
                        group
                    ],

                    response.text,

                    job
                );

            if (
                !one.valid.has(
                    group.groupId
                )
            ) {
                throw new Error(
                    `Resgate estrutural falhou ` +
                    `g=${group.groupId}.`
                );
            }

            result.set(
                group.groupId,

                one.valid.get(
                    group.groupId
                )
            );
        }
    }

    return result;
}

async function translateBatchResilient(
    groups,
    job,
    depth = 0
) {
    try {
        const first =
            await translateBatch(
                groups,
                job
            );

        if (
            !first
                .invalidGroups
                .length
        ) {
            return first.valid;
        }

        const jsonInvalid =
            first
                .issues
                .includes(
                    "JSON_INVALID"
                );

        const invalidRatio =
            first
                .invalidGroups
                .length /
            groups.length;

        if (
            (
                jsonInvalid ||
                invalidRatio >=
                    0.45
            ) &&
            groups.length >
                24 &&
            depth <
                5
        ) {
            if (
                jsonInvalid
            ) {
                job
                    .stats
                    .jsonInvalidSplits++;
            }

            job
                .stats
                .mainSplits++;

            const middle =
                Math.ceil(
                    groups.length /
                    2
                );

            console.warn(
                `[MISTRAL SPLIT] ` +
                `lote ${groups.length} com falha ampla -> ` +
                `${middle}+${groups.length - middle}; ` +
                `sem explosão de rescues.`
            );

            const left =
                await translateBatchResilient(
                    groups.slice(
                        0,
                        middle
                    ),

                    job,

                    depth +
                    1
                );

            const right =
                await translateBatchResilient(
                    groups.slice(
                        middle
                    ),

                    job,

                    depth +
                    1
                );

            return new Map([
                ...left,
                ...right
            ]);
        }

        job.stats.salvageGroups +=
            first.valid.size;

        console.warn(
            `[MISTRAL SALVAGE] ` +
            `válidos=${first.valid.size}/${groups.length}; ` +
            `resgatar=${first.invalidGroups.length}; ` +
            `${first
                .issues
                .slice(
                    0,
                    6
                )
                .join(
                    " | "
                )}`
        );

        const rescued =
            await rescueGroups(
                first.invalidGroups,
                job
            );

        return new Map([
            ...first.valid,
            ...rescued
        ]);
    }
    catch (
        error
    ) {
        if (
            error?.code ===
                "OUTPUT_TRUNCATED" &&
            groups.length >
                24 &&
            depth <
                5
        ) {
            job
                .stats
                .outputTruncations++;

            job
                .stats
                .mainSplits++;

            const middle =
                Math.ceil(
                    groups.length /
                    2
                );

            console.warn(
                `[MISTRAL SPLIT] ` +
                `saída truncada em ${groups.length} groups -> ` +
                `${middle}+${groups.length - middle}.`
            );

            const left =
                await translateBatchResilient(
                    groups.slice(
                        0,
                        middle
                    ),

                    job,

                    depth +
                    1
                );

            const right =
                await translateBatchResilient(
                    groups.slice(
                        middle
                    ),

                    job,

                    depth +
                    1
                );

            return new Map([
                ...left,
                ...right
            ]);
        }

        throw error;
    }
}

// ============================================================
// GEMINI MICRO-AUDIT
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

async function geminiRaw(
    groups,
    translations,
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
                                                GEMINI_AUDIT_PROMPT
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
                                                    `Audite estes ${groups.length} groups:\n` +
                                                    `${JSON.stringify({
                                                        groups:
                                                            groups.map(
                                                                group =>
                                                                    reviewPayload(
                                                                        group,
                                                                        translations
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
                                        5000
                                },

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
                                ]
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

                if (
                    !text
                ) {
                    throw new Error(
                        "Gemini audit vazio."
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
                `HTTP ${response.status}`;

            const error =
                new Error(
                    `GEMINI HTTP ${response.status}: ` +
                    `${String(
                        message
                    ).slice(
                        0,
                        1000
                    )}`
                );

            error.status =
                response.status;

            if (
                response.status ===
                429
            ) {
                job.stats.gemini429++;

                const rawRetry =
                    response.headers.get(
                        "retry-after"
                    );

                const wait =
                    rawRetry &&
                    Number(
                        rawRetry
                    ) >
                        0
                        ? Math.min(
                            60000,

                            Number(
                                rawRetry
                            ) *
                            1000
                        )
                        : Math.min(
                            4000 *
                            attempt,

                            24000
                        );

                if (
                    attempt ===
                    GEMINI_MAX_RETRIES
                ) {
                    throw error;
                }

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
                    2000 *
                    attempt,

                    12000
                )
            );
        }
        catch (
            error
        ) {
            lastError =
                error?.name ===
                    "AbortError"
                    ? new Error(
                        "GEMINI: timeout desta request."
                    )
                    : error;

            if (
                attempt ===
                GEMINI_MAX_RETRIES
            ) {
                throw lastError;
            }

            await sleep(
                Math.min(
                    2000 *
                    attempt,

                    12000
                )
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
            "Gemini audit falhou."
        )
    );
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
            "Gemini audit JSON inválido."
        );
    }

    if (
        !Array.isArray(
            parsed?.items
        )
    ) {
        throw new Error(
            "Gemini audit sem items."
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
                            300
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
            `Gemini audit incompleto ` +
            `${seen.size}/${groups.length}.`
        );
    }

    return flags;
}

async function auditMicroBatch(
    groups,
    translations,
    job,
    depth = 0
) {
    try {
        const raw =
            await geminiRaw(
                groups,
                translations,
                job
            );

        const flags =
            parseAudit(
                groups,
                raw
            );

        job.stats.geminiReviewed +=
            groups.length;

        job.stats.geminiFlagged +=
            flags.size;

        return flags;
    }
    catch (
        error
    ) {
        if (
            groups.length >
                1 &&
            depth <
                6
        ) {
            const middle =
                Math.ceil(
                    groups.length /
                    2
                );

            console.warn(
                `[GEMINI AUDIT] ` +
                `split ${groups.length} por ` +
                `${errorMessage(
                    error
                ).slice(
                    0,
                    100
                )}.`
            );

            const left =
                await auditMicroBatch(
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
                await auditMicroBatch(
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

        throw error;
    }
}

// ============================================================
// FINAL CLEAN
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
        );

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

// ============================================================
// KNOWN QUALITY RISKS
// ============================================================

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
    hint =
        ""
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
    const result =
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
                result,

                group.groupId,

                reasons,

                "Aplique o Style Pack e preserve todo o sentido do EN no mesmo cue."
            );
        }
    }

    return result;
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
                    500
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

// ============================================================
// TARGETED REPAIR
// ============================================================

async function repairIssueMap(
    groups,
    translations,
    issueMap,
    job,
    high =
        false
) {
    const selected =
        groups.filter(
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
        `[TARGETED REPAIR] ` +
        `${selected.length} group(s) -> ` +
        `${batches.length} lote(s).`
    );

    for (
        const batch
        of batches
    ) {
        const user =
            `Corrija estes groups sinalizados:\n` +
            `${JSON.stringify({
                groups:
                    batch.map(
                        group =>
                            repairPayload(
                                group,

                                translations,

                                issueMap.get(
                                    group.groupId
                                )
                            )
                    )
            })}`;

        const response =
            await mistralChat({
                system:
                    REPAIR_PROMPT,

                user,

                job,

                purpose:
                    "repair",

                reasoning:
                    high
                        ? "high"
                        : "none",

                temperature:
                    0,

                maxTokens:
                    REPAIR_MAX_TOKENS
            });

        if (
            response.finishReason ===
                "length" ||
            response.finishReason ===
                "max_tokens"
        ) {
            if (
                batch.length >
                1
            ) {
                const middle =
                    Math.ceil(
                        batch.length /
                        2
                    );

                const leftMap =
                    new Map();

                const rightMap =
                    new Map();

                for (
                    const group
                    of batch.slice(
                        0,
                        middle
                    )
                ) {
                    leftMap.set(
                        group.groupId,

                        issueMap.get(
                            group.groupId
                        )
                    );
                }

                for (
                    const group
                    of batch.slice(
                        middle
                    )
                ) {
                    rightMap.set(
                        group.groupId,

                        issueMap.get(
                            group.groupId
                        )
                    );
                }

                await repairIssueMap(
                    batch.slice(
                        0,
                        middle
                    ),

                    translations,

                    leftMap,

                    job,

                    high
                );

                await repairIssueMap(
                    batch.slice(
                        middle
                    ),

                    translations,

                    rightMap,

                    job,

                    high
                );

                continue;
            }

            throw new Error(
                `Reparo truncado ` +
                `g=${batch[0].groupId}.`
            );
        }

        const parsed =
            parseGroupResponse(
                batch,

                response.text,

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

            job.stats.repairedGroups++;
        }

        if (
            parsed
                .invalidGroups
                .length
        ) {
            const rescued =
                await rescueGroups(
                    parsed.invalidGroups,
                    job
                );

            for (
                const [
                    groupId,
                    segments
                ]
                of rescued
            ) {
                translations.set(
                    groupId,
                    segments
                );
            }
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
    cleaned
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
                    cleaned[
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

    const batches =
        splitByBudget(
            groups,

            MAIN_BATCH_CHARS,

            MAIN_BATCH_GROUPS,

            compactGroup
        );

    const translations =
        new Map();

    const auditIssues =
        new Map();

    let auditTail =
        Promise.resolve();

    console.log(
        `[PIPELINE 6.5.1] ` +
        `fonte=${job.sourceKind} | ` +
        `${blocks.length} cues -> ` +
        `${groups.length} groups -> ` +
        `${batches.length} lote(s) ` +
        `Mistral serial rate-aware.`
    );

    function scheduleAudit(
        batch
    ) {
        const micros =
            splitByBudget(
                batch,

                GEMINI_AUDIT_CHARS,

                GEMINI_AUDIT_GROUPS,

                group =>
                    reviewPayload(
                        group,
                        translations
                    )
            );

        for (
            const micro
            of micros
        ) {
            auditTail =
                auditTail.then(
                    async () => {
                        const flags =
                            await auditMicroBatch(
                                micro,

                                translations,

                                job
                            );

                        for (
                            const [
                                groupId,
                                info
                            ]
                            of flags
                        ) {
                            addIssue(
                                auditIssues,

                                groupId,

                                info.reasons,

                                info.hint
                            );
                        }

                        console.log(
                            `[GEMINI AUDIT] ` +
                            `${micro.length} revisados; ` +
                            `${flags.size} marcado(s); ` +
                            `total=${job.stats.geminiReviewed}/${groups.length}.`
                        );
                    }
                );
        }
    }

    for (
        let index = 0;
        index <
        batches.length;
        index++
    ) {
        const batch =
            batches[index];

        console.log(
            `[MISTRAL MAIN] ` +
            `Lote ${index + 1}/${batches.length}: ` +
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

        job.progress =
            Math.round(
                (
                    (
                        index +
                        1
                    ) /
                    batches.length
                ) *
                72
            );

        job.updatedAt =
            Date.now();

        console.log(
            `[MISTRAL MAIN] ` +
            `Lote ${index + 1}/${batches.length} aprovado; ` +
            `total=${translations.size}/${groups.length}.`
        );

        scheduleAudit(
            batch
        );
    }

    if (
        translations.size !==
        groups.length
    ) {
        throw new Error(
            `Tradução incompleta ` +
            `${translations.size}/${groups.length}.`
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

        "CHECKPOINT MISTRAL"
    );

    await auditTail;

    if (
        job.stats.geminiReviewed !==
        groups.length
    ) {
        throw new Error(
            `Gemini auditou ` +
            `${job.stats.geminiReviewed}/${groups.length}.`
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
        `${auditIssues.size} group(s) ` +
        `para retradução dirigida.`
    );

    if (
        auditIssues.size
    ) {
        await repairIssueMap(
            groups,

            translations,

            auditIssues,

            job,

            false
        );

        const repairedGroups =
            groups.filter(
                group =>
                    auditIssues.has(
                        group.groupId
                    )
            );

        const secondIssues =
            new Map();

        const microBatches =
            splitByBudget(
                repairedGroups,

                GEMINI_AUDIT_CHARS,

                GEMINI_AUDIT_GROUPS,

                group =>
                    reviewPayload(
                        group,
                        translations
                    )
            );

        for (
            const micro
            of microBatches
        ) {
            const flags =
                await auditMicroBatch(
                    micro,

                    translations,

                    job
                );

            for (
                const [
                    groupId,
                    info
                ]
                of flags
            ) {
                addIssue(
                    secondIssues,

                    groupId,

                    info.reasons,

                    info.hint
                );
            }
        }

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
                `reparo final dirigido.`
            );

            await repairIssueMap(
                groups,

                translations,

                secondIssues,

                job,

                true
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

    const residual =
        deterministicIssueMap(
            groups,
            translations,
            job
        );

    if (
        residual.size
    ) {
        console.warn(
            `[QUALITY GUARD] AVISO: ` +
            `${residual.size} group(s) ainda sinalizado(s); ` +
            `episódio não será bloqueado.`
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
        "FINAL 6.5.1"
    );

    const elapsed =
        (
            Date.now() -
            startedAt
        ) /
        1000;

    console.log(
        `[PIPELINE 6.5.1] OK em ${elapsed.toFixed(1)}s | ` +
        `fonte=${job.sourceKind} | ` +
        `MistralCalls=${job.stats.mistralCalls} | ` +
        `Attempts=${job.stats.mistralAttempts} | ` +
        `429=${job.stats.mistral429} | ` +
        `QuotaWait=${(
            job.stats.quotaWaitMs /
            1000
        ).toFixed(1)}s | ` +
        `MainSplits=${job.stats.mainSplits} | ` +
        `JsonSplits=${job.stats.jsonInvalidSplits} | ` +
        `Trunc=${job.stats.outputTruncations} | ` +
        `RescueCalls=${job.stats.rescueCalls} | ` +
        `GeminiChecks=${job.stats.geminiReviewed} | ` +
        `GeminiCalls=${job.stats.geminiCalls} | ` +
        `Gemini429=${job.stats.gemini429} | ` +
        `GeminiFlagged=${job.stats.geminiFlagged} | ` +
        `RepairCalls=${job.stats.repairCalls} | ` +
        `RepairedGroups=${job.stats.repairedGroups} | ` +
        `SecondPass=${job.stats.secondPassGroups} | ` +
        `Residual=${residual.size}.`
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
                        "Stremio-PTBR/6.5.1"
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
                        "Stremio-PTBR/6.5.1"
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
        "6.5.1",

    name:
        "Tradutor PT-BR Backend",

    description:
        "Backend-only: Mistral serial rate-aware + Gemini micro-audit + reparo dirigido.",

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
                MISTRAL_MODEL,

            reviewer:
                GEMINI_MODEL,

            mainBatchGroups:
                MAIN_BATCH_GROUPS,

            mainBatchChars:
                MAIN_BATCH_CHARS,

            mainMaxTokens:
                MAIN_MAX_TOKENS,

            rateLimitMode:
                "REAL_HEADERS_PLUS_LOCAL_QUERY_COST_WINDOW",

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
// RESULT SRT
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

        `Progresso: ${Number(
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
            " STREMIO PT-BR BACKEND 6.5.1 HOTFIX"
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
            "Ponte 4.1: NÃO PRECISA MUDAR ✅"
        );

        console.log(
            `Mistral main batch: até ` +
            `${MAIN_BATCH_GROUPS} groups / ` +
            `${MAIN_BATCH_CHARS} chars ✅`
        );

        console.log(
            `Mistral main max_tokens: ` +
            `${MAIN_MAX_TOKENS} ✅`
        );

        console.log(
            "top_p=1 explícito com greedy sampling: ATIVO ✅"
        );

        console.log(
            "JSON inválido/truncado em lote grande: " +
            "DIVIDE O LOTE, não explode em rescue ✅"
        );

        console.log(
            "Quota Mistral: usa x-ratelimit-tokens-query-cost " +
            "+ limite real de TPM ✅"
        );

        console.log(
            "Pacer arbitrário: NÃO EXISTE ✅"
        );

        console.log(
            "429: somente fallback residual quando a API ainda negar ✅"
        );

        console.log(
            `Gemini micro-audit: até ` +
            `${GEMINI_AUDIT_GROUPS} groups ✅`
        );

        console.log(
            "Style Pack Drag/Reality/Gen Z/Alpha 2026: ATIVO ✅"
        );

        console.log(
            "Cue-ID lock + timestamps embedded imutáveis: ATIVOS ✅"
        );

        console.log(
            "Teto global do episódio: NÃO EXISTE ✅"
        );

        console.log(
            `Namespace de cache Render: ` +
            `${CACHE_VERSION}`
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
