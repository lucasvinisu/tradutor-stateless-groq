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

const PORT = Number(
    process.env.PORT ||
    10000
);

const PUBLIC_URL = String(
    process.env.PUBLIC_URL ||
    ""
).replace(
    /\/+$/,
    ""
);

const LOCAL_BRIDGE_SECRET = String(
    process.env.LOCAL_BRIDGE_SECRET ||
    ""
).trim();

const MISTRAL_API_KEY = String(
    process.env.MISTRAL_API_KEY ||
    ""
).trim();

const MISTRAL_MODEL = String(
    process.env.MISTRAL_MODEL ||
    "mistral-medium-3-5"
).trim();

const GEMINI_API_KEY = String(
    process.env.GEMINI_API_KEY ||
    ""
).trim();

const GEMINI_MODEL = String(
    process.env.GEMINI_MODEL ||
    "gemini-3.5-flash-lite"
).trim();

const CACHE_VERSION =
    "6.3.0-final";

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

const SOURCE_FETCH_TIMEOUT_MS =
    20000;

// ============================================================
// PROVIDERS
// ============================================================

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

const MISTRAL_RESCUE_CHARS =
    Number(
        process.env.MISTRAL_RESCUE_CHARS ||
        8000
    );

const MISTRAL_RESCUE_GROUPS =
    Number(
        process.env.MISTRAL_RESCUE_GROUPS ||
        24
    );

const MISTRAL_CONCURRENCY =
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

const GEMINI_MAX_OUTPUT_TOKENS =
    Number(
        process.env.GEMINI_REVIEW_MAX_OUTPUT_TOKENS ||
        10000
    );

// ============================================================
// SOURCE ARBITER
// ============================================================
//
// Esse é o conserto principal da 6.3.
//
// Se OpenSubtitles aparecer primeiro,
// ele NÃO começa imediatamente.
//
// Esperamos alguns segundos pela Ponte.
// Se a embedded chegar, ela substitui o
// fallback ANTES de qualquer gasto Mistral.
//
// No log do Drag Race a embedded chegou
// ~30 segundos depois da primeira busca.
// 45 segundos deixa margem suficiente.
//

const LOCAL_SOURCE_GRACE_MS =
    Math.max(
        10000,
        Math.min(
            Number(
                process.env.LOCAL_SOURCE_GRACE_MS ||
                45000
            ),
            90000
        )
    );

// ============================================================
// STATE
// ============================================================

const translationCache =
    new Map();

const jobs =
    new Map();

const waitingByRelease =
    new Map();

const queue =
    [];

let queueRunning =
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

function getErrorMessage(
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

function normalizeFilename(
    value
) {
    let text =
        String(
            value ||
            ""
        );

    try {
        text =
            decodeURIComponent(
                text
            );
    }
    catch {
        // mantém original
    }

    return text
        .replace(
            /\+/g,
            " "
        )
        .replace(
            /\s+/g,
            " "
        )
        .trim()
        .toLowerCase();
}

function makeReleaseKey(
    type,
    videoId,
    filename
) {
    return [
        String(
            type ||
            ""
        ).toLowerCase(),

        String(
            videoId ||
            ""
        ).toLowerCase(),

        normalizeFilename(
            filename
        )
    ].join(
        "|"
    );
}

// ============================================================
// CACHE
// ============================================================

function makeCacheKey(
    type,
    videoId,
    sourceSrt
) {
    return [
        CACHE_VERSION,
        type,
        videoId,
        sha256(
            sourceSrt
        )
    ].join(
        ":"
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

// ============================================================
// JOBS
// ============================================================

function createJob({
    type,
    videoId,
    filename,
    sourceSrt,
    sourceName,
    sourceKind,
    status
}) {
    const sourceHash =
        sha256(
            sourceSrt
        );

    const now =
        Date.now();

    const job = {
        id:
            `job-${sourceHash.slice(
                0,
                24
            )}-${randomId()}`,

        type,

        videoId,

        filename,

        releaseKey:
            makeReleaseKey(
                type,
                videoId,
                filename
            ),

        sourceSrt,

        sourceHash,

        sourceName,

        sourceKind,

        cacheKey:
            makeCacheKey(
                type,
                videoId,
                sourceSrt
            ),

        status,

        result:
            null,

        error:
            null,

        progress:
            status ===
                "waiting_source"
                ? 0
                : 1,

        createdAt:
            now,

        queuedAt:
            now,

        processingStartedAt:
            null,

        updatedAt:
            now,

        expiresAt:
            now +
            JOB_TTL_MS,

        fallbackTimer:
            null,

        enqueued:
            false,

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

            qualityRepairs:
                0,

            hardFixes:
                0
        }
    };

    jobs.set(
        job.id,
        job
    );

    return job;
}

function findActiveByRelease(
    releaseKey
) {
    for (
        const job
        of jobs.values()
    ) {
        if (
            job.releaseKey ===
                releaseKey &&
            (
                job.status ===
                    "waiting_source" ||
                job.status ===
                    "processing"
            )
        ) {
            return job;
        }
    }

    return null;
}

function cleanupMemory() {
    const now =
        Date.now();

    for (
        const [
            key,
            item
        ]
        of translationCache
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
        of jobs
    ) {
        if (
            job.expiresAt <=
                now &&
            job.status !==
                "processing" &&
            job.status !==
                "waiting_source"
        ) {
            jobs.delete(
                id
            );
        }
    }
}

setInterval(
    cleanupMemory,
    10 *
    60 *
    1000
).unref();

// ============================================================
// LOCAL BRIDGE AUTH
// ============================================================

function isAuthorizedLocalBridge(
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
                req.headers
                    .authorization ||
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
// SRT
// ============================================================

const TIMING_RE =
    /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
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
            SPEAKER_RE
        );

    if (
        hidden
    ) {
        let speaker =
            "";

        try {
            speaker =
                normalizeSpeakerHint(
                    decodeURIComponent(
                        hidden[1]
                    )
                );
        }
        catch {
            // ignora
        }

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

function normalizeVocalElongations(
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

function isNonSemanticVocalization(
    text
) {
    const normalized =
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

    /*
     * Conservador:
     *
     * "ah ah"
     * "ha ha"
     * "heh heh"
     *
     * Mas NÃO remove:
     * uh-huh
     * uh-uh
     * hmm
     * Oh!
     */
    return /^(?:ah|ha|heh)(?:\s+(?:ah|ha|heh)){1,5}$/.test(
        normalized
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
        text.replace(
            /[♪♫♬]/gu,
            " "
        );

    text =
        normalizeVocalElongations(
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
        )
    ) {
        return "";
    }

    if (
        isNonSemanticVocalization(
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
                ).trim();

            const cleaned =
                cleanSourceLine(
                    before
                );

            if (
                !cleaned &&
                isNonSemanticVocalization(
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

    const result =
        [];

    if (
        !normalized
    ) {
        return result;
    }

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
                        normalizeSpeakerHint(
                            decodeURIComponent(
                                match[1]
                            )
                        );
                }
                catch {
                    speakerHint =
                        null;
                }

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
            `TIMING LOCK ${label}: ${source.length}/${final.length}.`
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
                `TIMING LOCK ${label}: cue ${source[i].index}.`
            );
        }
    }

    console.log(
        `[AUDIT TIMESTAMP] ${label}: PASSOU — ` +
        `${source.length}/${source.length}; 0 alterações.`
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

    if (
        !match
    ) {
        return {
            start:
                NaN,

            end:
                NaN
        };
    }

    return {
        start:
            parseTimeSeconds(
                match[1]
            ),

        end:
            parseTimeSeconds(
                match[2]
            )
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
        isMultiSpeakerSource(
            previous.text
        ) ||
        isMultiSpeakerSource(
            next.text
        )
    ) {
        return false;
    }

    if (
        previous.speakerHint &&
        next.speakerHint &&
        normalizeSpeakerHint(
            previous.speakerHint
        ).toLowerCase() !==
            normalizeSpeakerHint(
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

    if (
        !/[.!?…]["'”’)\]}]*$/u.test(
            previousText
        ) &&
        /\b(?:the|to|of|or|with|for|in|at|from|that|who|which|about|into|as|than|while)\s*$/iu.test(
            previousText
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
// PROMPTS
// ============================================================

const TRANSLATOR_PROMPT = `
Você é o tradutor principal de legendas EN→PT-BR.

OBJETIVO:
Legenda brasileira excelente, natural, contemporânea, oral, fiel e contextual.

Nunca soe:
- literal;
- engessado;
- antiquado;
- lusitano;
- com gíria de tiozão;
- nem com gíria jovem artificialmente enfiada.

Reality, drag, LGBTQIA+, música, moda, memes e cultura pop:
adapte culturalmente quando o contexto pedir.

Use linguagem Gen Z/Alpha quando ela combinar DE VERDADE com a pessoa, o contexto, o humor e a referência.
Não transforme todo mundo em caricatura de TikTok.

Preserve personalidade, humor, ironia, shade, camp, vulgaridade e intensidade.
Não censure.

"I'm gagged" não é "estou amordaçada".
"She ate" pode ser "ela arrasou", "entregou tudo", etc., conforme contexto.
"off the top" monetário = comissão/corte/porcentagem.
"closing ranks" = grupo se protegendo/panelinha.
"Carry the two" = "vai dois".

Preserve quando presentes:
Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

CONTRATO TEMPORAL:
Cada group possui n cues.
Entenda o group holisticamente, mas devolva EXATAMENTE n strings em s.
Cada string pertence ao cue correspondente.
Nunca antecipe nem atrase conteúdo entre cues.

speaker é contexto oculto.
Nunca escreva nome/rótulo de falante.

Se gênero não for seguro, reformule naturalmente.
Nunca use:
empolgado(a)
animado(a)
ele/ela
barras de gênero.

Não use travessão, hífen ou barra decorativa.
Não represente alongamentos vocais.
Não acrescente SDH/CC.

Responda SOMENTE JSON:
{"items":[{"g":1,"s":["...","..."]}]}
`;

const RESCUE_PROMPT = `
Faça resgate estrutural EN→PT-BR.

Use PT-BR natural e contemporâneo.

g deve permanecer igual.
s deve conter EXATAMENTE n strings.
Uma string por cue, na ordem original.
Nunca mova conteúdo entre cues.

speaker é oculto.
Sem nomes de falante.
Sem marcadores decorativos.
Sem alongamentos gráficos.

Responda SOMENTE:
{"items":[{"g":1,"s":["..."]}]}
`;

const GEMINI_REVIEW_PROMPT = `
Você é a segunda IA independente de revisão EN→PT-BR.

Revise TODOS os groups recebidos.

O PT foi feito por Mistral Medium 3.5.

Não reescreva o que já está bom.
Não faça mudanças cosméticas.

Proponha somente correções reais e de alta confiança.

Cheque:
- sentido;
- omissões;
- temporalidade;
- gênero;
- literalidade;
- português antiquado;
- gírias e referências contemporâneas;
- reality/drag/LGBTQIA+/Gen Z/Alpha/cultura pop;
- humor, shade e camp;
- palavrões;
- nomes de falante vazados;
- marcadores de diálogo;
- alongamentos;
- termos protegidos.

speaker é contexto oculto.

Mantenha exatamente o número de segmentos.

Responda SOMENTE:
{"reviewed":123,"proposals":[{"g":12,"s":["..."],"why":"motivo","confidence":0.98}]}

Se tudo estiver bom:
{"reviewed":123,"proposals":[]}
`;

const ARBITER_PROMPT = `
Você é o árbitro final Mistral.

Compare:
EN original,
PT original Mistral,
proposta Gemini.

Aceite apenas se a proposta for claramente mais correta, fiel, natural e contemporânea.

Na dúvida, mantenha o original.

Nunca altere número de segmentos.
Nunca mova conteúdo entre cues.
Nunca exponha speaker.
Nunca chute gênero.
Nunca introduza marcadores decorativos ou alongamentos.

Responda SOMENTE:
{"accepted":[{"g":123,"s":["..."],"why":"motivo"}]}
`;

const REPAIR_PROMPT = `
Você é o reparador final de legenda EN→PT-BR.

Você recebe somente groups com risco concreto.

Corrija SOMENTE o problema apontado.

Use PT-BR natural, atual, oral e fiel.

Evite:
literalidade,
português antiquado,
gíria datada,
gíria jovem forçada.

Preserve:
g,
quantidade de segmentos,
temporalidade,
speaker apenas como contexto oculto.

Responda SOMENTE:
{"items":[{"g":123,"s":["..."]}]}
`;

// ============================================================
// MISTRAL GOVERNOR
// ============================================================

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

    limit() {
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
                this.limit()
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

    note429(
        waitMs
    ) {
        const now =
            Date.now();

        this.cooldownUntil =
            Math.max(
                this.cooldownUntil,
                now +
                    waitMs
            );

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
}

const mistralGovernor =
    new AdaptiveGovernor(
        MISTRAL_CONCURRENCY
    );

// ============================================================
// PROVIDER HTTP
// ============================================================

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
                seconds *
                    1000,
                120000
            );
        }
    }

    return Math.min(
        Math.max(
            fallback ||
            4000,
            1000
        ),
        120000
    );
}

async function postJsonRetry({
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
            job.stats[
                attemptCounter
            ]++;
        }

        if (
            governor
        ) {
            await governor.acquire();
        }

        let slotHeld =
            Boolean(
                governor
            );

        const release =
            () => {
                if (
                    slotHeld
                ) {
                    slotHeld =
                        false;

                    governor.release();
                }
            };

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
                    job.stats[
                        successCounter
                    ]++;
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
                        1400
                    )}`
                );

            error.status =
                response.status;

            if (
                response.status ===
                429
            ) {
                if (
                    job &&
                    rateCounter
                ) {
                    job.stats[
                        rateCounter
                    ]++;
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
                    governor.note429(
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

                release();

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
                    20000
                );

            release();

            clearTimeout(
                timer
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
                    180
                )}; retry em ${(wait / 1000).toFixed(
                    1
                )}s.`
            );

            release();

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

            release();
        }
    }

    throw (
        lastError ||
        new Error(
            `${provider}: falha.`
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
    reasoning =
        "none",
    temperature =
        0.1,
    maxTokens =
        16000,
    purpose =
        "translation"
}) {
    const provider =
        purpose ===
            "arbiter"
            ? "MISTRAL_ARBITER"
            : purpose ===
                "repair"
                ? "MISTRAL_REPAIR"
                : "MISTRAL";

    const data =
        await postJsonRetry({
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
                        ? "stremio-ptbr-6-3-translator"
                        : "stremio-ptbr-6-3-editor"
            },

            timeoutMs:
                MISTRAL_TIMEOUT_MS,

            provider,

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
                "mistral429",

            governor:
                mistralGovernor
        });

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
            `${provider} retornou resposta vazia.`
        );
    }

    return text;
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

    const items =
        Array.isArray(
            parsed
        )
            ? parsed
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
                `${
                    rescue
                        ? "Resgate"
                        : "Traduza"
                } estes groups:\n` +
                JSON.stringify({
                    groups:
                        groups.map(
                            compactGroup
                        )
                }),

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
        raw
    );
}

async function rescueSingleGroup(
    group,
    job
) {
    job.stats.atomicRescues++;

    console.warn(
        `[MISTRAL RESCUE] Group ${group.groupId} isolado.`
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
                    `Traduza este group com EXATAMENTE ${group.cues.length} string(s):\n` +
                    JSON.stringify(
                        compactGroup(
                            group
                        )
                    ),

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
                raw
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
                    `Traduza SOMENTE o cue target=true para PT-BR natural. ` +
                    `Use os demais apenas como contexto. ` +
                    `speaker é oculto. Responda {"text":"..."}.`,

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
                    1600,

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
                `Resgate por cue falhou no group ${group.groupId}.`
            );
        }

        output.push(
            text.trim()
        );
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
        !first.invalidGroups.length
    ) {
        return result;
    }

    job.stats.salvageGroups +=
        first.valid.size;

    console.warn(
        `[MISTRAL SALVAGE] válidos=${first.valid.size}/${groups.length}; ` +
        `resgatar=${first.invalidGroups.length}.`
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
            batch.length ===
            1
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
            `[MISTRAL RESCUE] Lote pequeno com ${batch.length} group(s).`
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

    return result;
}

// ============================================================
// GEMINI REVIEW
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

async function geminiReview(
    entries,
    job
) {
    const data =
        await postJsonRetry({
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
                                    JSON.stringify({
                                        groups:
                                            entries.map(
                                                entry =>
                                                    compactReviewGroup(
                                                        entry.group,
                                                        entry.segments
                                                    )
                                            )
                                    })
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
            },

            timeoutMs:
                GEMINI_TIMEOUT_MS,

            provider:
                "GEMINI_REVIEW",

            maxRetries:
                GEMINI_MAX_RETRIES,

            job,

            attemptCounter:
                "geminiAttempts",

            successCounter:
                "geminiCalls",

            rateCounter:
                "gemini429"
        });

    const text =
        extractGeminiText(
            data
        );

    if (
        !text
    ) {
        throw new Error(
            "Gemini retornou resposta vazia."
        );
    }

    let parsed;

    try {
        parsed =
            JSON.parse(
                stripCodeFences(
                    text
                )
            );
    }
    catch {
        throw new Error(
            "Gemini retornou JSON inválido."
        );
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
        throw new Error(
            "Gemini não confirmou revisão integral."
        );
    }

    job.stats.geminiReviewed +=
        entries.length;

    const allowed =
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

    const proposals =
        [];

    for (
        const proposal
        of parsed.proposals
    ) {
        const groupId =
            Number(
                proposal?.g
            );

        const entry =
            allowed.get(
                groupId
            );

        if (
            !entry
        ) {
            continue;
        }

        const segments =
            proposal?.s;

        if (
            !Array.isArray(
                segments
            ) ||
            segments.length !==
                entry
                    .group
                    .cues
                    .length ||
            segments.some(
                value =>
                    typeof value !==
                        "string" ||
                    !value.trim()
            )
        ) {
            continue;
        }

        proposals.push({
            g:
                groupId,

            s:
                segments.map(
                    value =>
                        value.trim()
                ),

            why:
                String(
                    proposal?.why ||
                    "correção"
                ),

            confidence:
                Number(
                    proposal?.confidence ||
                    0.9
                ),

            group:
                entry.group,

            original:
                entry.segments
        });
    }

    job.stats.geminiProposals +=
        proposals.length;

    return proposals;
}

async function geminiReviewResilient(
    entries,
    job,
    depth = 0
) {
    try {
        return await geminiReview(
            entries,
            job
        );
    }
    catch (
        error
    ) {
        if (
            entries.length >
                1 &&
            depth <
                7
        ) {
            const middle =
                Math.ceil(
                    entries.length /
                    2
                );

            console.warn(
                `[GEMINI REVIEW] Split ${entries.length} -> ` +
                `${middle}+${entries.length - middle}.`
            );

            const left =
                await geminiReviewResilient(
                    entries.slice(
                        0,
                        middle
                    ),
                    job,
                    depth +
                        1
                );

            const right =
                await geminiReviewResilient(
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

        throw error;
    }
}

// ============================================================
// ARBITER
// ============================================================

async function arbitrateProposals(
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

    const builder =
        proposal => ({
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
        });

    const batches =
        splitByBudget(
            proposals,
            7000,
            30,
            builder
        );

    for (
        let index = 0;
        index <
        batches.length;
        index++
    ) {
        const batch =
            batches[
                index
            ];

        const raw =
            await mistralChat({
                system:
                    ARBITER_PROMPT,

                user:
                    JSON.stringify({
                        proposals:
                            batch.map(
                                builder
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
            parsed = {
                accepted: []
            };
        }

        let accepted =
            0;

        const allowed =
            new Map(
                batch.map(
                    proposal => [
                        proposal.g,
                        proposal
                    ]
                )
            );

        for (
            const item
            of Array.isArray(
                parsed?.accepted
            )
                ? parsed.accepted
                : []
        ) {
            const groupId =
                Number(
                    item?.g
                );

            const original =
                allowed.get(
                    groupId
                );

            if (
                !original ||
                !Array.isArray(
                    item?.s
                ) ||
                item.s.length !==
                    original
                        .group
                        .cues
                        .length
            ) {
                continue;
            }

            translations.set(
                groupId,

                item.s.map(
                    value =>
                        String(
                            value
                        ).trim()
                )
            );

            accepted++;
        }

        job.stats.arbiterAccepted +=
            accepted;

        console.log(
            `[MISTRAL ARBITER] ${index + 1}/${batches.length}: ` +
            `${accepted} aceita(s).`
        );
    }
}

// ============================================================
// FINAL CLEAN / HARD FIXES
// ============================================================

function cleanFinalText(
    text
) {
    return normalizeVocalElongations(
        String(
            text ||
            ""
        )
    )
        .split(
            "\n"
        )
        .map(
            line =>
                line
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
            text.replace(
                /\bworkroom\b/gi,
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
        /\blip[ -]?sync\b/i.test(
            en
        )
    ) {
        text =
            text
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
        String(
            target ||
            ""
        );

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
            );

    /*
     * Caso que já apareceu no Drag Race:
     * "goddamn check".
     *
     * Mantemos a intensidade,
     * mas não grudamos o palavrão
     * artificialmente no substantivo.
     */
    if (
        /\bgoddamn check\b/i.test(
            String(
                source ||
                ""
            )
        )
    ) {
        text =
            text
                .replace(
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
                    blocks[index]
                        .text,
                    output
                );

            const fixed =
                applyHardFixes(
                    blocks[index]
                        .text,
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
// QUALITY GUARD
// ============================================================

function finalRiskScan(
    blocks,
    texts
) {
    const issues =
        [];

    for (
        let index = 0;
        index <
        texts.length;
        index++
    ) {
        const pt =
            String(
                texts[
                    index
                ] ||
                ""
            );

        const en =
            String(
                blocks[
                    index
                ]?.text ||
                ""
            );

        const reasons =
            [];

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

        if (
            reasons.length
        ) {
            issues.push({
                id:
                    blocks[
                        index
                    ].index,

                reasons,

                source:
                    en,

                text:
                    pt
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

    const map =
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

        const reasons =
            map.get(
                groupId
            ) ||
            new Set();

        for (
            const reason
            of issue.reasons
        ) {
            reasons.add(
                reason
            );
        }

        map.set(
            groupId,
            reasons
        );
    }

    return map;
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

    const reasonsMap =
        mapIssuesToGroups(
            groups,
            issues
        );

    const risky =
        groups.filter(
            group =>
                reasonsMap.has(
                    group.groupId
                )
        );

    console.warn(
        `[QUALITY REPAIR] ${issues.length} risco(s) em ${risky.length} group(s).`
    );

    const batches =
        splitByBudget(
            risky,
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
                        ...reasonsMap.get(
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
                                            ...reasonsMap.get(
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
                raw
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
// PIPELINE
// ============================================================

async function translateSrt(
    sourceSrt,
    job
) {
    const blocks =
        parseSrt(
            sourceSrt
        );

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

    let completed =
        0;

    const startedAt =
        Date.now();

    console.log(
        `[PIPELINE 6.3] fonte=${job.sourceKind} | ` +
        `${blocks.length} cues -> ${groups.length} groups -> ` +
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

        reviewChain =
            reviewChain.then(
                async () => {
                    console.log(
                        `[GEMINI REVIEW] Revisando ${entries.length} group(s).`
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
                        `[GEMINI REVIEW] confirmado=${entries.length}; ` +
                        `propostas=${batchProposals.length}; ` +
                        `total=${job.stats.geminiReviewed}/${groups.length}.`
                    );
                }
            );
    }

    async function worker(
        workerId
    ) {
        while (
            true
        ) {
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

            completed++;

            job.progress =
                Math.round(
                    (
                        completed /
                        batches.length
                    ) *
                    72
                );

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
                    index +
                    1
                )
        )
    );

    if (
        translations.size !==
        groups.length
    ) {
        throw new Error(
            `Tradução incompleta: ${translations.size}/${groups.length}.`
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

    await reviewChain;

    if (
        job.stats.geminiReviewed !==
        groups.length
    ) {
        throw new Error(
            `Gemini revisou ${job.stats.geminiReviewed}/${groups.length}.`
        );
    }

    console.log(
        `[GEMINI REVIEW] COMPLETA: ` +
        `${job.stats.geminiReviewed}/${groups.length}; ` +
        `propostas=${proposals.length}.`
    );

    await arbitrateProposals(
        proposals,
        translations,
        job
    );

    let texts =
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
            texts
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

        texts =
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
                texts
            );
    }

    /*
     * Hard-fixes conhecidos são aplicados
     * novamente depois da IA.
     *
     * Portanto:
     * cabina de votação
     * cheque da porra
     *
     * não derrubam mais um episódio inteiro.
     */
    texts =
        cleanAllFinal(
            blocks,
            texts,
            job
        );

    risks =
        finalRiskScan(
            blocks,
            texts
        );

    if (
        risks.length
    ) {
        throw new Error(
            `Quality Guard ainda encontrou ${risks.length} risco(s): ` +
            risks
                .slice(
                    0,
                    12
                )
                .map(
                    issue =>
                        `${issue.id}:${issue.reasons.join(
                            "+"
                        )}`
                )
                .join(
                    ", "
                )
        );
    }

    console.log(
        "[QUALITY GUARD] PASSOU."
    );

    const finalSrt =
        buildSrt(
            blocks,
            texts
        );

    auditTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL 6.3"
    );

    const elapsed =
        (
            Date.now() -
            startedAt
        ) /
        1000;

    console.log(
        `[PIPELINE 6.3] OK em ${elapsed.toFixed(
            1
        )}s | ` +
        `fonte=${job.sourceKind} | ` +
        `MistralCalls=${job.stats.mistralCalls} | ` +
        `Attempts=${job.stats.mistralAttempts} | ` +
        `429=${job.stats.mistral429} | ` +
        `Gemini=${job.stats.geminiReviewed}/${groups.length} | ` +
        `Propostas=${job.stats.geminiProposals} | ` +
        `ArbiterAccepted=${job.stats.arbiterAccepted} | ` +
        `HardFixes=${job.stats.hardFixes}.`
    );

    return finalSrt;
}

// ============================================================
// JOB QUEUE
// ============================================================

async function processJob(
    job
) {
    job.processingStartedAt =
        Date.now();

    job.status =
        "processing";

    console.log(
        `[JOB ${job.id}] Iniciando fonte=${job.sourceKind}. SEM teto global.`
    );

    try {
        const cached =
            getCache(
                job.cacheKey
            );

        if (
            cached
        ) {
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
            getErrorMessage(
                error
            );

        console.error(
            `[JOB ${job.id}] Falhou: ${job.error}`
        );
    }

    job.updatedAt =
        Date.now();
}

function enqueue(
    job
) {
    if (
        job.enqueued
    ) {
        return;
    }

    job.enqueued =
        true;

    queue.push(
        job
    );

    console.log(
        `[JOB QUEUE] ${job.id} entrou na fila; aguardando=${queue.length}.`
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
    }
}

// ============================================================
// SOURCE ARBITRATION
// ============================================================

function activateOpenSubtitlesFallback(
    job
) {
    if (
        !job ||
        job.status !==
            "waiting_source"
    ) {
        return;
    }

    waitingByRelease.delete(
        job.releaseKey
    );

    job.status =
        "processing";

    job.sourceKind =
        "opensubtitles-fallback";

    job.queuedAt =
        Date.now();

    console.log(
        `[SOURCE ARBITER] Embedded não chegou em ` +
        `${LOCAL_SOURCE_GRACE_MS / 1000}s. ` +
        `Usando OpenSubtitles fallback.`
    );

    enqueue(
        job
    );
}

function createWaitingFallbackJob({
    type,
    videoId,
    filename,
    sourceSrt,
    sourceName
}) {
    const key =
        makeReleaseKey(
            type,
            videoId,
            filename
        );

    const existing =
        waitingByRelease.get(
            key
        );

    if (
        existing &&
        existing.status ===
            "waiting_source"
    ) {
        return existing;
    }

    const job =
        createJob({
            type,

            videoId,

            filename,

            sourceSrt,

            sourceName,

            sourceKind:
                "opensubtitles-pending",

            status:
                "waiting_source"
        });

    waitingByRelease.set(
        key,
        job
    );

    job.fallbackTimer =
        setTimeout(
            () =>
                activateOpenSubtitlesFallback(
                    job
                ),
            LOCAL_SOURCE_GRACE_MS
        );

    job.fallbackTimer.unref?.();

    console.log(
        `[SOURCE ARBITER] OpenSubtitles encontrado. ` +
        `Aguardando embedded por até ` +
        `${LOCAL_SOURCE_GRACE_MS / 1000}s ANTES do Mistral.`
    );

    return job;
}

function promoteEmbeddedOrCreate({
    type,
    videoId,
    filename,
    sourceSrt,
    sourceName
}) {
    const key =
        makeReleaseKey(
            type,
            videoId,
            filename
        );

    const waiting =
        waitingByRelease.get(
            key
        );

    if (
        waiting &&
        waiting.status ===
            "waiting_source"
    ) {
        if (
            waiting.fallbackTimer
        ) {
            clearTimeout(
                waiting.fallbackTimer
            );
        }

        waitingByRelease.delete(
            key
        );

        waiting.sourceSrt =
            sourceSrt;

        waiting.sourceHash =
            sha256(
                sourceSrt
            );

        waiting.cacheKey =
            makeCacheKey(
                type,
                videoId,
                sourceSrt
            );

        waiting.sourceName =
            sourceName;

        waiting.sourceKind =
            "embedded";

        waiting.status =
            "processing";

        waiting.progress =
            1;

        waiting.queuedAt =
            Date.now();

        console.log(
            `[SOURCE ARBITER] EMBEDDED venceu ✅ ` +
            `OpenSubtitles cancelado ANTES de gastar Mistral.`
        );

        enqueue(
            waiting
        );

        return waiting;
    }

    const active =
        findActiveByRelease(
            key
        );

    if (
        active
    ) {
        return active;
    }

    const job =
        createJob({
            type,

            videoId,

            filename,

            sourceSrt,

            sourceName,

            sourceKind:
                "embedded",

            status:
                "processing"
        });

    enqueue(
        job
    );

    return job;
}

// ============================================================
// OPENSUBTITLES
// ============================================================

async function fetchWithTimeout(
    url,
    options = {},
    timeout =
        SOURCE_FETCH_TIMEOUT_MS
) {
    const controller =
        new AbortController();

    const timer =
        setTimeout(
            () =>
                controller.abort(),
            timeout
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

function parseExtra(
    req
) {
    const params =
        new URLSearchParams(
            String(
                req.params
                    .extra ||
                ""
            )
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
        )[0] ||
        null;
}

async function findEnglishSubtitle(
    type,
    id,
    extra
) {
    const url =
        buildOpenSubtitlesUrl(
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
                        "Stremio-PTBR/6.3"
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

    console.log(
        target
            ? "[OPENSUBTITLES] Inglês encontrado."
            : "[OPENSUBTITLES] Nenhum inglês utilizável."
    );

    return target;
}

async function downloadAndClean(
    url
) {
    const response =
        await fetchWithTimeout(
            url
        );

    if (
        !response.ok
    ) {
        throw new Error(
            `Download legenda HTTP ${response.status}.`
        );
    }

    const raw =
        normalizeSrt(
            await response.text()
        );

    if (
        !raw ||
        raw.length >
            MAX_SOURCE_CHARS
    ) {
        throw new Error(
            "Legenda vazia ou grande demais."
        );
    }

    return cleanSrtForTranslation(
        raw
    );
}

// ============================================================
// SRT STATUS
// ============================================================

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

function processingSrt(
    job
) {
    const message =
        job.status ===
            "waiting_source"
            ? "Aguardando a faixa embedded exata antes do fallback..."
            : `Traduzindo e revisando... Progresso ${job.progress || 0}%.`;

    return [
        "1",
        "00:00:01,000 --> 00:00:09,000",
        message
    ].join(
        "\n"
    );
}

function errorSrt(
    error
) {
    return [
        "1",
        "00:00:01,000 --> 00:00:09,000",
        "Não foi possível concluir a legenda PT-BR.",
        "",
        "2",
        "00:00:09,500 --> 00:00:20,000",
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
                280
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
        "6.3.0",

    name:
        "Tradutor PT-BR Premium",

    description:
        "Embedded-first + Mistral Medium 3.5 + Gemini review + Mistral arbiter.",

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

    catalogs: []
};

// ============================================================
// ROUTES
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

            sourcePolicy:
                "embedded-first",

            localSourceGraceSeconds:
                LOCAL_SOURCE_GRACE_MS /
                1000,

            translator:
                MISTRAL_MODEL,

            reviewer:
                GEMINI_MODEL,

            cacheVersion:
                CACHE_VERSION
        })
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

        const releaseKey =
            makeReleaseKey(
                type,
                id,
                extra.filename
            );

        const active =
            findActiveByRelease(
                releaseKey
            );

        if (
            active
        ) {
            return safeJson(
                res,
                {
                    subtitles: [
                        {
                            id:
                                `${id}-ptbr-${active.id.slice(
                                    -8
                                )}`,

                            url:
                                `${cleanBaseUrl(
                                    req
                                )}/subtitle/${encodeURIComponent(
                                    active.id
                                )}.srt`,

                            lang:
                                "por"
                        }
                    ]
                }
            );
        }

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
            await downloadAndClean(
                target.url
            );

        let job;

        if (
            LOCAL_BRIDGE_SECRET &&
            extra.filename
        ) {
            job =
                createWaitingFallbackJob({
                    type,

                    videoId:
                        id,

                    filename:
                        extra.filename,

                    sourceSrt,

                    sourceName:
                        target.name ||
                        "OpenSubtitles"
                });
        }
        else {
            job =
                createJob({
                    type,

                    videoId:
                        id,

                    filename:
                        extra.filename,

                    sourceSrt,

                    sourceName:
                        target.name ||
                        "OpenSubtitles",

                    sourceKind:
                        "opensubtitles",

                    status:
                        "processing"
                });

            enqueue(
                job
            );
        }

        return safeJson(
            res,
            {
                subtitles: [
                    {
                        id:
                            `${id}-ptbr-${job.id.slice(
                                -8
                            )}`,

                        url:
                            `${cleanBaseUrl(
                                req
                            )}/subtitle/${encodeURIComponent(
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

            /*
             * Ponte Local 3.0 envia o
             * filename no campo name.
             */
            const filename =
                String(
                    req.body
                        ?.filename ||
                    req.body
                        ?.name ||
                    ""
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
                            "Campo srt obrigatório."
                    },
                    400
                );
            }

            const cleanedSrt =
                cleanSrtForTranslation(
                    rawSrt
                );

            console.log(
                `[EMBEDDED API] ${type}/${videoId} | ` +
                `${filename} | ` +
                `${parseSrt(
                    cleanedSrt
                ).length} cues.`
            );

            const job =
                promoteEmbeddedOrCreate({
                    type,

                    videoId,

                    filename,

                    sourceSrt:
                        cleanedSrt,

                    sourceName
                });

            return safeJson(
                res,
                {
                    ok:
                        true,

                    jobId:
                        job.id,

                    status:
                        job.status,

                    subtitleUrl:
                        `${cleanBaseUrl(
                            req
                        )}/subtitle/${encodeURIComponent(
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

app.get(
    "/job/:jobId",
    (
        req,
        res
    ) => {
        const job =
            jobs.get(
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

app.get(
    "/subtitle/:jobId.srt",
    (
        req,
        res
    ) => {
        const jobId =
            decodeURIComponent(
                String(
                    req.params
                        .jobId ||
                    ""
                )
            );

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
                        getErrorMessage(
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
            "no-store"
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
            " STREMIO PT-BR DUAL AI TRANSLATOR 6.3 FINAL"
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
            `Fonte: EMBEDDED TEM PRIORIDADE por ` +
            `${LOCAL_SOURCE_GRACE_MS / 1000}s ✅`
        );

        console.log(
            "OpenSubtitles: FALLBACK; não começa Mistral enquanto aguarda embedded ✅"
        );

        console.log(
            "Jobs duplicados OpenSubtitles + embedded: BLOQUEADOS ✅"
        );

        console.log(
            `Mistral concorrência adaptativa: ATÉ ${MISTRAL_CONCURRENCY} ✅`
        );

        console.log(
            "Gemini revisa 100%: ATIVO ✅"
        );

        console.log(
            "Mistral arbitra propostas Gemini: ATIVO ✅"
        );

        console.log(
            'Vocalização repetida isolada "ah ah": FILTRADA ✅'
        );

        console.log(
            "Hard-fixes conhecidos antes do Quality Guard final: ATIVOS ✅"
        );

        console.log(
            "Teto global: NÃO EXISTE ✅"
        );

        console.log(
            "Timestamps: IMUTÁVEIS ✅"
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
