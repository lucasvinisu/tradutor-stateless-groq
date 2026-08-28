const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

// ============================================================
// CONFIG
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();

const MISTRAL_API_KEY = String(process.env.MISTRAL_API_KEY || "").trim();
const MISTRAL_MODEL = String(process.env.MISTRAL_MODEL || "mistral-medium-3-5").trim();

const GROQ_API_KEY = String(process.env.GROQ_API_KEY || "").trim();
const GROQ_REVIEW_MODEL = String(
    process.env.GROQ_REVIEW_MODEL ||
    "groq/compound-mini"
).trim();

// Mantido somente como rollback de emergência/manual.
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();

const GEMINI_MODEL = String(
    process.env.GEMINI_MODEL ||
    "gemini-3.5-flash-lite"
).trim();

const TRANSLATION_CACHE_VERSION = "6.0";

const SOURCE_FETCH_TIMEOUT_MS = 20_000;

const MISTRAL_TIMEOUT_MS = Number(
    process.env.MISTRAL_TIMEOUT_MS ||
    120_000
);

const GROQ_REVIEW_TIMEOUT_MS = Number(
    process.env.GROQ_REVIEW_TIMEOUT_MS ||
    90_000
);

const MAX_TRANSLATION_TIME_MS = Number(
    process.env.MAX_TRANSLATION_TIME_MS ||
    480_000
);

const MAX_SOURCE_CHARS = 800_000;

const CACHE_TTL_MS =
    7 * 24 * 60 * 60 * 1000;

const JOB_TTL_MS =
    24 * 60 * 60 * 1000;

const MAX_CACHE_ENTRIES = 200;
const MAX_JOBS = 300;

const AI_REQUEST_MAX_RETRIES = 4;

// Poucos lotes grandes:
// reduz prompt repetido e chamadas.
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

// O reviewer vê EN + PT
// e devolve SOMENTE correções.
const REVIEW_BATCH_CHARS = Number(
    process.env.REVIEW_BATCH_CHARS ||
    42_000
);

const REVIEW_BATCH_GROUPS = Number(
    process.env.REVIEW_BATCH_GROUPS ||
    450
);

const GROQ_REVIEW_MAX_OUTPUT_TOKENS = Number(
    process.env.GROQ_REVIEW_MAX_OUTPUT_TOKENS ||
    8_000
);

// ============================================================
// MEMÓRIA / FILA
// ============================================================

const translationCache = new Map();
const jobs = new Map();

const translationJobQueue = [];

let translationJobWorkerRunning = false;

// ============================================================
// UTILS
// ============================================================

function sleep(ms) {
    return new Promise(
        resolve =>
            setTimeout(
                resolve,
                ms
            )
    );
}

function sha256(value) {
    return crypto
        .createHash("sha256")
        .update(
            String(value),
            "utf8"
        )
        .digest("hex");
}

function randomId(bytes = 8) {
    return crypto
        .randomBytes(bytes)
        .toString("hex");
}

function getErrorMessage(error) {
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
    res
        .status(status)
        .json(payload);
}

function cleanBaseUrl(req) {
    if (PUBLIC_URL) {
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

    return `${proto}://${host}`
        .replace(/\/+$/, "");
}

function normalizeSrt(value) {
    return String(
        value ||
        ""
    )
        .replace(/^\uFEFF/, "")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n")
        .trim();
}

function stripCodeFences(value) {
    return String(
        value ||
        ""
    )
        .replace(
            /^\s*```(?:json)?\s*/i,
            ""
        )
        .replace(
            /\s*```\s*$/i,
            ""
        )
        .trim();
}

function translationTimeoutError() {
    const error =
        new Error(
            "Tempo máximo da tradução atingido."
        );

    error.code =
        "TRANSLATION_TIMEOUT";

    return error;
}

function badModelOutputError(
    message
) {
    const error =
        new Error(message);

    error.code =
        "BAD_MODEL_OUTPUT";

    return error;
}

function remainingBeforeDeadline(
    deadlineAt
) {
    return Number.isFinite(
        deadlineAt
    )
        ? deadlineAt -
            Date.now()
        : Infinity;
}

function assertBeforeDeadline(
    deadlineAt
) {
    if (
        remainingBeforeDeadline(
            deadlineAt
        ) <= 0
    ) {
        throw translationTimeoutError();
    }
}

// ============================================================
// CACHE / JOBS
// ============================================================

function pruneMapByTime(
    map,
    ttlMs,
    maxEntries
) {
    const now =
        Date.now();

    for (
        const [
            key,
            value
        ]
        of map
    ) {
        const timestamp =
            Number(
                value?.updatedAt ||
                value?.createdAt ||
                value?.storedAt ||
                0
            );

        if (
            timestamp &&
            now - timestamp >
                ttlMs
        ) {
            map.delete(key);
        }
    }

    while (
        map.size >
        maxEntries
    ) {
        const first =
            map
                .keys()
                .next()
                .value;

        if (
            first ===
            undefined
        ) {
            break;
        }

        map.delete(first);
    }
}

function getTranslationCache(
    key
) {
    const item =
        translationCache.get(
            key
        );

    if (!item) {
        return null;
    }

    if (
        Date.now() -
            item.storedAt >
        CACHE_TTL_MS
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
            storedAt:
                Date.now()
        }
    );

    pruneMapByTime(
        translationCache,
        CACHE_TTL_MS,
        MAX_CACHE_ENTRIES
    );
}

function getJob(id) {
    const job =
        jobs.get(id);

    if (!job) {
        return null;
    }

    if (
        Date.now() -
            job.updatedAt >
        JOB_TTL_MS
    ) {
        jobs.delete(id);

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

        deadlineAt:
            now +
            MAX_TRANSLATION_TIME_MS,

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

        providerUsage:
            {}
    };

    jobs.set(
        job.id,
        job
    );

    pruneMapByTime(
        jobs,
        JOB_TTL_MS,
        MAX_JOBS
    );

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

// ============================================================
// AUTH PONTE LOCAL
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
        );

    const expected =
        `Bearer ${LOCAL_BRIDGE_SECRET}`;

    const a =
        Buffer.from(auth);

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

    if (hidden) {
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

    if (bracket) {
        const speaker =
            normalizeSpeakerHint(
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
            normalizeSpeakerHint(
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
     * home-e-e-e
     * amo-o-o-o
     */
    value =
        value.replace(
            /([A-Za-zÀ-ÖØ-öø-ÿ]+?)([-–—])([A-Za-zÀ-ÖØ-öø-ÿ])(?:\2\3){2,}/gu,
            "$1$3"
        );

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

    if (!text) {
        return "";
    }

    /*
     * SDH em colchetes.
     * Speaker já foi
     * extraído antes.
     */
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
        text.replace(
            /[ \t]{2,}/g,
            " "
        ).trim();

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

    const out = [];

    let removed = 0;
    let speakerBlocks = 0;
    let elongationChanges = 0;

    for (
        const rawBlock
        of rawBlocks
    ) {
        const lines =
            rawBlock
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
        `[CLEAN] SDH/CC: ${rawBlocks.length} -> ${out.length}; removidos=${removed}; speakerHints=${speakerBlocks}; alongamentos=${elongationChanges}.`
    );

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
        (
            out.length
                ? "\n"
                : ""
        )
    );
}

function parseSrt(
    srt
) {
    const normalized =
        normalizeSrt(srt);

    if (!normalized) {
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
                .split("\n");

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
            lines.slice(2);

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

            if (match) {
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
                    .join("\n")
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

    if (!m) {
        return NaN;
    }

    return (
        Number(m[1]) *
            3600 +
        Number(m[2]) *
            60 +
        Number(m[3]) +
        Number(m[4]) /
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
            .split("\n")
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

CONTRATO TEMPORAL:
Cada Sentence Group é uma frase/ideia com um ou mais cues.
Traduza holisticamente para entender contexto, MAS devolva exatamente um segmento PT para cada cue EN, na mesma ordem.
Cada segmento PT deve conter somente o conteúdo semanticamente pronunciado naquele cue.
Não antecipe conteúdo do próximo cue e não atrase conteúdo para outro cue.

Preserve humor, ironia, shade, camp, personalidade, vulgaridade e intensidade.
Não censure.

Adapte idioms, gírias e memes de reality, drag, LGBTQIA+, música, moda e cultura pop quando houver equivalente brasileiro natural, sem forçar internetês ou gíria queer.

"I'm gagged" não é "estou amordaçada".
"She ate" não é comer literalmente quando for gíria.
"off the top" monetário é comissão/corte.
"closing ranks" pode ser panelinha/proteção.
"Carry the two" é "vai dois".

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
Prefira formulação brasileira naturalmente neutra.

Nunca use:
empolgado(a)
ele/ela
ela/ele
barras de gênero

Não desenhe notas sustentadas:
nada de você-e-e-e
amo-o-o-o
sooooo
nãããão

O áudio já carrega a duração.

Não acrescente hífen, travessão, meia-risca, barra ou marcador decorativo de diálogo.

Em cue com mais de um falante, use linhas limpas sem prefixos.

Não acrescente SDH/CC, sons, explicações ou markdown.

Traduza letras de música somente quando a letra real estiver transcrita.

Responda SOMENTE JSON no formato:

{"items":[{"g":1,"s":["segmento cue 1","segmento cue 2"]}]}

g deve repetir exatamente o group id.
s deve ter exatamente a mesma quantidade de elementos que c.
`;

const REVIEWER_SYSTEM_PROMPT = `
Você é a SEGUNDA IA, editora independente de legendas EN→PT-BR.

A tradução já está pronta.

Revise TODO o lote e corrija SOMENTE erros reais e de alta confiança.

NÃO retraduza tudo.
NÃO altere frases que já estão boas.

Procure especialmente:

1. sentido errado, omissão ou antecipação;
2. conteúdo colocado no cue errado;
3. português literal, engessado, lusitano ou datado;
4. idiom, gíria, meme, reality, drag, LGBTQIA+, camp, shade, música, moda ou cultura pop culturalmente deslocados;
5. palavrão censurado ou colocado de forma artificial;
6. concordância ou gênero errado;
7. speaker vazado, como [Kelly]:, Kelly:, personagem:;
8. alongamento gráfico como você-e-e-e, amo-o-o-o ou sooooo;
9. hífen, travessão, barra ou símbolo decorativo de diálogo;
10. catchphrases ou termos protegidos alterados.

Termos/catchphrases protegidos incluem:
Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

GÊNERO:

Se speaker/contexto identifica homem, NÃO use feminino.

Se speaker/contexto identifica mulher, NÃO use masculino.

Se não há segurança, use frase natural sem marca de gênero.

speaker é pista oculta e NUNCA pode aparecer na saída.

CONTRATO TEMPORAL:

EN e PT são arrays por cue.

Qualquer correção deve manter exatamente o mesmo número de segmentos.

Cada segmento continua preso ao mesmo cue EN.

Nunca mova conteúdo entre cues.

Não faça mudanças cosméticas pequenas.

Se está correto e natural, não inclua em corrections.

Responda SOMENTE:

{"corrections":[{"g":123,"s":["segmento corrigido"],"why":"motivo curto"}]}

Se não houver correção:

{"corrections":[]}
`;

// ============================================================
// HTTP PARA IAs
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
            .join("");
    }

    return "";
}

function retryAfterMs(
    response,
    fallbackMs
) {
    const header =
        response?.headers?.get(
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
    deadlineAt,
    provider,
    job,
    counter
}) {
    let lastError =
        null;

    for (
        let attempt = 1;
        attempt <=
        AI_REQUEST_MAX_RETRIES;
        attempt++
    ) {
        assertBeforeDeadline(
            deadlineAt
        );

        const remaining =
            remainingBeforeDeadline(
                deadlineAt
            );

        const requestTimeout =
            Math.max(
                1,
                Math.min(
                    timeoutMs,
                    Number.isFinite(
                        remaining
                    )
                        ? remaining
                        : timeoutMs
                )
            );

        const controller =
            new AbortController();

        const timer =
            setTimeout(
                () =>
                    controller.abort(),
                requestTimeout
            );

        try {
            console.log(
                `[${provider}] Request ${attempt}/${AI_REQUEST_MAX_RETRIES}.`
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
                if (job) {
                    job[counter] =
                        Number(
                            job[counter] ||
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
                data?.error?.message ||
                data?.message ||
                raw ||
                `${provider} HTTP ${response.status}`;

            const retryable =
                response.status ===
                    408 ||
                response.status ===
                    429 ||
                response.status >=
                    500;

            if (
                !retryable ||
                attempt ===
                    AI_REQUEST_MAX_RETRIES
            ) {
                throw new Error(
                    `${provider} HTTP ${response.status}: ${String(
                        message
                    ).slice(
                        0,
                        1500
                    )}`
                );
            }

            const wait =
                retryAfterMs(
                    response,
                    4000 *
                        attempt
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
        catch (error) {
            lastError =
                error?.name ===
                    "AbortError"
                    ? new Error(
                        `${provider}: timeout.`
                    )
                    : error;

            if (
                attempt ===
                AI_REQUEST_MAX_RETRIES
            ) {
                throw lastError;
            }

            const message =
                getErrorMessage(
                    lastError
                );

            if (
                /HTTP 4\d\d/i.test(
                    message
                ) &&
                !/HTTP 408|HTTP 429/i.test(
                    message
                )
            ) {
                throw lastError;
            }

            const wait =
                2500 *
                attempt;

            console.warn(
                `[${provider}] ${message.slice(
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

async function mistralTranslate(
    payload,
    deadlineAt,
    job
) {
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
                            TRANSLATOR_SYSTEM_PROMPT
                    },

                    {
                        role:
                            "user",

                        content:
                            `Traduza este lote. JSON de entrada:\n${JSON.stringify(
                                {
                                    groups:
                                        payload
                                }
                            )}`
                    }
                ],

                response_format: {
                    type:
                        "json_object"
                },

                reasoning_effort:
                    "none",

                temperature:
                    0.15,

                max_tokens:
                    MISTRAL_MAX_OUTPUT_TOKENS,

                prompt_cache_key:
                    "stremio-ptbr-v6-translator"
            },

            timeoutMs:
                MISTRAL_TIMEOUT_MS,

            deadlineAt,

            provider:
                "MISTRAL",

            job,

            counter:
                "mistralCalls"
        });

    const text =
        extractProviderText(
            data
                ?.choices
                ?.[0]
                ?.message
                ?.content
        );

    if (!text) {
        throw badModelOutputError(
            "Mistral retornou resposta vazia."
        );
    }

    return text;
}

// ============================================================
// GROQ REVIEWER
// ============================================================

async function groqReview(
    payload,
    deadlineAt,
    job,
    mandatory = false
) {
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
                            REVIEWER_SYSTEM_PROMPT
                    },

                    {
                        role:
                            "user",

                        content:
                            `${
                                mandatory
                                    ? "Quality Guard encontrou riscos. Corrija obrigatoriamente os erros reais destes grupos."
                                    : "Revise integralmente este lote; não mexa no que já está bom."
                            }\nJSON de entrada:\n${JSON.stringify(
                                {
                                    groups:
                                        payload
                                }
                            )}`
                    }
                ],

                response_format: {
                    type:
                        "json_object"
                },

                temperature:
                    0,

                max_completion_tokens:
                    GROQ_REVIEW_MAX_OUTPUT_TOKENS,

                /*
                 * Compound Mini não deve
                 * usar web/code/tools
                 * nesta função.
                 */
                tool_choice:
                    "none",

                citation_options:
                    "disabled"
            },

            timeoutMs:
                GROQ_REVIEW_TIMEOUT_MS,

            deadlineAt,

            provider:
                "GROQ_REVIEW",

            job,

            counter:
                "groqReviewCalls"
        });

    const text =
        extractProviderText(
            data
                ?.choices
                ?.[0]
                ?.message
                ?.content
        );

    if (!text) {
        throw badModelOutputError(
            "Groq reviewer retornou resposta vazia."
        );
    }

    return text;
}

// ============================================================
// TRADUÇÃO ESTRUTURADA
// ============================================================

function validateMistralBatch(
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
        throw badModelOutputError(
            "Mistral retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(
            parsed?.items
        ) ||
        parsed.items.length !==
            groups.length
    ) {
        throw badModelOutputError(
            `Mistral: groups esperados=${groups.length}, recebidos=${parsed?.items?.length ?? "?"}.`
        );
    }

    const map =
        new Map();

    for (
        let i = 0;
        i <
        groups.length;
        i++
    ) {
        const group =
            groups[i];

        const item =
            parsed.items[i];

        if (
            !item ||
            Number(
                item.g
            ) !==
                group.groupId ||
            !Array.isArray(
                item.s
            ) ||
            item.s.length !==
                group.cues.length ||
            item.s.some(
                text =>
                    typeof text !==
                        "string" ||
                    !text.trim()
            )
        ) {
            throw badModelOutputError(
                `Contrato temporal inválido no group ${group.groupId}.`
            );
        }

        map.set(
            group.groupId,

            item.s.map(
                text =>
                    String(
                        text
                    ).trim()
            )
        );
    }

    return map;
}

async function translateGroupBatch(
    groups,
    deadlineAt,
    job,
    depth = 0,
    atomicRetry = 0
) {
    try {
        const raw =
            await mistralTranslate(
                groups.map(
                    compactTranslationGroup
                ),
                deadlineAt,
                job
            );

        return validateMistralBatch(
            groups,
            raw
        );
    }
    catch (error) {
        if (
            error?.code !==
            "BAD_MODEL_OUTPUT"
        ) {
            throw error;
        }

        if (
            groups.length ===
                1 &&
            atomicRetry <
                1
        ) {
            console.warn(
                `[MISTRAL] Repetindo group atômico ${groups[0].groupId}.`
            );

            return translateGroupBatch(
                groups,
                deadlineAt,
                job,
                depth,
                atomicRetry +
                    1
            );
        }

        if (
            groups.length <=
                1 ||
            depth >=
                7
        ) {
            throw error;
        }

        const middle =
            Math.ceil(
                groups.length /
                2
            );

        const left =
            groups.slice(
                0,
                middle
            );

        const right =
            groups.slice(
                middle
            );

        console.warn(
            `[MISTRAL] Lote inválido; split ${groups.length} -> ${left.length}+${right.length}.`
        );

        const a =
            await translateGroupBatch(
                left,
                deadlineAt,
                job,
                depth +
                    1,
                0
            );

        const b =
            await translateGroupBatch(
                right,
                deadlineAt,
                job,
                depth +
                    1,
                0
            );

        return new Map([
            ...a,
            ...b
        ]);
    }
}

// ============================================================
// REVIEW
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
        throw badModelOutputError(
            "Groq reviewer retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(
            parsed
                ?.corrections
        )
    ) {
        throw badModelOutputError(
            "Groq reviewer não retornou corrections[]."
        );
    }

    const allowed =
        new Map(
            batch.map(
                group => [
                    group.groupId,
                    group
                ]
            )
        );

    const seen =
        new Set();

    const accepted = [];

    for (
        const correction
        of parsed.corrections
    ) {
        const groupId =
            Number(
                correction?.g
            );

        const group =
            allowed.get(
                groupId
            );

        if (
            !group ||
            seen.has(
                groupId
            ) ||
            !Array.isArray(
                correction?.s
            ) ||
            correction.s.length !==
                group.cues.length ||
            correction.s.some(
                text =>
                    typeof text !==
                        "string" ||
                    !text.trim()
            )
        ) {
            console.warn(
                `[GROQ REVIEW] Correção inválida ignorada g=${correction?.g}.`
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
            correction.s.map(
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

async function reviewAll(
    groups,
    translations,
    deadlineAt,
    job
) {
    const batches =
        splitByBudget(
            groups,
            REVIEW_BATCH_CHARS,
            REVIEW_BATCH_GROUPS,
            group =>
                compactReviewGroup(
                    group,
                    translations.get(
                        group.groupId
                    )
                )
        );

    const changes = [];

    console.log(
        `[GROQ REVIEW] Revisão completa: ${batches.length} lote(s).`
    );

    for (
        let i = 0;
        i <
        batches.length;
        i++
    ) {
        const batch =
            batches[i];

        const payload =
            batch.map(
                group =>
                    compactReviewGroup(
                        group,
                        translations.get(
                            group.groupId
                        )
                    )
            );

        const raw =
            await groqReview(
                payload,
                deadlineAt,
                job,
                false
            );

        const accepted =
            parseReviewerCorrections(
                raw,
                batch,
                translations
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

        job.progress =
            Math.max(
                job.progress,
                72 +
                    Math.round(
                        (
                            (
                                i +
                                1
                            ) /
                            batches.length
                        ) *
                        23
                    )
            );

        job.updatedAt =
            Date.now();
    }

    return changes;
}

// ============================================================
// LIMPEZA FINAL / QUALITY GUARD
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
            .split("\n")
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
                     * Sem - fala
                     * Sem — fala
                     * Sem / fala
                     */
                    clean =
                        clean.replace(
                            /^\s*[-–—/]+\s*(?=\S)/u,
                            ""
                        );

                    clean =
                        clean
                            .replace(
                                /[♪♫♬]+/gu,
                                ""
                            )
                            .replace(
                                /[ \t]{2,}/g,
                                " "
                            )
                            .trim();

                    return clean;
                }
            )
            .filter(Boolean)
            .join("\n");

    /*
     * Slash usado como divisor
     * artificial de dois falantes
     * vira quebra limpa de linha.
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

    return text;
}

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
            /empolgado\(a\)|empolgada\(o\)|\bele\/ela\b|\bela\/ele\b/i.test(
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
            /Family that slays/i.test(
                source
            ) &&
            /Família que mata/i.test(
                text
            )
        ) {
            reasons.push(
                "SLAY_LITERAL"
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

                reasons,

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

function cleanAllFinal(
    blocks,
    texts
) {
    return texts.map(
        (
            text,
            i
        ) =>
            applyProtectedRules(
                blocks[i]
                    .text,

                cleanFinalText(
                    text,
                    blocks[i]
                        .speakerHint
                )
            )
    );
}

async function targetedRiskRepair(
    blocks,
    groups,
    translations,
    issues,
    deadlineAt,
    job
) {
    if (
        !issues.length
    ) {
        return [];
    }

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

    const wanted =
        new Set(
            issues
                .map(
                    issue =>
                        cueToGroup.get(
                            issue.id
                        )
                )
                .filter(
                    Boolean
                )
        );

    const riskyGroups =
        groups.filter(
            group =>
                wanted.has(
                    group.groupId
                )
        );

    if (
        !riskyGroups.length
    ) {
        return [];
    }

    console.warn(
        `[QUALITY GUARD] ${issues.length} risco(s) em ${riskyGroups.length} group(s); reparo localizado.`
    );

    const payload =
        riskyGroups.map(
            group =>
                compactReviewGroup(
                    group,
                    translations.get(
                        group.groupId
                    )
                )
        );

    const raw =
        await groqReview(
            payload,
            deadlineAt,
            job,
            true
        );

    const accepted =
        parseReviewerCorrections(
            raw,
            riskyGroups,
            translations
        );

    for (
        const change
        of accepted
    ) {
        translations.set(
            change.groupId,
            change.after
        );
    }

    return accepted;
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

    const batches =
        splitByBudget(
            groups,
            MISTRAL_BATCH_CHARS,
            MISTRAL_BATCH_GROUPS,
            compactTranslationGroup
        );

    const deadlineAt =
        job.deadlineAt;

    const startedAt =
        Date.now();

    const translations =
        new Map();

    job.sentenceGroups =
        groups.length;

    job.totalBatches =
        batches.length;

    job.completedBatches =
        0;

    job.progress =
        1;

    console.log(
        `[PIPELINE 6.0] ${blocks.length} cues -> ${groups.length} Sentence Groups -> ${batches.length} lote(s) Mistral.`
    );

    for (
        let i = 0;
        i <
        batches.length;
        i++
    ) {
        assertBeforeDeadline(
            deadlineAt
        );

        console.log(
            `[MISTRAL] Lote ${i + 1}/${batches.length}: ${batches[i].length} group(s).`
        );

        const result =
            await translateGroupBatch(
                batches[i],
                deadlineAt,
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

        job.completedBatches =
            i +
            1;

        job.progress =
            Math.round(
                (
                    (
                        i +
                        1
                    ) /
                    batches.length
                ) *
                70
            );

        job.updatedAt =
            Date.now();
    }

    if (
        translations.size !==
        groups.length
    ) {
        throw new Error(
            `Tradução incompleta: ${translations.size}/${groups.length} groups.`
        );
    }

    const reviewChanges =
        await reviewAll(
            groups,
            translations,
            deadlineAt,
            job
        );

    job.reviewChanges =
        reviewChanges.length;

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
        const repaired =
            await targetedRiskRepair(
                blocks,
                groups,
                translations,
                risks,
                deadlineAt,
                job
            );

        job.reviewChanges +=
            repaired.length;

        if (
            repaired.length
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
     * Estes quatro tipos jamais
     * podem chegar ao usuário.
     */
    const hard =
        risks.filter(
            issue =>
                issue.reasons.some(
                    reason =>
                        [
                            "SPEAKER_LABEL",
                            "DIALOGUE_MARKER",
                            "VOCAL_ELONGATION",
                            "ARTIFICIAL_GENDER"
                        ].includes(
                            reason
                        )
                )
        );

    if (
        hard.length
    ) {
        throw new Error(
            `Quality Guard bloqueou ${hard.length} cue(s) por speaker/marcador/alongamento/gênero artificial.`
        );
    }

    if (
        risks.length
    ) {
        console.warn(
            `[QUALITY GUARD] ${risks.length} alerta(s) não estrutural(is): ${risks
                .slice(
                    0,
                    15
                )
                .map(
                    item =>
                        `${item.id}:${item.reasons.join(
                            "+"
                        )}`
                )
                .join(
                    ", "
                )}`
        );
    }
    else {
        console.log(
            "[QUALITY GUARD] PASSOU — 0 padrão(s) conhecido(s) restante(s)."
        );
    }

    const finalSrt =
        buildSrt(
            blocks,
            texts
        );

    auditFinalTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL 6.0"
    );

    job.timestampAuditPassed =
        true;

    job.contentAuditPassed =
        true;

    job.qualityGuardRisks =
        risks.length;

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
        `[PIPELINE 6.0] OK em ${elapsed.toFixed(
            1
        )}s | Mistral=${job.mistralCalls} | GroqReview=${job.groqReviewCalls} | correções=${job.reviewChanges} | riscos finais=${risks.length}.`
    );

    return finalSrt;
}

// ============================================================
// FILA DE JOBS
// ============================================================

async function processJob(
    job
) {
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
         * Cache recebe somente
         * versão final revisada.
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
    catch (error) {
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
                `[JOB QUEUE] ${job.id} entrou na fila; aguardando=${translationJobQueue.length}.`
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

    const cached =
        getTranslationCache(
            cacheKey
        );

    if (cached) {
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

    let job =
        findProcessingJob(
            cacheKey
        );

    if (job) {
        return job;
    }

    const jobId =
        `job-${sourceHash.slice(
            0,
            24
        )}-${randomId(6)}`;

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

    job.promise =
        enqueueTranslationJob(
            job
        ).catch(
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
// OPEN SUBTITLES - RELEASE AWARE
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
    let score = 0;

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
                sub => {
                    const lang =
                        String(
                            sub?.lang ||
                            ""
                        ).toLowerCase();

                    return (
                        (
                            lang ===
                                "eng" ||
                            lang ===
                                "en"
                        ) &&
                        typeof sub?.url ===
                            "string" &&
                        /^https?:\/\//i.test(
                            sub.url
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
                        "Stremio-PTBR-DualAI/6.0"
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
                        "Stremio-PTBR-DualAI/6.0"
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

    if (!raw) {
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

    if (!clean) {
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
            req.params.extra ||
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
// RESPOSTAS SRT
// ============================================================

function sendSubtitleResponse(
    res,
    srt,
    cacheControl =
        "no-store"
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
        String(
            srt ||
            ""
        )
    );
}

function buildProcessingSrt(
    job
) {
    return [
        "1",

        "00:00:01,000 --> 00:00:06,000",

        "Traduzindo e revisando legenda...",

        "",

        "2",

        "00:00:06,500 --> 00:00:12,000",

        `Progresso: ${Number(
            job?.progress ||
            0
        )}%. Aguarde alguns instantes.`
    ].join("\n");
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
    ].join("\n");
}

// ============================================================
// STREMIO
// ============================================================

const manifest = {
    /*
     * Mantido para não
     * duplicar o addon
     * já instalado.
     */
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "6.0.0",

    name:
        "Tradutor PT-BR Premium",

    description:
        "Traduz com Mistral Medium 3.5, revisa com Groq Compound Mini e preserva os timestamps da fonte.",

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
                sentenceGroups:
                    true,

                secondAiReview:
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
            `[STREMIO] filename=${extra.filename || "(não enviado)"}; videoSize=${extra.videoSize || "(não enviado)"}; videoHash=${extra.videoHash || "(não enviado)"}`
        );

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
    catch (error) {
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
// PROTOCOLO COMPATÍVEL
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

            if (
                !cleanedSrt ||
                !parseSrt(
                    cleanedSrt
                ).length
            ) {
                throw new Error(
                    "Legenda embutida vazia/inválida após limpeza."
                );
            }

            console.log(
                `[EMBEDDED API] ${type}/${videoId} | ${sourceName} | ${parseSrt(
                    cleanedSrt
                ).length} cues.`
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
        catch (error) {
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
// RESULTADO / STATUS DO JOB
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

        if (!job) {
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

                sentenceGroups:
                    job.sentenceGroups ||
                    0,

                mistralCalls:
                    job.mistralCalls ||
                    0,

                groqReviewCalls:
                    job.groqReviewCalls ||
                    0,

                reviewChanges:
                    job.reviewChanges ||
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
// MANUTENÇÃO
// ============================================================

setInterval(
    () => {
        pruneMapByTime(
            translationCache,
            CACHE_TTL_MS,
            MAX_CACHE_ENTRIES
        );

        pruneMapByTime(
            jobs,
            JOB_TTL_MS,
            MAX_JOBS
        );
    },
    10 * 60 * 1000
).unref();

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
            " STREMIO PT-BR DUAL AI TRANSLATOR 6.0"
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
            `Revisor independente: ${GROQ_REVIEW_MODEL}`
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
            `Groq review batch: ${REVIEW_BATCH_GROUPS} groups / ${REVIEW_BATCH_CHARS} chars`
        );

        console.log(
            "Sentence Groups + segmentação temporal: ATIVA ✅"
        );

        console.log(
            "Segunda IA revisando EN↔PT-BR: ATIVA ✅"
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
