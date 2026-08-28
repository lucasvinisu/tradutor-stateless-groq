const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// STREMIO PT-BR BACKEND 6.5 — QUALITY + SPEED
// ============================================================
// - Ponte Local é a única interface visível no Stremio.
// - Embedded inglesa é a autoridade temporal quando existir.
// - Mistral Medium 3.5 traduz UMA vez, em fila serial rápida.
// - Gemini Flash-Lite audita em microblocos EM PARALELO.
// - Só groups realmente suspeitos voltam ao Mistral.
// - Sem árbitro global, sem pacer inventado, sem teto global.
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

const CACHE_VERSION = "6.5.0-final";

const MAX_SOURCE_CHARS = 800000;
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS = 25000;

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
    7
);

const GEMINI_MAX_RETRIES = Number(
    process.env.GEMINI_MAX_RETRIES ||
    6
);

/*
 * Com a quota gratuita observada:
 *
 * DUAS requests Mistral ao mesmo tempo geravam
 * uma tempestade de 429.
 *
 * Uma request por vez tende a ser MAIS RÁPIDA no total,
 * porque a próxima sai imediatamente quando a anterior termina.
 *
 * Não existe sleep artificial entre requests.
 */
const MISTRAL_BATCH_CHARS = Number(
    process.env.MISTRAL_BATCH_CHARS ||
    19000
);

const MISTRAL_BATCH_GROUPS = Number(
    process.env.MISTRAL_BATCH_GROUPS ||
    240
);

const GEMINI_AUDIT_GROUPS = Number(
    process.env.GEMINI_AUDIT_GROUPS ||
    32
);

const GEMINI_AUDIT_CHARS = Number(
    process.env.GEMINI_AUDIT_CHARS ||
    10500
);

const REPAIR_BATCH_GROUPS = Number(
    process.env.REPAIR_BATCH_GROUPS ||
    20
);

const REPAIR_BATCH_CHARS = Number(
    process.env.REPAIR_BATCH_CHARS ||
    9000
);

// ============================================================
// STATE
// ============================================================

const translationCache = new Map();
const jobs = new Map();
const queue = [];

let queueRunning = false;

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

function makeCacheKey(
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
        translationCache.delete(key);
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
            makeCacheKey(
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

            mistralRetryWaitMs:
                0,

            structuralSingleCueJoins:
                0,

            salvageGroups:
                0,

            rescueCalls:
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

function getOrCreateJob(args) {
    const key =
        makeCacheKey(
            args.type,
            args.videoId,
            args.sourceSrt
        );

    const cached =
        getCache(key);

    if (cached) {
        let job =
            findJobByCache(
                key,
                ["completed"]
            );

        if (!job) {
            job =
                createJob(args);

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
            ["processing"]
        );

    if (active) {
        return active;
    }

    const done =
        findJobByCache(
            key,
            ["completed"]
        );

    if (done) {
        return done;
    }

    const job =
        createJob(args);

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
                jobs.delete(id);
            }
        }
    },

    10 * 60 * 1000
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
                        bracket[0].length
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
                        colon[0].length
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
            .split(/\n{2,}/)
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
            speakers.size ===
            1
        ) {
            const speaker =
                [...speakers][0];

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
        Number(match[1]) *
            3600 +
        Number(match[2]) *
            60 +
        Number(match[3]) +
        Number(match[4]) /
            1000
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
                builder(item)
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
Você é o tradutor principal EN→PT-BR de legendas de entretenimento em 2026.

OBJETIVO:
português brasileiro natural, oral, atual, conciso, fiel e culturalmente inteligente.

Qualidade e sincronização semântica por cue são obrigatórias.

NUNCA soe:
literal,
engessado,
antiquado,
lusitano,
burocrático
ou com "gíria de tiozão".

Também NÃO enfie internetês ou gíria jovem sem contexto.

Gen Z/Alpha, drag, LGBTQIA+, reality, moda, música,
memes e cultura pop devem soar como brasileiros reais
daquele universo.

REGRAS EDITORIAIS CONCRETAS:

- "bitch" como VOCATIVO amigável entre queens/amigas
  pode ser "bicha", "gata", "amiga", "menina"
  ou pode ser omitido conforme tom.
  NÃO use "puta" automaticamente.

- "bitch" como insulto pode pedir "vadia",
  "desgraçada" etc.
  Decida pelo contexto.

- "I'm gagged" / "gagged" em reação:
  "Tô passada",
  "Tô muito passada",
  "Tô em choque",
  "Tô sem reação".
  NUNCA "amordaçada".

- "she ate":
  "ela arrasou",
  "entregou tudo",
  "serviu demais"
  etc. quando for gíria.

- "no crumbs":
  "não deixou nada pra ninguém"
  quando fizer sentido.

- "motherfucking/fucking" é intensificador.
  Preserve a força de modo brasileiro.
  NÃO produza:
  "competição da porra",
  "competição do caralho",
  "lip sync da porra",
  "lip sync do caralho",
  "cheque da porra",
  "cheque do caralho".

- "fucking lip sync":
  pode ser
  "um puta lip sync",
  "um lip sync foda",
  "um lip sync absurdo"
  conforme contexto.

- "supportive":
  prefira
  "sempre me apoiou muito",
  "sempre esteve do meu lado"
  etc.
  Evite "super apoiador".

- no Drag Race,
  "judges" = "jurados",
  não "juízes".

- "the judgers are now the judgees":
  "agora quem julgava vai ser julgado"
  ou
  "agora os jurados é que vão ser julgados".
  NÃO:
  "os juízes viraram os julgados".

- "plucking pussy hairs":
  preserve significado e vulgaridade.
  Exemplo:
  "catar pelo de xereca",
  "arrancar pelo de xereca".
  NUNCA transforme em
  "fio de bigode".

- vitória compartilhada / double win
  NÃO é "empate duplo".
  Use:
  "vitória dupla",
  "as duas ganharam"
  ou formulação equivalente ao sentido real.

- "week one" =
  "primeira semana".

- "off the top" sobre dinheiro =
  comissão/corte/porcentagem.

- "closing ranks" =
  grupo se protegendo/panelinha.

- "Carry the two" em conta =
  "vai dois".

- evite "apoiante".

- "The talent performers"
  não é
  "As artistas do talento".

PRESERVE quando presentes:

Werkroom
Condragulations
Shantay, you stay
Sashay away
You betta werk
Racers, start your engines

PALAVRÃO:
não censure.
Preserve intensidade,
mas posicione como brasileiro realmente fala.

GÊNERO:
speaker é contexto oculto.
Use quando seguro.
Se não for seguro,
reformule naturalmente.

Nunca escreva:

empolgado(a)
animado(a)
ele/ela
ela/ele

FORMATAÇÃO:

sem nomes/labels de falante
sem [NOME]
sem NOME:
sem barra "/" como separador
sem hífen/travessão decorativo
sem SDH/CC
sem alongamentos gráficos

CUE LOCK ABSOLUTO:

Você recebe Sentence Groups para entender contexto,
mas CADA cue tem um id "i".

Devolva exatamente UM "pt" para CADA "i" recebido,
com o MESMO "i".

Não resuma.

Não omita final de frase/raciocínio.

Não mova fala para outro cue.

Se a frase atravessa vários cues,
use o contexto para naturalidade,
mas mantenha cada parte semanticamente alinhada
ao cue onde é falada.

Responda SOMENTE JSON válido:

{
  "groups": [
    {
      "g": 1,
      "c": [
        {"i": 10, "pt": "..."},
        {"i": 11, "pt": "..."}
      ]
    }
  ]
}
`;

const RESCUE_PROMPT = `
Você está corrigindo estrutura de uma tradução EN→PT-BR.

Mantenha a MESMA qualidade editorial:

natural
atual
não literal
completa
fiel

Cada cue "i" deve voltar exatamente uma vez,
com seu próprio "pt".

Não omita conteúdo.

Não mova conteúdo entre cues.

speaker é contexto oculto
e nunca aparece.

Sem labels.
Sem barras.
Sem marcadores de diálogo.
Sem alongamentos.

Responda SOMENTE JSON:

{
  "groups": [
    {
      "g": 1,
      "c": [
        {"i": 10, "pt": "..."}
      ]
    }
  ]
}
`;

const GEMINI_AUDIT_PROMPT = `
Você é um AUDITOR editorial independente
de legendas EN→PT-BR em 2026.

NÃO traduza o episódio inteiro.

NÃO faça reescrita cosmética.

Analise CADA group recebido,
comparando EN x PT cue por cue.

Para CADA group,
devolva obrigatoriamente um veredito:

"ok"
=
tradução correta,
completa,
natural
e bem alinhada aos cues.

"fix"
=
existe problema REAL
que merece retradução.

Marque fix por qualquer um destes motivos:

SEMANTIC
=
sentido errado ou referência errada.

OMISSION
=
conteúdo ou fim do raciocínio sumiu.

CUE_SYNC
=
conteúdo foi antecipado ou atrasado
entre cues.

LITERAL
=
inglês vestido de português.

REGISTER
=
português antiquado,
duro,
tiozão
ou gíria artificial.

CULTURE
=
drag,
reality,
LGBTQIA+,
Gen Z/Alpha,
meme,
moda,
música
ou cultura pop
mal adaptados.

PROFANITY
=
palavrão censurado
ou colocado de maneira
que um brasileiro não falaria.

GENDER
=
concordância/gênero inadequado.

FORMAT
=
label,
barra,
"--" cru,
marcador
ou ruído visual.

CRITÉRIOS OBRIGATÓRIOS:

- "bitch" como vocativo amigável
  pode ser
  bicha/gata/amiga/menina.
  Não "puta" automaticamente.

- motherfucking/fucking
  é intensificador.
  "competição da porra",
  "lip sync da porra",
  "cheque da porra"
  são sinais de literalidade ruim.

- supportive
  não deve virar
  "super apoiador"
  se
  "sempre me apoiou"
  ou
  "esteve do meu lado"
  for mais natural.

- judges em Drag Race =
  jurados.

- judgers/judgees
  não deve virar
  "juízes viraram os julgados".

- plucking pussy hairs
  deve preservar pussy hairs
  e vulgaridade.
  "fio de bigode"
  é erro grave.

- double win/shared win
  não é
  "empate duplo".

- gagged em reação:
  "Tô passada",
  "Tô muito passada",
  "Tô em choque"
  são soluções naturais.

Não invente defeitos.

Se estiver bom,
marque "ok".

Para "fix",
inclua "reasons"
e um "hint" curto
explicando COMO corrigir,
sem escrever uma tradução longa.

Responda SOMENTE JSON
e inclua EXATAMENTE
um item por group recebido:

{
  "items": [
    {"g": 1, "v": "ok"},
    {
      "g": 2,
      "v": "fix",
      "reasons": ["LITERAL", "CULTURE"],
      "hint": "bitch é vocativo amigável; use bicha/gata conforme tom"
    }
  ]
}
`;

const REPAIR_PROMPT = `
Você é o editor final EN→PT-BR.

Recebe apenas Sentence Groups
que uma auditoria marcou
como problemáticos.

Para cada group,
considere:

EN original
PT atual
reasons
hint
speaker oculto

RETRADUZA somente o necessário
para eliminar o problema,
mantendo:

naturalidade
contemporaneidade
fidelidade
vulgaridade
contexto cultural
sincronização semântica

STYLE PACK OBRIGATÓRIO:

- bitch vocativo amigável:
  bicha/gata/amiga/menina
  ou omitir.
  Não "puta" automaticamente.

- motherfucking/fucking:
  intensificador natural.
  Nunca:
  competição da porra/do caralho,
  lip sync da porra/do caralho,
  cheque da porra/do caralho.

- supportive:
  "sempre me apoiou",
  "esteve do meu lado"
  etc.
  Não "super apoiador".

- judges no Drag Race:
  jurados.

- judgers/judgees:
  "quem julgava vai ser julgado"
  ou
  "os jurados é que vão ser julgados".

- plucking pussy hairs:
  preserve pelo de xereca
  ou equivalente vulgar.
  Nunca fio de bigode.

- double win/shared win:
  vitória dupla/as duas ganharam.
  Nunca "empate duplo"
  quando não existe empate.

- gagged:
  Tô passada,
  Tô muito passada,
  Tô em choque,
  conforme contexto.

- preserve:
  Werkroom
  Condragulations
  Shantay, you stay
  Sashay away
  You betta werk

CUE LOCK:

devolva exatamente
um "pt" para cada cue "i".

Mesmo "i".

Sem omissão.

Sem mover fala entre cues.

speaker é contexto oculto.

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
        {"i": 10, "pt": "..."}
      ]
    }
  ]
}
`;

// ============================================================
// MISTRAL — UMA FILA, SEM PACER ARTIFICIAL
// ============================================================

let mistralLane =
    Promise.resolve();

let mistralCooldownUntil =
    0;

function withMistralLane(fn) {
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

function dynamicMaxTokens(
    user,
    purpose
) {
    const chars =
        String(user || "")
            .length;

    if (
        purpose ===
        "translation"
    ) {
        return Math.max(
            3200,
            Math.min(
                7000,
                Math.ceil(
                    chars /
                    3.2
                )
            )
        );
    }

    if (
        purpose ===
        "repair"
    ) {
        return Math.max(
            1800,
            Math.min(
                5000,
                Math.ceil(
                    chars /
                    3.0
                )
            )
        );
    }

    return Math.max(
        1200,
        Math.min(
            3500,
            Math.ceil(
                chars /
                3.0
            )
        )
    );
}

function parseWaitHeaderValue(raw) {
    if (!raw) {
        return null;
    }

    const value =
        String(raw)
            .trim()
            .toLowerCase();

    if (
        /^\d+(?:\.\d+)?$/.test(
            value
        )
    ) {
        return Math.max(
            1000,
            Number(value) *
                1000
        );
    }

    const ms =
        value.match(
            /^(\d+(?:\.\d+)?)ms$/
        );

    if (ms) {
        return Math.max(
            250,
            Number(ms[1])
        );
    }

    const sec =
        value.match(
            /^(\d+(?:\.\d+)?)s$/
        );

    if (sec) {
        return Math.max(
            1000,
            Number(sec[1]) *
                1000
        );
    }

    const date =
        Date.parse(raw);

    if (
        Number.isFinite(
            date
        )
    ) {
        return Math.max(
            1000,
            date -
                Date.now()
        );
    }

    return null;
}

function retryWaitMs(
    response,
    attempt
) {
    for (
        const name
        of [
            "retry-after",
            "x-ratelimit-reset",
            "x-ratelimit-reset-tokens",
            "x-ratelimit-reset-requests"
        ]
    ) {
        const parsed =
            parseWaitHeaderValue(
                response
                    ?.headers
                    ?.get(name)
            );

        if (parsed) {
            return Math.min(
                parsed +
                    250,
                90000
            );
        }
    }

    /*
     * Só usado quando a própria API
     * realmente devolveu 429
     * e não informou Retry-After.
     *
     * Evita a sequência inútil
     * 4, 8, 12, 16...
     * que estava gerando dezenas de 429.
     */
    const schedule = [
        12000,
        20000,
        30000,
        40000,
        50000,
        60000,
        70000
    ];

    return schedule[
        Math.min(
            attempt - 1,
            schedule.length - 1
        )
    ];
}

function rateHeaderSummary(response) {
    const out =
        [];

    try {
        for (
            const [
                key,
                value
            ]
            of response.headers.entries()
        ) {
            const name =
                key.toLowerCase();

            if (
                name.startsWith(
                    "x-ratelimit"
                ) ||
                name ===
                    "retry-after"
            ) {
                out.push(
                    `${key}=${value}`
                );
            }
        }
    }
    catch {}

    return out.join(
        " | "
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
                        "text"
            )
            .map(
                item =>
                    item.text ||
                    ""
            )
            .join("");
    }

    return "";
}

async function mistralChat({
    system,
    user,
    job,
    purpose = "translation",
    reasoning = "none",
    temperature = 0.1
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

                const now =
                    Date.now();

                if (
                    now <
                    mistralCooldownUntil
                ) {
                    await sleep(
                        mistralCooldownUntil -
                        now
                    );
                }

                const controller =
                    new AbortController();

                const timer =
                    setTimeout(
                        () =>
                            controller.abort(),
                        MISTRAL_TIMEOUT_MS
                    );

                try {
                    const max_tokens =
                        dynamicMaxTokens(
                            user,
                            purpose
                        );

                    console.log(
                        `[MISTRAL ${purpose.toUpperCase()}] ` +
                        `Request ${attempt}/${MISTRAL_MAX_RETRIES} | ` +
                        `max_tokens=${max_tokens}.`
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

                                        max_tokens,

                                        prompt_cache_key:
                                            purpose ===
                                            "translation"
                                                ? "stremio-ptbr-6-5-main"
                                                : "stremio-ptbr-6-5-editor"
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

                    const rateInfo =
                        rateHeaderSummary(
                            response
                        );

                    if (rateInfo) {
                        console.log(
                            `[MISTRAL RATE] ${rateInfo}`
                        );
                    }

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

                        if (!text) {
                            throw new Error(
                                "Mistral retornou resposta vazia."
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
                            retryWaitMs(
                                response,
                                attempt
                            );

                        job.stats.mistralRetryWaitMs +=
                            wait;

                        mistralCooldownUntil =
                            Math.max(
                                mistralCooldownUntil,
                                Date.now() +
                                    wait
                            );

                        if (
                            attempt ===
                            MISTRAL_MAX_RETRIES
                        ) {
                            throw error;
                        }

                        console.warn(
                            `[MISTRAL] 429 real; aguardando ` +
                            `${(wait / 1000).toFixed(
                                1
                            )}s antes de tentar novamente.`
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
                            MISTRAL_MAX_RETRIES
                    ) {
                        throw error;
                    }

                    const wait =
                        Math.min(
                            2500 *
                                attempt,
                            15000
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
                                "MISTRAL: timeout desta request."
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
// PARSER DE TRADUÇÃO POR CUE-ID
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

        /*
         * Compatibilidade/resiliência
         * caso o modelo ainda devolva
         * o formato antigo "s".
         */
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

        /*
         * 1 cue + modelo dividiu em vários pedaços.
         *
         * Não chama outra IA.
         * Mantém TODO o conteúdo no MESMO timestamp.
         */
        if (
            group.cues.length ===
                1 &&
            Array.isArray(
                cueItems
            ) &&
            cueItems.length >
                1 &&
            cueItems.every(
                item =>
                    typeof (
                        item?.pt ??
                        item?.text
                    ) ===
                    "string"
            )
        ) {
            const joined =
                cueItems
                    .map(
                        item =>
                            String(
                                item.pt ??
                                item.text
                            ).trim()
                    )
                    .filter(Boolean)
                    .join("\n");

            if (joined) {
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

                job
                    .stats
                    .structuralSingleCueJoins++;

                console.log(
                    `[STRUCTURAL FIX] ` +
                    `g${groupId}: oversplit unido ` +
                    `no mesmo cue ${group.cues[0].index}.`
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
            const item
            of cueItems
        ) {
            const id =
                Number(
                    item?.i ??
                    item?.id
                );

            const pt =
                String(
                    item?.pt ??
                    item?.text ??
                    ""
                ).trim();

            if (
                !id ||
                !pt ||
                byId.has(id)
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

        if (bad) {
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

            if (!pt) {
                bad =
                    true;

                break;
            }

            segments.push(
                pt
            );
        }

        if (bad) {
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
                .join(",")}`
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
                    ? "Corrija a estrutura"
                    : "Traduza"
                } destes Sentence Groups:\n` +
                `${JSON.stringify({
                    groups:
                        groups.map(
                            compactGroup
                        )
                })}`,

            job,

            purpose:
                rescue
                    ? "rescue"
                    : "translation",

            reasoning:
                "none",

            temperature:
                rescue
                    ? 0
                    : 0.12
        });

    return parseGroupResponse(
        groups,
        raw,
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
            6500,
            12,
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
            const raw =
                await mistralChat({
                    system:
                        RESCUE_PROMPT,

                    user:
                        `Corrija SOMENTE este group. ` +
                        `Preserve todos os cues por id:\n` +
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
                        0
                });

            const one =
                parseGroupResponse(
                    [group],
                    raw,
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
                8
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

    for (
        const [
            groupId,
            segments
        ]
        of rescued
    ) {
        result.set(
            groupId,
            segments
        );
    }

    return result;
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

async function geminiRaw(
    groups,
    translations,
    job
) {
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

                if (!text) {
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

                const wait =
                    retryWaitMs(
                        response,
                        attempt
                    );

                if (
                    attempt ===
                    GEMINI_MAX_RETRIES
                ) {
                    throw error;
                }

                console.warn(
                    `[GEMINI AUDIT] 429; ` +
                    `aguardando ${(wait / 1000).toFixed(
                        1
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
                    2000 *
                        attempt,
                    12000
                )
            );
        }
        catch (error) {
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
        new Map(
            groups.map(
                group => [
                    group.groupId,
                    group
                ]
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
            const reasons =
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
                    ];

            flags.set(
                groupId,
                {
                    reasons,

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
    catch (error) {
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
                `[GEMINI AUDIT] split ${groups.length} ` +
                `por ${errorMessage(
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
                    depth + 1
                );

            const right =
                await auditMicroBatch(
                    groups.slice(
                        middle
                    ),
                    translations,
                    job,
                    depth + 1
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
// FINAL CLEAN + KNOWN-RISK DETECTION
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
     * Remove separador visual:
     *
     * "fala 1 / fala 2"
     * ->
     * duas linhas limpas.
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
                        String(line || "")
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
                            .trim();

                    return clean;
                }
            )
            .filter(Boolean)
            .join("\n")
            .trim();

    return value;
}

function applySafeFixes(
    source,
    target
) {
    let text =
        String(target || "");

    const before =
        text;

    /*
     * Apenas correções 100% seguras,
     * sem tentar "reescrever estilo"
     * por regex.
     */
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
        String(source || "");

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

    return {
        text,

        changed:
            text !==
            before
    };
}

function wordCount(text) {
    return (
        String(text || "")
            .match(
                /[\p{L}\p{N}]+(?:['’][\p{L}\p{N}]+)?/gu
            ) ||
        []
    ).length;
}

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
            .join(" ");

    const pt =
        segments.join(" ");

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

    /*
     * Só marca para revisão contextual.
     * Não substitui automaticamente,
     * porque "bitch" pode ser insulto de verdade.
     */
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

    /*
     * Detector conservador de possível omissão.
     * Não condena traduções naturalmente mais curtas.
     */
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
    hint = ""
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
                String(reason)
            );
    }

    if (hint) {
        current
            .hints
            .push(
                String(hint)
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
    const out =
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
                out,
                group.groupId,
                reasons,
                "Aplique o Style Pack e preserve todo o sentido do EN no mesmo cue."
            );
        }
    }

    return out;
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
                .join(" | ")
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
                            )[index],

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

async function repairIssueMap(
    groups,
    translations,
    issueMap,
    job,
    high = false
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
        const raw =
            await mistralChat({
                system:
                    REPAIR_PROMPT,

                user:
                    `Corrija estes groups marcados pela auditoria:\n` +
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
                    })}`,

                job,

                purpose:
                    "repair",

                reasoning:
                    high
                        ? "high"
                        : "none",

                temperature:
                    0
            });

        const parsed =
            parseGroupResponse(
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
        ) => {
            let output =
                cleanFinalText(
                    text
                );

            output =
                applySafeFixes(
                    blocks[index].text,
                    output
                ).text;

            return output;
        }
    );
}

function writeCleanBackToTranslations(
    blocks,
    groups,
    translations,
    cleanedTexts
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
                    cleanedTexts[
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
            MISTRAL_BATCH_CHARS,
            MISTRAL_BATCH_GROUPS,
            compactGroup
        );

    const translations =
        new Map();

    /*
     * Gemini é independente do lane Mistral.
     *
     * Assim ele revisa o lote já pronto
     * enquanto o Mistral traduz o próximo.
     */
    const auditIssues =
        new Map();

    let auditTail =
        Promise.resolve();

    console.log(
        `[PIPELINE 6.5] ` +
        `fonte=${job.sourceKind} | ` +
        `${blocks.length} cues -> ` +
        `${groups.length} groups -> ` +
        `${batches.length} lote(s) Mistral SERIAL.`
    );

    function scheduleAudit(
        translatedBatch
    ) {
        const micros =
            splitByBudget(
                translatedBatch,
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

    /*
     * MISTRAL PRINCIPAL:
     *
     * propositalmente uma request por vez.
     *
     * Não há intervalo artificial.
     * Acabou uma -> começa a seguinte.
     */
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

    let primaryTexts =
        cleanAll(
            blocks,

            flattenTranslations(
                blocks,
                groups,
                translations
            )
        );

    writeCleanBackToTranslations(
        blocks,
        groups,
        translations,
        primaryTexts
    );

    const checkpointSrt =
        buildSrt(
            blocks,
            primaryTexts
        );

    auditTimestamps(
        sourceSrt,
        checkpointSrt,
        "CHECKPOINT MISTRAL"
    );

    /*
     * Normalmente o Gemini já terminou
     * ou está muito perto porque revisou
     * enquanto Mistral traduzia.
     */
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

    /*
     * Une:
     *
     * 1. defeitos que Gemini detectou;
     * 2. erros conhecidos do Style Pack;
     * 3. provável omissão extrema.
     */
    const deterministic =
        deterministicIssueMap(
            groups,
            translations,
            job
        );

    mergeIssueMaps(
        auditIssues,
        deterministic
    );

    console.log(
        `[QUALITY MAP] Gemini+guard => ` +
        `${auditIssues.size} group(s) ` +
        `para retradução dirigida.`
    );

    if (
        auditIssues.size
    ) {
        /*
         * Não existe árbitro global.
         *
         * O Mistral recebe SOMENTE
         * o que realmente foi marcado.
         */
        await repairIssueMap(
            groups,
            translations,
            auditIssues,
            job,
            false
        );

        /*
         * Reaudita SOMENTE o reparado.
         *
         * Não revisa os 1150 outra vez.
         */
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

        const guard2 =
            deterministicIssueMap(
                repairedGroups,
                translations,
                job
            );

        mergeIssueMaps(
            secondIssues,
            guard2
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

    let finalTexts =
        cleanAll(
            blocks,

            flattenTranslations(
                blocks,
                groups,
                translations
            )
        );

    writeCleanBackToTranslations(
        blocks,
        groups,
        translations,
        finalTexts
    );

    /*
     * Não derruba um episódio inteiro
     * por uma regra estilística residual.
     *
     * Estrutura/timestamp, sim, continuam
     * sendo invariantes rígidas.
     */
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
            `${residual.size} group(s) ` +
            `ainda sinalizado(s) após dois reparos; ` +
            `episódio NÃO será bloqueado.`
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
            finalTexts
        );

    auditTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL 6.5"
    );

    const elapsed =
        (
            Date.now() -
            startedAt
        ) /
        1000;

    console.log(
        `[PIPELINE 6.5] OK em ${elapsed.toFixed(
            1
        )}s | ` +
        `fonte=${job.sourceKind} | ` +
        `MistralCalls=${job.stats.mistralCalls} | ` +
        `Attempts=${job.stats.mistralAttempts} | ` +
        `429=${job.stats.mistral429} | ` +
        `RetryWait=${(
            job.stats.mistralRetryWaitMs /
            1000
        ).toFixed(1)}s | ` +
        `SingleCueJoin=${job.stats.structuralSingleCueJoins} | ` +
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

async function processJob(job) {
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
            `[JOB ${job.id}] Falhou: ` +
            `${job.error}`
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
    timeoutMs = FETCH_TIMEOUT_MS
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
                    score(b) -
                    score(a)
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
                        "Stremio-PTBR/6.5"
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
            "OpenSubtitles não encontrou inglês utilizável."
        );
    }

    const subtitleResponse =
        await fetchWithTimeout(
            target.url,
            {
                headers: {
                    "User-Agent":
                        "Stremio-PTBR/6.5"
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

    if (!clean) {
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
        "6.5.0",

    name:
        "Tradutor PT-BR Backend",

    description:
        "Backend-only: Mistral principal + Gemini micro-audit + reparo dirigido.",

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

            mistralMode:
                "SERIAL_NO_ARTIFICIAL_PACER",

            geminiAuditGroups:
                GEMINI_AUDIT_GROUPS,

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
        "[STREMIO PUBLIC] Backend-only: 0 legendas; use Ponte Local."
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

                stats:
                    job.stats
            }
        );
    }
);

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
            " STREMIO PT-BR BACKEND 6.5 FINAL"
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
            "Render no menu do Stremio: DESATIVADO — BACKEND ONLY ✅"
        );

        console.log(
            "Mistral principal: SERIAL, sem concorrência TPM autodestrutiva ✅"
        );

        console.log(
            "Pacer artificial de 23500 TPM: REMOVIDO ✅"
        );

        console.log(
            "429: somente backoff quando a própria API realmente devolver 429 ✅"
        );

        console.log(
            "max_tokens: DINÂMICO por request ✅"
        );

        console.log(
            `Gemini micro-audit: ATÉ ${GEMINI_AUDIT_GROUPS} groups por chamada ✅`
        );

        console.log(
            "Gemini: veredito obrigatório EN×PT para CADA group ✅"
        );

        console.log(
            "Mistral árbitro global: REMOVIDO ✅"
        );

        console.log(
            "Retradução Mistral: SOMENTE groups sinalizados ✅"
        );

        console.log(
            "Reauditoria: SOMENTE groups reparados ✅"
        );

        console.log(
            "Style Pack Drag/Reality/Gen Z/Alpha 2026: ATIVO ✅"
        );

        console.log(
            "bitch vocativo → bicha/gata/amiga conforme contexto; puta automática PROIBIDA ✅"
        );

        console.log(
            "competição/lip sync/cheque 'da porra': BLOQUEADOS PELO GUARD ✅"
        );

        console.log(
            "supportive→super apoiador: BLOQUEADO ✅"
        );

        console.log(
            "judgers→juízes viraram os julgados: BLOQUEADO ✅"
        );

        console.log(
            "plucking pussy hairs→fio de bigode: BLOQUEADO ✅"
        );

        console.log(
            "empate duplo: BLOQUEADO ✅"
        );

        console.log(
            "Cue-ID lock + timestamps imutáveis: ATIVOS ✅"
        );

        console.log(
            "SRT temporário: backend pode expor status, Ponte nunca serve ao player ✅"
        );

        console.log(
            "Teto global do episódio: NÃO EXISTE ✅"
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
