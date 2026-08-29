const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));

// ============================================================
// STREMIO PT-BR 8.1 CLOUD + EMBEDDED API
// Gemini 3.5 Flash-Lite
// ============================================================

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const LOCAL_BRIDGE_SECRET = String(process.env.LOCAL_BRIDGE_SECRET || "").trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || "").trim();
const GEMINI_MODEL = "gemini-3.5-flash-lite";

const CACHE_VERSION = "8.1.0";
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_SOURCE_CHARS = 800000;
const FETCH_TIMEOUT_MS = 25000;

// Aproximadamente 14 inícios de request/min.
// O mutex impede dois workers de furarem o intervalo juntos.
const GEMINI_MIN_START_INTERVAL_MS = 4300;

// --------------------
// Planner curto
// --------------------

const PLAN_THINKING = "minimal";
const PLAN_MAX_OUTPUT_TOKENS = 2200;
const PLAN_TIMEOUT_MS = 60000;
const PLAN_RETRIES = 2;
const PLAN_SAMPLE_MAX_CUES = 900;

// --------------------
// Tradução principal
// --------------------

const MAIN_BATCH_MAX_CUES = 90;
const MAIN_BATCH_MAX_CHARS = 18000;
const MAIN_CONCURRENCY = 2;

const CONTEXT_CUES_BEFORE = 8;
const CONTEXT_CUES_AFTER = 8;

const MAIN_THINKING = "low";
const MAIN_MAX_OUTPUT_TOKENS = 22000;
const MAIN_TIMEOUT_MS = 120000;
const MAIN_HTTP_RETRIES = 4;
const MAIN_PARSE_ATTEMPTS = 2;

// --------------------
// Reparo cirúrgico
// --------------------

const REPAIR_ENABLED = true;

const REPAIR_MAX_CUES_TOTAL = 40;
const REPAIR_BATCH_MAX_CUES = 20;

const REPAIR_THINKING = "medium";
const REPAIR_MAX_OUTPUT_TOKENS = 9000;
const REPAIR_TIMEOUT_MS = 90000;

const REPAIR_HTTP_RETRIES = 3;
const REPAIR_PARSE_ATTEMPTS = 2;

// ============================================================
// STATE
// ============================================================

const translationCache = new Map();
const jobs = new Map();

let lastGeminiRequestStart = 0;

// Mutex do rate gate.
// Cada request precisa adquirir este gate antes de iniciar.
let geminiGate = Promise.resolve();

const sleep = ms =>
    new Promise(
        resolve =>
            setTimeout(
                resolve,
                ms
            )
    );

// ============================================================
// GENERIC HELPERS
// ============================================================

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
        .replace(
            /\/+$/,
            ""
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
        String(
            srt ||
            ""
        )
    );
}

function authorized(req) {
    if (!LOCAL_BRIDGE_SECRET) {
        return false;
    }

    const provided =
        Buffer.from(
            String(
                req.headers.authorization ||
                ""
            ).trim()
        );

    const expected =
        Buffer.from(
            `Bearer ${LOCAL_BRIDGE_SECRET}`
        );

    return (
        provided.length ===
            expected.length &&
        crypto.timingSafeEqual(
            provided,
            expected
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
        translationCache.get(
            key
        );

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
    sourceKind,
    lazy = false
}) {
    const sourceHash =
        sha256(
            sourceSrt
        );

    const now =
        Date.now();

    const job = {
        id:
            `job-` +
            `${sourceHash.slice(0, 24)}-` +
            `${randomId()}`,

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
            lazy
                ? "pending"
                : "processing",

        progress:
            lazy
                ? 0
                : 1,

        result:
            null,

        safeDraft:
            null,

        error:
            null,

        started:
            false,

        promise:
            null,

        createdAt:
            now,

        updatedAt:
            now,

        expiresAt:
            now +
            JOB_TTL_MS,

        stats: {
            sourceCues:
                0,

            planCalls:
                0,

            planFailures:
                0,

            mainBatches:
                0,

            mainCalls:
                0,

            mainAttempts:
                0,

            main429:
                0,

            mainParseRetries:
                0,

            localFlags:
                0,

            repairSelected:
                0,

            repairCalls:
                0,

            repairAttempts:
                0,

            repair429:
                0,

            repairParseRetries:
                0,

            repairFailures:
                0,

            pacerWaitMs:
                0,

            inputTokens:
                0,

            outputTokens:
                0,

            thoughtTokens:
                0,

            usedSafeDraftFallback:
                false
        }
    };

    jobs.set(
        job.id,
        job
    );

    return job;
}

function findReusableJob(
    cacheKey
) {
    for (
        const job
        of jobs.values()
    ) {
        if (
            job.cacheKey ===
                cacheKey &&
            [
                "pending",
                "processing",
                "completed"
            ].includes(
                job.status
            )
        ) {
            return job;
        }
    }

    return null;
}

function getOrCreateJob(
    args,
    {
        lazy = false
    } = {}
) {
    const cacheKey =
        makeCacheKey(
            args.type,
            args.videoId,
            args.sourceSrt
        );

    const cached =
        getCache(
            cacheKey
        );

    if (cached) {
        let job =
            findReusableJob(
                cacheKey
            );

        if (!job) {
            job =
                createJob({
                    ...args,
                    lazy:
                        false
                });
        }

        job.status =
            "completed";

        job.progress =
            100;

        job.result =
            cached;

        return job;
    }

    const existing =
        findReusableJob(
            cacheKey
        );

    if (existing) {
        return existing;
    }

    const job =
        createJob({
            ...args,
            lazy
        });

    if (!lazy) {
        startJob(
            job
        );
    }

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
// SRT
// ============================================================

const TIMING_RE =
    /^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/;

const SPEAKER_RE =
    /^@@SPK:([^@]+)@@\s*/u;

const SDH_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough|speaking indistinctly|speaks? indistinctly/i;

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

function looksLikeSpeakerLabel(
    value
) {
    const speaker =
        normalizeSpeaker(
            value
        );

    if (!speaker) {
        return false;
    }

    const parts =
        speaker
            .split(/\s+/)
            .filter(Boolean);

    if (
        !parts.length ||
        parts.length >
            4
    ) {
        return false;
    }

    if (
        /^(?:okay|ok|well|look|listen|so|now|then|actually|basically|because|but|and|or|yes|no|right|wait|hey|wow|girl|bitch|previously|meanwhile|later|earlier|tonight|today|tomorrow)$/i.test(
            speaker
        )
    ) {
        return false;
    }

    const letters =
        speaker.replace(
            /[^A-Za-zÀ-ÿ]/g,
            ""
        );

    const allUpper =
        Boolean(
            letters
        ) &&
        letters ===
            letters.toUpperCase();

    const titleLike =
        parts.every(
            part =>
                /^[A-ZÀ-Ý][A-Za-zÀ-ÿ'’.-]*$/u.test(
                    part
                )
        );

    return (
        allUpper ||
        titleLike
    );
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

        const clean =
            original.replace(
                SPEAKER_RE,
                ""
            );

        return {
            speaker,

            text:
                clean,

            hadDialogueDash:
                /^\s*[-–—]\s*/u.test(
                    clean
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

        if (
            speaker &&
            !SDH_WORDS.test(
                bracket[1]
            )
        ) {
            return {
                speaker,

                text:
                    original.slice(
                        bracket[0].length
                    ),

                hadDialogueDash:
                    /^\s*[-–—]\s*/u.test(
                        original
                    )
            };
        }
    }

    const colon =
        original.match(
            /^\s*([-–—]\s*)?([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,45})\s*:\s+(?=\S)/u
        );

    if (colon) {
        const speaker =
            normalizeSpeaker(
                colon[2]
            );

        if (
            speaker &&
            looksLikeSpeakerLabel(
                colon[2]
            )
        ) {
            return {
                speaker,

                text:
                    original.slice(
                        colon[0].length
                    ),

                hadDialogueDash:
                    Boolean(
                        colon[1]
                    )
            };
        }
    }

    return {
        speaker:
            "",

        text:
            original,

        hadDialogueDash:
            /^\s*[-–—]\s*/u.test(
                original
            )
    };
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

    return /^(?:ah|ha|heh)(?:\s+(?:ah|ha|heh)){1,6}$/.test(
        value
    );
}

function removeSdhSegments(
    text
) {
    return String(
        text ||
        ""
    )
        .replace(
            /\[([^\]]+)\]/gu,

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
        )
        .replace(
            /\(([^)]+)\)/gu,

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

    text =
        removeSdhSegments(
            text
        )
            .replace(
                /<[^>]+>/g,
                ""
            )
            .replace(
                /\{\\[^}]+\}/g,
                " "
            )
            .replace(
                /[♪♫♬]/gu,
                " "
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

            let cleaned =
                cleanSourceLine(
                    info.text
                );

            if (!cleaned) {
                continue;
            }

            if (
                info.hadDialogueDash &&
                !/^\s*[-–—]\s*/u.test(
                    cleaned
                )
            ) {
                cleaned =
                    `- ${cleaned}`;
            }
            else {
                cleaned =
                    cleaned.replace(
                        /^\s*[-–—]\s*/u,
                        "- "
                    );
            }

            dialogue.push(
                cleaned
            );
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
        `[CLEAN] ` +
        `${rawBlocks.length} -> ${out.length}; ` +
        `removidos=${removed}; ` +
        `speakerHints=${speakerHints}.`
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

function parseSrt(
    srt
) {
    const normalized =
        normalizeSrt(
            srt
        );

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
            lines.length <
                3 ||
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
            lines.slice(
                2
            );

        let speakerHint =
            null;

        if (
            textLines.length
        ) {
            const match =
                textLines[0].match(
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
                    textLines[0].replace(
                        SPEAKER_RE,
                        ""
                    );
            }
        }

        result.push({
            index:
                Number(
                    lines[0].trim()
                ),

            timing:
                lines[1].trim(),

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
    translations
) {
    return (
        blocks
            .map(
                block =>
                    [
                        block.index,

                        block.timing,

                        String(
                            translations.get(
                                block.index
                            ) ??
                            block.text
                        ).trim()
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
            throw new Error(
                `TIMING LOCK ${label}: ` +
                `cue ${source[i].index}.`
            );
        }
    }

    console.log(
        `[TIMING LOCK] ${label}: PASSOU — ` +
        `${source.length}/${source.length}; ` +
        `0 alterações.`
    );
}

// ============================================================
// STYLE / CONTEXTO
// ============================================================

const STYLE_PACK = `
PORTUGUÊS BRASILEIRO NATURAL — GUIA EDITORIAL

OBJETIVO

Traduza como legenda profissional brasileira contemporânea:
natural, oral, concisa, contextual e fiel.

Nunca faça "inglês vestido de português".

CONTEXTO CULTURAL E GERACIONAL

- Entenda Gen Z e Gen Alpha como repertório cultural,
  não como licença para encher toda fala de gíria.

- Reconheça meme, internetês, stan culture, fandom,
  shade, camp, serve, slay, ate, gagged, mother,
  iconic, cringe, delulu, tea, read, clock, drag
  e afins pelo SENTIDO no contexto.

- Adapte para PT-BR atual quando houver equivalente natural.

- Preserve termos e catchphrases quando a tradução
  destruir a referência cultural.

- Diferencie fala jovem, adulta, formal, regional,
  profissional, reality-show, entrevista,
  competição e conversa íntima.

LGBTQIAPN+ / DRAG / BALLROOM / REALITY

- Trate cultura LGBTQIAPN+ com letramento e respeito,
  sem apagar humor, sexualidade, irreverência,
  camp ou shade.

- Respeite identidade e gênero quando o contexto
  deixar claro.

- Se gênero não estiver claro, reformule
  naturalmente sem inventar.

- bitch como vocativo amigável:
  bicha, gata, amiga, menina ou omitir.
  Nunca "puta" automaticamente.

- gagged como reação:
  passada,
  em choque,
  sem reação.
  "amordaçada" só no sentido físico.

- she ate / you ate:
  arrasou,
  entregou tudo,
  serviu.
  Nunca "comeu" literalmente nesse sentido.

- no crumbs:
  não deixou nada pra ninguém,
  quando couber.

- judges em competição/reality:
  jurados.

- supportive:
  me apoiou muito /
  esteve do meu lado.
  Evite "super apoiador".

- double/shared win:
  vitória dupla /
  as duas ganharam.
  Não "empate duplo" sem empate.

- Preserve quando reconhecíveis:
  Werkroom
  Condragulations
  Shantay, you stay
  Sashay away
  You betta werk

NATURALIDADE

- Em fala casual, "tô", "tá", "pra", "né"
  podem ser usados se combinarem com a pessoa.

- Não force gíria jovem em todo mundo.

- Preserve humor, ironia, sarcasmo, flerte,
  shade, emoção e intensidade.

- Não use lusitanismos.

- Não use linguagem burocrática ou artificial.

- Não traduza expressão idiomática palavra por palavra.

PALAVRÃO

- Não censure.

- Preserve intensidade de forma brasileira natural.

- Não transforme automaticamente todo "fucking"
  em "da porra" ou "do caralho".

FIDELIDADE E SINCRONIZAÇÃO

- Não resuma.

- Não invente.

- Não omita finais de frase.

- Não mova conteúdo de um cue para outro.

- Não antecipe fala do cue seguinte.

- Cada id recebido deve voltar exatamente uma vez.

- O Gemini NÃO cria timestamps.

- Os tempos são sempre copiados do SRT original
  pelo JavaScript.

FORMATO

- Não adicione nome de speaker.

- Não adicione [NOME].

- Não adicione "NOME:".

- Não adicione SDH/CC.

- Preserve quebra de linha quando ela separar
  duas falas dentro do mesmo cue.
`;

const PLAN_PROMPT = `
Você é editor de continuidade de legendas EN→PT-BR.

Leia uma amostra ampla do episódio e produza
uma BÍBLIA EDITORIAL CURTA.

Extraia SOMENTE o que ajuda a tradução:

- tom geral;
- nomes e relações quando claras;
- gênero apenas quando realmente claro;
- termos recorrentes;
- referências de fandom/cultura;
- catchphrases;
- escolhas de tradução que precisam permanecer consistentes.

Não traduza o episódio.

Não invente fatos.

Se algo não estiver claro, não inclua.
`;

const TRANSLATOR_PROMPT = `
Você é o tradutor principal de legendas EN→PT-BR.

${STYLE_PACK}

Você receberá:

1) bíblia editorial;
2) contexto anterior;
3) cues target;
4) contexto posterior.

Traduza SOMENTE target.

Contexto anterior/posterior serve somente
para compreender continuidade, intenção,
gírias, referência, sujeito e gênero.

Nunca devolva cues de contexto.

Nunca antecipe conteúdo futuro.

Para cada target:
- mantenha exatamente o mesmo id em "i";
- coloque a tradução em "pt".
`;

const REPAIR_PROMPT = `
Você é editor final EN→PT-BR.

${STYLE_PACK}

Você receberá SOMENTE cues sinalizados
por um detector local objetivo.

Corrija apenas defeitos reais.

Se já estiver bom, preserve.

Não redistribua conteúdo entre ids.
`;

// ============================================================
// SCHEMAS
// ============================================================

const PLAN_SCHEMA = {
    type:
        "object",

    additionalProperties:
        false,

    properties: {
        tone: {
            type:
                "string"
        },

        people: {
            type:
                "array",

            items: {
                type:
                    "string"
            },

            maxItems:
                25
        },

        glossary: {
            type:
                "array",

            items: {
                type:
                    "string"
            },

            maxItems:
                40
        },

        continuity: {
            type:
                "array",

            items: {
                type:
                    "string"
            },

            maxItems:
                30
        }
    },

    required: [
        "tone",
        "people",
        "glossary",
        "continuity"
    ]
};

function cueTranslationSchema(
    expectedCount
) {
    return {
        type:
            "object",

        additionalProperties:
            false,

        properties: {
            cues: {
                type:
                    "array",

                minItems:
                    expectedCount,

                maxItems:
                    expectedCount,

                items: {
                    type:
                        "object",

                    additionalProperties:
                        false,

                    properties: {
                        i: {
                            type:
                                "integer"
                        },

                        pt: {
                            type:
                                "string"
                        }
                    },

                    required: [
                        "i",
                        "pt"
                    ]
                }
            }
        },

        required: [
            "cues"
        ]
    };
}

// ============================================================
// GEMINI INTERACTIONS API
// ============================================================

function parseDurationMs(
    value
) {
    const text =
        String(
            value ||
            ""
        )
            .trim()
            .toLowerCase();

    if (!text) {
        return null;
    }

    const ms =
        text.match(
            /^(\d+(?:\.\d+)?)ms$/
        );

    if (ms) {
        return Math.max(
            250,

            Number(
                ms[1]
            )
        );
    }

    const sec =
        text.match(
            /^(\d+(?:\.\d+)?)s$/
        );

    if (sec) {
        return Math.max(
            1000,

            Number(
                sec[1]
            ) *
            1000
        );
    }

    const num =
        Number(
            text
        );

    if (
        Number.isFinite(
            num
        ) &&
        num >
            0
    ) {
        return Math.max(
            1000,

            num *
            1000
        );
    }

    return null;
}

function retryDelayMs(
    response,
    data,
    attempt
) {
    const header =
        parseDurationMs(
            response
                ?.headers
                ?.get(
                    "retry-after"
                )
        );

    if (header) {
        return Math.min(
            120000,

            header +
            500
        );
    }

    const details =
        Array.isArray(
            data?.error?.details
        )
            ? data.error.details
            : [];

    for (
        const detail
        of details
    ) {
        const parsed =
            parseDurationMs(
                detail?.retryDelay ||
                detail?.retry_delay ||
                detail?.metadata?.retryDelay ||
                detail?.metadata?.retry_delay
            );

        if (parsed) {
            return Math.min(
                120000,

                parsed +
                500
            );
        }
    }

    return Math.min(
        10000 *
        attempt,

        60000
    );
}

function extractInteractionText(
    data
) {
    if (
        typeof data?.output_text ===
            "string" &&
        data.output_text.trim()
    ) {
        return data.output_text.trim();
    }

    const steps =
        Array.isArray(
            data?.steps
        )
            ? data.steps
            : [];

    let out =
        "";

    for (
        const step
        of steps
    ) {
        if (
            step?.type !==
                "model_output" ||
            !Array.isArray(
                step.content
            )
        ) {
            continue;
        }

        out +=
            step.content
                .filter(
                    part =>
                        part?.type ===
                            "text" &&
                        typeof part?.text ===
                            "string"
                )
                .map(
                    part =>
                        part.text
                )
                .join("");
    }

    return out.trim();
}

async function acquireGeminiSlot(
    job
) {
    let release;

    const previous =
        geminiGate;

    geminiGate =
        new Promise(
            resolve => {
                release =
                    resolve;
            }
        );

    await previous;

    try {
        const wait =
            Math.max(
                0,

                GEMINI_MIN_START_INTERVAL_MS -
                (
                    Date.now() -
                    lastGeminiRequestStart
                )
            );

        if (
            wait >
            0
        ) {
            if (job) {
                job
                    .stats
                    .pacerWaitMs +=
                    wait;
            }

            console.log(
                `[GEMINI GATE] ` +
                `aguardando ` +
                `${(wait / 1000).toFixed(1)}s.`
            );

            await sleep(
                wait
            );
        }

        lastGeminiRequestStart =
            Date.now();
    }
    finally {
        release();
    }
}

function markAttempt(
    job,
    metric
) {
    if (!job) {
        return;
    }

    if (
        metric ===
        "main"
    ) {
        job.stats.mainAttempts++;
    }

    if (
        metric ===
        "repair"
    ) {
        job.stats.repairAttempts++;
    }
}

function mark429(
    job,
    metric
) {
    if (!job) {
        return;
    }

    if (
        metric ===
        "main"
    ) {
        job.stats.main429++;
    }

    if (
        metric ===
        "repair"
    ) {
        job.stats.repair429++;
    }
}

function markSuccess(
    job,
    metric,
    data
) {
    if (!job) {
        return;
    }

    if (
        metric ===
        "plan"
    ) {
        job.stats.planCalls++;
    }

    if (
        metric ===
        "main"
    ) {
        job.stats.mainCalls++;
    }

    if (
        metric ===
        "repair"
    ) {
        job.stats.repairCalls++;
    }

    job.stats.inputTokens +=
        Number(
            data
                ?.usage
                ?.total_input_tokens ||
            0
        );

    job.stats.outputTokens +=
        Number(
            data
                ?.usage
                ?.total_output_tokens ||
            0
        );

    job.stats.thoughtTokens +=
        Number(
            data
                ?.usage
                ?.total_thought_tokens ||
            0
        );
}

async function geminiRequest({
    system,
    user,
    schema,
    thinkingLevel,
    maxOutputTokens,
    timeoutMs,
    maxRetries,
    job = null,
    metric = "main"
}) {
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
            maxRetries;
        attempt++
    ) {
        markAttempt(
            job,
            metric
        );

        await acquireGeminiSlot(
            job
        );

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
                `[GEMINI ${metric.toUpperCase()}] ` +
                `${GEMINI_MODEL} ` +
                `request ${attempt}/${maxRetries} | ` +
                `thinking=${thinkingLevel}.`
            );

            const response =
                await fetch(
                    "https://generativelanguage.googleapis.com/v1beta/interactions",

                    {
                        method:
                            "POST",

                        headers: {
                            "Content-Type":
                                "application/json",

                            "x-goog-api-key":
                                GEMINI_API_KEY,

                            "Api-Revision":
                                "2026-05-20"
                        },

                        body:
                            JSON.stringify({
                                model:
                                    GEMINI_MODEL,

                                input:
                                    user,

                                system_instruction:
                                    system,

                                response_format: {
                                    type:
                                        "text",

                                    mime_type:
                                        "application/json",

                                    schema
                                },

                                generation_config: {
                                    max_output_tokens:
                                        maxOutputTokens,

                                    thinking_level:
                                        thinkingLevel
                                },

                                store:
                                    false
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
                const status =
                    String(
                        data?.status ||
                        "completed"
                    )
                        .toLowerCase();

                const text =
                    extractInteractionText(
                        data
                    );

                if (
                    [
                        "failed",
                        "cancelled",
                        "budget_exceeded"
                    ].includes(
                        status
                    )
                ) {
                    const error =
                        new Error(
                            `Gemini ${metric} status=${status}: ` +
                            `${String(
                                data?.error?.message ||
                                data?.message ||
                                "sem detalhe"
                            ).slice(
                                0,
                                1200
                            )}`
                        );

                    error.nonRetryable =
                        status ===
                        "budget_exceeded";

                    throw error;
                }

                if (
                    status ===
                        "incomplete" ||
                    !text
                ) {
                    throw new Error(
                        status ===
                            "incomplete"
                            ? `Gemini ${metric} retornou INCOMPLETE.`
                            : `Gemini ${metric} retornou vazio.`
                    );
                }

                markSuccess(
                    job,
                    metric,
                    data
                );

                console.log(
                    `[GEMINI ${metric.toUpperCase()}] OK | ` +
                    `input=${Number(
                        data
                            ?.usage
                            ?.total_input_tokens ||
                        0
                    )} | ` +
                    `output=${Number(
                        data
                            ?.usage
                            ?.total_output_tokens ||
                        0
                    )} | ` +
                    `thought=${Number(
                        data
                            ?.usage
                            ?.total_thought_tokens ||
                        0
                    )}.`
                );

                return {
                    text,

                    status,

                    usage:
                        data?.usage ||
                        {}
                };
            }

            const error =
                new Error(
                    `GEMINI ${GEMINI_MODEL} ` +
                    `HTTP ${response.status}: ` +
                    `${String(
                        data?.error?.message ||
                        data?.message ||
                        raw ||
                        "erro"
                    ).slice(
                        0,
                        1600
                    )}`
                );

            error.status =
                response.status;

            if (
                response.status ===
                429
            ) {
                mark429(
                    job,
                    metric
                );

                if (
                    attempt ===
                    maxRetries
                ) {
                    throw error;
                }

                const wait =
                    retryDelayMs(
                        response,
                        data,
                        attempt
                    );

                console.warn(
                    `[GEMINI ${metric.toUpperCase()}] ` +
                    `429; repetindo O MESMO lote em ` +
                    `${(wait / 1000).toFixed(1)}s.`
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

            await sleep(
                Math.min(
                    4000 *
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
                        `Gemini ${metric}: timeout.`
                    )
                    : error;

            if (
                lastError
                    ?.nonRetryable
            ) {
                throw lastError;
            }

            if (
                lastError?.status ===
                429
            ) {
                if (
                    attempt ===
                    maxRetries
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
                maxRetries
            ) {
                throw lastError;
            }

            await sleep(
                Math.min(
                    4000 *
                    attempt,

                    20000
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
            `Gemini ${metric} falhou.`
        )
    );
}

// ============================================================
// PLANNER
// ============================================================

function compactCue(
    block
) {
    return {
        i:
            block.index,

        en:
            block.text,

        ...(
            block.speakerHint
                ? {
                    speaker:
                        block.speakerHint
                }
                : {}
        )
    };
}

function plannerSample(
    blocks
) {
    if (
        blocks.length <=
        PLAN_SAMPLE_MAX_CUES
    ) {
        return blocks.map(
            compactCue
        );
    }

    const out =
        [];

    const step =
        blocks.length /
        PLAN_SAMPLE_MAX_CUES;

    const used =
        new Set();

    for (
        let i = 0;
        i <
            PLAN_SAMPLE_MAX_CUES;
        i++
    ) {
        const index =
            Math.min(
                blocks.length -
                    1,

                Math.floor(
                    i *
                    step
                )
            );

        if (
            used.has(
                index
            )
        ) {
            continue;
        }

        used.add(
            index
        );

        out.push(
            compactCue(
                blocks[index]
            )
        );
    }

    return out;
}

function fallbackPlan() {
    return {
        tone:
            "PT-BR natural, contextual e fiel ao registro.",

        people:
            [],

        glossary:
            [],

        continuity:
            []
    };
}

async function buildEpisodePlan(
    blocks,
    job
) {
    try {
        const response =
            await geminiRequest({
                system:
                    PLAN_PROMPT,

                user:
                    `Arquivo: ${job.filename || "desconhecido"}\n` +
                    `Tipo: ${job.type}\n` +
                    `ID: ${job.videoId}\n\n` +
                    `Amostra:\n` +
                    `${JSON.stringify({
                        cues:
                            plannerSample(
                                blocks
                            )
                    })}`,

                schema:
                    PLAN_SCHEMA,

                thinkingLevel:
                    PLAN_THINKING,

                maxOutputTokens:
                    PLAN_MAX_OUTPUT_TOKENS,

                timeoutMs:
                    PLAN_TIMEOUT_MS,

                maxRetries:
                    PLAN_RETRIES,

                job,

                metric:
                    "plan"
            });

        const plan =
            JSON.parse(
                stripCodeFences(
                    response.text
                )
            );

        console.log(
            `[EPISODE PLAN] OK | ` +
            `people=${plan.people?.length || 0} | ` +
            `glossary=${plan.glossary?.length || 0}.`
        );

        return plan;
    }
    catch (error) {
        job.stats.planFailures++;

        console.warn(
            `[EPISODE PLAN] ` +
            `Falhou sem bloquear: ` +
            `${errorMessage(error).slice(0, 300)}`
        );

        return fallbackPlan();
    }
}

// ============================================================
// BATCHES
// ============================================================

function buildMainBatches(
    blocks
) {
    const batches =
        [];

    let current =
        [];

    let chars =
        0;

    for (
        const block
        of blocks
    ) {
        const size =
            block.text.length +
            80;

        if (
            current.length &&
            (
                current.length >=
                    MAIN_BATCH_MAX_CUES ||
                chars +
                    size >
                    MAIN_BATCH_MAX_CHARS
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
            block
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

function positionMap(
    blocks
) {
    const map =
        new Map();

    blocks.forEach(
        (
            block,
            index
        ) =>
            map.set(
                block.index,
                index
            )
    );

    return map;
}

function contextPayload(
    allBlocks,
    posMap,
    batch,
    translations
) {
    const first =
        posMap.get(
            batch[0].index
        );

    const last =
        posMap.get(
            batch[
                batch.length -
                1
            ].index
        );

    return {
        before:
            allBlocks
                .slice(
                    Math.max(
                        0,

                        first -
                        CONTEXT_CUES_BEFORE
                    ),

                    first
                )
                .map(
                    block => ({
                        ...compactCue(
                            block
                        ),

                        ...(
                            translations.has(
                                block.index
                            )
                                ? {
                                    pt:
                                        translations.get(
                                            block.index
                                        )
                                }
                                : {}
                        )
                    })
                ),

        target:
            batch.map(
                compactCue
            ),

        after:
            allBlocks
                .slice(
                    last +
                        1,

                    Math.min(
                        allBlocks.length,

                        last +
                        1 +
                        CONTEXT_CUES_AFTER
                    )
                )
                .map(
                    compactCue
                )
    };
}

function parseCueTranslation(
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
        throw new Error(
            "JSON de tradução inválido."
        );
    }

    if (
        !Array.isArray(
            parsed?.cues
        )
    ) {
        throw new Error(
            "Resposta sem cues."
        );
    }

    const ids =
        batch.map(
            block =>
                block.index
        );

    const expected =
        new Set(
            ids
        );

    const byId =
        new Map();

    for (
        const item
        of parsed.cues
    ) {
        const id =
            Number(
                item?.i
            );

        const pt =
            String(
                item?.pt ??
                ""
            ).trim();

        if (
            !expected.has(
                id
            )
        ) {
            throw new Error(
                `ID inesperado ${id}.`
            );
        }

        if (
            byId.has(
                id
            )
        ) {
            throw new Error(
                `ID duplicado ${id}.`
            );
        }

        if (!pt) {
            throw new Error(
                `Cue ${id} vazio.`
            );
        }

        byId.set(
            id,
            pt
        );
    }

    if (
        byId.size !==
        ids.length
    ) {
        throw new Error(
            `Tradução incompleta ` +
            `${byId.size}/${ids.length}.`
        );
    }

    return byId;
}

async function translateMainBatch({
    blocks,
    posMap,
    batch,
    translations,
    plan,
    job
}) {
    let lastError;

    for (
        let parseAttempt = 1;
        parseAttempt <=
            MAIN_PARSE_ATTEMPTS;
        parseAttempt++
    ) {
        try {
            const payload =
                contextPayload(
                    blocks,
                    posMap,
                    batch,
                    translations
                );

            const response =
                await geminiRequest({
                    system:
                        TRANSLATOR_PROMPT,

                    user:
                        `BÍBLIA EDITORIAL:\n` +
                        `${JSON.stringify(plan)}\n\n` +

                        `CONTEXTO:\n` +
                        `${JSON.stringify(payload)}\n\n` +

                        `Traduza somente target. ` +
                        `Output exatamente ${batch.length} cues.`,

                    schema:
                        cueTranslationSchema(
                            batch.length
                        ),

                    thinkingLevel:
                        MAIN_THINKING,

                    maxOutputTokens:
                        MAIN_MAX_OUTPUT_TOKENS,

                    timeoutMs:
                        MAIN_TIMEOUT_MS,

                    maxRetries:
                        MAIN_HTTP_RETRIES,

                    job,

                    metric:
                        "main"
                });

            return parseCueTranslation(
                batch,
                response.text
            );
        }
        catch (error) {
            lastError =
                error;

            if (
                parseAttempt >=
                MAIN_PARSE_ATTEMPTS
            ) {
                throw error;
            }

            job.stats.mainParseRetries++;

            console.warn(
                `[MAIN VALIDATION] ` +
                `repetindo o MESMO lote: ` +
                `${errorMessage(error).slice(0, 240)}`
            );
        }
    }

    throw lastError;
}

async function translateAllMain(
    blocks,
    plan,
    job
) {
    const batches =
        buildMainBatches(
            blocks
        );

    const translations =
        new Map();

    const posMap =
        positionMap(
            blocks
        );

    job.stats.mainBatches =
        batches.length;

    console.log(
        `[MAIN] ${blocks.length} cues -> ` +
        `${batches.length} lote(s) | ` +
        `concorrência=${MAIN_CONCURRENCY} | ` +
        `até ${MAIN_BATCH_MAX_CUES} cues/lote.`
    );

    let cursor =
        0;

    let completed =
        0;

    async function worker(
        workerId
    ) {
        while (true) {
            const batchIndex =
                cursor++;

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
                `[MAIN W${workerId}] ` +
                `lote ${batchIndex + 1}/${batches.length}: ` +
                `${batch.length} cues.`
            );

            const translated =
                await translateMainBatch({
                    blocks,
                    posMap,
                    batch,
                    translations,
                    plan,
                    job
                });

            for (
                const [
                    id,
                    pt
                ]
                of translated
            ) {
                translations.set(
                    id,
                    pt
                );
            }

            completed++;

            job.progress =
                Math.min(
                    90,

                    5 +
                    Math.round(
                        85 *
                        completed /
                        batches.length
                    )
                );

            job.updatedAt =
                Date.now();

            console.log(
                `[MAIN W${workerId}] ` +
                `lote ${batchIndex + 1} OK | ` +
                `${translations.size}/${blocks.length} | ` +
                `${job.progress}%.`
            );
        }
    }

    await Promise.all(
        Array.from(
            {
                length:
                    Math.min(
                        MAIN_CONCURRENCY,
                        batches.length
                    )
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
        blocks.length
    ) {
        throw new Error(
            `Tradução principal incompleta: ` +
            `${translations.size}/${blocks.length}.`
        );
    }

    return translations;
}

// ============================================================
// LOCAL QUALITY DETECTOR
// ============================================================

function words(text) {
    return (
        String(
            text ||
            ""
        )
            .toLowerCase()
            .match(
                /[\p{L}\p{N}]+(?:['’][\p{L}\p{N}]+)?/gu
            ) ||
        []
    );
}

function normalizedWordSet(
    text
) {
    return new Set(
        words(
            text
        )
            .map(
                value =>
                    value
                        .normalize(
                            "NFD"
                        )
                        .replace(
                            /[\u0300-\u036f]/g,
                            ""
                        )
            )
            .filter(
                value =>
                    value.length >
                    2
            )
    );
}

function copiedEnglishRatio(
    en,
    pt
) {
    const english =
        normalizedWordSet(
            en
        );

    const portuguese =
        normalizedWordSet(
            pt
        );

    if (
        !english.size
    ) {
        return 0;
    }

    let copied =
        0;

    for (
        const word
        of english
    ) {
        if (
            portuguese.has(
                word
            )
        ) {
            copied++;
        }
    }

    return (
        copied /
        english.size
    );
}

function isDragContext(
    filename,
    en
) {
    return (
        /rupaul|drag[ ._-]*race|dragula/i.test(
            String(
                filename ||
                ""
            )
        ) ||

        /\bwerkroom\b|\blip sync\b|\bshantay\b|\bsashay\b|\bcondragulations\b/i.test(
            String(
                en ||
                ""
            )
        )
    );
}

function localReasonsForCue(
    block,
    pt,
    filename
) {
    const en =
        String(
            block.text ||
            ""
        );

    const translated =
        String(
            pt ||
            ""
        );

    const reasons =
        [];

    const enCount =
        words(
            en
        ).length;

    const ptCount =
        words(
            translated
        ).length;

    if (
        !translated.trim()
    ) {
        reasons.push(
            "EMPTY"
        );
    }

    if (
        enCount >=
            5 &&
        copiedEnglishRatio(
            en,
            translated
        ) >=
            0.60
    ) {
        reasons.push(
            "POSSIBLE_UNTRANSLATED"
        );
    }

    if (
        enCount >=
            12 &&
        ptCount <=
            Math.max(
                2,

                Math.floor(
                    enCount *
                    0.28
                )
            )
    ) {
        reasons.push(
            "POSSIBLE_OMISSION"
        );
    }

    if (
        isDragContext(
            filename,
            en
        )
    ) {
        if (
            /\bgagged\b/i.test(
                en
            ) &&
            /\bamordaçad[oa]s?\b/i.test(
                translated
            )
        ) {
            reasons.push(
                "LITERAL_GAGGED"
            );
        }

        if (
            /\b(?:she|he|you|they)\s+ate(?:\s+that)?\b/i.test(
                en
            ) &&
            /\b(?:comeu|comeram|comeste)\b/i.test(
                translated
            )
        ) {
            reasons.push(
                "LITERAL_ATE"
            );
        }

        if (
            /\bjudges?\b/i.test(
                en
            ) &&
            /\bju[ií]zes?\b/i.test(
                translated
            )
        ) {
            reasons.push(
                "JUDGES_AS_JUIZES"
            );
        }

        if (
            /\bsupportive\b/i.test(
                en
            ) &&
            /\bsuper\s+apoiador(?:a|es|as)?\b/i.test(
                translated
            )
        ) {
            reasons.push(
                "LITERAL_SUPPORTIVE"
            );
        }

        if (
            /\b(?:double|shared)\s+win\b/i.test(
                en
            ) &&
            /\bempate\s+duplo\b/i.test(
                translated
            )
        ) {
            reasons.push(
                "DOUBLE_WIN"
            );
        }
    }

    return [
        ...new Set(
            reasons
        )
    ];
}

function detectLocalIssues(
    blocks,
    translations,
    filename
) {
    const issues =
        [];

    for (
        const block
        of blocks
    ) {
        const reasons =
            localReasonsForCue(
                block,

                translations.get(
                    block.index
                ),

                filename
            );

        if (
            reasons.length
        ) {
            issues.push({
                id:
                    block.index,

                reasons
            });
        }
    }

    return issues;
}

// ============================================================
// REPAIR
// ============================================================

function repairPayload(
    blocks,
    posMap,
    translations,
    issue
) {
    const pos =
        posMap.get(
            issue.id
        );

    const block =
        blocks[pos];

    return {
        i:
            block.index,

        en:
            block.text,

        pt:
            translations.get(
                block.index
            ),

        reasons:
            issue.reasons,

        ...(
            block.speakerHint
                ? {
                    speaker:
                        block.speakerHint
                }
                : {}
        ),

        before:
            blocks
                .slice(
                    Math.max(
                        0,
                        pos -
                        2
                    ),

                    pos
                )
                .map(
                    item => ({
                        i:
                            item.index,

                        en:
                            item.text,

                        pt:
                            translations.get(
                                item.index
                            ) ||
                            ""
                    })
                ),

        after:
            blocks
                .slice(
                    pos +
                        1,

                    Math.min(
                        blocks.length,

                        pos +
                        3
                    )
                )
                .map(
                    item => ({
                        i:
                            item.index,

                        en:
                            item.text,

                        pt:
                            translations.get(
                                item.index
                            ) ||
                            ""
                    })
                )
    };
}

async function repairBatch(
    blocks,
    posMap,
    translations,
    issues,
    plan,
    job
) {
    let lastError;

    for (
        let parseAttempt = 1;
        parseAttempt <=
            REPAIR_PARSE_ATTEMPTS;
        parseAttempt++
    ) {
        try {
            const response =
                await geminiRequest({
                    system:
                        REPAIR_PROMPT,

                    user:
                        `BÍBLIA:\n` +
                        `${JSON.stringify(plan)}\n\n` +
                        `CUES:\n` +
                        `${JSON.stringify({
                            cues:
                                issues.map(
                                    issue =>
                                        repairPayload(
                                            blocks,
                                            posMap,
                                            translations,
                                            issue
                                        )
                                )
                        })}`,

                    schema:
                        cueTranslationSchema(
                            issues.length
                        ),

                    thinkingLevel:
                        REPAIR_THINKING,

                    maxOutputTokens:
                        REPAIR_MAX_OUTPUT_TOKENS,

                    timeoutMs:
                        REPAIR_TIMEOUT_MS,

                    maxRetries:
                        REPAIR_HTTP_RETRIES,

                    job,

                    metric:
                        "repair"
                });

            return parseCueTranslation(
                issues.map(
                    issue =>
                        blocks[
                            posMap.get(
                                issue.id
                            )
                        ]
                ),

                response.text
            );
        }
        catch (error) {
            lastError =
                error;

            if (
                parseAttempt >=
                REPAIR_PARSE_ATTEMPTS
            ) {
                throw error;
            }

            job.stats.repairParseRetries++;
        }
    }

    throw lastError;
}

async function tryFocusedRepair(
    blocks,
    translations,
    plan,
    job
) {
    if (
        !REPAIR_ENABLED
    ) {
        return translations;
    }

    let issues;

    try {
        issues =
            detectLocalIssues(
                blocks,
                translations,
                job.filename
            );
    }
    catch (error) {
        console.warn(
            `[LOCAL GUARD] ` +
            `falhou; mantendo principal: ` +
            `${errorMessage(error)}`
        );

        return translations;
    }

    job.stats.localFlags =
        issues.length;

    if (
        !issues.length
    ) {
        console.log(
            "[LOCAL GUARD] 0 suspeitos."
        );

        return translations;
    }

    issues.sort(
        (
            a,
            b
        ) =>
            b.reasons.length -
            a.reasons.length
    );

    const selected =
        issues.slice(
            0,

            REPAIR_MAX_CUES_TOTAL
        );

    job.stats.repairSelected =
        selected.length;

    console.log(
        `[LOCAL GUARD] ` +
        `${issues.length} suspeitos; ` +
        `reparando até ${selected.length}.`
    );

    const posMap =
        positionMap(
            blocks
        );

    const updated =
        new Map(
            translations
        );

    try {
        for (
            let i = 0;
            i < selected.length;
            i +=
                REPAIR_BATCH_MAX_CUES
        ) {
            const batch =
                selected.slice(
                    i,

                    i +
                    REPAIR_BATCH_MAX_CUES
                );

            const repaired =
                await repairBatch(
                    blocks,
                    posMap,
                    updated,
                    batch,
                    plan,
                    job
                );

            for (
                const [
                    id,
                    pt
                ]
                of repaired
            ) {
                updated.set(
                    id,
                    pt
                );
            }
        }

        return updated;
    }
    catch (error) {
        job.stats.repairFailures++;

        console.warn(
            `[REPAIR] ` +
            `falhou sem matar episódio: ` +
            `${errorMessage(error).slice(0, 350)}`
        );

        return translations;
    }
}

// ============================================================
// PIPELINE
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

    job.stats.sourceCues =
        blocks.length;

    console.log(
        `[PIPELINE 8.1] ` +
        `fonte=${job.sourceKind} | ` +
        `${blocks.length} cues.`
    );

    const plan =
        await buildEpisodePlan(
            blocks,
            job
        );

    job.progress =
        5;

    const mainTranslations =
        await translateAllMain(
            blocks,
            plan,
            job
        );

    const mainSrt =
        buildSrt(
            blocks,
            mainTranslations
        );

    auditTimestamps(
        sourceSrt,
        mainSrt,
        "MAIN"
    );

    // A partir daqui existe legenda completa
    // e sincronizada. Nada posterior pode destruí-la.
    job.safeDraft =
        mainSrt;

    job.progress =
        92;

    console.log(
        `[SAFE DRAFT] ` +
        `${blocks.length}/${blocks.length} protegido.`
    );

    const finalTranslations =
        await tryFocusedRepair(
            blocks,
            mainTranslations,
            plan,
            job
        );

    const finalSrt =
        buildSrt(
            blocks,
            finalTranslations
        );

    auditTimestamps(
        sourceSrt,
        finalSrt,
        "FINAL"
    );

    console.log(
        `[PIPELINE 8.1] FINAL OK | ` +
        `${blocks.length} cues | ` +
        `${(
            (
                Date.now() -
                startedAt
            ) /
            1000
        ).toFixed(1)}s.`
    );

    return finalSrt;
}

// ============================================================
// JOB PROCESSING
// ============================================================

async function processJob(
    job
) {
    job.status =
        "processing";

    job.progress =
        Math.max(
            1,

            job.progress ||
            0
        );

    job.updatedAt =
        Date.now();

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
    }
    catch (error) {
        if (
            job.safeDraft
        ) {
            try {
                auditTimestamps(
                    job.sourceSrt,
                    job.safeDraft,
                    "SAFE-DRAFT-FALLBACK"
                );

                setCache(
                    job.cacheKey,
                    job.safeDraft
                );

                job.result =
                    job.safeDraft;

                job.status =
                    "completed";

                job.progress =
                    100;

                job
                    .stats
                    .usedSafeDraftFallback =
                    true;

                console.warn(
                    `[JOB ${job.id}] ` +
                    `entregando SAFE DRAFT após erro opcional: ` +
                    `${errorMessage(error).slice(0, 300)}`
                );

                return;
            }
            catch {}
        }

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
    finally {
        job.updatedAt =
            Date.now();
    }
}

function startJob(
    job
) {
    if (
        job.promise
    ) {
        return job.promise;
    }

    job.started =
        true;

    job.status =
        "processing";

    job.promise =
        processJob(
            job
        )
            .finally(
                () => {
                    job.promise =
                        null;
                }
            );

    return job.promise;
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
            `${baseUrl(req)}/` +
            `subtitle/` +
            `${encodeURIComponent(job.id)}` +
            `.srt`
    };
}

// ============================================================
// OPENSUBTITLES CLOUD
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

function parseExtra(
    extra
) {
    const params =
        new URLSearchParams(
            extra ||
            ""
        );

    return {
        filename:
            params.get(
                "filename"
            ) ||
            "",

        videoSize:
            params.get(
                "videoSize"
            ) ||
            "",

        videoHash:
            params.get(
                "videoHash"
            ) ||
            ""
    };
}

function buildOpenSubtitlesUrl(
    type,
    id,
    {
        filename,
        videoSize,
        videoHash
    }
) {
    const base =
        `https://opensubtitles-v3.strem.io/` +
        `subtitles/` +
        `${encodeURIComponent(type)}/` +
        `${encodeURIComponent(id)}`;

    const params =
        new URLSearchParams();

    if (
        videoHash
    ) {
        params.set(
            "videoHash",
            videoHash
        );
    }

    if (
        videoSize
    ) {
        params.set(
            "videoSize",
            videoSize
        );
    }

    if (
        filename
    ) {
        params.set(
            "filename",
            filename
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
                    score(b) -
                    score(a)
                );
            }
        )[0] ||
        null;
}

async function fetchOpenSubtitlesSource({
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
        `[OPENSUBTITLES CLOUD] ${url}`
    );

    const response =
        await fetchWithTimeout(
            url,

            {
                headers: {
                    Accept:
                        "application/json",

                    "User-Agent":
                        "Stremio-PTBR/8.1"
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

    if (
        !target
    ) {
        return null;
    }

    const subtitleResponse =
        await fetchWithTimeout(
            target.url,

            {
                headers: {
                    "User-Agent":
                        "Stremio-PTBR/8.1"
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
            "Legenda OpenSubtitles vazia/grande demais."
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
            "Legenda OpenSubtitles vazia após limpeza."
        );
    }

    return clean;
}

async function publicSubtitlesHandler(
    req,
    res
) {
    const type =
        String(
            req.params.type ||
            ""
        );

    const id =
        String(
            req.params.id ||
            ""
        );

    const {
        filename,
        videoSize,
        videoHash
    } =
        parseExtra(
            req.params.extra ||
            ""
        );

    console.log(
        `[STREMIO CLOUD] ` +
        `${type}/${id} | ` +
        `${filename || "sem filename"}`
    );

    try {
        const sourceSrt =
            await fetchOpenSubtitlesSource({
                type,
                id,
                filename,
                videoSize,
                videoHash
            });

        if (
            !sourceSrt
        ) {
            console.log(
                "[STREMIO CLOUD] nenhuma legenda inglesa utilizável."
            );

            return safeJson(
                res,

                {
                    subtitles:
                        []
                }
            );
        }

        const job =
            getOrCreateJob(
                {
                    type,

                    videoId:
                        id,

                    filename,

                    sourceSrt,

                    sourceKind:
                        "opensubtitles-cloud"
                },

                {
                    lazy:
                        true
                }
            );

        const subtitleUrl =
            `${baseUrl(req)}/` +
            `subtitle/` +
            `${encodeURIComponent(job.id)}` +
            `.srt`;

        console.log(
            `[CLOUD LAZY] ` +
            `opção criada sem Gemini | ` +
            `job=${job.id}`
        );

        return safeJson(
            res,

            {
                subtitles: [
                    {
                        id:
                            `ptbr-cloud-opensub-` +
                            `${job.sourceHash.slice(0, 12)}`,

                        url:
                            subtitleUrl,

                        lang:
                            "por"
                    }
                ]
            }
        );
    }
    catch (error) {
        console.error(
            `[STREMIO CLOUD] ` +
            `${errorMessage(error)}`
        );

        return safeJson(
            res,

            {
                subtitles:
                    []
            }
        );
    }
}

// ============================================================
// MANIFEST / ROUTES
// ============================================================

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "8.1.0",

    name:
        "PT-BR Cloud • OpenSubtitles",

    description:
        "OpenSubtitles inglês → Gemini 3.5 Flash-Lite → PT-BR natural. Universal e independente da Ponte Local.",

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

            model:
                GEMINI_MODEL,

            mode:
                "CLOUD_OPEN_SUBTITLES_PLUS_EMBEDDED_API",

            mainBatchMaxCues:
                MAIN_BATCH_MAX_CUES,

            mainConcurrency:
                MAIN_CONCURRENCY,

            pacerMs:
                GEMINI_MIN_START_INTERVAL_MS,

            cache:
                translationCache.size,

            jobs:
                jobs.size
        })
);

app.get(
    "/subtitles/:type/:id.json",
    publicSubtitlesHandler
);

app.get(
    "/subtitles/:type/:id/:extra.json",
    publicSubtitlesHandler
);

// ============================================================
// EMBEDDED API — SOMENTE PONTE LOCAL
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

            if (
                !sourceSrt ||
                !parseSrt(
                    sourceSrt
                ).length
            ) {
                throw new Error(
                    "Embedded inválida após limpeza."
                );
            }

            console.log(
                `[EMBEDDED API] ` +
                `${type}/${videoId} | ` +
                `${parseSrt(sourceSrt).length} cues.`
            );

            const job =
                getOrCreateJob(
                    {
                        type,

                        videoId,

                        filename,

                        sourceSrt,

                        sourceKind:
                            "embedded"
                    },

                    {
                        lazy:
                            false
                    }
                );

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
                `${errorMessage(error)}`
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

                safeDraft:
                    Boolean(
                        job.safeDraft
                    ),

                stats:
                    job.stats
            }
        );
    }
);

// ============================================================
// SUBTITLE DELIVERY
// ============================================================

function processingSrt(
    job
) {
    return [
        "1",

        "00:00:01,000 --> 00:00:08,000",

        "Traduzindo legenda para PT-BR...",

        "",

        "2",

        "00:00:08,500 --> 00:00:15,000",

        `Progresso: ${Number(
            job?.progress ||
            0
        )}%.`
    ].join("\n");
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

        // CLOUD LAZY:
        // consultar lista não usa Gemini.
        // A tradução começa quando esta URL é realmente acessada.
        if (
            job.status ===
                "pending" &&
            !job.started
        ) {
            console.log(
                `[CLOUD LAZY] ` +
                `URL selecionada; ` +
                `iniciando ${job.id}.`
            );

            startJob(
                job
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
            " STREMIO PT-BR 8.1 CLOUD + EMBEDDED API"
        );

        console.log(
            "============================================================"
        );

        console.log(
            `Gemini API: ` +
            `${
                GEMINI_API_KEY
                    ? "CONFIGURADA ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            `Local Bridge Secret: ` +
            `${
                LOCAL_BRIDGE_SECRET
                    ? "CONFIGURADA ✅"
                    : "FALTANDO ❌"
            }`
        );

        console.log(
            `Modelo: ${GEMINI_MODEL} ✅`
        );

        console.log(
            "Cloud OpenSubtitles: ATIVO + LAZY ✅"
        );

        console.log(
            "Embedded API para Ponte: ATIVA ✅"
        );

        console.log(
            `Main: até ${MAIN_BATCH_MAX_CUES} cues / ` +
            `${MAIN_BATCH_MAX_CHARS} chars | ` +
            `concorrência=${MAIN_CONCURRENCY} ✅`
        );

        console.log(
            `Contexto: ` +
            `${CONTEXT_CUES_BEFORE} antes + ` +
            `${CONTEXT_CUES_AFTER} depois ✅`
        );

        console.log(
            `Gate global: ` +
            `${GEMINI_MIN_START_INTERVAL_MS}ms ` +
            `entre inícios ✅`
        );

        console.log(
            "Planner: minimal e não-bloqueante ✅"
        );

        console.log(
            "Tradução: thinking=low ✅"
        );

        console.log(
            "Repair cirúrgico: thinking=medium ✅"
        );

        console.log(
            "Gen Z/Alpha + LGBTQIAPN+ + drag/reality/fandom: ATIVO ✅"
        );

        console.log(
            "Timestamp lock: absoluto ✅"
        );

        console.log(
            "Gemini nunca gera timestamps ✅"
        );

        console.log(
            "Auditoria ampla: REMOVIDA ✅"
        );

        console.log(
            "Deep Audit: REMOVIDA ✅"
        );

        console.log(
            "Split por 429: PROIBIDO ✅"
        );

        console.log(
            "SAFE DRAFT: ATIVO ✅"
        );

        console.log(
            `Cache namespace: ${CACHE_VERSION}`
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
