const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();

app.use(cors());
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

// ============================================================
// CONFIGURAÇÃO
// ============================================================

const PORT = Number(
    process.env.PORT || 10000
);

const GEMINI_API_KEY =
    String(
        process.env.GEMINI_API_KEY || ""
    ).trim();

const GEMINI_MODEL =
    String(
        process.env.GEMINI_MODEL ||
        "gemini-3.5-flash-lite"
    ).trim();

const PUBLIC_URL =
    String(
        process.env.PUBLIC_URL || ""
    ).replace(/\/+$/, "");

const LOCAL_BRIDGE_SECRET =
    String(
        process.env.LOCAL_BRIDGE_SECRET || ""
    ).trim();

const SOURCE_FETCH_TIMEOUT_MS =
    20_000;

const GEMINI_TIMEOUT_MS =
    90_000;

const MAX_TRANSLATION_TIME_MS =
    Number(
        process.env.MAX_TRANSLATION_TIME_MS ||
        480_000
    );

const MAX_BATCH_BLOCKS =
    Number(
        process.env.MAX_BATCH_BLOCKS ||
        300
    );

const MAX_BATCH_CHARS =
    Number(
        process.env.MAX_BATCH_CHARS ||
        28_000
    );

const MIN_REQUEST_INTERVAL_MS =
    Number(
        process.env.MIN_REQUEST_INTERVAL_MS ||
        3000
    );

const MAX_OUTPUT_TOKENS =
    Number(
        process.env.MAX_OUTPUT_TOKENS ||
        16_000
    );

const GEMINI_CONCURRENCY = 1;

const MAX_NORMAL_RETRIES = 2;

const MAX_RATE_LIMIT_COOLDOWN_MS =
    120_000;

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

const MAX_CACHE_ENTRIES = 200;

const MAX_JOBS = 300;

const MAX_SOURCE_CHARS =
    800_000;

// ============================================================
// MEMÓRIA / FILA
// ============================================================

const translationCache =
    new Map();

const jobs =
    new Map();

const geminiQueue = [];

let geminiWorkerRunning =
    false;

let lastGeminiRequestAt =
    0;

let geminiCooldownUntil =
    0;

// ============================================================
// HELPERS GERAIS
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

function sha256(text) {
    return crypto
        .createHash(
            "sha256"
        )
        .update(
            String(text),
            "utf8"
        )
        .digest(
            "hex"
        );
}

function randomId(
    length = 8
) {
    return crypto
        .randomBytes(
            length
        )
        .toString(
            "hex"
        );
}

function getErrorMessage(
    error
) {
    if (!error) {
        return "Erro desconhecido.";
    }

    if (
        typeof error ===
        "string"
    ) {
        return error;
    }

    return (
        error.message ||
        error.statusText ||
        "Erro desconhecido."
    );
}

function translationTimeoutError() {
    const error =
        new Error(
            "Tempo máximo de tradução atingido."
        );

    error.code =
        "TRANSLATION_TIMEOUT";

    return error;
}

function assertBeforeDeadline(
    deadlineAt
) {
    if (
        Number.isFinite(
            deadlineAt
        ) &&
        Date.now() >=
        deadlineAt
    ) {
        throw translationTimeoutError();
    }
}

function remainingBeforeDeadline(
    deadlineAt
) {
    if (
        !Number.isFinite(
            deadlineAt
        )
    ) {
        return Infinity;
    }

    return Math.max(
        0,
        deadlineAt -
        Date.now()
    );
}

async function sleepWithDeadline(
    ms,
    deadlineAt
) {
    const wait =
        Math.max(
            0,
            Number(ms) || 0
        );

    const remaining =
        remainingBeforeDeadline(
            deadlineAt
        );

    if (
        Number.isFinite(
            remaining
        ) &&
        remaining <=
        wait
    ) {
        if (
            remaining >
            0
        ) {
            await sleep(
                remaining
            );
        }

        throw translationTimeoutError();
    }

    if (
        wait >
        0
    ) {
        await sleep(
            wait
        );
    }

    assertBeforeDeadline(
        deadlineAt
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

    const protocol =
        req.headers[
            "x-forwarded-proto"
        ] ||
        req.protocol ||
        "https";

    const host =
        req.headers[
            "x-forwarded-host"
        ] ||
        req.get(
            "host"
        );

    return `${protocol}://${host}`;
}

function safeJson(
    res,
    data,
    status = 200
) {
    res.status(
        status
    );

    res.set(
        "Cache-Control",
        "no-store, no-cache, must-revalidate"
    );

    return res.json(
        data
    );
}

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
            req.headers.authorization ||
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

async function fetchWithTimeout(
    url,
    options = {},
    timeoutMs = 20_000
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
    } finally {
        clearTimeout(
            timer
        );
    }
}

// ============================================================
// LIMPEZA DE MEMÓRIA
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
    5 *
    60 *
    1000
).unref();

// ============================================================
// SRT + LIMPEZA SDH/CC
// ============================================================

function normalizeSrt(
    text
) {
    return String(
        text || ""
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
    text
) {
    return String(
        text || ""
    )
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

const SDH_CUE_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

function cleanDialogueLine(
    line
) {
    let text =
        String(
            line || ""
        ).trim();

    if (
        !text
    ) {
        return "";
    }

    /*
     * Exemplos:
     *
     * [Heidi] Hello
     * [laughing]
     * Hello [laughs]
     */
    text =
        text.replace(
            /\s*\[[^\]]+\]\s*/gu,
            " "
        );

    /*
     * Remove:
     *
     * (laughing)
     * (music playing)
     *
     * mas preserva frases normais:
     *
     * (I know.)
     */
    text =
        text.replace(
            /\s*\(([^)]*)\)\s*/gu,
            (
                match,
                inside
            ) => {
                return SDH_CUE_WORDS.test(
                    String(
                        inside || ""
                    )
                )
                    ? " "
                    : match;
            }
        );

    /*
     * PRODUCER: Hello
     * HEIDI: Hello
     * Lucas: Hello
     */
    text =
        text.replace(
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

    /*
     * Se restou apenas um hífen,
     * símbolo musical ou espaços.
     */
    if (
        /^[-–—♪♫♬\s]*$/u.test(
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
        normalized.split(
            /\n{2,}/
        );

    const cleanedBlocks = [];

    let removedBlocks = 0;
    let changedLines = 0;

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
            timingIndex ===
            -1
        ) {
            continue;
        }

        const timing =
            lines[
                timingIndex
            ].trim();

        const dialogue = [];

        for (
            const line
            of lines.slice(
                timingIndex + 1
            )
        ) {
            const cleaned =
                cleanDialogueLine(
                    line
                );

            if (
                cleaned !==
                line.trim()
            ) {
                changedLines++;
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
            removedBlocks++;

            continue;
        }

        cleanedBlocks.push({
            timing,
            dialogue
        });
    }

    const result =
        cleanedBlocks
            .map(
                (
                    block,
                    index
                ) =>
                    [
                        index + 1,
                        block.timing,
                        ...block.dialogue
                    ].join(
                        "\n"
                    )
            )
            .join(
                "\n\n"
            )
            .trim();

    console.log(
        `[CLEAN] SDH/CC: ${rawBlocks.length} -> ${cleanedBlocks.length} blocos; ${removedBlocks} removidos; ${changedLines} linha(s) alterada(s).`
    );

    return result
        ? `${result}\n`
        : "";
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
            3
        ) {
            continue;
        }

        const indexLine =
            lines[0].trim();

        const timingLine =
            lines[1].trim();

        if (
            !/^\d+$/.test(
                indexLine
            )
        ) {
            continue;
        }

        if (
            !/^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/.test(
                timingLine
            )
        ) {
            continue;
        }

        result.push({
            index:
                Number(
                    indexLine
                ),

            timing:
                timingLine,

            text:
                lines
                    .slice(
                        2
                    )
                    .join(
                        "\n"
                    )
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
                    index
                ) =>
                    [
                        block.index,
                        block.timing,
                        translatedTexts[
                            index
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

// ============================================================
// CACHE
// ============================================================

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

// ============================================================
// LOTES
// ============================================================

function splitIntoBatches(
    blocks
) {
    const batches = [];

    let current = [];

    let currentChars = 0;

    for (
        const block
        of blocks
    ) {
        const blockChars =
            String(
                block.text || ""
            ).length +
            50;

        const exceedChars =
            current.length >
            0 &&
            currentChars +
            blockChars >
            MAX_BATCH_CHARS;

        const exceedBlocks =
            current.length >=
            MAX_BATCH_BLOCKS;

        if (
            exceedChars ||
            exceedBlocks
        ) {
            batches.push(
                current
            );

            current = [];

            currentChars = 0;
        }

        current.push(
            block
        );

        currentChars +=
            blockChars;
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
// RATE LIMIT / COOLDOWN
// ============================================================

function getCooldownRemaining() {
    return Math.max(
        0,
        geminiCooldownUntil -
        Date.now()
    );
}

function setGeminiCooldown(
    ms
) {
    const safeMs =
        Math.min(
            Math.max(
                Number(ms) ||
                30_000,
                1000
            ),
            MAX_RATE_LIMIT_COOLDOWN_MS
        );

    geminiCooldownUntil =
        Math.max(
            geminiCooldownUntil,
            Date.now() +
            safeMs
        );

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
                MAX_RATE_LIMIT_COOLDOWN_MS
            );
        }
    }

    const message =
        String(
            errorData
                ?.error
                ?.message ||
            ""
        );

    let match =
        message.match(
            /retry in\s+([\d.]+)s/i
        );

    if (
        match
    ) {
        const seconds =
            Number(
                match[1]
            );

        if (
            Number.isFinite(
                seconds
            )
        ) {
            return Math.min(
                (
                    seconds +
                    1
                ) *
                1000,
                MAX_RATE_LIMIT_COOLDOWN_MS
            );
        }
    }

    match =
        message.match(
            /retry in\s+(\d+)m\s*(\d+(?:\.\d+)?)?s?/i
        );

    if (
        match
    ) {
        const minutes =
            Number(
                match[1]
            );

        const seconds =
            Number(
                match[2] ||
                0
            );

        return Math.min(
            (
                minutes *
                60 +
                seconds +
                1
            ) *
            1000,
            MAX_RATE_LIMIT_COOLDOWN_MS
        );
    }

    match =
        message.match(
            /retry in\s+(\d+)m/i
        );

    if (
        match
    ) {
        return Math.min(
            (
                Number(
                    match[1]
                ) *
                60 +
                1
            ) *
            1000,
            MAX_RATE_LIMIT_COOLDOWN_MS
        );
    }

    return 30_000;
}

function isRateLimitError(
    status,
    message
) {
    return (
        status ===
        429 ||
        /quota|rate.?limit|resource.?exhausted|too many requests/i.test(
            String(
                message || ""
            )
        )
    );
}

// ============================================================
// GEMINI
// ============================================================

async function rawGeminiRequest(
    prompt,
    deadlineAt
) {
    if (
        !GEMINI_API_KEY
    ) {
        throw new Error(
            "GEMINI_API_KEY não configurada."
        );
    }

    assertBeforeDeadline(
        deadlineAt
    );

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            GEMINI_MODEL
        )}:generateContent`;

    const body = {
        systemInstruction: {
            parts: [
                {
                    text:
                        "Você é um tradutor profissional de legendas cinematográficas. " +
                        "Traduza inglês para Português do Brasil. " +
                        "Seja natural, fluente e fiel ao significado. " +
                        "Preserve nomes próprios, marcas, termos técnicos, humor, gírias, palavrões, intensidade emocional e intenção. " +
                        "Não censure. Não resuma. Não explique. " +
                        "Não acrescente nomes de falantes, descrições de sons, rubricas SDH/CC ou observações que não existam no texto recebido. " +
                        "Traduza somente o campo text. Mantenha exatamente os IDs recebidos. " +
                        "Preserve tags de formatação como <i>, </i>, <b>, </b>, {\\i1}, {\\i0} e similares."
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
                            prompt
                    }
                ]
            }
        ],

        generationConfig: {
            responseMimeType:
                "application/json",

            responseSchema: {
                type:
                    "ARRAY",

                items: {
                    type:
                        "OBJECT",

                    properties: {
                        id: {
                            type:
                                "INTEGER"
                        },

                        text: {
                            type:
                                "STRING"
                        }
                    },

                    required: [
                        "id",
                        "text"
                    ]
                },

                minItems:
                    1
            },

            maxOutputTokens:
                MAX_OUTPUT_TOKENS
        }
    };

    const remaining =
        remainingBeforeDeadline(
            deadlineAt
        );

    if (
        remaining <=
        0
    ) {
        throw translationTimeoutError();
    }

    const requestTimeoutMs =
        Math.max(
            1,

            Math.min(
                GEMINI_TIMEOUT_MS,

                Number.isFinite(
                    remaining
                )
                    ? remaining
                    : GEMINI_TIMEOUT_MS
            )
        );

    const controller =
        new AbortController();

    const timer =
        setTimeout(
            () =>
                controller.abort(),
            requestTimeoutMs
        );

    let response;
    let rawText;

    try {
        response =
            await fetch(
                endpoint,
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
                        JSON.stringify(
                            body
                        ),

                    signal:
                        controller.signal
                }
            );

        rawText =
            await response.text();
    } catch (
        error
    ) {
        if (
            Number.isFinite(
                deadlineAt
            ) &&
            Date.now() >=
            deadlineAt
        ) {
            throw translationTimeoutError();
        }

        throw error;
    } finally {
        clearTimeout(
            timer
        );
    }

    assertBeforeDeadline(
        deadlineAt
    );

    let data;

    try {
        data =
            JSON.parse(
                rawText
            );
    } catch {
        const error =
            new Error(
                `Resposta não-JSON do Gemini. HTTP ${response.status}.`
            );

        error.status =
            response.status;

        throw error;
    }

    if (
        !response.ok
    ) {
        const message =
            data
                ?.error
                ?.message ||
            `HTTP ${response.status}`;

        const error =
            new Error(
                message
            );

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
        data
            ?.candidates
            ?.[0]
            ?.content
            ?.parts
            ?.map(
                part =>
                    part?.text ||
                    ""
            )
            .join(
                ""
            )
            .trim();

    if (
        !text
    ) {
        throw new Error(
            "Gemini não retornou conteúdo."
        );
    }

    return text;
}

function enqueueGemini(
    prompt,
    deadlineAt
) {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            if (
                Number.isFinite(
                    deadlineAt
                ) &&
                Date.now() >=
                deadlineAt
            ) {
                reject(
                    translationTimeoutError()
                );

                return;
            }

            geminiQueue.push({
                prompt,
                deadlineAt,
                resolve,
                reject
            });

            processGeminiQueue();
        }
    );
}

async function processGeminiQueue() {
    if (
        geminiWorkerRunning
    ) {
        return;
    }

    geminiWorkerRunning =
        true;

    try {
        while (
            geminiQueue.length
        ) {
            const item =
                geminiQueue.shift();

            if (
                !item
            ) {
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

            while (
                !finished
            ) {
                try {
                    assertBeforeDeadline(
                        item.deadlineAt
                    );

                    const cooldown =
                        getCooldownRemaining();

                    if (
                        cooldown >
                        0
                    ) {
                        console.log(
                            `[GEMINI] Fila aguardando cooldown de ${Math.ceil(
                                cooldown /
                                1000
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
                        lastGeminiRequestAt >
                        0 &&
                        sinceLast <
                        MIN_REQUEST_INTERVAL_MS
                    ) {
                        await sleepWithDeadline(
                            MIN_REQUEST_INTERVAL_MS -
                            sinceLast,

                            item.deadlineAt
                        );
                    }

                    assertBeforeDeadline(
                        item.deadlineAt
                    );

                    console.log(
                        `[GEMINI] Request ${normalAttempt}/${MAX_NORMAL_RETRIES + 1}`
                    );

                    lastGeminiRequestAt =
                        Date.now();

                    const result =
                        await rawGeminiRequest(
                            item.prompt,
                            item.deadlineAt
                        );

                    item.resolve(
                        result
                    );

                    finished =
                        true;
                } catch (
                    error
                ) {
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

                        finished =
                            true;

                        continue;
                    }

                    console.error(
                        `[GEMINI] Erro: ${getErrorMessage(
                            error
                        )}`
                    );

                    /*
                     * 429 NÃO consome
                     * uma tentativa normal.
                     */
                    if (
                        error?.rateLimit
                    ) {
                        setGeminiCooldown(
                            error.retryAfterMs ||
                            30_000
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
                                wait /
                                1000
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

                            finished =
                                true;
                        }

                        continue;
                    }

                    item.reject(
                        error
                    );

                    finished =
                        true;
                }
            }
        }
    } finally {
        geminiWorkerRunning =
            false;

        if (
            geminiQueue.length
        ) {
            processGeminiQueue();
        }
    }
}

function buildTranslationPrompt(
    blocks
) {
    const payload =
        blocks.map(
            block => ({
                id:
                    block.index,

                text:
                    block.text
            })
        );

    return `
Traduza os textos abaixo do inglês para Português do Brasil.

REGRAS OBRIGATÓRIAS:

1. Retorne exatamente um objeto para cada entrada.
2. Preserve todos os IDs exatamente.
3. Não crie nem remova IDs.
4. Traduza somente o campo "text".
5. Não escreva explicações, markdown ou texto fora do JSON.
6. Não resuma.
7. Preserve toda informação falada presente no texto recebido.
8. Preserve nomes próprios, marcas, termos técnicos, humor, gírias e palavrões.
9. Não censure.
10. Preserve tags HTML/ASS como <i>, </i>, <b>, </b>, {\\i1}, {\\i0}.
11. Use Português do Brasil natural, não tradução literal artificial.
12. Preserve sentido, intenção e intensidade emocional.
13. Não acrescente informações.
14. Não acrescente nomes de falantes, descrições de sons, rubricas SDH/CC ou observações.
15. Não misture textos entre IDs.
16. Cada ID deve receber somente a tradução do próprio texto.

RETORNE SOMENTE UM ARRAY JSON NO FORMATO:

[{"id":123,"text":"tradução"}]

ENTRADAS:

${JSON.stringify(
    payload
)}
`;
}

function badModelOutputError(
    message
) {
    const error =
        new Error(
            message
        );

    error.code =
        "BAD_MODEL_OUTPUT";

    return error;
}

async function translateBatchOnce(
    blocks,
    deadlineAt
) {
    assertBeforeDeadline(
        deadlineAt
    );

    const raw =
        await enqueueGemini(
            buildTranslationPrompt(
                blocks
            ),

            deadlineAt
        );

    let parsed;

    try {
        parsed =
            JSON.parse(
                stripCodeFences(
                    raw
                )
            );
    } catch {
        throw badModelOutputError(
            "Gemini retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(
            parsed
        )
    ) {
        throw badModelOutputError(
            "Gemini não retornou uma lista."
        );
    }

    const translatedById =
        new Map();

    for (
        const item
        of parsed
    ) {
        if (
            item &&
            Number.isInteger(
                item.id
            ) &&
            typeof item.text ===
            "string"
        ) {
            translatedById.set(
                item.id,
                item.text
            );
        }
    }

    const translated =
        blocks.map(
            block =>
                translatedById.get(
                    block.index
                )
        );

    if (
        translated.some(
            text =>
                typeof text !==
                "string"
        )
    ) {
        throw badModelOutputError(
            "Gemini não devolveu todos os blocos."
        );
    }

    return translated;
}

async function translateBatch(
    blocks,
    deadlineAt,
    splitDepth = 0
) {
    try {
        return await translateBatchOnce(
            blocks,
            deadlineAt
        );
    } catch (
        error
    ) {
        const canSplit =
            error?.code ===
            "BAD_MODEL_OUTPUT" &&
            blocks.length >=
            80 &&
            splitDepth <
            3;

        if (
            !canSplit
        ) {
            throw error;
        }

        assertBeforeDeadline(
            deadlineAt
        );

        const middle =
            Math.ceil(
                blocks.length /
                2
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
            `[TRANSLATE] Saída inválida em lote de ${blocks.length} blocos. Dividindo em ${left.length} + ${right.length}.`
        );

        const translatedLeft =
            await translateBatch(
                left,
                deadlineAt,
                splitDepth + 1
            );

        const translatedRight =
            await translateBatch(
                right,
                deadlineAt,
                splitDepth + 1
            );

        return [
            ...translatedLeft,
            ...translatedRight
        ];
    }
}

async function translateSrt(
    originalSrt,
    job
) {
    const blocks =
        parseSrt(
            originalSrt
        );

    if (
        !blocks.length
    ) {
        throw new Error(
            "Nenhum bloco SRT válido."
        );
    }

    const batches =
        splitIntoBatches(
            blocks
        );

    const translatedTexts =
        new Array(
            blocks.length
        );

    const originalPositions =
        new Map(
            blocks.map(
                (
                    block,
                    index
                ) =>
                    [
                        block,
                        index
                    ]
            )
        );

    const startedAt =
        Date.now();

    const deadlineAt =
        Number.isFinite(
            job?.deadlineAt
        )
            ? job.deadlineAt
            : startedAt +
            MAX_TRANSLATION_TIME_MS;

    console.log(
        `[TRANSLATE] ${blocks.length} blocos.`
    );

    console.log(
        `[TRANSLATE] ${batches.length} lote(s).`
    );

    console.log(
        `[TRANSLATE] Limite: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} caracteres.`
    );

    for (
        let batchIndex = 0;
        batchIndex <
        batches.length;
        batchIndex++
    ) {
        assertBeforeDeadline(
            deadlineAt
        );

        const batch =
            batches[
                batchIndex
            ];

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

        console.log(
            `[TRANSLATE] Lote ${batchIndex + 1}/${batches.length} - ${batch.length} blocos / ${batchChars} caracteres.`
        );

        const translated =
            await translateBatch(
                batch,
                deadlineAt
            );

        for (
            let i = 0;
            i <
            batch.length;
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

        job.progress =
            Math.round(
                (
                    (
                        batchIndex +
                        1
                    ) /
                    batches.length
                ) *
                100
            );

        job.completedBatches =
            batchIndex +
            1;

        job.totalBatches =
            batches.length;

        job.updatedAt =
            Date.now();

        console.log(
            `[TRANSLATE] Lote ${batchIndex + 1}/${batches.length} concluído.`
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
            "A tradução terminou com blocos ausentes."
        );
    }

    console.log(
        `[TRANSLATE] Finalizada em ${(
            (
                Date.now() -
                startedAt
            ) /
            1000
        ).toFixed(
            1
        )}s.`
    );

    return buildSrt(
        blocks,
        translatedTexts
    );
}

// ============================================================
// JOBS
// ============================================================

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

        /*
         * O prazo total começa
         * no momento em que o job nasce.
         *
         * Portanto tempo na fila
         * também conta nos 8 minutos.
         */
        deadlineAt:
            now +
            MAX_TRANSLATION_TIME_MS,

        expiresAt:
            now +
            JOB_TTL_MS,

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

function getJob(
    jobId
) {
    const job =
        jobs.get(
            jobId
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
            jobId
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

async function processJob(
    job
) {
    console.log(
        `[JOB ${job.id}] Iniciando.`
    );

    try {
        const cached =
            getTranslationCache(
                job.cacheKey
            );

        if (
            cached
        ) {
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
    } catch (
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
    }
}

function buildProcessingSrt(
    job
) {
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
    ].join(
        "\n"
    );
}

function buildErrorSrt(
    message
) {
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
    ].join(
        "\n"
    );
}

function sendSubtitleResponse(
    res,
    srt,
    cacheControl =
        "no-store"
) {
    res.status(
        200
    );

    res.set({
        "Content-Type":
            "text/plain; charset=utf-8",

        "Cache-Control":
            cacheControl,

        "Access-Control-Allow-Origin":
            "*"
    });

    return res.send(
        srt
    );
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
        await sleep(
            500
        );
    }

    return (
        job.status ===
        "completed"
    );
}

// ============================================================
// MANIFEST / STATUS
// ============================================================

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "5.1.0",

    name:
        "Tradutor Gemini PT-BR",

    description:
        "Traduz automaticamente legendas em inglês para Português do Brasil usando Gemini.",

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
            name:
                manifest.name,

            version:
                manifest.version,

            status:
                "online",

            model:
                GEMINI_MODEL,

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

            maxTranslationTimeMs:
                MAX_TRANSLATION_TIME_MS
        });
    }
);

// ============================================================
// OPENSUBTITLES
// ============================================================

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
    } else if (
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
    id
) {
    const searchUrl =
        `https://opensubtitles-v3.strem.io/subtitles/${encodeURIComponent(
            type
        )}/${encodeURIComponent(
            id
        )}.json`;

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
                        "Stremio-Gemini-Subtitle-Translator/5.1"
                }
            },

            SOURCE_FETCH_TIMEOUT_MS
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

    return selectBestSubtitle(
        data?.subtitles ||
        []
    );
}

async function downloadSubtitle(
    url
) {
    console.log(
        `[SOURCE] Baixando legenda: ${url}`
    );

    const response =
        await fetchWithTimeout(
            url,

            {
                headers: {
                    "User-Agent":
                        "Stremio-Gemini-Subtitle-Translator/5.1"
                }
            },

            SOURCE_FETCH_TIMEOUT_MS
        );

    if (
        !response.ok
    ) {
        throw new Error(
            `Falha ao baixar legenda: HTTP ${response.status}.`
        );
    }

    const text =
        normalizeSrt(
            await response.text()
        );

    if (
        !text
    ) {
        throw new Error(
            "Legenda vazia."
        );
    }

    if (
        text.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda muito grande: ${text.length} caracteres.`
        );
    }

    return text;
}

function createCachedJob({
    jobId,
    cacheKey,
    type,
    videoId,
    sourceHash,
    sourceSrt,
    cached
}) {
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
                sourceSrt
            });

        job.status =
            "completed";

        job.result =
            cached;

        job.progress =
            100;

        job.updatedAt =
            Date.now();
    }

    return job;
}

// ============================================================
// JOB EMBUTIDO
// ============================================================

async function createEmbeddedTranslationJob({
    type,
    videoId,
    sourceSrt,
    sourceName =
        "embedded"
}) {
    const rawSrt =
        normalizeSrt(
            sourceSrt
        );

    if (
        !rawSrt
    ) {
        throw new Error(
            "A legenda embutida está vazia."
        );
    }

    if (
        rawSrt.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda embutida muito grande: ${rawSrt.length} caracteres.`
        );
    }

    /*
     * Limpeza antes do hash
     * e antes do Gemini.
     */
    const normalizedSrt =
        cleanSrtForTranslation(
            rawSrt
        );

    if (
        !normalizedSrt
    ) {
        throw new Error(
            "A legenda ficou vazia após a limpeza SDH/CC."
        );
    }

    const blocks =
        parseSrt(
            normalizedSrt
        );

    if (
        !blocks.length
    ) {
        throw new Error(
            "A legenda embutida não possui blocos SRT válidos."
        );
    }

    const sourceHash =
        sha256(
            normalizedSrt
        );

    /*
     * Cache baseado no conteúdo
     * limpo da legenda.
     */
    const cacheKey =
        `embedded:${sourceHash}`;

    const cached =
        getTranslationCache(
            cacheKey
        );

    if (
        cached
    ) {
        const cachedJobId =
            `embedded-cached-${sourceHash.slice(
                0,
                24
            )}`;

        console.log(
            `[EMBEDDED] Cache utilizado para ${sourceName}.`
        );

        return createCachedJob({
            jobId:
                cachedJobId,

            cacheKey,

            type,

            videoId,

            sourceHash,

            sourceSrt:
                normalizedSrt,

            cached
        });
    }

    const existingJob =
        findProcessingJob(
            cacheKey
        );

    if (
        existingJob
    ) {
        console.log(
            `[EMBEDDED] Job existente reutilizado: ${existingJob.id}`
        );

        return existingJob;
    }

    const jobId =
        `embedded-${sourceHash.slice(
            0,
            24
        )}-${randomId(
            8
        )}`;

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

    job.totalBatches =
        splitIntoBatches(
            blocks
        ).length;

    job.promise =
        processJob(
            job
        ).catch(
            error => {
                console.error(
                    `[EMBEDDED JOB ${job.id}] Erro inesperado:`,
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

    console.log(
        `[EMBEDDED] Novo job ${job.id} criado.`
    );

    return job;
}

// ============================================================
// STREMIO / OPENSUBTITLES
// ============================================================

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

    const rawExtra =
        String(
            req.params.extra ||
            ""
        ).trim();

    if (
        rawExtra
    ) {
        const params =
            new URLSearchParams(
                rawExtra
            );

        console.log(
            `[STREMIO EXTRA] filename: ${
                params.get(
                    "filename"
                ) ||
                "(não enviado)"
            }`
        );

        console.log(
            `[STREMIO EXTRA] videoSize: ${
                params.get(
                    "videoSize"
                ) ||
                "(não enviado)"
            }`
        );

        console.log(
            `[STREMIO EXTRA] videoHash: ${
                params.get(
                    "videoHash"
                ) ||
                "(não enviado)"
            }`
        );
    } else {
        console.log(
            "[STREMIO EXTRA] Nenhuma informação extra recebida."
        );
    }

    if (
        !type ||
        !id
    ) {
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
                id
            );

        if (
            !target
        ) {
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

        /*
         * Baixa a legenda original.
         */
        const downloadedSrt =
            await downloadSubtitle(
                target.url
            );

        /*
         * Limpeza SDH/CC também vale
         * para OpenSubtitles.
         */
        const sourceSrt =
            cleanSrtForTranslation(
                downloadedSrt
            );

        if (
            !sourceSrt
        ) {
            console.log(
                "[STREMIO] Legenda vazia após limpeza SDH/CC."
            );

            return safeJson(
                res,
                {
                    subtitles: []
                }
            );
        }

        const blocks =
            parseSrt(
                sourceSrt
            );

        if (
            !blocks.length
        ) {
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
            `${type}:${id}:${sourceHash}`;

        const baseUrl =
            cleanBaseUrl(
                req
            );

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

            createCachedJob({
                jobId,
                cacheKey,
                type,

                videoId:
                    id,

                sourceHash,
                sourceSrt,
                cached
            });

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

        if (
            !job
        ) {
            const jobId =
                `job-${sourceHash.slice(
                    0,
                    24
                )}-${randomId(
                    8
                )}`;

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

            job.promise =
                processJob(
                    job
                ).catch(
                    error => {
                        console.error(
                            `[JOB ${job.id}] Erro inesperado:`,
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
    } catch (
        error
    ) {
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
);

// ============================================================
// API LOCAL -> RENDER
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
        } catch (
            error
        ) {
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

// ============================================================
// RESULTADO SRT
// ============================================================

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

    if (
        !jobId
    ) {
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

    /*
     * Esperamos até 15 segundos
     * nesta conexão.
     *
     * O job continua no Render.
     */
    const completed =
        await waitForJob(
            job,
            15_000
        );

    if (
        completed &&
        job.status ===
        "completed" &&
        job.result
    ) {
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

// ============================================================
// START
// ============================================================

app.listen(
    PORT,
    () => {
        console.log(
            "=============================================="
        );

        console.log(
            " STREMIO GEMINI SUBTITLE TRANSLATOR 5.1"
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
            `PUBLIC_URL: ${
                PUBLIC_URL ||
                "(automático)"
            }`
        );

        console.log(
            `Batch máximo: ${MAX_BATCH_BLOCKS} blocos`
        );

        console.log(
            `Batch máximo: ${MAX_BATCH_CHARS} caracteres`
        );

        console.log(
            `Saída máxima Gemini: ${MAX_OUTPUT_TOKENS} tokens`
        );

        console.log(
            `Intervalo Gemini: ${MIN_REQUEST_INTERVAL_MS}ms`
        );

        console.log(
            `Concorrência Gemini: ${GEMINI_CONCURRENCY}`
        );

        console.log(
            `Timeout por request Gemini: ${GEMINI_TIMEOUT_MS}ms`
        );

        console.log(
            `Teto total tradução: ${MAX_TRANSLATION_TIME_MS}ms`
        );

        console.log(
            `Cache TTL: ${
                CACHE_TTL_MS /
                3600000
            }h`
        );

        console.log(
            "Limpeza SDH/CC: ATIVA"
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
