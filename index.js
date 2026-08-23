const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();

app.use(cors());
app.disable("x-powered-by");

/*
|--------------------------------------------------------------------------
| CONFIGURAÇÃO
|--------------------------------------------------------------------------
*/

const PORT = Number(process.env.PORT || 10000);

const GEMINI_API_KEY =
    process.env.GEMINI_API_KEY || "";

const GEMINI_MODEL =
    process.env.GEMINI_MODEL || "gemini-3.6-flash";

/*
 * URL pública do Render.
 *
 * Exemplo:
 *
 * PUBLIC_URL=https://meu-addon.onrender.com
 *
 * Se não definir, tentamos descobrir pelo request.
 */
const PUBLIC_URL =
    (process.env.PUBLIC_URL || "").replace(/\/+$/, "");

/*
|--------------------------------------------------------------------------
| LIMITES DE REDE
|--------------------------------------------------------------------------
*/

const SOURCE_FETCH_TIMEOUT_MS =
    Number(process.env.SOURCE_FETCH_TIMEOUT_MS || 20_000);

const GEMINI_TIMEOUT_MS =
    Number(process.env.GEMINI_TIMEOUT_MS || 60_000);

/*
 * Quanto tempo o endpoint /subtitle/:jobId.srt
 * espera antes de devolver uma legenda de processamento.
 */
const WAIT_FOR_TRANSLATION_MS =
    Number(process.env.WAIT_FOR_TRANSLATION_MS || 25_000);

/*
|--------------------------------------------------------------------------
| CACHE E JOBS
|--------------------------------------------------------------------------
*/

const CACHE_TTL_MS =
    Number(
        process.env.CACHE_TTL_MS ||
        24 * 60 * 60 * 1000
    );

const JOB_TTL_MS =
    Number(
        process.env.JOB_TTL_MS ||
        2 * 60 * 60 * 1000
    );

const MAX_CACHE_ENTRIES =
    Number(process.env.MAX_CACHE_ENTRIES || 500);

const MAX_JOBS =
    Number(process.env.MAX_JOBS || 500);

/*
|--------------------------------------------------------------------------
| LIMITES DA TRADUÇÃO
|--------------------------------------------------------------------------
|
| Não usamos somente quantidade de blocos.
|
| Um lote precisa respeitar:
|
| - MAX_BATCH_BLOCKS
| - MAX_BATCH_CHARS
|
| O primeiro limite atingido encerra o lote.
|--------------------------------------------------------------------------
*/

const MAX_BATCH_BLOCKS =
    Number(process.env.MAX_BATCH_BLOCKS || 30);

const MAX_BATCH_CHARS =
    Number(process.env.MAX_BATCH_CHARS || 12_000);

const MAX_SOURCE_CHARS =
    Number(process.env.MAX_SOURCE_CHARS || 300_000);

/*
|--------------------------------------------------------------------------
| RATE LIMIT DO GEMINI
|--------------------------------------------------------------------------
|
| Como a API é gratuita, preferimos segurança.
|
| Por padrão:
|
| - somente 1 requisição Gemini simultânea;
| - intervalo mínimo entre requisições;
| - retry limitado.
|
| IMPORTANTE:
|
| Os limites reais de RPM/TPM/RPD dependem do projeto
| e podem ser diferentes. Consulte o AI Studio.
|--------------------------------------------------------------------------
*/

const MAX_CONCURRENT_GEMINI =
    Math.max(
        1,
        Number(process.env.MAX_CONCURRENT_GEMINI || 1)
    );

const MIN_GEMINI_INTERVAL_MS =
    Math.max(
        0,
        Number(process.env.MIN_GEMINI_INTERVAL_MS || 3500)
    );

const MAX_GEMINI_RETRIES =
    Math.max(
        0,
        Number(process.env.MAX_GEMINI_RETRIES || 2)
    );

const RETRY_BASE_MS =
    Math.max(
        500,
        Number(process.env.RETRY_BASE_MS || 4000)
    );

/*
 * Número máximo de tentativas para corrigir JSON
 * ou resultado estrutural inválido.
 *
 * Isso é separado do retry HTTP.
 */
const MAX_TRANSLATION_RETRIES =
    Math.max(
        0,
        Number(
            process.env.MAX_TRANSLATION_RETRIES || 1
        )
    );

/*
|--------------------------------------------------------------------------
| PROTEÇÃO CONTRA EXCESSO DE JOBS
|--------------------------------------------------------------------------
*/

const MAX_ACTIVE_JOBS =
    Math.max(
        1,
        Number(process.env.MAX_ACTIVE_JOBS || 3)
    );

/*
|--------------------------------------------------------------------------
| ESTRUTURAS EM MEMÓRIA
|--------------------------------------------------------------------------
*/

const translationCache = new Map();

const jobs = new Map();

/*
 * Relação:
 *
 * cacheKey -> jobId
 *
 * Isso evita procurar todos os jobs.
 */
const jobsByCacheKey = new Map();

/*
|--------------------------------------------------------------------------
| FILA GLOBAL DO GEMINI
|--------------------------------------------------------------------------
*/

const geminiQueue = [];

let activeGeminiRequests = 0;

let lastGeminiRequestAt = 0;

/*
|--------------------------------------------------------------------------
| ESTATÍSTICAS
|--------------------------------------------------------------------------
*/

const stats = {
    geminiRequests: 0,
    geminiSuccess: 0,
    geminiErrors: 0,
    geminiRateLimited: 0,

    translationsStarted: 0,
    translationsCompleted: 0,
    translationsFailed: 0,

    cacheHits: 0,

    sourceDownloads: 0,

    startedAt: Date.now()
};

/*
|--------------------------------------------------------------------------
| MANIFEST
|--------------------------------------------------------------------------
*/

const manifest = {
    id: "org.tradutor.stateless.gemini.async",
    version: "4.0.0",

    name: "Tradutor Gemini Async",

    description:
        "Traduz automaticamente legendas em inglês para Português do Brasil usando Google Gemini.",

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

/*
|--------------------------------------------------------------------------
| HELPERS
|--------------------------------------------------------------------------
*/

function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}

function sha256(text) {
    return crypto
        .createHash("sha256")
        .update(text, "utf8")
        .digest("hex");
}

function randomId(length = 16) {
    return crypto
        .randomBytes(length)
        .toString("hex");
}

function cleanBaseUrl(req) {
    if (PUBLIC_URL) {
        return PUBLIC_URL;
    }

    const protocol =
        req.headers["x-forwarded-proto"] ||
        req.protocol ||
        "https";

    const host =
        req.headers["x-forwarded-host"] ||
        req.get("host");

    return `${protocol}://${host}`;
}

function safeJson(
    res,
    data,
    status = 200
) {
    res.status(status);

    res.set(
        "Cache-Control",
        "no-store"
    );

    return res.json(data);
}

function getErrorMessage(error) {
    if (!error) {
        return "Erro desconhecido";
    }

    if (typeof error === "string") {
        return error;
    }

    return (
        error.message ||
        error.statusText ||
        "Erro desconhecido"
    );
}

/*
|--------------------------------------------------------------------------
| FETCH COM TIMEOUT
|--------------------------------------------------------------------------
*/

async function fetchWithTimeout(
    url,
    options = {},
    timeoutMs = 20_000
) {
    const controller =
        new AbortController();

    const timer =
        setTimeout(() => {
            controller.abort();
        }, timeoutMs);

    try {
        return await fetch(
            url,
            {
                ...options,
                signal: controller.signal
            }
        );
    } finally {
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

    /*
     * CACHE
     */
    for (
        const [key, item]
        of translationCache.entries()
    ) {
        if (item.expiresAt <= now) {
            translationCache.delete(key);
        }
    }

    /*
     * JOBS
     */
    for (
        const [key, job]
        of jobs.entries()
    ) {
        if (
            job.expiresAt <= now &&
            job.status !== "processing"
        ) {
            jobs.delete(key);

            if (
                jobsByCacheKey.get(
                    job.cacheKey
                ) === key
            ) {
                jobsByCacheKey.delete(
                    job.cacheKey
                );
            }
        }
    }

    /*
     * Limite do cache.
     */
    while (
        translationCache.size >
        MAX_CACHE_ENTRIES
    ) {
        const firstKey =
            translationCache
                .keys()
                .next()
                .value;

        if (
            firstKey === undefined
        ) {
            break;
        }

        translationCache.delete(
            firstKey
        );
    }

    /*
     * Limite dos jobs.
     *
     * Nunca remove processamento ativo.
     */
    while (
        jobs.size >
        MAX_JOBS
    ) {
        let removed = false;

        for (
            const [key, job]
            of jobs.entries()
        ) {
            if (
                job.status !== "processing"
            ) {
                jobs.delete(key);

                if (
                    jobsByCacheKey.get(
                        job.cacheKey
                    ) === key
                ) {
                    jobsByCacheKey.delete(
                        job.cacheKey
                    );
                }

                removed = true;
                break;
            }
        }

        if (!removed) {
            break;
        }
    }
}

setInterval(
    cleanupMemory,
    5 * 60 * 1000
).unref();

/*
|--------------------------------------------------------------------------
| NORMALIZAÇÃO SRT
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

/*
|--------------------------------------------------------------------------
| PARSER SRT
|--------------------------------------------------------------------------
*/

function parseSrt(srt) {
    const normalized =
        normalizeSrt(srt);

    if (!normalized) {
        return [];
    }

    const blocks =
        normalized
            .split(/\n{2,}/)
            .map(block => block.trim())
            .filter(Boolean);

    const result = [];

    for (
        const block
        of blocks
    ) {
        const lines =
            block.split("\n");

        if (lines.length < 3) {
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
            !/^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/
                .test(timingLine)
        ) {
            continue;
        }

        const textLines =
            lines.slice(2);

        result.push({
            index: Number(indexLine),

            timing:
                timingLine,

            text:
                textLines.join("\n")
        });
    }

    return result;
}

/*
|--------------------------------------------------------------------------
| RECONSTRUÇÃO SRT
|--------------------------------------------------------------------------
*/

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
                ) => {
                    const translated =
                        translatedTexts[index] ??
                        block.text;

                    return [
                        block.index,
                        block.timing,
                        translated
                    ].join("\n");
                }
            )
            .join("\n\n")
            .trim() + "\n"
    );
}

/*
|--------------------------------------------------------------------------
| VALIDAÇÃO
|--------------------------------------------------------------------------
*/

function validateTranslation(
    originalBlocks,
    translatedTexts
) {
    if (
        !Array.isArray(
            translatedTexts
        )
    ) {
        return {
            valid: false,
            reason:
                "Resultado não é array."
        };
    }

    if (
        translatedTexts.length !==
        originalBlocks.length
    ) {
        return {
            valid: false,
            reason:
                `Quantidade incorreta. Esperado ${originalBlocks.length}, recebido ${translatedTexts.length}.`
        };
    }

    for (
        let i = 0;
        i < translatedTexts.length;
        i++
    ) {
        const text =
            translatedTexts[i];

        if (
            typeof text !==
            "string"
        ) {
            return {
                valid: false,
                reason:
                    `Bloco ${i + 1} não é texto.`
            };
        }

        /*
         * Texto vazio só é aceitável se o original
         * também estiver vazio.
         */
        if (
            !text.trim() &&
            originalBlocks[i]
                .text
                .trim()
        ) {
            return {
                valid: false,
                reason:
                    `Bloco ${originalBlocks[i].index} ficou vazio.`
            };
        }

        /*
         * Não permitimos que o Gemini transforme
         * um bloco em dezenas de linhas.
         */
        const originalLines =
            originalBlocks[i]
                .text
                .split("\n")
                .length;

        const translatedLines =
            text
                .split("\n")
                .length;

        if (
            Math.abs(
                originalLines -
                translatedLines
            ) > 1
        ) {
            return {
                valid: false,
                reason:
                    `Alteração excessiva de linhas no bloco ${originalBlocks[i].index}.`
            };
        }
    }

    return {
        valid: true
    };
}

/*
|--------------------------------------------------------------------------
| SELEÇÃO DE LEGENDA
|--------------------------------------------------------------------------
*/

function scoreSubtitle(
    subtitle
) {
    let score = 0;

    const lang =
        String(
            subtitle?.lang || ""
        ).toLowerCase();

    if (lang === "eng") {
        score += 100;
    } else if (lang === "en") {
        score += 90;
    }

    if (
        subtitle?.hearingImpaired ===
        false
    ) {
        score += 20;
    }

    if (
        subtitle?.format &&
        String(
            subtitle.format
        ).toLowerCase() ===
            "srt"
    ) {
        score += 20;
    }

    if (
        subtitle?.name &&
        /english/i.test(
            String(
                subtitle.name
            )
        )
    ) {
        score += 10;
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

    const candidates =
        subtitles
            .filter(sub => {
                const lang =
                    String(
                        sub?.lang || ""
                    ).toLowerCase();

                return (
                    (
                        lang === "eng" ||
                        lang === "en"
                    ) &&
                    typeof sub?.url ===
                        "string" &&
                    /^https?:\/\//i.test(
                        sub.url
                    )
                );
            })
            .sort(
                (
                    a,
                    b
                ) =>
                    scoreSubtitle(b) -
                    scoreSubtitle(a)
            );

    return (
        candidates[0] ||
        null
    );
}

/*
|--------------------------------------------------------------------------
| DOWNLOAD DA LEGENDA
|--------------------------------------------------------------------------
*/

async function downloadSubtitle(
    url
) {
    console.log(
        `[SOURCE] Baixando legenda: ${url}`
    );

    stats.sourceDownloads++;

    const response =
        await fetchWithTimeout(
            url,
            {
                headers: {
                    "User-Agent":
                        "Stremio-Gemini-Subtitle-Translator/4.0"
                }
            },
            SOURCE_FETCH_TIMEOUT_MS
        );

    if (!response.ok) {
        throw new Error(
            `Falha ao baixar legenda: HTTP ${response.status}`
        );
    }

    const text =
        normalizeSrt(
            await response.text()
        );

    if (!text) {
        throw new Error(
            "A legenda baixada está vazia."
        );
    }

    if (
        text.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda muito grande (${text.length} caracteres). Limite: ${MAX_SOURCE_CHARS}.`
        );
    }

    return text;
}

/*
|--------------------------------------------------------------------------
| CONSTRUÇÃO DOS LOTES
|--------------------------------------------------------------------------
|
| Não usamos somente número de blocos.
|
| Um lote termina quando atingir:
|
| - MAX_BATCH_BLOCKS
| OU
| - MAX_BATCH_CHARS
|--------------------------------------------------------------------------
*/

function createBatches(
    blocks
) {
    const batches = [];

    let current = [];

    let currentChars = 0;

    for (
        const block
        of blocks
    ) {
        const blockPayload =
            JSON.stringify({
                id: block.index,
                text: block.text
            });

        const blockChars =
            blockPayload.length;

        const exceedsBlocks =
            current.length >=
            MAX_BATCH_BLOCKS;

        const exceedsChars =
            current.length > 0 &&
            currentChars +
                blockChars >
                MAX_BATCH_CHARS;

        if (
            exceedsBlocks ||
            exceedsChars
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
        current.length > 0
    ) {
        batches.push(
            current
        );
    }

    return batches;
}

/*
|--------------------------------------------------------------------------
| SCHEMA GEMINI
|--------------------------------------------------------------------------
*/

const translationResponseSchema = {
    type: "ARRAY",

    items: {
        type: "OBJECT",

        properties: {
            id: {
                type: "INTEGER"
            },

            text: {
                type: "STRING"
            }
        },

        required: [
            "id",
            "text"
        ]
    }
};

/*
|--------------------------------------------------------------------------
| CLASSIFICAÇÃO DE ERROS
|--------------------------------------------------------------------------
*/

function classifyGeminiError(
    error,
    status
) {
    const message =
        String(
            getErrorMessage(error)
        ).toLowerCase();

    if (
        status === 429 ||
        message.includes(
            "resource_exhausted"
        ) ||
        message.includes(
            "rate limit"
        ) ||
        message.includes(
            "quota"
        )
    ) {
        return "rate_limit";
    }

    if (
        status === 400
    ) {
        return "bad_request";
    }

    if (
        status === 401 ||
        status === 403
    ) {
        return "auth";
    }

    if (
        status >= 500
    ) {
        return "server";
    }

    if (
        message.includes(
            "timeout"
        ) ||
        message.includes(
            "aborted"
        )
    ) {
        return "timeout";
    }

    return "unknown";
}

/*
|--------------------------------------------------------------------------
| FILA GEMINI
|--------------------------------------------------------------------------
*/

function enqueueGemini(
    task
) {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            geminiQueue.push({
                task,
                resolve,
                reject
            });

            processGeminiQueue();
        }
    );
}

async function processGeminiQueue() {
    while (
        activeGeminiRequests <
            MAX_CONCURRENT_GEMINI &&
        geminiQueue.length > 0
    ) {
        const item =
            geminiQueue.shift();

        activeGeminiRequests++;

        try {
            const now =
                Date.now();

            const elapsed =
                now -
                lastGeminiRequestAt;

            if (
                elapsed <
                MIN_GEMINI_INTERVAL_MS
            ) {
                await sleep(
                    MIN_GEMINI_INTERVAL_MS -
                        elapsed
                );
            }

            lastGeminiRequestAt =
                Date.now();

            const result =
                await item.task();

            item.resolve(
                result
            );
        } catch (
            error
        ) {
            item.reject(
                error
            );
        } finally {
            activeGeminiRequests--;

            /*
             * Continua processando a fila.
             */
            setImmediate(
                processGeminiQueue
            );
        }
    }
}

/*
|--------------------------------------------------------------------------
| GEMINI REQUEST
|--------------------------------------------------------------------------
*/

async function callGeminiDirect(
    prompt
) {
    if (
        !GEMINI_API_KEY
    ) {
        throw new Error(
            "GEMINI_API_KEY não foi configurada no Render."
        );
    }

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            GEMINI_MODEL
        )}:generateContent`;

    let lastError =
        null;

    for (
        let attempt = 1;
        attempt <=
            MAX_GEMINI_RETRIES + 1;
        attempt++
    ) {
        try {
            stats.geminiRequests++;

            console.log(
                `[GEMINI] Request ${attempt}/${MAX_GEMINI_RETRIES + 1}`
            );

            const response =
                await fetchWithTimeout(
                    endpoint,
                    {
                        method: "POST",

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
                                                "Traduza legendas de inglês para Português do Brasil. Preserve significado, tom, nomes próprios, gírias, palavrões, tags de formatação e quebras de linha. Não resuma, não censure, não explique. Responda exclusivamente no JSON definido pelo schema."
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

                                    responseSchema:
                                        translationResponseSchema,

                                    thinkingConfig: {
                                        thinkingLevel:
                                            "low"
                                    },

                                    maxOutputTokens:
                                        8192
                                }
                            })
                    },
                    GEMINI_TIMEOUT_MS
                );

            const rawText =
                await response.text();

            let data;

            try {
                data =
                    JSON.parse(
                        rawText
                    );
            } catch {
                const error =
                    new Error(
                        `Gemini retornou resposta não-JSON. HTTP ${response.status}.`
                    );

                error.status =
                    response.status;

                throw error;
            }

            if (
                !response.ok
            ) {
                const apiMessage =
                    data?.error?.message ||
                    `HTTP ${response.status}`;

                const error =
                    new Error(
                        apiMessage
                    );

                error.status =
                    response.status;

                error.retryAfter =
                    response.headers.get(
                        "retry-after"
                    );

                throw error;
            }

            const text =
                data?.candidates?.[0]
                    ?.content
                    ?.parts
                    ?.map(
                        part =>
                            part?.text ||
                            ""
                    )
                    .join("")
                    .trim();

            if (!text) {
                throw new Error(
                    "Gemini não retornou conteúdo."
                );
            }

            stats.geminiSuccess++;

            return text;
        } catch (
            error
        ) {
            lastError =
                error;

            stats.geminiErrors++;

            const category =
                classifyGeminiError(
                    error,
                    error?.status
                );

            if (
                category ===
                "rate_limit"
            ) {
                stats.geminiRateLimited++;
            }

            console.error(
                `[GEMINI] Erro (${category}): ${getErrorMessage(error)}`
            );

            /*
             * Nunca insistir em erros permanentes.
             */
            if (
                category ===
                    "auth" ||
                category ===
                    "bad_request"
            ) {
                break;
            }

            if (
                attempt >
                MAX_GEMINI_RETRIES
            ) {
                break;
            }

            /*
             * Para 429 respeitamos Retry-After
             * quando disponível.
             */
            let waitMs =
                RETRY_BASE_MS *
                Math.pow(
                    2,
                    attempt - 1
                );

            const retryAfter =
                Number(
                    error?.retryAfter
                );

            if (
                Number.isFinite(
                    retryAfter
                ) &&
                retryAfter > 0
            ) {
                waitMs =
                    Math.min(
                        retryAfter *
                            1000,
                        60_000
                    );
            }

            /*
             * Jitter.
             */
            waitMs +=
                Math.floor(
                    Math.random() *
                        1000
                );

            /*
             * Rate limit merece uma espera
             * maior.
             */
            if (
                category ===
                "rate_limit"
            ) {
                waitMs =
                    Math.max(
                        waitMs,
                        10_000
                    );
            }

            console.log(
                `[GEMINI] Retry em ${waitMs}ms`
            );

            await sleep(
                waitMs
            );
        }
    }

    throw (
        lastError ||
        new Error(
            "Falha desconhecida no Gemini."
        )
    );
}

/*
|--------------------------------------------------------------------------
| GEMINI COM FILA
|--------------------------------------------------------------------------
*/

async function callGemini(
    prompt
) {
    return enqueueGemini(
        () =>
            callGeminiDirect(
                prompt
            )
    );
}

/*
|--------------------------------------------------------------------------
| TRADUÇÃO DE UM LOTE
|--------------------------------------------------------------------------
*/

async function translateBatch(
    blocks,
    attempt = 0
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

    const prompt = `
Traduza para Português do Brasil.

Regras:
- devolva exatamente um objeto por bloco;
- mantenha cada id exatamente igual;
- traduza somente text;
- preserve tags como <i>, </i>, <b>, </b>, {\i1}, {\i0};
- preserve quebras de linha quando possível;
- mantenha nomes próprios;
- preserve tom, gírias, palavrões e intenção;
- não resuma;
- não explique;
- não censure.

JSON:
${JSON.stringify(payload)}
`;

    const raw =
        await callGemini(
            prompt
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
        if (
            attempt <
            MAX_TRANSLATION_RETRIES
        ) {
            console.warn(
                "[TRANSLATE] JSON inválido. Repetindo lote."
            );

            return translateBatch(
                blocks,
                attempt + 1
            );
        }

        throw new Error(
            "Gemini retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(
            parsed
        )
    ) {
        if (
            attempt <
            MAX_TRANSLATION_RETRIES
        ) {
            return translateBatch(
                blocks,
                attempt + 1
            );
        }

        throw new Error(
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

    /*
     * Verifica IDs.
     */
    const translatedTexts =
        blocks.map(
            block =>
                translatedById.get(
                    block.index
                )
        );

    if (
        translatedTexts.some(
            text =>
                typeof text !==
                "string"
        )
    ) {
        if (
            attempt <
            MAX_TRANSLATION_RETRIES
        ) {
            console.warn(
                "[TRANSLATE] Gemini perdeu blocos. Repetindo."
            );

            return translateBatch(
                blocks,
                attempt + 1
            );
        }

        throw new Error(
            "Gemini não devolveu todos os blocos."
        );
    }

    const validation =
        validateTranslation(
            blocks,
            translatedTexts
        );

    if (
        !validation.valid
    ) {
        if (
            attempt <
            MAX_TRANSLATION_RETRIES
        ) {
            console.warn(
                `[TRANSLATE] Validação falhou: ${validation.reason}`
            );

            return translateBatch(
                blocks,
                attempt + 1
            );
        }

        throw new Error(
            `Resultado inválido: ${validation.reason}`
        );
    }

    return translatedTexts;
}

/*
|--------------------------------------------------------------------------
| TRADUÇÃO COMPLETA
|--------------------------------------------------------------------------
*/

async function translateSrt(
    originalSrt
) {
    const blocks =
        parseSrt(
            originalSrt
        );

    if (
        blocks.length === 0
    ) {
        throw new Error(
            "Não foi possível identificar blocos SRT válidos."
        );
    }

    console.log(
        `[TRANSLATE] ${blocks.length} blocos.`
    );

    const batches =
        createBatches(
            blocks
        );

    console.log(
        `[TRANSLATE] ${batches.length} lote(s).`
    );

    const translatedTexts =
        [];

    for (
        let i = 0;
        i < batches.length;
        i++
    ) {
        const batch =
            batches[i];

        console.log(
            `[TRANSLATE] Lote ${i + 1}/${batches.length} - ${batch.length} blocos.`
        );

        const translated =
            await translateBatch(
                batch
            );

        translatedTexts.push(
            ...translated
        );
    }

    if (
        translatedTexts.length !==
        blocks.length
    ) {
        throw new Error(
            "Quantidade final de traduções incorreta."
        );
    }

    const translatedSrt =
        buildSrt(
            blocks,
            translatedTexts
        );

    /*
     * Validação final do SRT.
     */
    const finalBlocks =
        parseSrt(
            translatedSrt
        );

    if (
        finalBlocks.length !==
        blocks.length
    ) {
        throw new Error(
            "O SRT reconstruído perdeu blocos."
        );
    }

    return translatedSrt;
}

/*
|--------------------------------------------------------------------------
| CACHE
|--------------------------------------------------------------------------
*/

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

    /*
     * Atualiza posição no Map.
     *
     * Isso transforma o acesso em algo
     * próximo de LRU.
     */
    translationCache.delete(
        key
    );

    translationCache.set(
        key,
        item
    );

    stats.cacheHits++;

    return item.srt;
}

/*
|--------------------------------------------------------------------------
| JOBS
|--------------------------------------------------------------------------
*/

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

        createdAt:
            now,

        updatedAt:
            now,

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

    jobsByCacheKey.set(
        cacheKey,
        jobId
    );

    return job;
}

function getJob(
    jobId
) {
    const job =
        jobs.get(
            jobId
        );

    if (!job) {
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

        if (
            jobsByCacheKey.get(
                job.cacheKey
            ) === jobId
        ) {
            jobsByCacheKey.delete(
                job.cacheKey
            );
        }

        return null;
    }

    return job;
}

function getJobByCacheKey(
    cacheKey
) {
    const jobId =
        jobsByCacheKey.get(
            cacheKey
        );

    if (!jobId) {
        return null;
    }

    const job =
        getJob(
            jobId
        );

    if (!job) {
        jobsByCacheKey.delete(
            cacheKey
        );

        return null;
    }

    return job;
}

/*
|--------------------------------------------------------------------------
| JOB ATIVO
|--------------------------------------------------------------------------
*/

function countActiveJobs() {
    let count = 0;

    for (
        const job
        of jobs.values()
    ) {
        if (
            job.status ===
            "processing"
        ) {
            count++;
        }
    }

    return count;
}

/*
|--------------------------------------------------------------------------
| PROCESSAMENTO
|--------------------------------------------------------------------------
*/

async function processJob(
    job
) {
    stats.translationsStarted++;

    console.log(
        `[JOB ${job.id}] Iniciando.`
    );

    try {
        /*
         * Verifica cache novamente.
         *
         * Importante em caso de jobs duplicados.
         */
        const cached =
            getTranslationCache(
                job.cacheKey
            );

        if (cached) {
            job.status =
                "completed";

            job.result =
                cached;

            job.updatedAt =
                Date.now();

            stats.translationsCompleted++;

            return;
        }

        const translated =
            await translateSrt(
                job.sourceSrt
            );

        setTranslationCache(
            job.cacheKey,
            translated
        );

        job.status =
            "completed";

        job.result =
            translated;

        job.updatedAt =
            Date.now();

        stats.translationsCompleted++;

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

        stats.translationsFailed++;

        console.error(
            `[JOB ${job.id}] Falhou: ${job.error}`
        );
    }
}

/*
|--------------------------------------------------------------------------
| WAIT JOB
|--------------------------------------------------------------------------
*/

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
            300
        );
    }

    return (
        job.status ===
        "completed"
    );
}

/*
|--------------------------------------------------------------------------
| SRT DE PROCESSAMENTO
|--------------------------------------------------------------------------
*/

function buildProcessingSrt() {
    return [
        "1",
        "00:00:01,000 --> 00:00:07,000",
        "⏳ Traduzindo a legenda com Gemini...",
        "",
        "2",
        "00:00:07,500 --> 00:00:15,000",
        "Aguarde alguns segundos e recarregue as legendas."
    ].join("\n");
}

function buildErrorSrt(
    message
) {
    const safeMessage =
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
                180
            );

    return [
        "1",
        "00:00:01,000 --> 00:00:08,000",
        "Não foi possível traduzir esta legenda.",
        "",
        "2",
        "00:00:08,500 --> 00:00:18,000",
        safeMessage
    ].join("\n");
}

/*
|--------------------------------------------------------------------------
| RESPOSTA SRT
|--------------------------------------------------------------------------
*/

function sendSubtitleResponse(
    res,
    srt,
    cacheControl = "no-store"
) {
    res.status(200);

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

/*
|--------------------------------------------------------------------------
| MANIFEST
|--------------------------------------------------------------------------
*/

app.get(
    "/manifest.json",
    (
        req,
        res
    ) => {
        res.json(
            manifest
        );
    }
);

/*
|--------------------------------------------------------------------------
| HOME
|--------------------------------------------------------------------------
*/

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

            geminiQueue:
                geminiQueue.length,

            activeGeminiRequests:
                activeGeminiRequests
        });
    }
);

/*
|--------------------------------------------------------------------------
| HEALTH
|--------------------------------------------------------------------------
*/

app.get(
    "/health",
    (
        req,
        res
    ) => {
        const uptime =
            process.uptime();

        res.json({
            status:
                "ok",

            uptime,

            model:
                GEMINI_MODEL,

            jobs:
                jobs.size,

            activeJobs:
                countActiveJobs(),

            cache:
                translationCache.size,

            geminiQueue:
                geminiQueue.length,

            activeGeminiRequests:
                activeGeminiRequests,

            stats: {
                ...stats
            },

            config: {
                maxConcurrentGemini:
                    MAX_CONCURRENT_GEMINI,

                minGeminiIntervalMs:
                    MIN_GEMINI_INTERVAL_MS,

                maxBatchBlocks:
                    MAX_BATCH_BLOCKS,

                maxBatchChars:
                    MAX_BATCH_CHARS
            }
        });
    }
);

/*
|--------------------------------------------------------------------------
| BUSCAR LEGENDA NO STREMIO
|--------------------------------------------------------------------------
*/

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
                        "Stremio-Gemini-Subtitle-Translator/4.0"
                }
            },
            SOURCE_FETCH_TIMEOUT_MS
        );

    if (
        !response.ok
    ) {
        throw new Error(
            `OpenSubtitles retornou HTTP ${response.status}.`
        );
    }

    const data =
        await response.json();

    return selectBestSubtitle(
        data?.subtitles ||
            []
    );
}

/*
|--------------------------------------------------------------------------
| SUBTITLES HANDLER
|--------------------------------------------------------------------------
*/

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
        /*
         * 1. Encontrar inglês.
         */
        const targetSub =
            await findEnglishSubtitle(
                type,
                id
            );

        if (
            !targetSub
        ) {
            console.log(
                "[STREMIO] Nenhuma legenda inglesa."
            );

            return safeJson(
                res,
                {
                    subtitles: []
                }
            );
        }

        /*
         * 2. Baixar.
         */
        const sourceSrt =
            await downloadSubtitle(
                targetSub.url
            );

        /*
         * 3. Validar SRT.
         */
        const originalBlocks =
            parseSrt(
                sourceSrt
            );

        if (
            originalBlocks.length ===
            0
        ) {
            console.warn(
                "[STREMIO] Legenda não parece SRT válido."
            );

            return safeJson(
                res,
                {
                    subtitles: []
                }
            );
        }

        /*
         * 4. Hash.
         */
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

        /*
         * 5. Cache pronto.
         */
        const cached =
            getTranslationCache(
                cacheKey
            );

        if (cached) {
            console.log(
                `[CACHE] HIT ${type}/${id}`
            );

            const existingJobId =
                `cached-${sourceHash.slice(
                    0,
                    24
                )}`;

            let cachedJob =
                getJob(
                    existingJobId
                );

            if (
                !cachedJob
            ) {
                cachedJob =
                    createJob({
                        jobId:
                            existingJobId,

                        cacheKey,

                        type,

                        videoId:
                            id,

                        sourceHash,

                        sourceSrt
                    });

                cachedJob.status =
                    "completed";

                cachedJob.result =
                    cached;
            }

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
                                    existingJobId
                                )}.srt`,

                            lang:
                                "por"
                        }
                    ]
                }
            );
        }

        /*
         * 6. Procurar job existente.
         */
        let job =
            getJobByCacheKey(
                cacheKey
            );

        /*
         * 7. Se não existe, criar.
         */
        if (
            !job
        ) {
            /*
             * Proteção contra muitos jobs.
             */
            const activeJobs =
                countActiveJobs();

            if (
                activeJobs >=
                MAX_ACTIVE_JOBS
            ) {
                console.warn(
                    `[STREMIO] Limite de jobs ativos atingido: ${activeJobs}/${MAX_ACTIVE_JOBS}`
                );

                return safeJson(
                    res,
                    {
                        subtitles: []
                    }
                );
            }

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

            /*
             * Dispara em background.
             */
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

        /*
         * 8. URL estável.
         */
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
            "[STREMIO] Erro:",
            error
        );

        return safeJson(
            res,
            {
                subtitles: []
            }
        );
    }
}

/*
|--------------------------------------------------------------------------
| ROTAS SUBTITLES
|--------------------------------------------------------------------------
*/

app.get(
    "/subtitles/:type/:id.json",
    subtitlesHandler
);

app.get(
    "/subtitles/:type/:id/:extra.json",
    subtitlesHandler
);

/*
|--------------------------------------------------------------------------
| ENTREGA DA LEGENDA
|--------------------------------------------------------------------------
*/

async function subtitleResultHandler(
    req,
    res
) {
    const rawJobId =
        String(
            req.params.jobId ||
                ""
        ).trim();

    let jobId;

    try {
        jobId =
            decodeURIComponent(
                rawJobId
            );
    } catch {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(
                "Job inválido."
            )
        );
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

    /*
     * Resultado pronto.
     */
    if (
        job.status ===
            "completed" &&
        job.result
    ) {
        return sendSubtitleResponse(
            res,
            job.result,
            "public, max-age=86400"
        );
    }

    /*
     * Falhou.
     */
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
     * Ainda processando.
     *
     * Esperamos antes de responder.
     */
    const completed =
        await waitForJob(
            job,
            WAIT_FOR_TRANSLATION_MS
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
            "public, max-age=86400"
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
     * Continua processando.
     */
    return sendSubtitleResponse(
        res,
        buildProcessingSrt(),
        "no-store, no-cache, must-revalidate"
    );
}

/*
|--------------------------------------------------------------------------
| ROTA DA LEGENDA
|--------------------------------------------------------------------------
*/

app.get(
    "/subtitle/:jobId.srt",
    subtitleResultHandler
);

/*
|--------------------------------------------------------------------------
| INICIALIZAÇÃO
|--------------------------------------------------------------------------
*/

app.listen(
    PORT,
    () => {
        console.log(
            "=============================================="
        );

        console.log(
            "  STREMIO GEMINI SUBTITLE TRANSLATOR 4.0"
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
            `Cache TTL: ${
                CACHE_TTL_MS /
                1000 /
                60 /
                60
            }h`
        );

        console.log(
            `Job TTL: ${
                JOB_TTL_MS /
                1000 /
                60
            }min`
        );

        console.log(
            `Max batch blocks: ${MAX_BATCH_BLOCKS}`
        );

        console.log(
            `Max batch chars: ${MAX_BATCH_CHARS}`
        );

        console.log(
            `Gemini concorrência: ${MAX_CONCURRENT_GEMINI}`
        );

        console.log(
            `Intervalo Gemini: ${MIN_GEMINI_INTERVAL_MS}ms`
        );

        console.log(
            `Max retries Gemini: ${MAX_GEMINI_RETRIES}`
        );

        console.log(
            `Max jobs ativos: ${MAX_ACTIVE_JOBS}`
        );

        console.log(
            "Status: ONLINE"
        );

        console.log(
            "=============================================="
        );
    }
);

/*
|--------------------------------------------------------------------------
| PROCESSOS
|--------------------------------------------------------------------------
*/

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
