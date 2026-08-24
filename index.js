const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();

app.use(cors());
app.disable("x-powered-by");

/*
 * Necessário para receber o SRT enviado
 * pelo componente local.
 */
app.use(
    express.json({
        limit: "1mb"
    })
);

/*
|--------------------------------------------------------------------------
| CONFIGURAÇÃO
|--------------------------------------------------------------------------
*/

const PORT = Number(
    process.env.PORT || 10000
);

const GEMINI_API_KEY =
    String(
        process.env.GEMINI_API_KEY || ""
    ).trim();

/*
 * IMPORTANTE:
 *
 * O Render está configurado para:
 *
 * gemini-3.5-flash-lite
 *
 * Este também é o fallback.
 */
const GEMINI_MODEL =
    String(
        process.env.GEMINI_MODEL ||
        "gemini-3.5-flash-lite"
    ).trim();

const PUBLIC_URL =
    String(
        process.env.PUBLIC_URL || ""
    )
        .replace(/\/+$/, "");

/*
 * Segredo compartilhado somente entre
 * o Render e o componente local.
 */
const LOCAL_BRIDGE_SECRET =
    String(
        process.env.LOCAL_BRIDGE_SECRET || ""
    ).trim();

/*
|--------------------------------------------------------------------------
| TEMPOS
|--------------------------------------------------------------------------
*/

const SOURCE_FETCH_TIMEOUT_MS =
    20_000;

const GEMINI_TIMEOUT_MS =
    90_000;

/*
 * O job não pode ficar eternamente tentando.
 *
 * 8 minutos é suficiente para uma legenda
 * normal dentro da arquitetura gratuita.
 */
const MAX_TRANSLATION_TIME_MS =
    Number(
        process.env.MAX_TRANSLATION_TIME_MS ||
        480_000
    );

/*
|--------------------------------------------------------------------------
| LOTES
|--------------------------------------------------------------------------
*/

/*
 * Limite configurado no Render:
 *
 * MAX_BATCH_CHARS=18000
 */
const MAX_BATCH_CHARS =
    Number(
        process.env.MAX_BATCH_CHARS ||
        18_000
    );

/*
 * IMPORTANTE:
 *
 * Não usamos somente caracteres.
 *
 * Também limitamos a quantidade de blocos.
 *
 * Isso evita situações como:
 *
 * Lote 1/5 - 352 blocos
 *
 * que eram ruins para a estabilidade.
 */
const MAX_BATCH_BLOCKS = 60;

/*
|--------------------------------------------------------------------------
| GEMINI
|--------------------------------------------------------------------------
*/

/*
 * Uma única requisição por vez.
 */
const GEMINI_CONCURRENCY = 1;

/*
 * Configurado no Render:
 *
 * MIN_REQUEST_INTERVAL_MS=3000
 */
const MIN_REQUEST_INTERVAL_MS =
    Number(
        process.env.MIN_REQUEST_INTERVAL_MS ||
        3000
    );

/*
 * Não fazemos dezenas de retries normais.
 */
const MAX_NORMAL_RETRIES = 2;

/*
 * Se recebermos 429, não ficamos martelando
 * a API.
 */
const MAX_RATE_LIMIT_COOLDOWN_MS =
    120_000;

/*
|--------------------------------------------------------------------------
| CACHE
|--------------------------------------------------------------------------
*/

const CACHE_TTL_MS =
    7 * 24 * 60 * 60 * 1000;

const JOB_TTL_MS =
    24 * 60 * 60 * 1000;

const MAX_CACHE_ENTRIES = 200;

const MAX_JOBS = 300;

/*
|--------------------------------------------------------------------------
| SEGURANÇA
|--------------------------------------------------------------------------
*/

const MAX_SOURCE_CHARS =
    800_000;

/*
|--------------------------------------------------------------------------
| MEMÓRIA
|--------------------------------------------------------------------------
*/

const translationCache =
    new Map();

const jobs =
    new Map();

/*
|--------------------------------------------------------------------------
| FILA GLOBAL GEMINI
|--------------------------------------------------------------------------
*/

const geminiQueue = [];

let geminiWorkerRunning =
    false;

let lastGeminiRequestAt =
    0;

let geminiCooldownUntil =
    0;

/*
|--------------------------------------------------------------------------
| HELPERS
|--------------------------------------------------------------------------
*/

function sleep(ms) {
    return new Promise(
        resolve =>
            setTimeout(
                resolve,
                ms
            )
    );
}

function sha256(text) {
    return crypto
        .createHash("sha256")
        .update(
            String(text),
            "utf8"
        )
        .digest("hex");
}

function randomId(length = 8) {
    return crypto
        .randomBytes(length)
        .toString("hex");
}

function getErrorMessage(error) {
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

function cleanBaseUrl(req) {
    if (PUBLIC_URL) {
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
        "no-store, no-cache, must-revalidate"
    );

    return res.json(data);
}

/*
|--------------------------------------------------------------------------
| AUTENTICAÇÃO DA PONTE LOCAL
|--------------------------------------------------------------------------
*/

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

    if (!auth) {
        return false;
    }

    const expected =
        `Bearer ${LOCAL_BRIDGE_SECRET}`;

    const authBuffer =
        Buffer.from(auth);

    const expectedBuffer =
        Buffer.from(expected);

    /*
     * timingSafeEqual exige buffers
     * exatamente do mesmo tamanho.
     */
    if (
        authBuffer.length !==
        expectedBuffer.length
    ) {
        return false;
    }

    return crypto.timingSafeEqual(
        authBuffer,
        expectedBuffer
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
        clearTimeout(timer);
    }
}

/*
|--------------------------------------------------------------------------
| LIMPEZA
|--------------------------------------------------------------------------
*/

function cleanupMemory() {
    const now =
        Date.now();

    /*
     * Cache expirado.
     */
    for (
        const [
            key,
            item
        ] of translationCache.entries()
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

    /*
     * Jobs expirados.
     */
    for (
        const [
            key,
            job
        ] of jobs.entries()
    ) {
        if (
            job.expiresAt <=
                now &&
            job.status !==
                "processing"
        ) {
            jobs.delete(key);
        }
    }

    /*
     * Limite do cache.
     */
    while (
        translationCache.size >
        MAX_CACHE_ENTRIES
    ) {
        const key =
            translationCache.keys()
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

    /*
     * Limite dos jobs.
     *
     * Nunca removemos job em processamento.
     */
    while (
        jobs.size >
        MAX_JOBS
    ) {
        const key =
            jobs.keys()
                .next()
                .value;

        if (
            key ===
            undefined
        ) {
            break;
        }

        const job =
            jobs.get(key);

        if (
            job &&
            job.status ===
                "processing"
        ) {
            break;
        }

        jobs.delete(key);
    }
}

setInterval(
    cleanupMemory,
    5 * 60 * 1000
).unref();

/*
|--------------------------------------------------------------------------
| SRT
|--------------------------------------------------------------------------
*/

function normalizeSrt(text) {
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
            .map(
                block =>
                    block.trim()
            )
            .filter(Boolean);

    const result = [];

    for (
        const block of blocks
    ) {
        const lines =
            block.split("\n");

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
                Number(indexLine),

            timing:
                timingLine,

            text:
                lines
                    .slice(2)
                    .join("\n")
        });
    }

    return result;
}

/*
|--------------------------------------------------------------------------
| CONSTRUTOR SRT
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
                        translatedTexts[
                            index
                        ] ??
                        block.text;

                    return [
                        block.index,
                        block.timing,
                        translated
                    ].join("\n");
                }
            )
            .join("\n\n")
            .trim() +
        "\n"
    );
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

    return item.srt;
}

/*
|--------------------------------------------------------------------------
| DIVISÃO INTELIGENTE DE LOTES
|--------------------------------------------------------------------------
|
| Usa DOIS limites:
|
| 1. máximo de caracteres
| 2. máximo de blocos
|
| O primeiro limite atingido encerra o lote.
|--------------------------------------------------------------------------
*/

function splitIntoBatches(
    blocks
) {
    const batches = [];

    let current = [];

    let currentChars = 0;

    for (
        const block of blocks
    ) {
        const text =
            String(
                block.text ||
                ""
            );

        /*
         * Estimativa de tamanho do objeto
         * enviado ao Gemini.
         */
        const blockChars =
            text.length +
            50;

        const wouldExceedChars =
            current.length >
                0 &&
            currentChars +
                blockChars >
                MAX_BATCH_CHARS;

        const wouldExceedBlocks =
            current.length >=
            MAX_BATCH_BLOCKS;

        if (
            wouldExceedChars ||
            wouldExceedBlocks
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
        current.length >
        0
    ) {
        batches.push(
            current
        );
    }

    return batches;
}

/*
|--------------------------------------------------------------------------
| COOLDOWN
|--------------------------------------------------------------------------
*/

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

    const until =
        Date.now() +
        safeMs;

    if (
        until >
        geminiCooldownUntil
    ) {
        geminiCooldownUntil =
            until;
    }

    console.log(
        `[GEMINI] RATE LIMIT. Cooldown global de ${Math.ceil(
            safeMs / 1000
        )}s.`
    );
}

/*
|--------------------------------------------------------------------------
| RETRY-AFTER
|--------------------------------------------------------------------------
*/

function getRetryAfterMs(
    response,
    errorData
) {
    const header =
        response?.headers?.get(
            "retry-after"
        );

    if (header) {
        const seconds =
            Number(header);

        if (
            Number.isFinite(
                seconds
            ) &&
            seconds > 0
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
            errorData?.error
                ?.message ||
                ""
        );

    /*
     * Exemplo:
     *
     * Please retry in 51.98s.
     */
    const secondsMatch =
        message.match(
            /retry in\s+([\d.]+)s/i
        );

    if (
        secondsMatch
    ) {
        const seconds =
            Number(
                secondsMatch[1]
            );

        if (
            Number.isFinite(
                seconds
            )
        ) {
            return Math.min(
                (seconds + 1) *
                    1000,
                MAX_RATE_LIMIT_COOLDOWN_MS
            );
        }
    }

    /*
     * Exemplo:
     *
     * retry in 2m30s
     */
    const minuteSecondMatch =
        message.match(
            /retry in\s+(\d+)m\s*(\d+(?:\.\d+)?)?s?/i
        );

    if (
        minuteSecondMatch
    ) {
        const minutes =
            Number(
                minuteSecondMatch[1]
            );

        const seconds =
            Number(
                minuteSecondMatch[2] ||
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

    /*
     * Exemplo:
     *
     * retry in 2m
     */
    const minuteMatch =
        message.match(
            /retry in\s+(\d+)m/i
        );

    if (
        minuteMatch
    ) {
        return Math.min(
            (
                Number(
                    minuteMatch[1]
                ) *
                    60 +
                1
            ) *
                1000,
            MAX_RATE_LIMIT_COOLDOWN_MS
        );
    }

    /*
     * Se a API não disser quanto tempo,
     * esperamos 30 segundos.
     */
    return 30_000;
}

/*
|--------------------------------------------------------------------------
| DETECÇÃO DE RATE LIMIT
|--------------------------------------------------------------------------
*/

function isRateLimitError(
    status,
    message
) {
    return (
        status === 429 ||
        /quota|rate.?limit|resource.?exhausted|too many requests/i.test(
            String(
                message || ""
            )
        )
    );
}

/*
|--------------------------------------------------------------------------
| REQUEST GEMINI
|--------------------------------------------------------------------------
*/

async function rawGeminiRequest(
    prompt
) {
    if (
        !GEMINI_API_KEY
    ) {
        throw new Error(
            "GEMINI_API_KEY não configurada."
        );
    }

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            GEMINI_MODEL
        )}:generateContent`;

    const body = {
        /*
         * System instruction separada.
         */
        systemInstruction: {
            parts: [
                {
                    text:
                        "Você é um tradutor profissional de legendas cinematográficas. " +
                        "Traduza inglês para Português do Brasil. " +
                        "Seja natural, fluente e fiel ao significado. " +
                        "Preserve nomes próprios, marcas, termos técnicos, humor, " +
                        "gírias, palavrões, intensidade emocional e intenção. " +
                        "Não censure. Não resuma. Não explique. " +
                        "Não omita conteúdo. " +
                        "Traduza somente o campo text. " +
                        "Mantenha exatamente os IDs recebidos. " +
                        "Preserve tags de formatação como <i>, </i>, <b>, </b>, " +
                        "{\\i1}, {\\i0} e similares."
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
            /*
             * JSON estruturado.
             */
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

                minItems: 1
            },

            /*
             * 18k chars de entrada normalmente
             * não precisam de uma saída enorme.
             */
            maxOutputTokens:
                12000
        }
    };

    /*
     * IMPORTANTE:
     *
     * NÃO usamos:
     *
     * temperature
     * top_p
     * top_k
     *
     * porque os modelos Gemini 3.5
     * introduziram mudanças nesses parâmetros.
     */

    const response =
        await fetchWithTimeout(
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
                    )
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
            data?.error
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
        data?.candidates?.[0]
            ?.content?.parts
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

    return text;
}

/*
|--------------------------------------------------------------------------
| FILA GEMINI
|--------------------------------------------------------------------------
*/

function enqueueGemini(
    prompt
) {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            geminiQueue.push(
                {
                    prompt,
                    resolve,
                    reject
                }
            );

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
            geminiQueue.length >
            0
        ) {
            /*
             * Primeiro respeitamos cooldown.
             */
            const cooldown =
                getCooldownRemaining();

            if (
                cooldown >
                0
            ) {
                console.log(
                    `[GEMINI] Fila aguardando cooldown de ${Math.ceil(
                        cooldown / 1000
                    )}s.`
                );

                await sleep(
                    cooldown
                );
            }

            /*
             * Intervalo mínimo.
             */
            const sinceLast =
                Date.now() -
                lastGeminiRequestAt;

            if (
                lastGeminiRequestAt >
                    0 &&
                sinceLast <
                    MIN_REQUEST_INTERVAL_MS
            ) {
                const wait =
                    MIN_REQUEST_INTERVAL_MS -
                    sinceLast;

                await sleep(
                    wait
                );
            }

            const item =
                geminiQueue.shift();

            if (!item) {
                continue;
            }

            let resolved =
                false;

            /*
             * Retries normais.
             */
            for (
                let attempt = 1;
                attempt <=
                    MAX_NORMAL_RETRIES +
                        1;
                attempt++
            ) {
                /*
                 * Verifica cooldown novamente.
                 */
                const remaining =
                    getCooldownRemaining();

                if (
                    remaining >
                    0
                ) {
                    await sleep(
                        remaining
                    );
                }

                try {
                    console.log(
                        `[GEMINI] Request ${attempt}/${
                            MAX_NORMAL_RETRIES +
                            1
                        }`
                    );

                    lastGeminiRequestAt =
                        Date.now();

                    const result =
                        await rawGeminiRequest(
                            item.prompt
                        );

                    item.resolve(
                        result
                    );

                    resolved =
                        true;

                    break;
                } catch (
                    error
                ) {
                    const message =
                        getErrorMessage(
                            error
                        );

                    console.error(
                        `[GEMINI] Erro: ${message}`
                    );

                    /*
                     * RATE LIMIT
                     *
                     * Não desperdiçamos
                     * as outras tentativas.
                     */
                    if (
                        error?.rateLimit
                    ) {
                        const cooldownMs =
                            Math.min(
                                error.retryAfterMs ||
                                    30_000,
                                MAX_RATE_LIMIT_COOLDOWN_MS
                            );

                        setGeminiCooldown(
                            cooldownMs
                        );

                        /*
                         * Devolvemos o item
                         * ao começo da fila.
                         *
                         * Assim ele será processado
                         * depois do cooldown.
                         */
                        geminiQueue.unshift(
                            item
                        );

                        resolved =
                            true;

                        break;
                    }

                    /*
                     * Erro normal.
                     */
                    if (
                        attempt <=
                        MAX_NORMAL_RETRIES
                    ) {
                        const wait =
                            1500 *
                            attempt;

                        console.log(
                            `[GEMINI] Retry normal em ${Math.ceil(
                                wait / 1000
                            )}s.`
                        );

                        await sleep(
                            wait
                        );

                        continue;
                    }

                    item.reject(
                        error
                    );

                    resolved =
                        true;

                    break;
                }
            }

            if (
                !resolved
            ) {
                item.reject(
                    new Error(
                        "Falha desconhecida na fila Gemini."
                    )
                );
            }
        }
    } finally {
        geminiWorkerRunning =
            false;

        if (
            geminiQueue.length >
            0
        ) {
            processGeminiQueue();
        }
    }
}

/*
|--------------------------------------------------------------------------
| PROMPT
|--------------------------------------------------------------------------
*/

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
3. Não crie IDs.
4. Não remova IDs.
5. Traduza somente o campo "text".
6. Não escreva explicações.
7. Não escreva markdown.
8. Não escreva texto fora do JSON.
9. Não resuma.
10. Não omita informação.
11. Preserve nomes próprios.
12. Preserve marcas e nomes de produtos.
13. Preserve termos técnicos quando apropriado.
14. Preserve gírias.
15. Preserve palavrões com intensidade equivalente.
16. Não censure.
17. Preserve tags HTML e ASS.
18. Preserve <i>, </i>, <b>, </b>.
19. Preserve {\\i1}, {\\i0} e tags semelhantes.
20. Use Português do Brasil natural.
21. Não traduza literalmente quando isso produzir português artificial.
22. Preserve o sentido e a intenção.
23. Não acrescente informações.
24. Não remova informações.
25. Não altere os IDs.
26. Não misture textos entre IDs.
27. Cada ID deve receber somente a tradução do seu próprio texto.

RETORNE SOMENTE UM ARRAY JSON NO FORMATO:

[
  {
    "id": 123,
    "text": "tradução"
  }
]

ENTRADAS:

${JSON.stringify(
    payload
)}
`;
}

/*
|--------------------------------------------------------------------------
| TRADUZIR LOTE
|--------------------------------------------------------------------------
*/

async function translateBatch(
    blocks
) {
    const prompt =
        buildTranslationPrompt(
            blocks
        );

    const raw =
        await enqueueGemini(
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
        throw new Error(
            "Gemini retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(
            parsed
        )
    ) {
        throw new Error(
            "Gemini não retornou uma lista."
        );
    }

    /*
     * Mapeia ID -> tradução.
     */
    const translatedById =
        new Map();

    for (
        const item of parsed
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
     * Recupera exatamente na ordem
     * original.
     */
    const translated =
        blocks.map(
            block =>
                translatedById.get(
                    block.index
                )
        );

    /*
     * Nenhum bloco pode faltar.
     */
    if (
        translated.some(
            text =>
                typeof text !==
                "string"
        )
    ) {
        throw new Error(
            "Gemini não devolveu todos os blocos."
        );
    }

    if (
        translated.length !==
        blocks.length
    ) {
        throw new Error(
            "Quantidade de traduções diferente da quantidade de blocos."
        );
    }

    return translated;
}

/*
|--------------------------------------------------------------------------
| TRADUÇÃO COMPLETA
|--------------------------------------------------------------------------
*/

async function translateSrt(
    originalSrt,
    job
) {
    const blocks =
        parseSrt(
            originalSrt
        );

    if (
        blocks.length ===
        0
    ) {
        throw new Error(
            "Nenhum bloco SRT válido."
        );
    }

    console.log(
        `[TRANSLATE] ${blocks.length} blocos.`
    );

    const batches =
        splitIntoBatches(
            blocks
        );

    console.log(
        `[TRANSLATE] ${batches.length} lote(s).`
    );

    /*
     * Mostra exatamente como os lotes
     * foram divididos.
     */
    console.log(
        `[TRANSLATE] Limite: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} caracteres.`
    );

    const translatedTexts =
        new Array(
            blocks.length
        );

    /*
     * Mapas para localizar cada bloco
     * sem fazer findIndex repetidamente.
     */
    const originalPositions =
        new Map();

    blocks.forEach(
        (
            block,
            index
        ) => {
            originalPositions.set(
                block,
                index
            );
        }
    );

    const startedAt =
        Date.now();

    /*
     * Processamento sequencial.
     */
    for (
        let batchIndex = 0;
        batchIndex <
            batches.length;
        batchIndex++
    ) {
        /*
         * Tempo máximo.
         */
        if (
            Date.now() -
                startedAt >
            MAX_TRANSLATION_TIME_MS
        ) {
            throw new Error(
                "Tempo máximo de tradução atingido."
            );
        }

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
            `[TRANSLATE] Lote ${
                batchIndex + 1
            }/${batches.length} - ${
                batch.length
            } blocos / ${
                batchChars
            } caracteres.`
        );

        const translated =
            await translateBatch(
                batch
            );

        /*
         * Salva diretamente na posição correta.
         */
        for (
            let i = 0;
            i < batch.length;
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
            batchIndex + 1;

        job.totalBatches =
            batches.length;

        job.updatedAt =
            Date.now();

        console.log(
            `[TRANSLATE] Lote ${
                batchIndex + 1
            }/${batches.length} concluído.`
        );
    }

    /*
     * Verificação final.
     */
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

    return buildSrt(
        blocks,
        translatedTexts
    );
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

        return null;
    }

    return job;
}

/*
|--------------------------------------------------------------------------
| PROCESSAMENTO DO JOB
|--------------------------------------------------------------------------
*/

async function processJob(
    job
) {
    console.log(
        `[JOB ${job.id}] Iniciando.`
    );

    try {
        /*
         * Cache primeiro.
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

            job.progress =
                100;

            job.updatedAt =
                Date.now();

            console.log(
                `[JOB ${job.id}] Cache utilizado.`
            );

            return;
        }

        const translated =
            await translateSrt(
                job.sourceSrt,
                job
            );

        /*
         * Cacheia o resultado.
         */
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

/*
|--------------------------------------------------------------------------
| JOB EM PROCESSAMENTO
|--------------------------------------------------------------------------
*/

function findProcessingJob(
    cacheKey
) {
    for (
        const job of jobs.values()
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

/*
|--------------------------------------------------------------------------
| SRT DE PROCESSAMENTO
|--------------------------------------------------------------------------
*/

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
    ].join("\n");
}

/*
|--------------------------------------------------------------------------
| SRT DE ERRO
|--------------------------------------------------------------------------
*/

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
    cacheControl =
        "no-store"
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
| ESPERAR JOB
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
            500
        );
    }

    return (
        job.status ===
        "completed"
    );
}

/*
|--------------------------------------------------------------------------
| MANIFEST STREMIO
|--------------------------------------------------------------------------
*/

const manifest = {
    id:
        "org.tradutor.stateless.gemini.free",

    version:
        "5.0.0",

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
                MIN_REQUEST_INTERVAL_MS
        });
    }
);

/*
|--------------------------------------------------------------------------
| PONTUAÇÃO DA LEGENDA
|--------------------------------------------------------------------------
*/

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
        lang === "eng"
    ) {
        score +=
            100;
    } else if (
        lang === "en"
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

/*
|--------------------------------------------------------------------------
| ESCOLHER LEGENDA INGLESA
|--------------------------------------------------------------------------
*/

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

    return subtitles
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
        null;
}

/*
|--------------------------------------------------------------------------
| PROCURAR LEGENDA
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
                        "Stremio-Gemini-Subtitle-Translator/5.0"
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

/*
|--------------------------------------------------------------------------
| DOWNLOAD SRT
|--------------------------------------------------------------------------
*/

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
                        "Stremio-Gemini-Subtitle-Translator/5.0"
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

    if (!text) {
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

/*
|--------------------------------------------------------------------------
| JOB DE LEGENDA EMBUTIDA
|--------------------------------------------------------------------------
*/

async function createEmbeddedTranslationJob({
    type,
    videoId,
    sourceSrt,
    sourceName = "embedded"
}) {
    const normalizedSrt =
        normalizeSrt(
            sourceSrt
        );

    if (!normalizedSrt) {
        throw new Error(
            "A legenda embutida está vazia."
        );
    }

    if (
        normalizedSrt.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda embutida muito grande: ${normalizedSrt.length} caracteres.`
        );
    }

    const blocks =
        parseSrt(
            normalizedSrt
        );

    if (
        blocks.length ===
        0
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
     * O cache da fonte embutida depende
     * somente do conteúdo real do SRT.
     *
     * Se o mesmo SRT aparecer novamente,
     * não gastamos outra chamada Gemini.
     */
    const cacheKey =
        `embedded:${sourceHash}`;

    /*
     * Cache já pronto.
     */
    const cached =
        getTranslationCache(
            cacheKey
        );

    if (cached) {
        const cachedJobId =
            `embedded-cached-${sourceHash.slice(
                0,
                24
            )}`;

        let cachedJob =
            getJob(
                cachedJobId
            );

        if (!cachedJob) {
            cachedJob =
                createJob({
                    jobId:
                        cachedJobId,

                    cacheKey,

                    type,

                    videoId,

                    sourceHash,

                    sourceSrt:
                        normalizedSrt
                });

            cachedJob.status =
                "completed";

            cachedJob.result =
                cached;

            cachedJob.progress =
                100;

            cachedJob.updatedAt =
                Date.now();
        }

        console.log(
            `[EMBEDDED] Cache utilizado para ${sourceName}.`
        );

        return cachedJob;
    }

    /*
     * Se a mesma legenda já está sendo
     * traduzida, reutilizamos o job.
     */
    const existingJob =
        findProcessingJob(
            cacheKey
        );

    if (existingJob) {
        console.log(
            `[EMBEDDED] Job existente reutilizado: ${existingJob.id}`
        );

        return existingJob;
    }

    /*
     * Novo job.
     */
    const jobId =
        `embedded-${sourceHash.slice(
            0,
            24
        )}-${randomId(8)}`;

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

    /*
     * Importantíssimo:
     *
     * usamos o MESMO processJob() do
     * fluxo atual. Portanto continuamos
     * usando a mesma fila Gemini, o mesmo
     * intervalo, cooldown, cache e limites.
     */
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

/*
|--------------------------------------------------------------------------
| ENDPOINT SUBTITLES
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

    /*
     * DIAGNÓSTICO:
     * mostra quais informações o Stremio
     * enviou sobre o arquivo em reprodução.
     *
     * Isto NÃO chama Gemini.
     */
    const rawExtra =
        String(
            req.params.extra || ""
        ).trim();

    if (rawExtra) {
        const extraParams =
            new URLSearchParams(
                rawExtra
            );

        const videoHash =
            extraParams.get(
                "videoHash"
            ) || "";

        const videoSize =
            extraParams.get(
                "videoSize"
            ) || "";

        const filename =
            extraParams.get(
                "filename"
            ) || "";

        console.log(
            `[STREMIO EXTRA] filename: ${
                filename ||
                "(não enviado)"
            }`
        );

        console.log(
            `[STREMIO EXTRA] videoSize: ${
                videoSize ||
                "(não enviado)"
            }`
        );

        console.log(
            `[STREMIO EXTRA] videoHash: ${
                videoHash ||
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
        /*
         * 1. Procurar legenda inglesa.
         */
        const target =
            await findEnglishSubtitle(
                type,
                id
            );

        if (!target) {
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
         * 2. Baixar SRT.
         */
        const sourceSrt =
            await downloadSubtitle(
                target.url
            );

        /*
         * 3. Validar SRT.
         */
        const blocks =
            parseSrt(
                sourceSrt
            );

        if (
            blocks.length ===
            0
        ) {
            return safeJson(
                res,
                {
                    subtitles: []
                }
            );
        }

        /*
         * 4. Hash da legenda real.
         *
         * Isso evita confundir episódios diferentes
         * ou versões diferentes da mesma legenda.
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
         * 5. Cache.
         */
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

                        videoId:
                            id,

                        sourceHash,

                        sourceSrt
                    });

                job.status =
                    "completed";

                job.result =
                    cached;

                job.progress =
                    100;
            }

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

        /*
         * 6. Verifica se outro pedido
         * já está traduzindo a mesma legenda.
         */
        let job =
            findProcessingJob(
                cacheKey
            );

        /*
         * 7. Se não existe, cria.
         */
        if (!job) {
            const jobId =
                `job-${sourceHash.slice(
                    0,
                    24
                )}-${randomId(8)}`;

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
             * Calcula previamente
             * quantos lotes serão necessários.
             */
            job.totalBatches =
                splitIntoBatches(
                    blocks
                ).length;

            /*
             * Não bloqueia a resposta do Stremio.
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

/*
|--------------------------------------------------------------------------
| ROTAS STREMIO
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
| API DA PONTE LOCAL - LEGENDA EMBUTIDA
|--------------------------------------------------------------------------
*/

app.post(
    "/api/translate-embedded",
    async (
        req,
        res
    ) => {
        /*
         * Segurança.
         */
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
                req.body || {};

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

            /*
             * Tamanho antes de qualquer
             * processamento.
             */
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

/*
|--------------------------------------------------------------------------
| RESULTADO DA LEGENDA
|--------------------------------------------------------------------------
*/

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

    if (!jobId) {
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

    if (!job) {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(
                "Esta tradução expirou. Recarregue as legendas."
            )
        );
    }

    /*
     * Já terminou.
     */
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
     * Esperamos no máximo 15 segundos
     * nesta conexão.
     *
     * O processamento continua no Render.
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

    /*
     * Ainda processando.
     */
    return sendSubtitleResponse(
        res,
        buildProcessingSrt(
            job
        ),
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
| START
|--------------------------------------------------------------------------
*/

app.listen(
    PORT,
    () => {
        console.log(
            "=============================================="
        );

        console.log(
            " STREMIO GEMINI SUBTITLE TRANSLATOR 5.0"
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
            `Intervalo Gemini: ${MIN_REQUEST_INTERVAL_MS}ms`
        );

        console.log(
            `Concorrência Gemini: ${GEMINI_CONCURRENCY}`
        );

        console.log(
            `Timeout tradução: ${MAX_TRANSLATION_TIME_MS}ms`
        );

        console.log(
            `Cache TTL: ${CACHE_TTL_MS / 3600000}h`
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
