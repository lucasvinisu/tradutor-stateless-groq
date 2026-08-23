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

const PORT = Number(
    process.env.PORT || 10000
);

const GEMINI_API_KEY =
    String(
        process.env.GEMINI_API_KEY || ""
    ).trim();

/*
 * Modelo atual.
 *
 * IMPORTANTE:
 * Se GEMINI_MODEL existir no Render,
 * ele será utilizado.
 *
 * Caso contrário, usamos este modelo.
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
|--------------------------------------------------------------------------
| LIMITES
|--------------------------------------------------------------------------
*/

/*
 * Timeout para OpenSubtitles.
 */
const SOURCE_FETCH_TIMEOUT_MS =
    20_000;

/*
 * Timeout individual do Gemini.
 */
const GEMINI_TIMEOUT_MS =
    90_000;

/*
 * Tempo máximo de uma tradução.
 *
 * 8 minutos.
 */
const MAX_TRANSLATION_TIME_MS =
    Number(
        process.env.MAX_TRANSLATION_TIME_MS ||
        480000
    );

/*
 * Tamanho máximo de cada lote.
 *
 * Mantemos lotes grandes para reduzir
 * drasticamente o número de requests.
 */
const MAX_BATCH_CHARS =
    Number(
        process.env.MAX_BATCH_CHARS ||
        25000
    );

/*
 * Uma única chamada Gemini por vez.
 *
 * Isso é intencional para o plano gratuito.
 */
const GEMINI_CONCURRENCY = 1;

/*
 * Intervalo mínimo entre requests.
 *
 * 4 segundos evita disparar requests
 * em sequência muito rápida.
 */
const MIN_REQUEST_INTERVAL_MS =
    Number(
        process.env.MIN_REQUEST_INTERVAL_MS ||
        4000
    );

/*
 * Cache das traduções.
 *
 * 7 dias.
 */
const CACHE_TTL_MS =
    7 * 24 * 60 * 60 * 1000;

/*
 * Jobs disponíveis por 24 horas.
 */
const JOB_TTL_MS =
    24 * 60 * 60 * 1000;

/*
 * Limites de memória.
 */
const MAX_CACHE_ENTRIES = 200;

const MAX_JOBS = 300;

/*
 * Tamanho máximo da legenda original.
 */
const MAX_SOURCE_CHARS =
    800_000;

/*
 * Tentativas para erros normais.
 *
 * 429 possui tratamento separado.
 */
const MAX_NORMAL_RETRIES = 2;

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
| FILA GLOBAL DO GEMINI
|--------------------------------------------------------------------------
*/

const geminiQueue = [];

let geminiWorkerRunning =
    false;

let lastGeminiRequestAt =
    0;

/*
 * Quando o Gemini retorna 429,
 * nenhuma nova request será feita
 * até esse momento.
 */
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

function randomId(
    length = 8
) {
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
            () => {
                controller.abort();
            },
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
| LIMPEZA DE MEMÓRIA
|--------------------------------------------------------------------------
*/

function cleanupMemory() {
    const now =
        Date.now();

    /*
     * Remove cache expirado.
     */
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

    /*
     * Remove jobs expirados,
     * mas nunca jobs processando.
     */
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
            jobs.delete(key);
        }
    }

    /*
     * Limita cache.
     */
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

    /*
     * Limita jobs.
     */
    while (
        jobs.size >
        MAX_JOBS
    ) {
        const key =
            jobs
                .keys()
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

        /*
         * Nunca removemos job em andamento.
         */
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
        const block
        of blocks
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
            !/^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/
                .test(
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
                    .slice(2)
                    .join("\n")
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
| VALIDAÇÃO
|--------------------------------------------------------------------------
*/

function validateTranslations(
    blocks,
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
        blocks.length
    ) {
        return {
            valid: false,
            reason:
                `Esperado ${blocks.length}, recebido ${translatedTexts.length}.`
        };
    }

    for (
        let i = 0;
        i <
        translatedTexts.length;
        i++
    ) {
        if (
            typeof translatedTexts[i] !==
            "string"
        ) {
            return {
                valid: false,
                reason:
                    `Bloco ${blocks[i].index} não é texto.`
            };
        }
    }

    return {
        valid: true
    };
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
    translationCache.set(
        key,
        {
            srt,

            createdAt:
                Date.now(),

            expiresAt:
                Date.now() +
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
| DIVISÃO DE LOTES
|--------------------------------------------------------------------------
*/

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
        /*
         * O Gemini recebe apenas:
         *
         * id
         * text
         */
        const blockChars =
            String(
                block.text ||
                ""
            ).length +
            40;

        /*
         * Se ultrapassar o limite,
         * fecha o lote anterior.
         */
        if (
            current.length >
                0 &&
            currentChars +
                blockChars >
                MAX_BATCH_CHARS
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
| COOLDOWN GEMINI
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
                ms,
                1000
            ),
            120_000
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
        `[GEMINI] Cooldown global: ${Math.ceil(
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
                seconds * 1000,
                120_000
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
                (
                    seconds +
                    1
                ) *
                    1000,
                120_000
            );
        }
    }

    /*
     * Exemplo:
     *
     * Please retry in 2m.
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
            120_000
        );
    }

    /*
     * Fallback.
     */
    return 30_000;
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
                    JSON.stringify({
                        systemInstruction:
                            {
                                parts:
                                    [
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

                        contents:
                            [
                                {
                                    role:
                                        "user",

                                    parts:
                                        [
                                            {
                                                text:
                                                    prompt
                                            }
                                        ]
                                }
                            ],

                        generationConfig:
                            {
                                responseMimeType:
                                    "application/json",

                                responseSchema:
                                    {
                                        type:
                                            "ARRAY",

                                        items:
                                            {
                                                type:
                                                    "OBJECT",

                                                properties:
                                                    {
                                                        id:
                                                            {
                                                                type:
                                                                    "INTEGER"
                                                            },

                                                        text:
                                                            {
                                                                type:
                                                                    "STRING"
                                                            }
                                                    },

                                                required:
                                                    [
                                                        "id",
                                                        "text"
                                                    ]
                                            }
                                    },

                                /*
                                 * O Gemini 3.5 Flash-Lite
                                 * permite uma saída grande.
                                 *
                                 * Não usamos temperature,
                                 * top_p ou top_k porque
                                 * esses parâmetros foram
                                 * descontinuados nos modelos
                                 * Gemini 3.x.
                                 */
                                maxOutputTokens:
                                    12000
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
                `Resposta não-JSON do Gemini. HTTP ${response.status}.`
            );

        error.status =
            response.status;

        throw error;
    }

    /*
     * Erro HTTP.
     */
    if (
        !response.ok
    ) {
        const message =
            data?.error?.message ||
            `HTTP ${response.status}`;

        const error =
            new Error(
                message
            );

        error.status =
            response.status;

        error.rateLimit =
            response.status ===
                429 ||
            /quota|rate.?limit|resource.?exhausted/i
                .test(
                    message
                );

        error.retryAfterMs =
            getRetryAfterMs(
                response,
                data
            );

        throw error;
    }

    /*
     * Extrai texto da resposta.
     */
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
             * Cooldown global.
             */
            let cooldown =
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

                console.log(
                    `[GEMINI] Intervalo de segurança: ${Math.ceil(
                        wait / 1000
                    )}s.`
                );

                await sleep(
                    wait
                );
            }

            const item =
                geminiQueue.shift();

            if (!item) {
                continue;
            }

            let finished =
                false;

            /*
             * Tentativas normais.
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
                        `[GEMINI] Request ${attempt}/${MAX_NORMAL_RETRIES + 1}`
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

                    finished =
                        true;

                    break;
                } catch (
                    error
                ) {
                    console.error(
                        `[GEMINI] Erro: ${getErrorMessage(
                            error
                        )}`
                    );

                    /*
                     * RATE LIMIT.
                     *
                     * Não fazemos retry
                     * imediatamente.
                     */
                    if (
                        error?.rateLimit
                    ) {
                        setGeminiCooldown(
                            error.retryAfterMs ||
                                30_000
                        );

                        /*
                         * Recoloca a tarefa
                         * no começo da fila.
                         */
                        geminiQueue.unshift(
                            item
                        );

                        finished =
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
                            2000 *
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

                    finished =
                        true;

                    break;
                }
            }

            if (
                !finished
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

REGRAS:

1. Retorne exatamente um objeto para cada entrada.
2. Preserve todos os IDs exatamente.
3. Não crie IDs.
4. Não remova IDs.
5. Traduza somente "text".
6. Não escreva explicações.
7. Não escreva markdown.
8. Não escreva nada fora do JSON.
9. Não resuma.
10. Não omita informação.
11. Preserve nomes próprios.
12. Preserve termos técnicos quando apropriado.
13. Preserve gírias e palavrões com intensidade equivalente.
14. Não censure.
15. Preserve tags HTML/ASS de formatação.
16. Use português brasileiro natural.
17. Não traduza literalmente quando isso produzir português artificial.
18. Preserve o sentido, intenção e contexto.
19. Não acrescente informações que não estejam no original.
20. Mantenha a quantidade de entradas exatamente igual à entrada recebida.

RETORNE SOMENTE:

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
| TRADUZIR UM LOTE
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

    /*
     * Nenhum bloco pode desaparecer.
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

    const validation =
        validateTranslations(
            blocks,
            translated
        );

    if (
        !validation.valid
    ) {
        throw new Error(
            validation.reason
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

    const translatedTexts =
        new Array(
            blocks.length
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
         * Limite absoluto.
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

        console.log(
            `[TRANSLATE] Lote ${
                batchIndex + 1
            }/${batches.length} - ${
                batch.length
            } blocos.`
        );

        const translated =
            await translateBatch(
                batch
            );

        /*
         * Como cada bloco possui seu ID,
         * podemos localizar diretamente.
         */
        const translatedById =
            new Map();

        for (
            let i = 0;
            i < batch.length;
            i++
        ) {
            translatedById.set(
                batch[i].index,
                translated[i]
            );
        }

        for (
            let i = 0;
            i < blocks.length;
            i++
        ) {
            const value =
                translatedById.get(
                    blocks[i].index
                );

            if (
                typeof value ===
                "string"
            ) {
                translatedTexts[
                    i
                ] = value;
            }
        }

        /*
         * Progresso.
         */
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
     * Confirma que todos foram traduzidos.
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
| PROCESSAMENTO
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

        const translated =
            await translateSrt(
                job.sourceSrt,
                job
            );

        /*
         * Salva no cache.
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
| JOB EXISTENTE
|--------------------------------------------------------------------------
*/

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
        `Progresso: ${progress}%. Aguarde e tente novamente.`
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
                180
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
| ESPERA
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
| MANIFEST
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

            batchChars:
                MAX_BATCH_CHARS,

            requestIntervalMs:
                MIN_REQUEST_INTERVAL_MS,

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

            batchChars:
                MAX_BATCH_CHARS,

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
                )
        });
    }
);

/*
|--------------------------------------------------------------------------
| ESCOLHA DA LEGENDA
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
        score += 100;
    } else if (
        lang === "en"
    ) {
        score += 90;
    }

    if (
        subtitle?.hearingImpaired ===
        false
    ) {
        score += 20;
    }

    if (
        String(
            subtitle?.format ||
            ""
        ).toLowerCase() ===
        "srt"
    ) {
        score += 20;
    }

    if (
        /english/i.test(
            String(
                subtitle?.name ||
                ""
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

    return subtitles
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
        null;
}

/*
|--------------------------------------------------------------------------
| BUSCAR LEGENDA INGLESA
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
| HANDLER SUBTITLES
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
         * 1. Buscar legenda inglesa.
         */
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
         * 2. Baixar legenda.
         */
        const sourceSrt =
            await downloadSubtitle(
                target.url
            );

        /*
         * 3. Validar.
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
         * 4. Hash da legenda.
         *
         * Isso permite identificar exatamente
         * a mesma legenda mesmo que o ID do
         * filme/série seja igual.
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
         * 6. Procurar job já existente.
         */
        let job =
            findProcessingJob(
                cacheKey
            );

        /*
         * 7. Criar job.
         */
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

            /*
             * Calcula quantidade de lotes.
             */
            job.totalBatches =
                splitIntoBatches(
                    blocks
                ).length;

            /*
             * Começa em background.
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
     * Tradução pronta.
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
     * Aguarda até 20 segundos.
     */
    const completed =
        await waitForJob(
            job,
            20_000
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

    /*
     * Falhou durante a espera.
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
     * O job continua rodando no Render.
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
            `Batch máximo: ${MAX_BATCH_CHARS} caracteres`
        );

        console.log(
            `Intervalo Gemini: ${MIN_REQUEST_INTERVAL_MS}ms`
        );

        console.log(
            `Timeout tradução: ${MAX_TRANSLATION_TIME_MS}ms`
        );

        console.log(
            `PUBLIC_URL: ${
                PUBLIC_URL ||
                "(automático)"
            }`
        );

        console.log(
            `Concorrência Gemini: ${GEMINI_CONCURRENCY}`
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
