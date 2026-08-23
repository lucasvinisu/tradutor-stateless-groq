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

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const GEMINI_MODEL =
    process.env.GEMINI_MODEL || "gemini-3.6-flash";

const PUBLIC_URL =
    (process.env.PUBLIC_URL || "").replace(/\/+$/, "");

/*
|--------------------------------------------------------------------------
| LIMITES DE REDE
|--------------------------------------------------------------------------
*/

const SOURCE_FETCH_TIMEOUT_MS = 20_000;

const GEMINI_TIMEOUT_MS = 90_000;

/*
|--------------------------------------------------------------------------
| LIMITES DO SISTEMA
|--------------------------------------------------------------------------
*/

/*
 * IMPORTANTE:
 *
 * Nunca fazemos mais de uma chamada Gemini simultaneamente.
 */
const MAX_CONCURRENT_GEMINI = 1;

/*
 * Intervalo mínimo entre chamadas.
 *
 * 16 segundos = aproximadamente 3,75 requests/minuto.
 *
 * Isso é deliberadamente conservador para Free Tier.
 */
const MIN_GEMINI_INTERVAL_MS = 16_000;

/*
 * Se o Gemini mandar esperar mais tempo,
 * respeitamos o tempo informado pela API.
 */
const MAX_SERVER_RETRY_WAIT_MS = 5 * 60 * 1000;

/*
 * Número de tentativas para erros realmente transitórios.
 *
 * Não fazemos retry agressivo em 429.
 */
const MAX_GEMINI_RETRIES =
    Number(process.env.MAX_GEMINI_RETRIES || 2);

/*
|--------------------------------------------------------------------------
| BATCH
|--------------------------------------------------------------------------
*/

/*
 * Tamanho máximo aproximado do lote.
 *
 * O número de blocos sozinho não é suficiente,
 * porque uma legenda pode conter muito texto.
 */
const MAX_BATCH_BLOCKS =
    Number(process.env.MAX_BATCH_BLOCKS || 80);

const MAX_BATCH_CHARS =
    Number(process.env.MAX_BATCH_CHARS || 24_000);

/*
|--------------------------------------------------------------------------
| JOBS
|--------------------------------------------------------------------------
*/

const MAX_ACTIVE_JOBS =
    Number(process.env.MAX_ACTIVE_JOBS || 3);

const JOB_TTL_MS =
    Number(
        process.env.JOB_TTL_MS ||
        2 * 60 * 60 * 1000
    );

/*
|--------------------------------------------------------------------------
| CACHE
|--------------------------------------------------------------------------
*/

const CACHE_TTL_MS =
    Number(
        process.env.CACHE_TTL_MS ||
        24 * 60 * 60 * 1000
    );

const MAX_CACHE_ENTRIES = 500;

const MAX_JOBS = 500;

/*
|--------------------------------------------------------------------------
| SEGURANÇA
|--------------------------------------------------------------------------
*/

const MAX_SOURCE_CHARS = 500_000;

/*
|--------------------------------------------------------------------------
| MEMÓRIA
|--------------------------------------------------------------------------
*/

const translationCache = new Map();

const jobs = new Map();

/*
|--------------------------------------------------------------------------
| CONTROLE GLOBAL GEMINI
|--------------------------------------------------------------------------
*/

let geminiActiveRequests = 0;

let lastGeminiRequestAt = 0;

let geminiCooldownUntil = 0;

/*
 * Fila global.
 *
 * Cada item:
 *
 * {
 *   fn,
 *   resolve,
 *   reject
 * }
 */
const geminiQueue = [];

/*
|--------------------------------------------------------------------------
| ESTATÍSTICAS
|--------------------------------------------------------------------------
*/

const stats = {
    geminiRequests: 0,
    geminiSuccess: 0,
    geminiRateLimits: 0,
    geminiErrors: 0,
    cacheHits: 0,
    cacheMisses: 0,
    jobsCreated: 0,
    jobsCompleted: 0,
    jobsFailed: 0
};

/*
|--------------------------------------------------------------------------
| MANIFEST
|--------------------------------------------------------------------------
*/

const manifest = {
    id: "org.tradutor.stateless.gemini.async",
    version: "5.0.0",

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

function getErrorMessage(error) {
    if (!error) {
        return "Erro desconhecido.";
    }

    if (typeof error === "string") {
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
        req.headers["x-forwarded-proto"] ||
        req.protocol ||
        "https";

    const host =
        req.headers["x-forwarded-host"] ||
        req.get("host");

    return `${protocol}://${host}`;
}

function safeJson(res, data, status = 200) {
    res.status(status);

    res.set(
        "Cache-Control",
        "no-store"
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

    const timer = setTimeout(() => {
        controller.abort();
    }, timeoutMs);

    try {
        return await fetch(url, {
            ...options,
            signal: controller.signal
        });
    } finally {
        clearTimeout(timer);
    }
}

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

    for (const block of blocks) {
        const lines =
            block.split("\n");

        if (lines.length < 3) {
            continue;
        }

        const indexLine =
            lines[0].trim();

        const timingLine =
            lines[1].trim();

        if (!/^\d+$/.test(indexLine)) {
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
            timing: timingLine,
            text: textLines.join("\n")
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
            .map((block, index) => {
                const translated =
                    translatedTexts[index] ??
                    block.text;

                return [
                    block.index,
                    block.timing,
                    translated
                ].join("\n");
            })
            .join("\n\n")
            .trim() + "\n"
    );
}

/*
|--------------------------------------------------------------------------
| VALIDAÇÃO
|--------------------------------------------------------------------------
*/

function validateTranslations(
    originalBlocks,
    translatedTexts
) {
    if (!Array.isArray(translatedTexts)) {
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
                `Quantidade diferente. Esperado ${originalBlocks.length}, recebido ${translatedTexts.length}.`
        };
    }

    for (
        let i = 0;
        i < translatedTexts.length;
        i++
    ) {
        const translated =
            translatedTexts[i];

        if (
            typeof translated !==
            "string"
        ) {
            return {
                valid: false,
                reason:
                    `Bloco ${i + 1} não é texto.`
            };
        }

        if (
            translated.length >
            10_000
        ) {
            return {
                valid: false,
                reason:
                    `Bloco ${originalBlocks[i].index} ficou excessivamente grande.`
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

function scoreSubtitle(subtitle) {
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
        ).toLowerCase() === "srt"
    ) {
        score += 20;
    }

    if (
        subtitle?.name &&
        /english/i.test(
            String(subtitle.name)
        )
    ) {
        score += 10;
    }

    return score;
}

function selectBestSubtitle(
    subtitles
) {
    if (!Array.isArray(subtitles)) {
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
                    sub.url.startsWith(
                        "http"
                    )
                );
            })
            .sort(
                (a, b) =>
                    scoreSubtitle(b) -
                    scoreSubtitle(a)
            );

    return candidates[0] || null;
}

/*
|--------------------------------------------------------------------------
| DOWNLOAD
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
            `Legenda muito grande: ${text.length} caracteres.`
        );
    }

    return text;
}

/*
|--------------------------------------------------------------------------
| CRIAÇÃO DE LOTES
|--------------------------------------------------------------------------
|
| Não usamos apenas quantidade de blocos.
|
| Um lote termina quando:
|
| - atingir MAX_BATCH_BLOCKS
| OU
| - atingir MAX_BATCH_CHARS
|
|--------------------------------------------------------------------------
*/

function createBatches(blocks) {
    const batches = [];

    let current = [];

    let currentChars = 0;

    for (const block of blocks) {
        const blockChars =
            block.text.length;

        const wouldExceedBlocks =
            current.length >=
            MAX_BATCH_BLOCKS;

        const wouldExceedChars =
            current.length > 0 &&
            currentChars +
                blockChars >
                MAX_BATCH_CHARS;

        if (
            wouldExceedBlocks ||
            wouldExceedChars
        ) {
            batches.push(current);

            current = [];

            currentChars = 0;
        }

        current.push(block);

        currentChars += blockChars;
    }

    if (current.length > 0) {
        batches.push(current);
    }

    return batches;
}

/*
|--------------------------------------------------------------------------
| CONTROLE DE QUOTA
|--------------------------------------------------------------------------
*/

function extractRetryDelayMs(
    error
) {
    if (!error) {
        return null;
    }

    /*
     * Primeiro tentamos Retry-After.
     */
    const retryAfter =
        Number(
            error.retryAfter
        );

    if (
        Number.isFinite(
            retryAfter
        ) &&
        retryAfter > 0
    ) {
        return Math.min(
            retryAfter * 1000,
            MAX_SERVER_RETRY_WAIT_MS
        );
    }

    /*
     * Depois procuramos mensagens como:
     *
     * Please retry in 51.983610009s
     */
    const message =
        getErrorMessage(error);

    const match =
        message.match(
            /retry in\s+([\d.]+)s/i
        );

    if (match) {
        const seconds =
            Number(match[1]);

        if (
            Number.isFinite(
                seconds
            ) &&
            seconds > 0
        ) {
            return Math.min(
                Math.ceil(
                    seconds * 1000
                ),
                MAX_SERVER_RETRY_WAIT_MS
            );
        }
    }

    return null;
}

function isRateLimitError(
    error
) {
    return (
        error?.status === 429 ||
        error?.code === 429 ||
        /quota exceeded/i.test(
            getErrorMessage(error)
        ) ||
        /rate.?limit/i.test(
            getErrorMessage(error)
        ) ||
        /resource.?exhausted/i.test(
            getErrorMessage(error)
        )
    );
}

function isPermanentGeminiError(
    error
) {
    return (
        error?.status === 400 ||
        error?.status === 401 ||
        error?.status === 403
    );
}

/*
|--------------------------------------------------------------------------
| FILA GEMINI
|--------------------------------------------------------------------------
*/

function enqueueGemini(fn) {
    return new Promise(
        (resolve, reject) => {
            geminiQueue.push({
                fn,
                resolve,
                reject
            });

            processGeminiQueue();
        }
    );
}

async function processGeminiQueue() {
    if (
        geminiActiveRequests >=
        MAX_CONCURRENT_GEMINI
    ) {
        return;
    }

    const item =
        geminiQueue.shift();

    if (!item) {
        return;
    }

    geminiActiveRequests++;

    try {
        /*
         * Respeita cooldown global.
         */
        const now = Date.now();

        if (
            geminiCooldownUntil >
            now
        ) {
            const wait =
                geminiCooldownUntil -
                now;

            console.log(
                `[GEMINI] Cooldown global: aguardando ${Math.ceil(wait / 1000)}s.`
            );

            await sleep(wait);
        }

        /*
         * Respeita intervalo mínimo.
         */
        const elapsed =
            Date.now() -
            lastGeminiRequestAt;

        if (
            elapsed <
            MIN_GEMINI_INTERVAL_MS
        ) {
            const wait =
                MIN_GEMINI_INTERVAL_MS -
                elapsed;

            console.log(
                `[GEMINI] Intervalo de segurança: ${Math.ceil(wait / 1000)}s.`
            );

            await sleep(wait);
        }

        const result =
            await item.fn();

        item.resolve(result);
    } catch (error) {
        item.reject(error);
    } finally {
        geminiActiveRequests--;

        /*
         * Continua processando a fila.
         */
        setImmediate(
            processGeminiQueue
        );
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
    if (!GEMINI_API_KEY) {
        throw new Error(
            "GEMINI_API_KEY não configurada."
        );
    }

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
            GEMINI_MODEL
        )}:generateContent`;

    let lastError = null;

    for (
        let attempt = 1;
        attempt <=
        MAX_GEMINI_RETRIES + 1;
        attempt++
    ) {
        try {
            console.log(
                `[GEMINI] Request ${attempt}/${MAX_GEMINI_RETRIES + 1}`
            );

            /*
             * Marcamos o instante ANTES da chamada.
             */
            lastGeminiRequestAt =
                Date.now();

            stats.geminiRequests++;

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

                        body: JSON.stringify({
                            systemInstruction: {
                                parts: [
                                    {
                                        text:
                                            "Você é um tradutor profissional de legendas cinematográficas. " +
                                            "Traduza exclusivamente do inglês para Português do Brasil. " +
                                            "Use português brasileiro natural, fluente e contemporâneo. " +
                                            "Preserve sentido, contexto, humor, ironia, gírias, palavrões, " +
                                            "tom emocional, nomes próprios, marcas e termos técnicos. " +
                                            "Não censure. Não resuma. Não explique. Não omita. " +
                                            "Preserve tags de legenda como <i>, </i>, <b>, </b>, " +
                                            "{\\i1}, {\\i0} e semelhantes. " +
                                            "Retorne exclusivamente o JSON solicitado."
                                    }
                                ]
                            },

                            contents: [
                                {
                                    role: "user",

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
                                },

                                /*
                                 * Tradução não precisa
                                 * de raciocínio profundo.
                                 */
                                thinkingConfig: {
                                    thinkingLevel:
                                        "minimal"
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
                        `Resposta não-JSON do Gemini. HTTP ${response.status}.`
                    );

                error.status =
                    response.status;

                throw error;
            }

            if (!response.ok) {
                const message =
                    data?.error?.message ||
                    `HTTP ${response.status}`;

                const error =
                    new Error(message);

                error.status =
                    response.status;

                error.retryAfter =
                    response.headers.get(
                        "retry-after"
                    );

                throw error;
            }

            const text =
                data
                    ?.candidates?.[0]
                    ?.content?.parts
                    ?.map(
                        part =>
                            part?.text || ""
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
        } catch (error) {
            lastError = error;

            if (
                isRateLimitError(
                    error
                )
            ) {
                stats.geminiRateLimits++;

                const delay =
                    extractRetryDelayMs(
                        error
                    );

                /*
                 * Se a API nos disse quanto esperar,
                 * usamos exatamente isso.
                 */
                const wait =
                    delay ||
                    60_000;

                geminiCooldownUntil =
                    Math.max(
                        geminiCooldownUntil,
                        Date.now() +
                            wait
                    );

                console.warn(
                    `[GEMINI] RATE LIMIT. Cooldown global de ${Math.ceil(wait / 1000)}s.`
                );

                /*
                 * NÃO repetimos imediatamente.
                 *
                 * Se ainda houver tentativa disponível,
                 * esperamos o cooldown.
                 */
                if (
                    attempt <=
                    MAX_GEMINI_RETRIES
                ) {
                    await sleep(
                        wait
                    );

                    continue;
                }

                break;
            }

            stats.geminiErrors++;

            console.error(
                `[GEMINI] Erro: ${getErrorMessage(error)}`
            );

            /*
             * Erros permanentes não devem
             * ser repetidos.
             */
            if (
                isPermanentGeminiError(
                    error
                )
            ) {
                break;
            }

            /*
             * Retry simples para erros
             * transitórios.
             */
            if (
                attempt <=
                MAX_GEMINI_RETRIES
            ) {
                const wait =
                    Math.min(
                        5_000 *
                            Math.pow(
                                2,
                                attempt - 1
                            ),
                        30_000
                    );

                console.log(
                    `[GEMINI] Erro transitório. Retry em ${Math.ceil(wait / 1000)}s.`
                );

                await sleep(
                    wait
                );
            }
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
| CHAMADA GEMINI ATRAVÉS DA FILA
|--------------------------------------------------------------------------
*/

function callGemini(prompt) {
    return enqueueGemini(
        () =>
            callGeminiDirect(
                prompt
            )
    );
}

/*
|--------------------------------------------------------------------------
| TRADUZIR LOTE
|--------------------------------------------------------------------------
*/

async function translateBatch(
    blocks
) {
    const payload =
        blocks.map(block => ({
            id: block.index,
            text: block.text
        }));

    const prompt = `
Traduza os blocos abaixo de inglês para Português do Brasil.

REGRAS:

1. Deve existir exatamente um objeto para cada bloco.
2. Preserve exatamente cada ID.
3. Não crie IDs.
4. Não remova IDs.
5. Traduza somente o campo "text".
6. Preserve o significado.
7. Preserve contexto e intenção.
8. Preserve humor, ironia e sarcasmo.
9. Preserve gírias.
10. Preserve palavrões com peso equivalente.
11. Não censure.
12. Não resuma.
13. Não explique.
14. Não adicione comentários.
15. Preserve nomes próprios.
16. Preserve termos técnicos apropriados.
17. Preserve tags de formatação.
18. Não use markdown.
19. Retorne SOMENTE o array JSON.
20. Preserve as quebras de linha internas sempre que possível.

FORMATO EXATO:

[
  {
    "id": 123,
    "text": "Texto traduzido"
  }
]

DADOS:

${JSON.stringify(
    payload
)}
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
        throw new Error(
            "Gemini retornou JSON inválido."
        );
    }

    if (
        !Array.isArray(parsed)
    ) {
        throw new Error(
            "Gemini não retornou uma lista."
        );
    }

    const byId =
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
            byId.set(
                item.id,
                item.text
            );
        }
    }

    const translatedTexts =
        blocks.map(block =>
            byId.get(
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
        throw new Error(
            "Gemini não devolveu todos os blocos."
        );
    }

    const validation =
        validateTranslations(
            blocks,
            translatedTexts
        );

    if (
        !validation.valid
    ) {
        throw new Error(
            validation.reason
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
            "Nenhum bloco SRT válido encontrado."
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

        console.log(
            `[TRANSLATE] Lote ${i + 1}/${batches.length} concluído.`
        );
    }

    if (
        translatedTexts.length !==
        blocks.length
    ) {
        throw new Error(
            "Quantidade final de traduções não corresponde à original."
        );
    }

    const translatedSrt =
        buildSrt(
            blocks,
            translatedTexts
        );

    const finalBlocks =
        parseSrt(
            translatedSrt
        );

    if (
        finalBlocks.length !==
        blocks.length
    ) {
        throw new Error(
            "SRT final perdeu blocos."
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
        stats.cacheMisses++;

        return null;
    }

    if (
        item.expiresAt <=
        Date.now()
    ) {
        translationCache.delete(
            key
        );

        stats.cacheMisses++;

        return null;
    }

    stats.cacheHits++;

    return item.srt;
}

/*
|--------------------------------------------------------------------------
| LIMPEZA
|--------------------------------------------------------------------------
*/

function cleanupMemory() {
    const now =
        Date.now();

    for (
        const [
            key,
            item
        ]
        of translationCache
            .entries()
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
            translationCache.keys()
                .next()
                .value;

        if (
            key === undefined
        ) {
            break;
        }

        translationCache.delete(
            key
        );
    }

    while (
        jobs.size >
        MAX_JOBS
    ) {
        const key =
            jobs.keys()
                .next()
                .value;

        if (
            key === undefined
        ) {
            break;
        }

        const job =
            jobs.get(key);

        if (
            job?.status ===
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
    /*
     * Proteção contra excesso de jobs simultâneos.
     */
    const activeJobs =
        [...jobs.values()]
            .filter(
                job =>
                    job.status ===
                    "processing"
            ).length;

    if (
        activeJobs >=
        MAX_ACTIVE_JOBS
    ) {
        throw new Error(
            "O tradutor está ocupado no momento. Tente novamente em alguns segundos."
        );
    }

    const now =
        Date.now();

    const job = {
        id: jobId,

        cacheKey,

        type,

        videoId,

        sourceHash,

        sourceSrt,

        status:
            "processing",

        result: null,

        error: null,

        createdAt: now,

        updatedAt: now,

        expiresAt:
            now + JOB_TTL_MS,

        promise: null
    };

    jobs.set(
        jobId,
        job
    );

    stats.jobsCreated++;

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

            stats.jobsCompleted++;

            console.log(
                `[JOB ${job.id}] Cache encontrado.`
            );

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

        stats.jobsCompleted++;

        console.log(
            `[JOB ${job.id}] Concluído.`
        );
    } catch (error) {
        job.status =
            "failed";

        job.error =
            getErrorMessage(
                error
            );

        job.updatedAt =
            Date.now();

        stats.jobsFailed++;

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
        await sleep(300);
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
        "⏳ Traduzindo legenda com Gemini...",
        "",
        "2",
        "00:00:07,500 --> 00:00:15,000",
        "A tradução está sendo processada. Tente recarregar as legendas em alguns segundos."
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
| RESPONSE SRT
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
| MANIFEST
|--------------------------------------------------------------------------
*/

app.get(
    "/manifest.json",
    (req, res) => {
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
    (req, res) => {
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

            geminiCooldown:
                Math.max(
                    0,
                    geminiCooldownUntil -
                        Date.now()
                ),

            stats
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
    (req, res) => {
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

            geminiActive:
                geminiActiveRequests,

            geminiCooldownMs:
                Math.max(
                    0,
                    geminiCooldownUntil -
                        Date.now()
                ),

            stats
        });
    }
);

/*
|--------------------------------------------------------------------------
| OPEN SUBTITLES
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

    if (!response.ok) {
        throw new Error(
            `OpenSubtitles retornou HTTP ${response.status}.`
        );
    }

    const data =
        await response.json();

    return selectBestSubtitle(
        data?.subtitles || []
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

        if (!targetSub) {
            console.log(
                `[STREMIO] Nenhuma legenda inglesa encontrada.`
            );

            return safeJson(
                res,
                {
                    subtitles: []
                }
            );
        }

        /*
         * 2. Download.
         */
        const sourceSrt =
            await downloadSubtitle(
                targetSub.url
            );

        /*
         * 3. Validar.
         */
        const originalBlocks =
            parseSrt(
                sourceSrt
            );

        if (
            originalBlocks.length ===
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
         * 5. Cache.
         */
        const cached =
            getTranslationCache(
                cacheKey
            );

        if (cached) {
            const cachedJobId =
                `cached-${sourceHash.slice(
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

            console.log(
                `[CACHE] Resultado pronto.`
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
                                    cachedJobId
                                )}.srt`,

                            lang:
                                "por"
                        }
                    ]
                }
            );
        }

        /*
         * 6. Procurar job em andamento.
         */
        let job = null;

        for (
            const currentJob
                of jobs.values()
        ) {
            if (
                currentJob.cacheKey ===
                    cacheKey &&
                currentJob.status ===
                    "processing"
            ) {
                job =
                    currentJob;

                break;
            }
        }

        /*
         * 7. Criar novo job.
         */
        if (!job) {
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
             * IMPORTANTE:
             *
             * Não bloqueamos o endpoint
             * do Stremio.
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
    } catch (error) {
        console.error(
            "[STREMIO] Erro:",
            getErrorMessage(
                error
            )
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
| RESULTADO DA TRADUÇÃO
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
        jobId =
            rawJobId;
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
     * Já pronto.
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
     * Espera até 25 segundos.
     */
    const completed =
        await waitForJob(
            job,
            25_000
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
     * Ainda não acabou.
     */
    return sendSubtitleResponse(
        res,
        buildProcessingSrt(),
        "no-store, no-cache, must-revalidate"
    );
}

/*
|--------------------------------------------------------------------------
| ROTA SRT
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
            " STREMIO GEMINI SUBTITLE TRANSLATOR v5"
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
            `Intervalo Gemini: ${
                MIN_GEMINI_INTERVAL_MS /
                1000
            }s`
        );

        console.log(
            `Concorrência Gemini: ${MAX_CONCURRENT_GEMINI}`
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
            "Status: ONLINE"
        );

        console.log(
            "=============================================="
        );
    }
);

/*
|--------------------------------------------------------------------------
| PROCESS
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
