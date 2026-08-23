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
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-3.6-flash";

/*
 * No Render, recomendo definir:
 *
 * PUBLIC_URL=https://seu-addon.onrender.com
 *
 * Se não definir, o addon tenta descobrir automaticamente pelo request.
 */
const PUBLIC_URL = (process.env.PUBLIC_URL || "").replace(/\/+$/, "");

/*
 * Limites de segurança/performance.
 */
const SOURCE_FETCH_TIMEOUT_MS = 20_000;
const GEMINI_TIMEOUT_MS = 60_000;

const WAIT_FOR_TRANSLATION_MS = 25_000;

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const JOB_TTL_MS = 2 * 60 * 60 * 1000;   // 2h
const MAX_CACHE_ENTRIES = 500;
const MAX_JOBS = 500;

const BATCH_SIZE = 60;
const MAX_SOURCE_CHARS = 500_000;

const MAX_GEMINI_RETRIES = 3;
const RETRY_BASE_MS = 1_500;

/*
|--------------------------------------------------------------------------
| CACHE E JOBS
|--------------------------------------------------------------------------
|
| translationCache:
|   guarda resultados já concluídos.
|
| jobs:
|   acompanha traduções em andamento/concluídas.
|
| Tudo fica em RAM.
| Portanto, se o Render reiniciar, o cache é perdido.
|--------------------------------------------------------------------------
*/

const translationCache = new Map();
const jobs = new Map();

/*
|--------------------------------------------------------------------------
| MANIFEST
|--------------------------------------------------------------------------
*/

const manifest = {
    id: "org.tradutor.stateless.gemini.async",
    version: "3.0.0",
    name: "Tradutor Gemini Async",
    description:
        "Traduz automaticamente legendas em inglês para Português do Brasil usando Google Gemini.",
    logo: "",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: [],
    behaviorHints: {
        configurable: false,
        adult: false
    }
};

/*
|--------------------------------------------------------------------------
| HELPERS GERAIS
|--------------------------------------------------------------------------
*/

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
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
    if (PUBLIC_URL) return PUBLIC_URL;

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
    res.set("Cache-Control", "no-store");
    return res.json(data);
}

function getErrorMessage(error) {
    if (!error) return "Erro desconhecido";

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

async function fetchWithTimeout(url, options = {}, timeoutMs = 20_000) {
    const controller = new AbortController();

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
| LIMPEZA DE CACHE
|--------------------------------------------------------------------------
*/

function cleanupMemory() {
    const now = Date.now();

    for (const [key, item] of translationCache.entries()) {
        if (item.expiresAt <= now) {
            translationCache.delete(key);
        }
    }

    for (const [key, job] of jobs.entries()) {
        if (
            job.expiresAt <= now &&
            job.status !== "processing"
        ) {
            jobs.delete(key);
        }
    }

    /*
     * Limites máximos para impedir crescimento infinito.
     */
    while (translationCache.size > MAX_CACHE_ENTRIES) {
        const firstKey = translationCache.keys().next().value;

        if (firstKey === undefined) break;

        translationCache.delete(firstKey);
    }

    while (jobs.size > MAX_JOBS) {
        const firstKey = jobs.keys().next().value;

        if (firstKey === undefined) break;

        const job = jobs.get(firstKey);

        /*
         * Nunca eliminamos um job em processamento.
         */
        if (job?.status === "processing") {
            break;
        }

        jobs.delete(firstKey);
    }
}

/*
 * Executa limpeza periodicamente.
 */
setInterval(cleanupMemory, 5 * 60 * 1000).unref();

/*
|--------------------------------------------------------------------------
| NORMALIZAÇÃO DE SRT
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
        .replace(/^```(?:srt|text|plaintext)?\s*/i, "")
        .replace(/\s*```$/i, "")
        .trim();
}

/*
|--------------------------------------------------------------------------
| PARSER SRT
|--------------------------------------------------------------------------
*/

function parseSrt(srt) {
    const normalized = normalizeSrt(srt);

    if (!normalized) {
        return [];
    }

    const blocks = normalized
        .split(/\n{2,}/)
        .map(block => block.trim())
        .filter(Boolean);

    const result = [];

    for (const block of blocks) {
        const lines = block.split("\n");

        if (lines.length < 3) {
            continue;
        }

        const indexLine = lines[0].trim();
        const timingLine = lines[1].trim();

        if (!/^\d+$/.test(indexLine)) {
            continue;
        }

        if (!/^\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{3}/.test(timingLine)) {
            continue;
        }

        const textLines = lines.slice(2);

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

function buildSrt(blocks, translatedTexts) {
    return blocks
        .map((block, index) => {
            const translated = translatedTexts[index] ?? block.text;

            return [
                block.index,
                block.timing,
                translated
            ].join("\n");
        })
        .join("\n\n")
        .trim() + "\n";
}

/*
|--------------------------------------------------------------------------
| VALIDAÇÃO DO SRT
|--------------------------------------------------------------------------
*/

function validateSrt(originalBlocks, translatedTexts) {
    if (!Array.isArray(translatedTexts)) {
        return {
            valid: false,
            reason: "Resultado do Gemini não é um array."
        };
    }

    if (translatedTexts.length !== originalBlocks.length) {
        return {
            valid: false,
            reason: `Quantidade diferente de blocos. Esperado: ${originalBlocks.length}. Recebido: ${translatedTexts.length}.`
        };
    }

    for (let i = 0; i < translatedTexts.length; i++) {
        if (typeof translatedTexts[i] !== "string") {
            return {
                valid: false,
                reason: `Bloco ${i + 1} não é texto.`
            };
        }

        const originalLineCount =
            originalBlocks[i].text.split("\n").length;

        const translatedLineCount =
            translatedTexts[i].split("\n").length;

        /*
         * Permite até uma pequena diferença.
         *
         * A regra ideal é manter o mesmo número de linhas,
         * mas não vale a pena rejeitar tudo por uma diferença
         * mínima em algumas legendas mal formatadas.
         */
        if (Math.abs(originalLineCount - translatedLineCount) > 1) {
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
|
| O OpenSubtitles pode devolver várias opções.
| Tentamos priorizar:
| - inglês
| - não-HI
| - formato SRT
|--------------------------------------------------------------------------
*/

function scoreSubtitle(subtitle) {
    let score = 0;

    const lang = String(subtitle?.lang || "").toLowerCase();

    if (lang === "eng") score += 100;
    else if (lang === "en") score += 90;

    if (subtitle?.hearingImpaired === false) {
        score += 20;
    }

    if (
        subtitle?.format &&
        String(subtitle.format).toLowerCase() === "srt"
    ) {
        score += 20;
    }

    if (
        subtitle?.name &&
        /english/i.test(String(subtitle.name))
    ) {
        score += 10;
    }

    return score;
}

function selectBestSubtitle(subtitles) {
    if (!Array.isArray(subtitles)) {
        return null;
    }

    const candidates = subtitles
        .filter(sub => {
            const lang = String(sub?.lang || "").toLowerCase();

            return (
                (lang === "eng" || lang === "en") &&
                typeof sub?.url === "string" &&
                sub.url.startsWith("http")
            );
        })
        .sort((a, b) => scoreSubtitle(b) - scoreSubtitle(a));

    return candidates[0] || null;
}

/*
|--------------------------------------------------------------------------
| BAIXAR LEGENDA
|--------------------------------------------------------------------------
*/

async function downloadSubtitle(url) {
    console.log(`[SOURCE] Baixando legenda: ${url}`);

    const response = await fetchWithTimeout(
        url,
        {
            headers: {
                "User-Agent":
                    "Stremio-Gemini-Subtitle-Translator/3.0"
            }
        },
        SOURCE_FETCH_TIMEOUT_MS
    );

    if (!response.ok) {
        throw new Error(
            `Falha ao baixar legenda: HTTP ${response.status}`
        );
    }

    const text = normalizeSrt(await response.text());

    if (!text) {
        throw new Error("A legenda baixada está vazia.");
    }

    if (text.length > MAX_SOURCE_CHARS) {
        throw new Error(
            `Legenda muito grande (${text.length} caracteres).`
        );
    }

    return text;
}

/*
|--------------------------------------------------------------------------
| SCHEMA JSON DO GEMINI
|--------------------------------------------------------------------------
|
| Pedimos uma lista:
|
| [
|   { "id": 1, "text": "Olá..." },
|   { "id": 2, "text": "Tudo bem?" }
| ]
|
| Dessa maneira o servidor reconstrói o SRT sozinho.
|--------------------------------------------------------------------------
*/

const translationResponseSchema = {
    type: "ARRAY",
    items: {
        type: "OBJECT",
        properties: {
            id: {
                type: "INTEGER",
                description:
                    "Identificador do bloco recebido. Deve ser devolvido exatamente igual."
            },
            text: {
                type: "STRING",
                description:
                    "Texto traduzido para Português do Brasil."
            }
        },
        required: ["id", "text"]
    }
};

/*
|--------------------------------------------------------------------------
| GEMINI
|--------------------------------------------------------------------------
*/

async function callGemini(prompt) {
    if (!GEMINI_API_KEY) {
        throw new Error(
            "GEMINI_API_KEY não foi configurada no ambiente."
        );
    }

    const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`;

    let lastError = null;

    for (let attempt = 1; attempt <= MAX_GEMINI_RETRIES; attempt++) {
        try {
            console.log(
                `[GEMINI] Tentativa ${attempt}/${MAX_GEMINI_RETRIES}`
            );

            const response = await fetchWithTimeout(
                endpoint,
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "x-goog-api-key": GEMINI_API_KEY
                    },
                    body: JSON.stringify({
                        systemInstruction: {
                            parts: [
                                {
                                    text:
                                        "Você é um tradutor profissional especializado em legendas de filmes e séries. " +
                                        "Traduza do inglês para Português do Brasil natural, fluente e contemporâneo. " +
                                        "Preserve nomes próprios, marcas, expressões quando fizer sentido, intenção, tom, humor, " +
                                        "gírias, palavrões e nuances do diálogo. Não faça censura. " +
                                        "Não explique nada. Não resuma. Não omita informações. " +
                                        "Não altere o significado. " +
                                        "Mantenha a quantidade e os IDs dos blocos exatamente como recebidos. " +
                                        "Retorne exclusivamente o JSON solicitado."
                                }
                            ]
                        },

                        contents: [
                            {
                                role: "user",
                                parts: [
                                    {
                                        text: prompt
                                    }
                                ]
                            }
                        ],

                        generationConfig: {
                            responseMimeType: "application/json",
                            responseSchema:
                                translationResponseSchema,

                            /*
                             * Para tradução estruturada não queremos
                             * um raciocínio extremamente longo.
                             */
                            thinkingConfig: {
                                thinkingLevel: "low"
                            },

                            maxOutputTokens: 8192
                        }
                    })
                },
                GEMINI_TIMEOUT_MS
            );

            const rawText = await response.text();

            let data;

            try {
                data = JSON.parse(rawText);
            } catch {
                throw new Error(
                    `Gemini retornou resposta não-JSON. HTTP ${response.status}.`
                );
            }

            if (!response.ok) {
                const apiMessage =
                    data?.error?.message ||
                    `HTTP ${response.status}`;

                const error = new Error(apiMessage);

                error.status = response.status;
                error.retryAfter =
                    response.headers.get("retry-after");

                throw error;
            }

            const text =
                data?.candidates?.[0]?.content?.parts
                    ?.map(part => part?.text || "")
                    .join("")
                    .trim();

            if (!text) {
                throw new Error(
                    "Gemini não retornou conteúdo."
                );
            }

            return text;
        } catch (error) {
            lastError = error;

            console.error(
                `[GEMINI] Erro na tentativa ${attempt}:`,
                getErrorMessage(error)
            );

            /*
             * Erros de autenticação ou requisição inválida
             * não devem ficar sendo repetidos.
             */
            if (
                error?.status === 400 ||
                error?.status === 401 ||
                error?.status === 403
            ) {
                break;
            }

            if (attempt >= MAX_GEMINI_RETRIES) {
                break;
            }

            let waitMs =
                RETRY_BASE_MS * Math.pow(2, attempt - 1);

            const retryAfterSeconds =
                Number(error?.retryAfter);

            if (
                Number.isFinite(retryAfterSeconds) &&
                retryAfterSeconds > 0
            ) {
                waitMs = Math.min(
                    retryAfterSeconds * 1000,
                    30_000
                );
            }

            /*
             * Pequeno jitter para evitar várias tentativas
             * simultâneas exatamente no mesmo instante.
             */
            waitMs += Math.floor(Math.random() * 500);

            console.log(
                `[GEMINI] Aguardando ${waitMs}ms antes do retry...`
            );

            await sleep(waitMs);
        }
    }

    throw lastError || new Error("Falha desconhecida no Gemini.");
}

/*
|--------------------------------------------------------------------------
| TRADUZ UM LOTE
|--------------------------------------------------------------------------
*/

async function translateBatch(blocks, attempt = 1) {
    const payload = blocks.map(block => ({
        id: block.index,
        text: block.text
    }));

    const prompt = `
Traduza os diálogos abaixo de inglês para Português do Brasil.

REGRAS OBRIGATÓRIAS:

1. Retorne exatamente um objeto para cada bloco recebido.
2. Não remova nenhum ID.
3. Não crie IDs novos.
4. Cada "id" deve permanecer exatamente igual.
5. Traduza somente o conteúdo de "text".
6. Preserve as quebras de linha de cada bloco sempre que possível.
7. Preserve tags de formatação, como <i>, </i>, <b>, </b>, {\i1}, {\i0}, etc.
8. Não coloque markdown.
9. Não coloque explicações.
10. Não escreva texto fora do JSON.
11. Não transforme fala em resumo.
12. Mantenha nomes próprios e termos técnicos apropriados.
13. Prefira português brasileiro natural em vez de tradução literal.
14. Palavrões e linguagem informal devem continuar com o mesmo peso.
15. Não suavize ou censure o texto.
16. Preserve a intenção e o contexto da fala.

FORMATO:
[
  {
    "id": 123,
    "text": "Texto traduzido"
  }
]

DADOS:
${JSON.stringify(payload, null, 2)}
`;

    const raw = await callGemini(prompt);

    let parsed;

    try {
        parsed = JSON.parse(stripCodeFences(raw));
    } catch (error) {
        if (attempt < 2) {
            console.warn(
                "[TRANSLATE] JSON inválido. Repetindo lote..."
            );

            return translateBatch(blocks, attempt + 1);
        }

        throw new Error(
            "Gemini retornou JSON inválido."
        );
    }

    if (!Array.isArray(parsed)) {
        if (attempt < 2) {
            return translateBatch(blocks, attempt + 1);
        }

        throw new Error(
            "Gemini não retornou uma lista de traduções."
        );
    }

    const translatedById = new Map();

    for (const item of parsed) {
        if (
            item &&
            Number.isInteger(item.id) &&
            typeof item.text === "string"
        ) {
            translatedById.set(
                item.id,
                item.text.trim()
            );
        }
    }

    const translatedTexts = blocks.map(block => {
        return translatedById.get(block.index);
    });

    /*
     * Se qualquer bloco desapareceu, falhamos o lote.
     */
    if (translatedTexts.some(text => typeof text !== "string")) {
        if (attempt < 2) {
            console.warn(
                "[TRANSLATE] Gemini perdeu algum bloco. Repetindo lote..."
            );

            return translateBatch(blocks, attempt + 1);
        }

        throw new Error(
            "Gemini não devolveu todos os blocos."
        );
    }

    const validation =
        validateSrt(blocks, translatedTexts);

    if (!validation.valid) {
        if (attempt < 2) {
            console.warn(
                `[TRANSLATE] Validação falhou: ${validation.reason}. Repetindo lote...`
            );

            return translateBatch(blocks, attempt + 1);
        }

        /*
         * Caso a segunda tentativa ainda tenha alguma
         * diferença estrutural, usamos o resultado em vez
         * de perder a tradução inteira.
         */
        console.warn(
            `[TRANSLATE] Resultado aceito apesar da validação: ${validation.reason}`
        );
    }

    return translatedTexts;
}

/*
|--------------------------------------------------------------------------
| TRADUÇÃO COMPLETA
|--------------------------------------------------------------------------
*/

async function translateSrt(originalSrt) {
    const blocks = parseSrt(originalSrt);

    if (blocks.length === 0) {
        throw new Error(
            "Não foi possível identificar blocos SRT válidos."
        );
    }

    console.log(
        `[TRANSLATE] ${blocks.length} blocos encontrados.`
    );

    const batches = [];

    for (let i = 0; i < blocks.length; i += BATCH_SIZE) {
        batches.push(
            blocks.slice(i, i + BATCH_SIZE)
        );
    }

    console.log(
        `[TRANSLATE] Dividindo em ${batches.length} lote(s).`
    );

    const translatedTexts = [];

    for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];

        console.log(
            `[TRANSLATE] Lote ${i + 1}/${batches.length} (${batch.length} blocos)...`
        );

        const translated =
            await translateBatch(batch);

        translatedTexts.push(...translated);

        /*
         * Pequeno intervalo entre lotes.
         * Ajuda a evitar rajadas desnecessárias.
         */
        if (i < batches.length - 1) {
            await sleep(150);
        }
    }

    if (translatedTexts.length !== blocks.length) {
        throw new Error(
            "Quantidade final de blocos traduzidos não corresponde à original."
        );
    }

    const translatedSrt =
        buildSrt(blocks, translatedTexts);

    /*
     * Validação final.
     */
    const finalBlocks = parseSrt(translatedSrt);

    if (finalBlocks.length !== blocks.length) {
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

function setTranslationCache(key, srt) {
    translationCache.set(key, {
        srt,
        createdAt: Date.now(),
        expiresAt: Date.now() + CACHE_TTL_MS
    });

    cleanupMemory();
}

function getTranslationCache(key) {
    const item = translationCache.get(key);

    if (!item) {
        return null;
    }

    if (item.expiresAt <= Date.now()) {
        translationCache.delete(key);
        return null;
    }

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
    const now = Date.now();

    const job = {
        id: jobId,

        cacheKey,
        type,
        videoId,
        sourceHash,

        sourceSrt,

        status: "processing",

        result: null,
        error: null,

        createdAt: now,
        updatedAt: now,

        expiresAt: now + JOB_TTL_MS,

        promise: null
    };

    jobs.set(jobId, job);

    return job;
}

function getJob(jobId) {
    const job = jobs.get(jobId);

    if (!job) {
        return null;
    }

    if (
        job.expiresAt <= Date.now() &&
        job.status !== "processing"
    ) {
        jobs.delete(jobId);
        return null;
    }

    return job;
}

/*
|--------------------------------------------------------------------------
| PROCESSAMENTO DO JOB
|--------------------------------------------------------------------------
*/

async function processJob(job) {
    console.log(
        `[JOB ${job.id}] Iniciando tradução.`
    );

    try {
        /*
         * Primeiro verifica cache global.
         */
        const cached =
            getTranslationCache(job.cacheKey);

        if (cached) {
            job.status = "completed";
            job.result = cached;
            job.updatedAt = Date.now();

            console.log(
                `[JOB ${job.id}] Resultado já estava no cache.`
            );

            return;
        }

        const translated =
            await translateSrt(job.sourceSrt);

        setTranslationCache(
            job.cacheKey,
            translated
        );

        job.status = "completed";
        job.result = translated;
        job.updatedAt = Date.now();

        console.log(
            `[JOB ${job.id}] Tradução concluída.`
        );
    } catch (error) {
        job.status = "failed";
        job.error = getErrorMessage(error);
        job.updatedAt = Date.now();

        console.error(
            `[JOB ${job.id}] Falha: ${job.error}`
        );
    }
}

/*
|--------------------------------------------------------------------------
| ESPERAR JOB POR UM TEMPO LIMITADO
|--------------------------------------------------------------------------
*/

async function waitForJob(job, timeoutMs) {
    if (job.status === "completed") {
        return true;
    }

    if (job.status === "failed") {
        return false;
    }

    const start = Date.now();

    while (
        job.status === "processing" &&
        Date.now() - start < timeoutMs
    ) {
        await sleep(300);
    }

    return job.status === "completed";
}

/*
|--------------------------------------------------------------------------
| LEGENDA DE AVISO
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
        "Aguarde alguns segundos e tente recarregar as legendas."
    ].join("\n");
}

function buildErrorSrt(message) {
    const safeMessage = String(message || "Erro desconhecido")
        .replace(/\s+/g, " ")
        .slice(0, 180);

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
| RESPOSTA DE SUBTITLE
|--------------------------------------------------------------------------
*/

function sendSubtitleResponse(res, srt, cacheControl = "no-store") {
    res.status(200);

    res.set({
        "Content-Type":
            "text/plain; charset=utf-8",

        "Cache-Control": cacheControl,

        "Access-Control-Allow-Origin": "*"
    });

    return res.send(srt);
}

/*
|--------------------------------------------------------------------------
| MANIFEST
|--------------------------------------------------------------------------
*/

app.get("/manifest.json", (req, res) => {
    res.json(manifest);
});

/*
|--------------------------------------------------------------------------
| HEALTH CHECK
|--------------------------------------------------------------------------
*/

app.get("/", (req, res) => {
    res.json({
        name: manifest.name,
        version: manifest.version,
        status: "online",
        model: GEMINI_MODEL
    });
});

app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        uptime: process.uptime(),
        jobs: jobs.size,
        cache: translationCache.size,
        model: GEMINI_MODEL
    });
});

/*
|--------------------------------------------------------------------------
| BUSCA DE LEGENDA NO STREMIO
|--------------------------------------------------------------------------
*/

async function findEnglishSubtitle(type, id) {
    const searchUrl =
        `https://opensubtitles-v3.strem.io/subtitles/${encodeURIComponent(type)}/${encodeURIComponent(id)}.json`;

    console.log(
        `[STREMIO] Buscando legendas originais: ${searchUrl}`
    );

    const response = await fetchWithTimeout(
        searchUrl,
        {
            headers: {
                "Accept": "application/json",
                "User-Agent":
                    "Stremio-Gemini-Subtitle-Translator/3.0"
            }
        },
        SOURCE_FETCH_TIMEOUT_MS
    );

    if (!response.ok) {
        throw new Error(
            `OpenSubtitles retornou HTTP ${response.status}.`
        );
    }

    const data = await response.json();

    return selectBestSubtitle(
        data?.subtitles || []
    );
}

/*
|--------------------------------------------------------------------------
| ENDPOINT SUBTITLES
|--------------------------------------------------------------------------
*/

async function subtitlesHandler(req, res) {
    const type = String(req.params.type || "").trim();
    const id = String(req.params.id || "").trim();

    console.log(
        `[STREMIO] Pedido de legenda: ${type}/${id}`
    );

    if (!type || !id) {
        return safeJson(res, {
            subtitles: []
        });
    }

    try {
        /*
         * 1. Procurar legenda em inglês.
         */
        const targetSub =
            await findEnglishSubtitle(type, id);

        if (!targetSub) {
            console.log(
                `[STREMIO] Nenhuma legenda em inglês encontrada para ${type}/${id}.`
            );

            return safeJson(res, {
                subtitles: []
            });
        }

        /*
         * 2. Baixar o SRT original.
         *
         * Fazemos isso ANTES de gerar o job porque
         * o hash do conteúdo da legenda será a nossa
         * verdadeira identidade.
         */
        const sourceSrt =
            await downloadSubtitle(targetSub.url);

        /*
         * 3. Validar se parece realmente SRT.
         */
        const originalBlocks =
            parseSrt(sourceSrt);

        if (originalBlocks.length === 0) {
            console.warn(
                `[STREMIO] Legenda encontrada, mas não parece um SRT válido.`
            );

            return safeJson(res, {
                subtitles: []
            });
        }

        /*
         * 4. Hash do conteúdo.
         *
         * Isso é melhor que usar somente:
         * type + id
         *
         * porque diferentes versões da mesma legenda
         * podem existir para o mesmo filme/episódio.
         */
        const sourceHash = sha256(sourceSrt);

        const cacheKey =
            `${type}:${id}:${sourceHash}`;

        /*
         * 5. Cache já concluído?
         */
        const cached =
            getTranslationCache(cacheKey);

        const baseUrl = cleanBaseUrl(req);

        if (cached) {
            console.log(
                `[CACHE] Tradução pronta para ${type}/${id}.`
            );

            /*
             * Criamos um job concluído para que a URL
             * de subtitle seja estável.
             */
            const existingJobId =
                `cached-${sourceHash.slice(0, 24)}`;

            let cachedJob =
                getJob(existingJobId);

            if (!cachedJob) {
                cachedJob = createJob({
                    jobId: existingJobId,
                    cacheKey,
                    type,
                    videoId: id,
                    sourceHash,
                    sourceSrt
                });

                cachedJob.status = "completed";
                cachedJob.result = cached;
            }

            return safeJson(res, {
                subtitles: [
                    {
                        id:
                            `${id}-gemini-${sourceHash.slice(0, 12)}`,

                        url:
                            `${baseUrl}/subtitle/${encodeURIComponent(existingJobId)}.srt`,

                        lang: "por"
                    }
                ]
            });
        }

        /*
         * 6. Encontrar job existente.
         *
         * Isso impede duas traduções simultâneas da mesma
         * legenda.
         */
        let job = null;

        for (const currentJob of jobs.values()) {
            if (
                currentJob.cacheKey === cacheKey &&
                currentJob.status === "processing"
            ) {
                job = currentJob;
                break;
            }
        }

        /*
         * 7. Se não existe job, cria.
         */
        if (!job) {
            const jobId =
                `job-${sourceHash.slice(0, 24)}-${randomId(8)}`;

            job = createJob({
                jobId,
                cacheKey,
                type,
                videoId: id,
                sourceHash,
                sourceSrt
            });

            /*
             * IMPORTANTE:
             *
             * Disparamos sem bloquear a resposta do Stremio.
             */
            job.promise = processJob(job)
                .catch(error => {
                    console.error(
                        `[JOB ${job.id}] Erro inesperado:`,
                        error
                    );

                    job.status = "failed";
                    job.error =
                        getErrorMessage(error);
                });
        }

        /*
         * 8. Entregamos uma URL estável.
         *
         * O Stremio poderá então pedir esse arquivo.
         * Esse endpoint consegue esperar a tradução por
         * alguns segundos antes de devolver a legenda.
         */
        const subtitleUrl =
            `${baseUrl}/subtitle/${encodeURIComponent(job.id)}.srt`;

        console.log(
            `[STREMIO] Entregando URL de job: ${subtitleUrl}`
        );

        return safeJson(res, {
            subtitles: [
                {
                    id:
                        `${id}-gemini-${sourceHash.slice(0, 12)}`,

                    url: subtitleUrl,

                    lang: "por"
                }
            ]
        });
    } catch (error) {
        console.error(
            "[STREMIO] Erro no endpoint de subtitles:",
            error
        );

        return safeJson(res, {
            subtitles: []
        });
    }
}

/*
 * Compatibilidade com as duas formas comuns:
 *
 * /subtitles/movie/tt123.json
 * /subtitles/movie/tt123/qualquer-extra.json
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
| ENTREGA DA LEGENDA TRADUZIDA
|--------------------------------------------------------------------------
*/

async function subtitleResultHandler(req, res) {
    const rawJobId =
        String(req.params.jobId || "").trim();

    const jobId = decodeURIComponent(rawJobId);

    if (!jobId) {
        return sendSubtitleResponse(
            res,
            buildErrorSrt("Job inválido.")
        );
    }

    const job = getJob(jobId);

    if (!job) {
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
    if (job.status === "completed" && job.result) {
        return sendSubtitleResponse(
            res,
            job.result,
            "public, max-age=86400"
        );
    }

    /*
     * Falhou.
     */
    if (job.status === "failed") {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(job.error),
            "no-store"
        );
    }

    /*
     * Ainda processando.
     *
     * Em vez de responder imediatamente,
     * aguardamos um pouco.
     *
     * Isso aumenta bastante a chance de o Stremio
     * receber a tradução na primeira tentativa.
     */
    const completed =
        await waitForJob(
            job,
            WAIT_FOR_TRANSLATION_MS
        );

    if (
        completed &&
        job.status === "completed" &&
        job.result
    ) {
        return sendSubtitleResponse(
            res,
            job.result,
            "public, max-age=86400"
        );
    }

    /*
     * Ainda não terminou.
     *
     * Não devemos devolver erro HTTP para o Stremio.
     */
    if (job.status === "failed") {
        return sendSubtitleResponse(
            res,
            buildErrorSrt(job.error),
            "no-store"
        );
    }

    return sendSubtitleResponse(
        res,
        buildProcessingSrt(),
        "no-store, no-cache, must-revalidate"
    );
}

/*
|--------------------------------------------------------------------------
| ENDPOINT DA LEGENDA
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

app.listen(PORT, () => {
    console.log("==============================================");
    console.log("  STREMIO GEMINI SUBTITLE TRANSLATOR");
    console.log("==============================================");
    console.log(`Porta: ${PORT}`);
    console.log(`Modelo Gemini: ${GEMINI_MODEL}`);
    console.log(
        `PUBLIC_URL: ${PUBLIC_URL || "(automático)"}`
    );
    console.log(
        `Cache TTL: ${CACHE_TTL_MS / 1000 / 60 / 60}h`
    );
    console.log(
        `Batch size: ${BATCH_SIZE}`
    );
    console.log("Status: ONLINE");
    console.log("==============================================");
});

/*
|--------------------------------------------------------------------------
| TRATAMENTO DE PROCESSOS
|--------------------------------------------------------------------------
*/

process.on("unhandledRejection", error => {
    console.error(
        "[PROCESS] Unhandled rejection:",
        error
    );
});

process.on("uncaughtException", error => {
    console.error(
        "[PROCESS] Uncaught exception:",
        error
    );
});
