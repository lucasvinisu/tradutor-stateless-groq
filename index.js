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
 * Tamanho de lote otimizado para reduzir
 * chamadas sem exagerar no tamanho da resposta.
 */
const MAX_BATCH_CHARS =
    Number(
        process.env.MAX_BATCH_CHARS ||
        28_000
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
const MAX_BATCH_BLOCKS =
    Number(
        process.env.MAX_BATCH_BLOCKS ||
        250
    );

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
 * É apenas um teto de saída, não uma quantidade
 * de tokens consumida obrigatoriamente.
 * 16k deixa folga para lotes de até 28k caracteres.
 */
const MAX_OUTPUT_TOKENS =
    Number(
        process.env.MAX_OUTPUT_TOKENS ||
        16_000
    );

/*
 * Não fazemos dezenas de retries normais.
 */
const MAX_NORMAL_RETRIES = 2;

/*
 * Resposta estrutural inválida pode ser dividida
 * em grupos progressivamente menores.
 *
 * A recuperação parcial de IDs é preferida e
 * normalmente evita chegar a este fallback.
 */
const MAX_BAD_OUTPUT_SPLIT_DEPTH = 5;

/*
 * Se até um único bloco vier com saída estrutural
 * inválida, permitimos uma tentativa estrutural extra.
 */
const MAX_SINGLE_BLOCK_OUTPUT_RETRIES = 1;

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
| FILA DE JOBS COMPLETOS
|--------------------------------------------------------------------------
|
| Apenas uma legenda inteira é traduzida por vez.
|
| Isso impede que OpenSubtitles e legenda embutida
| alternem seus lotes e disputem a mesma fila Gemini.
|
| A fila individual do Gemini continua existindo
| logo abaixo como segunda camada de proteção.
|
*/

const translationJobQueue = [];

let translationJobWorkerRunning =
    false;

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
    const safeMs =
        Math.max(
            0,
            Number(ms) || 0
        );

    if (
        safeMs === 0
    ) {
        assertBeforeDeadline(
            deadlineAt
        );

        return;
    }

    const remaining =
        remainingBeforeDeadline(
            deadlineAt
        );

    if (
        Number.isFinite(
            remaining
        ) &&
        remaining <=
            safeMs
    ) {
        if (
            remaining > 0
        ) {
            await sleep(
                remaining
            );
        }

        throw translationTimeoutError();
    }

    await sleep(
        safeMs
    );

    assertBeforeDeadline(
        deadlineAt
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
| LIMPEZA SDH / CC
|--------------------------------------------------------------------------
*/

const SDH_CUE_WORDS =
    /laugh|laughing|chuckle|giggle|sigh|gasp|inhale|exhale|whimper|cry|sobb|music|song playing|applause|cheer|clap|door|phone|ring|buzz|beep|groan|grunt|scream|yell|shout|whisper|murmur|inaudible|indistinct|foreign language|clears? throat|sniff|cough/i;

/*
|--------------------------------------------------------------------------
| CONTEXTO OCULTO DE FALANTE - 5.6
|--------------------------------------------------------------------------
|
| Algumas fontes trazem informações úteis como:
|
| [Adam] I'm excited.
| KELLY: I loved it.
|
| Antes nós removíamos isso para deixar a legenda limpa e a informação
| desaparecia completamente antes de chegar ao Gemini.
|
| Agora preservamos o nome apenas INTERNAMENTE em um marcador que nunca
| aparece na legenda final. O parser remove o marcador e envia o nome ao
| Gemini no campo opcional "speaker".
|
| Se um bloco tiver dois nomes diferentes, não atribuímos um único falante
| ao bloco inteiro, evitando fornecer contexto incorreto.
|
*/

const SPEAKER_HINT_MARKER_REGEX =
    /^@@SPK:([^@]+)@@\s*/u;

function normalizeSpeakerHint(
    value
) {
    const speaker =
        String(
            value || ""
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
        speaker.length > 60
    ) {
        return "";
    }

    /*
     * Não confundimos rubricas SDH com nomes.
     */
    if (
        SDH_CUE_WORDS.test(
            speaker
        )
    ) {
        return "";
    }

    /*
     * Evita guardar frases completas como se fossem nome.
     */
    if (
        /[!?;]/u.test(
            speaker
        )
    ) {
        return "";
    }

    return speaker;
}

function encodeSpeakerHint(
    speaker
) {
    return encodeURIComponent(
        String(
            speaker || ""
        )
    );
}

function decodeSpeakerHint(
    encoded
) {
    try {
        return normalizeSpeakerHint(
            decodeURIComponent(
                String(
                    encoded || ""
                )
            )
        );
    } catch {
        return "";
    }
}

function extractSpeakerHint(
    line
) {
    const original =
        String(
            line || ""
        );

    /*
     * Marcador interno já existente.
     * Isso torna a limpeza idempotente.
     */
    const hiddenMatch =
        original.match(
            SPEAKER_HINT_MARKER_REGEX
        );

    if (
        hiddenMatch
    ) {
        return {
            speaker:
                decodeSpeakerHint(
                    hiddenMatch[1]
                ),

            lineForCleaning:
                original.replace(
                    SPEAKER_HINT_MARKER_REGEX,
                    ""
                )
        };
    }

    /*
     * [Adam] Hello
     * - [Kelly Clarkson] Hello
     */
    const bracketMatch =
        original.match(
            /^\s*[-–—]?\s*\[([^\]]{1,60})\]\s*/u
        );

    if (
        bracketMatch
    ) {
        const speaker =
            normalizeSpeakerHint(
                bracketMatch[1]
            );

        if (speaker) {
            return {
                speaker,
                lineForCleaning:
                    original
            };
        }
    }

    /*
     * ADAM: Hello
     * Kelly Clarkson: Hello
     *
     * Esta heurística é conservadora e usa apenas o prefixo curto
     * que o próprio limpador já tratava como rótulo de falante.
     */
    const colonMatch =
        original.match(
            /^\s*[-–—]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9 .'-]{0,50}):\s+(?=\S)/u
        );

    if (
        colonMatch
    ) {
        const speaker =
            normalizeSpeakerHint(
                colonMatch[1]
            );

        if (speaker) {
            return {
                speaker,
                lineForCleaning:
                    original
            };
        }
    }

    return {
        speaker:
            "",

        lineForCleaning:
            original
    };
}

/*
|--------------------------------------------------------------------------
| NORMALIZAÇÃO DE ALONGAMENTOS VOCAIS - 5.6
|--------------------------------------------------------------------------
|
| A duração de uma nota ou de uma sílaba já é transmitida pelo áudio.
| A legenda deve mostrar a palavra, não desenhar a duração da voz.
|
| Exemplos:
|
| casa-a-a-a-a-a  -> casa
| ce-e-e-e-e-rto  -> certo
| ye-e-e-e-es      -> yes
| nãããããão         -> não   (pós-tradução)
|
| A regra com hífen é extremamente específica: só colapsa a MESMA letra
| repetida várias vezes. Portanto não mexe em palavras normais como
| "bem-vindo", "guarda-chuva" ou "segunda-feira".
|
*/

function normalizeHyphenatedVocalElongations(
    text
) {
    return String(
        text ?? ""
    ).replace(
        /([A-Za-zÀ-ÖØ-öø-ÿ])(?:[-–—]\1){2,}[-–—]?/giu,
        "$1"
    );
}

function normalizeTranslatedVocalElongations(
    text
) {
    let result =
        normalizeHyphenatedVocalElongations(
            text
        );

    /*
     * Depois da tradução, quatro ou mais vogais idênticas consecutivas
     * são quase sempre representação visual de voz prolongada.
     *
     * O limiar de 4 é deliberadamente conservador: não toca em palavras
     * legítimas com vogal dupla, como "voo", "enjoo" ou "creem".
     */
    result =
        result.replace(
            /([AEIOUÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜaeiouáàâãäéèêëíìîïóòôõöúùûü])\1{3,}/gu,
            "$1"
        );

    return result;
}

function cleanDialogueLine(
    line
) {
    let text =
        String(
            line || ""
        ).trim();

    if (!text) {
        return "";
    }

    /*
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
     * Remove rubricas entre parênteses apenas
     * quando parecem descrição de som/ação.
     * Não apaga qualquer texto entre parênteses.
     */
    text =
        text.replace(
            /\s*\(([^)]*)\)\s*/gu,
            (
                match,
                inside
            ) =>
                SDH_CUE_WORDS.test(
                    String(
                        inside || ""
                    )
                )
                    ? " "
                    : match
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
     * Linhas que ficaram somente com marcação
     * de diálogo ou símbolos musicais.
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

    if (!normalized) {
        return "";
    }

    const rawBlocks =
        normalized.split(
            /\n{2,}/
        );

    const cleanedBlocks = [];

    let removedBlocks = 0;
    let changedLines = 0;
    let speakerHintBlocks = 0;
    let elongatedLines = 0;

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
            timingIndex === -1
        ) {
            continue;
        }

        const timing =
            lines[
                timingIndex
            ].trim();

        const cleanedDialogue = [];
        const speakerHints =
            new Set();

        for (
            const line
            of lines.slice(
                timingIndex + 1
            )
        ) {
            const speakerInfo =
                extractSpeakerHint(
                    line
                );

            if (
                speakerInfo.speaker
            ) {
                speakerHints.add(
                    speakerInfo.speaker
                );
            }

            const cleanedBeforeElongation =
                cleanDialogueLine(
                    speakerInfo.lineForCleaning
                );

            const cleaned =
                normalizeHyphenatedVocalElongations(
                    cleanedBeforeElongation
                );

            if (
                cleaned !==
                cleanedBeforeElongation
            ) {
                elongatedLines++;
            }

            if (
                cleaned !==
                line.trim()
            ) {
                changedLines++;
            }

            if (cleaned) {
                cleanedDialogue.push(
                    cleaned
                );
            }
        }

        if (
            cleanedDialogue.length ===
            0
        ) {
            removedBlocks++;
            continue;
        }

        /*
         * Só preservamos um speakerHint quando há exatamente
         * um único nome confiável no bloco inteiro.
         */
        let speakerHint =
            "";

        if (
            speakerHints.size ===
            1
        ) {
            speakerHint =
                Array.from(
                    speakerHints
                )[0];

            cleanedDialogue[0] =
                `@@SPK:${encodeSpeakerHint(
                    speakerHint
                )}@@ ${cleanedDialogue[0]}`;

            speakerHintBlocks++;
        }

        cleanedBlocks.push({
            timing,
            dialogue:
                cleanedDialogue
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
                    ].join("\n")
            )
            .join("\n\n")
            .trim();

    console.log(
        `[CLEAN] SDH/CC: ${rawBlocks.length} -> ${cleanedBlocks.length} blocos; ${removedBlocks} removidos; ${changedLines} linha(s) alterada(s).`
    );

    console.log(
        `[CLEAN] Contexto de falante: ${speakerHintBlocks} bloco(s) preservado(s).`
    );

    console.log(
        `[CLEAN] Alongamentos vocais na fonte: ${elongatedLines} linha(s) normalizada(s).`
    );

    return result
        ? result + "\n"
        : "";
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

        const textLines =
            lines.slice(2);

        let speakerHint =
            "";

        if (
            textLines.length >
            0
        ) {
            const markerMatch =
                textLines[0].match(
                    SPEAKER_HINT_MARKER_REGEX
                );

            if (
                markerMatch
            ) {
                speakerHint =
                    decodeSpeakerHint(
                        markerMatch[1]
                    );

                textLines[0] =
                    textLines[0].replace(
                        SPEAKER_HINT_MARKER_REGEX,
                        ""
                    );
            }
        }

        result.push({
            index:
                Number(indexLine),

            timing:
                timingLine,

            text:
                textLines
                    .join("\n"),

            speakerHint:
                speakerHint ||
                null
        });
    }

    return result;
}

/*
|--------------------------------------------------------------------------
| LIMPEZA DE MARCADORES DE DIÁLOGO TRADUZIDOS
|--------------------------------------------------------------------------
|
| Remove hífen, meia-risca, travessão ou barra quando aparecem apenas
| como marcador solto no começo de uma fala.
|
| Exemplo:
|
| - Eu não acredito nisso.
|
| vira:
|
| Eu não acredito nisso.
|
| Mas um bloco com dois falantes continua preservado:
|
| - Você está pronta?
| - Estou.
|
| Assim não destruímos a indicação real de troca de falante.
|
*/

function cleanTranslatedDialogueMarkers(
    text
) {
    const normalized =
        String(
            text ?? ""
        )
            .replace(
                /\r\n/g,
                "\n"
            )
            .replace(
                /\r/g,
                "\n"
            );

    const lines =
        normalized.split(
            "\n"
        );

    const markerRegex =
        /^\s*[-–—/]+\s+(?=\S)/u;

    let markedDialogueLines =
        0;

    let nonEmptyLines =
        0;

    for (
        const line
        of lines
    ) {
        if (
            line.trim()
        ) {
            nonEmptyLines++;
        }

        if (
            markerRegex.test(
                line
            )
        ) {
            markedDialogueLines++;
        }
    }

    /*
     * Dois ou mais marcadores em linhas distintas
     * normalmente significam troca real de falantes.
     */
    const isRealMultiSpeakerBlock =
        nonEmptyLines >= 2 &&
        markedDialogueLines >= 2;

    if (
        isRealMultiSpeakerBlock
    ) {
        return normalized;
    }

    return lines
        .map(
            line =>
                line.replace(
                    /^\s*[-–—/]+\s+(?=\S)/u,
                    ""
                )
        )
        .join("\n");
}

function cleanAllTranslatedDialogueMarkers(
    translatedTexts
) {
    let changedBlocks =
        0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(
                        text ?? ""
                    );

                const result =
                    cleanTranslatedDialogueMarkers(
                        original
                    );

                if (
                    result !==
                    original
                ) {
                    changedBlocks++;
                }

                return result;
            }
        );

    console.log(
        `[CLEAN] Marcadores de diálogo: ${changedBlocks} bloco(s) ajustado(s).`
    );

    return cleaned;
}

/*
|--------------------------------------------------------------------------
| POLIMENTO DE ALONGAMENTOS NA TRADUÇÃO - 5.6
|--------------------------------------------------------------------------
*/

function cleanAllTranslatedVocalElongations(
    translatedTexts
) {
    let changedBlocks =
        0;

    const cleaned =
        translatedTexts.map(
            text => {
                const original =
                    String(
                        text ?? ""
                    );

                const result =
                    normalizeTranslatedVocalElongations(
                        original
                    );

                if (
                    result !==
                    original
                ) {
                    changedBlocks++;
                }

                return result;
            }
        );

    console.log(
        `[CLEAN] Alongamentos vocais traduzidos: ${changedBlocks} bloco(s) ajustado(s).`
    );

    return cleaned;
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
                        "Você é um tradutor, localizador e adaptador profissional de legendas para Português do Brasil, especializado em diálogo audiovisual contemporâneo. " +
                        "Seu objetivo não é produzir uma tradução de dicionário: é fazer a fala soar como uma pessoa brasileira realmente falaria naquela situação, preservando intenção, humor, personalidade, ironia, shade, camp, provocação, carinho e ritmo cômico. " +
                        "A entrada será majoritariamente em inglês, mas pode conter trechos em italiano, espanhol, francês ou outros idiomas; traduza também esses trechos para PT-BR quando forem conteúdo falado ou cantado relevante. " +
                        "Em reality shows, competição, conversa informal, cultura pop e cenas descontraídas, prefira PT-BR oral, espontâneo e natural. Use gírias brasileiras somente quando combinarem de verdade com o contexto; não force caricaturas nem gírias aleatórias. " +
                        "Antes de traduzir literalmente uma expressão curta ou ambígua, interprete a intenção usando os blocos vizinhos. Pontuação, quebras de linha e segmentação da legenda podem ser imperfeitas; trate os blocos próximos como contexto de uma conversa contínua, mas nunca mova conteúdo de um ID para outro. " +
                        "Reconheça vocativos coloquiais. Palavras como girl, bitch, honey, sis, queen, baby e babe nem sempre são substantivos literais. Dependendo do tom, podem equivaler a gata, amiga, mana, bicha, querida, amor ou até ser omitidas quando isso soar mais natural. " +
                        "Nunca traduza automaticamente girl como garota nem bitch como vadia. Use vadia apenas quando houver intenção real de insulto. Em fala camp, afetiva, debochada ou entre queens, escolha a solução brasileira que preserve o humor e o tom. " +
                        "Exemplo de interpretação: 'Kenya got you, girl' em tom de shade significa algo como 'Mas a Kenya te pegou, gata', e não 'Kenya pegou a sua garota'. " +
                        "Exemplo de registro: 'Vita is quiet, but this bitch is a silent killer' em contexto camp pode soar como 'A Vita é calada, mas essa bicha é uma assassina silenciosa', em vez de traduzir bitch mecanicamente como vadia. " +
                        "Adapte expressões idiomáticas, piadas e trocadilhos quando existir uma solução natural em PT-BR que preserve a intenção e a graça. Se a adaptação ficar forçada, confusa ou perder um bordão reconhecível, preserve o termo original. " +
                        "Exemplo: preserve 'Condragulations' como 'Condragulations'; não invente neologismos como 'Parabravas'. " +
                        "Quando houver letra de música realmente transcrita na legenda, traduza seu conteúdo para PT-BR, mesmo que esteja em um idioma diferente do inglês. Não invente letras quando houver apenas marcações como [music], símbolos musicais ou descrições de som. " +
                        "Preserve nomes próprios, marcas, títulos, termos técnicos, palavrões, intensidade emocional e intenção. Não censure. Não resuma. Não explique. " +
                        "Não acrescente nomes de falantes, descrições de sons, rubricas SDH/CC ou observações que não existam no texto recebido. Traduza somente o campo text e mantenha exatamente os IDs recebidos. " +
                        "Algumas entradas podem trazer um campo opcional speaker. Esse campo é somente contexto oculto de quem fala: nunca o copie para o campo text e nunca acrescente o nome do falante à legenda final. Quando speaker identificar claramente uma pessoa, respeite o gênero gramatical dessa pessoa em adjetivos, particípios e construções em primeira pessoa. " +
                        "Quando não houver speaker ou a identidade do falante não estiver clara, NÃO chute masculino ou feminino. Prefira uma formulação brasileira natural sem marca de gênero sempre que isso evitar uma escolha incerta. Por exemplo, em vez de adivinhar entre 'estou empolgado' e 'estou empolgada', uma solução como 'Mal posso esperar', 'Que empolgação' ou outra construção neutra pode ser melhor conforme o contexto. Nunca use formas artificiais como empolgado(a), empolgade ou barras de gênero. " +
                        "Não reproduza graficamente notas ou sílabas sustentadas. Alongamentos como home-e-e-e, ce-e-e-e-rto, sooooo ou nããããão representam duração da voz e devem virar a palavra normal adequada ao sentido. O áudio já transmite a duração. Preserve apenas vocalizações que tenham valor real de fala, como 'ah', 'oh' ou 'hmm', sem multiplicar letras desnecessariamente. " +
                        "Não acrescente traços, travessões, barras ou marcadores de diálogo que não sejam necessários. Preserve marcadores somente quando forem realmente necessários para distinguir dois ou mais falantes no mesmo bloco. " +
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

                minItems: 1
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
        remaining <= 0
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

            geminiQueue.push(
                {
                    prompt,
                    deadlineAt,
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
            const item =
                geminiQueue.shift();

            if (!item) {
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

            while (!finished) {
                try {
                    assertBeforeDeadline(
                        item.deadlineAt
                    );

                    const cooldown =
                        getCooldownRemaining();

                    if (
                        cooldown > 0
                    ) {
                        console.log(
                            `[GEMINI] Fila aguardando cooldown de ${Math.ceil(
                                cooldown / 1000
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
                        const wait =
                            MIN_REQUEST_INTERVAL_MS -
                            sinceLast;

                        await sleepWithDeadline(
                            wait,
                            item.deadlineAt
                        );
                    }

                    assertBeforeDeadline(
                        item.deadlineAt
                    );

                    console.log(
                        `[GEMINI] Request ${normalAttempt}/${
                            MAX_NORMAL_RETRIES + 1
                        }`
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

                    const message =
                        getErrorMessage(
                            error
                        );

                    console.error(
                        `[GEMINI] Erro: ${message}`
                    );

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
                         * Rate limit não consome uma tentativa normal.
                         * Esperamos o cooldown e tentamos de novo,
                         * sempre respeitando o deadline do job.
                         */
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
                                wait / 1000
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
            block => {
                const item = {
                    id:
                        block.index,

                    text:
                        block.text
                };

                /*
                 * speaker é CONTEXTO OCULTO.
                 * Só aparece quando a fonte realmente ofereceu
                 * uma identificação confiável de falante.
                 */
                if (
                    block.speakerHint
                ) {
                    item.speaker =
                        block.speakerHint;
                }

                return item;
            }
        );

    return `
Traduza e localize os textos abaixo para Português do Brasil natural.
A entrada será principalmente em inglês, mas pode conter trechos em outros idiomas. Traduza também esses trechos quando forem fala ou letra de música realmente transcrita.

OBJETIVO DE ESTILO:

A legenda deve soar como diálogo brasileiro real e contemporâneo, e não como tradução literal de dicionário.
Preserve a personalidade da pessoa, a intenção da fala, ironia, sarcasmo, shade, humor, camp, provocação, flerte, carinho, deboche e ritmo cômico.
Em reality shows, cultura pop e conversas informais, prefira uma linguagem oral, descontraída e natural em PT-BR quando o contexto pedir isso.
Não exagere nem invente gírias brasileiras sem necessidade.

CONTEXTO DE FALANTE E GÊNERO:

Algumas entradas podem conter o campo opcional "speaker". Ele é somente uma pista interna de quem está falando e NÃO faz parte da legenda.
Quando "speaker" identificar claramente uma pessoa, use essa informação para manter concordância natural de gênero em primeira pessoa, adjetivos e particípios.
Nunca copie, traduza nem acrescente o conteúdo de "speaker" ao texto final.
Se "speaker" não existir, estiver ambíguo ou não permitir saber com segurança o gênero de quem fala, NÃO ADIVINHE masculino ou feminino.
Nesse caso, reescreva naturalmente a frase em PT-BR para evitar marca de gênero quando possível, sem mudar o sentido.
Exemplo: se não souber quem diz "I'm so excited", não escolha aleatoriamente "estou empolgado" ou "estou empolgada"; prefira algo natural ao contexto como "Mal posso esperar", "Que empolgação!" ou outra formulação neutra adequada.
Nunca use grafias artificiais como "empolgado(a)", "empolgade", "ele/ela" ou barras para contornar o gênero.

POLIMENTO DE FALA E CANTO:

A legenda deve registrar O QUE foi dito ou cantado, não desenhar quanto tempo uma nota ou sílaba durou.
Se a fonte representar uma voz prolongada repetindo letras, vogais ou segmentos com hífens, normalize para a palavra correta.
Exemplos de fenômeno, não de tradução literal: "home-e-e-e-e" deve ser entendido como "home"; "ce-e-e-e-rto" deve ficar "certo"; "sooooo" representa apenas "so" prolongado; "nããããão" representa "não" prolongado.
Não reproduza sequências como "a-a-a-a-a", "e-e-e-e-e" ou vogais multiplicadas só porque a pessoa sustentou a nota.
Preserve vocalizações reais com função comunicativa, como "ah", "oh", "hmm", quando fizerem parte da fala, mas sem alongamento ortográfico excessivo.

REGRAS OBRIGATÓRIAS:

1. Retorne exatamente um objeto para cada entrada.
2. Preserve todos os IDs exatamente.
3. Não crie nem remova IDs.
4. Traduza somente o campo "text".
5. O campo opcional "speaker" é apenas contexto e NUNCA deve aparecer na saída.
6. Não escreva explicações, markdown ou texto fora do JSON.
7. Não resuma nem omita informação falada ou cantada presente no texto.
8. Antes de traduzir uma expressão curta, ambígua ou coloquial, interprete primeiro o SENTIDO e a INTENÇÃO usando os IDs vizinhos como contexto.
9. Considere que pontuação, vírgulas, quebras de linha e segmentação da legenda-fonte podem estar imperfeitas. Não deixe uma segmentação ruim transformar uma expressão idiomática em uma frase sem sentido.
10. Use os IDs anteriores e seguintes do mesmo lote somente como contexto. Nunca transfira, duplique ou mova conteúdo de um ID para outro.
11. Adapte expressões idiomáticas, gírias, humor, piadas, trocadilhos e shade quando houver uma solução brasileira natural que preserve o efeito da fala.
12. Preserve o humor e a intenção mesmo quando isso exigir se afastar da estrutura gramatical literal do inglês.
13. Reconheça VOCATIVOS. Em fala informal, palavras como "girl", "bitch", "honey", "sis", "queen", "baby" e "babe" frequentemente são vocativos e não devem ser tratadas automaticamente como substantivos literais.
14. "girl" NÃO deve virar automaticamente "garota". Conforme o contexto, pode ser "gata", "amiga", "mana", "querida", "bicha" ou simplesmente não precisar de tradução explícita como substantivo.
15. "bitch" NÃO deve virar automaticamente "vadia". Use "vadia" somente quando houver intenção genuína de insulto. Em contexto camp, afetivo, debochado ou entre queens, considere alternativas naturais como "bicha", "gata", "amiga" ou outra solução adequada ao tom.
16. Exemplo de sentido: "Kenya got you, girl" em tom de shade deve ser entendido como algo como "Mas a Kenya te pegou, gata", e NÃO como "Kenya pegou a sua garota".
17. Exemplo de registro: "Vita is quiet, but this bitch is a silent killer" em contexto camp pode ser localizado como "A Vita é calada, mas essa bicha é uma assassina silenciosa", em vez de traduzir "bitch" mecanicamente como "vadia".
18. Não copie os exemplos mecanicamente. Eles demonstram como interpretar intenção, vocativo e registro; escolha sempre a formulação que melhor encaixar na cena real.
19. Se uma adaptação de trocadilho, bordão ou termo cunhado ficar forçada, confusa ou sem graça em PT-BR, preserve o termo original em vez de inventar uma palavra nova.
20. Preserve bordões, nomes próprios, marcas, nomes de personagens, títulos e termos icônicos quando não houver equivalente natural e reconhecível em PT-BR. Exemplo: "Condragulations" deve permanecer "Condragulations".
21. Não censure palavrões, linguagem sexual, sarcasmo, deboche ou intensidade emocional.
22. Se houver fala ou letra de música em italiano, espanhol, francês ou qualquer outro idioma, traduza-a para PT-BR também, desde que seja conteúdo real da legenda e não apenas um nome próprio ou título.
23. Quando houver letra de música realmente transcrita, traduza o conteúdo da letra. Não invente letras para marcações de música, descrições de som ou símbolos musicais.
24. Preserve vocalizações, nomes de artistas, nomes de músicas e nomes próprios quando funcionarem como nomes, não como frases a serem traduzidas.
25. Normalize alongamentos ortográficos causados por nota sustentada ou fala arrastada; não devolva palavras com letras ou sílabas repetidas apenas para imitar duração sonora.
26. Preserve tags HTML/ASS como <i>, </i>, <b>, </b>, {\\i1}, {\\i0}.
27. Não acrescente informações, notas explicativas, nomes de falantes, descrições de sons, rubricas SDH/CC ou observações.
28. Não misture textos entre IDs.
29. Cada ID deve receber somente a tradução correspondente ao próprio texto.
30. Não acrescente hífen, travessão, meia-risca ou barra no início de uma fala apenas por estilo. Use marcadores de diálogo somente quando forem realmente necessários para diferenciar dois ou mais falantes dentro do mesmo bloco.
31. Em diálogos informais, contrações brasileiras como "tá", "tô" e "pra" podem ser usadas quando soarem naturais ao personagem e à situação; não force essas formas em registros formais.
32. Concordância de gênero deve seguir informação confiável do falante ou do próprio contexto. Quando não houver informação confiável, prefira formulação naturalmente neutra a chutar gênero.
33. Priorize sempre esta ordem: SENTIDO DA FALA → TOM/INTENÇÃO → NATURALIDADE EM PT-BR → CONCORDÂNCIA CONTEXTUAL → fidelidade à estrutura literal.

RETORNE SOMENTE UM ARRAY JSON NO FORMATO:

[{"id":123,"text":"tradução"}]

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

    const prompt =
        buildTranslationPrompt(
            blocks
        );

    const raw =
        await enqueueGemini(
            prompt,
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

    /*
     * Só aceitamos IDs que realmente pertencem
     * ao lote enviado.
     *
     * IDs inventados pelo modelo são ignorados.
     */
    const requestedIds =
        new Set(
            blocks.map(
                block =>
                    block.index
            )
        );

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
            requestedIds.has(
                item.id
            ) &&
            typeof item.text ===
                "string" &&
            item.text.trim().length >
                0
        ) {
            translatedById.set(
                item.id,
                item.text
            );
        }
    }

    /*
     * Se nenhum ID solicitado voltou corretamente,
     * tratamos como saída estrutural inválida.
     *
     * Nesse caso o fallback de divisão entra em ação.
     */
    if (
        translatedById.size ===
        0
    ) {
        throw badModelOutputError(
            "Gemini não devolveu nenhum bloco válido solicitado."
        );
    }

    /*
     * IMPORTANTE - 5.4:
     *
     * Não jogamos fora uma resposta parcialmente boa.
     * Se o Gemini devolveu 247 de 250 IDs, guardamos
     * os 247 e pedimos novamente APENAS os 3 ausentes.
     */
    const missingBlocks =
        blocks.filter(
            block =>
                !translatedById.has(
                    block.index
                )
        );

    return {
        translatedById,
        missingBlocks
    };
}

async function translateBatch(
    blocks,
    deadlineAt,
    splitDepth = 0,
    singleBlockRetry = 0
) {
    assertBeforeDeadline(
        deadlineAt
    );

    let result;

    try {
        result =
            await translateBatchOnce(
                blocks,
                deadlineAt
            );
    } catch (
        error
    ) {
        if (
            error?.code !==
            "BAD_MODEL_OUTPUT"
        ) {
            throw error;
        }

        assertBeforeDeadline(
            deadlineAt
        );

        /*
         * Se sobrou apenas um bloco e a estrutura
         * ainda veio inválida, fazemos uma tentativa
         * estrutural extra antes de desistir.
         */
        if (
            blocks.length ===
                1 &&
            singleBlockRetry <
                MAX_SINGLE_BLOCK_OUTPUT_RETRIES
        ) {
            console.warn(
                `[TRANSLATE] Saída inválida para 1 bloco. Tentativa estrutural extra ${singleBlockRetry + 1}/${MAX_SINGLE_BLOCK_OUTPUT_RETRIES}.`
            );

            return translateBatch(
                blocks,
                deadlineAt,
                splitDepth,
                singleBlockRetry + 1
            );
        }

        /*
         * Para JSON totalmente inválido ou resposta
         * sem nenhum ID útil, dividimos o lote.
         *
         * Diferente da 5.3, não existe mais o corte
         * fixo de 80 blocos que fez um lote de 63
         * falhar imediatamente.
         */
        const canSplit =
            blocks.length >
                1 &&
            splitDepth <
                MAX_BAD_OUTPUT_SPLIT_DEPTH;

        if (!canSplit) {
            throw error;
        }

        const middle =
            Math.ceil(
                blocks.length / 2
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
            `[TRANSLATE] Saída estrutural inválida em lote de ${blocks.length} blocos. Dividindo automaticamente em ${left.length} + ${right.length}.`
        );

        const translatedLeft =
            await translateBatch(
                left,
                deadlineAt,
                splitDepth + 1,
                0
            );

        const translatedRight =
            await translateBatch(
                right,
                deadlineAt,
                splitDepth + 1,
                0
            );

        return [
            ...translatedLeft,
            ...translatedRight
        ];
    }

    /*
     * Resposta completa: caminho normal.
     */
    if (
        result.missingBlocks.length ===
        0
    ) {
        return blocks.map(
            block =>
                result.translatedById.get(
                    block.index
                )
        );
    }

    /*
     * Resposta parcial: recuperamos SOMENTE
     * os IDs que faltaram.
     */
    const receivedCount =
        blocks.length -
        result.missingBlocks.length;

    console.warn(
        `[TRANSLATE] Resposta parcial: ${receivedCount}/${blocks.length} IDs recebidos. Recuperando somente ${result.missingBlocks.length} ID(s) ausente(s).`
    );

    assertBeforeDeadline(
        deadlineAt
    );

    const recoveredTexts =
        await translateBatch(
            result.missingBlocks,
            deadlineAt,
            splitDepth,
            0
        );

    const recoveredById =
        new Map();

    for (
        let i = 0;
        i <
            result.missingBlocks.length;
        i++
    ) {
        recoveredById.set(
            result.missingBlocks[i]
                .index,
            recoveredTexts[i]
        );
    }

    const merged =
        blocks.map(
            block => {
                if (
                    result.translatedById.has(
                        block.index
                    )
                ) {
                    return result.translatedById.get(
                        block.index
                    );
                }

                return recoveredById.get(
                    block.index
                );
            }
        );

    if (
        merged.some(
            text =>
                typeof text !==
                    "string"
        )
    ) {
        throw badModelOutputError(
            "A recuperação parcial terminou com blocos ausentes."
        );
    }

    console.log(
        `[TRANSLATE] Recuperação parcial concluída: ${result.missingBlocks.length} ID(s) recuperado(s).`
    );

    return merged;
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

    console.log(
        `[TRANSLATE] Limite: ${MAX_BATCH_BLOCKS} blocos / ${MAX_BATCH_CHARS} caracteres.`
    );

    const translatedTexts =
        new Array(
            blocks.length
        );

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
     * O teto total nasce junto com o job.
     *
     * Portanto o tempo aguardando na fila de jobs
     * também conta para o limite absoluto de 8 minutos.
     */
    const deadlineAt =
        Number.isFinite(
            job?.deadlineAt
        )
            ? job.deadlineAt
            : startedAt +
              MAX_TRANSLATION_TIME_MS;

    job.startedAt =
        startedAt;

    if (
        !Number.isFinite(
            job.deadlineAt
        )
    ) {
        job.deadlineAt =
            deadlineAt;
    }

    assertBeforeDeadline(
        deadlineAt
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
                batch,
                deadlineAt
            );

        assertBeforeDeadline(
            deadlineAt
        );

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

    const elapsedMs =
        Date.now() -
        startedAt;

    console.log(
        `[TRANSLATE] Finalizada em ${(elapsedMs / 1000).toFixed(1)}s.`
    );

    const withoutVocalElongations =
        cleanAllTranslatedVocalElongations(
            translatedTexts
        );

    const cleanedTranslatedTexts =
        cleanAllTranslatedDialogueMarkers(
            withoutVocalElongations
        );

    return buildSrt(
        blocks,
        cleanedTranslatedTexts
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

        /*
         * O teto absoluto começa no momento
         * em que o job é criado.
         *
         * Tempo de espera na fila também conta.
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

        assertBeforeDeadline(
            job.deadlineAt
        );

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
| FILA DE JOBS COMPLETOS
|--------------------------------------------------------------------------
|
| 5.3+:
|
| Um job inteiro termina antes do próximo começar.
| Assim OpenSubtitles e Embedded não alternam lotes.
|
*/

function enqueueTranslationJob(
    job
) {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            if (
                job.status !==
                    "processing"
            ) {
                resolve();
                return;
            }

            translationJobQueue.push(
                {
                    job,
                    resolve,
                    reject
                }
            );

            console.log(
                `[JOB QUEUE] ${job.id} entrou na fila. Aguardando: ${translationJobQueue.length}.`
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
            translationJobQueue.length >
            0
        ) {
            const item =
                translationJobQueue.shift();

            if (!item) {
                continue;
            }

            const {
                job,
                resolve,
                reject
            } =
                item;

            try {
                if (
                    job.status !==
                        "processing"
                ) {
                    resolve();
                    continue;
                }

                /*
                 * O tempo aguardando nesta fila conta
                 * no teto absoluto do job.
                 */
                if (
                    Number.isFinite(
                        job.deadlineAt
                    ) &&
                    Date.now() >=
                        job.deadlineAt
                ) {
                    job.status =
                        "failed";

                    job.error =
                        "Tempo máximo de tradução atingido.";

                    job.updatedAt =
                        Date.now();

                    console.error(
                        `[JOB ${job.id}] Falhou antes de iniciar: ${job.error}`
                    );

                    resolve();
                    continue;
                }

                console.log(
                    `[JOB QUEUE] Iniciando job completo ${job.id}. Restantes na fila: ${translationJobQueue.length}.`
                );

                await processJob(
                    job
                );

                resolve();
            } catch (
                error
            ) {
                reject(
                    error
                );
            }
        }
    } finally {
        translationJobWorkerRunning =
            false;

        if (
            translationJobQueue.length >
            0
        ) {
            processTranslationJobQueue();
        }
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
        "5.6.0",

    name:
        "Tradutor Gemini PT-BR",

    description:
        "Traduz automaticamente legendas para Português do Brasil usando Gemini.",

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

            translationJobQueue:
                translationJobQueue.length,

            activeTranslationJob:
                translationJobWorkerRunning,

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

            translationJobQueue:
                translationJobQueue.length,

            translationJobWorkerRunning,

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

            maxOutputTokens:
                MAX_OUTPUT_TOKENS,

            translationTimeoutMs:
                MAX_TRANSLATION_TIME_MS,

            maxTranslationTimeMs:
                MAX_TRANSLATION_TIME_MS
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
                        "Stremio-Gemini-Subtitle-Translator/5.6"
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
                        "Stremio-Gemini-Subtitle-Translator/5.6"
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

    const rawText =
        normalizeSrt(
            await response.text()
        );

    if (!rawText) {
        throw new Error(
            "Legenda vazia."
        );
    }

    if (
        rawText.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda muito grande: ${rawText.length} caracteres.`
        );
    }

    const text =
        cleanSrtForTranslation(
            rawText
        );

    if (!text) {
        throw new Error(
            "A legenda ficou vazia após a limpeza SDH/CC."
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
    const rawNormalizedSrt =
        normalizeSrt(
            sourceSrt
        );

    if (!rawNormalizedSrt) {
        throw new Error(
            "A legenda embutida está vazia."
        );
    }

    if (
        rawNormalizedSrt.length >
        MAX_SOURCE_CHARS
    ) {
        throw new Error(
            `Legenda embutida muito grande: ${rawNormalizedSrt.length} caracteres.`
        );
    }

    const normalizedSrt =
        cleanSrtForTranslation(
            rawNormalizedSrt
        );

    if (!normalizedSrt) {
        throw new Error(
            "A legenda embutida ficou vazia após a limpeza SDH/CC."
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
     * usamos a fila de jobs completos.
     *
     * O job Embedded só começa quando o job
     * anterior terminar. Dentro dele continuamos
     * usando a mesma fila Gemini, intervalo,
     * cooldown, cache e limites.
     */
    job.promise =
        enqueueTranslationJob(
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
                enqueueTranslationJob(
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
            " STREMIO GEMINI SUBTITLE TRANSLATOR 5.6"
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
            `Saída máxima Gemini: ${MAX_OUTPUT_TOKENS} tokens`
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
            `Cache TTL: ${CACHE_TTL_MS / 3600000}h`
        );

        console.log(
            "Limpeza SDH/CC: ATIVA"
        );

        console.log(
            "Adaptação audiovisual 5.6: ATIVA"
        );

        console.log(
            "Localização coloquial/contextual: ATIVA"
        );

        console.log(
            "Contexto oculto de falante: ATIVO"
        );

        console.log(
            "Proteção contra chute de gênero: ATIVA"
        );

        console.log(
            "Normalização de alongamentos vocais: ATIVA"
        );

        console.log(
            "Limpeza de traços soltos: ATIVA"
        );

        console.log(
            "Recuperação parcial de IDs: ATIVA"
        );

        console.log(
            "Fila de jobs completos: SEQUENCIAL (1 por vez)"
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
