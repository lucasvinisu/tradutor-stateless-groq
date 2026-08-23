const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());

// Memória RAM temporária para guardar traduções e evitar chamadas duplicadas
const translationCache = new Map();
const processingJobs = new Set();

const manifest = {
    id: "org.tradutor.stateless.gemini.async",
    version: "2.0.0",
    name: "Tradutor Gemini Async",
    description: "Traduz legendas em segundo plano usando Google Gemini.",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: []
};

app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// Função que traduz no background
async function backgroundTranslate(cacheKey, srtUrl) {
    if (processingJobs.has(cacheKey)) return;
    processingJobs.add(cacheKey);

    try {
        console.log(`[BACKGROUND] Baixando legenda original para ${cacheKey}...`);
        const srtResp = await fetch(srtUrl);
        const srtText = await srtResp.text();

        const apiKey = process.env.GEMINI_API_KEY;
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=${apiKey}`;

        const prompt = `Você é um tradutor profissional de legendas de filmes e séries para o Português do Brasil. Mantenha exatamente a mesma estrutura, quebras de linha e números dos blocos de legenda SRT. Não adicione nenhum comentário, markdown extra ou introdução, devolva apenas o texto traduzido no formato SRT puro:\n\n${srtText}`;

        console.log(`[BACKGROUND] Enviando para o Google Gemini...`);
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
        });

        const data = await response.json();
        if (data.error) {
            console.error("[ERRO GEMINI BACKGROUND]", data.error.message);
            processingJobs.delete(cacheKey);
            return;
        }

        const translatedText = data.candidates?.[0]?.content?.parts?.[0]?.text;
        if (translatedText) {
            const base64Sub = Buffer.from(translatedText, 'utf-8').toString('base64');
            translationCache.set(cacheKey, `data:text/plain;base64,${base64Sub}`);
            console.log(`[SUCESSO] Tradução concluída e salva em cache para ${cacheKey}!`);
        }
    } catch (err) {
        console.error("[ERRO CRÍTICO BACKGROUND]", err);
    } finally {
        processingJobs.delete(cacheKey);
    }
}

app.get('/subtitles/:type/:id/:extra?.json', async (req, res) => {
    const { type, id } = req.params;
    const cacheKey = `${type}-${id}`;
    console.log(`[STREMIO] Pedido recebido para ${type} ID: ${id}`);

    // 1. Se já estiver traduzido e salvo na memória, entrega instantaneamente!
    if (translationCache.has(cacheKey)) {
        console.log(`[CACHE] Entregando legenda traduzida instantaneamente para ${cacheKey}`);
        return res.json({
            subtitles: [{ id: `${id}-gemini-pt`, url: translationCache.get(cacheKey), lang: 'por' }]
        });
    }

    try {
        const subSearchUrl = `https://opensubtitles-v3.strem.io/subtitles/${type}/${id}.json`;
        const searchResp = await fetch(subSearchUrl);
        const searchData = await searchResp.json();

        if (!searchData.subtitles || searchData.subtitles.length === 0) {
            return res.json({ subtitles: [] });
        }

        const targetSub = searchData.subtitles.find(s => s.lang === 'eng' || s.lang === 'en') || searchData.subtitles[0];
        if (!targetSub || !targetSub.url) {
            return res.json({ subtitles: [] });
        }

        // 2. Se não estiver traduzido, disparar a tradução em background e devolver um aviso imediato ao Stremio
        if (!processingJobs.has(cacheKey)) {
            backgroundTranslate(cacheKey, targetSub.url);
        }

        // Cria uma legenda temporária de aviso para respeitar o tempo limite do Stremio
        const warningSrt = `1\n00:00:01,000 --> 00:00:08,000\n⏳ Traduzindo com Gemini...\n\n2\n00:00:08,500 --> 00:00:15,000\nAguarde 1 minuto e recarregue a legenda.`;
        const warningBase64 = Buffer.from(warningSrt, 'utf-8').toString('base64');

        res.json({
            subtitles: [{
                id: `${id}-processing`,
                url: `data:text/plain;base64,${warningBase64}`,
                lang: 'por'
            }]
        });

    } catch (err) {
        console.error("[ERRO GERAL]", err);
        res.json({ subtitles: [] });
    }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
    console.log(`[SISTEMA] Servidor Async Gemini rodando na porta ${PORT}`);
});
