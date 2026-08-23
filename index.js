const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());

const translationCache = new Map();
const processingJobs = new Set();

const manifest = {
    id: "org.tradutor.stateless.gemini.async",
    version: "2.1.0",
    name: "Tradutor Gemini Async Otimizado",
    description: "Traduz legendas em blocos seguros usando Google Gemini.",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: []
};

app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// Função para fatiar o SRT em blocos seguros para o Gemini processar rápido
function chunkSrt(srtText, maxBlockSize = 40) {
    const blocks = srtText.replace(/\r\n/g, '\n').split(/\n\s*\n/).filter(b => b.trim() !== '');
    const chunks = [];
    for (let i = 0; i < blocks.length; i += maxBlockSize) {
        chunks.push(blocks.slice(i, i + maxBlockSize).join('\n\n'));
    }
    return chunks;
}

async function backgroundTranslate(cacheKey, srtUrl) {
    if (processingJobs.has(cacheKey)) return;
    processingJobs.add(cacheKey);

    try {
        console.log(`[BACKGROUND] Baixando legenda original para ${cacheKey}...`);
        const srtResp = await fetch(srtUrl);
        const srtText = await srtResp.text();

        const chunks = chunkSrt(srtText, 40); // Blocos de 40 legendas por requisição
        let translatedChunks = [];
        const apiKey = process.env.GEMINI_API_KEY;
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=${apiKey}`;

        console.log(`[BACKGROUND] Traduzindo ${chunks.chunks?.length || chunks.length} blocos via Gemini...`);

        for (let i = 0; i < chunks.length; i++) {
            const prompt = `Traduza os blocos de legenda SRT abaixo para o Português do Brasil. Mantenha exatamente a mesma estrutura, quebras de linha e números dos blocos. Não adicione comentários:\n\n${chunks[i]}`;

            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
            });

            const data = await response.json();
            if (data.error) {
                console.error(`[ERRO GEMINI LOTE ${i+1}]`, data.error.message);
                translatedChunks.push(chunks[i]); // Mantém o original se falhar
            } else {
                const text = data.candidates?.[0]?.content?.parts?.[0]?.text || chunks[i];
                translatedChunks.push(text);
            }

            // Pequena pausa para estabilidade
            await new Promise(resolve => setTimeout(resolve, 200));
        }

        const finalTranslatedText = translatedChunks.join('\n\n');
        const base64Sub = Buffer.from(finalTranslatedText, 'utf-8').toString('base64');
        translationCache.set(cacheKey, `data:text/plain;base64,${base64Sub}`);
        console.log(`[SUCESSO] Tradução completa e salva em cache para ${cacheKey}!`);

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

        if (!processingJobs.has(cacheKey)) {
            backgroundTranslate(cacheKey, targetSub.url);
        }

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
    console.log(`[SISTEMA] Servidor Async Gemini Otimizado rodando na porta ${PORT}`);
});
