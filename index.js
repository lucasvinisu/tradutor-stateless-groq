const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());

const translationCache = new Map();
const processingJobs = new Set();

const manifest = {
    id: "org.tradutor.stateless.gemini.optimized",
    version: "3.1.0",
    name: "Tradutor Gemini 2-Parts",
    description: "Traduz legendas divididas em duas metades para máxima velocidade e estabilidade.",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: []
};

app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// Função para dividir o SRT exatamente em 2 metades equilibradas
function splitSrtInHalves(srtText) {
    const blocks = srtText.replace(/\r\n/g, '\n').split(/\n\s*\n/).filter(b => b.trim() !== '');
    const mid = Math.ceil(blocks.length / 2);
    const half1 = blocks.slice(0, mid).join('\n\n');
    const half2 = blocks.slice(mid).join('\n\n');
    return [half1, half2];
}

async function backgroundTranslate(cacheKey, srtUrl) {
    if (processingJobs.has(cacheKey)) return;
    processingJobs.add(cacheKey);

    try {
        console.log(`[BACKGROUND] Baixando legenda original para ${cacheKey}...`);
        const srtResp = await fetch(srtUrl);
        const srtText = await srtResp.text();

        const halves = splitSrtInHalves(srtText);
        let translatedHalves = [];
        const apiKey = process.env.GEMINI_API_KEY;
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=${apiKey}`;

        console.log(`[BACKGROUND] Traduzindo 2 metades via Gemini...`);

        for (let i = 0; i < halves.length; i++) {
            const prompt = `Você é um tradutor profissional de legendas para o Português do Brasil. Traduza o texto SRT abaixo mantendo exatamente a mesma estrutura, quebras de linha e números dos blocos. Não adicione comentários:\n\n${halves[i]}`;

            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
            });

            const data = await response.json();
            if (data.error) {
                console.error(`[ERRO GEMINI METADE ${i+1}]`, data.error.message);
                translatedHalves.push(halves[i]); // Mantém original se falhar
            } else {
                const text = data.candidates?.[0]?.content?.parts?.[0]?.text || halves[i];
                translatedHalves.push(text);
            }

            // Pausa rápida de segurança entre as duas metades
            await new Promise(resolve => setTimeout(resolve, 300));
        }

        const finalTranslatedText = translatedHalves.join('\n\n');
        const base64Sub = Buffer.from(finalTranslatedText, 'utf-8').toString('base64');
        translationCache.set(cacheKey, `data:text/plain;base64,${base64Sub}`);
        console.log(`[SUCESSO] Tradução das 2 metades concluída e salva em cache para ${cacheKey}!`);

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

        const warningSrt = `1\n00:00:01,000 --> 00:00:08,000\n⏳ Traduzindo com Gemini (2 Partes)...\n\n2\n00:00:08,500 --> 00:00:15,000\nAguarde 10 segundos e recarregue a legenda.`;
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
    console.log(`[SISTEMA] Servidor Gemini 2-Parts rodando na porta ${PORT}`);
});
