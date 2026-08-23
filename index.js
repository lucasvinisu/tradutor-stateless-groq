const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());

const manifest = {
    id: "org.tradutor.stateless.gemini",
    version: "1.0.0",
    name: "Tradutor Gemini Stateless",
    description: "Traduz legendas em tempo real usando Google Gemini direto na memória.",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: []
};

app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// Função para traduzir o SRT inteiro de uma vez usando a API do Gemini
async function translateWithGemini(srtText) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        console.error("[ERRO GEMINI] Chave GEMINI_API_KEY não configurada no Render!");
        return srtText;
    }

    // Usando o modelo gemini-2.0-flash (compatível com a sua conta)
    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${apiKey}`;

    const prompt = `Você é um tradutor profissional de legendas de filmes e séries para o Português do Brasil. Mantenha exatamente a mesma estrutura, quebras de linha e números dos blocos de legenda SRT. Não adicione nenhum comentário, markdown extra ou introdução, devolva apenas o texto traduzido no formato SRT puro:\n\n${srtText}`;

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                contents: [{
                    parts: [{ text: prompt }]
                }]
            })
        });

        const data = await response.json();
        
        if (data.error) {
            console.error("[ERRO API GEMINI]", data.error.message);
            return srtText;
        }

        const translatedText = data.candidates?.[0]?.content?.parts?.[0]?.text || srtText;
        return translatedText;
    } catch (error) {
        console.error("[ERRO REDE GEMINI]", error.message);
        return srtText;
    }
}

app.get('/subtitles/:type/:id/:extra?.json', async (req, res) => {
    const { type, id } = req.params;
    console.log(`[STREMIO] Pedido recebido para ${type} ID: ${id}`);

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

        const srtResp = await fetch(targetSub.url);
        const srtText = await srtResp.text();

        console.log("[TRADUÇÃO] Enviando legenda inteira para o Google Gemini...");
        const translatedText = await translateWithGemini(srtText);

        const base64Sub = Buffer.from(translatedText, 'utf-8').toString('base64');
        const dataUrl = `data:text/plain;base64,${base64Sub}`;

        res.json({
            subtitles: [
                {
                    id: `${id}-gemini-pt`,
                    url: dataUrl,
                    lang: 'por'
                }
            ]
        });

    } catch (err) {
        console.error("[ERRO GERAL]", err);
        res.json({ subtitles: [] });
    }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
    console.log(`[SISTEMA] Servidor Stateless Gemini rodando na porta ${PORT}`);
});
