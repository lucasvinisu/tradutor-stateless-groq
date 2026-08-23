const express = require('express');
const cors = require('cors');
const Groq = require('groq-sdk');

const app = express();
app.use(cors());

// Inicializa o cliente Groq usando a variável de ambiente
const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

// Manifesto oficial do Addon para o Stremio
const manifest = {
    id: "org.tradutor.stateless.groq",
    version: "1.0.0",
    name: "Tradutor Groq Stateless",
    description: "Traduz legendas em tempo real usando Groq IA direto na memória (Sem Banco de Dados).",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: []
};

app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// Função para traduzir a legenda usando o modelo rápido e inteligente do Groq
async function translateWithGroq(textBlock) {
    try {
        const completion = await groq.chat.completions.create({
            messages: [
                {
                    role: "system",
                    content: "Você é um tradutor profissional de legendas de filmes e séries para o Português do Brasil. Mantenha exatamente a mesma estrutura, quebras de linha e números dos blocos de legenda SRT. Não adicione nenhum comentário ou introdução, apenas traduza o texto."
                },
                {
                    role: "user",
                    content: textBlock
                }
            ],
            model: "llama-3.1-70b-versatile",
            temperature: 0.3,
        });
        return completion.choices[0]?.message?.content || textBlock;
    } catch (error) {
        console.error("[ERRO GROQ]", error.message);
        return textBlock;
    }
}

// Rota principal que intercepta o pedido de legendas do Stremio
app.get('/subtitles/:type/:id/:extra?.json', async (req, res) => {
    const { type, id } = req.params;
    console.log(`[STREMIO] Pedido recebido para ${type} ID: ${id}`);

    try {
        // 1. Busca legendas disponíveis nas fontes públicas do Stremio
        const subSearchUrl = `https://opensubtitles-v3.strem.io/subtitles/${type}/${id}.json`;
        const searchResp = await fetch(subSearchUrl);
        const searchData = await searchResp.json();

        if (!searchData.subtitles || searchData.subtitles.length === 0) {
            return res.json({ subtitles: [] });
        }

        // Pega a primeira legenda disponível (priorizando inglês)
        const targetSub = searchData.subtitles.find(s => s.lang === 'eng' || s.lang === 'en') || searchData.subtitles[0];
        
        if (!targetSub || !targetSub.url) {
            return res.json({ subtitles: [] });
        }

        // 2. Baixa o arquivo .srt bruto
        const srtResp = await fetch(targetSub.url);
        const srtText = await srtResp.text();

        console.log("[TRADUÇÃO] Enviando texto para o Groq...");
        
        // 3. Traduz utilizando a IA na memória do servidor
        const translatedText = await translateWithGroq(srtText);

        // 4. Converte a legenda traduzida em um formato seguro para o Stremio ler diretamente
        const base64Sub = Buffer.from(translatedText, 'utf-8').toString('base64');
        const dataUrl = `data:text/plain;base64,${base64Sub}`;

        res.json({
            subtitles: [
                {
                    id: `${id}-groq-pt`,
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`[SISTEMA] Servidor Stateless rodando na porta ${PORT}`);
});
