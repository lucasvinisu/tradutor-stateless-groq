const express = require('express');
const cors = require('cors');
const Groq = require('groq-sdk');

const app = express();
app.use(cors());

const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

const manifest = {
    id: "org.tradutor.stateless.groq",
    version: "1.0.0",
    name: "Tradutor Groq Stateless",
    description: "Traduz legendas em tempo real usando Groq IA fatiando os blocos na memória.",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"],
    catalogs: []
};

app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// Função para fatiar o SRT em blocos menores (evita o Erro 413 de limite de tokens)
function chunkSrt(srtText, maxBlockSize = 20) {
    const blocks = srtText.replace(/\r\n/g, '\n').split(/\n\s*\n/).filter(b => b.trim() !== '');
    const chunks = [];
    for (let i = 0; i < blocks.length; i += maxBlockSize) {
        chunks.push(blocks.slice(i, i + maxBlockSize).join('\n\n'));
    }
    return chunks;
}

// Função de tradução em lotes sequenciais com pausa de segurança
async function translateWithGroq(srtText) {
    const chunks = chunkSrt(srtText, 20); // Pedaços de 20 blocos por vez
    let translatedChunks = [];

    console.log(`[TRADUÇÃO] Dividido em ${chunks.length} lotes para envio seguro ao Groq.`);

    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        try {
            const completion = await groq.chat.completions.create({
                messages: [
                    {
                        role: "system",
                        content: "Você é um tradutor profissional de legendas de filmes e séries para o Português do Brasil. Mantenha exatamente a mesma estrutura, quebras de linha e números dos blocos de legenda SRT. Não adicione nenhum comentário ou introdução, apenas traduza o texto."
                    },
                    {
                        role: "user",
                        content: chunk
                    }
                ],
                model: "openai/gpt-oss-120b",
                temperature: 0.3,
            });
            const translated = completion.choices[0]?.message?.content || chunk;
            translatedChunks.push(translated);
            
            // Pausa rápida de 300ms entre os lotes para respeitar a taxa de requisições por minuto (RPM)
            await new Promise(resolve => setTimeout(resolve, 300));
        } catch (error) {
            console.error(`[ERRO NO LOTE ${i + 1}]`, error.message);
            translatedChunks.push(chunk); // Mantém o original se der erro em um lote específico
        }
    }

    return translatedChunks.join('\n\n');
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

        console.log("[TRADUÇÃO] Iniciando fatiamento e tradução via Groq...");
        const translatedText = await translateWithGroq(srtText);

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

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
    console.log(`[SISTEMA] Servidor Stateless rodando na porta ${PORT}`);
});
