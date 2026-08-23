const express = require('express');
const cors = require('cors');

const app = express();

// Permite que o aplicativo do Stremio acesse este servidor de qualquer IP ou dispositivo
app.use(cors()); 

// 1. O Manifesto do Addon
const manifest = {
    id: "org.tradutor.stateless.groq",
    version: "1.0.0",
    name: "Tradutor Groq (Stateless)",
    description: "Tradutor de legendas em tempo real usando Groq IA direto na memória.",
    resources: ["subtitles"],
    types: ["movie", "series"],
    idPrefixes: ["tt"], // Essencial: avisa que responderemos a IDs do IMDB
    catalogs: []
};

// Rota obrigatória de configuração
app.get('/manifest.json', (req, res) => {
    res.json(manifest);
});

// 2. O Interceptador de Pedidos de Legenda (Estrutura Preparatória)
app.get('/subtitles/:type/:id/:extra?.json', async (req, res) => {
    const { type, id } = req.params;
    console.log(`[LOG] Pedido recebido: Tipo = ${type}, ID = ${id}`);
    
    // Ponto de injeção da lógica futura de download e tradução via Groq.
    // Retornamos um array vazio provisoriamente para evitar erros de "Timeout" no Stremio.
    res.json({ subtitles: [] });
});

// 3. Inicialização do Servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`[SISTEMA] Servidor operacional na porta ${PORT}`);
});
