export default async function handler(req, res) {
  // O token que configurou nas variáveis de ambiente da Vercel
  const GITHUB_TOKEN = process.env.GH_TOKEN; 
  
  // ATENÇÃO: Substitua pelos seus dados corretos
  const OWNER = "copsocmg2-dev"; // Pelo caminho da pasta, parece ser este o seu utilizador/organização
  const REPO = "packing"; // O nome do seu repositório no GitHub

  try {
    const response = await fetch(`https://api.github.com/repos/${OWNER}/${REPO}/dispatches`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GITHUB_TOKEN}`,
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
      },
      // Este nome tem de ser exatamente igual ao que está no main.yml
      body: JSON.stringify({ event_type: "trigger_robo_logistica" }), 
    });

    if (response.ok) {
      res.status(200).send("Robô acionado com sucesso no GitHub!");
    } else {
      const errorText = await response.text();
      res.status(500).send(`Erro ao acionar robô: ${errorText}`);
    }
  } catch (error) {
    res.status(500).send(`Erro interno: ${error.message}`);
  }
}
