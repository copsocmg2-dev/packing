export default async function handler(req, res) {
  const GITHUB_TOKEN = process.env.GH_TOKEN; 
  const OWNER = "copsocmg2-dev"; 
  const REPO = "packing"; 

  // Função auxiliar para disparar um evento específico no GitHub
  const dispatchGitHubEvent = async (eventType) => {
    return fetch(`https://api.github.com/repos/${OWNER}/${REPO}/dispatches`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GITHUB_TOKEN}`,
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ event_type: eventType }), 
    });
  };

  try {
    // Dispara os dois workflows simultaneamente usando Promise.all
    const [mainResponse, outboundResponse] = await Promise.all([
      dispatchGitHubEvent("trigger_robo_logistica"),
      dispatchGitHubEvent("trigger_robo_outbound")
    ]);

    if (mainResponse.ok && outboundResponse.ok) {
      res.status(200).send("Robôs Logística e Outbound acionados com sucesso no GitHub!");
    } else {
      // Se algum deles falhar, tentamos capturar o erro
      const errorTextMain = await mainResponse.text().catch(() => "Sem detalhes");
      const errorTextOutbound = await outboundResponse.text().catch(() => "Sem detalhes");
      
      res.status(500).send(`Erro ao acionar robôs. Main: ${errorTextMain} | Outbound: ${errorTextOutbound}`);
    }
  } catch (error) {
    res.status(500).send(`Erro interno: ${error.message}`);
  }
}
