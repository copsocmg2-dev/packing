export default async function handler(req, res) {
  const GITHUB_TOKEN = process.env.GH_TOKEN; 
  const OWNER = "copsocmg2-dev"; 
  const REPO = "packing"; 

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
    // Dispara apenas o evento específico para o script de SOCS
    const response = await dispatchGitHubEvent("trigger_lh_all_socs");

    if (response.ok) {
      res.status(200).send("Robô LH ALL SOCS acionado com sucesso no GitHub!");
    } else {
      const errorText = await response.text().catch(() => "Sem detalhes");
      res.status(500).send(`Erro ao acionar robô LH ALL SOCS: ${errorText}`);
    }
  } catch (error) {
    res.status(500).send(`Erro interno: ${error.message}`);
  }
}
