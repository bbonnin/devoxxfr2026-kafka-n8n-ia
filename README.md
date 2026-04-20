# devoxxfr2026-kafka-n8n-ia

> Code source de la démo pour "Automatisation intelligente en temps réel : quand Kafka rencontre n8n et l’IA"


## Composants

Eléments utilisés dans la démo (certains sont lancé via le docker compose) :
* Evénements :
  * **Apache Kafka** : les producteurs publient des messages en JSON sur un topic unique `events.all`.  
  * **Kafdrop** : UI web pour Kafka (topics, messages, ...). Pour montrer les événements au début de la démo.
* Orchestrateur :
  * **n8n** : orchestrateur low-code. Le workflow est déclenché par le **Kafka Trigger** à chaque nouveau message Kafka. Les fichiers de configuration des worflows se trouvent dans le répertoire `n8n`.
* Agent IA (composants utilisé par le noeud `Agent IA` dans n8n)
  * Modèle LLM : les exemples utilisent les modèles d'`Anthropic` (Sonnet, Haiku).
  * **Redis**: pour la mémoire court terme de l'agent IA.
  * **MCP server** : expose des **tools** (lookup CMDB, déploiements, incidents similaires, exécution runbook). n8n connecte ces tools à l’agent via la partie `Tools` du noeud Agent IA. Ce erveur MCP utilise les services OPS fournis via une API. 
* Outils complémentaires :
  * **Ticket Management** : Kanban "Jira-like" (TODO / IN_PROGRESS / DONE). n8n crée des tickets + ajoute des commentaires (décision, rationale, résultat de remédiation).
  * **Mattermost** : chat "Slack-like" local. n8n poste dans `#incidents` via le noeud Mattermost, authentifié par **API access token** (PAT).


> Dans les exemples de workflow, pour l'agent IA, il y a aussi l'utilisation d'Anthropic/Claude Sonnet, meilleur LLM à date pour cette démo.


### Lancement des composants de la démo

Utilisation du `docker-compose.yml`: 
```bash
docker compose up 
# ou
docker compose up --build
# ou
docker compose up <nom du composant à rebuilder> --build
```

## Workflow n8n

* Chargement:
	* Voir le contenu du répertoire `n8n`
	+ Pour les charger, ouvrez http://localhost:5632, puis allez dans "Import Workflow"
* Lancement: cliquez sur le bouton `Execute workflow` (pour des tests) ou `Publish`


## Démos

### Génération d'événements

```bash
cd eventgen

# Installation
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Lancement
python eventgen.py noise 
python eventgen.py vip
python eventgen.py lag

# Replay à faire !!!!!
python eventgen.py replay --template incident-vip --event-id 11111111-1111-1111-1111-111111111111

```

## Scénarios de démo

| Scénario | Type | Décision de l'agent | Résultat |
|----------|------|---------------------|----------|
| Catalog timeout | bruit | IGNORE | Pas de ticket (faux positif évité) |
| Catalog maintenance | planifiée | IGNORE | Pas de ticket (contexte reconnu) |
| Régression Payment | post-déploiement | TICKET P1 | Escalade par criticité + déploiement récent |
| SLO burn Payment | critique | TICKET P0 | Escalade maximale VIP |
| Lag events-router | auto-remédiation | REMEDIATE | Runbook scale-consumer exécuté |

> Chaque scénario a sa commande correspondante dans eventgen.py.
> pour plus de détails, voir [DEMO.md](DEMO.md)


## Notes complémentaires

### Mattermost

Utilisation d'un bot:
* Aller dans la system console
* Créer un bot 
* Retourner sur la page utilisateur (admin)
* Aller dans Integrations
* Créer sur "Create new token" du bot 
* Ne pas oublier d'ajouter le bot à la team (System Console > Teams) ou directement sur le channel ?
