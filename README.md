# devoxxfr2026-kafka-n8n-ia

> Code source de la démo pour "Automatisation intelligente en temps réel : quand Kafka rencontre n8n et l’IA"


## Composants

- **Apache Kafka (1 nœud)** : bus d’événements temps réel. Les producteurs publient des JSON sur un topic unique `events.v1`.  
- **Kafdrop** : UI web pour Kafka (topics, messages, consumer groups). Idéal pour montrer “les events arrivent” pendant les tests / au début de la démo. Kafdrop est une *Kafka Web UI* pour voir les topics et parcourir les consumer groups/messages. [web:181][web:183]
- **n8n** : orchestrateur low-code. Le workflow est déclenché par le **Kafka Trigger** à chaque nouveau message Kafka. [web:1]
- **Ollama (sur le host Mac)** : LLM **offline**. n8n y accède via `http://host.docker.internal:11434`.
- **MCP server (Python, SSE)** : expose des **tools** (lookup CMDB, déploiements, incidents similaires, exécution runbook). n8n connecte ces tools à l’agent via le nœud **MCP Client Tool** (SSE endpoint). [page:32]
- **Runbook API (Python/FastAPI)** : exécute une remédiation simulée (ex: scale-consumer) avec preuve (`/state` + retour JSON).
- **Ticket UI (Python/FastAPI + SQLite + Kanban)** : UI “Jira-like” (TODO / IN_PROGRESS / DONE). n8n crée des tickets + ajoute des commentaires (décision, rationale, résultat de remédiation).
- **Mattermost (self-host Docker)** : chat “Slack-like” local. n8n poste dans `#incidents` via le nœud Mattermost, authentifié par **API access token** (PAT). [web:280][web:281]


## Lancement des composants de la démo

### Kafka, n8n, ...

Pour démarrer un noeud Kafka et n8n, utilisez la commande: `docker compose up`

### Tools (MCP)

Pour lancer le serveur MCP (simulation de tools utiliiable par l'agent): `uv run mcp-tools`


## Workflow n8n

* Chargement:
	Le fichier du workflow est `demo-workflow.json`.
	Pour le charger, ouvrez (n8n)[http://localhost:5632], puis allez dans "Import Workflow", vous obtenez le contenu suivant: [docs/n8n-workflow-step-1.png](Workflow n8n)
* Lancement: cliquez sur le bouton `Start`


## Démos

### Génération d'événements

```bash
cd eventgen

# Installation
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Lancement
python eventgen.py noise --count 10
python eventgen.py incident-vip
python eventgen.py replay --template incident-vip --event-id 11111111-1111-1111-1111-111111111111
python eventgen.py replay --template incident-vip --event-id 11111111-1111-1111-1111-111111111111
python eventgen.py remediate-lag

```

