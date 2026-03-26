# devoxxfr2026-kafka-n8n-ia

> Code source de la démo pour "Automatisation intelligente en temps réel : quand Kafka rencontre n8n et l’IA"


## Composants

Ce qui est lancé par le docker compose:
* **Apache Kafka** : les producteurs publient des JSON sur un topic unique `events.all`.  
* **Kafdrop** : UI web pour Kafka (topics, messages, ...). Pour montrer les événements au début de la démo.
* **n8n** : orchestrateur low-code. Le workflow est déclenché par le **Kafka Trigger** à chaque nouveau message Kafka.
* **MCP server (Python, SSE)** : expose des **tools** (lookup CMDB, déploiements, incidents similaires, exécution runbook). n8n connecte ces tools à l’agent via le nœud **MCP Client Tool** (SSE endpoint).
* **Runbook API (Python/FastAPI)** : exécute une remédiation simulée (ex: scale-consumer) avec preuve (`/state` + retour JSON).
* **Ticket Management (Python/FastAPI + SQLite + Kanban)** : Kanban "Jira-like" (TODO / IN_PROGRESS / DONE). n8n crée des tickets + ajoute des commentaires (décision, rationale, résultat de remédiation).
* **Mattermost (self-host Docker)** : chat "Slack-like" local. n8n poste dans `#incidents` via le noeud Mattermost, authentifié par **API access token** (PAT).

En complément:
* **Ollama** (si présent sur le host) : n8n y accède via `http://host.docker.internal:11434`.

> Dans les exemples de workflow, il y a aussi l'utilisation d'Anthropic/Claude Sonnet, meilleur LLM à date pour cette démo.


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
* Lancement: cliquez sur le bouton `Start` (ou Publish ?)


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

## Notes complémentaires

### Mattermost

Utilisation d'un bot:
* Aller dans la system console
* Créer un bot 
* Retourner sur la page utilisateur (admin)
* Aller dans Integrations
* Créer sur "Create new token" du bot 
* Ne pas oublier d'ajouter le bot à la team (System Console > Teams) ou directement sur le channel ?