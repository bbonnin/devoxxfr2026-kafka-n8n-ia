# Demo

## Scénarios à montrer (5 x 2min = 10min)

Chaque événement Kafka est traité par l'agent IA qui :
1. Appelle les tools MCP (investigation)
2. Applique les règles déterministes
3. Décide : IGNORE / ALERT / TICKET / REMEDIATE
4. Exécute : création ticket OU runbook OU rien

```bash
# Terminal 1 : Lancer les services
docker compose up --build

# Terminal 2 : Lancer la démo
cd event_generator
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Envoyer les 5 scénarios un par un (ou la séquence)
python eventgen.py demo-sequence --interval 15

# OU envoyer des cas individuels en live pour des questions de l'audience
python eventgen.py noise        # Catalog timeout -> IGNORE (bruit)
python eventgen.py maintenance  # Catalog down -> IGNORE (maintenance prévue)
python eventgen.py regression   # Payment error -> TICKET (post-deploy)
python eventgen.py vip          # Payment SLO burn -> TICKET P0 (VIP)
python eventgen.py lag          # Events-router lag -> REMEDIATE (auto-heal)
```

---

## En détail

### **NOISE (Catalog timeout)**
- Event: `type=app.error, service=catalog, error_rate=0.2%`
- MCP investigation:
  - `get_service_context(catalog)` -> "Search timeouts are often noise"
  - `get_similar_incidents("catalog:prod:app.error:/search")` -> action=IGNORE
  - Logs: "Search response time exceeded threshold (2.1s > 1.0s)"
- **Decision: IGNORE**
- **Ce qu'on voit**: l'agent reconnaît un pattern connu, ne crée pas de false positive

### **MAINTENANCE (Catalog service.down)**
- Event: `type=service.down, service=catalog`
- MCP investigation:
  - `get_service_context(catalog)` -> notes contiennent "Maintenance planifiée chaque mardi"
  - `get_similar_incidents("catalog:prod:service.down")` -> action=IGNORE
- **Decision: IGNORE**
- **Ce qu'on voit**: le contexte métier sauve un ticket inutile

### **REGRESSION (Payment app error post-deploy)**
- Event: `type=app.error, service=payment, error_rate=5%`
- MCP investigation:
  - `get_service_context(payment)` -> tier=critical, "VIP impact possible"
  - `get_recent_investigation_data(payment)` -> déploiement v1.42.0 il y a 5 min
  - `fetch_service_logs(payment)` -> "Stripe API returned 500", "Circuit breaker opened"
- **Decision: TICKET P1** (critical tier + recent deploy)
- **Ce qu'on voit**:
  - Ticket créé automatiquement avec raison + contexte
  - Mattermost notifié
  - Lien ticket visible

### **VIP INCIDENT (Payment SLO burn)**
- Event: `type=slo.burn, service=payment, error_rate=15%, slo_burn=8.2, customerTier=VIP`
- MCP investigation:
  - `get_service_context(payment)` -> tier=critical, SLO=99.9%
  - `get_similar_incidents("payment:prod:slo.burn")` -> action=TICKET
- **Decision: TICKET P0** (VIP + SLO burn > 5)
- **Ce qu'on voit**:
  - P0 (rouge) créé d'office
  - "VIP impact" dans le titre
  - Confiance élevée (0.95)

### **AUTO-REMEDIATION (Events-router lag)**
- Event: `type=consumer.lag, service=events-router, lag=350k`
- MCP investigation:
  - `get_service_context(events-router)` -> tier=critical, notes="scale-consumer allowed"
  - `get_similar_incidents("events-router:prod:consumer.lag")` -> action=REMEDIATE
  - Logs: "Consumer lag rising: 50k -> 150k in 30s"
- **Decision: REMEDIATE** (avec runbook scale-consumer)
  - Appel : `execute_remediation(runbook_id=scale-consumer, service=events-router, dry_run=false, reason="Lag runaway detected")`
  - ops_controller répond : `status=running`
  - Quelques secondes plus tard : `status=success`, `replicas_desired=2`
  - Mattermost notifié avec lien d'exécution
- **Ce qu'on voit**:
  - Action automatique lancée ET exécutée en temps réel
  - Avant/après state visible
  - "Consumer lag will decrease in 2-5 min"

---

## Pre-Demo Checklist

- [ ] Docker compose up et tous les services sains
- [ ] n8n accessible sur http://localhost:5678
- [ ] Workflow "Demo-Kafka-Agent_IA-Anthropic" importé et activé (Published)
- [ ] Credentials Kafka, Anthropic, Mattermost configurées dans n8n
- [ ] Kafdrop accessible sur http://localhost:9000 (pour montrer les événements)
- [ ] Ticket Management sur http://localhost:7001 (pour montrer le Kanban)
- [ ] Mattermost sur http://localhost:8065 (pour montrer les notifications)
- [ ] eventgen.py prêt (requirements installés, venv activé)

### Test rapide
```bash
# Terminal 1
docker compose up

# Terminal 2 (une fois que tout est up)
cd event_generator && source .venv/bin/activate
python eventgen.py noise

# Vérifier:
# - Kafka topic "events.all" contient le message (Kafdrop)
# - n8n workflow a tournéet a une entrée dans executions
# - Mattermost #incidents reçoit la notification (ou pas si IGNORE)
# - Ticket Management a un ticket (ou pas si IGNORE)
```
