# TODO

## A faire

* [ ] Tester l'intégration complète en local (docker-compose up)
* [ ] Vérifier que eventgen.py génère du JSON valide pour les 5 scénarios
* [ ] Valider le workflow n8n avec de vrais événements Kafka
* [ ] Tester la connectivité du serveur MCP vers ops-controller
* [ ] Préparer les slides
* Cohérence du langage
  * [ ] Relire les fichiers JSON (CMDB, incidents, deployments) - mélange anglais/français
  * [ ] S'assurer que tous les messages UI sont en anglais pour un public international


## Fait

### **MCP Tools** (ops_mcp_server/main.py)

* `fetch_service_logs`
  * [x] Logs variants par service (payment, catalog, events-router)
  * [x] Erreurs contextualisées : Stripe, Elasticsearch, Consumer lag, etc.

* `get_similar_incidents`
  * [x] Support exact match ET partial match (service:env:type)
  * [x] Gère les variations d'endpoint (/search vs /list)
  * [x] Retourne match_type pour l'audit

* `execute_remediation`
  * [x] 3 runbooks supportés : `scale-consumer`, `restart-service`, `disable-feature-flag`
  * [x] Policy-gated : chaque runbook autorisé sur certains services
  * [x] Dry-run par défaut (safe) -> agent peut explorer avant d'agir
  * [x] Idempotency key pour éviter les doublons

### **Workflow n8n**

* [x] **Claude Sonnet 4.6** (au lieu de Haiku) pour décisions plus robustes
* [x] System prompt amélioré 

### **Runbooks dans ops_controller**

- `scale-consumer` : Augmente replicas (events-router uniquement)
- `restart-service` : Redémarre service (events-router, catalog)
- `disable-feature-flag` : Rollback feature (payment, catalog)
