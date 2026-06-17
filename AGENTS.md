# AGENTS.md

Plugin Wazo (PBX basé sur Asterisk) ajoutant la gestion des files d'attente et la
diffusion temps réel de leur état. GPL-3.0+. Version dans `wazo/plugin.yml`.

## Structure

Un seul paquet, deux plugins déclarés dans `setup.py` (entry points) :

- `wazo_calld_queue/` — plugin `wazo-calld` : API REST `/queues/*` + pont d'événements bus.
- `wazo_call_logd_queue/` — plugin `wazo-call-logd` : persistance des `queue_log` Asterisk en base + publication bus.
- `etc/` — configuration déployée (dialplan Asterisk, ACL, activation des plugins).
- `tests/` — vide (aucune couverture).

### `wazo_calld_queue/` (cœur)

- `plugin.py` — point d'entrée : instancie les clients (amid, confd, agentd, ari), enregistre les resources et abonne le handler d'événements.
- `resources.py` — endpoints REST (`AuthResource`, ACL `required_acl`).
- `services.py` — `QueueService` : actions AMI (queuesummary/status/add/remove/pause/withdrawcaller).
- `bus_consume.py` — `QueuesBusEventHandler` : s'abonne aux événements Asterisk, met à jour l'état, republie sur le bus (multi-tenant).
- `events.py` / `schema.py` — événements bus (`TenantEvent`) et schémas marshmallow.

## Fonctionnement clé

- **API → AMI** : les endpoints REST traduisent les requêtes en actions Asterisk Manager Interface.
- **Bus → état → bus** : `bus_consume.py` consomme les événements `QueueCaller*`/`QueueMember*`, maintient deux dicts globaux en mémoire (`stats`, `agents`) et republie des événements enrichis pour le websocket front.
- **État global en mémoire** : `stats` et `agents` ne sont pas partagés entre workers et sont perdus au redémarrage (par design, temps réel).
- **Multi-tenant** : `_extract_tenant_uuid` lit `WAZO_TENANT_UUID` ou résout via confd.

## Conventions

- Python, en-tête de copyright Wazo + `SPDX-License-Identifier: GPL-3.0+` en tête de fichier.
- Commits conventionnels (`feat:`, `fix:`, `chore:`, ...).
- Le numéro de version vit dans `wazo/plugin.yml` (lu par `setup.py`).

## Dette technique connue

- `agent.py` (`AgentStatusHandler`) et `queue.py` (`QueueStatusHandler`) : refactoring OO de la logique de `bus_consume.py`, **non branchés** (importés nulle part).
- `print()` de debug laissés en place : `bus_consume.py:219`, `agent.py:20`.
- Aucun test.
