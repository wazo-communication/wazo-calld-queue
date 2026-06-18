# wazo-calld-queue

Plugin Wazo qui ajoute le contrôle des files d'attente à `wazo-calld` (API REST
`/queues/*`) et publie l'état des queues et des agents en temps réel sur le
websocket. Le plugin `wazo-call-logd` associé persiste les entrées `queue_log`
d'Asterisk.

## Endpoints

Disponibles dans l'API `wazo-calld` (`http://<wazo>/api`, section calld) :

| Méthode et route | Description |
|---|---|
| `GET /queues` | Liste les files d'attente |
| `GET /queues/{queue_name}` | Détail d'une file |
| `PUT /queues/{queue_name}/add_member` | Ajoute un membre |
| `PUT /queues/{queue_name}/remove_member` | Retire un membre |
| `PUT /queues/{queue_name}/pause_member` | Met un membre en pause |
| `GET /queues/{queue_name}/livestats` | Statistiques temps réel |
| `GET /queues/agents_status` | Statut de tous les agents |
| `POST /queues/intercept/{queue_name}` | Intercepte un appel en attente |

Un agent peut appartenir à plusieurs files. Pour la sémantique REST/événements
et l'intégration côté client, voir
[docs/FRONTEND_INTEGRATION.md](docs/FRONTEND_INTEGRATION.md).

## Installation

```bash
wazo-plugind-cli -c "install git https://github.com/wazo-communication/wazo-calld-queue"
```

Nécessite Wazo 26.06 ou supérieure.

## Auteurs

- Sylvain Boily
- Mathias WOLFF

## Licence

GPL-3.0+. Voir le fichier `LICENSE`.

## Support

Ouvrez une issue sur
[GitHub](https://github.com/wazo-communication/wazo-calld-queue/issues).
