# Journal des modifications

Toutes les modifications notables de ce projet sont documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/),
et ce projet adhère à la [gestion sémantique de version](https://semver.org/lang/fr/).

## [Non publié]

## [1.1.0] - 2026-11-24

### Ajouté

- Envoi des données vers Prometheus par *remote write*, sans dépendance supplémentaire.
- Envoi simultané possible vers InfluxDB et Prometheus.
- Tableau de bord Grafana pour Prometheus (`dashboard_grafana_prometheus.json`).
- Publication de versions sous forme d'archive `.tar.gz`, avec somme de contrôle SHA-256 et
  attestation de provenance.
- Script d'installation et de mise à jour automatique (`install.sh`).
- Commande `teleinfo-linky`, avec les options `--config` (chemin du fichier de configuration)
  et `--version`.
- Tests automatisés et intégration continue.

### Modifié

- Le code est découpé en un paquet Python (`teleinfo_linky`). `linky.py` reste disponible
  comme lanceur de compatibilité.
- L'installation se fait désormais dans un environnement virtuel Python (venv), qui utilise
  les paquets Debian `python3-yaml` et `python3-serial` pour éviter de longues compilations sur
  Raspberry Pi Zero.
- Le service systemd utilise `%h` au lieu de `/home/pi` et fonctionne pour tout utilisateur.
  Il attend 10 secondes avant de relancer le programme après un échec.
- La section `influxdb` du fichier de configuration devient facultative.
- La bibliothèque `influxdb-client` n'est nécessaire que pour l'envoi vers InfluxDB.
- Après une erreur du port série en cours de lecture, le port est fermé puis rouvert 10
  secondes plus tard, au lieu de produire des erreurs en boucle. Les trames en attente d'envoi
  sont conservées.

### Corrigé

- Un groupe d'information dont la somme de contrôle est un espace n'est plus ignoré (par
  exemple une intensité instantanée de 9 A).
- Le dernier groupe d'information de chaque trame n'est plus ignoré.
- Une trame n'est plus modifiée lors de son envoi, ce qui permet de l'envoyer vers plusieurs
  destinations.
- Tableau de bord InfluxDB : les graphiques « Mois en cours » et « 12 derniers mois » ne
  dépassent plus le délai de requête de Grafana sur un an de données, suivent la période du
  tableau de bord et datent chaque barre par le début de sa période. « 12 derniers mois »
  affiche les douze derniers mois au lieu de l'année civile précédente.

## [1.0.0] - 2023-02-28

### Ajouté

- Version initiale : lecture de la téléinformation client (TIC) du compteur Linky, envoi vers
  InfluxDB 2.x, service systemd et tableau de bord Grafana.

[Non publié]: https://github.com/hleroy/teleinfo-linky/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/hleroy/teleinfo-linky/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/hleroy/teleinfo-linky/releases/tag/v1.0.0
