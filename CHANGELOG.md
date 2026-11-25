# Journal des modifications

Toutes les modifications notables de ce projet sont documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/),
et ce projet adhère à la [gestion sémantique de version](https://semver.org/lang/fr/).

## [Non publié]

## [1.2.1] - 2026-11-25

### Ajouté

- Le journal confirme que tout fonctionne au démarrage : liste des étiquettes retenues dans la
  première trame (et de celles qui en sont absentes), puis premier envoi réussi vers InfluxDB et
  vers Prometheus.
- Bilan toutes les 24 heures : nombre de trames reçues, et envoyées vers chaque destination. Il
  passe en avertissement si aucune trame n'a été reçue ou envoyée, avec le nombre de trames en
  attente, ou si des trames ont été perdues.
- Avertissement si aucune trame n'est reçue dans la minute qui suit le démarrage (câblage, port
  série ou mode TIC à vérifier), ou si les trames ne contiennent aucune des étiquettes de
  `linky.keys`.
- Message lorsque l'envoi vers InfluxDB ou Prometheus reprend après une erreur.

### Modifié

- Les messages commencent par le nom du composant concerné (`Téléinfo`, `InfluxDB`,
  `Prometheus`) et sont formulés de la même façon pour InfluxDB et Prometheus. Le message
  `Connexion à …` d'InfluxDB, trompeur car aucune connexion n'est établie à ce moment, devient
  `InfluxDB : envoi vers …, bucket …`.
- InfluxDB : un token refusé (HTTP 401) est signalé comme un accès refusé, comme HTTP 403, et un
  serveur injoignable n'affiche plus de trace d'exception.
- Le mode TIC (historique ou standard) est indiqué à l'ouverture du port série.

### Corrigé

- Une fausse première trame, sans aucune étiquette, pouvait être lue au démarrage : un octet mal
  reçu pendant la synchronisation du port série était pris pour un début de trame.

## [1.2.0] - 2026-11-24

Cette version s'inspire des forks de [François Charlier](https://github.com/fcharlier) et de
[Heat Miser](https://github.com/Heat-Miser) : merci à eux.

### Ajouté

- Prise en charge des étiquettes textuelles : `OPTARIF`, `PTEC`, `DEMAIN` et `HHPHC` en mode
  historique, `NGTF`, `LTARF`, `MSG1`, `MSG2`, `PJOURF+1` et `PPOINTE` en mode standard. La
  couleur du lendemain en option Tempo peut ainsi être enregistrée. Elles sont envoyées sous
  forme de texte vers InfluxDB, et deviennent des métriques d'information
  `linky_<étiquette>_info` dans Prometheus.
- Prise en charge des groupes horodatés du mode standard (`SMAXSN`, `CCASN`, `UMOY1`…) : la
  somme de contrôle porte sur le groupe entier et seule la valeur est conservée.
- Tableaux de bord Grafana pour l'option heures pleines / heures creuses avec InfluxDB, en mode
  historique (`dashboard_grafana_hphc.json`) et en mode standard
  (`dashboard_grafana_standard_hphc.json`, avec l'énergie injectée).
- Message à la réception de la première trame, qui confirme que la liaison série fonctionne.
- Les messages sont horodatés lorsque le programme n'est pas lancé par systemd (journald les
  horodate déjà).

### Modifié

- Les messages émis à chaque trame (réception, écriture dans InfluxDB ou Prometheus) passent au
  niveau debug, pour ne plus remplir le journal.
- Une somme de contrôle erronée est signalée par un avertissement, et plus seulement en mode
  debug.
- Prometheus : les caractères `+` et `-` des étiquettes du mode standard sont remplacés par `_`
  (`SMAXSN-1` devient `linky_smaxsn_1`), au lieu d'ignorer l'étiquette.

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

[Non publié]: https://github.com/hleroy/teleinfo-linky/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/hleroy/teleinfo-linky/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/hleroy/teleinfo-linky/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/hleroy/teleinfo-linky/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/hleroy/teleinfo-linky/releases/tag/v1.0.0
