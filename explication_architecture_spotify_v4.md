# Architecture Big Data — Spotify 550K Songs
## Explication et justification des choix architecturaux

---

## Table des matières

1. [Concept central du projet](#1-concept-central-du-projet)
2. [Vue d'ensemble de l'architecture](#2-vue-densemble-de-larchitecture)
3. [Couche ① — Sources de données](#3-couche--sources-de-données)
4. [Couche ② — Ingestion des données](#4-couche--ingestion-des-données)
5. [Couche ③ — Apache Kafka, le bus de messages](#5-couche--apache-kafka-le-bus-de-messages)
6. [Couche ④a — HDFS, le data lake batch](#6-couche-a--hdfs-le-data-lake-batch)
7. [Couche ④b — Batch Processing avec Apache Spark](#7-couche-b--batch-processing-avec-apache-spark)
8. [Couche ④c — Stream Processing avec Spark Structured Streaming](#8-couche-c--stream-processing-avec-spark-structured-streaming)
9. [Couche ⑤ — Storage multi-database](#9-couche--storage-multi-database)
10. [Couche ⑥ — Visualisation et dashboarding](#10-couche--visualisation-et-dashboarding)
11. [Patterns architecturaux utilisés](#11-patterns-architecturaux-utilisés)
12. [Justification globale de la complexité](#12-justification-globale-de-la-complexité)

---

## 1. Concept central du projet

Le projet repose sur une idée originale : le **Mood Intelligence System**. L'objectif n'est pas simplement d'analyser des chansons, mais de construire en continu une **carte émotionnelle de la musique** à double lecture :

- En **batch**, on analyse les paroles de 550 000 chansons avec du traitement NLP pour révéler l'ADN émotionnel permanent de chaque genre musical, et on classifie les chansons en clusters de similarité acoustique via K-Means.
- En **streaming**, on observe les comportements d'écoute en temps réel pour mesurer l'humeur instantanée des auditeurs à travers des métriques audio comme la valence (positivité) et l'énergie.

Le croisement de ces deux dimensions produit un insight que ni Spotify ni aucune plateforme n'offre publiquement : est-ce que les gens cherchent de la musique qui correspond à leur humeur, ou cherchent-ils à la changer ?

---

## 2. Vue d'ensemble de l'architecture

L'architecture est organisée en six couches numérotées qui forment un pipeline de données complet, de la source brute jusqu'aux dashboards de visualisation.

```
① [Sources de données]
          ↓
② [Ingestion — Python producers]
          ↓
③ [Apache Kafka — bus de messages]
   song-metadata · song-plays · user-events · genre-signals · mood-index
       ↙                  ↓                        ↘
④a [HDFS]          ④c [Stream Spark]          [Elasticsearch]
snapshot              Mood · Tendances         (index statique
   ↓                  Anomalies · Fusion        title + lyrics)
④b [Batch Spark]         ↓                          ↓
  K-Means · NLP      [Cassandra]              [React Web App]
  Profilage            ↓
     ↓            [Grafana]
  [PostgreSQL]
     ↓
  [Metabase]
```

Ce pipeline suit le pattern **Lambda Architecture** : une voie batch pour la précision analytique, une voie streaming pour la réactivité temps réel, et une couche de stockage qui fusionne les deux.

---

## 3. Couche ① — Sources de données

L'architecture repose sur trois sources de données distinctes.

Le **Dataset Kaggle** (550 000 chansons CSV) constitue la base analytique statique du projet. Chaque chanson est accompagnée de ses métadonnées audio (BPM, énergie, dansabilité, valence, acousticité) et de ses paroles.

Le **Simulateur Python** produit des événements d'écoute en continu : play events, skips, likes. Puisqu'on n'a pas accès à de vrais flux Spotify en temps réel, ce simulateur relit le dataset de façon réaliste pour alimenter toute la couche streaming avec des données cohérentes et non arbitraires.

Les **APIs externes** (Musixmatch pour les paroles complètes, AcousticBrainz pour les features audio avancées) enrichissent les métadonnées que le dataset Kaggle ne couvre pas complètement.

---

## 4. Couche ② — Ingestion des données

### Ce qu'elle fait

La couche d'ingestion est composée de trois producers Python, chacun dédié à une source et à un topic Kafka cible.

Le **Producer batch CSV → Kafka** charge le dataset Kaggle statique et publie les métadonnées de chaque chanson dans le topic `song-metadata`. C'est un chargement ponctuel, déclenché au démarrage du système ou lors d'une mise à jour du dataset.

Le **Producer enrichissement données additionnelles** collecte les données des APIs externes et les publie également dans le topic `song-metadata`, complétant les entrées du dataset Kaggle avec des paroles plus complètes et des features audio supplémentaires.

Le **Producer stream événements simulés** génère en continu des événements d'écoute simulés et les publie dans les topics `song-plays` (événements de lecture) et `user-events` (actions : skip, like, repeat).

### Pourquoi tout passe par Kafka

Faire transiter toutes les données — statiques comme dynamiques — par Kafka comme point d'entrée unique présente plusieurs avantages. Cela crée un **point de collecte unifié** : un seul système reçoit tous les flux entrants, quelle que soit leur nature. Cela permet aussi un **rejeu des données** : si un consommateur tombe en panne, il reprend depuis son dernier offset sans perte. Enfin, cela découple complètement les producers des consommateurs : HDFS, Elasticsearch et Spark Streaming lisent depuis Kafka à leur propre rythme, sans jamais interagir directement avec la couche d'ingestion.

---

## 5. Couche ③ — Apache Kafka, le bus de messages

### Ce qu'elle fait

Apache Kafka joue le rôle de **colonne vertébrale** de toute l'architecture. Il reçoit tous les événements produits par la couche d'ingestion et les distribue vers les bons consommateurs. On définit **cinq topics** distincts :

- `song-metadata` : les métadonnées et paroles des chansons, produites par le Producer batch et le Producer enrichissement. Ce topic est consommé par deux consommateurs en parallèle : un **consumer HDFS** qui persiste les données en snapshot complet, et **Elasticsearch** qui indexe les champs textuels.
- `song-plays` : chaque événement d'écoute simulé (chanson, utilisateur, durée).
- `user-events` : les actions utilisateurs (skip, like, repeat).
- `genre-signals` : les centroïdes et labels de clusters K-Means produits par le batch Spark, renvoyés dans Kafka pour être consommés par le job de fusion batch→stream.
- `mood-index` : les scores de mood calculés en temps réel par le streaming, publiés pour être consommés directement par Grafana.

### Pourquoi Kafka plutôt qu'autre chose

Kafka est choisi pour trois raisons fondamentales.

La première est le **découplage total**. Aucun composant de l'architecture n'appelle directement un autre. HDFS, Elasticsearch et Spark Streaming lisent depuis le même topic `song-metadata` à des vitesses radicalement différentes, sans se bloquer mutuellement.

La deuxième raison est la **rétention des messages**. Kafka conserve les messages pendant une durée configurable (7 jours). Cela permet à HDFS de constituer un snapshot complet même si le consumer redémarre en cours de route.

La troisième raison est la **scalabilité**. Kafka est partitionné par genre musical, ce qui distribue la charge de traitement sur plusieurs nœuds en parallèle.

---

## 6. Couche ④a — HDFS, le data lake batch

### Ce qu'elle fait

HDFS (Hadoop Distributed File System) est le **data lake** de l'architecture. Il reçoit les données du topic `song-metadata` via un consumer dédié qui persiste l'intégralité des messages sous forme d'un **fichier complet** (snapshot). Ce snapshot représente l'état total du dataset — les 550 000 chansons avec leurs métadonnées et paroles enrichies — dans un format lisible nativement par Spark.

HDFS est physiquement intégré dans la couche batch (④a est un sous-composant de ④b) car il n'a d'existence utile que comme source d'alimentation pour Spark Batch. Il ne sert aucun autre consommateur.

### Pourquoi HDFS entre Kafka et Spark Batch

On pourrait se demander pourquoi ne pas faire lire Spark directement depuis Kafka. La réponse est fondamentale : Spark lisant un topic Kafka en mode batch le traite en réalité comme une série de **micro-batches**, ce qui correspond au pattern streaming et non au pattern batch. HDFS rompt cette ambiguïté : le consumer HDFS matérialise le topic `song-metadata` en un fichier complet et statique, que Spark lit en **une seule passe parallèle** sur l'intégralité des données. C'est la définition exacte du vrai traitement batch.

Ce pattern — Kafka comme point d'entrée unifié, HDFS comme matérialisation pour le batch — est le meilleur compromis entre l'unification de l'ingestion (tout passe par Kafka) et la rigueur du pattern Lambda (le batch lit un dataset complet, pas un flux).

---

## 7. Couche ④b — Batch Processing avec Apache Spark

### Ce qu'elle fait

Le batch processing s'applique à l'intégralité du dataset une fois par nuit. Spark lit le snapshot complet depuis **HDFS** et produit des résultats analytiques profonds et précis. Tous les jobs batch écrivent leurs résultats dans **PostgreSQL**.

**Trois jobs batch** sont définis.

Le premier est le **K-Means clustering** : Spark MLlib applique un algorithme K-Means sur les features audio (BPM, énergie, valence, acousticité, etc.) pour regrouper les 550 000 chansons en clusters de similarité musicale. Ces clusters révèlent des proximités acoustiques que les genres déclarés n'expriment pas — un morceau de hip-hop peut acoustiquement ressembler à un morceau de pop. Les labels de clusters sont écrits dans PostgreSQL et les centroïdes sont renvoyés dans le topic Kafka `genre-signals` pour enrichir la couche streaming.

Le deuxième job est l'**analyse de sentiment NLP des paroles** avec Spark NLP. Chaque chanson reçoit un score de sentiment (positif, négatif, neutre) et un score de confiance. Ces scores constituent la dimension "émotionnelle permanente" de la Mood Map et sont stockés dans PostgreSQL.

Le troisième job est le **profilage par genre** : Spark agrège pour chaque genre musical les moyennes de BPM, énergie et valence, construisant une empreinte sonore stable par genre. Ce profil, stocké dans PostgreSQL, permet de mesurer l'écart entre le mood attendu d'un genre et le mood observé en temps réel lors des écoutes streaming.

### Pourquoi Spark pour le batch

Apache Spark est le standard industriel pour le traitement distribué à grande échelle. Sur 550 000 chansons avec des algorithmes de machine learning (K-Means) et du NLP, un traitement Python séquentiel prendrait des heures là où Spark distribue le calcul sur plusieurs nœuds en parallèle. Spark intègre nativement MLlib, Spark NLP, et des connecteurs directs vers HDFS, Kafka et PostgreSQL.

---

## 8. Couche ④c — Stream Processing avec Spark Structured Streaming

### Ce qu'elle fait

La couche streaming tourne en continu, traitant les événements par micro-batches de 30 secondes. Elle produit des résultats avec une latence inférieure à 5 secondes. Elle lit exclusivement depuis Kafka et écrit dans Cassandra.

**Quatre jobs streaming** sont définis.

Le premier est le **détecteur de tendances** : une fenêtre glissante de 5 minutes compte le nombre de plays par chanson et par genre. Une chanson qui dépasse un seuil d'écoutes dans cette fenêtre entre dans le top trending. Le résultat est persisté dans Cassandra et mis à jour chaque minute.

Le deuxième job est le **tracker de mood** : il calcule en continu le score de mood par genre en combinant la valence moyenne et l'énergie moyenne des chansons écoutées. Ce score est publié dans le topic `mood-index` (consommé par Grafana) et persisté dans Cassandra.

Le troisième job est la **détection d'anomalies** : le système identifie des comportements suspects, comme une même chanson jouée plus de 100 fois en une minute (bot) ou un utilisateur qui saute entre plus de 10 genres en 30 secondes. Les anomalies sont persistées dans Cassandra.

Le quatrième mécanisme est la **fusion batch→stream** : les centroïdes K-Means produits la nuit par le batch, renvoyés dans le topic `genre-signals`, sont consommés par ce job pour enrichir les calculs de mood en temps réel. C'est ce mécanisme qui réalise la jonction des deux couches et constitue le cœur du pattern Lambda.

### Pourquoi Spark Structured Streaming

Utiliser le même framework pour le batch et le streaming offre un avantage considérable : même API, mêmes connecteurs, mêmes abstractions. Cela réduit la complexité opérationnelle et garantit le traitement **exactement une fois** (exactly-once semantics), crucial pour la fiabilité des métriques.

---

## 9. Couche ⑤ — Storage multi-database

### Ce qu'elle fait et pourquoi trois bases

L'architecture utilise trois systèmes de stockage spécialisés avec des responsabilités exclusives et non redondantes. Ce pattern s'appelle la **polyglot persistence**.

**Apache Cassandra** est la base des **données chaudes produites par le streaming**. Elle stocke les événements de play en temps réel, les scores de mood, les chansons tendance et les anomalies détectées. Cassandra est optimisée pour les écritures massives et les séries temporelles — elle absorbe des milliers d'écritures par seconde sans dégradation de performance. Sa structure distribuée sans point de défaillance unique la rend très résiliente.

**PostgreSQL** est la base des **résultats du batch**. Elle est l'unique destination de tous les jobs Spark Batch : labels de clusters K-Means, scores de sentiment NLP et profils audio par genre. PostgreSQL excelle pour les requêtes analytiques complexes, les jointures entre tables et la cohérence transactionnelle (ACID). Elle est aussi consultée directement par la React Web App pour afficher les métadonnées enrichies lors des résultats de recherche.

**Elasticsearch** est dédiée exclusivement à la **recherche full-text sur les paroles**. Lors du chargement initial, le consumer `song-metadata` indexe trois champs par chanson : `song_id`, `title` et `lyrics`. Cet index est **statique** — ni le batch ni le streaming n'y écrivent. Lorsqu'un utilisateur soumet un fragment de paroles dans la React Web App, Elasticsearch retourne les `song_id` correspondants. L'application joint ensuite ces identifiants avec PostgreSQL pour récupérer les métadonnées complètes et les scores analytiques. Cette séparation évite toute redondance : les données computées restent dans PostgreSQL, seule la clé d'accès textuelle est fournie par Elasticsearch.

### Pourquoi pas une seule base

Aucune base de données existante ne gère bien simultanément les trois cas d'usage. Cassandra est inadaptée aux requêtes analytiques complexes. PostgreSQL ne supporte pas des milliers d'écritures par seconde en time-series. Elasticsearch ne garantit pas la cohérence transactionnelle et n'est pas conçue pour du stockage analytique. Choisir une seule base aurait imposé des compromis dégradant l'ensemble du système.

---

## 10. Couche ⑥ — Visualisation et dashboarding

### Ce qu'elle fait

La couche de visualisation expose les résultats à trois audiences différentes via trois interfaces distinctes.

**Grafana** est l'outil de monitoring temps réel. Il consomme le topic Kafka `mood-index` et lit dans Cassandra pour afficher en direct l'évolution du mood par genre, les chansons tendance, les anomalies détectées et les alertes volumétriques. Grafana est pensé pour être consulté en permanence sur un écran de monitoring.

**Metabase** est l'outil d'analytics pour les analystes de données. Il se connecte à PostgreSQL et permet d'explorer les clusters de chansons, la distribution des sentiments par genre, et l'évolution du profil audio d'un genre sur plusieurs décennies.

**La React Web App** est l'interface destinée aux utilisateurs finaux. Elle orchestre deux sources complémentaires : Elasticsearch pour la recherche full-text par paroles (qui retourne des `song_id`), PostgreSQL pour les métadonnées complètes, les scores de sentiment et les profils audio, et Cassandra pour les données de mood et tendances live. Elle propose une Mood Map — visualisation originale croisant sentiment des paroles et mood des écoutes — ainsi qu'un radar chart audio par chanson.

### Pourquoi trois outils de visualisation

Chaque outil est adapté à un usage et une audience précis. Grafana est conçu pour le monitoring opérationnel en temps réel. Metabase simplifie l'exploration analytique sans écrire de code. La React Web App offre une expérience utilisateur personnalisée qu'aucun outil générique ne peut fournir.

---

## 11. Patterns architecturaux utilisés

### Lambda Architecture

La Lambda Architecture est le pattern central du projet. Elle postule que tout système Big Data doit avoir deux voies de traitement parallèles :

La **batch layer** traite toutes les données avec une précision maximale mais une latence élevée. Spark Batch lit depuis HDFS la nuit et produit des résultats exacts (clusters K-Means, sentiment NLP, profilage par genre) stockés dans PostgreSQL.

La **speed layer** traite les données nouvelles avec une latence minimale. Spark Streaming lit depuis Kafka et produit des métriques toutes les 30 secondes (mood live, tendances, anomalies) stockées dans Cassandra.

La **serving layer** fusionne les deux : PostgreSQL expose les résultats batch stables, Cassandra expose les métriques live, Elasticsearch expose l'accès textuel. La React Web App orchestre les trois pour une expérience unifiée.

La jonction physique des deux couches se fait via le topic `genre-signals` : les centroïdes K-Means (batch) enrichissent les calculs de mood (streaming) en temps réel.

### Polyglot Persistence

Chaque base de données a une responsabilité exclusive et non redondante : Cassandra pour le streaming chaud, PostgreSQL pour le batch froid, Elasticsearch pour la recherche textuelle statique. Aucune donnée n'est dupliquée entre elles.

### Event-Driven Architecture

Kafka est le point d'entrée unifié de toute l'architecture. Tous les flux — statiques comme dynamiques — y transitent, permettant un découplage total entre producers et consommateurs. Chaque consommateur (HDFS, Elasticsearch, Spark Streaming) lit à son propre rythme sans impacter les autres.

---

## 12. Justification globale de la complexité

Chaque élément de complexité est justifié par un besoin réel.

**Kafka** est le point d'entrée unifié de tous les flux. Ses **cinq topics** ont des rôles précis et non redondants : `song-metadata` pour les données statiques des chansons, `song-plays` et `user-events` pour les événements streaming, `genre-signals` pour la jonction batch→stream, `mood-index` pour le monitoring temps réel.

**HDFS** est nécessaire pour matérialiser le topic `song-metadata` en dataset complet avant que Spark Batch le lise. Sans lui, Spark lirait Kafka en micro-batches, trahissant le pattern Lambda.

**Spark** est nécessaire parce que 550 000 chansons avec K-Means et Spark NLP ne peuvent pas être traitées efficacement par un processus séquentiel.

**Trois bases de données** répondent à trois besoins fondamentalement différents et non superposables : écritures massives time-series (Cassandra), requêtes analytiques complexes sur données batch (PostgreSQL), recherche full-text statique (Elasticsearch).

**Trois interfaces de visualisation** servent trois audiences distinctes : monitoring temps réel (Grafana), exploration analytique (Metabase), expérience utilisateur finale (React Web App).

L'originalité du projet réside dans l'intégration cohérente de ces technologies autour du concept unificateur — le **Mood Intelligence System** — qui donne un sens et une direction à chacun des choix techniques effectués.

---

*Projet Big Data — Architecture Spotify 550K Songs — Mood Intelligence System — v4*
