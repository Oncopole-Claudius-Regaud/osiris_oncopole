#  OSIRIS Data Pipeline
Ce projet est un pipeline de traitement de données cliniques basé sur le modèle OSIRIS RWD, développé dans le cadre d’un ETL automatisé pour l’IUCT Oncopole. Il permet l’extraction de données depuis Oracle IRIS, leur transformation et leur chargement dans PostgreSQL, avec une structure conforme aux standards OSIRIS/OMOP.

---

##  Fonctionnalités principales

-  ETL automatisé via Airflow (Oracle ➝ PostgreSQL)
-  Modélisation alignée OSIRIS RWD (Patient, Measure, Condition, TreatmentLine, etc.)
-  Détection et journalisation des cas manquants (patients non retrouvés ou sans traitements)
-  Intégration future vers de la BI (analytiques)

---

##  Stack technique

-  **Python 3.12**
-  **Apache Airflow**
-  **Oracle + IRIS + PostgreSQL**
-  **Pandas / cx_Oracle / psycopg2**
-  **GIT + SSH**
-  (Prochainement : tests unitaires)

---

##  Structure du projet

.
├── credentials.yml
├── docs
│   └── OSIRIS RWD_GT2_MODELE_2025.05.14.png
├── extract_chimio.py
├── osiris.py
├── requete_finale.sql
└── utils
    └── enrich_measurements.py

---

##  Lancer le pipeline

```bash
# 1. Activer l'environnement
source airflow_env/bin/activate

# 2. Démarrer Airflow
airflow scheduler &
airflow webserver &
```
---

##  Sécurité & Confidentialité

Aucun fichier contenant des données sensibles ou identifiants patients ne doit être versionné.
Le fichier credentials.yml est ignoré via .gitignore.

---

## À venir

---

##  👨‍💻 Auteur

Djouldé Barry
