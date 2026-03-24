# Elections CI 2025 — Chat with your data

Application de chat pour interroger les résultats des élections législatives ivoiriennes du 27 décembre 2025 (élection des députés à l'Assemblée Nationale).

## Architecture

```
election-chat/
├── ingestion/          # Pipeline d'extraction PDF → DuckDB
│   ├── extract.py      # OCR + parsing des tableaux PDF
│   ├── normalize.py    # Normalisation accents, casse, alias partis
│   ├── load_db.py      # Schéma DuckDB + chargement
│   └── pipeline.py     # Orchestrateur CLI
├── data/
│   ├── elections.duckdb    # Base générée (gitignore)
│   ├── schema.sql          # DDL référence
│   └── raw/                # PDF source + CSV intermédiaires
├── agent/              # Agent Text-to-SQL (Level 1)
│   ├── sql_agent.py    # LLM → SQL → exécution → réponse
│   ├── guardrails.py   # Validation SQL, détection injections
│   ├── intent.py       # Classification d'intention
│   └── chart.py        # Génération graphiques Plotly
├── rag/                # Hybride SQL+RAG (Level 2)
│   ├── indexer.py      # Embeddings des lignes
│   ├── retriever.py    # Recherche vectorielle
│   └── router.py       # SQL vs RAG
├── agentic/            # Agent avec clarification (Level 3)
│   ├── disambiguate.py # Détection entités ambiguës
│   ├── clarifier.py    # Pose des questions ou propose des options
│   └── session.py      # Mémoire de session
├── observability/      # Tracing + logs (Level 4)
│   └── tracer.py
├── evals/              # Suite d'évaluation offline (Level 4)
│   ├── eval_runner.py
│   ├── fixtures.json   # Gold set Q&A
│   └── test_*.py
├── app/
│   └── main.py         # Interface Streamlit
├── tests/              # Tests unitaires
├── Makefile
├── pyproject.toml
└── .env.example
```

## Prérequis

- Python 3.10+
- Tesseract OCR
- Poppler (pdf2image)
- Une clé API Anthropic

### Installation système (Ubuntu/Debian)
```bash
sudo apt-get install tesseract-ocr tesseract-ocr-fra poppler-utils
```

### Installation système (macOS)
```bash
brew install tesseract tesseract-lang poppler
```

## Installation Python

```bash
# Cloner le repo
git clone <repo-url>
cd election-chat

# Créer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## Configuration

```bash
cp .env.example .env
# Éditez .env et ajoutez votre clé API Anthropic :
# ANTHROPIC_API_KEY=sk-ant-...
```

## Lancement

### 1. Pipeline d'ingestion (une seule fois)

```bash
# Copier le PDF dans data/raw/
cp /chemin/vers/EDAN_2025_RESULTAT_NATIONAL_DETAILS.pdf data/raw/edan_2025.pdf

# Lancer l'ingestion complète
make ingest
# ou directement :
python -m ingestion.pipeline --pdf data/raw/edan_2025.pdf --db data/elections.duckdb
```

L'ingestion dure environ **5-10 minutes** (OCR de 35 pages à 250 DPI).

### 2. Lancer l'application

```bash
make run
# ou :
streamlit run app/main.py
```

Ouvrir [http://localhost:8501](http://localhost:8501)

### 3. Lancer les tests

```bash
make test
```

### 4. Lancer l'évaluation (Level 4)

```bash
make eval
```

## Commandes Makefile

```bash
make ingest     # Pipeline complet PDF → DuckDB
make run        # Lance l'app Streamlit
make test       # Tests unitaires
make eval       # Suite d'évaluation offline
make clean      # Supprime la base et les CSV générés
make lint       # Vérification code (ruff)
```

## Questions supportées (exemples)

| Question | Type |
|---|---|
| Combien de sièges a remporté le RHDP ? | Agrégation |
| Top 10 candidats par score | Classement |
| Taux de participation par circonscription | Agrégation |
| Histogramme des élus par parti | Graphique |
| Qui a gagné à Agboville ? | Lookup |
| Quelle circonscription a le plus fort taux de participation ? | Classement |
| Quel est le score de Koffi Aka Charles ? | Lookup |

## Guardrails de sécurité

- **SELECT uniquement** : INSERT/UPDATE/DELETE/DROP bloqués
- **LIMIT imposé** : max 100 lignes (configurable jusqu'à 500)
- **Tables autorisées** : liste blanche explicite
- **Détection d'injection de prompt** : patterns connus bloqués
- **Hors dataset** : réponse explicite "non disponible dans le dataset"

## Normalisation des entités

Le module `normalize.py` gère :
- Accents manquants : `"Cote d Ivoire"` → `"CÔTE D'IVOIRE"`
- Typos : `"Tiapum"` → `"TIAPOUM"` (Levenshtein ≤ 3)
- Alias partis : `"R.H.D.P"`, `"rhdp"` → `"RHDP"`
- Casse : tout normalisé en MAJUSCULES pour les noms propres CI

## Stack technique

| Composant | Technologie |
|---|---|
| Extraction PDF | pdfplumber + pdf2image + pytesseract |
| Base de données | DuckDB |
| LLM | Claude (Anthropic API) |
| Interface | Streamlit |
| Graphiques | Plotly |
| Embeddings (L2) | sentence-transformers + FAISS |
| Tests | pytest |

## Limitations connues

- OCR imparfait sur les caractères spéciaux et noms ivoiriens
- Certains scores de candidats non extraits (lignes mal formatées)
- Le PDF de la CEI est vectoriel sans texte encodé → nécessite OCR
- Taux de participation calculé sur les données extraites, peut différer légèrement du document officiel

## Prochaines étapes

- Level 2 : Ajouter RAG pour les questions narratives
- Level 3 : Détection d'ambiguïté et clarification
- Level 4 : Tracing end-to-end et suite d'évaluation automatisée
- Améliorer le parser OCR pour les noms avec apostrophes (N'GUESSAN, etc.)
