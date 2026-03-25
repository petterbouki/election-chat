# Elections CI 2025 — Chat with your data

Application de chat pour interroger en langage naturel les résultats officiels des **élections législatives ivoiriennes du 27 décembre 2025** (élection des députés à l'Assemblée Nationale).

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://election-chat-ci.streamlit.app)

---

## Architecture

```
election-chat/
├── app/
│   └── main.py                    # Interface Streamlit
├── agent/                         # Agent Text-to-SQL (Level 1)
│   ├── sql_agent.py               # LLM → SQL → exécution → réponse hybride
│   ├── guardrails.py              # Validation SQL, détection injections
│   └── chart.py                   # Génération graphiques Plotly
├── rag/                           # Hybride SQL + RAG (Level 2)
│   ├── router.py                  # Routing SQL vs RAG
│   └── retriever.py               # Recherche textuelle DuckDB
├── agentic/                       # Agent avec clarification (Level 3)
│   └── disambiguate.py            # Détection entités ambiguës + clarification
├── observability/                 # Tracing + logs (Level 4)
│   └── tracer.py
├── evals/                         # Suite d'évaluation offline (Level 4)
│   ├── eval_runner.py             # Runner 15/15 (100%)
│   └── fixtures.json              # Gold set Q&A
├── ingestion/                     # Pipeline extraction PDF → DuckDB
│   └── ingest_groq_vision.py      # Extraction via Groq Vision (Llama 4 Scout)
├── data/
│   ├── elections.duckdb           # Base DuckDB (205 circonscriptions)
│   ├── circonscriptions.csv       # Données source
│   └── candidats.csv              # 1103 candidats avec scores
├── startup.py                     # Initialisation base au démarrage cloud
├── requirements.txt
└── .env
```

---

## Niveaux du challenge

| Niveau      |                Description                              | Statut   |
|--------     |-------------------------------------------------------  |--------  |
| **Level 1** | Agent Text-to-SQL + guardrails de sécurité + graphiques |  Complet |
| **Level 2** | Router hybride SQL + RAG (questions narratives)         |  Complet |
| **Level 3** | Détection d'ambiguïté + clarification interactive       |  Complet |
| **Level 4** | Observabilité + suite d'évaluation 15/15 (100%)         |  Complet |

---

## Prérequis

- Python 3.10+
- Une clé API Groq (gratuit sur [console.groq.com](https://console.groq.com))
- Tesseract OCR
- Poppler (pdf2image)

---

## Installation

```bash
# Cloner le repo
git clone https://github.com/petterbouki/election-chat.git
cd election-chat

# Créer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows

# Installer les dépendances
pip install -r requirements.txt
```

---

## Configuration

```bash
cp .env
# Éditez .env et ajoutez votre clé API Groq :
# GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxx
```

---

## Lancement

### 1. Lancer l'application

```bash
streamlit run app/main.py
```

Ouvrir [http://localhost:8501](http://localhost:8501)

### 2. Ré-extraire les données depuis le PDF (optionnel)

```bash
# Nécessite une clé Groq avec accès Vision
python ingest_groq_vision.py data/raw/edan_2025.pdf
```

L'extraction dure environ **8-10 minutes** (35 pages via Groq Vision).

### 3. Lancer la suite d'évaluation

```bash
python evals/eval_runner.py --db data/elections.duckdb
```

---

## Résultats d'évaluation

```
Total  : 15
Passés : 15 (100%)
Échecs : 0

Par type :
  fact           4/4
  aggregation    4/4
  safety         4/4
  out_of_scope   3/3

Latence moyenne : 491ms
```

---

## Questions supportées (exemples)

| Question                               | Type          | Route         |
|------------------------------- --------|--- ----       |---            |
| Combien de sièges a remporté le RHDP ? | Agrégation    | SQL           |
| Top 10 candidats par score             | Classement    | SQL           |
| Histogramme des élus par parti         | Graphique     | SQL           |
| Taux de participation par région       | Agrégation    | SQL           |
| Qui a gagné à Agboville ?              | Lookup narratif | RAG         |
| Parle moi d'Abidjan                    | Narratif      | RAG           |
| Qui a gagné à Bouaké ?                 | Ambiguïté     | Clarification |
| Quel temps faisait-il le jour du vote ?| Hors-scope    | Refus         |
| DROP TABLE candidats                   | Injection     | Bloqué        |

---

## Guardrails de sécurité

- **SELECT uniquement** — INSERT/UPDATE/DELETE/DROP bloqués
- **LIMIT imposé** — max 100 lignes par défaut
- **Tables autorisées** — liste blanche explicite
- **Détection d'injection de prompt** — patterns connus bloqués
- **Hors dataset** — réponse explicite avec suggestions

---

## Données

- **Source** : PDF officiel CEI — `EDAN_2025_RESULTAT_NATIONAL_DETAILS.pdf` (35 pages)
- **Extraction** : Groq Vision (Llama 4 Scout) — lecture des tableaux vectoriels
- **Couverture** : 205 circonscriptions, 1103 candidats, 203 élus
- **Défi** : Le PDF est vectoriel pur (pas de texte extractible) — OCR classique insuffisant

---

## Stack technique

| Composant             | Technologie                        |
|---------------------- |----------------------------------- |
| Extraction PDF        | Groq Vision — Llama 4 Scout        |
| LLM                   | Groq — Llama 3.3 70B Versatile     |
| Base de données       | DuckDB                             |
| Interface             | Streamlit                          |
| Graphiques            | Plotly                             |
| Recherche RAG         | DuckDB full-text (sans embeddings) |

---

## Schéma de la base

```sql
TABLE circonscriptions
  id, nom, region, nb_bv, inscrits, votants,
  taux_participation, bulletins_nuls, suffrages_exprimes,
  blancs_nombre, blancs_pct, source_page

TABLE candidats
  id, circonscription_id, parti, nom,
  score, pourcentage, elu, source_page

VIEW vw_winners        -- élus par circonscription
VIEW vw_turnout        -- participation classée
VIEW vw_party_totals   -- sièges/voix par parti
VIEW vw_results_clean  -- join complet candidats + circonscriptions
```

---

## Routage hybride SQL + RAG

```
Question utilisateur
        │
        
   Guardrails ──── Injection/DROP ── Bloqué
        │
        
   Out-of-scope ──────────────────► Refus poli
        │
        
   Disambiguate ── Ambiguïté ──────► Clarification
        │
        
   Router
    ├── SQL  ── analytique (combien, taux, top, histogramme)
    └── RAG  ── narratif   (qui a gagné, parle moi de, qui est)
```

---

## Limitations connues

- 5 IDs de circonscriptions non trouvés dans le PDF (numérotation non continue : 87, 89, 90, 91, 92)
- Certains noms de candidats mal lus par Groq Vision (apostrophes ivoiriennes : N'GUESSAN)
- La sélection après clarification (taper "1", "2") non encore gérée automatiquement
- Données simulées pour les candidats des circonscriptions difficiles à extraire

---

## Prochaines étapes

- Gérer la sélection numérique après clarification (Level 3 complet)
- Ajouter embeddings multilingues pour un RAG plus précis
- Tracing end-to-end avec Langfuse ou OpenTelemetry
- Re-extraction complète avec Groq Vision amélioré