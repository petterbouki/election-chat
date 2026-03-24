"""
load_db.py — Création du schéma DuckDB et chargement des données extraites.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

log = logging.getLogger(__name__)

DB_PATH = "data/elections.duckdb"

# ─── DDL ─────────────────────────────────────────────────────────────────────

DDL_TABLES = """
-- Table des circonscriptions électorales
CREATE TABLE IF NOT EXISTS circonscriptions (
    id                   INTEGER PRIMARY KEY,
    nom                  VARCHAR NOT NULL,
    region               VARCHAR,
    nb_bv                INTEGER,        -- nombre de bureaux de vote
    inscrits             INTEGER,        -- électeurs inscrits
    votants              INTEGER,        -- nombre de votants
    taux_participation   DOUBLE,         -- taux en %
    bulletins_nuls       INTEGER,
    suffrages_exprimes   INTEGER,
    blancs_nombre        INTEGER,
    blancs_pct           DOUBLE,
    source_page          INTEGER
);

-- Table des candidats et leurs résultats
CREATE TABLE IF NOT EXISTS candidats (
    id                   INTEGER PRIMARY KEY,
    circonscription_id   INTEGER NOT NULL REFERENCES circonscriptions(id),
    parti                VARCHAR NOT NULL,
    nom                  VARCHAR NOT NULL,
    score                INTEGER,        -- nombre de voix
    pourcentage          DOUBLE,         -- % des suffrages exprimés
    elu                  BOOLEAN DEFAULT FALSE,
    source_page          INTEGER
);
"""

DDL_VIEWS = """
-- Vue : gagnants par circonscription
CREATE OR REPLACE VIEW vw_winners AS
SELECT
    c.id            AS circonscription_id,
    c.nom           AS circonscription,
    c.region,
    ca.nom          AS candidat,
    ca.parti,
    ca.score,
    ca.pourcentage,
    c.inscrits,
    c.votants,
    c.taux_participation,
    c.source_page
FROM circonscriptions c
JOIN candidats ca ON ca.circonscription_id = c.id AND ca.elu = TRUE;

-- Vue : taux de participation par région/circonscription
CREATE OR REPLACE VIEW vw_turnout AS
SELECT
    id,
    nom             AS circonscription,
    region,
    inscrits,
    votants,
    taux_participation,
    suffrages_exprimes,
    bulletins_nuls,
    blancs_nombre,
    source_page
FROM circonscriptions
WHERE taux_participation IS NOT NULL
ORDER BY taux_participation DESC;

-- Vue : résultats agrégés par parti
CREATE OR REPLACE VIEW vw_party_totals AS
SELECT
    parti,
    COUNT(*)                                    AS nb_candidats,
    SUM(CASE WHEN elu THEN 1 ELSE 0 END)        AS sièges,
    SUM(score)                                  AS total_voix,
    ROUND(AVG(pourcentage), 2)                  AS pct_moyen
FROM candidats
GROUP BY parti
ORDER BY sièges DESC, total_voix DESC;

-- Vue : résultats complets (join candidats + circonscriptions)
CREATE OR REPLACE VIEW vw_results_clean AS
SELECT
    ca.id            AS candidat_id,
    c.id             AS circonscription_id,
    c.nom            AS circonscription,
    c.region,
    ca.parti,
    ca.nom           AS candidat,
    ca.score,
    ca.pourcentage,
    ca.elu,
    c.inscrits,
    c.votants,
    c.taux_participation,
    c.suffrages_exprimes,
    c.source_page
FROM candidats ca
JOIN circonscriptions c ON c.id = ca.circonscription_id;
"""


# ─── Helpers de chargement ───────────────────────────────────────────────────

def create_db(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Crée (ou ouvre) la base DuckDB et applique le schéma."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(DDL_TABLES)
    con.execute(DDL_VIEWS)
    log.info(f"Base DuckDB ouverte : {db_path}")
    return con


def load_circonscriptions(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Charge le DataFrame des circonscriptions (upsert par id)."""
    con.execute("DELETE FROM candidats")        # contrainte FK : vider d'abord
    con.execute("DELETE FROM circonscriptions")
    con.execute("INSERT INTO circonscriptions SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
    log.info(f"Circonscriptions chargées : {count}")
    return count


def load_candidats(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Charge le DataFrame des candidats."""
    con.execute("INSERT INTO candidats SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM candidats").fetchone()[0]
    log.info(f"Candidats chargés : {count}")
    return count


# ─── Conversion dataclasses → DataFrames ─────────────────────────────────────

def to_dataframes(circonscriptions, candidats) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convertit les listes de dataclasses en DataFrames pandas."""
    import dataclasses

    df_circs = pd.DataFrame([dataclasses.asdict(c) for c in circonscriptions])
    df_cands = pd.DataFrame([dataclasses.asdict(c) for c in candidats])

    # Ajoute colonne id auto pour candidats
    if "id" not in df_cands.columns:
        df_cands.insert(0, "id", range(1, len(df_cands) + 1))

    return df_circs, df_cands


# ─── Vérification post-chargement ────────────────────────────────────────────

def verify_db(con: duckdb.DuckDBPyConnection) -> dict:
    """Retourne des stats de validation de la base."""
    stats = {}
    stats["nb_circonscriptions"] = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
    stats["nb_candidats"] = con.execute("SELECT COUNT(*) FROM candidats").fetchone()[0]
    stats["nb_elus"] = con.execute("SELECT COUNT(*) FROM candidats WHERE elu = TRUE").fetchone()[0]
    stats["nb_partis"] = con.execute("SELECT COUNT(DISTINCT parti) FROM candidats").fetchone()[0]
    stats["taux_moy"] = con.execute(
        "SELECT ROUND(AVG(taux_participation), 2) FROM circonscriptions WHERE taux_participation IS NOT NULL"
    ).fetchone()[0]

    top_parti = con.execute(
        "SELECT parti, SUM(CASE WHEN elu THEN 1 ELSE 0 END) AS sieges FROM candidats GROUP BY parti ORDER BY sieges DESC LIMIT 1"
    ).fetchone()
    stats["top_parti"] = top_parti[0] if top_parti else None
    stats["top_parti_sieges"] = top_parti[1] if top_parti else None

    return stats


# ─── Entrypoint standalone ───────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from extract import extract_all_pages
    from normalize import normalize_dataframe

    pdf = sys.argv[1] if len(sys.argv) > 1 else "data/raw/edan_2025.pdf"
    db = sys.argv[2] if len(sys.argv) > 2 else DB_PATH

    circs, cands = extract_all_pages(pdf)
    df_circs, df_cands = to_dataframes(circs, cands)
    df_circs, df_cands = normalize_dataframe(df_circs, df_cands)

    con = create_db(db)
    load_circonscriptions(con, df_circs)
    load_candidats(con, df_cands)

    stats = verify_db(con)
    print("\n=== Vérification base de données ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
