"""
startup.py — Initialise la base DuckDB depuis les CSV au démarrage.
"""
import duckdb
import pandas as pd
from pathlib import Path


def init_db(db_path: str = "data/elections.duckdb"):
    Path("data").mkdir(exist_ok=True)

    print(" Chargement des CSV...")
    df_c = pd.read_csv("data/circonscriptions.csv", on_bad_lines='skip')
    df_k = pd.read_csv("data/candidats.csv", on_bad_lines='skip')
    print(f" Circonscriptions : {len(df_c)} lignes")
    print(f" Candidats : {len(df_k)} lignes")

    # Supprime et recrée proprement
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()

    con = duckdb.connect(db_path)

    try:
        con.execute("DROP TABLE IF EXISTS candidats")
        con.execute("DROP TABLE IF EXISTS circonscriptions")
        con.execute("DROP VIEW IF EXISTS vw_winners")
        con.execute("DROP VIEW IF EXISTS vw_party_totals")
        con.execute("DROP VIEW IF EXISTS vw_turnout")
        con.execute("DROP VIEW IF EXISTS vw_results_clean")

        con.execute("CREATE TABLE circonscriptions AS SELECT * FROM df_c")
        con.execute("CREATE TABLE candidats AS SELECT * FROM df_k")

        con.execute("""CREATE VIEW vw_winners AS
            SELECT c.id AS circonscription_id, c.nom AS circonscription, c.region,
                   ca.nom AS candidat, ca.parti, ca.score, ca.pourcentage,
                   c.inscrits, c.votants, c.taux_participation, c.source_page
            FROM circonscriptions c JOIN candidats ca
            ON ca.circonscription_id=c.id AND ca.elu=TRUE""")

        con.execute("""CREATE VIEW vw_party_totals AS
            SELECT parti, COUNT(*) AS nb_candidats,
                   SUM(CASE WHEN elu THEN 1 ELSE 0 END) AS sieges,
                   SUM(score) AS total_voix, ROUND(AVG(pourcentage),2) AS pct_moyen
            FROM candidats GROUP BY parti ORDER BY sieges DESC""")

        con.execute("""CREATE VIEW vw_turnout AS
            SELECT id, nom AS circonscription, region, inscrits, votants,
                   taux_participation, suffrages_exprimes, bulletins_nuls
            FROM circonscriptions WHERE taux_participation IS NOT NULL
            ORDER BY taux_participation DESC""")

        con.execute("""CREATE VIEW vw_results_clean AS
            SELECT ca.id AS candidat_id, c.id AS circonscription_id,
                   c.nom AS circonscription, c.region, ca.parti,
                   ca.nom AS candidat, ca.score, ca.pourcentage, ca.elu,
                   c.inscrits, c.votants, c.taux_participation, c.source_page
            FROM candidats ca JOIN circonscriptions c ON c.id=ca.circonscription_id""")

        print(f" Base créée : {db_path}")

    except Exception as e:
        print(f" Erreur : {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    init_db()