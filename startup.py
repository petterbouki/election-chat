"""
startup.py — Initialise la base DuckDB depuis les CSV.
"""
import duckdb
import pandas as pd
from pathlib import Path
import time


def init_db(db_path: str = "data/elections.duckdb"):
    Path("data").mkdir(exist_ok=True)
    
    lock_file = Path("data/.db_lock")
    db_file = Path(db_path)
    
    # Attend si un autre processus crée la base
    timeout = 30
    waited = 0
    while lock_file.exists() and waited < timeout:
        time.sleep(1)
        waited += 1
    
    # Si la base existe déjà et est valide, on sort
    if db_file.exists():
        try:
            con = duckdb.connect(db_path, read_only=True)
            count = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
            con.close()
            if count > 0:
                print(f"✅ Base existante OK : {count} circs")
                return
        except:
            pass
    
    # Pose le verrou
    lock_file.touch()
    
    try:
        print("📥 Chargement des CSV...")
        df_c = pd.read_csv("data/circonscriptions.csv", on_bad_lines='skip')
        df_k = pd.read_csv("data/candidats.csv", on_bad_lines='skip')
        print(f"✔ Circonscriptions : {len(df_c)}")
        print(f"✔ Candidats : {len(df_k)}")

        # Supprime l'ancienne base
        if db_file.exists():
            db_file.unlink()

        # Crée via fichier temporaire CSV → import direct
        con = duckdb.connect(db_path)
        
        # Import direct depuis les fichiers CSV (plus fiable sur cloud)
        con.execute(f"""
            CREATE TABLE circonscriptions AS 
            SELECT * FROM read_csv_auto('data/circonscriptions.csv', 
                                         ignore_errors=true)
        """)
        con.execute(f"""
            CREATE TABLE candidats AS 
            SELECT * FROM read_csv_auto('data/candidats.csv',
                                         ignore_errors=true)
        """)

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

        nb_c = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
        nb_k = con.execute("SELECT COUNT(*) FROM candidats").fetchone()[0]
        con.close()
        print(f"✅ Base créée : {nb_c} circs, {nb_k} candidats")

    finally:
        # Supprime le verrou
        if lock_file.exists():
            lock_file.unlink()


if __name__ == "__main__":
    init_db()