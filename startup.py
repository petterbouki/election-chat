"""
startup.py — Initialise la base DuckDB depuis les CSV au démarrage.
"""
import duckdb
import pandas as pd
from pathlib import Path


def init_db(db_path: str = "data/elections.duckdb"):
    Path("data").mkdir(exist_ok=True)
    db_file = Path(db_path)

    # Vérifie si la base existe et est valide
    if db_file.exists():
        try:
            con = duckdb.connect(db_path, read_only=True)
            count = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
            con.close()
            if count > 0:
                print(f"✅ Base existante OK : {count} circs")
                return
        except:
            db_file.unlink()

    # Chemins absolus pour compatibilité cloud
    base_dir = Path(__file__).parent
    csv_circs = str(base_dir / "data" / "circonscriptions.csv")
    csv_cands = str(base_dir / "data" / "candidats.csv")

    print(f"📥 Chargement CSV depuis {base_dir}...")
    df_c = pd.read_csv(csv_circs, on_bad_lines='skip')
    df_k = pd.read_csv(csv_cands, on_bad_lines='skip')
    print(f"✔ Circonscriptions : {len(df_c)}")
    print(f"✔ Candidats : {len(df_k)}")

    if db_file.exists():
        db_file.unlink()

    con = duckdb.connect(db_path)
    try:
        con.register("df_circs", df_c)
        con.register("df_cands", df_k)

        con.execute("CREATE TABLE circonscriptions AS SELECT * FROM df_circs")
        con.execute("CREATE TABLE candidats AS SELECT * FROM df_cands")

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
        print(f"✅ Base créée : {nb_c} circs, {nb_k} candidats")

    finally:
        con.close()


if __name__ == "__main__":
    init_db()