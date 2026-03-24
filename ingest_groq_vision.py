"""
ingest_groq_vision.py — Extraction PDF via Groq Vision (gratuit)
Llama 3.2 Vision lit chaque page et retourne du JSON structuré.
"""

import base64, json, sys, logging, time
import fitz
import duckdb
import pandas as pd
from groq import Groq
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

PDF_PATH = r"data\raw\edan_2025.pdf"
DB_PATH  = r"data\elections.duckdb"

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

PROMPT = """Tu regardes une page du tableau officiel des résultats électoraux 
de Côte d'Ivoire 2025 (élection des députés à l'Assemblée Nationale).

Extrais TOUTES les données du tableau et retourne UNIQUEMENT un JSON valide :

{
  "circonscriptions": [
    {
      "id": 1,
      "nom": "NOM COMPLET DE LA CIRCONSCRIPTION",
      "nb_bv": 144,
      "inscrits": 52106,
      "votants": 14070,
      "taux_participation": 27.00,
      "bulletins_nuls": 388,
      "suffrages_exprimes": 13682,
      "blancs_nombre": 76,
      "blancs_pct": 0.56,
      "candidats": [
        {
          "parti": "RHDP",
          "nom": "KOFFI AKA CHARLES",
          "score": 9078,
          "pourcentage": 66.35,
          "elu": true
        }
      ]
    }
  ]
}

Règles importantes :
- Extraire TOUTES les circonscriptions visibles sur la page
- Les élus ont ELU(E) écrit et sont sur fond vert
- Pourcentages : remplacer virgule par point (27,00 → 27.00)
- Si une valeur est illisible : mettre null
- Retourner UNIQUEMENT le JSON, rien d'autre
"""

def page_to_base64(pdf_path, page_num, dpi=150):
    doc = fitz.open(pdf_path)
    page = doc[page_num]
    mat = fitz.Matrix(dpi/72, dpi/72)
    pix = page.get_pixmap(matrix=mat)
    return base64.standard_b64encode(pix.tobytes("png")).decode("utf-8")

def extract_page(pdf_path, page_num, retries=3):
    img_b64 = page_to_base64(pdf_path, page_num, dpi=150)
    
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model="meta-llama/llama-4-scout-17b-16e-instruct",
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_b64}"
                            }
                        },
                        {
                            "type": "text",
                            "text": PROMPT
                        }
                    ]
                }],
                max_tokens=4000,
                temperature=0,
            )
            
            text = response.choices[0].message.content.strip()
            # Nettoie les backticks si présents
            text = text.replace("```json", "").replace("```", "").strip()
            
            data = json.loads(text)
            return data
            
        except json.JSONDecodeError as e:
            log.warning(f"  JSON invalide (tentative {attempt+1}): {e}")
            if attempt == retries - 1:
                return {"circonscriptions": []}
            time.sleep(2)
        except Exception as e:
            log.warning(f"  Erreur (tentative {attempt+1}): {e}")
            if attempt == retries - 1:
                return {"circonscriptions": []}
            time.sleep(5)

def load_to_duckdb(all_circs, all_cands, db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)

    con.execute("DROP TABLE IF EXISTS candidats")
    con.execute("DROP TABLE IF EXISTS circonscriptions")

    con.execute("""CREATE TABLE circonscriptions (
        id INTEGER PRIMARY KEY, nom VARCHAR, region VARCHAR DEFAULT '',
        nb_bv INTEGER, inscrits INTEGER, votants INTEGER,
        taux_participation DOUBLE, bulletins_nuls INTEGER,
        suffrages_exprimes INTEGER, blancs_nombre INTEGER,
        blancs_pct DOUBLE, source_page INTEGER)""")

    con.execute("""CREATE TABLE candidats (
        id INTEGER PRIMARY KEY, circonscription_id INTEGER,
        parti VARCHAR, nom VARCHAR, score INTEGER,
        pourcentage DOUBLE, elu BOOLEAN DEFAULT FALSE,
        source_page INTEGER)""")

    df_c = pd.DataFrame(all_circs)
    df_k = pd.DataFrame(all_cands)

    if not df_c.empty:
        con.execute("INSERT INTO circonscriptions SELECT * FROM df_c")
    if not df_k.empty:
        con.execute("INSERT INTO candidats SELECT * FROM df_k")

    # Vues
    con.execute("""CREATE OR REPLACE VIEW vw_winners AS
        SELECT c.id AS circonscription_id, c.nom AS circonscription, c.region,
               ca.nom AS candidat, ca.parti, ca.score, ca.pourcentage,
               c.inscrits, c.votants, c.taux_participation, c.source_page
        FROM circonscriptions c
        JOIN candidats ca ON ca.circonscription_id=c.id AND ca.elu=TRUE""")

    con.execute("""CREATE OR REPLACE VIEW vw_party_totals AS
        SELECT parti, COUNT(*) AS nb_candidats,
               SUM(CASE WHEN elu THEN 1 ELSE 0 END) AS sieges,
               SUM(score) AS total_voix, ROUND(AVG(pourcentage),2) AS pct_moyen
        FROM candidats GROUP BY parti ORDER BY sieges DESC""")

    con.execute("""CREATE OR REPLACE VIEW vw_turnout AS
        SELECT id, nom AS circonscription, region, inscrits, votants,
               taux_participation, suffrages_exprimes, bulletins_nuls
        FROM circonscriptions WHERE taux_participation IS NOT NULL
        ORDER BY taux_participation DESC""")

    con.execute("""CREATE OR REPLACE VIEW vw_results_clean AS
        SELECT ca.id AS candidat_id, c.id AS circonscription_id,
               c.nom AS circonscription, c.region, ca.parti,
               ca.nom AS candidat, ca.score, ca.pourcentage, ca.elu,
               c.inscrits, c.votants, c.taux_participation, c.source_page
        FROM candidats ca
        JOIN circonscriptions c ON c.id=ca.circonscription_id""")

    nb_c = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
    nb_k = con.execute("SELECT COUNT(*) FROM candidats").fetchone()[0]
    nb_e = con.execute("SELECT COUNT(*) FROM candidats WHERE elu=TRUE").fetchone()[0]
    con.close()
    return nb_c, nb_k, nb_e

def main():
    pdf = sys.argv[1] if len(sys.argv) > 1 else PDF_PATH
    db  = sys.argv[2] if len(sys.argv) > 2 else DB_PATH

    doc = fitz.open(pdf)
    nb_pages = len(doc)
    log.info(f"PDF: {nb_pages} pages")

    all_circs = []
    all_cands = []
    ids_vus   = set()
    cand_id   = 1

    for page_num in range(nb_pages):
        log.info(f"  Page {page_num+1}/{nb_pages}...")

        data = extract_page(pdf, page_num)
        circs = data.get("circonscriptions", [])
        log.info(f"    {len(circs)} circonscriptions")

        for circ in circs:
            circ_id = circ.get("id")
            if not circ_id or circ_id in ids_vus:
                continue
            ids_vus.add(circ_id)

            all_circs.append({
                "id": circ_id,
                "nom": circ.get("nom", f"CIRC_{circ_id}"),
                "region": "",
                "nb_bv": circ.get("nb_bv"),
                "inscrits": circ.get("inscrits"),
                "votants": circ.get("votants"),
                "taux_participation": circ.get("taux_participation"),
                "bulletins_nuls": circ.get("bulletins_nuls"),
                "suffrages_exprimes": circ.get("suffrages_exprimes"),
                "blancs_nombre": circ.get("blancs_nombre"),
                "blancs_pct": circ.get("blancs_pct"),
                "source_page": page_num + 1,
            })

            for cand in circ.get("candidats", []):
                all_cands.append({
                    "id": cand_id,
                    "circonscription_id": circ_id,
                    "parti": cand.get("parti", "INCONNU"),
                    "nom": cand.get("nom", ""),
                    "score": cand.get("score"),
                    "pourcentage": cand.get("pourcentage"),
                    "elu": bool(cand.get("elu", False)),
                    "source_page": page_num + 1,
                })
                cand_id += 1

        # Pause entre pages pour respecter les limites Groq
        time.sleep(3)

        # Sauvegarde intermédiaire toutes les 5 pages
        if (page_num + 1) % 5 == 0:
            ids = sorted(ids_vus)
            log.info(f"  Progression: {len(ids)} circs trouvées jusqu'ici")

    # Résumé final
    ids = sorted(ids_vus)
    print(f"\n{'='*50}")
    print(f"Circonscriptions : {len(ids)}")
    print(f"Candidats        : {len(all_cands)}")
    manquants = sorted(set(range(1, 206)) - set(ids))
    if manquants:
        print(f"Manquants ({len(manquants)}): {manquants[:20]}")
    print(f"{'='*50}")

    # Charge dans DuckDB
    nb_c, nb_k, nb_e = load_to_duckdb(all_circs, all_cands, db)
    print(f"\nBase créée : {nb_c} circs, {nb_k} candidats, {nb_e} élus")
    print(f"Fichier    : {db}")
    print("\nLance maintenant : streamlit run app\\main.py")

if __name__ == "__main__":
    main()