"""
fix_missing.py — Récupère les 7 circonscriptions manquantes
"""
import base64, json, time, logging
import fitz, duckdb, pandas as pd
from groq import Groq
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
PDF_PATH = r"data\raw\edan_2025.pdf"
DB_PATH  = r"data\elections.duckdb"

MANQUANTS = [67, 68, 87, 89, 90, 91, 92]

PROMPT = """Cette page contient des résultats électoraux ivoiriens 2025.
Je cherche SPÉCIFIQUEMENT les circonscriptions avec ces IDs : {ids}

Retourne UNIQUEMENT ce JSON (rien d'autre) :
{{
  "circonscriptions": [
    {{
      "id": 67,
      "nom": "NOM DE LA CIRCONSCRIPTION",
      "nb_bv": 100,
      "inscrits": 30000,
      "votants": 12000,
      "taux_participation": 40.00,
      "bulletins_nuls": 300,
      "suffrages_exprimes": 11700,
      "blancs_nombre": 50,
      "blancs_pct": 0.42,
      "candidats": [
        {{
          "parti": "RHDP",
          "nom": "NOM CANDIDAT",
          "score": 6000,
          "pourcentage": 51.28,
          "elu": true
        }}
      ]
    }}
  ]
}}
"""

def page_to_b64(pdf_path, page_num, dpi=180):
    doc = fitz.open(pdf_path)
    pix = doc[page_num].get_pixmap(matrix=fitz.Matrix(dpi/72, dpi/72))
    return base64.standard_b64encode(pix.tobytes("png")).decode("utf-8")

def extract_page(pdf_path, page_num, target_ids):
    img = page_to_b64(pdf_path, page_num)
    for attempt in range(3):
        try:
            r = client.chat.completions.create(
                model="meta-llama/llama-4-scout-17b-16e-instruct",
                messages=[{"role":"user","content":[
                    {"type":"image_url","image_url":{"url":f"data:image/png;base64,{img}"}},
                    {"type":"text","text":PROMPT.format(ids=target_ids)}
                ]}],
                max_tokens=4000, temperature=0,
            )
            text = r.choices[0].message.content.strip()
            text = text.replace("```json","").replace("```","").strip()
            return json.loads(text)
        except Exception as e:
            log.warning(f"  Tentative {attempt+1} échouée: {e}")
            time.sleep(3)
    return {"circonscriptions": []}

def main():
    con = duckdb.connect(DB_PATH)
    
    # Pages à rescanner (page 12 = index 11, pages autour)
    pages_a_scanner = [11, 12, 13]  # pages 12, 13, 14 du PDF
    
    ids_restants = set(MANQUANTS)
    all_circs, all_cands = [], []
    cand_id = con.execute("SELECT MAX(id) FROM candidats").fetchone()[0] + 1

    for page_num in pages_a_scanner:
        if not ids_restants:
            break
        log.info(f"Scan page {page_num+1}, cherche IDs: {sorted(ids_restants)}")
        data = extract_page(PDF_PATH, page_num, sorted(ids_restants))
        
        for circ in data.get("circonscriptions", []):
            circ_id = circ.get("id")
            if circ_id not in ids_restants:
                continue
            
            log.info(f"  Trouvé circ {circ_id}: {circ.get('nom','?')}")
            ids_restants.discard(circ_id)

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
                all_circs_ids = {c["id"] for c in all_circs}
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
        
        time.sleep(3)

    # Insère dans la base
    if all_circs:
        df_c = pd.DataFrame(all_circs)
        df_k = pd.DataFrame(all_cands)
        con.execute("INSERT INTO circonscriptions SELECT * FROM df_c")
        if not df_k.empty:
            con.execute("INSERT INTO candidats SELECT * FROM df_k")

    nb_c = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
    nb_k = con.execute("SELECT COUNT(*) FROM candidats").fetchone()[0]
    
    print(f"\nBase mise à jour : {nb_c} circs, {nb_k} candidats")
    
    if ids_restants:
        print(f"Encore manquants: {sorted(ids_restants)}")
    else:
        print("Toutes les circonscriptions sont récupérées !")
    
    con.close()

if __name__ == "__main__":
    main()