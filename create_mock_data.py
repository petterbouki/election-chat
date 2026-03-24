"""
Crée une base DuckDB avec des données simulées fidèles au PDF CEI 2025.
205 circonscriptions, partis réels, candidats fictifs mais réalistes.
"""

import duckdb
import random
import pandas as pd
from pathlib import Path

random.seed(42)
Path("data").mkdir(exist_ok=True)

# Données réelles extraites du PDF (page 1)
REAL_DATA = [
    {"id":1,"nom":"ABOUDE, ATTOBROU, GUESSIGUIE, GRAND-MORIÉ, LOVIGUIE, ORESS-KROBOU","region":"AGNEBY-TIASSA","nb_bv":144,"inscrits":52106,"votants":14070,"taux":27.00,"nuls":388,"exprimes":13682,"blancs":76,"blancs_pct":0.56,"winner":"KOFFI AKA CHARLES","winner_parti":"RHDP","winner_score":9078,"winner_pct":66.35},
    {"id":2,"nom":"AGBOVILLE COMMUNE","region":"AGNEBY-TIASSA","nb_bv":133,"inscrits":48710,"votants":12821,"taux":26.32,"nuls":317,"exprimes":12504,"blancs":81,"blancs_pct":0.65,"winner":"DIMBA N'GOU PIERRE","winner_parti":"RHDP","winner_score":10675,"winner_pct":85.37},
    {"id":3,"nom":"AZAGUIE COMMUNE ET SOUS-PREFECTURE","region":"AGNEBY-TIASSA","nb_bv":44,"inscrits":15515,"votants":5174,"taux":33.35,"nuls":73,"exprimes":5101,"blancs":24,"blancs_pct":0.47,"winner":"ALAIN EKISSI","winner_parti":"RHDP","winner_score":1673,"winner_pct":32.80},
    {"id":4,"nom":"ANANGUIE, CECHI ET RUBINO, COMMUNES ET SOUS-PREFECTURES","region":"AGNEBY-TIASSA","nb_bv":72,"inscrits":23466,"votants":7650,"taux":32.60,"nuls":241,"exprimes":7409,"blancs":49,"blancs_pct":0.66,"winner":"KOUASSI MARIE VIRGINIE","winner_parti":"RHDP","winner_score":2607,"winner_pct":35.19},
    {"id":5,"nom":"GOMON ET SIKENSI, COMMUNES ET SOUS-PREFECTURES","region":"AGNEBY-TIASSA","nb_bv":102,"inscrits":37720,"votants":10768,"taux":28.55,"nuls":347,"exprimes":10421,"blancs":134,"blancs_pct":1.29,"winner":"N'GATA BRIE JOSEPH","winner_parti":"RHDP","winner_score":3574,"winner_pct":34.30},
]

# Régions de Côte d'Ivoire
REGIONS = [
    "AGNEBY-TIASSA","BAFING","BAGOUE","BELIER","BERE","BOUNKANI","CAVALLY",
    "DISTRICT ABIDJAN","FOLON","GBEKE","GBOKLE","GÔH","GONTOUGO","GRANDS-PONTS",
    "GUEMON","HAMBOL","HAUT-SASSANDRA","IFFOU","INDENIE-DJUABLIN","KABADOUGOU",
    "LA ME","LOH-DJIBOUA","MARAHOUE","MORONOU","NAWA","N'ZI","PORO",
    "SAN-PEDRO","SASSANDRA-MARAHOUE","SUD-BANDAMA","SUD-COMOE","TCHOLOGO",
    "TONKPI","WORODOUGOU","YAMOUSSOUKRO","ZANZAN"
]

PARTIS_DIST = [
    ("RHDP", 0.55),
    ("INDEPENDANT", 0.25),
    ("PDCI-RDA", 0.12),
    ("FPI", 0.04),
    ("ADCI", 0.02),
    ("MGC", 0.01),
    ("UDPCI", 0.01),
]

NOMS_CI = [
    "KONAN","KOUASSI","KOFFI","BAMBA","COULIBALY","DIALLO","TRAORE","OUATTARA",
    "YAPI","N'GUESSAN","GNANGO","ASSOUMOU","TAPE","ACHI","LAGO","SORO","FOFANA",
    "KONE","DOSSO","TOURE","AHOUA","EBOUE","NIAMKEY","ANOH","DIARRASSOUBA"
]
PRENOMS_CI = [
    "JEAN","MARIE","PIERRE","PAUL","CHARLES","JOSEPH","FELIX","ALBERT",
    "VICTOR","CLAUDE","HENRI","ALPHONSE","MAMADOU","MOUSSA","IBRAHIM",
    "SEKOU","FATOU","AMINATA","MARIAM","VIRGINIE","ALICE","CECILE"
]

NOMS_CIRCS = [
    "COMMUNE ET SOUS-PREFECTURE","COMMUNES ET SOUS-PREFECTURES",
    "COMMUNE","SOUS-PREFECTURE ET COMMUNE","COMMUNES ET SOUS-PREFECTURE"
]

LOCALITES = [
    "ABIDJAN","BOUAKE","DALOA","SAN-PEDRO","KORHOGO","YAMOUSSOUKRO","GAGNOA",
    "ABENGOUROU","DIVO","MAN","DUEKOUE","ODIENNE","BONDOUKOU","SOUBRE","AGBOVILLE",
    "ADZOPE","ABOISSO","TABOU","SASSANDRA","GRAND-BASSAM","LAKOTA","TIASSALE",
    "TOUMODI","MANKONO","SINFRA","BONGOUANOU","DAOUKRO","KATIOLA","FERKESSEDOUGOU",
    "BOUNDIALI","TENGRELA","TANDA","NASSIAN","BOUNA","DOROPO","TIAPOUM","GRAND-LAHOU",
    "FRESCO","JACQUEVILLE","ABOBO","ADJAME","COCODY","MARCORY","PLATEAU","PORT-BOUET",
    "TREICHVILLE","YOPOUGON","ATTIECOUBE","BINGERVILLE","DABOU","JACQUEVILLE"
]

def rand_nom():
    return f"{random.choice(NOMS_CI)} {random.choice(PRENOMS_CI)}"

def rand_parti():
    r = random.random()
    cumul = 0
    for parti, prob in PARTIS_DIST:
        cumul += prob
        if r <= cumul:
            return parti
    return "INDEPENDANT"

# Génère 205 circonscriptions
circs = []
region_idx = 0

for i in range(1, 206):
    # Utilise les vraies données pour les 5 premières
    if i <= 5:
        r = REAL_DATA[i-1]
        circs.append({
            "id": r["id"],
            "nom": r["nom"],
            "region": r["region"],
            "nb_bv": r["nb_bv"],
            "inscrits": r["inscrits"],
            "votants": r["votants"],
            "taux_participation": r["taux"],
            "bulletins_nuls": r["nuls"],
            "suffrages_exprimes": r["exprimes"],
            "blancs_nombre": r["blancs"],
            "blancs_pct": r["blancs_pct"],
            "source_page": 1,
        })
    else:
        # Génère des données réalistes
        region = REGIONS[region_idx % len(REGIONS)]
        localite = random.choice(LOCALITES)
        suffixe = random.choice(NOMS_CIRCS)
        nom = f"{localite}, {suffixe}"

        inscrits = random.randint(8000, 120000)
        taux = round(random.uniform(20, 70), 2)
        votants = int(inscrits * taux / 100)
        nuls = int(votants * random.uniform(0.005, 0.025))
        exprimes = votants - nuls
        blancs = int(votants * random.uniform(0.003, 0.015))
        blancs_pct = round(blancs / votants * 100, 2) if votants > 0 else 0
        nb_bv = int(inscrits / random.randint(200, 500))
        page = (i // 6) + 1

        circs.append({
            "id": i,
            "nom": nom,
            "region": region,
            "nb_bv": nb_bv,
            "inscrits": inscrits,
            "votants": votants,
            "taux_participation": taux,
            "bulletins_nuls": nuls,
            "suffrages_exprimes": exprimes,
            "blancs_nombre": blancs,
            "blancs_pct": blancs_pct,
            "source_page": page,
        })
        region_idx += 1

# Génère les candidats
cands = []
cand_id = 1

for circ in circs:
    circ_id = circ["id"]
    exprimes = circ["suffrages_exprimes"] or 10000
    page = circ["source_page"]

    # Vraies données pour les 5 premières circs
    if circ_id <= 5:
        rd = REAL_DATA[circ_id - 1]
        # Élu
        cands.append({
            "id": cand_id, "circonscription_id": circ_id,
            "parti": rd["winner_parti"], "nom": rd["winner"],
            "score": rd["winner_score"], "pourcentage": rd["winner_pct"],
            "elu": True, "source_page": page
        })
        cand_id += 1
        # Quelques autres candidats réels
        autres = {
            1: [("INDEPENDANT","TCHIMOU GNAMON BERTRAND",1991,14.55),("INDEPENDANT","KOTO EHOU SOPIE",547,4.00),("ADCI","EDI DOFFOU PAUL",331,2.42),("FPI","N'GUESSAN KOTCHI REMI",474,3.46)],
            2: [("INDEPENDANT","OCHO KOKOU BERTRAND",20,0.16),("PDCI-RDA","OHOUNA N'TAKPE NICAISE",1327,10.61),("ADCI","OCHOU WROHOUM MARIE-PASCALE",296,2.37)],
            3: [("INDEPENDANT","BAMBA IDRISSA",256,5.02),("PDCI-RDA","EKISSI HUBERSON EVARISTHO ALVARIS",163,3.20),("INDEPENDANT","KOUAME YAO FREDERIC",439,8.61)],
            4: [("INDEPENDANT","ATIN ERIC ALAIN",355,4.79),("PDCI-RDA","N'GUESSAN AKA ARNAUD",1211,16.34),("INDEPENDANT","OFFO ABOLE SYLVAIN",827,11.16)],
            5: [("INDEPENDANT","SAHORE N'GUESSAN PASCAL",157,1.51),("INDEPENDANT","ADANGBA KASSA-KASSA RAPHAEL",32,0.31)],
        }
        for parti, nom, score, pct in autres.get(circ_id, []):
            cands.append({"id":cand_id,"circonscription_id":circ_id,"parti":parti,"nom":nom,"score":score,"pourcentage":pct,"elu":False,"source_page":page})
            cand_id += 1
    else:
        # Génère entre 3 et 8 candidats
        nb_cands = random.randint(3, 8)
        scores_bruts = [random.randint(100, exprimes//2) for _ in range(nb_cands)]
        total = sum(scores_bruts)
        # Normalise pour que ça fasse ~exprimes
        scores = [int(s * exprimes / total) for s in scores_bruts]
        pcts = [round(s / exprimes * 100, 2) for s in scores]

        # Le gagnant
        winner_idx = scores.index(max(scores))
        winner_parti = rand_parti()

        for j in range(nb_cands):
            parti = winner_parti if j == winner_idx else rand_parti()
            nom = rand_nom()
            cands.append({
                "id": cand_id,
                "circonscription_id": circ_id,
                "parti": parti,
                "nom": nom,
                "score": scores[j],
                "pourcentage": pcts[j],
                "elu": (j == winner_idx),
                "source_page": page,
            })
            cand_id += 1

# Charge dans DuckDB
Path("data").mkdir(exist_ok=True)
con = duckdb.connect("data/elections.duckdb")

con.execute("DROP TABLE IF EXISTS candidats")
con.execute("DROP TABLE IF EXISTS circonscriptions")

con.execute("""CREATE TABLE circonscriptions (
    id INTEGER PRIMARY KEY, nom VARCHAR, region VARCHAR,
    nb_bv INTEGER, inscrits INTEGER, votants INTEGER,
    taux_participation DOUBLE, bulletins_nuls INTEGER,
    suffrages_exprimes INTEGER, blancs_nombre INTEGER,
    blancs_pct DOUBLE, source_page INTEGER)""")

con.execute("""CREATE TABLE candidats (
    id INTEGER PRIMARY KEY, circonscription_id INTEGER,
    parti VARCHAR, nom VARCHAR, score INTEGER,
    pourcentage DOUBLE, elu BOOLEAN, source_page INTEGER)""")

df_c = pd.DataFrame(circs)
df_k = pd.DataFrame(cands)
con.execute("INSERT INTO circonscriptions SELECT * FROM df_c")
con.execute("INSERT INTO candidats SELECT * FROM df_k")

# Vues
con.execute("""CREATE OR REPLACE VIEW vw_winners AS
    SELECT c.id AS circonscription_id, c.nom AS circonscription, c.region,
           ca.nom AS candidat, ca.parti, ca.score, ca.pourcentage,
           c.inscrits, c.votants, c.taux_participation, c.source_page
    FROM circonscriptions c JOIN candidats ca ON ca.circonscription_id=c.id AND ca.elu=TRUE""")

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
    FROM candidats ca JOIN circonscriptions c ON c.id=ca.circonscription_id""")

# Stats
nb_c = con.execute("SELECT COUNT(*) FROM circonscriptions").fetchone()[0]
nb_k = con.execute("SELECT COUNT(*) FROM candidats").fetchone()[0]
nb_e = con.execute("SELECT COUNT(*) FROM candidats WHERE elu=TRUE").fetchone()[0]

print(f"Base créée avec succès !")
print(f"  Circonscriptions : {nb_c}")
print(f"  Candidats        : {nb_k}")
print(f"  Elus             : {nb_e}")

# Test query
print("\nTop 5 partis par sièges :")
rows = con.execute("SELECT parti, sieges, total_voix FROM vw_party_totals LIMIT 5").fetchall()
for r in rows:
    print(f"  {r[0]:15s} : {r[1]} sièges, {r[2]} voix")

print("\nTaux de participation (top 5) :")
rows = con.execute("SELECT circonscription, taux_participation FROM vw_turnout LIMIT 5").fetchall()
for r in rows:
    print(f"  {r[0][:40]:40s} : {r[1]}%")

con.close()
print(f"\nFichier : data/elections.duckdb")