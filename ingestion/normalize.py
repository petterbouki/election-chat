"""
normalize.py — Normalisation des entités (accents, casse, alias partis/lieux)
Utilisé après extraction OCR pour uniformiser les données.
"""

import re
import unicodedata
from typing import Optional


# ─── Normalisation texte général ─────────────────────────────────────────────

def strip_accents(s: str) -> str:
    """Supprime les accents : 'Côte' → 'Cote'."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(s: str) -> str:
    """Minuscule + sans accents + espaces normalisés."""
    return re.sub(r"\s+", " ", strip_accents(s).lower().strip())


def title_case_fr(s: str) -> str:
    """Met en forme 'DUPONT JEAN MARIE' → 'Dupont Jean Marie'."""
    mots_minuscules = {"de", "du", "des", "le", "la", "les", "et", "en", "au", "aux", "d", "l"}
    parts = s.lower().split()
    result = []
    for i, p in enumerate(parts):
        result.append(p if (i > 0 and p in mots_minuscules) else p.capitalize())
    return " ".join(result)


# ─── Alias de partis ──────────────────────────────────────────────────────────

PARTY_ALIASES: dict[str, str] = {
    # RHDP variantes
    "rhdp": "RHDP",
    "r.h.d.p": "RHDP",
    "r h d p": "RHDP",
    "rassemblement des houphouetistes pour la democratie et la paix": "RHDP",
    "ecs": "RHDP",  # ECS = liste RHDP dans certains contextes
    # PDCI variantes
    "pdci": "PDCI-RDA",
    "pdci-rda": "PDCI-RDA",
    "p.d.c.i": "PDCI-RDA",
    "pdci rda": "PDCI-RDA",
    "parti democratique de cote d ivoire": "PDCI-RDA",
    # FPI
    "fpi": "FPI",
    "front populaire ivoirien": "FPI",
    # Indépendant
    "independant": "INDEPENDANT",
    "indépendant": "INDEPENDANT",
    "ind.": "INDEPENDANT",
    # Autres
    "adci": "ADCI",
    "mgc": "MGC",
    "udpci": "UDPCI",
    "gjpa-ci": "GJPA-CI",
    "gjpa ci": "GJPA-CI",
    "ppa-ci": "PPA-CI",
    "ppa ci": "PPA-CI",
}


def normalize_party(raw: str) -> str:
    """Normalise un nom de parti vers sa forme canonique."""
    key = normalize_text(raw)
    return PARTY_ALIASES.get(key, raw.upper().strip())


# ─── Alias de localités ───────────────────────────────────────────────────────

LOCALITY_ALIASES: dict[str, str] = {
    "abidjan": "ABIDJAN",
    "abijan": "ABIDJAN",
    "tiapoum": "TIAPOUM",
    "tiapum": "TIAPOUM",
    "tiapoume": "TIAPOUM",
    "grand bassam": "GRAND-BASSAM",
    "grand-bassam": "GRAND-BASSAM",
    "cote d ivoire": "CÔTE D'IVOIRE",
    "cote divoire": "CÔTE D'IVOIRE",
    "bouake": "BOUAKÉ",
    "bouaké": "BOUAKÉ",
    "korhogo": "KORHOGO",
    "yamoussoukro": "YAMOUSSOUKRO",
    "san pedro": "SAN-PÉDRO",
    "san-pedro": "SAN-PÉDRO",
    "daloa": "DALOA",
    "agboville": "AGBOVILLE",
    "gagnoa": "GAGNOA",
    "divo": "DIVO",
    "man": "MAN",
    "odienne": "ODIENNÉ",
    "odienné": "ODIENNÉ",
}


def normalize_locality(raw: str) -> str:
    """Normalise un nom de localité (gère typos et accents manquants)."""
    key = normalize_text(raw)
    # Cherche correspondance exacte
    if key in LOCALITY_ALIASES:
        return LOCALITY_ALIASES[key]
    # Recherche floue simple : substring
    for alias_key, canonical in LOCALITY_ALIASES.items():
        if alias_key in key or key in alias_key:
            return canonical
    return raw.strip().upper()


# ─── Fuzzy matching (typos) ───────────────────────────────────────────────────

def levenshtein(a: str, b: str) -> int:
    """Distance de Levenshtein entre deux chaînes."""
    if len(a) < len(b):
        return levenshtein(b, a)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
        prev = curr
    return prev[-1]


def find_best_locality_match(query: str, known_localities: list[str], threshold: int = 3) -> Optional[str]:
    """
    Trouve la localité la plus proche par Levenshtein.
    Retourne None si la distance dépasse le seuil.
    """
    q = normalize_text(query)
    best = None
    best_dist = threshold + 1
    for loc in known_localities:
        d = levenshtein(q, normalize_text(loc))
        if d < best_dist:
            best_dist = d
            best = loc
    return best if best_dist <= threshold else None


# ─── Nettoyage des artefacts OCR ─────────────────────────────────────────────

OCR_ARTIFACTS = re.compile(r"[|\\/_]{2,}|^\W+|\W+$")
MULTI_SPACES = re.compile(r"\s{2,}")


def clean_ocr_name(raw: str) -> str:
    """Nettoie les artefacts OCR dans un nom de personne ou lieu."""
    s = OCR_ARTIFACTS.sub(" ", raw)
    s = MULTI_SPACES.sub(" ", s)
    return s.strip()


def normalize_candidate_name(raw: str) -> str:
    """
    Normalise un nom de candidat OCR.
    Ex: "N'GUESSAN AKA ARNAUD" → conservé en majuscules (noms propres CI)
    """
    s = clean_ocr_name(raw)
    # Garde les majuscules pour noms ivoiriens (convention locale)
    return re.sub(r"\s+", " ", s).strip().upper()


# ─── Normalisation d'un DataFrame complet ────────────────────────────────────

def normalize_dataframe(df_circs, df_cands):
    """
    Applique toutes les normalisations sur les DataFrames pandas.
    Appelé par load_db.py après extraction.
    """
    import pandas as pd

    # Circonscriptions
    df_circs = df_circs.copy()
    df_circs["nom"] = df_circs["nom"].apply(lambda x: re.sub(r"\s+", " ", str(x)).strip().upper())

    # Candidats
    df_cands = df_cands.copy()
    df_cands["parti"] = df_cands["parti"].apply(normalize_party)
    df_cands["nom"] = df_cands["nom"].apply(normalize_candidate_name)

    return df_circs, df_cands


# ─── Tests rapides ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests_partis = ["rhdp", "R.H.D.P", "PDCI", "pdci rda", "independant", "INDÉPENDANT", "ECS"]
    print("=== Normalisation partis ===")
    for t in tests_partis:
        print(f"  '{t}' → '{normalize_party(t)}'")

    tests_locs = ["Tiapum", "grand bassam", "Cote d Ivoire", "bouaké", "ABIJAN"]
    print("\n=== Normalisation localités ===")
    for t in tests_locs:
        print(f"  '{t}' → '{normalize_locality(t)}'")

    print("\n=== Fuzzy matching ===")
    locs = ["TIAPOUM", "ABIDJAN", "GRAND-BASSAM", "BOUAKÉ"]
    for query in ["Tiapoume", "Abidjane", "Grand Bassam"]:
        match = find_best_locality_match(query, locs)
        print(f"  '{query}' → '{match}'")
