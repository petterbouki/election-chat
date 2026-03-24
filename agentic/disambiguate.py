"""
agentic/disambiguate.py — Détection et résolution d'ambiguïtés (Level 3).
Identifie les entités ambiguës (localités existant dans plusieurs contextes).
"""

import re
import logging
from typing import Optional

import duckdb

log = logging.getLogger(__name__)


class AmbiguityDetector:
    def __init__(self, db_path: str):
        self.con = duckdb.connect(db_path, read_only=True)
        self._locality_index = self._build_locality_index()

    def _build_locality_index(self) -> dict[str, list[dict]]:
        """Construit un index nom → liste de circonscriptions correspondantes."""
        rows = self.con.execute(
            "SELECT id, nom FROM circonscriptions ORDER BY id"
        ).fetchall()

        index: dict[str, list[dict]] = {}
        for circ_id, nom in rows:
            # Indexe par chaque mot significatif du nom (>= 4 lettres)
            words = re.findall(r"[A-ZÀÂÄÉÈÊËÎÏÔÙÛÜÇ]{4,}", nom.upper())
            for word in words:
                index.setdefault(word, []).append({"id": circ_id, "nom": nom})
        return index

    def detect(self, question: str) -> Optional[dict]:
        """Analyse la question et retourne un dict d'ambiguïté si détecté."""
    
    # Mots génériques à ne PAS utiliser pour la recherche
        GENERIC_WORDS = {
            "ville", "commune", "region", "district", "sous", "prefecture",
           "autonome", "liste", "elus", "élus", "depute", "député",
            "candidat", "parti", "score", "election", "résultat"
             }
    
        q_upper = question.upper()
        matches: dict[str, list[dict]] = {}

        for keyword, circs in self._locality_index.items():
        # Ignore les mots génériques
          if keyword.lower() in GENERIC_WORDS:
               continue
        # Ignore les mots trop courts
          if len(keyword) < 4:
              continue
          if keyword in q_upper:
              if len(circs) > 1:
                matches[keyword] = circs

        if not matches:
           return None

        key = max(matches, key=lambda k: len(matches[k]))
        circs = matches[key]

        return {
          "keyword": key,
          "matches": circs[:5],
          "count": len(circs),
       }

    def build_clarification_message(self, ambiguity: dict) -> str:
        """Génère un message de clarification pour l'utilisateur."""
        keyword = ambiguity["keyword"]
        matches = ambiguity["matches"]

        lines = [f"Plusieurs circonscriptions correspondent à **{keyword}** :"]
        for i, m in enumerate(matches, 1):
            lines.append(f"  {i}. [{m['id']:03d}] {m['nom']}")
        lines.append("\nPréciséz laquelle vous intéresse (numéro ou nom exact).")
        return "\n".join(lines)
