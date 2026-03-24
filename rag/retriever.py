"""
rag/retriever.py — Recherche textuelle dans DuckDB sans embeddings.
"""

import re
import logging
from typing import Optional
import duckdb

log = logging.getLogger(__name__)


class ElectionRetriever:

    def __init__(self, db_path: str):
        self.con = duckdb.connect(db_path, read_only=True)

    def search(self, question: str, top_k: int = 5) -> list[dict]:
        results = []
        entities = self._extract_entities(question)
        log.debug(f"Entités: {entities}")

        for entity in entities:
            results.extend(self._search_circonscriptions(entity))
            results.extend(self._search_candidats(entity))
            if len(entity) >= 3:
                results.extend(self._search_partis(entity))

        # Déduplique
        seen, unique = set(), []
        for r in results:
            key = f"{r['type']}_{r.get('id', r.get('nom', ''))}"
            if key not in seen:
                seen.add(key)
                unique.append(r)
        return unique[:top_k]

    def _extract_entities(self, question: str) -> list[str]:
      stop_words = {
        "qui", "que", "quoi", "quel", "quelle", "est", "sont",
        "dans", "pour", "avec", "les", "des", "une", "par", "sur",
        "comment", "combien", "parle", "moi", "de", "du", "le", "la",
        "et", "ou", "gagné", "remporté", "élu", "candidat", "parti",
        "score", "elus", "district", "autonome",
        }
      extra_stop = {
        "ville", "commune", "sous", "prefecture", "liste",
        "prefectures", "prefect",
       }

      entities = []

    # Séquences en majuscules
      upper_sequences = re.findall(
        r"[A-ZÀÂÄÉÈÊËÎÏÔÙÛÜÇ][A-ZÀÂÄÉÈÊËÎÏÔÙÛÜÇa-zàâäéèêëîïôùûüç'\-]+"
        r"(?:\s+[A-ZÀÂÄÉÈÊËÎÏÔÙÛÜÇ][A-ZÀÂÄÉÈÊËÎÏÔÙÛÜÇa-zàâäéèêëîïôùûüç'\-]+)*",
        question
       )
      for seq in upper_sequences:
        if seq.lower() not in stop_words and seq.lower() not in extra_stop and len(seq) >= 3:
            entities.append(seq)

    # Mots simples
      for w in re.findall(r"[A-Za-zÀ-ÿ'\-]{4,}", question):
        if w.lower() not in stop_words and w.lower() not in extra_stop and w not in entities:
            entities.append(w)

    # Villes importantes
      q_lower = question.lower()
      city_map = {
        "abidjan": ["ABIDJAN", "ABOBO", "ADJAME", "COCODY", "PLATEAU"],
        "bouake": ["BOUAKE"], "bouaké": ["BOUAKE"],
        "yamoussoukro": ["YAMOUSSOUKRO"],
        "korhogo": ["KORHOGO"],
        "daloa": ["DALOA"],
        "agboville": ["AGBOVILLE"],
        "bingerville": ["BINGERVILLE"],
      }
      for city, keywords in city_map.items():
        if city in q_lower:
            for k in keywords:
                if k not in entities:
                    entities.append(k)

      return entities[:6]
    def _search_circonscriptions(self, query: str) -> list[dict]:
        try:
            rows = self.con.execute("""
                SELECT c.id, c.nom, c.region, c.inscrits, c.votants,
                       c.taux_participation, c.suffrages_exprimes
                FROM circonscriptions c
                WHERE c.nom ILIKE ? OR c.region ILIKE ?
                LIMIT 5
            """, [f"%{query}%", f"%{query}%"]).fetchall()

            results = []
            for r in rows:
                elu = self.con.execute("""
                    SELECT ca.nom, ca.parti, ca.score, ca.pourcentage
                    FROM candidats ca
                    WHERE ca.circonscription_id = ? AND ca.elu = TRUE
                    LIMIT 1
                """, [r[0]]).fetchone()

                results.append({
                    "type": "circonscription",
                    "id": r[0], "nom": r[1], "region": r[2],
                    "inscrits": r[3], "votants": r[4],
                    "taux_participation": r[5], "suffrages_exprimes": r[6],
                    "elu_nom": elu[0] if elu else None,
                    "elu_parti": elu[1] if elu else None,
                    "elu_score": elu[2] if elu else None,
                    "elu_pct": elu[3] if elu else None,
                })
            return results
        except Exception as e:
            log.warning(f"Erreur circ '{query}': {e}")
            return []

    def _search_candidats(self, query: str) -> list[dict]:
        try:
            rows = self.con.execute("""
                SELECT ca.id, ca.nom, ca.parti, ca.score, ca.pourcentage,
                       ca.elu, c.nom AS circ, c.region
                FROM candidats ca
                JOIN circonscriptions c ON c.id = ca.circonscription_id
                WHERE ca.nom ILIKE ?
                ORDER BY ca.score DESC NULLS LAST
                LIMIT 3
            """, [f"%{query}%"]).fetchall()

            return [{
                "type": "candidat", "id": r[0], "nom": r[1], "parti": r[2],
                "score": r[3], "pourcentage": r[4], "elu": r[5],
                "circonscription": r[6], "region": r[7],
            } for r in rows]
        except Exception as e:
            log.warning(f"Erreur candidat '{query}': {e}")
            return []

    def _search_partis(self, query: str) -> list[dict]:
        try:
            row = self.con.execute("""
                SELECT parti, nb_candidats, sieges, total_voix, pct_moyen
                FROM vw_party_totals WHERE parti ILIKE ? LIMIT 1
            """, [f"%{query}%"]).fetchone()
            if row:
                return [{"type": "parti", "nom": row[0], "nb_candidats": row[1],
                         "sieges": row[2], "total_voix": row[3], "pct_moyen": row[4]}]
            return []
        except Exception as e:
            log.warning(f"Erreur parti '{query}': {e}")
            return []

    def format_context(self, results: list[dict]) -> str:
        """Formate les résultats en phrases narratives claires."""
        if not results:
            return ""

        phrases = []
        for r in results:
            if r["type"] == "circonscription":
                phrase = (
                    f"La circonscription {r['nom']} (région {r['region'] or 'non précisée'}) "
                    f"compte {r['inscrits']} électeurs inscrits, "
                    f"{r['votants']} votants soit un taux de {r['taux_participation']}%."
                )
                if r.get("elu_nom"):
                    phrase += (
                        f" Le député élu est {r['elu_nom']} ({r['elu_parti']}) "
                        f"avec {r['elu_score']} voix ({r['elu_pct']}%)."
                    )
                phrases.append(phrase)

            elif r["type"] == "candidat":
                statut = "a été élu(e) député(e)" if r["elu"] else "était candidat(e)"
                phrases.append(
                    f"{r['nom']} ({r['parti']}) {statut} dans {r['circonscription']} "
                    f"avec {r['score']} voix ({r['pourcentage']}%)."
                )

            elif r["type"] == "parti":
                phrases.append(
                    f"Le parti {r['nom']} a remporté {r['sieges']} sièges "
                    f"avec {r['nb_candidats']} candidats et {r['total_voix']} voix au total."
                )

        return "\n".join(phrases)