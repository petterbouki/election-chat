"""
rag/router.py — Router hybride SQL vs RAG (Level 2).
Décide quelle stratégie utiliser selon l'intention détectée.
"""

import re
import logging

log = logging.getLogger(__name__)


class RouteDecision:
    SQL = "sql"
    RAG = "rag"


# Questions analytiques → SQL
SQL_PATTERNS = [
    re.compile(r"\bcombien\b", re.I),
    re.compile(r"\btotal\b|\bsomme\b|\bmoyenne\b", re.I),
    re.compile(r"\btop\s+\d+\b", re.I),
    re.compile(r"\bclassement\b|\brang\b", re.I),
    re.compile(r"\bhistogramme\b|\bgraphique\b|\bdiagramme\b|\bcamembert\b", re.I),
    re.compile(r"\btaux\b.*\bparticipation\b", re.I),
    re.compile(r"\bsiège[s]?\b|\bsiege[s]?\b", re.I),
    re.compile(r"\bpourcentage\b|\b%\b", re.I),
    re.compile(r"\bplus\s+(fort|élevé|grand|faible)\b", re.I),
    re.compile(r"\bmeilleur\b|\bpremier\b|\bdernière?\b", re.I),
    re.compile(r"\bcompare[r]?\b|\bvsb\b", re.I),
]

# Questions narratives/floues → RAG
RAG_PATTERNS = [
    re.compile(r"\bqui\s+a\s+(gagné|remporté|élu|été\s+élu)\b", re.I),
    re.compile(r"\bqui\s+est\b", re.I),
    re.compile(r"\bparle[r]?\s*(moi\s+)?d[e']?\b", re.I),
    re.compile(r"\bdis[- ]moi\b", re.I),
    re.compile(r"\bquel\s+(candidat|parti|élu)\b", re.I),
    re.compile(r"\bcomment\b", re.I),
    re.compile(r"\bdécri[sv]\b", re.I),
    re.compile(r"\btrouver?\b|\bcherche[r]?\b", re.I),
    re.compile(r"\bprésente[r]?\b|\binforme[r]?\b", re.I),
    re.compile(r"\bprofil\b|\bbiographie\b", re.I),
]


def route(question: str) -> str:
    """
    Retourne RouteDecision.SQL ou RouteDecision.RAG.
    SQL pour analytique, RAG pour narratif/flou.
    """
    sql_score = sum(1 for p in SQL_PATTERNS if p.search(question))
    rag_score = sum(1 for p in RAG_PATTERNS if p.search(question))

    # SQL gagne en cas d'égalité (plus fiable)
    decision = RouteDecision.RAG if rag_score > sql_score else RouteDecision.SQL

    log.debug(f"Router: sql={sql_score}, rag={rag_score} → {decision}")
    return decision


if __name__ == "__main__":
    tests = [
        ("Combien de sièges a le RHDP ?", "sql"),
        ("Qui a gagné à Agboville ?", "rag"),
        ("Top 10 candidats par score", "sql"),
        ("Parle-moi de KOFFI AKA CHARLES", "rag"),
        ("Taux de participation par région", "sql"),
        ("Qui est le candidat DIMBA N'GOU PIERRE ?", "rag"),
        ("Histogramme des élus par parti", "sql"),
        ("Quel candidat a gagné à Bouaké ?", "rag"),
    ]
    print("=== Test du router ===")
    for question, expected in tests:
        result = route(question)
        status = "✓" if result == expected else "✗"
        print(f"  [{status}] {result:4s} ← {question}")