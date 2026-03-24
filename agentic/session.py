"""
agentic/session.py — Mémoire de session pour l'agent (Level 3).
Retient les choix de l'utilisateur pour éviter de re-demander.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SessionMemory:
    """Mémoire de session : retient les entités résolues."""
    resolved_entities: dict[str, str] = field(default_factory=dict)
    selected_circonscription: Optional[int] = None
    context_history: list[dict] = field(default_factory=list)

    def remember(self, keyword: str, resolved: str):
        """Mémorise la résolution d'une entité ambiguë."""
        self.resolved_entities[keyword.upper()] = resolved

    def recall(self, keyword: str) -> Optional[str]:
        """Rappelle la résolution mémorisée pour un mot-clé."""
        return self.resolved_entities.get(keyword.upper())

    def reset(self):
        self.resolved_entities.clear()
        self.selected_circonscription = None
        self.context_history.clear()
