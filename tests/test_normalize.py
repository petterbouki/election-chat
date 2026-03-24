"""
tests/test_normalize.py — Tests de normalisation des entités
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.normalize import (
    normalize_party,
    normalize_locality,
    normalize_candidate_name,
    find_best_locality_match,
    strip_accents,
)


class TestNormalizeParty:
    def test_rhdp_lowercase(self):
        assert normalize_party("rhdp") == "RHDP"

    def test_rhdp_avec_points(self):
        assert normalize_party("R.H.D.P") == "RHDP"

    def test_pdci_court(self):
        assert normalize_party("PDCI") == "PDCI-RDA"

    def test_independant_accent(self):
        assert normalize_party("INDÉPENDANT") == "INDEPENDANT"

    def test_independant_sans_accent(self):
        assert normalize_party("independant") == "INDEPENDANT"

    def test_parti_inconnu_majuscule(self):
        assert normalize_party("nouveau_parti") == "NOUVEAU_PARTI"


class TestNormalizeLocality:
    def test_tiapum_typo(self):
        assert normalize_locality("Tiapum") == "TIAPOUM"

    def test_grand_bassam_sans_tiret(self):
        assert normalize_locality("grand bassam") == "GRAND-BASSAM"

    def test_cote_ivoire_sans_accent(self):
        assert normalize_locality("Cote d Ivoire") == "CÔTE D'IVOIRE"

    def test_bouake_sans_accent(self):
        assert normalize_locality("bouake") == "BOUAKÉ"

    def test_abidjan_majuscule(self):
        assert normalize_locality("ABIDJAN") == "ABIDJAN"


class TestFuzzyMatch:
    LOCS = ["TIAPOUM", "ABIDJAN", "GRAND-BASSAM", "BOUAKÉ", "AGBOVILLE"]

    def test_tiapoume(self):
        result = find_best_locality_match("Tiapoume", self.LOCS)
        assert result == "TIAPOUM"

    def test_abijane(self):
        result = find_best_locality_match("Abijane", self.LOCS)
        assert result == "ABIDJAN"

    def test_trop_different_retourne_none(self):
        result = find_best_locality_match("XXXXXXXX", self.LOCS, threshold=2)
        assert result is None


class TestStripAccents:
    def test_accents_supprimes(self):
        assert strip_accents("Côte d'Ivoire") == "Cote d'Ivoire"

    def test_sans_accent_inchange(self):
        assert strip_accents("Abidjan") == "Abidjan"


class TestCandidateName:
    def test_nom_clean(self):
        result = normalize_candidate_name("N'GUESSAN AKA ARNAUD")
        assert result == "N'GUESSAN AKA ARNAUD"

    def test_artefact_ocr(self):
        result = normalize_candidate_name("DUPONT  ||  JEAN")
        assert "||" not in result
        assert "DUPONT" in result
