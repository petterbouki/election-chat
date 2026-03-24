"""
pipeline.py — Orchestrateur CLI du pipeline d'ingestion complet.
Usage : python -m ingestion.pipeline [--pdf PATH] [--db PATH] [--dpi N]
"""

import argparse
import logging
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def download_pdf(url: str, dest: Path) -> Path:
    """Télécharge le PDF si pas encore présent."""
    if dest.exists():
        log.info(f"PDF déjà présent : {dest} ({dest.stat().st_size / 1024:.0f} KB)")
        return dest

    import requests
    log.info(f"Téléchargement : {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    log.info(f"PDF téléchargé : {dest} ({len(r.content) / 1024:.0f} KB)")
    return dest


def run(pdf_path: str, db_path: str, dpi: int = 250):
    """Pipeline complet : OCR → parse → normalise → charge DuckDB."""
    from ingestion.extract import extract_all_pages
    from ingestion.normalize import normalize_dataframe
    from ingestion.load_db import to_dataframes, create_db, load_circonscriptions, load_candidats, verify_db

    t0 = time.time()
    log.info("=" * 50)
    log.info("DÉMARRAGE DU PIPELINE D'INGESTION")
    log.info("=" * 50)

    # ── Étape 1 : extraction OCR
    log.info(f"[1/3] Extraction OCR (DPI={dpi})...")
    t1 = time.time()
    circs, cands = extract_all_pages(pdf_path, dpi=dpi)
    log.info(f"      Terminé en {time.time()-t1:.1f}s : {len(circs)} circonscriptions, {len(cands)} candidats")

    # ── Étape 2 : normalisation
    log.info("[2/3] Normalisation des entités...")
    t2 = time.time()
    df_circs, df_cands = to_dataframes(circs, cands)
    df_circs, df_cands = normalize_dataframe(df_circs, df_cands)

    # Sauvegarde CSV intermédiaire
    raw_dir = Path(db_path).parent / "raw"
    raw_dir.mkdir(exist_ok=True)
    df_circs.to_csv(raw_dir / "circonscriptions.csv", index=False, encoding="utf-8")
    df_cands.to_csv(raw_dir / "candidats.csv", index=False, encoding="utf-8")
    log.info(f"      CSV sauvegardés dans {raw_dir}/")
    log.info(f"      Terminé en {time.time()-t2:.1f}s")

    # ── Étape 3 : chargement DuckDB
    log.info(f"[3/3] Chargement dans DuckDB : {db_path}")
    t3 = time.time()
    con = create_db(db_path)
    load_circonscriptions(con, df_circs)
    load_candidats(con, df_cands)
    log.info(f"      Terminé en {time.time()-t3:.1f}s")

    # ── Vérification
    stats = verify_db(con)
    log.info("")
    log.info("=" * 50)
    log.info("PIPELINE TERMINÉ en {:.1f}s".format(time.time() - t0))
    log.info("=" * 50)
    for k, v in stats.items():
        log.info(f"  {k}: {v}")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Pipeline d'ingestion élections CEI 2025")
    parser.add_argument(
        "--pdf",
        default="data/raw/edan_2025.pdf",
        help="Chemin vers le PDF source",
    )
    parser.add_argument(
        "--db",
        default="data/elections.duckdb",
        help="Chemin vers la base DuckDB",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=250,
        help="Résolution OCR (défaut: 250)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Télécharge le PDF depuis le site CEI avant traitement",
    )
    args = parser.parse_args()

    if args.download:
        CEI_URL = "https://www.cei.ci/wp-content/uploads/2025/12/EDAN_2025_RESULTAT_NATIONAL_DETAILS.pdf"
        download_pdf(CEI_URL, Path(args.pdf))

    if not Path(args.pdf).exists():
        log.error(f"PDF introuvable : {args.pdf}")
        log.error("Utilisez --download pour le télécharger ou copiez-le manuellement.")
        sys.exit(1)

    run(args.pdf, args.db, args.dpi)


if __name__ == "__main__":
    main()
