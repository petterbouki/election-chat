"""
rag/indexer.py — Indexation des données électorales pour la recherche vectorielle (Level 2).
Convertit chaque ligne candidate en chunk textuel, génère les embeddings, stocke dans FAISS.
"""

import logging
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import duckdb

log = logging.getLogger(__name__)

INDEX_PATH = "data/rag_index.faiss"
META_PATH  = "data/rag_meta.pkl"


def rows_to_chunks(db_path: str) -> list[dict]:
    """
    Convertit les lignes de la base en chunks textuels.
    Chaque chunk = un candidat avec son contexte.
    """
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute("""
        SELECT
            ca.id,
            c.id   AS circ_id,
            c.nom  AS circ_nom,
            c.region,
            ca.parti,
            ca.nom AS candidat,
            ca.score,
            ca.pourcentage,
            ca.elu,
            c.taux_participation,
            c.inscrits,
            c.source_page
        FROM candidats ca
        JOIN circonscriptions c ON c.id = ca.circonscription_id
    """).fetchall()

    cols = ["id","circ_id","circ_nom","region","parti","candidat",
            "score","pourcentage","elu","taux_participation","inscrits","source_page"]

    chunks = []
    for row in rows:
        r = dict(zip(cols, row))
        elu_str = "ELU(E)" if r["elu"] else ""
        text = (
            f"Circonscription {r['circ_id']} {r['circ_nom']}. "
            f"Candidat : {r['candidat']} ({r['parti']}). "
            f"Score : {r['score']} voix, {r['pourcentage']}% des suffrages. {elu_str} "
            f"Participation : {r['taux_participation']}% sur {r['inscrits']} inscrits. "
            f"Page {r['source_page']}."
        )
        chunks.append({
            "text": text,
            "metadata": r,
        })

    log.info(f"Chunks générés : {len(chunks)}")
    return chunks


def build_index(db_path: str, index_path: str = INDEX_PATH, meta_path: str = META_PATH):
    """
    Génère les embeddings et construit l'index FAISS.
    """
    try:
        from sentence_transformers import SentenceTransformer
        import faiss
    except ImportError:
        log.error("Installez : pip install sentence-transformers faiss-cpu")
        return

    log.info("Chargement du modèle d'embedding...")
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    chunks = rows_to_chunks(db_path)
    texts = [c["text"] for c in chunks]
    metas = [c["metadata"] for c in chunks]

    log.info(f"Calcul des embeddings pour {len(texts)} chunks...")
    embeddings = model.encode(texts, batch_size=64, show_progress_bar=True, normalize_embeddings=True)

    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)   # Inner product = cosine sur vecteurs normalisés
    index.add(embeddings.astype(np.float32))

    Path(index_path).parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, index_path)

    with open(meta_path, "wb") as f:
        pickle.dump({"chunks": chunks, "metas": metas}, f)

    log.info(f"Index FAISS sauvegardé : {index_path} ({index.ntotal} vecteurs)")


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "data/elections.duckdb"
    build_index(db)
