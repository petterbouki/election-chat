"""
extract.py - Extraction PDF CEI elections 2025
"""

import re, io, logging
from dataclasses import dataclass
from typing import Optional

import pdfplumber
import fitz
import pytesseract
from PIL import Image, ImageEnhance, ImageOps, ImageFilter

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPPLER_PATH = r"C:\Release-25.12.0-0\poppler-25.12.0\Library\bin"

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

COLOR_BLACK = (0.0, 0.0, 0.0)
COLOR_GREEN = (0.847059, 0.894118, 0.737255)

@dataclass
class Circonscription:
    id: int
    nom: str
    region: str = ""
    nb_bv: Optional[int] = None
    inscrits: Optional[int] = None
    votants: Optional[int] = None
    taux_participation: Optional[float] = None
    bulletins_nuls: Optional[int] = None
    suffrages_exprimes: Optional[int] = None
    blancs_nombre: Optional[int] = None
    blancs_pct: Optional[float] = None
    source_page: Optional[int] = None

@dataclass
class Candidat:
    circonscription_id: int
    parti: str
    nom: str
    score: Optional[int] = None
    pourcentage: Optional[float] = None
    elu: bool = False
    source_page: Optional[int] = None

RE_PCT = re.compile(r"(\d{1,3}[,\.]\d{1,2})\s*%")
RE_ELU = re.compile(r"\bELU\(?E?\)?\b", re.IGNORECASE)
RE_CIRC_ID = re.compile(r"[\[\(]?0*(\d{1,3})[\]\)\}|]")

PARTIS = {"INDEPENDANT","INDÉPENDANT","RHDP","PDCI-RDA","PDCI","FPI","ADCI",
          "MGC","UDPCI","PPA-CI","GJPA-CI","ECS","GPS","UVRD","CODE","PDC-RDA"}
RE_PARTI = re.compile(r"\b(" + "|".join(re.escape(p) for p in PARTIS) + r")\b", re.I)

HEADERS = re.compile(
    r"ELECTION|RESULTATS\s+DES|BULL\.\s*BLANCS|GROUPEMENTS|^NOMBRE\s*%|"
    r"^PART\.|NBBV|^TOTAL\b|TAUXDE|Page\s+\d+\s+de|COMMISSION|SCRUTIN\s+DU", re.I|re.M)

def is_header(t): return bool(HEADERS.search(t))
def get_pct(s):
    m = RE_PCT.search(s); return float(m.group(1).replace(",",".")) if m else None
def get_ints(s):
    s2 = re.sub(r"\d{1,3}[,\.]\d{1,2}\s*%","",s)
    return [int(n) for n in re.findall(r"\b(\d{1,6})\b",s2) if 0<int(n)<500000]
def norm_parti(p):
    return {"INDÉPENDANT":"INDEPENDANT","PDCI":"PDCI-RDA","PDC-RDA":"PDCI-RDA","ECS":"RHDP"}.get(p.upper(),p.upper())

def ocr_band(img, psm=6):
    img = img.convert("L")
    img = ImageOps.autocontrast(img, cutoff=1)
    img = ImageEnhance.Contrast(img).enhance(2.5)
    img = img.filter(ImageFilter.SHARPEN)
    return pytesseract.image_to_string(img, lang="fra",
        config=f"--psm {psm} --oem 3").strip()

def get_page_bands(p_pl, p_fitz, page_num, zoom=5.0):
    mat = fitz.Matrix(zoom, zoom)

    # Tous les rects noirs horizontaux larges = separateurs de lignes
    black_lines = sorted([r for r in p_pl.rects
        if r.get("non_stroking_color") == COLOR_BLACK
        and r["width"] > 300
        and r["height"] < 3],
        key=lambda x: x["top"])

    ys = [r["top"] for r in black_lines]

    # Lignes vertes = elus
    green_tops = {round(r["top"]) for r in p_pl.rects
                  if r.get("non_stroking_color") == COLOR_GREEN and r["width"] > 50}

    bands = []
    for i in range(len(ys)-1):
        y0, y1 = ys[i], ys[i+1]
        h = y1 - y0
        # Accepte les bandes entre 5px et 100px de hauteur
        if not (5 < h < 100):
            continue

        is_elu = any(abs(g-y0) < 8 for g in green_tops)

        def crop_ocr(x0, x1, y0=y0, y1=y1, psm=6):
            pix = p_fitz.get_pixmap(matrix=mat, clip=fitz.Rect(x0, y0, x1, y1))
            return ocr_band(Image.open(io.BytesIO(pix.tobytes("png"))), psm)

        bands.append({
            "page": page_num,
            "y0": y0, "y1": y1,
            "is_elu": is_elu,
            "full": crop_ocr(14, 828),
            "left": crop_ocr(14, 460),
            "right": crop_ocr(460, 828),
        })

    return bands

def try_parse_circ(text, page):
    if not text or len(text) < 5: return None
    # Cherche un numero de circ dans les 30 premiers caracteres
    m = RE_CIRC_ID.search(text[:30])
    if not m: return None
    circ_id = int(m.group(1))
    if not (1 <= circ_id <= 250): return None
    reste = text[m.end():].strip()
    # Nom = texte alphabetique avant les nombres
    nom_m = re.match(r"([A-ZÀÂÄÉÈÊËÎÏÔÙÛÜÇ][^0-9|]{4,120}?)(?=\s+\d|\s*$)", reste, re.I)
    nom = re.sub(r"\s+", " ", nom_m.group(1)).strip().rstrip(",-") if nom_m else f"CIRC_{circ_id}"
    nom = re.sub(r"[|\\]+", " ", nom).strip()
    if len(nom) < 3: nom = f"CIRC_{circ_id}"
    pcts = [float(p.replace(",",".")) for p in re.findall(r"\d{1,3}[,\.]\d{2}(?=\s*%)", reste)]
    ints = get_ints(reste)
    c = Circonscription(id=circ_id, nom=nom, source_page=page)
    for i, col in enumerate(["nb_bv","inscrits","votants","bulletins_nuls","suffrages_exprimes","blancs_nombre"]):
        if i < len(ints): setattr(c, col, ints[i])
    if pcts: c.taux_participation = pcts[0]
    if len(pcts) > 1: c.blancs_pct = pcts[1]
    return c

def try_parse_cand(text, full, circ_id, page, is_elu):
    if not text or len(text) < 5: return None
    elu = is_elu or bool(RE_ELU.search(full))
    m = RE_PARTI.search(text)
    if not m:
        if elu and RE_PCT.search(text):
            pct = get_pct(text)
            score = next((v for v in get_ints(text) if 100 <= v <= 200000), None)
            nom = re.sub(r"\d[\d\s,\.%|]+$", "", text).strip()
            nom = re.sub(r"[|_\-]{2,}", "", nom).strip()
            if len(nom) >= 4 and re.search(r"[A-Z]{2,}", nom):
                return Candidat(circ_id, "RHDP", nom, score, pct, True, page)
        return None
    parti = m.group(1)
    reste = text[m.end():].strip()
    pct = get_pct(reste)
    score = next((v for v in get_ints(reste) if 100 <= v <= 200000), None)
    nom = re.sub(r"\d[\d\s,\.%|]+$", "", reste).strip()
    nom = RE_PCT.sub("", nom).strip()
    if score: nom = re.sub(r"\b"+str(score)+r"\b", "", nom).strip()
    nom = re.sub(r"\s+", " ", nom).strip().strip("-|_").strip()
    if len(nom) < 3 or not re.search(r"[A-Z]{2,}", nom, re.I): return None
    return Candidat(circ_id, norm_parti(parti), nom, score, pct, elu, page)

def parse_bands(bands):
    circs, cands, ids_vus = [], [], set()
    current_circ = None
    for b in bands:
        if is_header(b["full"]): continue
        c = try_parse_circ(b["left"], b["page"])
        if c and c.id not in ids_vus:
            circs.append(c); ids_vus.add(c.id); current_circ = c
        if current_circ:
            cand = try_parse_cand(b["right"], b["full"], current_circ.id, b["page"], b["is_elu"])
            if not cand:
                cand = try_parse_cand(b["full"], b["full"], current_circ.id, b["page"], b["is_elu"])
            if cand: cands.append(cand)
    return circs, cands

def extract_all_pages(pdf_path, dpi=350):
    log.info(f"Ouverture du PDF : {pdf_path}")
    pdf_pl = pdfplumber.open(pdf_path)
    doc = fitz.open(pdf_path)
    log.info(f"{len(doc)} pages a traiter")
    zoom = dpi / 72.0
    all_bands = []
    for page_num in range(len(doc)):
        log.info(f"  Page {page_num+1}/{len(doc)}...")
        try:
            bands = get_page_bands(pdf_pl.pages[page_num], doc[page_num], page_num+1, zoom)
            all_bands.extend(bands)
            log.info(f"    {len(bands)} bandes")
        except Exception as e:
            log.warning(f"    Erreur: {e}")
    circs, cands = parse_bands(all_bands)
    log.info(f"Termine: {len(circs)} circs, {len(cands)} candidats")
    return circs, cands

if __name__ == "__main__":
    import sys
    pdf = sys.argv[1] if len(sys.argv) > 1 else r"data\raw\edan_2025.pdf"
    circs, cands = extract_all_pages(pdf)
    print(f"\nResultat : {len(circs)} circonscriptions, {len(cands)} candidats")
    print(f"Elus     : {sum(1 for c in cands if c.elu)}")
    print(f"Avec score: {sum(1 for c in cands if c.score)}")
    print("\nCirconscriptions :")
    for c in sorted(circs, key=lambda x: x.id)[:10]:
        print(f"  [{c.id:03d}] {c.nom[:45]:45s} | inscrits={c.inscrits} | taux={c.taux_participation}%")
    ids = sorted(c.id for c in circs)
    print(f"\nIDs ({len(ids)}): {ids[:30]}")
    manquants = sorted(set(range(1,206)) - set(ids))
    if manquants: print(f"Manquants ({len(manquants)}): {manquants[:20]}")