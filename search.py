#!/usr/bin/env python3
"""
Paper Radar — Weekly Search Script
Sucht auf PubMed und bioRxiv, verifiziert alle DOIs, schreibt weekly_update.json
"""

import json
import time
import sys
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta
from typing import Optional

# ─── Konfiguration ────────────────────────────────────────────────────────────

SEARCH_BLOCKS = {
    "A": {
        "label": "SolV & Lanthanide",
        "queries": [
            "Methylacidiphilum fumariolicum",
            "XoxF methanol dehydrogenase lanthanide",
            "lanthanide switch transcriptomics bacteria",
            "rare earth elements bacteria gene expression",
            "verrucomicrobia methanotroph lanthanide",
        ]
    },
    "B": {
        "label": "Actinide & Radionuklide",
        "queries": [
            "actinide bacteria growth metabolism",
            "americium microbiology",
            "radionuclide microbial response gene expression",
            "uranium bacteria transcriptome",
            "plutonium microorganism",
        ]
    },
    "C": {
        "label": "Transkriptomische Strahlenantworten",
        "queries": [
            "ionizing radiation transcriptome bacteria",
            "alpha radiation gene expression microorganism",
            "Deinococcus radiodurans RNA sequencing transcriptome",
            "radiation stress response transcriptomics",
            "DNA damage response ionizing radiation bacteria RNAseq",
            "radiotolerant bacteria transcriptome",
        ]
    }
}

MONTHS_BACK = 6
MAX_PER_BLOCK = 3

# ─── HTTP Helpers ──────────────────────────────────────────────────────────────

def http_get(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PaperRadar/1.0 (research tool)"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  HTTP GET error: {e} — {url[:80]}")
        return None

def http_head_ok(url, timeout=8):
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "PaperRadar/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status < 400
    except urllib.error.HTTPError as e:
        return e.code < 400
    except Exception:
        return False

def date_range():
    to_date = datetime.now()
    from_date = to_date - timedelta(days=MONTHS_BACK * 30)
    return from_date.strftime("%Y/%m/%d"), to_date.strftime("%Y/%m/%d")

# ─── PubMed ───────────────────────────────────────────────────────────────────

def pubmed_search(query, date_from, date_to, max_results=15):
    params = urllib.parse.urlencode({
        "db": "pubmed",
        "term": query,
        "mindate": date_from,
        "maxdate": date_to,
        "datetype": "pdat",
        "retmax": max_results,
        "retmode": "json",
    })
    data = http_get(f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?{params}")
    if not data:
        return []
    return data.get("esearchresult", {}).get("idlist", [])

def pubmed_summary(pmids):
    if not pmids:
        return {}
    params = urllib.parse.urlencode({
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
        "version": "2.0"
    })
    data = http_get(f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?{params}")
    if not data:
        return {}
    return data.get("result", {})

def parse_pubmed_article(pmid, result):
    """Parsed einen PubMed esummary Eintrag in unser Paper-Format."""
    try:
        authors = result.get("authors", [])
        author_str = authors[0].get("name", "") + " et al." if len(authors) > 1 else (authors[0].get("name", "") if authors else "")
        doi = ""
        for art_id in result.get("articleids", []):
            if art_id.get("idtype") == "doi":
                doi = art_id.get("value", "")
                break
        return {
            "pmid": pmid,
            "title": result.get("title", "").rstrip("."),
            "authors": author_str,
            "year": result.get("pubdate", "")[:4],
            "journal": result.get("source", ""),
            "doi": doi,
            "source": "pubmed",
            "abstract": result.get("title", ""),  # esummary hat keinen Abstract
        }
    except Exception as e:
        print(f"  Parse error for PMID {pmid}: {e}")
        return None

# ─── bioRxiv ──────────────────────────────────────────────────────────────────

def biorxiv_search(date_from, date_to, category="microbiology", limit=100):
    """Holt bioRxiv Preprints aus einer Kategorie und filtert danach."""
    # bioRxiv API hat keine Keyword-Suche — wir holen nach Kategorie und filtern
    from_str = date_from.replace("/", "-")[:10]
    to_str = date_to.replace("/", "-")[:10]
    url = f"https://api.biorxiv.org/details/biorxiv/{from_str}/{to_str}/0/json"
    data = http_get(url)
    if not data:
        return []
    return data.get("collection", [])

def filter_biorxiv(papers, keywords):
    """Filtert bioRxiv-Paper nach Keywords in Titel und Abstract."""
    results = []
    kw_lower = [k.lower() for k in keywords]
    for p in papers:
        text = (p.get("title", "") + " " + p.get("abstract", "")).lower()
        if any(kw in text for kw in kw_lower):
            results.append({
                "title": p.get("title", "").rstrip("."),
                "authors": p.get("authors", "").split(";")[0].strip() + " et al." if ";" in p.get("authors", "") else p.get("authors", ""),
                "year": p.get("date", "")[:4],
                "journal": "bioRxiv",
                "doi": p.get("doi", ""),
                "source": "biorxiv",
            })
    return results

# ─── DOI Verifikation ─────────────────────────────────────────────────────────

def verify_doi(doi):
    """Prüft ob ein DOI wirklich existiert."""
    if not doi:
        return False
    clean = doi.strip().lstrip("https://doi.org/").lstrip("http://doi.org/")

    # 1. DOI.org HEAD-Request
    if http_head_ok(f"https://doi.org/{clean}"):
        print(f"    ✓ DOI verifiziert (doi.org): {clean}")
        return True

    # 2. PubMed DOI-Suche
    time.sleep(0.3)
    params = urllib.parse.urlencode({"db": "pubmed", "term": f"{clean}[doi]", "retmode": "json"})
    data = http_get(f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?{params}")
    if data and int(data.get("esearchresult", {}).get("count", "0")) > 0:
        print(f"    ✓ DOI verifiziert (PubMed): {clean}")
        return True

    # 3. bioRxiv API für Preprints
    if clean.startswith("10.1101/"):
        data = http_get(f"https://api.biorxiv.org/details/biorxiv/{clean}/na/json")
        if data and data.get("collection"):
            print(f"    ✓ DOI verifiziert (bioRxiv): {clean}")
            return True

    print(f"    ✗ DOI nicht verifiziert: {clean}")
    return False

# ─── Relevanz-Scoring ─────────────────────────────────────────────────────────

RELEVANCE_KEYWORDS = {
    "high": ["methylacidiphilum", "solv", "fumariolicum", "lanthanide", "xoxf",
             "actinide", "americium", "radionuclide", "transcriptom", "verrucomicrobia"],
    "medium": ["rare earth", "methylotroph", "methanotroph", "radiation", "bacteria",
               "gene expression", "rnaseq", "extremophile", "uranium", "deinococcus"],
}

def score_relevance(paper):
    text = (paper.get("title", "") + " " + paper.get("abstract", "")).lower()
    score = 0
    for kw in RELEVANCE_KEYWORDS["high"]:
        if kw in text:
            score += 3
    for kw in RELEVANCE_KEYWORDS["medium"]:
        if kw in text:
            score += 1
    return score

# ─── Hauptlogik ───────────────────────────────────────────────────────────────

def run_search():
    date_from, date_to = date_range()
    print(f"\n{'='*60}")
    print(f"Paper Radar — Weekly Search")
    print(f"Zeitraum: {date_from} bis {date_to}")
    print(f"{'='*60}\n")

    # Bekannte DOIs aus history.json laden (falls vorhanden)
    known_dois = set()
    try:
        with open("history.json", "r") as f:
            history = json.load(f)
        for entry in history:
            for p in entry.get("papers", []):
                if p.get("doi"):
                    known_dois.add(p["doi"].lower().strip())
        print(f"Bekannte DOIs aus Verlauf: {len(known_dois)}")
    except FileNotFoundError:
        print("Kein Verlauf gefunden — erste Suche.")

    # Zotero Library optional einlesen (falls zotero_library.csv im Repository liegt)
    zotero_dois = set()
    zotero_titles = []
    zotero_csv_path = "zotero_library.csv"
    try:
        import csv
        with open(zotero_csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # DOI einlesen — Spalte heißt "DOI" in Zotero CSV
                doi_col = next((k for k in row if k.strip().lower() == "doi"), None)
                title_col = next((k for k in row if k.strip().lower() == "title"), None)
                if doi_col and row[doi_col].strip():
                    zotero_dois.add(row[doi_col].strip().lower())
                if title_col and row[title_col].strip():
                    zotero_titles.append(row[title_col].strip().lower())
        known_dois.update(zotero_dois)
        print(f"Zotero Library geladen: {len(zotero_dois)} DOIs · {len(zotero_titles)} Titel")
        print(f"  → Filter gesamt: {len(known_dois)} bekannte DOIs\n")
    except FileNotFoundError:
        print("Keine zotero_library.csv gefunden — wird ohne Zotero-Filter gesucht.")
        print("  (Optional: zotero_library.csv ins Repository legen um bekannte Paper zu filtern)\n")

    # bioRxiv Preprints vorab holen (eine Anfrage für alle Blocks)
    print("Lade bioRxiv Microbiology Preprints…")
    biorxiv_raw = biorxiv_search(date_from, date_to, limit=200)
    print(f"  {len(biorxiv_raw)} Preprints geladen\n")

    results = []
    seen_dois = set()
    seen_pmids = set()

    for block_id, block in SEARCH_BLOCKS.items():
        print(f"─── Block {block_id}: {block['label']} ───")
        block_papers = []

        # PubMed Suche
        all_pmids = []
        for query in block["queries"]:
            print(f"  PubMed: {query}")
            pmids = pubmed_search(query, date_from, date_to)
            new_pmids = [p for p in pmids if p not in seen_pmids]
            all_pmids.extend(new_pmids)
            seen_pmids.update(new_pmids)
            time.sleep(0.4)  # Rate limiting

        # PubMed Metadaten holen
        if all_pmids:
            print(f"  Hole Metadaten für {len(all_pmids)} PMIDs…")
            summary = pubmed_summary(list(set(all_pmids)))
            for pmid in set(all_pmids):
                if pmid == "uids":
                    continue
                article_data = summary.get(pmid)
                if article_data and article_data.get("title"):
                    paper = parse_pubmed_article(pmid, article_data)
                    if paper:
                        paper["block"] = block_id
                        paper["block_label"] = block["label"]
                        block_papers.append(paper)
            time.sleep(0.4)

        # bioRxiv filtern
        all_query_keywords = []
        for q in block["queries"]:
            all_query_keywords.extend(q.lower().split())
        biorxiv_matches = filter_biorxiv(biorxiv_raw, block["queries"])
        for p in biorxiv_matches:
            p["block"] = block_id
            p["block_label"] = block["label"]
            block_papers.append(p)

        print(f"  {len(block_papers)} Paper gefunden vor Verifikation")

        # Sortieren nach Relevanz
        block_papers.sort(key=score_relevance, reverse=True)

        # Verifikation + Deduplizierung
        verified = []
        for paper in block_papers:
            doi = paper.get("doi", "").lower().strip()

            # Skip bekannte DOIs
            if doi and doi in known_dois:
                print(f"  → Skip (bereits bekannt): {paper['title'][:50]}")
                continue

            # Skip Duplikate in diesem Lauf
            if doi and doi in seen_dois:
                continue
            if doi:
                seen_dois.add(doi)

            # DOI verifizieren
            print(f"  Verifiziere: {paper['title'][:55]}…")
            if paper.get("doi") and verify_doi(paper["doi"]):
                verified.append(paper)
                if len(verified) >= MAX_PER_BLOCK:
                    break
            elif not paper.get("doi"):
                # Paper ohne DOI überspringen
                print(f"    → Kein DOI, übersprungen")

            time.sleep(0.2)

        print(f"  → {len(verified)} verifiziert\n")

        if verified:
            results.extend(verified)
        else:
            # Kein-Treffer Platzhalter
            results.append({
                "block": block_id,
                "block_label": block["label"],
                "title": "Keine neuen verifizierten Treffer",
                "authors": "",
                "year": "",
                "journal": "",
                "doi": "",
                "source": "",
                "no_result": True
            })

    # weekly_update.json schreiben
    output = {
        "generated": datetime.now().isoformat(),
        "date_from": date_from,
        "date_to": date_to,
        "papers": results,
        "total_verified": sum(1 for p in results if not p.get("no_result")),
        "zotero_loaded": len(zotero_dois) > 0,
        "zotero_dois_filtered": len(zotero_dois),
        "history_dois_filtered": len(known_dois) - len(zotero_dois)
    }

    with open("weekly_update.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"{'='*60}")
    print(f"Fertig! {output['total_verified']} verifizierte Paper")
    if output['zotero_loaded']:
        print(f"Zotero-Filter aktiv: {output['zotero_dois_filtered']} DOIs aus zotero_library.csv")
    print(f"Verlaufs-Filter: {output['history_dois_filtered']} DOIs aus history.json")
    print(f"Gespeichert in weekly_update.json")
    print(f"{'='*60}\n")

    # Auch history.json aktualisieren
    history_entry = {
        "date": datetime.now().isoformat(),
        "papers": [p for p in results if not p.get("no_result")]
    }
    try:
        with open("history.json", "r") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []

    history.insert(0, history_entry)
    history = history[:12]  # max 12 Einträge

    with open("history.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    print("history.json aktualisiert.")

    # Exit code für GitHub Actions
    sys.exit(0)

if __name__ == "__main__":
    run_search()
