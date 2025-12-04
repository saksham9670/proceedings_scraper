#!/usr/bin/env python3
"""
aaai_conferences_to_csv.py  -- updated to avoid passing PDF bytes to BeautifulSoup
Writes ONE CSV (aaai_all_papers_authors.csv) with columns:
site,year,conference,track,paper_url,pdf_url,email,name,affiliation

Major fixes:
 - Detect PDF responses via Content-Type or PDF signature and skip BeautifulSoup parsing for them.
 - Optional PDF email extraction with pdfminer.six (only if installed).
 - Graceful KeyboardInterrupt handling so CSV is preserved.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re, csv, time, random, os, sys
from urllib.parse import urljoin, urlparse, urlunparse

# Optional PDF parsing
try:
    from io import BytesIO
    from pdfminer.high_level import extract_text
    PDFMINER_AVAILABLE = True
except Exception:
    PDFMINER_AVAILABLE = False

# ---------- CONFIG ----------
START_URL = "https://www.aaai.org/Library/conferences-library.php"
SITE_NAME = "AAAI"
OUTPUT_CSV = "aaai_all_papers_authors.csv"

USER_AGENT = "AAAI-FullScraper/1.0 (+https://yourdomain.example)"
REQUEST_TIMEOUT = 15
RETRIES = 3
BACKOFF = 0.5
DELAY_BASE = 0.8
MAX_PAPERS_PER_YEAR = None
MAX_YEARS_PER_CONFERENCE = None
# ----------------------------

def make_session():
    s = requests.Session()
    retry = Retry(total=RETRIES, backoff_factor=BACKOFF,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(['GET', 'POST', 'HEAD']))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def safe_get(session, url):
    return session.get(url, timeout=REQUEST_TIMEOUT)

def normalize_url(base, href):
    if not href:
        return None
    joined = urljoin(base, href)
    p = urlparse(joined)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, p.query, ""))

def jitter_sleep():
    time.sleep(DELAY_BASE + random.uniform(0, DELAY_BASE * 0.3))

def extract_all_meta_values(soup, name_regex):
    tags = soup.find_all('meta', attrs={'name': re.compile(name_regex, re.I)})
    vals = []
    for t in tags:
        c = t.get('content', '').strip()
        if c:
            vals.append(c)
    return vals

def extract_authors_from_meta(soup):
    names = extract_all_meta_values(soup, r'citation_author$')
    emails = extract_all_meta_values(soup, r'citation_author_email$')
    affs = extract_all_meta_values(soup, r'citation_author_institution$')
    authors = []
    for i, n in enumerate(names):
        a = {'name': n, 'email': None, 'affiliation': ''}
        if i < len(emails):
            e = emails[i].strip()
            if '@' in e:
                a['email'] = e
        if i < len(affs):
            a['affiliation'] = affs[i].strip()
        authors.append(a)
    return authors

def extract_mailto_links(soup):
    out = []
    for a in soup.select('a[href^=mailto]'):
        href = a.get('href', '')
        email = href.split(':',1)[1].split('?')[0].strip() if ':' in href else ''
        if not email:
            continue
        name = a.get_text(" ", strip=True) or ''
        out.append({'email': email, 'name': name, 'affiliation': ''})
    return out

def fallback_text_author_search(soup):
    text = soup.get_text(" ", strip=True)
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    uniq = []
    for e in emails:
        if e not in uniq:
            uniq.append(e)
    out = []
    for e in uniq:
        idx = text.find(e)
        name_guess = ''
        if idx > 0:
            start = max(0, idx-120)
            snippet = text[start:idx]
            words = re.findall(r"[A-Za-z\-\']{2,}", snippet)
            if words:
                candidate = " ".join(words[-4:])
                name_guess = candidate
        out.append({'email': e, 'name': name_guess, 'affiliation': ''})
    return out

def try_extract_from_pdf(session, pdf_url):
    if not PDFMINER_AVAILABLE:
        return []
    try:
        r = safe_get(session, pdf_url)
        if r.status_code != 200:
            return []
        data = r.content
        # Use pdfminer to extract text
        text = extract_text(BytesIO(data))
        emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        uniq = []
        out = []
        for e in emails:
            if e not in uniq:
                uniq.append(e)
                idx = text.find(e)
                name_guess = ''
                if idx > 0:
                    start = max(0, idx-120)
                    snippet = text[start:idx]
                    words = re.findall(r"[A-Za-z\-\']{2,}", snippet)
                    if words:
                        name_guess = " ".join(words[-4:])
                out.append({'email': e, 'name': name_guess, 'affiliation': ''})
        return out
    except Exception:
        return []

def extract_pdf_url_from_page(soup, base_url):
    meta = soup.find('meta', attrs={'name': re.compile(r'citation_pdf_url', re.I)})
    if meta and meta.get('content'):
        return normalize_url(base_url, meta['content'])
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.pdf' in href.lower() or 'download' in href.lower():
            return normalize_url(base_url, href)
    return None

def find_candidate_paper_links(soup, base_url):
    candidates = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        lower = href.lower()
        if lower.startswith('#') or lower.startswith('mailto:') or lower.endswith('.jpg') or lower.endswith('.png'):
            continue
        if any(k in lower for k in ['/paper', '/paper/', '/papers/', '/article', '/article/view', '.pdf']) or re.search(r'/\d{4}\.', lower) or re.search(r'20\d{2}', a.get_text() or ''):
            candidates.append(normalize_url(base_url, href))
    seen = set(); out = []
    for u in candidates:
        if u and u not in seen:
            seen.add(u); out.append(u)
    return out

def is_pdf_response(resp):
    # Check content-type header first
    ctype = resp.headers.get('Content-Type', '').lower()
    if 'application/pdf' in ctype:
        return True
    # If header absent/misleading, check start bytes for %PDF signature
    start = resp.content[:5] if getattr(resp, 'content', None) else b''
    try:
        if isinstance(start, str):
            start = start.encode('utf-8', errors='ignore')
    except Exception:
        pass
    if start.startswith(b'%PDF'):
        return True
    return False

def write_csv_header(path):
    if not os.path.exists(path):
        with open(path, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.writer(fh)
            writer.writerow(["site","year","conference","track","paper_url","pdf_url","email","name","affiliation"])

def append_row(path, row):
    with open(path, 'a', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(row)

def scrape():
    session = make_session()
    print("Fetching index:", START_URL)
    try:
        r = safe_get(session, START_URL)
    except Exception as e:
        print("Failed to fetch START_URL:", e)
        return

    if r.status_code != 200:
        print("Bad status for START_URL:", r.status_code)
        return

    soup = BeautifulSoup(r.content, 'html.parser')
    blocks = soup.select('.libraryconf')
    if not blocks:
        possible = []
        for div in soup.find_all(['div','section']):
            if div.find('h2') and div.find('a'):
                possible.append(div)
        blocks = possible

    print(f"Found {len(blocks)} conference blocks (heuristic).")
    write_csv_header(OUTPUT_CSV)

    try:
        for b_idx, block in enumerate(blocks, start=1):
            title_tag = block.find('h2')
            conf_title = title_tag.get_text(" ", strip=True) if title_tag else "Unknown Conference"
            ps = block.find_all('p')
            description = " ".join([p.get_text(" ", strip=True) for p in ps]).strip() if ps else ""
            year_links = []
            for a in block.find_all('a', href=True):
                txt = a.get_text(strip=True)
                m = re.findall(r'\b(19\d{2}|20\d{2})\b', txt)
                if m:
                    for year_text in m:
                        year_links.append((int(year_text), normalize_url(START_URL, a['href'])))
                else:
                    href = a['href']
                    m2 = re.search(r'(19\d{2}|20\d{2})', href)
                    if m2:
                        year_links.append((int(m2.group(1)), normalize_url(START_URL, href)))

            seen_years = set(); unique_years = []
            for y,u in year_links:
                if y not in seen_years:
                    seen_years.add(y); unique_years.append((y,u))
            unique_years = sorted(unique_years, key=lambda x: x[0], reverse=True)
            if MAX_YEARS_PER_CONFERENCE:
                unique_years = unique_years[:MAX_YEARS_PER_CONFERENCE]

            print(f"[{b_idx}/{len(blocks)}] Conference: {conf_title} — {len(unique_years)} years found")

            for (year, year_url) in unique_years:
                print(f"   -> Year {year}: {year_url}")
                try:
                    ry = safe_get(session, year_url)
                except Exception as e:
                    print(f"      Failed to fetch year page {year_url}: {e}")
                    continue
                if ry.status_code != 200:
                    print(f"      Year page returned {ry.status_code}")
                    continue

                year_soup = BeautifulSoup(ry.content, 'html.parser')
                track = ""
                paper_candidates = find_candidate_paper_links(year_soup, year_url)
                if not paper_candidates:
                    for a in year_soup.find_all('a', href=True):
                        href = a['href']
                        if '.pdf' in href.lower():
                            paper_candidates.append(normalize_url(year_url, href))
                if not paper_candidates:
                    print(f"      No candidate papers found on {year_url}")
                    continue
                if MAX_PAPERS_PER_YEAR:
                    paper_candidates = paper_candidates[:MAX_PAPERS_PER_YEAR]
                print(f"      Found {len(paper_candidates)} candidate paper URLs (heuristic).")

                for p_idx, paper_url in enumerate(paper_candidates, start=1):
                    print(f"        [{p_idx}/{len(paper_candidates)}] Visiting {paper_url}")
                    try:
                        rp = safe_get(session, paper_url)
                    except Exception as e:
                        print(f"           Failed to GET: {e}")
                        continue

                    # If this response is a PDF, don't parse with BeautifulSoup
                    if is_pdf_response(rp):
                        print("           Response is a PDF — recording pdf_url and skipping HTML parsing.")
                        pdf_url = paper_url if paper_url.lower().endswith('.pdf') else extract_pdf_url_from_page(BeautifulSoup(b"", 'html.parser'), paper_url) or paper_url
                        # Try to extract emails from PDF if pdfminer is available
                        pdf_authors = try_extract_from_pdf(session, pdf_url) if PDFMINER_AVAILABLE else []
                        if pdf_authors:
                            for a in pdf_authors:
                                append_row(OUTPUT_CSV, [SITE_NAME, year, conf_title, track, paper_url, pdf_url, a.get('email',''), a.get('name',''), a.get('affiliation','')])
                        else:
                            append_row(OUTPUT_CSV, [SITE_NAME, year, conf_title, track, paper_url, pdf_url, "", "", ""])
                        jitter_sleep()
                        continue

                    # Otherwise treat as HTML
                    try:
                        paper_soup = BeautifulSoup(rp.content, 'html.parser')
                    except Exception as e:
                        # If BeautifulSoup still rejects (rare), log and skip parsing as HTML
                        print(f"           BeautifulSoup parser error for {paper_url}: {e}")
                        append_row(OUTPUT_CSV, [SITE_NAME, year, conf_title, track, paper_url, "", "", "", ""])
                        jitter_sleep()
                        continue

                    pdf_url = extract_pdf_url_from_page(paper_soup, paper_url)
                    authors = extract_authors_from_meta(paper_soup)
                    mailtos = extract_mailto_links(paper_soup)
                    existing_emails = {a['email'] for a in authors if a.get('email')}
                    for m in mailtos:
                        if m['email'] not in existing_emails:
                            authors.append(m); existing_emails.add(m['email'])
                    if not authors:
                        fb = fallback_text_author_search(paper_soup)
                        for f in fb:

                            if f['email'] not in existing_emails:
                                authors.append(f); existing_emails.add(f['email'])
                    if (not authors or not any(a.get('email') for a in authors)) and pdf_url:
                        pdf_auths = try_extract_from_pdf(session, pdf_url) if PDFMINER_AVAILABLE else []
                        for pa in pdf_auths:
                            if pa['email'] not in existing_emails:
                                authors.append(pa); existing_emails.add(pa['email'])
                    if not authors:
                        title_tag = paper_soup.find(['h1','h2','title'])
                        paper_title = title_tag.get_text(" ", strip=True) if title_tag else ""
                        append_row(OUTPUT_CSV, [SITE_NAME, year, conf_title, track, paper_url, pdf_url or "", "", paper_title, ""])
                    else:
                        for a in authors:
                            name = a.get('name','') or ""
                            email = a.get('email','') or ""
                            aff = a.get('affiliation','') or ""
                            if not name and email:
                                local = email.split('@')[0]
                                name = local.replace('.', ' ').replace('_',' ').title()
                            append_row(OUTPUT_CSV, [SITE_NAME, year, conf_title, track, paper_url, pdf_url or "", email, name, aff])
                    jitter_sleep()
                jitter_sleep()
    except KeyboardInterrupt:
        print("\n\nKeyboardInterrupt received — exiting gracefully.")
        print("Partial results saved to:", os.path.abspath(OUTPUT_CSV))
        return

    print("\nScraping complete. Output CSV:", os.path.abspath(OUTPUT_CSV))

if __name__ == "__main__":
    print("PDFMiner available:", PDFMINER_AVAILABLE)
    scrape()
