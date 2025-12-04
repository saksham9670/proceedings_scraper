#!/usr/bin/env python3
"""
ceur_scraper.py - Scraper for CEUR Workshop Proceedings (ceur-ws.org)
Extracts author emails, names, and affiliations from workshop proceedings.
Output: ceur_all_papers.csv
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import random
import os
from urllib.parse import urljoin
from io import BytesIO

# Try to import PDF parsing library
try:
    from pdfminer.high_level import extract_text

    PDFMINER_AVAILABLE = True
except ImportError:
    try:
        import PyPDF2

        PDFMINER_AVAILABLE = False
        PYPDF2_AVAILABLE = True
    except ImportError:
        PYPDF2_AVAILABLE = False

# ---------- CONFIG ----------
START_URL = "https://ceur-ws.org/"
SITE_NAME = "CEUR-WS"
OUTPUT_CSV = "ceur_all_papers.csv"

USER_AGENT = "CEUR-Scraper/1.0"
REQUEST_TIMEOUT = 15
DELAY_BASE = 1.0  # Be respectful to CEUR servers

# Testing limits
MAX_VOLUMES = None  # Set to number for testing (e.g., 5)
MAX_PAPERS_PER_VOLUME = None  # Set to number for testing (e.g., 3)


# ----------------------------


def make_session():
    """Create a requests session with proper headers."""
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def jitter_sleep():
    """Sleep with random jitter to be respectful."""
    time.sleep(DELAY_BASE + random.uniform(0, DELAY_BASE * 0.3))


def extract_emails_from_text(text):
    """Extract all email addresses from text."""
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    # Clean and deduplicate
    clean_emails = set()
    for email in emails:
        email = email.strip().lower()
        if '@' in email and '.' in email.split('@')[1]:
            clean_emails.add(email)
    return list(clean_emails)


def extract_mailto_links(soup):
    """Extract emails from mailto: links."""
    emails = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('mailto:'):
            email = href.split(':')[1].split('?')[0].strip().lower()
            if email and '@' in email:
                name = a.get_text(strip=True) or ''
                emails.append({'email': email, 'name': name})
    return emails


def extract_emails_from_pdf_pdfminer(pdf_url, session):
    """Extract emails from PDF using pdfminer.six."""
    try:
        print(f"      Extracting from PDF (pdfminer): {pdf_url}")
        response = session.get(pdf_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            return []

        text = extract_text(BytesIO(response.content))
        emails = extract_emails_from_text(text)

        # Try to guess author names near emails
        results = []
        for email in emails:
            idx = text.find(email)
            name = ''
            if idx > 0:
                # Get text before email
                snippet = text[max(0, idx - 120):idx]
                # Find capitalized words (likely names)
                words = re.findall(r'\b[A-Z][a-z]+\b', snippet)
                if words:
                    name = ' '.join(words[-2:]) if len(words) >= 2 else words[-1]
            results.append({'email': email, 'name': name, 'affiliation': ''})

        print(f"      Found {len(results)} emails in PDF")
        return results
    except Exception as e:
        print(f"      Error extracting from PDF: {e}")
        return []


def extract_emails_from_pdf_pypdf2(pdf_url, session):
    """Extract emails from PDF using PyPDF2."""
    try:
        print(f"      Extracting from PDF (PyPDF2): {pdf_url}")
        response = session.get(pdf_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            return []

        reader = PyPDF2.PdfReader(BytesIO(response.content))
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""

        emails = extract_emails_from_text(text)
        results = []
        for email in emails:
            results.append({'email': email, 'name': '', 'affiliation': ''})

        print(f"      Found {len(results)} emails in PDF")
        return results
    except Exception as e:
        print(f"      Error extracting from PDF: {e}")
        return []


def discover_volumes(session, max_volumes=None):
    """Discover all workshop volumes from the main page."""
    print(f"Discovering volumes from: {START_URL}")

    try:
        response = session.get(START_URL, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"Failed to fetch main page: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching main page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    volumes = []

    # CEUR lists volumes with "Vol-XXXX" pattern
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text()

        # Look for volume links like "Vol-4120" or "Vol-4119"
        if re.search(r'Vol-\d{4}', text) or re.search(r'Vol-\d{4}', href):
            vol_match = re.search(r'Vol-(\d{4})', text + href)
            if vol_match:
                vol_num = vol_match.group(1)
                vol_url = urljoin(START_URL, href)

                # Get workshop title if available
                title = text if text and 'Vol-' not in text else f"Volume {vol_num}"

                volumes.append({
                    'volume': vol_num,
                    'url': vol_url,
                    'title': title
                })

    # Remove duplicates based on volume number
    seen = set()
    unique_volumes = []
    for vol in volumes:
        if vol['volume'] not in seen:
            seen.add(vol['volume'])
            unique_volumes.append(vol)

    # Sort by volume number (descending - newest first)
    unique_volumes.sort(key=lambda x: int(x['volume']), reverse=True)

    if max_volumes:
        unique_volumes = unique_volumes[:max_volumes]

    print(f"Found {len(unique_volumes)} volumes")
    return unique_volumes


def scrape_volume(session, volume_info, max_papers=None):
    """Scrape a single workshop volume."""
    vol_num = volume_info['volume']
    vol_url = volume_info['url']
    vol_title = volume_info['title']

    print(f"\nScraping Vol-{vol_num}: {vol_title}")
    print(f"  URL: {vol_url}")

    try:
        response = session.get(vol_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"  Failed to fetch volume page: {response.status_code}")
            return []
    except Exception as e:
        print(f"  Error fetching volume: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    results = []

    # Extract year from page
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', soup.get_text())
    year = year_match.group(1) if year_match else "Unknown"

    # Find all paper links (PDFs)
    paper_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith('.pdf'):
            paper_url = urljoin(vol_url, href)
            paper_title = a.get_text(strip=True)
            paper_links.append({'url': paper_url, 'title': paper_title})

    if not paper_links:
        print(f"  No papers found in Vol-{vol_num}")
        return []

    if max_papers:
        paper_links = paper_links[:max_papers]

    print(f"  Found {len(paper_links)} papers")

    # Process each paper
    for idx, paper in enumerate(paper_links, 1):
        print(f"    [{idx}/{len(paper_links)}] {paper['title'][:60]}...")

        # Try to extract emails from PDF
        authors = []

        if PDFMINER_AVAILABLE:
            authors = extract_emails_from_pdf_pdfminer(paper['url'], session)
        elif PYPDF2_AVAILABLE:
            authors = extract_emails_from_pdf_pypdf2(paper['url'], session)

        if authors:
            for author in authors:
                results.append({
                    'site': SITE_NAME,
                    'year': year,
                    'conference': vol_title,
                    'track': f"Vol-{vol_num}",
                    'paper_url': paper['url'],
                    'pdf_url': paper['url'],
                    'email': author['email'],
                    'name': author['name'],
                    'affiliation': author.get('affiliation', '')
                })
        else:
            # No emails found - record paper anyway
            results.append({
                'site': SITE_NAME,
                'year': year,
                'conference': vol_title,
                'track': f"Vol-{vol_num}",
                'paper_url': paper['url'],
                'pdf_url': paper['url'],
                'email': '',
                'name': '',
                'affiliation': ''
            })

        jitter_sleep()

    print(f"  Vol-{vol_num} complete: {len([r for r in results if r['email']])} emails found")
    return results


def write_csv_header():
    """Write CSV header if file doesn't exist."""
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['site', 'year', 'conference', 'track', 'paper_url',
                             'pdf_url', 'email', 'name', 'affiliation'])


def append_results(results):
    """Append results to CSV file."""
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for r in results:
            writer.writerow([
                r['site'], r['year'], r['conference'], r['track'],
                r['paper_url'], r['pdf_url'], r['email'], r['name'], r['affiliation']
            ])


def main():
    """Main scraping function."""
    print("=" * 60)
    print("CEUR Workshop Proceedings Scraper")
    print("=" * 60)
    print(
        f"PDF extraction available: pdfminer={PDFMINER_AVAILABLE}, PyPDF2={PYPDF2_AVAILABLE if not PDFMINER_AVAILABLE else 'N/A'}")
    print()

    # Clear old results
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
        print(f"Cleared previous {OUTPUT_CSV}")

    write_csv_header()

    session = make_session()

    # Discover all volumes
    volumes = discover_volumes(session, max_volumes=MAX_VOLUMES)

    if not volumes:
        print("No volumes found!")
        return

    print(f"\nStarting to scrape {len(volumes)} volumes...")

    total_emails = 0

    try:
        for idx, vol_info in enumerate(volumes, 1):
            print(f"\n[{idx}/{len(volumes)}]")
            results = scrape_volume(session, vol_info, max_papers=MAX_PAPERS_PER_VOLUME)

            if results:
                append_results(results)
                emails_found = len([r for r in results if r['email']])
                total_emails += emails_found
                print(f"  Saved {emails_found} emails from this volume")

            jitter_sleep()

    except KeyboardInterrupt:
        print("\n\nKeyboardInterrupt - Stopping gracefully...")
        print(f"Partial results saved to: {OUTPUT_CSV}")
        return

    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Total emails extracted: {total_emails}")
    print(f"Results saved to: {os.path.abspath(OUTPUT_CSV)}")


if __name__ == "__main__":
    main()