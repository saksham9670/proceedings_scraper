#!/usr/bin/env python3
"""
swj_scraper.py - Scraper for Semantic Web Journal (semantic-web-journal.net)
Extracts author emails, names, and affiliations from journal papers.
Output: swj_all_papers.csv
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
START_URL = "https://www.semantic-web-journal.net/issues"
SITE_NAME = "Semantic Web Journal"
OUTPUT_CSV = "swj_all_papers.csv"

USER_AGENT = "SWJ-Scraper/1.0"
REQUEST_TIMEOUT = 15
DELAY_BASE = 1.0

# Testing limits
MAX_YEARS = None  # Set to number for testing (e.g., 2)
MAX_PAPERS_PER_YEAR = None  # Set to number for testing (e.g., 3)


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
    clean_emails = set()
    for email in emails:
        email = email.strip().lower()
        if '@' in email and '.' in email.split('@')[1]:
            clean_emails.add(email)
    return list(clean_emails)


def extract_emails_from_pdf_pdfminer(pdf_url, session):
    """Extract emails from PDF using pdfminer.six."""
    try:
        print(f"        Extracting from PDF (pdfminer): {pdf_url}")
        response = session.get(pdf_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            return []

        text = extract_text(BytesIO(response.content))
        emails = extract_emails_from_text(text)

        results = []
        for email in emails:
            idx = text.find(email)
            name = ''
            if idx > 0:
                snippet = text[max(0, idx - 120):idx]
                words = re.findall(r'\b[A-Z][a-z]+\b', snippet)
                if words:
                    name = ' '.join(words[-2:]) if len(words) >= 2 else words[-1]
            results.append({'email': email, 'name': name, 'affiliation': ''})

        print(f"        Found {len(results)} emails in PDF")
        return results
    except Exception as e:
        print(f"        Error extracting from PDF: {e}")
        return []


def extract_emails_from_pdf_pypdf2(pdf_url, session):
    """Extract emails from PDF using PyPDF2."""
    try:
        print(f"        Extracting from PDF (PyPDF2): {pdf_url}")
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

        print(f"        Found {len(results)} emails in PDF")
        return results
    except Exception as e:
        print(f"        Error extracting from PDF: {e}")
        return []


def discover_year_issues(session, max_years=None):
    """Discover all year issues from the main issues page."""
    print(f"Discovering issues from: {START_URL}")

    try:
        response = session.get(START_URL, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"Failed to fetch issues page: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching issues page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    year_links = []

    # Find all year links (Issues in 2025, Issues in 2024, etc.)
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        # Pattern: "Issues in 2025", "Issues in 2024"
        match = re.search(r'Issues in (\d{4})', text)
        if match:
            year = match.group(1)
            year_url = urljoin(START_URL, a['href'])
            year_links.append({
                'year': year,
                'url': year_url,
                'title': text
            })

    # Remove duplicates and sort by year (descending)
    seen = set()
    unique_years = []
    for item in year_links:
        if item['year'] not in seen:
            seen.add(item['year'])
            unique_years.append(item)

    unique_years.sort(key=lambda x: int(x['year']), reverse=True)

    if max_years:
        unique_years = unique_years[:max_years]

    print(f"Found {len(unique_years)} years")
    return unique_years


def scrape_year_papers(session, year_info, max_papers=None):
    """Scrape all papers from a specific year."""
    year = year_info['year']
    year_url = year_info['url']

    print(f"\nScraping {year}: {year_url}")

    try:
        response = session.get(year_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"  Failed to fetch year page: {response.status_code}")
            return []
    except Exception as e:
        print(f"  Error fetching year page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    results = []

    # Find all paper links - they contain article titles and lead to individual paper pages
    paper_links = []

    # Look for links that lead to /content/ pages (individual papers)
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/content/' in href and not href.endswith('.pdf'):
            paper_url = urljoin(year_url, href)
            paper_title = a.get_text(strip=True)

            # Avoid duplicate links
            if paper_url not in [p['url'] for p in paper_links]:
                paper_links.append({'url': paper_url, 'title': paper_title})

    if not paper_links:
        print(f"  No papers found for {year}")
        return []

    if max_papers:
        paper_links = paper_links[:max_papers]

    print(f"  Found {len(paper_links)} papers")

    # Process each paper
    for idx, paper in enumerate(paper_links, 1):
        print(f"    [{idx}/{len(paper_links)}] {paper['title'][:60]}...")

        # Visit individual paper page to get author info
        paper_results = scrape_paper_page(session, paper['url'], year)
        results.extend(paper_results)

        jitter_sleep()

    print(f"  {year} complete: {len([r for r in results if r['email']])} emails found")
    return results


def scrape_paper_page(session, paper_url, year):
    """Scrape individual paper page for author information."""
    try:
        response = session.get(paper_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"      Failed to fetch paper page: {response.status_code}")
            return []
    except Exception as e:
        print(f"      Error fetching paper page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    results = []

    # Extract paper title
    title_tag = soup.find('h1') or soup.find('title')
    paper_title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"

    # Extract authors from the page - they're listed with names
    authors = []
    author_section = soup.find('div', class_='field-name-field-authors') or soup.find_all('div', class_='author')

    if author_section:
        # Find all author names in the author section
        if isinstance(author_section, list):
            for auth_div in author_section:
                name = auth_div.get_text(strip=True)
                if name:
                    authors.append({'name': name, 'email': '', 'affiliation': ''})
        else:
            author_text = author_section.get_text()
            # Split by common separators
            author_names = re.split(r'[,;]|\band\b', author_text)
            for name in author_names:
                name = name.strip()
                if name and len(name) > 2:
                    authors.append({'name': name, 'email': '', 'affiliation': ''})

    # Find PDF link
    pdf_url = None
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.pdf' in href.lower() or 'download' in href.lower():
            pdf_url = urljoin(paper_url, href)
            break

    # Extract emails from page text
    page_text = soup.get_text()
    emails_on_page = extract_emails_from_text(page_text)

    # If we have emails on page, match them with authors
    if emails_on_page:
        for i, email in enumerate(emails_on_page):
            name = ''
            if i < len(authors):
                name = authors[i]['name']
            elif authors:
                name = authors[0]['name']

            results.append({
                'site': SITE_NAME,
                'year': year,
                'conference': 'Semantic Web Journal',
                'track': f'Volume {year}',
                'paper_url': paper_url,
                'pdf_url': pdf_url or '',
                'email': email,
                'name': name,
                'affiliation': ''
            })

    # If no emails on page but we have PDF, try extracting from PDF
    elif pdf_url:
        if PDFMINER_AVAILABLE:
            pdf_authors = extract_emails_from_pdf_pdfminer(pdf_url, session)
        elif PYPDF2_AVAILABLE:
            pdf_authors = extract_emails_from_pdf_pypdf2(pdf_url, session)
        else:
            pdf_authors = []

        if pdf_authors:
            for i, pdf_auth in enumerate(pdf_authors):
                name = pdf_auth['name']
                if not name and i < len(authors):
                    name = authors[i]['name']

                results.append({
                    'site': SITE_NAME,
                    'year': year,
                    'conference': 'Semantic Web Journal',
                    'track': f'Volume {year}',
                    'paper_url': paper_url,
                    'pdf_url': pdf_url,
                    'email': pdf_auth['email'],
                    'name': name,
                    'affiliation': ''
                })
        else:
            # No emails found anywhere - record paper with author names only
            if authors:
                for author in authors:
                    results.append({
                        'site': SITE_NAME,
                        'year': year,
                        'conference': 'Semantic Web Journal',
                        'track': f'Volume {year}',
                        'paper_url': paper_url,
                        'pdf_url': pdf_url or '',
                        'email': '',
                        'name': author['name'],
                        'affiliation': ''
                    })
            else:
                # Record paper without any author info
                results.append({
                    'site': SITE_NAME,
                    'year': year,
                    'conference': 'Semantic Web Journal',
                    'track': f'Volume {year}',
                    'paper_url': paper_url,
                    'pdf_url': pdf_url or '',
                    'email': '',
                    'name': '',
                    'affiliation': ''
                })
    else:
        # No emails and no PDF - record author names if available
        if authors:
            for author in authors:
                results.append({
                    'site': SITE_NAME,
                    'year': year,
                    'conference': 'Semantic Web Journal',
                    'track': f'Volume {year}',
                    'paper_url': paper_url,
                    'pdf_url': '',
                    'email': '',
                    'name': author['name'],
                    'affiliation': ''
                })

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
    print("Semantic Web Journal Scraper")
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

    # Discover all years
    years = discover_year_issues(session, max_years=MAX_YEARS)

    if not years:
        print("No years found!")
        return

    print(f"\nStarting to scrape {len(years)} years...")

    total_emails = 0

    try:
        for idx, year_info in enumerate(years, 1):
            print(f"\n[{idx}/{len(years)}]")
            results = scrape_year_papers(session, year_info, max_papers=MAX_PAPERS_PER_YEAR)

            if results:
                append_results(results)
                emails_found = len([r for r in results if r['email']])
                total_emails += emails_found
                print(f"  Saved {emails_found} emails from {year_info['year']}")

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