#!/usr/bin/env python3
"""
acm_scraper.py - Scraper for ACM Proceedings (dl.acm.org/proceedings)
Extracts author emails, names, and affiliations from conference papers.
Output: acm_all_papers.csv
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import random
import os
import json
from urllib.parse import urljoin, urlparse, parse_qs

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
START_URL = "https://dl.acm.org/proceedings"
SITE_NAME = "ACM Digital Library"
OUTPUT_CSV = "acm_all_papers.csv"

# IMPORTANT: A specific User-Agent is less likely to be blocked.
USER_AGENT = "ACMExtractor/1.0 (Contact: user@example.com)"
REQUEST_TIMEOUT = 20  # Increased timeout for large files/slow response
DELAY_BASE = 3.0  # **Crucial: Be very polite to ACM**

# Testing limits
MAX_PROCEEDINGS_GROUPS = 2  # Max number of alphabetical groups (e.g., '3', 'd')
MAX_CONFERENCES_PER_GROUP = 3  # Max conferences to visit per group
MAX_PAPERS_PER_CONFERENCE = 5  # Max papers to scrape per conference


# ----------------------------

def make_session():
    """Create a requests session with proper headers."""
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def jitter_sleep():
    """Sleep with random jitter to be respectful."""
    time.sleep(DELAY_BASE + random.uniform(0, DELAY_BASE * 0.5))


def extract_emails_from_text(text):
    """Extract all email addresses from text."""
    # ACM often hides emails behind images or requires login, but we check text anyway
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    clean_emails = set()
    for email in emails:
        email = email.strip().lower()
        if '@' in email and '.' in email.split('@')[1]:
            clean_emails.add(email)
    return list(clean_emails)


def extract_emails_from_pdf(pdf_url, session):
    """Placeholder for PDF extraction - ACM PDF links often require cookies/login."""
    # ACM PDFs are often restricted or behind complex URL paths, making direct
    # PDF download via standard requests challenging without sophisticated session handling.
    print(f"      Skipping PDF extraction for ACM: {pdf_url}")
    return []


def discover_proceedings_groups(session, max_groups=None):
    """Discover alphabetical groups of proceedings (e.g., '3', 'pD')."""
    print(f"Discovering proceedings index from: {START_URL}")

    try:
        response = session.get(START_URL, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"Failed to fetch proceedings page: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching proceedings page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    proceedings_groups = []

    # Find the sections like '3', 'pD-Sec', '5G-MeMU' (these are accordion headers)
    groups = soup.find_all('div', class_=re.compile(r'proc-group-header-'))

    for group in groups:
        title = group.get_text(strip=True).split('\n')[0].split('(')[0].strip()

        # The actual conference links are inside the subsequent sibling (accordion body)
        # However, for simplicity and stability, we use the URL to the main proceedings page
        # as the context and rely on later scraping to find the conference links.
        proceedings_groups.append({
            'title': title,
            'url': START_URL  # We start processing from the main page and navigate internally
        })

    if max_groups:
        proceedings_groups = proceedings_groups[:max_groups]

    print(f"Found {len(proceedings_groups)} proceedings groups")
    return proceedings_groups


def scrape_proceedings_group(session, group_info, max_conferences=None):
    """Scrape conferences within a proceedings group."""
    group_title = group_info['title']

    print(f"\nScraping Group: {group_title}")

    # Refetch the main page to find the specific group content
    try:
        response = session.get(START_URL, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            return []
    except Exception as e:
        print(f"  Error fetching page for group {group_title}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    conference_links = []

    # Find the specific group header based on text
    target_header = soup.find('div', class_=re.compile(r'proc-group-header-'),
                              text=re.compile(r'^\s*' + re.escape(group_title.split(':')[0].strip()) + r'.*'))

    if target_header:
        # The list of conferences is in the sibling element (the accordion body)
        conference_list_div = target_header.find_next_sibling('div')
        if conference_list_div:
            # Find all conference links within that list
            for a in conference_list_div.find_all('a', href=True):
                if '/proceedings/' in a['href']:
                    conference_url = urljoin(START_URL, a['href'])
                    conference_title = a.get_text(strip=True)
                    conference_links.append({
                        'title': conference_title,
                        'url': conference_url
                    })

    if not conference_links:
        print(f"  No conferences found in group {group_title}")
        return []

    if max_conferences:
        conference_links = conference_links[:max_conferences]

    print(f"  Found {len(conference_links)} conferences in {group_title}")

    group_results = []
    for idx, conf in enumerate(conference_links, 1):
        print(f"    [{idx}/{len(conference_links)}] Visiting Conference: {conf['title'][:60]}...")

        # Scrape the individual conference page
        conf_results = scrape_conference_page(session, conf, MAX_PAPERS_PER_CONFERENCE)
        group_results.extend(conf_results)

        jitter_sleep()

    return group_results


def scrape_conference_page(session, conf_info, max_papers=None):
    """Scrape individual conference page for paper links."""
    conf_url = conf_info['url']
    conf_title = conf_info['title']

    try:
        response = session.get(conf_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"      Failed to fetch conference page: {response.status_code}")
            return []
    except Exception as e:
        print(f"      Error fetching conference page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    results = []

    # Find all paper links
    paper_links = []

    # ACM uses <a data-issue-id="..." href="/doi/abs/..."
    for a in soup.find_all('a', href=True, class_='issue-item-title'):
        href = a['href']
        if '/doi/' in href:
            paper_url = urljoin(conf_url, href)
            paper_title = a.get_text(strip=True)
            paper_links.append({'title': paper_title, 'url': paper_url})

    if not paper_links:
        print("      No paper links found on conference page. Likely a layout change.")
        return []

    if max_papers:
        paper_links = paper_links[:max_papers]

    print(f"      Found {len(paper_links)} papers to process.")

    for paper in paper_links:
        paper_results = scrape_paper_page(session, paper['url'], conf_title)
        results.extend(paper_results)
        jitter_sleep()

    return results


def scrape_paper_page(session, paper_url, conference_title):
    """Scrape individual paper page for authors and emails."""
    try:
        response = session.get(paper_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"        Failed to fetch paper page: {response.status_code}")
            return []
    except Exception as e:
        print(f"        Error fetching paper page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    results = []

    # --- Extract Metadata ---
    title_tag = soup.find('h1', class_='citation__title')
    paper_title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"

    year = "Unknown"
    date_tag = soup.find('span', class_='citation__date')
    if date_tag:
        year_match = re.search(r'(19\d{2}|20\d{2})', date_tag.get_text())
        year = year_match.group(1) if year_match else "Unknown"

    pdf_url = None
    # PDF link often involves a "fulltext" or "download" button with a complex URL
    pdf_button = soup.find('a', class_='issue-navigation__content-link', text=re.compile(r'PDF'))
    if pdf_button and 'href' in pdf_button.attrs:
        pdf_url = urljoin(paper_url, pdf_button['href'])

    # --- Extract Authors and Affiliations ---
    authors_data = []
    # Find all author list items
    author_list = soup.find_all('li', class_='author-list__item')

    for author_item in author_list:
        name_tag = author_item.find('a', class_='author-name')
        name = name_tag.get_text(strip=True) if name_tag else 'Unknown Author'

        # Affiliation is often in a sibling div
        aff_tag = author_item.find('div', class_='author-affiliation')
        affiliation = aff_tag.get_text(strip=True) if aff_tag else ''

        # ACM rarely exposes emails on the public page
        authors_data.append({'name': name, 'affiliation': affiliation, 'email': ''})

    # Since direct email extraction is hard on ACM, we record what we have
    if authors_data:
        for author in authors_data:
            # We skip PDF extraction here as ACM PDF URLs are often not directly downloadable
            # without proper session/cookie handling from the abstract page.

            results.append({
                'site': SITE_NAME,
                'year': year,
                'conference': conference_title,
                'track': paper_title,  # Using paper title as the 'track' for specificity
                'paper_url': paper_url,
                'pdf_url': pdf_url or '',
                'email': author['email'],  # Will be empty unless we get more sophisticated
                'name': author['name'],
                'affiliation': author['affiliation']
            })
    else:
        # Fallback for papers with no easily scraped author list
        results.append({
            'site': SITE_NAME,
            'year': year,
            'conference': conference_title,
            'track': paper_title,
            'paper_url': paper_url,
            'pdf_url': pdf_url or '',
            'email': '',
            'name': 'No Authors Found',
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
    print(f"{SITE_NAME} Scraper")
    print("=" * 60)
    # PDF extraction is disabled due to ACM complexity, but we print the status
    print(
        f"PDF extraction available: pdfminer={PDFMINER_AVAILABLE}, PyPDF2={PYPDF2_AVAILABLE if not PDFMINER_AVAILABLE else 'N/A'}")
    print("NOTE: PDF extraction is currently disabled due to ACM site restrictions.")
    print(f"Delay set to {DELAY_BASE} seconds to respect ACM servers.")
    print()

    # Clear old results
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
        print(f"Cleared previous {OUTPUT_CSV}")

    write_csv_header()

    session = make_session()

    # Discover all proceedings groups (e.g., those starting with 3, pD, 5G)
    groups = discover_proceedings_groups(session, max_groups=MAX_PROCEEDINGS_GROUPS)

    if not groups:
        print("No proceedings groups found! ACM site structure may have changed.")
        return

    print(f"\nStarting to scrape {len(groups)} proceedings groups...")

    total_authors = 0
    total_emails = 0  # Will likely be 0 for this version

    try:
        for idx, group_info in enumerate(groups, 1):
            print(f"\n[{idx}/{len(groups)}]")
            results = scrape_proceedings_group(session, group_info, max_conferences=MAX_CONFERENCES_PER_GROUP)

            if results:
                append_results(results)
                authors_found = len(results)
                emails_found = len([r for r in results if r['email']])
                total_authors += authors_found
                total_emails += emails_found
                print(f"  Saved {authors_found} author/paper records from this group.")

            jitter_sleep()

    except KeyboardInterrupt:
        print("\n\nKeyboardInterrupt - Stopping gracefully...")
        print(f"Partial results saved to: {OUTPUT_CSV}")
        return

    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Total author/paper records extracted: {total_authors}")
    print(f"Total emails extracted (expected low/zero): {total_emails}")
    print(f"Results saved to: {os.path.abspath(OUTPUT_CSV)}")


if __name__ == "__main__":
    main()