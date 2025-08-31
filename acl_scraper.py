import requests
from bs4 import BeautifulSoup
import re
import PyPDF2
from io import BytesIO
import time
from urllib.parse import urljoin
import os


def extract_emails_from_pdf(pdf_url):
    """Download a PDF and extract emails from it."""
    try:
        print(f"    Processing PDF: {pdf_url}")
        response = requests.get(pdf_url, timeout=15)
        response.raise_for_status()

        pdf_file = BytesIO(response.content)
        reader = PyPDF2.PdfReader(pdf_file)

        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""

        emails = set()
        email_matches = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
        for email in email_matches:
            clean_email = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", email)
            if clean_email:
                emails.add(clean_email.group(1))

        print(f"    Found {len(emails)} emails in PDF")
        return emails

    except Exception as e:
        print(f"    Error processing PDF {pdf_url}: {e}")
        return set()


def discover_all_conferences_for_year(year, max_discoveries=None):
    """Dynamically discover ALL conferences for a given year."""
    print(f"\nDiscovering ALL conferences for year {year}...")

    discovered_conferences = []

    # Strategy 1: Crawl volumes directory
    try:
        volumes_url = "https://aclanthology.org/volumes/"
        response = requests.get(volumes_url, timeout=15)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")

            for link in soup.find_all("a", href=True):
                href = link["href"]

                # Find URLs containing the year but NOT individual papers (ending with numbers)
                if (str(year) in href and
                        "/volumes/" in href and
                        not re.search(r'\.\d+/?$', href) and
                        href.endswith('/')):

                    full_url = urljoin(volumes_url, href)
                    conf_info = extract_conference_info_from_url(full_url, year)

                    if conf_info and check_url_exists(full_url):
                        discovered_conferences.append((full_url, conf_info['conference'], conf_info['track']))
                        print(f"  Found: {conf_info['conference']} ({conf_info['track']}) - {full_url}")

                        if max_discoveries and len(discovered_conferences) >= max_discoveries:
                            print(f"  Limited to {max_discoveries} discoveries for testing")
                            break

    except Exception as e:
        print(f"Error discovering volumes: {e}")

    # Strategy 2: Crawl events directory
    try:
        events_url = "https://aclanthology.org/events/"
        response = requests.get(events_url, timeout=15)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")

            for link in soup.find_all("a", href=True):
                href = link["href"]

                if (str(year) in href and
                        "/events/" in href and
                        not re.search(r'\.\d+/?$', href)):

                    full_url = urljoin(events_url, href)
                    conf_info = extract_conference_info_from_url(full_url, year)

                    if conf_info and check_url_exists(full_url):
                        discovered_conferences.append((full_url, conf_info['conference'], conf_info['track']))
                        print(f"  Found: {conf_info['conference']} ({conf_info['track']}) - {full_url}")

                        if max_discoveries and len(discovered_conferences) >= max_discoveries:
                            break

    except Exception as e:
        print(f"Error discovering events: {e}")

    # Remove duplicates
    discovered_conferences = list(set(discovered_conferences))

    print(f"Total discovered for {year}: {len(discovered_conferences)} conferences")
    return discovered_conferences


def extract_conference_info_from_url(url, year):
    """Extract conference name and track info from ANY URL pattern."""
    try:
        # Remove trailing slash for consistent processing
        clean_url = url.rstrip('/')

        # Pattern 1: Modern format like 2023.acl-main, 2023.emnlp-industry
        match = re.search(rf"{year}\.([^-/]+)-(.+?)/?$", clean_url)
        if match:
            conf_name = match.group(1).upper()
            track = match.group(2).title()
            return {'conference': conf_name, 'track': track}

        # Pattern 2: Complex names like 2003.jeptalnrecital-tutorial
        match = re.search(rf"{year}\.([^/]+)/?$", clean_url)
        if match:
            full_name = match.group(1)
            if '-' in full_name:
                parts = full_name.split('-')
                conf_name = parts[0].upper()
                track = '-'.join(parts[1:]).title()
            else:
                conf_name = full_name.upper()
                track = "Main"
            return {'conference': conf_name, 'track': track}

        # Pattern 3: Old format like P23, N23, W00-13
        match = re.search(r"/([A-Z])(\d{2})-?(\w*)/?$", clean_url)
        if match:
            letter = match.group(1)
            track_info = match.group(3) if match.group(3) else "Main"

            conf_map = {
                'P': 'ACL', 'N': 'NAACL', 'E': 'EACL',
                'D': 'EMNLP', 'C': 'COLING', 'W': 'Workshop'
            }
            conf_name = conf_map.get(letter, f"Conference-{letter}")
            return {'conference': conf_name, 'track': track_info.title()}

        # Pattern 4: Event-based like /events/acl-2023/
        match = re.search(r"/events/([^-/]+)-(\d+)/?$", clean_url)
        if match:
            conf_name = match.group(1).upper()
            return {'conference': conf_name, 'track': 'Event'}

        return None

    except Exception as e:
        print(f"Error extracting info from URL {url}: {e}")
        return None


def scrape_conference_page(url, conference_name, track, year, max_papers=None):
    """Scrape a conference page to find individual paper pages."""
    print(f"\nScraping {year} {conference_name} {track}: {url}")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "lxml")

    # Find individual paper page links
    paper_links = []

    for link in soup.find_all("a", href=True):
        href = link["href"]

        # Look for individual paper patterns
        patterns = [
            rf"/{year}\.[^/]+\.\d+/?$",  # 2025.acl-main.123/
            r"/[A-Z]\d{2}-\d+/?$",  # P23-1001/
            r"/W\d{2}-\d+/?$",  # W00-1301/
        ]

        for pattern in patterns:
            if re.search(pattern, href):
                full_url = urljoin(url, href)
                paper_links.append(full_url)
                break

    # Remove duplicates and apply limit
    paper_links = list(set(paper_links))

    if max_papers and len(paper_links) > max_papers:
        print(f"  Limiting to {max_papers} papers for faster testing")
        paper_links = paper_links[:max_papers]

    print(f"Found {len(paper_links)} individual paper pages")

    # Process each paper page (NESTED PROCESSING)
    results = []

    for i, paper_url in enumerate(paper_links, 1):
        print(f"Processing paper {i}/{len(paper_links)}: {paper_url}")

        # NESTED: Visit individual paper page
        paper_data = extract_author_info_from_paper_page(paper_url, year, conference_name)
        if not paper_data:
            continue

        # Extract emails from PDF
        if paper_data['pdf_link']:
            pdf_emails = extract_emails_from_pdf(paper_data['pdf_link'])

            for j, email in enumerate(pdf_emails):
                name = "Unknown"
                affiliation = "Unknown"

                if paper_data['authors_info'] and j < len(paper_data['authors_info']):
                    author_info = paper_data['authors_info'][j]
                    name = author_info['name']
                    affiliation = author_info['affiliation']
                elif paper_data['authors_info']:
                    author_info = paper_data['authors_info'][0]
                    name = author_info['name']
                    affiliation = author_info['affiliation']

                results.append({
                    "site": "ACL Anthology",
                    "year": str(year),
                    "conference": conference_name,
                    "track": track,
                    "paper_url": paper_url,
                    "pdf_url": paper_data['pdf_link'],
                    "email": email,
                    "name": name,
                    "affiliation": affiliation
                })

        time.sleep(0.3)  # Be respectful to server

    return results


def extract_author_info_from_paper_page(paper_url, year, conference):
    """NESTED: Extract author info from individual paper page."""
    try:
        print(f"  Scraping paper page: {paper_url}")
        response = requests.get(paper_url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")

        authors_info = []

        # Look for author links
        author_links = soup.find_all("a", href=re.compile(r"/people/"))

        for link in author_links:
            author_name = link.get_text().strip()
            if author_name:
                authors_info.append({
                    'name': author_name,
                    'affiliation': "Unknown"
                })

        # Extract emails from page
        page_text = soup.get_text()
        emails_on_page = set()
        email_matches = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", page_text)
        for email in email_matches:
            clean_email = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", email)
            if clean_email:
                emails_on_page.add(clean_email.group(1))

        # Find PDF link
        pdf_link = None
        for link in soup.find_all("a", href=True):
            if link["href"].endswith(".pdf"):
                if link["href"].startswith("http"):
                    pdf_link = link["href"]
                else:
                    pdf_link = urljoin("https://aclanthology.org", link["href"])
                break

        print(f"  Found {len(authors_info)} authors, {len(emails_on_page)} emails on page")

        return {
            'authors_info': authors_info,
            'emails_on_page': emails_on_page,
            'pdf_link': pdf_link
        }

    except Exception as e:
        print(f"  Error processing paper page {paper_url}: {e}")
        return None


def scrape_acl_dynamic(start_year, end_year, max_conferences_per_year=None, max_papers_per_conference=None):
    """Main function: Dynamic discovery and scraping with testing controls."""
    all_results = []

    for year in range(start_year, end_year + 1):
        print(f"\n=== YEAR {year} ===")

        # Discover all conferences for this year
        discovered_conferences = discover_all_conferences_for_year(year, max_conferences_per_year)

        if not discovered_conferences:
            print(f"No conferences found for {year}")
            continue

        # Process each discovered conference
        for url, conference_name, track in discovered_conferences:
            try:
                conf_results = scrape_conference_page(url, conference_name, track, year, max_papers_per_conference)
                all_results.extend(conf_results)
                print(f"{year} {conference_name} {track}: {len(conf_results)} emails found")

            except Exception as e:
                print(f"Error scraping {year} {conference_name}: {e}")
                continue

        year_emails = len([r for r in all_results if r['year'] == str(year)])
        print(f"Year {year} complete: {year_emails} total emails")

    return all_results


def check_url_exists(url):
    """Check if a URL exists without downloading full content."""
    try:
        response = requests.head(url, timeout=5, allow_redirects=True)
        return response.status_code == 200
    except:
        return False


# Wrapper for backward compatibility
def scrape_acl_robust(start_year, end_year):
    """Backward compatibility wrapper."""
    return scrape_acl_dynamic(start_year, end_year, max_conferences_per_year=5, max_papers_per_conference=10)