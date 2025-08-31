from acl_scraper import scrape_acl_dynamic
import csv
import os
from datetime import datetime

if __name__ == "__main__":
    print("Dynamic ACL Anthology Scraper")
    print("=" * 50)
    print("Discovers ALL conferences and tracks automatically")
    print("Includes testing controls for faster runs")
    print()

    # Clear previous results file at start
    results_file = "results_dynamic.csv"
    if os.path.exists(results_file):
        os.remove(results_file)
        print(f"Previous {results_file} cleared")

    # Get user input for year range
    try:
        start_year = int(input("Enter start year (e.g., 2023): "))
        end_year = int(input("Enter end year (e.g., 2024): "))

        if start_year > end_year:
            print("Start year cannot be greater than end year!")
            exit()

        if start_year < 1979:
            print("ACL Anthology data starts from 1979. Setting start year to 1979.")
            start_year = 1979

        # Testing controls for faster runs
        print("\nTESTING CONTROLS (for faster execution):")
        print("Enter 0 or press Enter for no limits (full scraping)")

        try:
            max_conf = input("Max conferences per year (e.g., 3): ").strip()
            max_conf = int(max_conf) if max_conf and max_conf != "0" else None

            max_papers = input("Max papers per conference (e.g., 5): ").strip()
            max_papers = int(max_papers) if max_papers and max_papers != "0" else None

        except ValueError:
            max_conf = None
            max_papers = None
            print("Using no limits (full scraping)")

        print(f"\nDynamic scraping from {start_year} to {end_year}")
        if max_conf:
            print(f"Limited to {max_conf} conferences per year")
        if max_papers:
            print(f"Limited to {max_papers} papers per conference")
        print()

    except ValueError:
        print("Please enter valid years!")
        exit()

    # Record start time
    start_time = datetime.now()

    # Run dynamic scraping
    all_results = scrape_acl_dynamic(
        start_year,
        end_year,
        max_conferences_per_year=max_conf,
        max_papers_per_conference=max_papers
    )

    if all_results:
        print(f"\nFound {len(all_results)} total emails. Saving to {results_file}...")

        # Save to CSV (overwrites previous file)
        with open(results_file, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["site", "year", "conference", "track", "paper_url", "pdf_url", "email", "name", "affiliation"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)

        print(f"Results saved in {results_file}")

        # Show detailed summary
        summary = {}
        conference_tracks = {}

        for result in all_results:
            # Summary by year-conference-track
            key = f"{result['year']} {result['conference']} ({result['track']})"
            if key not in summary:
                summary[key] = 0
            summary[key] += 1

            # Track unique conferences discovered
            conf_key = f"{result['conference']} ({result['track']})"
            if conf_key not in conference_tracks:
                conference_tracks[conf_key] = set()
            conference_tracks[conf_key].add(result['year'])

        print(f"\nDISCOVERY SUMMARY:")
        print(f"Total unique conferences/tracks discovered: {len(conference_tracks)}")
        print(f"Total emails extracted: {len(all_results)}")

        print("\nDiscovered Conferences/Tracks:")
        for conf_track in sorted(conference_tracks.keys()):
            years = sorted(conference_tracks[conf_track])
            year_range = f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0])
            print(f"  {conf_track}: {year_range}")

        print("\nResults by year, conference, and track:")
        for conf in sorted(summary.keys()):
            print(f"  {conf}: {summary[conf]} emails")

        # Show sample results
        print("\nSample results:")
        for i, result in enumerate(all_results[:5]):
            print(
                f"{i + 1}. {result['name']} - {result['email']} - {result['year']} {result['conference']} ({result['track']})")

        # Calculate execution time
        end_time = datetime.now()
        execution_time = end_time - start_time
        print(f"\nExecution time: {execution_time}")

    else:
        print("No emails found in the specified year range.")

    print(f"\nDynamic scraping complete!")
    print(f"Tip: Use smaller limits for testing, remove limits for full data collection")