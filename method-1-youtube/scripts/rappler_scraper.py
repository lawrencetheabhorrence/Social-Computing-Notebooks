#!/usr/bin/env python3
import argparse, time, random, sys
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlencode, quote_plus

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
}

def build_listing_url(mode: str, slug_or_query: str, page: int) -> str:
    # Three base types for extra credit
    if mode == "topic":
        return f"https://www.rappler.com/topic/{slug_or_query}/page/{page}/"
    elif mode == "person":
        return f"https://www.rappler.com/persons/{slug_or_query}/page/{page}/"
    elif mode == "search":
        # Rappler WP search tends to use ?s=QUERY with &paged=N
        q = quote_plus(slug_or_query)
        return f"https://www.rappler.com/?s={q}&paged={page}"
    else:
        raise ValueError("mode must be one of: topic, person, search")

def extract_article(url: str) -> Optional[Dict]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"[WARN] Fetch failed {url}: {e}")
        return None

    soup = BeautifulSoup(r.content, "html.parser")

    # SELECTOR: Title
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    # SELECTOR: Date
    # often a <time> tag with datetime attr
    date_tag = soup.find("time", attrs={"datetime": True})
    date_published = date_tag["datetime"] if date_tag and date_tag.has_attr("datetime") else None

    # SELECTOR: Author
    author = None
    # common patterns
    author_tag = soup.find(attrs={"class": lambda c: c and ("author" in c or "byline" in c)})
    if author_tag:
        author = author_tag.get_text(" ", strip=True)

    # SELECTOR: Text content
    text_parts: List[str] = []
    # typical article body container; keep a couple of fallbacks:
    body = soup.find("div", class_="post-single__content") or \
           soup.find("div", class_="entry-content") or \
           soup.find("article")
    if body:
        for p in body.find_all(["p", "h2", "li"]):
            t = p.get_text(" ", strip=True)
            if t:
                text_parts.append(t)
    text = "\n".join(text_parts).strip() if text_parts else None

    # SELECTOR: Tags
    tags = []
    tags_container = soup.find(attrs={"class": lambda c: c and ("tags" in c or "post-tags" in c)})
    if tags_container:
        for a in tags_container.find_all("a"):
            t = a.get_text(strip=True)
            if t:
                tags.append(t)

    return {
        "link": url,
        "title": title,
        "date_published": date_published,
        "text": text,
        "author": author,
        "tags": ", ".join(tags) if tags else None,
    }

def get_article_links_from_listing(listing_url: str) -> List[str]:
    try:
        r = requests.get(listing_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"[WARN] Listing fetch failed {listing_url}: {e}")
        return []
    soup = BeautifulSoup(r.content, "html.parser")

    links = set()
    # SELECTOR: per-card links (anchor inside headlines)
    for a in soup.select("h3 a, h2 a, .post-card a"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.rappler.com" + href
        # basic filter to keep article links
        if "rappler.com" in href and "/video/" not in href:
            links.add(href)
    return list(links)

def scrape_rappler(mode: str, slug_or_query: str, pages: int) -> pd.DataFrame:
    rows = []
    for page in range(1, pages + 1):
        url = build_listing_url(mode, slug_or_query, page)
        print(f"[INFO] Listing page {page}: {url}")
        time.sleep(random.uniform(1.0, 2.0))
        article_links = get_article_links_from_listing(url)
        print(f"[INFO] Found {len(article_links)} article links on page {page}")
        for link in article_links:
            time.sleep(random.uniform(0.8, 1.5))
            data = extract_article(link)
            if data and data.get("title") and data.get("text"):
                rows.append(data)
    df = pd.DataFrame(rows, columns=[
        "link","title","date_published","text","author","tags"
    ])
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["topic","person","search"], required=True,
                    help="Type of Rappler listing to crawl")
    ap.add_argument("--slug", help="Slug for topic/person (e.g., 'pogo' or 'risa-hontiveros')")
    ap.add_argument("--query", help="Search query if mode=search")
    ap.add_argument("--pages", type=int, default=5, help="Number of listing pages to crawl (>=5)")
    ap.add_argument("--out", default="rappler_articles.xlsx", help="Excel output path")
    args = ap.parse_args()

    if args.mode in ("topic","person"):
        if not args.slug:
            ap.error("--slug is required for mode=topic/person")
        target = args.slug
    else:
        if not args.query:
            ap.error("--query is required for mode=search")
        target = args.query

    df = scrape_rappler(args.mode, target, args.pages)

    # Save only the base required columns in Sheet1
    base_cols = ["link","title","date_published","text"]
    base = df[base_cols]
    with pd.ExcelWriter(args.out, engine="openpyxl") as xw:
        base.to_excel(xw, index=False, sheet_name="articles_base")
        # extras on another sheet for extra points
        df.to_excel(xw, index=False, sheet_name="articles_with_extras")
    print(f"[OK] Saved to {args.out}")

if __name__ == "__main__":
    main()
