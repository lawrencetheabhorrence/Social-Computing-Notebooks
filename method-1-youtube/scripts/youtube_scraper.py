#!/usr/bin/env python3
import argparse, time, random, os, sys
from typing import List, Dict, Tuple, Optional
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
from tqdm import tqdm

BASE_COLUMNS = ['title', 'link', 'date_published', 'text', 'like_count', 'reply_parent_id']

EXTRA_COLUMNS = [
    'channel_id','channel_title','video_id','video_title','comment_id','author','is_reply'
]

def build_youtube():
    load_dotenv()
    key = os.getenv("YT_API_KEY")
    if not key:
        print("[ERROR] YT_API_KEY not found. Create a .env with YT_API_KEY=...")
        sys.exit(1)
    return build('youtube', 'v3', developerKey=key)

def search_videos(youtube, keywords: str, max_results: int = 50, order='relevance') -> List[Dict]:
    # returns search items for videos only
    res = youtube.search().list(
        q=keywords, part='snippet', type='video', maxResults=max_results, order=order
    ).execute()
    return res.get('items', [])

def video_stats(youtube, video_ids: List[str]) -> Dict[str, Dict]:
    out = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        res = youtube.videos().list(
            id=",".join(chunk), part='snippet,statistics'
        ).execute()
        for it in res.get('items', []):
            vid = it['id']
            stats = it.get('statistics', {})
            snip = it.get('snippet', {})
            out[vid] = {
                'commentCount': int(stats.get('commentCount', 0)),
                'channelId': snip.get('channelId'),
                'channelTitle': snip.get('channelTitle'),
                'title': snip.get('title'),
                'publishedAt': snip.get('publishedAt')
            }
    return out

def pick_5_channels_5_videos(youtube, keywords: str, min_comments: int = 25) -> List[Tuple[str, List[str]]]:
    """
    Returns list of tuples: (channelId, [video_ids...]) where each list has 5 videos
    Each candidate video must have >= min_comments (as reported by YouTube stats).
    """
    items = search_videos(youtube, keywords, max_results=50, order='relevance')
    video_ids = [it['id']['videoId'] for it in items]
    stats = video_stats(youtube, video_ids)

    # group by channel
    by_channel: Dict[str, List[str]] = {}
    for vid, meta in stats.items():
        if meta['commentCount'] >= min_comments:
            ch = meta['channelId']
            by_channel.setdefault(ch, []).append(vid)

    # prefer videos with more comments
    for ch, vids in by_channel.items():
        vids.sort(key=lambda v: stats[v]['commentCount'], reverse=True)
        by_channel[ch] = vids[:5]  # top 5

    # select 5 channels that have 5 qualifying videos
    selected = [(ch, vids) for ch, vids in by_channel.items() if len(vids) == 5]
    if len(selected) >= 5:
        return selected[:5]

    # If not enough, fetch more search pages (by switching order and keyword variants)
    variants = [order for order in ('viewCount','date','rating')]
    for ordv in variants:
        items2 = search_videos(youtube, keywords, max_results=50, order=ordv)
        video_ids2 = [it['id']['videoId'] for it in items2]
        stats2 = video_stats(youtube, video_ids2)
        for vid, meta in stats2.items():
            if meta['commentCount'] >= min_comments:
                ch = meta['channelId']
                by_channel.setdefault(ch, [])
                if vid not in by_channel[ch]:
                    by_channel[ch].append(vid)
                    by_channel[ch] = sorted(by_channel[ch],
                        key=lambda v: (stats.get(v, stats2.get(v))['commentCount']), reverse=True)[:5]
        selected = [(ch, vids) for ch, vids in by_channel.items() if len(vids) == 5]
        if len(selected) >= 5:
            return selected[:5]

    # final fallback: take top 5 channels with the most qualifying videos, pad with fewer if needed
    fallback = sorted(by_channel.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]
    return fallback

def fetch_comments_for_video(youtube, video_id: str, target: int = 150) -> List[Dict]:
    """
    Fetch top-level comments and replies up to `target` rows (best-effort).
    """
    rows: List[Dict] = []
    next_page = None
    fetched = 0

    while True:
        kwargs = dict(videoId=video_id, part='snippet,replies', maxResults=100, order='relevance')
        if next_page:
            kwargs['pageToken'] = next_page
        resp = youtube.commentThreads().list(**kwargs).execute()

        for item in resp.get('items', []):
            top = item['snippet']['topLevelComment']
            c = top['snippet']
            # base + extras
            rows.append({
                'title': c.get('textDisplay', ''),
                'link': f"https://www.youtube.com/watch?v={video_id}&lc={top['id']}",
                'date_published': c.get('publishedAt'),
                'text': c.get('textOriginal', ''),
                'like_count': c.get('likeCount', 0),
                'reply_parent_id': None,
                'channel_id': c.get('authorChannelId', {}).get('value'),
                'channel_title': c.get('authorDisplayName'),
                'video_id': video_id,
                'video_title': None,  # fill later
                'comment_id': top['id'],
                'author': c.get('authorDisplayName'),
                'is_reply': False
            })
            fetched += 1
            # replies
            for reply in item.get('replies', {}).get('comments', []):
                rc = reply['snippet']
                rows.append({
                    'title': rc.get('textDisplay', ''),
                    'link': f"https://www.youtube.com/watch?v={video_id}&lc={reply['id']}",
                    'date_published': rc.get('publishedAt'),
                    'text': rc.get('textOriginal', ''),
                    'like_count': rc.get('likeCount', 0),
                    'reply_parent_id': top['id'],
                    'channel_id': rc.get('authorChannelId', {}).get('value'),
                    'channel_title': rc.get('authorDisplayName'),
                    'video_id': video_id,
                    'video_title': None,
                    'comment_id': reply['id'],
                    'author': rc.get('authorDisplayName'),
                    'is_reply': True
                })
                fetched += 1

            if fetched >= target:
                break
        if fetched >= target:
            break

        next_page = resp.get('nextPageToken')
        if not next_page:
            break

        time.sleep(random.uniform(0.4, 0.8))

    return rows

def fill_video_titles(youtube, rows: List[Dict]):
    # collect unique video_ids
    vids = sorted(set(r['video_id'] for r in rows if r['video_id']))
    meta = {}
    for i in range(0, len(vids), 50):
        chunk = vids[i:i+50]
        res = youtube.videos().list(id=",".join(chunk), part='snippet').execute()
        for it in res.get('items', []):
            meta[it['id']] = it['snippet'].get('title')
    for r in rows:
        if r['video_id'] in meta:
            r['video_title'] = meta[r['video_id']]

def build_corpus_df(rows: List[Dict]) -> pd.DataFrame:
    base_df = pd.DataFrame(rows, columns=BASE_COLUMNS + EXTRA_COLUMNS)
    # Ensure base columns are present even if extras are None
    for col in BASE_COLUMNS:
        if col not in base_df.columns:
            base_df[col] = None
    return base_df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keywords", required=True, help="Topic/person/search keywords (e.g., 'pogo alice guo rappler')")
    ap.add_argument("--channels", type=int, default=5, help="Number of channels to include (default 5)")
    ap.add_argument("--videos-per-channel", type=int, default=5, help="Videos per channel (default 5)")
    ap.add_argument("--min-comments", type=int, default=25, help="Minimum total comments a video must have to qualify")
    ap.add_argument("--target-per-video", type=int, default=150, help="Target number of comments to scrape per video")
    ap.add_argument("--out", default="youtube_comments.xlsx", help="Excel output path")
    args = ap.parse_args()

    youtube = build_youtube()

    # choose 5 channels × 5 videos (>= min comments)
    selected = pick_5_channels_5_videos(youtube, args.keywords, min_comments=args.min_comments)
    if len(selected) == 0:
        print("[ERROR] No channels/videos found with the given constraints.")
        sys.exit(2)

    # Limit to requested number
    selected = selected[:args.channels]
    for ch, vids in selected:
        if len(vids) > args.videos_per_channel:
            vids[:] = vids[:args.videos_per_channel]

    # fetch comments
    all_rows: List[Dict] = []
    pbar = tqdm(total=sum(len(v) for _, v in selected), desc="Fetching video comments")
    for _, vids in selected:
        for vid in vids:
            rows = fetch_comments_for_video(youtube, vid, target=args.target_per_video)
            all_rows.extend(rows)
            pbar.update(1)
            time.sleep(random.uniform(0.2, 0.5))
    pbar.close()

    # fill video titles
    fill_video_titles(youtube, all_rows)

    df = build_corpus_df(all_rows)

    # Base sheet (exact columns required)
    base_df = df[['title','link','date_published','text','like_count','reply_parent_id']]
    with pd.ExcelWriter(args.out, engine="openpyxl") as xw:
        base_df.to_excel(xw, index=False, sheet_name="youtube_base")
        df.to_excel(xw, index=False, sheet_name="youtube_with_extras")
    print(f"[OK] Saved to {args.out}")

if __name__ == "__main__":
    main()
