import os, time, random
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build

TARGET_CHANNEL_NAMES = [
    "Rappler", "GMA News", "ABS-CBN News", "News5", "INQUIRER.net"
]
KEYWORDS = "POGO Philippines Senate hearing Rappler"
MIN_COMMENTS = 25
TARGET_PER_VIDEO = 120
OUT = "youtube_comments.xlsx"

BASE_COLUMNS = ['title','link','date_published','text','like_count','reply_parent_id']
EXTRA_COLUMNS = [
    'channel_id','channel_title','video_id','video_title','comment_id','author','is_reply',
    'video_uploader_channel_id','video_uploader_channel_title'
]

def yt():
    load_dotenv()
    key = os.getenv("YT_API_KEY")
    if not key:
        raise SystemExit("Set YT_API_KEY in .env or the environment first")
    return build('youtube','v3',developerKey=key)

def find_channel_id(y, name):
    res = y.search().list(q=name, part='snippet', type='channel', maxResults=1).execute()
    items = res.get('items',[])
    if not items: return None, None
    ch = items[0]
    return ch['snippet']['channelId'], ch['snippet']['channelTitle']

def video_stats(y, ids):
    out={}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        if not chunk: continue
        res = y.videos().list(id=",".join(chunk), part='snippet,statistics').execute()
        for it in res.get('items',[]):
            out[it['id']] = {
                'commentCount': int(it.get('statistics',{}).get('commentCount',0)),
                'title': it.get('snippet',{}).get('title'),
                'channelId': it.get('snippet',{}).get('channelId'),
                'channelTitle': it.get('snippet',{}).get('channelTitle'),
            }
    return out

def search_channel_videos(y, channel_id, query, max_results=30):
    res = y.search().list(q=query, channelId=channel_id, part='snippet', type='video',
                          maxResults=max_results, order='viewCount').execute()
    return [it['id']['videoId'] for it in res.get('items',[])]

def fetch_comments(y, video_id, target=TARGET_PER_VIDEO):
    rows=[]; nextp=None; got=0
    while True:
        kw = dict(videoId=video_id, part='snippet,replies', maxResults=100, order='relevance')
        if nextp: kw['pageToken']=nextp
        resp = y.commentThreads().list(**kw).execute()
        for item in resp.get('items',[]):
            top=item['snippet']['topLevelComment']; c=top['snippet']
            rows.append({
                'title': c.get('textDisplay',''),
                'link': f"https://www.youtube.com/watch?v={video_id}&lc={top['id']}",
                'date_published': c.get('publishedAt'),
                'text': c.get('textOriginal',''),
                'like_count': c.get('likeCount',0),
                'reply_parent_id': None,
                'channel_id': c.get('authorChannelId',{}).get('value'),
                'channel_title': c.get('authorDisplayName'),
                'video_id': video_id,
                'video_title': None,
                'comment_id': top['id'],
                'author': c.get('authorDisplayName'),
                'is_reply': False
            }); got+=1
            for reply in item.get('replies',{}).get('comments',[]):
                rc=reply['snippet']
                rows.append({
                    'title': rc.get('textDisplay',''),
                    'link': f"https://www.youtube.com/watch?v={video_id}&lc={reply['id']}",
                    'date_published': rc.get('publishedAt'),
                    'text': rc.get('textOriginal',''),
                    'like_count': rc.get('likeCount',0),
                    'reply_parent_id': top['id'],
                    'channel_id': rc.get('authorChannelId',{}).get('value'),
                    'channel_title': rc.get('authorDisplayName'),
                    'video_id': video_id,
                    'video_title': None,
                    'comment_id': reply['id'],
                    'author': rc.get('authorDisplayName'),
                    'is_reply': True
                }); got+=1
            if got>=target: break
        if got>=target: break
        nextp = resp.get('nextPageToken')
        if not nextp: break
        time.sleep(random.uniform(0.3,0.6))
    return rows

def fill_video_meta(y, rows):
    vids = sorted(set(r['video_id'] for r in rows if r.get('video_id')))
    meta = video_stats(y, vids)
    for r in rows:
        m = meta.get(r['video_id'])
        if m:
            r['video_title'] = m['title']
            r['video_uploader_channel_id'] = m['channelId']
            r['video_uploader_channel_title'] = m['channelTitle']

def main():
    y = yt()
    existing = []
    if os.path.exists(OUT):
        df = pd.read_excel(OUT, sheet_name='youtube_with_extras')
        existing = df.to_dict('records')
    existing_vids = set(r.get('video_id') for r in existing if r.get('video_id'))

    # Count per uploader channel from existing
    per_ch = {}
    for r in existing:
        ch = r.get('video_uploader_channel_id') or r.get('channel_id')
        vid = r.get('video_id')
        if ch and vid:
            per_ch.setdefault(ch, set()).add(vid)

    added=[]
    for name in TARGET_CHANNEL_NAMES:
        ch_id, ch_title = find_channel_id(y, name)
        if not ch_id: continue
        have = len(per_ch.get(ch_id, set()))
        need = max(0, 5 - have)
        if need==0: continue

        vids = search_channel_videos(y, ch_id, KEYWORDS, max_results=40)
        meta = video_stats(y, vids)
        cand = [v for v in vids if meta.get(v,{}).get('commentCount',0) >= MIN_COMMENTS and v not in existing_vids]
        cand.sort(key=lambda v: meta[v]['commentCount'], reverse=True)
        pick = cand[:need]
        for vid in pick:
            rows = fetch_comments(y, vid, target=TARGET_PER_VIDEO)
            fill_video_meta(y, rows)
            for r in rows:
                m = meta[vid]
                r['video_uploader_channel_id'] = m['channelId']
                r['video_uploader_channel_title'] = m['channelTitle']
            added.extend(rows)
            existing_vids.add(vid)
            per_ch.setdefault(ch_id, set()).add(vid)

    all_rows = existing + added
    df = pd.DataFrame(all_rows, columns=BASE_COLUMNS + EXTRA_COLUMNS)
    base = df[['title','link','date_published','text','like_count','reply_parent_id']]
    with pd.ExcelWriter(OUT, engine='openpyxl') as xw:
        base.to_excel(xw, index=False, sheet_name='youtube_base')
        df.to_excel(xw, index=False, sheet_name='youtube_with_extras')
    print("[OK] Topped up. Videos now:", df['video_id'].nunique())

if __name__ == "__main__":
    main()
