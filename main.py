import json
import os
import re
import sys
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def read_config(file_path):
    """設定ファイル（JSON）からキーワードなどを読み込み、APIキーは環境変数から取得する"""
    # APIキーを環境変数 'YOUTUBE_API_KEY' から取得
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        # 環境変数が見つからない場合はエラーメッセージを表示してプログラムを終了
        print("エラー: 環境変数 'YOUTUBE_API_KEY' が設定されていません。")
        sys.exit(1)

    # 設定ファイルからキーワードと開始日時を読み込む
    with open(file_path, 'r', encoding='utf-8') as file:
        config = json.load(file)
    
    # configファイル内にapi_keyが含まれていても無視される（環境変数が優先）
    return api_key, config['keywords'], config['start_datetime']

def jst_to_utc(jst_str):
    """JST日時文字列をUTCのISO8601に変換"""
    jst_dt = datetime.strptime(jst_str, "%Y-%m-%d %H:%M:%S")
    utc_dt = jst_dt - timedelta(hours=9)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def iso8601_to_duration(iso_duration):
    """PT表記（YouTube ISO8601）をHH:MM:SS化"""
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return "00:00:00"
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return str(timedelta(hours=hours, minutes=minutes, seconds=seconds))

def convert_to_japan_time(utc_time):
    """UTC時刻をJST変換し表示用に"""
    utc_datetime = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%SZ")
    japan_datetime = utc_datetime + timedelta(hours=9)
    return japan_datetime.strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_time():
    """現在時刻 (JST表示)"""
    now_utc = datetime.utcnow()
    now_jst = now_utc + timedelta(hours=9)
    return now_jst.strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_digit_date():
    """今日の日付 (JST, シート名用 'YYYYMMDD' フォーマット)"""
    now_utc = datetime.utcnow()
    now_jst = now_utc + timedelta(hours=9)
    return now_jst.strftime("%Y%m%d")

def calc_engagement_rate(like_count, comment_count, view_count):
    """エンゲージメント率 (％)"""
    if view_count == 0:
        return 0.0
    return round((like_count + comment_count) / view_count * 100, 2)

def get_youtube_data(api_key, keyword, start_datetime_jst, end_datetime_jst, max_total_results=100):
    """
    指定キーワード・期間のYouTube動画情報を100件上限で取得
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    start_utc = jst_to_utc(start_datetime_jst)
    end_utc = jst_to_utc(end_datetime_jst)
    start_dt = datetime.strptime(start_datetime_jst, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_datetime_jst, "%Y-%m-%d %H:%M:%S")

    video_ids = []
    next_page_token = None

    while len(video_ids) < max_total_results:
        search_response = youtube.search().list(
            q=keyword,
            part='snippet',
            type='video',
            maxResults=min(50, max_total_results - len(video_ids)),
            publishedAfter=start_utc,
            publishedBefore=end_utc,
            pageToken=next_page_token
        ).execute()

        video_ids += [item['id']['videoId'] for item in search_response['items']]
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token or len(video_ids) >= max_total_results:
            break

    video_data = []
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        video_response = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(batch_ids)
        ).execute()

        for item in video_response['items']:
            snippet = item['snippet']
            statistics = item.get('statistics', {})
            content_details = item['contentDetails']

            published_at_utc = snippet['publishedAt']
            published_at_jst = datetime.strptime(published_at_utc, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=9)

            # 厳密な時間チェック（YouTube APIのpublishedBefore/Afterは多少曖昧なため）
            if not (start_dt <= published_at_jst <= end_dt):
                continue

            # 'likeCount'や'commentCount'が存在しない場合があるためgetを使用
            video_data.append({
                'title': snippet['title'],
                'channel': snippet['channelTitle'],
                'published_at': snippet['publishedAt'],
                'video_id': item['id'],
                'view_count': int(statistics.get('viewCount', 0)),
                'like_count': int(statistics.get('likeCount', 0)),
                'comment_count': int(statistics.get('commentCount', 0)),
                'duration': content_details.get('duration', "PT0S")
            })

    return video_data

def merge_and_deduplicate(video_data_list, keywords):
    """重複削除＋キーワードをタイトルに含む動画のみ抽出"""
    merged = {}
    for video_data in video_data_list:
        for video in video_data:
            # キーワードがタイトルに含まれているかチェック
            if any(k in video['title'] for k in keywords):
                merged[video['video_id']] = video
    return list(merged.values())

def export_to_google_sheet(video_data, spreadsheet_id, exec_time_jst, sheet_name):
    """
    Googleスプレッドシートに出力（新規シート作成しデータ追加）
    """
    # サービスアカウント認証 (GCP_SERVICE_ACCOUNT_KEYは環境変数/Secretsから取得)
    credentials_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)

    # 新しいシートを作成
    worksheet = sh.add_worksheet(title=sheet_name, rows="100", cols="20")

    headers = [
        "動画タイトル", "チャンネル名", "投稿日時（日本時間）", "動画ID",
        "動画URL", "再生回数", "高評価数", "視聴者コメント数", "動画の長さ",
        "エンゲージメント率(%)", "ダウンロード実行時間（日本時間）"
    ]
    rows = []
    for video in video_data:
        engagement_rate = calc_engagement_rate(video['like_count'], video['comment_count'], video['view_count'])
        video_url = f"https://www.youtube.com/watch?v={video['video_id']}"
        rows.append([
            video['title'],
            video['channel'],
            convert_to_japan_time(video['published_at']),
            video['video_id'],
            video_url,
            video['view_count'],
            video['like_count'],
            video['comment_count'],
            iso8601_to_duration(video['duration']),
            engagement_rate,
            exec_time_jst
        ])
    
    # ヘッダーとデータをシートに追加
    worksheet.append_row(headers)
    worksheet.append_rows(rows, value_input_option='USER_ENTERED')

def main():
    # 設定ファイル名
    config_file = '動画リストconfig.txt'
    # 自身のスプレッドシートID
    spreadsheet_id = '1MloHGh089FVzMxP5migrOpHz5VkGuQ-W0-8Ki9MUhdU'

    # 設定とAPIキーの読み込み（APIキーは環境変数から）
    api_key, keywords, start_datetime_jst = read_config(config_file)

    # 今日の日付と実行時間をJSTで取得
    sheet_name = get_current_japan_digit_date()
    exec_time_jst = get_current_japan_time()
    
    # 検索終了日時を今日の10:01:00 JSTに設定
    end_datetime_jst = f"{sheet_name[:4]}-{sheet_name[4:6]}-{sheet_name[6:]} 10:01:00"

    # --- シート存在チェック（APIアクセス前） ---
    # GCPサービスアカウント認証
    try:
        credentials_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    except KeyError:
        print("エラー: 環境変数 'GCP_SERVICE_ACCOUNT_KEY' が設定されていません。")
        sys.exit(1)
        
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    existing_sheets = [ws.title for ws in sh.worksheets()]
    
    if sheet_name in existing_sheets:
        print(f"✅ {sheet_name}シートは既に存在しているためAPIアクセスせずにスキップします。")
        return

    # --- 以降のみYouTube Data APIアクセス ---
    video_data_list = []
    print(f"➡️ YouTubeデータ取得開始 (キーワード: {len(keywords)}件, 期間: {start_datetime_jst} 〜 {end_datetime_jst})")
    for keyword in keywords:
        video_data = get_youtube_data(api_key, keyword, start_datetime_jst, end_datetime_jst, max_total_results=100)
        video_data_list.append(video_data)
        print(f"   - キーワード '{keyword}': {len(video_data)}件取得")

    # データ統合、重複排除、タイトルフィルタリング
    merged_video_data = merge_and_deduplicate(video_data_list, keywords)
    print(f"➡️ フィルタリング・重複排除後: {len(merged_video_data)}件")
    
    # 再生回数でソート
    merged_video_data.sort(key=lambda x: x['view_count'], reverse=True)
    
    # Googleスプレッドシートに出力
    export_to_google_sheet(merged_video_data, spreadsheet_id, exec_time_jst, sheet_name)
    print(f"🎉 処理完了（シート名: {sheet_name}、動画数: {len(merged_video_data)}件）")

if __name__ == "__main__":
    main()
