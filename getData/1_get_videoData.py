import json
import pandas as pd
import requests
import os
import time
from pathlib import Path
from icecream import ic

def main():

    API_KEY = os.getenv("YOUTUBEAPI")
    base_url = "https://www.googleapis.com/youtube/v3"

    #期間指定ファイル読み込み
    with open(os.getcwd() + "/inputTime.txt", encoding="utf-8") as f:
        line_count = 0
        for line in f:
            if line_count == 0:
                tmpFirstTime = line.strip()
                ic(tmpFirstTime)
                line_count += 1
            else:
                tmpEndTime = line.strip()
                ic(tmpEndTime)

    #チャンネルID一覧
    with open(r"D:\Programs\youtubeAPI\holo_comment_user_graph\config\holo_streamer_channel_IDs.json", encoding="utf-8") as f:
        channel_ID_config = json.load(f)


    for streamer_name, channel_URL in channel_ID_config.items():
        if os.path.exists(f"video_list_output/{streamer_name}_videos.csv"):
            print(f"already exists: {streamer_name}")
            continue
        print(f"now processing...{streamer_name}")
        CHANNEL_ID = channel_URL.split("/")[-1]

        loop = 0
        while True:
            #loop回数チェック
            loop += 1
            ic(loop)

            firstTime = tmpFirstTime + "T00:00:00Z"
            endTime = tmpEndTime + "T00:00:00Z"
            url = ""
            if url == "":
                url = (
                    #期間指定
                    base_url
                        + "/search?key=%s&channelId=%s&part=snippet,id&order=date&maxResults=50"
                        + "&publishedBefore=" + endTime + "&publishedAfter=" + firstTime
                )
            infos = []

            time.sleep(10)
            response = requests.get(url % (API_KEY, CHANNEL_ID))
            if response.status_code != 200:
                print("exit by error")
                continue

            result = response.json()
            infos.extend(
                [
                    [
                        streamer_name,
                        item["id"]["videoId"],
                        item["snippet"]["title"],
                        item["snippet"]["description"],
                        item["snippet"]["publishedAt"],
                    ]
                    for item in result["items"]
                    if item["id"]["kind"] == "youtube#video"
                ]
            )
            #nextPageToken初期化
            nextPageToken = ""

            if "nextPageToken" in result.keys():
                if "pageToken" in url:
                    url = url.split("&pageToken")[0]
                nextPageToken = f'&pageToken={result["nextPageToken"]}'   
                if nextPageToken == "":
                    print("success2")
                    break
                else:
                    url += nextPageToken
            else:
                print("success")
                break
        videos = pd.DataFrame(
            infos,
            columns=["streamer_name", "videoId", "title", "description", "publishedAt"],
        )
        writeDir = os.getcwd() + "/" + "video_list_output"
        os.makedirs(writeDir, exist_ok=True)
        videos.to_csv(writeDir + "/" + f"{streamer_name}_videos.csv", index=None)


if __name__ == "__main__":
    main()
