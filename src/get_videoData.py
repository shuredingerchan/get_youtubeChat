import json
import pandas as pd
import requests
from datetime import date
from calendar import monthrange
import os
import time
from pathlib import Path
from icecream import ic

def get_videoData_function(year,month,channeljsonPath):
    API_KEY = os.getenv("YOUTUBEAPI")
    base_url = "https://www.googleapis.com/youtube/v3"

    #チャンネルID一覧
    with open(channeljsonPath, encoding="utf-8") as f:
        channel_ID_config = json.load(f)
        firstTime = year + "-" + month + "-" + "01T00:00:00Z"
        ic(month)
        endTime = year + "-" + month + "-" + str(monthrange(int(year),int(month))[1]) + "T00:00:00Z"
        ic(firstTime)
        ic(endTime)


    timeDir = os.getcwd() + "/"  + year + "-" + month
    os.makedirs(timeDir, exist_ok=True)
    infos = []
    for streamer_name, channel_URL in channel_ID_config.items():
        if os.path.exists(timeDir + f"/video_list_output/{streamer_name}_videos.csv"):
            print(f"already exists: {streamer_name}")
            continue
        print(f"now processing...{streamer_name}")
        CHANNEL_ID = channel_URL.split("/")[-1]
        loop = 0
        url = ""
        while True:
            #loop回数チェック
            loop += 1
            ic(loop)
            #エラー落ち感知
            if loop == 5:
                print("errorBreak")
                break
            if url == "":
                url = (
                    #期間指定
                    base_url
                        + "/search?key=%s&channelId=%s&part=snippet,id&order=date&maxResults=50"
                        + "&publishedBefore=" + endTime + "&publishedAfter=" + firstTime
                )

            time.sleep(10)
            response = requests.get(url % (API_KEY, CHANNEL_ID))
            if response.status_code != 200:
                print("exit by error")
                break

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
                ic(nextPageToken)
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

        writeDir = timeDir + "/" + "video_list_output"
        os.makedirs(writeDir, exist_ok=True)
        videos.to_csv(writeDir + "/" + f"{streamer_name}_videos.csv", index=None)