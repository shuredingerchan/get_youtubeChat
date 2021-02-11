import os
from datetime import date
from calendar import monthrange
from src.get_videoData import get_videoData_function
from src.concat_video_list import concat_video_list
from icecream import ic

def main():
    print("yyyy入力(例:2021)")
    year = input()
    print("mm入力（例:01)")
    month = input()
    channeljsonPath = os.getcwd() + "/config/holo_streamer_channel_IDs.json"
    get_videoData_function(year,month,channeljsonPath)

    timeDir = os.getcwd() + "/" + year + "-" + month
    concat_video_list(timeDir)

if __name__ == "__main__":
    main()
