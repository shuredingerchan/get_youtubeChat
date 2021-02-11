from bs4 import BeautifulSoup
import pandas as pd
import json
import requests
import requests_html
from urllib.parse import urlparse, parse_qs
from icecream import ic
import os
import signal
from src.get_chatData import get_chatData

def main():
    print("ディレクトリ選択(例：2021-01-01)")
    timeDir = input()
    inputvideoListDir =  os.getcwd() + "/" + timeDir
    inputvideoListPath = inputvideoListDir + "/all_video_list.csv"
    get_chatData(inputvideoListDir,inputvideoListPath)        

if __name__ == '__main__':
    main()