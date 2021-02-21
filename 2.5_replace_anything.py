import pandas as pd
import os
import sys
import signal
import json
import glob
from icecream import ic
import csv
import re

class MyDict(dict):
    def __missing__(self,key):
        v = self[key] = type(self)()
        return v

def main():
    streamerList = [] #作業用のリスト
    #streamer名を取得
    streamerjsonPath = os.getcwd() + "/config/holo_streamer_channel_IDs.json"
    with open(streamerjsonPath, encoding="utf-8") as f:
        streamerjson = json.load(f)
    for s, _ in streamerjson.items():
        streamerList.append(s)
    #yyyy-mmディレクトリ一覧を取得
    dataDir = os.getcwd() + "/DataDir"
    ymDirList = glob.glob(dataDir + "/" + "????-??")
    #yyy-mmDir
    for ymDir in ymDirList:
        #streamerDir
        for streamer in streamerList:
            chatDir = ymDir + "/chat_list_output"
            oldDir = ymDir + "/chat_list_output - コピー"
            streamerDir = chatDir + "/" + streamer
            oldstreamerDir = oldDir + "/" + streamer 
            #ic(streamerDir)
            if os.path.exists(streamerDir) == False:
                #print("nothing_" + yearmonth + streamer + "_Directory")
                os.makedirs(streamerDir)
            oldcommentfileList = glob.glob(oldstreamerDir + "/*csv")
            ic(streamer)
            #videofile
            comArray = []
            comArray.clear()
            for commentfile in oldcommentfileList:
                title = os.path.basename(commentfile)      
                ic(title)              
                with open(commentfile,"r",encoding="utf-8") as f:
                    newName = os.path.basename(commentfile)
                    print('openFile')
                    for commentRow in f:
                        flg = 0
                        if '"' in commentRow:
                            flg = 1
                            print(commentRow)
                            henkou = re.sub('"', '',commentRow)
                            print('置換完了1')
                        else:
                            henkou = commentRow
                        if 'EOF' in henkou.upper():
                            flg = 1
                            henkou = henkou.upper().replace('EOF', '_')
                            print('置換完了2')
                        else:
                            henkou = henkou
                        if flg == 1:
                            ic(henkou)
                        comArray.append(henkou)

                newfile = streamerDir + "/" + newName
                with open(newfile,"w",encoding="utf-8") as o:
                    o.writelines(comArray)

if __name__ == "__main__":
    main()