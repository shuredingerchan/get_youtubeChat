import pandas as pd
import os
import sys
import signal
import json
import glob
from icecream import ic

class MyDict(dict):
    def __missing__(self,key):
        v = self[key] = type(self)()
        return v


def main():
    #辞書

    #構成
    #streamerDict{streamername : userIndexDict. "yearmonth" : ymArray}
    #   userIndexDict{"index": userDict}
    #        userDict{"userName" : name , membershipTrue" : "true" , videoTitle : commentDict,videoname : ...}
    #            commentDict{normalChat : nCount, superChat : sCount, "memberHistory":memberHistory}


    #配列
    streamerList = [] #作業用のリスト

    #streamer名を取得
    streamerjsonPath = os.getcwd() + "/config/holo_streamer_channel_IDs.json"
    with open(streamerjsonPath, encoding="utf-8") as f:
        streamerjson = json.load(f)
    for s, _ in streamerjson.items(): 
        streamerList.append(s)

    #BIGDATA取り出す
    bigdataDir = os.getcwd() + "/BIGDATA"
    os.makedirs(bigdataDir, exist_ok=True)
    bigdataPath = os.getcwd() + "/BIGDATA/streamerMainDump.json"
    bigdataexistFLG = False
    if os.path.exists(bigdataPath):         
        with open(bigdataPath, encoding="utf-8") as f:
            bigdatajson = json.load(f)
        bigdataexistFLG = True
        streamerDict =  MyDict()
        streamerDict.update(bigdatajson.items())
    if bigdataexistFLG == False:
        streamerDict =  MyDict()
    #yyyy-mmディレクトリ一覧を取得
    dataDir = os.getcwd() + "/DataDir"
    ymDirList = glob.glob(dataDir + "/" + "????-??")
    #yyy-mmDir
    for ymDir in ymDirList:
        yearmonth = os.path.basename(ymDir)
        #streamerDir
        for streamer in streamerList:
            nothingFlg = True
            chatDir = ymDir + "/chat_list_output"
            streamerDir = chatDir + "/" + streamer
            if os.path.exists(streamerDir) == False:
                print("nothing_" + yearmonth + streamer + "_Directory")
                continue
            #調査済みかチェック
            ic(streamer)
            if streamer in streamerDict.keys():
                print("aa")
                if yearmonth in streamerDict[streamer].keys():
                    bigdataexistFLG = True
                    print(yearmonth + streamer + "は既に調査済みです")
                    continue
            #ストリーマー毎のユーザーカウントを設定
            if streamer not in streamerDict.keys():
                streamerDict[streamer]
                streamerDict[streamer]["userCount"] = 0
            streamerDict[streamer][yearmonth] = "True" #valueは適当（チェック用）
            commentfileList = glob.glob(streamerDir + "/*csv")
            #videofile
            for commentfile in commentfileList:
                commentdata = pd.read_csv(commentfile,encoding="utf-8",header=0,engine="python")
                videoTitle = os.path.basename(commentfile)
                ic(videoTitle)
                #commentline
                nothingFlg = True
                for _ , commentRow in commentdata.iterrows():
                    membershipBoolean = "False"
                    userName = commentRow[0]
                    #commentTime = commentRow[1]
                    membershipHistory = commentRow[2]
                    #commentValue = commentRow[3]
                    superChat = str(commentRow[4])
                    if membershipHistory != "通常視聴者":
                        membershipBoolean = True
                    #既存ユーザーかチェック。                    
                    if userName in streamerDict[streamer]["userDict"].keys():
                        #メンバー入った？
                        if membershipBoolean == "True" and streamerDict[streamer]["userDict"][userName]["memberShipBoolean"] == False:
                            streamerDict[streamer]["userDict"][userName]["memberShipBoolean"] = True
                        if videoTitle not in streamerDict[streamer]["userDict"][userName].keys():
                            #この動画に初コメ
                            streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle].update({"normalChat" : 0 , "superChat" : 0})
                        if superChat == "0":
                            streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle]["normalChat"] += 1
                            streamerDict[streamer]["userDict"][userName]["totalChat"]["totalNormalChat"] += 1
                        else:
                            streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle]["superChat"] += 1
                            streamerDict[streamer]["userDict"][userName]["totalChat"]["totalSuperChat"] += 1
                    else:
                        #初コメ、ユーザー登録
                        streamerDict[streamer]["userDict"][userName]["userName"] = userName
                        streamerDict[streamer]["userDict"][userName]["membershipBoolean"] = membershipBoolean
                        streamerDict[streamer]["userDict"][userName]["totalChat"]
                        streamerDict[streamer]["userDict"][userName]["totalChat"].update({"totalNormalChat" : 0 , "totalSuperChat" : 0})
                        streamerDict[streamer]["userDict"][userName][yearmonth]
                        streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle]
                        streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle].update({"normalChat" : 0 , "superChat" : 0})
                        if superChat == "0":
                            streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle].update({"normalChat" : 1})
                            streamerDict[streamer]["userDict"][userName]["totalChat"].update({"totalNormalChat" : 1})
                        else:
                            streamerDict[streamer]["userDict"][userName][yearmonth][videoTitle].update({"superChat" : 1})
                            streamerDict[streamer]["userDict"][userName]["totalChat"].update({"totalSuperChat" : 1})
                        streamerDict[streamer]["userCount"] += 1
                    nothingFlg = False
                #ストリーマーkeyに入れ込む
    #json形式でdumpをおとす
            if nothingFlg == False:
                with open(bigdataPath,  mode="w", encoding="utf-8") as f:
                    json.dump(streamerDict,f,ensure_ascii=False,indent=4)
                    print("writed")
            else:
                print("nothing to do...")

if __name__ == "__main__":
    main()