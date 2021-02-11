from bs4 import BeautifulSoup
import pandas as pd
import json
import requests
import requests_html
from urllib.parse import urlparse, parse_qs
import sys
from icecream import ic
import os
import signal

def chatdataInsert(samp):
    #スパチャ、メンバー加入振り分け
    if "liveChatTextMessageRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
        chatType = "liveChatTextMessageRenderer"
    elif "liveChatPaidMessageRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
        chatType = "liveChatPaidMessageRenderer"
    else:
        check3 = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"])
        ic(check3)
    timeSimpleText = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]["timestampText"]["simpleText"])
    simpleText = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]["authorName"]["simpleText"])
    
    #debug
    text = ""
    if "message" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]:
        for message in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]["message"]["runs"]:
            if "emoji" in message:
                if "emojiId" in message["emoji"]:
                    tmppart = str(message["emoji"]["emojiId"])
                else:
                    tmppart = str(message["emoji"]["image"]["accessibility"]["accessibilityData"]["label"])
                textpart = "emojiId[" + tmppart + "]"
            else:
                textpart = str(message["text"])
                textpart = textpart.replace(",","_")                
            text = text + textpart

    userId = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]["id"])
    try:
        cliantId = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["clientId"])
    except:
        cliantId = "NocliantId"    
    try:
        paid = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]["purchaseAmountText"]["simpleText"])
    except:
        paid = "0"        
    try:
        membershipHistory = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"][chatType]["authorBadges"][0]["liveChatAuthorBadgeRenderer"]["accessibility"]["accessibilityData"]["label"])
    except:    
        membershipHistory = "通常視聴者"
    chatdataFull =  timeSimpleText + "," +simpleText + "," + membershipHistory + "," + text + ","  + paid + "," + userId + "," + cliantId
    return chatdataFull

def get_continuation(ytInitialData):
    continuation = ytInitialData['continuationContents']['liveChatContinuation']['continuations'][0].get('liveChatReplayContinuationData', {}).get('continuation')
    return(continuation)

def preparetion(inputvideoListPath):
    #csvデータからstreamer、videoId、titleを抜き出す（panda)
    all_video_list = pd.read_csv(inputvideoListPath)
    streamer_videoId_title_List = all_video_list[["streamer_name", "videoId", "title"]]
    return streamer_videoId_title_List

def get_chatData(inputDir,inputvideoListPath):
    streamer_videoId_title_List = preparetion(inputvideoListPath)

    for _ , row_line in streamer_videoId_title_List.iterrows():
        videoId = row_line["videoId"]
        streamer = row_line["streamer_name"]
        tmpTitle =  row_line["title"]
        #タイトルからファイル文字不可文字を消す
        tmpTitle = tmpTitle.replace(" ","")
        tmpTitle = tmpTitle.replace("/","_")
        tmpTitle = tmpTitle.replace("|","_")
        tmpTitle = tmpTitle.replace(":","_")
        tmpTitle = tmpTitle.replace("?","_")
        tmpTitle = tmpTitle.replace("<","_")
        tmpTitle = tmpTitle.replace(">","_")
        tmpTitle = tmpTitle.replace("*","_")
        tmpTitle = tmpTitle.replace('"',"_")
        title = tmpTitle.replace(".","_")
        ic(title)
        target_url = "https://www.youtube.com/watch?v=" + videoId
        dict_str = ""
        next_url = ""
        comment_data = []
        memberText = []
        session = requests_html.HTMLSession()
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}

        #ファイル存在してたらとばす
        chat_list_output = inputDir + "\\chat_list_output"
        os.makedirs(chat_list_output, exist_ok=True)
        writeDir = chat_list_output +"\\" + streamer
        os.makedirs(writeDir, exist_ok=True)
        filePath = writeDir + "\\" + title + ".csv"
        if os.path.exists(filePath):
            print(f"already exists:" + title)
            continue
        #1行目
        indexName = "時間,ユーザー名,メンバーシップ暦,テキスト,スパチャ額,ユーザーID？,クライアントId\n"
        comment_data.append(indexName)

        # まず動画ページにrequestsを実行しhtmlソースを手に入れてlive_chat_replayの先頭のurlを入手
        resp = session.get(target_url)
        resp.html.render()

        #continuation_prefix = "https://www.youtube.com/live_chat_replay?continuation="
#        for iframe in resp.html.find("iframe"):
        for iframe in resp.html.find("iframe"):
            if("live_chat_replay" in iframe.attrs["src"]):
               next_url= "".join(["https://www.youtube.com", iframe.attrs["src"]])
        while True:
            try:
                html = session.get(next_url, headers=headers)
                soup = BeautifulSoup(html.text,"lxml")

                # 次に飛ぶurlのデータがある部分をfind_allで探してsplitで整形
                for scrp in soup.find_all("script"):
                    if "window[\"ytInitialData\"]" in scrp.next:
                        dict_str = scrp.next.split(" = ", 1)[1]

                # 辞書形式と認識すると簡単にデータを取得できるが, 末尾に邪魔なのがあるので消しておく（「空白2つ + \n + ;」を消す）
                dict_str = dict_str.rstrip("  \n;")
                # 辞書形式に変換
                dics = json.loads(dict_str)

                # "https://www.youtube.com/live_chat_replay?continuation=" + continue_url が次のlive_chat_replayのurl
                continue_url = dics["continuationContents"]["liveChatContinuation"]["continuations"][0]["liveChatReplayContinuationData"]["continuation"]
                next_url = "https://www.youtube.com/live_chat_replay?continuation=" + continue_url

                # dics["continuationContents"]["liveChatContinuation"]["actions"]がコメントデータのリスト。
                for samp in dics["continuationContents"]["liveChatContinuation"]["actions"][1:]:
                    #ctrl+Cで止める用
                    signal.signal(signal.SIGINT, signal.SIG_DFL)

                    if 'addChatItemAction' not in samp["replayChatItemAction"]["actions"][0]:
                        continue
                    if 'item' not in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]:
                        continue
                    #対象外（スティッカー、コメント撤回（編集？）
                    if "liveChatPaidStickerRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                        # スパチャスティッカー
                        continue
                    if "liveChatTickerPaidStickerItemRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                        # スパチャ絵文字のみの額
                        continue    
                    if "liveChatMembershipItemRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                        # メンバーシップ
                        time = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatMembershipItemRenderer"]["timestampText"]["simpleText"])
                        name = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatMembershipItemRenderer"]["authorName"]["simpleText"])
                        mtext = time + "," + name + "\n"
                        memberText.append(mtext)
                        continue
                    if "liveChatTickerSponsorItemRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                        # メンバーシップ
                        time = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatTickerSponsorItemRenderer"]["timestampText"]["simpleText"])
                        name = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatTickerSponsorItemRenderer"]["authorName"]["simpleText"])
                        mtext = time + "," + name + "\n"
                        memberText.append(mtext)
                        continue
                    if "liveChatPlaceholderItemRenderer" in samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                        # 多分コメント撤回したやつ
                        continue
                    #str1 = str(samp["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatTextMessageRenderer"]["message"]["runs"])
                    #if 'emoji' in str1:
                    #    continue
                    chatdata = chatdataInsert(samp)
                    comment_data.append(chatdata)
                    comment_data.append("\n")
            # next_urlが入手できなくなったら終わり
            except:
                break
        with open(filePath, mode='w', encoding="utf-8") as f:
            f.writelines(comment_data)
        #メンバーシップ加入データを書き込む
        membershipDir = writeDir + "\\membership"
        membershipPath = membershipDir + "\\" + title + ".csv"
        os.makedirs(membershipDir, exist_ok=True)
        with open(membershipPath, mode='w', encoding="utf-8") as f:
                f.writelines(memberText)           
    print("実行終了")
