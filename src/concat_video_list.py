import glob
import pandas as pd
import os

def concat_video_list(timeDir):
    outputfile = timeDir + "/all_video_list.csv"
    video_list = glob.glob(timeDir + "/video_list_output/*csv")
    with open(outputfile,mode="w") as f:
        print(f)
    df_list = []
    for v in video_list:
        df_list.append(pd.read_csv(v, parse_dates=[4]))
    concat_df = pd.concat(df_list)
    concat_df.sort_values(
        ["streamer_name", "publishedAt"], ascending=[True, False]
    ).to_csv(outputfile, index=False)