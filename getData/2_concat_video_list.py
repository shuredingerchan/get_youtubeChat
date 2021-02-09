import glob
import pandas as pd
import os
def main():
    currentdir = os.getcwd() + "/"
    video_list = glob.glob(currentdir + "video_list_output/*.csv")
    outputfile = currentdir + "all_video_list.csv"
    with open(outputfile,mode="w") as f:
        print(f)
    df_list = []
    for v in video_list:
        df_list.append(pd.read_csv(v, parse_dates=[4]))
    concat_df = pd.concat(df_list)
    concat_df.sort_values(
        ["streamer_name", "publishedAt"], ascending=[True, False]
    ).to_csv(outputfile, index=False)


if __name__ == "__main__":
    main()
