import json5
import pandas as pd


def main():
    with open("data/master_data.json", "r") as f:
        data = json5.load(f)

    df = pd.json_normalize(data, "info", ["atccode", "drug"])

    df_agg = df.groupby("journal").atccode.nunique()
    return print(df_agg.idxmax())


if __name__ == "__main__":
    main()
