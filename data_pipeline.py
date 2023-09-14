import re

import click
import json5
import pandas as pd


def find_drugs(drugs, data, type, field, pat):
    df = pd.merge(
        drugs,
        data,
        left_on=drugs["drug"].str.lower(),
        right_on=data[field]
        .str.extract(pat, flags=re.IGNORECASE, expand=False)
        .str.lower(),
        how="inner",
    ).drop("key_0", axis=1)

    df["type"] = type
    return df


def nest_data(data, groupby_fields, to_nest_fields, name):
    return (
        data.groupby(groupby_fields, dropna=False)
        .apply(lambda x: x[to_nest_fields].to_dict(orient="records"))
        .reset_index()
        .rename(columns={0: name})
    )


@click.command()
def main():
    drugs = pd.read_csv("data/drugs.csv")
    pubmed_csv = pd.read_csv("data/pubmed.csv")

    with open("data/pubmed.json", "r") as f:
        data = json5.load(f)
    pubmed_json = pd.DataFrame.from_dict(data)

    clinical = pd.read_csv("data/clinical_trials.csv")

    pat = f"({'|'.join(drugs['drug'])})"
    drugs_pubmed_csv = find_drugs(drugs, pubmed_csv, "pubmed", "title", pat)
    drugs_pubmed_json = find_drugs(drugs, pubmed_json, "pubmed", "title", pat)
    drugs_pubmed = pd.concat([drugs_pubmed_csv, drugs_pubmed_json], axis=0)

    drugs_clinical_trials = find_drugs(
        drugs, clinical, "clinical_trials", "scientific_title", pat
    )
    drugs_clinical_trials.rename(columns={"scientific_title": "title"}, inplace=True)

    # Gather all data
    concat_data = pd.concat([drugs_pubmed, drugs_clinical_trials], axis=0)

    # Infer date format
    concat_data["date"] = pd.to_datetime(
        concat_data["date"], infer_datetime_format=True
    ).astype("str")

    # Get all drugs
    all_data = (
        pd.merge(drugs, concat_data, on="atccode", how="left")
        .drop(columns="drug_y")
        .rename(columns={"drug_x": "drug"})
    )

    # Nest data for json creation
    grouped = nest_data(
        all_data,
        ["atccode", "drug", "journal"],
        ["id", "title", "date", "type"],
        "content",
    )
    master_data = nest_data(
        grouped, ["atccode", "drug"], ["journal", "content"], "info"
    )

    # Write master data to json
    master_data.to_json("data/master_data.json", orient="records", indent=2)


if __name__ == "__main__":
    main()
