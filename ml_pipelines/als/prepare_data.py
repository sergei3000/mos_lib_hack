import datetime
import json
import numpy as np
import pandas as pd
from utils import load_mapping, save_mapping


def prepare_cat_file():
     cat = []
     for i in range(1, 4):
         cat.append(
             pd.read_csv(f"../../data/raw/cat_{i}.csv", encoding="cp1251", delimiter=";")
         )
     cat = pd.concat(cat).reset_index(drop=True)
     cat.to_parquet(
         path="../../data/cat.parquet.gzip",
         engine="fastparquet",
         compression="gzip",
         index=False,
     )


def prepare_books_full_file():
    parsed_books_full = []
    invalid_rows = []
    with open('../../data/raw/books_full.jsn.tag') as f:
        for i, line in enumerate(f):
            try:
                data = json.loads(line[:-2])
                parsed_books_full.append([
                    data["id"], data["title"], data["title_orig"],
                    data["author_fullName"], data["author_surname"], data["author_fullNameAlt"],
                    data["rubric_name"],
                    data["smart_collapse_field"],
                ])
            except:
                invalid_rows.append(i)
    books_full = pd.DataFrame(
        parsed_books_full,
        columns=[
            "id", "title", "title_orig",
            "author_fullName", "author_surname", "author_fullNameAlt",
            "rubric_name", "smart_collapse_field"
        ]
    )

    conditions = [
        (books_full['title_orig'] != '') & (books_full['title_orig'].isna() == False),
        (books_full['title'] != '') & (books_full['title'].isna() == False)
    ]
    choices = [books_full['title_orig'], books_full['title']]
    books_full['title_full'] = np.select(conditions, choices, default='Без названия')

    books_full = books_full[["id", "title_full", "author_fullName", "rubric_name"]]
    books_full = books_full.rename(columns={"id": "recId", "title_full": "title", "author_fullName": "author"})

    cat = pd.read_parquet("../../data/cat.parquet.gzip")
    from_cat2books_full = cat[cat['recId'].isin(set(cat["recId"]) - set(books_full["recId"]))][["recId", "title", "aut", "rubrics"]]
    from_cat2books_full = from_cat2books_full.rename(columns={"aut": "author", "rubrics": "rubric_name"})

    books_full = pd.concat([books_full, from_cat2books_full]).reset_index(drop=True)
    books_full["author"] = books_full["author"].fillna("неизвестный")
    books_full["title"] = books_full["title"].fillna("неизвестно")
    books_full.to_parquet(
        path="../../data/books_full.parquet.gzip", engine="fastparquet", compression="gzip", index=False
    )


def prepare_mappings_for_recid():
    books_full = pd.read_parquet("../../data/books_full.parquet.gzip")
    books_full["item_id"] = (
        (books_full["author"].str.lower() + " " + books_full["title"].str.lower())
        .astype("category")
        .cat
        .codes
    )
    recid2iid = dict(zip(books_full["recId"], books_full["item_id"]))
    iid2recid = dict(zip(books_full["item_id"], books_full["recId"]))

    save_mapping(recid2iid, "../../data/recid2iid")
    save_mapping(iid2recid, "../../data/iid2recid")


def prepare_circulatons_file():
    circulatons = []
    for i in range(1, 17):
        temp = pd.read_csv(
            f"../../data/raw/circulaton_{i}.csv", encoding="cp1251", delimiter=";"
        )
        if temp.shape[1] == 10:
            temp = temp.drop(["Unnamed: 8", "Unnamed: 9"], axis=1)
        circulatons.append(temp)

    circulatons = pd.concat(circulatons).reset_index(drop=True)

    books_full = pd.read_parquet("../../data/books_full.parquet.gzip", columns=["recId", "title", "author"])
    recid2iid = load_mapping("../../data/recid2iid")
    unknown_recid = list(
        set(circulatons["catalogueRecordID"]) - set(books_full["recId"])
    )
    circulatons = circulatons[~circulatons["catalogueRecordID"].isin(unknown_recid)]
    circulatons["item_id"] = circulatons["catalogueRecordID"].map(recid2iid)
    circulatons = circulatons.rename(columns={"readerID": "user_id", "startDate": "dt"})
    circulatons["dt"] = circulatons["dt"].apply(
        lambda x: datetime.datetime.strptime(x, "%d.%m.%Y").strftime("%Y-%m-%d")
    )
    circulatons["dt"] = pd.to_datetime(circulatons["dt"])

    circulatons = circulatons.sort_values("dt").reset_index(drop=True)
    circulatons = circulatons.drop_duplicates(
        subset=["user_id", "item_id"], ignore_index=True, keep="last"
    )
    circulatons = circulatons[["user_id", "item_id", "dt"]]
    circulatons.to_parquet(
        path="../../data/circulatons.parquet.gzip",
        engine="fastparquet",
        compression="gzip",
        index=False,
    )


def prepare_dataset_knigi_file():
    dataset_knigi = pd.read_excel("../../data/raw/dataset_knigi_1.xlsx")
    recid2iid = load_mapping("../../data/recid2iid")

    dataset_knigi["recId"] = (
        dataset_knigi["source_url"].apply(lambda x: x.split("/")[-2]).astype(int)
    )
    dataset_knigi["item_id"] = dataset_knigi["recId"].map(recid2iid)
    dataset_knigi = dataset_knigi.drop(["source_url", "recId", "event"], axis=1)
    dataset_knigi = dataset_knigi[["user_id", "item_id", "dt"]]
    dataset_knigi.to_csv("../../data/dataset_knigi.csv", index=False)
