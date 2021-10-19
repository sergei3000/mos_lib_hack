import numpy as np
import pandas as pd
import implicit
from scipy import sparse
from utils import save_mapping, load_mapping, prepare_for_saving

RANDOM_SEED = 42
CONFIDENCE = 40


def read_data(circulatons_filename: str, dataset_knigi_filename: str) -> pd.DataFrame:
    """Read 2 tables and create a single for training model.

    Args:
        circ_filename (str): File with circulations
        dataset_knigi_filename (str): File from dataset_knigi_1

    Returns:
        pd.DataFrame: Concated table with interactions.
    """

    circulatons = pd.read_parquet(circulatons_filename)
    dataset_knigi = pd.read_csv(dataset_knigi_filename)
    dataset_knigi["dt"] = pd.to_datetime(dataset_knigi["dt"])

    interactions = pd.concat([dataset_knigi, circulatons]).reset_index(drop=True)

    interactions["user_id"] = interactions["user_id"].astype("category")
    interactions["item_id"] = interactions["item_id"].astype("category")
    return interactions


def save_mappings_dicts(interactions: pd.DataFrame):
    """Save all mappings from items and users to indices and vice versa.

    Args:
        interactions (pd.DataFrame): Table with users and items.
    """

    user2idx = dict(zip(interactions["user_id"], interactions["user_id"].cat.codes))
    idx2user = {v: k for k, v in user2idx.items()}
    item2idx = dict(zip(interactions["item_id"], interactions["item_id"].cat.codes))
    idx2item = {v: k for k, v in item2idx.items()}

    save_mapping(user2idx, "../../data/user2idx")
    save_mapping(idx2user, "../../data/idx2user")
    save_mapping(item2idx, "../../data/item2idx")
    save_mapping(idx2item, "../../data/idx2item")


def train_model(interactions: pd.DataFrame):
    """Train ALS model using interactions table.

    Args:
        interactions (pd.DataFrame): [description]
    """
    sparse_item_user = sparse.csr_matrix(
        (
            np.ones(len(interactions), dtype=np.float32),
            (interactions["item_id"].cat.codes, interactions["user_id"].cat.codes),
        )
    )

    model = implicit.als.AlternatingLeastSquares(
        factors=64, iterations=50, num_threads=8, random_state=RANDOM_SEED
    )
    model.fit(sparse_item_user * CONFIDENCE)
    return model, sparse_item_user


def generate_recommendations_file(
    model: implicit.als.AlternatingLeastSquares,
    user_items: sparse.csr_matrix,
    N: int = 5,
):
    all_recs = model.recommend_all(user_items=user_items, N=N)
    iid2recid = load_mapping("../../data/iid2recid")
    idx2user = load_mapping("../../data/idx2user")
    idx2item = load_mapping("../../data/idx2item")
    books_full = pd.read_parquet("../../data/books_full.parquet.gzip", columns=["recId", "title", "author"])

    all_recs_df = pd.DataFrame(
        all_recs, columns=["item1", "item2", "item3", "item4", "item5"]
    )
    all_recs_df = all_recs_df.reset_index().rename(columns={"index": "user_idx"})
    all_recs_df["user_id"] = all_recs_df["user_idx"].map(idx2user)
    all_recs_df = all_recs_df.drop("user_idx", axis=1)
    all_recs_df = pd.melt(
        all_recs_df,
        id_vars=["user_id"],
        value_vars=["item1", "item2", "item3", "item4", "item5"],
        var_name="ranking",
        value_name="item_idx",
    )
    all_recs_df["ranking"] = all_recs_df["ranking"].apply(
        lambda x: int(x[-1])
    )  # 'item1' -> 1
    all_recs_df["item_id"] = all_recs_df["item_idx"].map(idx2item)
    all_recs_df = all_recs_df.drop("item_idx", axis=1)
    all_recs_df = prepare_for_saving(
        all_recs_df, is_recs=True, books_full=books_full, itemid2recid=iid2recid
    )
    all_recs_df = all_recs_df.sort_values(["user_id", "ranking"])
    all_recs_df.to_csv("../../data/recommendations.csv", index=False)


def generate_history_file(interactions: pd.DataFrame):
    iid2recid = load_mapping("../../data/iid2recid")
    books_full = pd.read_parquet("../../data/books_full.parquet.gzip", columns=["recId", "title", "author"])
    history = interactions.copy()
    history = history.sort_values(["user_id", "dt"], ascending=[True, False])
    history = history[history.groupby("user_id").cumcount() < 20]
    history = prepare_for_saving(
        history, is_recs=False, books_full=books_full, itemid2recid=iid2recid
    )
    history.to_csv("../../data/history.csv", index=False)
