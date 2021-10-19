import pandas as pd
import pickle


def save_mapping(structure: dict, filename: str):
    """Save mapping dictionary.

    Args:
        structure (dict): dictionary to save
        filename (str): file name
    """
    with open(filename + ".pkl", "wb") as handle:
        pickle.dump(structure, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_mapping(filename: str) -> dict:
    """Load mapping dictionary.

    Args:
        filename (str): file name

    Returns:
        dict: Mapping dictionary.
    """
    with open(filename + ".pkl", "rb") as handle:
        return pickle.load(handle)


def generate_implicit_recs_mapper(
    model, train_matrix, N, user_mapping, item_inv_mapping
):
    """Wrapper for recommendations generation.

    Args:
        model (implicit.als.AlternatingLeastSquares): fitted recommendation model
        train_matrix (scipy.sparse.csr_matrix): User x Items sparse matrix of ratings (interactions)
        N (int): number of items to recommend
        user_mapping (dict): Mapping dictionary from user_id to index in train_matrix
        item_inv_mapping (dict): Mapping dictionary from index in train_matrix to item_id
    """

    def _recs_mapper(user):
        user_id = user_mapping[user]
        recs = model.recommend(
            user_id, train_matrix, N=N, filter_already_liked_items=True
        )
        return [item_inv_mapping[item] for item, _ in recs]

    return _recs_mapper


def get_title_and_author(data: pd.DataFrame, books_full: pd.DataFrame, itemid2recid: dict):
    """Get author annd title for item_id.

    Args:
        data (pd.DataFrame): Table with user_id, item_id
        cat (pd.DataFrame): Table with authors, titles and recId
        itemid2recid (dict): Mapping dictionary from item_id to recId

    Returns:
        pd.DataFrame: Table with user_id, recId, author, title
    """
    data_copy = data.copy()
    data_copy["recId"] = data_copy["item_id"].map(itemid2recid)
    data_copy = data_copy.merge(books_full[["recId", "title", "author"]], on="recId")
    data_copy = data_copy.drop("item_id", axis=1)
    data_copy["user_id"] = data_copy["user_id"].astype(int)
    data_copy["recId"] = data_copy["recId"].astype(int)
    return data_copy


def prepare_for_saving(
    data: pd.DataFrame, is_recs: bool, books_full: pd.DataFrame, itemid2recid: dict
):
    data = get_title_and_author(data, books_full, itemid2recid=itemid2recid)
    cols = (
        ["user_id", "recId", "title", "author", "ranking"]
        if is_recs
        else ["user_id", "recId", "title", "author"]
    )
    data = data[cols]
    data["title"] = data["title"].str[:100]
    data["author"] = data["author"].str[:100]
    data = data.rename(columns={"recId": "item_id"})
    data = (
        data.replace(",", "", regex=True)
        .replace(r"\n", " ", regex=True)
        .replace(r"\r", " ", regex=True)
    )
    data = data.fillna("неизвестно")
    return data
