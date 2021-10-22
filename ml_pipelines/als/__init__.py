import pandas as pd
from prepare_data import (
    prepare_cat_file,
    prepare_books_full_file,
    prepare_mappings_for_recid,
    prepare_circulatons_file,
    prepare_dataset_knigi_file,
)
from generate_recommendations import (
    read_data,
    save_mappings_dicts,
    train_model,
    generate_recommendations_file,
    generate_history_file,
    get_top_items,
)

top_K = 5
CIRCULATONS_FILE = "../../data/circulatons.parquet.gzip"
DATASET_KNIGI_FILE = "../../data/dataset_knigi.csv"


def main():
    prepare_mappings_for_recid()
    prepare_circulatons_file()
    prepare_dataset_knigi_file()

    interactions = read_data(
        circulatons_filename=CIRCULATONS_FILE, dataset_knigi_filename=DATASET_KNIGI_FILE
    )
    save_mappings_dicts(interactions)

    model, sparse_item_user = train_model(interactions)

    generate_history_file(interactions)
    all_recs_df = generate_recommendations_file(model=model, user_items=sparse_item_user.T, N=top_K)
    top_items_for_cold_user = get_top_items(interactions, N=top_K)
    all_recs_df = pd.concat([top_items_for_cold_user, all_recs_df]).reset_index(drop=True)
    all_recs_df.to_csv("../../data/recommendations.csv", index=False)


if __name__ == "__main__":
    main()
