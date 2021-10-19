from prepare_data import (
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

    generate_recommendations_file(model=model, user_items=sparse_item_user.T, N=top_K)
    generate_history_file(interactions)


if __name__ == "__main__":
    main()
