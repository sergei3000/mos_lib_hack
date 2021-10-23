from gensim.models import Word2Vec
import os
import pandas as pd
import pickle5 as pickle

def load_mapping(filename):
    """
    Helper function for reading pickle data.
    """
    with open(filename + '.pkl', 'rb') as handle:
        return pickle.load(handle)

class Word2VecRecommender(object):
    """
    Recommender instance based on books context co-occurences.
    
    Args:
        model_path (str): path to the saved model.
        data_path (str): path the data folder.
        vector_size (int): hidden size for embeddings each of which represent a certain book.
        window (int): context window size.
        workers (int): number of cpu cores to train the model.
        iter (int): number of epochs. 
    """
    def __init__(self, model_path=None, data_path='data', vector_size=100, window=100, workers=4, iter=5, **kwargs):
        
        df_cat = pd.read_parquet(os.path.join(data_path, 'cat.parquet.gzip'))
        self.recid2title = dict(zip(df_cat['recId'], df_cat['title']))
        self.recid2iid = load_mapping(os.path.join(data_path, 'recid2iid'))
        self.iid2recid = {v:k for k, v in self.recid2iid.items()}
            
        if not model_path:
            df = pd.read_parquet(os.path.join(data_path,'circulatons.parquet.gzip'))            
            grouped = df.groupby('user_id').agg(list).reset_index()
            self.items = grouped.item_id.apply(lambda x: [str(y) for y in x])
            self.items = list(items.values)

            # train w2v model
            self.model = Word2Vec(sentences=self.items, size=vector_size, window=window,
                                  workers=workers, min_count=1, **kwargs)
            
        else:
            # raise NotImplementedError
            self.model = Word2Vec.load(model_path)

    
    
    def recommend(self, item_ids, topN=5):
        """
        Recommend books using the history.
        
        Args:
            items_ids (List[str]): feedback history.
            topN (int) number of recommendations to return.
            
        Returns:
            dict: A dictionary with "recommendations" and "history" fields, both of them are lists of books' titles.
        """
        positives = [str(x) for x in item_ids if str(x) in self.model.wv]
        recommendations = self.model.wv.most_similar(positives, topn=topN)
        recommendations = [self.recid2title[self.iid2recid[int(x[0])]] for x in recommendations]
        history = [self.recid2title[self.iid2recid[int(y)]] for y in item_ids]
        return {'recommendations': recommendations, 'history': history}
    
    
    def save(self, model_path):
        """
        Save item2vec model.
        """
        self.model.save(model_path)


def main(data_path='data', topN=5):
    # create new instance (train item2vec)
    w2v = Word2VecRecommender(data_path=data_path, model_path=None)
    # assuming dataset_knigi.csv – already preprocessed .xlsx file
    # https://github.com/sergei3000/mos_lib_hack/blob/d443af99b348bc0dec63d078c8ee48b90be9b24b/ml_pipelines/als/prepare_data.py#L122
    grouped = pd.read_csv(os.path.join(data_path, 'dataset_knigi.csv')).dropna().groupby('user_id').agg(list).reset_index()
    items = grouped.item_id.apply(lambda x: [str(int(y)) for y in x]).values
    users = grouped.user_id
    recs = []
    # recommend for each user
    for i in range(len(users)):
        recs.append(w2v.recommend(items[i], topN=topN)['recommendations'])
    # crate a resulting dataframe
    
    result = pd.DataFrame(recs, columns = ['book_id_{}'.format(i+1) for i in range(topN)]) 
    result['user_id'] = users
    # save dataframe
    result.to_csv('recommendations_item2vec.csv', index=False)
    
if __name__ == '__main__':
    main()
