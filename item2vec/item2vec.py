from gensim.models import Word2Vec
import os
import pandas as pd
import pickle5 as pickle

def load_mapping(filename):
    with open(filename + '.pkl', 'rb') as handle:
        return pickle.load(handle)

class Word2VecRecommender(object):
    def __init__(self, model_path=None, data_path='data', vector_size=100, window=100, workers=4, iter=5, **kwargs):
        
        df_cat = pd.read_parquet(os.path.join(data_path, 'cat.parquet.gzip'))
        self.recid2title = dict(zip(df_cat['recId'], df_cat['title']))
        self.recid2iid = load_mapping('recid2iid')
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

    
    
    def recommend(self, item_ids, topN=5, user_id=None):
        positives = [str(x) for x in item_ids if str(x) in self.model.wv]
        recommendations = self.model.wv.most_similar(positives, topn=topN)
        recommendations = [self.recid2title[self.iid2recid[int(x[0])]] for x in recommendations]
        history = [self.recid2title[self.iid2recid[int(y)]] for y in item_ids]
        return {'recommendations': recommendations, 'history': history}
    
    
    def save(self, model_path):
        self.model.save(model_path)
