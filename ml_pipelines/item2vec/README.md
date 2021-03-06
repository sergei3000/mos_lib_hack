# Item2vec

* Модель, базированная на методе **skip-gram**, учится для слова (в данном случае книги) предсказывать его контекст. Основной класс – **Word2VecRecommender**.
* Основная идея – выучить векторы книг аналогично тому, как **word2vec** выучивает векторы слов имея множество примеров каждого слова в разных контекстах (под контекстом подразумеваются слова, стоящие рядом в предложении). В данном случае мы выучиваем векторы книг в зависимости от того, с какими книгами они берутся вместе.
* Далее считаем, что среднее векторов книг по истории пользователя – вектор пользователя (=средняя книга среди взятых), к которому можно найти ближайшие *topN* книг по косинусной близости. 
* Чтобы обучить модель и подготовить рекомендации, нужно запустить команду `python item2vec.py`.
* В файл *recommendations_item2vec.csv* сохранятся рекомендации для каждого из пользователей *dataset_knigi.csv*.
* По умолчанию, все необходимые данные должны лежать в папке **/data** относительно корня модуля.
