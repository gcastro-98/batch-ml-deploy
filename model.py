from pandas import DataFrame, Series
from sklearn.linear_model import LogisticRegression
from sklearn.utils import shuffle
import pickle

RANDOM_STATE: int = 123


def serialize(train: DataFrame) -> None:
    features: DataFrame = train.drop(["Species"], axis=1)
    labels: Series = train["Species"]

    features, labels = shuffle(features, labels, random_state=RANDOM_STATE)

    clf = LogisticRegression(random_state=RANDOM_STATE)
    clf.fit(features, labels)

    with open('model.pkl', 'wb') as serialized_model:
        pickle.dump(clf, serialized_model)


def load() -> LogisticRegression:
    with open('model.pkl', 'rb') as serialized_model:
        return pickle.load(serialized_model)
