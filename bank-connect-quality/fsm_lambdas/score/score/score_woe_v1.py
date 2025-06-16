import warnings
import pandas as pd
import numpy as np
from score.automatic_woe import *


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def scale_fbcs_score(sc):
    buckets_score = {
        (0, 0.0008): (900, 880),
        (0.0008, 0.2236): (880, 400),
        (0.2236, 1): (400, 300)
    }
    for bucket in buckets_score:
        if ((sc < bucket[1]) & (sc >= bucket[0])):
            bucket_to_apply = bucket
            break
    distanceFromUpper = (bucket[1] - sc)/(bucket[1] - bucket[0])
    valueToScale = buckets_score[bucket]
    distanceMultiplier = round(valueToScale[1] + distanceFromUpper * (valueToScale[0] - valueToScale[1]),0)
    return distanceMultiplier

def score_function(bank_data):
    autogrouping = pd.read_pickle('pickle-files/2022-11-08/autogrouping_bank_v6_all.pkl')
    model = pd.read_pickle('pickle-files/2022-11-08/lrmodel_cv_bank_v6_all.pkl')
    pred = pd.read_pickle('pickle-files/2022-11-08/lrmodel_cv_bank_v6_predictors_all.pkl')

    x_bank_woe =  get_woe_transformed_data(bank_data[[x.replace('_woe','') for x in pred]].fillna(np.nan), autogrouping)
    pred_bank = model.predict_proba(x_bank_woe[pred])[:,1]
    return scale_fbcs_score(pred_bank[0])