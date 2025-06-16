import warnings
import pandas as pd
from score.automatic_woe import *


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def scale_fbcs_score(sc):
    '''
    This function is used to calculate scaled score of the probability
    Parameter:
        sc(float): Probability of Score
    Returns:
        distancemultiplier(int) : Scaled score between 300-900 of the probability
    '''
    buckets_score = {
        (0, 0.042329): (900, 800),
        (0.042329, 0.22): (800, 400),
        (0.22, 1): (400, 300),
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
    '''
    This Function is used to calculate the score base on the given parameters
    Parameter:
        bank_data(dataframe): It contains a df having all the predictor values
    Return:
        It calls a funtion(scale_fbcs_score) which takes probability as
        input and returns scaled score
    '''
    autogrouping = pd.read_pickle('pickle-files/2023-03-02/Autogrouping_BankConncet_score_v2_2023-03-02.pkl')
    model = pd.read_pickle('pickle-files/2023-03-02/BankConnect_score_v2_model_2023-03-02.pkl')
    pred = pd.read_pickle('pickle-files/2023-03-02/Predictors_BankConnect_score_v2_2023-03-02.pkl')

    x_bank_woe =  get_woe_transformed_data(bank_data[[x.replace('_woe','')\
                    for x in pred]].fillna(np.nan), autogrouping)
    pred_bank = model.predict_proba(x_bank_woe[pred])[:,1]
    return scale_fbcs_score(pred_bank[0])
