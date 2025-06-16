import numpy as np
import warnings
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.utils import  assert_all_finite
from sklearn.utils.validation import check_consistent_length, column_or_1d


warnings.simplefilter(action = "ignore", category = FutureWarning)
pd.options.mode.chained_assignment = None


def get_woe_transformed_data(X, dictGrouping, raiseErrorIfNotInGrouping=False, printLog=True):
    dfWoe = pd.DataFrame()
    if printLog:
        iterator = X.columns
    else:
        iterator = X.columns

    for col in iterator:
        if (col in dictGrouping):
            bins_woes = dictGrouping[col]['bins_woes']
            bins = dictGrouping[col]['bins']
            unknown_woe = dictGrouping[col]['unknown_woe']
            colType = dictGrouping[col]['type']

            if(colType == 'categorical'):
                dfWoe[col+'_woe'] = X[col].map(bins_woes).fillna(unknown_woe)

            elif(colType == 'numerical'):
                dfWoe[col+'_woe'] = pd.cut(X[col], bins, labels = False).map(bins_woes).fillna(unknown_woe)
        else:
            if (raiseErrorIfNotInGrouping):
                raise ValueError(f'{col} is not present in the grouping dictionary')
    return dfWoe


def get_auto_grouping(X, y, cols_pred_num, cols_pred_cat, group_count, min_samples, min_samples_cat, max_levels=500, woe_smooth_coef=0.0001, bins=None, w=None):
    dictAllGrouping = {}

    for col in X.columns:        
        if col in cols_pred_num :
            dictColGrouping = {}
            x = X[col].copy()
            
            if (len(x.unique()) > 1):
                bins, woes, unknown_woe = auto_group_continuous(x, y, group_count, min_samples, woe_smooth_coef)

                dictColGrouping['type'] = 'numerical'
                dictColGrouping['bins'] = bins
                dictColGrouping['woes'] = woes
                dictColGrouping['unknown_woe'] = unknown_woe

                dict_bins_woe_map = {}
                for kvp in range(len(woes)):
                    dict_bins_woe_map[kvp] = woes[kvp]

                dictColGrouping['bins_woes'] = dict_bins_woe_map
                dictAllGrouping[col] = dictColGrouping

        elif col in cols_pred_cat:            
            dictColGrouping = {}
            x = X[col].astype(str)

            if len(x.unique()) > max_levels:
                raise ValueError(f'{col} has more than {max_levels} levels.')

            if (len(x.unique()) > 1):                
                bins, woes, unknown_woe = auto_group_categorical(x, y, group_count, min_samples, min_samples_cat, woe_smooth_coef)

                dictColGrouping['type'] = 'categorical'
                dictColGrouping['bins'] = bins
                dictColGrouping['woes'] = woes
                dictColGrouping['unknown_woe'] = unknown_woe

                dict_bins_woe_map = {}
                for kvp in bins:
                    dict_bins_woe_map[kvp] = woes[bins[kvp]]

                dictColGrouping['bins_woes'] = dict_bins_woe_map
                dictAllGrouping[col] = dictColGrouping

    return dictAllGrouping


def get_categorical_auto_woe_grouping(X, y, group_count, min_samples, min_samples_cat, max_levels = 500, woe_smooth_coef = 0.0001, bins=None, w=None):
    dictAllGrouping = {}
    
    for col in X.columns:
        dictColGrouping = {}
        x = X[col].astype(str)
        if x.nunique() > max_levels:
            raise ValueError(f'{col} has more than {max_levels} levels.')
        bins, woes, unknown_woe = auto_group_categorical(x, y, group_count, min_samples, min_samples_cat, woe_smooth_coef)

        dictColGrouping['type'] = 'categorical'
        dictColGrouping['bins'] = bins
        dictColGrouping['woes'] = woes
        dictColGrouping['unknown_woe'] = unknown_woe
        
        dict_bins_woe_map = {}

        for kvp in bins:
            dict_bins_woe_map[kvp] = woes[bins[kvp]]
        
        dictColGrouping['bins_woes'] = dict_bins_woe_map        
        dictAllGrouping[col] = dictColGrouping

    return dictAllGrouping


def get_continuous_auto_woe_grouping(X, y, group_count, min_samples,  woe_smooth_coef = 0.0001, bins=None, w=None):    
    dictAllGrouping = {}

    for col in X.columns:        
        dictColGrouping = {}
        x = X[col].copy()

        bins, woes, unknown_woe = auto_group_continuous(x, y, group_count, min_samples, woe_smooth_coef)

        dictColGrouping['type'] = 'numerical'
        dictColGrouping['bins'] = bins
        dictColGrouping['woes'] = woes
        dictColGrouping['unknown_woe'] = unknown_woe

        dict_bins_woe_map = {}
        for kvp in range(len(woes)):
            dict_bins_woe_map[kvp] = woes[kvp]

        dictColGrouping['bins_woes'] = dict_bins_woe_map        
        dictAllGrouping[col] = dictColGrouping
    
    return dictAllGrouping


def get_categorical_woe_transformed_data(X, dictGrouping):
    dfWoe = pd.DataFrame()
    for col in X.columns:
        if(col in dictGrouping):
            dfWoe[col+'_woe'] = X[col].map(dictGrouping[col]['bins_woes']).fillna(dictGrouping[col]['unknown_woe'])
        
    return dfWoe


def get_continuous_woe_transformed_data(X, dictGrouping):
    dfWoe = pd.DataFrame()
    for col in X.columns:
        if(col in dictGrouping):
            dfWoe[col+'_woe'] = pd.cut(X[col], dictGrouping[col]['bins'], labels=False).map(dictGrouping[col]['bins_woes']).fillna(dictGrouping[col]['unknown_woe'])
        
    return dfWoe


def woe(y, y_full, smooth_coef =0.0001, w=None, w_full=None):

    if smooth_coef < 0:
        raise ValueError('Smooth_coef should be non-negative')

    y = column_or_1d(y)
    y_full = column_or_1d(y_full)

    if y.size > y_full.size:
        raise ValueError('Length of y_full should be >= length of y')

    if len(set(y) - {0, 1}) > 0:
        raise ValueError('y should consist just of {0, 1}')

    if not np.array_equal(np.unique(y_full), [0, 1]):
        raise ValueError('y_full should consist of {0, 1}, not should be presented')

    if w is not None and y.size != w.size:
        raise ValueError('Size of y and w must be the same')

    if w_full is not None and y_full.size != w_full.size:
        raise ValueError('Size of y_full and w_full must be the same')

    if w is None:
        w = np.ones(len(y))

    if w_full is None:
        w_full = np.ones(len(y_full))

    if y.size == 0:
        return 0.

    woe = np.log( (sum((1-y) * w)/sum(w) + smooth_coef) / (sum(y * w)/sum(w) + smooth_coef) ) - \
          np.log( (sum((1-y_full) * w_full)/sum(w_full) + smooth_coef) / (sum(y_full * w_full)/sum(w_full) + smooth_coef) )

    return woe


def tree_based_grouping(x, y, group_count, min_samples, w=None):
    check_consistent_length(x, y)
    x = column_or_1d(x)
    assert_all_finite(x)
    y = column_or_1d(y)

    if len(set(y) - {0, 1}) > 0:
        raise ValueError('y should consist just of {0,1}')

    notnan_mask = ~np.isnan(x)
    
    if w is not None:
        check_consistent_length(y, w)
        w = column_or_1d(w)[notnan_mask]
    
    x = x.reshape(x.shape[0], -1)  # (n,) -> (n, 1)
        
    clf = DecisionTreeClassifier(
        max_leaf_nodes=group_count, min_samples_leaf=min_samples)
    clf.fit(x[notnan_mask], y[notnan_mask], sample_weight=w)

    return np.concatenate([np.array([-np.inf]), np.sort(clf.tree_.threshold[clf.tree_.feature == 0]), np.array([np.inf])])


def auto_group_continuous(x, y, group_count, min_samples, woe_smooth_coef, bins=None, w=None):
    notnan_mask = x.notnull()
    if w is not None:
        w_nna = w[notnan_mask]
    else:
        w_nna = None
        
    if bins is None:
        bins = tree_based_grouping(x[notnan_mask], y[notnan_mask], group_count, min_samples, w=w_nna)
        # temporary DataFrame since we need both x and y in grouping / aggregation
    
    if w is not None:
        df = pd.DataFrame({'x': x, 'y': y, 'w': w})
    else:
        w1 = np.ones(len(x))
        df = pd.DataFrame({'x': x, 'y': y, 'w': w1})

    df.loc[pd.isnull(df['y']),'w'] = np.nan
    bin_indices = pd.cut(df[notnan_mask]['x'], bins=bins, right=False, labels=False)
    #sg Some values can be missing in new data
    woes = np.zeros(bins.shape[0] - 1)
    new_woes = df.groupby(bin_indices).apply(lambda rows: woe(rows['y'], df['y'], woe_smooth_coef, w=rows['w'], w_full=df['w'])).to_dict()

    np.put(woes, list(new_woes.keys()), list(new_woes.values()))
    nan_woe = woe(df[~notnan_mask]['y'], df['y'], woe_smooth_coef, w=df[~notnan_mask]['w'], w_full=df['w'])
    return bins, woes, nan_woe
    
    
def auto_group_categorical(x, y, group_count, min_samples, min_samples_cat, woe_smooth_coef, bins=None, w=None):
    """
    :returns: (bins, woes)
              bins - dict(value->group number)
              woes - array of woes
    """
    # temporary DataFrame since we need both x and y in grouping / aggregation
    #print('auto_group_categorical')
    if w is not None:
        df = pd.DataFrame({'x': x, 'y': y, 'w': w})
    else:
        w1 = np.ones(len(x))
        df = pd.DataFrame({'x': x, 'y': y, 'w': w1})
    df.loc[pd.isnull(df['y']),'w'] = np.nan
    df['wy'] = df['w'] * df['y']

    if bins is None:
        stats = df.groupby('x').apply(lambda rows: pd.Series(index=['cnt', 'cnt_bads'], data=[rows['w'].sum(), rows['wy'].sum()]))
        stats['event_rate'] = np.nan
        stats.loc[stats['cnt'] > 0, 'event_rate'] = stats['cnt_bads']/stats['cnt']
        nan_stat = pd.Series(index=['cnt', 'cnt_bads'], data=[df[df.x.isnull()]['w'].sum(), df[df.x.isnull()]['wy'].sum()])
        if nan_stat['cnt'] > 0:
            nan_stat['event_rate'] = nan_stat['cnt_bads']/nan_stat['cnt']
        else:
            nan_stat['event_rate'] = np.nan

        rare_mask = stats['cnt'] < min_samples_cat
        rare_values = stats[rare_mask].index.values   
        rare_w = df.join(pd.DataFrame(index=rare_values), on='x', how='inner')['w'].values
        rare_wy = df.join(pd.DataFrame(index=rare_values), on='x', how='inner')['wy'].values

        #sg!!!
        # cat -> statistically significant event-rate
        mapping = stats[~rare_mask]['event_rate'].to_dict()

        if nan_stat['cnt'] >= min_samples_cat:
            mapping[np.nan] = nan_stat['event_rate']

        elif nan_stat['cnt'] > 0:
            rare_values = np.append(rare_values, np.nan)
            rare_w = np.append(rare_w, df[df.x.isnull()].w.values)
            rare_wy = np.append(rare_wy, df[df.x.isnull()].wy.values)
        
        mapping.update({v: rare_wy.sum()/rare_w.sum() for v in rare_values})
        
        # new continuous column
        x2 = df.x.replace(mapping)
       
        bins = tree_based_grouping(
            x2, y, group_count, min_samples, w=w)
        
        #mapping: cat -> ER
        #bins: ER [-inf, 0.1, 0.34,+inf]] 
        
        # sg - rewrite - now duplicated functionality with "else" below
        bin_indices = pd.cut(x2, bins=bins, right=False, labels=False)
        woes = df.groupby(bin_indices).apply(lambda rows: woe( \
            rows['y'], df['y'], woe_smooth_coef, w=rows['w'], w_full=df['w'])).values

        # cat -> group number Series
        #groups = pd.cut(pd.Series({value: er for value, er in mapping.items() if not (isinstance(value, float) and np.isnan(value))}), 

        groups = pd.cut(pd.Series(mapping), bins=bins, right=False, labels=False)
        bins = groups.to_dict()

        if nan_stat['cnt'] == 0:
            #nan_group = pd.cut([m[np.nan]], bins=bins, right=False, labels=False)[0]
            # new group for nan with WOE=0.
            nan_group = groups.max() + 1
            woes = np.append(woes, [0.]) 
            bins[np.nan] = nan_group
        
        #WOE for values that are not present in the training set
        unknown_woe = 0
        
    else:
        #sg duplication here!
        # this branch was added to support "recalc WOEs on fixed bins (splits)" mode
        #{1: 2, 3: 2, 4: 0}
        df['tmp'] = np.nan
        for cat, g in bins.items():
            if type(cat) == float and np.isnan(cat):
                df.loc[df.x.isnull(), 'tmp'] = g
            else:
                df.loc[df.x == cat, 'tmp'] = g
        #sg Some values can be missing in new data
        woes = np.zeros(len(bins))
        new_woes = df.groupby(df['tmp']).apply(lambda rows: woe( \
            rows['y'], df['y'], woe_smooth_coef, w=rows['w'], w_full=df['w'])).to_dict()
        np.put(woes, list(new_woes.keys()), list(new_woes.values()))
        unknown_woe = 0
    return bins, woes, unknown_woe     
