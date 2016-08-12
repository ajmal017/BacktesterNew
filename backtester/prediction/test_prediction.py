import numpy as np
from sklearn import svm

from prediction.cross_validation import cross_validation, cross_validation_multi, clf_output
import prediction.featutils as featutils


def test_classifier(data, pred_window, thresh, features, classifier=svm.LinearSVC, p_cutoffs=(0.5, 0.5),
                    pred_col_prefix='log_returns', k=3, return_new_clf=False, omit0=False, **kwargs):
    pred_col = '{}_{}w+'.format(pred_col_prefix, pred_window)
    
    featutils.label_data(data, pred_col, thresh)

    if omit0:
        data.ix[data['label'] == 0, 'label'] = 1

    clf = classifier(**kwargs)
    cv_results = cross_validation(data, clf, features, label='label', k=k, p_cutoffs=p_cutoffs)
    y = data['label'].values
    # yo
    print """
    {}, {}-class {}-fold CV, {}-p-cutoffs, auto class weights
    """.format(classifier, len(np.unique(y)), k, str(p_cutoffs))
    print "Pred_col = {}".format(pred_col)
    print "thresh = {}".format(thresh)
    for key in kwargs:
        print "{} = {}".format(key, kwargs[key])
    clf_output(cv_results, y, k)

    if return_new_clf:
        clf = classifier(**kwargs)
        clf.fit(data[features].values, data['label'].values)
        return clf
    else:
        return clf


def test_classifier2(data, pred_window, spread_penalty, thresh, features, classifier=svm.LinearSVC, p_cutoffs=(0.5, 0.5),
                    pred_col_prefix='future_pnl_l2', k=3, return_new_clf=False, **kwargs):
    pred_col_suffix = '{}w+_{}sp'.format(pred_window, spread_penalty)

    featutils.label_data_future_pnl(data, pred_col_prefix, pred_col_suffix, thresh)

    clf = classifier(**kwargs)
    cv_results = cross_validation(data, clf, features, label='label', k=k, p_cutoffs=p_cutoffs)
    y = data['label'].values

    print """
    {}, {}-class {}-fold CV, auto class weights
    """.format(classifier, len(np.unique(y)), k)
    print "Pred_col_suffix = {}".format(pred_col_suffix)
    print "thresh = {}".format(thresh)
    for key in kwargs:
        print "{} = {}".format(key, kwargs[key])
    clf_output(cv_results, y, k)

    if return_new_clf:
        clf = classifier(**kwargs)
        clf.fit(data[features].values, data['label'].values)
        return clf
    else:
        return clf


def test_multi_classifier(data, pred_windows, thresh, features, classifier=svm.LinearSVC, p_cutoffs=(0.5, 0.5),
                    pred_col_prefix='log_returns', k=3, return_new_clf=False, omit0=False, **kwargs):

    for pred_window in pred_windows:
        pred_col = '{}_{}w+'.format(pred_col_prefix, pred_window)
        featutils.label_data(data, pred_col, thresh, label_name='label_{}'.format(pred_window))
        if omit0:
            data.ix[data['label_{}'.format(pred_window)] == 0, 'label_{}'.format(pred_window)] = 1

    clf = classifier(**kwargs)
    cv_results = cross_validation_multi(data, clf, features, pred_windows, label_prefix='label', k=k, p_cutoffs=p_cutoffs)
    y = data['label_{}'.format(pred_windows[0])].values

    print """
    {}, {}-class {}-fold CV, {}-p-cutoffs, auto class weights
    """.format(classifier, len(np.unique(y)), k, str(p_cutoffs))
    print "Pred_windows = {}".format(str(pred_windows))
    print "thresh = {}".format(thresh)
    for key in kwargs:
        print "{} = {}".format(key, kwargs[key])
    clf_output(cv_results, y, k)

    if return_new_clf:
        clf = classifier(**kwargs)
        clf.fit(data[features].values, data['label_{}'.format(pred_windows[0])].values)
        return clf
    else:
        return clf