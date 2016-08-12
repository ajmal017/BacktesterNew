from __future__ import division
import numpy as np
import pandas as pd


def cross_validation(data, clf, feature_columns, label='label', k=3, p_cutoffs=(0.5, 0.5),
                     fit_method=lambda cl, X, y: cl.fit(X, y),
                     predict_method=lambda cl, X: cl.predict(X)):

    #data.apply(np.random.shuffle, axis=0)
    partitions = np.array_split(data, k)

    weights = []

    three_class = len(data[data['label'] == 0]) > 0

    results = {
        'run': range(1, k+1) + ['Total'],
        'acc': [0]*(k+1),
        'acc*': [0]*(k+1),
        'fpr': [0]*(k+1),
        'fnr': [0]*(k+1),
        'bpr': [0]*(k+1),
        'gpr': [0]*(k+1),
        '-1': [0]*(k+1),
        '1': [0]*(k+1)
    }

    if three_class:
        results['0'] = [None]*(k+1)

    for k in xrange(k):
        training_data = pd.concat(partitions[:k] + partitions[(k+1):])
        testing_data = partitions[k]
        train_x, train_y = training_data[feature_columns], training_data[label]
        test_x, test_y = testing_data[feature_columns], testing_data[label]
        fit_method(clf, train_x, train_y)
        pred_y = predict_method(clf, test_x)
        probs = clf.predict_proba(test_x).T[1]
        test_y = test_y[(probs <= p_cutoffs[0]) | (probs >= p_cutoffs[1])]
        pred_y = pred_y[(probs <= p_cutoffs[0]) | (probs >= p_cutoffs[1])]
        n = np.size(test_y)
        results['acc'][k] = np.sum(pred_y == test_y) / n
        results['acc*'][k] = np.mean([np.sum((pred_y == test_y) & (test_y == L)) / np.sum(test_y == L) for L in ([-1, 0, 1] if three_class else [-1, 1])])
        if three_class:
            results['fpr'][k] = np.sum((pred_y != test_y) & (pred_y != 0)) / np.sum(pred_y != 0)
            results['fnr'][k] = np.sum((pred_y != test_y) & (pred_y == 0)) / np.sum(pred_y == 0)
        results['bpr'][k] = np.sum((pred_y * test_y) == -1) / np.sum(test_y != 0)
        results['gpr'][k] = np.sum((pred_y == test_y) & (test_y != 0)) / np.sum(test_y != 0)
        results['-1'][k] = np.sum(pred_y == -1)
        if three_class:
            results['0'][k] = np.sum(pred_y == 0)
        results['1'][k] = np.sum(pred_y == 1)

        if hasattr(clf, 'coef_'):
            W = clf.coef_
            weights.append(pd.DataFrame(W.T, index=feature_columns))

    for col in results:
        if col != 'run':
            results[col][k] = np.mean(results[col][:k])
            print results[col][:k]
            print np.mean(results[col][:k])

    if hasattr(clf, 'coef_'):
        mean_weights = sum(weights) / k
    else:
        mean_weights = []

    return pd.DataFrame(results).set_index('run'), mean_weights


def cross_validation_multi(data, clf, feature_columns, pred_windows, label_prefix='label', k=3, p_cutoffs=(0.5, 0.5),
                     fit_method=lambda cl, X, y: cl.fit(X, y),
                     predict_method=lambda cl, X: cl.predict(X)):

    #data.apply(np.random.shuffle, axis=0)
    partitions = np.array_split(data, k)

    weights = []

    #three_class = len(data[data['label'] == 0]) > 0
    three_class = False

    results = {
        'run': range(1, k+1) + ['Total'],
        'acc': [0]*(k+1),
        'acc*': [0]*(k+1),
        'fpr': [0]*(k+1),
        'fnr': [0]*(k+1),
        'bpr': [0]*(k+1),
        'gpr': [0]*(k+1),
        '-1': [0]*(k+1),
        '1': [0]*(k+1)
    }

    best_prob_counts = [pd.DataFrame()] * (k+1)

    if three_class:
        results['0'] = [None]*(k+1)

    for k in xrange(k):
        training_data = pd.concat(partitions[:k] + partitions[(k+1):])
        testing_data = partitions[k]
        probs = pd.DataFrame()
        pred_y = pd.DataFrame()
        test_y = pd.DataFrame()
        for pred_window in pred_windows:
            label = "{}_{}".format(label_prefix, pred_window)
            train_x, train_y = training_data[feature_columns], training_data[label]
            test_x, test_y[pred_window] = testing_data[feature_columns], testing_data[label]
            fit_method(clf, train_x, train_y)
            pred_y[pred_window] = predict_method(clf, test_x)
            probs[pred_window] = clf.predict_proba(test_x).T[1]
        test_y = test_y.reset_index()
        pred_y = pred_y.reset_index()
        best_windows = pd.Series(np.abs(probs[pred_windows] - 0.5).idxmax(axis=1))
        best_test_y = test_y.lookup(best_windows.index, best_windows.values)
        best_pred_y = pred_y.lookup(best_windows.index, best_windows.values)
        best_probs = probs.reset_index().lookup(best_windows.index, best_windows.values)
        test_y = best_test_y[(best_probs <= p_cutoffs[0]) | (best_probs >= p_cutoffs[1])]
        pred_y = best_pred_y[(best_probs <= p_cutoffs[0]) | (best_probs >= p_cutoffs[1])]
        n = np.size(test_y)
        results['acc'][k] = np.sum(pred_y == test_y) / n
        results['acc*'][k] = np.mean([np.sum((pred_y == test_y) & (test_y == L)) / np.sum(test_y == L) for L in ([-1, 0, 1] if three_class else [-1, 1])])
        if three_class:
            results['fpr'][k] = np.sum((pred_y != test_y) & (pred_y != 0)) / np.sum(pred_y != 0)
            results['fnr'][k] = np.sum((pred_y != test_y) & (pred_y == 0)) / np.sum(pred_y == 0)
        results['bpr'][k] = np.sum((pred_y * test_y) == -1) / np.sum(test_y != 0)
        results['gpr'][k] = np.sum((pred_y == test_y) & (test_y != 0)) / np.sum(test_y != 0)
        results['-1'][k] = np.sum(pred_y == -1)
        if three_class:
            results['0'][k] = np.sum(pred_y == 0)
        results['1'][k] = np.sum(pred_y == 1)

        best_prob_counts[k] = pd.Series(best_windows).value_counts()
        best_prob_counts[k]['None'] = len(test_x) - len(test_y)

        if hasattr(clf, 'coef_'):
            W = clf.coef_
            weights.append(pd.DataFrame(W.T, index=feature_columns))

    for col in results:
        if col != 'run':
            results[col][k] = np.mean(results[col][:k])
            print results[col][:k]
            print np.mean(results[col][:k])

    if hasattr(clf, 'coef_'):
        mean_weights = sum(weights) / k
    else:
        mean_weights = []

    return pd.DataFrame(results).set_index('run'), mean_weights, best_prob_counts


def clf_output(cv_results, y, K):
    print """
                                 Results
==============================================================================
    """
    print pd.DataFrame({'%': np.array([len(y[y == -1]),
                                len(y[y == 0]),
                                len(y[y == 1])])/len(y)},
                       index=[-1, 0, 1])
    print pd.DataFrame({'values': [(len(y)/K)*(K-1)]}, index=['Training Size / Fold'])
    print cv_results[0]
    if len(cv_results) > 2:
        print cv_results[2]
    print cv_results[1]
    print "=============================================================================="