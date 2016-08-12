import numpy as np
import pandas as pd


def pca_output(pca, features):
    components = {}
    for i, c in enumerate(pca.components_):
        vr = pca.explained_variance_ratio_[i]
        components[i] = np.concatenate([[vr], c])
    print pd.DataFrame(components, index=['variance_ratio'] + features)


def ica_output(ica, features):
    components = {}
    for i, c in enumerate(ica.components_):
        vr = np.linalg.norm(c)
        components[i] = np.concatenate([[vr], c])
    print pd.DataFrame(components, index=['significance'] + features)
