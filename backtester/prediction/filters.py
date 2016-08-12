from __future__ import division

import features as feat


@feat.feature
def bbo_change(data):
    if 'bbo_change' not in data.columns:
        feat.bbo_change()(data)
    return (data['bid_change'] != 0) | (data['ask_change'] != 0).values