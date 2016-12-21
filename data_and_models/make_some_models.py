from match_contracts2news import headline_finder

# Build features for a model:

from datetime import datetime, time

def create_training_set_for_contract(d):
    """ Accepts a dataset d, which should be all the data for a single contract, in order. """

    # Build x-vals
    d['nighttime'] = [(x.time() > time(19, 0) and x.time() < time(23, 59, 59)) or (x.time() >= time(0, 0) and x.time() < time(12, 0)) for x in d.record_timestamp]
    d['close_to_limit'] = [1 if x> .92 or x < .08 else 0 for x in d['best_buy_yes_cost']]
    d['bid_ask_spread'] = d['best_buy_yes_cost'] - d['best_sell_yes_cost']
    d['yes_buy_delta_1minback'] = [0.0] + [d['best_buy_yes_cost'][x] - d['best_buy_yes_cost'][x-1] for x in d.index[1:]]
    d['yes_buy_delta_2minback'] = [0.0, 0.0] + [d['best_buy_yes_cost'][x] - d['best_buy_yes_cost'][x-2] for x in d.index[2:]]
    d['yes_buy_delta_3minback'] = [0.0, 0.0, 0.0] + [d['best_buy_yes_cost'][x] - d['best_buy_yes_cost'][x-3] for x in d.index[3:]]
    d['bid_ask_spread_delta_1minback'] = [0.0] + [d['bid_ask_spread'][x] - d['bid_ask_spread'][x-1] for x in d.index[1:]]
    d['bid_ask_spread_delta_2minback'] = [0.0, 0.0] + [d['bid_ask_spread'][x] - d['bid_ask_spread'][x-2] for x in d.index[2:]]
    d['bid_ask_spread_delta_3minback'] = [0.0, 0.0, 0.0] + [d['bid_ask_spread'][x] - d['bid_ask_spread'][x-3] for x in d.index[3:]]
    d['last_trade_price_delta_1minback'] = [0.0] + [d['last_trade_price'][x] - d['last_trade_price'][x-1] for x in d.index[1:]]
    d['last_trade_price_delta_2minback'] = [0.0, 0.0] + [d['last_trade_price'][x] - d['last_trade_price'][x-2] for x in d.index[2:]]
    d['last_trade_price_delta_3minback'] = [0.0, 0.0, 0.0] + [d['last_trade_price'][x] - d['last_trade_price'][x-3] for x in d.index[3:]]

    # What is the rolling mean up to and including this point? How far off from it is this point?
    d['buy_yes_rolling_mean'] = pd.rolling_mean(arg = d['best_buy_yes_cost'], window = 10)
    d['buy_yes_rolling_mean_diff'] = d['best_buy_yes_cost'] - d['buy_yes_rolling_mean']
    d['sell_yes_rolling_mean'] = pd.rolling_mean(arg = d['best_sell_yes_cost'], window = 10)
    d['sell_yes_rolling_mean_diff'] = d['best_sell_yes_cost'] - d['sell_yes_rolling_mean']

    # Let's add the news headline data
    h = headline_finder()
    d['headlines_in_last_30min'] = [len(h.find(row['contract_ticker'], row['record_timestamp'])) for i, row in d.iterrows()]

    x_vals = d[['nighttime', 'close_to_limit', 'bid_ask_spread', 'yes_buy_delta_1minback',
                'yes_buy_delta_2minback', 'yes_buy_delta_3minback', 'bid_ask_spread_delta_1minback',
                'bid_ask_spread_delta_2minback', 'bid_ask_spread_delta_3minback', 'last_trade_price_delta_1minback',
                'last_trade_price_delta_2minback', 'last_trade_price_delta_3minback', 'best_buy_yes_cost',
                'buy_yes_rolling_mean_diff', 'sell_yes_rolling_mean_diff', 'headlines_in_last_30min']]

    # Build y-vals
    extended1_buy_yes_costs = d['best_buy_yes_cost'].append(pd.Series(d['best_buy_yes_cost'][len(d['best_buy_yes_cost']) - 1], index=[len(d['best_buy_yes_cost'])]))
    d['profit_in_1_min'] = [1 if (extended1_buy_yes_costs[x] < extended1_buy_yes_costs[x + 1]) else 0 for x in d.index]
    extended2_buy_yes_costs = extended1_buy_yes_costs.append(pd.Series(extended1_buy_yes_costs[len(extended1_buy_yes_costs) - 1], index=[len(extended1_buy_yes_costs)]))
    d['profit_in_2_min'] = [1 if (extended2_buy_yes_costs[x] < extended2_buy_yes_costs[x + 2]) else 0 for x in d.index]
    extended3_buy_yes_costs = extended2_buy_yes_costs.append(pd.Series(extended2_buy_yes_costs[len(extended2_buy_yes_costs) - 1], index=[len(extended2_buy_yes_costs)]))
    d['profit_in_3_min'] = [1 if (extended3_buy_yes_costs[x] < extended3_buy_yes_costs[x + 3]) else 0 for x in d.index]
    extended4_buy_yes_costs = extended3_buy_yes_costs.append(pd.Series(extended3_buy_yes_costs[len(extended3_buy_yes_costs) - 1], index=[len(extended3_buy_yes_costs)]))
    d['profit_in_4_min'] = [1 if (extended4_buy_yes_costs[x] < extended4_buy_yes_costs[x + 3]) else 0 for x in d.index]
    extended5_buy_yes_costs = extended4_buy_yes_costs.append(pd.Series(extended4_buy_yes_costs[len(extended4_buy_yes_costs) - 1], index=[len(extended4_buy_yes_costs)]))
    d['profit_in_5_min'] = [1 if (extended5_buy_yes_costs[x] < extended5_buy_yes_costs[x + 3]) else 0 for x in d.index]
    extended6_buy_yes_costs = extended5_buy_yes_costs.append(pd.Series(extended5_buy_yes_costs[len(extended5_buy_yes_costs) - 1], index=[len(extended5_buy_yes_costs)]))
    d['profit_in_6_min'] = [1 if (extended6_buy_yes_costs[x] < extended6_buy_yes_costs[x + 3]) else 0 for x in d.index]
    extended7_buy_yes_costs = extended6_buy_yes_costs.append(pd.Series(extended6_buy_yes_costs[len(extended6_buy_yes_costs) - 1], index=[len(extended6_buy_yes_costs)]))
    d['profit_in_7_min'] = [1 if (extended7_buy_yes_costs[x] < extended7_buy_yes_costs[x + 3]) else 0 for x in d.index]
    extended8_buy_yes_costs = extended7_buy_yes_costs.append(pd.Series(extended7_buy_yes_costs[len(extended7_buy_yes_costs) - 1], index=[len(extended7_buy_yes_costs)]))
    d['profit_in_8_min'] = [1 if (extended8_buy_yes_costs[x] < extended8_buy_yes_costs[x + 3]) else 0 for x in d.index]
    extended9_buy_yes_costs = extended8_buy_yes_costs.append(pd.Series(extended8_buy_yes_costs[len(extended8_buy_yes_costs) - 1], index=[len(extended8_buy_yes_costs)]))
    d['profit_in_9_min'] = [1 if (extended9_buy_yes_costs[x] < extended9_buy_yes_costs[x + 3]) else 0 for x in d.index]

    y_vals = [1 if (d['profit_in_1_min'][x] or d['profit_in_2_min'][x] or d['profit_in_3_min'][x] or
                    d['profit_in_4_min'][x] or d['profit_in_5_min'][x] or d['profit_in_6_min'][x] or
                    d['profit_in_7_min'][x] or d['profit_in_8_min'][x] or d['profit_in_9_min'][x]) else 0 for x in d['profit_in_9_min'].index ]

    return x_vals[15:-15], y_vals[15:-15]


def get_contract_data(contract_ticker, limit = 100):
    """ Pulls all data for contract, up to limit rows. """

    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    sql_statement = "select * from all_contracts where contract_ticker='{}' limit {}".format(contract_ticker,
                                                                                             limit)
    df = pd.read_sql(sql_statement, con)

    return df


# Get all contracts into a pd.series so we can cycle through them and building a training set

con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
# Get contracts with at least 3 different best_buy_yes_cost values:
sql_statement = "select contract_ticker from distinct_vals_per_contract where distinct_best_buy_yes_cost > 3 limit 50"
all_contracts = pd.read_sql(sql_statement, con).values.tolist()
print "Total contracts with enough distinct values: {}".format(len(all_contracts))

# del all_x # Should be commented out if running for the first time.
# del all_y

all_y = []
for i, c in enumerate(all_contracts[:100]):
    if i % 10 == 0: print i
    d = get_contract_data(c[0], limit = 3000)
    print i, c

    x1, y1 = create_training_set_for_contract(d)
    try:
        all_x = pd.concat([all_x, x1])
    except NameError:
        all_x = x1
    all_y += y1

all_x.index = range(len(all_x))

print len(all_x), len(all_y)


# Create train/test sets.

from random import random

train_indices = [True if random() < .8 else False for x in range(len(all_x))]
test_indices = [False if x else True for x in train_indices]
X_train = all_x.ix[train_indices]
y_train = [all_y[i] for i in range(len(all_y)) if train_indices[i]]
X_test = all_x.ix[test_indices]
y_test = [all_y[i] for i in range(len(all_y)) if test_indices[i]]
print 'Len(x_train):{}, len(y_train):{}, len(x_test):{}, len(y_test):{}'.format(len(X_train),
                                                                               len(y_train),
                                                                               len(X_test),
                                                                               len(y_test))



# Machine learn.
from sklearn.metrics import precision_recall_fscore_support
from sklearn.ensemble import RandomForestClassifier

clf = RandomForestClassifier(n_estimators=200, verbose= True,
                            max_features = .8)
clf = clf.fit(X_train, y_train)
y_pred = clf.predict(X_test)
print 'Identified {} datapoints as times to buy'.format(sum(y_pred))
print precision_recall_fscore_support(y_test, y_pred)
