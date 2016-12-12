import MySQLdb as mdb

n = """create table all_contracts
(record_timestamp datetime,
contract_ticker varchar(30),
 market_ticker varchar(30),
 end_date varchar(30),
status varchar(30),
last_trade_price varchar(30),
best_buy_yes_cost varchar(30),
best_buy_no_cost varchar(30),
best_sell_yes_cost varchar(30),
best_sell_no_cost varchar(30),
last_close_price varchar(30));"""

con = mdb.connect('localhost', 'root', '', 'predictit_db');

with con:

    cur = con.cursor()
    cur.execute(n)
