import psycopg2
from math import log10, floor
from tabulate import tabulate
import datetime
# import pandas as pd


def round_sig(x, sig=5):
    return round(x, sig-int(floor(log10(abs(x))))-1)

try:
    conn = psycopg2.connect(host="apidemo.tokusystems.com",port="5432",dbname="tsdb",user="data_viewer",password="tokuapidemosystems")
    cur = conn.cursor()
    cur.execute('''SELECT a.name, h.name, s.name, sd.t, sd.y
    FROM assets a
    JOIN hardpoints h ON a.id = h.asset_id
    JOIN signals s ON h.id = s.hardpoint_id
    JOIN LATERAL (
        SELECT x.t, x.y
        FROM signal_data x
        WHERE x.signal_id = s.id
        ORDER BY x.t DESC
        LIMIT 1
    ) sd ON true
''')
    query_results = cur.fetchall()
    formatted_results = []
    for row in query_results:
        formatted_row = []
        for value in row:
            if isinstance(value,float)and value!= 0.0 :
             formatted_row.append(round_sig(value,5))  
            elif isinstance(value,datetime.datetime):
                formatted_row.append( value.strftime('%c'))
            else:
                formatted_row.append(value)
        formatted_results.append(formatted_row)

    print(tabulate(formatted_results,headers=["Asset name","Hardpoint", "Signal name","Last Time","Last reading"]))

finally:    
    cur.close()
    conn.close()
