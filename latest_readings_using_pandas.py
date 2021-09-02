import pandas as pd
from math import floor, log10
from datetime import datetime
df = pd.read_sql('''SELECT a.name, h.name, s.name, sd.t, sd.y
    FROM assets a
    JOIN hardpoints h ON a.id = h.asset_id
    JOIN signals s ON h.id = s.hardpoint_id
    JOIN LATERAL (
        SELECT x.t, x.y
        FROM signal_data x
        WHERE x.signal_id = s.id
        ORDER BY x.t DESC
        LIMIT 1
    ) sd ON true''', "postgresql://data_viewer:tokuapidemosystems@apidemo.tokusystems.com/tsdb")

df_new = df.set_axis(['Asset name', 'Hardpoint', 'Signal name', 'Last time', 'Last reading'], axis=1, inplace=False)
for i,row in df_new.iterrows():
    df_new.iloc[i,[4]] = row['Last reading']
    df_new.iloc[i,[3]] = pd.to_datetime(row['Last time'])
    
print(df_new.to_string(
    formatters={
        'Last time': lambda x : f'{pd.to_datetime(x,unit="D"):%X}',
        'Last reading' : lambda x : f'{x:.5g}'
    }))


