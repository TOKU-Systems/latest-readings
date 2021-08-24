import pandas as pd
from math import floor, log10
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
    ) sd ON true''', "postgresql://data_viewer:tokuapidemosystems@apidemo.tokusystems.com/tsdb",parse_dates={"t": {"format": "%m/%d/%y"}},)

df_new = df.set_axis(['Asset name', 'Hardpoint', 'Signal name', 'Last time', 'Last reading'], axis=1, inplace=False)
print(df_new)



def smarter_round(sig):
    def rounder(x):
        offset = sig - floor(log10(abs(x)))
        initial_result = round(x, offset)
        if str(initial_result)[-1] == '5' and initial_result == x:
            return round(x, offset - 2)
        else:
            return round(x, offset - 1)
    return rounder

    
print(df['y'].apply(smarter_round(3)))