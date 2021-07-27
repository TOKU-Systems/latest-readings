import psycopg2
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
) sd ON true''')
query_results = cur.fetchall()
print(query_results)
cur.close()
conn.close()
