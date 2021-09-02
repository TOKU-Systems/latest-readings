import psycopg2
from math import log10, floor
from tabulate import tabulate
import datetime 

def round_sig(x, sig=5):
    return round(x, sig-int(floor(log10(abs(x))))-1)

def height_calculation(pressure, acceleration_due_to_gravity, specific_gravity):
    height = pressure/(acceleration_due_to_gravity * specific_gravity)
    return height

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
    where s.name='Pressure' ''')
    query_results = cur.fetchall()
    print('Enter the specific gravity of the liquid in kg/m3: ')
    user_inputted_specific_gravity = float(input())
    g = 9.81
    formatted_results = []
    for row in query_results:
        newRow = []
        for value in row:
        
            height = 0.0
            if isinstance(value,float)and value!= 0.0 : 
                height = height_calculation(value, g, user_inputted_specific_gravity)    
                newRow.append(round_sig(height,5)) 
            elif isinstance(value,datetime.datetime):
                newRow.append( value.strftime('%c'))

            else:
                newRow.append(value)
        
        formatted_results.append(newRow)

    print(tabulate(formatted_results,headers=["Asset name","Hardpoint", "Signal name","Last Time","Last Height"]))
       
finally:    
    cur.close()
    conn.close()
    

