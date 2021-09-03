import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import fft
from math import floor, log10
from datetime import datetime

from pandas.core.indexes.base import Index 

df = pd.read_sql('''select sd.t,sd.y
from signal_data sd 
where signal_id = 125 and sd.t between '2021-08-30 12:00:00' and '2021-08-30 13:00:00'
order by sd.t
 ''', "postgresql://data_viewer:tokuapidemosystems@apidemo.tokusystems.com/tsdb")

df_new = df.set_axis(['Last time','Amplitude'], axis=1, inplace=False)
print(df_new)

plt.subplot(1, 2, 1)
plt.plot('Last time','Amplitude',data=df_new)
plt.title('Time signal')
plt.xlabel('time')
plt.ylabel('Amplitude')
plt.subplot(1, 2, 2)
sp1 = np.fft.fft(df_new['Amplitude'])
freq1 = np.fft.fftfreq(df_new['Last time'].shape[-1])
fourier_signal=pd.DataFrame(dict(frequency = freq1, spectrum = sp1)).reset_index()
print(fourier_signal)
plt.plot(freq1, sp1.real, freq1, sp1.imag)
plt.title('fourier signal')
plt.xlabel('w')
plt.ylabel('f(w)')
plt.show()