import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import numpy as np

style.use("ggplot")

data = pd.read_csv("arkk.csv")
data.columns = ['offset', 'low', 'high', 'open', 'close', 'volume']

iex_only_data = pd.read_json("iex_arkk.json")

time_range = np.arange(9*60+30, 16*60)
# plt.plot(time_range, data['close'][time_range] / 10000, label='all')
plt.plot(time_range, iex_only_data['close'], label='iex')
plt.legend()
plt.show()