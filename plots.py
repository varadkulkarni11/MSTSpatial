import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import subprocess
import re
import numpy as np



# y1=[15.328,89.412]
# y2=[42.810,410.829]
# y3=[38.321,390.969]

# width=0.1
# x = np.arange(2)
# plt.title("Index Creation Time w.r.t 3 Approaches")
# plt.xticks(x, ['OSM Medium 1.6M','OSM Large 20M'])
# plt.bar(x-0.1, y1, width, color='blue',edgecolor='black')
# plt.bar(x, y2, width, color='red',edgecolor='black')
# plt.bar(x+0.1, y3, width, color='orange',edgecolor='black')



# plt.ylabel("Time(s)")
# plt.grid()
# plt.xlabel("Dataset")
# plt.legend(["Naive Index","Index without MST","Index with MST"])
# plt.show()

y1=[810.62]
y2=[65.10]


width=0.05
x = np.arange(1)
plt.title("Query Execution Time on OSM Medium 1.6M For Better Scale Understanding")
plt.xticks(x, ['                                                         OSM Medium 1.6M'])
plt.bar(x, y1, width, color='pink',edgecolor='black')
plt.bar(x+0.05, y2, width, color='orange',edgecolor='black')



plt.ylabel("Time(s)")
plt.grid()
plt.xlabel("Dataset")
plt.legend(["Naive Query Answring","Efficient Query Answering"])
plt.show()