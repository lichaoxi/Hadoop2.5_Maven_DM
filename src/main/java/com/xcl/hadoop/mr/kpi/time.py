# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl  

dates = []
counts = []
myfile = open("E:\Workspaces\MyEclipse\MedicalDataMining\hadoop\kpi\part-00000", 'rb')
line = myfile.readline()
count = 0
while('' != line):
    dates.append(line.split('\t')[0])
    counts.append(int(line.split('\t')[1].split('\r\n')[0]))
    line = myfile.readline()
myfile.close()

dates = pd.to_datetime(dates,format='%Y%m%d%H')

fig = plt.figure() 
plt.title('plot')
plt.xlabel('time')
plt.ylabel('page_look_num')
ax = plt.gca()  
# 使用plot_date绘制日期图像  
ax.plot_date(dates, counts, linestyle = "-", marker = ".")  
# 设置日期的显示格式  
date_format = mpl.dates.DateFormatter("%Y-%m-%d-%H")  
ax.xaxis.set_major_formatter(date_format)  
# 日期的排列根据图像的大小自适应  
fig.autofmt_xdate()  
plt.show()