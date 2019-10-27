from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, unix_timestamp
from pyspark.sql.functions import count, avg
import datetime
import uuid

#for the chart

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import matplotlib.animation as animation
from IPython.display import HTML

def log(msg):
    print(str(datetime.datetime.now()) + " - " + msg)

fig, ax = plt.subplots(figsize=(15, 8))
colormap = plt.cm.gist_ncar #nipy_spectral, Set1,Paired
colorst = [colormap(i) for i in np.linspace(0, 0.9,len(ax.collections))]


def draw_chart(current_year):
    # log(month)
    monthdf = sorteddf.filter(sorteddf["dot"] == month).sort(sorteddf["count"].desc()).limit(5)
    pddf = monthdf.toPandas()
    pddf = pddf.iloc[::-1]
    ax.barh(pddf["district"], pddf["count"], align='center')

    dx = pddf['count'].max() / 20

    for i, (value, name) in enumerate(zip(pddf['count'], pddf['district'])):
        ax.text(value - dx, i, name, size=14, weight=600, ha='right', va='bottom')
        # ax.text(value - dx, i - .25, group_lk[name], size=10, color='#444444', ha='right', va='baseline')
        ax.text(value + dx, i, f'{value:,.0f}', size=14, ha='left', va='center')
    ax.text(1, 0.4, current_year, transform=ax.transAxes, color='#000000', size=30, ha='right', weight=800)
    ax.text(0, 1.06, '# house sold', transform=ax.transAxes, size=12, color='#777777')
    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    ax.xaxis.set_ticks_position('top')
    ax.tick_params(axis='x', colors='#777777', labelsize=12)
    ax.set_yticks([])
    ax.margins(0, 0.01)
    ax.grid(which='major', axis='x', linestyle='-')
    ax.set_axisbelow(True)
    ax.text(0, 1.15, 'Top selling places in London since 1995',
            transform=ax.transAxes, size=24, weight=600, ha='left', va='top')
    # ax.text(1, 0, 'by @pratapvardhan; credit @jburnmurdoch', transform=ax.transAxes, color='#777777', ha='right',
    #         bbox=dict(facecolor='white', alpha=0.8, edgecolor='white'))

    # plt.box(False)
    plt.show()

session = SparkSession \
    .builder \
    .appName("TopLondonTransactions") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = session.read

log("Loading file...")

pricePaid = df.option("header", "true").csv("data/*.csv")

filterGreaterLondon = pricePaid.filter(pricePaid["county"] == "GREATER LONDON");

formatteddf = filterGreaterLondon.select("district","county",date_format(unix_timestamp("date_of_transfer", "yyyy-MM-dd").cast("timestamp"),"MM/yyyy").alias("dot"))

log("---1--")

sorteddf = formatteddf.groupBy("dot","district").count()

log("--2---")

current_year = 2018
months = ["01","02","03","04","05","06","07","08","09","10","11","12"]

for x in months:
    month = str(x) + "/" + str(current_year)
    # draw_chart(month)
    next = input("Enter to do next")

log("Start writting to file...")

# sorteddf.write.csv("out/"+uuid.uuid1().hex)

log("File writting done...")

session.stop()

log("exiting...")