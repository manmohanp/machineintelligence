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
# from IPython.display import HTML
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def log(msg):
    print(str(datetime.datetime.now()) + " - " + msg)

# colormap = plt.cm.gist_ncar #nipy_spectral, Set1,Paired
# colorst = [colormap(i) for i in np.linspace(0, 0.9,len(ax.collections))]

schema = StructType([
    StructField("dot", StringType(), True),
    StructField("district", StringType(), True),
    StructField("count", IntegerType(), True)
])

colors =    {'CITY OF LONDON':'#FFBF00',
            'BARKING AND DAGENHAM':'#9966CC',
            'BARNET':'#FBCEB1',
            'BEXLEY':'#7FFFD4',
            'BRENT':'#007FFF',
            'BROMLEY':'#89CFF0',
            'CAMDEN':'#0000FF',
            'CROYDON':'#0095B6',
            'EALING':'#8A2BE2',
            'ENFIELD':'#DE5D83',
            'GREENWICH':'#CD7F32',
            'HACKNEY':'#964B00',
            'HAMMERSMITH AND FULHAM':'#800020',
            'HARINGEY':'#702963',
            'HARROW':'#960018',
            'HAVERING':'#DE3163',
            'HILLINGDON':'#F7E7CE',
            'HOUNSLOW':'#7FFF00',
            'ISLINGTON':'#FF7F50',
            'KENSINGTON AND CHELSEA':'#7DF9FF',
            'KINGSTON UPON THAMES':'#50C878',
            'LAMBETH':'#FFD700',
            'LEWISHAM':'#808080',
            'MERTON':'#008000',
            'NEWHAM':'#4B0082',
            'REDBRIDGE':'#B57EDC',
            'RICHMOND UPON THAMES':'#C8A2C8',
            'SOUTHWARK':'#FF00AF',
            'SUTTON':'#FF00FF',
            'TOWER HAMLETS':'#FF6600',
            'WALTHAM FOREST':'#CCCCFF',
            'WANDSWORTH':'#1C39BB',
            'WESTMINSTER':'#003153',
            'CITY OF WESTMINSTER':'#7F00FF'}

fig, ax = plt.subplots(figsize=(12, 6))

# def draw_year(year):
#     # months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
#     for y in range(1,13):
#         draw_chart(year, y)

base_year = 2008

def draw_chart(num):
    log("Number - " + str(num))

    i = np.floor_divide(num,12)

    month = num + 1  - (i*12)
    year = base_year + i

    strmonth = str(month)
    if month < 10:
        strmonth = "0" + str(month)

    log(str(year) + "/" + strmonth)

    pricePaid = df.csv("out/"+str(year)+"/"+strmonth+"/*.csv", enforceSchema=True, schema=schema)
    # monthdf = sorteddf.filter(sorteddf["dot"] == month).sort(sorteddf["count"].desc()).limit(5)

    ax.clear()
    pddf = pricePaid.toPandas()
    pddf = pddf.iloc[::-1]
    ax.barh(pddf["district"], pddf["count"], align='center', color=[colors[x] for x in pddf['district']])

    dx = int(pddf["count"].max()/20)

    ax.set_title('', pad=200)

    for i, (value, name) in enumerate(zip(pddf["count"], pddf["district"])):
        ax.text(value - dx, i, name, size=14, weight=600, ha='right', va='bottom')
        # ax.text(value - dx, i - .25, group_lk[name], size=10, color='#444444', ha='right', va='baseline')
        ax.text(value + dx, i, f'{value:,.0f}', size=14, ha='left', va='center')
    # ax.text(1, 0.4, strmonth+"/"+str(year), transform=ax.transAxes, color='#000000', size=30, ha='right', weight=800)
    # ax.text(0, 1.06, '# of house sold ->', transform=ax.transAxes, size=12, color='#777777')
    ax.text(0, 1.06, '   ', transform=ax.transAxes, size=12, color='#777777')
    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    ax.xaxis.set_ticks_position('top')
    ax.tick_params(axis='x', colors='#777777', labelsize=12)
    ax.set_yticks([])
    ax.margins(0, 0.01)
    ax.grid(which='major', axis='x', linestyle='-')
    ax.set_axisbelow(True)
    ax.text(0, 1.15, 'No. of houses sold in London - ' + strmonth + "/" + str(year),
            transform=ax.transAxes, size=24, weight=600, ha='left', va='top')
    # ax.text(1, 0, ' ', transform=ax.transAxes, color='#777777', ha='right', va='baseline',
    #         bbox=dict(facecolor='white', alpha=0.8, edgecolor='white'))
    ax.text(1, 0, 'by manmohanp', transform=ax.transAxes, color='#777777', ha='right', va='baseline',
            bbox=dict(facecolor='white', alpha=0.8, edgecolor='white'))

    plt.box(False)

session = SparkSession \
    .builder \
    .master("local")\
    .appName("TopLondonTransactions") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = session.read

# 11 year (132 months) plot
animator = animation.FuncAnimation(fig, draw_chart, frames=range(0,132), interval=500, repeat=False)
# animator.save("mymovie.html")
# HTML(animator.to_jshtml())
# animator.save('seawave_1d_ani.mp4',writer='ffmpeg',fps=30)

plt.show()

session.stop()

log("exiting...")