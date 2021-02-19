import pyspark as ps
spark = (ps.sql.SparkSession.builder 
        .master("local[4]") 
        .appName("sparkSQL exercise") 
        .getOrCreate()
        )
sc = spark.sparkContext


data = 'FrenchTweets/data/data/french_tweets.json'

df = spark.read.json(data)
df2 = df.select("created_at", "id", "text",  "in_reply_to_status_id", "in_reply_to_user_id", 
                "in_reply_to_screen_name", "user", "geo", "coordinates", "place", 
                "contributors", "is_quote_status", "entities", "lang", "timestamp_ms").sort('created_at')
df2.createOrReplaceTempView('firstpass')
result = spark.sql("""
SELECT *
FROM firstpass 
WHERE 
(LOWER(text) LIKE '%macron%' OR LOWER(text) LIKE '%le pen%' OR LOWER(text) LIKE '%lepen%' OR LOWER(text) like '%marine%' OR LOWER(text) like '%emmanuel%' OR LOWER(text) like '%national front%' OR LOWER(text) like '%fn%' OR LOWER(text) like '% em %' OR LOWER(text) like '%en marche%' OR LOWER(text) LIKE '%président%' OR LOWER(text) LIKE '%élection%')
AND place.`country` = 'France'
""")
result.count()





result.createOrReplaceTempView('results')
names = spark.sql('''
Select 
count(*),
    CASE
        WHEN (LOWER(text) LIKE '%macron%' OR LOWER(text) LIKE '%emmanuel%') THEN 'macron' 
        WHEN (LOWER(text) LIKE '%le pen%' OR LOWER(text) LIKE '%lepen%' OR LOWER(text) like '%marine%') THEN 'le pen'
    END AS candidate
FROM results
GROUP BY candidate
''')

names.show()




parties = spark.sql('''
Select 
count(*), 
    CASE
        WHEN (LOWER(text) LIKE '% em %' OR LOWER(text) LIKE '%en marche%' OR LOWER(text) LIKE '%enmarche%' OR LOWER(text) LIKE '% rem %' OR LOWER(text) LIKE '%lrm%') THEN 'EM' 
        WHEN (LOWER(text) LIKE '%fn%' OR LOWER(text) like '%national front%' OR LOWER(text) like '%front national%') THEN 'FN'
    END as party
FROM results
GROUP BY party
''')

parties.show()


import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
names2 = names.toPandas()
names2.drop(1, inplace=True)
names2



fig, ax = plt.subplots()
x = np.arange(2)
bars = ax.bar(x, names2['count(1)']) 
ax.set_xticks(x)
ax.set_xticklabels(names2['candidate'])
ax.set_title('Candidate Mention Count')
plt.savefig('Candidate Mention Count.png')


parties2 = parties.toPandas()
parties2.drop([0], inplace=True)
parties2

fig, ax = plt.subplots()
x = np.arange(2)
bars = ax.bar(x, parties2['count(1)']) 
ax.set_xticks(x)
ax.set_xticklabels(parties2['party'])
ax.set_title('Party Mention Count')
plt.savefig('Party Mention Count.png')
plt.show()