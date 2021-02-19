import pyspark as ps
spark = (ps.sql.SparkSession.builder
         .master("local[4]")
         .appName("sparkSQL exercise")
         .getOrCreate()
        )
sc=spark.sparkContext
import pyspark as ps
spark = (ps.sql.SparkSession.builder
         .master("local[4]")
         .appName("sparkSQL exercise")
         .getOrCreate()
        )
sc=spark.sparkContext

data = '../data/french_tweets.json'
#read in our initial data set
df = spark.read.json(data)

#select only the columns we care about
df2 = df.select("created_at", "id", "text",
               "in_reply_to_status_id", "in_reply_to_user_id","in_reply_to_screen_name",
               "user", "geo", "coordinates", "place", "contributors", "is_quote_status", "entities", 
               "lang", "timestamp_ms")

df2.createOrReplaceTempView('firstpass')

# filter out only things related to candidate names, and only from france
result = spark.sql("""
SELECT *
FROM firstpass
WHERE
(LOWER(text) LIKE '%macron%' OR LOWER(text) LIKE '%le pen%' OR LOWER(text) LIKE '%lepen%' OR LOWER(text) like '%marine%' OR LOWER(text) like '%emmanuel%' OR LOWER(text) like '%national front%' OR LOWER(text) like '%fn%' OR LOWER(text) like '% em %' OR LOWER(text) like '%en marche%'OR LOWER(text) LIKE '%président%' OR LOWER(text) LIKE '%élection%')
AND place.`country` = 'France'
""")


result.createOrReplaceTempView('culled')

#top 20 hashtags
hashtags = spark.sql("""
select hash, count(hash) as cnt
from culled
lateral view explode(entities.hashtags) as hash
group by hash
order by cnt desc
limit 20
""")

hashtags.show(20, truncate=False)

#which tweets are most replied to?
most_replied = spark.sql("""
select culled.text, count(*) as cnt
   from culled
   left join firstpass as replies
   on culled.id = replies.in_reply_to_status_id
   group by culled.text
   order by cnt desc
   limit 10
""")

most_replied.show(10, truncate=False)

most_replied_verified = spark.sql("""
select culled.text, count(*) as cnt
   from culled
   left join firstpass as replies
   on culled.id = replies.in_reply_to_status_id
   where culled.user.verified = true
   group by culled.text
   order by cnt desc
   limit 10
""")

most_replied_verified.show(10, truncate=False)
