# FrenchTweets
Analysis of French Tweets surrounding the 2017 Presidential Election
## Data
The data is found [here](https://s3.us-east-2.amazonaws.com/jgartner-test-data/twitter/zippedData.zip)

scripts are written expecting the data to be in the /data directory

## Cleaning and Data Exploration
The tweet data was stored in a json file, which we pulled into a spark dataframe.  We then paired down the dataframe down to the columns with the most relevant information, such as the text, the date the tweet was posted, the user information, and country of origin.  

Upon viewing the text of a sample of the tweets, we noticed that a large portion of the tweets were completely irrelevant to the French election.  For example:

"Is yogurt associated with a lower cardiometabolic risk in children?"

"Working lunch in the back garden"

We filtered out the irrelevant tweets by creating another dataframe that only including tweets that include references to the election or its candidates (i.e. Macron, Le Pen, président, etc...) using the SQL LIKE feature.  This significantly shrunk the dataset from ~214,000 tweets to ~14,000 tweets  

We then compared the number of mentions of each of the two candidates running as well as their corresponding parties.  We found that Macron was mention more than double the amount of times as his opponent, Le Pen.  Interestingly, Macron's party, En Marche!, was rarely mentioned compared to Le Pen's party, national front.

To filter by hashtag, because of the way twitter return the hashtags (stored in a nested list) some additional work was needed to extract the hashtag itself from the nested structure of the entities structure. Once we looked at this we found that Marcon was mentioned in half of the tags within the top 10 (notably Marcon, BlocusNiFnNiMarcon, JeVoteMarcon, marcon, MacronPrésident)

The most challenging part of this excercise was cleaning the dataset of irrelevant tweets when the content of the tweets were in a foreign language. Our attempts to filter the data by looking for specific search terms may have been too draconian, filtering out some relevant data that could have been used in the dataset.


## Graphs

### Graphs of hashtag frequencies
#### Hashtags from all tweets
![Graph of hastags from all tweets](pics/hashtags_all_data.png)

#### Hashtags from selected filtered tweets
![Graph of hashtags from selected tweets](pics/hashtags.png)


### Number of Tweets Referencing each Candidate
![Number of Tweets Referencing each Candidate](pics/Candidate_Mention_Count.png)

### Number of Tweets Referencing each Party
![Number of Tweets Referencing each Party](pics/Party_Mention_Count.png)
