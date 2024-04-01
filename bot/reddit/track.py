import asyncpraw
import re
import orjson
import pendulum as pend

from kafka import KafkaProducer
from os import getenv
from dotenv import load_dotenv
load_dotenv()

subreddit = "ClashOfClansRecruit"
secret = getenv("SECRET")
RPW = getenv("RPW")
reddit = asyncpraw.Reddit(
    client_id="-dOCgLIHqUJK7g",
    client_secret= secret,
    username="Powerful-Flight2605",
    password=RPW,
    user_agent="Reply Recruit"
)

async def main():
    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))

    while True:
        try:
            count = 0
            sub = await reddit.subreddit(subreddit)
            async for submission in sub.stream.submissions():
                '''if count < 100:  # This removes the 100 historical submissions that SubredditStream pulls.
                    count += 1
                    continue'''
                if submission.link_flair_text == 'Searching':
                    text = f"{submission.selftext} {submission.title}"
                    tags = re.findall('[#PYLQGRJCUVOpylqgrjcuvo0289]{5,11}', text)
                    json_data = {"type": "reddit",
                                 "data" : {"title" : submission.title,
                                           "selftext" : submission.selftext,
                                           "score" : submission.score,
                                           "url" : submission.url,
                                           "id" : submission.id,
                                           "comments_link" : f"https://www.reddit.com/r/{subreddit}/comments/{submission.id}",
                                           "tags" : tags}}
                    producer.send(topic="reddit", value=orjson.dumps(json_data), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000)
        except Exception as e:
            print(e)
            continue

