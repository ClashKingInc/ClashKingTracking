import asyncio

import asyncpraw
import re
import orjson
import pendulum as pend
from loguru import logger

from kafka import KafkaProducer
from os import getenv
from dotenv import load_dotenv
load_dotenv()

subreddit = "ClashOfClansRecruit"
secret = getenv("REDDIT_SECRET")
RPW = getenv("REDDIT_PW")

reddit = asyncpraw.Reddit(
    client_id="-dOCgLIHqUJK7g",
    client_secret= secret,
    username="Powerful-Flight2605",
    password=RPW,
    user_agent="Reply Recruit"
)

producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))


async def post_stream():
    logger.info("Started Post Stream")
    while True:
        try:
            count = 0
            sub = await reddit.subreddit(subreddit)
            async for submission in sub.stream.submissions():
                if count < 100:  # This removes the 100 historical submissions that SubredditStream pulls.
                    count += 1
                    continue
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
            logger.error(str(e))
            continue

async def comment_stream():
    logger.info("Started Comment Stream")
    while True:
        try:
            count = 0
            sub = await reddit.subreddit(subreddit)
            async for comment in sub.stream.comments():
                if count < 100:  # This removes the 100 historical submissions that SubredditStream pulls.
                    count += 1
                    continue
                json_data = {"type": "redditcomment",
                             "data" : {"author" : comment.author.name,
                                       "avatar" : comment.author.icon_img,
                                       "body" : comment.body,
                                       "url" : comment.permalink,
                                       "score" : comment.score,
                                       "submission_author" : comment.submission.author.name,
                                       "submission_title" : comment.submission.title
                                       }}
                producer.send(topic="reddit", value=orjson.dumps(json_data), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000)
        except Exception as e:
            logger.error(str(e))
            continue


async def main():
    loop = asyncio.get_event_loop()
    loop.create_task(comment_stream())
    loop.create_task(post_stream())


