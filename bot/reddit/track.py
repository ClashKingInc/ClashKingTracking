import asyncio
import pprint
import re
import traceback
from os import getenv

import asyncpraw
import orjson
import pendulum as pend
from dotenv import load_dotenv
from kafka import KafkaProducer
from loguru import logger

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=['85.10.200.219:9092'], api_version=(3, 6, 0)
)

subreddit = 'ClashOfClansRecruit'


async def post_stream(reddit: asyncpraw.Reddit):
    logger.info('Started Post Stream')
    while True:
        try:
            sub = await reddit.subreddit(subreddit)
            async for submission in sub.stream.submissions(skip_existing=True):
                if submission.link_flair_text == 'Searching':
                    text = f'{submission.selftext} {submission.title}'
                    tags = re.findall(
                        '[#PYLQGRJCUVOpylqgrjcuvo0289]{5,11}', text
                    )
                    json_data = {
                        'type': 'reddit',
                        'data': {
                            'title': submission.title,
                            'selftext': submission.selftext,
                            'score': submission.score,
                            'url': submission.url,
                            'id': submission.id,
                            'comments_link': f'https://www.reddit.com/r/{subreddit}/comments/{submission.id}',
                            'tags': tags,
                        },
                    }
                    producer.send(
                        topic='reddit',
                        value=orjson.dumps(json_data),
                        timestamp_ms=int(pend.now(tz=pend.UTC).timestamp())
                        * 1000,
                    )
        except Exception as e:
            logger.error(traceback.format_exc())
            continue


async def comment_stream(reddit: asyncpraw.Reddit):
    logger.info('Started Comment Stream')
    while True:
        try:
            sub = await reddit.subreddit(subreddit)
            async for comment in sub.stream.comments(skip_existing=True):
                await comment.author.load()
                json_data = {
                    'type': 'redditcomment',
                    'data': {
                        'author': comment.author.name,
                        'avatar': comment.author.icon_img,
                        'body': comment.body,
                        'url': comment.permalink,
                        'score': comment.score,
                        'submission_author': comment.link_author,
                        'submission_title': comment.link_title,
                    },
                }
                producer.send(
                    topic='reddit',
                    value=orjson.dumps(json_data),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000,
                )
        except Exception as e:
            logger.error(traceback.format_exc())
            continue


async def create_reddit():
    secret = getenv('REDDIT_SECRET')
    RPW = getenv('REDDIT_PW')
    return asyncpraw.Reddit(
        client_id='-dOCgLIHqUJK7g',
        client_secret=secret,
        username='Powerful-Flight2605',
        password=RPW,
        user_agent='Reply Recruit',
    )


async def main():
    reddit = await create_reddit()
    loop = asyncio.get_event_loop()
    loop.create_task(comment_stream(reddit))
    loop.create_task(post_stream(reddit))
