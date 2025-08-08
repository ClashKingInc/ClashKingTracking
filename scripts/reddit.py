import asyncio
import re
import traceback

import asyncpraw

from .tracking import Tracking, TrackingType

subreddit = "ClashOfClansRecruit"


class RedditTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.REDDIT)

    async def _post_stream(self, reddit: asyncpraw.Reddit):
        self.logger.info("Started Post Stream")
        while True:
            try:
                sub = await reddit.subreddit(subreddit)
                async for submission in sub.stream.submissions(skip_existing=True):
                    if submission.link_flair_text == "Searching":
                        text = f"{submission.selftext} {submission.title}"
                        tags = re.findall("[#PYLQGRJCUVOpylqgrjcuvo0289]{5,11}", text)
                        json_data = {
                            "type": "reddit",
                            "data": {
                                "title": submission.title,
                                "selftext": submission.selftext,
                                "score": submission.score,
                                "url": submission.url,
                                "id": submission.id,
                                "comments_link": f"https://www.reddit.com/r/{subreddit}/comments/{submission.id}",
                                "tags": tags,
                            },
                        }
                        self._send_to_kafka(topic="reddit", data=json_data, key=None)
            except Exception:
                self.logger.error(traceback.format_exc())
                continue

    async def _comment_stream(self, reddit: asyncpraw.Reddit):
        self.logger.info("Started Comment Stream")
        while True:
            try:
                sub = await reddit.subreddit(subreddit)
                async for comment in sub.stream.comments(skip_existing=True):
                    await comment.author.load()
                    json_data = {
                        "type": "redditcomment",
                        "data": {
                            "author": comment.author.name,
                            "avatar": comment.author.icon_img,
                            "body": comment.body,
                            "url": comment.permalink,
                            "score": comment.score,
                            "submission_author": comment.link_author,
                            "submission_title": comment.link_title,
                        },
                    }
                    self._send_to_kafka(topic="reddit", data=json_data, key=None)

            except Exception:
                self.logger.error(traceback.format_exc())
                continue

    async def create_reddit(self):
        return asyncpraw.Reddit(
            client_id="-dOCgLIHqUJK7g",
            client_secret=self.config.reddit_user_secret,
            username="Powerful-Flight2605",
            password=self.config.reddit_user_password,
            user_agent="Reply Recruit",
        )

    async def run(self):
        await self.initialize()
        reddit = await self.create_reddit()
        await self._post_stream(reddit)

