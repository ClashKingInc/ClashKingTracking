import aiohttp
from urllib.parse import urlencode


class HTTPClient():

    async def request(self, route, **kwargs):
        method = route.method
        url = route.url
        kwargs["headers"] = kwargs.get("headers")
        if "json" in kwargs:
            kwargs["headers"]["Content-Type"] = "application/json"
        async with aiohttp.ClientSession() as session:

            async with session.request(method, url, **kwargs) as response:
                try:
                    if response.status != 200:
                        raise Exception
                    data = await response.json()
                    await session.close()
                    return data
                except Exception as e:
                    await session.close()
                    return None


class Route:
    """Helper class to create endpoint URLs."""

    BASE = "https://api.clashofclans.com/v1"

    def __init__(self, method: str, path: str, **kwargs: dict):
        """
        The class is used to create the final URL used to fetch the data
        from the API. The parameters that are passed to the API are all in
        the GET request packet. This class will parse the `kwargs` dictionary
        and concatenate any parameters passed in.

        Parameters
        ----------
        method:
            :class:`str`: HTTP method used for the HTTP request
        path:
            :class:`str`: URL path used for the HTTP request
        kwargs:
            :class:`dict`: Optional options used to concatenate into the final
            URL
        """
        if "#" in path:
            path = path.replace("#", "%23")

        self.method = method
        self.path = path
        url = self.BASE + self.path

        if kwargs:
            self.url = "{}?{}".format(url, urlencode({k: v for k, v in kwargs.items() if v is not None}, True))
        else:
            self.url = url


