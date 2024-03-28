import tomllib

import twitchio


class Client(twitchio.Client):
    async def connect(self):
        # setup goes here
        await super().connect()

    async def close(self):
        # shutdown goes here
        await super().close()


def main():
    with open("config.toml", "rb") as fp:
        config = tomllib.load(fp)
    client = Client(token=config["token"])
    client.run()
