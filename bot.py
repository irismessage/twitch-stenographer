import datetime
import tomllib
from typing import Optional

import twitchio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.types import String
from twitchio.ext import routines

CONFIG_PATH = "config.toml"

Base = declarative_base()


class Message(Base):
    __tablename__ = "message"
    # RFC4122 UUID, 36 ASCII characters long
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    # IRC message content, max 512 bytes long
    content: Mapped[str] = mapped_column(String(512), nullable=False)
    timestamp: Mapped[datetime.datetime] = mapped_column()


def load_config() -> dict:
    with open("config.toml", "rb") as fp:
        config = tomllib.load(fp)
    return config


class Client(twitchio.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_async_engine("sqlite+aiosqlite:///test.db")
        self.session: Optional[AsyncSession] = None

    async def connect(self):
        # setup goes here
        print("connect")

        self.session = AsyncSession(self.engine)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        self.refresh_channels.start()

        await super().connect()

    async def close(self):
        # shutdown goes here
        print("close")
        await self.session.close()
        self.refresh_channels.cancel()

        await super().close()

    async def event_message(self, message: twitchio.Message):
        print(message)
        message_row = Message(
            id=message.id, content=message.content, timestamp=message.timestamp
        )
        async with self.session.begin():
            self.session.add(message_row)

    @routines.routine(minutes=10, wait_first=True)
    async def refresh_channels(self):
        config = load_config()
        target_channels = frozenset(config["channels"])
        current_channels = frozenset(
            channel.name for channel in self.connected_channels
        )
        channels_to_leave = current_channels - target_channels
        channels_to_join = target_channels - current_channels

        if channels_to_leave:
            await self.part_channels(list(channels_to_leave))
        if channels_to_join:
            await self.join_channels(list(channels_to_join))


def main():
    config = load_config()
    client = Client(token=config["token"], initial_channels=config["channels"])
    client.run()


if __name__ == "__main__":
    main()
