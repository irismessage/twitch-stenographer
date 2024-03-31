import logging
import tomllib
from datetime import datetime
from sys import stdout
from typing import Optional, Self

import twitchio
from sqlalchemy import ForeignKey, desc, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.types import String
from twitchio.ext import routines

CONFIG_PATH = "config.toml"
# character lengths / limits
# RFC4122 UUID, 36 ASCII characters long
CHARS_UUID = 36
# IRC message content, max 512 bytes long
CHARS_IRC_MESSAGE = 512
CHARS_TWITCH_USERNAME = 25
CHARS_HEX_RGB = 6

Base = declarative_base()

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(stdout))
log.setLevel(logging.INFO)


class Message(Base):
    __tablename__ = "Message"
    id: Mapped[str] = mapped_column(String(CHARS_UUID), primary_key=True)
    content: Mapped[str] = mapped_column(String(CHARS_IRC_MESSAGE), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(nullable=False)
    chatter_name: Mapped[str] = mapped_column(
        String(CHARS_TWITCH_USERNAME), nullable=False
    )
    channel_name: Mapped[str] = mapped_column(
        String(CHARS_TWITCH_USERNAME), nullable=False
    )

    @classmethod
    def from_message(cls, message: twitchio.Message) -> Self:
        return cls(
            id=message.id,
            content=message.content,
            timestamp=message.timestamp,
            chatter_name=message.author.name,
            channel_name=message.channel.name,
        )


class FirstMessage(Base):
    __tablename__ = "FirstMessage"
    id: Mapped[str] = mapped_column(
        String(CHARS_UUID), ForeignKey(Message.id), primary_key=True
    )


class Chatter(Base):
    __tablename__ = "Chatter"
    # composite primary key
    name: Mapped[str] = mapped_column(
        String(CHARS_TWITCH_USERNAME),
        ForeignKey(Message.chatter_name),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        ForeignKey(Message.timestamp), primary_key=True
    )
    id: Mapped[int] = mapped_column(nullable=True)
    display_name: Mapped[str] = mapped_column(nullable=True)
    color: Mapped[int] = mapped_column(nullable=True)
    is_broadcaster: Mapped[bool] = mapped_column(nullable=True)
    is_mod: Mapped[bool] = mapped_column(nullable=True)
    is_subscriber: Mapped[bool] = mapped_column(nullable=True)
    is_turbo: Mapped[bool] = mapped_column(nullable=True)
    is_vip: Mapped[bool] = mapped_column(nullable=True)
    prediction: Mapped[twitchio.PredictionEnum] = mapped_column(nullable=True)

    def values(self) -> tuple:
        return (
            self.name,
            self.timestamp,
            self.id,
            self.display_name,
            self.color,
            self.is_broadcaster,
            self.is_mod,
            self.is_subscriber,
            self.is_turbo,
            self.is_vip,
            self.prediction,
        )

    @classmethod
    def from_message(cls, message: twitchio.Message) -> Self:
        author = message.author
        # id may be None
        chatter_id = author.id and int(author.id)
        chatter_color = int(author.color.removeprefix("#"), 16)
        return Chatter(
            name=author.name,
            timestamp=message.timestamp,
            id=chatter_id,
            display_name=author.display_name,
            color=chatter_color,
            is_broadcaster=author.is_broadcaster,
            is_mod=author.is_mod,
            is_subscriber=author.is_subscriber,
            # todo submit bug to twitchio
            is_turbo=author.is_turbo == "1",
            is_vip=author.is_vip,
            prediction=author.prediction,
        )

    def __eq__(self, other):
        if self is other:
            return True
        elif isinstance(other, type(self)):
            if self.values() == other.values():
                return True
        else:
            return False


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
        log.info("connect start")

        self.session = AsyncSession(self.engine)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        self.refresh_channels.start()

        await super().connect()

        log.info("connect done")

    async def close(self):
        # shutdown goes here
        log.info("close start")
        await self.session.close()
        self.refresh_channels.cancel()

        await super().close()

        log.info("close done")

    async def event_ready(self):
        log.info("ready")

    async def event_message(self, message: twitchio.Message):
        author_name = message.author.name
        log.debug(
            "message - %d characters from %s in %s",
            len(message.content),
            author_name,
            message.channel.name,
        )
        message_row = Message.from_message(message)

        # find the most recent chatter record for this author.
        # if it exists and matches the current badges etc.,
        # we don't need to insert a new one.
        last_chatter_query = (
            select(Chatter)
            .where(Chatter.name == author_name)
            .order_by(desc(Chatter.timestamp))
            .limit(1)
        )
        async with self.session.begin():
            last_chatter_record = await self.session.scalar(last_chatter_query)

            if isinstance(message.author, twitchio.Chatter):
                # has extra info about the chatter
                chatter = Chatter.from_message(message)
            else:
                # doesn't have extra info, use null columns
                chatter = Chatter(name=author_name, timestamp=message.timestamp)

            self.session.add(message_row)
            if message.first:
                self.session.add(FirstMessage(id=message.id))
            if last_chatter_record != chatter:
                self.session.add(chatter)

    @routines.routine(minutes=10, wait_first=True)
    async def refresh_channels(self):
        log.debug("refresh channels start")
        try:
            config = load_config()
        except tomllib.TOMLDecodeError:
            log.error("refresh channels - failed to read config")
            return

        try:
            target_channels = frozenset(config["channels"])
        except KeyError:
            log.error("refresh channels - missing config key")
            return

        current_channels = frozenset(
            channel.name for channel in self.connected_channels
        )
        channels_to_leave = current_channels - target_channels
        channels_to_join = target_channels - current_channels

        if channels_to_leave:
            log.info("leaving channels: %s", " ".join(channels_to_leave))
            await self.part_channels(list(channels_to_leave))
        if channels_to_join:
            log.info("joining channels: %s", " ".join(channels_to_join))
            await self.join_channels(list(channels_to_join))
        log.debug("refresh channels done")


def main():
    config = load_config()
    client = Client(token=config["token"], initial_channels=config["channels"])
    client.run()


if __name__ == "__main__":
    main()
