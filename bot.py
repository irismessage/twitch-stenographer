import logging
import tomllib
from datetime import datetime
from sys import stdout
from typing import Optional, Self

import sqlalchemy.exc
import twitchio
from sqlalchemy import ForeignKey, desc, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import String
from twitchio.ext.routines import routine

# todo script to output all data to csv from a db
#   for the lay folk
# currently not recorded:
#   - message deletion - in progress
#   - votes and predictions
#   - subscriptions
#   - stream details
#   - highlighted messages
#   - cheers
#   - user join and leave not worth it
#   - room state - emote only etc.
#   - mass message deletion (ban and timeout)
#   - raid
#   - replies

CONFIG_PATH = "config.toml"
DATABASE_PATH = "archive.db"
COUNTER_INTERVAL_MINUTES = 60
# character lengths / limits
# should I make these just sqla types instead
# RFC4122 UUID, 36 ASCII characters long
CHARS_UUID = 36
# IRC message content, max 512 bytes long
CHARS_IRC_MESSAGE = 512
CHARS_TWITCH_USERNAME = 25
CHARS_HEX_RGB = 6


log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(stdout))
log.setLevel(logging.INFO)


class Base(DeclarativeBase):
    pass


class ComparableRow:
    def _values(self) -> tuple:
        raise NotImplemented

    def __eq__(self, other):
        if self is other:
            return True
        elif isinstance(other, type(self)):
            if self._values() == other._values():
                return True
        else:
            return False


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


class DeletedMessage(Base):
    __tablename__ = "DeletedMessage"
    id: Mapped[str] = mapped_column(
        String(CHARS_UUID), ForeignKey(Message.id), primary_key=True
    )
    # timestamp of deletion
    timestamp: Mapped[str] = mapped_column(nullable=False)
    # name of the moderator that deleted it isn't given?


# could split some of this off into a User class
# with all that info in there
# and also have the same relation for Chatter
class Channel(ComparableRow, Base):
    __tablename__ = "Channel"
    name: Mapped[str] = mapped_column(
        String(CHARS_TWITCH_USERNAME),
        ForeignKey(Message.channel_name),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        ForeignKey(Message.timestamp), primary_key=True
    )
    # always given for User
    id: Mapped[int] = mapped_column(nullable=False)

    def _values(self) -> tuple:
        return self.name, self.id


class Chatter(ComparableRow, Base):
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
    channel: Mapped[str] = mapped_column(
        String(CHARS_TWITCH_USERNAME), ForeignKey(Channel.name)
    )
    # could make this some kind of complex relation to save negligible space
    badges: Mapped[Optional[str]] = mapped_column(nullable=True)
    id: Mapped[Optional[int]] = mapped_column(nullable=True)
    display_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    color: Mapped[Optional[int]] = mapped_column(nullable=True)
    # note is_broadcaster is redundant since
    # you can just tell whether name matches channel
    is_mod: Mapped[Optional[bool]] = mapped_column(nullable=True)
    is_subscriber: Mapped[Optional[bool]] = mapped_column(nullable=True)
    is_turbo: Mapped[Optional[bool]] = mapped_column(nullable=True)
    is_vip: Mapped[Optional[bool]] = mapped_column(nullable=True)
    prediction: Mapped[Optional[twitchio.PredictionEnum]] = mapped_column(nullable=True)

    @classmethod
    def from_message(cls, message: twitchio.Message) -> Self:
        author = message.author
        # id may be None
        chatter_id = author.id and int(author.id)
        # color may also be empty string, docs don't mention this
        if author.color:
            chatter_color = int(author.color.removeprefix("#"), 16)
        else:
            chatter_color = None
        # same as _badges attr but inspections doesn't like
        badges = ",".join(f"{k}/{v}" for k, v in author.badges.items())
        return cls(
            name=author.name,
            timestamp=message.timestamp,
            channel=message.channel.name,
            badges=badges,
            id=chatter_id,
            display_name=author.display_name,
            color=chatter_color,
            is_mod=author.is_mod,
            is_subscriber=author.is_subscriber,
            # twitchio bug, this should be bool
            is_turbo=author.is_turbo == "1",
            is_vip=author.is_vip,
            prediction=author.prediction,
        )

    def _values(self) -> tuple:
        return (
            self.name,
            self.channel,
            self.badges,
            self.id,
            self.display_name,
            self.color,
            self.is_mod,
            self.is_subscriber,
            self.is_turbo,
            self.is_vip,
            self.prediction,
        )


def load_config() -> dict:
    with open("config.toml", "rb") as fp:
        config = tomllib.load(fp)
    return config


class LogCounter:
    def __init__(self):
        self.messages = 0
        self.channels = 0
        self.chatters = 0

    def reset(self):
        self.messages = 0
        self.channels = 0
        self.chatters = 0


class Client(twitchio.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{DATABASE_PATH}")
        self.async_session = async_sessionmaker(self.engine)
        self.counter = LogCounter()

    async def connect(self):
        # setup goes here
        log.info("connect start")

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await super().connect()
        self.refresh_channels.start()
        self.log_counter.start()

        log.info("connect done")

    async def close(self):
        # shutdown goes here
        log.info("close start")

        self.log_counter.cancel()
        self.refresh_channels.cancel()
        await super().close()
        await self.engine.dispose()

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

        channel_user = await message.channel.user()

        message_row = Message.from_message(message)
        if isinstance(message.author, twitchio.Chatter):
            # has extra info about the chatter
            chatter = Chatter.from_message(message)
            log.debug("author - Chatter")
        else:
            # doesn't have extra info, use null columns
            log.debug("author - PartialChatter")
            chatter = Chatter(
                name=author_name,
                timestamp=message.timestamp,
                channel=message.channel.name,
            )
        channel = Channel(
            name=channel_user.name, timestamp=message.timestamp, id=channel_user.id
        )

        # find the most recent chatter record for this author.
        # if it exists and matches the current badges etc.,
        # we don't need to insert a new one.
        last_chatter_query = (
            select(Chatter)
            .where(Chatter.name == author_name)
            .order_by(desc(Chatter.timestamp))
            .limit(1)
        )
        last_channel_query = (
            select(Channel)
            .where(Channel.name == message.channel.name)
            .order_by(desc(Channel.timestamp))
            .limit(1)
        )

        try:
            async with self.async_session.begin() as session:
                last_chatter_record = await session.scalar(last_chatter_query)
                last_channel_record = await session.scalar(last_channel_query)

            session.add(message_row)
            self.counter.messages += 1
            if message.first:
                log.debug("first message")
                session.add(FirstMessage(id=message.id))
            if last_channel_record != channel:
                log.debug("adding channel record")
                session.add(channel)
                self.counter.channels += 1
            if last_chatter_record != chatter:
                log.debug("adding chatter record")
                session.add(chatter)
                self.counter.chatters += 1
        except sqlalchemy.exc.OperationalError as error:
            # limit traceback spam and provide useful information
            # in case of I/O timeout
            log.error(error)
            streams = await self.fetch_streams(user_ids=[channel_user.id])
            stream_info = streams[0]
            now = datetime.now()
            stream_started = stream_info.started_at
            stream_uptime = now - stream_started
            # todo test
            # todo handle no stream in progress
            log.error(
                "stream started: %s, current time: %s, stream uptime: %s",
                stream_started,
                now,
                stream_uptime,
            )

    # file watch requires another library
    # todo this repeatedly reconnects if config gives uppercase
    @routine(minutes=10, wait_first=True)
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

    @routine(minutes=COUNTER_INTERVAL_MINUTES, wait_first=True)
    async def log_counter(self):
        log.info(
            "last %d minutes: %d messages, %d channels, %d chatters",
            COUNTER_INTERVAL_MINUTES,
            self.counter.messages,
            self.counter.channels,
            self.counter.chatters,
        )
        self.counter.reset()


def main():
    config = load_config()
    log.info("initial channels: %s", " ".join(config["channels"]))
    client = Client(token=config["token"], initial_channels=config["channels"])
    client.run()


if __name__ == "__main__":
    main()
