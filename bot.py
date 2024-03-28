import datetime
import tomllib
from typing import Optional

import twitchio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, mapped_column, Mapped
from sqlalchemy.types import String


Base = declarative_base()


class Message(Base):
    __tablename__ = "message"
    # RFC4122 UUID, 36 ASCII characters long
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    # IRC message content, max 512 bytes long
    content: Mapped[str] = mapped_column(String(512), nullable=False)
    timestamp: Mapped[datetime.datetime] = mapped_column()


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
        await super().connect()

    async def close(self):
        # shutdown goes here
        print("close")
        await self.session.close()
        await super().close()

    async def event_message(self, message: twitchio.Message):
        print(message)
        message_row = Message(id=message.id, content=message.content, timestamp=message.timestamp)
        async with self.session.begin():
            self.session.add(message_row)


def main():
    with open("config.toml", "rb") as fp:
        config = tomllib.load(fp)
    client = Client(token=config["token"], initial_channels=["joelsgp"])
    client.run()


if __name__ == '__main__':
    main()
