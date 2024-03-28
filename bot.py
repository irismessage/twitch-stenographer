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
    id: Mapped[int] = mapped_column(primary_key=True)
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

    async def event_message(self, message: Message):
        print(message)


def main():
    with open("config.toml", "rb") as fp:
        config = tomllib.load(fp)
    client = Client(token=config["token"], initial_channels=["joelsgp"])
    client.run()


if __name__ == '__main__':
    main()
