import json
from aiohttp import web
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.exc import IntegrityError


app = web.Application()

PG_DSN = 'postgresql+asyncpg://postgres:postgres@10.168.88.113:5432/hwadtdb'

engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
#DbSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class HTTPError(web.HTTPException):
    def __init__(self, *, headers=None, reason=None, body=None, message=None):
        json_response = json.dumps({"error": message})
        super().__init__(
            headers=headers,
            reason=reason,
            body=body,
            text=json_response,
            content_type="application/json",
        )


class BadRequest(HTTPError):
    status_code = 400


class NotFound(HTTPError):
    status_code = 400


class Adt(Base):
    __tablename__ = 'adt'
    id = Column(Integer, primary_key=True)
    owner = Column(String(120), unique=False, nullable=False)
    header = Column(String(120), nullable=False)
    description = Column(String, nullable=False)
    creation_time = Column(DateTime, server_default=func.now())


async def get_adt(adt_id: int, session) -> Adt:
    adt = await session.get(Adt, adt_id)
    if not adt:
        raise NotFound(message="advertisment not found")
    return adt


class AdtView(web.View):
    async def post(self):
        adt_data = await self.request.json()
        new_adt = Adt(**adt_data)
        async with app.async_session_maker() as session:
            try:
                session.add(new_adt)
                await session.commit()
                return web.json_response({"id": new_adt.id})
            except IntegrityError as er:
                raise BadRequest(message="advertisment already exists")

    async def get(self):
        adt_id = int(self.request.match_info["adt_id"])
        async with app.async_session_maker() as session:
            adt = await get_adt(adt_id, session)
            return web.json_response(
                {
                    "owner": adt.owner,
                    "header": adt.header,
                    "description": adt.description,
                    "creation_time": int(adt.creation_time.timestamp()),
                }
            )

    async def delete(self):
        adt_id = int(self.request.match_info["adt_id"])
        async with app.async_session_maker() as session:
            adt = await get_adt(adt_id, session)
            await session.delete(adt)
            await session.commit()
            return web.json_response({"status": "success"})

    async def patch(self):
        adt_id = int(self.request.match_info["adt_id"])
        adt_data = await self.request.json()
        async with app.async_session_maker() as session:
            adt = await get_adt(adt_id, session)
            for column, value in adt_data.items():
                setattr(adt, column, value)
            session.add(adt)
            await session.commit()
            return web.json_response({"status": "success"})


async def init_orm(app: web.Application):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        async_session_maker = sessionmaker(
            engine, expire_on_commit=False, class_=AsyncSession
        )
        app.async_session_maker = async_session_maker
        yield


app.cleanup_ctx.append(init_orm)
app.add_routes([web.get("/adt/{adt_id:\d+}", AdtView)])
app.add_routes([web.patch("/adt/{adt_id:\d+}", AdtView)])
app.add_routes([web.delete("/adt/{adt_id:\d+}", AdtView)])
app.add_routes([web.post("/adt/", AdtView)])
web.run_app(app)
