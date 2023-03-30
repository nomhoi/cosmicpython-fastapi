import config
import model
import repository
import services
from fastapi import FastAPI, HTTPException
from orm import start_mappers
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

start_mappers()

async_engine = create_async_engine(
    config.get_postgres_uri(),
    future=True,
    echo=True,
)
async_session = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)


app = FastAPI()


@app.post("/allocate")
async def allocate_endpoint(item: model.OrderLine):
    session = async_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(item.orderid, item.sku, item.qty)

    try:
        batchref = await services.allocate(line, repo, session)
    except (model.OutOfStock, services.InvalidSku) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"batchref": batchref}
