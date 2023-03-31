import config
from adapters import orm, repository
from domain import model
from fastapi import FastAPI, HTTPException, status
from service_layer import services
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from . import schemas

orm.start_mappers()

async_engine = create_async_engine(
    config.get_postgres_uri(),
    future=True,
    echo=True,
)
get_session = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)


app = FastAPI()


@app.post("/add_batch", status_code=status.HTTP_201_CREATED)
async def add_batch(batch: schemas.AddBatchRequest):
    session = get_session()  # TODO
    repo = repository.SqlAlchemyRepository(session)
    eta = batch.eta
    await services.add_batch(
        batch.ref,
        batch.sku,
        batch.qty,
        eta,
        repo,
        session,
    )
    return "OK"


@app.post("/allocate", status_code=status.HTTP_201_CREATED)
async def allocate_endpoint(line: schemas.OrderLineRequest):
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    try:
        batchref = await services.allocate(
            line.orderid,
            line.sku,
            line.qty,
            repo,
            session,
        )
    except (model.OutOfStock, services.InvalidSku) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"batchref": batchref}
