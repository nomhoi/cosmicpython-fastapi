import uvicorn
from allocation import config
from allocation.adapters import orm, repository
from allocation.domain import model
from allocation.entrypoints import schemas
from allocation.service_layer import services, unit_of_work
from fastapi import FastAPI, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

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
    session = get_session()
    repository.SqlAlchemyRepository(session)
    eta = batch.eta
    await services.add_batch(
        batch.ref,
        batch.sku,
        batch.qty,
        eta,
        unit_of_work.SqlAlchemyUnitOfWork(),
    )
    return "OK"


@app.post("/allocate", status_code=status.HTTP_201_CREATED)
async def allocate_endpoint(line: schemas.OrderLineRequest):
    session = get_session()
    repository.SqlAlchemyRepository(session)
    try:
        batchref = await services.allocate(
            line.orderid,
            line.sku,
            line.qty,
            unit_of_work.SqlAlchemyUnitOfWork(),
        )
    except (model.OutOfStock, services.InvalidSku) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"batchref": batchref}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
