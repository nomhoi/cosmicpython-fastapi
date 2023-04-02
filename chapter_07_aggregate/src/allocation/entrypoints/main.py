import uvicorn
from allocation.adapters import orm
from allocation.domain import model
from allocation.entrypoints import schemas
from allocation.service_layer import services, unit_of_work
from fastapi import FastAPI, HTTPException, status

orm.start_mappers()

app = FastAPI()


@app.post("/add_batch", status_code=status.HTTP_201_CREATED)
async def add_batch(batch: schemas.AddBatchRequest):
    await services.add_batch(
        batch.ref,
        batch.sku,
        batch.qty,
        batch.eta,
        unit_of_work.SqlAlchemyUnitOfWork(),
    )
    return "OK"


@app.post("/allocate", status_code=status.HTTP_201_CREATED)
async def allocate_endpoint(line: schemas.OrderLineRequest):
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
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
