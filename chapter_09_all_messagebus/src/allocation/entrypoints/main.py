import uvicorn
from allocation.adapters import orm
from allocation.domain import events
from allocation.entrypoints import schemas
from allocation.service_layer import messagebus, unit_of_work
from allocation.service_layer.handlers import InvalidSku
from fastapi import FastAPI, HTTPException, status

orm.start_mappers()

app = FastAPI()


@app.post("/add_batch", status_code=status.HTTP_201_CREATED)
async def add_batch(batch: schemas.AddBatchRequest):
    event = events.BatchCreated(
        batch.ref,
        batch.sku,
        batch.qty,
        batch.eta,
    )
    await messagebus.handle(event, unit_of_work.SqlAlchemyUnitOfWork())

    return "OK"


@app.post("/allocate", status_code=status.HTTP_201_CREATED)
async def allocate_endpoint(line: schemas.OrderLineRequest):
    try:
        event = events.AllocationRequired(
            line.orderid,
            line.sku,
            line.qty,
        )
        results = await messagebus.handle(event, unit_of_work.SqlAlchemyUnitOfWork())
        batchref = results.pop(0)
    except InvalidSku as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"batchref": batchref}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
