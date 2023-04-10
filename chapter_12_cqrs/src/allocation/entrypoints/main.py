import uvicorn
from allocation import views
from allocation.adapters import orm
from allocation.domain import commands
from allocation.entrypoints import schemas
from allocation.service_layer import messagebus, unit_of_work
from allocation.service_layer.handlers import InvalidSku
from fastapi import FastAPI, HTTPException, status

orm.start_mappers()

app = FastAPI()


@app.post("/add_batch", status_code=status.HTTP_201_CREATED)
async def add_batch(batch: schemas.AddBatchRequest):
    cmd = commands.CreateBatch(
        batch.ref,
        batch.sku,
        batch.qty,
        batch.eta,
    )
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    await messagebus.handle(cmd, uow)

    return "OK"


@app.post("/allocate", status_code=status.HTTP_202_ACCEPTED)
async def allocate_endpoint(line: schemas.OrderLineRequest):
    try:
        cmd = commands.Allocate(
            line.orderid,
            line.sku,
            line.qty,
        )
        uow = unit_of_work.SqlAlchemyUnitOfWork()
        await messagebus.handle(cmd, uow)
    except InvalidSku as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return "OK"


@app.get("/allocations/{orderid}", status_code=status.HTTP_200_OK)
async def allocations_view_endpoint(orderid):
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    result = await views.allocations(orderid, uow)
    if not result:
        raise HTTPException(status_code=404, detail="not found")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
