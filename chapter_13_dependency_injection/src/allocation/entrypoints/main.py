import uvicorn
from allocation import bootstrap, views
from allocation.domain import commands
from allocation.entrypoints import schemas
from allocation.service_layer.handlers import InvalidSku
from fastapi import FastAPI, HTTPException, status

bus = bootstrap.bootstrap()

app = FastAPI()


@app.post("/add_batch", status_code=status.HTTP_201_CREATED)
async def add_batch(batch: schemas.AddBatchRequest):
    cmd = commands.CreateBatch(
        batch.ref,
        batch.sku,
        batch.qty,
        batch.eta,
    )
    await bus.handle(cmd)

    return "OK"


@app.post("/allocate", status_code=status.HTTP_202_ACCEPTED)
async def allocate_endpoint(line: schemas.OrderLineRequest):
    try:
        cmd = commands.Allocate(
            line.orderid,
            line.sku,
            line.qty,
        )
        await bus.handle(cmd)
    except InvalidSku as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return "OK"


@app.get("/allocations/{orderid}", status_code=status.HTTP_200_OK)
async def allocations_view_endpoint(orderid):
    result = await views.allocations(orderid, bus.uow)
    if not result:
        raise HTTPException(status_code=404, detail="not found")
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
