from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import rest
import ws

app = FastAPI(title="Traffic Monitoring API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rest.router)
app.include_router(ws.router)


@app.get("/health")
def health():
    return {"status": "ok"}
