from fastapi import FastAPI, HTTPException
from pymongo import MongoClient, WriteConcern, ReadPreference

MONGO_URI = "mongodb+srv://hw3_vm:homework@cluster0.efyvds6.mongodb.net/"

app = FastAPI()
client = MongoClient(MONGO_URI)
db = client["hw3_vm"]
collection = db["vehicles"]

@app.get("/")
def root():
    return {"message": "EV API is running"}

#fast but unsafe write
@app.post("/insert-fast")
def insert_fast(payload: dict):
    try:
        insert_fast_collection = collection.with_options(write_concern=WriteConcern(w=1))
        ret = insert_fast_collection.insert_one(payload)
        return {"inserted_id": str(ret.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#highly durable write
@app.post("/insert-safe")
def insert_safe(payload: dict):
    try:
        insert_safe_collection = collection.with_options(write_concern=WriteConcern("majority"))
        ret = insert_safe_collection.insert_one(payload)
        return {"inserted_id": str(ret.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#strongly consistent read
@app.get("/count-tesla-primary")
def count_tesla_primary():
    try:
        tesla_primary_collection = collection.with_options(read_preference=ReadPreference.PRIMARY)
        count = tesla_primary_collection.count_documents({"make": "TESLA"})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#eventullay consistent analytical read
@app.get("/count-bmw-secondary")
def count_bmw_secondary():
    try:
        bmw_secondary_collection = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
        count = bmw_secondary_collection.count_documents({"make": "BMW"})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
