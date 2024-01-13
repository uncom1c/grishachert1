from fastapi import FastAPI, Body
import motor.motor_asyncio
import pymongo.errors
import time
from bson.objectid import ObjectId
# db: motor.motor_asyncio.AsyncIOMotorClient

mongodb_link = "mongodb://mongo:mongo@localhost:27029/"
# mongodb_link = "mongodb://mongo:mongo@mongodb:27017/"

client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_link)


db = client.data
collection = db["test"]
app = FastAPI()

async def testdb(body):
    try:
        await collection.insert_one(body)
    except pymongo.errors.DuplicateKeyError as e:
        raise e
    except pymongo.errors.ConnectionFailure as e:
        raise e
    except BaseException as e:
        
        raise e

async def message_to_db_service( message_str):
    timestamp = time.time()  
    body = {"message": message_str, "timestamp" : timestamp}
    await testdb(body)


@app.get("/db/{message}")
async def message_to_db(message: str):
    await message_to_db_service(message)
    return f"Эта функция {message}"


@app.get("/")
async def root():
    return "Hello World"

async def db_all():
    x = collection.find()
    allValues = await x.to_list(None)
    return (allValues)


# async def all_messages_service():
#     x = collection.find()
#     allValues = await x.to_list(None)


@app.get("/message")
async def all_messages():
    x = await db_all()
    return f"{x}"




async def id_db(body):
    x = (collection.find({"_id" : ObjectId('65a2dbe492766f0227e6691e') } ))
    allValues = await x.to_list(None)

    return (allValues)
        

async def id_to_db_service( id):
    
    return await id_db(id)

    
@app.get("/message/{id}")
async def id_message(id):
    x = await id_to_db_service(id)
    return f"{x}"




@app.post("/message/")
async def post_message(data = Body(embed=True)):
    print(data)
    message = data
    await message_to_db_service(message)
    return f"Эта функция {message}"

