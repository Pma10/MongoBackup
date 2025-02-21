import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv, find_dotenv
import asyncio

load_dotenv(find_dotenv())

MONGO_MAIN = os.getenv("MONGO_MAIN")
MONGO_BACKUP = os.getenv("MONGO_BACKUP")
BACKUP_DBS = os.getenv("BACKUP_DBS").split(',')
BATCH_SIZE = os.getenv("BATCH_SIZE")

client_main = AsyncIOMotorClient(MONGO_MAIN)
client_backup = AsyncIOMotorClient(MONGO_BACKUP)

async def backup_data():
    for db in BACKUP_DBS:
        colls = await client_main[db].list_collection_names()
        for coll in colls:
            source_collection = client_main[db][coll]
            target_collection = client_backup[db][coll]

            async for doc in source_collection.find().batch_size(BATCH_SIZE):
                doc_id = doc["_id"]
                await target_collection.replace_one({"_id": doc_id}, doc, upsert=True)

            print(f"Backup {db}/{coll} done")

if __name__ == '__main__':
    asyncio.run(backup_data())
