import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv, find_dotenv
import asyncio


load_dotenv(find_dotenv())

MONGO_MAIN = os.getenv("MONGO_MAIN")
MONGO_BACKUP = os.getenv("MONGO_BACKUP")
BACKUP_DBS = os.getenv("BACKUP_DBS").split(',')

client_main = AsyncIOMotorClient(MONGO_MAIN)
client_backup = AsyncIOMotorClient(MONGO_BACKUP)


async def backup_data():
    for db_name in BACKUP_DBS:
        db_main = client_main[db_name]
        db_backup = client_backup[db_name]

        collections = await db_main.list_collection_names()

        for coll_name in collections:
            collection_main = db_main[coll_name]
            collection_backup = db_backup[coll_name]

            async for document in collection_main.find():
                doc_id = document["_id"]
                await collection_backup.replace_one({"_id": doc_id}, document, upsert=True)

            print(f"Backup : {db_name}/{coll_name} Done")


if __name__ == '__main__':
    asyncio.run(backup_data())
