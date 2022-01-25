import asyncio
from loguru import logger
import motor.motor_asyncio
from pymongo import ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017/")
db = client["InvarsatAnalyzer"]
col = db["Portus"]
col.create_index(
    [
        ("variable", ASCENDING),
        ("punto", ASCENDING),
        ("parametro", ASCENDING),
        ("datos.fecha", ASCENDING),
    ],
    name="dateIndex",
    unique=True,
    background=True,
)


class PortusCollections:
    WAVES = "Waves"
    SEA_LEVEL = "SeaLevel"
    WIND = "Wind"
    PORT_AGITATION = "PortAgitation"
    TEMPERATURE = "Temperature"
    AIR_PRESSURE = "AirPressure"
    CURRENTS = "Currents"
    AIR_TEMPERATURE = "AirTemperature"
    SALINITY = "Salinity"


async def insert_many_documents(list_to_insert):
    client.get_io_loop = asyncio.get_running_loop

    if list_to_insert:
        chunks = [
            list_to_insert[i : i + 1000] for i in range(0, len(list_to_insert), 1000)
        ]
        for chunk in chunks:
            operations = [
                UpdateOne(item, {"$set": item}, upsert=True) for item in chunk
            ]
            try:
                await col.bulk_write(operations, ordered=False)
            except BulkWriteError as bwe:
                logger.error(bwe.details)
    else:
        logger.warning("Lista para insertar sin datos")


def get_last_item_date_from_collection(var_name, point):
    last_date_item = list(
        col.find({"variable": var_name, "punto": point})
        .sort("datos.fecha", -1)
        .limit(1)
    )

    if len(last_date_item) != 0:
        return last_date_item[0]["datos"]["fecha"]
    else:
        return None


def get_documents_between_date_range(var_name, date_ini, date_end):
    return list(
        col.find(
            {
                "variable": var_name,
                "datos.fecha": {"$gte": date_ini},
                "datos.fecha": {"$lte": date_end},
            }
        )
    )


# from datetime import datetime
# get_documents_between_date_range(
#     PortusCollections.WAVES, datetime(2020, 1, 1), datetime(2020, 1, 30)
# )
