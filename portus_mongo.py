from pymongo import MongoClient, ASCENDING

connection = MongoClient("mongodb://localhost:27017/")

db = connection["Portus"]


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


def insert_many_documents(collection_name, list_to_insert):
    if list_to_insert:
        col = db[collection_name]

        if collection_name == PortusCollections.SEA_LEVEL:
            col.create_index(
                [("datos.fecha", ASCENDING), ("periodo", ASCENDING), ("punto", ASCENDING)],
                name="dateIndex",
                unique=True,
                background=True,
            )
        else:
            col.create_index(
                [("datos.fecha", ASCENDING), ("punto", ASCENDING)],
                name="dateIndex",
                unique=True,
                background=True,
            )

        [col.update(item, item, upsert=True) for item in list_to_insert]

    else:
        print("Lista para insertar sin datos")


def get_last_item_date_from_collection(collection_name, point, period="hourly"):
    col = db[collection_name]
    last_date_item = list(
        col.find({"periodo": period, "punto": point}).sort("datos.fecha", -1).limit(1)
    )

    if(len(last_date_item) != 0):
        return last_date_item[0]["datos"]["fecha"]
    else:
        return None


def get_documents_between_date_range(collection_name, date_ini, date_end):
    col = db[collection_name]
    return list(
        col.find(
            {"datos.fecha": {"$gte": date_ini}, "datos.fecha": {"$lte": date_end},}
        )
    )


# from datetime import datetime
# get_documents_between_date_range(
#     PortusCollections.WAVES, datetime(2020, 1, 1), datetime(2020, 1, 30)
# )
