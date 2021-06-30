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
                [("datos.fecha", ASCENDING), ("periodo", ASCENDING)],
                name="dateIndex",
                unique=True,
                background=True,
            )
        else:
            col.create_index(
                [("datos.fecha", ASCENDING)],
                name="dateIndex",
                unique=True,
                background=True,
            )

        [col.update(item, item, upsert=True) for item in list_to_insert]

    else:
        print("Lista para insertar sin datos")


def get_last_item_date_from_collection(collection_name, period="hourly"):
    from dateutil import parser

    col = db[collection_name]
    last_date_item = (
        col.find({"periodo": period}).sort("datos.fecha", -1).limit(1).next()
    )
    return parser.parse(last_date_item["datos"]["fecha"])
