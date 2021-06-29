import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["Portus"]


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


def insert_many_documents(collection, dict_to_insert):
    if dict_to_insert:
        collection = db[collection]
        collection.insert_many(dict_to_insert, ordered=False)
    else:
        print("Lista para insertar sin datos")
