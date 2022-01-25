import sys
import asyncio
from loguru import logger
from datetime import datetime

# Import from parent
sys.path[0] += "\\.."

from portus_api import (
    AirPressure,
    AirTemperature,
    Currents,
    PortAgitation,
    Salinity,
    SeaLevel,
    Temperature,
    Waves,
    Wind,
    date_to_api_utc,
)
from portus_mongo import insert_many_documents


date_ini = date_to_api_utc(datetime(2018, 1, 1))
date_end = date_to_api_utc(datetime(2022, 1, 1))


logger.info("Buscando datos entre " + date_ini.__str__() + " y " + date_end.__str__())


# WAVES
logger.info("Buscando datos de waves")
dataWaves = Waves.get_hourly_data(
    Waves.Points.Boya_Costera_de_Bilbao_II,
    Waves.Params.Altura_Máxima_del_Oleaje,
    date_ini,
    date_end,
)
logger.info("Waves data length: " + str(len(dataWaves)))

# SEA LEVEL HOURLY
logger.info("Buscando datos de sea level hourly")
dataSeaLevel = SeaLevel.get_hourly_data(
    SeaLevel.Points.Mareografo_de_Bilbao_III,
    date_ini,
    date_end,
)
logger.info("Sea level hourly data length: " + str(len(dataSeaLevel)))

# WIND
logger.info("Buscando datos de wind")
dataWind = Wind.get_hourly_data(
    Wind.Points.Boya_de_Bilbao_Vizcaya,
    Wind.Params.Direccion_procedencia_viento,
    date_ini,
    date_end,
)
logger.info("Wind data length: " + str(len(dataWind)))

# PORT AGITATION
logger.info("Buscando datos de port agitation")
dataPortAgitation = PortAgitation.get_hourly_data(
    PortAgitation.Points.Mareografo_de_Bilbao_III,
    PortAgitation.Params.Altura_Máxima_del_Oleaje,
    date_ini,
    date_end,
)
logger.info("Port agitation data length: " + str(len(dataPortAgitation)))

# TEMPERATURE
logger.info("Buscando datos de temperature")
dataTemperature = Temperature.get_hourly_data(
    Temperature.Points.Boya_Costera_de_Bilbao_II,
    date_ini,
    date_end,
)
logger.info("Temperature data length: " + str(len(dataTemperature)))

# AIR PRESSURE
logger.info("Buscando datos de air pressure")
dataAirPressure = AirPressure.get_hourly_data(
    AirPressure.Points.Boya_de_Bilbao_Vizcaya,
    date_ini,
    date_end,
)
logger.info("Air pressure data length: " + str(len(dataAirPressure)))

# CURRENTS
logger.info("Buscando datos de currents")
dataCurrents = Currents.get_hourly_data(
    Currents.Points.Boya_de_Bilbao_Vizcaya,
    Currents.Params.Direccion_prop_de_corriente,
    date_ini,
    date_end,
)
logger.info("Currents data length: " + str(len(dataCurrents)))

# AIR TEMPERATURE
logger.info("Buscando datos de air temperature")
dataAirTemperature = AirTemperature.get_hourly_data(
    AirTemperature.Points.Boya_de_Bilbao_Vizcaya,
    date_ini,
    date_end,
)
logger.info("Air temperature data length: " + str(len(dataAirTemperature)))

# SALINITY
logger.info("Buscando datos de salinity")
dataSalinity = Salinity.get_hourly_data(
    Salinity.Points.Boya_de_Bilbao_Vizcaya,
    date_ini,
    date_end,
)
logger.info("Salinity data length: " + str(len(dataSalinity)))


# Actualizar datos hasta el dia de ejecucion
# logger.info("Buscando datos de Waves hasta el dia de hoy")
# new_items = Waves.get_hourly_until_today(
#     Waves.Points.Boya_Costera_de_Bilbao_II,
#     Waves.Params.Altura_Máxima_del_Oleaje,
# )
# logger.info("Se encontraron " + str(len(new_items)) + " nuevo datos")
# insert_many_documents(new_items)
# logger.info("Datos de Waves actualizados hasta " + datetime.now().__str__())

logger.info("Inserting data to mongo")


async def main():
    await asyncio.gather(
        insert_many_documents(dataWaves),
        insert_many_documents(dataSeaLevel),
        insert_many_documents(dataWind),
        insert_many_documents(dataPortAgitation),
        insert_many_documents(dataTemperature),
        insert_many_documents(dataAirPressure),
        insert_many_documents(dataCurrents),
        insert_many_documents(dataAirTemperature),
        insert_many_documents(dataSalinity),
    )


asyncio.run(main())
logger.success("Data inserted")
