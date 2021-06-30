from datetime import datetime
import sys

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
from portus_mongo import PortusCollections, insert_many_documents


date_ini = date_to_api_utc(datetime(2020, 1, 1))
date_end = date_to_api_utc(datetime(2020, 1, 30))


print("Buscando datos entre " + date_ini.__str__() + " y " + date_end.__str__())


# WAVES
print("Buscando datos de waves")
dataWaves = Waves.get_hourly_data(
    Waves.Points.Boya_Costera_de_Bilbao_II,
    Waves.Params.Altura_Máxima_del_Oleaje,
    date_ini,
    date_end,
)
print("Waves data length: " + str(len(dataWaves)))
insert_many_documents(PortusCollections.WAVES, dataWaves)
print("Datos de waves insertados \n")


# SEA LEVEL HOURLY
print("Buscando datos de sea level hourly")
dataSeaLevel1 = SeaLevel.get_hourly_data(
    SeaLevel.Points.Mareografo_de_Bilbao_III,
    SeaLevel.ReferenceLevel.CERO_REDMAR,
    date_ini,
    date_end,
)
print("Sea level hourly data length: " + str(len(dataSeaLevel1)))
insert_many_documents(PortusCollections.SEA_LEVEL, dataSeaLevel1)
print("Datos de sea level hourly insertados \n")


# SEA LEVEL DAILY
print("Buscando datos de sea level daily")
dataSeaLevel2 = SeaLevel.get_daily_data(
    SeaLevel.Points.Mareografo_de_Bilbao_III,
    SeaLevel.ReferenceLevel.CERO_REDMAR,
    date_ini,
    date_end,
)
print("Sea level daily data length: " + str(len(dataSeaLevel2)))
insert_many_documents(PortusCollections.SEA_LEVEL, dataSeaLevel2)
print("Datos de sea level daily insertados \n")


# SEA LEVEL MONTHLY
print("Buscando datos de sea level monthly")
dataSeaLevel3 = SeaLevel.get_monthly_data(
    SeaLevel.Points.Mareografo_de_Bilbao_III,
    SeaLevel.ReferenceLevel.CERO_REDMAR,
    SeaLevel.MonthlyParam.Carreras,
    date_ini,
    date_end,
)
print("Sea level monthly data length: " + str(len(dataSeaLevel3)))
insert_many_documents(PortusCollections.SEA_LEVEL, dataSeaLevel3)
print("Datos de sea level monthly insertados \n")


# WIND
print("Buscando datos de wind")
dataWind = Wind.get_hourly_data(
    Wind.Points.Boya_de_Bilbao_Vizcaya,
    Wind.Params.Direccion_procedencia_viento,
    date_ini,
    date_end,
)
print("Wind data length: " + str(len(dataWind)))
insert_many_documents(PortusCollections.WIND, dataWind)
print("Datos de wind insertados \n")


# PORT AGITATION
print("Buscando datos de port agitation")
dataPortAgitation = PortAgitation.get_20min_data(
    PortAgitation.Points.Mareografo_de_Bilbao_III,
    PortAgitation.Params.Altura_Máxima_del_Oleaje,
    date_ini,
    date_end,
)
print("Port agitation data length: " + str(len(dataPortAgitation)))
insert_many_documents(PortusCollections.PORT_AGITATION, dataPortAgitation)
print("Datos de port agitation insertados \n")


# TEMPERATURE
print("Buscando datos de temperature")
dataTemperature = Temperature.get_hourly_data(
    Temperature.Points.Boya_Costera_de_Bilbao_II, date_ini, date_end,
)
print("Temperature data length: " + str(len(dataTemperature)))
insert_many_documents(PortusCollections.TEMPERATURE, dataTemperature)
print("Datos de temperature insertados \n")


# AIR PRESSURE
print("Buscando datos de air pressure")
dataAirPressure = AirPressure.get_hourly_data(
    AirPressure.Points.Boya_de_Bilbao_Vizcaya, date_ini, date_end,
)
print("Air pressure data length: " + str(len(dataAirPressure)))
insert_many_documents(PortusCollections.AIR_PRESSURE, dataAirPressure)
print("Datos de air pressure insertados \n")


# CURRENTS
print("Buscando datos de currents")
dataCurrents = Currents.get_hourly_data(
    Currents.Points.Boya_de_Bilbao_Vizcaya,
    Currents.Params.Direccion_prop_de_corriente,
    date_ini,
    date_end,
)
print("Currents data length: " + str(len(dataCurrents)))
insert_many_documents(PortusCollections.CURRENTS, dataCurrents)
print("Datos de currents insertados \n")


# AIR TEMPERATURE
print("Buscando datos de air temperature")
dataAirTemperature = AirTemperature.get_hourly_data(
    AirTemperature.Points.Boya_de_Bilbao_Vizcaya, date_ini, date_end,
)
print("Air temperature data length: " + str(len(dataAirTemperature)))
insert_many_documents(PortusCollections.AIR_TEMPERATURE, dataAirTemperature)
print("Datos de air temperature insertados \n")


# SALINITY
print("Buscando datos de salinity")
dataSalinity = Salinity.get_hourly_data(
    Salinity.Points.Boya_de_Bilbao_Vizcaya, date_ini, date_end,
)
print("Salinity data length: " + str(len(dataSalinity)))
insert_many_documents(PortusCollections.SALINITY, dataSalinity)
print("Datos de salinity insertados \n")


import time

print("Añadiendo delay de 5s..")
time.sleep(5)
print("Buscando datos de Waves hasta el dia de hoy")
new_items = Waves.get_hourly_until_today(
    Waves.Points.Boya_Costera_de_Bilbao_II, Waves.Params.Altura_Máxima_del_Oleaje,
)
print("Se encontraron " + str(len(new_items)) + " nuevo datos")
insert_many_documents(PortusCollections.WAVES, new_items)
print("Datos de Waves actualizados hasta " + datetime.now().__str__())
