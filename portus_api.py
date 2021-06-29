import requests
from datetime import datetime


def make_request(url, postObject):
    response = requests.post(
        url, json=postObject, headers={"Content-type": "application/json;charset=UTF-8"}
    )
    return response.json()


date_to_api_utc = lambda date: date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Waves:
    class Points:
        Boya_de_Abra_Ciervana = "1138"
        Boya_Costera_de_Bilbao_II = "1103"
        Boya_de_Bilbao_Vizcaya = "2136"

    class Params:
        Altura_Signif_del_Oleaje = "hm0"
        Altura_Máxima_del_Oleaje = "hmax"
        Periodo_de_Pico = "tp"
        Periodo_Medio = "tm"

    @staticmethod
    def get_hourly_data(point, param, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/WAVE/"
            + point
            + "?locale=es",
            {
                "graficos": [
                    {"text": "Datos horarios", "grafico": "DATOS", "parametro": param}
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "WAVE",
            },
        )


# print(
#     Waves.get_hourly_data(
#         Waves.Points.Boya_Costera_de_Bilbao_II,
#         Waves.Params.Altura_Máxima_del_Oleaje,
#         date_to_api_utc(datetime(2019, 6, 28)),
#         date_to_api_utc(datetime(2019, 6, 30)),
#     )
# )


class SeaLevel:
    class Points:
        Mareografo_de_Bilbao_III = "3114"

    class MonthlyParam:
        Niveles = "niv"
        Carreras = "rec"

    class ReferenceLevel:
        CERO_REDMAR = "ceroRedmar"
        CERO_HIDROGRAFICO = "ceroHidro"
        MEDIUM_LEVEL = "nivelMedio"

    @staticmethod
    def get_hourly_data(reference_level, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosData/nivelHorario/estacion/"
            + SeaLevel.Points.Mareografo_de_Bilbao_III
            + "?locale=es",
            {
                "parametros": {"nivelRef": reference_level,},
                "desde": date_ini,
                "hasta": date_end,
                "variable": "SEA_LEVEL",
            },
        )

    @staticmethod
    def get_daily_data(reference_level, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosData/nivelDia/estacion/"
            + SeaLevel.Points.Mareografo_de_Bilbao_III
            + "?locale=es",
            {
                "parametros": {"nivelRef": reference_level,},
                "desde": date_ini,
                "hasta": date_end,
                "variable": "SEA_LEVEL",
            },
        )

    @staticmethod
    def get_monthly_data(reference_level, param, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosData/nivelMes/estacion/"
            + SeaLevel.Points.Mareografo_de_Bilbao_III
            + "?locale=es",
            {
                "parametros": {"parametroX": param, "nivelRef": reference_level},
                "desde": date_ini,
                "hasta": date_end,
                "variable": "SEA_LEVEL",
            },
        )


# print(
#     SeaLevel.get_hourly_data(
#         SeaLevel.ReferenceLevel.CERO_REDMAR,
#         date_to_api_utc(datetime(1992, 12, 1)),
#         date_to_api_utc(datetime(1992, 12, 2)),
#     )
# )

# print(
#     SeaLevel.get_daily_data(
#         SeaLevel.ReferenceLevel.CERO_REDMAR,
#         date_to_api_utc(datetime(1992, 12, 1)),
#         date_to_api_utc(datetime(1992, 12, 3)),
#     )
# )


# print(
#     SeaLevel.get_monthly_data(
#         SeaLevel.ReferenceLevel.CERO_REDMAR,
#         SeaLevel.MonthlyParam.Carreras,
#         date_to_api_utc(datetime(1992, 12, 1)),
#         date_to_api_utc(datetime(1992, 12, 3)),
#     )
# )


class Wind:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    class Params:
        Velocidad_viento = "vv_md"
        Direccion_procedencia_viento = "dv_md"

    @staticmethod
    def get_hourly_data(param, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/WIND/"
            + Wind.Points.Boya_de_Bilbao_Vizcaya
            + "?locale=es",
            {
                "graficos": [
                    {"text": "Datos horarios", "grafico": "DATOS", "parametro": param}
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "WIND",
            },
        )


# print(
#     Wind.get_hourly_data(
#         Wind.Params.Direccion_procedencia_viento,
#         date_to_api_utc(datetime(2020, 6, 1)),
#         date_to_api_utc(datetime(2020, 6, 15)),
#     )
# )


class PortAgitation:
    class Points:
        Mareografo_de_Bilbao_III = "3114"

    class Params:
        Altura_Signif_del_Oleaje = "hm0"
        Altura_Máxima_del_Oleaje = "hmax"
        Periodo_Medio = "tm02"

    @staticmethod
    def get_20min_data(param, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/AGITATION/"
            + PortAgitation.Points.Mareografo_de_Bilbao_III
            + "?locale=es",
            {
                "graficos": [
                    {
                        "text": "Datos cada 20 min",
                        "grafico": "DATOS_AGITACION",
                        "parametro": param,
                    }
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "AGITATION",
            },
        )


# print(
#     PortAgitation.get_20min_data(
#         PortAgitation.Params.Altura_Máxima_del_Oleaje,
#         date_to_api_utc(datetime(2019, 6, 29)),
#         date_to_api_utc(datetime(2019, 7, 2)),
#     )
# )


class Temperature:
    class Points:
        Boya_Costera_de_Bilbao_II = "1103"
        Boya_de_Bilbao_Vizcaya = "2136"

    @staticmethod
    def getPointParam(point):
        return "ts" if point == "1103" else "ts2"

    @staticmethod
    def get_hourly_data(point, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/WATER_TEMP/"
            + point
            + "?locale=es",
            {
                "graficos": [
                    {
                        "text": "Datos horarios",
                        "grafico": "DATOS",
                        "parametro": Temperature.getPointParam(point),
                    }
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "WATER_TEMP",
            },
        )


# print(
#     Temperature.get_hourly_data(
#         Temperature.Points.Boya_Costera_de_Bilbao_II,
#         date_to_api_utc(datetime(2019, 6, 29)),
#         date_to_api_utc(datetime(2019, 7, 1)),
#     )
# )


class AirPressure:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    @staticmethod
    def get_hourly_data(date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/AIR_PRESURE/"
            + AirPressure.Points.Boya_de_Bilbao_Vizcaya
            + "?locale=es",
            {
                "graficos": [
                    {"text": "Datos horarios", "grafico": "DATOS", "parametro": "ps"}
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "AIR_PRESURE",
            },
        )


# print(
#     AirPressure.get_hourly_data(
#         date_to_api_utc(datetime(2019, 6, 1)), date_to_api_utc(datetime(2019, 6, 29))
#     )
# )


class Currents:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    class Params:
        Velocidad_de_corriente = "vc_md"
        Direccion_prop_de_corriente = "dc_md"

    @staticmethod
    def get_hourly_data(param, date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/CURRENTS/"
            + Currents.Points.Boya_de_Bilbao_Vizcaya
            + "?locale=es",
            {
                "graficos": [
                    {"text": "Datos horarios", "grafico": "DATOS", "parametro": param}
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "CURRENTS",
            },
        )


# print(
#     Currents.get_hourly_data(
#         Currents.Params.Direccion_prop_de_corriente,
#         date_to_api_utc(datetime(2020, 1, 1)),
#         date_to_api_utc(datetime(2020, 1, 31)),
#     )
# )


class AirTemperature:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    @staticmethod
    def get_hourly_data(date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/AIR_TEMP/"
            + AirTemperature.Points.Boya_de_Bilbao_Vizcaya
            + "?locale=es",
            {
                "graficos": [
                    {"text": "Datos horarios", "grafico": "DATOS", "parametro": "ta"}
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "AIR_TEMP",
            },
        )


# print(
#     AirTemperature.get_hourly_data(
#         date_to_api_utc(datetime(2020, 1, 1)), date_to_api_utc(datetime(2020, 3, 1))
#     )
# )


class Salinity:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    @staticmethod
    def get_hourly_data(date_ini, date_end):
        return make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/SALINITY/"
            + Salinity.Points.Boya_de_Bilbao_Vizcaya
            + "?locale=es",
            {
                "graficos": [
                    {"text": "Datos horarios", "grafico": "DATOS", "parametro": "sa2"}
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "SALINITY",
            },
        )


# print(
#     Salinity.get_hourly_data(
#         date_to_api_utc(datetime(2020, 3, 1)), date_to_api_utc(datetime(2020, 3, 30))
#     )
# )
