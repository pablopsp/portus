import requests
from datetime import datetime
from dateutil import parser
from portus_mongo import PortusCollections, get_last_item_date_from_collection


def make_request(url, postObject):
    response = requests.post(
        url, json=postObject, headers={"Content-type": "application/json;charset=UTF-8"}
    )
    json_response = response.json()

    # Clean the json
    json_response.pop("series", None)
    json_response.pop("groupFields", None)
    json_response.pop("fieldDecimals", None)
    json_response.pop("ignoreFields", None)

    return json_response


def date_to_api_utc(date):
    return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


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
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/WAVE/"
            + point
            + "?locale=es",
            {
                "graficos": [{"text": "valor", "grafico": "DATOS", "parametro": param}],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "WAVE",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.WAVES
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point, param):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.WAVES, point)
        )
        print(date_ini)
        date_end = date_to_api_utc(datetime.now())
        return Waves.get_hourly_data(point, param, date_ini, date_end)


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

    @staticmethod
    def get_hourly_data(point, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/SEA_LEVEL/"
            + point
            + "?locale=es",
            {
                "desde": date_ini,
                "hasta": date_end,
                "variable": "SEA_LEVEL",
                "graficos": [
                    {
                        "text": "valor",
                        "grafico": "DATOS_NIV",
                        "parametro": "niv_h_m",
                    }
                ],
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.SEA_LEVEL
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point, reference_level):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.SEA_LEVEL, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return SeaLevel.get_hourly_data(point, reference_level, date_ini, date_end)


# print(
#     SeaLevel.get_hourly_data(
#         SeaLevel.Points.Mareografo_de_Bilbao_III,
#         date_to_api_utc(datetime(1992, 12, 1)),
#         date_to_api_utc(datetime(1992, 12, 2)),
#     )
# )


class Wind:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    class Params:
        Velocidad_viento = "vv_md"
        Direccion_procedencia_viento = "dv_md"

    @staticmethod
    def get_hourly_data(point, param, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/WIND/"
            + point
            + "?locale=es",
            {
                "graficos": [{"text": "valor", "grafico": "DATOS", "parametro": param}],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "WIND",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.WIND
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point, param):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.WIND, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return Wind.get_hourly_data(point, param, date_ini, date_end)


# print(
#     Wind.get_hourly_data(
#         Wind.Points.Boya_de_Bilbao_Vizcaya,
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
    def get_hourly_data(point, param, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/AGITATION/"
            + point
            + "?locale=es",
            {
                "graficos": [
                    {
                        "text": "valor",
                        "grafico": "DATOS_AGITACION",
                        "parametro": param,
                    }
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "AGITATION",
            },
        )

        data = response["datos"]
        # Remove all the data that is not an exact hour, PortAgitation comes in 20 min intervals
        data = list(filter(lambda x: parser.parse(x["fecha"]).minute == 0, data))
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.PORT_AGITATION
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point, param):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.PORT_AGITATION, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return PortAgitation.get_hourly_data(point, param, date_ini, date_end)


# print(
#     PortAgitation.get_20min_data(
#         PortAgitation.Points.Mareografo_de_Bilbao_III,
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
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/WATER_TEMP/"
            + point
            + "?locale=es",
            {
                "graficos": [
                    {
                        "text": "valor",
                        "grafico": "DATOS",
                        "parametro": Temperature.getPointParam(point),
                    }
                ],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "WATER_TEMP",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.TEMPERATURE
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.TEMPERATURE, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return Temperature.get_hourly_data(point, date_ini, date_end)


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
    def get_hourly_data(point, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/AIR_PRESURE/"
            + point
            + "?locale=es",
            {
                "graficos": [{"text": "valor", "grafico": "DATOS", "parametro": "ps"}],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "AIR_PRESURE",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.AIR_PRESSURE
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.AIR_PRESSURE, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return AirPressure.get_hourly_data(point, date_ini, date_end)


# print(
#     AirPressure.get_hourly_data(
#         AirPressure.Points.Boya_de_Bilbao_Vizcaya,
#         date_to_api_utc(datetime(2019, 6, 1)),
#         date_to_api_utc(datetime(2019, 6, 29)),
#     )
# )


class Currents:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    class Params:
        Velocidad_de_corriente = "vc_md"
        Direccion_prop_de_corriente = "dc_md"

    @staticmethod
    def get_hourly_data(point, param, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/CURRENTS/"
            + point
            + "?locale=es",
            {
                "graficos": [{"text": "valor", "grafico": "DATOS", "parametro": param}],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "CURRENTS",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.CURRENTS
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point, param):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.CURRENTS, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return Currents.get_hourly_data(point, param, date_ini, date_end)


# print(
#     Currents.get_hourly_data(
#         Currents.Points.Boya_de_Bilbao_Vizcaya,
#         Currents.Params.Direccion_prop_de_corriente,
#         date_to_api_utc(datetime(2020, 1, 1)),
#         date_to_api_utc(datetime(2020, 1, 31)),
#     )
# )


class AirTemperature:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    @staticmethod
    def get_hourly_data(point, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/AIR_TEMP/"
            + point
            + "?locale=es",
            {
                "graficos": [{"text": "valor", "grafico": "DATOS", "parametro": "ta"}],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "AIR_TEMP",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.AIR_TEMPERATURE
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.AIR_TEMPERATURE, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return AirTemperature.get_hourly_data(point, date_ini, date_end)


# print(
#     AirTemperature.get_hourly_data(
#         AirTemperature.Points.Boya_de_Bilbao_Vizcaya,
#         date_to_api_utc(datetime(2020, 1, 1)),
#         date_to_api_utc(datetime(2020, 3, 1)),
#     )
# )


class Salinity:
    class Points:
        Boya_de_Bilbao_Vizcaya = "2136"

    @staticmethod
    def get_hourly_data(point, date_ini, date_end):
        response = make_request(
            "https://portus.puertos.es/portussvr/api/historicosSerialTime/estacion/SALINITY/"
            + point
            + "?locale=es",
            {
                "graficos": [{"text": "valor", "grafico": "DATOS", "parametro": "sa2"}],
                "desde": date_ini,
                "hasta": date_end,
                "variable": "SALINITY",
            },
        )

        data = response["datos"]
        del response["datos"]

        clean_response = []
        for item in data:
            response_copy = response.copy()
            response_copy["variable"] = PortusCollections.SALINITY
            response_copy["periodo"] = "hourly"
            response_copy["punto"] = point
            item["fecha"] = parser.parse(item["fecha"])
            response_copy["datos"] = item
            clean_response.append(response_copy)

        return clean_response

    @staticmethod
    def get_hourly_until_today(point):
        date_ini = date_to_api_utc(
            get_last_item_date_from_collection(PortusCollections.SALINITY, point)
        )
        date_end = date_to_api_utc(datetime.now())
        return Salinity.get_hourly_data(point, date_ini, date_end)


# print(
#     Salinity.get_hourly_data(
#         Salinity.Points.Boya_de_Bilbao_Vizcaya,
#         date_to_api_utc(datetime(2020, 1, 1)),
#         date_to_api_utc(datetime(2020, 1, 30)),
#     )
# )
