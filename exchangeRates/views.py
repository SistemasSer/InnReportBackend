import requests
from django.core.cache import cache
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import ExchangeRateSerializer, ExchangeRateRawMaterialsSerializer, CombinedExchangeRateSerializer

class ExchangeRatesFX(APIView):
    def get(self, request):
        # api_key = "3T408UWJLOA36LRD"
        api_key = "demo"
        url = "https://www.alphavantage.co/query"
        # currencies = ["USD", "CLP", "MXN", "EUR"]
        currencies = ["EUR"]

        results = []

        for currency in currencies:
            try:
                params = {
                    "function": "FX_DAILY",
                    "from_symbol": currency,
                    "to_symbol": "USD",
                    "apikey": api_key
                }

                response = requests.get(url, params=params)
                data = response.json()

                print(f"Datos recibidos para {currency}: {data}")

                if "Time Series FX (Daily)" not in data:
                    results.append({
                        "currency": currency,
                        "error": "No hay datos disponibles"
                    })
                    continue

                time_series = data["Time Series FX (Daily)"]

                # Obtener las dos fechas más recientes
                sorted_dates = sorted(time_series.keys(), reverse=True)
                
                if len(sorted_dates) < 2:
                    results.append({
                        "currency": currency,
                        "error": "No hay suficientes datos históricos"
                    })
                    continue

                # Tomar los dos primeros registros (más recientes)
                current_date = sorted_dates[0]
                previous_date = sorted_dates[1]

                current_close = time_series[current_date]["4. close"]
                previous_close = time_series[previous_date]["4. close"]

                print(f"Datos procesados para {currency}: current_date={current_date}, current_close={current_close}, previous_date={previous_date}, previous_close={previous_close}")

                results.append({
                    "currency": currency,
                    "current": {
                        "date": current_date,
                        "close": float(current_close)
                    },
                    "previous": {
                        "date": previous_date,
                        "close": float(previous_close)
                    }
                })

            except Exception as e:
                print(f"Error procesando datos para {currency}: {str(e)}")
                results.append({
                    "currency": currency,
                    "error": f"Error: {str(e)}"
                })

        print(f"Resultados finales: {results}")

        serializer = ExchangeRateSerializer(data=results, many=True)
        if serializer.is_valid():
            return Response(serializer.data, status=status.HTTP_200_OK)
        else:
            return Response(serializer.errors, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class ExchangeRatesCommodities(APIView):
    def get(self, request):
        api_key = "demo"
        url = "https://www.alphavantage.co/query"
        rawMaterials = ["WTI", "BRENT", "NATURAL_GAS", "COFFEE", "COPPER" ,"SUGAR",] 

        results = []

        for rm in rawMaterials: 
            try:
                params = {
                    "function": rm,
                    "interval": "monthly",
                    "apikey": api_key
                }

                response = requests.get(url, params=params)
                data = response.json()

                time_series = data.get("data", [])

                # Validar estructura de los datos
                if not isinstance(time_series, list) or len(time_series) < 2:
                    error_msg = "Datos insuficientes" if isinstance(time_series, list) else "Formato inválido"
                    results.append({"rawMaterial": rm, "error": error_msg})
                    continue

                # Ordenar y obtener registros más recientes
                sorted_entries = sorted(time_series, key=lambda x: x['date'], reverse=True)
                current_entry = sorted_entries[0]
                previous_entry = sorted_entries[1]

                # Extraer valores
                current_date = current_entry.get("date")
                current_value = current_entry.get("value")
                previous_date = previous_entry.get("date")
                previous_value = previous_entry.get("value")

                # Validar y convertir valores
                if not all([current_date, current_value, previous_date, previous_value]):
                    results.append({"rawMaterial": rm, "error": "Datos incompletos"})
                    continue

                try:
                    current_close = float(current_value)
                    previous_close = float(previous_value)
                except ValueError:
                    results.append({"rawMaterial": rm, "error": "Valor no numérico"})
                    continue

                # Construir respuesta
                results.append({
                    "rawMaterial": rm,
                    "current": {"date": current_date, "close": current_close},
                    "previous": {"date": previous_date, "close": previous_close}
                })

            except Exception as e:
                results.append({"rawMaterial": rm, "error": f"Error: {str(e)}"})

        # Validar y retornar respuesta
        serializer = ExchangeRateRawMaterialsSerializer(data=results, many=True)
        if serializer.is_valid():
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class CombinedExchangeRates(APIView):

    def get(self, request):
        # Verificar si los datos están en caché
        cache_key = "combined_exchange_rates"
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return Response(cached_data, status=status.HTTP_200_OK)

        api_key = "3T408UWJLOA36LRD"
        # api_key = "demo"
        url = "https://www.alphavantage.co/query"
        currencies = ["USD", "EUR", "JPY", "GBP", "ARS", "CAD", "MXN", "CLP", "UYU", "BRL"]
        # currencies = ["EUR"]
        rawMaterials = ["WTI", "BRENT", "NATURAL_GAS", "COFFEE", "SUGAR"]

        results = []

        # Procesar divisas
        for currency in currencies:
            try:
                params = {
                    "function": "FX_DAILY",
                    "from_symbol": currency,
                    "to_symbol": "COP",
                    # "to_symbol": "USD",
                    "apikey": api_key
                }

                response = requests.get(url, params=params)
                data = response.json()

                if "Time Series FX (Daily)" not in data:
                    results.append({
                        "materialName": currency,
                        "error": "No hay datos disponibles"
                    })
                    continue

                time_series = data["Time Series FX (Daily)"]
                sorted_dates = sorted(time_series.keys(), reverse=True)

                if len(sorted_dates) < 2:
                    results.append({
                        "materialName": currency,
                        "error": "No hay suficientes datos históricos"
                    })
                    continue

                current_date = sorted_dates[0]
                previous_date = sorted_dates[1]
                current_close = time_series[current_date]["4. close"]
                previous_close = time_series[previous_date]["4. close"]

                results.append({
                    "materialName": currency,
                    "current": {
                        "date": current_date,
                        "close": float(current_close)
                    },
                    "previous": {
                        "date": previous_date,
                        "close": float(previous_close)
                    }
                })

            except Exception as e:
                results.append({
                    "materialName": currency,
                    "error": f"Error: {str(e)}"
                })

        # Procesar materias primas
        for rm in rawMaterials:
            try:
                params = {
                    "function": rm,
                    "interval": "daily",
                    # "interval": "monthly",
                    "apikey": api_key
                }
                response = requests.get(url, params=params)
                data = response.json()

                unitRaw = data.get ("unit")
                time_series = data.get("data", [])

                if not isinstance(time_series, list) or len(time_series) < 2:
                    error_msg = "Datos insuficientes" if isinstance(time_series, list) else "Formato inválido"
                    results.append({"materialName": rm, "error": error_msg})
                    continue

                sorted_entries = sorted(time_series, key=lambda x: x['date'], reverse=True)
                current_entry = sorted_entries[0]
                previous_entry = sorted_entries[1]

                current_date = current_entry.get("date")
                current_value = current_entry.get("value")
                previous_date = previous_entry.get("date")
                previous_value = previous_entry.get("value")

                if not all([current_date, current_value, previous_date, previous_value]):
                    results.append({"materialName": rm, "error": "Datos incompletos"})
                    continue

                try:
                    current_close = float(current_value)
                    previous_close = float(previous_value)
                except ValueError:
                    results.append({"materialName": rm, "error": "Valor no numérico"})
                    continue

                results.append({
                    "materialName": rm,
                    "unit": unitRaw,
                    "current": {"date": current_date, "close": current_close},
                    "previous": {"date": previous_date, "close": previous_close}
                })

            except Exception as e:
                results.append({"materialName": rm, "error": f"Error: {str(e)}"})

        serializer = CombinedExchangeRateSerializer(data=results, many=True)
        if serializer.is_valid():
            cache.set(cache_key, serializer.data, 86400)
            return Response(serializer.data, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)