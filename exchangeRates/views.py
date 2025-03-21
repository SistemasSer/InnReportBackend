import requests
import calendar
import time
import httpx
import backoff
import asyncio
from django.core.cache import cache
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import ExchangeRateSerializer, ExchangeRateRawMaterialsSerializer, CombinedExchangeRateSerializer

from datetime import datetime, date

from concurrent.futures import ThreadPoolExecutor, as_completed


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

class SolidarityFinancialData(APIView):

    MONTHS_ES = {
        1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic'
    }

    FINANCIERA_URL = "https://www.datos.gov.co/resource/mxk5-ce6w.json"

    def get_api_params_solidarity(self, year: int) -> tuple:
        if year == 2020:
            return ("https://www.datos.gov.co/resource/78xz-k3hv.json", 'codcuenta')
        elif year == 2021:
            return ("https://www.datos.gov.co/resource/irgu-au8v.json", 'codrenglon')
        return ("https://www.datos.gov.co/resource/tic6-rbue.json", 'codrenglon')

    CHUNK_SIZE = 1000  

    @backoff.on_exception( backoff.expo, (httpx.RequestError, httpx.TimeoutException), max_tries=10 )

    async def fetch_page(self, client, url: str, params: dict, entity: str):
        """Obtiene una página de datos de la API."""
        try:
            # print(f"[{entity}] Consultando: {url} con {params}")
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # print(f"[{entity}] Datos recibidos: {len(data)} registros")
            return data if isinstance(data, list) else []
        except httpx.HTTPStatusError as e:
            # print(f"[{entity}] Error HTTP {e.response.status_code}: {e}")
            return []

    async def fetch_year_data(self, url: str, params: dict, entity: str) -> list:
        """Realiza solicitudes en paralelo por CHUNKS de 3,000 registros."""
        all_data = []
        limit = self.CHUNK_SIZE
        offsets = list(range(0, 1000, limit))

        async with httpx.AsyncClient(timeout=httpx.Timeout(1.0, read=3.0)) as client:
            tasks = [
                self.fetch_page(client, url, {**params, "$limit": limit, "$offset": offset}, entity)
                for offset in offsets
            ]
            results = await asyncio.gather(*tasks)

        for data in results:
            all_data.extend(data)

        return all_data

    async def process_financiera_year_data(self, year: int) -> dict:
        """Procesa datos financieros de manera asíncrona."""
        start_date = f"{year}-01-01T00:00:00"
        end_date = f"{year}-12-31T23:59:59"
        params = {  
            "$where": (
                f"fecha_corte BETWEEN '{start_date}' AND '{end_date}' "
                f"AND cuenta = '100000' AND moneda = '0' AND tipo_entidad IN (1, 4, 32)"
            )
        }
        # print(f"[FINANCIERA] Obteniendo datos del año {year}...")
        data = await self.fetch_year_data(self.FINANCIERA_URL, params, "FINANCIERA")
        
        month_data = {abbr: False for abbr in self.MONTHS_ES.values()}
        for record in data:
            fecha_corte = record.get("fecha_corte")
            if fecha_corte:
                try:
                    month_num = int(fecha_corte[5:7])
                    month_abbr = self.MONTHS_ES.get(month_num)
                    month_data[month_abbr] = True
                except Exception as e:
                    print(f"[FINANCIERA] Error procesando fecha '{fecha_corte}': {e}")
        return month_data

    async def process_solidaria_year_data(self, year: int) -> dict:
        """Procesa datos solidarios de manera asíncrona."""
        url, codigo_str = self.get_api_params_solidarity(year)
        nits = ["804-009-752-8", "860-025-596-6", "860-007-327-5", "890-505-363-6", "890-203-225-1"]
        where_clause = f"a_o='{year}' AND {codigo_str}='100000' AND nit IN({','.join(f'\"{nit}\"' for nit in nits)})"
        params = {
            "$where": where_clause
        }
        # print(f"[SOLIDARIA] Obteniendo datos del año {year}...")
        data = await self.fetch_year_data(url, params, "SOLIDARIA")
        
        month_data = {abbr: False for abbr in self.MONTHS_ES.values()}
        for record in data:
            mes_value = record.get("mes")
            if mes_value:
                for month_num, month_full in enumerate(
                        ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", 
                        "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"],
                        start=1):
                    if mes_value.upper() == month_full:
                        month_abbr = self.MONTHS_ES.get(month_num)
                        month_data[month_abbr] = True
                        break
        return month_data

    async def get_async(self, year: int):
        """Ejecuta las consultas de ambas entidades en paralelo."""
        financiera_task = self.process_financiera_year_data(year)
        solidaria_task = self.process_solidaria_year_data(year)

        financiera_result, solidaria_result = await asyncio.gather(financiera_task, solidaria_task)

        year_data = {
            "año": year,
            "datos": [
                {"entidad": "Financiera", "valores": financiera_result},
                {"entidad": "Solidaria", "valores": solidaria_result},
            ]
        }
        return [year_data]

    def get(self, request):
        """Maneja la solicitud HTTP GET."""
        year_param = request.GET.get('year')
        if not year_param:
            return Response(
                {'error': 'El parámetro "year" es requerido'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            year = int(year_param)
        except ValueError:
            return Response(
                {'error': 'El parámetro "year" debe ser un número entero válido'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # print(f"\n=== Procesando el año: {year} ===")
        result = asyncio.run(self.get_async(year))

        # print("\nResultado final:", result)
        return Response(result, status=status.HTTP_200_OK)