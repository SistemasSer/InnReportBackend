import requests
import concurrent.futures
import threading

from django.db import transaction
from django.db.models import Q

from decimal import Decimal

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from balSup.models import BalSupModel
from balSup.serializers import BalSupSerializer

from pucSup.models import PucSupModel

from datetime import datetime, timedelta
from collections import defaultdict

def get_month_name(month_number):
    month_names = [
        "ENERO", "FEBRERO", "MARZO", "ABRIL",
        "MAYO", "JUNIO", "JULIO", "AGOSTO",
        "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
    ]
    
    if 1 <= month_number <= 12:
        return month_names[month_number - 1]
    else:
        raise ValueError("Número de mes inválido. Debe estar entre 1 y 12.")

class BalSupApiView(APIView):
    def get(self, request):
        serializer = BalSupSerializer(BalSupModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    
    def post(self, request):
        data_list = request.data.get('extractedData', [])
        is_staff = request.data.get('isStaff', False)
        
        serializer = BalSupSerializer(data=data_list, many=True)
        serializer.is_valid(raise_exception=True)

        # Obtener instancias existentes
        existing_instances = BalSupModel.objects.filter(
            Q(periodo__in=[data['periodo'] for data in data_list]) &
            Q(mes__in=[data['mes'] for data in data_list]) &
            Q(entidad_RS__in=[data['entidad_RS'] for data in data_list])
        )
        
        # Agrupar las instancias existentes
        existing_dict = defaultdict(list)
        for instance in existing_instances:
            key = (instance.periodo, instance.mes, instance.entidad_RS)
            existing_dict[key].append(instance)

        new_instances = []
        update_instances = []
        errors = set()

        for data in data_list:
            key = (data['periodo'], data['mes'], data['entidad_RS'])
            if key in existing_dict:
                instances = existing_dict[key]
                
                if is_staff:
                    new_instances.append(BalSupModel(**data))
                else:
                    error_message = f"Datos ya existentes para periodo {data['periodo']}, mes {get_month_name(data['mes'])}, Entidad: {data['entidad_RS']}."
                    errors.add(error_message)
            else:
                # Si no existe, se agrega una nueva instancia
                new_instances.append(BalSupModel(**data))

        with transaction.atomic():
            if new_instances:
                BalSupModel.objects.bulk_create(new_instances)
            if update_instances:
                for instance, fields_to_update in update_instances:
                    BalSupModel.objects.bulk_update([instance], fields_to_update)

        response_data = {
            "created": len(new_instances),
            "updated": len(update_instances),
            "errors": list(errors)
        }

        if errors:
            return Response(status=status.HTTP_400_BAD_REQUEST, data=response_data)
        return Response(status=status.HTTP_200_OK, data=response_data)

class BalSupApiViewDetail(APIView):
    def get_object(self, entidad_nit):
        try:
            return BalSupModel.objects.filter(entidad_nit=entidad_nit)
        except BalSupModel.DoesNotExist:
            return None

    def get(self, request, id):
        post = self.get_object(id)
        serializer = BalSupSerializer(post)
        # request_data = {
        #     "method": request.method,
        #     "path": request.path,
        #     "headers": dict(request.headers),
        #     "query_params": dict(request.query_params),
        #     "body": request.data
        # }

        return Response(status=status.HTTP_200_OK, data=serializer.data)
        # return Response(status=status.HTTP_200_OK, data=request_data)

    # def put(self, request, id):
    #     post = self.get_object(id)
    #     if(post==None):
    #         return Response(status=status.HTTP_200_OK, data={ 'error': 'Not found data'})
    #     serializer = BalSupSerializer(post, data=request.data)
    #     if serializer.is_valid():
    #         serializer.save()
    #         return Response(status=status.HTTP_200_OK, data=serializer.data)
    #     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    # def delete(self, request, id):
    #     post = self.get_object(id)
    #     post.delete()
    #     response = { 'deleted': True }
    #     return Response(status=status.HTTP_204_NO_CONTENT, data=response)

thread_BalSupA = threading.local()

class BalSupApiViewA(APIView):
    def post(self, request):
        data = request.data
        transformed_results = {}
        saldos_cache = defaultdict(lambda: defaultdict(Decimal))
        for item in data:
            superfinanciera_data = item.get("nit", {}).get("superfinanciera", [])
            if not superfinanciera_data:
                return Response([], status=status.HTTP_200_OK)
        bloques = self.dividir_en_bloques(data)
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.procesar_bloque, bloque, transformed_results, saldos_cache) for bloque in bloques]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        final_results = list(transformed_results.values())
        return Response(data=final_results, status=status.HTTP_200_OK)

    def dividir_en_bloques(self, datos):
        return [item for item in datos]

    def get_saldo_from_db(self, razon_social, periodo, puc_codigo, mes):
        if not hasattr(thread_BalSupA, 'last_periodo'):
            thread_BalSupA.last_periodo = None
            thread_BalSupA.last_mes_number = None
            thread_BalSupA.saved_query_results = None
        if thread_BalSupA.last_periodo == periodo and thread_BalSupA.last_mes_number == mes:
            all_query_results = thread_BalSupA.saved_query_results
        else:
            q_current_period = Q(periodo=periodo, puc_codigo=puc_codigo, mes=mes)
            all_query_results = BalSupModel.objects.filter(q_current_period).values("entidad_RS", "periodo", "mes", "saldo")
            thread_BalSupA.saved_query_results = all_query_results
        thread_BalSupA.last_periodo = periodo
        thread_BalSupA.last_mes_number = mes
        filtered_results = [result for result in all_query_results if result['entidad_RS'] == razon_social]
        return filtered_results

    def procesar_bloque(self, bloque, transformed_results, saldos_cache):
        periodo = int(bloque.get("periodo"))
        mes = bloque.get("mes")
        puc_codigo = bloque.get("puc_codigo")

        if puc_codigo == "230000": 
            puc_codigo = "240000"
        if puc_codigo == "350000": 
            puc_codigo = "391500"

        fecha1 = datetime(periodo, mes, 1)
        fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        fecha1_str = fecha1.strftime("%Y-%m-%dT00:00:00")
        fecha2_str = fecha2.strftime("%Y-%m-%dT23:59:59")
        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"
        url_financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta='{puc_codigo}' AND moneda ='0'"
        response_financiera = requests.get(url_financiera)
        all_data = response_financiera.json() if response_financiera.status_code == 200 else []
        for nit_info in bloque.get("nit", {}).get("superfinanciera", []):
            nit = nit_info.get("nit")
            razon_social = nit_info.get("RazonSocial")
            sigla = nit_info.get("sigla")
            key = (razon_social, puc_codigo)
            if key not in transformed_results:
                transformed_results[key] = {
                    "razon_social": razon_social,
                    "sigla": sigla,
                    "puc_codigo": puc_codigo,
                    "saldos": [],
                }
            entity_data = [data for data in all_data if data['nombre_entidad'] == razon_social]
            saldo_en_bd = False
            if entity_data:
                for result in entity_data:
                    saldo = float(result.get('valor', '0'))
                    saldos_cache[razon_social][puc_codigo] = saldo
                    transformed_results[key]["saldos"].append({"periodo": periodo, "mes": mes, "saldo": saldo})
            else:
                query_results = self.get_saldo_from_db(razon_social, periodo, puc_codigo, mes)
                if query_results:
                    first_result = query_results[0]
                    saldo = float(first_result["saldo"])
                    saldos_cache[razon_social][puc_codigo] = saldo
                    transformed_results[key]["saldos"].append(
                        {"periodo": first_result["periodo"], "mes": first_result["mes"], "saldo": saldo}
                    )
                    saldo_en_bd = True
            if not entity_data and not saldo_en_bd:
                transformed_results[key]["saldos"].append({"periodo": periodo, "mes": mes, "saldo": 0})
            transformed_results[key]["saldos"] = sorted(transformed_results[key]["saldos"], key=lambda x: (x["periodo"], x["mes"]))

thread_Indicador = threading.local()
thread_lock_Indicador = threading.Lock()

class BalSupApiViewIndicador(APIView):
    def post(self, request):
        data = request.data
        results = []
        periodo_anterior = None
        mes_anterior = None
        for item in data:
            superfinanciera_data = item.get("nit", {}).get("superfinanciera", [])
            if not superfinanciera_data:
                return Response([], status=status.HTTP_200_OK)
        bloques = self.dividir_en_bloques(data)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.procesar_bloque, bloque, results, periodo_anterior, mes_anterior) for bloque in bloques]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        return Response(results)

    def dividir_en_bloques(self, datos):
        return [item for item in datos]

    def procesar_bloque(self, bloque, results, periodo_anterior, mes_anterior):
        periodo = int(bloque.get("periodo"))
        mes = bloque.get("mes")
        mes_decimal = Decimal(mes)
        fecha1_str, fecha2_str = self.build_dates(periodo, mes)
        puc_codes_current = ["100000", "110000", "120000", "130000", "140000", "210000", "240000", "250000", "300000", "310000", "320000", "370500", "391500", "410200", "510200", "510300"]
        puc_codes_prev = ["100000", "140000", "210000", "240000", "300000"]
        saldos_current = self.get_saldos(fecha1_str, fecha2_str, puc_codes_current)
        periodo_anterior_actual = periodo - 1
        mes_ultimo = 12
        fecha1_str_pre, fecha2_str_pre = self.build_dates(periodo_anterior_actual, mes_ultimo)
        if periodo_anterior != periodo_anterior_actual or mes_anterior != mes_ultimo:
            saldos_previous = self.get_saldos(fecha1_str_pre, fecha2_str_pre, puc_codes_prev, previous=True)
            periodo_anterior, mes_anterior = periodo_anterior_actual, mes_ultimo
        else:
            saldos_previous = thread_Indicador.saved_saldos_previous
        thread_Indicador.saved_saldos_previous = saldos_previous
        self.process_indicators(bloque, saldos_current, saldos_previous, results, mes_decimal, periodo, mes, periodo_anterior_actual,puc_codes_current,  puc_codes_prev)
        return Response(results)

    def build_dates(self, periodo, mes):
        fecha1 = datetime(periodo, mes, 1)
        fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        return fecha1.strftime("%Y-%m-%dT00:00:00"), fecha2.strftime("%Y-%m-%dT23:59:59")

    def get_saldos(self, fecha1_str, fecha2_str, puc_codes, previous=False):
        saldos = defaultdict(lambda: defaultdict(Decimal))
        puc_codes_str = ','.join(f"'{code}'" for code in puc_codes)
        url = f"https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=100000&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta IN ({puc_codes_str}) AND moneda ='0'"
        try:
            response = requests.get(url)
            response.raise_for_status()
            all_data = response.json()
            for result in all_data:
                razon_social = result.get("nombre_entidad")
                cuenta = result.get("cuenta")
                total_saldo = Decimal(result.get("valor", 0))
                saldos[razon_social][cuenta] += total_saldo
        except requests.RequestException as e:
            print(f"Error al obtener saldos: {e}")
        return saldos

    def process_indicators(self, item, saldos_current, saldos_previous, results, mes_decimal, periodo, mes, periodo_anterior_actual, puc_codes_current, puc_codes_prev):
        for nit_info in item.get("nit", {}).get("superfinanciera", []):
            razon_social = nit_info.get("RazonSocial")
            if not any(saldos_current[razon_social].values()):
                self.load_saldos_from_db(razon_social, saldos_current, periodo, mes, puc_codes_current, is_current_period=True)
            if not any(saldos_previous[razon_social].values()):
                self.load_saldos_from_db(razon_social, saldos_previous, periodo_anterior_actual, 12, puc_codes_prev, is_current_period=False)
            try:
                indicadores = self.calculate_indicators(razon_social, saldos_current, saldos_previous, mes_decimal)
                result_entry = {
                    "entidad_RS": razon_social,
                    "sigla": nit_info.get("sigla"),
                    "periodo": periodo,
                    "mes": mes,
                    **indicadores
                }
                with thread_lock_Indicador:
                    results.append(result_entry)
            except Exception as e:
                print(f"Error en cálculo de indicadores para {razon_social}: {e}")
        with thread_lock_Indicador:
            results.sort(key=lambda x: (x['periodo'], x['mes']))

    def load_saldos_from_db(self, razon_social, saldos_current, periodo, mes, puc_codes, is_current_period=True):
        if is_current_period:
            if not hasattr(thread_Indicador, 'currentPeriodo'):
                thread_Indicador.currentPeriodo = None
                thread_Indicador.currentMes = None
                thread_Indicador.saved_query_current = None
            if thread_Indicador.currentPeriodo == periodo and thread_Indicador.currentMes == mes:
                all_query_results = thread_Indicador.saved_query_current
            else:
                q_current_period = Q(periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes)
                all_query_results = BalSupModel.objects.filter(q_current_period).values('entidad_RS', 'puc_codigo', 'saldo')
                thread_Indicador.saved_query_current = all_query_results
                thread_Indicador.currentPeriodo = periodo
                thread_Indicador.currentMes = mes
        else:
            if not hasattr(thread_Indicador, 'last_periodo'):
                thread_Indicador.last_periodo = None
                thread_Indicador.last_Mes = None
                thread_Indicador.saved_query_Last = None
            if thread_Indicador.last_periodo == periodo and thread_Indicador.last_Mes == mes:
                all_query_results = thread_Indicador.saved_query_Last
            else:
                q_previous_period = Q(periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes)
                all_query_results = BalSupModel.objects.filter(q_previous_period).values('entidad_RS', 'puc_codigo', 'saldo')
                thread_Indicador.saved_query_Last = all_query_results
                thread_Indicador.last_periodo = periodo
                thread_Indicador.last_Mes = mes
        for result in all_query_results:
            if result['entidad_RS'] == razon_social:
                puc_codigo = result['puc_codigo']
                saldo = result['saldo']
                if razon_social not in saldos_current:
                    saldos_current[razon_social] = {}
                saldos_current[razon_social][puc_codigo] = saldo

    def calculate_indicators(self, razon_social, saldos_current, saldos_previous, mes_decimal):
        def safe_division(numerator, denominator):
            return (numerator / denominator * 100) if denominator else 0

        indicador_cartera = safe_division(saldos_current[razon_social]["140000"], saldos_current[razon_social]["100000"])
        indicador_deposito = safe_division(saldos_current[razon_social]["210000"], saldos_current[razon_social]["100000"])
        indicador_obligaciones = safe_division(saldos_current[razon_social]["240000"], saldos_current[razon_social]["100000"])
        indicador_cap_social = safe_division(saldos_current[razon_social]["310000"], saldos_current[razon_social]["100000"])
        indicador_cap_inst = safe_division(saldos_current[razon_social]["320000"] + saldos_current[razon_social]["370500"], saldos_current[razon_social]["100000"])
        denominator_roe = (saldos_previous[razon_social]["300000"] + (saldos_current[razon_social]["300000"] / mes_decimal) * 12) / 2
        indicador_roe = safe_division(saldos_current[razon_social]["391500"], denominator_roe)
        denominator_roa = (saldos_previous[razon_social]["100000"] + (saldos_current[razon_social]["100000"] / mes_decimal) * 12) / 2
        indicador_roa = safe_division(saldos_current[razon_social]["391500"], denominator_roa)
        denominator_ingreso_cartera = (saldos_previous[razon_social]["140000"] + (saldos_current[razon_social]["140000"] / mes_decimal) * 12) / 2
        indicador_ingreso_cartera = safe_division(saldos_current[razon_social]["410200"], denominator_ingreso_cartera)
        denominator_costos_deposito = (saldos_previous[razon_social]["210000"] + (saldos_current[razon_social]["210000"] / mes_decimal) * 12) / 2
        indicador_costos_deposito = safe_division(saldos_current[razon_social]["510200"], denominator_costos_deposito)
        denominator_credito_banco = (saldos_previous[razon_social]["240000"] + (saldos_current[razon_social]["240000"] / mes_decimal) * 12) / 2
        indicador_credito_banco = safe_division(saldos_current[razon_social]["510300"], denominator_credito_banco)
        denominator_disponible = saldos_current[razon_social]["210000"]
        indicador_disponible = safe_division(
            (saldos_current[razon_social]["110000"] + saldos_current[razon_social]["120000"] + saldos_current[razon_social]["130000"] - (saldos_current[razon_social]["250000"] * 20 / 100)),
            denominator_disponible
        )

        return {
            "indicadorCartera": indicador_cartera,
            "indicadorDeposito": indicador_deposito,
            "indicadorObligaciones": indicador_obligaciones,
            "indicadorCapSocial": indicador_cap_social,
            "indicadorCapInst": indicador_cap_inst,
            "indicadorRoe": indicador_roe,
            "indicadorRoa": indicador_roa,
            "indicadorIngCartera": indicador_ingreso_cartera,
            "indicadorCostDeposito": indicador_costos_deposito,
            "indicadorCredBanco": indicador_credito_banco,
            "indicadorDisponible": indicador_disponible,
        }

thread_IndicadorC = threading.local()
thread_lock_IndicadorC = threading.Lock()

class BalSupApiViewIndicadorC(APIView):

    def post(self, request):
        data = request.data
        results = []
        for item in data:
            superfinanciera_data = item.get("nit", {}).get("superfinanciera", [])
            if not superfinanciera_data:
                return Response([], status=status.HTTP_200_OK)
        bloques = self.dividir_en_bloques(data)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.procesar_bloque, bloque, results) for bloque in bloques]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        return Response(results)

    def dividir_en_bloques(self, datos):
        return [item for item in datos]

    def procesar_bloque(self, bloque, results):
        periodo = int(bloque.get("periodo"))
        mes = bloque.get("mes")
        mes_decimal = Decimal(mes)
        puc_codes_current = ["140800", "140805", "140810", "140815", "140820", "140825","149100", "141200", "141205", "141210", "141215", "141220","141225", "149300", "141000", "141005", "141010", "141015","141020", "141025", "149500", "148900", "140400", "140405","140410", "140415", "140420", "140425", "140430", "140435", "140440", "140445", "140450", "141400", "141405","141410", "141415", "141420", "141425", "148800", "141430","141435", "141440", "141445", "141450", "141460", "141465","141470", "141475", "141480", "812000",]
        fecha1_str, fecha2_str = self.build_dates(periodo, mes)
        saldos_current = self.get_saldos(fecha1_str, fecha2_str, puc_codes_current)
        self.process_indicators(bloque, saldos_current, results, mes_decimal, periodo, mes,puc_codes_current)
        return Response(results)

    def build_dates(self, periodo, mes):
        fecha1 = datetime(periodo, mes, 1)
        fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        return fecha1.strftime("%Y-%m-%dT00:00:00"), fecha2.strftime("%Y-%m-%dT23:59:59")

    def get_saldos(self, fecha1_str, fecha2_str, puc_codes, previous=False):
        saldos = defaultdict(lambda: defaultdict(Decimal))
        puc_codes_str = ','.join(f"'{code}'" for code in puc_codes)
        url = f"https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=100000&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta IN ({puc_codes_str}) AND moneda ='0'"
        try:
            response = requests.get(url)
            response.raise_for_status()
            all_data = response.json()
            for result in all_data:
                razon_social = result.get("nombre_entidad")
                cuenta = result.get("cuenta")
                total_saldo = Decimal(result.get("valor", 0))
                saldos[razon_social][cuenta] += total_saldo
        except requests.RequestException as e:
            print(f"Error al obtener saldos: {e}")
        return saldos

    def process_indicators(self, item, saldos_current, results, mes_decimal, periodo, mes,puc_codes_current):
        for nit_info in item.get("nit", {}).get("superfinanciera", []):
            razon_social = nit_info.get("RazonSocial")
            if not any(saldos_current[razon_social].values()):
                self.load_saldos_from_db(razon_social, saldos_current, periodo, mes, puc_codes_current)
            try:
                indicadores = self.calculate_indicators(razon_social, saldos_current, mes_decimal)
                
                result_entry = {
                    "entidad_RS": razon_social,
                    "sigla": nit_info.get("sigla"),
                    "periodo": periodo,
                    "mes": mes,
                    **indicadores
                }
                with thread_lock_IndicadorC:
                    results.append(result_entry)
            except Exception as e:
                print(f"Error en cálculo de indicadores para {razon_social}: {e}")
        with thread_lock_IndicadorC:
            results.sort(key=lambda x: (x['periodo'], x['mes']))

    def load_saldos_from_db(self, razon_social, saldos_current, periodo, mes_number, puc_codes):
        if not hasattr(thread_IndicadorC, 'last_periodo'):
            thread_IndicadorC.last_periodo = None
            thread_IndicadorC.last_mes_number = None
            thread_IndicadorC.saved_query_results = None
        if thread_IndicadorC.last_periodo == periodo and thread_IndicadorC.last_mes_number == mes_number:
            all_query_results = thread_IndicadorC.saved_query_results
        else:
            q_current_period = Q(periodo=periodo, mes=mes_number) & Q(puc_codigo__in=puc_codes)
            all_query_results = BalSupModel.objects.filter(q_current_period).values('entidad_RS', 'puc_codigo', 'saldo')
            thread_IndicadorC.saved_query_results = all_query_results
        for result in all_query_results:
            if result['entidad_RS'] == razon_social:
                puc_codigo = result['puc_codigo']
                saldo = result['saldo']
                if razon_social not in saldos_current:
                    saldos_current[razon_social] = {}
                saldos_current[razon_social][puc_codigo] = saldo
        thread_IndicadorC.last_periodo = periodo
        thread_IndicadorC.last_mes_number = mes_number
        return saldos_current

    def calculate_indicators(self, razon_social, saldos_current, mes_decimal):
        def safe_division(numerator, denominator):
            return (numerator / denominator * 100) if denominator else 0

        #Consumo
        consumo_a = saldos_current[razon_social]["140805"]
        consumo_b = saldos_current[razon_social]["140810"]
        consumo_c = saldos_current[razon_social]["140815"]
        consumo_d = saldos_current[razon_social]["140820"]
        consumo_e = saldos_current[razon_social]["140825"]
        consumo_total = saldos_current[razon_social]["140800"]
        consumo_ind_mora = safe_division((consumo_b + consumo_c + consumo_d + consumo_e), (consumo_a + consumo_b + consumo_c + consumo_d + consumo_e))
        consumo_deterioro = saldos_current[razon_social]["149100"]
        denominator_consumo_ind_mora = (consumo_a + consumo_b + consumo_c + consumo_d + consumo_e)
        consumo_cartera_improductiva = safe_division((consumo_c + consumo_d + consumo_e), denominator_consumo_ind_mora)
        denominator_consumo_porc_cobertura = (consumo_b + consumo_c + consumo_d + consumo_e)
        consumo_porc_cobertura = safe_division(consumo_deterioro, denominator_consumo_porc_cobertura)

        #microcredito
        microcredito_a = saldos_current[razon_social]["141205"]
        microcredito_b = saldos_current[razon_social]["141210"]
        microcredito_c = saldos_current[razon_social]["141215"]
        microcredito_d = saldos_current[razon_social]["141220"]
        microcredito_e = saldos_current[razon_social]["141225"]
        microcredito_total = saldos_current[razon_social]["141200"]
        microcredito_deterioro = saldos_current[razon_social]["149300"]
        denominator_microcredito_ind_mora = (microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e)
        microcredito_ind_mora = safe_division((microcredito_b + microcredito_c + microcredito_d + microcredito_e), denominator_microcredito_ind_mora)
        microcredito_cartera_improductiva = safe_division((microcredito_c + microcredito_d + microcredito_e), denominator_microcredito_ind_mora)
        denominator_microcredito_porc_cobertura = (microcredito_b + microcredito_c + microcredito_d + microcredito_e)
        microcredito_porc_cobertura = safe_division(microcredito_deterioro, denominator_microcredito_porc_cobertura)

        #comercioal
        comercial_a = saldos_current[razon_social]["141005"]
        comercial_b = saldos_current[razon_social]["141010"]
        comercial_c = saldos_current[razon_social]["141015"]
        comercial_d = saldos_current[razon_social]["141020"]
        comercial_e = saldos_current[razon_social]["141025"]
        comercial_total = saldos_current[razon_social]["141000"]
        comercial_deterioro = saldos_current[razon_social]["149500"]
        denominator_comercial_ind_mora = (comercial_a + comercial_b + comercial_c + comercial_d + comercial_e)
        comercial_ind_mora = safe_division((comercial_b + comercial_c + comercial_d + comercial_e), denominator_comercial_ind_mora)
        comercial_cartera_improductiva = safe_division((comercial_c + comercial_d + comercial_e), denominator_comercial_ind_mora)
        denominator_comercial_porc_cobertura = (comercial_b + comercial_c + comercial_d + comercial_e)
        comercial_porc_cobertura = safe_division(comercial_deterioro, denominator_comercial_porc_cobertura)

        vivienda_a = saldos_current[razon_social]["140405"] + saldos_current[razon_social]["140410"]
        vivienda_b = saldos_current[razon_social]["140415"] + saldos_current[razon_social]["140420"]
        vivienda_c = saldos_current[razon_social]["140425"] + saldos_current[razon_social]["140430"]
        vivienda_d = saldos_current[razon_social]["140435"] + saldos_current[razon_social]["140440"]
        vivienda_e = saldos_current[razon_social]["140445"] + saldos_current[razon_social]["140450"]
        vivienda_total = saldos_current[razon_social]["140400"]
        vivienda_deterioro = saldos_current[razon_social]["148900"]
        denominator_vivienda_ind_mora = (vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e)
        vivienda_ind_mora = safe_division((vivienda_b + vivienda_c + vivienda_d + vivienda_e), denominator_vivienda_ind_mora)
        vivienda_cartera_improductiva = safe_division((vivienda_c + vivienda_d + vivienda_e), denominator_vivienda_ind_mora)
        denominator_vivienda_porc_cobertura = (vivienda_b + vivienda_c + vivienda_d + vivienda_e)
        vivienda_porc_cobertura = safe_division(vivienda_deterioro, denominator_vivienda_porc_cobertura)

        #EMPLEADOS
        empleados_a = (saldos_current[razon_social]["141405"] + saldos_current[razon_social]["141430"] + saldos_current[razon_social]["141460"])
        empleados_b = (saldos_current[razon_social]["141410"] + saldos_current[razon_social]["141435"] + saldos_current[razon_social]["141465"])
        empleados_c = (saldos_current[razon_social]["141415"] + saldos_current[razon_social]["141440"] + saldos_current[razon_social]["141470"])
        empleados_d = (saldos_current[razon_social]["141420"] + saldos_current[razon_social]["141445"] + saldos_current[razon_social]["141475"])
        empleados_e = (saldos_current[razon_social]["141425"] + saldos_current[razon_social]["141450"] + saldos_current[razon_social]["141480"])
        empleados_total = (saldos_current[razon_social]["141400"])
        empleados_deterioro = saldos_current[razon_social]["149100"]
        denominator_empleados_ind_mora = (empleados_a + empleados_b + empleados_c + empleados_d + empleados_e)
        empleados_ind_mora = safe_division((empleados_b + empleados_c + empleados_d + empleados_e), denominator_empleados_ind_mora)
        empleados_cartera_improductiva = safe_division((empleados_c + empleados_d + empleados_e), denominator_empleados_ind_mora)
        denominator_empleados_porc_cobertura = (empleados_b + empleados_c + empleados_d + empleados_e)
        empleados_porc_cobertura = safe_division(empleados_deterioro, denominator_empleados_porc_cobertura)

        #TOTAL
        total_a = (consumo_a + microcredito_a + comercial_a + vivienda_a + empleados_a)
        total_b = (consumo_b + microcredito_b + comercial_b + vivienda_b + empleados_b)
        total_c = (consumo_c + microcredito_c + comercial_c + vivienda_c + empleados_c)
        total_d = (consumo_d + microcredito_d + comercial_d + vivienda_d + empleados_d)
        total_e = (consumo_e + microcredito_e + comercial_e + vivienda_e + empleados_e)
        total_castigos = saldos_current[razon_social]["812000"]
        total_total = (consumo_total + microcredito_total + comercial_total + vivienda_total + empleados_total)
        total_deterioro = (consumo_deterioro + microcredito_deterioro + comercial_deterioro + vivienda_deterioro + empleados_deterioro)
        denominator_total_ind_mora = (total_a + total_b + total_c + total_d + total_e)
        total_ind_mora = (((total_b + total_c + total_d + total_e) / denominator_total_ind_mora) * 100 if denominator_total_ind_mora else 0)
        denominator_total_porc_cobertura = (total_b + total_c + total_d + total_e)
        total_porc_cobertura = ((total_deterioro / denominator_total_porc_cobertura) * 100 if denominator_total_porc_cobertura else 0)

        return {
            "consumoA": consumo_a,
            "consumoB": consumo_b,
            "consumoC": consumo_c,
            "consumoD": consumo_d,
            "consumoE": consumo_e,
            "consumoTotal": consumo_total,
            "consumoIndMora": consumo_ind_mora,
            "consumoCartImprod": consumo_cartera_improductiva,
            "consumoDeterioro": consumo_deterioro,
            "consumoPorcCobertura": consumo_porc_cobertura,
            "microcreditoA": microcredito_a,
            "microcreditoB": microcredito_b,
            "microcreditoC": microcredito_c,
            "microcreditoD": microcredito_d,
            "microcreditoE": microcredito_e,
            "microcreditoTotal": microcredito_total,
            "microcreditoIndMora": microcredito_ind_mora,
            "microcreditoCartImprod": microcredito_cartera_improductiva,
            "microcreditoDeterioro": microcredito_deterioro,
            "microcreditoPorcCobertura": microcredito_porc_cobertura,
            "comercialA": comercial_a,
            "comercialB": comercial_b,
            "comercialC": comercial_c,
            "comercialD": comercial_d,
            "comercialE": comercial_e,
            "comercialTotal": comercial_total,
            "comercialIndMora": comercial_ind_mora,
            "comercialCartImprod": comercial_cartera_improductiva,
            "comercialDeterioro": comercial_deterioro,
            "comercialPorcCobertura": comercial_porc_cobertura,
            "viviendaA": vivienda_a,
            "viviendaB": vivienda_b,
            "viviendaC": vivienda_c,
            "viviendaD": vivienda_d,
            "viviendaE": vivienda_e,
            "viviendaTotal": vivienda_total,
            "viviendaIndMora": vivienda_ind_mora,
            "viviendaCartImprod": vivienda_cartera_improductiva,
            "viviendaDeterioro": vivienda_deterioro,
            "viviendaPorcCobertura": vivienda_porc_cobertura,
            "empleadosA": empleados_a,
            "empleadosB": empleados_b,
            "empleadosC": empleados_c,
            "empleadosD": empleados_d,
            "empleadosE": empleados_e,
            "empleadosTotal": empleados_total,
            "empleadosIndMora": empleados_ind_mora,
            "empleadosCartImprod": empleados_cartera_improductiva,
            "empleadosDeterioro": empleados_deterioro,
            "empleadosPorcCobertura": empleados_porc_cobertura,
            "totalA": total_a,
            "totalB": total_b,
            "totalC": total_c,
            "totalD": total_d,
            "totalE": total_e,
            "totalTotal": total_total,
            "totalCastigos": total_castigos,
            "totalIndMora": total_ind_mora,
            "totalDeterioro": total_deterioro,
            "totalPorcCobertura": total_porc_cobertura,
        }

class BalSupApiViewBalanceCuenta(APIView):
    def post(self, request):
        data = request.data
        results = []
        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"

        entidades_financieras = data.get("entidad", {}).get("superfinanciera", [])
        periodo = data.get("año")
        mes = data.get("mes")
        pucCodigo = data.get("pucCodigo")
        pucName = data.get("pucName")

        fecha1 = datetime(periodo, mes, 1)
        fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        fecha1_str = fecha1.strftime("%Y-%m-%dT00:00:00")
        fecha2_str = fecha2.strftime("%Y-%m-%dT23:59:59")

        def obtener_saldos_api():
            url_financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta ='{pucCodigo}' AND moneda ='0'"
            response_financiera = requests.get(url_financiera)

            if response_financiera.status_code == 200:
                all_data = response_financiera.json()
                for result in all_data:
                    razon_social = result.get("nombre_entidad")
                    total_saldo = Decimal(result.get("valor", 0))
                    yield razon_social, total_saldo
            else:
                print("No data fetched from API.")

        def obtener_saldos_db():
            for nit_info in entidades_financieras:
                razon_social = nit_info.get("RazonSocial")
                q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo=pucCodigo)
                query_results_current = BalSupModel.objects.filter(q_current_period).values("puc_codigo", "saldo")

                for result in query_results_current:
                    yield razon_social, result["saldo"]
                    break

        saldos_current = defaultdict(Decimal)

        for razon_social, total_saldo in obtener_saldos_api():
            saldos_current[razon_social] += total_saldo

        for razon_social, total_saldo in obtener_saldos_db():
            saldos_current[razon_social] += total_saldo

        def formatear_resultados():
            for nit_info in entidades_financieras:
                razon_social = nit_info.get("RazonSocial")
                saldo = saldos_current.get(razon_social, 0)
                yield {
                    "nit": nit_info.get("nit"),
                    "sigla": nit_info.get("sigla"),
                    "RazonSocial": razon_social,
                    "saldo": saldo
                }

        resultados_entidades = list(formatear_resultados())

        results.append({
            "año": periodo,
            "mes": mes,
            "puc_codigo": pucCodigo,
            "pucName": pucName,
            "entidades": resultados_entidades
        })

        return Response(data=results, status=status.HTTP_200_OK)

class BalSupApiViewBalanceIndependiente(APIView):
    def post(self, request):
        data = request.data

        results = []
        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"
        entidad_financiera = data.get("entidad", {}).get("superfinanciera", [])

        periodo = data.get("año")
        mes = data.get("mes")

        fecha1_str, fecha2_str = self.build_dates(periodo, mes)

        Razon_Social = entidad_financiera[0].get("RazonSocial")

        def obtener_saldos_api():
            url_financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND nombre_entidad = '{Razon_Social}' AND moneda = '0'"
            response_financiera = requests.get(url_financiera)
            if response_financiera.status_code == 200:
                all_data = response_financiera.json()
                if all_data: 
                    for result in all_data:
                        razon_social = result.get("nombre_entidad")
                        cuenta = result.get("cuenta")
                        nombreCuenta = result.get("nombre_cuenta")
                        valor = Decimal(result.get("valor", 0))
                        yield {
                            "razon_social": razon_social,
                            "cuenta": cuenta,
                            "nombreCuenta": nombreCuenta,
                            "valor": valor
                        }
            else:
                print(f"Error al obtener datos de la API. Status code: {response_financiera.status_code}")
            return []  

        def obtener_saldos_db():
            for nit_info in entidad_financiera:
                razon_social = nit_info.get("RazonSocial")
                q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes)
                query_results_current = BalSupModel.objects.filter(q_current_period).values("entidad_RS", "puc_codigo", "saldo")

                for result in query_results_current:
                    nombreCuenta = PucSupModel.objects.filter(Codigo=result["puc_codigo"]).first()
                    nombreCuenta = nombreCuenta.Descripcion if nombreCuenta else "Cuenta no encontrada"

                    yield {
                        "razon_social": razon_social,
                        "cuenta": result["puc_codigo"],
                        "nombreCuenta": nombreCuenta,
                        "valor": Decimal(result["saldo"])
                    }

        saldos_current = defaultdict(lambda: {"saldo": Decimal(0), "nombreCuenta": ""})

        api_data = list(obtener_saldos_api())
        if api_data:
            for saldo in api_data:
                saldos_current[saldo["cuenta"]]["saldo"] = saldo["valor"]
                saldos_current[saldo["cuenta"]]["nombreCuenta"] = saldo["nombreCuenta"]
        else:
            for saldo in obtener_saldos_db():
                saldos_current[saldo["cuenta"]]["saldo"] = saldo["valor"]
                saldos_current[saldo["cuenta"]]["nombreCuenta"] = saldo["nombreCuenta"]

        cuentas_detalles = [
            {
                "cuenta": cuenta,
                "nombreCuenta": data["nombreCuenta"],
                "total_saldo": data["saldo"]
            }
            for cuenta, data in saldos_current.items()
        ]

        cuentas_detalles = sorted(cuentas_detalles, key=lambda x: x["cuenta"])

        for entidad in entidad_financiera:
            razon_social = entidad.get("RazonSocial")
            results.append({
                "año": periodo,
                "mes": mes,
                "nit": entidad.get("nit"),
                "sigla": entidad.get("sigla"),
                "RazonSocial": razon_social,
                "cuentas_detalles": cuentas_detalles
            })
        return Response(data=results, status=status.HTTP_200_OK)

    def build_dates(self, periodo, mes):
        fecha1 = datetime(periodo, mes, 1)
        fecha2 = (fecha1.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        return fecha1.strftime("%Y-%m-%dT00:00:00"), fecha2.strftime("%Y-%m-%dT23:59:59")