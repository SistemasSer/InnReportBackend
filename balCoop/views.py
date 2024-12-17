import requests
import concurrent.futures
import threading

from django.db import transaction
from django.db.models import Q

from decimal import Decimal, InvalidOperation
from collections import defaultdict

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from balCoop.models import BalCoopModel
from balCoop.serializers import BalCoopSerializer
from pucCoop.models import PucCoopModel

def format_nit_dv(nit, dv):
    nit_str = str(nit).zfill(9)
    dv_str = str(dv).zfill(1) 

    formatted_nit_dv = f"{nit_str[:3]}-{nit_str[3:6]}-{nit_str[6:]}-{dv_str}"
    return formatted_nit_dv

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

def get_saldo(nit_dv, razon_social, cuenta, saldos):
    return saldos.get(nit_dv, {}).get(cuenta, saldos.get(razon_social, {}).get(cuenta, 0))

def clean_currency_value_Decimal(value):
    try:
        cleaned_value = value.replace('$', '').replace(',', '').strip()
        
        if cleaned_value.startswith('-'):
            cleaned_value = '-' + cleaned_value[1:].replace(' ', '')
        else:
            cleaned_value = cleaned_value.replace(' ', '')

        return Decimal(cleaned_value)
    except InvalidOperation:
        # print(f"Error convirtiendo el valor: {value}")
        return Decimal(0)

class BalCoopApiView(APIView):
    def get(self, request):
        serializer = BalCoopSerializer(BalCoopModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    
    def post(self, request):
        data_list = request.data.get('extractedData', [])
        is_staff = request.data.get('isStaff', False)
        
        serializer = BalCoopSerializer(data=data_list, many=True)
        serializer.is_valid(raise_exception=True)

        # Obtener instancias existentes
        existing_instances = BalCoopModel.objects.filter(
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
                    # Si es staff, se permite la actualización
                    new_instances.append(BalCoopModel(**data))
                    # for instance in instances:
                    #     fields_to_update = []
                    #     for attr, value in data.items():
                    #         if getattr(instance, attr) != value:
                    #             setattr(instance, attr, value)
                    #             fields_to_update.append(attr)
                    #     if fields_to_update:
                    #         update_instances.append((instance, fields_to_update))
                else:
                    # No se permite la inserción si ya existe
                    error_message = f"Datos ya existentes para periodo {data['periodo']}, mes {get_month_name(data['mes'])}, Entidad: {data['entidad_RS']}."
                    errors.add(error_message)
            else:
                # Si no existe, se agrega una nueva instancia
                new_instances.append(BalCoopModel(**data))

        with transaction.atomic():
            if new_instances:
                BalCoopModel.objects.bulk_create(new_instances)
            if update_instances:
                for instance, fields_to_update in update_instances:
                    BalCoopModel.objects.bulk_update([instance], fields_to_update)

        response_data = {
            "created": len(new_instances),
            "updated": len(update_instances),
            "errors": list(errors)
        }

        if errors:
            return Response(status=status.HTTP_400_BAD_REQUEST, data=response_data)
        return Response(status=status.HTTP_200_OK, data=response_data)

class BalCoopApiViewDetail(APIView):
    def get_object(self, entidad_nit):
        try:
            return BalCoopModel.objects.get(entidad_nit=entidad_nit)
        except BalCoopModel.DoesNotExist:
            return None

    def get(self, request, id):
        post = self.get_object(id)
        serializer = BalCoopSerializer(post)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    # def put(self, request, id):
    #     post = self.get_object(id)
    #     if(post==None):
    #         return Response(status=status.HTTP_200_OK, data={ 'error': 'Not found data'})
    #     serializer = BalCoopSerializer(post, data=request.data)
    #     if serializer.is_valid():
    #         serializer.save()
    #         return Response(status=status.HTTP_200_OK, data=serializer.data)
    #     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    # def delete(self, request, id):
    #     post = self.get_object(id)
    #     post.delete()
    #     response = { 'deleted': True }
    #     return Response(status=status.HTTP_204_NO_CONTENT, data=response)

thread_BalCoopA = threading.local()

class BalCoopApiViewA(APIView):
    def post(self, request):
        data = request.data
        transformed_results = {}
        saldos_cache = defaultdict(lambda: defaultdict(Decimal))
        for item in data:
            solidaria_data = item.get("nit", {}).get("solidaria", [])
            if not solidaria_data:
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
        if not hasattr(thread_BalCoopA, 'last_periodo'):
            thread_BalCoopA.last_periodo = None
            thread_BalCoopA.last_mes_number = None
            thread_BalCoopA.saved_query_results = None
        if thread_BalCoopA.last_periodo == periodo and thread_BalCoopA.last_mes_number == mes:
            all_query_results = thread_BalCoopA.saved_query_results
        else:
            q_current_period = Q(periodo=periodo, puc_codigo=puc_codigo, mes=mes)
            all_query_results = BalCoopModel.objects.filter(q_current_period).values("entidad_RS", "periodo", "mes", "saldo")
            thread_BalCoopA.saved_query_results = all_query_results
        thread_BalCoopA.last_periodo = periodo
        thread_BalCoopA.last_mes_number = mes
        filtered_results = [result for result in all_query_results if result['entidad_RS'] == razon_social]
        return filtered_results

    def procesar_bloque(self, bloque, transformed_results, saldos_cache):
        periodo = int(bloque.get("periodo"))
        mes_number = bloque.get("mes")
        # mes = get_month_name(mes_number)

        if periodo == 2024 and mes_number == 8:
            mes = get_month_name(mes_number).capitalize()
        elif periodo == 2024 and mes_number >= 9:
            mes = get_month_name(mes_number).lower()
        else:
            mes = get_month_name(mes_number)

        puc_codigo = bloque.get("puc_codigo")
        baseUrl_entidadesSolidaria, campoCuenta = self.get_api_params(periodo)
        url_solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND mes='{mes}' AND {campoCuenta}='{puc_codigo}'"
        response_otras = requests.get(url_solidaria)
        all_data = response_otras.json() if response_otras.status_code == 200 else []
        for nit_info in bloque.get("nit", {}).get("solidaria", []):
            nit = nit_info.get("nit")
            razon_social = nit_info.get("RazonSocial")
            sigla = nit_info.get("sigla")
            dv = nit_info.get("dv")
            formatted_nit_dv = format_nit_dv(nit, dv)
            key = (razon_social, puc_codigo)
            if key not in transformed_results:
                transformed_results[key] = {
                    "razon_social": razon_social,
                    "sigla": sigla,
                    "puc_codigo": puc_codigo,
                    "saldos": [],
                }
            entity_data = [data for data in all_data if data['nit'] == formatted_nit_dv]
            saldo_en_bd = False
            if entity_data:
                for result in entity_data:
                    saldo = clean_currency_value_Decimal(result.get('valor_en_pesos', '$ 0'))
                    saldos_cache[formatted_nit_dv][puc_codigo] += saldo
                    transformed_results[key]["saldos"].append({"periodo": periodo, "mes": mes_number, "saldo": saldo})
            else:
                query_results = self.get_saldo_from_db(razon_social, periodo, puc_codigo, mes_number)
                if query_results:
                    first_result = query_results[0]
                    saldo = float(first_result["saldo"])
                    saldos_cache[razon_social][puc_codigo] = saldo
                    transformed_results[key]["saldos"].append(
                        {"periodo": first_result["periodo"], "mes": first_result["mes"], "saldo": saldo}
                    )
                    saldo_en_bd = True
            if not entity_data and not saldo_en_bd:
                transformed_results[key]["saldos"].append({"periodo": periodo, "mes": mes_number, "saldo": 0})
            transformed_results[key]["saldos"] = sorted(transformed_results[key]["saldos"], key=lambda x: (x["periodo"], x["mes"]))

    def get_api_params(self, periodo):
        if periodo == 2020:
            return ("https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=500000", 'codcuenta')
        elif periodo == 2021:
            return ("https://www.datos.gov.co/resource/irgu-au8v.json?$limit=500000", 'codrenglon')
        else:
            return ("https://www.datos.gov.co/resource/tic6-rbue.json?$limit=500000", 'codrenglon')

thread_Indicador = threading.local()
thread_lock_Indicador = threading.Lock()

class BalCoopApiViewIndicador(APIView):
    def post(self, request):
        data = request.data
        results = []
        periodo_anterior = None
        mes_anterior = None
        for item in data:
            solidaria_data = item.get("nit", {}).get("solidaria", [])
            if not solidaria_data:
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
        mes_number = bloque.get("mes")
        mes_decimal = Decimal(mes_number)
        # mes = get_month_name(mes_number)

        if periodo == 2024 and mes_number == 8:
            mes = get_month_name(mes_number).capitalize()
        elif periodo == 2024 and mes_number >= 9:
            mes = get_month_name(mes_number).lower()
        else:
            mes = get_month_name(mes_number)

        puc_codes_current = ["100000", "110000", "120000", "140000", "210000", "230000", "240000", "300000", "310000", "311010", "320000", "330500", "340500", "350000", "415000", "615005", "615010", "615015", "615020", "615035"]
        puc_codes_prev = ["100000", "140000", "210000", "230000", "300000"]
        base_url, campo_cuenta = self.get_api_details(periodo)
        saldos_current = self.get_saldos(periodo, mes, campo_cuenta, puc_codes_current, base_url)
        periodo_anterior_actual = periodo - 1
        mes_ultimo_str = get_month_name(12)
        base_url_prev, campo_cuenta_prev = self.get_api_details(periodo_anterior_actual)
        if periodo_anterior != periodo_anterior_actual or mes_anterior != mes_ultimo_str:
            saldos_previous = self.get_saldos(periodo_anterior_actual, mes_ultimo_str, campo_cuenta_prev, puc_codes_prev, base_url_prev, previous=True)
            periodo_anterior, mes_anterior = periodo_anterior_actual, mes_ultimo_str
        else:
            saldos_previous = thread_Indicador.saved_saldos_previous
        thread_Indicador.saved_saldos_previous = saldos_previous
        self.process_indicators(bloque, saldos_current, saldos_previous, results, mes_decimal, periodo, periodo_anterior_actual, mes_number, puc_codes_current, puc_codes_prev)
        return Response(results)

    def get_api_details(self, periodo):
        if periodo == 2020:
            return ("https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000", 'codcuenta')
        elif periodo == 2021:
            return ("https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000", 'codrenglon')
        else:
            return ("https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000", 'codrenglon')

    def get_saldos(self, periodo, mes, campo_cuenta, puc_codes, base_url, previous=False):
        saldos = defaultdict(lambda: defaultdict(Decimal))
        puc_codes_str = ','.join(f"'{code}'" for code in puc_codes)
        url = f"{base_url}&$where=a_o='{periodo}' AND mes='{mes}' AND {campo_cuenta} IN ({puc_codes_str})"
        try:
            response = requests.get(url)
            response.raise_for_status()
            all_data = response.json()
            if all_data:
                for result in all_data:
                    nit = result.get("nit")
                    cuenta = result.get(campo_cuenta)
                    valor_en_pesos = clean_currency_value_Decimal(result.get('valor_en_pesos', '0'))
                    if cuenta in puc_codes:
                        saldos[nit][cuenta] += valor_en_pesos
        except requests.RequestException as e:
            print(f"Error al obtener los saldos de la API: {e}")
        return saldos   

    def process_indicators(self, item, saldos_current, saldos_previous, results, mes_decimal, periodo, periodo_anterior_actual, mes_number, puc_codes_current, puc_codes_prev):
        for nit_info in item.get("nit", {}).get("solidaria", []):
            razon_social = nit_info.get("RazonSocial")
            nit = nit_info.get("nit")
            dv = nit_info.get("dv")
            formatted_nit_dv = format_nit_dv(nit, dv)
            if not any(saldos_current[formatted_nit_dv].values()):
                self.load_saldos_from_db(razon_social, saldos_current, periodo, mes_number, puc_codes_current, is_current_period=True)
            if not any(saldos_previous[formatted_nit_dv].values()):
                self.load_saldos_from_db(razon_social, saldos_previous, periodo_anterior_actual, 12, puc_codes_prev, is_current_period=False)
            try:
                indicadores = self.calculate_indicators(formatted_nit_dv, razon_social, saldos_current, saldos_previous, mes_decimal)
                result_entry = {
                    "entidad_RS": razon_social,
                    "sigla": nit_info.get("sigla"),
                    "periodo": periodo,
                    "mes": mes_number,
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
                all_query_results = BalCoopModel.objects.filter(q_current_period).values('entidad_RS', 'puc_codigo', 'saldo')
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
                all_query_results = BalCoopModel.objects.filter(q_previous_period).values('entidad_RS', 'puc_codigo', 'saldo')
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

    def calculate_indicators(self, formatted_nit_dv, razon_social, saldos_current, saldos_previous, mes_decimal):
        indicador_cartera = self.safe_division(get_saldo(formatted_nit_dv, razon_social, "140000", saldos_current),get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current))
        indicador_deposito = self.safe_division(get_saldo(formatted_nit_dv, razon_social, "210000", saldos_current),get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current))
        indicador_obligaciones = ((get_saldo(formatted_nit_dv, razon_social, "230000", saldos_current) / get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current)) * 100 if get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current) else 0)
        indicador_cap_social = ((get_saldo(formatted_nit_dv, razon_social, "310000", saldos_current) / get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current)) * 100 if get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current) else 0)
        indicador_cap_inst = (((get_saldo(formatted_nit_dv, razon_social, "311010", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "320000", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "330500", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "340500", saldos_current)) / get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current)) * 100 if get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current) else 0)
        denominator_roe = (get_saldo(formatted_nit_dv, razon_social, "300000", saldos_previous) + (get_saldo(formatted_nit_dv, razon_social, "300000", saldos_current) / mes_decimal) * 12) / 2
        indicador_roe = (get_saldo(formatted_nit_dv, razon_social, "350000", saldos_current) / denominator_roe * 100) if denominator_roe else 0
        denominator_roa = (get_saldo(formatted_nit_dv, razon_social, "100000", saldos_previous) + (get_saldo(formatted_nit_dv, razon_social, "100000", saldos_current) / mes_decimal) * 12) / 2
        indicador_roa = ((get_saldo(formatted_nit_dv, razon_social, "350000", saldos_current) / denominator_roa) * 100 if denominator_roa else 0)
        denominator_ingreso_cartera = (get_saldo(formatted_nit_dv, razon_social, "140000", saldos_previous) + (get_saldo(formatted_nit_dv, razon_social, "140000", saldos_current) / mes_decimal) * 12) / 2
        indicador_ingreso_cartera = ((get_saldo(formatted_nit_dv, razon_social, "415000", saldos_current) / denominator_ingreso_cartera) * 100 if denominator_ingreso_cartera else 0)
        denominator_costos_deposito = (get_saldo(formatted_nit_dv, razon_social, "210000", saldos_previous) + (get_saldo(formatted_nit_dv, razon_social, "210000", saldos_current) / mes_decimal) * 12) / 2
        indicador_costos_deposito = (((get_saldo(formatted_nit_dv, razon_social, "615005", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "615010", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "615015", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "615020", saldos_current)) / denominator_costos_deposito) * 100 if denominator_costos_deposito else 0)
        denominator_credito_banco = (get_saldo(formatted_nit_dv, razon_social, "230000", saldos_previous) + (get_saldo(formatted_nit_dv, razon_social, "230000", saldos_current) / mes_decimal) * 12) / 2
        indicador_credito_banco = ((get_saldo(formatted_nit_dv, razon_social, "615035", saldos_current) / denominator_credito_banco) * 100 if denominator_credito_banco else 0)
        indicador_disponible = (((get_saldo(formatted_nit_dv, razon_social, "110000", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "120000", saldos_current) - (get_saldo(formatted_nit_dv, razon_social, "240000", saldos_current) * 20 / 100)) / get_saldo(formatted_nit_dv, razon_social, "210000", saldos_current)) * 100 if get_saldo(formatted_nit_dv, razon_social, "210000", saldos_current) else 0)

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

    def safe_division(self, numerator, denominator):
        return (numerator / denominator * 100) if denominator else 0

thread_IndicadorC = threading.local()
thread_lock_IndicadorC = threading.Lock()

class BalCoopApiViewIndicadorC(APIView):
    def post(self, request):
        data = request.data
        results = []
        for item in data:
            solidaria_data = item.get("nit", {}).get("solidaria", [])
            if not solidaria_data:
                return Response([], status=status.HTTP_200_OK)
        bloques = self.dividir_en_bloques(data)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.procesar_bloque, bloque, results) for bloque in bloques]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        return Response(results)

    def dividir_en_bloques(self, datos):
        bloques = []
        for item in datos:
            bloques.append(item)
        return bloques

    def procesar_bloque(self, bloque, results):
        periodo = int(bloque.get("periodo"))
        mes_number = bloque.get("mes")
        mes_decimal = Decimal(mes_number)
        # mes = get_month_name(mes_number)

        if periodo == 2024 and mes_number == 8:
            mes = get_month_name(mes_number).capitalize()
        elif periodo == 2024 and mes_number >= 9:
            mes = get_month_name(mes_number).lower()
        else:
            mes = get_month_name(mes_number)

        base_url, campo_cuenta = self.get_api_details(periodo)
        puc_codes_current = ["141105", "141205", "144105", "144205", "141110", "141210", "144110", "144210", "141115", "141215", "144115","144215", "141120", "141220", "144120", "144220", "141125", "141225", "144125", "144225", "144805", "145505","145405", "144810", "145410", "145510", "144815", "145515", "145415", "144820", "145520", "145420", "144825","145425", "145525", "146105", "146205", "146110", "146210", "146115", "146215", "146120", "146220", "146125","146225", "140405", "140505", "140410", "140510", "140415", "140515", "140420", "140520", "140425", "140525","146905", "146930", "146910", "146935", "146915", "146940", "146920", "146945", "146925", "146950", "831000","144500", "145100", "145800", "146500", "140800", "147100", "147600", "147605", "147610", "147615", "147620","147625", "147900"]
        saldos_current = self.get_saldos(periodo, mes, campo_cuenta, puc_codes_current, base_url)
        self.process_indicators(bloque, saldos_current, results, mes_decimal, periodo, mes_number, puc_codes_current)

    def get_api_details(self, periodo):
        if periodo == 2020:
            return ("https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000", 'codcuenta')
        elif periodo == 2021:
            return ("https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000", 'codrenglon')
        else:
            return ("https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000", 'codrenglon')

    def get_saldos(self, periodo, mes, campo_cuenta, puc_codes, base_url):
        saldos = defaultdict(lambda: defaultdict(Decimal))
        puc_codes_str = ','.join(f"'{code}'" for code in puc_codes)
        url = f"{base_url}&$where=a_o='{periodo}' AND mes='{mes}' AND {campo_cuenta} IN ({puc_codes_str})"
        response = requests.get(url)
        if response.status_code == 200:
            all_data = response.json()
            for result in all_data:
                nit = result.get("nit")
                cuenta = result.get(campo_cuenta)
                valor_en_pesos = clean_currency_value_Decimal(result.get('valor_en_pesos', '0'))
                if cuenta in puc_codes:
                    saldos[nit][cuenta] += valor_en_pesos
        return saldos

    def process_indicators(self, item, saldos_current, results, mes_decimal, periodo, mes_number, puc_codes_current):
        for nit_info in item.get("nit", {}).get("solidaria", []):
            razon_social = nit_info.get("RazonSocial")
            nit = nit_info.get("nit")
            dv = nit_info.get("dv")
            formatted_nit_dv = format_nit_dv(nit, dv)
            if not any(saldos_current[formatted_nit_dv].values()):
                self.load_saldos_from_db(razon_social, saldos_current, periodo, mes_number, puc_codes_current)
            try:
                indicadores = self.calculate_indicators(formatted_nit_dv, razon_social, saldos_current)
                result_entry = {
                    "entidad_RS": razon_social,
                    "sigla": nit_info.get("sigla"),
                    "periodo": periodo,
                    "mes": mes_number,
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
            all_query_results = BalCoopModel.objects.filter(q_current_period).values('entidad_RS', 'puc_codigo', 'saldo')
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

    def calculate_indicators(self, formatted_nit_dv, razon_social, saldos_current):
        #Indicadores de Consumo
        consumo_a = (get_saldo(formatted_nit_dv, razon_social, "141105", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "141205", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144105", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144205", saldos_current))
        consumo_b = (get_saldo(formatted_nit_dv, razon_social, "141110", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "141210", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144110", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144210", saldos_current))
        consumo_c = (get_saldo(formatted_nit_dv, razon_social, "141115", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "141215", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144115", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144215", saldos_current))
        consumo_d = (get_saldo(formatted_nit_dv, razon_social, "141120", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "141220", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144120", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144220", saldos_current))
        consumo_e = (get_saldo(formatted_nit_dv, razon_social, "141125", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "141225", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144125", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "144225", saldos_current))
        consumo_total = (consumo_a + consumo_b + consumo_c + consumo_d + consumo_e)
        consumo_deterioro = get_saldo(formatted_nit_dv, razon_social, "144500", saldos_current)
        denominator_consumo_ind_mora = (consumo_a + consumo_b + consumo_c + consumo_d + consumo_e)
        consumo_ind_mora = (((consumo_b + consumo_c + consumo_d + consumo_e) / denominator_consumo_ind_mora) * 100 if denominator_consumo_ind_mora else 0)
        denominator_consumo_cartera_improductiva = (denominator_consumo_ind_mora)
        consumo_cartera_improductiva = (((consumo_c + consumo_d + consumo_e) / denominator_consumo_cartera_improductiva) * 100 if denominator_consumo_cartera_improductiva else 0)
        denominator_consumo_porc_cobertura = (consumo_b + consumo_c + consumo_d + consumo_e)
        consumo_porc_cobertura = ((consumo_deterioro / denominator_consumo_porc_cobertura) * 100 if denominator_consumo_porc_cobertura else 0)

        # Indicadores de Microcredito
        microcredito_a = (get_saldo(formatted_nit_dv, razon_social, "144805", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145505", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145405", saldos_current))
        microcredito_b = (get_saldo(formatted_nit_dv, razon_social, "144810", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145410", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145510", saldos_current))
        microcredito_c = (get_saldo(formatted_nit_dv, razon_social, "144815", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145515", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145415", saldos_current))
        microcredito_d = (get_saldo(formatted_nit_dv, razon_social, "144820", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145520", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145420", saldos_current))
        microcredito_e = (get_saldo(formatted_nit_dv, razon_social, "144825", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145425", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145525", saldos_current))
        microcredito_total = (microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e)
        microcredito_deterioro = (get_saldo(formatted_nit_dv, razon_social, "145100", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "145800", saldos_current))
        denominator_microcredito_ind_mora = (microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e)
        microcredito_ind_mora = (((microcredito_b + microcredito_c + microcredito_d + microcredito_e) / denominator_microcredito_ind_mora) * 100 if denominator_microcredito_ind_mora else 0)
        denominator_microcredito_cartera_improductiva = (denominator_microcredito_ind_mora)
        microcredito_cartera_improductiva = (((microcredito_c + microcredito_d + microcredito_e) / denominator_microcredito_cartera_improductiva) * 100 if denominator_microcredito_cartera_improductiva else 0)
        denominator_microcredito_porc_cobertura = (microcredito_b + microcredito_c + microcredito_d + microcredito_e)
        microcredito_porc_cobertura = ((microcredito_deterioro / denominator_microcredito_porc_cobertura) * 100 if denominator_microcredito_porc_cobertura else 0)

        #Indicadores de  Productos
        producto_a = (get_saldo(formatted_nit_dv, razon_social, "147605", saldos_current))
        producto_b = (get_saldo(formatted_nit_dv, razon_social, "147610", saldos_current))
        producto_c = (get_saldo(formatted_nit_dv, razon_social, "147615", saldos_current))
        producto_d = (get_saldo(formatted_nit_dv, razon_social, "147620", saldos_current))
        producto_e = (get_saldo(formatted_nit_dv, razon_social, "147625", saldos_current))
        producto_total = (producto_a + producto_b + producto_c + producto_d + producto_e)
        producto_deterioro = (get_saldo(formatted_nit_dv, razon_social, "147900", saldos_current))
        denominator_producto_ind_mora = (producto_total)
        producto_ind_mora = (((producto_b + producto_c + producto_d + producto_e) / denominator_producto_ind_mora) * 100 if denominator_producto_ind_mora else 0)
        denominator_producto_cartera_improductiva = (denominator_producto_ind_mora)
        producto_cartera_improductiva = (((producto_c + producto_d + producto_e) / denominator_producto_cartera_improductiva) * 100 if denominator_producto_cartera_improductiva else 0)
        denominator_producto_porc_cobertura = (producto_b + producto_c + producto_d + producto_e)
        producto_porc_cobertura = ((producto_deterioro / denominator_producto_porc_cobertura) * 100 if denominator_producto_porc_cobertura else 0)

        # Indicadores de Comercial
        comercial_a = (get_saldo(formatted_nit_dv, razon_social, "146105", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146205", saldos_current))
        comercial_b = (get_saldo(formatted_nit_dv, razon_social, "146110", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146210", saldos_current))
        comercial_c = (get_saldo(formatted_nit_dv, razon_social, "146115", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146215", saldos_current))
        comercial_d = (get_saldo(formatted_nit_dv, razon_social, "146120", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146220", saldos_current))
        comercial_e = (get_saldo(formatted_nit_dv, razon_social, "146125", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146225", saldos_current))
        comercial_total = (comercial_a + comercial_b + comercial_c + comercial_d + comercial_e)
        comercial_deterioro = get_saldo(formatted_nit_dv, razon_social, "146500", saldos_current)
        denominator_comercial_ind_mora = (comercial_a + comercial_b + comercial_c + comercial_d + comercial_e)
        comercial_ind_mora = (((comercial_b + comercial_c + comercial_d + comercial_e) / denominator_comercial_ind_mora) * 100 if denominator_comercial_ind_mora else 0)
        denominator_comercial_cartera_improductiva = (denominator_comercial_ind_mora)
        comercial_cartera_improductiva = (((comercial_c + comercial_d + comercial_e) / denominator_comercial_cartera_improductiva) * 100 if denominator_comercial_cartera_improductiva else 0)
        denominator_comercial_porc_cobertura = (comercial_b + comercial_c + comercial_d + comercial_e)
        comercial_porc_cobertura = ((comercial_deterioro / denominator_comercial_porc_cobertura) * 100 if denominator_comercial_porc_cobertura else 0)

        # Indicadores de Vivienda
        vivienda_a = (get_saldo(formatted_nit_dv, razon_social, "140405", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "140505", saldos_current))
        vivienda_b = (get_saldo(formatted_nit_dv, razon_social, "140410", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "140510", saldos_current))
        vivienda_c = (get_saldo(formatted_nit_dv, razon_social, "140415", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "140515", saldos_current))
        vivienda_d = (get_saldo(formatted_nit_dv, razon_social, "140420", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "140520", saldos_current))
        vivienda_e = (get_saldo(formatted_nit_dv, razon_social, "140425", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "140525", saldos_current))
        vivienda_total = (vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e)
        vivienda_deterioro = get_saldo(formatted_nit_dv, razon_social, "140800", saldos_current)
        denominator_vivienda_ind_mora = (vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e)
        vivienda_ind_mora = (((vivienda_b + vivienda_c + vivienda_d + vivienda_e) / denominator_vivienda_ind_mora) * 100 if denominator_vivienda_ind_mora else 0)
        denominator_vivienda_cartera_improductiva = (denominator_vivienda_ind_mora)
        vivienda_cartera_improductiva = (((vivienda_c + vivienda_d + vivienda_e) / denominator_vivienda_cartera_improductiva) * 100 if denominator_vivienda_cartera_improductiva else 0)
        denominator_vivienda_porc_cobertura = (vivienda_b + vivienda_c + vivienda_d + vivienda_e)
        vivienda_porc_cobertura = ((vivienda_deterioro / denominator_vivienda_porc_cobertura) * 100 if denominator_vivienda_porc_cobertura else 0)

        # Ïndicadores de Empleado
        empleados_a = (get_saldo(formatted_nit_dv, razon_social, "146905", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146930", saldos_current))
        empleados_b = (get_saldo(formatted_nit_dv, razon_social, "146910", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146935", saldos_current))
        empleados_c = (get_saldo(formatted_nit_dv, razon_social, "146915", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146940", saldos_current))
        empleados_d = (get_saldo(formatted_nit_dv, razon_social, "146920", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146945", saldos_current))
        empleados_e = (get_saldo(formatted_nit_dv, razon_social, "146925", saldos_current) + get_saldo(formatted_nit_dv, razon_social, "146950", saldos_current))
        empleados_total = (empleados_a + empleados_b + empleados_c + empleados_d + empleados_e)
        empleados_deterioro = get_saldo(formatted_nit_dv, razon_social, "147100", saldos_current)
        denominator_empleados_ind_mora = (empleados_a + empleados_b + empleados_c + empleados_d + empleados_e)
        empleados_ind_mora = (((empleados_b + empleados_c + empleados_d + empleados_e) / denominator_empleados_ind_mora) * 100 if denominator_empleados_ind_mora else 0)
        denominator_empleados_cartera_improductiva = (denominator_empleados_ind_mora)
        empleados_cartera_improductiva = (((empleados_c + empleados_d + empleados_e) / denominator_empleados_cartera_improductiva) * 100 if denominator_empleados_cartera_improductiva else 0)
        denominator_empleados_porc_cobertura = (empleados_b + empleados_c + empleados_d + empleados_e)
        empleados_porc_cobertura = ((empleados_deterioro / denominator_empleados_porc_cobertura) * 100 if denominator_empleados_porc_cobertura else 0)

        # Indicador de Total General

        total_a = (consumo_a + microcredito_a + comercial_a + vivienda_a + empleados_a + producto_a)
        total_b = (consumo_b + microcredito_b + comercial_b + vivienda_b + empleados_b + producto_b)
        total_c = (consumo_c + microcredito_c + comercial_c + vivienda_c + empleados_c + producto_c)
        total_d = (consumo_d + microcredito_d + comercial_d + vivienda_d + empleados_d + producto_d)
        total_e = (consumo_e + microcredito_e + comercial_e + vivienda_e + empleados_e + producto_e)
        total_castigos = get_saldo(formatted_nit_dv, razon_social, "831000", saldos_current)
        total_total = (total_a + total_b + total_c + total_d + total_e)
        total_deterioro = (consumo_deterioro + microcredito_deterioro + comercial_deterioro + vivienda_deterioro + empleados_deterioro + producto_deterioro)
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
            "productoA": producto_a,
            "productoB": producto_b,
            "productoC": producto_c,
            "productoD": producto_d,
            "productoE": producto_e,
            "productoTotal": producto_total,
            "productoIndMora": producto_ind_mora,
            "productoCartImprod": producto_cartera_improductiva,
            "productooDeterioro": producto_deterioro,
            "productoPorcCobertura": producto_porc_cobertura,
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

    def safe_division(self, numerator, denominator):
        return (numerator / denominator * 100) if denominator else 0

class BalCoopApiViewBalanceCuenta(APIView):
    def get_saldos_solidaria(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            for result in response.json():
                nit = result.get("nit")
                valor_en_pesos = result.get('valor_en_pesos', '$ 0')
                total_saldo = clean_currency_value_Decimal(valor_en_pesos)
                yield nit, total_saldo
        else:
            print("No data fetched from API.")
    
    def get_saldos_locales(self, entidades_Solidaria, periodo, mes, pucCodigo):
        for nit_info in entidades_Solidaria:
            razon_social = nit_info.get("RazonSocial")
            q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo=pucCodigo)

            query_results_current = BalCoopModel.objects.filter(q_current_period).values("puc_codigo", "saldo")

            for result in query_results_current:
                yield razon_social, result["saldo"]
                break

    def post(self, request):
        data = request.data
        entidades_Solidaria = data.get("entidad", {}).get("solidaria", [])
        periodo = data.get("año")
        mes = data.get("mes")
        # mes_str = get_month_name(mes)

        if periodo == 2024 and mes == 8:
            mes_str = get_month_name(mes).capitalize()
        elif periodo == 2024 and mes >= 9:
            mes_str = get_month_name(mes).lower()
        else:
            mes_str = get_month_name(mes)

        pucCodigo = data.get("pucCodigo")
        pucName = data.get("pucName")
        results = []

        if periodo == 2020:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000"
            campoCuenta = 'codcuenta'
        elif periodo == 2021:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000"
            campoCuenta = 'codrenglon'
        else:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000"
            campoCuenta = 'codrenglon'

        url_solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND mes='{mes_str}' AND {campoCuenta}='{pucCodigo}'"

        saldos_current = defaultdict(Decimal)
        for nit, total_saldo in self.get_saldos_solidaria(url_solidaria):
            saldos_current[nit] += total_saldo

        for razon_social, total_saldo in self.get_saldos_locales(entidades_Solidaria, periodo, mes, pucCodigo):
            if razon_social not in saldos_current:
                saldos_current[razon_social] = total_saldo 

        entidades = []
        for nit_info in entidades_Solidaria:
            razon_social = nit_info.get("RazonSocial")
            nit = nit_info.get("nit")
            dv = nit_info.get("dv")
            formatted_nit_dv = format_nit_dv(nit, dv)
            saldo = saldos_current.get(formatted_nit_dv, saldos_current.get(razon_social, 0))
            entidades.append({
                "nit": nit_info.get("nit"),
                "sigla": nit_info.get("sigla"),
                "RazonSocial": razon_social,
                "saldo": saldo
            })

        results.append({
            "año": periodo,
            "mes": mes,
            "puc_codigo": pucCodigo,
            "pucName": pucName,
            "entidades": entidades
        })

        return Response(data=results, status=status.HTTP_200_OK)

class BalCoopApiViewBalanceIndependiente(APIView):
    def post(self, request):
        data = request.data
        entidades_Solidaria = data.get("entidad", {}).get("solidaria", [])
        periodo = data.get("año")
        mes = data.get("mes")
        # mes_str = get_month_name(mes)

        if periodo == 2024 and mes == 8:
            mes_str = get_month_name(mes).capitalize()
        elif periodo == 2024 and mes >= 9:
            mes_str = get_month_name(mes).lower()
        else:
            mes_str = get_month_name(mes)

        nit = entidades_Solidaria[0].get("nit")
        dv = entidades_Solidaria[0].get("dv")
        formatted_nit_dv = format_nit_dv(nit, dv)
        results = []

        if periodo == 2020:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000"
            campoCuenta = 'codcuenta'
        elif periodo == 2021:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000"
            campoCuenta = 'codrenglon'
        else:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000"
            campoCuenta = 'codrenglon'

        def obtener_saldos_api():
            url_solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND mes='{mes_str}' AND nit='{formatted_nit_dv}'"
            response_solidaria = requests.get(url_solidaria)
            if response_solidaria.status_code == 200:
                all_data = response_solidaria.json()
                if all_data:  # Si hay datos, los retornamos
                    for result in all_data:
                        nit = result.get("nit")
                        cuenta = result.get(campoCuenta)
                        nombreCuenta = result.get("nombre_cuenta")
                        valor_en_pesos = result.get('valor_en_pesos', '$ 0')
                        valor = clean_currency_value_Decimal(valor_en_pesos)
                        yield {
                            "nit": nit,
                            "cuenta": cuenta,
                            "nombreCuenta": nombreCuenta,
                            "valor": valor
                        }
            else:
                print(f"Error al obtener datos de la API. Status code: {response_solidaria.status_code}")
            return []  

        def obtener_saldos_db():
            for nit_info in entidades_Solidaria:
                razon_social = nit_info.get("RazonSocial")
                q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes)
                query_results_current = BalCoopModel.objects.filter(q_current_period).values("entidad_RS", "puc_codigo", "saldo")

                for result in query_results_current:
                    nombreCuenta = PucCoopModel.objects.filter(Codigo=result["puc_codigo"]).first()
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

        for entidad in entidades_Solidaria:
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