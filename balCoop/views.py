import logging
import requests
from django.db.models import Sum
from django.db import transaction
from django.db.models import Q
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from collections import defaultdict

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from rest_framework.decorators import authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.authentication import JWTAuthentication

from balCoop.models import BalCoopModel
from balCoop.serializers import BalCoopSerializer


logger = logging.getLogger("django")

def format_nit_dv(nit, dv):
    """
    Formatea el NIT y el DV en el formato 'NNN-NNN-NNN-N'.
    """
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

def clean_currency_value(value):
    """
    Limpia el valor monetario, eliminando el símbolo de moneda y las comas,
    y convirtiéndolo a un número Decimal.
    """
    return Decimal(value.replace('$', '').replace(',', '').strip())

def clean_currency_value_Decimal(value):
    """
    Limpia el valor monetario, eliminando el símbolo de moneda y las comas,
    y convirtiéndolo a un número Decimal.
    """
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

@authentication_classes([JWTAuthentication])
@permission_classes([IsAuthenticated])

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

def get_saldo(nit_dv, razon_social, cuenta, saldos):
    return saldos.get(nit_dv, {}).get(cuenta, saldos.get(razon_social, {}).get(cuenta, 0))


class BalCoopApiViewA(APIView):
    def post(self, request):
        data = request.data
        transformed_results = {}

        def get_saldo_from_db(razon_social, periodo, puc_codigo, mes):
            q_objects = Q(entidad_RS=razon_social, periodo=periodo, puc_codigo=puc_codigo, mes=mes)
            return BalCoopModel.objects.filter(q_objects).values("periodo", "mes", "saldo")

        for item in data:
            periodo = int(item.get("periodo")) 
            mes_number = item.get("mes")
            mes = get_month_name(mes_number)
            puc_codigo = item.get("puc_codigo")

            print(f"Buscando datos para:  PUC Código: {puc_codigo}, Periodo: {periodo}, Mes: {mes}")

            baseUrl_entidadesSolidaria, campoCuenta = self.get_api_params(periodo)
            
            url_solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND mes='{mes}' AND {campoCuenta}='{puc_codigo}'"
            response_otras = requests.get(url_solidaria)

            all_data = response_otras.json() if response_otras.status_code == 200 else []

            for nit_info in item.get("nit", {}).get("solidaria", []):
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
                    print(f"Se encontraron {len(entity_data)} registros para la entidad {razon_social}.")
                    for result in entity_data:
                        saldo = clean_currency_value_Decimal(result.get('valor_en_pesos', '$ 0'))
                        transformed_results[key]["saldos"].append({"periodo": periodo, "mes": mes_number, "saldo": saldo})
                else:
                    print(f"No se encontraron registros en la API para la entidad {razon_social}, buscando en la base de datos.")
                    query_results = get_saldo_from_db(razon_social, periodo, puc_codigo, mes_number)
                    if query_results:
                        print(f"Se encontró saldo en la base de datos para {razon_social}.")
                        # Solo tomar el primer resultado si hay más de uno
                        first_result = query_results[0]
                        transformed_results[key]["saldos"].append(
                            {"periodo": first_result["periodo"], "mes": first_result["mes"], "saldo": float(first_result["saldo"])}
                        )
                        saldo_en_bd = True

                if not entity_data and not saldo_en_bd:
                    print(f"No se encontraron datos para {razon_social}, saldo asignado: 0.0")
                    transformed_results[key]["saldos"].append({"periodo": periodo, "mes": mes_number, "saldo": 0})

        final_results = list(transformed_results.values())
        return Response(data=final_results, status=status.HTTP_200_OK)

    def get_api_params(self, periodo):
        if periodo == 2020:
            return ("https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=500000", 'codcuenta')
        elif periodo == 2021:
            return ("https://www.datos.gov.co/resource/irgu-au8v.json?$limit=500000", 'codrenglon')
        else:
            return ("https://www.datos.gov.co/resource/tic6-rbue.json?$limit=500000", 'codrenglon')

class BalCoopApiViewIndicador(APIView):
    def post(self, request):
        # print(f"(●ˇ∀ˇ●)"*25)
        # print(f"Inicio de BalCoopApiViewIndicador")
        # print(f"(●ˇ∀ˇ●)"*25)

        data = request.data
        results = []
        periodo_anterior = None
        mes_anterior = None

        puc_codes_current = ["100000", "110000", "120000", "140000", "210000", "230000", "240000", "300000", "310000", "311010", "320000", "330500", "340500", "350000", "415000", "615005", "615010", "615015", "615020", "615035"]

        puc_codes_prev = ["100000", "140000", "210000", "230000", "300000"]

        for item in data:
            periodo = int(item.get("periodo"))
            mes_number = item.get("mes")
            mes_decimal = Decimal(mes_number)
            mes = get_month_name(mes_number)

            # Obtiene URL y campo de cuenta según el periodo
            base_url, campo_cuenta = self.get_api_details(periodo)

            # Obtiene saldos del periodo actual
            saldos_current = self.get_saldos(periodo, mes, campo_cuenta, puc_codes_current, base_url)
            # print(f"Datos del periodo actual ({periodo}, mes {mes}): {len(saldos_current)} registros.")

            # Manejo de datos del periodo anterior
            periodo_anterior_actual = periodo - 1
            mes_ultimo_str = get_month_name(12)
            base_url_prev, campo_cuenta_prev = self.get_api_details(periodo_anterior_actual)

            if periodo_anterior != periodo_anterior_actual or mes_anterior != mes_ultimo_str:
                saldos_previous = self.get_saldos(periodo_anterior_actual, mes_ultimo_str, campo_cuenta_prev, puc_codes_prev, base_url_prev, previous=True)
                # print(f"Datos del periodo anterior ({periodo_anterior_actual}, mes {mes_ultimo_str}): {len(saldos_previous)} registros.")

                periodo_anterior, mes_anterior = periodo_anterior_actual, mes_ultimo_str

            # Procesa indicadores
            self.process_indicators(item, saldos_current, saldos_previous, results, mes_decimal, periodo, periodo_anterior_actual, mes_number, puc_codes_current, puc_codes_prev)

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
        
        response = requests.get(url)
        if response.status_code == 200:
            all_data = response.json()
            print(f"Recibidos {len(all_data)} registros de la API para el periodo {periodo} y mes {mes}.")
            for result in all_data:
                nit = result.get("nit")
                cuenta = result.get(campo_cuenta)
                valor_en_pesos = clean_currency_value_Decimal(result.get('valor_en_pesos', '0'))
                if cuenta in puc_codes:
                    saldos[nit][cuenta] += valor_en_pesos
        
        return saldos   

    def process_indicators(self, item, saldos_current, saldos_previous, results, mes_decimal, periodo, periodo_anterior_actual, mes_number, puc_codes_current, puc_codes_prev):
        for nit_info in item.get("nit", {}).get("solidaria", []):
            razon_social = nit_info.get("RazonSocial")
            nit = nit_info.get("nit")
            dv = nit_info.get("dv")
            formatted_nit_dv = format_nit_dv(nit, dv)

            # Solo cargar desde la base de datos si saldos_current está vacío
            if not any(saldos_current[formatted_nit_dv].values()):
                # print(f"Cargando saldos desde la base de datos para: {razon_social}")
                self.load_saldos_from_db(razon_social, saldos_current, periodo, mes_number, puc_codes_current)
            # else:
                # print(f"Datos encontrados en la API para: {razon_social}, no se carga desde la base de datos.")

            if not any(saldos_previous[formatted_nit_dv].values()):
                # print(f"Cargando saldos desde la base de datos para: {razon_social}")
                self.load_saldos_from_db(razon_social, saldos_previous,periodo_anterior_actual , 12, puc_codes_prev)
            # else:
                # print(f"Datos encontrados en la API para: {razon_social}, no se carga desde la base de datos.")

            try:
                indicadores = self.calculate_indicators(formatted_nit_dv, razon_social, saldos_current, saldos_previous, mes_decimal)
                
                result_entry = {
                    "entidad_RS": razon_social,
                    "sigla": nit_info.get("sigla"),
                    "periodo": periodo,
                    "mes": mes_number,
                    **indicadores 
                }
                results.append(result_entry)
            except Exception as e:
                print(f"Error en cálculo de indicadores: {e}")

    def load_saldos_from_db(self, razon_social, saldos, periodo, mes, puc_codes):
        # Construimos la consulta para obtener los registros correspondientes al periodo, mes y códigos de PUC
        q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes)
        
        # Realizamos la consulta para obtener los distintos "puc_codigo" y el primer saldo asociado
        query_results = BalCoopModel.objects.filter(q_current_period).values("puc_codigo")
        
        print(f"Cargando {query_results.count()} registros de la base de datos para {razon_social}.")
        
        # Iteramos sobre los resultados para obtener el primer saldo de cada "puc_codigo"
        for result in query_results.iterator():
            # Obtenemos el primer saldo para el "puc_codigo" correspondiente
            first_saldo = BalCoopModel.objects.filter(q_current_period, puc_codigo=result["puc_codigo"]).first()
            
            # Si encontramos un saldo, lo asignamos
            if first_saldo:
                saldos[razon_social][result["puc_codigo"]] = first_saldo.saldo

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

class BalCoopApiViewIndicadorC(APIView):
    def post(self, request):
        print(f"(●ˇ∀ˇ●)"*25)
        print(f"Inicio de BalCoopApiViewIndicadorCartera")
        print(f"(●ˇ∀ˇ●)"*25)

        data = request.data
        results = []

        puc_codes_current = ["141105","141205","144105","144205","141110","141210","144110","144210","141115","141215","144115","144215","141120","141220","144120","144220","141125","141225","144125","144225","144805","145505","145405","144810","145410","145510","144815","145515","145415","144820","145520","145420","144825","145425","145525","146105","146205","146110","146210","146115","146215","146120","146220","146125","146225","140405","140505","140410","140510","140415","140515","140420","140520","140425","140525","146905","146930","146910","146935","146915","146940","146920","146945","146925","146950","831000","144500","145100","145800","146500","140800","147100", "147600", "147605", "147610", "147615", "147620" , "147625", "147900"]

        for item in data:
            periodo = int(item.get("periodo"))
            mes_number = item.get("mes")
            mes_decimal = Decimal(mes_number)
            mes = get_month_name(mes_number)

            # Obtiene URL y campo de cuenta según el periodo
            base_url, campo_cuenta = self.get_api_details(periodo)

            # Obtiene saldos del periodo actual
            saldos_current = self.get_saldos(periodo, mes, campo_cuenta, puc_codes_current, base_url)
            print(f"Datos del periodo actual ({periodo}, mes {mes}): {len(saldos_current)} registros.")

            self.process_indicators(item, saldos_current, results, mes_decimal, periodo, mes_number, puc_codes_current)

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
        
        response = requests.get(url)
        if response.status_code == 200:
            all_data = response.json()
            print(f"Recibidos {len(all_data)} registros de la API para el periodo {periodo} y mes {mes}.")
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

            # Solo cargar desde la base de datos si saldos_current está vacío
            if not any(saldos_current[formatted_nit_dv].values()):
                # print(f"Cargando saldos desde la base de datos para: {razon_social}")
                self.load_saldos_from_db(razon_social, saldos_current, periodo, mes_number, puc_codes_current)
            # else:
                # print(f"Datos encontrados en la API para: {razon_social}, no se carga desde la base de datos.")

            try:
                indicadores = self.calculate_indicators(formatted_nit_dv, razon_social, saldos_current)
                
                result_entry = {
                    "entidad_RS": razon_social,
                    "sigla": nit_info.get("sigla"),
                    "periodo": periodo,
                    "mes": mes_number,
                    **indicadores 
                }
                results.append(result_entry)
            except Exception as e:
                print(f"Error en cálculo de indicadores: {e}")

    def load_saldos_from_db(self, razon_social, saldos_current, periodo, mes_number, puc_codes):
        q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes_number) & Q(puc_codigo__in=puc_codes)
        query_results_current = BalCoopModel.objects.filter(q_current_period).values("puc_codigo")
        print(f"Cargando {query_results_current.count()} registros de la base de datos para {razon_social}.")
        for result in query_results_current:
            first_saldo = BalCoopModel.objects.filter(q_current_period, puc_codigo=result["puc_codigo"]).first()
            if first_saldo:
                saldos_current[razon_social][result["puc_codigo"]] = first_saldo.saldo

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
        total_e = (consumo_e + microcredito_e + comercial_e + vivienda_e + empleados_e + comercial_e)
        total_castigos = get_saldo(formatted_nit_dv, razon_social, "831000", saldos_current)
        total_total = (consumo_total + microcredito_total + comercial_total + vivienda_total + empleados_total + producto_total)
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

"""
# class BalCoopApiViewBalanceCuenta(APIView):
#     def post(self, request):
#         data = request.data
#         entidades_Solidaria = data.get("entidad", {}).get("solidaria", [])
#         periodo = data.get("año")
#         mes = data.get("mes")
#         mes_str = get_month_name(mes)
#         pucCodigo = data.get("pucCodigo")
#         pucName = data.get("pucName")
#         results = []

#         if periodo == 2020:
#             baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000"
#             campoCuenta = 'codcuenta'
#         elif periodo == 2021:
#             baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000"
#             campoCuenta = 'codrenglon'
#         else:
#             baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000"
#             campoCuenta = 'codrenglon'

#         url_solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND mes='{mes_str}' AND {campoCuenta}='{pucCodigo}'"
#         response_Solidaria = requests.get(url_solidaria)

#         saldos_current = defaultdict(Decimal)
#         if response_Solidaria.status_code == 200:
#             all_data = response_Solidaria.json()

#             for result in all_data:
#                 nit = result.get("nit")
#                 valor_en_pesos = result.get('valor_en_pesos', '$ 0')
#                 total_saldo = clean_currency_value_Decimal(valor_en_pesos)
#                 saldos_current[nit] += total_saldo
#         else:
#             print("No data fetched from API.")

#         for nit_info in entidades_Solidaria:
#             razon_social = nit_info.get("RazonSocial")
#             if razon_social not in saldos_current:
#                 q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo=pucCodigo)
#                 query_results_current = BalCoopModel.objects.filter(q_current_period).values("puc_codigo").annotate(total_saldo=Sum("saldo"))

#                 for result in query_results_current:
#                     saldos_current[razon_social] += result["total_saldo"]

#         entidades = []
#         for nit_info in entidades_Solidaria:
#             razon_social = nit_info.get("RazonSocial")
#             nit = nit_info.get("nit")
#             dv = nit_info.get("dv")
#             formatted_nit_dv = format_nit_dv(nit, dv)
#             saldo = saldos_current.get(formatted_nit_dv, saldos_current.get(razon_social, 0))
#             entidades.append({
#                 "nit": nit_info.get("nit"),
#                 "sigla": nit_info.get("sigla"),
#                 "RazonSocial": razon_social,
#                 "saldo": saldo
#             })
#         results.append({
#             "año": periodo,
#             "mes": mes,
#             "puc_codigo": pucCodigo,
#             "pucName": pucName,
#             "entidades": entidades
#         })

#         return Response(data=results, status=status.HTTP_200_OK)
"""

class BalCoopApiViewBalanceCuenta(APIView):
    def get_saldos_solidaria(self, url):
        """Generador para obtener saldos de la API y evitar cargar todo en memoria."""
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
        mes_str = get_month_name(mes)
        pucCodigo = data.get("pucCodigo")
        pucName = data.get("pucName")
        results = []

        # Determinar URL y campo según el año
        if periodo == 2020:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000"
            campoCuenta = 'codcuenta'
        elif periodo == 2021:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000"
            campoCuenta = 'codrenglon'
        else:
            baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000"
            campoCuenta = 'codrenglon'

        # Construir la URL para la consulta
        url_solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND mes='{mes_str}' AND {campoCuenta}='{pucCodigo}'"

        # Procesar saldos desde la API con generador
        saldos_current = defaultdict(Decimal)
        for nit, total_saldo in self.get_saldos_solidaria(url_solidaria):
            saldos_current[nit] += total_saldo

        # Procesar saldos locales con generador
        for razon_social, total_saldo in self.get_saldos_locales(entidades_Solidaria, periodo, mes, pucCodigo):
            if razon_social not in saldos_current:
                saldos_current[razon_social] = total_saldo 

        # Preparar el resultado final
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
        
        # Formar la respuesta final
        results.append({
            "año": periodo,
            "mes": mes,
            "puc_codigo": pucCodigo,
            "pucName": pucName,
            "entidades": entidades
        })

        return Response(data=results, status=status.HTTP_200_OK)
