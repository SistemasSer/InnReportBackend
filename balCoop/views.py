import logging
from django.db.models import Sum
from django.db import transaction
from django.db.models import Q
from decimal import Decimal

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


class BalCoopApiView(APIView):
    def get(self, request):
        serializer = BalCoopSerializer(BalCoopModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    
    def post(self, request):
        data_list = request.data.get('extractedData', [])
        is_staff = request.data.get('isStaff', False)
        
        serializer = BalCoopSerializer(data=data_list, many=True)
        serializer.is_valid(raise_exception=True)
        
        existing_instances = BalCoopModel.objects.filter(
            Q(periodo__in=[data['periodo'] for data in data_list]) &
            Q(mes__in=[data['mes'] for data in data_list]) &
            Q(entidad_RS__in=[data['entidad_RS'] for data in data_list]) &
            Q(puc_codigo__in=[data['puc_codigo'] for data in data_list])
        )
        
        existing_dict = {
            (instance.periodo, instance.mes, instance.entidad_RS, instance.puc_codigo): instance
            for instance in existing_instances
        }
        
        new_instances = []
        update_instances = []
        errors = set()
        
        for data in data_list:
            key = (data['periodo'], data['mes'], data['entidad_RS'], data['puc_codigo'])
            if key in existing_dict:
                instance = existing_dict[key]
                
                if is_staff or not all(getattr(instance, field) == data[field] for field in data):
                    fields_to_update = []
                    for attr, value in data.items():
                        if getattr(instance, attr) != value:
                            setattr(instance, attr, value)
                            fields_to_update.append(attr)
                    update_instances.append((instance, fields_to_update))
                else:
                    error_message = f"Datos ya existentes para periodo {data['periodo']}, mes {data['mes']}, entidad_RS {data['entidad_RS']}."
                    errors.add(error_message)
                    # Si hay un error, detén el procesamiento y devuelve la respuesta
                    if errors:
                        return Response(status=status.HTTP_400_BAD_REQUEST, data={"errors": list(errors)})
            else:
                if is_staff:
                    new_instances.append(BalCoopModel(**data))
                else:
                    if not BalCoopModel.objects.filter(
                        periodo=data['periodo'],
                        mes=data['mes'],
                        entidad_RS=data['entidad_RS'],
                        puc_codigo=data['puc_codigo']
                    ).exists():
                        new_instances.append(BalCoopModel(**data))
                    else:
                        error_message = f"Datos ya existentes: {data['entidad_RS']} Año{data['periodo']} - Mes {data['mes']},  ."
                        errors.add(error_message)
                        # Si hay un error, detén el procesamiento y devuelve la respuesta
                        if errors:
                            return Response(status=status.HTTP_400_BAD_REQUEST, data={"errors": list(errors)})

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


"""
class BalCoopApiViewA(APIView):
    def post(self, request):
        data = request.data
        q_objects = Q() 
        found_valid_nits = False

        # Initialize transformed results with specified months, setting saldo as 0
        transformed_results = {}
        for item in data:
            for nit_info in item.get("nit", {}).get("solidaria", []):
                nit = nit_info.get("nit")
                sigla = nit_info.get("sigla")
                RazonSocial = nit_info.get("RazonSocial")
                periodo = item.get("periodo")
                puc_codigo = item.get("puc_codigo")
                mes = item.get("mes")

                key = (nit, puc_codigo)
                if key not in transformed_results:
                    transformed_results[key] = {
                        'entidad_nit': nit,
                        'sigla': sigla,
                        'RazonSocial': RazonSocial,
                        'puc_codigo': puc_codigo,
                        'saldos': []
                    }
                transformed_results[key]['saldos'].append({
                    'periodo': periodo,
                    'mes': mes,
                    'saldo': 0
                })

                # Add conditions to the query for each NIT
                q_objects |= Q(
                    entidad_nit=nit,
                    RazonSocial=RazonSocial,
                    periodo=periodo,
                    puc_codigo=puc_codigo,
                    mes=mes
                )
                found_valid_nits = True

        if not found_valid_nits:
            return Response(data=[], status=status.HTTP_200_OK)

        query_results = BalCoopModel.objects.filter(q_objects).values('entidad_nit', 'periodo', 'puc_codigo', 'mes', 'saldo')

        # Update saldos based on query results
        for result in query_results:
            key = (result['entidad_nit'], result['puc_codigo'])
            for saldo_info in transformed_results[key]['saldos']:
                if saldo_info['periodo'] == result['periodo'] and saldo_info['mes'] == result['mes']:
                    saldo_info['saldo'] = result['saldo']

        # Convert the dictionary to a list
        final_results = list(transformed_results.values())

        return Response(data=final_results, status=status.HTTP_200_OK)
"""
# views.py


class BalCoopApiViewA(APIView):
    def post(self, request):
        data = request.data
        # logger.debug("Datos recibidos: %s", data)

        q_objects = Q()
        found_valid_entries = False

        transformed_results = {}
        for item in data:
            for nit_info in item.get("nit", {}).get("solidaria", []):
                razon_social = nit_info.get("RazonSocial")
                sigla = nit_info.get("sigla")
                periodo = item.get("periodo")
                puc_codigo = item.get("puc_codigo")
                mes = item.get("mes")

                key = (razon_social, puc_codigo)
                if key not in transformed_results:
                    transformed_results[key] = {
                        "razon_social": razon_social,
                        "sigla": sigla,
                        "puc_codigo": puc_codigo,
                        "saldos": [],
                    }
                transformed_results[key]["saldos"].append(
                    {"periodo": periodo, "mes": mes, "saldo": 0}
                )

                q_objects |= Q(
                    entidad_RS=razon_social,
                    periodo=periodo,
                    puc_codigo=puc_codigo,
                    mes=mes,
                )
                found_valid_entries = True

        if not found_valid_entries:
            return Response(data=[], status=status.HTTP_200_OK)

        query_results = BalCoopModel.objects.filter(q_objects).values(
            "entidad_RS", "periodo", "puc_codigo", "mes", "saldo"
        )

        # logger.debug("Resultados de la consulta solidaria: %s", query_results)

        for result in query_results:
            key = (result["entidad_RS"], result["puc_codigo"])
            if key in transformed_results:
                for saldo_info in transformed_results[key]["saldos"]:
                    if (
                        saldo_info["periodo"] == result["periodo"]
                        and saldo_info["mes"] == result["mes"]
                    ):
                        saldo_info["saldo"] = result["saldo"]

        final_results = list(transformed_results.values())

        # logger.debug("Resultados finales transformados solidaria: %s", final_results)

        return Response(data=final_results, status=status.HTTP_200_OK)


"""
class BalCoopApiViewIndicador(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            
            for nit_info in item.get("nit", {}).get("solidaria", []):
                nit = nit_info.get("nit")
                periodo = item.get("periodo")
                periodo_anterior = periodo - 1
                mes_12 = 12
                mes = item.get("mes")
                mes_decimal = Decimal(mes)


                puc_codes_current = ['100000','110000','120000','140000', '210000','230000','240000','300000','310000', '311010', '320000','330500','340500','350000','415000','615005','615010', '615015','615020','615035']
                puc_codes_previous = ['100000','140000','210000','300000','230000']

                q_current_period = Q(entidad_nit=nit, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)
                q_previous_period = Q(entidad_nit=nit, periodo=periodo_anterior, mes=mes_12) & Q(puc_codigo__in=puc_codes_previous)

                query_results_current = BalCoopModel.objects.filter(q_current_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))
                query_results_previous = BalCoopModel.objects.filter(q_previous_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))

                saldos_current = {puc: 0 for puc in puc_codes_current}
                for result in query_results_current:
                    saldos_current[result['puc_codigo']] = result['total_saldo']

                saldos_previous = {puc: 0 for puc in puc_codes_previous}
                indicador_roe = indicador_roa = indicador_ingreso_cartera = indicador_credito_banco = 0

                for result in query_results_previous:
                    saldos_previous[result['puc_codigo']] = result['total_saldo']

                indicador_cartera = (saldos_current['140000'] / saldos_current['100000'])*100 if saldos_current['100000'] else 0
                indicador_deposito = (saldos_current['210000'] / saldos_current['100000'])*100 if saldos_current['100000'] else 0
                indicador_obligaciones = (saldos_current['230000'] / saldos_current['100000'])*100 if saldos_current['100000'] else 0
                indicador_cap_social = (saldos_current['310000'] / saldos_current['100000'])*100 if saldos_current['100000'] else 0
                indicador_cap_inst = ((saldos_current['311010'] + saldos_current['320000'] + saldos_current['330500'] + saldos_current['340500']) / saldos_current['100000'])*100 if saldos_current['100000'] else 0

                denominator_roe = (saldos_previous['300000'] + (saldos_current['300000'] / mes_decimal) * 12) / 2
                indicador_roe = (saldos_current['350000'] / denominator_roe) * 100 if denominator_roe else 0

                denominator_roa = (saldos_previous['100000']+(saldos_current['100000']/mes_decimal)*12)/2
                indicador_roa = (saldos_current['350000']/denominator_roa) * 100 if denominator_roa else 0

                denominator_ingreso_cartera = (saldos_previous['140000']+(saldos_current['140000']/mes_decimal)*12)/2
                indicador_ingreso_cartera = (saldos_current['415000']/denominator_ingreso_cartera)*100 if denominator_ingreso_cartera else 0

                denominator_costos_deposito = (saldos_previous['210000']+(saldos_current['210000']/mes_decimal)*12)/2
                indicador_costos_deposito = ((saldos_current['615005'] + saldos_current['615010'] + saldos_current['615015'] + saldos_current['615020'])/denominator_costos_deposito)*100  if denominator_costos_deposito else 0
                denominator_credito_banco = (saldos_previous['230000']+(saldos_current['230000']/mes_decimal)*12)/2
                indicador_credito_banco = (saldos_current['615035']/denominator_credito_banco)*100 if denominator_credito_banco else 0

                indicador_disponible = ((saldos_current['110000'] + saldos_current['120000'] - (saldos_current['240000']*20/100)) / saldos_current['210000'])*100 if saldos_current['210000'] else 0

                results.append({
                    'entidad_nit': nit,
                    'sigla': nit_info.get("sigla"),
                    'periodo': periodo,
                    'mes': mes,
                    'indicadorCartera': indicador_cartera,
                    'indicadorDeposito': indicador_deposito,
                    'indicadorObligaciones': indicador_obligaciones,
                    'indicadorCapSocial': indicador_cap_social,
                    'indicadorCapInst': indicador_cap_inst,                    
                    'indicadorRoe': indicador_roe,
                    'indicadorRoa': indicador_roa,
                    'indicadorIngCartera': indicador_ingreso_cartera,
                    'indicadorCostDeposito': indicador_costos_deposito,
                    'indicadorCredBanco': indicador_credito_banco,
                    'indicadorDisponible': indicador_disponible,
                })

        sorted_results = sorted(results, key=lambda x: x['entidad_nit'])

        return Response(data=sorted_results, status=status.HTTP_200_OK)
"""
"""
class BalCoopApiViewIndicadorC(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            for nit_info in item.get("nit", {}).get("solidaria", []):
                nit = nit_info.get("nit")
                periodo = item.get("periodo")
                mes = item.get("mes")

                puc_codes_current = ['141105', '141205', '144105', '144205', 
                                        '141110', '141210', '144110', '144210', 
                                        '141115', '141215', '144115', '144215', 
                                        '141120', '141220', '144120', '144220', 
                                        '141125', '141225', '144125', '144225', 
                                        '144805', '145505', '145405', 
                                        '144810', '145410', '145510', 
                                        '144815', '145515', '145415', 
                                        '144820', '145520', '145420', 
                                        '144825', '145425', '145525', 
                                        '146105', '146205', 
                                        '146110', '146210',
                                        '146115', '146215', 
                                        '146120', '146220', 
                                        '146125', '146225', 
                                        '140405', '140505', 
                                        '140410', '140510', 
                                        '140415', '140515', 
                                        '140420', '140520', 
                                        '140425', '140525', 
                                        '146905', '146930', 
                                        '146910', '146935', 
                                        '146915', '146940', 
                                        '146920', '146945', 
                                        '146925', '146950',
                                        '831000', '144500', '145100','145800','146500','140800','147100'
                                    ]

                q_current_period = Q(entidad_nit=nit, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)

                query_results_current = BalCoopModel.objects.filter(q_current_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))

                saldos_current = {puc: 0 for puc in puc_codes_current}

                consumo_a = consumo_b = consumo_c = consumo_d = consumo_e = consumo_total = consumo_deterioro = 0
                denominator_consumo_ind_mora = denominator_consumo_cartera_improductiva = denominator_consumo_porc_cobertura = 0
                consumo_ind_mora = consumo_cartera_improductiva = consumo_porc_cobertura = 0

                microcredito_a = microcredito_b = microcredito_c = microcredito_d = microcredito_e = microcredito_total = microcredito_deterioro = 0
                denominator_microcredito_ind_mora = denominator_microcredito_cartera_improductiva = denominator_microcredito_porc_cobertura = 0
                microcredito_ind_mora = microcredito_cartera_improductiva = microcredito_porc_cobertura = 0

                comercial_a = comercial_b = comercial_c = comercial_d = comercial_e = comercial_total = comercial_deterioro = 0
                denominator_comercial_ind_mora = denominator_comercial_cartera_improductiva = denominator_comercial_porc_cobertura = 0
                comercial_ind_mora = comercial_cartera_improductiva = comercial_porc_cobertura = 0

                vivienda_a = vivienda_b = vivienda_c = vivienda_d = vivienda_e = vivienda_total = vivienda_deterioro = 0
                denominator_vivienda_ind_mora = denominator_vivienda_cartera_improductiva = denominator_vivienda_porc_cobertura = 0
                vivienda_ind_mora = vivienda_cartera_improductiva = vivienda_porc_cobertura = 0

                empleados_a = empleados_b = empleados_c = empleados_d = empleados_e = empleados_total = empleados_deterioro = 0
                denominator_empleados_ind_mora = denominator_empleados_cartera_improductiva = denominator_empleados_porc_cobertura = 0
                empleados_ind_mora = empleados_cartera_improductiva = empleados_porc_cobertura = 0

                total_a = total_b = total_c = total_d = total_e = total_total = total_deterioro = 0
                denominator_total_ind_mora = total_castigos = denominator_total_porc_cobertura = 0
                total_ind_mora = total_cartera_improductiva = total_porc_cobertura = 0

                for result in query_results_current:
                    saldos_current[result['puc_codigo']] = result['total_saldo']

                    consumo_a = saldos_current['141105'] + saldos_current['141205'] + saldos_current['144105'] + saldos_current['144205']
                    consumo_b = saldos_current['141110'] + saldos_current['141210'] + saldos_current['144110'] + saldos_current['144210']
                    consumo_c = saldos_current['141115'] + saldos_current['141215'] + saldos_current['144115'] + saldos_current['144215']
                    consumo_d = saldos_current['141120'] + saldos_current['141220'] + saldos_current['144120'] + saldos_current['144220']
                    consumo_e = saldos_current['141125'] + saldos_current['141225'] + saldos_current['144125'] + saldos_current['144225'] 
                    consumo_total = consumo_a + consumo_b + consumo_c + consumo_d + consumo_e
                    consumo_deterioro = saldos_current['144500']
                    denominator_consumo_ind_mora = consumo_a + consumo_b + consumo_c + consumo_d + consumo_e
                    consumo_ind_mora = ((consumo_b + consumo_c + consumo_d + consumo_e)/denominator_consumo_ind_mora)*100 if denominator_consumo_ind_mora else 0
                    denominator_consumo_cartera_improductiva = denominator_consumo_ind_mora
                    consumo_cartera_improductiva = ((consumo_c + consumo_d + consumo_e) / denominator_consumo_cartera_improductiva) * 100 if denominator_consumo_cartera_improductiva else 0
                    denominator_consumo_porc_cobertura = consumo_b + consumo_c + consumo_d + consumo_e
                    consumo_porc_cobertura = (consumo_deterioro  / denominator_consumo_porc_cobertura) * 100 if denominator_consumo_porc_cobertura else 0

                    microcredito_a = saldos_current['144805'] + saldos_current['145505'] + saldos_current['145405']
                    microcredito_b = saldos_current['144810'] + saldos_current['145410'] + saldos_current['145510']
                    microcredito_c = saldos_current['144815'] + saldos_current['145515'] + saldos_current['145415'] 
                    microcredito_d = saldos_current['144820'] + saldos_current['145520'] + saldos_current['145420']
                    microcredito_e = saldos_current['144825'] + saldos_current['145425'] + saldos_current['145525'] 
                    microcredito_total = microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_deterioro = saldos_current['145100'] + saldos_current['145800']
                    denominator_microcredito_ind_mora = microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_ind_mora = ((microcredito_b + microcredito_c + microcredito_d + microcredito_e)/denominator_microcredito_ind_mora)*100 if denominator_microcredito_ind_mora else 0
                    denominator_microcredito_cartera_improductiva = denominator_microcredito_ind_mora
                    microcredito_cartera_improductiva = ((microcredito_c + microcredito_d + microcredito_e) / denominator_microcredito_cartera_improductiva) * 100 if denominator_microcredito_cartera_improductiva else 0
                    denominator_microcredito_porc_cobertura = microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_porc_cobertura = (microcredito_deterioro  / denominator_microcredito_porc_cobertura) * 100 if denominator_microcredito_porc_cobertura else 0

                    comercial_a = saldos_current['146105'] + saldos_current['146205']
                    comercial_b = saldos_current['146110'] + saldos_current['146210']
                    comercial_c = saldos_current['146115'] + saldos_current['146215']
                    comercial_d = saldos_current['146120'] + saldos_current['146220']
                    comercial_e = saldos_current['146125'] + saldos_current['146225']

                    comercial_total = comercial_a + comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_deterioro = saldos_current['146500']
                    denominator_comercial_ind_mora = comercial_a + comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_ind_mora = ((comercial_b + comercial_c + comercial_d + comercial_e)/denominator_comercial_ind_mora)*100 if denominator_comercial_ind_mora else 0
                    denominator_comercial_cartera_improductiva = denominator_comercial_ind_mora
                    comercial_cartera_improductiva = ((comercial_c + comercial_d + comercial_e) / denominator_comercial_cartera_improductiva) * 100 if denominator_comercial_cartera_improductiva else 0
                    denominator_comercial_porc_cobertura = comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_porc_cobertura = (comercial_deterioro  / denominator_comercial_porc_cobertura) * 100 if denominator_comercial_porc_cobertura else 0

                    vivienda_a = saldos_current['140405'] + saldos_current['140505']
                    vivienda_b = saldos_current['140410'] + saldos_current['140510']
                    vivienda_c = saldos_current['140415'] + saldos_current['140515']
                    vivienda_d = saldos_current['140420'] + saldos_current['140520']
                    vivienda_e = saldos_current['140425']  + saldos_current['140525']
                    vivienda_total = vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_deterioro = saldos_current['140800']
                    denominator_vivienda_ind_mora = vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_ind_mora = ((vivienda_b + vivienda_c + vivienda_d + vivienda_e)/denominator_vivienda_ind_mora)*100 if denominator_vivienda_ind_mora else 0
                    denominator_vivienda_cartera_improductiva = denominator_vivienda_ind_mora
                    vivienda_cartera_improductiva = ((vivienda_c + vivienda_d + vivienda_e) / denominator_vivienda_cartera_improductiva) * 100 if denominator_vivienda_cartera_improductiva else 0
                    denominator_vivienda_porc_cobertura = vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_porc_cobertura = (vivienda_deterioro  / denominator_vivienda_porc_cobertura) * 100 if denominator_vivienda_porc_cobertura else 0

                    empleados_a = saldos_current['146905'] + saldos_current['146930']
                    empleados_b = saldos_current['146910'] + saldos_current['146935']
                    empleados_c = saldos_current['146915'] + saldos_current['146940']
                    empleados_d = saldos_current['146920'] + saldos_current['146945']
                    empleados_e = saldos_current['146925']  + saldos_current['146950']
                    empleados_total = empleados_a + empleados_b + empleados_c + empleados_d + empleados_e
                    empleados_deterioro = saldos_current['147100']
                    denominator_empleados_ind_mora = empleados_a + empleados_b + empleados_c + empleados_d + empleados_e
                    empleados_ind_mora = ((empleados_b + empleados_c + empleados_d + empleados_e)/denominator_empleados_ind_mora)*100 if denominator_empleados_ind_mora else 0
                    denominator_empleados_cartera_improductiva = denominator_empleados_ind_mora
                    empleados_cartera_improductiva = ((empleados_c + empleados_d + empleados_e) / denominator_empleados_cartera_improductiva) * 100 if denominator_empleados_cartera_improductiva else 0
                    denominator_empleados_porc_cobertura = empleados_b + empleados_c + empleados_d + empleados_e
                    empleados_porc_cobertura = (empleados_deterioro  / denominator_empleados_porc_cobertura) * 100 if denominator_empleados_porc_cobertura else 0

                    total_a = consumo_a + microcredito_a + comercial_a + vivienda_a + empleados_a
                    total_b = consumo_b + microcredito_b + comercial_b + vivienda_b + empleados_b
                    total_c = consumo_c + microcredito_c + comercial_c + vivienda_c + empleados_c
                    total_d = consumo_d + microcredito_d + comercial_d + vivienda_d + empleados_d
                    total_e = consumo_e + microcredito_e + comercial_e + vivienda_e + empleados_e
                    total_castigos = saldos_current['831000']
                    total_total = consumo_total + microcredito_total + comercial_total + vivienda_total + empleados_total
                    total_deterioro = consumo_deterioro + microcredito_deterioro + comercial_deterioro + vivienda_deterioro + empleados_deterioro
                    denominator_total_ind_mora = total_a + total_b + total_c + total_d + total_e
                    total_ind_mora = ((total_b + total_c + total_d + total_e)/denominator_total_ind_mora)*100 if denominator_total_ind_mora else 0
                    denominator_total_porc_cobertura = total_b + total_c + total_d + total_e
                    total_porc_cobertura = (total_deterioro  / denominator_total_porc_cobertura) * 100 if denominator_total_porc_cobertura else 0

                results.append({
                    'entidad_nit': nit,
                    'sigla': nit_info.get("sigla"),
                    'periodo': periodo,
                    'mes': mes,
                    'consumoA': consumo_a,
                    'consumoB': consumo_b,
                    'consumoC': consumo_c,
                    'consumoD': consumo_d,
                    'consumoE': consumo_e,
                    'consumoTotal': consumo_total,
                    'consumoIndMora': consumo_ind_mora,
                    'consumoCartImprod': consumo_cartera_improductiva,
                    'consumoDeterioro': consumo_deterioro,
                    'consumoPorcCobertura': consumo_porc_cobertura,
                    'microcreditoA': microcredito_a,
                    'microcreditoB': microcredito_b,
                    'microcreditoC': microcredito_c,
                    'microcreditoD': microcredito_d,
                    'microcreditoE': microcredito_e,
                    'microcreditoTotal': microcredito_total,
                    'microcreditoIndMora': microcredito_ind_mora,
                    'microcreditoCartImprod': microcredito_cartera_improductiva,
                    'microcreditoDeterioro': microcredito_deterioro,
                    'microcreditoPorcCobertura': microcredito_porc_cobertura,
                    'comercialA': comercial_a,
                    'comercialB': comercial_b,
                    'comercialC': comercial_c,
                    'comercialD': comercial_d,
                    'comercialE': comercial_e,
                    'comercialTotal': comercial_total,
                    'comercialIndMora': comercial_ind_mora,
                    'comercialCartImprod': comercial_cartera_improductiva,
                    'comercialDeterioro': comercial_deterioro,
                    'comercialPorcCobertura': comercial_porc_cobertura,
                    'viviendaA': vivienda_a,
                    'viviendaB': vivienda_b,
                    'viviendaC': vivienda_c,
                    'viviendaD': vivienda_d,
                    'viviendaE': vivienda_e,
                    'viviendaTotal': vivienda_total,
                    'viviendaIndMora': vivienda_ind_mora,
                    'viviendaCartImprod': vivienda_cartera_improductiva,
                    'viviendaDeterioro': vivienda_deterioro,
                    'viviendaPorcCobertura': vivienda_porc_cobertura,
                    'empleadosA': empleados_a,
                    'empleadosB': empleados_b,
                    'empleadosC': empleados_c,
                    'empleadosD': empleados_d,
                    'empleadosE': empleados_e,
                    'empleadosTotal': empleados_total,
                    'empleadosIndMora': empleados_ind_mora,
                    'empleadosCartImprod': empleados_cartera_improductiva,
                    'empleadosDeterioro': empleados_deterioro,
                    'empleadosPorcCobertura': empleados_porc_cobertura,
                    'totalA': total_a,
                    'totalB': total_b,
                    'totalC': total_c,
                    'totalD': total_d,
                    'totalE': total_e,
                    'totalTotal': total_total,
                    'totalCastigos': total_castigos,
                    'totalIndMora': total_ind_mora,
                    'totalDeterioro': total_deterioro,
                    'totalPorcCobertura': total_porc_cobertura,
                })

        return Response(data=results, status=status.HTTP_200_OK)
"""


class BalCoopApiViewIndicador(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            for nit_info in item.get("nit", {}).get("solidaria", []):
                razon_social = nit_info.get("RazonSocial")
                periodo = item.get("periodo")
                periodo_anterior = periodo - 1
                mes_12 = 12
                mes = item.get("mes")
                mes_decimal = Decimal(mes)

                puc_codes_current = [
                    "100000",
                    "110000",
                    "120000",
                    "140000",
                    "210000",
                    "230000",
                    "240000",
                    "300000",
                    "310000",
                    "311010",
                    "320000",
                    "330500",
                    "340500",
                    "350000",
                    "415000",
                    "615005",
                    "615010",
                    "615015",
                    "615020",
                    "615035",
                ]
                puc_codes_previous = ["100000", "140000", "210000", "300000", "230000"]

                q_current_period = Q(
                    entidad_RS=razon_social, periodo=periodo, mes=mes
                ) & Q(puc_codigo__in=puc_codes_current)
                q_previous_period = Q(
                    entidad_RS=razon_social, periodo=periodo_anterior, mes=mes_12
                ) & Q(puc_codigo__in=puc_codes_previous)

                query_results_current = (
                    BalCoopModel.objects.filter(q_current_period)
                    .values("puc_codigo")
                    .annotate(total_saldo=Sum("saldo"))
                )
                query_results_previous = (
                    BalCoopModel.objects.filter(q_previous_period)
                    .values("puc_codigo")
                    .annotate(total_saldo=Sum("saldo"))
                )

                saldos_current = {puc: 0 for puc in puc_codes_current}
                for result in query_results_current:
                    saldos_current[result["puc_codigo"]] = result["total_saldo"]

                saldos_previous = {puc: 0 for puc in puc_codes_previous}
                indicador_roe = indicador_roa = indicador_ingreso_cartera = (
                    indicador_credito_banco
                ) = 0

                for result in query_results_previous:
                    saldos_previous[result["puc_codigo"]] = result["total_saldo"]

                indicador_cartera = (
                    (saldos_current["140000"] / saldos_current["100000"]) * 100
                    if saldos_current["100000"]
                    else 0
                )
                indicador_deposito = (
                    (saldos_current["210000"] / saldos_current["100000"]) * 100
                    if saldos_current["100000"]
                    else 0
                )
                indicador_obligaciones = (
                    (saldos_current["230000"] / saldos_current["100000"]) * 100
                    if saldos_current["100000"]
                    else 0
                )
                indicador_cap_social = (
                    (saldos_current["310000"] / saldos_current["100000"]) * 100
                    if saldos_current["100000"]
                    else 0
                )
                indicador_cap_inst = (
                    (
                        (
                            saldos_current["311010"]
                            + saldos_current["320000"]
                            + saldos_current["330500"]
                            + saldos_current["340500"]
                        )
                        / saldos_current["100000"]
                    )
                    * 100
                    if saldos_current["100000"]
                    else 0
                )

                denominator_roe = (
                    saldos_previous["300000"]
                    + (saldos_current["300000"] / mes_decimal) * 12
                ) / 2
                indicador_roe = (
                    (saldos_current["350000"] / denominator_roe) * 100
                    if denominator_roe
                    else 0
                )

                denominator_roa = (
                    saldos_previous["100000"]
                    + (saldos_current["100000"] / mes_decimal) * 12
                ) / 2
                indicador_roa = (
                    (saldos_current["350000"] / denominator_roa) * 100
                    if denominator_roa
                    else 0
                )

                denominator_ingreso_cartera = (
                    saldos_previous["140000"]
                    + (saldos_current["140000"] / mes_decimal) * 12
                ) / 2
                indicador_ingreso_cartera = (
                    (saldos_current["415000"] / denominator_ingreso_cartera) * 100
                    if denominator_ingreso_cartera
                    else 0
                )

                denominator_costos_deposito = (
                    saldos_previous["210000"]
                    + (saldos_current["210000"] / mes_decimal) * 12
                ) / 2
                indicador_costos_deposito = (
                    (
                        (
                            saldos_current["615005"]
                            + saldos_current["615010"]
                            + saldos_current["615015"]
                            + saldos_current["615020"]
                        )
                        / denominator_costos_deposito
                    )
                    * 100
                    if denominator_costos_deposito
                    else 0
                )
                denominator_credito_banco = (
                    saldos_previous["230000"]
                    + (saldos_current["230000"] / mes_decimal) * 12
                ) / 2
                indicador_credito_banco = (
                    (saldos_current["615035"] / denominator_credito_banco) * 100
                    if denominator_credito_banco
                    else 0
                )

                indicador_disponible = (
                    (
                        (
                            saldos_current["110000"]
                            + saldos_current["120000"]
                            - (saldos_current["240000"] * 20 / 100)
                        )
                        / saldos_current["210000"]
                    )
                    * 100
                    if saldos_current["210000"]
                    else 0
                )

                results.append(
                    {
                        "entidad_RS": razon_social,
                        "sigla": nit_info.get("sigla"),
                        "periodo": periodo,
                        "mes": mes,
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
                )

        sorted_results = sorted(results, key=lambda x: x["entidad_RS"])

        return Response(data=sorted_results, status=status.HTTP_200_OK)


class BalCoopApiViewIndicadorC(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            for nit_info in item.get("nit", {}).get("solidaria", []):
                razon_social = nit_info.get("RazonSocial")
                periodo = item.get("periodo")
                mes = item.get("mes")

                puc_codes_current = [
                    "141105",
                    "141205",
                    "144105",
                    "144205",
                    "141110",
                    "141210",
                    "144110",
                    "144210",
                    "141115",
                    "141215",
                    "144115",
                    "144215",
                    "141120",
                    "141220",
                    "144120",
                    "144220",
                    "141125",
                    "141225",
                    "144125",
                    "144225",
                    "144805",
                    "145505",
                    "145405",
                    "144810",
                    "145410",
                    "145510",
                    "144815",
                    "145515",
                    "145415",
                    "144820",
                    "145520",
                    "145420",
                    "144825",
                    "145425",
                    "145525",
                    "146105",
                    "146205",
                    "146110",
                    "146210",
                    "146115",
                    "146215",
                    "146120",
                    "146220",
                    "146125",
                    "146225",
                    "140405",
                    "140505",
                    "140410",
                    "140510",
                    "140415",
                    "140515",
                    "140420",
                    "140520",
                    "140425",
                    "140525",
                    "146905",
                    "146930",
                    "146910",
                    "146935",
                    "146915",
                    "146940",
                    "146920",
                    "146945",
                    "146925",
                    "146950",
                    "831000",
                    "144500",
                    "145100",
                    "145800",
                    "146500",
                    "140800",
                    "147100",
                ]

                q_current_period = Q(
                    entidad_RS=razon_social, periodo=periodo, mes=mes
                ) & Q(puc_codigo__in=puc_codes_current)

                query_results_current = (
                    BalCoopModel.objects.filter(q_current_period)
                    .values("puc_codigo")
                    .annotate(total_saldo=Sum("saldo"))
                )

                saldos_current = {puc: 0 for puc in puc_codes_current}

                consumo_a = consumo_b = consumo_c = consumo_d = consumo_e = (
                    consumo_total
                ) = consumo_deterioro = 0
                denominator_consumo_ind_mora = (
                    denominator_consumo_cartera_improductiva
                ) = denominator_consumo_porc_cobertura = 0
                consumo_ind_mora = consumo_cartera_improductiva = (
                    consumo_porc_cobertura
                ) = 0

                microcredito_a = microcredito_b = microcredito_c = microcredito_d = (
                    microcredito_e
                ) = microcredito_total = microcredito_deterioro = 0
                denominator_microcredito_ind_mora = (
                    denominator_microcredito_cartera_improductiva
                ) = denominator_microcredito_porc_cobertura = 0
                microcredito_ind_mora = microcredito_cartera_improductiva = (
                    microcredito_porc_cobertura
                ) = 0

                comercial_a = comercial_b = comercial_c = comercial_d = comercial_e = (
                    comercial_total
                ) = comercial_deterioro = 0
                denominator_comercial_ind_mora = (
                    denominator_comercial_cartera_improductiva
                ) = denominator_comercial_porc_cobertura = 0
                comercial_ind_mora = comercial_cartera_improductiva = (
                    comercial_porc_cobertura
                ) = 0

                vivienda_a = vivienda_b = vivienda_c = vivienda_d = vivienda_e = (
                    vivienda_total
                ) = vivienda_deterioro = 0
                denominator_vivienda_ind_mora = (
                    denominator_vivienda_cartera_improductiva
                ) = denominator_vivienda_porc_cobertura = 0
                vivienda_ind_mora = vivienda_cartera_improductiva = (
                    vivienda_porc_cobertura
                ) = 0

                empleados_a = empleados_b = empleados_c = empleados_d = empleados_e = (
                    empleados_total
                ) = empleados_deterioro = 0
                denominator_empleados_ind_mora = (
                    denominator_empleados_cartera_improductiva
                ) = denominator_empleados_porc_cobertura = 0
                empleados_ind_mora = empleados_cartera_improductiva = (
                    empleados_porc_cobertura
                ) = 0

                total_a = total_b = total_c = total_d = total_e = total_total = (
                    total_deterioro
                ) = 0
                denominator_total_ind_mora = total_castigos = (
                    denominator_total_porc_cobertura
                ) = 0
                total_ind_mora = total_cartera_improductiva = total_porc_cobertura = 0

                for result in query_results_current:
                    saldos_current[result["puc_codigo"]] = result["total_saldo"]

                    # indicadores de consumo
                    consumo_a = (
                        saldos_current["141105"]
                        + saldos_current["141205"]
                        + saldos_current["144105"]
                        + saldos_current["144205"]
                    )
                    consumo_b = (
                        saldos_current["141110"]
                        + saldos_current["141210"]
                        + saldos_current["144110"]
                        + saldos_current["144210"]
                    )
                    consumo_c = (
                        saldos_current["141115"]
                        + saldos_current["141215"]
                        + saldos_current["144115"]
                        + saldos_current["144215"]
                    )
                    consumo_d = (
                        saldos_current["141120"]
                        + saldos_current["141220"]
                        + saldos_current["144120"]
                        + saldos_current["144220"]
                    )
                    consumo_e = (
                        saldos_current["141125"]
                        + saldos_current["141225"]
                        + saldos_current["144125"]
                        + saldos_current["144225"]
                    )
                    consumo_total = (
                        consumo_a + consumo_b + consumo_c + consumo_d + consumo_e
                    )
                    consumo_deterioro = saldos_current["144500"]
                    denominator_consumo_ind_mora = (
                        consumo_a + consumo_b + consumo_c + consumo_d + consumo_e
                    )
                    consumo_ind_mora = (
                        (
                            (consumo_b + consumo_c + consumo_d + consumo_e)
                            / denominator_consumo_ind_mora
                        )
                        * 100
                        if denominator_consumo_ind_mora
                        else 0
                    )
                    denominator_consumo_cartera_improductiva = (
                        denominator_consumo_ind_mora
                    )
                    consumo_cartera_improductiva = (
                        (
                            (consumo_c + consumo_d + consumo_e)
                            / denominator_consumo_cartera_improductiva
                        )
                        * 100
                        if denominator_consumo_cartera_improductiva
                        else 0
                    )
                    denominator_consumo_porc_cobertura = (
                        consumo_b + consumo_c + consumo_d + consumo_e
                    )
                    consumo_porc_cobertura = (
                        (consumo_deterioro / denominator_consumo_porc_cobertura) * 100
                        if denominator_consumo_porc_cobertura
                        else 0
                    )

                    # Indicadores de Microcredito
                    microcredito_a = (
                        saldos_current["144805"]
                        + saldos_current["145505"]
                        + saldos_current["145405"]
                    )
                    microcredito_b = (
                        saldos_current["144810"]
                        + saldos_current["145410"]
                        + saldos_current["145510"]
                    )
                    microcredito_c = (
                        saldos_current["144815"]
                        + saldos_current["145515"]
                        + saldos_current["145415"]
                    )
                    microcredito_d = (
                        saldos_current["144820"]
                        + saldos_current["145520"]
                        + saldos_current["145420"]
                    )
                    microcredito_e = (
                        saldos_current["144825"]
                        + saldos_current["145425"]
                        + saldos_current["145525"]
                    )
                    microcredito_total = (
                        microcredito_a
                        + microcredito_b
                        + microcredito_c
                        + microcredito_d
                        + microcredito_e
                    )
                    microcredito_deterioro = (
                        saldos_current["145100"] + saldos_current["145800"]
                    )
                    denominator_microcredito_ind_mora = (
                        microcredito_a
                        + microcredito_b
                        + microcredito_c
                        + microcredito_d
                        + microcredito_e
                    )
                    microcredito_ind_mora = (
                        (
                            (
                                microcredito_b
                                + microcredito_c
                                + microcredito_d
                                + microcredito_e
                            )
                            / denominator_microcredito_ind_mora
                        )
                        * 100
                        if denominator_microcredito_ind_mora
                        else 0
                    )
                    denominator_microcredito_cartera_improductiva = (
                        denominator_microcredito_ind_mora
                    )
                    microcredito_cartera_improductiva = (
                        (
                            (microcredito_c + microcredito_d + microcredito_e)
                            / denominator_microcredito_cartera_improductiva
                        )
                        * 100
                        if denominator_microcredito_cartera_improductiva
                        else 0
                    )
                    denominator_microcredito_porc_cobertura = (
                        microcredito_b
                        + microcredito_c
                        + microcredito_d
                        + microcredito_e
                    )
                    microcredito_porc_cobertura = (
                        (
                            microcredito_deterioro
                            / denominator_microcredito_porc_cobertura
                        )
                        * 100
                        if denominator_microcredito_porc_cobertura
                        else 0
                    )

                    # Indicadores de Comercial
                    comercial_a = saldos_current["146105"] + saldos_current["146205"]
                    comercial_b = saldos_current["146110"] + saldos_current["146210"]
                    comercial_c = saldos_current["146115"] + saldos_current["146215"]
                    comercial_d = saldos_current["146120"] + saldos_current["146220"]
                    comercial_e = saldos_current["146125"] + saldos_current["146225"]
                    comercial_total = (
                        comercial_a
                        + comercial_b
                        + comercial_c
                        + comercial_d
                        + comercial_e
                    )
                    comercial_deterioro = saldos_current["146500"]
                    denominator_comercial_ind_mora = (
                        comercial_a
                        + comercial_b
                        + comercial_c
                        + comercial_d
                        + comercial_e
                    )
                    comercial_ind_mora = (
                        (
                            (comercial_b + comercial_c + comercial_d + comercial_e)
                            / denominator_comercial_ind_mora
                        )
                        * 100
                        if denominator_comercial_ind_mora
                        else 0
                    )
                    denominator_comercial_cartera_improductiva = (
                        denominator_comercial_ind_mora
                    )
                    comercial_cartera_improductiva = (
                        (
                            (comercial_c + comercial_d + comercial_e)
                            / denominator_comercial_cartera_improductiva
                        )
                        * 100
                        if denominator_comercial_cartera_improductiva
                        else 0
                    )
                    denominator_comercial_porc_cobertura = (
                        comercial_b + comercial_c + comercial_d + comercial_e
                    )
                    comercial_porc_cobertura = (
                        (comercial_deterioro / denominator_comercial_porc_cobertura)
                        * 100
                        if denominator_comercial_porc_cobertura
                        else 0
                    )

                    # Indicadores de Vivienda
                    vivienda_a = saldos_current["140405"] + saldos_current["140505"]
                    vivienda_b = saldos_current["140410"] + saldos_current["140510"]
                    vivienda_c = saldos_current["140415"] + saldos_current["140515"]
                    vivienda_d = saldos_current["140420"] + saldos_current["140520"]
                    vivienda_e = saldos_current["140425"] + saldos_current["140525"]
                    vivienda_total = (
                        vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    )
                    vivienda_deterioro = saldos_current["140800"]
                    denominator_vivienda_ind_mora = (
                        vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    )
                    vivienda_ind_mora = (
                        (
                            (vivienda_b + vivienda_c + vivienda_d + vivienda_e)
                            / denominator_vivienda_ind_mora
                        )
                        * 100
                        if denominator_vivienda_ind_mora
                        else 0
                    )
                    denominator_vivienda_cartera_improductiva = (
                        denominator_vivienda_ind_mora
                    )
                    vivienda_cartera_improductiva = (
                        (
                            (vivienda_c + vivienda_d + vivienda_e)
                            / denominator_vivienda_cartera_improductiva
                        )
                        * 100
                        if denominator_vivienda_cartera_improductiva
                        else 0
                    )
                    denominator_vivienda_porc_cobertura = (
                        vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    )
                    vivienda_porc_cobertura = (
                        (vivienda_deterioro / denominator_vivienda_porc_cobertura) * 100
                        if denominator_vivienda_porc_cobertura
                        else 0
                    )

                    # Ïndicadores de Empleado
                    empleados_a = saldos_current["146905"] + saldos_current["146930"]
                    empleados_b = saldos_current["146910"] + saldos_current["146935"]
                    empleados_c = saldos_current["146915"] + saldos_current["146940"]
                    empleados_d = saldos_current["146920"] + saldos_current["146945"]
                    empleados_e = saldos_current["146925"] + saldos_current["146950"]
                    empleados_total = (
                        empleados_a
                        + empleados_b
                        + empleados_c
                        + empleados_d
                        + empleados_e
                    )
                    empleados_deterioro = saldos_current["147100"]
                    denominator_empleados_ind_mora = (
                        empleados_a
                        + empleados_b
                        + empleados_c
                        + empleados_d
                        + empleados_e
                    )
                    empleados_ind_mora = (
                        (
                            (empleados_b + empleados_c + empleados_d + empleados_e)
                            / denominator_empleados_ind_mora
                        )
                        * 100
                        if denominator_empleados_ind_mora
                        else 0
                    )
                    denominator_empleados_cartera_improductiva = (
                        denominator_empleados_ind_mora
                    )
                    empleados_cartera_improductiva = (
                        (
                            (empleados_c + empleados_d + empleados_e)
                            / denominator_empleados_cartera_improductiva
                        )
                        * 100
                        if denominator_empleados_cartera_improductiva
                        else 0
                    )
                    denominator_empleados_porc_cobertura = (
                        empleados_b + empleados_c + empleados_d + empleados_e
                    )
                    empleados_porc_cobertura = (
                        (empleados_deterioro / denominator_empleados_porc_cobertura)
                        * 100
                        if denominator_empleados_porc_cobertura
                        else 0
                    )

                    # Indicadores de Cartera General
                    total_a = (
                        consumo_a
                        + microcredito_a
                        + comercial_a
                        + vivienda_a
                        + empleados_a
                    )
                    total_b = (
                        consumo_b
                        + microcredito_b
                        + comercial_b
                        + vivienda_b
                        + empleados_b
                    )
                    total_c = (
                        consumo_c
                        + microcredito_c
                        + comercial_c
                        + vivienda_c
                        + empleados_c
                    )
                    total_d = (
                        consumo_d
                        + microcredito_d
                        + comercial_d
                        + vivienda_d
                        + empleados_d
                    )
                    total_e = (
                        consumo_e
                        + microcredito_e
                        + comercial_e
                        + vivienda_e
                        + empleados_e
                    )
                    total_castigos = saldos_current["831000"]
                    total_total = (
                        consumo_total
                        + microcredito_total
                        + comercial_total
                        + vivienda_total
                        + empleados_total
                    )
                    total_deterioro = (
                        consumo_deterioro
                        + microcredito_deterioro
                        + comercial_deterioro
                        + vivienda_deterioro
                        + empleados_deterioro
                    )
                    denominator_total_ind_mora = (
                        total_a + total_b + total_c + total_d + total_e
                    )
                    total_ind_mora = (
                        (
                            (total_b + total_c + total_d + total_e)
                            / denominator_total_ind_mora
                        )
                        * 100
                        if denominator_total_ind_mora
                        else 0
                    )
                    denominator_total_porc_cobertura = (
                        total_b + total_c + total_d + total_e
                    )
                    total_porc_cobertura = (
                        (total_deterioro / denominator_total_porc_cobertura) * 100
                        if denominator_total_porc_cobertura
                        else 0
                    )

                results.append(
                    {
                        "entidad_RS": razon_social,
                        "sigla": nit_info.get("sigla"),
                        "periodo": periodo,
                        "mes": mes,
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
                )

        return Response(data=results, status=status.HTTP_200_OK)
