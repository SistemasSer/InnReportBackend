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

from balSup.models import BalSupModel
from balSup.serializers import BalSupSerializer 

logger = logging.getLogger('django')

class BalSupApiView(APIView):
    def get(self, request):
        serializer = BalSupSerializer(BalSupModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    
    def post(self, request):
        data_list = request.data
        
        serializer = BalSupSerializer(data=data_list, many=True)
        serializer.is_valid(raise_exception=True)
        
        new_instances = []
        update_instances = []
        
        existing_instances = BalSupModel.objects.filter(
            Q(periodo__in=[data['periodo'] for data in data_list]) &
            Q(mes__in=[data['mes'] for data in data_list]) &
            Q(entidad_RS__in=[data['entidad_RS'] for data in data_list]) &
            Q(puc_codigo__in=[data['puc_codigo'] for data in data_list])
        )
        
        existing_dict = {
            (instance.periodo, instance.mes, instance.entidad_RS, instance.puc_codigo): instance
            for instance in existing_instances
        }
        
        for data in data_list:
            key = (data['periodo'], data['mes'], data['entidad_RS'], data['puc_codigo'])
            if key in existing_dict:
                instance = existing_dict[key]
               
                if all(getattr(instance, field) == data[field] for field in data):
                    
                    for attr, value in data.items():
                        setattr(instance, attr, value)
                    update_instances.append(instance)
                else:
                    for attr, value in data.items():
                        setattr(instance, attr, value)
                    update_instances.append(instance)
            else:
                new_instances.append(BalSupModel(**data))
        
        with transaction.atomic():
            if new_instances:
                BalSupModel.objects.bulk_create(new_instances)
            if update_instances:
                BalSupModel.objects.bulk_update(update_instances, ['periodo', 'mes', 'entidad_RS', 'puc_codigo', 'saldo', 'updated_at'])
        
        return Response(status=status.HTTP_200_OK, data={"created": len(new_instances), "updated": len(update_instances)})

@authentication_classes([JWTAuthentication]) 
@permission_classes([IsAuthenticated])

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

"""
class BalSupApiViewA(APIView):
    def post(self, request):
        data = request.data
        q_objects = Q() 
        found_valid_nits = False

        # Initialize transformed results with specified months, setting saldo as 0
        transformed_results = {}
        for item in data:
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                nit = nit_info.get("nit")
                sigla = nit_info.get("sigla")
                periodo = item.get("periodo")
                puc_codigo = item.get("puc_codigo")
                mes = item.get("mes")

                key = (nit, puc_codigo)
                if key not in transformed_results:
                    transformed_results[key] = {
                        'entidad_nit': nit,
                        'sigla': sigla,
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
                    periodo=periodo,
                    puc_codigo=puc_codigo,
                    mes=mes
                )
                found_valid_nits = True

        if not found_valid_nits:
            return Response(data=[], status=status.HTTP_200_OK)

        query_results = BalSupModel.objects.filter(q_objects).values('entidad_nit', 'periodo', 'puc_codigo', 'mes', 'saldo')

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
class BalSupApiViewA(APIView):
    def post(self, request):
        data = request.data
        #logger.debug("Datos recibidos: %s", data)
        
        q_objects = Q()
        found_valid_entries = False

        transformed_results = {}
        for item in data:
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                razon_social = nit_info.get("RazonSocial")  
                sigla = nit_info.get("sigla")
                periodo = item.get("periodo")
                puc_codigo = item.get("puc_codigo")
                mes = item.get("mes")

                key = (razon_social, puc_codigo)
                if key not in transformed_results:
                    transformed_results[key] = {
                        'razon_social': razon_social,
                        'sigla': sigla,
                        'puc_codigo': puc_codigo,
                        'saldos': []
                    }
                transformed_results[key]['saldos'].append({
                    'periodo': periodo,
                    'mes': mes,
                    'saldo': 0
                })

                q_objects |= Q(
                    entidad_RS=razon_social,
                    periodo=periodo,
                    puc_codigo=puc_codigo,
                    mes=mes
                )
                found_valid_entries = True

        if not found_valid_entries:
            return Response(data=[], status=status.HTTP_200_OK)

        query_results = BalSupModel.objects.filter(q_objects).values(
            'entidad_RS', 'periodo', 'puc_codigo', 'mes', 'saldo'
        )

        #logger.debug("Resultados de la consulta financiera: %s", query_results)

        for result in query_results:
            key = (result['entidad_RS'], result['puc_codigo'])
            if key in transformed_results:
                for saldo_info in transformed_results[key]['saldos']:
                    if saldo_info['periodo'] == result['periodo'] and saldo_info['mes'] == result['mes']:
                        saldo_info['saldo'] = result['saldo']

        final_results = list(transformed_results.values())

        #logger.debug("Resultados finales transformados financiera: %s", final_results)

        return Response(data=final_results, status=status.HTTP_200_OK)

"""
class BalSupApiViewIndicador(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            indicador_cartera = indicador_deposito = indicador_obligaciones = 0
            indicador_cap_social = indicador_cap_inst = indicador_roe = 0
            indicador_roa = indicador_ingreso_cartera = indicador_costos_deposito = 0
            indicador_credito_banco = indicador_disponible = 0
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                nit = nit_info.get("nit")
                periodo = item.get("periodo")
                periodo_anterior = periodo - 1
                mes_12 = 12
                mes = item.get("mes")
                mes_decimal = Decimal(mes)

                puc_codes_current = ['100000','110000','120000','130000','140000', '210000','240000', '250000','300000', '310000', '320000', '370500','391500','410200','510200','510300']
                puc_codes_previous = ['100000','140000','210000','240000','300000']

                q_current_period = Q(entidad_nit=nit, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)
                q_previous_period = Q(entidad_nit=nit, periodo=periodo_anterior, mes=mes_12) & Q(puc_codigo__in=puc_codes_previous)

                query_results_current = BalSupModel.objects.filter(q_current_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))
                query_results_previous = BalSupModel.objects.filter(q_previous_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))

                saldos_current = {puc: 0 for puc in puc_codes_current}
                for result in query_results_current:
                    saldos_current[result['puc_codigo']] = result['total_saldo']

                saldos_previous = {puc: 0 for puc in puc_codes_previous}
                for result in query_results_previous:
                    saldos_previous[result['puc_codigo']] = result['total_saldo']

                    indicador_cartera = (saldos_current['140000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                    indicador_deposito = (saldos_current['210000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                    indicador_obligaciones = (saldos_current['240000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                    indicador_cap_social = (saldos_current['310000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                    indicador_cap_inst = ((saldos_current['320000'] + saldos_current['370500']) / saldos_current['100000']) * 100 if saldos_current['100000'] else 0

                    # For the next calculations, ensure that the denominators are not zero
                    denominator_roe = (saldos_previous['300000'] + (saldos_current['300000'] / mes_decimal) * 12) / 2
                    indicador_roe = (saldos_current['391500'] / denominator_roe) * 100 if denominator_roe else 0

                    denominator_roa = (saldos_previous['100000'] + (saldos_current['100000'] / mes_decimal) * 12) / 2
                    indicador_roa = (saldos_current['391500'] / denominator_roa) * 100 if denominator_roa else 0

                    denominator_ingreso_cartera = (saldos_previous['140000'] + (saldos_current['140000'] / mes_decimal) * 12) / 2
                    indicador_ingreso_cartera = (saldos_current['410200'] / denominator_ingreso_cartera) * 100 if denominator_ingreso_cartera else 0

                    denominator_costos_deposito = (saldos_previous['210000'] + (saldos_current['210000'] / mes_decimal) * 12) / 2
                    indicador_costos_deposito = (saldos_current['510200'] / denominator_costos_deposito) * 100 if denominator_costos_deposito else 0

                    denominator_credito_banco = (saldos_previous['240000'] + (saldos_current['240000'] / mes_decimal) * 12) / 2
                    indicador_credito_banco = (saldos_current['510300'] / denominator_credito_banco) * 100 if denominator_credito_banco else 0

                    denominator_disponible = saldos_current['210000']
                    indicador_disponible = ((saldos_current['110000'] + saldos_current['120000'] + saldos_current['130000'] - (saldos_current['250000'] * 20 / 100)) / denominator_disponible) * 100 if denominator_disponible else 0


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
class BalSupApiViewIndicadorC(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                nit = nit_info.get("nit")
                periodo = item.get("periodo")
                mes = item.get("mes")

                puc_codes_current = ['140800','140805','140810','140815','140820','140825','149100',
                                        '141200','141205','141210','141215','141220','141225', '149300',
                                        '141000','141005','141010','141015','141020','141025', '149500',
                                        '140200','140205','140210','140215','140220','140225', '148900',
                                        '140400','140405','140410','140415','140420','140425',
                                        '141400','141405','141410','141415','141420','141425', '148800',
                                        '141430','141435','141440','141445','141450',
                                        '141460','141465','141470','141475','141480','812000'

                                    ]

                q_current_period = Q(entidad_nit=nit, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)

                query_results_current = BalSupModel.objects.filter(q_current_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))

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

                    consumo_a = saldos_current['140805']
                    consumo_b = saldos_current['140810']
                    consumo_c = saldos_current['140815'] 
                    consumo_d = saldos_current['140820']
                    consumo_e = saldos_current['140825'] 
                    consumo_total = saldos_current['140800']
                    denominator_consumo_ind_mora = saldos_current['140805'] +saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']
                    consumo_ind_mora = ((saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825'])/denominator_consumo_ind_mora)*100 if denominator_consumo_ind_mora else 0
                    denominator_consumo_cartera_improductiva = saldos_current['140805'] + saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']
                    consumo_cartera_improductiva = ((saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']) / denominator_consumo_cartera_improductiva) * 100 if denominator_consumo_cartera_improductiva else 0
                    consumo_deterioro = saldos_current['149100']
                    denominator_consumo_porc_cobertura = saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']
                    consumo_porc_cobertura = (saldos_current['149100']  / denominator_consumo_porc_cobertura) * 100 if denominator_consumo_porc_cobertura else 0

                    microcredito_a = saldos_current['141205']
                    microcredito_b = saldos_current['141210']
                    microcredito_c = saldos_current['141215'] 
                    microcredito_d = saldos_current['141220']
                    microcredito_e = saldos_current['141225'] 
                    microcredito_total = saldos_current['141200']
                    microcredito_deterioro = saldos_current['149300']
                    denominator_microcredito_ind_mora = microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_ind_mora = ((microcredito_b + microcredito_c + microcredito_d + microcredito_e)/denominator_microcredito_ind_mora)*100 if denominator_microcredito_ind_mora else 0
                    denominator_microcredito_cartera_improductiva = denominator_microcredito_ind_mora
                    microcredito_cartera_improductiva = ((microcredito_c + microcredito_d + microcredito_e) / denominator_microcredito_cartera_improductiva) * 100 if denominator_microcredito_cartera_improductiva else 0
                    denominator_microcredito_porc_cobertura = microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_porc_cobertura = (microcredito_deterioro  / denominator_microcredito_porc_cobertura) * 100 if denominator_microcredito_porc_cobertura else 0

                    comercial_a = saldos_current['141005']
                    comercial_b = saldos_current['141010']
                    comercial_c = saldos_current['141015'] 
                    comercial_d = saldos_current['141020']
                    comercial_e = saldos_current['141025'] 
                    comercial_total = saldos_current['141000']
                    comercial_deterioro = saldos_current['149500']
                    denominator_comercial_ind_mora = comercial_a + comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_ind_mora = ((comercial_b + comercial_c + comercial_d + comercial_e)/denominator_comercial_ind_mora)*100 if denominator_comercial_ind_mora else 0
                    denominator_comercial_cartera_improductiva = denominator_comercial_ind_mora
                    comercial_cartera_improductiva = ((comercial_c + comercial_d + comercial_e) / denominator_comercial_cartera_improductiva) * 100 if denominator_comercial_cartera_improductiva else 0
                    denominator_comercial_porc_cobertura = comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_porc_cobertura = (comercial_deterioro  / denominator_comercial_porc_cobertura) * 100 if denominator_comercial_porc_cobertura else 0

                    vivienda_a = saldos_current['140205'] + saldos_current['140405']
                    vivienda_b = saldos_current['140210'] + saldos_current['140410']
                    vivienda_c = saldos_current['140215'] + saldos_current['140415']
                    vivienda_d = saldos_current['140220'] + saldos_current['140420']
                    vivienda_e = saldos_current['140225']  + saldos_current['140425']
                    vivienda_total = saldos_current['140200'] + saldos_current['140400']
                    vivienda_deterioro = saldos_current['148900']
                    denominator_vivienda_ind_mora = vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_ind_mora = ((vivienda_b + vivienda_c + vivienda_d + vivienda_e)/denominator_vivienda_ind_mora)*100 if denominator_vivienda_ind_mora else 0
                    denominator_vivienda_cartera_improductiva = denominator_vivienda_ind_mora
                    vivienda_cartera_improductiva = ((vivienda_c + vivienda_d + vivienda_e) / denominator_vivienda_cartera_improductiva) * 100 if denominator_vivienda_cartera_improductiva else 0
                    denominator_vivienda_porc_cobertura = vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_porc_cobertura = (vivienda_deterioro  / denominator_vivienda_porc_cobertura) * 100 if denominator_vivienda_porc_cobertura else 0

                    empleados_a = saldos_current['141405'] + saldos_current['141430'] + saldos_current['141460']
                    empleados_b = saldos_current['141410'] + saldos_current['141435'] + saldos_current['141465']
                    empleados_c = saldos_current['141415'] + saldos_current['141440'] + saldos_current['141470']
                    empleados_d = saldos_current['141420'] + saldos_current['141445'] + saldos_current['141475']
                    empleados_e = saldos_current['141425']  + saldos_current['141450'] + saldos_current['141480']
                    empleados_total = saldos_current['141400']
                    empleados_deterioro = saldos_current['148800']
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
                    total_castigos = saldos_current['812000']
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

class BalSupApiViewIndicador(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            indicador_cartera = indicador_deposito = indicador_obligaciones = 0
            indicador_cap_social = indicador_cap_inst = indicador_roe = 0
            indicador_roa = indicador_ingreso_cartera = indicador_costos_deposito = 0
            indicador_credito_banco = indicador_disponible = 0
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                razon_social = nit_info.get("RazonSocial")
                periodo = item.get("periodo")
                periodo_anterior = periodo - 1
                mes_12 = 12
                mes = item.get("mes")
                mes_decimal = Decimal(mes)

                puc_codes_current = [
                    '100000', '110000', '120000', '130000', '140000', '210000', '240000', '250000',
                    '300000', '310000', '320000', '370500', '391500', '410200', '510200', '510300'
                ]
                puc_codes_previous = ['100000', '140000', '210000', '240000', '300000']

                q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)
                q_previous_period = Q(entidad_RS=razon_social, periodo=periodo_anterior, mes=mes_12) & Q(puc_codigo__in=puc_codes_previous)

                query_results_current = BalSupModel.objects.filter(q_current_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))
                query_results_previous = BalSupModel.objects.filter(q_previous_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))

                saldos_current = {puc: 0 for puc in puc_codes_current}
                for result in query_results_current:
                    saldos_current[result['puc_codigo']] = result['total_saldo']

                saldos_previous = {puc: 0 for puc in puc_codes_previous}
                for result in query_results_previous:
                    saldos_previous[result['puc_codigo']] = result['total_saldo']

                indicador_cartera = (saldos_current['140000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                indicador_deposito = (saldos_current['210000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                indicador_obligaciones = (saldos_current['240000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                indicador_cap_social = (saldos_current['310000'] / saldos_current['100000']) * 100 if saldos_current['100000'] else 0
                indicador_cap_inst = ((saldos_current['320000'] + saldos_current['370500']) / saldos_current['100000']) * 100 if saldos_current['100000'] else 0

                denominator_roe = (saldos_previous['300000'] + (saldos_current['300000'] / mes_decimal) * 12) / 2
                indicador_roe = (saldos_current['391500'] / denominator_roe) * 100 if denominator_roe else 0

                denominator_roa = (saldos_previous['100000'] + (saldos_current['100000'] / mes_decimal) * 12) / 2
                indicador_roa = (saldos_current['391500'] / denominator_roa) * 100 if denominator_roa else 0

                denominator_ingreso_cartera = (saldos_previous['140000'] + (saldos_current['140000'] / mes_decimal) * 12) / 2
                indicador_ingreso_cartera = (saldos_current['410200'] / denominator_ingreso_cartera) * 100 if denominator_ingreso_cartera else 0

                denominator_costos_deposito = (saldos_previous['210000'] + (saldos_current['210000'] / mes_decimal) * 12) / 2
                indicador_costos_deposito = (saldos_current['510200'] / denominator_costos_deposito) * 100 if denominator_costos_deposito else 0

                denominator_credito_banco = (saldos_previous['240000'] + (saldos_current['240000'] / mes_decimal) * 12) / 2
                indicador_credito_banco = (saldos_current['510300'] / denominator_credito_banco) * 100 if denominator_credito_banco else 0

                denominator_disponible = saldos_current['210000']
                indicador_disponible = ((saldos_current['110000'] + saldos_current['120000'] + saldos_current['130000'] - (saldos_current['250000'] * 20 / 100)) / denominator_disponible) * 100 if denominator_disponible else 0

                results.append({
                    'entidad_RS': razon_social,
                    'sigla': nit_info.get("sigla"),
                    'periodo': periodo,
                    'mes': mes,
                    'indicadorCartera': indicador_cartera,#
                    'indicadorDeposito': indicador_deposito,#
                    'indicadorObligaciones': indicador_obligaciones,#
                    'indicadorCapSocial': indicador_cap_social,#
                    'indicadorCapInst': indicador_cap_inst,#                    
                    'indicadorRoe': indicador_roe,
                    'indicadorRoa': indicador_roa,
                    'indicadorIngCartera': indicador_ingreso_cartera,
                    'indicadorCostDeposito': indicador_costos_deposito,
                    'indicadorCredBanco': indicador_credito_banco,
                    'indicadorDisponible': indicador_disponible,#
                })

        sorted_results = sorted(results, key=lambda x: x['entidad_RS'])

        return Response(data=sorted_results, status=status.HTTP_200_OK)
    

class BalSupApiViewIndicadorC(APIView):
    def post(self, request):
        data = request.data

        results = []
        for item in data:
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                razon_social = nit_info.get("RazonSocial")
                periodo = item.get("periodo")
                mes = item.get("mes")

                puc_codes_current = [
                    '140800','140805','140810','140815','140820','140825','149100',
                    '141200','141205','141210','141215','141220','141225', '149300',
                    '141000','141005','141010','141015','141020','141025', '149500',
                    '140200','140205','140210','140215','140220','140225', '148900',
                    '140400','140405','140410','140415','140420','140425',
                    '141400','141405','141410','141415','141420','141425', '148800',
                    '141430','141435','141440','141445','141450',
                    '141460','141465','141470','141475','141480','812000'
                ]

                q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)

                query_results_current = BalSupModel.objects.filter(q_current_period).values('puc_codigo').annotate(total_saldo=Sum('saldo'))

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

                    consumo_a = saldos_current['140805']
                    consumo_b = saldos_current['140810']
                    consumo_c = saldos_current['140815'] 
                    consumo_d = saldos_current['140820']
                    consumo_e = saldos_current['140825'] 
                    consumo_total = saldos_current['140800']
                    denominator_consumo_ind_mora = saldos_current['140805'] + saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']
                    consumo_ind_mora = ((saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']) / denominator_consumo_ind_mora) * 100 if denominator_consumo_ind_mora else 0   
                    denominator_consumo_cartera_improductiva = saldos_current['140805'] + saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']
                    consumo_cartera_improductiva = ((saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']) / denominator_consumo_cartera_improductiva) * 100 if denominator_consumo_cartera_improductiva else 0
                    consumo_deterioro = saldos_current['149100']
                    denominator_consumo_porc_cobertura = saldos_current['140810'] + saldos_current['140815'] + saldos_current['140820'] + saldos_current['140825']
                    consumo_porc_cobertura = (saldos_current['149100'] / denominator_consumo_porc_cobertura) * 100 if denominator_consumo_porc_cobertura else 0

                    microcredito_a = saldos_current['141205']
                    microcredito_b = saldos_current['141210']
                    microcredito_c = saldos_current['141215'] 
                    microcredito_d = saldos_current['141220']
                    microcredito_e = saldos_current['141225'] 
                    microcredito_total = saldos_current['141200']
                    microcredito_deterioro = saldos_current['149300']
                    denominator_microcredito_ind_mora = microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_ind_mora = ((microcredito_b + microcredito_c + microcredito_d + microcredito_e) / denominator_microcredito_ind_mora) * 100 if denominator_microcredito_ind_mora else 0
                    denominator_microcredito_cartera_improductiva = denominator_microcredito_ind_mora
                    microcredito_cartera_improductiva = ((microcredito_c + microcredito_d + microcredito_e) / denominator_microcredito_cartera_improductiva) * 100 if denominator_microcredito_cartera_improductiva else 0
                    denominator_microcredito_porc_cobertura = microcredito_b + microcredito_c + microcredito_d + microcredito_e
                    microcredito_porc_cobertura = (microcredito_deterioro / denominator_microcredito_porc_cobertura) * 100 if denominator_microcredito_porc_cobertura else 0

                    comercial_a = saldos_current['141005']
                    comercial_b = saldos_current['141010']
                    comercial_c = saldos_current['141015'] 
                    comercial_d = saldos_current['141020']
                    comercial_e = saldos_current['141025'] 
                    comercial_total = saldos_current['141000']
                    comercial_deterioro = saldos_current['149500']
                    denominator_comercial_ind_mora = comercial_a + comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_ind_mora = ((comercial_b + comercial_c + comercial_d + comercial_e) / denominator_comercial_ind_mora) * 100 if denominator_comercial_ind_mora else 0
                    denominator_comercial_cartera_improductiva = denominator_comercial_ind_mora
                    comercial_cartera_improductiva = ((comercial_c + comercial_d + comercial_e) / denominator_comercial_cartera_improductiva) * 100 if denominator_comercial_cartera_improductiva else 0
                    denominator_comercial_porc_cobertura = comercial_b + comercial_c + comercial_d + comercial_e
                    comercial_porc_cobertura = (comercial_deterioro / denominator_comercial_porc_cobertura) * 100 if denominator_comercial_porc_cobertura else 0

                    vivienda_a = saldos_current['140205'] + saldos_current['140405']
                    vivienda_b = saldos_current['140210'] + saldos_current['140410']
                    vivienda_c = saldos_current['140215'] + saldos_current['140415']
                    vivienda_d = saldos_current['140220'] + saldos_current['140420']
                    vivienda_e = saldos_current['140225'] + saldos_current['140425']
                    vivienda_total = saldos_current['140200'] + saldos_current['140400']
                    vivienda_deterioro = saldos_current['148900']
                    denominator_vivienda_ind_mora = vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_ind_mora = ((vivienda_b + vivienda_c + vivienda_d + vivienda_e) / denominator_vivienda_ind_mora) * 100 if denominator_vivienda_ind_mora else 0
                    denominator_vivienda_cartera_improductiva = denominator_vivienda_ind_mora
                    vivienda_cartera_improductiva = ((vivienda_c + vivienda_d + vivienda_e) / denominator_vivienda_cartera_improductiva) * 100 if denominator_vivienda_cartera_improductiva else 0
                    denominator_vivienda_porc_cobertura = vivienda_b + vivienda_c + vivienda_d + vivienda_e
                    vivienda_porc_cobertura = (vivienda_deterioro / denominator_vivienda_porc_cobertura) * 100 if denominator_vivienda_porc_cobertura else 0

                    empleados_a = saldos_current['141405'] + saldos_current['141430'] + saldos_current['141460']
                    empleados_b = saldos_current['141410'] + saldos_current['141435'] + saldos_current['141465']
                    empleados_c = saldos_current['141415'] + saldos_current['141440'] + saldos_current['141470']
                    empleados_d = saldos_current['141420'] + saldos_current['141445'] + saldos_current['141475']
                    empleados_e = saldos_current['141425'] + saldos_current['141450'] + saldos_current['141480']
                    empleados_total = saldos_current['141400']
                    empleados_deterioro = saldos_current['148800']
                    denominator_empleados_ind_mora = empleados_a + empleados_b + empleados_c + empleados_d + empleados_e
                    empleados_ind_mora = ((empleados_b + empleados_c + empleados_d + empleados_e) / denominator_empleados_ind_mora) * 100 if denominator_empleados_ind_mora else 0
                    denominator_empleados_cartera_improductiva = denominator_empleados_ind_mora
                    empleados_cartera_improductiva = ((empleados_c + empleados_d + empleados_e) / denominator_empleados_cartera_improductiva) * 100 if denominator_empleados_cartera_improductiva else 0
                    denominator_empleados_porc_cobertura = empleados_b + empleados_c + empleados_d + empleados_e
                    empleados_porc_cobertura = (empleados_deterioro / denominator_empleados_porc_cobertura) * 100 if denominator_empleados_porc_cobertura else 0

                    total_a = consumo_a + microcredito_a + comercial_a + vivienda_a + empleados_a
                    total_b = consumo_b + microcredito_b + comercial_b + vivienda_b + empleados_b
                    total_c = consumo_c + microcredito_c + comercial_c + vivienda_c + empleados_c
                    total_d = consumo_d + microcredito_d + comercial_d + vivienda_d + empleados_d
                    total_e = consumo_e + microcredito_e + comercial_e + vivienda_e + empleados_e
                    total_castigos = saldos_current['812000']
                    total_total = consumo_total + microcredito_total + comercial_total + vivienda_total + empleados_total
                    total_deterioro = consumo_deterioro + microcredito_deterioro + comercial_deterioro + vivienda_deterioro + empleados_deterioro
                    denominator_total_ind_mora = total_a + total_b + total_c + total_d + total_e
                    total_ind_mora = ((total_b + total_c + total_d + total_e) / denominator_total_ind_mora) * 100 if denominator_total_ind_mora else 0
                    denominator_total_porc_cobertura = total_b + total_c + total_d + total_e
                    total_porc_cobertura = (total_deterioro / denominator_total_porc_cobertura) * 100 if denominator_total_porc_cobertura else 0

                results.append({
                    'entidad_RS': razon_social,
                    'sigla': nit_info.get("sigla"),
                    'periodo': periodo,
                    'mes': mes,
                    'consumoA': consumo_a,#
                    'consumoB': consumo_b,#
                    'consumoC': consumo_c,#
                    'consumoD': consumo_d,#
                    'consumoE': consumo_e,#
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