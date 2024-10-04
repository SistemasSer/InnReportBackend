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

from datetime import datetime, timedelta
from collections import defaultdict
import requests


logger = logging.getLogger("django")

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
            Q(periodo__in=[data["periodo"] for data in data_list])
            & Q(mes__in=[data["mes"] for data in data_list])
            & Q(entidad_RS__in=[data["entidad_RS"] for data in data_list])
            & Q(puc_codigo__in=[data["puc_codigo"] for data in data_list])
        )
        existing_dict = {
            (
                instance.periodo,
                instance.mes,
                instance.entidad_RS,
                instance.puc_codigo,
            ): instance
            for instance in existing_instances
        }
        for data in data_list:
            key = (data["periodo"], data["mes"], data["entidad_RS"], data["puc_codigo"])
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
                BalSupModel.objects.bulk_update(
                    update_instances,
                    [
                        "periodo",
                        "mes",
                        "entidad_RS",
                        "puc_codigo",
                        "saldo",
                        "updated_at",
                    ],
                )

        return Response(
            status=status.HTTP_200_OK,
            data={"created": len(new_instances), "updated": len(update_instances)},
        )

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



class BalSupApiViewA(APIView):
    def post(self, request):
        data = request.data
        transformed_results = {}

        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"

        for item in data:
            periodo = item.get("periodo")
            mes = item.get("mes")
            puc_codigo = item.get("puc_codigo")

            fecha1 = datetime(periodo, mes, 1)
            fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            fecha1_str = fecha1.strftime("%Y-%m-%dT00:00:00")
            fecha2_str = fecha2.strftime("%Y-%m-%dT23:59:59")
            url_otras = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta='{puc_codigo}' AND moneda ='0'"
            all_data = []

            response_otras = requests.get(url_otras)
            print(f"Response: {response_otras.json()}")

            if response_otras.status_code == 200:
                all_data.extend(response_otras.json())

            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                razon_social = nit_info.get("RazonSocial")
                sigla = nit_info.get("sigla", razon_social)
                key = (razon_social, puc_codigo)
                if key not in transformed_results:
                    transformed_results[key] = {
                        "razon_social": razon_social,
                        "sigla": sigla,
                        "puc_codigo": puc_codigo,
                        "saldos": [],
                    }
                entity_data = [data for data in all_data if data['nombre_entidad'] == razon_social]

                q_objects = Q(
                    entidad_RS=razon_social,
                    periodo=periodo,
                    puc_codigo=puc_codigo,
                    mes=mes,
                )
                query_results = BalSupModel.objects.filter(q_objects).values(
                    "entidad_RS", "periodo", "puc_codigo", "mes", "saldo"
                )
                saldo_en_bd = False
                if entity_data:
                    for result in entity_data:
                        transformed_results[key]["saldos"].append(
                            {"periodo": periodo, "mes": mes, "saldo": float(result["valor"])}
                        )
                else:
                    if query_results:
                        for result in query_results:
                            transformed_results[key]["saldos"].append(
                                {"periodo": result["periodo"], "mes": result["mes"], "saldo": float(result["saldo"])}
                            )
                            saldo_en_bd = True
                if not entity_data and not saldo_en_bd:
                    transformed_results[key]["saldos"].append(
                        {"periodo": periodo, "mes": mes, "saldo": 0.0}
                    )
        for key, value in transformed_results.items():
            value["saldos"] = value["saldos"][:6]

        final_results = list(transformed_results.values())
        return Response(data=final_results, status=status.HTTP_200_OK)


class BalSupApiViewIndicador(APIView):
    def post(self, request):
        data = request.data
        results = []
        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"
        for item in data:
            periodo = item.get("periodo")
            mes = item.get("mes")
            mes_decimal = Decimal(mes)
            periodo_anterior = periodo - 1
            mes_ultimo = 12

            fecha1 = datetime(periodo, mes, 1)
            fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            fecha1_str = fecha1.strftime("%Y-%m-%dT00:00:00")
            fecha2_str = fecha2.strftime("%Y-%m-%dT23:59:59")

            url_financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND moneda ='0'"

            all_data = []

            response_financiera = requests.get(url_financiera)
            if response_financiera.status_code == 200:
                all_data.extend(response_financiera.json())

            saldos_current = defaultdict(lambda: defaultdict(Decimal))
            for result in all_data:
                razon_social = result.get("nombre_entidad")
                cuenta = result.get("cuenta")
                total_saldo = Decimal(result.get("valor", 0))
                saldos_current[razon_social][cuenta] += total_saldo

            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                razon_social = nit_info.get("RazonSocial")

                puc_codes_current = [
                    "100000", "110000", "120000", "130000", "140000",
                    "210000", "240000", "250000", "300000", "310000",
                    "320000", "370500", "391500", "410200", "510200", "510300",
                ]

                # Buscar en la base de datos si no hay datos en la API
                if not any(saldos_current[razon_social].values()):
                    q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)
                    query_results_current = BalSupModel.objects.filter(q_current_period).values("puc_codigo").annotate(total_saldo=Sum("saldo"))

                    for result in query_results_current:
                        saldos_current[razon_social][result["puc_codigo"]] = result["total_saldo"]
                # Si sigue sin haber datos, asignar saldo 0
                if not any(saldos_current[razon_social].values()):
                    for puc in puc_codes_current:
                        saldos_current[razon_social][puc] = 0

            # Obtener datos del periodo anterior
            fecha1_prev = datetime(periodo_anterior, mes_ultimo, 1)
            fecha2_prev = (fecha1_prev + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            fecha1_str_prev = fecha1_prev.strftime("%Y-%m-%dT00:00:00")
            fecha2_str_prev = fecha2_prev.strftime("%Y-%m-%dT23:59:59")

            url_financiera_prev = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str_prev}' AND '{fecha2_str_prev}' AND moneda ='0'"
            all_data_prev = []

            response_financiera_prev = requests.get(url_financiera_prev)
            if response_financiera_prev.status_code == 200:
                all_data_prev.extend(response_financiera_prev.json())

            # Agrupar datos del periodo anterior
            saldos_previous = defaultdict(lambda: defaultdict(Decimal))
            for result in all_data_prev:
                razon_social = result.get("nombre_entidad")
                cuenta = result.get("cuenta")
                total_saldo = Decimal(result.get("valor", 0))
                saldos_previous[razon_social][cuenta] += total_saldo
                
            for nit_info in item.get("nit", {}).get("superfinanciera", []):
                razon_social = nit_info.get("RazonSocial")

                puc_codes_prev = ["100000", "140000", "210000", "240000", "300000"]

                if not any(saldos_previous[razon_social].values()):
                    q_previous_period = Q(entidad_RS=razon_social, periodo=periodo_anterior, mes=mes_ultimo) & Q(puc_codigo__in=puc_codes_prev)
                    query_results_previous = BalSupModel.objects.filter(q_previous_period).values("puc_codigo").annotate(total_saldo=Sum("saldo"))

                    for result in query_results_previous:
                        saldos_previous[razon_social][result["puc_codigo"]] = result["total_saldo"]

                # Asignar 0 si aún no hay datos
                if not any(saldos_previous[razon_social].values()):
                    for puc in puc_codes_prev:
                        saldos_previous[razon_social][puc] = 0

                # Calcular indicadores
                try:
                    indicador_cartera = (saldos_current[razon_social]["140000"] / saldos_current[razon_social]["100000"] * 100) if saldos_current[razon_social]["100000"] else 0
                    indicador_deposito = (saldos_current[razon_social]["210000"] / saldos_current[razon_social]["100000"] * 100) if saldos_current[razon_social]["100000"] else 0
                    indicador_obligaciones = (saldos_current[razon_social]["240000"] / saldos_current[razon_social]["100000"] * 100) if saldos_current[razon_social]["100000"] else 0
                    indicador_cap_social = (saldos_current[razon_social]["310000"] / saldos_current[razon_social]["100000"] * 100) if saldos_current[razon_social]["100000"] else 0
                    indicador_cap_inst = ((saldos_current[razon_social]["320000"] + saldos_current[razon_social]["370500"]) / saldos_current[razon_social]["100000"] * 100) if saldos_current[razon_social]["100000"] else 0
                    denominator_roe = (saldos_previous[razon_social]["300000"] + (saldos_current[razon_social]["300000"] / mes_decimal) * 12) / 2
                    indicador_roe = (saldos_current[razon_social]["391500"] / denominator_roe * 100) if denominator_roe else 0
                    denominator_roa = (saldos_previous[razon_social]["100000"] + (saldos_current[razon_social]["100000"] / mes_decimal) * 12) / 2
                    indicador_roa = (saldos_current[razon_social]["391500"] / denominator_roa * 100) if denominator_roa else 0
                    denominator_ingreso_cartera = (saldos_previous[razon_social]["140000"] + (saldos_current[razon_social]["140000"] / mes_decimal) * 12) / 2
                    indicador_ingreso_cartera = (saldos_current[razon_social]["410200"] / denominator_ingreso_cartera * 100) if denominator_ingreso_cartera else 0
                    denominator_costos_deposito = (saldos_previous[razon_social]["210000"] + (saldos_current[razon_social]["210000"] / mes_decimal) * 12) / 2
                    indicador_costos_deposito = (saldos_current[razon_social]["510200"] / denominator_costos_deposito * 100) if denominator_costos_deposito else 0
                    denominator_credito_banco = (saldos_previous[razon_social]["240000"] + (saldos_current[razon_social]["240000"] / mes_decimal) * 12) / 2
                    indicador_credito_banco = (saldos_current[razon_social]["510300"] / denominator_credito_banco * 100) if denominator_credito_banco else 0
                    denominator_disponible = saldos_current[razon_social]["210000"]
                    indicador_disponible = (
                        (saldos_current[razon_social]["110000"] + saldos_current[razon_social]["120000"] + saldos_current[razon_social]["130000"] - (saldos_current[razon_social]["250000"] * 20 / 100)) / denominator_disponible * 100
                    ) if denominator_disponible else 0

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
                except ZeroDivisionError:
                    results.append(
                        {
                            "entidad_RS": razon_social,
                            "sigla": nit_info.get("sigla"),
                            "periodo": periodo,
                            "mes": mes,
                            "error": "Cálculo fallido debido a división por cero.",
                        }
                    )

        sorted_results = sorted(results, key=lambda x: x["entidad_RS"])

        return Response(data=sorted_results, status=status.HTTP_200_OK)

def safe_divide(numerator, denominator):
    return (numerator / denominator * 100) if denominator else 0

class BalSupApiViewIndicadorC(APIView):
    def post(self, request):
        data = request.data
        results = []
        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"

        for item in data:
            periodo = item.get("periodo")
            mes = item.get("mes")

            fecha1 = datetime(periodo, mes, 1)
            fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            fecha1_str = fecha1.strftime("%Y-%m-%dT00:00:00")
            fecha2_str = fecha2.strftime("%Y-%m-%dT23:59:59")
            # Consultar API
            url_financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND moneda ='0'"
            response_financiera = requests.get(url_financiera)

            if response_financiera.status_code == 200:
                all_data = response_financiera.json()

                saldos_current = defaultdict(lambda: defaultdict(Decimal))

                for result in all_data:
                    razon_social = result.get("nombre_entidad")
                    cuenta = result.get("cuenta")
                    total_saldo = Decimal(result.get("valor", 0))
                    saldos_current[razon_social][cuenta] += total_saldo

                puc_codes_current = ["140800", "140805", "140810", "140815", "140820", "140825","149100", "141200", "141205", "141210", "141215", "141220","141225", "149300", "141000", "141005", "141010", "141015","141020", "141025", "149500", "140200", "140205", "140210","140215", "140220", "140225", "148900", "140400", "140405","140410", "140415", "140420", "140425", "141400", "141405","141410", "141415", "141420", "141425", "148800", "141430","141435", "141440", "141445", "141450", "141460", "141465","141470", "141475", "141480", "812000",]

                for nit_info in item.get("nit", {}).get("superfinanciera", []):
                    razon_social = nit_info.get("RazonSocial")
                    q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo__in=puc_codes_current)
                    query_results_current = BalSupModel.objects.filter(q_current_period).values("puc_codigo").annotate(total_saldo=Sum("saldo"))
                    for result in query_results_current:
                        saldos_current[razon_social][result["puc_codigo"]] = result["total_saldo"]

                for nit_info in item.get("nit", {}).get("superfinanciera", []):
                    razon_social = nit_info.get("RazonSocial")

                    if not any(saldos_current[razon_social].values()):
                        for puc in puc_codes_current:
                            saldos_current[razon_social][puc] = 0

                    #consumo indicadores
                    consumo_a = saldos_current[razon_social]["140805"]
                    consumo_b = saldos_current[razon_social]["140810"]
                    consumo_c = saldos_current[razon_social]["140815"]
                    consumo_d = saldos_current[razon_social]["140820"]
                    consumo_e = saldos_current[razon_social]["140825"]
                    consumo_total = saldos_current[razon_social]["140800"]
                    consumo_ind_mora = safe_divide((consumo_b + consumo_c + consumo_d + consumo_e), (consumo_a + consumo_b + consumo_c + consumo_d + consumo_e))
                    consumo_deterioro = saldos_current[razon_social]["149100"]
                    denominator_consumo_ind_mora = (consumo_a + consumo_b + consumo_c + consumo_d + consumo_e)
                    consumo_cartera_improductiva = safe_divide((consumo_c + consumo_d + consumo_e), denominator_consumo_ind_mora)
                    denominator_consumo_porc_cobertura = (consumo_b + consumo_c + consumo_d + consumo_e)
                    consumo_porc_cobertura = safe_divide(consumo_deterioro, denominator_consumo_porc_cobertura)

                    #microcredito
                    microcredito_a = saldos_current[razon_social]["141205"]
                    microcredito_b = saldos_current[razon_social]["141210"]
                    microcredito_c = saldos_current[razon_social]["141215"]
                    microcredito_d = saldos_current[razon_social]["141220"]
                    microcredito_e = saldos_current[razon_social]["141225"]
                    microcredito_total = saldos_current[razon_social]["141200"]
                    microcredito_deterioro = saldos_current[razon_social]["149300"]
                    denominator_microcredito_ind_mora = (microcredito_a + microcredito_b + microcredito_c + microcredito_d + microcredito_e)
                    microcredito_ind_mora = safe_divide((microcredito_b + microcredito_c + microcredito_d + microcredito_e), denominator_microcredito_ind_mora)
                    microcredito_cartera_improductiva = safe_divide((microcredito_c + microcredito_d + microcredito_e), denominator_microcredito_ind_mora)
                    denominator_microcredito_porc_cobertura = (microcredito_b + microcredito_c + microcredito_d + microcredito_e)
                    microcredito_porc_cobertura = safe_divide(microcredito_deterioro, denominator_microcredito_porc_cobertura)

                    #comercioal
                    comercial_a = saldos_current[razon_social]["141005"]
                    comercial_b = saldos_current[razon_social]["141010"]
                    comercial_c = saldos_current[razon_social]["141015"]
                    comercial_d = saldos_current[razon_social]["141020"]
                    comercial_e = saldos_current[razon_social]["141025"]
                    comercial_total = saldos_current[razon_social]["141000"]
                    comercial_deterioro = saldos_current[razon_social]["149500"]
                    denominator_comercial_ind_mora = (comercial_a + comercial_b + comercial_c + comercial_d + comercial_e)
                    comercial_ind_mora = safe_divide((comercial_b + comercial_c + comercial_d + comercial_e), denominator_comercial_ind_mora)
                    comercial_cartera_improductiva = safe_divide((comercial_c + comercial_d + comercial_e), denominator_comercial_ind_mora)
                    denominator_comercial_porc_cobertura = (comercial_b + comercial_c + comercial_d + comercial_e)
                    comercial_porc_cobertura = safe_divide(comercial_deterioro, denominator_comercial_porc_cobertura)

                    #vivienda 
                    vivienda_a = saldos_current[razon_social]["140205"] + saldos_current[razon_social]["140405"]
                    vivienda_b = saldos_current[razon_social]["140210"] + saldos_current[razon_social]["140410"]
                    vivienda_c = saldos_current[razon_social]["140215"] + saldos_current[razon_social]["140415"]
                    vivienda_d = saldos_current[razon_social]["140220"] + saldos_current[razon_social]["140420"]
                    vivienda_e = saldos_current[razon_social]["140225"] + saldos_current[razon_social]["140425"]
                    vivienda_total = saldos_current[razon_social]["140200"] + saldos_current[razon_social]["140400"]
                    vivienda_deterioro = saldos_current[razon_social]["148900"]
                    denominator_vivienda_ind_mora = (vivienda_a + vivienda_b + vivienda_c + vivienda_d + vivienda_e)
                    vivienda_ind_mora = safe_divide((vivienda_b + vivienda_c + vivienda_d + vivienda_e), denominator_vivienda_ind_mora)
                    vivienda_cartera_improductiva = safe_divide((vivienda_c + vivienda_d + vivienda_e), denominator_vivienda_ind_mora)
                    denominator_vivienda_porc_cobertura = (vivienda_b + vivienda_c + vivienda_d + vivienda_e)
                    vivienda_porc_cobertura = safe_divide(vivienda_deterioro, denominator_vivienda_porc_cobertura)


                    #EMPLEADOS
                    empleados_a = (saldos_current[razon_social]["141405"] + saldos_current[razon_social]["141430"] + saldos_current[razon_social]["141460"])
                    empleados_b = (saldos_current[razon_social]["141410"] + saldos_current[razon_social]["141435"] + saldos_current[razon_social]["141465"])
                    empleados_c = (saldos_current[razon_social]["141415"] + saldos_current[razon_social]["141440"] + saldos_current[razon_social]["141470"])
                    empleados_d = (saldos_current[razon_social]["141420"] + saldos_current[razon_social]["141445"] + saldos_current[razon_social]["141475"])
                    empleados_e = (saldos_current[razon_social]["141425"] + saldos_current[razon_social]["141450"] + saldos_current[razon_social]["141480"])
                    empleados_total = (saldos_current[razon_social]["141400"] + saldos_current[razon_social]["141200"])
                    empleados_deterioro = saldos_current[razon_social]["149100"]
                    denominator_empleados_ind_mora = (empleados_a + empleados_b + empleados_c + empleados_d + empleados_e)
                    empleados_ind_mora = safe_divide((empleados_b + empleados_c + empleados_d + empleados_e), denominator_empleados_ind_mora)
                    empleados_cartera_improductiva = safe_divide((empleados_c + empleados_d + empleados_e), denominator_empleados_ind_mora)
                    denominator_empleados_porc_cobertura = (empleados_b + empleados_c + empleados_d + empleados_e)
                    empleados_porc_cobertura = safe_divide(empleados_deterioro, denominator_empleados_porc_cobertura)

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

                    # Append the result
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
    
class BalSupApiViewBalance(APIView):
    def post(self, request):
        data = request.data
        results = []
        baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"

        entidades_financieras = data.get("entidad", {}).get("superfinanciera", [])
        periodo = data.get("año")
        mes = data.get("mes")
        pucCodigo = data.get("pucCodigo")
        pucName = data.get("pucName")

        # Fechas para el periodo actual
        fecha1 = datetime(periodo, mes, 1)
        fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        fecha1_str = fecha1.strftime("%Y-%m-%dT00:00:00")
        fecha2_str = fecha2.strftime("%Y-%m-%dT23:59:59")
        # Consultar API
        url_financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta ='{pucCodigo}' AND moneda ='0'"
        response_financiera = requests.get(url_financiera)

        saldos_current = defaultdict(Decimal)

        if response_financiera.status_code == 200:
            all_data = response_financiera.json()

            for result in all_data:
                razon_social = result.get("nombre_entidad")
                total_saldo = Decimal(result.get("valor", 0))
                saldos_current[razon_social] += total_saldo
        else:
            print("No data fetched from API.")

        for nit_info in entidades_financieras:
            razon_social = nit_info.get("RazonSocial")
            if razon_social not in saldos_current:
                q_current_period = Q(entidad_RS=razon_social, periodo=periodo, mes=mes) & Q(puc_codigo=pucCodigo)
                query_results_current = BalSupModel.objects.filter(q_current_period).values("puc_codigo").annotate(total_saldo=Sum("saldo"))

                for result in query_results_current:
                    saldos_current[razon_social] += result["total_saldo"]

        # Formatear resultados
        entidades = []
        for nit_info in entidades_financieras:
            razon_social = nit_info.get("RazonSocial")
            saldo = saldos_current.get(razon_social, 0)
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