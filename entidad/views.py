import logging
import requests
from django.db.models import Case, When, IntegerField, DecimalField, Value, Subquery, OuterRef
from django.db.models.functions import Coalesce
from decimal import Decimal, InvalidOperation
from datetime import datetime
from rest_framework import serializers
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from entidad.models import EntidadModel
from balSup.models import BalSupModel
from balCoop.models import BalCoopModel
from balCoop.serializers import BalCoopSerializer 
from entidad.serializers import EntidadSerializer

logger = logging.getLogger('django')


class EntidadDefaulApiView(APIView):
    def get(self, request):
        serializer = EntidadSerializer(EntidadModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    def post(self, request):
        # res = request.data.get('name')
        serializer = EntidadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    
def determinar_grupo(saldo):
    if saldo < 10000000000:
        return 1
    elif saldo <= 50000000000:
        return 2
    elif saldo <= 200000000000:
        return 3
    elif saldo <= 500000000000:
        return 4
    else:
        return 5

    
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
        print(f"Error convirtiendo el valor: {value}")
        return Decimal(0)
    
def obtener_saldo_y_periodo(queryset, puc_param, grupo_activo_values):
    baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/tic6-rbue.json?$limit=500000"
    baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"

    resultado = []
    periodo_actual = 2024
    fecha1_str = f"{periodo_actual}-01-01T00:00:00.000"
    fecha2_str = f"{periodo_actual}-12-31T23:59:59.999"  

    # Hacer la consulta para TipoEntidad = 2
    url_tipo2 = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo_actual}' AND codrenglon='{puc_param}'"
    response_tipo2 = requests.get(url_tipo2)

    if response_tipo2.status_code != 200 or not response_tipo2.json():
        tipo2_data = []
    else:
        tipo2_data = response_tipo2.json()

    # Hacer la consulta para otras entidades
    url_otras = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta='{puc_param}' AND moneda ='0'"
    response_otras = requests.get(url_otras)

    if response_otras.status_code != 200 or not response_otras.json():
        otras_data = []
    else:
        otras_data = response_otras.json()

    # Procesar entidades de TipoEntidad = 2
    for entidad in queryset:
        if entidad.TipoEntidad == 2:
            nit = entidad.Nit
            dv = entidad.Dv
            formatted_nit_dv = format_nit_dv(nit, dv)

            entity_data = [data for data in tipo2_data if data['nit'] == formatted_nit_dv]

            if entity_data:
                most_recent_data = max(entity_data, key=lambda x: x['mes'])
                saldo_decimal = clean_currency_value_Decimal((most_recent_data['valor_en_pesos']))
                grupo_activo = determinar_grupo(saldo_decimal)

                # print(f"Entidad solidaria: {entidad}, Saldo: {saldo_decimal}, Grupo Activo: {grupo_activo}")
                # print(f"Periodo: {periodo_actual}, Mes: {most_recent_data['mes']}")

                entidad_data = {
                    'id': entidad.id,
                    'Nit': entidad.Nit,
                    'Dv': entidad.Dv,
                    'RazonSocial': entidad.RazonSocial,
                    'Sigla': entidad.Sigla,
                    'TipoEntidad': entidad.TipoEntidad,
                    'Gremio': entidad.Gremio,
                    'Grupo_Activo': grupo_activo,
                    'periodo': periodo_actual,
                    'mes': most_recent_data['mes']
                }

                if grupo_activo in grupo_activo_values:
                    resultado.append(entidad_data)

    # Procesar entidades de otros tipos
    for entidad in queryset:
        if entidad.TipoEntidad != 2:
            nit = entidad.Nit
            dv = entidad.Dv
            razon_social = entidad.RazonSocial

            entity_data = [data for data in otras_data if data['nombre_entidad'] == razon_social]

            if entity_data:
                most_recent_data = max(entity_data, key=lambda x: datetime.strptime(x['fecha_corte'], '%Y-%m-%dT%H:%M:%S.%f'))
                saldo_decimal = float(most_recent_data['valor'])  # Asegúrate de que esto sea un número
                grupo_activo = determinar_grupo(saldo_decimal)

                # print(f"Entidad Financiera: {entidad}, Saldo: {saldo_decimal}, Grupo Activo: {grupo_activo}")
                # print(f"Periodo: {periodo_actual}, Mes: {most_recent_data['fecha_corte']}")

                entidad_data = {
                    'id': entidad.id,
                    'Nit': entidad.Nit,
                    'Dv': entidad.Dv,
                    'RazonSocial': entidad.RazonSocial,
                    'Sigla': entidad.Sigla,
                    'TipoEntidad': entidad.TipoEntidad,
                    'Gremio': entidad.Gremio,
                    'Grupo_Activo': grupo_activo,
                    'periodo': periodo_actual,
                    'fecha_corte': most_recent_data['fecha_corte']
                }

                if grupo_activo in grupo_activo_values:
                    resultado.append(entidad_data)

    
    # print("---" * 20)
    # print("Resultado final:", resultado)
    # print("---" * 20)

    return resultado

class EntidadApiView(APIView):

    def get(self, request):
        tipo_entidad_param = request.query_params.getlist("TipoEntidad")
        gremio_param = request.query_params.getlist("Gremio")
        grupo_activo_param = request.query_params.getlist("Grupo_Activo")
        puc_param = request.query_params.get("puc")

        try:
            tipo_entidad_values = [int(value) for value in tipo_entidad_param] if tipo_entidad_param else []
            gremio_values = [int(value) for value in gremio_param] if gremio_param else []
            grupo_activo_values = [int(value) for value in grupo_activo_param] if grupo_activo_param else []
        except ValueError:
            return Response(
                status=status.HTTP_400_BAD_REQUEST,
                data={"error": "Invalid filter values"},
            )

        queryset = EntidadModel.objects.all()

        if tipo_entidad_values:
            queryset = queryset.filter(TipoEntidad__in=tipo_entidad_values)

        if gremio_values:
            queryset = queryset.filter(Gremio__in=gremio_values)

        if grupo_activo_values:
            print(f"QUERY: {queryset}, PUC: {puc_param}, Grupo Activo: {grupo_activo_values}")
            resultado = obtener_saldo_y_periodo(queryset, puc_param, grupo_activo_values)
            return Response(status=status.HTTP_200_OK, data=resultado)
        else:
            entidades = queryset.values('id', 'Nit', 'Dv', 'RazonSocial', 'Sigla', 'TipoEntidad', 'Gremio')
            return Response(status=status.HTTP_200_OK, data=list(entidades))

    def post(self, request):
        serializer = EntidadSerializer(data=request.data)

        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(status=status.HTTP_200_OK, data=serializer.data)


class EntidadApiViewDetail(APIView):
    def get_object(self, pk):
        try:
            return EntidadModel.objects.get(pk=pk)
        except EntidadModel.DoesNotExist:
            return None

    def get(self, request, id):
        post = self.get_object(id)
        serializer = EntidadSerializer(post)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    def put(self, request, id):
        post = self.get_object(id)
        if post == None:
            return Response(status=status.HTTP_200_OK, data={"error": "Not found data"})
        serializer = EntidadSerializer(post, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(status=status.HTTP_200_OK, data=serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, id):
        post = self.get_object(id)
        post.delete()
        response = {"deleted": True}
        return Response(status=status.HTTP_204_NO_CONTENT, data=response)
