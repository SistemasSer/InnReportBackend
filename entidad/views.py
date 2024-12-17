import logging
import requests
from django.db.models import Case, When, IntegerField, DecimalField, Value, Subquery, OuterRef
from django.db.models.functions import Coalesce
from decimal import Decimal, InvalidOperation
from datetime import datetime
from rest_framework import serializers, generics
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
    nit_str = str(nit).zfill(9)
    dv_str = str(dv).zfill(1) 

    formatted_nit_dv = f"{nit_str[:3]}-{nit_str[3:6]}-{nit_str[6:]}-{dv_str}"
    return formatted_nit_dv


def get_month_name(month_input):
    month_names = [
        "ENERO", "FEBRERO", "MARZO", "ABRIL",
        "MAYO", "JUNIO", "JULIO", "AGOSTO",
        "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
    ]

    if isinstance(month_input, int):
        if 1 <= month_input <= 12:
            return month_names[month_input - 1]
        else:
            raise ValueError("Número de mes inválido. Debe estar entre 1 y 12.")

    elif isinstance(month_input, str):
        month_input = month_input.upper()
        if month_input in month_names:
            return month_names.index(month_input) + 1
        else:
            raise ValueError("Nombre de mes inválido. Debe ser uno de los meses en mayúsculas.")
    else:
        raise TypeError("El input debe ser un número entero o un string en mayúsculas.")

def clean_currency_value_Decimal(value):
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

def get_month_from_date(date_string):
    try:
        date_part, time_part = date_string.split('T')
        year, month, day = date_part.split('-')

        month = int(month)
        if 1 <= month <= 12:
            return month
        else:
            raise ValueError("Mes inválido. Debe estar entre 1 y 12.")
    
    except ValueError as e:
        raise ValueError("Formato de fecha incorrecto o inválido: " + str(e))

def obtener_saldo_y_periodo(queryset, puc_param, grupo_activo_values):
    baseUrl_entidadesSolidaria = "https://www.datos.gov.co/resource/tic6-rbue.json?$limit=500000"
    baseUrl_entidadesFinanciera = "https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=500000"

    periodo_actual = datetime.now().year

    print(f"Periodo actual: {periodo_actual}")

    fecha1_str = f"{periodo_actual}-01-01T00:00:00.000"
    fecha2_str = f"{periodo_actual}-12-31T23:59:59.999"

    def obtener_datos_solidaria(periodo):
        # url_Solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND codrenglon='{puc_param}'"
        url_Solidaria = f"{baseUrl_entidadesSolidaria}&$where=a_o='{periodo}' AND codrenglon='{puc_param}' AND codigo_entidad IN ('90', '93', '127', '197', '246', '271', '284', '330', '374', '424', '446', '561', '631', '715', '752', '757', '821', '824', '902', '912', '970', '978', '991', '997', '1093', '1100', '1119', '1128', '1190', '1198', '1266', '1302', '1306', '1319', '1339', '1344', '1355', '1356', '1360', '1365', '1370', '1377', '1386', '1388', '1390', '1411', '1414', '1421', '1437', '1442', '1450', '1457', '1459', '1477', '1510', '1512', '1615', '1630', '1632', '1644', '1648', '1649', '1661', '1663', '1691', '1698', '1703', '1751', '1755', '1756', '1760', '1805', '1811', '1813', '1824', '1827', '1851', '1852', '1859', '1889', '1894', '1961', '1991', '1997', '2006', '2012', '2021', '2024', '2028', '2058', '2077', '2078', '2109', '2130', '2196', '2199', '2223', '2231', '2246', '2336', '2337', '2392', '2398', '2426', '2434', '2483', '2506', '2520', '2525', '2540', '2560', '2641', '2655', '2660', '2675', '2688', '2773', '2783', '2814', '2829', '2871', '2878', '3018', '3033', '3034', '3048', '3049', '3070', '3072', '3123', '3246', '3249', '3278', '3282', '3316', '3341', '3360', '3386', '3391', '3399', '3400', '3402', '3438', '3446', '3488', '3620', '3640', '4004', '4011', '4054', '4403', '4458', '4617', '7099', '7571', '7961', '8024', '8202', '8480', '8487', '8825', '10300', '10555', '11085', '11128', '11327', '11488', '13022', '13024', '13813', '15236', '20009')"
        print(f"URL Solidaria: {url_Solidaria}")
        response_Solidaria = requests.get(url_Solidaria)
        print(f"Response Solidaria Status: {response_Solidaria.status_code}")
        datos = response_Solidaria.json() if response_Solidaria.status_code == 200 and response_Solidaria.json() else []
        print(f"Cantidad de datos Solidaria (año {periodo}): {len(datos)}")
        return datos

    def obtener_datos_financiera(periodo):
        if puc_param == "350000":
            puc_param_F = "391500"
        elif puc_param == "230000":
            puc_param_F = "240000"
        else:
            puc_param_F = puc_param

        url_Financiera = f"{baseUrl_entidadesFinanciera}&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND cuenta='{puc_param_F}' AND moneda ='0' AND tipo_entidad IN ('1', '4', '32')"
        print(f"URL Financiera: {url_Financiera}")
        response_Financiera = requests.get(url_Financiera)
        print(f"Response Financiera Status: {response_Financiera.status_code}")
        datos = response_Financiera.json() if response_Financiera.status_code == 200 and response_Financiera.json() else []
        print(f"Cantidad de datos Financiera (año {periodo}): {len(datos)}")
        return datos

    # Buscar datos de la API para el año actual, si no hay, buscar en el año anterior
    def obtener_datos(periodo):
        Solidaria_data = obtener_datos_solidaria(periodo)
        Financiera_data = obtener_datos_financiera(periodo)
        return Solidaria_data, Financiera_data

    # Intentar obtener los datos del año actual, si no se encuentran, buscar el año anterior
    def buscar_datos(periodo_inicial):
        periodo = periodo_inicial
        while periodo >= 2000:  # Por ejemplo, podrías poner un límite de búsqueda de 2000 hacia atrás
            Solidaria_data, Financiera_data = obtener_datos(periodo)
            if Solidaria_data or Financiera_data:  # Si encontramos datos, los retornamos
                return Solidaria_data, Financiera_data, periodo  # Retornar también el año en que se encontraron los datos
            periodo -= 1  # Buscar en el año anterior si no hay datos
        return [], [], periodo  # Retornar vacío si no se encuentran datos en los años anteriores

    # Intentar obtener los datos
    Solidaria_data, Financiera_data, periodo_final = buscar_datos(periodo_actual)

    def generar_resultado_Solidaria():
        for entidad in queryset:
            if entidad.TipoEntidad == 2:
                nit = entidad.Nit
                dv = entidad.Dv
                formatted_nit_dv = format_nit_dv(nit, dv)

                entity_data = (data for data in Solidaria_data if data['nit'] == formatted_nit_dv)
                most_recent_data = max(
                    entity_data,
                    key=lambda x: convertir_mes_a_numero(x['mes']),
                    default=None
                )

                if most_recent_data:
                    mes_numero = get_month_name(most_recent_data['mes'])
                    saldo_decimal = clean_currency_value_Decimal(most_recent_data['valor_en_pesos'])
                    grupo_activo = determinar_grupo(saldo_decimal)

                    fecha_tamaño = f"{(most_recent_data['mes']).upper()} - {periodo_final}"

                    print(f"MES_NUMERO {mes_numero}, SALDO_DECIMAL {saldo_decimal}, FECHA_TAMAÑO {fecha_tamaño}")

                    if grupo_activo in grupo_activo_values:
                        yield {
                            'id': entidad.id,
                            'Nit': entidad.Nit,
                            'Dv': entidad.Dv,
                            'RazonSocial': entidad.RazonSocial,
                            'Sigla': entidad.Sigla,
                            'TipoEntidad': entidad.TipoEntidad,
                            'Departamento': entidad.Departamento,
                            'Gremio': entidad.Gremio,
                            'Grupo_Activo': grupo_activo,
                            'periodo': periodo_final,  # Usar el año en que se encontraron los datos
                            'mes': mes_numero,
                            'fecha_tamaño': fecha_tamaño
                        }

    def generar_resultado_Financiera():
        for entidad in queryset:
            if entidad.TipoEntidad != 2:
                nit = entidad.Nit
                dv = entidad.Dv
                razon_social = entidad.RazonSocial

                entity_data = (data for data in Financiera_data if data['nombre_entidad'] == razon_social)
                most_recent_data = max(
                    entity_data,
                    key=lambda x: datetime.strptime(x['fecha_corte'], '%Y-%m-%dT%H:%M:%S.%f'),
                    default=None
                )

                if most_recent_data:
                    mes_numero_financiera = get_month_from_date(most_recent_data['fecha_corte'])
                    saldo_decimal = float(most_recent_data['valor'])
                    grupo_activo = determinar_grupo(saldo_decimal)

                    fecha_tamaño = f"{get_month_name(mes_numero_financiera)} - {periodo_final}"

                    print(f"MES_NUMERO {mes_numero_financiera}, SALDO_DECIMAL {saldo_decimal}, FECHA_TAMAÑO {fecha_tamaño}")

                    if grupo_activo in grupo_activo_values:
                        yield {
                            'id': entidad.id,
                            'Nit': entidad.Nit,
                            'Dv': entidad.Dv,
                            'RazonSocial': entidad.RazonSocial,
                            'Sigla': entidad.Sigla,
                            'TipoEntidad': entidad.TipoEntidad,
                            'Departamento': entidad.Departamento,
                            'Gremio': entidad.Gremio,
                            'Grupo_Activo': grupo_activo,
                            'periodo': periodo_final,  # Usar el año en que se encontraron los datos
                            'mes': mes_numero_financiera,
                            'fecha_tamaño': fecha_tamaño
                        }

    return list(generar_resultado_Solidaria()) + list(generar_resultado_Financiera())

def convertir_mes_a_numero(mes):
    MESES = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    mes = mes.lower()
    numero_mes = MESES.get(mes)
    if numero_mes is None:
        raise ValueError(f"Mes inválido: '{mes}'. Asegúrate de que sea un nombre válido.")
    return numero_mes

class EntidadApiView(APIView):

    def get(self, request):
        tipo_entidad_param = request.query_params.getlist("TipoEntidad")
        departamento_param = request.query_params.getlist("Departamento")
        gremio_param = request.query_params.getlist("Gremio")
        grupo_activo_param = request.query_params.getlist("Grupo_Activo")
        puc_param = request.query_params.get("puc")


        try:
            tipo_entidad_values = [int(value) for value in tipo_entidad_param] if tipo_entidad_param else []
            departamento_values = departamento_param if departamento_param else []
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
        if departamento_values:
            queryset = queryset.filter(Departamento__in=departamento_values)
        if gremio_values:
            queryset = queryset.filter(Gremio__in=gremio_values)
        if grupo_activo_values:
            resultado = obtener_saldo_y_periodo(queryset, puc_param, grupo_activo_values)         
            return Response(status=status.HTTP_200_OK, data=resultado)
        else:
            entidades = queryset.values('id', 'Nit', 'Dv', 'RazonSocial', 'Sigla', 'TipoEntidad', 'CodigoSuper', 'Descripcion', 'Departamento', 'Ciudad', 'Direccion', 'Telefono', 'Email', 'CIIU', 'RepresentanteLegal', 'Gremio')

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

class EntidadModelUpdateView(generics.UpdateAPIView):
    queryset = EntidadModel.objects.all() 
    serializer_class = EntidadSerializer 

    def get_object(self):
        obj = super().get_object()
        return obj

    def update(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=True)

        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        else:
            return Response(serializer.errors, status=400)