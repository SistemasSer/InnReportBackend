import requests
import concurrent.futures
from time import sleep
from django.db.models import Q

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from collections import defaultdict
from decimal import Decimal, InvalidOperation

from datetime import datetime, timedelta

from entidad.models import EntidadModel
from balSup.models import BalSupModel
from balCoop.models import BalCoopModel

def build_dates(periodo, mes):
    fecha1 = datetime(periodo, mes, 1)
    fecha2 = (fecha1 + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    return fecha1.strftime("%Y-%m-%dT00:00:00"), fecha2.strftime("%Y-%m-%dT23:59:59")

def tipo_entidad_str(codigo):
    entidad = {
        0: "Bancos",
        1: "Cooperativas Financieras",
        2: "Cooperativas de Ahorro y Crédito",
        3: "Compañías de Financiamiento",
        4: "Otras Cooperativas",
        5: "Fondos de Empleados",
        6: "Cooperativas de Aporte y Credito"
    }
    return entidad.get(codigo, "Código no válido")

def valores_cuentas_solidaria(cuentas):
    # Diccionario que asocia cada cuenta con su valor
    mapping = {
        "Activos": 100000,
        "Disponible": 110000,
        "Cartera": 140000,
        "Pasivos": 200000,
        "Deposito": 210000,
        "Obligaciones Financieras": 230000,
        "Patrimonio": 300000,
        "Capital Social": 310000,
        "Excedentes": 350000,
        "Ingresos": 400000,
        "Gastos": 500000,
        "Costos": 600000
    }
    codigo_a_texto = {v: k for k, v in mapping.items()}

    def convertir(dato):
        if isinstance(dato, str):
            if dato not in mapping:
                raise ValueError(f"La cuenta '{dato}' no está definida en el mapeo.")
            return mapping[dato]
        elif isinstance(dato, int):
            if dato not in codigo_a_texto:
                raise ValueError(f"El código '{dato}' no está definido en el mapeo.")
            return codigo_a_texto[dato]
        else:
            raise TypeError("Tipo de dato no soportado")

    if isinstance(cuentas, list):
        return [convertir(dato) for dato in cuentas]
    else:
        return convertir(cuentas)

def valores_cuentas_financiera(cuentas):
    mapping = {
        "Activos": 100000,
        "Disponible": 110000,
        "Cartera": 140000,
        "Pasivos": 200000,
        "Deposito": 210000,
        "Obligaciones Financieras": 240000,
        "Patrimonio": 300000,
        "Capital Social": 310000,
        "Excedentes": 391500,
        "Ingresos": 400000,
        "Gastos": 500000,
        "Costos": 600000
    }
    codigo_a_texto = {v: k for k, v in mapping.items()}

    def convertir(dato):
        if isinstance(dato, str):
            if dato not in mapping:
                raise ValueError(f"La cuenta '{dato}' no está definida en el mapeo.")
            return mapping[dato]
        elif isinstance(dato, int):
            if dato not in codigo_a_texto:
                raise ValueError(f"El código '{dato}' no está definido en el mapeo.")
            return codigo_a_texto[dato]
        else:
            raise TypeError("Tipo de dato no soportado")

    if isinstance(cuentas, list):
        return [convertir(dato) for dato in cuentas]
    else:
        return convertir(cuentas)

# Financiera

def get_api_financiera(anio, mes, puc_codes, entities):
    
    fecha1_str, fecha2_str = build_dates(anio, mes)
    
    saldos = defaultdict(lambda: defaultdict(Decimal))
    puc_codes_str = ','.join(f"'{code}'" for code in puc_codes)
    entities = ','.join(f"'{entity}'" for entity in entities)
    url = f"https://www.datos.gov.co/resource/mxk5-ce6w.json?$limit=100000&$where=fecha_corte BETWEEN '{fecha1_str}' AND '{fecha2_str}' AND NOMBRE_ENTIDAD IN ({entities}) AND cuenta IN ({puc_codes_str}) AND moneda ='0'"
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

def get_db_financiera(anio, mes, data_entities, cuentas_num, razones_sociales):

    def limpiar_entidades(data):
        return [ent.strip() for ent in data.keys()]

    def convertir_cuentas(cuentas):
        return [int(c) for c in cuentas]

    def convertir_mes(mes_valor):
        try:
            return int(mes_valor)
        except ValueError:
            print("❌ Error: El mes debe ser un número válido.")
            return None

    def consultar_db(entidades, cuentas, anio, mes):
        return BalSupModel.objects.filter(
            Q(periodo=anio, mes=mes),
            puc_codigo__in=[str(c) for c in cuentas],
            entidad_RS__in=entidades
        ).values("entidad_RS", "puc_codigo", "saldo")

    def estructurar_db_resultado(queryset):
        datos = {}
        for item in queryset:
            entidad = item["entidad_RS"]
            cuenta = int(item["puc_codigo"])
            saldo = float(item["saldo"])

            if entidad not in datos:
                datos[entidad] = {}
            datos[entidad][cuenta] = saldo
        return datos

    def completar_cuentas(data_entities, db_data, cuentas_num):
        resultado = {}

        for entidad, cuentas_existentes in data_entities.items():
            cuentas_entidad = {int(k): v for k, v in cuentas_existentes.items()}
            
            for cuenta in cuentas_num:
                if cuenta not in cuentas_entidad:
                    cuentas_entidad[cuenta] = db_data.get(entidad, {}).get(cuenta, 0.0)
            cuentas_ordenadas = dict(sorted(cuentas_entidad.items()))

            resultado[entidad] = cuentas_ordenadas

        return resultado

    # Inicio del proceso
    entidades_list = limpiar_entidades(data_entities)
    cuentas_num = convertir_cuentas(cuentas_num)
    mes = convertir_mes(mes)
    if mes is None:
        return {}
    
    if not entidades_list:
        entidades_list = razones_sociales
        data_entities  = {
        nombre: defaultdict(Decimal)
        for nombre in razones_sociales
        }

    resultados_db = consultar_db(entidades_list, cuentas_num, anio, mes)
    db_data = estructurar_db_resultado(resultados_db)
    resultado_final = completar_cuentas(data_entities, db_data, cuentas_num)

    return resultado_final

def sumar_cuentas_financiera(año, mes, tipoEntidad, datosFinanciera):
    sumas = {}
    
    for institucion in datosFinanciera.values():
        for cuenta, valor in institucion.items():
            
            codigo_int = int(cuenta)
            nombre_cuenta = valores_cuentas_financiera(codigo_int)
            
            if not isinstance(valor, Decimal):
                valor = Decimal(str(valor))
            
            if nombre_cuenta in sumas:
                sumas[nombre_cuenta] += valor
            else:
                sumas[nombre_cuenta] = valor
    
    return {
        "año": año,
        "mes": mes,
        "TipoEntidad": tipoEntidad,
        "cuentas_sumadas": sumas
    }

def total_Financiera(anio, mes, tipoEntidad, cuenta):
    print(f"Ejecutando total_Financiera con valor: {tipo_entidad_str(tipoEntidad)} para año: {anio} y mes: {mes}")

    entidades = EntidadModel.objects.filter(TipoEntidad=tipoEntidad)
    razones_sociales = [entidad.RazonSocial for entidad in entidades]

    cuenta_numeros = valores_cuentas_financiera(cuenta)

    resultadoDatosApi = get_api_financiera(anio, mes, cuenta_numeros, razones_sociales)

    resultadofinal = get_db_financiera(anio, mes, resultadoDatosApi, cuenta_numeros, razones_sociales)

    resultadofinalsumado = sumar_cuentas_financiera(anio, mes, tipo_entidad_str(tipoEntidad), resultadofinal)

    return resultadofinalsumado

# Solidaria

def get_url_solidaria(periodo):
    if periodo == 2020:
        return ("https://www.datos.gov.co/resource/78xz-k3hv.json?$limit=100000", 'codcuenta')
    elif periodo == 2021:
        return ("https://www.datos.gov.co/resource/irgu-au8v.json?$limit=100000", 'codrenglon')
    else:
        return ("https://www.datos.gov.co/resource/tic6-rbue.json?$limit=100000", 'codrenglon')

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

def get_api_solidaria(anio, mes, cuenta_numeros, formatted_nits_dvs):
    
    base_url, campo_cuenta = get_url_solidaria(anio)
    
    mes_str = get_month_name(mes)
    
    saldos = defaultdict(lambda: defaultdict(Decimal))
    puc_codes_str = ','.join(f"'{code}'" for code in cuenta_numeros)
    formatted_nits_dvs_str = ','.join(f"'{nit_dv}'" for nit_dv in formatted_nits_dvs)
    # url = f"{base_url}&$where=a_o='{anio}' AND mes='{mes_str}' AND nit IN({formatted_nits_dvs_str}) AND {campo_cuenta} IN ({puc_codes_str})"
    url = f"{base_url}&$where=a_o='{anio}' AND mes='{mes_str}' AND {campo_cuenta} IN ({puc_codes_str})"

    max_retries = 20
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            all_data = response.json()
            print(f"TOTALES Obtenidos {len(all_data)} registros de la API para el periodo {anio} y mes {mes} en el intento {attempt + 1}")
            
            if all_data:

                for result in all_data:
                    nit = result.get("nit")
                    if nit not in formatted_nits_dvs_str:
                        continue

                    cuenta = result.get(campo_cuenta)
                    valor_en_pesos = clean_currency_value_Decimal(result.get('valor_en_pesos', '0'))
                    saldos[nit][cuenta] += valor_en_pesos
            break

        except requests.exceptions.Timeout:
            print(f"TOTALES Timeout en intento {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                sleep(retry_delay)
        except requests.RequestException as e:
            print(f"TOTALES Error no manejado en el intento {attempt + 1}: {e}")
            break

    return saldos

def get_db_solidaria(anio, mes, data_entities, cuentas_num, razones_sociales, nit_a_razon_social):

    def nits_razon(data, dic_nit_rs):
        for nit in list(data.keys()):
            razon_social = dic_nit_rs.get(nit)
            if razon_social:
                data[razon_social] = data.pop(nit)
            else:
                print(f"Advertencia: NIT {nit} no encontrado y se mantiene")
        return data

    def limpiar_entidades(data):
        return [ent.strip() for ent in data.keys()]

    def convertir_cuentas(cuentas):
        return [int(c) for c in cuentas]

    def convertir_mes(mes_valor):
        try:
            return int(mes_valor)
        except ValueError:
            print("❌ Error: El mes debe ser un número válido.")
            return None

    def consultar_db(entidades, cuentas, anio, mes):
        return BalCoopModel.objects.filter(
            Q(periodo=anio, mes=mes),
            puc_codigo__in=[str(c) for c in cuentas],
            entidad_RS__in=entidades
        ).values("entidad_RS", "puc_codigo", "saldo")

    def estructurar_db_resultado(queryset):
        datos = {}
        for item in queryset:
            entidad = item["entidad_RS"]
            cuenta = int(item["puc_codigo"])
            saldo = float(item["saldo"])

            if entidad not in datos:
                datos[entidad] = {}
            datos[entidad][cuenta] = saldo
        return datos

    def completar_cuentas(data_entities, db_data, cuentas_num):
        resultado = {}

        for entidad, cuentas_existentes in data_entities.items():
            cuentas_entidad = {int(k): v for k, v in cuentas_existentes.items()}
            
            for cuenta in cuentas_num:
                if cuenta not in cuentas_entidad:
                    cuentas_entidad[cuenta] = db_data.get(entidad, {}).get(cuenta, 0.0)
            cuentas_ordenadas = dict(sorted(cuentas_entidad.items()))

            resultado[entidad] = cuentas_ordenadas

        return resultado

    # Inicio del proceso
    data_entities = nits_razon(data_entities, nit_a_razon_social)
    
    entidades_list = limpiar_entidades(data_entities)
    cuentas_num = convertir_cuentas(cuentas_num)
    mes = convertir_mes(mes)
    if mes is None:
        return {}

    if not entidades_list:
        entidades_list = razones_sociales
        data_entities  = {
        nombre: defaultdict(Decimal)
        for nombre in razones_sociales
        }

    resultados_db = consultar_db(entidades_list, cuentas_num, anio, mes)
    db_data = estructurar_db_resultado(resultados_db)
    resultado_final = completar_cuentas(data_entities, db_data, cuentas_num)

    return resultado_final

def sumar_cuentas_Solidaria(año, mes, tipoEntidad, datosFinanciera):
    sumas = {}
    
    for institucion in datosFinanciera.values():
        for cuenta, valor in institucion.items():
            
            codigo_int = int(cuenta)
            nombre_cuenta = valores_cuentas_solidaria(codigo_int)
            
            if not isinstance(valor, Decimal):
                valor = Decimal(str(valor))
            if nombre_cuenta in sumas:
                sumas[nombre_cuenta] += valor
            else:
                sumas[nombre_cuenta] = valor
    
    return {
        "año": año,
        "mes": mes,
        "TipoEntidad": tipoEntidad,
        "cuentas_sumadas": sumas
    }

def total_solidaria(anio, mes, tipoEntidad, cuenta):
    print(f"Ejecutando total_solidaria con valor: {tipo_entidad_str(tipoEntidad)} para año: {anio} y mes: {mes}")

    entidades = EntidadModel.objects.filter(TipoEntidad=tipoEntidad)
    razones_sociales = [entidad.RazonSocial for entidad in entidades]
    nits_formateados = []
    nit_a_razon_social = {}
    
    for entidad in entidades:
        nit = f"{entidad.Nit:09d}"
        dv = str(entidad.Dv)[0]
        formato_final = f"{nit[:3]}-{nit[3:6]}-{nit[6:9]}-{dv}"
        
        nits_formateados.append(formato_final)
        nit_a_razon_social[formato_final] = entidad.RazonSocial  # Mapeo directo

    cuenta_numeros = valores_cuentas_solidaria(cuenta)

    resultadoDatosApi = get_api_solidaria(anio, mes, cuenta_numeros, nits_formateados)
    
    resultadofinal = get_db_solidaria(anio, mes, resultadoDatosApi, cuenta_numeros, razones_sociales, nit_a_razon_social)
    
    resultadofinalsumado = sumar_cuentas_Solidaria(anio, mes, tipo_entidad_str(tipoEntidad), resultadofinal)

    return resultadofinalsumado


def ordenar_datos(data_cruda, cuentita):
    # Estructura base del resultado
    resultado = {
        "cuentas_disponibles": cuentita,
        "cuentas": {}
    }

    # Inicializamos estructura vacía
    for cuenta in cuentita:
        resultado["cuentas"][cuenta] = {}

    # Procesamos cada fila de data_cruda
    for fila in data_cruda:
        anio = fila.get("año")
        mes = fila.get("mes")
        tipo_entidad = fila.get("TipoEntidad")
        cuentas_sumadas = fila.get("cuentas_sumadas", {})

        for cuenta in cuentita:
            saldo = cuentas_sumadas.get(cuenta)
            if saldo is not None:
                if tipo_entidad not in resultado["cuentas"][cuenta]:
                    resultado["cuentas"][cuenta][tipo_entidad] = []
                
                resultado["cuentas"][cuenta][tipo_entidad].append({
                    "anio": anio,
                    "mes": mes,
                    "saldo": saldo
                })
    
    for cuenta in resultado["cuentas"]:
        for tipo_entidad in resultado["cuentas"][cuenta]:
            resultado["cuentas"][cuenta][tipo_entidad].sort(
                key=lambda x: (x["anio"], x["mes"])
            )

    return resultado

class TotalAccounts(APIView):
    def post(self, request, *args, **kwargs):
        data = request.data
        resultados_totales = []

        bloques = self.dividir_en_bloques(data)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.procesar_bloque, bloque)
                for bloque in bloques
            ]
            for future in concurrent.futures.as_completed(futures):
                bloque_resultado = future.result()
                if bloque_resultado:
                    resultados_totales.extend(bloque_resultado)

        cuentas_disponibles = data[0].get("cuentas") if data else []
        resultado_final = ordenar_datos(resultados_totales, cuentas_disponibles)

        return Response(resultado_final, status=status.HTTP_200_OK)

    def dividir_en_bloques(self, datos):
        return [[item] for item in datos]

    def procesar_bloque(self, bloque):
        resultados = []

        for item in bloque:
            anio = item.get('anio', 'No disponible')
            mes = item.get('mes', 'No disponible')
            # print(f"Año: {anio}, Mes: {mes}")

            tipo_entidad = item.get("TipoEntidad", [])
            cuentas_disponibles = item.get("cuentas")

            if not isinstance(tipo_entidad, list):
                tipo_entidad = [tipo_entidad]

            for num in tipo_entidad:
                if num in (0, 1, 3):
                    resultadoFinanciera = total_Financiera(anio, mes, num, cuentas_disponibles)
                    resultados.append(resultadoFinanciera)
                # elif num == 2:
                elif num in (2, 4, 5, 6):
                    resultadoSolidaria = total_solidaria(anio, mes, num, cuentas_disponibles)
                    resultados.append(resultadoSolidaria)
                else:
                    print(f"Valor {num} no reconocido en TipoEntidad")

        return resultados