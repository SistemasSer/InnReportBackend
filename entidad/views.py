import logging
from django.db.models import Case, When, IntegerField, DecimalField, Value, Subquery, OuterRef
from django.db.models.functions import Coalesce

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

class EntidadApiView(APIView):

    def get(self, request):
        tipo_entidad_param = request.query_params.getlist("TipoEntidad")
        gremio_param = request.query_params.getlist("Gremio")
        grupo_activo_param = request.query_params.getlist("Grupo_Activo")

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
            bal_coop_subquery = BalCoopModel.objects.filter(
                entidad_RS=OuterRef('RazonSocial'),
                puc_codigo='100000'
            ).order_by('-periodo', '-mes')

            bal_sup_subquery = BalSupModel.objects.filter(
                entidad_RS=OuterRef('RazonSocial'),
                puc_codigo='100000'
            ).order_by('-periodo', '-mes')

            queryset = queryset.annotate(
                saldo=Coalesce(
                    Subquery(bal_coop_subquery.values('saldo')[:1]),
                    Subquery(bal_sup_subquery.values('saldo')[:1]),
                    output_field=DecimalField()
                )
            )

            resultado = []
            for entidad in queryset:
                saldo = entidad.saldo if entidad.saldo is not None else 0
                grupo_activo = determinar_grupo(saldo)

                entidad_data = {
                    'id': entidad.id,
                    'Nit': entidad.Nit,
                    'Dv': entidad.Dv,
                    'RazonSocial': entidad.RazonSocial,
                    'Sigla': entidad.Sigla,
                    'TipoEntidad': entidad.TipoEntidad,
                    'Gremio': entidad.Gremio,
                    'Grupo_Activo': grupo_activo
                }

                if grupo_activo in grupo_activo_values:
                    resultado.append(entidad_data)

            return Response(status=status.HTTP_200_OK, data=resultado)

        else:
            entidades = queryset.values('id','Nit', 'Dv', 'RazonSocial', 'Sigla', 'TipoEntidad', 'Gremio')
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
