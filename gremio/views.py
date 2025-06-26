from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import GremioModel, GremioToEntity
from .serializers import GremioSerializer, GremioToEntitySerializer, GremioConRelacionesSerializer

class GremioView(APIView):
    def get(self, request):
        gremios = GremioModel.objects.prefetch_related('users', 'entidades').all()
        serializer = GremioConRelacionesSerializer(gremios, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        serializer = GremioSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk):
        try:
            # Buscar el gremio por ID
            gremio = GremioModel.objects.get(pk=pk)
            gremio.delete()
            return Response({"message": "Gremio eliminado"}, status=status.HTTP_204_NO_CONTENT)
        except GremioModel.DoesNotExist:
            return Response({"error": "Gremio no encontrado"}, status=status.HTTP_404_NOT_FOUND)

    def patch(self, request, pk):
        try:
            gremio = GremioModel.objects.get(pk=pk)
        except GremioModel.DoesNotExist:
            return Response({"error": "Gremio no encontrado"}, status=status.HTTP_404_NOT_FOUND)

        # Permitir actualización parcial
        serializer = GremioSerializer(gremio, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class GremioToEntityView(APIView):
    def get(self, request):
        gremios = GremioToEntity.objects.all()
        serializer = GremioToEntitySerializer(gremios, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        serializer = GremioToEntitySerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk):
        try:
            # Buscar el gremio por ID
            gremio = GremioToEntity.objects.get(pk=pk)
            gremio.delete()
            return Response({"message": "Relacion de la entidad eliminada"}, status=status.HTTP_204_NO_CONTENT)
        except GremioToEntity.DoesNotExist:
            return Response({"error": "Relacion no encontrada"}, status=status.HTTP_404_NOT_FOUND)