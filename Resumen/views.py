from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics
from rest_framework.parsers import MultiPartParser, FormParser

from .models import Documento
from .serializers import DocumentoSerializer

from django.http import FileResponse, Http404
import os
import mimetypes

class DocumentoListCreateView(generics.ListCreateAPIView):
    serializer_class = DocumentoSerializer

    def get_queryset(self):
        return Documento.objects.all().order_by("-fecha")


class DocumentoUploadView(APIView):

    def post(self, request, *args, **kwargs):
        serializer = DocumentoSerializer(data=request.data)

        if serializer.is_valid():
            serializer.save()
            return Response(
                {"message": "Documento subido exitosamente."},
                status=status.HTTP_201_CREATED,
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class DocumentoUpdateView(generics.UpdateAPIView):
    queryset = Documento.objects.all()
    serializer_class = DocumentoSerializer
    parser_classes = (MultiPartParser, FormParser)

    def update(self, request, *args, **kwargs):
        print("Request data:", request.data)  # Imprime los datos de la solicitud
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=True)

        try:
            serializer.is_valid(raise_exception=True)
            self.perform_update(serializer)
            return Response(serializer.data)
        except Exception as e:
            print(e)  # Imprime el error en la consola del servidor
            return Response(
                {
                    "error": "Ocurrió un error durante la actualización. Por favor, inténtelo de nuevo más tarde."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )


class DocumentoDeleteView(generics.DestroyAPIView):
    queryset = Documento.objects.all()
    serializer_class = DocumentoSerializer

    def delete(self, request, *args, **kwargs):
        try:
            response = super().delete(request, *args, **kwargs)
            return Response(
                {"message": "Documento eliminado con éxito."},
                status=status.HTTP_204_NO_CONTENT,
            )
        except Documento.DoesNotExist:
            return Response(
                {"error": "Documento no encontrado."}, status=status.HTTP_404_NOT_FOUND
            )

"""
class DocumentoDescargaView(APIView):
    def get(self, request, pk, format=None):
        try:
            documento = Documento.objects.get(pk=pk)
            archivo_path = documento.archivo.path

            if not os.path.exists(archivo_path):
                return Response({'error': 'El archivo no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

            mime_type, _ = mimetypes.guess_type(archivo_path)
            if mime_type is None:
                mime_type = 'application/octet-stream'
            
            # print("-----"*30)
            # print(f"Archivo: {archivo_path}, Tipo MIME: {mime_type}")

            response = FileResponse(open(archivo_path, 'rb'), content_type=mime_type)
            response['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo_path)}"'
            
            return response

        except Documento.DoesNotExist:
            raise Http404("Documento no encontrado")
        except Exception as e:
            # print(f"Error inesperado: {str(e)}")
            return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
"""

class DocumentoDescargaView(APIView):
    def get(self, request, pk, format=None):
        try:
            documento = Documento.objects.get(pk=pk)
            archivo_path = documento.archivo.path

            if not os.path.exists(archivo_path):
                print(f"Archivo no encontrado: {archivo_path}")
                return Response({'error': 'El archivo no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

            mime_type, _ = mimetypes.guess_type(archivo_path)
            print(f"Archivo: {archivo_path}, Tipo MIME: {mime_type}")
            response = FileResponse(open(archivo_path, 'rb'), content_type=mime_type)
            response['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo_path)}"'
            return response
        except Documento.DoesNotExist:
            print("Documento no encontrado")
            raise Http404("Documento no encontrado")
        except Exception as e:
            print(f"Error inesperado: {str(e)}")
            return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
