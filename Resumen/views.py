from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics
from rest_framework.parsers import MultiPartParser, FormParser

from .models import Documento
from .serializers import DocumentoSerializer

from docx2pdf import convert

from django.conf import settings
from django.shortcuts import get_object_or_404
from django.http import FileResponse, HttpResponse, Http404
import os
import mimetypes
import pypandoc
import logging

logger = logging.getLogger(__name__)

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


class DocumentoDescargaView(APIView):
    def get(self, request, pk, format=None):
        try:
            # Obtener el documento por ID
            documento = Documento.objects.get(pk=pk)
            archivo_path = documento.archivo.path

            # Comprobar si el archivo existe
            if not os.path.exists(archivo_path):
                return Response({'error': 'El archivo no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

            # Detectar el tipo MIME correcto
            mime_type, _ = mimetypes.guess_type(archivo_path)
            if mime_type is None:
                mime_type = 'application/octet-stream'

            # Usar FileResponse para servir el archivo
            response = FileResponse(open(archivo_path, 'rb'), content_type=mime_type)
            response['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo_path)}"'
            
            return response

        except Documento.DoesNotExist:
            raise Http404("Documento no encontrado")
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# class DocumentoVistaPDF(APIView):

#     def get(self, request, documento_id, format=None):
#         documento = get_object_or_404(Documento, id=documento_id)
#         input_path = documento.archivo.path
#         output_path = os.path.join(settings.MEDIA_ROOT, 'archivo', f'{documento_id}.pdf')

#         if not input_path.lower().endswith('.docx'):
#             return Response({'error': 'El archivo no es un archivo DOCX.'}, status=status.HTTP_400_BAD_REQUEST)

#         try:
#             convert(input_path, output_path)
#         except Exception as e:
#             return Response({'error': f'Error al convertir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#         if not os.path.exists(output_path):
#             return Response({'error': 'El archivo PDF no se pudo generar.'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#         response = FileResponse(open(output_path, 'rb'), content_type='application/pdf')
#         response['Content-Disposition'] = 'inline; filename="documento.pdf"'
#         return response

# class DocumentoDescargaView(APIView):

#     def get(self, request, pk, format=None):
#         documento = get_object_or_404(Documento, id=pk)
#         input_path = documento.archivo.path

#         if not input_path.lower().endswith('.docx'):
#             return Response({'error': 'El archivo no es un archivo DOCX.'}, status=status.HTTP_400_BAD_REQUEST)

#         if not os.path.exists(input_path):
#             return Response({'error': 'El archivo DOC no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

#         try:
#             response = FileResponse(open(input_path, 'rb'), content_type='application/msword')
#             response['Content-Disposition'] = f'attachment; filename="{pk}.docx"'
#             return response
#         except Exception as e:
#             return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
