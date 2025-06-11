import os
import mimetypes
import urllib.parse


from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics
from rest_framework.parsers import MultiPartParser, FormParser

from .models import Documento
from .serializers import DocumentoSerializer

from django.http import HttpResponse
from django.http import FileResponse, Http404

import logging

logger = logging.getLogger('custom_logger')

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
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=True)

        try:
            serializer.is_valid(raise_exception=True)
            self.perform_update(serializer)
            return Response(serializer.data)
        except Exception as e:
            print(e) 
            return Response(
                {
                    "error": "Ocurrió un error durante la actualización. Por favor, inténtelo de nuevo más tarde."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

# class DocumentoDeleteView(generics.DestroyAPIView):
#     queryset = Documento.objects.all()
#     serializer_class = DocumentoSerializer

#     def delete(self, request, *args, **kwargs):
#         try:
#             response = super().delete(request, *args, **kwargs)
#             return Response(
#                 {"message": "Documento eliminado con éxito."},
#                 status=status.HTTP_204_NO_CONTENT,
#             )
#         except Documento.DoesNotExist:
#             return Response(
#                 {"error": "Documento no encontrado."}, status=status.HTTP_404_NOT_FOUND
#             )

class DocumentoDeleteView(generics.DestroyAPIView):
    queryset = Documento.objects.all()
    serializer_class = DocumentoSerializer

    def get_object(self):
        try:
            # Obtener el objeto a eliminar
            return super().get_object()
        except Documento.DoesNotExist:
            # Si el documento no existe, lanzar una excepción
            raise Http404("Documento no encontrado.")

    def delete(self, request, *args, **kwargs):
        try:
            # Intentar eliminar el documento
            instance = self.get_object()
            self.perform_destroy(instance)
            
            # Agregar los encabezados de no cache
            response = Response(
                {"message": "Documento eliminado con éxito."},
                status=status.HTTP_200_OK,  # Cambiado a 200 OK
            )
            response['Cache-Control'] = 'no-store'
            response['Pragma'] = 'no-cache'
            response['Expires'] = '0'
            
            return response
        except Http404 as e:
            # Manejar el caso en que el documento no existe
            return Response(
                {"error": str(e)},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            # Manejar cualquier otro error
            return Response(
                {"error": f"Error al eliminar el documento: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# Extender los tipos MIME conocidos
mimetypes.add_type('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')  # Word moderno
mimetypes.add_type('application/msword', '.doc')  # Word antiguo
mimetypes.add_type('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx')  # Excel moderno
mimetypes.add_type('application/vnd.ms-excel', '.xls')  # Excel antiguo
mimetypes.add_type('application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx')  # PowerPoint moderno
mimetypes.add_type('application/vnd.ms-powerpoint', '.ppt')  # PowerPoint antiguo
mimetypes.add_type('application/pdf', '.pdf')  # PDF


# class DocumentoDescargaView(APIView):
#     def get(self, request, pk, format=None):
#         try:
#             # Busca el archivo en la base de datos
#             documento = Documento.objects.get(pk=pk)
#             archivo_path = documento.archivo.path

#             if not os.path.exists(archivo_path):
#                 return Response({'error': 'El archivo no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

#             # Detecta el tipo MIME
#             mime_type, _ = mimetypes.guess_type(archivo_path)
#             if not mime_type:
#                 mime_type = 'application/octet-stream' 

#             # Genera la respuesta
#             response = FileResponse(open(archivo_path, 'rb'), content_type=mime_type)
#             response['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo_path)}"'
#             return response

#         except Documento.DoesNotExist:
#             raise Http404("Documento no encontrado")
#         except Exception as e:
#             return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# class DocumentoDescargaView(APIView):
#     def get(self, request, pk, format=None):
#         try:
#             documento = Documento.objects.get(pk=pk)
#             archivo_path = documento.archivo.path

#             if not os.path.exists(archivo_path):
#                 return Response({'error': 'El archivo no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

#             mime_type, _ = mimetypes.guess_type(archivo_path)
#             if not mime_type:
#                 mime_type = 'application/octet-stream'

#             response = FileResponse(open(archivo_path, 'rb'), content_type=mime_type)
#             response['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo_path)}"'
#             response['Access-Control-Allow-Origin'] = 'https://www.innreport.com.co'
#             response['Access-Control-Allow-Credentials'] = 'true'
#             return response

#         except Documento.DoesNotExist:
#             raise Http404("Documento no encontrado")
#         except Exception as e:
#             return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class DocumentoDescargaView(APIView):
    def get(self, request, pk, format=None):
        try:
            documento = Documento.objects.get(pk=pk)
            archivo_path = documento.archivo.path

            if not os.path.exists(archivo_path):
                return Response({'error': 'El archivo no se pudo encontrar.'}, status=status.HTTP_404_NOT_FOUND)

            mime_type, _ = mimetypes.guess_type(archivo_path)
            if not mime_type:
                mime_type = 'application/octet-stream'

            # Usar FileResponse sin agregar manualmente Content-Disposition
            response = FileResponse(open(archivo_path, 'rb'), content_type=mime_type)
            response['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo_path)}"'
            
            # Agregar cabeceras CORS manualmente
            response['Access-Control-Allow-Origin'] = 'https://www.innreport.com.co'
            response['Access-Control-Allow-Credentials'] = 'true'
            
            return response

        except Documento.DoesNotExist:
            raise Http404("Documento no encontrado")
        except Exception as e:
            return Response({'error': f'Error al servir el archivo: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)