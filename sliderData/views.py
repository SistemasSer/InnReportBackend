import os
import json
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status


class SliderDataView(APIView):
    def get(self, request):
        try:
            file_path = os.path.join(settings.BASE_DIR, "sliderData", "data", "slider_data.json")
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return Response(data)
        except FileNotFoundError:
            return Response({"error": "Datos no disponibles aún."}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
