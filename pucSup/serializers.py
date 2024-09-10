from rest_framework.serializers import ModelSerializer
from pucSup.models import PucSupModel


class PucSupSerializer(ModelSerializer):
    class Meta:
        model = PucSupModel
        fields = ['Codigo', 'Descripcion']
        # fields = '__all__'