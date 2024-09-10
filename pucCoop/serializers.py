from rest_framework.serializers import ModelSerializer
from pucCoop.models import PucCoopModel


class PucCoppSerializer(ModelSerializer):
    class Meta:
        model = PucCoopModel
        fields = ['Codigo', 'Descripcion']
        # fields = '__all__'