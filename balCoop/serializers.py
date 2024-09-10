from rest_framework.serializers import ModelSerializer
from balCoop.models import BalCoopModel


class BalCoopSerializer(ModelSerializer):
    class Meta:
        model = BalCoopModel
        fields = ['periodo', 'mes', 'entidad_RS', 'puc_codigo', 'saldo']
        #fields = '__all__'

   



