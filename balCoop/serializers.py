from rest_framework.serializers import ModelSerializer
from balCoop.models import BalCoopModel


class BalCoopSerializer(ModelSerializer):
    class Meta:
        model = BalCoopModel
        fields = ['periodo', 'mes', 'entidad_RS', 'puc_codigo', 'saldo']
        read_only_fields = ('puc_codigo', 'periodo', 'mes', 'entidad_RS')
        #fields = '__all__'





