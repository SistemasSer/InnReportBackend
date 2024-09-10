from rest_framework.serializers import ModelSerializer
from balSup.models import BalSupModel


class BalSupSerializer(ModelSerializer):
    class Meta:
        model = BalSupModel
        fields = ['periodo', 'mes', 'entidad_RS', 'puc_codigo', 'saldo']
        # fields = '__all__'

        



