from rest_framework.serializers import ModelSerializer
from rest_framework import serializers
from entidad.models import EntidadModel


class EntidadSerializer(ModelSerializer):
    class Meta:
        model = EntidadModel
        # fields = ['id', 'name', 'address', 'phone']
        fields = '__all__'

    
# class EntidadWithGroupSerializer(serializers.ModelSerializer):
#     grupo = serializers.IntegerField(required=False, allow_null=True)

#     class Meta:
#         model = EntidadModel
#         fields = '__all__'  # Incluye todos los campos del modelo más el campo 'grupo'