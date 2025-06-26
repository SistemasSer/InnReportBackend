from rest_framework import serializers
from entidad.serializers import EntidadSerializer
from .models import GremioModel, GremioToEntity
from entidad.models import EntidadModel

class GremioConRelacionesSerializer(serializers.ModelSerializer):
    entidades = serializers.SerializerMethodField()
    usuarios = serializers.SerializerMethodField()

    class Meta:
        model = GremioModel
        fields = ['id', 'nombre', 'descripcion', 'entidades', 'usuarios']

    def get_entidades(self, obj):
        return list(obj.entidades.values_list('entidad_id', flat=True))

    def get_usuarios(self, obj):
        return list(obj.users.values_list('id', flat=True))

class GremioSerializer(serializers.ModelSerializer):
    class Meta:
        model = GremioModel
        fields = ['id', 'nombre', 'descripcion']
        extra_kwargs = { 'descripcion': {'required': False} }

class GremioToEntitySerializer(serializers.ModelSerializer):
    class Meta:
        model = GremioToEntity
        fields = ['id', 'Gremio', 'entidad']
        extra_kwargs = {
            'Gremio': {'queryset': GremioModel.objects.all()},
            'entidad': {'queryset': EntidadModel.objects.all()},
        }

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data['Gremio'] = GremioSerializer(instance.Gremio).data
        data['entidad'] = EntidadSerializer(instance.entidad).data
        return data