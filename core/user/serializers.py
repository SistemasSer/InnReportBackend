from rest_framework import serializers
from core.user.models import User, Subscription, UserEntidad

# class UserSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = User
#         fields = ['id', 'username', 'is_superuser', 'email', 'is_active', 'created_at', 'updated_at', 'is_staff']
#         read_only_fields = ['is_active', 'created_at', 'updated_at']
class UserEntidadSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserEntidad
        fields = ['user', 'entidad', 'fecha_vinculacion']

class SubscriptionSerializer(serializers.ModelSerializer):
    subscription_is_active = serializers.BooleanField(source='is_active', read_only=True)

    class Meta:
        model = Subscription
        fields = ['fecha_inicio_suscripcion', 'fecha_final_suscripcion', 'subscription_is_active']

class UserSerializer(serializers.ModelSerializer):
    subscriptions = serializers.SerializerMethodField()
    entities = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'is_active', 'is_staff', 'is_superuser', 'created_at', 'updated_at', 'subscriptions', 'entities']
        read_only_fields = ['is_active', 'created_at', 'updated_at']

    def get_subscriptions(self, obj):

        active_subscriptions = obj.subscriptions.filter(is_active=True)
        return SubscriptionSerializer(active_subscriptions, many=True).data
    
    def get_entities(self, obj):
        user_entidades = obj.usuario_entidades.all()  # Usa el related_name definido en el modelo UserEntidad
        return [user_entidad.entidad.id for user_entidad in user_entidades]
