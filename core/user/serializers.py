from rest_framework import serializers
from core.user.models import User, Subscription

# class UserSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = User
#         fields = ['id', 'username', 'is_superuser', 'email', 'is_active', 'created_at', 'updated_at', 'is_staff']
#         read_only_fields = ['is_active', 'created_at', 'updated_at']

class SubscriptionSerializer(serializers.ModelSerializer):
    subscription_is_active = serializers.BooleanField(source='is_active', read_only=True)

    class Meta:
        model = Subscription
        fields = ['fecha_inicio_suscripcion', 'fecha_final_suscripcion', 'subscription_is_active']

class UserSerializer(serializers.ModelSerializer):
    subscriptions = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'is_active', 'is_staff', 'is_superuser', 'created_at', 'updated_at', 'subscriptions']
        read_only_fields = ['is_active', 'created_at', 'updated_at']

    def get_subscriptions(self, obj):
        # Filtrar las suscripciones activas
        active_subscriptions = obj.subscriptions.filter(is_active=True)
        # Serializar solo las suscripciones activas
        return SubscriptionSerializer(active_subscriptions, many=True).data