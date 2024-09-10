from core.user.models import User
from rest_framework import serializers

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username', 'is_superuser', 'email', 'is_active', 'created_at', 'updated_at', 'is_staff']
        read_only_fields = ['is_active', 'created_at', 'updated_at']