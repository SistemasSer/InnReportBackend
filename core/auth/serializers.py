from rest_framework import serializers
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.settings import api_settings
from django.contrib.auth import authenticate, update_session_auth_hash
from django.contrib.auth.models import update_last_login
from django.core.exceptions import ObjectDoesNotExist


from core.user.serializers import UserSerializer
from core.user.models import User


class LoginSerializer(TokenObtainPairSerializer):

    def validate(self, attrs):
        data = super().validate(attrs)

        refresh = self.get_token(self.user)

        data['user'] = UserSerializer(self.user).data
        data['refresh'] = str(refresh)
        data['access'] = str(refresh.access_token)

        if api_settings.UPDATE_LAST_LOGIN:
            update_last_login(None, self.user)

        return data

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(max_length=128, min_length=8, write_only=True, required=True)
    email = serializers.EmailField(required=True, write_only=True, max_length=128)
    is_staff = serializers.BooleanField(required=False, default=False)  # Añadir is_staff

    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'password', 'is_staff', 'is_active', 'created_at', 'updated_at']

    def create(self, validated_data):
        # Crear el usuario con el campo is_staff
        user = User.objects.create_user(**validated_data)
        return user

# class RegisterSerializer(UserSerializer):
#     password = serializers.CharField(max_length=128, min_length=8, write_only=True, required=True)
#     email = serializers.EmailField(required=True, write_only=True, max_length=128)

#     class Meta:
#         model = User
#         fields = ['id', 'username', 'email', 'password', 'is_staff', 'is_active', 'created_at', 'updated_at']

#     def create(self, validated_data):
#         try:
#             user = User.objects.get(email=validated_data['email'])
#         except ObjectDoesNotExist:
#             user = User.objects.create_user(**validated_data)
#         return user
    
class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'is_staff', 'is_active', 'created_at', 'updated_at']  # Incluye los campos que deseas permitir actualizar
    
class UserUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['username', 'email']
    
    def update(self, instance, validated_data):
        instance.username = validated_data.get('username', instance.username)
        instance.email = validated_data.get('email', instance.email)
        instance.save()
        return instance

class ChangePasswordSerializer(serializers.Serializer):
    old_password = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True)

    def validate(self, data):
        old_password = data.get('old_password')
        new_password = data.get('new_password')

        # Check if old password is correct
        user = authenticate(username=self.context['request'].user.username, password=old_password)
        if not user:
            raise serializers.ValidationError({"old_password": "Old password is incorrect."})

        # Ensure new password is not the same as old password (optional)
        if old_password == new_password:
            raise serializers.ValidationError({"new_password": "New password must be different from the old password."})

        return data

    def save(self):
        user = self.context['request'].user
        user.set_password(self.validated_data['new_password'])
        user.save()
        update_session_auth_hash(self.context['request'], user)