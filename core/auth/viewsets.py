import logging
from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import default_token_generator
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from django.utils.encoding import force_bytes
from django.template.loader import render_to_string
from django.core.mail import EmailMessage
from django.conf import settings
from itsdangerous import URLSafeTimedSerializer
from rest_framework import viewsets, status, generics
from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.exceptions import TokenError, InvalidToken
from core.auth.serializers import (
    LoginSerializer,
    RegisterSerializer,
    UserSerializer,
    UserSerializerUpdate,
    PasswordResetRequestSerializer,
    PasswordResetSerializer
)
from rest_framework_simplejwt.views import TokenRefreshView
from rest_framework.permissions import IsAuthenticated
from core.user.models import User
from django.contrib.auth import update_session_auth_hash

class LoginViewSet(ModelViewSet, TokenObtainPairView):
    serializer_class = LoginSerializer
    permission_classes = (AllowAny,)
    http_method_names = ["post"]

    def create(self, request, *args, **kwargs):
        email = request.data.get("email")
        password = request.data.get("password")

        user = User.objects.filter(email=email).first()
        if not user:
            return Response({"detail": "El email no está registrado."}, status=status.HTTP_400_BAD_REQUEST)

        if not user.check_password(password):
            return Response({"detail": "La contraseña es incorrecta."}, status=status.HTTP_400_BAD_REQUEST)

        serializer = self.get_serializer(data=request.data)
        try:
            serializer.is_valid(raise_exception=True)
        except TokenError:
            return Response({"detail": "Error al generar el token."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = serializer.validated_data
        data["detail"] = "Inicio de sesión exitoso."
        return Response(data, status=status.HTTP_200_OK)

class RegistrationViewSet(ModelViewSet):
    serializer_class = RegisterSerializer
    permission_classes = (AllowAny,)
    http_method_names = ["post"]
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        email = serializer.validated_data.get('email')
        if User.objects.filter(email=email).exists():
            return Response(
                {"detail": "Este email ya está registrado."},
                status=status.HTTP_400_BAD_REQUEST
            )
        user = serializer.save()
        refresh = RefreshToken.for_user(user)
        res = {
            "refresh": str(refresh),
            "access": str(refresh.access_token),
        }
        return Response(
            {
                "user": serializer.data,
                "refresh": res["refresh"],
                "token": res["access"],
            },
            status=status.HTTP_201_CREATED,
        )

class RefreshViewSet(viewsets.ViewSet, TokenRefreshView):
    permission_classes = (AllowAny,)
    http_method_names = ["post"]
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        try:
            serializer.is_valid(raise_exception=True)
        except TokenError as e:
            raise InvalidToken(e.args[0])
        return Response(serializer.validated_data, status=status.HTTP_200_OK)

class UserUpdateViewSet(viewsets.ViewSet):
    permission_classes = (AllowAny,)
    def partial_update(self, request, pk=None):
        try:
            user = User.objects.get(pk=pk)
        except User.DoesNotExist:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)
        serializer = UserSerializer(user, data=request.data, partial=True)
        if "email" in request.data:
            email = request.data["email"]
            if User.objects.filter(email=email).exclude(pk=pk).exists():
                return Response(
                    {"detail": "El correo electrónico ya está en uso."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class ChangePasswordViewSet(viewsets.ViewSet):
    def partial_update(self, request, pk=None):
        try:
            user = User.objects.get(pk=pk)
        except User.DoesNotExist:
            return Response(
                {"detail": "Usuario no Encontrado"}, status=status.HTTP_404_NOT_FOUND
            )
        old_password = request.data.get("old_password")
        new_password = request.data.get("new_password")
        if not user.check_password(old_password):
            return Response(
                {"detail": "Contraseña Antigua es incorrecta"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if old_password == new_password:
            return Response(
                {"detail": "La nueva contraseña no puede ser igual a la anterior"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        user.set_password(new_password)
        user.save()
        update_session_auth_hash(request, user)
        return Response(
            {"detail": "La contraseña se ha actualizado correctamente."},
            status=status.HTTP_200_OK,
        )

class UserPasswordUpdateView(viewsets.ViewSet): 
    permission_classes = (AllowAny,)
    def partial_update(self, request, pk=None):
        try:
            user = User.objects.get(pk=pk)
        except User.DoesNotExist:
            return Response({"detail": "Not found."}, status=status.HTTP_404_NOT_FOUND)
        if "email" in request.data:
            email = request.data["email"]
            if User.objects.filter(email=email).exclude(pk=pk).exists():
                return Response(
                    {"detail": "El correo electrónico ya está en uso."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        original_password = user.password
        serializer = UserSerializerUpdate(user, data=request.data, partial=True)
        new_password = request.data.get("new_password")
        if new_password:
            if new_password == "":
                return Response(
                    {"detail": "La nueva contraseña no puede estar vacía"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            user.set_password(new_password)
        if serializer.is_valid():
            serializer.save()
            user.save()
            update_session_auth_hash(request, user)
            if user.password != original_password:
                password_message = "La contraseña ha sido actualizada."
            else:
                password_message = "La contraseña sigue siendo la misma."
            response_data = serializer.data
            response_data["password_status"] = password_message
            return Response(response_data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class PasswordResetRequestViewSet(viewsets.ViewSet):
    def create(self, request, *args, **kwargs):
        serializer = PasswordResetRequestSerializer(data=request.data)
        if serializer.is_valid():
            email = serializer.validated_data['email']
            try:
                user = User.objects.get(email=email)
            except User.DoesNotExist:
                return Response({"detail": "Correo Electrónico no se encuentra registrado."}, status=status.HTTP_404_NOT_FOUND)

            token = default_token_generator.make_token(user)
            serializer = URLSafeTimedSerializer(settings.SECRET_KEY)
            encrypted_id = serializer.dumps(user.id, salt="password-reset")
            reset_url = f"{settings.FRONTEND_URL}/reset-password/{encrypted_id}/{token}/"

            subject = "Restablecer tu contraseña de Inn-Report"
            
            html_message = render_to_string('emails/password_reset.html', {
                'username': user.username,
                'reset_url': reset_url,
                'project_name': settings.PROJECT_NAME,
            })
            email_message = EmailMessage(
                subject,
                html_message,
                settings.EMAIL_HOST_USER,
                [email],
            )
            email_message.content_subtype = 'html'

            email_message.send()

            return Response({"detail": "Se ha enviado un enlace de restablecimiento a tu correo."}, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class PasswordResetViewSet(viewsets.ViewSet):
    permission_classes = (AllowAny,)
    def create(self, request, encrypted_id, token, *args, **kwargs):
        from itsdangerous import URLSafeTimedSerializer, BadData
        try:
            serializer = URLSafeTimedSerializer(settings.SECRET_KEY)
            user_id = serializer.loads(encrypted_id, salt="password-reset", max_age=3600)
        except BadData:
            return Response({"detail": "Enlace inválido o expirado."}, status=status.HTTP_400_BAD_REQUEST)
        try:
            user = User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return Response({"detail": "Enlace inválido."}, status=status.HTTP_400_BAD_REQUEST)
        if not default_token_generator.check_token(user, token):
            return Response({"detail": "El enlace ha expirado o es inválido."}, status=status.HTTP_400_BAD_REQUEST)
        serializer = PasswordResetSerializer(data=request.data)
        if serializer.is_valid():
            new_password = serializer.validated_data["new_password"]
            confirm_password = serializer.validated_data["confirm_password"]
            if new_password != confirm_password:
                return Response({"detail": "Las contraseñas no coinciden."}, status=status.HTTP_400_BAD_REQUEST)
            user.set_password(new_password)
            user.save()
            return Response({"detail": "Contraseña restablecida con éxito."}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
