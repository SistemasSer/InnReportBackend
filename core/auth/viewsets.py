import logging
from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import default_token_generator
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from django.template.loader import render_to_string
from django.core.mail import send_mail
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
        serializer = self.get_serializer(data=request.data)

        try:
            serializer.is_valid(raise_exception=True)
        except TokenError as e:
            raise InvalidToken(e.args[0])

        return Response(serializer.validated_data, status=status.HTTP_200_OK)

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

# class PasswordResetRequestView(APIView):
#     permission_classes = [AllowAny]

#     def post(self, request, *args, **kwargs):
#         email = request.data.get('email')

#         # Validar que el correo esté registrado
#         try:
#             user = get_user_model().objects.get(email=email)
#         except get_user_model().DoesNotExist:
#             return Response({"detail": "No se ha encontrado un usuario con este correo."}, status=status.HTTP_400_BAD_REQUEST)

#         # Generar el token y el enlace de recuperación
#         token = default_token_generator.make_token(user)
#         uid = urlsafe_base64_encode(str(user.pk).encode()).decode()

#         # Generar la URL de recuperación (asegurándose de que la URL base sea la correcta)
#         reset_url = f"http://http://localhost:3000/reset/{uid}/{token}/"

#         # Configura el mensaje del correo
#         subject = "Recupera tu contraseña"
#         message = render_to_string('password_reset_email.html', {
#             'user': user,
#             'reset_url': reset_url
#         })

#         # Enviar el correo
#         send_mail(subject, message, 'no-reply@tusitio.com', [email])

#         return Response({"detail": "Se ha enviado un enlace de recuperación de contraseña a tu correo electrónico."}, status=status.HTTP_200_OK)

# class PasswordResetConfirmView(APIView):
#     permission_classes = [AllowAny]

#     def post(self, request, uidb64, token, *args, **kwargs):
#         try:
#             uid = urlsafe_base64_decode(uidb64).decode()
#             user = get_user_model().objects.get(pk=uid)
#         except (TypeError, ValueError, get_user_model().DoesNotExist):
#             return Response({"detail": "Enlace de recuperación no válido o expirado."}, status=status.HTTP_400_BAD_REQUEST)

#         if not default_token_generator.check_token(user, token):
#             return Response({"detail": "El token de recuperación no es válido o ha expirado."}, status=status.HTTP_400_BAD_REQUEST)

#         # Verificar y actualizar la contraseña
#         new_password = request.data.get('new_password')
#         if not new_password:
#             return Response({"detail": "La nueva contraseña no puede estar vacía."}, status=status.HTTP_400_BAD_REQUEST)

#         user.set_password(new_password)
#         user.save()

#         return Response({"detail": "Tu contraseña ha sido actualizada exitosamente."}, status=status.HTTP_200_OK)
