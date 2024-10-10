from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import AllowAny
from rest_framework import status
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.exceptions import TokenError, InvalidToken
from core.auth.serializers import (
    LoginSerializer,
    RegisterSerializer,
    UserUpdateSerializer,
    UserSerializer,
    ChangePasswordSerializer,
)
from rest_framework_simplejwt.views import TokenRefreshView
from rest_framework import viewsets, status
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


# class RegistrationViewSet(ModelViewSet):
#     serializer_class = RegisterSerializer
#     permission_classes = (AllowAny,)
#     http_method_names = ["post"]

#     def create(self, request, *args, **kwargs):
#         serializer = self.get_serializer(data=request.data)
#         serializer.is_valid(raise_exception=True)
#         user = serializer.save()

#         # Generar tokens JWT para el nuevo usuario
#         refresh = RefreshToken.for_user(user)
#         res = {
#             "refresh": str(refresh),
#             "access": str(refresh.access_token),
#         }

#         return Response(
#             {
#                 "user": serializer.data,
#                 "refresh": res["refresh"],
#                 "token": res["access"],
#             },
#             status=status.HTTP_201_CREATED,
#         )


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
                {"detail": "User not found."}, status=status.HTTP_404_NOT_FOUND
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
