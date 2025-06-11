from django.contrib.auth.tokens import default_token_generator
from django.db import transaction
from django.utils import timezone
from django.template.loader import render_to_string
from django.core.mail import EmailMessage
from django.conf import settings
from itsdangerous import URLSafeTimedSerializer
from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.exceptions import TokenError, InvalidToken
from core.auth.serializers import (
    LoginSerializer,
    RegisterSerializer,
    UserSerializer,
    UserSerializerUpdate,
    PasswordResetRequestSerializer,
    PasswordResetSerializer,
)
from rest_framework_simplejwt.views import TokenRefreshView
from core.user.models import User, Subscription, UserEntidad
from django.contrib.auth import update_session_auth_hash

from entidad.models import EntidadModel

from django.contrib.sessions.models import Session
from rest_framework.permissions import IsAuthenticated
from django.utils.timezone import now
from django.contrib.auth import login, logout

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

        if User.objects.filter(email=email, is_active=False).first():
            return Response({"detail": "La Cuenta se encuentra Inhabilitada"}, status=status.HTTP_403_FORBIDDEN)

        if not user.check_password(password):
            return Response({"detail": "La contraseña es incorrecta."}, status=status.HTTP_400_BAD_REQUEST)

        serializer = self.get_serializer(data=request.data)
        try:
            serializer.is_valid(raise_exception=True)
        except TokenError:
            return Response({"detail": "Error al generar el token."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # Cerrar sesiones anteriores del usuario
        active_sessions = Session.objects.filter(expire_date__gte=now())
        user_sessions = [session for session in active_sessions if session.get_decoded().get('_auth_user_id') == str(user.id)]
        for session in user_sessions:
            session.delete()

        serializer = self.get_serializer(data=request.data)
        try:
            serializer.is_valid(raise_exception=True)
        except TokenError:
            return Response({"detail": "Error al generar el token."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Iniciar sesión del usuario
        login(request, user)

        data = serializer.validated_data
        data["detail"] = "Inicio de sesión exitoso."
        data["session_key"] = request.session.session_key
        return Response(data, status=status.HTTP_200_OK)

class RegistrationViewSet(ModelViewSet):
    serializer_class = RegisterSerializer
    permission_classes = (AllowAny,)
    http_method_names = ["post"]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        # email = serializer.validated_data.get('email')

        email = request.data.get("email")

        if User.objects.filter(email=email).exists():
            return Response(
                {"detail": "Este email ya está registrado."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with transaction.atomic():

                fecha_inicio = request.data.get('fecha_inicio_suscripcion')
                fecha_final = request.data.get('fecha_final_suscripcion')

                if fecha_inicio and fecha_final:
                    fecha_inicio = timezone.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
                    fecha_final = timezone.datetime.strptime(fecha_final, "%Y-%m-%d").date()

                    if fecha_final < timezone.now().date():
                        return Response(
                            {"detail": "La fecha final de la suscripción no puede ser una fecha pasada."},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    if fecha_final <= fecha_inicio:
                        return Response(
                            {"detail": "La fecha final de la suscripción debe ser posterior a la fecha de inicio."},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                else:
                    return Response(
                        {"detail": "Las fechas de inicio y fin de la suscripción son requeridas."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                user = serializer.save()

                subscription = Subscription.objects.create(
                    user=user,
                    fecha_inicio_suscripcion=fecha_inicio,
                    fecha_final_suscripcion=fecha_final,
                    is_active=True
                )

                refresh = RefreshToken.for_user(user)
                res = {
                    "refresh": str(refresh),
                    "access": str(refresh.access_token),
                }

                return Response(
                    {
                        "user": serializer.data,
                        "subscription": {
                            "fecha_inicio": subscription.fecha_inicio_suscripcion,
                            "fecha_final": subscription.fecha_final_suscripcion,
                        },
                        "refresh": res["refresh"],
                        "token": res["access"],
                    },
                    status=status.HTTP_201_CREATED,
                )

        except Exception as e:
            return Response(
                {"detail": f"Error al crear el usuario o la suscripción: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST
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
            
            if User.objects.filter(email=email, is_active=False).first():
                return Response({"detail": "La Cuenta se encuentra Inhabilitada"}, status=status.HTTP_403_FORBIDDEN)

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

#subscription

class CreateSubscriptionView(APIView):
    permission_classes = (AllowAny,)

    def post(self, request, *args, **kwargs):
        user_id = request.data.get('user_id')
        fecha_inicio = request.data.get('fecha_inicio_suscripcion')
        fecha_final = request.data.get('fecha_final_suscripcion')

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response(
                {"detail": "Usuario no encontrado."},
                status=status.HTTP_404_NOT_FOUND
            )

        if Subscription.objects.filter(user=user, is_active=True).exists():
            return Response(
                {"detail": "El usuario ya tiene una suscripción activa."},
                status=status.HTTP_400_BAD_REQUEST
            )

        if fecha_inicio and fecha_final:
            fecha_inicio = timezone.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
            fecha_final = timezone.datetime.strptime(fecha_final, "%Y-%m-%d").date()

            if fecha_final < timezone.now().date():
                return Response(
                    {"detail": "La fecha final de la suscripción no puede ser una fecha pasada."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if fecha_final <= fecha_inicio:
                return Response(
                    {"detail": "La fecha final de la suscripción debe ser posterior a la fecha de inicio."},
                    status=status.HTTP_400_BAD_REQUEST
                )
        else:
            return Response(
                {"detail": "Las fechas de inicio y fin de la suscripción son requeridas."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with transaction.atomic():
                subscription = Subscription.objects.create(
                    user=user,
                    fecha_inicio_suscripcion=fecha_inicio,
                    fecha_final_suscripcion=fecha_final,
                    is_active=True
                )

            return Response(
                {
                    "user": {
                        "username": user.username
                    },
                    "subscription": {
                        "fecha_inicio": subscription.fecha_inicio_suscripcion,
                        "fecha_final": subscription.fecha_final_suscripcion,
                    },
                    "detail": f"la suscripcion de creo correctamente entre {fecha_inicio} y {fecha_final}"
                },
                status=status.HTTP_201_CREATED
            )
        except Exception as e:
            return Response(
                {"detail": f"Error al crear la suscripción: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST
            )

class ExtendSubscriptionView(APIView):
    permission_classes = (AllowAny,)

    def post(self, request, *args, **kwargs):
        user_id = request.data.get('user_id')
        nueva_fecha_final = request.data.get('nueva_fecha_final_suscripcion')

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response(
                {"detail": "Usuario no encontrado."},
                status=status.HTTP_404_NOT_FOUND
            )

        subscription = Subscription.objects.filter(user=user, is_active=True).first()
        if not subscription:
            return Response(
                {"detail": "El usuario no tiene una suscripción activa."},
                status=status.HTTP_400_BAD_REQUEST
            )

        if nueva_fecha_final:
            nueva_fecha_final = timezone.datetime.strptime(nueva_fecha_final, "%Y-%m-%d").date()
            if nueva_fecha_final < timezone.now().date():
                return Response(
                    {"detail": "La nueva fecha final no puede ser una fecha pasada."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # if nueva_fecha_final <= subscription.fecha_final_suscripcion:
            #     return Response(
            #         {"detail": "La nueva fecha final debe ser posterior a la fecha final de la suscripción actual."},
            #         status=status.HTTP_400_BAD_REQUEST
            #     )

            subscription.fecha_final_suscripcion = nueva_fecha_final
            subscription.save()

            return Response(
                {
                    "user": {
                        "username": user.username
                    },
                    "subscription": {
                        "fecha_inicio": subscription.fecha_inicio_suscripcion,
                        "fecha_final": subscription.fecha_final_suscripcion,
                    },
                    "detail": f"La fecha de expiración a pasado a {nueva_fecha_final}"
                },
                status=status.HTTP_200_OK
            )

        else:
            return Response(
                {"detail": "La nueva fecha final es requerida."},
                status=status.HTTP_400_BAD_REQUEST
            )

class CancelSubscriptionView(APIView):
    permission_classes = (AllowAny,)

    def post(self, request, *args, **kwargs):
        user_id = request.data.get('user_id')

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response(
                {"detail": "Usuario no encontrado."},
                status=status.HTTP_404_NOT_FOUND
            )

        subscription = Subscription.objects.filter(user=user, is_active=True).first()
        if not subscription:
            return Response(
                {"detail": "El usuario no tiene una suscripción activa."},
                status=status.HTTP_400_BAD_REQUEST
            )

        subscription.is_active = False
        subscription.save()

        return Response(
            {
                "user": {
                    "username": user.username
                },
                "subscription": {
                    "fecha_inicio": subscription.fecha_inicio_suscripcion,
                    "fecha_final": subscription.fecha_final_suscripcion,
                    "is_active": subscription.is_active
                },
                "detail": f"La suscripcion de {user.username} ha sido anulada correctamente."
            },
            status=status.HTTP_200_OK
        )

#temporal

class ActiveSessionsView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        sessions = Session.objects.filter(expire_date__gte=now())
        active_sessions = []

        for session in sessions:
            data = session.get_decoded()
            user_id = data.get('_auth_user_id')
            if user_id:
                try:
                    user = User.objects.get(id=user_id)
                    active_sessions.append({
                        'session_key': session.session_key,
                        'user_id': user.id,
                        'username': user.username,
                        'last_activity': session.expire_date,
                    })
                except User.DoesNotExist:
                    continue

        return Response(active_sessions)

class CheckSessionView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        return Response({"session_key": request.session.session_key})

# userEntidad

class EntidadesUser(APIView):

    def post(self, request, user_id):
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({"detail": "Usuario no encontrado"}, status=status.HTTP_404_NOT_FOUND)

        entidades_ids = request.data.get('entities', [])

        # if not entidades_ids:
        #     return Response({"detail": "Debes proporcionar al menos una entidad"}, status=status.HTTP_400_BAD_REQUEST)

        relaciones_a_crear = []
        for entidad_id in entidades_ids:
            try:
                entidad = EntidadModel.objects.get(id=entidad_id)
            except EntidadModel.DoesNotExist:
                return Response({"detail": f"Entidad no encontrada"}, status=status.HTTP_404_NOT_FOUND)

            if not UserEntidad.objects.filter(user=user, entidad=entidad).exists():
                relaciones_a_crear.append(UserEntidad(user=user, entidad=entidad))

        if relaciones_a_crear:
            UserEntidad.objects.bulk_create(relaciones_a_crear)

        return Response({
            "user": user.id,
            "entidades": entidades_ids,
            "detail": "Relaciones creadas exitosamente."
        }, status=status.HTTP_201_CREATED)

    def delete(self, request, user_id):
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({"detail": "Usuario no encontrado"}, status=status.HTTP_404_NOT_FOUND)

        entidades_ids = request.data.get('entities', [])

        # if not entidades_ids:
        #     return Response({"detail": "Debes proporcionar al menos una entidad"}, status=status.HTTP_400_BAD_REQUEST)

        relaciones_a_eliminar = []
        for entidad_id in entidades_ids:
            try:
                entidad = EntidadModel.objects.get(id=entidad_id)
            except EntidadModel.DoesNotExist:
                return Response({"detail": f"Entidad no encontrada"}, status=status.HTTP_404_NOT_FOUND)

            relacion = UserEntidad.objects.filter(user=user, entidad=entidad).first()
            if relacion:
                relaciones_a_eliminar.append(relacion)
            else:
                return Response({"detail": f"No existe relación entre el usuario y la entidad"}, status=status.HTTP_404_NOT_FOUND)

        for relacion in relaciones_a_eliminar:
            relacion.delete()

        return Response({
            "user": user.id,
            "entidades": entidades_ids,
            "detail": "Relaciones eliminadas exitosamente."
        }, status=status.HTTP_200_OK)