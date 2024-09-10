from rest_framework.routers import SimpleRouter
from core.user.viewsets import UserViewSet
from core.auth.viewsets import (
    LoginViewSet,
    RegistrationViewSet,
    RefreshViewSet,
    UserUpdateViewSet,
    ChangePasswordViewSet,
)


routes = SimpleRouter()

# AUTHENTICATION
routes.register(r"auth/login", LoginViewSet, basename="auth-login")
routes.register(r"auth/register", RegistrationViewSet, basename="auth-register")
routes.register(r"auth/refresh", RefreshViewSet, basename="auth-refresh")

# USER UPDATE (based on ID)
routes.register(r"users", UserUpdateViewSet, basename="user-update")
routes.register(r'change-password', ChangePasswordViewSet, basename='change-password')

# USER
routes.register(r"user", UserViewSet, basename="user")


urlpatterns = [*routes.urls]
