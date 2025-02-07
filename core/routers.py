from django.urls import path
from rest_framework.routers import SimpleRouter
from core.user.viewsets import UserViewSet
from core.auth.viewsets import (
    LoginViewSet,
    RegistrationViewSet,
    RefreshViewSet,
    UserUpdateViewSet,
    ChangePasswordViewSet,
    UserPasswordUpdateView,
    PasswordResetRequestViewSet,
    PasswordResetViewSet,
    CreateSubscriptionView,
    ExtendSubscriptionView,
    CancelSubscriptionView,
    ActiveSessionsView,
    CheckSessionView,
)

routes = SimpleRouter()

# AUTHENTICATION
routes.register(r"auth/login", LoginViewSet, basename="auth-login")
routes.register(r"auth/register", RegistrationViewSet, basename="auth-register")
routes.register(r"auth/refresh", RefreshViewSet, basename="auth-refresh")

# USER UPDATE (based on ID)
routes.register(r"users", UserUpdateViewSet, basename="user-update")
routes.register(r'change-password', ChangePasswordViewSet, basename='change-password')
routes.register(r'user/update', UserPasswordUpdateView, basename='user-changedata'),
# USER RESET PASSWORD
routes.register(r'reset-request', PasswordResetRequestViewSet, basename='reset-request')
routes.register(r'reset-password/<int:user_id>/<str:token>/', PasswordResetViewSet, basename='password-reset')


urlpatterns = [
    path(r'reset-password/<str:encrypted_id>/<str:token>/',PasswordResetViewSet.as_view({'post': 'create'}),name='password-reset'),
    #subscription 
    path(r'create_subscription', CreateSubscriptionView.as_view(), name='create-subscription'),
    path(r'extend_subscription', ExtendSubscriptionView.as_view(), name='extend-subscription'),
    path(r'cancel_subscription', CancelSubscriptionView.as_view(), name='cancel-subscription'),

    path(r'active-sessions/', ActiveSessionsView.as_view(), name='active-sessions'),
    path(r'check-session/', CheckSessionView.as_view(), name='check-session'),
]

# USER
routes.register(r"user", UserViewSet, basename="user")


# urlpatterns = [*routes.urls]
urlpatterns += routes.urls