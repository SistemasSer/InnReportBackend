from core.user.serializers import UserSerializer
from core.user.models import User
from rest_framework.response import Response
from rest_framework import viewsets,status
from rest_framework.permissions import AllowAny
# from rest_framework.permissions import IsAuthenticated
from rest_framework import filters
from django.shortcuts import get_object_or_404


class UserViewSet(viewsets.ModelViewSet):
    http_method_names = ['get','delete'] 
    serializer_class = UserSerializer
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ['updated_at']
    ordering = ['-updated_at']
    permission_classes = [AllowAny]

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        self.perform_destroy(instance)
        return Response(status=status.HTTP_204_NO_CONTENT)

    def perform_destroy(self, instance):
        instance.delete()

    def get_queryset(self):
        return User.objects.all()

# class UserViewSet(viewsets.ModelViewSet):
#     http_method_names = ['get']
#     serializer_class = UserSerializer
#     filter_backends = [filters.OrderingFilter]
#     ordering_fields = ['updated']
#     # ordering = ['-updated']  # Descomenta esto si deseas ordenar por defecto

#     def get_queryset(self):
#         """
#         Retorna la lista de usuarios. Los superusuarios pueden ver todos los usuarios,
#         mientras que los usuarios normales solo pueden ver su propio perfil.
#         """
#         if self.request.user.is_superuser:
#             return User.objects.all()
#         return User.objects.filter(id=self.request.user.id)

#     def get_object(self):
#         """
#         Recupera un objeto de usuario basado en el ID en la URL.
#         """
#         lookup_field_value = self.kwargs[self.lookup_field]
#         obj = get_object_or_404(User, pk=lookup_field_value)
#         self.check_object_permissions(self.request, obj)
#         return obj
