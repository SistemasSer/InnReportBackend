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
