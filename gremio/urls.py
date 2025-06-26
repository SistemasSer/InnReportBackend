from django.urls import path
from gremio.views import GremioView, GremioToEntityView

urlpatterns_gremio = [
    path('v1/gremio/', GremioView.as_view(), name='gremio-list-create'),
    path('v1/gremio/<int:pk>/', GremioView.as_view(), name='gremio-delete-update'),

    path('v1/gremio-entity/', GremioToEntityView.as_view(), name='gremioEntity-list-create'),
    path('v1/gremio-entity/<int:pk>/', GremioToEntityView.as_view(), name='gremioEntity-update'),
]