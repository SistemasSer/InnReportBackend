from django.urls import path
from entidad.views import EntidadApiView, EntidadApiViewDetail, EntidadDefaulApiView
  
urlpatterns_entidad = [
    path('v1/entidad', EntidadApiView.as_view()), 
    path('v1/entidad_defaul', EntidadDefaulApiView.as_view()), 
    path('v1/entidad/<int:id>', EntidadApiViewDetail.as_view()), 
]