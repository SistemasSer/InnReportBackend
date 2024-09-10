from django.urls import path
from pucCoop.views import PucCoopApiView, PucCoppApiViewDetail
  
urlpatterns_pucCoop = [
    path('v1/puc_coop', PucCoopApiView.as_view()), 
    path('v1/puc_coop/<int:id>', PucCoppApiViewDetail.as_view()), 
]