from django.urls import path
from pucSup.views import PucSupApiView, PucSupApiViewDetail
  
urlpatterns_pucSup = [
    path('v1/puc_sup', PucSupApiView.as_view()), 
    path('v1/puc_sup/<int:id>', PucSupApiViewDetail.as_view()), 
]