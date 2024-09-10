from django.urls import path

# from pucSup.views import PucSupApiView, PucSupApiViewDetail
from Resumen.views import (
    DocumentoUploadView,
    DocumentoListCreateView,
    DocumentoUpdateView,
    DocumentoDeleteView,
    DocumentoDescargaView,
)

urlpatterns_Resumen = [
    path("v1/subirDocumento", DocumentoUploadView.as_view()),
    path("v1/Documento", DocumentoListCreateView.as_view()),
    path("v1/Documento/<int:pk>/update/",DocumentoUpdateView.as_view(),name="documento-update",),
    path("v1/Documento/<int:pk>/delete/",DocumentoDeleteView.as_view(),name="documento-delete",),
    path('v1/Documentodescargar/<int:pk>/', DocumentoDescargaView.as_view(), name='documento-descargar'),
]
