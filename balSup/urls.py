from django.urls import path
from balSup.views import BalSupApiView, BalSupApiViewDetail, BalSupApiViewA, BalSupApiViewIndicador, BalSupApiViewIndicadorC
  
urlpatterns_balSup = [
    path('v1/bal_sup', BalSupApiView.as_view()), 
    path('v1/bal_sup/<int:id>', BalSupApiViewDetail.as_view()), 
    path('v1/bal_sup_a', BalSupApiViewA.as_view()), 
    path('v1/bal_sup/indicador_financiero', BalSupApiViewIndicador.as_view()), 
    path('v1/bal_sup/indicador_cartera', BalSupApiViewIndicadorC.as_view()), 
]