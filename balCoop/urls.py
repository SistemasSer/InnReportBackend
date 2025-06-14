from django.urls import path
from balCoop.views import BalCoopApiView, BalCoopApiViewDetail, BalCoopApiViewA, BalCoopApiViewIndicador, BalCoopApiViewIndicadorC, BalCoopApiViewBalanceCuenta, BalCoopApiViewBalanceIndependiente
  
urlpatterns_balCoop = [
    path('v1/bal_coop', BalCoopApiView.as_view()), 
    path('v1/bal_coop/<int:id>', BalCoopApiViewDetail.as_view()), 
    path('v1/bal_coop_a', BalCoopApiViewA.as_view()), 
    path('v1/bal_coop/indicador_financiero', BalCoopApiViewIndicador.as_view()), 
    path('v1/bal_coop/indicador_cartera', BalCoopApiViewIndicadorC.as_view()), 
    path('v1/bal_coop/balance', BalCoopApiViewBalanceCuenta.as_view()), 
    path('v1/bal_coop/balanceIndependiente', BalCoopApiViewBalanceIndependiente.as_view()), 
]