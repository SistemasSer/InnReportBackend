from django.urls import path
from exchangeRates.views import ExchangeRatesFX, ExchangeRatesCommodities, CombinedExchangeRates, SolidarityFinancialData

urlpatterns_exchangeRates = [
    path('v1/exchange-rates/', ExchangeRatesFX.as_view(), name='exchange_rates'),
    path('v1/exchange-rates/rawMaterial', ExchangeRatesCommodities.as_view(), name='exchange_rates'),
    path('v1/exchange-rates/combined', CombinedExchangeRates.as_view(), name='exchange_rates'),
    path('v1/exchange-rates/combined', CombinedExchangeRates.as_view(), name='exchange_rates'),
    path('v1/SolidarityFinancialData', SolidarityFinancialData.as_view(), name='Solidarity and Financial Data'),
]