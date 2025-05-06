
from django.contrib import admin
from django.urls import path, include
from person.urls import urlpatterns_persons
from entidad.urls import urlpatterns_entidad
from pucCoop.urls import urlpatterns_pucCoop
from pucSup.urls import urlpatterns_pucSup
from balCoop.urls import urlpatterns_balCoop
from balSup.urls import urlpatterns_balSup
from Resumen.urls import urlpatterns_Resumen
from exchangeRates.urls import urlpatterns_exchangeRates
from totalaccounts.urls import urlpatterns_totalAccounts

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    # path('admin/', admin.site.urls),
    path('api/', include(urlpatterns_persons)),
    path('api/', include(urlpatterns_entidad)),
    path('api/', include(urlpatterns_pucCoop)),
    path('api/', include(urlpatterns_pucSup)),
    path('api/', include(urlpatterns_balCoop)),
    path('api/', include(urlpatterns_balSup)),
    path('api/', include(urlpatterns_Resumen)),
    path('api/', include(urlpatterns_exchangeRates)),
    path('api/', include(urlpatterns_totalAccounts)),
    path('api/', include(('core.routers', 'core'), namespace='core-api')),

]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

