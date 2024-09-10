"""
URL configuration for inn_report_b project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from person.urls import urlpatterns_persons
from entidad.urls import urlpatterns_entidad
from pucCoop.urls import urlpatterns_pucCoop
from pucSup.urls import urlpatterns_pucSup
from balCoop.urls import urlpatterns_balCoop
from balSup.urls import urlpatterns_balSup
from Resumen.urls import urlpatterns_Resumen

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
    path('api/', include(('core.routers', 'core'), namespace='core-api')),

]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

