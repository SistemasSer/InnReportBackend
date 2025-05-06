from django.urls import path
# from .views import EchoPostView

from totalaccounts.views import TotalAccounts


urlpatterns_totalAccounts = [
    path('v1/totalaccounts', TotalAccounts.as_view(), name='Total Accounts'),
]