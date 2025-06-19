from django.urls import path
from .views import SliderDataView

urlpatterns = [
    path("slider-data/", SliderDataView.as_view(), name="slider-data")
]
