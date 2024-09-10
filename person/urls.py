from django.urls import path
from person.views import PersonApiView, PersonApiViewDetail
  
urlpatterns_persons = [
    path('v1/persons', PersonApiView.as_view()), 
    path('v1/persons/<int:id>', PersonApiViewDetail.as_view()), 
]