from rest_framework.serializers import ModelSerializer
from person.models import PersonModel


class PersonSerializer(ModelSerializer):
    class Meta:
        model = PersonModel
        # fields = ['id', 'name', 'address', 'phone']
        fields = '__all__'