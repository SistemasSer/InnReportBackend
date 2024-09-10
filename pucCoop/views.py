from rest_framework import status
from rest_framework.views import APIView 
from rest_framework.response import Response

from rest_framework.decorators import authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.authentication import JWTAuthentication

from pucCoop.models import PucCoopModel
from pucCoop.serializers import PucCoppSerializer 

class PucCoopApiView(APIView):
    def get(self, request):
        serializer = PucCoppSerializer(PucCoopModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    def post(self, request): 
        #res = request.data.get('name')  
        serializer = PucCoppSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(status=status.HTTP_200_OK, data=serializer.data)

class PucCoppApiViewDetail(APIView):
    def get_object(self, Codigo):
        try:
            return PucCoopModel.objects.get(Codigo=Codigo)
        except PucCoopModel.DoesNotExist:
            return None
    def get(self, request, id):
        post = self.get_object(id)
        serializer = PucCoppSerializer(post)  
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    # def put(self, request, id):
    #     post = self.get_object(id)
    #     if(post==None):
    #         return Response(status=status.HTTP_200_OK, data={ 'error': 'Not found data'})
    #     serializer = PucCoppSerializer(post, data=request.data)
    #     if serializer.is_valid():
    #         serializer.save()
    #         return Response(status=status.HTTP_200_OK, data=serializer.data) 
    #     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    # def delete(self, request, id):
    #     post = self.get_object(id)
    #     post.delete()
    #     response = { 'deleted': True }
    #     return Response(status=status.HTTP_204_NO_CONTENT, data=response)

