from rest_framework import status
from rest_framework.views import APIView 
from rest_framework.response import Response

from rest_framework.decorators import authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.authentication import JWTAuthentication

from pucSup.models import PucSupModel
from pucSup.serializers import PucSupSerializer 

# @authentication_classes([JWTAuthentication])  # Ensure JWT is used for this view
# @permission_classes([IsAuthenticated])
class PucSupApiView(APIView):
    def get(self, request):
        serializer = PucSupSerializer(PucSupModel.objects.all(), many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    def post(self, request): 
        #res = request.data.get('name')  
        serializer = PucSupSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(status=status.HTTP_200_OK, data=serializer.data)

# @authentication_classes([JWTAuthentication]) 
# @permission_classes([IsAuthenticated])
class PucSupApiViewDetail(APIView):
    def get_object(self, Codigo):
        try:
            return PucSupModel.objects.get(Codigo=Codigo)
        except PucSupModel.DoesNotExist:
            return None
    def get(self, request, id):
        post = self.get_object(id)
        serializer = PucSupSerializer(post)  
        return Response(status=status.HTTP_200_OK, data=serializer.data)
    # def put(self, request, id):
    #     post = self.get_object(id)
    #     if(post==None):
    #         return Response(status=status.HTTP_200_OK, data={ 'error': 'Not found data'})
    #     serializer = PucSupSerializer(post, data=request.data)
    #     if serializer.is_valid():
    #         serializer.save()
    #         return Response(status=status.HTTP_200_OK, data=serializer.data) 
    #     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    # def delete(self, request, id):
    #     post = self.get_object(id)
    #     post.delete()
    #     response = { 'deleted': True }
    #     return Response(status=status.HTTP_204_NO_CONTENT, data=response)

