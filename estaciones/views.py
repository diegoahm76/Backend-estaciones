from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
from estaciones.utils import transfer_data_datos, get_data_from_postgresql

@api_view(['GET'])
def migrateData(request):
    transfer_data_datos()
    return Response({'detail':'Migrating data...'})

@api_view(['GET'])
def sendAlertas(request):
    prueba = get_data_from_postgresql()
    if prueba:
        return Response({'detail':'Pasó...'})
    return Response({'detail':'Error...'})