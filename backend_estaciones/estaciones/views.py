from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
from estaciones.utils import transfer_data

@api_view(['GET'])
def migrateData(request):
    transfer_data()
    return Response({'detail':'Migrating data...'})