from django.contrib import admin
from django.urls import path
from estaciones.views import migrateData, sendAlertas

urlpatterns = [
    path('migrar-data/', migrateData, name='migrate-data'),
    path('prueba/', sendAlertas, name='migrate-data-prueba'),
]
