from django.contrib import admin
from django.urls import path
from estaciones.views import migrateData

urlpatterns = [
    path('migrar-data/', migrateData, name='migrate-data'),
]
