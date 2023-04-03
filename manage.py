#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
from django.core.management import execute_from_command_line
import os
import sys

if 'runserver' not in sys.argv:
    # Ejecuta las tareas programadas automáticamente cuando el servidor no se está ejecutando
    execute_from_command_line(['manage.py', 'crontab', 'run'])

def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend_estaciones.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
