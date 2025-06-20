from django.apps import AppConfig
import os
from datetime import datetime
from django.conf import settings
from django.core.management import call_command
from threading import Thread


class SliderdataConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'sliderData'

    def ready(self):
        # Ejecutar verificación de slider_data.json al iniciar el servidor
        def verificar_slider_data():
            try:
                json_path = os.path.join(settings.BASE_DIR, 'sliderData', 'data', 'slider_data.json')
                hoy = datetime.today().date()

                # Si no existe el archivo o no es del día de hoy, genera los datos
                if not os.path.exists(json_path) or datetime.fromtimestamp(os.path.getmtime(json_path)).date() != hoy:
                    call_command('collect_slider_data')
            except Exception as e:
                # Log de error sin interrumpir el arranque del servidor
                print(f"❌ Error al verificar o ejecutar la tarea de slider_data: {e}")

        # Iniciar verificación en un hilo para no bloquear el startup
        Thread(target=verificar_slider_data, daemon=True).start()
