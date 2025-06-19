import os
import json
import requests
from datetime import date, timedelta
from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = "Recolecta datos de tasas de cambio y guarda un JSON para el carrusel"

    def handle(self, *args, **kwargs):
        today = date.today()
        yesterday = today - timedelta(days=1)

        today_str = today.isoformat()
        yesterday_str = yesterday.isoformat()

        try:
            # === TRM oficial de Colombia desde datos.gov.co ===
            trm_url = "https://www.datos.gov.co/resource/32sa-8pi3.json"
            trm_today = requests.get(trm_url, params={"vigenciadesde": today_str}).json()
            trm_yesterday = requests.get(trm_url, params={"vigenciadesde": yesterday_str}).json()

            trm_data_today = trm_today[0] if trm_today else {}
            trm_data_yesterday = trm_yesterday[0] if trm_yesterday else {}

            trm_today_value = float(trm_data_today.get("valor", 0))
            trm_yesterday_value = float(trm_data_yesterday.get("valor", 0))

            # === API de currencylayer via exchangerate.host (endpoint /live) ===
            access_key = settings.EXCHANGE_API_KEY
            url = f"https://api.exchangerate.host/live?access_key={access_key}"

            response = requests.get(url)
            data = response.json()

            if not data.get("success") or "quotes" not in data:
                raise Exception("No se pudo obtener datos de la API /live")

            quotes = data["quotes"]

            # === Monedas relevantes ===
            global_currencies = ["EUR", "GBP", "JPY", "CHF"]
            regional_currencies = ["MXN", "BRL", "CLP", "PEN", "ARS", "UYU", "BOB", "PYG", "VES", "DOP"]
            relevant_currencies = global_currencies + regional_currencies

            def convert_usd_to_cop(usd_value, usd_to_cop):
                return round(usd_value * usd_to_cop, 2)

            rates_today = {}
            for code in relevant_currencies:
                usd_to_curr = quotes.get(f"USD{code}")
                if usd_to_curr:
                    cop_value = convert_usd_to_cop(1 / usd_to_curr, trm_today_value)
                    rates_today[code] = cop_value

            rates_yesterday = {}
            for code in relevant_currencies:
                usd_to_curr = quotes.get(f"USD{code}")
                if usd_to_curr:
                    cop_value = convert_usd_to_cop(1 / usd_to_curr, trm_yesterday_value)
                    rates_yesterday[code] = cop_value

            # === Estructura final ===
            slider_data = {
                "data_slider_today": {
                    "current_datetime": today_str,
                    "current_TRM": trm_data_today,
                    "external_data_today": {
                        "base": "COP",
                        "converted_from_usd": True,
                        "rates": rates_today
                    }
                },
                "data_slider_yesterday": {
                    "yesterday_datetime": yesterday_str,
                    "yesterday_TRM": trm_data_yesterday,
                    "external_data_yesterday": {
                        "base": "COP",
                        "converted_from_usd": True,
                        "rates": rates_yesterday
                    }
                }
            }

            # === Guardar JSON
            output_path = os.path.join(settings.BASE_DIR, "sliderData", "data", "slider_data.json")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(slider_data, f, ensure_ascii=False, indent=2)

            self.stdout.write(self.style.SUCCESS(f"✅ Datos guardados en: {output_path}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Error al recolectar datos: {str(e)}"))
