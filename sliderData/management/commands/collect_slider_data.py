import os
import json
import requests
from datetime import date, timedelta
from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = "Recolecta datos de tasas de cambio y guarda un JSON para el carrusel"

    def get_trm_data_for_day(self, target_date: date) -> dict:
        """
        Intenta obtener la TRM usando primero `vigenciadesde`, si no existe, intenta con `vigenciahasta`.
        """
        trm_url = "https://www.datos.gov.co/resource/32sa-8pi3.json"
        day_str = target_date.isoformat()

        # 1. Buscar por vigenciadesde
        response = requests.get(trm_url, params={"vigenciadesde": day_str}).json()
        if response:
            return response[0]

        # 2. Fallback por vigenciahasta (caso festivo)
        response_fallback = requests.get(trm_url, params={"vigenciahasta": day_str}).json()
        if response_fallback:
            return response_fallback[0]

        return {}

    def find_previous_trm(self, today: date, trm_today: dict, max_days_back=7) -> tuple[date, dict]:
        """
        Busca la TRM más reciente anterior a la actual (no igual), retrocediendo hasta `max_days_back` días.
        """
        for i in range(1, max_days_back + 1):
            prev_date = today - timedelta(days=i)
            trm_prev = self.get_trm_data_for_day(prev_date)

            if not trm_prev:
                continue

            if trm_prev.get("valor") != trm_today.get("valor"):
                return prev_date, trm_prev

        return today - timedelta(days=1), {}

    def handle(self, *args, **kwargs):
        today = date.today()
        today_str = today.isoformat()

        try:
            # === Obtener TRM actual ===
            trm_data_today = self.get_trm_data_for_day(today)
            trm_today_value = float(trm_data_today.get("valor", 0))

            # === Buscar TRM válida anterior (no igual) ===
            yesterday_date, trm_data_yesterday = self.find_previous_trm(today, trm_data_today)
            yesterday_str = yesterday_date.isoformat()
            trm_yesterday_value = float(trm_data_yesterday.get("valor", 0))

            # === Obtener tasas internacionales ===
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
            rates_yesterday = {}
            percent_diff = {}
            abs_diff = {}

            for code in relevant_currencies:
                usd_to_curr = quotes.get(f"USD{code}")
                if usd_to_curr:
                    today_rate = convert_usd_to_cop(1 / usd_to_curr, trm_today_value)
                    yesterday_rate = convert_usd_to_cop(1 / usd_to_curr, trm_yesterday_value)

                    rates_today[code] = today_rate
                    rates_yesterday[code] = yesterday_rate

                    abs_diff[code] = round(today_rate - yesterday_rate, 2)
                    if yesterday_rate != 0:
                        percent = ((today_rate - yesterday_rate) / yesterday_rate) * 100
                        percent_diff[code] = f"{round(percent, 2)}%"
                    else:
                        percent_diff[code] = "0.0%"
                else:
                    rates_today[code] = 0
                    rates_yesterday[code] = 0
                    abs_diff[code] = 0
                    percent_diff[code] = "0.0%"

            # === TRM (USD → COP) ===
            abs_diff["USD"] = round(trm_today_value - trm_yesterday_value, 2)
            if trm_yesterday_value:
                percent = ((trm_today_value - trm_yesterday_value) / trm_yesterday_value) * 100
                percent_diff["USD"] = f"{round(percent, 2)}%"
            else:
                percent_diff["USD"] = "0.0%"

            # === Estructura final ===
            slider_data = {
                "data_slider_today": {
                    "current_datetime": today_str,
                    "current_TRM": trm_data_today,
                    "external_data_today": {
                        "base": "COP",
                        "converted_from_usd": True,
                        "rates": rates_today,
                    }
                },
                "data_slider_yesterday": {
                    "yesterday_datetime": yesterday_str,
                    "yesterday_TRM": trm_data_yesterday,
                    "external_data_yesterday": {
                        "base": "COP",
                        "converted_from_usd": True,
                        "rates": rates_yesterday,
                    }
                },
                "calculated_differences": {
                    "calculated_percent": percent_diff,
                    "calculated_difference": abs_diff,
                }
            }

            output_path = os.path.join(settings.BASE_DIR, "sliderData", "data", "slider_data.json")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(slider_data, f, ensure_ascii=False, indent=2)

            self.stdout.write(self.style.SUCCESS(f"✅ Datos guardados en: {output_path}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Error al recolectar datos: {str(e)}"))
