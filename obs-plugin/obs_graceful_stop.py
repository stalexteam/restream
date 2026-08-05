"""
OBS-скрипт: повідомляє контролер на VPS про свідоме натискання "Стоп
трансляцію", ДО того як OBS реально розірве з'єднання.

Навіщо: контролер на VPS сам по собі не може відрізнити "користувач
натиснув Стоп" від "провайдер обірвав зв'язок" — з боку RTMP-сервера
обидва випадки виглядають як звичайний розрив з'єднання. Цей скрипт
закриває саме цю прогалину: шле один HTTP-запит у момент STOPPING,
і лише тоді контролер завершує трансляцію штатно, а не вмикає заглушку.

Встановлення в OBS:
  Tools -> Scripts -> "+" -> обрати цей файл.
  У вкладці властивостей скрипта вказати:
    - URL контролера (http://ВАШ_VPS_IP:8790/obs/graceful-stop)
    - Токен (те саме значення, що obs_webhook_token у controller/config.json)
"""

import json
import urllib.request

import obspython as obs

controller_url = ""
webhook_token = ""
request_timeout_sec = 3


def send_graceful_stop_signal():
    if not controller_url or not webhook_token:
        obs.script_log(obs.LOG_WARNING, "Controller URL or token not set -- signal not sent")
        return

    request = urllib.request.Request(
        controller_url,
        method="POST",
        data=b"{}",
        headers={
            "Authorization": f"Bearer {webhook_token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=request_timeout_sec) as response:
            obs.script_log(obs.LOG_INFO, f"graceful-stop signal sent, response: {response.status}")
    except Exception as error:
        obs.script_log(obs.LOG_WARNING, f"failed to send signal to the controller: {error}")


def on_frontend_event(event):
    if event == obs.OBS_FRONTEND_EVENT_STREAMING_STOPPING:
        send_graceful_stop_signal()


def script_description():
    return (
        "Notifies the controller on the VPS about a deliberate stream stop, "
        "so it doesn't switch to the backup video on a normal Stop click."
    )


def script_properties():
    props = obs.obs_properties_create()
    obs.obs_properties_add_text(props, "controller_url", "Controller URL", obs.OBS_TEXT_DEFAULT)
    obs.obs_properties_add_text(props, "webhook_token", "Token", obs.OBS_TEXT_PASSWORD)
    return props


def script_defaults(settings):
    obs.obs_data_set_default_string(settings, "controller_url", "http://YOUR_VPS_IP:8790/obs/graceful-stop")
    obs.obs_data_set_default_string(settings, "webhook_token", "")


def script_update(settings):
    global controller_url, webhook_token
    controller_url = obs.obs_data_get_string(settings, "controller_url")
    webhook_token = obs.obs_data_get_string(settings, "webhook_token")


def script_load(settings):
    obs.obs_frontend_add_event_callback(on_frontend_event)


def script_unload():
    obs.obs_frontend_remove_event_callback(on_frontend_event)
