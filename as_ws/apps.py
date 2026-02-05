from django.apps import AppConfig


class WSConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'as_ws'
    verbose_name = 'Websocket'
