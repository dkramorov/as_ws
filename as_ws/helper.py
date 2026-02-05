import json
import logging
import requests
import traceback
from asgiref.sync import sync_to_async
from jose import jwt
from redis import Redis
from django.apps import apps
from django.conf import settings
from fastapi import WebSocket


logger = logging.getLogger()


api_url = 'http://127.0.0.1:8040'
if hasattr(settings, 'API_URL'):
    api_url = settings.API_URL.rstrip('/')
customer_model_str = settings.AUTH_USER_MODEL
if hasattr(settings, 'AS_WS_USER_MODEL'):
    customer_model_str = settings.AS_WS_USER_MODEL
customer_model = apps.get_model(customer_model_str)
as_ws_prefix = 'as_ws'


def json_pretty_print(json_obj, pass_fields: list = None, default_deserializator=str):
    """Вывести json в человеческом виде,
       например,
         json.dumps({'test': 2.34}, default_deserializator=str)
         - если встречается неизвестный тип, будет вызван default_deserializator сериализатор
       :param json_obj: джисонина
       :param pass_fields: пропустить поля (верхний уровень словаря пока)
       :param default_deserializator: функция вызываемая при Object of type X is not JSON serializable
    """
    if pass_fields and isinstance(json_obj, dict):
        new_json_obj = {k: v for k, v in json_obj.copy().items() if not k in pass_fields}
        return json.dumps(new_json_obj, sort_keys=True, indent=2,
                          separators=(',', ': '), ensure_ascii=False, default=default_deserializator)
    return json.dumps(json_obj, sort_keys=True, indent=2,
                      separators=(',', ': '), ensure_ascii=False, default=default_deserializator)


def get_customer_channel_name(customer_id: int):
    """Получение имени канала пользователя для подписки в redis
       :param customer_id: ид пользователя
    """
    channel_name = '%s_tasks_%s' % (as_ws_prefix, customer_id)
    if hasattr(settings, 'PROJECT_NAME'):
        channel_name = '%s_%s' % (settings.PROJECT_NAME, channel_name)
    return channel_name


def send_ws_message(channel_name: str, message: dict):
    """Отправка сообщения через вебсокет
       :param message: сообщение (словарь), например, {'data': {'test': '123'}}
    """
    with Redis.from_url(settings.CACHES['default']['LOCATION']) as redis:
        pub = redis.publish(channel_name, json.dumps(message))
        return pub


def send_ws_api(customer, data):
    """Отправка по апи сообщение в сокет
       :param customer: пользователь
       :param data: данные
    """
    headers = {}
    try:
        if customer:
            headers['Authorization'] = 'Bearer %s' % customer.token
        r = requests.post('%s/ws/post/' % api_url, json=data, headers=headers)
        return r.json()
    except Exception as e:
        traceback.print_exc()
    return {}


@sync_to_async
def get_customer_from_token(token):
    if not token:
        return None
    token = token.split(' ')[-1]
    try:
        payload = customer_model().jwt_decode(token=token)
    except jwt.ExpiredSignatureError:
        logger.info('Действие токена закончилось')
        return None
    pk = payload.get('sub')
    customer = customer_model.objects.filter(pk=pk).first()
    logger.info('Пользователь %s' % customer)
    return customer


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket, subprotocol: str = None):
        if subprotocol:
            await websocket.accept(subprotocol)
        else:
            await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

    async def get_websocket(self, websocket: WebSocket, customer_id: int = None):
        """Поиск нужного вебсокета
           пишем customer_id в класс вебсокета
        """
        for connection in self.active_connections:
            if connection == websocket:
                websocket.customer_id = customer_id
                return connection


connection_manager = ConnectionManager()
