import asyncio
import json
import logging

import traceback
from asgiref.sync import sync_to_async
from django.apps import apps
from django.conf import settings
from django.core.cache import cache
from fastapi import WebSocket, APIRouter
from jose import jwt
from redis.asyncio import Redis
from starlette.websockets import WebSocketDisconnect

from as_ws.helper import (
    json_pretty_print,
    get_customer_from_token,
    get_customer_channel_name,
    connection_manager,
    customer_model,
)


router = APIRouter(
    prefix='/ws/tasks',
    tags=['Работа через Websocket'],
)


logger = logging.getLogger()


async def listen_queue(websocket: WebSocket, channel_name: str):
    """Персональная очередь сообщений вебсокета
       Принимает отправку через redis_publish
    """
    redis = None
    try:
        redis = Redis.from_url(settings.CACHES['default']['LOCATION'])
        await redis.initialize()
        async with redis.pubsub() as pubsub:
            await pubsub.subscribe(channel_name)
            logger.info('%s subscribed' % (channel_name, ))
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message:
                    out_message = message.get('data')
                    if isinstance(out_message, bytes):
                        out_message = json.loads(out_message.decode())
                        out_message['action'] = 'event'
                        out_message['pretty'] = 1
                        await websocket.send_text(json_pretty_print(out_message))
                await asyncio.sleep(1)
            await pubsub.unsubscribe(channel_name)
    except Exception:
        print(traceback.format_exc())
    if redis:
        await redis.close()


async def redis_publish(channel_name: str, message: dict):
    """Записываем в канал редиса, чтобы сообщение отправилось всем
       :param channel_name: имя канала
       :param message: словарь с сообщением
    """
    redis = Redis.from_url(settings.CACHES['default']['LOCATION'])
    await redis.initialize()
    result = await redis.publish(channel_name, message=json.dumps(message))
    await redis.close()
    return result


@router.websocket('/')  # @router.websocket_route будет 403 отдавать
async def ws_tasks(websocket: WebSocket):
    """/ws/tasks для вебсокета
       https://www.starlette.io/websockets/
    """
    ###await connection_manager.connect(websocket)
    # Указыаем subprotocol, если хотим получать токен через заголовок 'Sec-WebSocket-Protocol'
    # при этом отправлять надо так:
    # var ws = new WebSocket(host + '/ws/tasks/', ['Token', 'auth|' + token]);
    await connection_manager.connect(websocket, subprotocol='Token')

    token = websocket.query_params.get('token')
    auth = websocket.headers.get('Authorization')
    if auth and not token:
        token = auth.split(' ')[-1]

    # Вебсокет с браузера не умеет ничего отправлять в заголовках, кроме протокола
    # ws = new WebSocket(`ws://localhost:8040/ws/tasks/?token=e...`);
    # ws = new WebSocket(`ws://localhost:8040/ws/tasks/`, ["protocol1", "protocol2"]);
    # Получать будем 'sec-websocket-protocol': 'protocol1, protocol2'
    logger.info(websocket.headers)
    sec_websocket_protocol = websocket.headers.get('Sec-WebSocket-Protocol')
    if sec_websocket_protocol and not token:
        for item in sec_websocket_protocol.split(','):
            # ожидаем auth|token
            item = item.strip()
            if item.startswith('auth|'):
                token = item.split('auth|')[-1]

    customer = await get_customer_from_token(token)
    if not customer:
        await connection_manager.send_personal_message(json_pretty_print({
            'error': 'Пользователь не найден',
        }), websocket=websocket)
        connection_manager.disconnect(websocket)
        return

    channel_name = get_customer_channel_name(customer_id=customer.id)
    asyncio.create_task(listen_queue(websocket, channel_name))

    while True:
        # Пробуем найти вебсокет и актуализировать ид пользователя
        ws = await connection_manager.get_websocket(websocket=websocket, customer_id=customer.id)
        if ws and hasattr(ws, 'customer_id'):
            logger.info('ws user found %s' % ws.customer_id)
        else:
            logger.info('ws user not found')

        request_id = websocket.query_params.get('request_id')
        client_id = None
        text = None
        try:
            text = await websocket.receive_text()
            data = json.loads(text)
            if data.get('request_id'):
                request_id = data['request_id']
            if data.get('client_id'):
                client_id = data['client_id']
        except WebSocketDisconnect as e:
            logger.info('[DISCONNECT]: %s, %s ws disconnected' % (str(e), customer))
            connection_manager.disconnect(websocket)
            return
        except Exception as e:
            #traceback.print_exc()
            logger.info('[ERROR]: %s, %s, text: %s' % (str(e), customer, text))
            await connection_manager.send_personal_message(json_pretty_print({
                'request_id': request_id,
                'client_id': client_id,
                'error': 'Произошла ошибка %s' % str(e),
            }), websocket=websocket)
            #connection_manager.disconnect(websocket)
            continue
        action = data.get('action', '')

        if not customer:
            await connection_manager.send_personal_message(json_pretty_print({
                'request_id': request_id,
                'client_id': client_id,
                'action': action,
                'pretty': 1,
                'error': 'Срок жизни токена закончился',
                'token_expired': True,
            }), websocket=websocket)
            continue

        broadcast = data.get('broadcast')
        if action == 'test':
            test_message = {
                'request_id': request_id,
                'client_id': client_id,
                'action': action,
                'pretty': 1,
                'result': 'success',
            }
            await websocket.send_text(json_pretty_print(test_message))
            # Рассылаем тест всем
            if broadcast:
                #if not user.is_superuser or not user.is_active or not user.is_staff:
                #    await connection_manager.send_personal_message(json_pretty_print({
                #        'request_id': request_id,
                #        'client_id': client_id,
                #        'error': 'Недостаточно прав для отправки широковещательного сообщения',
                #    }), websocket=websocket)
                #else:
                #    ###await redis_publish(channel_name=channel_name, message=test_message)
                #    await connection_manager.broadcast(json_pretty_print(test_message))
                pass
            else:
                #await connection_manager.send_personal_message(json_pretty_print(test_message), websocket=websocket)
                pass
        else:
            await connection_manager.send_personal_message(json_pretty_print({
                'request_id': request_id,
                'client_id': client_id,
                'pretty': 1,
                'error': 'action "%s" не поддерживается' % action,
            }), websocket=websocket)
