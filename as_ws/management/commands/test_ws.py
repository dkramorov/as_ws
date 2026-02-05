#-*- coding:utf-8 -*-
import os
import re
import time
import json
import datetime
import logging
import requests
import traceback

from django.db import models, connections
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.functional import cached_property

import websocket

from fastapi_core.models_abstract import JsonApiMixin
from as_ws.helper import api_url, customer_model, json_pretty_print, send_ws_api
#from apps.customer.models import Notify # моделька уведомлений


logger = logging.getLogger(__name__)


class Notify(JsonApiMixin):
    """Уведомление для пользователя
    """
    channel_choices = (
        (1, 'Email'),
        (2, 'Web push'),
        (3, 'Telegram'),
        (4, 'Websocket'),
    )
    event_choices = (
        (1, 'Оформление заказа'),
        (2, 'Изменение статуса заказа'),
        (3, 'Отмена заказа'),
        (4, 'Сообщение в чат (заказ)'),
        (5, 'Сообщение в чат (недвижимость)'),
        (6, 'Сообщение прочитано'),
        (7, 'Новое состояние чатов'),
    )
    customer = models.ForeignKey(customer_model, blank=True, null=True, on_delete=models.CASCADE,
        verbose_name='Пользователь')
    msg = models.CharField(max_length=255, blank=True, null=True,
        verbose_name='Текст сообщения')
    channel = models.IntegerField(blank=True, null=True, db_index=True,
        verbose_name='Канал доставки', choices=channel_choices)
    sent = models.BooleanField(blank=True, null=True, db_index=True,
        verbose_name='Отправлено', default=False)
    read = models.BooleanField(blank=True, null=True, db_index=True,
        verbose_name='Прочитано')
    event = models.IntegerField(blank=True, null=True, db_index=True,
        verbose_name='Событие', choices=event_choices)
    #template = models.ForeignKey(NotifyTemplate, blank=True, null=True, on_delete=models.SET_NULL,
    #    verbose_name='Шаблон')
    data = models.TextField(blank=True, null=True,
        verbose_name='Данные json')

    class Meta:
        abstract = True
        verbose_name = 'Уведомление'
        verbose_name_plural = 'Уведомления'

    def get_name(self):
        return '#%s %s' % (self.id, self.channel)

    def __str__(self):
        return self.get_name()

    def get_data(self):
        if not self.data:
            return {}
        try:
            return json.loads(self.data)
        except Exception:
            pass
        return {}

    @staticmethod
    def only_fields():
        """Поля для маршрутов
           для функции object_fields
        """
        return (
            'id',
            #'customer', # fk
            'msg',
            #'channel', # choices
            'sent',
            'read',
            #'event', # choices
            #'template', # fk
            #'data', # json
        )

    @cached_property
    def get_event(self) -> dict:
        if not self.event:
            return {}
        return {
            'id': self.event,
            'name': self.get_event_display(),
        }

    @cached_property
    def get_order(self) -> dict:
        """Получение информации по заказу
        """
        data = self.get_data()
        if data.get('order_id'):
            order_model = apps.get_model('order.Order')
            order = order_model.objects.filter(pk=data['order_id']).first()
            if order:
                return order.get_order
        return {}

    @cached_property
    def get_notify(self) -> dict:
        notify = object_fields(self, only_fields=self.only_fields())
        if self.customer_id and hasattr(self.customer, 'get_customer'):
            notify['customer'] = self.customer.get_customer
        if self.channel:
            notify['channel'] = {
                'id': self.channel,
                'name': self.get_channel_display(),
            }
        if self.event:
            notify['event'] = self.get_event
        notify['data'] = self.get_data()
        notify['order'] = self.get_order
        return notify

    @staticmethod
    def create_notify(customer,
                      event_type: int = 1,
                      channel: int = 4,
                      msg: str = None,
                      data: dict = None,
                      response_data: dict = None):
        """Создание уведомления
           :param customer: пользователь
           :param event_type: тип события из Notify.event_choices
           :param channel: канал для уведомления
           :param msg: сообщение
           :param data: данные json
           :param response_data: данные, которые отправляем в ws (ответ маршрута)
        """
        if not msg and event_type == 1:
            msg = 'Создано уведомление'
        notify = Notify.objects.create(
            customer=customer,
            msg=msg,
            channel=channel,
            sent=True,
            read=False,
            event=event_type,
            data=json.dumps(data) if data else None,
        )
        if isinstance(response_data, dict):
            # Преобразуем несериализуемые данные в сериализуемые (например, datetime.datetime)
            response_data = jsonable_encoder(response_data)
        else:
            response_data = None

        if channel == 4:
            # стукнуть в апи вебсокета
            send_ws_api(customer=customer, data={
                'action': 'new_notify',
                'event': notify.get_event,
                'data': response_data or notify.get_notify,
            })


def ws_client(customer_id: int):
    """Запустить websocket клиент
       :param customer_id: ид пользователя
    """
    customer = customer_model.objects.filter(pk=customer_id).first()
    if not customer:
        logger.info('Пользователь не найден')
        return
    token = customer.token
    uri = '%s/ws/tasks/' % api_url.replace('http://', 'ws://').replace('https://', 'wss://')

    # тест для прода
    #uri = 'wss://admin.bp-finance.ru/ws/tasks/'
    #token = 'eyJhbGciOiJ...'

    ws = None
    try:
        ws = websocket.create_connection(uri, header={
            #'Authorization': 'Bearer %s' % token,
            'Sec-WebSocket-Protocol': 'auth|%s' % token,
        })

        #msg = 'Hello, WebSocket!'
        #ws.send(msg)
        #logger.info('Sent: %s' % msg)

        #received_message = ws.recv()
        #logger.info('Received: %s' % received_message)

        #msg = {'a':'b', 'request_id': '1', 'client_id': '2'}
        #ws.send(json.dumps(msg))
        #logger.info('Sent: %s' % msg)

        #received_message = ws.recv()
        #logger.info('Received: %s' % received_message)

        while True:
            received_message = ws.recv()
            logger.info('Received: %s' % received_message)

    except Exception as e:
        #traceback.print_exc()
        logger.info('Error: %s' % e)
    finally:
        if ws:
            ws.close()


class Command(BaseCommand):
    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument('--broadcast',
            action = 'store_true',
            dest = 'broadcast',
            default = False,
            help = 'Send broadcast message')
        parser.add_argument('--customer_id',
            action = 'store',
            dest = 'customer_id',
            type = str,
            default = False,
            help = 'Send message to customer')
        parser.add_argument('--ws_client',
            action = 'store_true',
            dest = 'ws_client',
            default = False,
            help = 'Run websocket client')
        parser.add_argument('--resend_notify_id',
            action = 'store',
            dest = 'resend_notify_id',
            type = int,
            default = False,
            help = 'Resend notify with id')
        parser.add_argument('--get_ws_clients',
            action = 'store_true',
            dest = 'get_ws_clients',
            default = False,
            help = 'Get ws clients')

    def handle(self, *args, **options):
        """Тестирование вебсокета
           Слушать вебсокет для пользователя
           python manage.py test_ws --ws_client --customer_id=21
           Переотправить уведомление в вебсокет
           python manage.py test_ws --resend_notify_id=2
        """
        customer_id = None
        if options.get('customer_id'):
            customer_id = options['customer_id']

        query = customer_model.objects.all()
        if customer_id:
            customer = query.filter(pk=customer_id).first()
        else:
            customer = query.filter(login='jocker').first()
        assert customer

        if options.get('ws_client'):
            while True:
                try:
                    ws_client(customer_id=customer.id)
                except Exception as e:
                    #traceback.print_exc()
                    time.sleep(3)
            return

        if options.get('resend_notify_id'):
            notify = Notify.objects.get(pk=options['resend_notify_id'])
            send_ws_api(customer=notify.customer, data={
                'action': 'new_notify',
                'data': notify.get_notify,
            })
            return

        if options.get('get_ws_clients'):
            return

        if options.get('broadcast'):
            headers = {
                'Authorization': 'Bearer %s' % customer.token,
            }
            r = requests.post('%s/ws/tasks/' % api_url, json={
                'data': {
                    'request_id': 'test0',
                    'client_id': 'test0',
                    'broadcast': 1,
                    'pretty': 1,
                    'data': {
                        'abc': '123',
                    }
                },
            }, headers=headers)
            result = r.json()
            print(json_pretty_print(result))
        else:
            result = send_ws_api(customer, data = {
                'request_id': 'test0',
                'client_id': 'test0',
                'pretty': 1,
                'data': {
                    'abc': '123',
                }
            })
            print(json_pretty_print(result))
