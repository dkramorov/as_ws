import logging
from django.conf import settings
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse

from as_ws.helper import (
    json_pretty_print,
    connection_manager,
    get_customer_from_token,
    send_ws_message,
    get_customer_channel_name,
)


logger = logging.getLogger()


async def ws_route(request: Request, router_prefix: str, data: dict = None):
    """Websocket test
       ws = new WebSocket(`ws://localhost:8040/ws/tasks/?token=e...`);
       ws = new WebSocket(`ws://localhost:8040/ws/tasks/`, ["auth|token", "protocol2"]);
       ws.onopen = function() {
         console.log('соединение установлено');
       }
       ws.onmessage = function(event) {
         console.log(event);
       };
       ws.onerror = function(error) {
         console.log('ERROR', error);
       };
       ws.onclose = function(event) {
         if (event.wasClean) {
           console.log('Соединение закрыто, код=' + event.code + ' причина=' + event.reason);
         } else {
           console.log('Соединение разорвано, код=' + event.code + ' причина=' + event.reason);
         }
       };
    """
    if data:
        # Получение данных на вход для отправки на ws socket
        data['success'] = False
        customer = await get_customer_from_token(request.headers.get('Authorization'))
        logger.info('ws POST for %s received: %s' % (customer, json_pretty_print(data)))
        if customer:
            if data.get('broadcast'):
                data['success'] = False
                data['msg'] = 'Access restricted'
                #await connection_manager.broadcast(json_pretty_print(data))
            else:
                data['success'] = True
                channel_name = get_customer_channel_name(customer.id)
                send_ws_message(channel_name, data)
        return JSONResponse(content=data)
    return HTMLResponse("""
<!DOCTYPE html>
<html>
    <head>
        <title>Websocket test</title>
        <style>
            #websocket_form {
                display: none;
            }
            #websocket_form label {
                display: block;
            }
            .padd-30 {
                padding-top: 30px;
            }
            #websocket_form label {
                border: 1px solid #ccc;
                padding: 10px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="row padd-30">
                <form id="login_form">
                    <h2>Log In</h2>
                    <input id="username" name="username" />
                    <input id="password" name="password" type="password" />
                    <select id="host_path" name="host_path">
                      <option value="localhost:8040" selected>localhost:8040</option>
                      <option value="utils.bazar-agro.ru">utils.bazar-agro.ru</option>
                    </select>
                    <input name="grant_type" value="password" type="hidden" >
                    <button type="button" onclick="logIn()">Log In</button>
                </form>

                <div id="websocket_form">
                    <h1 id="websocket_form_title">Websocket:</h1>
                    <ul>
                        <li>"action": "test" # действие</li>
                        <li>"pretty": 1 # форматирование с отступами</li>
                        <li>"request_id": 3 # ид запроса</li>
                        <li>"client_id": adf # ид клиентского запроса</li>
                        <li>"broadcast": 1 # если передавать broadcast сообщение уйдет всем</li>
                    </ul>
                    <form action="" onsubmit="sendMessage(event)">
                        <label>
                            <input type="radio" name="messageText" autocomplete="off" value='{"action": "test", "request_id": 1, "client_id": "abc1"}'/>
                            <code>{"action": "test", "request_id": 1, "client_id": "abc1"}</code>
                            <div>Тестовое сообщение</div>
                        </label><br>
                        <label>
                            <input type="radio" name="messageText" autocomplete="off" value='{"action": "test", "request_id": 1, "client_id": "abc1", "broadcast": 1}'/>
                            <code>{"action": "test", "request_id": 2, "client_id": "abc2", "broadcast": 1}</code>
                            <div>Тестовое сообщение для всех</div>
                        </label><br>
                        <button>Отправить</button>
                        <button onclick="clearMessages(event)">Очистить</button>
                    </form>
                    <div style="padding-top: 30px;"><hr /></div>
                    <ul id='messages'></ul>
                </div>
            </div>
        </div>
        <script>
            var user_id = null;
            var user_token = null;
            var ws = null;

            function clearMessages(event) {
                document.getElementById('messages').innerHTML = '';
                if (event != undefined) {
                    event.preventDefault();
                }
            }
            
            function sendMessage(event) {
                var msg = '{"action": "unknown"}';
                var radio_arr = document.querySelectorAll('input[name=messageText]:checked');
                if (radio_arr != undefined && radio_arr.length > 0) {
                    msg = radio_arr[0].value;
                }
                ws.send(JSON.stringify({"data": JSON.parse(msg)}));
                if (event != undefined) {
                    event.preventDefault();
                }
            }

            function connect_ws(ws_scheme, host_path, user_token) {
                ws = new WebSocket(ws_scheme + `${host_path}%s/?token=${user_token}&request_id=req-01`);
                ws.onopen = function() {
                    console.log('соединение установлено');
                    ws.send(JSON.stringify({'data': {'action': 'test'}}));
                    document.getElementById('websocket_form_title').innerHTML = host_path;
                }
                ws.onmessage = function(event) {
                    console.log(event);
                    var messages = document.getElementById('messages');
                    var message = document.createElement('li');
                    var pre = document.createElement('pre');
                    var data = JSON.parse(event.data);
                    if (data.pretty != undefined) {
                        pre.innerHTML = event.data;
                    } else {
                        var content = document.createTextNode(JSON.stringify(data));
                        pre.appendChild(content);
                    }
                    message.appendChild(pre);
                    messages.appendChild(message);
                };
                ws.onclose = function(event) {
                    console.log('Соединение разорвано, код=' + event.code + ' причина=' + event.reason);
                    setTimeout(function() {
                      connect_ws(ws_scheme, host_path, user_token);
                    }, 1000);
                };
            }

            function logIn() {
                let formData = new FormData(document.getElementById('login_form'));
                let host_path = document.querySelector('select[name=host_path]').value;
                let scheme = 'https://';
                let ws_scheme = 'wss://';
                let login_path = '%s';
                if (host_path.startsWith('localhost') || host_path.startsWith('127.0.0.1')){
                    scheme = 'http://';
                    ws_scheme = 'ws://';
                }
                fetch(
                    scheme + host_path + login_path,
                    {
                        method: 'POST',
                        body: formData
                    }
                )
                .then(response => {
                    if (response.status == 200) {
                        document.getElementById('login_form').style.display = 'none';
                        document.getElementById('websocket_form').style.display = 'block';
                    }
                    return response.json()
                })
                .then(response => {
                    user_token = response.access_token;
                    console.log(JSON.stringify(response));
                    if (user_token == undefined) {
                        console.log('token not received');
                        return;
                    }
                    connect_ws(ws_scheme, host_path, user_token);
                })
            }

            //logIn(); // only with autocomplete filled form
        </script>
    </body>
</html>
""" % (
        router_prefix,
        settings.API_TOKEN_URL if hasattr(settings, 'API_TOKEN_URL') else '/api/token_auth/',
    ))
