Описание
-----------
Менеджер для работы с websocket

Установка пакетом
-----------
Для локальной разработки::
    pip install -e packages/as_ws

main.py::
```
from as_ws import router as ws_router, ws_routes

app.include_router(ws_router.router)

@app.post('/ws/post/')
async def ws_post_route(request: Request, ws_body: dict = None):
    """Websocket http запрос
       :param request: HttpRequest
       :param ws_body: данные
    """
    return await ws_routes.ws_route(
        request=request,
        router_prefix=ws_router.router.prefix,
        data=ws_body,
    )
```

Импорт
-----------
Проверка::
    from as_ws.router import router

Проверка с клиента::
```
       ws = new WebSocket(`wss://admin....ru/ws/tasks/`, ["auth|eyJhbGciOiJ...","protocol2"]); // тут похрен какие заголовки передавать и в каком порядке, ожидаем, что один из них будет auth|token
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
```

Удаление
-----------
Удалить пакет::
    pip uninstall as_ws

Для создания пакета
https://docs.python.org/3.10/distutils/introduction.html#distutils-simple-example
https://docs.python.org/3.10/distutils/sourcedist.html
::
    python setup.py sdist




