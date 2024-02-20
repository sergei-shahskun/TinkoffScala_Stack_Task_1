1 задание
Описание задачи, Java

У вас есть два эквивалентных сервиса, в которых можно узнать статус заявки при помощи функций `getApplicationStatus1` и `getApplicationStatus2`.

Информация в сервисах синхронизирована, поэтому источником правды можно считать тот сервис, который вернёт ответ первым.

Ваша задача написать метод для получения статуса заявки `performOperation`, который будет делать обращение к двум сервисам и возвращать ответ клиенту как только получен ответ хотя бы от одного из них.



Технические детали

1. У `performOperation` есть таймаут -15 секунд.

2. `performOperation` должен возвращать ответ клиенту как можно быстрее.

3. В теле `performOperation` должны выполняться запросы к сервисам (вызовы методов), а также обработка ответов сервисов и преобразование полученных данных в ответ нового метода.

4. Для успешно выполненной операции вернуть `ApplicationStatusResponse.Success`, где:

* `id` - идентификатор заявки (`Response.applicationId`)

* `status` - статус заявки (`Response.applicationStatus`)

5. В случае возникновения ошибки нужно вернуть `ApplicationStatusResponse.Failure`, где:

* `lastRequestTime` - время последнего запроса, завершившегося ошибкой (опциональное);

* `retriesCount` - количество неуспешных запросов к сервисам



Каждый из сервисов может вернуть один из ответов:

* `Response.Success` в случае успешно выполненного запроса

* `Response.RetryAfter` в случае, если сервис не может выполнить запрос; поле `delay` - желательная задержка перед повторным запросом

* `Response.Failure` в случае, если в ходе обработки запроса произошла ошибка



В качестве ответа пришлите полную реализацию интерфейса `Handler` (со списком импортов). В поле для ответа прикрепите ссылку на репозиторий с вашим решением.



Cниппеты кода

```java
public sealed interface Response {
  record Success(String applicationStatus, String applicationId) implements Response {}
  record RetryAfter(Duration delay) implements Response {}
  record Failure(Throwable ex) implements Response {}
}

public sealed interface ApplicationStatusResponse {
  record Failure(@Nullable Duration lastRequestTime, int retriesCount) implements ApplicationStatusResponse {}
  record Success(String id, String status) implements ApplicationStatusResponse {}
}

public interface Handler {
  ApplicationStatusResponse performOperation(String id);
}

public interface Client {
  //блокирующий вызов сервиса 1 для получения статуса заявки
  Response getApplicationStatus1(String id);
   
  //блокирующий вызов сервиса 2 для получения статуса заявки
  Response getApplicationStatus2(String id);

}
```