| Параметр    | значения    | описание                                    |
|-------------|--------|---------------------------------------------|
|requestType|CREATE_ORDER_FOR_EMISSION_IC CHANGE_STATUS|тип генерируемого запроса|
|quantity|число|число запрашиваемых кодов маркировки|
|cttId|строка|идентификатор пользователя в CTT|
|requestId|строка|идентификатор запроса CTT|
|status|строка|новый статус сообщения|
|time|число|новое время ожидания для сообщения|

Применимость параметров
- CREATE_ORDER_FOR_EMISSION_IC
    - cttId - если не задано то "77f5ccc7bd27d7"
    - quantity - если не задано то случайное число от 1 до 100
- CHANGE_STATUS
    - cttId - если не задано то "77f5ccc7bd27d7"
    - requestId
    - status
    - time
    
для формирования правильного запроса CHANGE_STATUS необходимо указать requestId и status и/или time

пример запуска 
node . --requestType=CREATE_ORDER_FOR_EMISSION_IC
