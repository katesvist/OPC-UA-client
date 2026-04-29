# OPC UA Client Service: запуск через custom Docker Compose

Этот документ является отдельным README-дубликатом для сценария, когда клиент запускается в Docker, а OPC UA сервер находится снаружи: на рабочей машине, в лабораторной сети, через VPN/маршрут или на удаленном промышленном стенде.

Основной `README.md` не изменяется.

## Что поднимает custom compose

Файл `docker-compose.server.yml` поднимает:

- `opcua-client` - сам OPC UA client service, API доступен на `http://127.0.0.1:8080`;
- `redis` - буфер событий при недоступности RabbitMQ;
- `rabbitmq` - брокер для публикации OPC UA событий;
- RabbitMQ Management UI - `http://127.0.0.1:15672`.

В отличие от `docker-compose.yaml`, custom server compose не поднимает mock OPC UA server. OPC UA endpoint должен быть доступен извне.

## Важная схема адресов

Внутри одного Docker Compose проекта сервисы видят друг друга по именам:

- Redis: `redis://redis:6379/0`
- RabbitMQ: `amqp://guest:guest@rabbitmq:5672/`

OPC UA сервер остается внешним:

- пример лабораторного адреса: `opc.tcp://192.168.0.120:4840`
- пример локального туннеля Prosys: `opc.tcp://host.docker.internal:15353/OPCUA/SimulationServer`

Если Redis/RabbitMQ запущены не в этом compose, а отдельными контейнерами на хосте, тогда в конфиге можно использовать:

- `redis://host.docker.internal:6379/0`
- `amqp://guest:guest@host.docker.internal:5672/`

Для текущего `docker-compose.server.yml` правильнее использовать имена сервисов `redis` и `rabbitmq`.

## Предварительная проверка сети

На Windows PowerShell перед запуском клиента проверьте, что OPC UA сервер доступен по TCP:

```powershell
Test-NetConnection 192.168.0.120 -Port 4840
```

Ожидаемый результат:

```text
TcpTestSucceeded : True
```

Важно: `opc.tcp://...` не обязан открываться в браузере. Это не HTTP URL. Для проверки используйте `Test-NetConnection`, UAExpert или сам OPC UA клиент.

Если доступ идет через отдельный маршрут, он может выглядеть так:

```bat
route add 192.168.0.0 mask 255.255.255.0 10.24.7.1
```

Маршрут и VPN должны быть активны до запуска контейнера клиента.

## Custom compose

Актуальная структура `docker-compose.server.yml`:

```yaml
services:
  redis:
    image: redis:7.4-alpine
    command: ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"]
    restart: unless-stopped
    ports:
      - "127.0.0.1:6379:6379"

  rabbitmq:
    image: rabbitmq:4-management
    restart: unless-stopped
    ports:
      - "127.0.0.1:5672:5672"
      - "127.0.0.1:15672:15672"

  opcua-client:
    build:
      context: .
    restart: unless-stopped
    environment:
      OPC_CONFIG_FILE: /service/examples/config.prosys.server.yaml
    volumes:
      - ./examples/config.prosys.server.yaml:/service/examples/config.prosys.server.yaml:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ports:
      - "127.0.0.1:8080:8080"
    depends_on:
      redis:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
```

Не добавляйте `network_mode: host`, если нужен доступ к API клиента через `127.0.0.1:8080` на Docker Desktop. При `network_mode: host` публикация `ports` может не работать так, как ожидается.

## Custom config

Compose передает путь до конфига через переменную:

```yaml
environment:
  OPC_CONFIG_FILE: /service/examples/config.prosys.server.yaml
```

И монтирует локальный файл внутрь контейнера:

```yaml
volumes:
  - ./examples/config.prosys.server.yaml:/service/examples/config.prosys.server.yaml:ro
```

Для реального стенда обычно удобнее сделать отдельный локальный файл, например:

```text
config.full.docker.yaml
```

И поменять compose так:

```yaml
environment:
  OPC_CONFIG_FILE: /service/config.full.docker.yaml
volumes:
  - ./config.full.docker.yaml:/service/config.full.docker.yaml:ro
```

Не коммитьте файл с реальными логинами и паролями.

## Пример конфига для реального OPC UA сервера

```yaml
service:
  name: opc-ua-client-service
  environment: windows-real-test
  mode: edge

api:
  host: 0.0.0.0
  port: 8080
  management_token: secret-token

logging:
  level: INFO
  json_logs: true

buffer:
  redis_url: redis://redis:6379/0
  key_prefix: opcua-client
  max_attempts: 10
  flush_batch_size: 100
  flush_interval_seconds: 5
  retry_base_delay_seconds: 5
  retry_max_delay_seconds: 300
  retention_hours: 72

publisher:
  mode: rabbitmq
  url: amqp://guest:guest@rabbitmq:5672/
  exchange: opcua.events
  exchange_type: direct
  routing_key: opcua.parameter.events
  queue_name: opcua.parameter.events
  declare_exchange: true
  declare_queue: true
  durable: true
  timeout_seconds: 5

endpoints:
  - id: remote-opc-server
    enabled: true
    url: opc.tcp://192.168.0.120:4840
    security_policy: None
    security_mode: None
    auth:
      mode: username_password
      username: "<OPC_USERNAME>"
      password: "<OPC_PASSWORD>"
    session_timeout_ms: 60000
    request_timeout_seconds: 5
    reconnect_policy:
      initial_delay_seconds: 2
      max_delay_seconds: 30
      backoff_multiplier: 2
      failure_threshold: 5
    subscription_defaults:
      publish_interval_ms: 1000
      keepalive_count: 10
      lifetime_count: 30
      queue_size: 100
    metadata:
      source_id: remote-opc-lab
      source_system_id: opc-server-192-168-0-120
      owner_type: rig
      owner_id: rig-01
      site_id: remote-lab
      asset_id: opc-sim
      tags:
        - edge
        - opcua
        - windows

nodes:
  - id: rw-float
    endpoint_id: remote-opc-server
    node_id: ns=3;s="OPC_Data_exchange"."OPC_Dif_Pressure_Reg_Limit"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_RW_FLOAT
    parameter_name: Test RW Float
    expected_type: float
    write_enabled: true
    unit: unit

  - id: rw-bool
    endpoint_id: remote-opc-server
    node_id: ns=3;s="OPC_Data_exchange"."OPC_Load_Reg_Setpoint_Visible"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_RW_BOOL
    parameter_name: Test RW Bool
    expected_type: bool
    write_enabled: true
    unit: state

  - id: rw-char
    endpoint_id: remote-opc-server
    node_id: ns=3;s="DB_For_Test"."cTest"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_RW_CTEST
    parameter_name: Test RW cTest
    expected_type: char
    write_enabled: true
    unit: char

  - id: rw-int
    endpoint_id: remote-opc-server
    node_id: ns=3;s="DB_For_Test"."iTest"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_RW_ITEST
    parameter_name: Test RW iTest
    expected_type: int
    write_enabled: true
    unit: unit

  - id: rw-real
    endpoint_id: remote-opc-server
    node_id: ns=3;s="DB_For_Test"."rTest"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_RW_RTEST
    parameter_name: Test RW rTest
    expected_type: float
    write_enabled: true
    unit: unit

  - id: rw-string
    endpoint_id: remote-opc-server
    node_id: ns=3;s="DB_For_Test"."sTest"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_RW_STEST
    parameter_name: Test RW sTest
    expected_type: str
    write_enabled: true
    unit: text
```

Для массивов у ноды нужно явно указать форму значения:

```yaml
  - id: test-load-array
    endpoint_id: remote-opc-server
    node_id: ns=3;s="DB_For_Test"."testLoad"
    acquisition_mode: subscription
    sampling_interval_ms: 1000
    parameter_code: TEST_LOAD_ARRAY
    parameter_name: Test Load Array
    expected_type: int
    value_shape: array
    write_enabled: true
    unit: unit
```

## Запуск

Из папки репозитория клиента:

```powershell
docker compose -f .\docker-compose.server.yml up --build -d
```

Проверить контейнеры:

```powershell
docker compose -f .\docker-compose.server.yml ps
```

Смотреть логи клиента:

```powershell
docker compose -f .\docker-compose.server.yml logs -f opcua-client
```

Перезапустить только клиента:

```powershell
docker compose -f .\docker-compose.server.yml restart opcua-client
```

Остановить стенд:

```powershell
docker compose -f .\docker-compose.server.yml down
```

## Проверка API клиента

Создать заголовок авторизации:

```powershell
$headers = @{ Authorization = "Bearer secret-token" }
```

Если используется стандартный примерный конфиг из репозитория, токен будет:

```powershell
$headers = @{ Authorization = "Bearer example-management-token" }
```

Health:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/health" -Headers $headers
```

Ready:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/ready" -Headers $headers | ConvertTo-Json -Depth 10
```

Connections:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/connections" -Headers $headers | ConvertTo-Json -Depth 10
```

Subscriptions:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/subscriptions" -Headers $headers | ConvertTo-Json -Depth 10
```

Buffer stats:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/buffer/stats" -Headers $headers | ConvertTo-Json -Depth 10
```

Metrics:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/metrics" -Headers $headers
```

## Browse

Browse показывает структуру address space OPC UA сервера.

```powershell
$browseBody = @{
  endpoint_id = "remote-opc-server"
  max_depth = 2
  include_variables = $true
  include_objects = $true
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8080/browse" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $browseBody | ConvertTo-Json -Depth 10
```

Если нужен browse от конкретной ноды:

```powershell
$browseBody = @{
  endpoint_id = "remote-opc-server"
  node_id = 'ns=3;s=PLC'
  max_depth = 2
  include_variables = $true
  include_objects = $true
} | ConvertTo-Json
```

## Read

```powershell
$readBody = @{
  endpoint_id = "remote-opc-server"
  node_id = 'ns=3;s="DB_For_Test"."sTest"'
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8080/read" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $readBody | ConvertTo-Json -Depth 10
```

В PowerShell удобнее заворачивать `node_id` в одинарные кавычки, тогда двойные кавычки внутри OPC UA NodeId не нужно экранировать.

## Write

Запись строки:

```powershell
$writeBody = @{
  endpoint_id = "remote-opc-server"
  node_id = 'ns=3;s="DB_For_Test"."sTest"'
  value = "ABCDE"
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8080/write" `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $writeBody | ConvertTo-Json -Depth 10
```

Запись CHAR:

```powershell
$writeBody = @{
  endpoint_id = "remote-opc-server"
  node_id = 'ns=3;s="DB_For_Test"."cTest"'
  value = "Z"
} | ConvertTo-Json
```

Запись массива:

```powershell
$writeBody = @{
  endpoint_id = "remote-opc-server"
  node_id = 'ns=3;s="DB_For_Test"."testLoad"'
  value = @(1, 2, 3, 4, 5)
} | ConvertTo-Json -Depth 10
```

## RabbitMQ

RabbitMQ Management UI:

```text
http://127.0.0.1:15672
```

Стандартные учетные данные образа:

```text
guest / guest
```

Очередь событий:

```text
opcua.parameter.events
```

Exchange:

```text
opcua.events
```

Routing key:

```text
opcua.parameter.events
```

В payload события важны поля:

- `parameter_code` - код параметра;
- `node_id` - исходная OPC UA нода;
- `value_raw` - исходное значение;
- `value_normalized` - нормализованное значение;
- `value_type` - тип после нормализации;
- `source_timestamp` - время значения на OPC UA сервере;
- `ingested_at` - время обработки клиентом;
- `acquisition_mode` - `subscription` или `polling`;
- `sequence_id` - порядковый номер события внутри процесса клиента.

## Частые проблемы

### API клиента недоступен на 127.0.0.1:8080

Проверьте, что в compose есть:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

И что для клиента не включен:

```yaml
network_mode: host
```

### В логах BadTooManySessions

OPC UA сервер достиг лимита сессий. Закройте UAExpert, старые контейнеры клиента и другие OPC UA клиенты.

Проверка TCP-сессий на Windows:

```powershell
Get-NetTCPConnection -RemoteAddress 192.168.0.120 -RemotePort 4840
```

Состояние `TimeWait` после закрытия клиента нормально и обычно проходит само.

### В логах BadNodeIdUnknown

Нода удалена или изменилась на OPC UA сервере. Нужно убрать ее из `nodes` или обновить `node_id`.

### Пароль в YAML стал числом

Если пароль состоит только из цифр, обязательно заключайте его в кавычки:

```yaml
password: "12345678"
```

Иначе YAML может прочитать его как число, а конфиг ожидает строку.

### RabbitMQ выключен, но Redis копит события

Это штатный сценарий деградации:

1. клиент продолжает читать OPC UA;
2. публикация в RabbitMQ падает;
3. события временно складываются в Redis;
4. после восстановления RabbitMQ buffer worker доотправляет события.

Проверка буфера:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/buffer/stats" -Headers $headers | ConvertTo-Json -Depth 10
```

### Docker не может скачать python:3.12-slim

Ошибка вида `TLS handshake timeout` обычно связана с сетью/Docker Hub, а не с кодом клиента.

Можно повторить:

```powershell
docker pull python:3.12-slim
docker compose -f .\docker-compose.server.yml up --build -d
```

### В браузере не открывается opc.tcp://...

Это нормально. `opc.tcp` не является HTTP-протоколом.

Проверяйте доступ так:

```powershell
Test-NetConnection 192.168.0.120 -Port 4840
```

## Минимальный smoke test после запуска

```powershell
$headers = @{ Authorization = "Bearer secret-token" }

Invoke-RestMethod -Uri "http://127.0.0.1:8080/health" -Headers $headers
Invoke-RestMethod -Uri "http://127.0.0.1:8080/ready" -Headers $headers | ConvertTo-Json -Depth 10
Invoke-RestMethod -Uri "http://127.0.0.1:8080/connections" -Headers $headers | ConvertTo-Json -Depth 10
Invoke-RestMethod -Uri "http://127.0.0.1:8080/subscriptions" -Headers $headers | ConvertTo-Json -Depth 10
Invoke-RestMethod -Uri "http://127.0.0.1:8080/buffer/stats" -Headers $headers | ConvertTo-Json -Depth 10
```

Ожидаемо:

- `/health` возвращает `status: ok`;
- `/ready` возвращает `ready: true`;
- `/connections` показывает `connected: true`;
- `/subscriptions` показывает активные ноды;
- `/buffer/stats` не растет при доступном RabbitMQ.
