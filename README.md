# Events Worker — RabbitMQ + Kafka → PostgreSQL

Worker assíncrono (Python 3.11+) que consome mensagens do RabbitMQ (`users.creation`)
e do Kafka (`api.messages`), processa-as através de um caso de uso único e
registra o resultado (sucesso/falha) na tabela `processed_events` do PostgreSQL.

## 1. Plano de Implementação

### Fase 0 — Fundamentos
1. Definir o contrato de dados (`ProcessedEvent`) e os enums de domínio
   (`EventSource`, `EventStatus`) — sem dependências externas.
2. Definir as **portas** (`EventRepositoryPort`, `MessageConsumerPort`) como
   `ABC`s. Todo o resto do sistema depende dessas abstrações, nunca de
   implementações concretas.

### Fase 1 — Infraestrutura transversal
3. `settings.py`: configuração via `pydantic-settings`, uma classe por
   domínio de config (Kafka/RabbitMQ/Postgres) agregadas em `Settings`.
4. `logging_config.py`: `structlog` configurado para emitir **JSON em
   stdout**, com timestamp ISO-8601 UTC, nível de log e suporte a
   `correlation_id` via `contextvars` (cada mensagem consumida gera um novo
   id, propagado automaticamente para todos os logs daquele processamento).

### Fase 2 — Persistência
5. Model SQLAlchemy 2.0 (`ProcessedEventModel`) mapeando 1:1 para a tabela
   `processed_events` (UUID, JSONB, índices em `source`/`status`).
6. `session.py`: engine assíncrona (`asyncpg`) + `async_sessionmaker`.
7. Migration Alembic criando a tabela (incluindo a extensão `pgcrypto` para
   `gen_random_uuid()` como default no servidor).
8. `PostgresEventRepository` implementa `EventRepositoryPort`, mapeando a
   entidade de domínio para o model ORM.

### Fase 3 — Aplicação (caso de uso)
9. `ProcessMessageUseCase`: recebe `EventRepositoryPort` via construtor
   (injeção de dependência). Aplica regra de negócio (`_apply_business_rules`,
   ponto de extensão), e **sempre** persiste um `ProcessedEvent`
   (`PROCESSED` ou `FAILED`), garantindo auditabilidade total — nenhuma
   mensagem é processada "silenciosamente".

### Fase 4 — Adaptadores de mensageria
10. `RabbitMQUsersCreationConsumer` (aio-pika): `connect_robust` (reconexão
    automática), QoS/prefetch configurável, ack manual via
    `message.process()` — falha de parsing não trava a fila.
11. `KafkaApiMessagesConsumer` (aiokafka): `enable_auto_commit=False` e
    commit manual **após** a persistência do resultado no Postgres,
    garantindo semântica *at-least-once* real (e não apenas "at most once
    do ponto de vista do Kafka, mas perdido no banco").

### Fase 5 — Orquestração
12. `main.py` (composition root): instancia engine → repository → use case
    → consumers, e roda ambos com `asyncio.gather`/`asyncio.wait` com
    `FIRST_COMPLETED`, tratando `SIGINT`/`SIGTERM` para shutdown gracioso
    (fecha conexões AMQP/Kafka e faz `engine.dispose()`).

### Fase 6 — Qualidade
13. Teste unitário do caso de uso usando um repositório fake em memória —
    demonstra que a arquitetura permite testar 100% da lógica de negócio
    sem subir Postgres, RabbitMQ ou Kafka.

## 2. Estrutura de Diretórios

```
worker_project/
├── alembic/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── 202607250001_create_processed_events_table.py
├── alembic.ini
├── src/
│   ├── domain/                        # Camada de domínio (sem dependências externas)
│   │   ├── entities.py                # ProcessedEvent, EventSource, EventStatus
│   │   └── ports.py                   # EventRepositoryPort, MessageConsumerPort (interfaces)
│   ├── application/
│   │   └── use_cases/
│   │       └── process_message_usecase.py
│   ├── infrastructure/
│   │   ├── config/
│   │   │   ├── settings.py            # pydantic-settings
│   │   │   └── logging_config.py      # structlog JSON
│   │   ├── database/
│   │   │   ├── models.py              # SQLAlchemy 2.0 ORM
│   │   │   ├── session.py             # engine/session factory async
│   │   │   └── repositories/
│   │   │       └── event_repository.py
│   │   └── messaging/
│   │       ├── rabbitmq_consumer.py   # aio-pika
│   │       └── kafka_consumer.py      # aiokafka
│   └── main.py                        # composition root + orquestração
├── tests/
│   └── test_process_message_usecase.py
├── .env.example
├── Dockerfile
├── requirements.txt
└── README.md
```

**Regra de dependência (Clean/Hexagonal):**
`infrastructure` → `application` → `domain`. O `domain` nunca importa nada
de `infrastructure`. Os consumidores (`infrastructure/messaging`) e o
repositório (`infrastructure/database`) só conhecem `ProcessMessageUseCase`
e as entidades de domínio — nunca um ao outro.

## 3. Como rodar

```bash
cp .env.example .env
pip install -r requirements.txt

# Aplicar migrations
alembic upgrade head

# Rodar o worker
python -m src.main
```

## 4. Como testar

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

## 5. Pontos de atenção para produção

- **Dead-letter**: hoje, payload malformado é apenas logado e descartado
  (`ack`/`commit` sem reprocessar). Em produção, redirecionar para uma
  DLX (RabbitMQ) ou tópico `.dlq` (Kafka).
- **Retries com backoff**: falhas transitórias de banco atualmente
  propagam a exceção para fora do `_on_message`/`_handle_message`; um
  middleware de retry (ex.: `tenacity`) pode ser adicionado na camada de
  infraestrutura sem tocar no domínio.
- **Idempotência**: se a mesma mensagem puder ser entregue mais de uma
  vez (garantia at-least-once), considere uma chave de deduplicação
  (ex.: `message_id` do broker) antes de inserir em `processed_events`.
- **Migrations em CI/CD**: `alembic upgrade head` deve rodar como um passo
  de deploy separado do worker, nunca no `main.py`.
