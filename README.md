# AviaBot

Voice-бот для TeamSpeak 3, который реализует дистанционное радиообщение для симулятора **IL-2 Sturmovik: Great Battles**.

Используется на проекте **Frontline** — [https://il2-fl.ru/](https://il2-fl.ru/)

Бот получает позиции игроков из RabbitMQ (Commander-IL2), определяет, кто находится в зоне досягаемости радиостанции, и шепчет (whisper) смикшированный голосовой поток только тем, кто должен слышать.

---

## Особенности

- **Дистанционная связь** — голос передаётся только в пределах заданного радиуса (`max_distance`) и только внутри своей коалиции.
- **Разделение лобби и игры** — игроки в лобби слышат только лобби, игроки в бою слышат только тех, кто в радиусе. Спектаторы обрабатываются как лобби.
- **Коалиции** — поддержка коалиций Allies (`101`) и Axis (`201`).
- **Радиоэффекты** — фильтрация голоса (полоса 300–3400 Гц), компрессия, soft-clip, шум радиостанции и squelch-хвост.
- **Sidetone** — говорящий игрок слышит короткий щелчок тангенты и squelch-хвост, но не слышит свой собственный голос в миксе.
- **Производительность** — аудиомикшер работает в выделенном потоке с приоритетом реального времени (Windows), обрабатывает 20 мс тики Opus.

---

## Архитектура

```
┌─────────────────┐     RabbitMQ      ┌──────────────┐
│ Commander-IL2   │ ─────────────────>│  AviaBot     │
│ (PlayerSession) │  avia_bot_queue   │  (Rust)      │
└─────────────────┘                   └──────┬───────┘
                                             │
                        ┌────────────────────┼────────────────────┐
                        │                    │                    │
                   ┌────▼────┐         ┌─────▼─────┐        ┌────▼────┐
                   │ TS3     │<────────│  Mixer    │<───────│ TS3     │
                   │ Server  │ whisper │  (thread) │  opus  │ Client  │
                   └─────────┘         └───────────┘        └─────────┘
```

### Потоки

- **Async main** — подключение к TS3 и RabbitMQ, получение событий.
- **Event task** — читает `StreamItem::Audio` (входящие whisper/голос) и отправляет в микшер.
- **Routing task** — каждую секунду строит снимок позиций и разрешает `UID → client_id` через TS3.
- **Mixer thread** — 20 мс тики: декодирование Opus → AGC → микширование → эффекты → кодирование Opus → whisper получателям.

---

## Требования

- Windows (для `thread_priority`)
- [Rust](https://rustup.rs/) (stable toolchain)
- TeamSpeak 3 сервер
- [RabbitMQ](https://www.rabbitmq.com/) (для приёма событий от Commander-IL2)

---

## Используемые библиотеки

| Библиотека | Назначение |
|------------|------------|
| [tsclientlib](https://github.com/ReSpeak/tsclientlib) | Клиент TeamSpeak 3 (подключение, whisper, аудиопотоки) |
| [tsproto-packets](https://github.com/ReSpeak/tsclientlib) | Низкоуровневая работа с TS3 пакетами (команды, audio data) |
| [opus](https://crates.io/crates/opus) | Кодек Opus: декодирование/кодирование голосовых пакетов |
| [tokio](https://crates.io/crates/tokio) | Асинхронный рантайм (подключение к TS3, RabbitMQ, таймеры) |
| [futures](https://crates.io/crates/futures) | Асинхронные примитивы (StreamExt и т.д.) |
| [lapin](https://crates.io/crates/lapin) | Клиент RabbitMQ (приём событий от Commander-IL2) |
| [crossbeam](https://crates.io/crates/crossbeam) | Lock-free каналы между async-потоком TS3 и audio-микшером |
| [tracing](https://crates.io/crates/tracing) + [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | Структурированное логирование с уровнями (info, debug, trace) |
| [serde](https://crates.io/crates/serde) + [serde_json](https://crates.io/crates/serde_json) + [toml](https://crates.io/crates/toml) | Сериализация/десериализация JSON (RabbitMQ) и TOML (конфиг) |
| [anyhow](https://crates.io/crates/anyhow) | Удобная обработка ошибок (`Result`, `Context`) |
| [thread-priority](https://crates.io/crates/thread-priority) | Повышение приоритета аудио-микшера до real-time (Windows) |
| [rustc-hash](https://crates.io/crates/rustc-hash) | Быстрые `FxHashMap` для кэшей позиций и клиентов |

---

## Сборка

```bash
cargo build --release
```

Исполняемый файл появится в `target/release/aviabot.exe`.

---

## Конфигурация

Создай файл `config.toml` рядом с `.exe`. **Не коммить его в репозиторий** — там пароли и приватные ключи.

```toml
[ts3]
address = "127.0.0.1:9987"
name = "Dispatcher"
channel = "Test"
channel_password = "password"
# server_password = ""
identity_key = "your-ts3-identity-key-here"

[rabbitmq]
enabled = true
hostname = "localhost"
port = 5672
username = "guest"
password = "guest"
queue = "avia_bot_queue"

[relay]
max_distance = 42000.0      # метры
coalition_check = true      # true = только своя коалиция
radio_effects_enabled = true

[audio]
output_gain = 5.0
synthetic_speakers = 0      # синтетические спикеры для нагрузочного теста
```

### Описание секций

| Секция | Поле | Описание |
|--------|------|----------|
| `ts3` | `address` | Адрес TS3 сервера |
| `ts3` | `name` | Ник бота на сервере |
| `ts3` | `channel` | Канал, в который заходит бот |
| `ts3` | `identity_key` | TS3 Identity (base64) |
| `rabbitmq` | `queue` | Очередь RabbitMQ от Commander-IL2 |
| `relay` | `max_distance` | Максимальная дистанция радиосвязи (метры) |
| `relay` | `coalition_check` | Фильтровать по коалиции |
| `relay` | `radio_effects_enabled` | Включить радиоэффекты |
| `audio` | `output_gain` | Усиление выходного сигнала |

---

## Запуск

```bash
aviabot.exe
```

Бот подключится к TS3, начнёт слушать RabbitMQ и будет шептать голос тем, кто должен слышать.

---

## Формат сообщений RabbitMQ

Бот ожидает сообщения в очереди RabbitMQ в формате JSON. Каждое сообщение описывает событие игрока.

### Пример сообщения

```json
{
  "event": "position",
  "id": 123,
  "gamerName": "PilotName",
  "country": 101,
  "teamSpeakId": "uid base64",
  "x": 15234.5,
  "y": 120.0,
  "z": 8765.2,
  "type": "aircraft",
  "name": "Bf-109 G-14"
}
```

### Поля

| Поле | Тип | Описание |
|------|-----|----------|
| `event` | `string` | Тип события: `join`, `spawn`, `position`, `despawn`, `detach`, `leave`, `clear` |
| `id` | `integer` | ID игрока в игре |
| `gamerName` | `string` | Имя игрока |
| `country` | `integer` | Коалиция: `101` (Allies) или `201` (Axis) |
| `teamSpeakId` | `string`\|null | TS3 UID игрока. Если `null` — игрок игнорируется ботом |
| `x`, `y`, `z` | `number`\|null | Координаты в мире (метры) |
| `type` | `string`\|null | Тип объекта: `aircraft`, `Spectator` и др. |
| `name` | `string`\|null | Имя объекта (например, название самолёта) |

### События

| Событие | Действие бота |
|---------|---------------|
| `join` | Игрок добавляется в лобби своей коалиции |
| `spawn` | Игрок перемещается из лобби в активную зону (катка) |
| `position` | Обновляются координаты активного игрока |
| `despawn` | Игрок возвращается в лобби |
| `detach` | Аналогично `despawn` |
| `leave` | Игрок удаляется из всех списков |
| `clear` | Очищаются все словари (конец миссии) |

---

## Принцип работы voice routing

1. **join** — игрок попадает в лобби своей коалиции.
2. **spawn** — игрок переходит из лобби в активную зону (катка).
3. **position** — обновляются координаты активного игрока.
4. **despawn / detach** — игрок возвращается в лобби.
5. **leave** — игрок удаляется из всех списков.

### Кто кого слышит

| Спикер | Слушатели |
|--------|-----------|
| Лобби / Спектатор | Все в лобби той же коалиции (broadcast) |
| Активный (в бою) | Активные игроки той же коалиции в радиусе `max_distance` |

Активные спикеры не слышат общий микс — вместо этого они получают только короткий sidetone (щелчок тангенты + squelch-хвост).

---

## Лицензия

MIT
