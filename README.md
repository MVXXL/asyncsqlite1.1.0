# AsyncSQLite

## Введение

AsyncSQLite — это современная асинхронная библиотека для работы с базами данных SQLite в Python. Она сочетает в себе мощь модулей asyncio и sqlite3, обеспечивая эффективное выполнение операций с базой данных в асинхронных приложениях. Библиотека идеально подходит для проектов, требующих неблокирующего ввода-вывода, таких как веб-приложения или высоконагруженные сервисы.

## Установка

Установить AsyncSQLite можно с помощью setup.py или склонировав репозиторий и выполнив установку вручную:

    python setup.py install

(Примечание: замените на актуальный способ установки, если библиотека доступна через PyPI или другой источник.)

## Особенности

- Асинхронный API: Все операции с базой данных полностью асинхронны, что позволяет интегрировать их в цикл событий asyncio.
- Пул соединений: Динамическое управление пулом соединений с автоматическим масштабированием и тайм-аутами.
- Обработка ошибок: Встроенные механизмы повторных попыток для операций, устойчивость к временным сбоям SQLite.
- Кэширование запросов: LRU-кэш для ускорения повторяющихся запросов с автоматическим сбросом при изменении данных.
- Потоковая выборка: Поддержка генераторов для обработки больших наборов данных порциями.
- Групповые операции: Эффективная массовая вставка данных с помощью bulk_insert.
- Транзакции: Атомарное выполнение набора запросов с откатом при ошибках.
- Работа с BLOB: Удобные методы для вставки и извлечения бинарных данных.
- Метрики: Статистика производительности (количество запросов, время выполнения, использование кэша).
- Управление таблицами: Создание, удаление и проверка таблиц с поддержкой индексов.

## Использование

### 1. Импортируйте и инициализируйте класс AsyncSQLite:
```py
    import asyncsqlite

    async def main():
        async with asyncsqlite.AsyncSQLite("my_database.db") as db:
            # Выполняйте операции с базой данных здесь
            pass

    if __name__ == "__main__":
        asyncio.run(main())
```

### 2. Выполняйте запросы:
- await db.execute(query, params=None): Выполняет запрос без возврата результата (например, для INSERT, UPDATE).
- await db.fetchall(query, params=None): Извлекает все строки в виде списка словарей.
- await db.fetchone(query, params=None): Возвращает первую строку в виде словаря.
- await db.fetch_stream(query, params=None, batch_size=100): Возвращает генератор для потоковой обработки данных порциями.
- await db.query_json(query, params=None): Возвращает результат запроса в формате JSON.

### 3. Вставляйте данные:
- await db.insert(table, data, replace=False): Вставляет одну строку, с опцией замены записи при конфликте.
- await db.bulk_insert(table, data): Вставляет множество строк за один запрос.

### 4. Обновляйте и удаляйте данные:
- await db.update(table, data, where, where_params): Обновляет строки с заданным условием.
- await db.delete(table, where, params): Удаляет строки по условию.
- await db.insert_blob(table, column, blob, where, params): Обновляет BLOB-данные в указанной колонке.

### 5. Извлекайте BLOB-данные:
- await db.fetch_blob(query, params=None): Извлекает бинарные данные из результата запроса.

### 6. Управление транзакциями:
- await db.transaction(queries): Выполняет набор запросов в одной транзакции с автоматическим откатом при ошибке.

### 7. Управление таблицами:
- await db.table_exists(table): Проверяет существование таблицы.
- await db.create_table(table, columns, indexes=None): Создает таблицу с указанными столбцами и индексами.
- await db.drop_table(table): Удаляет таблицу.

### 8. Метрики производительности:
- db.metrics(): Возвращает словарь с текущей статистикой работы базы данных.

## Конфигурация (опционально)

- max_workers (int, по умолчанию 10): Максимальное количество потоков в пуле ThreadPoolExecutor.
- initial_pool_size (int, по умолчанию 5): Начальное количество соединений в пуле.
- max_pool_size (int, по умолчанию 20): Максимальное количество соединений в пуле.
- cache_size (int, по умолчанию 1000): Максимальный размер кэша запросов (количество записей).
- connection_timeout (float, по умолчанию 5.0): Тайм-аут ожидания свободного соединения в секундах.
- pragma_settings (dict, опционально): Настройки PRAGMA для SQLite (по умолчанию: journal_mode=WAL, cache_size=-20000, synchronous=NORMAL).

## Пример полного использования
```py
    import asyncsqlite

    async def main():
        async with asyncsqlite.AsyncSQLite("example.db") as db:
            # Создание таблицы
            await db.create_table("users", {
                "id": "INTEGER PRIMARY KEY",
                "name": "TEXT",
                "data": "BLOB",
                "age": "INTEGER"
            }, indexes=["name"])

            # Вставка данных
            await db.insert("users", {"name": "Alice", "data": b"binary", "age": 25})

            # Пакетная вставка
            await db.bulk_insert("users", [
                {"name": "Bob", "data": b"bob_data", "age": 30},
                {"name": "Charlie", "data": b"charlie_data", "age": 35}
            ])

            # Обновление
            await db.update("users", {"age": 26}, "name = ?", ("Alice",))

            # Потоковая выборка
            async for batch in db.fetch_stream("SELECT * FROM users", batch_size=2):
                print(batch)

            # Получение JSON
            json_data = await db.query_json("SELECT name, age FROM users")
            print(json_data)

            # Работа с BLOB
            await db.insert_blob("users", "data", b"new_blob", "name = ?", ("Bob",))
            blob = await db.fetch_blob("SELECT data FROM users WHERE name = ?", ("Bob",))
            print(blob)

            # Метрики
            print(db.metrics())

    if __name__ == "__main__":
        asyncio.run(main())
```
## Дополнительные примечания

- Подробные примеры и описание методов доступны в документации в исходном коде (в конце файла).
- Использование контекстного менеджера (async with) гарантирует корректное закрытие соединений.
- Кэширование запросов активно по умолчанию и автоматически сбрасывается при изменении данных.
- Для отключения кэша установите cache_size=0 при инициализации.

## Вклад

Мы рады вашему участию в развитии AsyncSQLite! Создавайте pull-запросы или сообщайте об ошибках в репозитории проекта.
