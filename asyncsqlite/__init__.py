import sqlite3
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import List, Tuple, Optional, Dict, Any, AsyncGenerator, Union, Callable
from collections import OrderedDict
import json
import time
from functools import wraps, lru_cache
import threading
from contextlib import asynccontextmanager
import pickle

__version__ = '3.0.1'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class AsyncSQLiteError(Exception):
    def __init__(self, message: str, error_code: Optional[int] = None) -> None:
        self.message = message
        self.error_code = error_code
        super().__init__(f"{message} (Code: {error_code})" if error_code else message)

class ConnectionPoolError(AsyncSQLiteError):
    def __init__(self, message: str, error_code: Optional[int] = None, pool_size: Optional[int] = None) -> None:
        super().__init__(message, error_code)
        self.pool_size = pool_size

def retry_on_failure(max_attempts: int = 3, delay: float = 0.1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return await func(*args, **kwargs)
                except (sqlite3.OperationalError, ConnectionPoolError) as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise AsyncSQLiteError(f"Failed after {max_attempts} attempts: {e}", 1003)
                    await asyncio.sleep(delay * (2 ** attempts))
                    logger.warning(f"Retry {attempts}/{max_attempts} for {func.__name__}: {e}")
            return None
        return wrapper
    return decorator

class AsyncSQLite:
    def __init__(
        self,
        db_path: str,
        max_workers: int = 10,
        initial_pool_size: int = 5,
        max_pool_size: int = 20,
        cache_size: int = 1000,
        connection_timeout: float = 5.0,
        pragma_settings: Optional[Dict[str, Any]] = None
    ) -> None:
        self.db_path = db_path
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="AsyncSQLite")
        self._pool = asyncio.Queue(maxsize=max_pool_size)
        self._initial_pool_size = min(initial_pool_size, max_pool_size)
        self._max_pool_size = max_pool_size
        self._cache = OrderedDict()
        self._cache_size = cache_size
        self._timeout = connection_timeout
        self._metrics = {"queries": 0, "query_time": 0.0, "cache_hits": 0, "pool_size": 0}
        self._lock = asyncio.Lock()
        self._closed = False
        self._pragma = pragma_settings or {
            "journal_mode": "WAL",
            "cache_size": -20000,
            "synchronous": "NORMAL"
        }
        self._local = threading.local()
        self._init_task = asyncio.create_task(self._initialize())

    async def _initialize(self) -> None:
        await asyncio.gather(*[self._create_connection() for _ in range(self._initial_pool_size)])
        await self._apply_pragma()
        logger.info(f"Database initialized with {self._pool.qsize()} connections")

    @lru_cache(maxsize=128)
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None
        )
        conn.row_factory = sqlite3.Row
        conn.enable_load_extension(True)
        return conn

    async def _create_connection(self) -> None:
        async with self._lock:
            if self._pool.full() or self._closed:
                return
            conn = await asyncio.get_event_loop().run_in_executor(self._executor, self._connect)
            await self._pool.put(conn)
            self._metrics["pool_size"] = self._pool.qsize()

    async def _apply_pragma(self) -> None:
        async with self._connection() as conn:
            for key, value in self._pragma.items():
                await self._execute_raw(conn, f"PRAGMA {key} = {value}")

    @asynccontextmanager
    async def _connection(self) -> AsyncGenerator[sqlite3.Connection, None]:
        conn = await self._get_connection()
        try:
            yield conn
        finally:
            await self._release_connection(conn)

    async def _get_connection(self) -> sqlite3.Connection:
        if self._closed:
            raise AsyncSQLiteError("Database is closed", 1004)
        
        if self._pool.qsize() < self._initial_pool_size and not self._pool.full():
            await self._create_connection()

        try:
            return await asyncio.wait_for(self._pool.get(), timeout=self._timeout)
        except asyncio.TimeoutError:
            raise ConnectionPoolError(
                "No available connections",
                1001,
                self._pool.qsize()
            )

    async def _release_connection(self, conn: sqlite3.Connection) -> None:
        if not self._closed:
            await self._pool.put(conn)
            self._metrics["pool_size"] = self._pool.qsize()

    async def _execute_raw(self, conn: sqlite3.Connection, query: str, params: Tuple[Any, ...] = ()) -> Any:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            lambda: conn.execute(query, params)
        )

    def _cache_key(self, query: str, params: Optional[Tuple[Any, ...]]) -> Tuple[Any, ...]:
        return (query, params or ())

    async def _cached_execute(
        self, query: str, params: Optional[Tuple[Any, ...]] = None, fetch_one: bool = False
    ) -> Any:
        key = self._cache_key(query, params)
        if key in self._cache:
            self._metrics["cache_hits"] += 1
            return pickle.loads(self._cache[key])

        start = time.time()
        async with self._connection() as conn:
            cursor = await self._execute_raw(conn, query, params or ())
            result = cursor.fetchone() if fetch_one else cursor.fetchall()
            result = self._to_dict(result)
            if len(self._cache) >= self._cache_size:
                self._cache.popitem(last=False)
            self._cache[key] = pickle.dumps(result)
            self._metrics["queries"] += 1
            self._metrics["query_time"] += time.time() - start
            return result

    def _to_dict(self, rows: Any) -> Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]:
        if not rows:
            return None
        if isinstance(rows, list):
            return [dict(row) for row in rows]
        return dict(rows)

    def _clear_cache(self) -> None:
        self._cache.clear()

    @retry_on_failure(max_attempts=3)
    async def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        async with self._connection() as conn:
            await self._execute_raw(conn, query, params or ())
            conn.commit()
            self._clear_cache()

    async def insert(self, table: str, data: Dict[str, Any], replace: bool = False) -> int:
        if not await self.table_exists(table):
            await self.create_table(table, {k: self._guess_type(v) for k, v in data.items()})
        
        columns = list(data.keys())
        placeholders = ",".join("?" * len(columns))
        query = f"INSERT OR {'REPLACE' if replace else 'IGNORE'} INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        async with self._connection() as conn:
            cursor = await self._execute_raw(conn, query, tuple(data.values()))
            conn.commit()
            self._clear_cache()
            return cursor.lastrowid

    async def update(self, table: str, data: Dict[str, Any], where: str, where_params: Tuple[Any, ...]) -> int:
        set_clause = ",".join(f"{k}=?" for k in data.keys())
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        async with self._connection() as conn:
            cursor = await self._execute_raw(conn, query, tuple(data.values()) + where_params)
            conn.commit()
            self._clear_cache()
            return cursor.rowcount

    async def delete(self, table: str, where: str, params: Tuple[Any, ...]) -> int:
        query = f"DELETE FROM {table} WHERE {where}"
        async with self._connection() as conn:
            cursor = await self._execute_raw(conn, query, params)
            conn.commit()
            self._clear_cache()
            return cursor.rowcount

    async def fetchall(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
        result = await self._cached_execute(query, params)
        return result or []

    async def fetchone(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[Dict[str, Any]]:
        return await self._cached_execute(query, params, fetch_one=True)

    async def fetch_stream(self, query: str, params: Optional[Tuple[Any, ...]] = None, batch_size: int = 100) -> AsyncGenerator[List[Dict[str, Any]], None]:
        async with self._connection() as conn:
            cursor = await self._execute_raw(conn, query, params or ())
            while True:
                rows = await asyncio.get_event_loop().run_in_executor(self._executor, cursor.fetchmany, batch_size)
                if not rows:
                    break
                yield [dict(row) for row in rows]

    async def bulk_insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        if not data:
            return
        columns = list(data[0].keys())
        placeholders = ",".join("?" * len(columns))
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        values = [tuple(d[k] for k in columns) for d in data]
        async with self._connection() as conn:
            await asyncio.get_event_loop().run_in_executor(self._executor, conn.executemany, query, values)
            conn.commit()
            self._clear_cache()

    async def transaction(self, queries: List[Tuple[str, Optional[Tuple[Any, ...]]]]) -> None:
        async with self._connection() as conn:
            try:
                for query, params in queries:
                    await self._execute_raw(conn, query, params or ())
                conn.commit()
                self._clear_cache()
            except Exception as e:
                conn.rollback()
                raise AsyncSQLiteError(f"Transaction failed: {e}", 1002)

    async def table_exists(self, table: str) -> bool:
        query = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
        return bool(await self.fetchone(query, (table,)))

    async def create_table(self, table: str, columns: Dict[str, str], indexes: Optional[List[str]] = None) -> None:
        cols = ",".join(f"{k} {v}" for k, v in columns.items())
        query = f"CREATE TABLE IF NOT EXISTS {table} ({cols})"
        await self.execute(query)
        if indexes:
            for idx in indexes:
                await self.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{idx} ON {table} ({idx})")

    def _guess_type(self, value: Any) -> str:
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, bytes):
            return "BLOB"
        return "TEXT"

    async def drop_table(self, table: str) -> None:
        await self.execute(f"DROP TABLE IF EXISTS {table}")

    async def query_json(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> str:
        result = await self.fetchall(query, params)
        return json.dumps(result)

    async def insert_blob(self, table: str, column: str, blob: bytes, where: str, params: Tuple[Any, ...]) -> None:
        await self.update(table, {column: blob}, where, params)

    async def fetch_blob(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[bytes]:
        result = await self.fetchone(query, params)
        return result[next(iter(result))] if result else None

    def metrics(self) -> Dict[str, Union[int, float]]:
        return self._metrics.copy()

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            await self._init_task
            while not self._pool.empty():
                conn = await self._pool.get()
                await asyncio.get_event_loop().run_in_executor(self._executor, conn.close)
            self._executor.shutdown(wait=True)
            logger.info("Database closed")

    async def __aenter__(self) -> "AsyncSQLite":
        await self._init_task
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

async def main():
    db = AsyncSQLite("example.db", max_workers=4, initial_pool_size=2, max_pool_size=5)
    
    try:
        if await db.table_exists("users"):
            await db.drop_table("users")
            print("Существующая таблица users удалена")

        await db.create_table(
            "users",
            {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "data": "BLOB", "age": "INTEGER"},
            indexes=["name"]
        )
        print("Таблица users создана")

        last_id = await db.insert("users", {"id": 1, "name": "Alice", "data": b"binary_data", "age": 25})
        print(f"Вставлена запись с ID: {last_id}")

        last_id = await db.insert("users", {"id": 1, "name": "AliceUpdated", "data": b"new_data", "age": 26}, replace=True)
        print(f"Запись с ID 1 обновлена через replace: {last_id}")

        bulk_data = [
            {"id": 2, "name": "Bob", "data": b"bob_data", "age": 30},
            {"id": 3, "name": "Charlie", "data": b"charlie_data", "age": 35}
        ]
        await db.bulk_insert("users", bulk_data)
        print("Выполнена пакетная вставка 2 записей")

        rows_updated = await db.update("users", {"name": "Bobby", "age": 31}, "id = ?", (2,))
        print(f"Обновлено записей: {rows_updated}")

        transaction_queries = [
            ("INSERT INTO users (id, name, data, age) VALUES (?, ?, ?, ?)", (4, "David", b"david_data", 40)),
            ("UPDATE users SET age = ? WHERE id = ?", (32, 2))
        ]
        await db.transaction(transaction_queries)
        print("Транзакция выполнена")

        all_users = await db.fetchall("SELECT * FROM users ORDER BY id")
        print("Все пользователи:", all_users)

        single_user = await db.fetchone("SELECT * FROM users WHERE id = ?", (1,))
        print("Один пользователь (id=1):", single_user)

        print("Потоковая выборка (batch_size=2):")
        async for batch in db.fetch_stream("SELECT * FROM users ORDER BY id", batch_size=2):
            print("Порция:", batch)

        json_data = await db.query_json("SELECT id, name, age FROM users WHERE age > ?", (30,))
        print("Данные в JSON:", json_data)

        await db.insert_blob("users", "data", b"updated_blob", "id = ?", (3,))
        blob = await db.fetch_blob("SELECT data FROM users WHERE id = ?", (3,))
        print("Получен BLOB для id=3:", blob)

        rows_deleted = await db.delete("users", "id = ?", (4,))
        print(f"Удалено записей: {rows_deleted}")

        metrics = db.metrics()
        print("Метрики работы:", metrics)

        try:
            await db.fetchall("SELECT * FROM nonexistent_table")
        except AsyncSQLiteError as e:
            print(f"Ожидаемая ошибка при выборке из несуществующей таблицы: {e}")
        except sqlite3.OperationalError as e:
            print(f"Ожидаемая ошибка при выборке из несуществующей таблицы: {e}")

    except AsyncSQLiteError as e:
        print(f"Произошла ошибка: {e}")
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")
    finally:
        await db.close()
        print("База данных закрыта")

if __name__ == "__main__":
    asyncio.run(main())

"""
Как пользоваться библиотекой AsyncSQLite (версия 3.0.1) от А до Я:

1. Импорт:
    Чтобы начать использовать библиотеку, импортируйте класс AsyncSQLite:
    from asyncsqlite import AsyncSQLite

2. Создание объекта базы данных:
    Создайте объект базы данных, указав путь к файлу SQLite. Можно настроить параметры пула соединений и кэша:
    db = AsyncSQLite("my_database.db", max_workers=4, initial_pool_size=2, max_pool_size=5, cache_size=1000)

3. Работа с асинхронным контекстом:
    Используйте асинхронный контекстный менеджер для автоматического управления соединениями:
    async with AsyncSQLite("my_database.db") as db:
        # Здесь выполняйте операции с базой данных

4. Создание таблицы:
    Создайте таблицу с помощью метода create_table, указав имя таблицы, столбцы и опциональные индексы:
    await db.create_table("users", {
        "id": "INTEGER PRIMARY KEY",
        "name": "TEXT NOT NULL",
        "data": "BLOB",
        "age": "INTEGER"
    }, indexes=["name"])

5. Вставка данных:
    Вставляйте данные методом insert. Поддерживается опция замены существующей записи:
    await db.insert("users", {"name": "Alice", "data": b"binary_data", "age": 25})
    await db.insert("users", {"name": "Alice", "data": b"new_data", "age": 26}, replace=True)

6. Пакетная вставка:
    Для массовой вставки используйте bulk_insert:
    await db.bulk_insert("users", [
        {"name": "Bob", "data": b"bob_data", "age": 30},
        {"name": "Charlie", "data": b"charlie_data", "age": 35}
    ])

7. Обновление данных:
    Обновляйте записи с помощью метода update, указав условие:
    await db.update("users", {"name": "Bobby", "age": 31}, "id = ?", (2,))

8. Удаление данных:
    Удаляйте записи методом delete:
    await db.delete("users", "id = ?", (1,))

9. Получение всех данных:
    Используйте fetchall для выборки всех строк:
    users = await db.fetchall("SELECT * FROM users")
    print(users)

10. Получение одной строки:
    Используйте fetchone для выборки одной записи:
    user = await db.fetchone("SELECT * FROM users WHERE id = ?", (1,))
    print(user)

11. Потоковая выборка:
    Для обработки больших наборов данных используйте fetch_stream с указанием размера порции:
    async for batch in db.fetch_stream("SELECT * FROM users", batch_size=100):
        print(batch)

12. Транзакции:
    Выполняйте группу запросов в транзакции с помощью transaction:
    queries = [
        ("INSERT INTO users (name, age) VALUES (?, ?)", ("David", 40)),
        ("UPDATE users SET age = ? WHERE name = ?", (41, "David"))
    ]
    await db.transaction(queries)

13. Работа с JSON:
    Получайте результаты в формате JSON с помощью query_json:
    json_data = await db.query_json("SELECT * FROM users WHERE age > ?", (30,))
    print(json_data)

14. Работа с BLOB:
    Вставляйте и извлекайте BLOB-данные:
    await db.insert_blob("users", "data", b"updated_blob", "id = ?", (1,))
    blob = await db.fetch_blob("SELECT data FROM users WHERE id = ?", (1,))
    print(blob)

15. Проверка существования таблицы:
    Проверяйте наличие таблицы с помощью table_exists:
    exists = await db.table_exists("users")
    print(exists)

16. Удаление таблицы:
    Удаляйте таблицу методом drop_table:
    await db.drop_table("users")

17. Метрики производительности:
    Получайте статистику работы базы данных с помощью metrics:
    stats = db.metrics()
    print(stats)  # {'queries': ..., 'query_time': ..., 'cache_hits': ..., 'pool_size': ...}

18. Закрытие соединений:
    При использовании вне контекстного менеджера закрывайте базу явно:
    await db.close()

19. Кэширование запросов:
    Библиотека автоматически кэширует результаты запросов для повышения производительности. Кэш очищается при изменении данных.

20. Обработка ошибок:
    Ловите исключения AsyncSQLiteError и ConnectionPoolError для обработки ошибок:
    try:
        await db.fetchall("SELECT * FROM nonexistent_table")
    except AsyncSQLiteError as e:
        print(f"Ошибка: {e}")
"""