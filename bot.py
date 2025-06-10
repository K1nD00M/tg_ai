import logging
import pandas as pd
import aiohttp
import asyncio
import json
from datetime import datetime, time, timedelta
import pytz
import os
import base64
from io import BytesIO
import functools  # Import functools for partial
from dotenv import load_dotenv # Import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настраиваем более подробное логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Изменено с INFO на DEBUG
)
logger = logging.getLogger(__name__)

# Получаем токен из переменных окружения Railway ИЛИ .env файла
BOT_TOKEN = os.getenv('BOT_TOKEN')
# Удаляем получение EXCEL_DATA из переменных окружения
# EXCEL_DATA = os.getenv('EXCEL_DATA')

if not BOT_TOKEN:
    logger.error("Не найдена переменная окружения BOT_TOKEN")
    raise ValueError("Требуется BOT_TOKEN в переменных окружения или .env файле")
# Удаляем проверку EXCEL_DATA
# if not EXCEL_DATA:
#     logger.error("Не найдена переменная окружения EXCEL_DATA")
#     raise ValueError("Требуется EXCEL_DATA в переменных окружения или .env файле")


BASE_URL = f'https://api.telegram.org/bot{BOT_TOKEN}'
EXCEL_FILENAME = 'employees.xlsx' # Имя файла для чтения и сохранения

# Константы для уведомлений
MAX_NOTIFICATIONS = 3  # Максимальное количество уведомлений
NOTIFICATION_INTERVAL = timedelta(hours=2)  # Интервал между уведомлениями

# Словарь для хранения информации об уведомлениях
# Формат: {(recipient_id, birthday_person_id): {'count': int, 'last_sent': datetime, 'confirmed': bool}}
notification_tracking = {}

# Читаем Excel файл напрямую
try:
    # Удаляем декодирование из base64
    # excel_bytes = base64.b64decode(EXCEL_DATA)
    # Читаем из файла
    df = pd.read_excel(EXCEL_FILENAME, engine='openpyxl') # Указываем движок
    logger.info(f"Excel файл '{EXCEL_FILENAME}' загружен. Колонки: {df.columns.tolist()}")
    
    # Преобразуем числовые колонки в int
    numeric_columns = ['Tg_ID', 'Amount', 'NotificationDay', 'NotificationMonth']
    for col in numeric_columns:
        if col in df.columns:
            # Сначала преобразуем в числовой тип, обрабатывая ошибки
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Заполняем NaN нулями только если колонка существует и успешно преобразована
            if not df[col].isnull().all(): # Проверка, что колонка не состоит только из NaN
                 df[col] = df[col].fillna(0).astype('int64')
            else:
                 df[col] = df[col].fillna(0) # Если все NaN, просто заполняем нулями
except FileNotFoundError:
    logger.error(f"Ошибка: Файл '{EXCEL_FILENAME}' не найден в корне проекта.")
    raise
except Exception as e:
    logger.error(f"Ошибка при загрузке Excel файла '{EXCEL_FILENAME}': {e}")
    raise

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Словарь для хранения отправленных уведомлений (ключ теперь включает дату)
# Формат ключа: f"{recipient_tg_id}_{birthday_person_username}_{year}_{month}_{day}"
sent_notifications = {}

# Множество для хранения обработанных update_id
processed_updates = set()

def get_time_from_excel(time_str):
    try:
        # Проверяем сначала на datetime.time, т.к. Excel может его вернуть
        if isinstance(time_str, time):
             return time_str.hour, time_str.minute
        # Затем на datetime.datetime (если вдруг дата+время)
        elif isinstance(time_str, datetime):
            return time_str.hour, time_str.minute
        elif isinstance(time_str, str):
            # Попробуем разные форматы времени
            for fmt in ('%H:%M:%S', '%H:%M'):
                try:
                    time_obj = datetime.strptime(time_str, fmt).time()
                    return time_obj.hour, time_obj.minute
                except ValueError:
                    continue
        logger.warning(f"Не удалось распознать время: {time_str} (тип: {type(time_str)})")
        return None, None
    except Exception as e:
        logger.error(f"Ошибка при обработке времени '{time_str}': {e}")
        return None, None

# Функция для сохранения DataFrame в Excel в отдельном потоке
async def save_excel_async(dataframe, filename):
    logger.debug(f"Запуск сохранения Excel файла {filename} в отдельном потоке...")
    try:
        # Создаем частичную функцию для передачи аргументов
        # Используем BytesIO чтобы избежать проблем с временными файлами в некоторых средах
        buffer = BytesIO()
        # Используем partial для передачи аргументов в to_excel
        save_func = functools.partial(dataframe.to_excel, buffer, index=False, engine='openpyxl')
        await asyncio.to_thread(save_func)
        buffer.seek(0)
        # Здесь можно было бы загрузить buffer обратно в EXCEL_DATA base64 и обновить переменную окружения,
        # но это сложно и зависит от платформы. Проще сохранять в локальный файл, если бот перезапускается.
        # Пока просто логируем успешное сохранение в буфер.
        # Если нужно реальное сохранение, нужен доступ к файловой системе или другой механизм.
        # Раскомментируем запись в файл
        with open(filename, 'wb') as f:
             f.write(buffer.getvalue())
        # logger.info(f"Данные (Tg_ID) успешно подготовлены для сохранения (в памяти).") # Изменено сообщение
        logger.info(f"Данные успешно сохранены в файл {filename}.") # Новое сообщение
    except Exception as e:
        logger.error(f"Ошибка при сохранении Excel в потоке: {e}")


async def send_message(chat_id: int, text: str, keyboard=None) -> bool:
    # Устанавливаем таймаут для сессии
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        payload = {
            'chat_id': int(chat_id),
            'text': text
        }
        
        if keyboard:
            payload['reply_markup'] = json.dumps(keyboard)
            
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f'{BASE_URL}/sendMessage',
                json=payload
            ) as response:
                if response.status == 200:
                    logger.debug(f"Сообщение успешно отправлено chat_id={chat_id}")
                    return True
                else:
                    logger.error(f"Ошибка отправки сообщения chat_id={chat_id}: Статус {response.status}, Ответ: {await response.text()}")
                    return False
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка сети при отправке сообщения chat_id={chat_id}: {e}")
        return False
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при отправке сообщения chat_id={chat_id}")
        return False
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при отправке сообщения chat_id={chat_id}: {e}")
        return False


async def handle_callback_query(callback_query: dict) -> None:
    """Обрабатывает нажатия на inline кнопки."""
    try:
        data = callback_query['data']
        user_id = callback_query['from']['id']
        
        if data.startswith('confirm_'):
            birthday_person_id = int(data.split('_')[1])
            key = (user_id, birthday_person_id)
            
            if key in notification_tracking:
                notification_tracking[key]['confirmed'] = True
                await send_message(user_id, "✅ Спасибо за подтверждение!")
                
                # Отвечаем на callback query, чтобы убрать часики с кнопки
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f'{BASE_URL}/answerCallbackQuery',
                        json={'callback_query_id': callback_query['id']}
                    )
    except Exception as e:
        logger.error(f"Ошибка при обработке callback query: {e}")

async def handle_update(update: dict) -> None:
    if 'callback_query' in update:
        await handle_callback_query(update['callback_query'])
        return
        
    if 'message' not in update:
        logger.debug("Апдейт без 'message', пропускаем.")
        return

    message = update['message']
    text = message.get('text', '')
    chat_id = message['chat']['id'] # Используем chat_id напрямую

    if not text.startswith('/start'):
        logger.debug(f"Сообщение не /start (chat_id={chat_id}), пропускаем.")
        return

    user = message['from']
    username = user.get('username') # Берем без @
    user_id = user['id']

    logger.info(f"Получена команда /start от user_id={user_id}, username={username}")

    # Используем f-string для @username если он есть
    tg_username_with_at = f'@{username}' if username else None

    # Ищем пользователя по Tg_ID или Tg_Username
    user_row = df[(df['Tg_ID'] == user_id) | (df['Tg_Username'] == tg_username_with_at)]

    if not user_row.empty:
        # Пользователь найден, проверяем нужно ли обновить Tg_ID
        needs_update = False
        idx_to_update = []

        for idx in user_row.index:
            # Обновляем ID если он 0 или не совпадает (на случай смены username)
            if df.loc[idx, 'Tg_ID'] == 0 or df.loc[idx, 'Tg_ID'] != user_id:
                df.loc[idx, 'Tg_ID'] = int(user_id)
                needs_update = True
                idx_to_update.append(idx)
            # Обновляем Username если он пустой или не совпадает (на случай смены ID)
            if tg_username_with_at and (pd.isna(df.loc[idx, 'Tg_Username']) or df.loc[idx, 'Tg_Username'] != tg_username_with_at):
                 df.loc[idx, 'Tg_Username'] = tg_username_with_at
                 needs_update = True
                 if idx not in idx_to_update: # Добавляем индекс, если еще не там
                     idx_to_update.append(idx)


        if needs_update:
            logger.info(f"Обновляем Tg_ID и/или Tg_Username для user_id={user_id}, username={tg_username_with_at} по индексам {idx_to_update}")
            # Запускаем сохранение в Excel асинхронно
            await save_excel_async(df.copy(), EXCEL_FILENAME) # Передаем копию, чтобы не влиять на текущий df
            await send_message(user_id, 'Ваш ID и/или Username успешно обновлен в базе данных!')
        else:
            logger.info(f"Tg_ID и Tg_Username для user_id={user_id} уже актуальны.")
            await send_message(user_id, 'Вы уже зарегистрированы и ваши данные актуальны.')
    else:
        # Пользователь не найден ни по ID, ни по Username
        logger.warning(f"Пользователь user_id={user_id}, username={tg_username_with_at} не найден в базе.")
        await send_message(user_id, 'Вы не найдены в базе данных сотрудников.')


async def check_notifications() -> None:
    """Проверяет и отправляет уведомления о днях рождения."""
    while True:
        try:
            current_time = datetime.now(MOSCOW_TZ)
            current_month = current_time.month
            current_day = current_time.day
            
            # Получаем список всех сотрудников
            all_employees = df[df['Tg_ID'].notna() & (df['Tg_ID'] != 0)]
            
            # Находим сотрудников, у которых сегодня день рождения
            birthday_employees = all_employees[
                (all_employees['NotificationMonth'] == current_month) &
                (all_employees['NotificationDay'] == current_day)
            ]
            
            for _, birthday_person in birthday_employees.iterrows():
                birthday_person_id = int(birthday_person['Tg_ID'])
                birthday_person_name = birthday_person.get('Name', 'Сотрудник')
                
                # Отправляем уведомления всем сотрудникам, кроме именинника
                for _, recipient in all_employees.iterrows():
                    recipient_id = int(recipient['Tg_ID'])
                    if recipient_id != birthday_person_id:
                        await send_birthday_notification(
                            recipient_id,
                            birthday_person_name,
                            birthday_person_id
                        )
            
            # Ждем 1 минуту перед следующей проверкой
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка при проверке уведомлений: {e}")
            await asyncio.sleep(60)  # Ждем минуту при ошибке


async def get_updates(offset: int = 0, timeout: int = 30) -> dict:
    # Устанавливаем таймаут для сессии и для запроса
    client_timeout = aiohttp.ClientTimeout(total=timeout + 5) # Общий таймаут чуть больше таймаута запроса
    try:
        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            logger.debug(f"Запрос getUpdates с offset={offset}, timeout={timeout}")
            async with session.get(f'{BASE_URL}/getUpdates', params={'offset': offset, 'timeout': timeout}) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Получено {len(data.get('result', []))} апдейтов.")
                    return data
                elif response.status == 409: # Conflict (возможно, используется webhook)
                     logger.error(f"Ошибка getUpdates: Статус 409 Conflict. Возможно, активен webhook? Попытка удалить...")
                     await delete_webhook() # Пробуем удалить вебхук
                     return {'ok': False, 'error_code': 409, 'description': 'Conflict: terminated by other getUpdates request; make sure that only one bot instance is running'}
                else:
                    logger.error(f"Ошибка getUpdates: Статус {response.status}, Ответ: {await response.text()}")
                    return {'ok': False, 'error_code': response.status, 'description': await response.text()}
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка сети при getUpdates: {e}")
        return {'ok': False, 'description': f'Network error: {e}'}
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут ({timeout}s) при запросе getUpdates.")
        # Это ожидаемое поведение при long-polling, не ошибка
        return {'ok': True, 'result': []}
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при getUpdates: {e}")
        return {'ok': False, 'description': f'Unexpected error: {e}'}


async def delete_webhook() -> bool:
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            logger.info("Удаляем вебхук (если есть)...")
            async with session.get(f'{BASE_URL}/deleteWebhook') as response:
                if response.status == 200:
                    logger.info("Вебхук успешно удален или отсутствовал.")
                    return True
                else:
                    logger.error(f"Ошибка при удалении вебхука: Статус {response.status}, Ответ: {await response.text()}")
                    return False
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка сети при удалении вебхука: {e}")
        return False
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при удалении вебхука.")
        return False
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при удалении вебхука: {e}")
        return False


async def cleanup_old_connections(retries=3, delay=2) -> None:
    # Эта функция в режиме long polling необязательна, т.к. offset сам обрабатывает старые сообщения.
    # Но оставим ее для первоначальной очистки при старте.
    logger.info("Попытка очистить очередь старых апдейтов...")
    offset = 0
    for attempt in range(retries):
        try:
            updates = await get_updates(offset=offset, timeout=1) # Короткий таймаут для быстрой очистки
            if updates.get('ok') and updates.get('result'):
                last_update_id = updates['result'][-1]['update_id']
                offset = last_update_id + 1
                logger.info(f"Найдено {len(updates['result'])} старых апдейтов. Устанавливаем offset = {offset}")
                # Повторный вызов с новым offset для подтверждения очистки
                await get_updates(offset=offset, timeout=1)
                logger.info(f"Очередь апдейтов очищена до ID: {offset}")
                return # Успешно очищено
            elif updates.get('ok'):
                logger.info("Старых апдейтов для очистки нет.")
                return # Очередь пуста
            else:
                 logger.warning(f"Не удалось получить апдейты для очистки (попытка {attempt + 1}/{retries}): {updates.get('description')}")

        except Exception as e:
            logger.error(f"Ошибка при очистке старых подключений (попытка {attempt + 1}/{retries}): {e}")

        if attempt < retries - 1:
            await asyncio.sleep(delay)

    logger.error("Не удалось очистить очередь старых апдейтов после нескольких попыток.")


async def main() -> None:
    logger.info("Запуск бота...")

    # Логирование расписания уведомлений после загрузки df
    logger.info("="*70)
    logger.info("Загруженное расписание уведомлений:")
    logger.info("-"*70)
    logger.info(f"{'Имя':<25} | {'День':<5} | {'Месяц':<6} | {'Время (ЧЧ:ММ)':<15}")
    logger.info("-"*70)
    schedule_found = False
    try:
        # Проверяем, что df существует и не пуст
        if 'df' in globals() and not df.empty:
            for idx in df.index:
                name = df.loc[idx, 'Name']
                day = df.loc[idx, 'NotificationDay']
                month = df.loc[idx, 'NotificationMonth']
                time_str = df.loc[idx, 'NotificationTime']

                # Выводим только строки, где есть день, месяц и время уведомления
                if pd.notna(day) and pd.notna(month) and pd.notna(time_str):
                    notify_hour, notify_minute = get_time_from_excel(time_str)
                    if notify_hour is not None and notify_minute is not None:
                        time_formatted = f"{notify_hour:02d}:{notify_minute:02d}"
                    else:
                        time_formatted = f"Ошибка: {time_str}"

                    try:
                        # Преобразуем день/месяц в int для чистого вывода
                        day_formatted = str(int(day))
                        month_formatted = str(int(month))
                    except (ValueError, TypeError):
                        day_formatted = str(day) # Оставляем как есть, если не int
                        month_formatted = str(month)

                    logger.info(f"{str(name):<25} | {day_formatted:<5} | {month_formatted:<6} | {time_formatted:<15}")
                    schedule_found = True
        else:
            logger.warning("DataFrame 'df' не найден или пуст, расписание не может быть выведено.")

        if not schedule_found:
            logger.info("В файле не найдено настроенных уведомлений (с указанием дня, месяца и времени).")

    except Exception as e:
        logger.error(f"Ошибка при выводе расписания уведомлений: {e}")
    finally:
        logger.info("="*70)


    if not await delete_webhook(): # Сначала удаляем вебхук
         logger.warning("Не удалось удалить вебхук. Возможны проблемы с получением апдейтов.")
         # Можно либо прервать выполнение, либо продолжить с риском конфликта

    await cleanup_old_connections() # Очищаем очередь апдейтов

    offset = 0
    while True:
        try:
            # Получаем апдейты с long polling
            updates = await get_updates(offset=offset, timeout=60) # Таймаут 60 секунд

            if updates.get('ok'):
                if updates.get('result'):
                    for update in updates['result']:
                        update_id = update['update_id']
                        # Проверяем, не обработан ли уже апдейт (на всякий случай, хотя offset должен это предотвращать)
                        if update_id >= offset: # Строго говоря, offset должен быть update_id + 1
                           offset = update_id + 1
                           logger.debug(f"Обработка update_id={update_id}")
                           # Запускаем обработку апдейта как задачу, чтобы не блокировать получение следующих
                           asyncio.create_task(handle_update(update))
                        # else: # Логгирование старых апдейтов убрано, чтобы не засорять
                            # logger.warning(f"Получен старый update_id={update_id} при offset={offset}. Пропускаем.")

                    # Очищаем старые ключи sent_notifications (например, старше 1 дня), чтобы словарь не рос бесконечно
                    now_ts = datetime.now(MOSCOW_TZ).timestamp()
                    keys_to_delete = []
                    for key, timestamp in sent_notifications.items():
                        # Предполагаем, что значение - это True или временная метка. Сделаем его временной меткой.
                        # Но т.к. сейчас там True, добавим проверку на возраст ключа по дате в нем
                        try:
                             parts = key.split('_')
                             key_year, key_month, key_day = int(parts[-3]), int(parts[-2]), int(parts[-1])
                             key_date = datetime(key_year, key_month, key_day, tzinfo=MOSCOW_TZ)
                             # Удаляем ключи старше, например, 2 дней
                             if (now - key_date).days > 1:
                                 keys_to_delete.append(key)
                        except (IndexError, ValueError):
                             logger.warning(f"Не удалось разобрать дату из ключа уведомления: {key}")
                             # Можно удалить некорректные ключи
                             keys_to_delete.append(key)

                    if keys_to_delete:
                         logger.info(f"Удаление {len(keys_to_delete)} старых/некорректных ключей из sent_notifications.")
                         for key in keys_to_delete:
                             del sent_notifications[key]

                # else: # Если result пустой, значит не было новых апдейтов за timeout
                    # logger.debug("Нет новых апдейтов.")
                    pass # Ничего не делаем, идем на следующую итерацию getUpdates

            else:
                # Ошибка при получении апдейтов (не таймаут)
                logger.error(f"Ошибка получения апдейтов: {updates.get('description')}")
                # Пауза перед повторной попыткой
                await asyncio.sleep(10)
                # Сбрасываем offset? Нет, Telegram рекомендует увеличивать offset даже при ошибках, если update_id известен
                # Если ошибка 409, get_updates уже пытался удалить вебхук

            # Проверяем уведомления раз в минуту (или около того, т.к. getUpdates может занимать время)
            # Чтобы гарантировать проверку раз в минуту, лучше запускать ее отдельной задачей asyncio.
            # Пока оставим здесь, но вызов будет происходить после возврата getUpdates.
            await check_notifications()
            # Убираем sleep(1), т.к. long polling getUpdates сам обеспечивает ожидание
            # await asyncio.sleep(1) # УДАЛЕНО

        except asyncio.CancelledError:
             logger.info("Получен сигнал отмены. Завершение работы...")
             break # Выход из цикла while True
        except Exception as e:
            logger.exception(f"Критическая ошибка в основном цикле:") # Используем logger.exception для вывода стектрейса
            await asyncio.sleep(15) # Более длительная пауза при серьезных сбоях


async def send_birthday_notification(recipient_id: int, birthday_person_name: str, birthday_person_id: int) -> bool:
    """Отправляет уведомление о дне рождения с кнопкой подтверждения."""
    key = (recipient_id, birthday_person_id)
    current_time = datetime.now(MOSCOW_TZ)
    
    # Проверяем, не подтвердил ли уже пользователь
    if key in notification_tracking and notification_tracking[key]['confirmed']:
        return False
        
    # Проверяем количество отправленных уведомлений
    if key in notification_tracking:
        if notification_tracking[key]['count'] >= MAX_NOTIFICATIONS:
            return False
            
        # Проверяем интервал между уведомлениями
        last_sent = notification_tracking[key]['last_sent']
        if current_time - last_sent < NOTIFICATION_INTERVAL:
            return False
            
        notification_tracking[key]['count'] += 1
    else:
        notification_tracking[key] = {
            'count': 1,
            'confirmed': False
        }
    
    notification_tracking[key]['last_sent'] = current_time
    
    message_text = f"🎂 Напоминание: У {birthday_person_name} сегодня день рождения!"
    keyboard = {
        'inline_keyboard': [[
            {
                'text': 'Отправил',
                'callback_data': f'confirm_{birthday_person_id}'
            }
        ]]
    }
    
    return await send_message(recipient_id, message_text, keyboard)


if __name__ == '__main__':
    logger.info("Начало работы бота")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную (KeyboardInterrupt).")
    finally:
        logger.info("Завершение работы бота.") 