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
import functools
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настраиваем более подробное логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Получаем токен из переменных окружения Railway ИЛИ .env файла
BOT_TOKEN = os.getenv('BOT_TOKEN')

if not BOT_TOKEN:
    logger.error("Не найдена переменная окружения BOT_TOKEN")
    raise ValueError("Требуется BOT_TOKEN в переменных окружения или .env файле")

BASE_URL = f'https://api.telegram.org/bot{BOT_TOKEN}'
EXCEL_FILENAME = 'employees.xlsx'

# Константы для уведомлений
MAX_NOTIFICATIONS = 3  # Максимальное количество уведомлений
NOTIFICATION_INTERVAL = timedelta(hours=2)  # Интервал между уведомлениями

# Словарь для хранения информации об уведомлениях
# Формат: {(recipient_id, birthday_person_id): {'count': int, 'last_sent': datetime, 'confirmed': bool}}
notification_tracking = {}

# Читаем Excel файл
try:
    df = pd.read_excel(EXCEL_FILENAME, engine='openpyxl')
    logger.info(f"Excel файл '{EXCEL_FILENAME}' загружен. Колонки: {df.columns.tolist()}")
    
    # Преобразуем числовые колонки в int
    numeric_columns = ['Tg_ID', 'Amount', 'NotificationDay', 'NotificationMonth']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if not df[col].isnull().all():
                df[col] = df[col].fillna(0).astype('int64')
            else:
                df[col] = df[col].fillna(0)
except FileNotFoundError:
    logger.error(f"Ошибка: Файл '{EXCEL_FILENAME}' не найден в корне проекта.")
    raise
except Exception as e:
    logger.error(f"Ошибка при загрузке Excel файла '{EXCEL_FILENAME}': {e}")
    raise

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

async def send_message(chat_id: int, text: str, keyboard=None) -> bool:
    """Отправляет сообщение пользователю с опциональной клавиатурой."""
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
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения chat_id={chat_id}: {e}")
        return False

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
    """Обрабатывает входящие обновления от Telegram."""
    if 'callback_query' in update:
        await handle_callback_query(update['callback_query'])
        return
        
    if 'message' not in update:
        logger.debug("Апдейт без 'message', пропускаем.")
        return

    message = update['message']
    text = message.get('text', '')
    chat_id = message['chat']['id']

    if not text.startswith('/start'):
        logger.debug(f"Сообщение не /start (chat_id={chat_id}), пропускаем.")
        return

    user = message['from']
    username = user.get('username')
    user_id = user['id']

    logger.info(f"Получена команда /start от user_id={user_id}, username={username}")

    tg_username_with_at = f'@{username}' if username else None

    user_row = df[(df['Tg_ID'] == user_id) | (df['Tg_Username'] == tg_username_with_at)]

    if not user_row.empty:
        needs_update = False
        idx_to_update = []

        for idx in user_row.index:
            if df.loc[idx, 'Tg_ID'] == 0 or df.loc[idx, 'Tg_ID'] != user_id:
                df.loc[idx, 'Tg_ID'] = int(user_id)
                needs_update = True
                idx_to_update.append(idx)
            if tg_username_with_at and (pd.isna(df.loc[idx, 'Tg_Username']) or df.loc[idx, 'Tg_Username'] != tg_username_with_at):
                df.loc[idx, 'Tg_Username'] = tg_username_with_at
                needs_update = True
                if idx not in idx_to_update:
                    idx_to_update.append(idx)

        if needs_update:
            logger.info(f"Обновляем Tg_ID и/или Tg_Username для user_id={user_id}, username={tg_username_with_at}")
            await save_excel_async(df.copy(), EXCEL_FILENAME)
            await send_message(user_id, 'Ваш ID и/или Username успешно обновлен в базе данных!')
        else:
            logger.info(f"Tg_ID и Tg_Username для user_id={user_id} уже актуальны.")
            await send_message(user_id, 'Вы уже зарегистрированы и ваши данные актуальны.')
    else:
        logger.info(f"Пользователь user_id={user_id} не найден в базе данных.")
        await send_message(user_id, 'Вы не найдены в базе данных сотрудников.')

async def check_notifications() -> None:
    """Проверяет и отправляет уведомления о днях рождения."""
    while True:
        try:
            current_time = datetime.now(MOSCOW_TZ)
            current_month = current_time.month
            current_day = current_time.day
            
            logger.debug(f"Проверка уведомлений: {current_day}.{current_month}")
            
            # Получаем список всех сотрудников
            all_employees = df[df['Tg_ID'].notna() & (df['Tg_ID'] != 0)]
            logger.debug(f"Всего сотрудников в базе: {len(all_employees)}")
            
            # Находим сотрудников, у которых сегодня день рождения
            birthday_employees = all_employees[
                (all_employees['NotificationMonth'] == current_month) &
                (all_employees['NotificationDay'] == current_day)
            ]
            
            logger.info(f"Найдено именинников: {len(birthday_employees)}")
            
            for _, birthday_person in birthday_employees.iterrows():
                birthday_person_id = int(birthday_person['Tg_ID'])
                birthday_person_name = birthday_person.get('Name', 'Сотрудник')
                
                logger.info(f"Обработка ДР: {birthday_person_name} (ID: {birthday_person_id})")
                
                # Отправляем уведомления всем сотрудникам, кроме именинника
                for _, recipient in all_employees.iterrows():
                    recipient_id = int(recipient['Tg_ID'])
                    if recipient_id != birthday_person_id:
                        logger.debug(f"Отправка уведомления получателю ID: {recipient_id}")
                        success = await send_birthday_notification(
                            recipient_id,
                            birthday_person_name,
                            birthday_person_id
                        )
                        if success:
                            logger.info(f"Уведомление успешно отправлено получателю ID: {recipient_id}")
                        else:
                            logger.debug(f"Уведомление не отправлено получателю ID: {recipient_id} (возможно, уже подтверждено или превышен лимит)")
            
            # Ждем 1 минуту перед следующей проверкой
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка при проверке уведомлений: {e}")
            await asyncio.sleep(60)

async def get_updates(offset: int = 0, timeout: int = 30) -> dict:
    """Получает обновления от Telegram API."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'{BASE_URL}/getUpdates',
                params={'offset': offset, 'timeout': timeout}
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Ошибка получения обновлений: {response.status}")
                    return {'ok': False, 'result': []}
    except Exception as e:
        logger.error(f"Ошибка при получении обновлений: {e}")
        return {'ok': False, 'result': []}

async def delete_webhook() -> bool:
    """Удаляет вебхук при старте бота."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{BASE_URL}/deleteWebhook') as response:
                if response.status == 200:
                    logger.info("Вебхук успешно удален")
                    return True
                else:
                    logger.error(f"Ошибка удаления вебхука: {response.status}")
                    return False
    except Exception as e:
        logger.error(f"Ошибка при удалении вебхука: {e}")
        return False

async def save_excel_async(dataframe, filename):
    """Асинхронно сохраняет DataFrame в Excel файл."""
    logger.debug(f"Запуск сохранения Excel файла {filename} в отдельном потоке...")
    try:
        buffer = BytesIO()
        save_func = functools.partial(dataframe.to_excel, buffer, index=False, engine='openpyxl')
        await asyncio.to_thread(save_func)
        buffer.seek(0)
        with open(filename, 'wb') as f:
            f.write(buffer.getvalue())
        logger.info(f"Данные успешно сохранены в файл {filename}.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении Excel в потоке: {e}")

async def main() -> None:
    """Основная функция бота."""
    try:
        # Удаляем вебхук при старте
        await delete_webhook()
        
        # Запускаем проверку уведомлений в отдельной задаче
        notification_task = asyncio.create_task(check_notifications())
        
        offset = 0
        while True:
            try:
                updates = await get_updates(offset)
                if updates.get('ok'):
                    for update in updates['result']:
                        offset = update['update_id'] + 1
                        await handle_update(update)
                else:
                    logger.error("Получен некорректный ответ от Telegram API")
                    await asyncio.sleep(5)
                
                # Проверяем уведомления в основном цикле
                await check_notifications()
                
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}")
                await asyncio.sleep(5)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        # Отменяем задачу уведомлений при завершении
        notification_task.cancel()
        try:
            await notification_task
        except asyncio.CancelledError:
            pass

if __name__ == '__main__':
    asyncio.run(main()) 