import logging
import pandas as pd
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(message)s',
    level=logging.INFO
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
notification_tracking = {}

# Читаем Excel файл
try:
    df = pd.read_excel(EXCEL_FILENAME, engine='openpyxl')
except FileNotFoundError:
    logger.error(f"Ошибка: Файл '{EXCEL_FILENAME}' не найден в корне проекта.")
    raise
except Exception as e:
    logger.error(f"Ошибка при загрузке Excel файла '{EXCEL_FILENAME}': {e}")
    raise

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def escape_markdown(text: str) -> str:
    """Экранирует спецсимволы для Telegram MarkdownV2."""
    escape_chars = r'_()*[]~`>#+-=|{}.!'
    return ''.join(['\\' + c if c in escape_chars else c for c in text])

async def send_message(chat_id: int, text: str, keyboard=None) -> bool:
    """Отправляет сообщение пользователю с опциональной клавиатурой."""
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        payload = {
            'chat_id': int(chat_id),
            'text': text,
            'parse_mode': 'Markdown'
        }
        
        if keyboard:
            payload['reply_markup'] = json.dumps(keyboard)
            
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f'{BASE_URL}/sendMessage', json=payload) as response:
                if response.status == 200:
                    return True
                else:
                    return False
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        return False

async def send_birthday_notification(recipient_id: int, birthday_person_name: str, birthday_person_id: int) -> bool:
    key = (recipient_id, birthday_person_id)
    current_time = datetime.now(MOSCOW_TZ)

    # Если уже подтвердил — не отправлять
    if key in notification_tracking and notification_tracking[key].get('confirmed', False):
        return False

    # Если уже отправлено 3 раза — не отправлять
    if key in notification_tracking and notification_tracking[key].get('count', 0) >= MAX_NOTIFICATIONS:
        return False

    if key in notification_tracking and 'last_sent' in notification_tracking[key]:
        last_sent = notification_tracking[key]['last_sent']
        if (current_time - last_sent).total_seconds() < 120 * 60:
            return False

    # Обновляем счетчик и время
    if key not in notification_tracking:
        notification_tracking[key] = {'count': 1, 'last_sent': current_time, 'confirmed': False}
    else:
        notification_tracking[key]['count'] += 1
        notification_tracking[key]['last_sent'] = current_time

    # Получаем данные именинника
    birthday_person = df[df['Tg_ID'] == birthday_person_id]
    if birthday_person.empty:
        return False

    birthday_person_name = birthday_person.iloc[0].get('Name', 'Сотрудник')
    # Оставляем только день и месяц без года
    birthday_date_raw = str(birthday_person.iloc[0].get('BirthdayDate', '')).split()[0]
    try:
        # Попробуем распарсить дату в формате ГГГГ-ММ-ДД или ДД.ММ.ГГГГ
        if '-' in birthday_date_raw:
            parts = birthday_date_raw.split('-')
            birthday_date = f"{parts[2]}.{parts[1]}"
        elif '.' in birthday_date_raw:
            parts = birthday_date_raw.split('.')
            birthday_date = f"{parts[0]}.{parts[1]}"
        else:
            birthday_date = birthday_date_raw
    except Exception:
        birthday_date = birthday_date_raw
    amount = birthday_person.iloc[0].get('Amount', '')
    buddy_username = birthday_person.iloc[0].get('Buddy_Tg_Username', '')
    buddy_phone = birthday_person.iloc[0].get('Buddy_Phone', '')
    buddy_bank = birthday_person.iloc[0].get('Buddy_Bank', '')
    birthday_person_username = birthday_person.iloc[0].get('Tg_Username', '')
    birthday_person_time = str(birthday_person.iloc[0].get('NotificationTime', '00:00:00'))
    # Получаем NotificationTime для получателя
    recipient_row = df[df['Tg_ID'] == recipient_id]
    recipient_time = str(recipient_row.iloc[0].get('NotificationTime', '00:00:00')) if not recipient_row.empty else '00:00:00'
    current_time_str = datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')
    # Проверяем время только для самой первой отправки
    if key not in notification_tracking and current_time_str < recipient_time:
        return False

    # Получаем Amount для получателя
    amount = recipient_row.iloc[0].get('Amount', '') if not recipient_row.empty else ''

    message_text = (
        f"Привет!\n"
        f"У {birthday_person_name} ({birthday_person_username}) день рождения {birthday_date}.\n"
        f"Переведи, пожалуйста, сегодня {amount} рублей {buddy_username} по телефону {buddy_phone} в {buddy_bank} банк.\n\n"
        f"**После перевода нажми кнопку 'Отправил'**"
    )

    keyboard = {
        'inline_keyboard': [[
            {
                'text': 'Отправил',
                'callback_data': f'confirm_{birthday_person_id}'
            }
        ]]
    }

    success = await send_message(recipient_id, message_text, keyboard)
    if success:
        logger.info(f"Уведомление отправлено пользователю {recipient_id} о дне рождения {birthday_person_name}.")
    return success

async def handle_callback_query(callback_query: dict) -> None:
    """Обрабатывает нажатия на inline кнопки."""
    try:
        data = callback_query['data']
        user_id = callback_query['from']['id']

        # Проверка, что это кнопка "Отправил"
        if data.startswith('confirm_'):
            birthday_person_id = int(data.split('_')[1])
            key = (user_id, birthday_person_id)

            # Обновляем состояние уведомлений, если пользователь подтвердил
            if key in notification_tracking:
                notification_tracking[key]['confirmed'] = True
                await send_message(user_id, "✅ Спасибо за подтверждение!")

                # Отвечаем на callback query, чтобы убрать часики с кнопки
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f'{BASE_URL}/answerCallbackQuery',
                        json={'callback_query_id': callback_query['id']}
                    )

                # Логируем успешное подтверждение
                logger.info(f"Пользователь {user_id} подтвердил уведомление для {birthday_person_id}.")
                
            else:
                # Если ключ не найден в tracking или уже подтверждено, то уведомление уже было обработано
                await send_message(user_id, "Вы уже подтвердили перевод или истек срок для подтверждения.")

    except Exception as e:
        logger.error(f"Ошибка при обработке callback query: {e}")

async def check_notifications() -> None:
    """Проверяет и отправляет уведомления о днях рождения по индивидуальной дате и времени именинника."""
    while True:
        try:
            current_time = datetime.now(MOSCOW_TZ)
            current_month = current_time.month
            current_day = current_time.day
            current_time_str = current_time.strftime('%H:%M:%S')

            # Получаем список всех сотрудников
            all_employees = df[df['Tg_ID'].notna() & (df['Tg_ID'] != 0)]

            # Для каждого сотрудника проверяем его дату и время уведомления
            for _, birthday_person in all_employees.iterrows():
                birthday_person_id = int(birthday_person['Tg_ID'])
                birthday_person_name = birthday_person.get('Name', 'Сотрудник')
                notification_day = birthday_person.get('NotificationDay', None)
                notification_month = birthday_person.get('NotificationMonth', None)
                notification_time = str(birthday_person.get('NotificationTime', '00:00:00'))

                # Проверяем, совпадает ли дата и время
                if (
                    notification_month == current_month and
                    notification_day == current_day and
                    current_time_str >= notification_time
                ):
                    # Отправляем уведомления всем сотрудникам, кроме именинника
                    for _, recipient in all_employees.iterrows():
                        recipient_id = int(recipient['Tg_ID'])
                        if recipient_id != birthday_person_id:
                            success = await send_birthday_notification(
                                recipient_id,
                                birthday_person_name,
                                birthday_person_id
                            )
                            if success:
                                logger.info(f"Уведомление отправлено пользователю {recipient_id} о дне рождения {birthday_person_name}.")

            # Ждем 1 минуту перед следующей проверкой
            await asyncio.sleep(60)
        except Exception as e:
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
                    return {'ok': False, 'result': []}
    except Exception as e:
        return {'ok': False, 'result': []}

async def handle_update(update: dict) -> None:
    """Обрабатывает входящие обновления от Telegram API."""
    try:
        # Обработка callback-запросов (нажатия на кнопки)
        if 'callback_query' in update:
            await handle_callback_query(update['callback_query'])
            return

        # Обработка текстовых сообщений (если нужно)
        if 'message' in update and 'text' in update['message']:
            message = update['message']
            chat_id = message['chat']['id']
            text = message['text']

            # Пример обработки команд
            if text.startswith('/start'):
                await send_message(chat_id, "Привет! Я бот для поздравлений с днем рождения.")
            elif text.startswith('/help'):
                await send_message(chat_id, "Я отправляю уведомления о днях рождения сотрудников.")
            # Добавьте другие команды или обработку сообщений по необходимости

    except Exception as e:
        logger.error(f"Ошибка при обработке обновления: {e}")

async def main() -> None:
    """Основная функция бота."""
    try:
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
                    await asyncio.sleep(5)
            except Exception as e:
                await asyncio.sleep(5)
    except Exception as e:
        raise
    finally:
        notification_task.cancel()
        try:
            await notification_task
        except asyncio.CancelledError:
            pass

if __name__ == '__main__':
    asyncio.run(main())
