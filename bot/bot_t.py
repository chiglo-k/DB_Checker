import asyncio
import logging
from datetime import datetime
from psycopg2 import connect
from telegram.ext import ApplicationBuilder, ContextTypes
import toml

tlm = toml.load('bot_instance.toml')

# Настройки
BOT_TOKEN = tlm['bot']['bot_token']
CHAT_ID = tlm['bot']['chat_id']
DB_CONFIG = {
    "database": tlm['telegram']['database'],
    "user": tlm['telegram']['user'],
    "password": tlm['telegram']['password'],
    "host": tlm['telegram']['host'],
    "port": tlm['telegram']['port']
}


async def execute_query(query_name: str, sql_query: str, application):
    """Выполняет SQL-запрос и отправляет результат в отформатированном виде"""
    try:
        conn = None
        cursor = None

        try:
            conn = connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            data = cursor.fetchall()

            # Форматирование сообщения
            message = f"{query_name} на {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:\n\n"
            has_valid_data = False

            for description, value in data:
                # Пропускаем записи, где оба поля содержат текст "None"
                if str(description).lower() == "none" and str(value).lower() == "none":
                    continue

                has_valid_data = True

                if query_name == "Check Files":
                    # Сохраняем оригинальные значения
                    files_desc = description
                    altered_desc = value

                    # Проверяем, не пустые ли обе строки
                    if not files_desc and not altered_desc:
                        continue

                    message += f"Новые файлы: {files_desc} \nИзмененные файлы: {altered_desc}\n"
                else:
                    desc_text = description
                    value_text = value

                    # Проверяем, не пустые ли обе строки
                    if not desc_text and not value_text:
                        continue

                    message += f"{desc_text}: {value_text}\n"

            # Отправляем сообщение только если есть валидные данные
            if has_valid_data:
                await application.bot.send_message(
                    chat_id=CHAT_ID,
                    text=message,
                    parse_mode='Markdown'
                )
            else:
                logging.info(f"Нет данных для отправки ({query_name})")

        except Exception as e:
            logging.error(f"Ошибка при выполнении запроса {query_name}: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    except Exception as e:
        logging.error(f"Критическая ошибка в execute_query: {e}")
        error_message = f"Ошибка выполнения {query_name}: {str(e)}"
        await application.bot.send_message(
            chat_id=CHAT_ID,
            text=error_message
        )

async def periodic_task(interval: int, query_name: str, sql_query: str, application) -> None:
    """
    Выполняет задачу с указанным интервалом
    """
    while True:
        try:
            await execute_query(query_name, sql_query, application)
        except Exception as e:
            logging.error(f"Ошибка в periodic_task: {e}")
        finally:
            await asyncio.sleep(interval)


def main() -> None:
    """
    Основная функция приложения
    """
    try:
        # Инициализация приложения
        application = ApplicationBuilder().token(BOT_TOKEN).build()

        # Создание цикла событий
        loop = asyncio.get_event_loop()

        # Создание задачи ежедневной проверки
        loop.create_task(
            periodic_task(
                21600,  # 6 часов (5,11 (10 обнова), 17(17) 23)
                "Daily Check",
                "SELECT name_of_warning, count_of_warning FROM check_sys.daily_check",
                application
            )
        )

        # Создание задачи проверки файлов
        loop.create_task(
            periodic_task(
                720,  # 15 минут
                "Check Files",
                "SELECT new_files, alter_files FROM check_sys.check_files",
                application
            )
        )

        # Запуск приложения
        application.run_polling()

    except Exception as e:
        logging.error(f"Критическая ошибка в main: {e}")
        raise


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        main()
    except Exception as e:
        logging.error(f"Критическая ошибка при запуске: {e}")
