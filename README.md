## DB_Check.01.12.24

Проверка Базы данных по пропущенным значениям
при заполнении, проверка незакрытых дополнений хранения.

**init.py**:

 - Инициализирует выполнение приложения.

**App.py**:

 - Выполнение Аутентификации пользователя и запуск модулей проверки БД.

**FrameExcel.py**

 - Выполнение обработки файлов внутри сведенных данных БД;
 - БД представлена файлом excel сформированным через MS Power Query.

**CheckBL.py**

- Проверка закрытия дополнений хранения экспортными дополнениями.

**CheckConosament.py**

- Проверка на пропущенные значения в БД.

------
Database check for missing values during filling, verification of unclosed storage additions.

**init.py**: 

- Initializes application execution.

**App.py**: 

- Performs user authentication and launches database check modules.

**FrameExcel.py** :

- Processes files within consolidated database data; Database represented by Excel file formed via MS Power Query.

**CheckBL.py**:

- Verifies closure of storage additions by export additions.

**CheckConosament.py**: 

- Checks for missing values in the database.
