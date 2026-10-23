# Проверки

```powershell
uv sync --locked
uv run --locked prek install
just test
just cov
just ci
```

`just test` запускает `unittest` через `nose2` в шести процессах.
`just cov` собирает покрытие всех процессов: минимум 86,5% общего покрытия
и 76% ветвей. `just ci` также проверяет форматирование, типы, зависимости,
границы модулей, секреты и GitHub Actions.

Для запуска хуков вручную:

```powershell
uv run --locked prek run --all-files
uv run --locked prek run --all-files --stage pre-push
```

Тесты используют временные базы и локальные HTTP/WebSocket-серверы.
Пользовательские `.env`, `data/tokens.db`, Twitch и Trula не используются.
GitHub Actions запускает тесты на Windows и Ubuntu.

`selftest.bat` запускает тесты последовательно через стандартный `unittest`;
зависимости разработки для него не нужны.

У работающего бота `status.bat` читает локальное состояние, а `check-chat.bat`
отправляет одно сообщение в Twitch и ждёт его EventSub echo.
