# MongoDB
MongoDB должна быть в режиме ReplicaSet для анонимизации 
клиентов в реальном времени, так как этот функционал опирается на ChangeStream API.

# Конфигурация

Создайте файл .env с URI подключения к БД. [Пример конфигурационного файла](./.env.example).

# Установка зависимостей

```bash
npm install
```

# Сборка перед запуском приложений

```bash
npm run build
```

# Запуск генерации покупателей
```bash
npm run start:app
```

# Запуск анонимизации покупателей в реальном времени
```bash
npm run start:sync-real-time
```

Для возобновления Change Stream используется файл resume_token.txt, удалить можно командой

```bash
rm ./resume_token.txt
```

# Запуск анонимизации покупателей в режиме полной синхронизации
```bash
npm run start:sync-full-reindex
```