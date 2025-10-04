## Проведение миграции

Из корневой директории проекта

Обновление моделей:

```bash
alembic revision --autogenerate -m "message"
alembic upgrade head
```

Откат моделей:

```bash
alembic downgrade <target-revision>
```