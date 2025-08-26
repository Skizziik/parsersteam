# Steam Review Parser

Парсер для сбора негативных отзывов из Steam.

## Установка

```bash
pip install -r requirements.txt
```

## Использование

```bash
python steam_parser.py
```

Измените ID игры в файле `steam_parser.py` (строка 245):
```python
FM2024_APP_ID = "2252570"  # <-- ID вашей игры
```

## Как найти ID игры

1. Откройте страницу игры в Steam
2. В URL будет число: `https://store.steampowered.com/app/ЧИСЛО/`
3. Это число и есть ID

## Ограничения

- Steam API ограничивает ~10000 отзывов за сессию
- После лимита подождите несколько часов перед повторным запуском

## Результаты

- База данных: `fm2024_reviews.db`
- CSV файл: `fm2024_negative_reviews.csv`