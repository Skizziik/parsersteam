import requests
import sqlite3
import json
import time
from datetime import datetime
import re
from tqdm import tqdm
import sys

class SteamReviewParser:
    def __init__(self, app_id):
        self.app_id = app_id
        self.base_url = "https://store.steampowered.com/appreviews/"
        self.db_name = "fm2024_reviews.db"
        self.setup_database()
        
    def setup_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS negative_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT UNIQUE,
                author_steamid TEXT,
                review_text TEXT,
                review_url TEXT,
                hours_played REAL,
                posted_date TEXT,
                voted_up BOOLEAN,
                votes_helpful INTEGER,
                votes_funny INTEGER,
                language TEXT,
                created_timestamp INTEGER
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"База данных {self.db_name} готова")
    
    def get_reviews(self, cursor="*", num_per_page=100):
        params = {
            "json": 1,
            "filter": "recent",
            "language": "all",
            "num_per_page": num_per_page,
            "cursor": cursor,
            "purchase_type": "all",
            "review_type": "negative"  # ТОЛЬКО НЕГАТИВНЫЕ!
        }
        
        response = requests.get(f"{self.base_url}{self.app_id}", params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Ошибка при получении данных: {response.status_code}")
            return None
    
    def save_reviews(self, reviews_data):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        saved_count = 0
        for review in reviews_data:
            try:
                # Проверяем что это негативный отзыв
                if review.get('voted_up', True):
                    continue
                    
                review_id = review.get('recommendationid')
                author_steamid = review.get('author', {}).get('steamid')
                review_text = review.get('review', '')
                
                review_url = f"https://steamcommunity.com/profiles/{author_steamid}/recommended/{self.app_id}"
                
                hours_played = review.get('author', {}).get('playtime_forever', 0) / 60
                
                timestamp = review.get('timestamp_created', 0)
                posted_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') if timestamp else None
                
                voted_up = review.get('voted_up', False)
                votes_helpful = review.get('votes_up', 0)
                votes_funny = review.get('votes_funny', 0)
                language = review.get('language', 'unknown')
                
                cursor.execute('''
                    INSERT OR IGNORE INTO negative_reviews 
                    (review_id, author_steamid, review_text, review_url, hours_played, 
                     posted_date, voted_up, votes_helpful, votes_funny, language, created_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (review_id, author_steamid, review_text, review_url, hours_played,
                      posted_date, voted_up, votes_helpful, votes_funny, language, timestamp))
                
                if cursor.rowcount > 0:
                    saved_count += 1
                    
            except Exception as e:
                print(f"Ошибка при сохранении отзыва: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        return saved_count
    
    def get_existing_count(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM negative_reviews")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def parse_all_negative_reviews(self, max_reviews=None):
        print(f"\nSteam Review Parser для Football Manager 2024")
        print("=" * 60)
        
        existing_count = self.get_existing_count()
        if existing_count > 0:
            print(f"В базе данных уже есть {existing_count} отзывов")
            print("-" * 60)
        
        print(f"Цель: собрать ВСЕ негативные отзывы")
        print("-" * 60)
        
        cursor = "*"
        total_saved = 0
        total_processed = 0
        batch_num = 1
        empty_batches = 0
        
        pbar = tqdm(
            total=max_reviews if max_reviews else None,
            desc="Негативные отзывы",
            unit=" отзывов",
            ncols=100,
            bar_format='{desc}: {n_fmt}/{total_fmt} [{bar}] {percentage:3.0f}% | Найдено: {postfix[0]} | В БД: {postfix[1]}',
            postfix=[0, existing_count]
        )
        
        try:
            while True:
                if max_reviews and total_saved >= max_reviews:
                    break
                
                print(f"\nЗапрос партии #{batch_num}...")
                data = self.get_reviews(cursor=cursor, num_per_page=100)
                
                if not data or not data.get('success'):
                    print("Не удалось получить данные от Steam")
                    break
                
                reviews = data.get('reviews', [])
                
                if not reviews:
                    print("Больше отзывов нет")
                    break
                
                # Считаем негативные
                negative_count = sum(1 for r in reviews if not r.get('voted_up', True))
                total_processed += len(reviews)
                
                print(f"  Получено {len(reviews)} отзывов, из них {negative_count} негативных")
                
                if negative_count > 0:
                    saved = self.save_reviews(reviews)
                    total_saved += saved
                    empty_batches = 0
                    print(f"  Сохранено в БД: {saved}")
                else:
                    empty_batches += 1
                    if empty_batches > 5:
                        print("Слишком много пустых партий, останавливаем")
                        break
                
                current_db_count = existing_count + total_saved
                pbar.postfix = [total_processed, current_db_count]
                pbar.update(saved if negative_count > 0 else 0)
                
                cursor = data.get('cursor', None)
                
                if not cursor:
                    print("Достигнут конец списка")
                    break
                
                batch_num += 1
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n\nСбор прерван пользователем")
        
        finally:
            pbar.close()
        
        final_count = self.get_existing_count()
        
        print("\n" + "=" * 60)
        print("ИТОГОВАЯ СТАТИСТИКА:")
        print(f"  Новых отзывов добавлено: {total_saved}")
        print(f"  Всего отзывов в БД: {final_count}")
        print(f"  Всего просмотрено: {total_processed}")
        print(f"  База данных: {self.db_name}")
        print("=" * 60)
        
        return total_saved
    
    def export_to_csv(self):
        import csv
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                review_url,
                review_text,
                ROUND(hours_played, 1)
            FROM negative_reviews
            ORDER BY votes_helpful DESC, created_timestamp DESC
        """)
        
        reviews = cursor.fetchall()
        conn.close()
        
        csv_filename = "fm2024_negative_reviews.csv"
        
        with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
            
            writer.writerow(['Ссылка на отзыв', 'Текст отзыва', 'Часы в игре'])
            
            for url, text, hours in reviews:
                clean_text = text.replace('\n', ' ').replace('\r', ' ')
                writer.writerow([url, clean_text, f"{hours} ч"])
        
        print(f"\nЭкспортировано в {csv_filename}")
        print(f"   Формат: Ссылка на отзыв ; Текст отзыва ; Часы в игре")
        print(f"   Всего отзывов: {len(reviews)}")
        
        return csv_filename

if __name__ == "__main__":
    FM2024_APP_ID = "1142710"
    
    parser = SteamReviewParser(FM2024_APP_ID)
    
    total = parser.parse_all_negative_reviews()
    
    if total > 0:
        parser.export_to_csv()