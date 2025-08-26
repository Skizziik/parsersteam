import requests
import sqlite3
import time
from datetime import datetime, timedelta
from tqdm import tqdm
import csv
import json
import os

class UnlimitedSteamParser:
    def __init__(self, app_id):
        self.app_id = app_id
        self.base_url = "https://store.steampowered.com/appreviews/"
        self.db_name = f"reviews_{app_id}.db"
        self.state_file = f"parser_state_{app_id}.json"
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
    
    def save_state(self, cursor, batch_num, total_processed):
        """Сохраняем состояние парсера для продолжения"""
        state = {
            'cursor': cursor,
            'batch_num': batch_num,
            'total_processed': total_processed,
            'last_run': datetime.now().isoformat(),
            'app_id': self.app_id
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        print(f"💾 Состояние сохранено в {self.state_file}")
    
    def load_state(self):
        """Загружаем последнее состояние"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            
            last_run = datetime.fromisoformat(state['last_run'])
            time_passed = datetime.now() - last_run
            
            print(f"📂 Найдено сохранение от {last_run.strftime('%Y-%m-%d %H:%M')}")
            print(f"   Прошло времени: {time_passed}")
            print(f"   Последняя партия: #{state['batch_num']}")
            print(f"   Обработано отзывов: {state['total_processed']}")
            
            return state
        return None
    
    def get_reviews(self, cursor="*", num_per_page=100):
        params = {
            "json": 1,
            "filter": "recent",
            "language": "all",
            "num_per_page": num_per_page,
            "cursor": cursor,
            "purchase_type": "all",
            "review_type": "negative"
        }
        
        try:
            response = requests.get(f"{self.base_url}{self.app_id}", params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 502, 503]:
                print(f"⚠️ Steam вернул ошибку {response.status_code} - достигнут лимит")
                return None
            else:
                print(f"Ошибка: {response.status_code}")
                return None
        except Exception as e:
            print(f"Ошибка запроса: {e}")
            return None
    
    def save_reviews(self, reviews_data):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        saved_count = 0
        for review in reviews_data:
            try:
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
    
    def parse_with_resume(self, max_per_session=9000):
        """Парсим с возможностью продолжения"""
        print(f"\n{'='*60}")
        print(f"🚀 UNLIMITED STEAM PARSER")
        print(f"{'='*60}")
        print(f"Игра ID: {self.app_id}")
        print(f"Лимит за сессию: {max_per_session}")
        
        existing_count = self.get_existing_count()
        print(f"📊 В базе уже: {existing_count} отзывов")
        
        # Загружаем сохранение
        state = self.load_state()
        
        if state and state['cursor'] != "*":
            use_saved = input("\n❓ Продолжить с последнего места? (y/n): ").lower() == 'y'
            
            if use_saved:
                cursor = state['cursor']
                batch_num = state['batch_num']
                total_processed = state['total_processed']
                print("✅ Продолжаем с сохранения...")
            else:
                cursor = "*"
                batch_num = 1
                total_processed = 0
                print("🔄 Начинаем заново...")
        else:
            cursor = "*"
            batch_num = 1
            total_processed = 0
        
        print(f"{'='*60}\n")
        
        session_saved = 0
        errors_count = 0
        max_errors = 5
        
        pbar = tqdm(
            total=max_per_session,
            desc="📥 Негативные отзывы",
            unit=" отзывов"
        )
        
        try:
            while session_saved < max_per_session:
                # Проверка на слишком много ошибок
                if errors_count >= max_errors:
                    print(f"\n⛔ Слишком много ошибок ({max_errors}). Steam заблокировал.")
                    break
                
                print(f"\n[Партия #{batch_num}]", end=" ")
                data = self.get_reviews(cursor=cursor, num_per_page=100)
                
                if not data or not data.get('success'):
                    errors_count += 1
                    print(f"❌ Ошибка #{errors_count}")
                    
                    if errors_count >= 3:
                        print(f"⏸️ Пауза 30 секунд...")
                        time.sleep(30)
                    
                    continue
                
                reviews = data.get('reviews', [])
                
                if not reviews:
                    print("✅ Достигнут конец списка")
                    break
                
                negative_count = sum(1 for r in reviews if not r.get('voted_up', True))
                total_processed += len(reviews)
                
                print(f"Получено {negative_count} негативных")
                
                if negative_count > 0:
                    saved = self.save_reviews(reviews)
                    session_saved += saved
                    pbar.update(saved)
                    errors_count = 0  # Сбрасываем счетчик ошибок
                
                # Получаем новый курсор
                new_cursor = data.get('cursor', None)
                
                if not new_cursor or new_cursor == cursor:
                    print("✅ Больше отзывов нет")
                    break
                
                cursor = new_cursor
                batch_num += 1
                
                # Сохраняем состояние каждые 10 партий
                if batch_num % 10 == 0:
                    self.save_state(cursor, batch_num, total_processed)
                
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n\n⚠️ Прервано пользователем")
        
        finally:
            pbar.close()
            # Сохраняем финальное состояние
            self.save_state(cursor, batch_num, total_processed)
        
        final_count = self.get_existing_count()
        
        print(f"\n{'='*60}")
        print(f"📈 РЕЗУЛЬТАТЫ СЕССИИ:")
        print(f"  Новых отзывов: {session_saved}")
        print(f"  Всего в БД: {final_count}")
        print(f"  Обработано: {total_processed}")
        
        if errors_count >= max_errors:
            print(f"\n⏰ ДОСТИГНУТ ЛИМИТ STEAM")
            print(f"   Подождите 2-3 часа и запустите снова")
            print(f"   Парсер продолжит с места остановки")
        
        print(f"{'='*60}\n")
        
        return session_saved
    
    def export_to_csv(self):
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
        
        csv_filename = f"negative_reviews_{self.app_id}.csv"
        
        with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
            
            writer.writerow(['Ссылка на отзыв', 'Текст отзыва', 'Часы в игре'])
            
            for url, text, hours in reviews:
                clean_text = text.replace('\n', ' ').replace('\r', ' ')
                writer.writerow([url, clean_text, f"{hours} ч"])
        
        print(f"✅ Экспортировано в {csv_filename}")
        print(f"   Всего отзывов: {len(reviews)}")
        
        return csv_filename

def main():
    print("\n" + "="*60)
    print("UNLIMITED STEAM REVIEW PARSER")
    print("Парсер с автоматическим обходом лимитов")
    print("="*60)
    
    # Запрашиваем ID игры
    game_input = input("\nВведите ID игры или URL Steam: ").strip()
    
    # Извлекаем ID из URL если нужно
    if 'steam' in game_input.lower():
        import re
        match = re.search(r'/app/(\d+)', game_input)
        if match:
            app_id = match.group(1)
        else:
            print("❌ Не удалось извлечь ID из URL")
            return
    else:
        app_id = game_input
    
    print(f"\n🎮 ID игры: {app_id}")
    
    parser = UnlimitedSteamParser(app_id)
    
    while True:
        # Парсим до лимита
        saved = parser.parse_with_resume(max_per_session=9000)
        
        if saved == 0:
            print("\n💤 Нет новых отзывов или достигнут лимит")
            
            total = parser.get_existing_count()
            if total > 0:
                export = input("\n📊 Экспортировать в CSV? (y/n): ").lower() == 'y'
                if export:
                    parser.export_to_csv()
            break
        
        # Спрашиваем продолжить ли
        continue_parse = input("\n🔄 Продолжить сбор? (y/n): ").lower() == 'y'
        
        if not continue_parse:
            total = parser.get_existing_count()
            if total > 0:
                export = input("\n📊 Экспортировать в CSV? (y/n): ").lower() == 'y'
                if export:
                    parser.export_to_csv()
            break
        
        print("\n⏳ Ждем 1 минуту для обхода лимита...")
        print("   (Можете прервать Ctrl+C и запустить позже)")
        
        try:
            for i in range(60, 0, -1):
                print(f"\r⏱️ Осталось {i} секунд...", end="")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n⚠️ Ожидание прервано")
            break
    
    print("\n✨ Готово!")

if __name__ == "__main__":
    main()