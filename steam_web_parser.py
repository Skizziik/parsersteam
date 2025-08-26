import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
from tqdm import tqdm
import json

class SteamWebParser:
    def __init__(self, app_id):
        self.app_id = app_id
        self.base_url = f"https://steamcommunity.com/app/{app_id}/reviews/"
        self.db_name = "fm2024_reviews_web.db"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.setup_database()
    
    def setup_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS negative_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT UNIQUE,
                author_name TEXT,
                author_url TEXT,
                review_text TEXT,
                review_url TEXT,
                hours_played TEXT,
                posted_date TEXT,
                is_negative BOOLEAN,
                votes_helpful TEXT,
                language TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"База данных {self.db_name} готова")
    
    def get_reviews_ajax(self, cursor="*", filter_type="negative"):
        """Получаем отзывы через AJAX запрос как это делает сам Steam"""
        
        # Параметры для негативных отзывов
        params = {
            'filter': 'recent',
            'language': 'all',
            'day_range': '9223372036854775807',
            'cursor': cursor,
            'review_type': 'negative',
            'purchase_type': 'all',
            'num_per_page': '10',
            'filter_offtopic_activity': '0'
        }
        
        # URL для AJAX запросов
        url = f"https://steamcommunity.com/app/{self.app_id}/homecontent/"
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"Ошибка HTTP: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            return None
    
    def parse_review_card(self, review_html):
        """Парсим HTML карточки отзыва"""
        soup = BeautifulSoup(review_html, 'html.parser')
        
        review_data = {}
        
        try:
            # ID отзыва
            review_card = soup.find('div', class_='apphub_Card')
            if review_card:
                review_data['review_id'] = review_card.get('id', '').replace('ReviewContent', '')
            
            # Автор
            author_elem = soup.find('div', class_='apphub_CardContentAuthorName')
            if author_elem:
                author_link = author_elem.find('a')
                if author_link:
                    review_data['author_name'] = author_link.text.strip()
                    review_data['author_url'] = author_link.get('href', '')
            
            # Текст отзыва
            review_text_elem = soup.find('div', class_='apphub_CardTextContent')
            if review_text_elem:
                review_data['review_text'] = review_text_elem.text.strip()
            
            # Время в игре
            hours_elem = soup.find('div', class_='hours')
            if hours_elem:
                review_data['hours_played'] = hours_elem.text.strip()
            
            # Дата
            date_elem = soup.find('div', class_='date_posted')
            if date_elem:
                review_data['posted_date'] = date_elem.text.replace('Posted:', '').strip()
            
            # Рекомендация (негативная/позитивная)
            thumb = soup.find('div', class_='thumb')
            if thumb:
                if 'thumbsDown' in str(thumb):
                    review_data['is_negative'] = True
                else:
                    review_data['is_negative'] = False
            
            # Полезность
            helpful_elem = soup.find('div', class_='found_helpful')
            if helpful_elem:
                review_data['votes_helpful'] = helpful_elem.text.strip()
            
            return review_data
            
        except Exception as e:
            print(f"Ошибка парсинга отзыва: {e}")
            return None
    
    def save_review(self, review_data):
        """Сохраняем отзыв в базу данных"""
        if not review_data or not review_data.get('is_negative'):
            return False
            
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        try:
            review_url = f"https://steamcommunity.com/app/{self.app_id}/reviews/{review_data.get('review_id', '')}"
            
            cursor.execute('''
                INSERT OR IGNORE INTO negative_reviews 
                (review_id, author_name, author_url, review_text, review_url, 
                 hours_played, posted_date, is_negative, votes_helpful, language)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                review_data.get('review_id', ''),
                review_data.get('author_name', ''),
                review_data.get('author_url', ''),
                review_data.get('review_text', ''),
                review_url,
                review_data.get('hours_played', ''),
                review_data.get('posted_date', ''),
                review_data.get('is_negative', False),
                review_data.get('votes_helpful', ''),
                'unknown'
            ))
            
            conn.commit()
            success = cursor.rowcount > 0
            conn.close()
            return success
            
        except Exception as e:
            print(f"Ошибка сохранения: {e}")
            conn.close()
            return False
    
    def get_existing_count(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM negative_reviews")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def parse_all_reviews(self):
        print(f"\nWeb Parser для Football Manager 2024")
        print("=" * 60)
        print("Собираем негативные отзывы напрямую с сайта Steam...")
        print("-" * 60)
        
        existing_count = self.get_existing_count()
        if existing_count > 0:
            print(f"В базе уже есть {existing_count} отзывов")
        
        cursor = "*"
        total_saved = 0
        batch_num = 1
        
        pbar = tqdm(
            desc="Негативные отзывы",
            unit=" отзывов",
            ncols=100
        )
        
        try:
            while True:
                print(f"\n[Партия #{batch_num}] Запрашиваем отзывы...")
                
                data = self.get_reviews_ajax(cursor=cursor)
                
                if not data:
                    print("Не удалось получить данные")
                    break
                
                # Проверяем успех
                if not data.get('success'):
                    print("Steam вернул ошибку")
                    break
                
                # Получаем HTML с отзывами
                reviews_html = data.get('html', '')
                
                if not reviews_html:
                    print("Нет больше отзывов")
                    break
                
                # Парсим каждый отзыв
                soup = BeautifulSoup(reviews_html, 'html.parser')
                review_cards = soup.find_all('div', class_='apphub_Card')
                
                print(f"  Найдено {len(review_cards)} отзывов на странице")
                
                saved_in_batch = 0
                for card in review_cards:
                    review_data = self.parse_review_card(str(card))
                    if review_data and review_data.get('is_negative'):
                        if self.save_review(review_data):
                            saved_in_batch += 1
                            total_saved += 1
                
                print(f"  Сохранено {saved_in_batch} негативных отзывов")
                pbar.update(saved_in_batch)
                
                # Получаем следующий cursor
                cursor = data.get('cursor')
                
                if not cursor:
                    print("\nДостигнут конец списка")
                    break
                
                batch_num += 1
                time.sleep(3)  # Пауза между запросами
                
        except KeyboardInterrupt:
            print("\n\nПрервано пользователем")
        
        finally:
            pbar.close()
        
        final_count = self.get_existing_count()
        
        print("\n" + "=" * 60)
        print("ИТОГОВАЯ СТАТИСТИКА:")
        print(f"  Новых отзывов добавлено: {total_saved}")
        print(f"  Всего отзывов в БД: {final_count}")
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
                hours_played,
                author_name,
                posted_date
            FROM negative_reviews
            ORDER BY id DESC
        """)
        
        reviews = cursor.fetchall()
        conn.close()
        
        csv_filename = "fm2024_web_reviews.csv"
        
        with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
            
            writer.writerow(['Ссылка', 'Текст отзыва', 'Часы в игре', 'Автор', 'Дата'])
            
            for url, text, hours, author, date in reviews:
                clean_text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                writer.writerow([url, clean_text, hours, author, date])
        
        print(f"\nЭкспортировано в {csv_filename}")
        print(f"Всего отзывов: {len(reviews)}")
        
        return csv_filename

if __name__ == "__main__":
    FM2024_APP_ID = "2252570"
    
    parser = SteamWebParser(FM2024_APP_ID)
    
    total = parser.parse_all_reviews()
    
    if total > 0:
        parser.export_to_csv()
    else:
        print("\nНе удалось собрать отзывы. Возможные причины:")
        print("1. Steam заблокировал запросы")
        print("2. Изменилась структура сайта")
        print("3. Проблемы с интернетом")