import sqlite3
import pandas as pd
import json

class ReviewAnalyzer:
    def __init__(self, db_name="fm2024_reviews.db"):
        self.db_name = db_name
    
    def prepare_for_gpt(self, limit=100):
        conn = sqlite3.connect(self.db_name)
        
        query = """
        SELECT 
            review_text,
            hours_played,
            votes_helpful
        FROM negative_reviews
        WHERE LENGTH(review_text) > 50
        ORDER BY votes_helpful DESC, created_timestamp DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        
        reviews_for_analysis = []
        for _, row in df.iterrows():
            reviews_for_analysis.append({
                "text": row['review_text'],
                "hours": round(row['hours_played'], 1),
                "helpful_votes": row['votes_helpful']
            })
        
        output_file = "reviews_for_gpt_analysis.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reviews_for_analysis, f, ensure_ascii=False, indent=2)
        
        print(f"Подготовлено {len(reviews_for_analysis)} отзывов для анализа")
        print(f"Сохранено в: {output_file}")
        
        self.create_gpt_prompt(reviews_for_analysis)
        
        return reviews_for_analysis
    
    def create_gpt_prompt(self, reviews):
        prompt = """Проанализируй негативные отзывы на игру Football Manager 2024 и выдели:

1. ТОП-10 самых частых жалоб/проблем (с процентом упоминаний)
2. Основные категории проблем (геймплей, технические, интерфейс и т.д.)
3. Критические проблемы, которые заставляют игроков бросать игру
4. Проблемы новичков vs опытных игроков (на основе часов в игре)

Отзывы для анализа:
"""
        
        for i, review in enumerate(reviews[:50], 1):
            prompt += f"\n[Отзыв {i}, {review['hours']}ч в игре]: {review['text'][:500]}\n"
        
        with open("gpt_analysis_prompt.txt", 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print("\nПромпт для GPT сохранен в: gpt_analysis_prompt.txt")
        print("\nВы можете скопировать его и вставить в ChatGPT для анализа")
    
    def get_common_words(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute("SELECT review_text FROM negative_reviews")
        all_texts = cursor.fetchall()
        conn.close()
        
        common_issues = {
            'интерфейс': 0,
            'баг': 0,
            'вылет': 0,
            'оптимизация': 0,
            'fps': 0,
            'тормоз': 0,
            'match engine': 0,
            'движок': 0,
            'тактика': 0,
            'перевод': 0,
            'цена': 0,
            'дорого': 0,
            'fm23': 0,
            'обновление': 0
        }
        
        for text in all_texts:
            text_lower = text[0].lower()
            for keyword in common_issues:
                if keyword in text_lower:
                    common_issues[keyword] += 1
        
        sorted_issues = sorted(common_issues.items(), key=lambda x: x[1], reverse=True)
        
        print("\n=== ЧАСТЫЕ УПОМИНАНИЯ В НЕГАТИВНЫХ ОТЗЫВАХ ===")
        for keyword, count in sorted_issues[:10]:
            if count > 0:
                print(f"{keyword}: {count} упоминаний")

if __name__ == "__main__":
    analyzer = ReviewAnalyzer()
    
    analyzer.prepare_for_gpt(limit=100)
    
    analyzer.get_common_words()