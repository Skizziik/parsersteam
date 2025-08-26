import sys
from steam_parser import SteamReviewParser

def get_game_id_from_url(url):
    """Извлекает ID игры из URL Steam"""
    import re
    match = re.search(r'/app/(\d+)', url)
    if match:
        return match.group(1)
    return None

def main():
    print("\n" + "="*60)
    print("УНИВЕРСАЛЬНЫЙ ПАРСЕР STEAM ОТЗЫВОВ")
    print("="*60)
    
    # Популярные игры
    games = {
        '1': ('2252570', 'Football Manager 2024'),
        '2': ('2195250', 'EA FC 24'),
        '3': ('730', 'Counter-Strike 2'),
        '4': ('570', 'Dota 2'),
        '5': ('578080', 'PUBG'),
        '6': ('271590', 'GTA V'),
        '7': ('1172470', 'Apex Legends'),
        '8': ('1938090', 'Call of Duty MW III'),
    }
    
    print("\nВыберите игру:")
    for key, (app_id, name) in games.items():
        print(f"{key}. {name}")
    print("\n0. Ввести свой ID или URL игры")
    
    choice = input("\nВаш выбор: ").strip()
    
    if choice == '0':
        custom = input("Введите ID игры или URL страницы Steam: ").strip()
        
        # Проверяем, это URL или ID
        if 'steam' in custom.lower():
            app_id = get_game_id_from_url(custom)
            if not app_id:
                print("Не удалось извлечь ID из URL")
                return
        else:
            app_id = custom.replace('/', '').strip()
        
        game_name = f"Game_{app_id}"
    elif choice in games:
        app_id, game_name = games[choice]
    else:
        print("Неверный выбор")
        return
    
    print(f"\n🎮 Парсим игру: {game_name} (ID: {app_id})")
    print("-" * 60)
    
    # Меняем имя БД для каждой игры
    parser = SteamReviewParser(app_id)
    parser.db_name = f"reviews_{app_id}.db"
    parser.setup_database()
    
    total = parser.parse_all_negative_reviews()
    
    if total > 0 or parser.get_existing_count() > 0:
        # Экспортируем с именем игры
        csv_file = f"negative_reviews_{app_id}.csv"
        parser.export_to_csv()
        
        import os
        if os.path.exists("fm2024_negative_reviews.csv"):
            os.rename("fm2024_negative_reviews.csv", csv_file)
            print(f"\n✅ CSV файл: {csv_file}")

if __name__ == "__main__":
    main()