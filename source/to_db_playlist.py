from db_connection import get_db_connection
import json
import psycopg2
import os
import shutil
import datetime

# Параметры подключения к PostgreSQL
DB_NAME = "music_data"
DB_USER = "postgres"
DB_PASSWORD = "dslf;sdjfk25089FDAJDLKF352*"
DB_HOST = "localhost"

def load_to_db():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    versions_folder = os.path.join(project_root, "versions")
    os.makedirs(versions_folder, exist_ok=True)

    last_file_path = os.path.join(project_root, "last_file.txt")
    with open(last_file_path, "r", encoding="utf-8") as f:
        json_files = [line.strip() for line in f.readlines()]

    conn = get_db_connection()

    cursor = conn.cursor()

    for json_file_path in json_files:
        print(f"Обрабатывается файл: {json_file_path}")
        with open(json_file_path, "r", encoding="utf-8") as f:
            tracks = json.load(f)

        file_name = os.path.basename(json_file_path)
        user_id, playlist_title = file_name.split('_', 1)
        playlist_title = playlist_title.replace('_tracks.json', '').replace('_', ' ')

        cursor.execute("SELECT id FROM users WHERE user_id = %s", (user_id,))
        user = cursor.fetchone()
        if user is None:
            cursor.execute("INSERT INTO users (user_id) VALUES (%s) RETURNING id", (user_id,))
            user_id_in_db = cursor.fetchone()[0]
            print(f"Добавлен новый пользователь: {user_id}")
        else:
            user_id_in_db = user[0]
            print(f"Пользователь найден: {user_id}")

        cursor.execute("SELECT id FROM playlists WHERE title = %s AND user_id = %s", (playlist_title, user_id_in_db))
        playlist = cursor.fetchone()
        if playlist is None:
            cursor.execute("""
                INSERT INTO playlists (title, user_id) VALUES (%s, %s) RETURNING id
            """, (playlist_title, user_id_in_db))
            playlist_id = cursor.fetchone()[0]
            print(f"Добавлен новый плейлист: {playlist_title}")
        else:
            playlist_id = playlist[0]
            print(f"Плейлист найден: {playlist_title}")

        # Сравнение треков
        cursor.execute("""
            SELECT title, array_agg(name ORDER BY name) AS artists, album, duration
            FROM tracks
            LEFT JOIN artists ON tracks.id = artists.track_id
            WHERE tracks.playlist_id = %s
            GROUP BY tracks.id, title, album, duration
        """, (playlist_id,))
        existing_tracks = {
            (row[0], tuple(sorted(row[1])), row[2], row[3]) for row in cursor.fetchall()
        }

        new_tracks = {
            (track["title"], tuple(sorted(track["artists"])), track["album"], track["duration"]) for track in tracks
        }

        tracks_to_add = new_tracks - existing_tracks
        tracks_to_delete = existing_tracks - new_tracks

        # Отладочная информация
        print(f"Текущие треки в базе: {existing_tracks}")
        print(f"Новые треки из JSON: {new_tracks}")
        print(f"Треки для добавления: {tracks_to_add}")
        print(f"Треки для удаления: {tracks_to_delete}")

        # Добавление новых треков
        for track in tracks:
            track_key = (track["title"], tuple(sorted(track["artists"])), track["album"], track["duration"])
            if track_key in tracks_to_add:
                cursor.execute("""
                    INSERT INTO tracks (title, duration, playlist, album, year, playlist_id) 
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    track["title"],
                    track["duration"],
                    playlist_title,
                    track["album"],
                    track["year"],
                    playlist_id
                ))
                track_id = cursor.fetchone()[0]
                print(f"Добавлен новый трек: {track['title']} - {track['album']}")

                for artist in track["artists"]:
                    cursor.execute("""
                        INSERT INTO artists (name, track_id, playlist_id) VALUES (%s, %s, %s)
                    """, (artist, track_id, playlist_id))
                    print(f"Добавлен исполнитель: {artist}")

                # Фиксация добавления трека
                cursor.execute("""
                    INSERT INTO playlist_versions (playlist_id, change_type, track_title) 
                    VALUES (%s, 'added', %s)
                """, (playlist_id, track["title"]))

        # Удаление старых треков
        for title, artists, album, duration in tracks_to_delete:
            cursor.execute("""
                DELETE FROM artists
                WHERE track_id = (
                    SELECT id FROM tracks
                    WHERE title = %s AND album = %s AND duration = %s AND playlist_id = %s
                )
            """, (title, album, duration, playlist_id))
            print(f"Удалены исполнители для трека: {title} - {album}")

            cursor.execute("""
                DELETE FROM tracks
                WHERE title = %s AND album = %s AND duration = %s AND playlist_id = %s
            """, (title, album, duration, playlist_id))
            print(f"Удалён трек: {title} - {album}")

            # Фиксация удаления трека
            cursor.execute("""
                INSERT INTO playlist_versions (playlist_id, change_type, track_title) 
                VALUES (%s, 'removed', %s)
            """, (playlist_id, title))

        # Фиксация загрузки оригинального плейлиста
        if not existing_tracks:
            cursor.execute("""
                INSERT INTO playlist_versions (playlist_id, change_type, track_title) 
                VALUES (%s, 'loaded', NULL)
            """, (playlist_id,))

    # Очистка last_file.txt после обработки
    open(last_file_path, "w").close()
    conn.commit()
    cursor.close()
    conn.close()
    print("Все плейлисты успешно загружены в базу!")
