import os
from dotenv import load_dotenv

load_dotenv()

# Discord configs
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
REBOOT_CHANNEL_ID = os.getenv('REBOOT_CHANNEL_ID')
CHAT_JUKEBOX = os.getenv('CHAT_JUKEBOX')

# Notification channel
NOTIFICATION_CHANNEL_ID = int(os.getenv('NOTIFICATION_CHANNEL_ID', 0))

# MongoDB configs
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'noobsquad_bot')

# YouTube API
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# Last.fm API (https://www.last.fm/api/account/create)
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')

# Twitch API
TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

# Monitor intervals (em segundos)
CHECK_YOUTUBE_INTERVAL = int(os.getenv('CHECK_YOUTUBE_INTERVAL', 300))  # 5 minutos
CHECK_TWITCH_INTERVAL = int(os.getenv('CHECK_TWITCH_INTERVAL', 180))   # 3 minutos

# Sync de membros - Horário de execução (formato HH:MM em UTC)
SYNC_MEMBERS_TIME = os.getenv('SYNC_MEMBERS_TIME', '03:00')  # Padrão: 03:00 UTC

def parse_sync_time(time_str: str) -> tuple[int, int]:
    """Converte string HH:MM para tupla (hora, minuto)"""
    try:
        hour, minute = time_str.split(':')
        return int(hour), int(minute)
    except (ValueError, AttributeError):
        # Fallback para 03:00 se formato inválido
        return 3, 0

SYNC_MEMBERS_HOUR, SYNC_MEMBERS_MINUTE = parse_sync_time(SYNC_MEMBERS_TIME)

# Equalizer presets
EQUALIZER_PRESETS = {
    "padrao": '-filter_complex "equalizer=f=5000:g=2:w=1,equalizer=f=8000:g=2:w=1"',
    "bassboost": '-filter_complex "equalizer=f=60:g=7:w=1:t=h,equalizer=f=120:g=5:w=1:t=h,equalizer=f=250:g=3:w=1:t=h"',
    "pop": '-filter_complex "equalizer=f=80:g=4:w=1:t=h,equalizer=f=8000:g=4:w=1:t=h"',
    "rock": '-filter_complex "equalizer=f=120:g=-2:w=1:t=h,equalizer=f=2000:g=3:w=1:t=h,equalizer=f=5000:g=4:w=1:t=h"'
}
