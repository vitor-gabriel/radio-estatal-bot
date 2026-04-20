import os

# Funções utilitárias compartilhadas
def validar_canal(ctx):
    """Valida se o comando foi enviado no canal de texto permitido."""
    ALLOWED_CHANNEL_ID = int(os.getenv('CHAT_JUKEBOX', 0))
    if ctx.channel.id != ALLOWED_CHANNEL_ID:
        return False
    return True

# Variáveis globais compartilhadas
play_queue = {}
last_played_info = {}
autoplay_enabled = {}
active_preset = {}
