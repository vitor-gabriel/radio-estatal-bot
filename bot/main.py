import discord
import logging
from datetime import datetime
from discord.ext import commands

from config.settings import DISCORD_TOKEN, REBOOT_CHANNEL_ID, NOTIFICATION_CHANNEL_ID
from db.database import db
from bot.commands import MusicCommands, HelpCommands
from bot.commands_monitor import MonitorCommands
from bot.commands_ranking import RankingCommands
from bot.scheduler import MonitorScheduler
from bot.cogs_activity import ActivityTracker

# --- CONFIGURAÇÃO DE LOGGING ---
log_filename = datetime.now().strftime("bot_log_%Y-%m-%d.log")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

if not DISCORD_TOKEN:
    logging.error("Token do bot não encontrado no arquivo .env")
    raise ValueError("Token do bot não encontrado no arquivo .env")
if not REBOOT_CHANNEL_ID:
    logging.error("ID do canal de reboot não encontrado no arquivo .env")
    raise ValueError("ID do canal de reboot não encontrado no arquivo .env")

# --- CONFIGURAÇÃO DAS INTENTS E BOT ---
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.presences = True
intents.members = True  # Necessário para acessar guild.members
bot = commands.Bot(
    command_prefix="!", intents=intents, heartbeat_timeout=60.0, help_command=None
)

# Criar instância do scheduler
scheduler = MonitorScheduler(bot)
_startup_notice_sent = False


# --- INICIALIZAÇÃO DO BANCO DE DADOS ---
def setup_database():
    """Inicializa a conexão com o banco de dados"""
    try:
        db.connect()
        db.initialize_collections()  # Inicializa as coleções e índices
        logging.info("Banco de dados inicializado com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao inicializar banco de dados: {e}")
        raise e


# --- REGISTRAR OS COGS ---
async def setup_cogs():
    """Configura os Cogs do bot"""
    # Garante que o banco de dados está conectado antes de registrar os Cogs
    setup_database()

    await bot.add_cog(MusicCommands(bot))
    await bot.add_cog(MonitorCommands(bot))
    await bot.add_cog(RankingCommands(bot))
    await bot.add_cog(HelpCommands(bot))
    await bot.add_cog(ActivityTracker(bot))

    # Iniciar o scheduler de monitoramento
    await scheduler.start()

    logging.info("Cogs registrados com sucesso!")


@bot.event
async def on_ready():
    """Evento disparado quando o bot está pronto e conectado"""
    global _startup_notice_sent
    await setup_cogs()  # Registra os Cogs quando o bot iniciar
    logging.info(f"Bot conectado como {bot.user.name}")

    try:
        if _startup_notice_sent:
            return

        # Prioriza canal de notificação do .env, com fallback para REBOOT_CHANNEL_ID.
        channel_id = NOTIFICATION_CHANNEL_ID
        if not channel_id and REBOOT_CHANNEL_ID:
            try:
                channel_id = int(REBOOT_CHANNEL_ID)
            except (TypeError, ValueError):
                channel_id = 0

        if channel_id:
            reboot_channel = bot.get_channel(channel_id)
            if reboot_channel is None:
                try:
                    reboot_channel = await bot.fetch_channel(channel_id)
                except Exception as e:
                    logging.error(f"Erro ao localizar canal de notificação {channel_id}: {e}")
                    reboot_channel = None

            if reboot_channel:
                await reboot_channel.send("✅ Servidor carregado. Bot pronto para reproduzir músicas!")
                _startup_notice_sent = True
            else:
                logging.error(f"Canal de notificação {channel_id} não encontrado.")
        else:
            logging.warning("Nenhum canal de notificação configurado no .env.")
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem de reboot: {e}")


@bot.event
async def on_command_error(ctx, error):
    """Trata erros de comando."""
    if isinstance(error, commands.CommandNotFound):
        await ctx.send(
            f"🤔 Comando não encontrado. Digite `!ajuda` para ver a lista de comandos disponíveis."
        )
    else:
        # Para outros erros, loga e informa o usuário
        logging.error(f"Ocorreu um erro no comando '{ctx.command}': {error}")
        await ctx.send(
            "❌ Ocorreu um erro ao processar o comando. Por favor, tente novamente."
        )


@bot.event
async def on_error(event, *args, **kwargs):
    """Tratamento global de erros"""
    logging.error(f"Erro no evento {event}: ", exc_info=True)


# Cleanup quando o bot for desligado
def cleanup():
    """Limpa recursos ao desligar o bot"""
    try:
        scheduler.stop()  # Para as tasks de monitoramento
        db.close()
        logging.info("Recursos do bot liberados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao liberar recursos: {e}")


# Iniciar o bot
try:
    bot.run(DISCORD_TOKEN)
except Exception as e:
    logging.error(f"Erro ao iniciar o bot: {e}")
finally:
    cleanup()
