from pymongo import MongoClient
from datetime import datetime, UTC
import logging
from config.settings import MONGODB_URI, DATABASE_NAME
from .models import (
    UserProfile,
    Song,
    MusicPreference,
    MonitoredChannel,
    Activity,
    ActivityHistory,
)
from typing import List, Optional


class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.user_profiles = None  # Referência para a coleção user_profiles
        self.monitored_channels = None  # Nova coleção para canais monitorados
        self.activities = None
        self.activity_history = None

    def connect(self):
        """Estabelece conexão com o MongoDB"""
        try:
            self.client = MongoClient(MONGODB_URI)
            self.db = self.client[DATABASE_NAME]
            # Inicializa as coleções
            self.user_profiles = self.db.user_profiles
            self.monitored_channels = self.db.monitored_channels
            self.activities = self.db.activities
            self.activity_history = self.db.activity_history
            # Testa a conexão
            self.client.server_info()
            logging.info("Conexão com MongoDB estabelecida com sucesso!")
        except Exception as e:
            logging.error(f"Erro ao conectar ao MongoDB: {str(e)}")
            raise e

    def close(self):
        """Fecha a conexão com o MongoDB"""
        if self.client:
            self.client.close()
            logging.info("Conexão com MongoDB fechada.")

    def _ensure_connected(self) -> bool:
        """Garante que a conexão/coleções estejam prontas antes de operar."""
        if self.db is not None and self.user_profiles is not None:
            return True
        try:
            self.connect()
            return self.db is not None and self.user_profiles is not None
        except Exception as e:
            logging.error(f"Erro ao garantir conexão MongoDB: {e}")
            return False

    async def create_user_profile(
        self, discord_id: str, username: str, display_name: str = None
    ):
        """Cria um novo perfil de usuário se não existir"""
        try:
            if not self._ensure_connected():
                return False

            discord_id = str(discord_id)
            username = str(username or discord_id)

            # Criamos um novo perfil - os valores padrão serão inicializados pelo __post_init__
            profile = UserProfile(
                discord_id=discord_id,
                username=username,
                display_name=display_name or username,
            )

            result = self.db.user_profiles.update_one(
                {"discord_id": discord_id},
                {"$setOnInsert": profile.to_dict()},
                upsert=True,
            )
            if result.upserted_id:
                logging.info(
                    f"Novo perfil criado para usuário {username} (display: {display_name or username})"
                )
            return True
        except Exception as e:
            logging.error(f"Erro ao criar perfil do usuário: {str(e)}")
            return False

    async def add_music_preference(self, discord_id: str, name: str, pref_type: str):
        """Adiciona ou atualiza uma preferência musical"""
        try:
            now = datetime.now(UTC)
            result = self.db.user_profiles.update_one(
                {
                    "discord_id": discord_id,
                    "music_preferences": {
                        "$not": {"$elemMatch": {"name": name, "type": pref_type}}
                    },
                },
                {
                    "$push": {
                        "music_preferences": {
                            "name": name,
                            "type": pref_type,
                            "count": 1,
                            "last_updated": now,
                        }
                    }
                },
            )

            if result.modified_count == 0:
                # Preferência já existe, incrementa o contador
                self.db.user_profiles.update_one(
                    {
                        "discord_id": discord_id,
                        "music_preferences.name": name,
                        "music_preferences.type": pref_type,
                    },
                    {
                        "$inc": {"music_preferences.$.count": 1},
                        "$set": {"music_preferences.$.last_updated": now},
                    },
                )

            logging.info(
                f"Preferência musical {name} ({pref_type}) atualizada para usuário {discord_id}"
            )
            return True
        except Exception as e:
            logging.error(f"Erro ao adicionar preferência musical: {str(e)}")
            return False

    async def add_to_music_history(self, discord_id: str, song_info: dict):
        """Adiciona uma música ao histórico do usuário e atualiza preferências"""
        try:
            if not self._ensure_connected():
                return False

            discord_id = str(discord_id)
            if not isinstance(song_info, dict):
                logging.warning("add_to_music_history recebeu song_info inválido")
                return False

            title = (song_info.get("title") or "").strip()
            url = (song_info.get("url") or "").strip()
            if not title or not url:
                logging.warning(
                    f"add_to_music_history ignorado: title/url inválidos para usuário {discord_id}"
                )
                return False

            song = Song(
                title=title,
                url=url,
                played_at=datetime.now(UTC),
                artist=song_info.get("artist"),
                genre=song_info.get("genre"),
            )

            # Adiciona ao histórico com upsert para nunca perder o registro.
            now = datetime.now(UTC)
            result = self.db.user_profiles.update_one(
                {"discord_id": discord_id},
                {
                    "$setOnInsert": {
                        "discord_id": discord_id,
                        "username": str(song_info.get("username") or discord_id),
                        "display_name": str(song_info.get("display_name") or song_info.get("username") or discord_id),
                        "music_preferences": [],
                        "created_at": now,
                    },
                    "$push": {
                        "music_history": {
                            "$each": [vars(song)],
                            "$slice": -100,  # Mantém apenas as últimas 100 músicas
                        }
                    },
                },
                upsert=True,
            )

            # Atualiza preferências se houver artista ou gênero
            if song.artist:
                await self.add_music_preference(discord_id, song.artist, "artist")
            if song.genre:
                await self.add_music_preference(discord_id, song.genre, "genre")

            success = bool(result.modified_count > 0 or result.upserted_id is not None)
            logging.info(
                "Música adicionada ao histórico do usuário %s "
                "(matched=%s, modified=%s, upserted=%s)",
                discord_id,
                result.matched_count,
                result.modified_count,
                bool(result.upserted_id),
            )
            return success
        except Exception as e:
            logging.error(f"Erro ao adicionar música ao histórico: {str(e)}")
            return False

    async def get_top_preferences(
        self, discord_id: str, pref_type: Optional[str] = None, limit: int = 5
    ) -> List[MusicPreference]:
        """Retorna as principais preferências musicais do usuário"""
        try:
            user = await self.get_user_profile(discord_id)
            if not user:
                return []

            prefs = user.music_preferences
            if pref_type:
                prefs = [p for p in prefs if p.type == pref_type]

            return sorted(prefs, key=lambda x: x.count, reverse=True)[:limit]
        except Exception as e:
            logging.error(f"Erro ao recuperar preferências musicais: {str(e)}")
            return []

    async def get_user_profile(self, discord_id: str) -> UserProfile:
        """Recupera o perfil do usuário"""
        try:
            if not self._ensure_connected():
                return None
            data = self.db.user_profiles.find_one({"discord_id": discord_id})
            if data:
                return UserProfile.from_dict(data)
            return None
        except Exception as e:
            logging.error(f"Erro ao recuperar perfil do usuário: {str(e)}")
            return None

    async def add_monitored_channel(
        self, discord_id: str, channel: MonitoredChannel
    ) -> bool:
        """Adiciona um canal para monitoramento na coleção `monitored_channels`.

        Se o documento do canal já existir, apenas adiciona o usuário à lista de subscribers
        (se ainda não for assinante). Caso contrário cria o documento com o primeiro assinante.
        """
        try:
            # Tenta encontrar um documento existente pelo platform+channel_id
            existing = self.monitored_channels.find_one(
                {"platform": channel.platform, "channel_id": channel.channel_id}
            )

            if existing:
                # Se o usuário já é subscriber, não faz nada
                if str(discord_id) in existing.get("subscribers", []):
                    return False
                # Adiciona o subscriber ao documento do canal
                result = self.monitored_channels.update_one(
                    {"_id": existing["_id"]},
                    {"$addToSet": {"subscribers": str(discord_id)}},
                )
                return result.modified_count > 0

            # Cria novo documento de canal
            channel.subscribers = [str(discord_id)]
            result = self.monitored_channels.insert_one(channel.to_dict())
            return result.acknowledged
        except Exception as e:
            logging.error(f"Erro ao adicionar canal monitorado: {str(e)}")
            return False

    async def remove_monitored_channel(
        self, discord_id: str, platform: str, channel_name: str
    ) -> bool:
        """Remove um canal do monitoramento para um usuário específico.

        Se o usuário for o único subscriber, o documento do canal será removido.
        Caso contrário, apenas será removido da lista de subscribers.
        """
        try:
            # Tenta encontrar o canal pelo platform e channel_name
            doc = self.monitored_channels.find_one(
                {"platform": platform, "channel_name": channel_name}
            )
            if not doc:
                return False

            # Se o usuário não está na lista de subscribers
            if str(discord_id) not in doc.get("subscribers", []):
                return False

            # Se há mais de um subscriber, apenas remove o usuário
            if len(doc.get("subscribers", [])) > 1:
                result = self.monitored_channels.update_one(
                    {"_id": doc["_id"]}, {"$pull": {"subscribers": str(discord_id)}}
                )
                return result.modified_count > 0

            # Caso contrário, remove o documento do canal por completo
            result = self.monitored_channels.delete_one({"_id": doc["_id"]})
            return result.deleted_count > 0
        except Exception as e:
            logging.error(f"Erro ao remover canal monitorado: {str(e)}")
            return False

    async def update_channel_last_video(
        self, discord_id: str, channel_id: str, video_id: str
    ) -> bool:
        """Atualiza o ID do último vídeo de um canal do YouTube (baseado na coleção de canais monitorados)"""
        try:
            result = self.monitored_channels.update_one(
                {"channel_id": channel_id, "platform": "youtube"},
                {"$set": {"last_video_id": video_id}},
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"Erro ao atualizar último vídeo: {str(e)}")
            return False

    async def update_channel_stream_status(
        self, discord_id: str, channel_id: str, stream_id: str
    ) -> bool:
        """Atualiza o status de live de um canal da Twitch (na coleção de canais monitorados)"""
        try:
            result = self.monitored_channels.update_one(
                {"channel_id": channel_id, "platform": "twitch"},
                {"$set": {"last_stream_id": stream_id, "is_live": bool(stream_id)}},
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"Erro ao atualizar status de live: {str(e)}")
            return False

    async def get_all_monitored_channels(self) -> List[MonitoredChannel]:
        """Retorna todos os canais monitorados (cada documento contém subscribers)."""
        try:
            channels = []
            cursor = self.monitored_channels.find({})
            for doc in cursor:
                channels.append(MonitoredChannel.from_dict(doc))
            return channels
        except Exception as e:
            logging.error(f"Erro ao buscar canais monitorados: {str(e)}")
            return []

    async def get_profiles_with_monitored_channels(self) -> List[UserProfile]:
        """Compat layer: Retorna perfis dos usuários que têm ao menos um canal monitorado.

        Este método preserva a interface esperada pelo scheduler e outros módulos:
        - Antes: retornava UserProfile com `monitored_channels` embutidos.
        - Agora: retornará UserProfile com monitored_channels preenchidos a partir da coleção separada.
        """
        try:
            profiles = []
            # Obter todos os canais monitorados e agrupar por subscriber
            cursor = self.monitored_channels.find({})

            # mapa de discord_id -> list[MonitoredChannel]
            grouped = {}
            for doc in cursor:
                channel = MonitoredChannel.from_dict(doc)
                for sub in doc.get("subscribers", []):
                    grouped.setdefault(str(sub), []).append(channel)

            # Para cada subscriber, buscar o perfil e anexar a lista de canais
            for discord_id, channels in grouped.items():
                user_doc = self.user_profiles.find_one({"discord_id": discord_id})
                if user_doc:
                    profile = UserProfile.from_dict(user_doc)
                else:
                    # Cria um perfil mínimo caso não exista
                    profile = UserProfile(
                        discord_id=discord_id, username=str(discord_id)
                    )
                # Atribui a lista de canais para compatibilidade com o restante do código
                # Observação: UserProfile não tem mais o campo monitored_channels, mas o resto do
                # código espera que o objeto retornado tenha esse atributo; adicionamos dinamicamente.
                setattr(profile, "monitored_channels", channels)
                profiles.append(profile)

            return profiles
        except Exception as e:
            logging.error(f"Erro ao buscar perfis com canais monitorados: {str(e)}")
            return []

    async def get_user_top_activities(
        self, user_id: str, limit: int = 10
    ) -> List[dict]:
        """Retorna as atividades mais frequentes de um usuário calculando dinamicamente a partir do histórico"""
        try:
            # Usa agregação do MongoDB para calcular totais
            pipeline = [
                # Filtra apenas sessões do usuário que foram finalizadas
                {"$match": {"user_id": user_id, "end_time": {"$ne": None}}},
                # Agrupa por atividade e soma as durações
                {
                    "$group": {
                        "_id": "$activity_name",
                        "total_seconds": {
                            "$sum": {
                                "$divide": [
                                    {"$subtract": ["$end_time", "$start_time"]},
                                    1000,  # Converte milissegundos para segundos
                                ]
                            }
                        },
                        "last_seen": {"$max": "$end_time"},
                    }
                },
                # Ordena por tempo total decrescente
                {"$sort": {"total_seconds": -1}},
                # Limita os resultados
                {"$limit": limit},
            ]

            cursor = self.activity_history.aggregate(pipeline)
            results = []
            for doc in cursor:
                results.append(
                    {
                        "activity_name": doc["_id"],
                        "total_seconds": doc["total_seconds"],
                        "last_seen": doc["last_seen"],
                    }
                )
            return results
        except Exception as e:
            logging.error(f"Erro ao buscar atividades do usuário: {str(e)}")
            return []

    async def get_or_create_activity(self, name: str) -> Activity:
        """Retorna uma atividade existente ou cria uma nova"""
        try:
            # Case insensitive search
            doc = self.activities.find_one(
                {"name": {"$regex": f"^{name}$", "$options": "i"}}
            )
            if doc:
                return Activity.from_dict(doc)

            activity = Activity(name=name, created_at=datetime.now(UTC))
            self.activities.insert_one(activity.to_dict())
            return activity
        except Exception as e:
            logging.error(f"Erro ao buscar/criar atividade: {str(e)}")
            return None

    async def start_activity_session(
        self, user_id: str, username: str, activity_name: str
    ) -> bool:
        """Inicia uma sessão de atividade para o usuário"""
        try:
            # Garante que o perfil do usuário existe
            if not self.user_profiles.find_one({"discord_id": user_id}):
                await self.create_user_profile(user_id, username)

            # Primeiro garante que a atividade existe
            activity = await self.get_or_create_activity(activity_name)
            if not activity:
                return False

            # Verifica se já existe uma sessão aberta para essa atividade e usuário
            # Se existir, não faz nada (ou poderia fechar e abrir outra, mas vamos manter simples)
            existing_session = self.activity_history.find_one(
                {
                    "user_id": user_id,
                    "activity_name": activity.name,  # Usa o nome oficial da atividade
                    "end_time": None,
                }
            )

            if existing_session:
                return True

            session = ActivityHistory(
                user_id=user_id,
                activity_name=activity.name,
                start_time=datetime.now(UTC),
            )

            result = self.activity_history.insert_one(session.to_dict())
            return result.acknowledged
        except Exception as e:
            logging.error(f"Erro ao iniciar sessão de atividade: {str(e)}")
            return False

    async def end_activity_session(self, user_id: str, activity_name: str) -> bool:
        """Finaliza uma sessão de atividade aberta"""
        try:
            # Busca todas as sessões abertas do usuário
            cursor = self.activity_history.find({"user_id": user_id, "end_time": None})

            now = datetime.now(UTC)
            modified = False

            for doc in cursor:
                # Compara nomes de forma case-insensitive
                if doc["activity_name"].lower() == activity_name.lower():
                    # Atualiza o histórico com o end_time
                    self.activity_history.update_one(
                        {"_id": doc["_id"]}, {"$set": {"end_time": now}}
                    )
                    modified = True
                    logging.info(
                        f"Sessão de atividade {activity_name} finalizada para usuário {user_id}"
                    )

            return modified
        except Exception as e:
            logging.error(f"Erro ao finalizar sessão de atividade: {str(e)}")
            return False

    async def sync_member_profiles(self, members_data: List[dict]) -> int:
        """Sincroniza perfis de membros, criando se não existirem e atualizando display_name"""
        count = 0
        try:
            for member in members_data:
                discord_id = str(member["id"])
                username = member["name"]
                display_name = member.get("display_name", username)

                # Verifica se existe
                existing = self.db.user_profiles.find_one({"discord_id": discord_id})

                if not existing:
                    # Cria novo perfil
                    await self.create_user_profile(discord_id, username, display_name)
                    count += 1
                else:
                    # Atualiza username e display_name se mudaram
                    self.db.user_profiles.update_one(
                        {"discord_id": discord_id},
                        {"$set": {"username": username, "display_name": display_name}},
                    )

            if count > 0:
                logging.info(f"Sincronização concluída: {count} novos perfis criados.")
            return count
        except Exception as e:
            logging.error(f"Erro ao sincronizar membros: {str(e)}")
            return 0

    async def get_global_activity_rank(
        self, activity_name: str, limit: int = 10
    ) -> List[dict]:
        """Retorna o ranking global de usuários para uma atividade específica calculando dinamicamente"""
        try:
            # Usa agregação do MongoDB para calcular rankings por usuário
            pipeline = [
                # Filtra apenas sessões da atividade específica que foram finalizadas (case-insensitive)
                {
                    "$match": {
                        "activity_name": {
                            "$regex": f"^{activity_name}$",
                            "$options": "i",
                        },
                        "end_time": {"$ne": None},
                    }
                },
                # Agrupa por usuário e soma as durações
                {
                    "$group": {
                        "_id": "$user_id",
                        "activity_name": {"$first": "$activity_name"},
                        "total_seconds": {
                            "$sum": {
                                "$divide": [
                                    {"$subtract": ["$end_time", "$start_time"]},
                                    1000,  # Converte milissegundos para segundos
                                ]
                            }
                        },
                        "last_seen": {"$max": "$end_time"},
                    }
                },
                # Ordena por tempo total decrescente
                {"$sort": {"total_seconds": -1}},
                # Limita os resultados
                {"$limit": limit},
            ]

            cursor = self.activity_history.aggregate(pipeline)
            results = []
            for doc in cursor:
                results.append(
                    {
                        "user_id": doc["_id"],
                        "activity_name": doc["activity_name"],
                        "total_seconds": doc["total_seconds"],
                        "last_seen": doc["last_seen"],
                    }
                )
            return results
        except Exception as e:
            logging.error(f"Erro ao buscar ranking global da atividade: {str(e)}")
            return []

    async def get_top_activities_global(self, limit: int = 10) -> List[dict]:
        """Retorna as atividades mais realizadas globalmente, ranqueadas por tempo total"""
        try:
            # Usa agregação do MongoDB para calcular totais por atividade
            pipeline = [
                # Filtra apenas sessões finalizadas
                {"$match": {"end_time": {"$ne": None}}},
                # Agrupa por atividade e soma as durações
                {
                    "$group": {
                        "_id": "$activity_name",
                        "total_seconds": {
                            "$sum": {
                                "$divide": [
                                    {"$subtract": ["$end_time", "$start_time"]},
                                    1000,  # Converte milissegundos para segundos
                                ]
                            }
                        },
                        "unique_players": {"$addToSet": "$user_id"},
                        "session_count": {"$sum": 1},
                    }
                },
                # Adiciona contagem de jogadores únicos
                {
                    "$project": {
                        "activity_name": "$_id",
                        "total_seconds": 1,
                        "player_count": {"$size": "$unique_players"},
                        "session_count": 1,
                    }
                },
                # Ordena por tempo total decrescente
                {"$sort": {"total_seconds": -1}},
                # Limita os resultados
                {"$limit": limit},
            ]

            cursor = self.activity_history.aggregate(pipeline)
            results = []
            for doc in cursor:
                results.append(
                    {
                        "activity_name": doc["activity_name"],
                        "total_seconds": doc["total_seconds"],
                        "player_count": doc["player_count"],
                        "session_count": doc["session_count"],
                    }
                )
            return results
        except Exception as e:
            logging.error(f"Erro ao buscar ranking global de atividades: {str(e)}")
            return []

    async def get_top_members_by_activity_time(self, limit: int = 10) -> List[dict]:
        """Retorna os membros ranqueados por tempo total em atividades"""
        try:
            # Usa agregação do MongoDB para calcular totais por usuário
            pipeline = [
                # Filtra apenas sessões finalizadas
                {"$match": {"end_time": {"$ne": None}}},
                # Agrupa por usuário e soma as durações
                {
                    "$group": {
                        "_id": "$user_id",
                        "total_seconds": {
                            "$sum": {
                                "$divide": [
                                    {"$subtract": ["$end_time", "$start_time"]},
                                    1000,  # Converte milissegundos para segundos
                                ]
                            }
                        },
                        "activities": {
                            "$push": {
                                "name": "$activity_name",
                                "duration": {
                                    "$divide": [
                                        {"$subtract": ["$end_time", "$start_time"]},
                                        1000,
                                    ]
                                },
                            }
                        },
                    }
                },
                # Ordena por tempo total decrescente
                {"$sort": {"total_seconds": -1}},
                # Limita os resultados
                {"$limit": limit},
            ]

            cursor = self.activity_history.aggregate(pipeline)
            results = []
            for doc in cursor:
                # Agrupa e soma atividades duplicadas
                activity_totals = {}
                for activity in doc["activities"]:
                    name = activity["name"]
                    duration = activity["duration"]
                    activity_totals[name] = activity_totals.get(name, 0) + duration

                # Ordena atividades por duração e pega as top 3
                top_activities = sorted(
                    activity_totals.items(), key=lambda x: x[1], reverse=True
                )[:3]

                results.append(
                    {
                        "user_id": doc["_id"],
                        "total_seconds": doc["total_seconds"],
                        "top_activities": [
                            {"name": name, "seconds": seconds}
                            for name, seconds in top_activities
                        ],
                    }
                )
            return results
        except Exception as e:
            logging.error(f"Erro ao buscar ranking de membros por atividade: {str(e)}")
            return []

    def initialize_collections(self):
        """Inicializa as coleções necessárias se não existirem"""
        try:
            # Cria índices necessários
            self.user_profiles.create_index("discord_id", unique=True)
            # Índice para canais monitorados (evita duplicatas por platform+channel_id)
            self.monitored_channels.create_index(
                [("platform", 1), ("channel_id", 1)], unique=True
            )
            # Índice para buscas por channel_name
            self.monitored_channels.create_index("channel_name")

            # Índices para atividades
            self.activities.create_index("name", unique=True)
            self.activity_history.create_index(
                [("user_id", 1), ("activity_name", 1), ("end_time", 1)]
            )
            self.activity_history.create_index("start_time")

            logging.info("Índices do banco de dados criados/atualizados com sucesso!")
        except Exception as e:
            logging.error(f"Erro ao inicializar coleções: {str(e)}")
            raise e


# Instância global do banco de dados
db = Database()
