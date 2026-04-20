import logging
import discord
from discord.ext import commands
import yt_dlp
import asyncio
import random
import difflib
import re
import urllib.parse
import requests
from collections import deque

try:
    from ytmusicapi import YTMusic
except Exception:
    YTMusic = None

from db.database import db
from config.settings import EQUALIZER_PRESETS, LASTFM_API_KEY
from .utils import clean_youtube_url, is_youtube_url, stream_musica
from .commands_utils import validar_canal, play_queue, last_played_info, autoplay_enabled, active_preset


LASTFM_API_URL = "https://ws.audioscrobbler.com/2.0/"

if LASTFM_API_KEY:
    logging.info(f"[Last.fm] API key carregada com sucesso (primeiros 6 chars: {LASTFM_API_KEY[:6]}...)")
else:
    logging.warning("[Last.fm] LASTFM_API_KEY não configurada — recomendações via Last.fm desativadas.")

MAX_AUTOPLAY_RECENTES = 20
autoplay_recent_urls = {}
recent_played_titles = {}
recent_played_uploaders = {}
MAX_RECENT_TITLES = 12
MAX_RECENT_UPLOADERS = 6
manual_stop_guilds = set()
_ytmusic_client = None
_lastfm_artist_tags_cache = {}
MAX_SANITIZED_TEXT_LEN = 256
ENTRY_URL_KEYS = ('url', 'webpage_url', 'original_url', 'id')
ENTRY_ARTIST_KEYS = ('uploader', 'channel', 'channel_name', 'artist')
ENTRY_ARTIST_KEYS_LIGHT = ('uploader', 'channel', 'artist')


def _sanitizar_texto_bruto(value, max_len: int = MAX_SANITIZED_TEXT_LEN) -> str:
    """Sanitiza texto externo removendo caracteres de controle e limitando tamanho."""
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    text = " ".join(text.split())
    if max_len > 0 and len(text) > max_len:
        text = text[:max_len].rstrip()
    return text


def _normalizar_candidato_youtube(candidate):
    """Converte diferentes formatos para URL canônica de vídeo do YouTube."""
    if not candidate:
        return None

    value = str(candidate).strip()
    if not value:
        return None

    def _is_video_id(video_id: str) -> bool:
        return bool(video_id and re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id))

    def _extract_video_id(url: str):
        try:
            parsed = urllib.parse.urlparse(url)
            host = (parsed.netloc or '').lower()
            path = (parsed.path or '').strip('/')

            if host in ('youtu.be', 'www.youtu.be'):
                candidate_id = path.split('/')[0] if path else ''
                return candidate_id if _is_video_id(candidate_id) else None

            if 'youtube.com' in host or 'music.youtube.com' in host:
                query = urllib.parse.parse_qs(parsed.query)
                if 'v' in query and query['v']:
                    candidate_id = query['v'][0]
                    if _is_video_id(candidate_id):
                        return candidate_id

                segments = path.split('/') if path else []
                if len(segments) >= 2 and segments[0] in ('shorts', 'embed', 'live'):
                    candidate_id = segments[1]
                    if _is_video_id(candidate_id):
                        return candidate_id
        except Exception:
            return None

        return None

    if value.startswith('http://') or value.startswith('https://'):
        if not is_youtube_url(value):
            return None
        video_id = _extract_video_id(value)
        return f"https://www.youtube.com/watch?v={video_id}" if video_id else None

    # yt_dlp pode retornar apenas o id do vídeo em alguns cenários (extract_flat).
    if _is_video_id(value):
        return f"https://www.youtube.com/watch?v={value}"

    return None


def _normalizar_texto(value: str) -> str:
    if not value:
        return ""
    return _sanitizar_texto_bruto(value).lower()


def _entry_title(entry: dict) -> str:
    if not isinstance(entry, dict):
        return ""
    return (entry.get('title') or '').strip()


def _entry_artist(entry: dict, keys=ENTRY_ARTIST_KEYS, normalize: bool = False) -> str:
    if not isinstance(entry, dict):
        return ""

    value = ''
    for key in keys:
        candidate = entry.get(key)
        if candidate:
            value = str(candidate).strip()
            break

    if normalize:
        return _normalizar_texto(value)
    return value


def _titulo_canonico(title: str) -> str:
    """Normaliza variações comuns do mesmo vídeo/música para comparação."""
    normalized = _normalizar_texto(title)
    if not normalized:
        return ""

    normalized = re.sub(r"\[[^\]]*\]", " ", normalized)
    normalized = re.sub(r"\([^)]*\)", " ", normalized)
    noise_words = [
        "official", "video", "audio", "lyrics", "lyric", "clipe", "clip", "live",
        "legendado", "hd", "4k", "remastered", "version", "versao", "ptbr", "pt-br",
        "remix", "acoustic", "cover", "instrumental", "extended", "mix", "edit",
        "deluxe", "anniversary", "original", "explicit", "clean", "radio", "single",
        "album", "ost", "soundtrack", "slowed", "reverb", "sped", "nightcore",
        "visualizer", "performance", "session", "unplugged", "demo", "bonus"
    ]
    for word in noise_words:
        normalized = re.sub(rf"\b{re.escape(word)}\b", " ", normalized)

    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = " ".join(normalized.split())
    return normalized


def _token_set(title: str):
    stopwords = {
        "the", "a", "an", "of", "and", "in", "on", "feat", "ft", "with", "by", "official"
    }
    canonical = _titulo_canonico(title)
    return {t for t in canonical.split() if t and t not in stopwords}


def _titulo_equivalente(title_a: str, title_b: str) -> bool:
    """Compara títulos por similaridade textual e interseção de tokens."""
    a = _titulo_canonico(title_a)
    b = _titulo_canonico(title_b)
    if not a or not b:
        return False

    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    if ratio >= 0.65:
        return True

    ta = _token_set(a)
    tb = _token_set(b)
    if not ta or not tb:
        return False

    intersection = len(ta.intersection(tb))
    union = len(ta.union(tb))
    jaccard = (intersection / union) if union else 0
    return jaccard >= 0.50


def _titulo_muito_parecido(title_a: str, title_b: str) -> bool:
    """Detecta títulos muito parecidos para reduzir recomendações repetitivas."""
    a = _titulo_canonico(title_a)
    b = _titulo_canonico(title_b)
    if not a or not b:
        return False

    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    if ratio >= 0.65:
        return True

    ta = _token_set(a)
    tb = _token_set(b)
    if not ta or not tb:
        return False

    intersection = len(ta.intersection(tb))
    union = len(ta.union(tb))
    jaccard = (intersection / union) if union else 0
    return jaccard >= 0.50


def _parece_conteudo_nao_musical(title: str) -> bool:
    """Tenta eliminar resultados de busca que claramente não são músicas."""
    t = _normalizar_texto(title)
    if not t:
        return True

    non_music_markers = (
        "podcast", "interview", "entrevista", "aula", "lesson", "english",
        "how to", "tutorial", "documentary", "documentario", "reaction",
        "review", "news", "noticias", "analysis", "analise", "speech",
        "debate", "live stream", "livestream", "audiobook"
    )
    return any(marker in t for marker in non_music_markers)


def _tem_marcador_musical(title: str) -> bool:
    """Heurística positiva para identificar títulos que parecem música."""
    t = _normalizar_texto(title)
    if not t:
        return False

    music_markers = (
        "official", "audio", "lyrics", "lyric", "music", "song", "clipe", "clip",
        "live", "remix", "cover", "topic", "mv", "amv", "visualizer", "feat", "ft"
    )
    return any(marker in t for marker in music_markers)


def _texto_parecido(a: str, b: str) -> bool:
    """Compara textos curtos para validar afinidade entre artistas/canais."""
    aa = _normalizar_texto(a)
    bb = _normalizar_texto(b)
    if not aa or not bb:
        return False
    if aa in bb or bb in aa:
        return True
    return difflib.SequenceMatcher(None, aa, bb).ratio() >= 0.70


def _uploader_entry(entry: dict) -> str:
    return _entry_artist(entry, keys=ENTRY_ARTIST_KEYS, normalize=True)


def _entry_to_youtube_url(entry: dict):
    if not isinstance(entry, dict):
        return None

    for key in ENTRY_URL_KEYS:
        normalized = _normalizar_candidato_youtube(entry.get(key))
        if normalized:
            return normalized
    return None


def _get_ytmusic_client():
    """Inicializa cliente do YouTube Music sem autenticação para buscas públicas."""
    global _ytmusic_client
    if YTMusic is None:
        return None
    if _ytmusic_client is None:
        try:
            _ytmusic_client = YTMusic()
        except Exception as e:
            logging.warning(f"Falha ao inicializar ytmusicapi: {e}")
            _ytmusic_client = None
    return _ytmusic_client


async def _buscar_recomendacoes_musicais(query: str, max_results: int = 10, artist_hint: str = ""):
    """Busca recomendações priorizando o catálogo de músicas do YouTube Music."""
    q = _sanitizar_texto_bruto(query, max_len=160)
    if not q:
        return []

    client = _get_ytmusic_client()
    if client:
        try:
            results = await asyncio.to_thread(client.search, q, filter="songs", limit=max_results)
            entries = []
            for item in results or []:
                video_id = _sanitizar_texto_bruto(item.get('videoId'), max_len=32)
                title = _sanitizar_texto_bruto(item.get('title'))
                if not video_id or not title or _parece_conteudo_nao_musical(title):
                    continue

                artists = item.get('artists') or []
                artist_name = ''
                if artists and isinstance(artists, list):
                    artist_name = _sanitizar_texto_bruto(artists[0].get('name'))

                entries.append({
                    'url': f"https://www.youtube.com/watch?v={video_id}",
                    'title': title,
                    'uploader': artist_name,
                })
            if entries:
                return entries
        except Exception as e:
            logging.warning(f"Falha na busca ytmusicapi para '{q}': {e}")

    # Fallback para yt_dlp caso ytmusicapi indisponível/sem resultados.
    fallback_query = f"{q} official audio"
    raw_entries = await _buscar_musica_ou_artista(fallback_query, max_results=max_results)
    hint = _normalizar_texto(artist_hint)
    entries = []
    for item in raw_entries:
        title = _entry_title(item)
        if not title or _parece_conteudo_nao_musical(title):
            continue
        uploader = _entry_artist(item, keys=ENTRY_ARTIST_KEYS_LIGHT)

        # No fallback via yt_dlp, seja bem mais rígido para evitar lixo de busca.
        # Aceita se: tem marcador musical OU canal parecido com artista sugerido.
        if hint:
            uploader_matches_hint = _texto_parecido(uploader, hint)
        else:
            uploader_matches_hint = False
        if not _tem_marcador_musical(title) and not uploader_matches_hint:
            continue

        url = _entry_to_youtube_url(item)
        if not url:
            continue
        entries.append({
            'url': url,
            'title': title,
            'uploader': uploader,
        })
    return entries


async def _buscar_musica_ou_artista(query: str, max_results: int = 10):
    """Busca músicas por texto (artista/música) e retorna entradas do yt_dlp."""
    q = _sanitizar_texto_bruto(query, max_len=160)
    if not q:
        return []

    encoded_query = urllib.parse.quote_plus(q)
    ytmusic_url = f"https://music.youtube.com/search?q={encoded_query}"

    # 1) Tenta busca pelo endpoint do YouTube Music.
    try:
        with yt_dlp.YoutubeDL({'quiet': True, 'extract_flat': True}) as ydl:
            result = await asyncio.to_thread(ydl.extract_info, ytmusic_url, download=False)
        if result and isinstance(result, dict) and result.get('entries'):
            return [e for e in result['entries'] if e]
    except Exception as e:
        logging.info(f"Busca no YouTube Music falhou para '{q}', usando fallback ytsearch: {e}")

    # 2) Fallback robusto via ytsearch.
    try:
        with yt_dlp.YoutubeDL({
            'quiet': True,
            'extract_flat': True,
            'default_search': f'ytsearch{max_results}'
        }) as ydl:
            result = await asyncio.to_thread(
                ydl.extract_info,
                f"ytsearch{max_results}:{q.replace('&', 'and')}",
                download=False,
            )
        if result and isinstance(result, dict) and result.get('entries'):
            return [e for e in result['entries'] if e]
    except Exception as e:
        logging.warning(f"Falha no fallback ytsearch para '{q}': {e}")

    return []


def _registrar_autoplay_recente(guild_id: int, url: str):
    cleaned = _normalizar_candidato_youtube(url)
    if not cleaned:
        return

    if guild_id not in autoplay_recent_urls:
        autoplay_recent_urls[guild_id] = deque(maxlen=MAX_AUTOPLAY_RECENTES)
    autoplay_recent_urls[guild_id].append(cleaned)


def _registrar_faixa_tocada(guild_id: int, title: str, uploader: str):
    if guild_id not in recent_played_titles:
        recent_played_titles[guild_id] = deque(maxlen=MAX_RECENT_TITLES)
    if title:
        recent_played_titles[guild_id].append(title)

    if guild_id not in recent_played_uploaders:
        recent_played_uploaders[guild_id] = deque(maxlen=MAX_RECENT_UPLOADERS)
    uploader_normalized = _normalizar_texto(uploader)
    if uploader_normalized:
        recent_played_uploaders[guild_id].append(uploader_normalized)


def _escolher_candidato_diverso(
    candidatos,
    guild_id: int,
    last_title: str,
    last_uploader: str,
    extra_titles=None,
    extra_uploaders=None,
):
    """Escolhe um candidato privilegiando diversidade de título e uploader."""
    if not candidatos:
        return None, None

    faixa_diversa = []
    faixa_media = []
    faixa_fallback = []

    history_titles = list(recent_played_titles.get(guild_id, []))
    uploader_history = list(recent_played_uploaders.get(guild_id, []))
    if extra_titles:
        history_titles.extend(extra_titles)
    if extra_uploaders:
        uploader_history.extend(_normalizar_texto(u) for u in extra_uploaders if u)

    recent_uploader_window = set(uploader_history[-2:])

    for url, title, uploader in candidatos:
        same_uploader = bool(last_uploader and uploader and uploader == last_uploader)
        similar_title = _titulo_muito_parecido(last_title, title)
        equivalent_recent_title = any(_titulo_equivalente(title, old_title) for old_title in history_titles)
        recent_uploader = bool(uploader and uploader in recent_uploader_window)

        if equivalent_recent_title:
            continue

        if not same_uploader and not similar_title and not recent_uploader:
            faixa_diversa.append((url, title))
        elif not similar_title and not recent_uploader:
            faixa_media.append((url, title))
        else:
            faixa_fallback.append((url, title))

    for faixa in (faixa_diversa, faixa_media, faixa_fallback):
        if faixa:
            return random.choice(faixa)

    return None, None


def _limpar_nome_artista(uploader: str) -> str:
    """Remove sufixos do YouTube Music como ' - Topic' e ' VEVO'."""
    if not uploader:
        return ""
    uploader = re.sub(r'\s*-\s*Topic\s*$', '', uploader, flags=re.IGNORECASE)
    uploader = re.sub(r'\s*VEVO\s*$', '', uploader, flags=re.IGNORECASE)
    return uploader.strip()


def _inferir_artista_para_recomendacao(selected_entry: dict, entries: list, user_query: str = "") -> str:
    """Tenta inferir artista para melhorar qualidade das recomendações."""
    if isinstance(selected_entry, dict):
        direct = _entry_artist(selected_entry, keys=ENTRY_ARTIST_KEYS_LIGHT)
        if direct:
            return _limpar_nome_artista(str(direct))

    if isinstance(entries, list):
        counts = {}
        for e in entries:
            if not isinstance(e, dict):
                continue
            up = _entry_artist(e, keys=ENTRY_ARTIST_KEYS_LIGHT)
            up = _limpar_nome_artista(str(up)) if up else ''
            if not up:
                continue
            norm = _normalizar_texto(up)
            counts[norm] = counts.get(norm, 0) + 1
        if counts:
            best_norm = max(counts.items(), key=lambda kv: kv[1])[0]
            return best_norm

    if user_query and '-' in user_query:
        return _limpar_nome_artista(user_query.split('-', 1)[0].strip())

    return ""


async def _salvar_historico_mongodb(ctx, title: str, url: str, artist: str = ""):
    """Salva faixa no histórico do MongoDB com saneamento de dados."""
    try:
        if not ctx or not getattr(ctx, 'author', None):
            return False

        safe_title = _sanitizar_texto_bruto(title)
        if not safe_title:
            return False

        safe_url = _normalizar_candidato_youtube(url) if url else None
        if not safe_url and url is not None:
            safe_url = str(url).strip()
        if not safe_url:
            return False

        safe_artist = _sanitizar_texto_bruto(artist)

        await db.create_user_profile(str(ctx.author.id), ctx.author.name)
        ok = await db.add_to_music_history(str(ctx.author.id), {
            "title": safe_title,
            "url": safe_url,
            "artist": safe_artist or None,
        })

        if not ok:
            logging.warning(
                f"[MongoDB] Falha ao registrar música no histórico para usuário {ctx.author.id}: "
                f"title='{safe_title}', url='{safe_url}'"
            )
        return ok
    except Exception as e:
        logging.error(f"[MongoDB] Erro ao salvar histórico musical: {e}")
        return False


def _normalizar_tag(value: str) -> str:
    return re.sub(r"[^a-z0-9\s]", "", _normalizar_texto(value))


def _buscar_tags_artista_lastfm(artist: str, limit: int = 8) -> set[str]:
    """Retorna tags principais de um artista no Last.fm com cache simples em memória."""
    artist = _sanitizar_texto_bruto(artist)
    artist_norm = _normalizar_texto(artist)
    if not LASTFM_API_KEY or not artist_norm:
        return set()

    if artist_norm in _lastfm_artist_tags_cache:
        return _lastfm_artist_tags_cache[artist_norm]

    try:
        resp = requests.get(LASTFM_API_URL, params={
            'method': 'artist.getTopTags',
            'artist': artist,
            'api_key': LASTFM_API_KEY,
            'format': 'json',
        }, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        tags = data.get('toptags', {}).get('tag', [])
        parsed = set()
        for tag in tags[:limit]:
            name = _normalizar_tag(tag.get('name') or '')
            if name:
                parsed.add(name)
        _lastfm_artist_tags_cache[artist_norm] = parsed
        return parsed
    except Exception as e:
        logging.warning(f"[Last.fm] Falha ao buscar tags do artista '{artist}': {e}")
        _lastfm_artist_tags_cache[artist_norm] = set()
        return set()


def _filtrar_similares_por_tematica(seed_artist: str, similar_tracks: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Filtra similares fora do estilo dominante do artista base (ex.: rock -> evita kpop)."""
    if not similar_tracks or not seed_artist or not LASTFM_API_KEY:
        return similar_tracks

    seed_tags = _buscar_tags_artista_lastfm(seed_artist)
    if not seed_tags:
        return similar_tracks

    themed = []
    fallback = []

    seed_has_rock = any(t in seed_tags for t in ('rock', 'alternative rock', 'metal', 'nu metal', 'hard rock'))
    seed_has_pop = any(t in seed_tags for t in ('pop', 'dance pop', 'electropop'))

    for sim_artist, sim_track in similar_tracks:
        sim_tags = _buscar_tags_artista_lastfm(sim_artist)
        if not sim_tags:
            fallback.append((sim_artist, sim_track))
            continue

        inter = seed_tags.intersection(sim_tags)

        # Se origem é rock/metal, remove explicitamente candidatos de kpop/jpop quando sem interseção.
        if seed_has_rock:
            if any(t in sim_tags for t in ('kpop', 'k-pop', 'jpop', 'j-pop')) and not inter:
                continue

        # Se origem é pop, remove extremos de metal sem interseção.
        if seed_has_pop:
            if any(t in sim_tags for t in ('death metal', 'black metal', 'grindcore')) and not inter:
                continue

        if inter:
            themed.append((sim_artist, sim_track))
        else:
            fallback.append((sim_artist, sim_track))

    # Prefere manter temática; se não houver nenhum overlap, volta para lista original.
    return themed if themed else similar_tracks


def _buscar_similar_lastfm(track_title: str, artist: str, limit: int = 20) -> list[tuple[str, str]]:
    """Chama Last.fm track.getSimilar e retorna lista de (artista, titulo)."""
    track_title = _sanitizar_texto_bruto(track_title)
    artist = _sanitizar_texto_bruto(artist)
    if not LASTFM_API_KEY or not track_title or not artist:
        logging.warning(f"[Last.fm] Chamada ignorada — key={bool(LASTFM_API_KEY)}, title='{track_title}', artist='{artist}'")
        return []
    logging.info(f"[Last.fm] Buscando similares para: '{track_title}' de '{artist}'")
    try:
        resp = requests.get(LASTFM_API_URL, params={
            'method': 'track.getSimilar',
            'track': track_title,
            'artist': artist,
            'api_key': LASTFM_API_KEY,
            'format': 'json',
            'limit': limit,
        }, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        tracks = data.get('similartracks', {}).get('track', [])
        results = []
        for t in tracks:
            t_name = _sanitizar_texto_bruto(t.get('name'))
            t_artist = _sanitizar_texto_bruto((t.get('artist', {}) or {}).get('name'))
            if t_name and t_artist:
                results.append((t_artist, t_name))
        logging.info(f"[Last.fm] {len(results)} faixas similares encontradas para '{track_title}'")
        return results
    except Exception as e:
        logging.warning(f"[Last.fm] Falha ao buscar similares: {e}")
        return []


async def buscar_recomendacao_autoplay(guild_id, ctx=None):
    """Busca uma URL recomendada para autoplay com fallback por pesquisa."""
    info = last_played_info.get(guild_id) or {}
    queue_urls = set(autoplay_recent_urls.get(guild_id, []))
    candidatos = []
    seen_candidates = set()
    mongo_titles = []
    mongo_uploaders = []
    mongo_artist_hints = []

    last_title = info.get('title', '')
    last_uploader = _normalizar_texto(info.get('uploader') or info.get('channel') or info.get('artist'))

    # Usa histórico persistido no MongoDB como referência adicional para evitar repetições.
    try:
        if ctx and getattr(ctx, 'author', None):
            user_profile = await db.get_user_profile(str(ctx.author.id))
            if user_profile and user_profile.music_history:
                recent_db_songs = user_profile.music_history[-20:]
                for song in recent_db_songs:
                    song_title = getattr(song, 'title', '') or ''
                    song_url = getattr(song, 'url', None)
                    song_artist = getattr(song, 'artist', None)

                    if song_title:
                        mongo_titles.append(song_title)
                    if song_artist:
                        mongo_uploaders.append(song_artist)

                    if song_url:
                        normalized_song_url = _normalizar_candidato_youtube(song_url)
                        if normalized_song_url:
                            queue_urls.add(normalized_song_url)

                # Dicas de artistas recentes para fallback de busca.
                for song in reversed(recent_db_songs):
                    artist = (getattr(song, 'artist', None) or '').strip()
                    if artist and artist.lower() not in {a.lower() for a in mongo_artist_hints}:
                        mongo_artist_hints.append(artist)
                    if len(mongo_artist_hints) >= 3:
                        break
    except Exception as e:
        logging.warning(f"Falha ao carregar histórico MongoDB para autoplay na guild {guild_id}: {e}")

    for entry in play_queue.get(guild_id, []):
        try:
            queue_urls.add(clean_youtube_url(entry[0]))
        except Exception:
            continue

    current_url = _normalizar_candidato_youtube(
        info.get('webpage_url') or info.get('original_url') or info.get('requested_url')
    )
    if current_url:
        queue_urls.add(current_url)

    def collect_candidate(entry):
        if not isinstance(entry, dict):
            return

        title = _entry_title(entry)
        if not title:
            return
        if _parece_conteudo_nao_musical(title):
            return
        uploader = _uploader_entry(entry)

        for key in ENTRY_URL_KEYS:
            normalized = _normalizar_candidato_youtube(entry.get(key))
            if not normalized or normalized in queue_urls or normalized in seen_candidates:
                continue

            seen_candidates.add(normalized)
            candidatos.append((normalized, title, uploader))
            return

    related_videos = info.get('related_videos') or []
    if isinstance(related_videos, list):
        for video in related_videos:
            collect_candidate(video)

    chosen_url, chosen_title = _escolher_candidato_diverso(
        candidatos,
        guild_id,
        last_title,
        last_uploader,
        extra_titles=mongo_titles,
        extra_uploaders=mongo_uploaders,
    )
    if chosen_url:
        _registrar_autoplay_recente(guild_id, chosen_url)
        return chosen_url, chosen_title

    title = last_title.strip()
    uploader = (info.get('uploader', '') or '').strip()
    artist_clean = _limpar_nome_artista(uploader) or title.split('-')[0].strip()

    async def buscar_por_query(query_text: str, max_results: int = 10, strict_diversity: bool = True):
        if not query_text:
            return None, None

        try:
            search_entries = await _buscar_recomendacoes_musicais(
                query_text,
                max_results=max_results,
                artist_hint=artist_clean,
            )

            if search_entries:
                antes = len(candidatos)
                for entry in search_entries:
                    collect_candidate(entry)
                novos = len(candidatos) - antes
                novos_candidatos = candidatos[antes:]
                logging.info(f"[autoplay] Query '{query_text}': {len(search_entries)} resultados musicais, {novos} novos candidatos (total: {len(candidatos)})")

                selected_url, selected_title = _escolher_candidato_diverso(
                    candidatos,
                    guild_id,
                    last_title,
                    last_uploader,
                    extra_titles=mongo_titles,
                    extra_uploaders=mongo_uploaders,
                )
                if selected_url:
                    _registrar_autoplay_recente(guild_id, selected_url)
                    return selected_url, selected_title
                else:
                    logging.info(f"[autoplay] Filtro de diversidade rejeitou todos os {len(candidatos)} candidatos para '{query_text}'")

                # Modo relaxado (usado no bloco Last.fm): aceita o primeiro novo candidato
                # que não seja equivalente ao título atual.
                if not strict_diversity and novos_candidatos:
                    for fallback_url, fallback_title, _fallback_uploader in novos_candidatos:
                        if _titulo_equivalente(fallback_title, last_title):
                            continue
                        _registrar_autoplay_recente(guild_id, fallback_url)
                        logging.info(f"[autoplay] Modo relaxado escolheu '{fallback_title}' para query '{query_text}'")
                        return fallback_url, fallback_title
            else:
                logging.info(f"[autoplay] Query '{query_text}': sem resultados musicais")
        except Exception as e:
            logging.warning(f"Falha ao buscar autoplay com query '{query_text}' para guild {guild_id}: {e}")

        return None, None

    # 1) Last.fm: músicas similares de verdade (artistas diferentes)
    if LASTFM_API_KEY:
        logging.info(f"[Last.fm] Iniciando busca de similares para guild {guild_id} — title='{title}', artist='{artist_clean}'")
        similar_tracks = await asyncio.to_thread(_buscar_similar_lastfm, title, artist_clean)
        similar_tracks = await asyncio.to_thread(_filtrar_similares_por_tematica, artist_clean, similar_tracks)
        random.shuffle(similar_tracks)
        logging.info(f"[Last.fm] {len(similar_tracks)} faixas similares retornadas, testando as 10 primeiras")
        for sim_artist, sim_track in similar_tracks[:10]:
            query = f"{sim_artist} {sim_track} official audio"
            logging.info(f"[Last.fm] Buscando no YouTube: '{query}'")
            chosen_url, chosen_title = await buscar_por_query(query, max_results=8, strict_diversity=False)
            if chosen_url:
                logging.info(f"[Last.fm] Candidato aceito: '{chosen_title}' ({chosen_url})")
                return chosen_url, chosen_title
            else:
                logging.info(f"[Last.fm] Nenhum candidato aceito pelo filtro de diversidade para '{query}'")
        logging.warning(f"[Last.fm] Nenhum candidato válido encontrado via Last.fm para guild {guild_id}")

    # 2) Busca por outros títulos do mesmo artista (evita recomenda o mesmo título)
    uploader_only = _limpar_nome_artista(uploader) or _normalizar_texto(uploader)
    chosen_url, chosen_title = await buscar_por_query(uploader_only, max_results=20)
    if chosen_url:
        return chosen_url, chosen_title

    # 3) Fallback: artistas recentes do MongoDB
    for artist_hint in mongo_artist_hints:
        chosen_url, chosen_title = await buscar_por_query(f"{artist_hint} official", max_results=20)
        if chosen_url:
            return chosen_url, chosen_title

    # 4) Fallback final: artista extraído do título + "official"
    artista_hint = title.split('-')[0].strip() if '-' in title else ""
    extra_query = f"{artista_hint} official".strip()
    chosen_url, chosen_title = await buscar_por_query(extra_query, max_results=25)
    if chosen_url:
        return chosen_url, chosen_title

    return None, None


async def tocar_proxima_musica(vc, guild_id, ctx):
    """Toca a próxima música da fila ou busca uma recomendada se auto-play estiver ativo."""
    if guild_id in manual_stop_guilds:
        manual_stop_guilds.discard(guild_id)
        return

    if guild_id not in play_queue:
        play_queue[guild_id] = deque()

    if not play_queue[guild_id]:
        if autoplay_enabled.get(guild_id, True):
            next_url, next_title = await buscar_recomendacao_autoplay(guild_id, ctx)
            if next_url:
                logging.info(f"Fila vazia. Adicionando música recomendada: {next_url}")
                preset_name = active_preset.get(guild_id, 'padrao')
                play_queue[guild_id].append((next_url, preset_name))
                if next_title:
                    await ctx.send(f"Fila vazia. Auto-play escolheu: **{next_title}**")
                else:
                    await ctx.send("Fila vazia. Reproduzindo uma música recomendada.")
            else:
                await ctx.send(
                    "Auto-play não encontrou recomendação no momento. Vou permanecer no canal; "
                    "tente `!skip` novamente ou use `!play <url>`."
                )
                return
        else:
            await ctx.send("A fila de músicas está vazia. Desconectando do canal de voz.")
            await vc.disconnect()
            return

    url, preset_name = play_queue[guild_id].popleft()
    source, stream_title, info = await stream_musica(url, preset_name)

    if source:
        previous_info = last_played_info.get(guild_id) or {}
        track_info = info or {}
        track_info['requested_url'] = url
        if (
            'related_videos' not in track_info
            and previous_info.get('requested_url') == url
            and previous_info.get('related_videos')
        ):
            track_info['related_videos'] = previous_info.get('related_videos')
        last_played_info[guild_id] = track_info
        _registrar_faixa_tocada(
            guild_id,
            stream_title,
            (track_info.get('uploader') or track_info.get('channel') or track_info.get('artist') or '')
        )
        await _salvar_historico_mongodb(
            ctx,
            stream_title,
            url,
            (track_info.get('uploader') or track_info.get('channel') or track_info.get('artist') or '')
        )
        try:
            vc.play(source, after=lambda e: asyncio.run_coroutine_threadsafe(
                tocar_proxima_musica(vc, guild_id, ctx), ctx.bot.loop))
            await ctx.send(f'Transmitindo agora: **{stream_title}** com preset `{preset_name}`')
        except Exception as e:
            logging.error(f"Erro ao transmitir `{stream_title}`: {str(e)}")
            await ctx.send(f"Erro ao transmitir `{stream_title}`: {str(e)}")
            await tocar_proxima_musica(vc, guild_id, ctx)
    else:
        await ctx.send(f"Erro ao processar o stream. Pulando para a próxima música da fila.")
        await tocar_proxima_musica(vc, guild_id, ctx)


class MusicCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name='play')
    async def play(self, ctx, *input_parts):
        """Toca uma música ou adiciona à fila
        Uso: !play <artista e musica>"""
        # 1. Validar canal correto
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        # 2. Validar se o termo de busca foi fornecido
        query_or_url = " ".join(input_parts).strip()
        if not query_or_url:
            await ctx.send("Ei! Você precisa me dar um termo de busca. Uso correto: `!play <artista e musica>`")
            return

        # 3. Validar se está em um canal de voz
        if not ctx.author.voice:
            await ctx.send("Quer que eu adivinhe o canal para tocar musica ?, conecte-se a um canal de voz primeiro.")
            return

        guild_id = ctx.guild.id
        if guild_id not in play_queue:
            play_queue[guild_id] = deque()

        if not ctx.guild.voice_client:
            try:
                vc = await ctx.author.voice.channel.connect(reconnect=True, self_deaf=True)
            except Exception as e:
                logging.error(f'Erro ao conectar ao canal de voz: {e}')
                await ctx.send("Não foi possível conectar ao canal de voz.")
                return
        else:
            vc = ctx.guild.voice_client

        preset_name = active_preset.get(guild_id, 'padrao')

        try:
            async with ctx.typing():
                entries = await _buscar_musica_ou_artista(query_or_url, max_results=12)

            if not entries:
                await ctx.send("Não encontrei resultados para sua busca. Tente outro termo.")
                return

            selected_entry = None
            selected_url = None
            for entry in entries:
                candidate_url = _entry_to_youtube_url(entry)
                if candidate_url:
                    selected_entry = entry
                    selected_url = candidate_url
                    break

            if not selected_url:
                await ctx.send("Encontrei resultados, mas nenhum vídeo reproduzível. Tente outra busca.")
                return

            selected_title = _entry_title(selected_entry) or 'Resultado sem titulo'
            selected_artist = _entry_artist(selected_entry, keys=ENTRY_ARTIST_KEYS_LIGHT)
            artist_clean = _inferir_artista_para_recomendacao(selected_entry, entries, query_or_url)
            if not artist_clean and '-' in selected_title:
                artist_clean = _limpar_nome_artista(selected_title.split('-', 1)[0].strip())

            play_queue[guild_id].append((selected_url, preset_name))

            # Busca recomendações via Last.fm se disponível, senão usa resultados do YouTube
            related_candidates = []
            seen_related_titles = [selected_title]

            if LASTFM_API_KEY:
                similar_tracks = await asyncio.to_thread(_buscar_similar_lastfm, selected_title, artist_clean, limit=10)
                similar_tracks = await asyncio.to_thread(_filtrar_similares_por_tematica, artist_clean, similar_tracks)
                for sim_artist, sim_track in similar_tracks:
                    display = f"{sim_artist} - {sim_track}"
                    if any(_titulo_equivalente(sim_track, seen) for seen in seen_related_titles):
                        continue
                    seen_related_titles.append(sim_track)
                    related_candidates.append({
                        'url': None,
                        'title': display,
                        'uploader': sim_artist,
                        '_lastfm': True,
                    })
                    if len(related_candidates) >= 8:
                        break

            # Fallback: usar resultados do YouTube quando Last.fm não está disponível ou não retornou
            if not related_candidates:
                # Busca explícita por músicas do mesmo artista para evitar resultados não-musicais
                fallback_query = f"{artist_clean} official" if artist_clean else f"{selected_title} official"
                fallback_entries = await _buscar_recomendacoes_musicais(
                    fallback_query,
                    max_results=10,
                    artist_hint=artist_clean,
                )
                # Filtra apenas entradas com URL válida e que não sejam variações da música atual
                for entry in fallback_entries:
                    candidate_url = _entry_to_youtube_url(entry)
                    if not candidate_url or candidate_url == selected_url:
                        continue
                    candidate_title = _entry_title(entry)
                    if not candidate_title:
                        continue
                    if any(_titulo_equivalente(candidate_title, seen) for seen in seen_related_titles):
                        continue
                    seen_related_titles.append(candidate_title)
                    related_candidates.append({
                        'url': candidate_url,
                        'title': candidate_title,
                        'uploader': _entry_artist(entry, keys=ENTRY_ARTIST_KEYS_LIGHT)
                    })
                    if len(related_candidates) >= 8:
                        break

            # Para o autoplay, usa apenas entradas com URL válida do YouTube
            related_videos_for_autoplay = [r for r in related_candidates if r.get('url')]

            # Salva recomendações iniciais para o autoplay usar quando a fila acabar.
            last_played_info[guild_id] = {
                'requested_url': selected_url,
                'title': selected_title,
                'uploader': selected_artist,
                'related_videos': related_videos_for_autoplay,
            }

            sugestoes_txt = "\n".join(
                [f"{i + 1}. {item.get('title', 'Desconhecido')}" for i, item in enumerate(related_candidates[:5])]
            )
            if sugestoes_txt:
                fonte = "Last.fm" if LASTFM_API_KEY and any(r.get('_lastfm') for r in related_candidates) else "YouTube"
                await ctx.send(
                    f"Resultado da busca: **{selected_title}**\n"
                    f"Adicionado à fila com preset `{preset_name}`\n"
                    f"Próximas recomendações ({fonte}):\n{sugestoes_txt}"
                )
            else:
                await ctx.send(f"Resultado da busca: **{selected_title}**\nAdicionado à fila com preset `{preset_name}`")

            if not vc.is_playing() and not vc.is_paused():
                await tocar_proxima_musica(vc, guild_id, ctx)

        except Exception as e:
            logging.error(f'Erro ao processar música: {e}')
            await ctx.send("Ocorreu um erro ao processar a música.")

    @commands.command(name='stop')
    async def stop(self, ctx):
        """Para a reprodução atual"""
        if ctx.guild.voice_client and ctx.guild.voice_client.is_playing():
            guild_id = ctx.guild.id
            manual_stop_guilds.add(guild_id)
            if guild_id in play_queue:
                play_queue[guild_id].clear()
            ctx.guild.voice_client.stop()
            await ctx.send("Reprodução parada.")
        else:
            await ctx.send("Nenhuma música está tocando.")

    @commands.command(name='skip')
    async def skip(self, ctx):
        """Pula a música atual"""
        if ctx.guild.voice_client and ctx.guild.voice_client.is_playing():
            ctx.guild.voice_client.stop()
            await ctx.send("Música pulada!")
        else:
            await ctx.send("Nenhuma música está tocando.")

    @commands.command(name='leave')
    async def leave(self, ctx):
        """Faz o bot sair do canal de voz"""
        if ctx.guild.voice_client:
            guild_id = ctx.guild.id
            if guild_id in play_queue:
                play_queue[guild_id].clear()
            autoplay_enabled.pop(guild_id, None)
            last_played_info.pop(guild_id, None)
            active_preset.pop(guild_id, None)
            autoplay_recent_urls.pop(guild_id, None)
            recent_played_titles.pop(guild_id, None)
            recent_played_uploaders.pop(guild_id, None)
            manual_stop_guilds.discard(guild_id)
            await ctx.guild.voice_client.disconnect()
            await ctx.send("Desconectado do canal de voz.")
        else:
            await ctx.send("Não estou em nenhum canal de voz.")

    @commands.command(name='preset')
    async def preset(self, ctx, mode: str = None):
        """Controla o preset da equalização: padrao, bassboost, pop ou rock.

        Uso: !preset [padrao|bassboost|pop|rock|status]
        """
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        guild_id = ctx.guild.id
        current = active_preset.get(guild_id, 'padrao')

        if mode is None or mode.lower() == 'status':
            await ctx.send(f"Preset atual: **{current}**")
            return

        mode = mode.lower()
        if mode not in ('padrao', 'bassboost', 'pop', 'rock'):
            await ctx.send("Uso inválido. Use `!preset padrao`, `!preset bassboost`, `!preset pop`, `!preset rock` ou `!preset status`.")
            return

        if mode not in EQUALIZER_PRESETS:
            await ctx.send(f"Preset `{mode}` não está configurado no bot.")
            return

        active_preset[guild_id] = mode
        await ctx.send(f"Preset alterado para **{mode}**. Novas músicas usarão esse preset.")

    @commands.command(name='autoplay')
    async def autoplay(self, ctx, mode: str = None):
        """Controla o auto-play: on, off ou status.

        Uso: !autoplay [on|off|status]
        """
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        guild_id = ctx.guild.id
        status = autoplay_enabled.get(guild_id, True)

        if mode is None or mode.lower() == 'status':
            await ctx.send(f"Auto-play está **{'ativado' if status else 'desativado'}** neste servidor.")
            return

        mode = mode.lower()
        if mode in ('on', 'ligar', 'ativar', 'true', '1'):
            autoplay_enabled[guild_id] = True
            await ctx.send("Auto-play ativado.")
            return

        if mode in ('off', 'desligar', 'desativar', 'false', '0'):
            autoplay_enabled[guild_id] = False
            await ctx.send("Auto-play desativado.")
            return

        await ctx.send("Uso inválido. Use `!autoplay on`, `!autoplay off` ou `!autoplay status`.")

    @commands.command(name='profile')
    async def profile(self, ctx):
        """Mostra o perfil musical do usuário"""
        user_profile = await db.get_user_profile(str(ctx.author.id))
        if not user_profile:
            await ctx.send("Você ainda não tem um perfil! Use o comando `!play` para começar.")
            return

        history_text = "\n".join([
            f"• {song.title}" + (f" - {song.artist}" if song.artist else "") +
            f" ({song.played_at.strftime('%d/%m/%Y %H:%M')})"
            for song in user_profile.music_history[-5:]
        ]) or "Nenhuma música tocada ainda"

        top_artists = await db.get_top_preferences(str(ctx.author.id), 'artist', 5)
        top_genres = await db.get_top_preferences(str(ctx.author.id), 'genre', 5)

        artists_text = "\n".join([
            f"• {pref.name} ({pref.count} músicas)"
            for pref in top_artists
        ]) or "Nenhum artista definido"

        genres_text = "\n".join([
            f"• {pref.name} ({pref.count} músicas)"
            for pref in top_genres
        ]) or "Nenhum gênero definido"

        embed = discord.Embed(
            title=f"Perfil Musical de {user_profile.username}",
            color=0x00ff00,
            timestamp=user_profile.created_at
        )
        embed.add_field(name="📜 Histórico Recente", value=history_text, inline=False)
        embed.add_field(name="🎤 Artistas Favoritos", value=artists_text, inline=True)
        embed.add_field(name="🎵 Gêneros Favoritos", value=genres_text, inline=True)
        embed.set_footer(text="Perfil criado em")

        await ctx.send(embed=embed)

    @commands.command(name='recommend')
    async def recommend(self, ctx):
        """Recomenda músicas baseadas nas preferências"""
        user_profile = await db.get_user_profile(str(ctx.author.id))
        if not user_profile or not user_profile.music_preferences:
            await ctx.send("Você precisa ter preferências musicais registradas!")
            return

        top_prefs = sorted(user_profile.music_preferences, key=lambda x: x.count, reverse=True)[:3]
        search_terms = " OR ".join([f"\"{pref.name}\"" for pref in top_prefs])

        try:
            with yt_dlp.YoutubeDL({
                'quiet': True,
                'extract_flat': True,
                'default_search': 'ytsearch5'
            }) as ydl:
                result = await asyncio.to_thread(
                    ydl.extract_info,
                    f"ytsearch5:{search_terms.replace('&', 'and')}",
                    download=False
                )

                if result and 'entries' in result:
                    embed = discord.Embed(
                        title="🎵 Recomendações Musicais",
                        description=f"Com base em: {', '.join([p.name for p in top_prefs])}",
                        color=0x00ff00
                    )

                    for entry in result['entries'][:5]:
                        if entry:
                            embed.add_field(
                                name=entry.get('title', 'Sem título'),
                                value=f"[Tocar no YouTube]({entry.get('url', '')})",
                                inline=False
                            )

                    await ctx.send(embed=embed)
                else:
                    await ctx.send("Não foi possível encontrar recomendações.")
        except Exception as e:
            logging.error(f"Erro ao buscar recomendações: {str(e)}")
            await ctx.send("Erro ao buscar recomendações. Tente novamente.")

    @commands.command(name='reproduzir_historico')
    async def reproduzir_historico(self, ctx, count: int = 5, *flags):
        """Adiciona o histórico de reprodução do perfil do usuário à fila.

        Uso: !reproduzir_historico [count=5] [append] [search]
        - count: quantas músicas do histórico serão adicionadas (padrão 5)
        - append: se presente, adiciona as músicas ao final da fila; caso contrário, são inseridas para tocar em seguida
        - search: se presente, tenta buscar músicas pelo título quando uma entrada do histórico não tiver URL
        Ex.: `!reproduzir_historico 10 append search`
        """
        # Validar canal de comandos
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        # Certificar que o usuário está em um canal de voz
        if not ctx.author.voice:
            await ctx.send("Conecte-se a um canal de voz primeiro para reproduzir seu histórico.")
            return

        user_profile = await db.get_user_profile(str(ctx.author.id))
        if not user_profile or not user_profile.music_history:
            await ctx.send("Nenhum histórico de reprodução encontrado no seu perfil.")
            return

        guild_id = ctx.guild.id
        if guild_id not in play_queue:
            play_queue[guild_id] = deque()

        # Conectar ao canal de voz se necessário
        if not ctx.guild.voice_client:
            try:
                vc = await ctx.author.voice.channel.connect(reconnect=True, self_deaf=True)
            except Exception as e:
                logging.error(f'Erro ao conectar ao canal de voz: {e}')
                await ctx.send("Não foi possível conectar ao canal de voz.")
                return
        else:
            vc = ctx.guild.voice_client

        # Flags de modo (aceita várias flags, ex: 'append' 'search')
        flag_set = {f.lower() for f in flags}
        append_mode = 'append' in flag_set
        fallback_search = 'search' in flag_set or 'fallback' in flag_set

        # Pega as últimas `count` músicas (ordem cronológica: do mais antigo ao mais recente)
        songs = user_profile.music_history[-count:]

        # Construir lista de URLs candidatas (respeitando ordem)
        existing_urls = set()
        for entry in play_queue[guild_id]:
            try:
                existing_urls.add(clean_youtube_url(entry[0]))
            except Exception:
                pass

        candidates = []  # list of (url, title)
        added_urls = set()

        for s in songs:
            title = getattr(s, 'title', None) or ''
            url = getattr(s, 'url', None)
            chosen_url = None

            # Use URL from history when valid
            if url:
                try:
                    cleaned = clean_youtube_url(url)
                except Exception:
                    cleaned = None
                if cleaned and is_youtube_url(cleaned):
                    chosen_url = cleaned

            # Fallback: buscar por título usando yt_dlp (opcional)
            if not chosen_url and fallback_search and title:
                try:
                    with yt_dlp.YoutubeDL(
                            {'quiet': True, 'extract_flat': 'True', 'default_search': 'ytsearch1'}) as ydl:
                        info = await asyncio.to_thread(ydl.extract_info, f"ytsearch1:{title}", download=False)
                    if info and 'entries' in info and info['entries']:
                        entry = info['entries'][0]
                        # Try common fields that may contain a usable url/id
                        maybe = entry.get('url') or entry.get('webpage_url') or entry.get('id')
                        if maybe:
                            try:
                                cleaned = clean_youtube_url(maybe)
                            except Exception:
                                cleaned = None
                            if cleaned and is_youtube_url(cleaned):
                                chosen_url = cleaned
                except Exception as e:
                    logging.warning(f"Busca por título falhou para '{title}': {e}")

            # Se ainda não encontramos uma url válida, pula
            if not chosen_url:
                continue

            # Dedupe: pula se já existe na fila ou foi selecionada previamente
            if chosen_url in existing_urls or chosen_url in added_urls:
                continue

            candidates.append((chosen_url, title))
            added_urls.add(chosen_url)

        if not candidates:
            await ctx.send("Nenhuma música válida encontrada no seu histórico para adicionar (ou já estão na fila).")
            return

        # Montar mensagem de confirmação com a lista de títulos
        lines = []
        for i, (u, t) in enumerate(candidates, start=1):
            display_title = t or u
            lines.append(f"{i}. {display_title}")
        preview = "\n".join(lines[:20])  # limitar

        await ctx.send(
            f"Vou adicionar {len(candidates)} músicas do seu histórico:\n{preview}\n\nResponda 'sim' para confirmar (30s) ou qualquer outra coisa para cancelar.")

        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        try:
            reply = await self.bot.wait_for('message', check=check, timeout=30)
        except asyncio.TimeoutError:
            await ctx.send("Tempo esgotado. Operação cancelada.")
            return

        if reply.content.lower() not in ('sim', 's', 'yes', 'y'):
            await ctx.send("Operação cancelada pelo usuário.")
            return

        # Inserir na fila conforme modo
        added_count = 0
        preset_name = active_preset.get(guild_id, 'padrao')
        if append_mode:
            for (u, t) in candidates:
                play_queue[guild_id].append((u, preset_name))
                added_count += 1
        else:
            # inserir para tocar em seguida: percorre em ordem cronológica e appendleft
            # candidates are in chronological order because songs were iterated that way
            for (u, t) in candidates:
                play_queue[guild_id].appendleft((u, preset_name))
                added_count += 1

        await ctx.send(f'✅ Adicionados {added_count} músicas do seu histórico à fila.' + (
            " (no final)" if append_mode else " (serão reproduzidas em seguida)"))

        # Se não está tocando nada, inicia a reprodução
        if not vc.is_playing() and not vc.is_paused():
            await tocar_proxima_musica(vc, guild_id, ctx)
