"""
music/commands.py
=================
Comandos de música para o bot Discord: play, skip, stop, leave, preset,
autoplay, profile, recommend e reproduzir_historico.

Depende de:
  - yt_dlp, ytmusicapi (opcional), discord.py, requests
  - db.database.db
  - config.settings (EQUALIZER_PRESETS, LASTFM_API_KEY)
  - .utils (clean_youtube_url, is_youtube_url, stream_musica)
  - .commands_utils (validar_canal, play_queue, last_played_info,
                     autoplay_enabled, active_preset)
"""

from __future__ import annotations

import asyncio
import difflib
import logging
import random
import re
import urllib.parse
from collections import deque
from typing import Optional

import discord
import requests
import yt_dlp
from discord.ext import commands

try:
    from ytmusicapi import YTMusic
except Exception:
    YTMusic = None

from db.database import db
from config.settings import EQUALIZER_PRESETS, LASTFM_API_KEY
from .utils import clean_youtube_url, is_youtube_url, stream_musica
from .commands_utils import (
    active_preset,
    autoplay_enabled,
    last_played_info,
    play_queue,
    validar_canal,
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

LASTFM_API_URL = "https://ws.audioscrobbler.com/2.0/"

MAX_AUTOPLAY_RECENTES = 20
MAX_RECENT_TITLES = 12
MAX_RECENT_UPLOADERS = 6
MAX_SANITIZED_TEXT_LEN = 256

# Chaves de entrada do yt_dlp que podem conter uma URL de vídeo
ENTRY_URL_KEYS = ("url", "webpage_url", "original_url", "id")

# Chaves de entrada que identificam o artista/canal (completo e leve)
ENTRY_ARTIST_KEYS = ("uploader", "channel", "channel_name", "artist")
ENTRY_ARTIST_KEYS_LIGHT = ("uploader", "channel", "artist")

# ---------------------------------------------------------------------------
# Estado em memória (por guild)
# ---------------------------------------------------------------------------

autoplay_recent_urls: dict[int, deque] = {}
recent_played_titles: dict[int, deque] = {}
recent_played_uploaders: dict[int, deque] = {}
manual_stop_guilds: set[int] = set()

# Cache de tags Last.fm por artista normalizado
_lastfm_artist_tags_cache: dict[str, set[str]] = {}

# Cliente YTMusic singleton (inicializado sob demanda)
_ytmusic_client: Optional["YTMusic"] = None

# ---------------------------------------------------------------------------
# Logging inicial
# ---------------------------------------------------------------------------

if LASTFM_API_KEY:
    logging.info(
        f"[Last.fm] API key carregada com sucesso "
        f"(primeiros 6 chars: {LASTFM_API_KEY[:6]}...)"
    )
else:
    logging.warning(
        "[Last.fm] LASTFM_API_KEY não configurada — "
        "recomendações via Last.fm desativadas."
    )


# ===========================================================================
# Helpers de texto / sanitização
# ===========================================================================


def _sanitize_text(value, max_len: int = MAX_SANITIZED_TEXT_LEN) -> str:
    """Remove caracteres de controle e limita o tamanho do texto externo."""
    if value is None:
        return ""
    text = re.sub(r"[\x00-\x1f\x7f]", " ", str(value))
    text = " ".join(text.split())
    if max_len > 0 and len(text) > max_len:
        text = text[:max_len].rstrip()
    return text


def _normalize_text(value: str) -> str:
    return _sanitize_text(value).lower()


def _normalize_tag(value: str) -> str:
    return re.sub(r"[^a-z0-9\s]", "", _normalize_text(value))


def _clean_artist_name(uploader: str) -> str:
    """Remove sufixos do YouTube Music como ' - Topic' e 'VEVO'."""
    if not uploader:
        return ""
    uploader = re.sub(r"\s*-\s*Topic\s*$", "", uploader, flags=re.IGNORECASE)
    uploader = re.sub(r"\s*VEVO\s*$", "", uploader, flags=re.IGNORECASE)
    return uploader.strip()


# ===========================================================================
# Helpers de URL do YouTube
# ===========================================================================

_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


def _is_video_id(value: str) -> bool:
    return bool(value and _VIDEO_ID_RE.fullmatch(value))


def _extract_video_id(url: str) -> Optional[str]:
    """Extrai o ID de 11 caracteres de uma URL do YouTube."""
    try:
        parsed = urllib.parse.urlparse(url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").strip("/")

        if host in ("youtu.be", "www.youtu.be"):
            vid = path.split("/")[0] if path else ""
            return vid if _is_video_id(vid) else None

        if "youtube.com" in host or "music.youtube.com" in host:
            qs = urllib.parse.parse_qs(parsed.query)
            if "v" in qs and _is_video_id(qs["v"][0]):
                return qs["v"][0]
            segments = path.split("/") if path else []
            if len(segments) >= 2 and segments[0] in ("shorts", "embed", "live"):
                vid = segments[1]
                return vid if _is_video_id(vid) else None
    except Exception:
        pass
    return None


def _normalize_youtube_url(candidate) -> Optional[str]:
    """Converte diferentes formatos para URL canônica de vídeo do YouTube."""
    if not candidate:
        return None
    value = str(candidate).strip()
    if not value:
        return None

    if value.startswith("http://") or value.startswith("https://"):
        if not is_youtube_url(value):
            return None
        vid = _extract_video_id(value)
        return f"https://www.youtube.com/watch?v={vid}" if vid else None

    if _is_video_id(value):
        return f"https://www.youtube.com/watch?v={value}"

    return None


# ===========================================================================
# Helpers de entrada do yt_dlp
# ===========================================================================


def _entry_title(entry: dict) -> str:
    if not isinstance(entry, dict):
        return ""
    return (entry.get("title") or "").strip()


def _entry_artist(
    entry: dict,
    keys: tuple = ENTRY_ARTIST_KEYS,
    normalize: bool = False,
) -> str:
    if not isinstance(entry, dict):
        return ""
    for key in keys:
        val = entry.get(key)
        if val:
            text = str(val).strip()
            return _normalize_text(text) if normalize else text
    return ""


def _entry_uploader_normalized(entry: dict) -> str:
    return _entry_artist(entry, keys=ENTRY_ARTIST_KEYS, normalize=True)


def _entry_to_youtube_url(entry: dict) -> Optional[str]:
    if not isinstance(entry, dict):
        return None
    for key in ENTRY_URL_KEYS:
        url = _normalize_youtube_url(entry.get(key))
        if url:
            return url
    return None


# ===========================================================================
# Similaridade de títulos
# ===========================================================================

_NOISE_WORDS = (
    "official", "video", "audio", "lyrics", "lyric", "clipe", "clip", "live",
    "legendado", "hd", "4k", "remastered", "version", "versao", "ptbr", "pt-br",
    "remix", "acoustic", "cover", "instrumental", "extended", "mix", "edit",
    "deluxe", "anniversary", "original", "explicit", "clean", "radio", "single",
    "album", "ost", "soundtrack", "slowed", "reverb", "sped", "nightcore",
    "visualizer", "performance", "session", "unplugged", "demo", "bonus",
)

_STOP_WORDS = {"the", "a", "an", "of", "and", "in", "on", "feat", "ft", "with", "by", "official"}


def _canonical_title(title: str) -> str:
    """Normaliza variações comuns do mesmo vídeo/música para comparação."""
    text = _normalize_text(title)
    if not text:
        return ""
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"\([^)]*\)", " ", text)
    for word in _NOISE_WORDS:
        text = re.sub(rf"\b{re.escape(word)}\b", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def _token_set(title: str) -> set[str]:
    return {t for t in _canonical_title(title).split() if t and t not in _STOP_WORDS}


def _titles_equivalent(a: str, b: str) -> bool:
    """True quando dois títulos representam o mesmo conteúdo (sequência ou Jaccard)."""
    ca, cb = _canonical_title(a), _canonical_title(b)
    if not ca or not cb:
        return False
    if difflib.SequenceMatcher(None, ca, cb).ratio() >= 0.65:
        return True
    ta, tb = _token_set(ca), _token_set(cb)
    if not ta or not tb:
        return False
    return len(ta & tb) / len(ta | tb) >= 0.50


# Alias — mesma lógica, nome semântico diferente para detectar repetições.
_titles_too_similar = _titles_equivalent


def _text_similar(a: str, b: str) -> bool:
    """Comparação rápida de textos curtos (artistas/canais)."""
    aa, bb = _normalize_text(a), _normalize_text(b)
    if not aa or not bb:
        return False
    if aa in bb or bb in aa:
        return True
    return difflib.SequenceMatcher(None, aa, bb).ratio() >= 0.70


# ===========================================================================
# Heurísticas de conteúdo
# ===========================================================================

_NON_MUSIC_MARKERS = (
    "podcast", "interview", "entrevista", "aula", "lesson", "english",
    "how to", "tutorial", "documentary", "documentario", "reaction",
    "review", "news", "noticias", "analysis", "analise", "speech",
    "debate", "live stream", "livestream", "audiobook",
)

_MUSIC_MARKERS = (
    "official", "audio", "lyrics", "lyric", "music", "song", "clipe", "clip",
    "live", "remix", "cover", "topic", "mv", "amv", "visualizer", "feat", "ft",
)


def _looks_non_musical(title: str) -> bool:
    t = _normalize_text(title)
    return not t or any(m in t for m in _NON_MUSIC_MARKERS)


def _has_music_marker(title: str) -> bool:
    t = _normalize_text(title)
    return bool(t) and any(m in t for m in _MUSIC_MARKERS)


# ===========================================================================
# YTMusic client (singleton)
# ===========================================================================


def _get_ytmusic_client() -> Optional["YTMusic"]:
    global _ytmusic_client
    if YTMusic is None:
        return None
    if _ytmusic_client is None:
        try:
            _ytmusic_client = YTMusic()
        except Exception as exc:
            logging.warning(f"Falha ao inicializar ytmusicapi: {exc}")
    return _ytmusic_client


# ===========================================================================
# Buscas no YouTube / YouTube Music
# ===========================================================================


async def _search_yt_music_api(query: str, max_results: int) -> list[dict]:
    """Busca músicas via ytmusicapi (retorna lista de entradas normalizadas)."""
    client = _get_ytmusic_client()
    if not client:
        return []
    try:
        raw = await asyncio.to_thread(client.search, query, filter="songs", limit=max_results)
        entries = []
        for item in raw or []:
            vid = _sanitize_text(item.get("videoId"), max_len=32)
            title = _sanitize_text(item.get("title"))
            if not vid or not title or _looks_non_musical(title):
                continue
            artists = item.get("artists") or []
            artist = _sanitize_text(artists[0].get("name")) if artists else ""
            entries.append({
                "url": f"https://www.youtube.com/watch?v={vid}",
                "title": title,
                "uploader": artist,
            })
        return entries
    except Exception as exc:
        logging.warning(f"Falha na busca ytmusicapi para '{query}': {exc}")
        return []


async def _search_ytdlp(query: str, max_results: int) -> list[dict]:
    """Busca via yt_dlp: tenta endpoint do YT Music, depois fallback ytsearch."""
    encoded = urllib.parse.quote_plus(query)
    ytm_url = f"https://music.youtube.com/search?q={encoded}"

    # Tentativa 1: endpoint do YouTube Music
    try:
        with yt_dlp.YoutubeDL({"quiet": True, "extract_flat": True}) as ydl:
            result = await asyncio.to_thread(ydl.extract_info, ytm_url, download=False)
        if result and isinstance(result, dict) and result.get("entries"):
            return [e for e in result["entries"] if e]
    except Exception as exc:
        logging.info(f"Busca no YT Music falhou para '{query}': {exc}")

    # Tentativa 2: ytsearch
    safe_query = query.replace("&", "and")
    try:
        opts = {
            "quiet": True,
            "extract_flat": True,
            "default_search": f"ytsearch{max_results}",
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            result = await asyncio.to_thread(
                ydl.extract_info,
                f"ytsearch{max_results}:{safe_query}",
                download=False,
            )
        if result and isinstance(result, dict) and result.get("entries"):
            return [e for e in result["entries"] if e]
    except Exception as exc:
        logging.warning(f"Falha no fallback ytsearch para '{query}': {exc}")

    return []


async def _search_music(query: str, max_results: int = 10, artist_hint: str = "") -> list[dict]:
    """
    Busca recomendações priorizando o catálogo do YouTube Music.
    Fallback via yt_dlp com filtros de qualidade.
    """
    q = _sanitize_text(query, max_len=160)
    if not q:
        return []

    # Caminho principal: ytmusicapi
    entries = await _search_yt_music_api(q, max_results)
    if entries:
        return entries

    # Fallback: yt_dlp com heurísticas de filtro
    hint = _normalize_text(artist_hint)
    raw = await _search_ytdlp(f"{q} official audio", max_results)
    result = []
    for item in raw:
        title = _entry_title(item)
        if not title or _looks_non_musical(title):
            continue
        uploader = _entry_artist(item, keys=ENTRY_ARTIST_KEYS_LIGHT)
        uploader_matches = hint and _text_similar(uploader, hint)
        if not _has_music_marker(title) and not uploader_matches:
            continue
        url = _entry_to_youtube_url(item)
        if not url:
            continue
        result.append({"url": url, "title": title, "uploader": uploader})
    return result


# ===========================================================================
# Controle de histórico por guild
# ===========================================================================


def _register_autoplay_url(guild_id: int, url: str) -> None:
    cleaned = _normalize_youtube_url(url)
    if not cleaned:
        return
    autoplay_recent_urls.setdefault(guild_id, deque(maxlen=MAX_AUTOPLAY_RECENTES))
    autoplay_recent_urls[guild_id].append(cleaned)


def _register_played_track(guild_id: int, title: str, uploader: str) -> None:
    recent_played_titles.setdefault(guild_id, deque(maxlen=MAX_RECENT_TITLES))
    if title:
        recent_played_titles[guild_id].append(title)

    recent_played_uploaders.setdefault(guild_id, deque(maxlen=MAX_RECENT_UPLOADERS))
    uploader_norm = _normalize_text(uploader)
    if uploader_norm:
        recent_played_uploaders[guild_id].append(uploader_norm)


# ===========================================================================
# Seleção de candidatos com diversidade
# ===========================================================================

# Tipo: (url, title, uploader_normalized)
Candidate = tuple[str, str, str]


def _pick_diverse_candidate(
    candidates: list[Candidate],
    guild_id: int,
    last_title: str,
    last_uploader: str,
    extra_titles: list[str] | None = None,
    extra_uploaders: list[str] | None = None,
) -> tuple[Optional[str], Optional[str]]:
    """Escolhe um candidato privilegiando diversidade de título e uploader."""
    if not candidates:
        return None, None

    history_titles = list(recent_played_titles.get(guild_id, []))
    uploader_history = list(recent_played_uploaders.get(guild_id, []))
    if extra_titles:
        history_titles.extend(extra_titles)
    if extra_uploaders:
        uploader_history.extend(_normalize_text(u) for u in extra_uploaders if u)

    recent_uploader_window = set(uploader_history[-2:])

    tier_diverse, tier_medium, tier_fallback = [], [], []

    for url, title, uploader in candidates:
        if any(_titles_equivalent(title, old) for old in history_titles):
            continue  # já ouvido recentemente

        same_uploader = bool(last_uploader and uploader and uploader == last_uploader)
        similar_title = _titles_too_similar(last_title, title)
        recent_uploader = bool(uploader and uploader in recent_uploader_window)

        if not same_uploader and not similar_title and not recent_uploader:
            tier_diverse.append((url, title))
        elif not similar_title and not recent_uploader:
            tier_medium.append((url, title))
        else:
            tier_fallback.append((url, title))

    for tier in (tier_diverse, tier_medium, tier_fallback):
        if tier:
            return random.choice(tier)

    return None, None


# ===========================================================================
# Last.fm
# ===========================================================================


def _lastfm_artist_tags(artist: str, limit: int = 8) -> set[str]:
    """Retorna tags principais de um artista no Last.fm (com cache em memória)."""
    artist = _sanitize_text(artist)
    artist_norm = _normalize_text(artist)
    if not LASTFM_API_KEY or not artist_norm:
        return set()

    if artist_norm in _lastfm_artist_tags_cache:
        return _lastfm_artist_tags_cache[artist_norm]

    try:
        resp = requests.get(
            LASTFM_API_URL,
            params={
                "method": "artist.getTopTags",
                "artist": artist,
                "api_key": LASTFM_API_KEY,
                "format": "json",
            },
            timeout=5,
        )
        resp.raise_for_status()
        tags_raw = resp.json().get("toptags", {}).get("tag", [])
        tags = {_normalize_tag(t.get("name") or "") for t in tags_raw[:limit]} - {""}
    except Exception as exc:
        logging.warning(f"[Last.fm] Falha ao buscar tags do artista '{artist}': {exc}")
        tags = set()

    _lastfm_artist_tags_cache[artist_norm] = tags
    return tags


def _lastfm_similar_tracks(
    track_title: str,
    artist: str,
    limit: int = 20,
) -> list[tuple[str, str]]:
    """Chama Last.fm track.getSimilar e retorna lista de (artista, título)."""
    track_title = _sanitize_text(track_title)
    artist = _sanitize_text(artist)
    if not LASTFM_API_KEY or not track_title or not artist:
        logging.warning(
            f"[Last.fm] Chamada ignorada — key={bool(LASTFM_API_KEY)}, "
            f"title='{track_title}', artist='{artist}'"
        )
        return []

    logging.info(f"[Last.fm] Buscando similares para: '{track_title}' de '{artist}'")
    try:
        resp = requests.get(
            LASTFM_API_URL,
            params={
                "method": "track.getSimilar",
                "track": track_title,
                "artist": artist,
                "api_key": LASTFM_API_KEY,
                "format": "json",
                "limit": limit,
            },
            timeout=5,
        )
        resp.raise_for_status()
        tracks = resp.json().get("similartracks", {}).get("track", [])
        results = [
            (_sanitize_text((t.get("artist", {}) or {}).get("name")), _sanitize_text(t.get("name")))
            for t in tracks
        ]
        results = [(a, n) for a, n in results if a and n]
        logging.info(f"[Last.fm] {len(results)} faixas similares encontradas para '{track_title}'")
        return results
    except Exception as exc:
        logging.warning(f"[Last.fm] Falha ao buscar similares: {exc}")
        return []


def _filter_similar_by_theme(
    seed_artist: str,
    similar_tracks: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Filtra similares fora do estilo dominante do artista base."""
    if not similar_tracks or not seed_artist or not LASTFM_API_KEY:
        return similar_tracks

    seed_tags = _lastfm_artist_tags(seed_artist)
    if not seed_tags:
        return similar_tracks

    seed_is_rock = any(t in seed_tags for t in ("rock", "alternative rock", "metal", "nu metal", "hard rock"))
    seed_is_pop = any(t in seed_tags for t in ("pop", "dance pop", "electropop"))

    themed, fallback = [], []
    for sim_artist, sim_track in similar_tracks:
        sim_tags = _lastfm_artist_tags(sim_artist)
        if not sim_tags:
            fallback.append((sim_artist, sim_track))
            continue

        shared = seed_tags & sim_tags
        if seed_is_rock and any(t in sim_tags for t in ("kpop", "k-pop", "jpop", "j-pop")) and not shared:
            continue
        if seed_is_pop and any(t in sim_tags for t in ("death metal", "black metal", "grindcore")) and not shared:
            continue

        (themed if shared else fallback).append((sim_artist, sim_track))

    return themed if themed else similar_tracks


# ===========================================================================
# Inferência de artista
# ===========================================================================


def _infer_artist(
    selected_entry: dict,
    entries: list[dict],
    user_query: str = "",
) -> str:
    """Tenta inferir o artista para melhorar a qualidade das recomendações."""
    if isinstance(selected_entry, dict):
        direct = _entry_artist(selected_entry, keys=ENTRY_ARTIST_KEYS_LIGHT)
        if direct:
            return _clean_artist_name(direct)

    if isinstance(entries, list):
        counts: dict[str, int] = {}
        for e in entries:
            if not isinstance(e, dict):
                continue
            up = _clean_artist_name(_entry_artist(e, keys=ENTRY_ARTIST_KEYS_LIGHT))
            if not up:
                continue
            norm = _normalize_text(up)
            counts[norm] = counts.get(norm, 0) + 1
        if counts:
            return max(counts, key=counts.__getitem__)

    if user_query and "-" in user_query:
        return _clean_artist_name(user_query.split("-", 1)[0].strip())

    return ""


# ===========================================================================
# Persistência de histórico (MongoDB)
# ===========================================================================


async def _save_history(ctx, title: str, url: str, artist: str = "") -> bool:
    """Salva faixa no histórico do MongoDB com saneamento de dados."""
    try:
        if not ctx or not getattr(ctx, "author", None):
            return False

        safe_title = _sanitize_text(title)
        if not safe_title:
            return False

        safe_url = _normalize_youtube_url(url) if url else None
        if not safe_url and url:
            safe_url = str(url).strip()
        if not safe_url:
            return False

        safe_artist = _sanitize_text(artist)
        await db.create_user_profile(str(ctx.author.id), ctx.author.name)
        ok = await db.add_to_music_history(
            str(ctx.author.id),
            {"title": safe_title, "url": safe_url, "artist": safe_artist or None},
        )
        if not ok:
            logging.warning(
                f"[MongoDB] Falha ao registrar música — "
                f"user={ctx.author.id}, title='{safe_title}', url='{safe_url}'"
            )
        return ok
    except Exception as exc:
        logging.error(f"[MongoDB] Erro ao salvar histórico musical: {exc}")
        return False


# ===========================================================================
# Autoplay: busca de recomendação
# ===========================================================================


async def _autoplay_search(
    query: str,
    *,
    guild_id: int,
    last_title: str,
    last_uploader: str,
    artist_hint: str,
    queue_urls: set[str],
    seen_candidates: set[str],
    candidates: list[Candidate],
    mongo_titles: list[str],
    mongo_uploaders: list[str],
    max_results: int = 10,
    strict_diversity: bool = True,
) -> tuple[Optional[str], Optional[str]]:
    """
    Busca e filtra candidatos para autoplay a partir de uma query textual.

    Retorna (url, title) ou (None, None) se nenhum candidato for aceito.
    """
    if not query:
        return None, None

    try:
        entries = await _search_music(query, max_results=max_results, artist_hint=artist_hint)
        new_start = len(candidates)

        for entry in entries:
            _collect_autoplay_candidate(
                entry, queue_urls=queue_urls, seen=seen_candidates, out=candidates
            )

        new_candidates = candidates[new_start:]
        logging.info(
            f"[autoplay] Query '{query}': {len(entries)} resultados, "
            f"{len(new_candidates)} novos (total: {len(candidates)})"
        )

        url, title = _pick_diverse_candidate(
            candidates, guild_id, last_title, last_uploader,
            extra_titles=mongo_titles, extra_uploaders=mongo_uploaders,
        )
        if url:
            _register_autoplay_url(guild_id, url)
            return url, title

        logging.info(
            f"[autoplay] Filtro de diversidade rejeitou todos os {len(candidates)} "
            f"candidatos para '{query}'"
        )

        if not strict_diversity and new_candidates:
            for f_url, f_title, _ in new_candidates:
                if _titles_equivalent(f_title, last_title):
                    continue
                _register_autoplay_url(guild_id, f_url)
                logging.info(f"[autoplay] Modo relaxado escolheu '{f_title}' para '{query}'")
                return f_url, f_title
    except Exception as exc:
        logging.warning(f"Falha ao buscar autoplay com query '{query}' para guild {guild_id}: {exc}")

    return None, None


def _collect_autoplay_candidate(
    entry: dict,
    *,
    queue_urls: set[str],
    seen: set[str],
    out: list[Candidate],
) -> None:
    """Valida e adiciona uma entrada yt_dlp à lista de candidatos do autoplay."""
    if not isinstance(entry, dict):
        return
    title = _entry_title(entry)
    if not title or _looks_non_musical(title):
        return

    uploader = _entry_uploader_normalized(entry)

    for key in ENTRY_URL_KEYS:
        url = _normalize_youtube_url(entry.get(key))
        if url and url not in queue_urls and url not in seen:
            seen.add(url)
            out.append((url, title, uploader))
            return


async def find_autoplay_recommendation(
    guild_id: int,
    ctx=None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Busca uma URL recomendada para autoplay com múltiplos estágios de fallback.

    Retorna (url, title) ou (None, None).
    """
    info = last_played_info.get(guild_id) or {}
    queue_urls: set[str] = set(autoplay_recent_urls.get(guild_id, []))
    candidates: list[Candidate] = []
    seen_candidates: set[str] = set()

    last_title = info.get("title", "")
    last_uploader = _normalize_text(
        info.get("uploader") or info.get("channel") or info.get("artist") or ""
    )
    mongo_titles: list[str] = []
    mongo_uploaders: list[str] = []
    mongo_artist_hints: list[str] = []

    # ------------------------------------------------------------------
    # Carrega histórico do MongoDB para evitar repetições
    # ------------------------------------------------------------------
    try:
        if ctx and getattr(ctx, "author", None):
            user_profile = await db.get_user_profile(str(ctx.author.id))
            if user_profile and user_profile.music_history:
                recent = user_profile.music_history[-20:]
                for song in recent:
                    t = getattr(song, "title", "") or ""
                    u = getattr(song, "url", None)
                    a = getattr(song, "artist", None)
                    if t:
                        mongo_titles.append(t)
                    if a:
                        mongo_uploaders.append(a)
                    if u:
                        normed = _normalize_youtube_url(u)
                        if normed:
                            queue_urls.add(normed)
                for song in reversed(recent):
                    artist = (getattr(song, "artist", None) or "").strip()
                    if artist and artist.lower() not in {a.lower() for a in mongo_artist_hints}:
                        mongo_artist_hints.append(artist)
                    if len(mongo_artist_hints) >= 3:
                        break
    except Exception as exc:
        logging.warning(f"Falha ao carregar histórico MongoDB para guild {guild_id}: {exc}")

    # Adiciona URLs da fila atual
    for entry in play_queue.get(guild_id, []):
        try:
            queue_urls.add(clean_youtube_url(entry[0]))
        except Exception:
            pass

    current_url = _normalize_youtube_url(
        info.get("webpage_url") or info.get("original_url") or info.get("requested_url")
    )
    if current_url:
        queue_urls.add(current_url)

    # ------------------------------------------------------------------
    # Etapa 0: vídeos relacionados já disponíveis no last_played_info
    # ------------------------------------------------------------------
    for video in info.get("related_videos") or []:
        _collect_autoplay_candidate(
            video, queue_urls=queue_urls, seen=seen_candidates, out=candidates
        )

    url, title = _pick_diverse_candidate(
        candidates, guild_id, last_title, last_uploader,
        extra_titles=mongo_titles, extra_uploaders=mongo_uploaders,
    )
    if url:
        _register_autoplay_url(guild_id, url)
        return url, title

    uploader = (info.get("uploader") or "").strip()
    artist_clean = _clean_artist_name(uploader) or last_title.split("-")[0].strip()

    # Kwargs compartilhados para _autoplay_search
    base_kwargs = dict(
        guild_id=guild_id,
        last_title=last_title,
        last_uploader=last_uploader,
        artist_hint=artist_clean,
        queue_urls=queue_urls,
        seen_candidates=seen_candidates,
        candidates=candidates,
        mongo_titles=mongo_titles,
        mongo_uploaders=mongo_uploaders,
    )

    # ------------------------------------------------------------------
    # Etapa 1: Last.fm — músicas similares
    # ------------------------------------------------------------------
    if LASTFM_API_KEY:
        logging.info(
            f"[Last.fm] Iniciando busca de similares para guild {guild_id} "
            f"— title='{last_title}', artist='{artist_clean}'"
        )
        similar = await asyncio.to_thread(_lastfm_similar_tracks, last_title, artist_clean)
        similar = await asyncio.to_thread(_filter_similar_by_theme, artist_clean, similar)
        random.shuffle(similar)
        logging.info(f"[Last.fm] {len(similar)} faixas similares, testando as 10 primeiras")

        for sim_artist, sim_track in similar[:10]:
            query = f"{sim_artist} {sim_track} official audio"
            logging.info(f"[Last.fm] Buscando no YouTube: '{query}'")
            url, title = await _autoplay_search(
                query, max_results=8, strict_diversity=False, **base_kwargs
            )
            if url:
                logging.info(f"[Last.fm] Candidato aceito: '{title}' ({url})")
                return url, title
            logging.info(f"[Last.fm] Nenhum candidato aceito para '{query}'")

        logging.warning(f"[Last.fm] Nenhum candidato válido para guild {guild_id}")

    # ------------------------------------------------------------------
    # Etapa 2: outros títulos do mesmo artista
    # ------------------------------------------------------------------
    uploader_only = _clean_artist_name(uploader) or _normalize_text(uploader)
    url, title = await _autoplay_search(uploader_only, max_results=20, **base_kwargs)
    if url:
        return url, title

    # ------------------------------------------------------------------
    # Etapa 3: artistas recentes do MongoDB
    # ------------------------------------------------------------------
    for hint in mongo_artist_hints:
        url, title = await _autoplay_search(f"{hint} official", max_results=20, **base_kwargs)
        if url:
            return url, title

    # ------------------------------------------------------------------
    # Etapa 4: fallback final — artista extraído do título
    # ------------------------------------------------------------------
    title_artist = last_title.split("-")[0].strip() if "-" in last_title else ""
    if title_artist:
        url, title = await _autoplay_search(
            f"{title_artist} official", max_results=25, **base_kwargs
        )
        if url:
            return url, title

    return None, None


# ===========================================================================
# Reprodução
# ===========================================================================


async def play_next(vc, guild_id: int, ctx) -> None:
    """Toca a próxima música da fila ou busca uma via autoplay se a fila estiver vazia."""
    if guild_id in manual_stop_guilds:
        manual_stop_guilds.discard(guild_id)
        return

    play_queue.setdefault(guild_id, deque())

    if not play_queue[guild_id]:
        if not autoplay_enabled.get(guild_id, True):
            await ctx.send("A fila de músicas está vazia. Desconectando do canal de voz.")
            await vc.disconnect()
            return

        next_url, next_title = await find_autoplay_recommendation(guild_id, ctx)
        if not next_url:
            await ctx.send(
                "Auto-play não encontrou recomendação no momento. "
                "Tente `!skip` novamente ou use `!play <url>`."
            )
            return

        preset = active_preset.get(guild_id, "padrao")
        play_queue[guild_id].append((next_url, preset))
        msg = f"Fila vazia. Auto-play escolheu: **{next_title}**" if next_title else \
              "Fila vazia. Reproduzindo uma música recomendada."
        await ctx.send(msg)

    url, preset_name = play_queue[guild_id].popleft()
    source, stream_title, info = await stream_musica(url, preset_name)

    if not source:
        await ctx.send("Erro ao processar o stream. Pulando para a próxima música da fila.")
        await play_next(vc, guild_id, ctx)
        return

    # Preserva related_videos se a mesma URL já havia sido usada
    prev = last_played_info.get(guild_id) or {}
    track_info = info or {}
    track_info["requested_url"] = url
    if (
        "related_videos" not in track_info
        and prev.get("requested_url") == url
        and prev.get("related_videos")
    ):
        track_info["related_videos"] = prev["related_videos"]

    last_played_info[guild_id] = track_info
    uploader = track_info.get("uploader") or track_info.get("channel") or track_info.get("artist") or ""
    _register_played_track(guild_id, stream_title, uploader)
    await _save_history(ctx, stream_title, url, uploader)

    try:
        vc.play(
            source,
            after=lambda _: asyncio.run_coroutine_threadsafe(
                play_next(vc, guild_id, ctx), ctx.bot.loop
            ),
        )
        await ctx.send(f"Transmitindo agora: **{stream_title}** com preset `{preset_name}`")
    except Exception as exc:
        logging.error(f"Erro ao transmitir `{stream_title}`: {exc}")
        await ctx.send(f"Erro ao transmitir `{stream_title}`: {exc}")
        await play_next(vc, guild_id, ctx)


# ===========================================================================
# Cog de comandos
# ===========================================================================


class MusicCommands(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ------------------------------------------------------------------
    # Helpers internos do Cog
    # ------------------------------------------------------------------

    async def _connect_voice(self, ctx) -> Optional[discord.VoiceClient]:
        """Conecta ou reutiliza o VoiceClient do servidor."""
        if ctx.guild.voice_client:
            return ctx.guild.voice_client
        try:
            return await ctx.author.voice.channel.connect(reconnect=True, self_deaf=True)
        except Exception as exc:
            logging.error(f"Erro ao conectar ao canal de voz: {exc}")
            await ctx.send("Não foi possível conectar ao canal de voz.")
            return None

    def _cleanup_guild_state(self, guild_id: int) -> None:
        """Limpa todo o estado em memória de um servidor."""
        play_queue.pop(guild_id, None)
        autoplay_enabled.pop(guild_id, None)
        last_played_info.pop(guild_id, None)
        active_preset.pop(guild_id, None)
        autoplay_recent_urls.pop(guild_id, None)
        recent_played_titles.pop(guild_id, None)
        recent_played_uploaders.pop(guild_id, None)
        manual_stop_guilds.discard(guild_id)

    # ------------------------------------------------------------------
    # !play
    # ------------------------------------------------------------------

    @commands.command(name="play")
    async def play(self, ctx, *input_parts: str):
        """Toca uma música ou adiciona à fila.

        Uso: !play <artista e musica>
        """
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        query = " ".join(input_parts).strip()
        if not query:
            await ctx.send(
                "Ei! Você precisa me dar um termo de busca. "
                "Uso correto: `!play <artista e musica>`"
            )
            return

        if not ctx.author.voice:
            await ctx.send(
                "Quer que eu adivinhe o canal para tocar musica? "
                "Conecte-se a um canal de voz primeiro."
            )
            return

        guild_id = ctx.guild.id
        play_queue.setdefault(guild_id, deque())

        vc = await self._connect_voice(ctx)
        if not vc:
            return

        preset_name = active_preset.get(guild_id, "padrao")

        try:
            async with ctx.typing():
                entries = await _search_ytdlp(query, max_results=12)

            if not entries:
                await ctx.send("Não encontrei resultados para sua busca. Tente outro termo.")
                return

            # Seleciona o primeiro resultado com URL válida
            selected_entry, selected_url = None, None
            for entry in entries:
                url = _entry_to_youtube_url(entry)
                if url:
                    selected_entry, selected_url = entry, url
                    break

            if not selected_url:
                await ctx.send(
                    "Encontrei resultados, mas nenhum vídeo reproduzível. Tente outra busca."
                )
                return

            selected_title = _entry_title(selected_entry) or "Resultado sem título"
            selected_artist = _entry_artist(selected_entry, keys=ENTRY_ARTIST_KEYS_LIGHT)
            artist_clean = _infer_artist(selected_entry, entries, query)
            if not artist_clean and "-" in selected_title:
                artist_clean = _clean_artist_name(selected_title.split("-", 1)[0].strip())

            play_queue[guild_id].append((selected_url, preset_name))

            # ----------------------------------------------------------
            # Monta lista de recomendações para exibição e autoplay
            # ----------------------------------------------------------
            related_candidates: list[dict] = []
            seen_titles = [selected_title]

            if LASTFM_API_KEY:
                similar = await asyncio.to_thread(
                    _lastfm_similar_tracks, selected_title, artist_clean, 10
                )
                similar = await asyncio.to_thread(_filter_similar_by_theme, artist_clean, similar)
                for sim_artist, sim_track in similar:
                    if any(_titles_equivalent(sim_track, s) for s in seen_titles):
                        continue
                    seen_titles.append(sim_track)
                    related_candidates.append(
                        {
                            "url": None,
                            "title": f"{sim_artist} - {sim_track}",
                            "uploader": sim_artist,
                            "_lastfm": True,
                        }
                    )
                    if len(related_candidates) >= 8:
                        break

            if not related_candidates:
                fallback_q = f"{artist_clean} official" if artist_clean else f"{selected_title} official"
                fallback_entries = await _search_music(fallback_q, max_results=10, artist_hint=artist_clean)
                for entry in fallback_entries:
                    candidate_url = _entry_to_youtube_url(entry)
                    if not candidate_url or candidate_url == selected_url:
                        continue
                    ctitle = _entry_title(entry)
                    if not ctitle or any(_titles_equivalent(ctitle, s) for s in seen_titles):
                        continue
                    seen_titles.append(ctitle)
                    related_candidates.append(
                        {
                            "url": candidate_url,
                            "title": ctitle,
                            "uploader": _entry_artist(entry, keys=ENTRY_ARTIST_KEYS_LIGHT),
                        }
                    )
                    if len(related_candidates) >= 8:
                        break

            # Salva apenas os candidatos com URL para o autoplay usar
            autoplay_related = [r for r in related_candidates if r.get("url")]
            last_played_info[guild_id] = {
                "requested_url": selected_url,
                "title": selected_title,
                "uploader": selected_artist,
                "related_videos": autoplay_related,
            }

            # Mensagem de confirmação
            sugestoes = "\n".join(
                f"{i + 1}. {r.get('title', 'Desconhecido')}"
                for i, r in enumerate(related_candidates[:5])
            )
            if sugestoes:
                fonte = (
                    "Last.fm"
                    if LASTFM_API_KEY and any(r.get("_lastfm") for r in related_candidates)
                    else "YouTube"
                )
                await ctx.send(
                    f"Resultado da busca: **{selected_title}**\n"
                    f"Adicionado à fila com preset `{preset_name}`\n"
                    f"Próximas recomendações ({fonte}):\n{sugestoes}"
                )
            else:
                await ctx.send(
                    f"Resultado da busca: **{selected_title}**\n"
                    f"Adicionado à fila com preset `{preset_name}`"
                )

            if not vc.is_playing() and not vc.is_paused():
                await play_next(vc, guild_id, ctx)

        except Exception as exc:
            logging.error(f"Erro ao processar música: {exc}")
            await ctx.send("Ocorreu um erro ao processar a música.")

    # ------------------------------------------------------------------
    # !stop
    # ------------------------------------------------------------------

    @commands.command(name="stop")
    async def stop(self, ctx):
        """Para a reprodução atual."""
        vc = ctx.guild.voice_client
        if vc and vc.is_playing():
            guild_id = ctx.guild.id
            manual_stop_guilds.add(guild_id)
            play_queue.get(guild_id, deque()).clear()
            vc.stop()
            await ctx.send("Reprodução parada.")
        else:
            await ctx.send("Nenhuma música está tocando.")

    # ------------------------------------------------------------------
    # !skip
    # ------------------------------------------------------------------

    @commands.command(name="skip")
    async def skip(self, ctx):
        """Pula a música atual."""
        vc = ctx.guild.voice_client
        if vc and vc.is_playing():
            vc.stop()
            await ctx.send("Música pulada!")
        else:
            await ctx.send("Nenhuma música está tocando.")

    # ------------------------------------------------------------------
    # !leave
    # ------------------------------------------------------------------

    @commands.command(name="leave")
    async def leave(self, ctx):
        """Faz o bot sair do canal de voz."""
        vc = ctx.guild.voice_client
        if not vc:
            await ctx.send("Não estou em nenhum canal de voz.")
            return
        self._cleanup_guild_state(ctx.guild.id)
        await vc.disconnect()
        await ctx.send("Desconectado do canal de voz.")

    # ------------------------------------------------------------------
    # !preset
    # ------------------------------------------------------------------

    _VALID_PRESETS = ("padrao", "bassboost", "pop", "rock")

    @commands.command(name="preset")
    async def preset(self, ctx, mode: str = None):
        """Controla o preset da equalização.

        Uso: !preset [padrao|bassboost|pop|rock|status]
        """
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        guild_id = ctx.guild.id
        current = active_preset.get(guild_id, "padrao")

        if not mode or mode.lower() == "status":
            await ctx.send(f"Preset atual: **{current}**")
            return

        mode = mode.lower()
        if mode not in self._VALID_PRESETS:
            await ctx.send(
                "Uso inválido. Use `!preset padrao`, `!preset bassboost`, "
                "`!preset pop`, `!preset rock` ou `!preset status`."
            )
            return

        if mode not in EQUALIZER_PRESETS:
            await ctx.send(f"Preset `{mode}` não está configurado no bot.")
            return

        active_preset[guild_id] = mode
        await ctx.send(f"Preset alterado para **{mode}**. Novas músicas usarão esse preset.")

    # ------------------------------------------------------------------
    # !autoplay
    # ------------------------------------------------------------------

    _AUTOPLAY_ON = {"on", "ligar", "ativar", "true", "1"}
    _AUTOPLAY_OFF = {"off", "desligar", "desativar", "false", "0"}

    @commands.command(name="autoplay")
    async def autoplay(self, ctx, mode: str = None):
        """Controla o auto-play.

        Uso: !autoplay [on|off|status]
        """
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        guild_id = ctx.guild.id
        status = autoplay_enabled.get(guild_id, True)

        if not mode or mode.lower() == "status":
            state = "ativado" if status else "desativado"
            await ctx.send(f"Auto-play está **{state}** neste servidor.")
            return

        lower = mode.lower()
        if lower in self._AUTOPLAY_ON:
            autoplay_enabled[guild_id] = True
            await ctx.send("Auto-play ativado.")
        elif lower in self._AUTOPLAY_OFF:
            autoplay_enabled[guild_id] = False
            await ctx.send("Auto-play desativado.")
        else:
            await ctx.send("Uso inválido. Use `!autoplay on`, `!autoplay off` ou `!autoplay status`.")

    # ------------------------------------------------------------------
    # !profile
    # ------------------------------------------------------------------

    @commands.command(name="profile")
    async def profile(self, ctx):
        """Mostra o perfil musical do usuário."""
        user_profile = await db.get_user_profile(str(ctx.author.id))
        if not user_profile:
            await ctx.send("Você ainda não tem um perfil! Use o comando `!play` para começar.")
            return

        history_text = "\n".join(
            f"• {s.title}" + (f" - {s.artist}" if s.artist else "")
            + f" ({s.played_at.strftime('%d/%m/%Y %H:%M')})"
            for s in user_profile.music_history[-5:]
        ) or "Nenhuma música tocada ainda"

        top_artists = await db.get_top_preferences(str(ctx.author.id), "artist", 5)
        top_genres = await db.get_top_preferences(str(ctx.author.id), "genre", 5)

        artists_text = "\n".join(
            f"• {p.name} ({p.count} músicas)" for p in top_artists
        ) or "Nenhum artista definido"

        genres_text = "\n".join(
            f"• {p.name} ({p.count} músicas)" for p in top_genres
        ) or "Nenhum gênero definido"

        embed = discord.Embed(
            title=f"Perfil Musical de {user_profile.username}",
            color=0x00FF00,
            timestamp=user_profile.created_at,
        )
        embed.add_field(name="📜 Histórico Recente", value=history_text, inline=False)
        embed.add_field(name="🎤 Artistas Favoritos", value=artists_text, inline=True)
        embed.add_field(name="🎵 Gêneros Favoritos", value=genres_text, inline=True)
        embed.set_footer(text="Perfil criado em")
        await ctx.send(embed=embed)

    # ------------------------------------------------------------------
    # !recommend
    # ------------------------------------------------------------------

    @commands.command(name="recommend")
    async def recommend(self, ctx):
        """Recomenda músicas baseadas nas preferências do usuário."""
        user_profile = await db.get_user_profile(str(ctx.author.id))
        if not user_profile or not user_profile.music_preferences:
            await ctx.send("Você precisa ter preferências musicais registradas!")
            return

        top_prefs = sorted(user_profile.music_preferences, key=lambda x: x.count, reverse=True)[:3]
        search_terms = " OR ".join(f'"{p.name}"' for p in top_prefs)

        try:
            opts = {
                "quiet": True,
                "extract_flat": True,
                "default_search": "ytsearch5",
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                result = await asyncio.to_thread(
                    ydl.extract_info,
                    f"ytsearch5:{search_terms.replace('&', 'and')}",
                    download=False,
                )

            if not (result and "entries" in result):
                await ctx.send("Não foi possível encontrar recomendações.")
                return

            embed = discord.Embed(
                title="🎵 Recomendações Musicais",
                description=f"Com base em: {', '.join(p.name for p in top_prefs)}",
                color=0x00FF00,
            )
            for entry in result["entries"][:5]:
                if entry:
                    embed.add_field(
                        name=entry.get("title", "Sem título"),
                        value=f"[Tocar no YouTube]({entry.get('url', '')})",
                        inline=False,
                    )
            await ctx.send(embed=embed)

        except Exception as exc:
            logging.error(f"Erro ao buscar recomendações: {exc}")
            await ctx.send("Erro ao buscar recomendações. Tente novamente.")

    # ------------------------------------------------------------------
    # !reproduzir_historico
    # ------------------------------------------------------------------

    @commands.command(name="reproduzir_historico")
    async def reproduzir_historico(self, ctx, count: int = 5, *flags: str):
        """Adiciona o histórico de reprodução à fila.

        Uso: !reproduzir_historico [count=5] [append] [search]
          count  — quantas músicas do histórico serão adicionadas (padrão 5)
          append — adiciona no final da fila (padrão: insere para tocar em seguida)
          search — busca pelo título quando a URL do histórico for inválida
        """
        if not validar_canal(ctx):
            await ctx.send("O Animal, Use o canal JUKEBOX para comandos de música.")
            return

        if not ctx.author.voice:
            await ctx.send("Conecte-se a um canal de voz primeiro para reproduzir seu histórico.")
            return

        user_profile = await db.get_user_profile(str(ctx.author.id))
        if not user_profile or not user_profile.music_history:
            await ctx.send("Nenhum histórico de reprodução encontrado no seu perfil.")
            return

        guild_id = ctx.guild.id
        play_queue.setdefault(guild_id, deque())

        vc = await self._connect_voice(ctx)
        if not vc:
            return

        flag_set = {f.lower() for f in flags}
        append_mode = "append" in flag_set
        fallback_search = "search" in flag_set or "fallback" in flag_set

        songs = user_profile.music_history[-count:]

        # URLs já na fila
        existing_urls: set[str] = set()
        for entry in play_queue[guild_id]:
            try:
                existing_urls.add(clean_youtube_url(entry[0]))
            except Exception:
                pass

        candidates: list[tuple[str, str]] = []  # (url, title)
        added_urls: set[str] = set()

        for song in songs:
            title = getattr(song, "title", None) or ""
            url = getattr(song, "url", None)
            chosen_url: Optional[str] = None

            if url:
                try:
                    cleaned = clean_youtube_url(url)
                    if cleaned and is_youtube_url(cleaned):
                        chosen_url = cleaned
                except Exception:
                    pass

            if not chosen_url and fallback_search and title:
                chosen_url = await self._search_url_by_title(title)

            if not chosen_url or chosen_url in existing_urls or chosen_url in added_urls:
                continue

            candidates.append((chosen_url, title))
            added_urls.add(chosen_url)

        if not candidates:
            await ctx.send(
                "Nenhuma música válida encontrada no seu histórico para adicionar "
                "(ou já estão na fila)."
            )
            return

        # Confirmação do usuário
        preview = "\n".join(
            f"{i}. {t or u}" for i, (u, t) in enumerate(candidates[:20], start=1)
        )
        await ctx.send(
            f"Vou adicionar {len(candidates)} músicas do seu histórico:\n{preview}\n\n"
            "Responda 'sim' para confirmar (30s) ou qualquer outra coisa para cancelar."
        )

        def _check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        try:
            reply = await self.bot.wait_for("message", check=_check, timeout=30)
        except asyncio.TimeoutError:
            await ctx.send("Tempo esgotado. Operação cancelada.")
            return

        if reply.content.lower() not in ("sim", "s", "yes", "y"):
            await ctx.send("Operação cancelada pelo usuário.")
            return

        preset_name = active_preset.get(guild_id, "padrao")
        for url, _ in candidates:
            if append_mode:
                play_queue[guild_id].append((url, preset_name))
            else:
                play_queue[guild_id].appendleft((url, preset_name))

        pos = "no final" if append_mode else "para serem reproduzidas em seguida"
        await ctx.send(f"✅ Adicionadas {len(candidates)} músicas do seu histórico à fila ({pos}).")

        if not vc.is_playing() and not vc.is_paused():
            await play_next(vc, guild_id, ctx)

    async def _search_url_by_title(self, title: str) -> Optional[str]:
        """Busca uma URL do YouTube a partir do título da música."""
        try:
            with yt_dlp.YoutubeDL(
                {"quiet": True, "extract_flat": True, "default_search": "ytsearch1"}
            ) as ydl:
                info = await asyncio.to_thread(
                    ydl.extract_info, f"ytsearch1:{title}", download=False
                )
            if info and "entries" in info and info["entries"]:
                entry = info["entries"][0]
                raw = entry.get("url") or entry.get("webpage_url") or entry.get("id")
                if raw:
                    cleaned = clean_youtube_url(raw)
                    if cleaned and is_youtube_url(cleaned):
                        return cleaned
        except Exception as exc:
            logging.warning(f"Busca por título falhou para '{title}': {exc}")
        return None