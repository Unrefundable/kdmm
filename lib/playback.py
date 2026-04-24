import json


def _to_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return None


def _to_str(value):
    if value is None:
        return ""
    return str(value).strip()


def build_playback_context(media_id, imdb=None, tmdb=None, title=None, showtitle=None,
                           season=None, episode=None, year=None, player_file="kdmm.json"):
    season_num = _to_int(season)
    episode_num = _to_int(episode)
    year_num = _to_int(year)
    is_movie = not (season_num and episode_num)
    return {
        "media_id": _to_str(media_id),
        "imdb_id": _to_str(imdb),
        "tmdb_id": _to_str(tmdb),
        "title": _to_str(title),
        "showtitle": _to_str(showtitle),
        "season": season_num,
        "episode": episode_num,
        "year": year_num,
        "is_movie": is_movie,
        "player_file": _to_str(player_file) or "kdmm.json",
    }


def encode_playback_context(context):
    try:
        return json.dumps(context or {}, separators=(",", ":"))
    except Exception:
        return ""


def decode_playback_context(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def apply_playback_metadata(listitem, context):
    context = context or {}

    title = _to_str(context.get("title"))
    showtitle = _to_str(context.get("showtitle"))
    imdb_id = _to_str(context.get("imdb_id"))
    tmdb_id = _to_str(context.get("tmdb_id"))
    season = _to_int(context.get("season"))
    episode = _to_int(context.get("episode"))
    year = _to_int(context.get("year"))
    is_movie = bool(context.get("is_movie"))

    info = {}
    if is_movie:
        if title:
            info["title"] = title
        info["mediatype"] = "movie"
    else:
        if title:
            info["title"] = title
        elif episode is not None:
            info["title"] = f"Episode {episode}"
        if showtitle:
            info["tvshowtitle"] = showtitle
        if season is not None:
            info["season"] = season
        if episode is not None:
            info["episode"] = episode
        info["mediatype"] = "episode"

    if imdb_id:
        info["imdbnumber"] = imdb_id
    if year is not None:
        info["year"] = year

    try:
        if info:
            listitem.setInfo("video", info)
    except Exception:
        pass

    try:
        label = title or showtitle
        if label:
            listitem.setLabel(label)
    except Exception:
        pass

    try:
        vtag = listitem.getVideoInfoTag()
    except Exception:
        return

    try:
        if is_movie:
            vtag.setMediaType("movie")
        else:
            vtag.setMediaType("episode")
            if showtitle:
                vtag.setTvShowTitle(showtitle)
            if season is not None:
                vtag.setSeason(season)
            if episode is not None:
                vtag.setEpisode(episode)
        if title:
            vtag.setTitle(title)
        elif showtitle and is_movie:
            vtag.setTitle(showtitle)
        if imdb_id:
            vtag.setIMDBNumber(imdb_id)
        if year is not None:
            vtag.setYear(year)
    except Exception:
        pass

    unique_ids = {}
    if imdb_id:
        unique_ids["imdb"] = imdb_id
    if tmdb_id:
        unique_ids["themoviedb"] = tmdb_id
        if is_movie:
            unique_ids["tmdb"] = tmdb_id
        else:
            unique_ids["tvshow.tmdb"] = tmdb_id
            unique_ids["tmdbshow"] = tmdb_id
            unique_ids["tmdb_show"] = tmdb_id

    if unique_ids:
        try:
            default_id = "tmdb" if "tmdb" in unique_ids else "imdb"
            vtag.setUniqueIDs(unique_ids, default_id)
        except Exception:
            pass
