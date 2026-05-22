import os
from flask import Flask, request, jsonify, render_template
from elasticsearch import Elasticsearch, NotFoundError
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

ES_HOST    = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER    = os.getenv("ES_USER")
ES_PASS    = os.getenv("ES_PASS")
INDEX_NAME = os.getenv("ES_INDEX", "songs-search")


def get_es_client() -> Elasticsearch:
    if ES_USER and ES_PASS:
        return Elasticsearch([ES_HOST], basic_auth=(ES_USER, ES_PASS))
    return Elasticsearch([ES_HOST])


es = get_es_client()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search")
def search():
    query   = request.args.get("q", "").strip()
    field   = request.args.get("field", "lyrics")   # lyrics | title | primary_artist
    mode    = request.args.get("mode", "match")      # match | match_phrase
    genre   = request.args.get("genre", "")
    size    = int(request.args.get("size", 10))

    if not query:
        return jsonify({"hits": [], "total": 0})

    # Build the base text query
    if mode == "match_phrase":
        text_query = {"match_phrase": {field: query}}
    else:
        text_query = {"match": {field: {"query": query, "fuzziness": "AUTO"}}}

    # Optionally filter by genre
    if genre:
        es_query = {
            "bool": {
                "must":   [text_query],
                "filter": [{"term": {"genre": genre}}],
            }
        }
    else:
        es_query = text_query

    resp = es.search(
        index=INDEX_NAME,
        query=es_query,
        size=size,
        # Boost popular songs slightly in ranking
        sort=[
            "_score",
            {"popularity": {"order": "desc"}},
        ],
        # Only return fields we need — lyrics is large, exclude it from results
        source_excludes=["lyrics", "source"],
    )

    hits = []
    for h in resp["hits"]["hits"]:
        src = h["_source"]
        hits.append({
            "song_id":        h["_id"],
            "score":          round(h["_score"], 2),
            "title":          src.get("title", ""),
            "primary_artist": src.get("primary_artist", ""),
            "artists":        src.get("artists", []),
            "genre":          src.get("genre", ""),
            "niche_genres":   src.get("niche_genres", []),
            "popularity":     src.get("popularity", 0),
            "year":           src.get("year", 0),
        })

    total = resp["hits"]["total"]["value"]
    return jsonify({"hits": hits, "total": total})


@app.route("/song/<song_id>")
def song_detail(song_id):
    """Return a single song with its lyrics — used for the detail modal."""
    try:
        resp = es.get(index=INDEX_NAME, id=song_id)
        src  = resp["_source"]
        return jsonify({
            "song_id":        resp["_id"],
            "title":          src.get("title", ""),
            "primary_artist": src.get("primary_artist", ""),
            "artists":        src.get("artists", []),
            "genre":          src.get("genre", ""),
            "niche_genres":   src.get("niche_genres", []),
            "popularity":     src.get("popularity", 0),
            "year":           src.get("year", 0),
            "lyrics":         src.get("lyrics", ""),
        })
    except NotFoundError:
        return jsonify({"error": "Song not found"}), 404


@app.route("/genres")
def genres():
    """Return all distinct genres for the filter dropdown."""
    resp = es.search(
        index=INDEX_NAME,
        size=0,
        aggs={"genres": {"terms": {"field": "genre", "size": 100}}},
    )
    buckets = resp["aggregations"]["genres"]["buckets"]
    genre_list = sorted([b["key"] for b in buckets if b["key"]])
    return jsonify(genre_list)


if __name__ == "__main__":
    app.run(debug=True, port=5000)