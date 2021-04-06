import json
from flask import Flask, jsonify, request, abort
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
import time
import os

app = Flask("movies_service")


def el_search(es_object, search, index_name="movies"):
    res = es_object.search(
        index=index_name, filter_path=["hits.hits._source"], body=search
    )
    return res


@app.route("/")
def hello_world():
    return "Moe Flask приложение в контейнере Docker!!!!!!"


@app.route("/api/movies/<movie_id>", methods=["GET"])
def movie_details(movie_id: str) -> str:
    es = Elasticsearch(hosts=[{"host": "elasticsearch"}], retry_on_timeout=True)
    search_query = {"query": {"match": {"id": ""}}}
    search_query["query"]["match"]["id"] = movie_id

    try:
        search_output = el_search(es, json.dumps(search_query))
    except:
        abort(404)
        return

    if search_output:
        result = [d["_source"] for d in search_output["hits"]["hits"]][0]
        del result["writers_names"]
        del result["actors_names"]
        print(result)
        return jsonify(result)
    else:
        abort(404)
        return


@app.route("/api/movies", methods=["GET"], strict_slashes=False)
def movies_list() -> str:
    es = Elasticsearch(hosts=[{"host": "elasticsearch"}], retry_on_timeout=True)
    search_query = {
        "from": 0,
        "size": 50,
        "sort": [{"id": "asc"}],
        "query": {
            "multi_match": {
                "query": "he",
                "fuzziness": "auto",
                "fields": [
                    "title",
                    "description",
                    "genre",
                    "actors_names",
                    "writers_names",
                    "director",
                ],
            }
        },
    }
    limit = request.args.get("limit")
    page = request.args.get("page")
    search = request.args.get("search")
    sort = request.args.get("sort")
    sort_order = request.args.get("sort_order")

    if not limit and not search or search == "":
        search_query["query"] = {"match_all": {}}
    elif (
        not limit.isdigit()
        or int(limit) < 0
        or not page.isdigit()
        or int(page) <= 0
        or sort not in ("id", "title", "imdb_rating")
        or sort_order not in ("asc", "desc")
        or sort not in ("id", "title", "imdb_rating")
    ):
        abort(422)
        return

    if limit and page:
        search_query["size"] = limit
        search_query["from"] = int(limit) * (int(page) - 1)

    if search:
        search_query["query"]["multi_match"]["query"] = search
        print("SSS", type(search), search)
    print("SSS", type(search), search)

    if sort and sort_order:
        search_query["sort"] = {sort: sort_order}

    print(search_query)

    try:
        search_output = el_search(es, json.dumps(search_query))
    except:
        abort(404)
        return

    result = []
    if search_output:
        films_data = [d["_source"] for d in search_output["hits"]["hits"]]
        for film_data in films_data:
            film_data_cleaned = {
                "id": film_data["id"],
                "title": film_data["title"],
                "imdb_rating": film_data["imdb_rating"],
            }
            result.append(film_data_cleaned.copy())
        return jsonify(result)
    else:
        return


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, use_reloader=True)
