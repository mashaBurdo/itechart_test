import json
from flask import Flask, jsonify, request,  abort
from elasticsearch import Elasticsearch

app =\
    Flask('movies_service')


def el_search(es_object, search, index_name='movies'):
    res = es_object.search(index=index_name, filter_path =["hits.hits._source"], body=search)
    return res


@app.route('/api/movies/<movie_id>', methods=['GET'])
def movie_details(movie_id: str) -> str:
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    search_query = {
        "query": {
            "match": {
                "id": ""
            }
        }
    }
    search_query['query']['match']['id'] = movie_id
    search_output = el_search(es, json.dumps(search_query))
    # print(search_output, type(search_output))

    if search_output:
        result = [d['_source'] for d in search_output['hits']['hits']][0]
        del result['writers_names']
        del result['actors_names']
        print(result)
        return jsonify(result)
    else:
        abort(404)
        return


@app.route('/api/movies', methods=['GET'], strict_slashes=False)
def movies_list() -> str:
    limit = request.args.get('limit')
    page = request.args.get('page')
    sort = request.args.get('sort')
    sort_order = request.args.get('asc')
    search = request.args.get('desc')
    print(limit, page, sort, sort_order, search)
    search_query = {
        "query": {
            "multi_match": {
                "query": "campfir",
                "fuzziness": "auto",
                "fields": [
                    "title",
                    "description",
                    "genre",
                    "actors_names",
                    "writers_names",
                    "director"
                ]
            }
        }
    }
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    result = ''
    return jsonify(result)


if __name__ == '__main__':
    app.run(port=8000)