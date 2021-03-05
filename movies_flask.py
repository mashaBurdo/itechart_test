import json
from flask import Flask, jsonify, request,  abort
from elasticsearch import Elasticsearch

app =\
    Flask('movies_service')


def el_search(es_object, search, index_name='movies'):
    res = es_object.search(index=index_name, filter_path = ["hits.hits._source"], body=search)
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
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    search_query = {
      "from" : 0,
      "size": 50,
      "sort" : [
          {"id": "asc"}
      ],
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
            "director"
          ]
        }
      }
    }
    limit = 50
    page = 1
    sort_order = 'asc'
    sort = 'id'
    print(type(request.args.get('search') ))
    if request.args.get('limit') and request.args.get('page'):
        limit = request.args.get('limit')
        page = request.args.get('page')
        search_query['size'] = limit
        search_query['from'] = int(limit) * (int(page) - 1)

    if request.args.get('search'):
        search = request.args.get('search')
        search_query['query']['multi_match']['query'] = search
    # print(limit, type(limit), page, type(page), sort, sort_order, search)
    if request.args.get('page'):
        page = request.args.get('page')

    if request.args.get('sort') and request.args.get('sort_order'):
        sort = request.args.get('sort')
        sort_order = request.args.get('sort_order')
        search_query['sort'] = {sort: sort_order}
    # if request.args.get('sort_order'):
    #     sort_order = request.args.get('sort_order')


    # print(search_query)

    search_output = el_search(es, json.dumps(search_query))
    # print(search_output, type(search_output))
    # return search_output

    result = []
    if search_output:
        films_data = [d['_source'] for d in search_output['hits']['hits']]
        for film_data in films_data:
            film_data_cleaned = {'id': film_data['id'], 'title': film_data['title'], 'imdb_rating': film_data['imdb_rating']}
            # print(type(film_data_cleaned))
            result.append(film_data_cleaned.copy())
        # print(result)
        return jsonify(result)
    else:
        return


if __name__ == '__main__':
    app.run(port=8000)