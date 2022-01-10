from typing import List


def _transform_records(records: List[dict]) -> List:
    result_records_list = []
    records_not_none = []
    records_persons = []
    for record in records:
        record = {k: v for k, v in record.items() if v is not None}
        records_not_none.append(record)
    ids = set(d['id'] for d in records_not_none)
    for id in ids:
        dic = dict()
        for d in records_not_none:
            if d['id'] == id:
                dic.update(d)
        records_persons.append(dic)

    for record in records_persons:
        writer = record.get('writers')
        actor = record.get('actors')
        director = record.get('directors')
        genre = record.get('genre')
        if writer:
            writers_list = [x.split(' id:') for x in writer]
            writers = [{'id': _id, 'name': name} for name, _id in writers_list]
            record['writers'] = writers
            record['writers_names'] = [x['name'] for x in writers]

        if actor:
            actors_list = [x.split(' id:') for x in actor]
            actors = [{'id': _id, 'name': name} for name, _id in actors_list]
            record['actors'] = actors
            record['actors_names'] = [x['name'] for x in actors]

        if director:
            directors_list = [x.split(' id:') for x in director]
            directors = [{'id': _id, 'name': name} for name, _id in directors_list]
            record['directors'] = directors
            record['directors_names'] = [x['name'] for x in directors]

        if genre:
            genre_list = [x.split(' id:') for x in genre]
            genres = [{'id': _id, 'name': name} for name, _id in genre_list]
            record['genres'] = genres
            record['genre'] = [x['name'] for x in genres]

        result_records_list.append(record)

    return result_records_list
