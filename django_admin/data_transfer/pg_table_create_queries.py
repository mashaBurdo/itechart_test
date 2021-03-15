create_queries = [
    """CREATE TABLE IF NOT EXISTS content.film_work (
id uuid PRIMARY KEY,
title TEXT NOT NULL,
description TEXT,
creation_date DATE,
certificate TEXT,
file_path TEXT,
rating FLOAT,
type TEXT,
old_id TEXT
)""",
    """CREATE TABLE IF NOT EXISTS content.genre (
id uuid PRIMARY KEY,
name TEXT NOT NULL,
old_id TEXT
)""",
    """CREATE TABLE IF NOT EXISTS content.genre_film_work (
id uuid PRIMARY KEY,
film_work_id uuid NOT NULL,
genre_id uuid NOT NULL
)""",
    """CREATE TABLE content.person (
id uuid PRIMARY KEY,
name TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS content.person_film_work (
id uuid PRIMARY KEY,
film_work_id uuid NOT NULL,
person_id uuid NOT NULL,
role TEXT NOT NULL
)""",
]
