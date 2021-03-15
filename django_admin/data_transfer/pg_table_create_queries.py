create_queries = [
    """CREATE TABLE IF NOT EXISTS content.film_work (
id uuid PRIMARY KEY,
title TEXT NOT NULL,
description TEXT,
creation_date DATE,
certificate TEXT,
file_path TEXT,
rating FLOAT,
old_id TEXT,
type TEXT,
created_at timestamp with time zone,
updated_at timestamp with time zone
)""",
    """CREATE TABLE IF NOT EXISTS content.genre (
id uuid PRIMARY KEY,
name TEXT NOT NULL,
old_id TEXT,
description TEXT, 
created_at timestamp with time zone, 
updated_at timestamp with time zone
)""",
    """CREATE TABLE IF NOT EXISTS content.genre_film_work (
id uuid PRIMARY KEY,
film_work_id uuid NOT NULL,
genre_id uuid NOT NULL,
created_at timestamp with time zone
)""",
    """CREATE TABLE content.person (
id uuid PRIMARY KEY,
name TEXT NOT NULL,
birth_date DATE,
created_at timestamp with time zone,
updated_at timestamp with time zone
)""",
    """CREATE TABLE IF NOT EXISTS content.person_film_work (
id uuid PRIMARY KEY,
film_work_id uuid NOT NULL,
person_id uuid NOT NULL,
role TEXT NOT NULL,
created_at timestamp with time zone
)""",
]
