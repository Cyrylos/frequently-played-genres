set3 = LOAD 'output_mr/part-r-*' 
    USING PigStorage()
    AS (movie_id:chararray,
        number_of_actors:int);

set2 = LOAD 'input/datasource2/title.basica.tsv' 
    USING PigStorage()
    AS (id:chararray,
        title_type:chararray,
        primary_title:chararray,
        ooriginal_title:chararray,
        is_adult:boolean,
        start_year:int,
        end_year:int,
        runtime_minutes:int,
        genres:chararray);

ranked_set2 = RANK set2;
raw_set2 = FILTER ranked_set2 BY $0 > 1;

movies = FILTER raw_set2 BY title_type == 'movie';
filtered_movies = FILTER movies BY genres != '\\N';
genre_movie = FOREACH filtered_movies GENERATE id, flatten(TOKENIZE(genres,',')) AS genre;

actors_movies_relation = JOIN set3 BY movie_id, genre_movie BY id;

genre_group = GROUP actors_movies_relation BY genre;

result = foreach genre_group generate group AS genre, COUNT(actors_movies_relation.movie_id) AS movies, SUM(actors_movies_relation.number_of_actors) AS actors;

result_ordered = ORDER result BY actors DESC;

limited_result = LIMIT result_ordered 3;

STORE limited_result INTO 'output/' USING JsonStorage();