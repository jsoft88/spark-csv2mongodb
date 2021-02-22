package com.org.batch.models

case class MoviesMetadata(
                         adult: Boolean,
                         belongs_to_collection: java.util.List[Any],
                         budget: Double,
                         genres: java.util.List[Any],
                         homepage: String,
                         id: Int,
                         imdb_id: Int,
                         original_language: String,
                         original_title: String,
                         overview: String,
                         popularity: Double,
                         poster_path: String,
                         production_companies: java.util.List[Any],
                         production_countries: java.util.List[Any],
                         release_date: String,
                         revenue: Double,
                         runtime: Int,
                         spoken_languages: java.util.List[Any],
                         status: String,
                         tagline: String,
                         title: String,
                         video: Boolean,
                         vote_average: Double,
                         vote_count: Int
                         )
