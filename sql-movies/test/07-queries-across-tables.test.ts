import { Database } from "../src/database";
import { ACTORS, DIRECTORS, GENRES, KEYWORDS, MOVIES, MOVIE_ACTORS, MOVIE_DIRECTORS, MOVIE_GENRES, MOVIE_KEYWORDS, MOVIE_RATINGS } from "../src/table-names";
import { minutes } from "./utils";

describe("Queries Across Tables", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.fromExisting("06", "07");
  }, minutes(3));

  it(
    "should select top three directors ordered by total budget spent in their movies",
    async done => {
      const query = `SELECT full_name as director, ROUND(SUM(budget_adjusted), 2) as total_budget
      from ${MOVIE_DIRECTORS}
      inner join ${DIRECTORS} d on ${MOVIE_DIRECTORS}.director_id = d.id
      inner join ${MOVIES} m on ${MOVIE_DIRECTORS}.movie_id = m.id
      GROUP BY full_name
      ORDER BY SUM(budget_adjusted) DESC
      LIMIT 3`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          director: "Ridley Scott",
          total_budget: 722882143.58
        },
        {
          director: "Michael Bay",
          total_budget: 518297522.1
        },
        {
          director: "David Yates",
          total_budget: 504100108.5
        }
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select top 10 keywords ordered by their appearance in movies",
    async done => {
      const query = `SELECT keyword, count(*) as count
      FROM ${KEYWORDS}
      inner join ${MOVIE_KEYWORDS} mk on ${KEYWORDS}.id = mk.keyword_id
      inner join ${MOVIES} m on mk.movie_id = m.id
      group by keyword
      order by count DESC
      limit 10`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          keyword: "woman director",
          count: 162
        },
        {
          keyword: "independent film",
          count: 115
        },
        {
          keyword: "based on novel",
          count: 85
        },
        {
          keyword: "duringcreditsstinger",
          count: 82
        },
        {
          keyword: "biography",
          count: 78
        },
        {
          keyword: "murder",
          count: 66
        },
        {
          keyword: "sex",
          count: 60
        },
        {
          keyword: "revenge",
          count: 51
        },
        {
          keyword: "sport",
          count: 50
        },
        {
          keyword: "high school",
          count: 48
        }
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select all movies called Life and return amount of actors",
    async done => {
      const query = `SELECT original_title, count(*) as count
      FROM ${MOVIES}
      inner join ${MOVIE_ACTORS} ma on ${MOVIES}.id = ma.movie_id
      inner join ${ACTORS} a on a.id = ma.actor_id
      WHERE original_title = 'Life'`;
      const result = await db.selectSingleRow(query);

      expect(result).toEqual({
        original_title: "Life",
        count: 12
      });

      done();
    },
    minutes(3)
  );

  it(
    "should select three genres which has most ratings with 5 stars",
    async done => {
      const query = `SELECT genre, count(*) as five_stars_count
      FROM ${GENRES}
      inner join ${MOVIE_GENRES} mg on ${GENRES}.id = mg.genre_id
      inner join ${MOVIE_RATINGS} mr on mg.movie_id = mr.movie_id
      WHERE rating = 5
      group by genre
      order by five_stars_count DESC
      LIMIT 3`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          genre: "Drama",
          five_stars_count: 15052
        },
        {
          genre: "Thriller",
          five_stars_count: 11771
        },
        {
          genre: "Crime",
          five_stars_count: 8670
        }
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select top three genres ordered by average rating",
    async done => {
      const query = `SELECT genre, ROUND(AVG(rating), 2) as avg_rating
      FROM ${GENRES}
      inner join ${MOVIE_GENRES} mg on ${GENRES}.id = mg.genre_id
      inner join ${MOVIE_RATINGS} mr on mg.movie_id = mr.movie_id
      group by genre
      order by avg_rating DESC
      LIMIT 3`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          genre: "Crime",
          avg_rating: 3.79
        },
        {
          genre: "Music",
          avg_rating: 3.73
        },
        {
          genre: "Documentary",
          avg_rating: 3.71
        }
      ]);

      done();
    },
    minutes(3)
  );
});