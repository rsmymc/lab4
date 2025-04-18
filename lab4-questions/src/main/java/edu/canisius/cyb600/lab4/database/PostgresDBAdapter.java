package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<String> getAllDistinctCategory() {

            try (Statement statement = conn.createStatement()) {

                ResultSet results = statement.executeQuery("SELECT DISTINCT name FROM category");

                List<String> categoryNames = new ArrayList<>();

                while (results.next()) {
                    categoryNames.add(results.getString("name"));
                }
                //Return all the films.
                return categoryNames;
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
        return new ArrayList<>();
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        String sql = "Select * from film where length > ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setInt(1, length);
            ResultSet results = statement.executeQuery();
            List<Film> films = new ArrayList<>();
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter) {
        String sql = "Select * from actor where first_name LIKE ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setString(1, firstLetter + "%");
            ResultSet results = statement.executeQuery();
            List<Actor> actors = new ArrayList<>();
            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("actor_id"));
                actor.setFirstName(results.getString("first_name"));
                actor.setLastName(results.getString("last_name"));
                actor.setLastUpdate(results.getDate("last_update"));
                //Add film to the array
                actors.add(actor);
            }
            //Return all the films.
            return actors;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> insertAllActorsWithAnOddNumberLastName(List<Actor> actors) {

        List<Actor> insertedActors = new ArrayList<>();

        String sql = "INSERT INTO ACTOR (first_name, last_name) VALUES (? , ? ) returning ACTOR_ID, LAST_UPDATE";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            for (Actor actor : actors) {
                if (actor.getLastName().length() % 2 == 0) {
                    statement.setString(1, actor.getFirstName());
                    statement.setString(2, actor.getLastName());
                    ResultSet results = statement.executeQuery();
                    if (results.next()) {
                        actor.setActorId(results.getInt("ACTOR_ID"));
                        actor.setLastUpdate(results.getDate("LAST_UPDATE"));
                        insertedActors.add(actor);
                    }
                }
            }

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return insertedActors;
    }

    @Override
    public List<Film> getFilmsInCategory(Category category) {
        String sql = "select * from film, category, film_category where category.category_id = film_category.category_id and film.film_id = film_category.film_id and category.category_id = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setInt(1, category.getCategoryId());
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }


}
