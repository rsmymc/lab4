package edu.canisius.cyb600.in_class_assignment;

import edu.canisius.cyb600.in_class_assignment.jdbc.PostgresConnectionHandler;
import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.util.List;
import java.util.stream.Collectors;

public class SelectQuestions extends PostgresConnectionHandler {
    /**
     * Returns a list of all the films in the db
     * @return a list of all the films.
     */
    public List<Film> getAllFilms() {
        return this.dbAdapter.getAllFilms();
    }

    /**
     * Returns al list of all the actors in the db
     * @return a list of all the actors.
     */
    public List<Actor> getAllActors() {
        return this.dbAdapter.getAllActors();
    }

    /**
     * A method that uses code to downselect all the actors from getAllActors.
     * @param lastName The last name (case-insensitive) that should be included in the return list.
     * @return A list of actor objects that share a last name.
     */
    public List<Actor> getAllActorsWithLastNameWithCode(String lastName) {
        List<Actor> actors = this.dbAdapter.getAllActors();

        List<Actor> filteredActors = actors.stream()
                .filter(actor -> lastName.toUpperCase().equals(actor.getLastName()))
                .collect(Collectors.toList());

        return filteredActors;
    }

    /**
     * A method that uses postgres to select all the actors with the same last name
     * @param lastName the last name (case-insensitive) that should be included in the return list
     * @return A list actor objects that share a last name.
     */
    public List<Actor> getAllActorsWithLastName(String lastName) {
        return this.dbAdapter.getActorsWithLastName(lastName);
    }
}

