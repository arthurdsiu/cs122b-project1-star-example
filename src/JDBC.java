import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JDBC implements Parameters {
    ArrayList<Movie> getMoviesByRating(){
        ArrayList<Movie> topMovies = new ArrayList<>();
        String query = "select merged.id, title, year, director, rating, name " +
                "from " +
                "(select * " +
                "from movies " +
                "left join ratings " +
                "on movies.id = ratings.movieId " +
                "order by ratings.rating DESC " +
                "limit 10) as merged " +
                "left join stars_in_movies " +
                "on merged.id = stars_in_movies.movieId " +
                "left join stars " +
                "on stars.id = stars_in_movies.starId;" ;
        try {
            ResultSet result = sqlQuery(query);
            HashMap<String, Integer> year = new HashMap<>();
            HashMap<String, String> director = new HashMap<>();
            HashMap<String, Float> rating = new HashMap<>();
            HashMap<String, ArrayList<String>> genres = new HashMap<>();
            HashMap<String , ArrayList<String>> stars = new HashMap<>();

            while (result.next()) {
                String key = result.getString("title");
                if(year.containsKey(key)){
                    genres.get(key).add(result.getString("genre"));
                    stars.get(key).add(result.getString("star"));
                    continue;
                }
                year.put(key, result.getInt("year"));
                director.put(key, result.getString("director"));
                rating.put(key, result.getFloat("rating"));
                genres.put(key, new ArrayList<>());
                genres.get(key).add(result.getString("genre"));
                stars.put(key, new ArrayList<>());
                stars.get(key).add(result.getString("star"));
            }
            Iterator it = year.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry elem = (Map.Entry)it.next();
                String key = (String)elem.getKey();
                topMovies.add(Movie.builder()
                        .title(key)
                        .year((Integer)elem.getValue())
                        .director((String)director.get(key))
                        .genres((ArrayList<String>) genres.get(key))
                        .stars((ArrayList<String>) stars.get(key))
                        .rating((float)rating.get(key))
                        .build());
            }

        }
        catch(SQLException e) {
            System.out.println("Error making SQL query: " + e);
        }
        catch(Exception e) {
            System.out.println("Class Not Found Exception: " + e);
        }

        return topMovies;
    }

    ResultSet sqlQuery(String query) throws Exception{
        // Incorporate mySQL driver
        Class.forName("com.mysql.cj.jdbc.Driver");
        // Connect to the test database
        Connection connection = DriverManager.getConnection("jdbc:" + Parameters.dbtype + ":///" + Parameters.dbname + "?autoReconnect=true&useSSL=false",
                Parameters.username, Parameters.password);
        // Create an execute a SQL statement
        Statement select = connection.createStatement();
        ResultSet sqlResult = select.executeQuery(query);
        return sqlResult;
    }
    public static void main(String[] arg) throws Exception {
        JDBC test = new JDBC();
        ArrayList<Movie> result = test.getMoviesByRating();
        for (Movie elem : result){
            System.out.println(elem);
        }
        return;

//        // Incorporate mySQL driver
//        Class.forName("com.mysql.cj.jdbc.Driver");
//
//        // Connect to the test database
//        Connection connection = DriverManager.getConnection("jdbc:" + Parameters.dbtype + ":///" + Parameters.dbname + "?autoReconnect=true&useSSL=false",
//                Parameters.username, Parameters.password);
//
//        if (connection != null) {
//            System.out.println("Connection established!!");
//            System.out.println();
//
//            // Create an execute an SQL statement to select all of table "stars" records
//            Statement select = connection.createStatement();
//            String query = "select merged.id, title, year, director, rating, name " +
//                    "from " +
//                    "(select * " +
//                    "from movies " +
//                    "left join ratings " +
//                    "on movies.id = ratings.movieId " +
//                    "order by ratings.rating DESC " +
//                    "limit 10) as merged " +
//                    "left join stars_in_movies " +
//                    "on merged.id = stars_in_movies.movieId " +
//                    "left join stars " +
//                    "on stars.id = stars_in_movies.starId;" ;
//            ResultSet result = select.executeQuery(query);
//
//            // Get metadata from stars; print # of attributes in table
//            System.out.println("The results of the query");
//            ResultSetMetaData metadata = result.getMetaData();
//            System.out.println("There are " + metadata.getColumnCount() + " columns");
//
//            // Print type of each attribute
//            for (int i = 1; i <= metadata.getColumnCount(); i++)
//                System.out.println("Type of column " + i + " is " + metadata.getColumnTypeName(i));
//
//            // print table's contents, field by field
//            while (result.next()) {
//                System.out.println("Id = " + result.getString("id"));
//                System.out.println("Name = " + result.getString("name"));
//                //System.out.println("birthYear = " + result.getInt("birthYear"));
//                System.out.println();
//            }
//        } else {
//            System.out.println("no connection");
//        }
    }
}