import lombok.Builder;
import java.util.ArrayList;

@Builder
public class Movie {
    String title;
    int year;
    String director;
    ArrayList<String> genres;
    ArrayList<String> stars;
   float rating;
}
