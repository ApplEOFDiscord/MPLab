import java.util.*;

class RecommendItemComparator implements Comparator<RecommendItem> {
    public int compare(RecommendItem first, RecommendItem second) {
        Float firstRating = new Float(first.getRating());
        Float secondRating = new Float(second.getRating());
        return firstRating.compareTo(secondRating);
    }
}

public class RecommendItem {
    private int movieId;
    private float rating;

    public RecommendItem(int movieId, float rating) {
        this.movieId = movieId;
        this.rating = rating;
    }

    public int getMovieId() {
        return movieId;
    }

    public float getRating() {
        return rating;
    }
}