import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class ItemWritable implements Writable {
    private ArrayList<LongWritable> userIdList;
    private ArrayList<FloatWritable> ratingList;
    private MapWritable cooccurrenceVector;

    public ItemWritable() {
        this.userIdList = new ArrayList<>();
        this.ratingList = new ArrayList<>();
        this.cooccurrenceVector = new MapWritable();
    }

    public ItemWritable(ArrayList<LongWritable> userIdList, ArrayList<FloatWritable> ratingList, MapWritable cooccurrenceVector) {
        this.userIdList = userIdList;
        this.ratingList = ratingList;
        this.cooccurrenceVector = cooccurrenceVector;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userIdList.size());
        for(int i = 0; i < userIdList.size(); i++) {
            userIdList.get(i).write(out);
        }

        out.writeInt(ratingList.size());
        for(int i = 0; i < ratingList.size(); i++) {
            ratingList.get(i).write(out);
        }

        cooccurrenceVector.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len;

        len = in.readInt();
        userIdList = new ArrayList<LongWritable>(len);
        for(int i = 0; i < len; i++) {
            LongWritable user = new LongWritable();
            user.readFields(in);
            userIdList.add(user);
        }

        len = in.readInt();
        ratingList = new ArrayList<FloatWritable>(len);
        for(int i = 0; i < len; i++) {
            FloatWritable rating = new FloatWritable();
            rating.readFields(in);
            ratingList.add(rating);
        }

        cooccurrenceVector.readFields(in);
    }

    public ArrayList<LongWritable> getUserId() {
        return userIdList;
    }

    public ArrayList<FloatWritable> getRating() {
        return ratingList;
    }

    public MapWritable getCooccurrence() {
        return cooccurrenceVector;
    }
}