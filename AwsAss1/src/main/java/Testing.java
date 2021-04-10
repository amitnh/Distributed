import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Testing {
    public static void main(String[] args) throws FileNotFoundException {
        try {
            String src = "./input files/B001DZTJRQ.txt";
            JSONParser jsonParser = new JSONParser();
            JSONArray a = (JSONArray) jsonParser.parse(new FileReader(src));
            for (Object o : a) {
                // access your object here.
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

    }
}
