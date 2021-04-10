import org.apache.commons.lang3.StringEscapeUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class Testing {
    public static void main(String[] args) throws FileNotFoundException {
        try {
            String src = "./input files/B001DZTJRQ.txt";
            JSONParser jsonParser = new JSONParser();
            JSONArray a = (JSONArray) jsonParser.parse(new FileReader(src));
            for (Object o : a) {
                // access your object here.
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
