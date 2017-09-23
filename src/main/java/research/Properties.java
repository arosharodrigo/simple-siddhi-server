package research;

import java.io.IOException;
import java.io.InputStream;

public class Properties {

    public static java.util.Properties PROP;

    public static void loadProperties() {
        PROP = new java.util.Properties();
        InputStream input = null;
        try {
            String filename = "config.properties";
            input = Properties.class.getClassLoader().getResourceAsStream(filename);
            PROP.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
