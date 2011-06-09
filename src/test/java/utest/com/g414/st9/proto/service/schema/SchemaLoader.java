package utest.com.g414.st9.proto.service.schema;

import java.io.File;
import java.util.Scanner;

public class SchemaLoader {
    public static String loadSchema(String name) {
        StringBuilder contents = new StringBuilder();

        try {
            Scanner scan = new Scanner(new File("src/test/resources/" + name
                    + ".json.txt"));

            while (scan.hasNextLine()) {
                contents.append(scan.nextLine());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return contents.toString();
    }
}
