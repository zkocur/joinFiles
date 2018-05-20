package join;

import java.io.*;
import java.nio.file.*;

public class Main {

    public static void main(String[] args) {
        JoinFiles jf = new JoinFiles();
        Path p = Paths.get("/home/user/pliki");
        try {
            OutputStream os = new FileOutputStream("/home/zuza/Pulpit/bilot/Output");
            try {
                jf.join(p ,"LEFT Salary.user_id = User.id", (OutputStream) os);
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoMachingQueryPatternException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
