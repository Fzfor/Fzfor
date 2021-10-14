import java.net.StandardSocketOptions;
import java.util.ArrayList;
import java.util.zip.CheckedOutputStream;

/**
 * @author fzfor
 * @date 14:13 2021/07/06
 */
public class TestJava {
    public static void main(String[] args) {


        for (int i = 0; i < 10; i++) {
            if (i == 5) {
                break;
            }
            System.out.println(i);
        }

        System.out.println("------------------------");

        for (int i = 0; i < 10; i++) {
            if (i == 5) {
                continue;
            }
            System.out.println(i);
        }
    }
}
