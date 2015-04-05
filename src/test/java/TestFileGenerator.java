import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.util.Random;

/**
 * @author mh
 * @since 05.04.15
 */
public class TestFileGenerator {
    private static final int MILLION = 1_000_00;
    private final String dir;

    public TestFileGenerator(String dir) {
        this.dir = dir;
    }

    public static void main(String[] args) throws IOException {
        new TestFileGenerator("data").generate(3);
    }

    private void generate(int files) throws IOException {
        int total = files * MILLION;
        StringBuilder sb = new StringBuilder(1000);
        Random random = new Random(4711);
        for (int file=0;file<files;file++) {
            String fileName = dir + String.format("/friends-%03d______.txt.bz2", file); // friends-000______.txt.bz2
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new BZip2CompressorOutputStream(new FileOutputStream(fileName)), 1024 * 1024));
            for (int user = 0;user<MILLION;user++) {
                int userId = user + file*MILLION;
                sb.setLength(0);
                sb.append(userId).append(':');
                int friends = random.nextInt(100);
                if (friends % 10 == 0) sb.append("notfound");
                else if (friends % 11 == 0) sb.append("private");
                else {
                    for (int friend=0;friend<friends;friend++) {
                        if (friend!=0) sb.append(',');
                        int friendId = random.nextInt(total);
                        sb.append(friendId);
                    }
                }
                sb.append('\n');
                os.writeBytes(sb.toString());
            }
            os.close();
        }
    }
}
