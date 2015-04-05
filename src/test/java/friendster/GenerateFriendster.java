package friendster;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.util.Random;

/**
 * @author mh
 * @since 05.04.15
 */
public class GenerateFriendster {
    private final String dir;

    public GenerateFriendster(String dir) {
        this.dir = dir;
    }

    public static void main(String[] args) throws IOException {
        System.err.println("Usage: friendster.GenerateFriendster [file-dir:data] [files:10] [users per file:100 000]");
        String dataDir = args.length > 0 ? args[0] : "data";
        int files = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int usersPerFile = args.length > 2 ? Integer.parseInt(args[2]) : 100_000;
        System.out.println("Generating "+files+" files into "+dataDir+" with "+usersPerFile+" users per file");
        new GenerateFriendster(dataDir).generate(files, usersPerFile);
    }

    private void generate(int files, int usersPerFile) throws IOException {
        int total = files * usersPerFile;
        StringBuilder sb = new StringBuilder(1000);
        Random random = new Random(4711);
        for (int file=0;file<files;file++) {
            String fileName = dir + String.format("/friends-%03d______.txt.bz2", file); // friends-000______.txt.bz2
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new BZip2CompressorOutputStream(new FileOutputStream(fileName)), 1024 * 1024));
            for (int user = 0;user< usersPerFile;user++) {
                int userId = user + file* usersPerFile;
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
