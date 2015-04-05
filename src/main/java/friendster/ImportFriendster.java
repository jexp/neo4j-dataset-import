package friendster;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.lang.Long.parseLong;

/**
 * @author mh
 * @since 03.04.15
 */
public class ImportFriendster {
    public static final String[] LABELS = new String[]{"Person"};
    public static final String REL_TYPE = "FRIEND_OF";
    public static final Object[] NO_PROPS = new Object[0];


    public static void main(String[] args) throws IOException {
        if (args.length<2) {
            System.err.println("Usage ImportFriendster friendster.db file-or-directory");
        }
        File store = new File(args[0]);
        FileUtils.deleteRecursively(store);

        File[] files = getDataFiles(args[1]);

        final MultiFileParallelImporter importFriendster = new MultiFileParallelImporter(store);
        IdMapper idMapper = IdMappers.longs(NumberArrayFactory.AUTO);

        MultiFileParallelImporter.Generator generator = new MultiFileParallelImporter.Generator() {

            public InputNode createNode(String line, long position, String fileName, int lineNo) {
                int idx = line.indexOf(':');
                long nodeId = parseLong(line.substring(0, idx));
                return new InputNode(fileName, lineNo, position, nodeId, new Object[]{"id", nodeId}, null, LABELS, null);
            }

            public Iterable<InputRelationship> createRelationships(String line, long position, String fileName, int lineNo) {
                int idx = line.indexOf(':');
                long nodeId = parseLong(line.substring(0, idx));
                String[] ids = line.substring(idx + 1).split(",");
                List<InputRelationship> rels =new ArrayList<>(ids.length);
                for (String id : ids) {
                    if (id.equals("notfound") || id.equals("private") || id.trim().isEmpty()) continue;
                    rels.add(new InputRelationship(fileName, lineNo, position, NO_PROPS, null, nodeId, parseLong(id), REL_TYPE, null));
                }
                return rels;
            }
        };
        importFriendster.run(files, idMapper, 0, generator);
    }

    private static File[] getDataFiles(String arg) {
        File fileOrDirectory = new File(arg);
        File[] files;
        if (fileOrDirectory.isDirectory()) {
            files = fileOrDirectory.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.matches("friends-\\d+_+\\.txt\\.bz2");
                }
            });
        } else {
            files = new File[] {fileOrDirectory};
        }
        Arrays.sort(files, new Comparator<File>() {
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return files;
    }
}
