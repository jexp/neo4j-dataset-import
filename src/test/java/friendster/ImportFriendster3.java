package friendster;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

import static java.lang.Long.parseLong;

/**
 * @author mh
 * @since 03.04.15
 */
public class ImportFriendster3 {
    public static final String[] LABELS = new String[]{"Person"};
    public static final String REL_TYPE = "FRIEND_OF";
    public static final Object[] NO_PROPS = new Object[0];


    public static void main(String[] args) throws IOException {
        if (args.length<2) {
            System.err.println("Usage ImportFriendster3 friendster.db file-or-directory");
        }
        File store = new File(args[0]);
        FileUtils.deleteRecursively(store);

        File[] files = getDataFiles(args[1]);

        final MultiFileParallelImporter2 importFriendster = new MultiFileParallelImporter2(store);
        IdMapper idMapper = IdMappers.actual();//.longs(NumberArrayFactory.AUTO);

        Generator2 generator = new Generator2() {
            Extractors extractors = new Extractors(',',true);
            Extractors.LongExtractor longExtractor = extractors.long_();
            Extractor<long[]> longArrayExtractor = extractors.longArray();

            public InputNode createNode(CharSeeker seeker, Mark mark) throws IOException {
                if (seeker.seek(mark, ':')) {
                    long nodeId = seeker.extract(mark, longExtractor).longValue();
                    seeker.seek(mark, '\n');
                    return new InputNode(seeker.sourceDescription(), seeker.lineNumber(), seeker.position(), nodeId, new Object[]{"id", nodeId}, null, LABELS, null);
                }
                if (!mark.isEndOfLine()) seeker.seek(mark, '\n');
                return null;
            }

            public Iterable<InputRelationship> createRelationships(CharSeeker seeker, Mark mark) throws IOException {
                if (seeker.seek(mark, ':')) {
                    long nodeId = seeker.extract(mark, longExtractor).longValue();
                    if (seeker.seek(mark, '\n')) {
                        boolean found = false;
                        try {
                            found = seeker.tryExtract(mark, longArrayExtractor);
                        } catch(NumberFormatException nfe) {
                            // if (id.equals("notfound") || id.equals("private") || id.trim().isEmpty()) continue;
                            // ignore
                        }
                        if (!found) return Collections.emptyList();
                        long[] ids = longArrayExtractor.value();
                        List<InputRelationship> rels = new ArrayList<>(ids.length);
                        for (long id : ids) {
                            rels.add(new InputRelationship(seeker.sourceDescription(), seeker.lineNumber(), seeker.position(), NO_PROPS, null, nodeId, id, REL_TYPE, null));
                        }
                        return rels;
                    }
                }
                if (!mark.isEndOfLine()) seeker.seek(mark,'\n');
                return null;
            }
        };

        long start = System.currentTimeMillis();
        importFriendster.run(files, idMapper, 0, generator);
        System.err.println("Total time for importing "+files.length+" files: "+(System.currentTimeMillis() - start)/1000+" seconds.");
    }

    private static File[] getDataFiles(String arg) {
        File fileOrDirectory = new File(arg);
        File[] files;
        if (fileOrDirectory.isDirectory()) {
            files = fileOrDirectory.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.matches("friends-\\d+_+\\.txt\\.(bz2|gz)");
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
