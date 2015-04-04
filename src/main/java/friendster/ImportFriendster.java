package friendster;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.*;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import java.io.*;
import java.util.*;

/**
 * @author mh
 * @since 03.04.15
 */
public class ImportFriendster {
    public static final String[] LABELS = new String[]{"Person"};
    public static final String REL_TYPE = "FRIEND_OF";
    private final File store;

    public ImportFriendster(File store) {
        this.store = store;
    }

    public static void main(String[] args) throws IOException {
        if (args.length<2) {
            System.err.println("Usage ImportFriendster friendster.db file-or-directory");
        }
        File store = new File(args[0]);
        FileUtils.deleteRecursively(store);
        File fileOrDirectory = new File(args[1]);
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
        new ImportFriendster(store).run(files);
    }

    private void run(final File[] files) throws IOException {
        ParallelBatchImporter importer = new ParallelBatchImporter(store.getAbsolutePath(), Configuration.DEFAULT, new SystemOutLogging(), ExecutionMonitors.defaultVisible());
        importer.doImport(new Input() {
            public InputIterable<InputNode> nodes() {
                final InputFileIterator nodes = new InputFileIterator(files);
                return new InputIterable<InputNode>() {
                    public InputIterator<InputNode> iterator() {
                        return new InputIterator<InputNode>() {
                            public boolean hasNext() { return nodes.hasNext(); }
                            public InputNode next() { return nodes.next().first(); }
                            public void remove() { }
                            public void close() { }
                            public String sourceDescription() { return nodes.currentName; }
                            public long lineNumber() { return nodes.lineNo; }
                            public long position() { return 0; }
                        };
                    }

                    @Override
                    public boolean supportsMultiplePasses() {
                        return false;
                    }
                };
            }

            @Override
            public InputIterable<InputRelationship> relationships() {
                final InputFileIterator rels = new InputFileIterator(files);
                return new InputIterable<InputRelationship>() {
                    public InputIterator<InputRelationship> iterator() {
                        return new InputIterator<InputRelationship>() {
                            Iterator<InputRelationship> current = rels.hasNext() ? rels.next().other().iterator() : null;
                            public boolean hasNext() { return current != null && current.hasNext() || rels.hasNext(); }
                            public InputRelationship next() {
                                return nextRelationship();
                            }

                            private InputRelationship nextRelationship() {
                                if (current==null) throw new IllegalStateException("next called despite hasNext was false");
                                if (current.hasNext()) return current.next();
                                else {
                                    current = rels.next().other().iterator();
                                    return nextRelationship();
                                }
                            }

                            public void remove() { }
                            public void close() { }
                            public String sourceDescription() { return rels.currentName; }
                            public long lineNumber() { return rels.lineNo; }
                            public long position() { return 0; }
                        };
                    }

                    @Override
                    public boolean supportsMultiplePasses() {
                        return false;
                    }
                };
            }

            @Override
            public IdMapper idMapper() {
                return IdMappers.longs(NumberArrayFactory.AUTO);
            }

            @Override
            public IdGenerator idGenerator() {
                return IdGenerators.startingFromTheBeginning();
            }

            @Override
            public boolean specificRelationshipIds() {
                return false;
            }

            @Override
            public Collector<InputRelationship> badRelationshipsCollector(OutputStream out) {
                return Collectors.badRelationshipsCollector(System.err,0);
            }
        });
    }

    private static class InputFileIterator implements Iterator<Pair<InputNode,List<InputRelationship>>> {
        public static final Object[] NO_PROPS = new Object[0];
        private final File[] files;
        public int lineNo;
        int currentFile;
        boolean eof;
        BufferedReader reader;
        String currentName;
        String currentLine;

        public InputFileIterator(File[] files) {
            this.files = files;
            currentFile = -1;
            nextFile();
            currentLine = readLine();
        }

        public boolean hasNext() {
            return currentLine != null;
        }

        private String readLine() {
            try {
                String line = reader.readLine();
                if (line == null) {
                    if (!nextFile()) return null;
                    else line = reader.readLine();
                }
                lineNo++;
                return line;
            } catch(IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        public Pair<InputNode, List<InputRelationship>> next() {
            String line = currentLine;
            if (line==null) throw new IllegalStateException("next called even if there was no next");
            int idx = line.indexOf(':');
            long from = Long.parseLong(line.substring(0, idx));
            Pair<InputNode, List<InputRelationship>> result = Pair.of(new InputNode(currentName, lineNo, 0, from, new Object[]{"id", from}, null, LABELS, null), inputRels(line, from, idx));
            currentLine = readLine();
            return result;
        }

        private boolean nextFile() {
            if (++currentFile >= files.length) return false;
            try {
                if (reader != null) reader.close();
                if (currentName!=null) System.out.println("End  File "+currentName);
                reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(files[currentFile]))), 1 << 20);
                currentName = files[currentFile].getName();
                System.out.println("Start File "+currentName);
                lineNo = 0;
                return true;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        private List<InputRelationship> inputRels(String line, long from, int idx) {
            String[] ids = line.substring(idx + 1).split(",");
            List<InputRelationship> result=new ArrayList<InputRelationship>(ids.length);
            for (String id : ids) {
                if (id.equals("notfound") || id.equals("private") || id.trim().isEmpty()) continue;
                result.add(new InputRelationship(currentName, lineNo, idx, NO_PROPS, null, from, Long.parseLong(id), REL_TYPE, null));
            }
            return result;
        }

        public void remove() { }
    }
}
