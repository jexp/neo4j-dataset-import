package friendster;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.FormattedLogProvider;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author mh
 * @since 03.04.15
 */
public class ImportFriendster2 {
    public static final String[] LABELS = new String[]{"Person"};
    public static final String REL_TYPE = "FRIEND_OF";
    public static final int MEGA_BYTE = 1024 * 1024;
    private final File store;

    public ImportFriendster2(File store) {
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
        new ImportFriendster2(store).run(files);
    }

    private void run(final File[] files) throws IOException {
        ParallelBatchImporter importer = new ParallelBatchImporter(store, Configuration.DEFAULT, createLogging(), ExecutionMonitors.defaultVisible());
        importer.doImport(new Input() {
            public InputIterable<InputNode> nodes() {
                final InputFileIterator nodes = new InputFileIterator(files,true);
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
                final InputFileIterator rels = new InputFileIterator(files,false);
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
            public Collector badCollector() {
                return Collectors.badCollector(System.err,0);
            }
        });
    }

    protected LogService createLogging() {
        return new SimpleLogService(
                FormattedLogProvider.toOutputStream(System.err),
                FormattedLogProvider.toOutputStream(System.err));
    }

    private static class InputFileIterator implements Iterator<Pair<InputNode,List<InputRelationship>>> {
        public static final Object[] NO_PROPS = new Object[0];
        private final File[] files;

        private final boolean nodes;
        public int lineNo;
        int currentFile;
        boolean eof;
        InputStream reader;
        String currentName;

        public InputFileIterator(File[] files, boolean nodes) {
            this.files = files;
            this.nodes = nodes;
            currentFile = -1;
            nextFile();
            relCount = readLine();
        }

        public boolean hasNext() {
            return relCount != -1;
        }

        byte[] buffer = new byte[MEGA_BYTE];
        int offset = -1, size = buffer.length;
        int nodeId;
        int [] rels = new int[MEGA_BYTE];
        int relCount;
        List<Integer> rels2 = new ArrayList<Integer>(MEGA_BYTE);

        private int readLine() {
            try {
                if (!nextChunkOrFile()) return -1;
                nodeId = 0;
                while (buffer[offset] != ':') {
                    nodeId = nodeId * 10 + buffer[offset]-'0';
                    offset++;
                    if (offset>=size && !nextChunkOrFile()) return -1;
                }
                offset++;
                int relCount = 0;
                if (!rels2.isEmpty()) rels2.clear();
                int rel = -1;
                if (offset>=size && !nextChunkOrFile()) return -1;
                while (buffer[offset] != '\n') {
                    byte c = buffer[offset];
                    if (c=='p') {
                        offset += 6; //"private"
                    } else if (c =='n') {
                        offset += 8; // notfound
                    } else if (c ==',') {
                        if (rel != -1) {
                            if (relCount < rels.length) {
                                rels[relCount] = rel;
                            } else {
                                rels2.add(rel);
                            }
                            relCount++;
                        }
                        rel = -1;
                    } else if (c >= '0' && c <= '9') {
                        if (rel==-1) rel = 0;
                        rel = rel * 10 + c-'0';
                    } else {
                        throw new IllegalStateException("Illegal character "+Character.toString((char)c));
                    }
                    offset++;
                    if (offset>=size && !nextChunkOrFile()) return relCount;
                }
                if (offset<size && buffer[offset]=='\n') offset++;
                lineNo++;
                return relCount;
            } catch(IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        private boolean nextChunkOrFile() throws IOException {
            if (offset == -1 || offset >= size ) {
                int read  = reader.read(buffer);
                if (read == -1) {
                    if (!nextFile()) return false;
                    read = reader.read(buffer);
                    offset = 0;
                }
                if (read==-1) {
                    throw new IllegalStateException("current "+currentFile+" offset "+offset+" read "+read+" size "+size);
                }
                size = read;
                offset = offset > size ? offset - size : 0;
            }
            return true;
        }

        public Pair<InputNode, List<InputRelationship>> next() {
            if (relCount==-1) throw new IllegalStateException("next called even if there was no next");
            InputNode node = nodes ? new InputNode(currentName, lineNo, 0, nodeId, new Object[]{"id", nodeId}, null, LABELS, null) : null;
            List<InputRelationship> rels = nodes ? null : inputRels(nodeId, relCount);
            Pair<InputNode, List<InputRelationship>> result = Pair.of(node, rels);
            relCount = readLine();
            return result;
        }
/*
        public Pair<InputNode, List<InputRelationship>> next() {
            String line = currentLine;
            if (line==null) throw new IllegalStateException("next called even if there was no next");
            int idx = line.indexOf(':');
            long from = Long.parseLong(line.substring(0, idx));
            InputNode nodeId = nodes ? new InputNode(currentName, lineNo, 0, from, new Object[]{"id", from}, null, LABELS, null) : null;
            List<InputRelationship> rels = nodes ? null : inputRels(line, from, idx);
            Pair<InputNode, List<InputRelationship>> result = Pair.of(nodeId, rels);
            currentLine = readLine();
            return result;
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
*/

        private boolean nextFile() {
            if (++currentFile >= files.length) return false;
            try {
                if (reader != null) reader.close();
                if (currentName!=null) System.out.println("End  File "+currentName);
                reader = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(files[currentFile]), MEGA_BYTE));
                currentName = files[currentFile].getName();
                System.out.println("Start File "+currentName);
                lineNo = 0;
                return true;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        private List<InputRelationship> inputRels(long from, int idx) {
            List<InputRelationship> result=new ArrayList<>(relCount);
            int num = Math.min(rels.length, relCount);
            for (int i=0;i<num;i++) {
                result.add(new InputRelationship(currentName, lineNo, 0, NO_PROPS, null, from, rels[i], REL_TYPE, null));
            }
            if (relCount >= rels.length) {
                for (Integer relId : rels2) {
                    result.add(new InputRelationship(currentName, lineNo, 0, NO_PROPS, null, from, relId, REL_TYPE, null));
                }
            }
            return result;
        }

        public void remove() { }
    }
}
