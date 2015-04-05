package friendster;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.*;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import java.io.*;
import java.util.Iterator;

/**
 * @author mh
 * @since 03.04.15
 */
public class MultiFileParallelImporter {
    public static final int MEGA_BYTE = 1024 * 1024;
    private final File store;

    public MultiFileParallelImporter(File store) {
        this.store = store;
    }


    public void run(final File[] files, final IdMapper idMapper, final int badRelsTolerance, final Generator generator) throws IOException {
        ParallelBatchImporter importer = new ParallelBatchImporter(store.getAbsolutePath(), createConfiguration(), createLogging(), createExecutionMonitors());
        importer.doImport(new Input() {
            public InputIterable<InputNode> nodes() {
                final InputFileIterator nodes = new InputFileIterator(files,true, generator);
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
                final InputFileIterator rels = new InputFileIterator(files,false, generator);
                return new InputIterable<InputRelationship>() {
                    public InputIterator<InputRelationship> iterator() {
                        return new InputIterator<InputRelationship>() {
                            Iterator<InputRelationship> current = rels.hasNext() ? rels.next().other().iterator() : null;
                            public boolean hasNext() {
                                while (current == null || !current.hasNext()) {
                                    if (rels.hasNext()) {
                                        current = rels.next().other().iterator();
                                    } else {
                                        break;
                                    }
                                }
                                return current != null && current.hasNext();
                            }
                            public InputRelationship next() {
                                return nextRelationship();
                            }

                            private InputRelationship nextRelationship() {
                                if (current != null && current.hasNext()) return current.next();
                                throw new IllegalStateException("next called despite hasNext was false");
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
                return idMapper;
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
                return Collectors.badRelationshipsCollector(System.err, badRelsTolerance);
            }
        });
    }

    protected Configuration createConfiguration() {
        return Configuration.DEFAULT;
    }

    protected ExecutionMonitor createExecutionMonitors() {
        return ExecutionMonitors.defaultVisible();
    }

    protected SystemOutLogging createLogging() {
        return new SystemOutLogging();
    }

    public static interface Generator {
        Iterable<InputRelationship> createRelationships(String line, long position, String fileName, int lineNo);
        InputNode createNode(String line, long position, String fileName, int lineNo);
    }

    private static class InputFileIterator implements Iterator<Pair<InputNode,Iterable<InputRelationship>>> {
        private final File[] files;
        private final boolean returnNodes;
        public int lineNo;
        private int currentFile;
        private BufferedReader reader;
        private String currentName;
        private String currentLine;
        private final Generator generator;

        public InputFileIterator(File[] files, boolean returnNodes, Generator generator) {
            this.files = files;
            this.returnNodes = returnNodes;
            this.generator = generator;
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

        public Pair<InputNode, Iterable<InputRelationship>> next() {
            String line = currentLine;
            if (line==null) throw new IllegalStateException("next called even if there was no next");
            Pair<InputNode, Iterable<InputRelationship>> result;
            long position = 0;
            String fileName = currentName;
            int lineNo = this.lineNo;
            if (returnNodes) {
                result = Pair.of(generator.createNode(line, position, fileName, lineNo),  null);
            } else {
                result = Pair.of(null, generator.createRelationships(line, position, fileName, lineNo));
            }
            currentLine = readLine();
            return result;
        }

        private boolean nextFile() {
            try {
                if (reader != null) reader.close();
                if (currentName!=null) System.out.println("\nEnd  File " + currentName);
                if (++currentFile >= files.length) return false;
                reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(files[currentFile]), MEGA_BYTE))),MEGA_BYTE);
                currentName = files[currentFile].getName();
                System.out.println("\nStart File "+currentName);
                lineNo = 0;
                return true;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        public void remove() { }
    }

}
