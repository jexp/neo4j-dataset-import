package friendster;

import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.FormattedLogProvider;
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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author mh
 * @since 03.04.15
 */
public class MultiFileParallelImporter2 {
    public static final int MEGA_BYTE = 1024 * 1024;
    private final File store;

    public MultiFileParallelImporter2(File store) {
        this.store = store;
    }


    public void run(final File[] files, final IdMapper idMapper, final int badRelsTolerance, final Generator2 generator) throws IOException {
        ParallelBatchImporter importer = new ParallelBatchImporter(store, createConfiguration(), createLogging(), createExecutionMonitors());
        importer.doImport(new Input() {
            public InputIterable<InputNode> nodes() {
                final InputFileIteratorSeeker nodes = new InputFileIteratorSeeker(files,true, generator);
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
                final InputFileIteratorSeeker rels = new InputFileIteratorSeeker(files,false, generator);
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
            public Collector badCollector() {
                return Collectors.badCollector(System.err, badRelsTolerance);
            }
        });
    }

    protected Configuration createConfiguration() {
        return Configuration.DEFAULT;
    }

    protected ExecutionMonitor createExecutionMonitors() {
        return ExecutionMonitors.defaultVisible();
    }

    protected LogService createLogging() {
        return new SimpleLogService(
                FormattedLogProvider.toOutputStream(System.err),
                FormattedLogProvider.toOutputStream(System.err));
    }

}
