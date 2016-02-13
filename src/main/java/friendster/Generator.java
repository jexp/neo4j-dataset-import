package friendster;

import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * @author mh
 * @since 13.02.16
 */
public interface Generator {
    Iterable<InputRelationship> createRelationships(String line, long position, String fileName, int lineNo);

    InputNode createNode(String line, long position, String fileName, int lineNo);
}
