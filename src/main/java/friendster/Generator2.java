package friendster;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Mark;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.io.IOException;

/**
 * @author mh
 * @since 13.02.16
 */
public interface Generator2 {
    Iterable<InputRelationship> createRelationships(CharSeeker seeker, Mark mark) throws IOException;

    InputNode createNode(CharSeeker seeker, Mark mark) throws IOException;
}
