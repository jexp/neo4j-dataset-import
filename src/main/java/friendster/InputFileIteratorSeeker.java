package friendster;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.CharSeekers;
import org.neo4j.csv.reader.Mark;
import org.neo4j.csv.reader.Readables;
import org.neo4j.helpers.Pair;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

/**
 * @author mh
 * @since 13.02.16
 */
public class InputFileIteratorSeeker implements Iterator<Pair<InputNode, Iterable<InputRelationship>>> {
    private static final char QUOTE = '"';
    private final File[] files;
    private final boolean returnNodes;
    public int lineNo;
    private int currentFile;
//    private BufferedReader reader;
String currentName;
//    private String currentLine;
    private final Generator2 generator;
    private CharSeeker seeker;
    private InputStream stream;
    private Mark mark = new Mark();
    private Pair<InputNode, Iterable<InputRelationship>> pair;

    public InputFileIteratorSeeker(File[] files, boolean returnNodes, Generator2 generator) {
        this.files = files;
        this.returnNodes = returnNodes;
        this.generator = generator;
        currentFile = -1;
        nextFile();
        seeker = nextFile();
        pair = readNext();
    }

    public boolean hasNext() {
        return pair != null;
    }

    public Pair<InputNode, Iterable<InputRelationship>> next() {
        if (seeker == null) throw new IllegalStateException("next called even if there was no next");
        Pair<InputNode, Iterable<InputRelationship>> current = pair;
        pair = readNext();
        return current;
    }

    private Pair<InputNode, Iterable<InputRelationship>> readNext() {
        if (seeker == null) return null;
        try {
            boolean eof;
            Pair<InputNode, Iterable<InputRelationship>> result;
            if (returnNodes) {
                InputNode node = generator.createNode(seeker, mark);
                result = Pair.of(node, null);
                eof = node == null;
            } else {
                Iterable<InputRelationship> rels = generator.createRelationships(seeker, mark);
                result = Pair.of(null, rels);
                eof = rels == null;
            }
            if (eof) {
                seeker = nextFile();
                return readNext();
            }
            return result;
        } catch (IOException ioe) {
            throw new RuntimeException("Error reading from " + seeker,ioe);
        }
    }

    private CharSeeker nextFile() {
        try {
            if (stream != null) stream.close();
            if (currentName != null) System.out.println("\nEnd  File " + currentName);
            if (++currentFile >= files.length) return null;
            File currentFile = files[this.currentFile];
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(currentFile), MultiFileParallelImporter.MEGA_BYTE);
            stream = decompressedStream(bis, currentFile.getName());
//            reader = new BufferedReader(new InputStreamReader(stream), MultiFileParallelImporter.MEGA_BYTE);
            CharSeeker seeker = CharSeekers.charSeeker(Readables.wrap(stream, currentFile.getName(), Charset.defaultCharset()), MultiFileParallelImporter.MEGA_BYTE, true, QUOTE);
            currentName = currentFile.getName();
            System.out.println("\nStart File " + currentName);
            lineNo = 0;
            return seeker;
        } catch (IOException ioe) {
            throw new RuntimeException("Error reading file "+currentFile,ioe);
        }
    }

    private InputStream decompressedStream(InputStream is, String fileName) throws IOException {
        if (fileName.endsWith(".bz2")) return new BZip2CompressorInputStream(is);
        if (fileName.endsWith(".zip")) return new ZipInputStream(is);
        if (fileName.endsWith(".gz")) return new GZIPInputStream(is);
        return is;
    }

    public void remove() {
    }
}
