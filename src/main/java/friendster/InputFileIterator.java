package friendster;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.neo4j.helpers.Pair;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.io.*;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

/**
 * @author mh
 * @since 13.02.16
 */
public class InputFileIterator implements Iterator<Pair<InputNode, Iterable<InputRelationship>>> {
    private final File[] files;
    private final boolean returnNodes;
    public int lineNo;
    private int currentFile;
    private BufferedReader reader;
    String currentName;
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
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public Pair<InputNode, Iterable<InputRelationship>> next() {
        String line = currentLine;
        if (line == null) throw new IllegalStateException("next called even if there was no next");
        Pair<InputNode, Iterable<InputRelationship>> result;
        long position = 0;
        String fileName = currentName;
        int lineNo = this.lineNo;
        if (returnNodes) {
            result = Pair.of(generator.createNode(line, position, fileName, lineNo), null);
        } else {
            result = Pair.of(null, generator.createRelationships(line, position, fileName, lineNo));
        }
        currentLine = readLine();
        return result;
    }

    private boolean nextFile() {
        try {
            if (reader != null) reader.close();
            if (currentName != null) System.out.println("\nEnd  File " + currentName);
            if (++currentFile >= files.length) return false;
            File currentFile = files[this.currentFile];
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(currentFile), MultiFileParallelImporter.MEGA_BYTE);
            InputStream stream = decompressedStream(bis, currentFile.getName());
            reader = new BufferedReader(new InputStreamReader(stream), MultiFileParallelImporter.MEGA_BYTE);
            currentName = currentFile.getName();
            System.out.println("\nStart File " + currentName);
            lineNo = 0;
            return true;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
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
