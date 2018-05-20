package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class JoinFiles{
    static String queryPattern = "(LEFT|RIGHT) [A-Za-z0-9_]+\\.[A-Za-z0-9_]+ *= *[A-Za-z0-9]+\\.[A-Za-z0-9]+";
    Direction direction;
    ArrayList<String[]>  firstFile, secondFile;
    String[] header1, header2;
    String firstFileName, secondFileName;
    String firstColumnName, secondColumnName;
    Integer fColumnIdx, sColumnIdx;
    ArrayList<Integer[]> pairs = new ArrayList<>();

    public JoinFiles() {}

    /**
     * Check if query matches the pattern
     * @param query
     * @throws NoMachingQueryPatternException
     */
    void matchQuery(String query) throws NoMachingQueryPatternException {
        boolean match = Pattern.matches(queryPattern, query);
        if(!match) {
            throw new NoMachingQueryPatternException();
        }
    }

    /**
     * Read data from file to ArrayList of String[]
     * @param pathToDir
     * @param fileName
     * @return
     * @throws IOException
     */
    private ArrayList readFile(Path pathToDir, String fileName) throws IOException {
        FileReader fr = new FileReader(pathToDir+"/"+fileName);
        BufferedReader br = new BufferedReader(fr);
        //read headers
        String header = br.readLine();
        String[] headers = header.split(";");
        ArrayList<String[]> file = new ArrayList<>();
        file.add(headers);

        //read content of the file
        String line;
        String[] fields;
        while((line = br.readLine())!= null) {
            fields = line.split(";");
            file.add(fields);
        }

        return file;
    }

    /**
     * Extract all data needed for the join operation from query
     * @param pathToDir
     * @param query
     * @throws IOException
     */
    private void getDataFromQuery(Path pathToDir, String query) throws IOException {
        String[] queryParts = query.split("[\\ =]");

        this.direction = Direction.valueOf(queryParts[0]);

        String[] firstSplit = queryParts[1].split("[\\.]");
        firstFileName = firstSplit[0];
        firstColumnName = firstSplit[1];

        String[] secondSplit = queryParts[queryParts.length-1].split("[\\.]");
        secondFileName = secondSplit[0];
        secondColumnName = secondSplit[1];

        firstFile = this.readFile(pathToDir, firstFileName);
        secondFile = this.readFile(pathToDir, secondFileName);

        header1 = firstFile.get(0);
        header2 = secondFile.get(0);

        for(int i = 0; i < header1.length; i++) {
            if(header1[i].equals(firstColumnName)) {
                fColumnIdx = i;
                break;
            }
        }

        for(int i = 0; i < header2.length; i++) {
            if(header2[i].equals(secondColumnName)) {
                sColumnIdx = i;
                break;
            }
        }
    }

    /**
     * Find matching pairs in columns got from the query
     */
    private void findPairs() {
        for (int i = 0; i < firstFile.size(); i++) {
            for (int j = 0; j < secondFile.size(); j++) {
                if (firstFile.get(i)[fColumnIdx].equals(secondFile.get(j)[sColumnIdx])) {
                    Integer[] pair = new Integer[]{i, j};
                    pairs.add(pair);
                }
            }
        }
    }

    /**
     * Write headers of both files to OutputStream
     * @param stream
     * @throws IOException
     */
    private void writeHeader(OutputStream stream) throws IOException {
        for(int i = 0; i < header1.length; i++) {
            byte[] tmp = header1[i].getBytes();
            stream.write(tmp);
            stream.write(";".getBytes());
        }
        for(int i = 0; i < header2.length-1; i++) {
            byte[] tmp = header2[i].getBytes();
            stream.write(tmp);
            stream.write(";".getBytes());
        }
        byte[] tmp = header2[header2.length-1].getBytes();
        stream.write(tmp);
        stream.write("\n".getBytes());
    }

    /**
     * Write rows to OutputStream
     * If it's LEFT join
     * @param stream
     * @throws IOException
     */
    private void writeLEFT(OutputStream stream) throws IOException {
        int[] put = new int[firstFile.size()];
        for(Integer[] p : pairs) {
            for(String s : firstFile.get(p[0])) {
                stream.write((s+";").getBytes());
            }
            for(int i = 0; i < secondFile.get(p[1]).length-1; i++) {
                stream.write((secondFile.get(p[1])[i]+";").getBytes());
            }
            stream.write((secondFile.get(p[1])[header2.length-1]).getBytes());
            stream.write("\n".getBytes());
            put[p[0]] = 1;
        }
        for(int i = 1; i < firstFile.size(); i++) {
            if(put[i] == 0) {
                for(String s : firstFile.get(i)) {
                    stream.write((s+";").getBytes());
                }
                for(int j = 0; j < header2.length-1; j++) {
                    stream.write(";".getBytes());
                }
                stream.write("\n".getBytes());
            }
        }
    }

    /**
     * Write rows to OutputStream
     * If it's RIGHT join
     * @param stream
     * @throws IOException
     */
    void writeRIGHT(OutputStream stream) throws IOException {
        int[] put = new int[secondFile.size()];
        for(Integer[] p : pairs) {
            for(String s : firstFile.get(p[0])) {
                stream.write((s+";").getBytes());
            }
            for(int i = 0; i < secondFile.get(p[1]).length-1; i++) {
                stream.write((secondFile.get(p[1])[i]+";").getBytes());
            }
            stream.write((secondFile.get(p[1])[header2.length-1]).getBytes());
            stream.write("\n".getBytes());
            put[p[1]] = 1;
        }
        for(int i = 1; i < secondFile.size(); i++) {
            if (put[i] == 0) {
                for (int j = 0; j < header1.length; j++) {
                    stream.write(";".getBytes());
                }
                for (int j = 0; j < header2.length - 1; j++) {
                    stream.write((secondFile.get(i)[j] + ";").getBytes());
                }
                stream.write((secondFile.get(i)[header2.length - 1]).getBytes());
                stream.write("\n".getBytes());
            }
        }
    }

    void join(Path pathToDir, String query, OutputStream stream) throws IOException, NoMachingQueryPatternException {
        matchQuery(query);
        getDataFromQuery(pathToDir, query);
        findPairs();
        writeHeader(stream);
        if(direction.equals(Direction.LEFT)) {
            writeLEFT(stream);
        } else if(direction.equals(Direction.RIGHT)) {
            writeRIGHT(stream);
        }
    }
}
