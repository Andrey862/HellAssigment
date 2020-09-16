import java.io.*;
import java.util.Locale;
import java.util.StringTokenizer;
 

public class test {
    String filename = "subseq";//filename here, System.in/out if no file
 
    FastScanner in;
    PrintWriter out;
 
    void solve() {
        //your code here
        String str = in.next().replaceAll("[^a-zA-Z- ]", "");
        out.println(str);
    }
 
    void run() throws IOException {
        InputStream input = System.in;
        OutputStream output = System.out;
        try {
            File f = new File(filename + ".in");
            if (f.exists() && f.canRead()) {
                input = new FileInputStream(f);
                output = new FileOutputStream(filename + ".out");
            }
        } catch (IOException e) {
        }
        in = new FastScanner(input);
        out = new PrintWriter(new BufferedOutputStream(output));
        solve();
        in.close();
        out.close();
    }
 
    public static void main(String[] args) throws IOException {
        Locale.setDefault(Locale.US);
        new test().run();
    }
    
    class FastScanner implements Closeable {
        private BufferedReader br;
        private StringTokenizer tokenizer;
 
        public FastScanner(InputStream stream) throws FileNotFoundException {
            br = new BufferedReader(new InputStreamReader(stream));
        }
 
        public boolean hasNext() {
            while (tokenizer == null || !tokenizer.hasMoreTokens()) {
                try {
                    String s = br.readLine();
                    if (s == null) {
                        return false;
                    }
                    tokenizer = new StringTokenizer(s);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return tokenizer.hasMoreTokens();
        }
 
        public String next() {
            while (tokenizer == null || !tokenizer.hasMoreTokens()) {
                try {
                    tokenizer = new StringTokenizer(br.readLine());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return tokenizer.nextToken();
        }
        
        public String nextLine() {
            if (tokenizer == null || !tokenizer.hasMoreTokens()) {
                try {
                    return br.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return tokenizer.nextToken("\n");
        }
 
        public int nextInt() {
            return Integer.parseInt(next());
        }
 
        public long nextLong() {
            return Long.parseLong(next());
        }
 
        public double nextDouble() {
            return Double.parseDouble(next());
        }
 
        @Override
        public void close() throws IOException {
            br.close();
        }
    }
}