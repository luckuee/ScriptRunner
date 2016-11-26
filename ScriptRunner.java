//https://github.com/BenoitDuffez/ScriptRunner/blob/master/ScriptRunner.java
//https://www.mkyong.com/jdbc/how-to-run-a-mysql-script-using-java/
//http://www.codeproject.com/Articles/802383/Run-SQL-Script-sql-containing-DDL-DML-SELECT-state
/**
 
 */


import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tool to run database scripts
 */
public class ScriptRunner {


    public static void main(String[] args) throws Exception {
        Connection connection = DBUtil.getStandAloneConnection();


        if (!DBUtil.mDbInfo.getDbType().equalsIgnoreCase("oracle")) {
            DBUtil.fullLineDelimiter = "GO";
            File folder = new File(DBUtil.getCurrentProductionDirectory());
            File[] listOfFiles = folder.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {//_sysadmin
                    if (listOfFiles[i].getName().endsWith(".sql") && listOfFiles[i].getName().contains("SQLServer") && listOfFiles[i].getName().contains("sysadmin"))
                        DBUtil.adminScriptFile = listOfFiles[i].getName();
                    if (listOfFiles[i].getName().endsWith(".sql") && listOfFiles[i].getName().contains("SQLServer") && !listOfFiles[i].getName().contains("sysadmin"))
                        DBUtil.productScriptFile = listOfFiles[i].getName();
                }
            }
        } else {
            DBUtil.fullLineDelimiter = "/";
            File folder = new File(DBUtil.getCurrentProductionDirectory());
            File[] listOfFiles = folder.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {//_sysadmin
                    if (listOfFiles[i].getName().endsWith(".sql") && listOfFiles[i].getName().contains("Oracle") && listOfFiles[i].getName().contains("DBA"))
                        DBUtil.adminScriptFile = listOfFiles[i].getName();
                    if (listOfFiles[i].getName().endsWith(".sql") && listOfFiles[i].getName().contains("Oracle") && !listOfFiles[i].getName().contains("DBA"))
                        DBUtil.productScriptFile = listOfFiles[i].getName();
                }
            }
        }

        System.out.println("product script file name is: " + DBUtil.productScriptFile);
        System.out.println("admin script file name is: " + DBUtil.adminScriptFile);

        System.out.println("Database connected with product user credential.");
        ScriptRunner runner = new ScriptRunner(connection, false, true);
        String absoluteFilePath = DBUtil.getCurrentProductionDirectory() + File.separator + DBUtil.productScriptFile;
        if (DBUtil.fullLineDelimiter != null) {
            runner.setDelimiter(DBUtil.fullLineDelimiter, true);
        }
        runner.runScript(new BufferedReader(new FileReader(absoluteFilePath)));
        connection.close();

        connection = DBUtil.getStandAloneAdminConnection();
        //TODO if connection fails, then u should ask the admin credential again
        System.out.println("Database connected with admin user credential.");
        runner = new ScriptRunner(connection, false, true);
        absoluteFilePath = DBUtil.getCurrentProductionDirectory() + File.separator + DBUtil.adminScriptFile;
        if (DBUtil.fullLineDelimiter != null) {
            runner.setDelimiter(DBUtil.fullLineDelimiter, true);
        }
        runner.runScript(new BufferedReader(new FileReader(absoluteFilePath)));
        connection.close();

    }

    private static final String DEFAULT_DELIMITER = ";";
    /**
     * regex to detect delimiter.
     * ignores spaces, allows delimiter in comment, allows an equals-sign
     */
    public static final Pattern delimP = Pattern.compile("^\\s*(--)?\\s*delimiter\\s*=?\\s*([^\\s]+)+\\s*.*$", Pattern.CASE_INSENSITIVE);

    private final Connection connection;

    private final boolean stopOnError;
    private final boolean autoCommit;

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private PrintWriter logWriter = new PrintWriter(System.out);
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private PrintWriter errorLogWriter = new PrintWriter(System.err);

    private String delimiter = DEFAULT_DELIMITER;
    private boolean fullLineDelimiter = false;

    /**
     * Default constructor
     */
    public ScriptRunner(Connection connection, boolean autoCommit,
                        boolean stopOnError) {
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
    }

    public void setDelimiter(String delimiter, boolean fullLineDelimiter) {
        this.delimiter = delimiter;
        this.fullLineDelimiter = fullLineDelimiter;
    }

    /**
     * Setter for logWriter property
     *
     * @param logWriter - the new value of the logWriter property
     */
    public void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    /**
     * Setter for errorLogWriter property
     *
     * @param errorLogWriter - the new value of the errorLogWriter property
     */
    public void setErrorLogWriter(PrintWriter errorLogWriter) {
        this.errorLogWriter = errorLogWriter;
    }

    /**
     * Runs an SQL script (read in using the Reader parameter)
     *
     * @param reader - the source of the script
     */
    public void runScript(Reader reader) throws IOException, SQLException {
        try {
            boolean originalAutoCommit = connection.getAutoCommit();
            try {
                if (originalAutoCommit != this.autoCommit) {
                    connection.setAutoCommit(this.autoCommit);
                }
                runScript(connection, reader);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (IOException | SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error running script.  Cause: " + e, e);
        }
    }

    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     *
     * @param conn   - the connection to use for the script
     * @param reader - the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException  if there is an error reading from the Reader
     */
    private void runScript(Connection conn, Reader reader) throws IOException,
            SQLException {
        StringBuffer command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                final Matcher delimMatch = delimP.matcher(trimmedLine);
                if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (delimMatch.matches()) {
                    setDelimiter(delimMatch.group(2), false);
                } else if (trimmedLine.startsWith("--")) {
                    println(trimmedLine);
                } else if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (!fullLineDelimiter
                        && trimmedLine.endsWith(getDelimiter())
                        || fullLineDelimiter
                        && trimmedLine.equals(getDelimiter())) {
                    command.append(line.substring(0, line
                            .lastIndexOf(getDelimiter())));
                    command.append(" ");
                    this.execCommand(conn, command, lineReader);
                    command = null;
                } else {
                    command.append(line);
                    command.append("\n");
                }
            }
            if (command != null && command.length() > 0) {
                this.execCommand(conn, command, lineReader);
            }
            if (!autoCommit) {
                conn.commit();
            }
        } catch (Exception e) {
            throw new IOException(String.format("Error executing '%s': %s", command, e.getMessage()), e);
        } finally {
            conn.rollback();
            flush();
        }
    }

    private void execCommand(Connection conn, StringBuffer command,
                             LineNumberReader lineReader) throws SQLException {
        Statement statement = conn.createStatement();

        println(command);

        boolean hasResults = false;
        try {
            hasResults = statement.execute(command.toString());
        } catch (SQLException e) {
            final String errText = String.format("Error executing '%s' (line %d): %s", command, lineReader.getLineNumber(), e.getMessage());
            if (stopOnError) {
                throw new SQLException(errText, e);
            } else {
                println(errText);
            }
        }

        if (autoCommit && !conn.getAutoCommit()) {
            conn.commit();
        }

        ResultSet rs = statement.getResultSet();
        if (hasResults && rs != null) {
            //query is a select query
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                String name = md.getColumnLabel(i);
                print(name + "\t");
            }
            println("");
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    String value = rs.getString(i);
                    print(value + "\t");
                }
                println("");
            }
        } else if (!hasResults) {
            //query can be update or any query apart from select query
            int count = statement.getUpdateCount();
        }

        try {
            statement.close();
        } catch (Exception e) {
            // Ignore to workaround a bug in Jakarta DBCP
        }
    }

    private String getDelimiter() {
        return delimiter;
    }
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private void print(Object o) {
        if (logWriter != null) {
            System.out.print(o);
        }
    }

    private void println(Object o) {
        if (logWriter != null) {
            logWriter.println(o);
        }
    }

    private void printlnError(Object o) {
        if (errorLogWriter != null) {
            errorLogWriter.println(o);
        }
    }

    private void flush() {
        if (logWriter != null) {
            logWriter.flush();
        }
        if (errorLogWriter != null) {
            errorLogWriter.flush();
        }
    }
}
