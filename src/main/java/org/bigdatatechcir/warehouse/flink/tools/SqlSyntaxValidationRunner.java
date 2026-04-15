package org.bigdatatechcir.warehouse.flink.tools;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SqlSyntaxValidationRunner {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Usage: SqlSyntaxValidationRunner <sql-file> [<sql-file>...]");
            System.exit(1);
        }

        SqlParser.Config parserConfig = SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(Quoting.BACK_TICK)
                .withQuotedCasing(Casing.UNCHANGED)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withConformance(FlinkSqlConformance.DEFAULT);

        for (String arg : args) {
            Path path = Paths.get(arg);
            List<String> statements = splitStatements(path);
            int index = 1;
            for (String statement : statements) {
                try {
                    SqlParser.create(statement, parserConfig).parseStmt();
                } catch (Exception e) {
                    System.err.println("Parse failed: " + path);
                    System.err.println("Statement #" + index + ":");
                    System.err.println(statement);
                    e.printStackTrace(System.err);
                    System.exit(2);
                }
                index++;
            }
            System.out.println("OK  " + path + "  (" + statements.size() + " statements)");
        }
    }

    private static List<String> splitStatements(Path path) throws IOException {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            current.append(line).append(System.lineSeparator());
            if (line.trim().endsWith(";")) {
                String statement = current.toString().trim();
                if (!statement.isEmpty()) {
                    statements.add(stripTrailingSemicolon(statement));
                }
                current.setLength(0);
            }
        }

        String tail = current.toString().trim();
        if (!tail.isEmpty()) {
            statements.add(stripTrailingSemicolon(tail));
        }

        return statements;
    }

    private static String stripTrailingSemicolon(String statement) {
        if (statement.endsWith(";")) {
            return statement.substring(0, statement.length() - 1);
        }
        return statement;
    }
}
