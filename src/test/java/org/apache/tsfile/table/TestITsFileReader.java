package org.apache.tsfile.table;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.StringJoiner;

public class TestITsFileReader {

    private static final String path = "data/table.tsfile";
    private static final String tableName = "table1";
    private static final File f = FSFactoryProducer.getFSFactory().getFile(path);

    @BeforeTest
    public void GenerateTsFile() throws IOException {
        if (f.exists()) {
            Files.delete(f.toPath());
        }
        TableSchema tableSchema =
                new TableSchema(
                        tableName,
                        Arrays.asList(
                                new ColumnSchemaBuilder()
                                        .name("tag1")
                                        .dataType(TSDataType.STRING)
                                        .category(ColumnCategory.TAG)
                                        .build(),
                                new ColumnSchemaBuilder()
                                        .name("tag2")
                                        .dataType(TSDataType.STRING)
                                        .category(ColumnCategory.TAG)
                                        .build(),
                                new ColumnSchemaBuilder()
                                        .name("s1")
                                        .dataType(TSDataType.INT32)
                                        .category(ColumnCategory.FIELD)
                                        .build(),
                                new ColumnSchemaBuilder().name("s2").dataType(TSDataType.BOOLEAN).build()));

        Tablet tablet =
                new Tablet(
                        Arrays.asList("tag1", "tag2", "s1", "s2"),
                        Arrays.asList(
                                TSDataType.STRING, TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN));
        for (int row = 0; row < 5; row++) {
            tablet.addTimestamp(row, row);
            tablet.addValue(row, "tag1", "tag1_value_1");
            tablet.addValue(row, "tag2", "tag2_value_1");
            tablet.addValue(row, "s1", row);
            // tablet.addValue(row, "s2", true);
        }
        for (int row = 5; row < 10; row++) {
            tablet.addTimestamp(row, row);
            tablet.addValue(row, 0, "tag1_value_2");
            tablet.addValue(row, 1, "tag1_value_2");
            // tablet.addValue(row, 2, row);
            tablet.addValue(row, 3, false);
        }

        long memoryThreshold = 10 * 1024 * 1024;
        try (ITsFileWriter writer =
                     new TsFileWriterBuilder()
                             .file(f)
                             .tableSchema(tableSchema)
                             .memoryThreshold(memoryThreshold)
                             .build()) {
            writer.write(tablet);
        } catch (WriteProcessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试查询接口：query(String tableName, List<String> columnNames, long startTime, long endTime)
     */
    @Test
    public void testQuery1() {
        try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
             ResultSet resultSet = reader.query(tableName, Arrays.asList("tag1", "tag2", "s1", "s2"), 2, 8)) {
            ResultSetMetadata metadata = resultSet.getMetadata();
            System.out.println(metadata);
            StringJoiner sj = new StringJoiner(" ");
            for (int column = 1; column <= 5; column++) {
                sj.add(metadata.getColumnName(column) + "(" + metadata.getColumnType(column) + ") ");
            }
            System.out.println(sj);
            while (resultSet.next()) {
                Long timeField = resultSet.getLong("Time");
                String tag1 = resultSet.isNull("tag1") ? null : resultSet.getString("tag1");
                String tag2 = resultSet.isNull("tag2") ? null : resultSet.getString("tag2");
                Integer s1Field = resultSet.isNull("s1") ? null : resultSet.getInt(4);
                Boolean s2Field = resultSet.isNull("s2") ? null : resultSet.getBoolean(5);
                sj = new StringJoiner(" ");
                System.out.println(
                        sj.add(timeField + "")
                                .add(tag1)
                                .add(tag2)
                                .add(s1Field + "")
                                .add(s2Field + ""));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试查询接口：query(String tableName, List<String> columnNames, long startTime, long endTime, Filter tagFilter)
     */
    @Test
    public void testQuery2() {

    }


}
