package org.apache.tsfile.table;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
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
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import utils.ParserCSV;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestITsFileReader {

    private final String path = "data/tsfile/table.tsfile";
    private final String tableName = "table1";
    private final File f = FSFactoryProducer.getFSFactory().getFile(path);
    private List<String> columnNameList = new ArrayList<>();
    private List<TSDataType> dataTypeList = new ArrayList<>();
    private final List<ColumnSchema> columnSchemaList = new ArrayList<>();
    private int expectRowNum = 0;

    private Iterator<Object[]> getData() throws IOException {
        return new ParserCSV().load("data/csv/table.csv", ',');
    }

    @BeforeTest
    public void GenerateTsFile() throws IOException, WriteProcessException {
        if (f.exists()) {
            Files.delete(f.toPath());
        }
        columnNameList = Arrays.asList(
                "Tag1", "Tag2",
                "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10");
        dataTypeList = Arrays.asList(
                TSDataType.STRING, TSDataType.STRING,
                TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.DATE, TSDataType.TIMESTAMP);
        List<ColumnCategory> columnCategoryList = Arrays.asList(
                ColumnCategory.TAG, ColumnCategory.TAG,
                ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD,
                ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD);
        for (int i = 0; i < columnNameList.size(); i++) {
            columnSchemaList.add(new ColumnSchemaBuilder().name(columnNameList.get(i)).dataType(dataTypeList.get(i)).category(columnCategoryList.get(i)).build());
        }
        TableSchema tableSchema = new TableSchema(tableName, columnSchemaList);

        try (ITsFileWriter writer =
                     new TsFileWriterBuilder()
                             .file(f)
                             .tableSchema(tableSchema)
                             .build()) {
            Tablet tablet = new Tablet(columnNameList, dataTypeList);
            int rowNum = 0;
            Iterator<Object[]> data = getData();
            while (data.hasNext()) {
                Object[] row = data.next();
                tablet.addTimestamp(rowNum, Long.parseLong(row[0].toString()));
                for (int i = 0; i < columnNameList.size(); i++) {
                    switch (dataTypeList.get(i)) {
                        case TEXT:
                        case STRING:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), row[i + 1].toString());
                            }
                            break;
                        case INT32:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Integer.parseInt(row[i + 1].toString()));
                            }
                            break;
                        case BOOLEAN:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Boolean.parseBoolean(row[i + 1].toString()));
                            }
                            break;
                        case INT64:
                        case TIMESTAMP:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Long.parseLong(row[i + 1].toString()));
                            }
                            break;
                        case FLOAT:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Float.parseFloat(row[i + 1].toString()));
                            }
                            break;
                        case DOUBLE:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Double.parseDouble(row[i + 1].toString()));
                            }
                            break;
                        case BLOB:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), row[i + 1].toString().getBytes(Charset.defaultCharset()));
                            }
                            break;
                        case DATE:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), LocalDate.parse(row[i + 1].toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported data type: " + dataTypeList.get(i));
                    }
                }
                rowNum++;
                expectRowNum++;
            }
            writer.write(tablet);
        }
    }

    /**
     * 测试查询接口：query(String tableName, List<String> columnNames, long startTime, long endTime)
     */
    @Test
    public void testQuery1() throws IOException, ReadProcessException, NoTableException, NoMeasurementException {
        int actualRowNum = 0;
        try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
             ResultSet resultSet = reader.query(tableName, columnNameList, Long.MIN_VALUE, Long.MAX_VALUE)) {
            ResultSetMetadata metadata = resultSet.getMetadata();
            // 验证 Time 列的元数据
            assert metadata.getColumnName(1).equals("Time");
            assert metadata.getColumnType(1).equals(TSDataType.INT64);
            // 验证其他列的元数据
            for (int i = 0; i < columnNameList.size(); i++) {
                assert metadata.getColumnName(i + 2).equals(columnNameList.get(i));
            }
            for (int i = 0; i < dataTypeList.size(); i++) {
                assert metadata.getColumnType(i + 2).equals(dataTypeList.get(i));
            }
            // 验证数据
            while (resultSet.next()) {
//                System.out.println(resultSet.getLong("Time"));
//                System.out.println(resultSet.isNull("Tag1") ? null : resultSet.getString("Tag1"));
//                System.out.println(resultSet.isNull("Tag2") ? null : resultSet.getString("Tag2"));
//                System.out.println(resultSet.isNull("S1") ? null : resultSet.getInt(4));
//                System.out.println(resultSet.isNull("S2") ? null : resultSet.getBoolean(5));
                actualRowNum++;
            }
            assert actualRowNum == expectRowNum : "Actual row number: " + actualRowNum + ", expected row number: " + expectRowNum;
        }
    }

    /**
     * 测试查询接口：query(String tableName, List<String> columnNames, long startTime, long endTime, Filter tagFilter)
     */
    @Test
    public void testQuery2() {

    }


}
