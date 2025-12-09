package examples;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * An example of writing data with Tablet to TsFile
 */
public class TsFileWriteWithTablet {

    private static final Logger LOGGER = LoggerFactory.getLogger(TsFileWriteWithTablet.class);

    public static void main(String[] args) {
        try {
            String path = "data/tree.tsfile";
            File f = FSFactoryProducer.getFSFactory().getFile(path);
            if (f.exists()) {
                Files.delete(f.toPath());
            }

            String database = "root.db1.db2.db3.db4";
            String deviceId = database + ".d1";

            try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
                List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
                measurementSchemas.add(new MeasurementSchema("m1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m2", TSDataType.INT32, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m3", TSDataType.INT64, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m4", TSDataType.FLOAT, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m5", TSDataType.DOUBLE, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m6", TSDataType.TEXT, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m7", TSDataType.STRING, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m8", TSDataType.BLOB, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m9", TSDataType.DATE, TSEncoding.PLAIN));
                measurementSchemas.add(new MeasurementSchema("m10", TSDataType.TIMESTAMP, TSEncoding.PLAIN));

                // register nonAligned timeseries
                for (IMeasurementSchema measurementSchema : measurementSchemas) {
                    tsFileWriter.registerTimeseries(deviceId, measurementSchema);
                }

                // example 1
                writeWithTablet(tsFileWriter, deviceId, measurementSchemas, 100, 0);
                writeWithTablet(tsFileWriter, deviceId, measurementSchemas, 10, 10);
            }
        } catch (Exception e) {
            LOGGER.error("meet error in TsFileWrite with tablet", e);
        }
    }

    private static void writeWithTablet(
            TsFileWriter tsFileWriter,
            String deviceId,
            List<IMeasurementSchema> schemas,
            long rowNum,
            long startTime)
            throws IOException, WriteProcessException {
        Tablet tablet = new Tablet(deviceId, schemas);

        for (int r = 0; r < rowNum; r++) {
            int row = tablet.getRowSize();
            tablet.addTimestamp(row, startTime++);
            if (r % 2 == 0) {
                tablet.addValue("m1", r, r % 3 != 0);
                tablet.addValue("m2", r, r % 3 != 0 ? r * 100 : r * -100);
                tablet.addValue("m3", r, r % 3 != 0 ? r * 100L : r * -100L);
                tablet.addValue("m4", r, r % 3 != 0 ? r * 12345.12345F : r * -12345.12345F);
                tablet.addValue("m5", r, r % 3 != 0 ? r * 12345.12345 : r * -12345.12345);
                tablet.addValue("m6", r, String.valueOf(r));
                tablet.addValue("m7", r, String.valueOf(r));
                tablet.addValue("m8", r, new Binary(String.valueOf(r).getBytes(StandardCharsets.UTF_8)));
                tablet.addValue("m9", r, LocalDate.ofEpochDay(r));
                tablet.addValue("m10", r, r % 3 != 0 ? r * 10000L : r * -10000L);
            }
            // write
            if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
                tsFileWriter.writeTree(tablet);
                tablet.reset();
            }
        }
        // write
        if (tablet.getRowSize() != 0) {
            tsFileWriter.writeTree(tablet);
            tablet.reset();
        }
    }
}