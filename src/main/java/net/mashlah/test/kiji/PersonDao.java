package net.mashlah.test.kiji;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTablePool;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PersonDao {


    private final KijiTablePool pool;

    public PersonDao(int zkClientPort, String[] zkQuorum, String instanceName) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Kiji kiji = Kiji.Factory.open(
                KijiURI.newBuilder().withZookeeperQuorum(zkQuorum).withZookeeperClientPort(zkClientPort).withInstanceName(instanceName).build(),
                config
        );
        pool = KijiTablePool.newBuilder(kiji).build();
    }


    public void storePerson(Person person, byte[] image) throws IOException {
        KijiTable table = null;
        KijiTable imageTable = null;
        AtomicKijiPutter putter = null;
        AtomicKijiPutter imagePutter = null;
        try {
            table = pool.get("persons");
            putter = table.getWriterFactory().openAtomicPutter();
            EntityId entityId = table.getEntityId(person.getId());
            putter.begin(entityId);

            putter.put("meta_data", "id", person.getId());
            putter.put("meta_data", "name", person.getName());
            putter.commit();

            imageTable = pool.get("images");
            imagePutter = imageTable.getWriterFactory().openAtomicPutter();
            entityId = imageTable.getEntityId(person.getId());
            imagePutter.begin(entityId);
            imagePutter.put("images", "data", ByteBuffer.wrap(image));
            imagePutter.commit();
        } finally {
            if (table != null) {
                table.release();
            }
            if (putter != null) {
                putter.close();
            }
            if (imageTable != null) {
                imageTable.release();
            }
            if (imagePutter != null) {
                imagePutter.close();
            }
        }
    }

    public void deletePersons() throws IOException {
        KijiTable table = pool.get("persons");
        KijiTableWriter writer = null;
        KijiTableReader reader = null;
        try {
            writer = table.getWriterFactory().openTableWriter();
            reader = table.getReaderFactory().openTableReader();
            KijiDataRequest dataRequest = KijiDataRequest.create("meta_data");
            KijiRowScanner scanner = reader.getScanner(dataRequest);
            for (KijiRowData kijiRowData : scanner) {
                writer.deleteRow(kijiRowData.getEntityId());
            }
        } finally {
            table.release();
            if (writer != null) {
                writer.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
    }
}
