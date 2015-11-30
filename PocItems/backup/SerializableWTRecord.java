package com.ibm.poc;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * For now, Spark does not support Avro. This class is just a quick
 * workaround that (de)serializes AminoAcid objects using Avro.
 */
public class SerializableWTRecord extends WTRecord implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 6184913263356997382L;

	private void setValues(WTRecord record) {
    	setRecordtime(record.getRecordtime());
    	setWtid(record.getWtid());
        this.setSwitchitems(record.getSwitchitems());
        this.setSimitems(record.getSimitems());
    }

    public SerializableWTRecord(WTRecord record) {
        setValues(record);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        DatumWriter<WTRecord> writer = new SpecificDatumWriter<WTRecord>(WTRecord.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        DatumReader<WTRecord> reader =
                new SpecificDatumReader<WTRecord>(WTRecord.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        setValues(reader.read(null, decoder));
    }

    private void readObjectNoData()
            throws ObjectStreamException {
    }

}