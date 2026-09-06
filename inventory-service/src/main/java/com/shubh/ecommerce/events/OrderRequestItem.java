package com.shubh.ecommerce.events;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class OrderRequestItem extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -2837551061198244201L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderRequestItem\",\"namespace\":\"com.shubh.ecommerce.events\",\"doc\":\"Represents an individual product included in the confirmed order.\",\"fields\":[{\"name\":\"productId\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<OrderRequestItem> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<OrderRequestItem> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  public static BinaryMessageEncoder<OrderRequestItem> getEncoder() {
    return ENCODER;
  }

  public static BinaryMessageDecoder<OrderRequestItem> getDecoder() {
    return DECODER;
  }

  public static BinaryMessageDecoder<OrderRequestItem> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  public static OrderRequestItem fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private long productId;
  private java.lang.CharSequence name;
  private int quantity;

  public OrderRequestItem() {}

  public OrderRequestItem(java.lang.Long productId, java.lang.CharSequence name, java.lang.Integer quantity) {
    this.productId = productId;
    this.name = name;
    this.quantity = quantity;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return productId;
    case 1: return name;
    case 2: return quantity;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: productId = (java.lang.Long)value$; break;
    case 1: name = (java.lang.CharSequence)value$; break;
    case 2: quantity = (java.lang.Integer)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  public long getProductId() {
    return productId;
  }

  public void setProductId(long value) {
    this.productId = value;
  }

  public java.lang.CharSequence getName() {
    return name;
  }

  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  public int getQuantity() {
    return quantity;
  }

  public void setQuantity(int value) {
    this.quantity = value;
  }

  public static com.shubh.ecommerce.events.OrderRequestItem.Builder newBuilder() {
    return new com.shubh.ecommerce.events.OrderRequestItem.Builder();
  }

  public static com.shubh.ecommerce.events.OrderRequestItem.Builder newBuilder(com.shubh.ecommerce.events.OrderRequestItem.Builder other) {
    if (other == null) {
      return new com.shubh.ecommerce.events.OrderRequestItem.Builder();
    } else {
      return new com.shubh.ecommerce.events.OrderRequestItem.Builder(other);
    }
  }

  public static com.shubh.ecommerce.events.OrderRequestItem.Builder newBuilder(com.shubh.ecommerce.events.OrderRequestItem other) {
    if (other == null) {
      return new com.shubh.ecommerce.events.OrderRequestItem.Builder();
    } else {
      return new com.shubh.ecommerce.events.OrderRequestItem.Builder(other);
    }
  }

  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<OrderRequestItem>
    implements org.apache.avro.data.RecordBuilder<OrderRequestItem> {

    private long productId;
    private java.lang.CharSequence name;
    private int quantity;

    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    private Builder(com.shubh.ecommerce.events.OrderRequestItem.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.productId)) {
        this.productId = data().deepCopy(fields()[0].schema(), other.productId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.quantity)) {
        this.quantity = data().deepCopy(fields()[2].schema(), other.quantity);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    private Builder(com.shubh.ecommerce.events.OrderRequestItem other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.productId)) {
        this.productId = data().deepCopy(fields()[0].schema(), other.productId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.quantity)) {
        this.quantity = data().deepCopy(fields()[2].schema(), other.quantity);
        fieldSetFlags()[2] = true;
      }
    }

    public long getProductId() {
      return productId;
    }

    public com.shubh.ecommerce.events.OrderRequestItem.Builder setProductId(long value) {
      validate(fields()[0], value);
      this.productId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    public boolean hasProductId() {
      return fieldSetFlags()[0];
    }

    public com.shubh.ecommerce.events.OrderRequestItem.Builder clearProductId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    public java.lang.CharSequence getName() {
      return name;
    }

    public com.shubh.ecommerce.events.OrderRequestItem.Builder setName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    public boolean hasName() {
      return fieldSetFlags()[1];
    }

    public com.shubh.ecommerce.events.OrderRequestItem.Builder clearName() {
      name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    public int getQuantity() {
      return quantity;
    }

    public com.shubh.ecommerce.events.OrderRequestItem.Builder setQuantity(int value) {
      validate(fields()[2], value);
      this.quantity = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    public boolean hasQuantity() {
      return fieldSetFlags()[2];
    }

    public com.shubh.ecommerce.events.OrderRequestItem.Builder clearQuantity() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OrderRequestItem build() {
      try {
        OrderRequestItem record = new OrderRequestItem();
        record.productId = fieldSetFlags()[0] ? this.productId : (java.lang.Long) defaultValue(fields()[0]);
        record.name = fieldSetFlags()[1] ? this.name : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.quantity = fieldSetFlags()[2] ? this.quantity : (java.lang.Integer) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<OrderRequestItem>
    WRITER$ = (org.apache.avro.io.DatumWriter<OrderRequestItem>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<OrderRequestItem>
    READER$ = (org.apache.avro.io.DatumReader<OrderRequestItem>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeLong(this.productId);

    out.writeString(this.name);

    out.writeInt(this.quantity);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.productId = in.readLong();

      this.name = in.readString(this.name instanceof Utf8 ? (Utf8)this.name : null);

      this.quantity = in.readInt();

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.productId = in.readLong();
          break;

        case 1:
          this.name = in.readString(this.name instanceof Utf8 ? (Utf8)this.name : null);
          break;

        case 2:
          this.quantity = in.readInt();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + Long.hashCode(productId);
    result = 31 * result + (name == null ? 0 : name.hashCode());
    result = 31 * result + Integer.hashCode(quantity);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OrderRequestItem)) {
      return false;
    }
    OrderRequestItem other = (OrderRequestItem) o;
    if (this.productId != other.productId) {
      return false;
    }
    if (Utf8.compareSequences(this.name, other.name) != 0) {
      return false;
    }
    if (this.quantity != other.quantity) {
      return false;
    }
    return true;
  }
}










