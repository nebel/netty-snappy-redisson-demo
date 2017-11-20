package demo.snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class Main {
  private final static List<Integer> SIZES = Arrays.asList(1, 10, 100, 1_000, 10_000, 30_000, 44_895, 44_896, 50_000, 100_000, 1_000_000, 10_000_000, 100_000_000);

  private final static int RANDOM_SEED = 0;

  private final static int NETTY_PADDING = 128;

  public static void main(final String[] args) throws IOException {
    SIZES.forEach(size -> testEncoder("Netty", Main::encodeNetty, Main::decodeNetty, size));
    printSep();
    SIZES.forEach(size -> testEncoder("Netty Framed", Main::encodeNettyFramed, Main::decodeNettyFramed, size));
    printSep();
    SIZES.forEach(size -> testEncoder("Xerial", Main::encodeXerial, Main::decodeXerial, size));
    printSep();
    SIZES.forEach(size -> testEncoder("Xerial Framed", Main::encodeXerialFramed, Main::decodeXerialFramed, size));
  }

  private static void printSep() {
    System.out.println("\n##########\n");
  }

  private static void testEncoder(final String name, final Encoder encoder, final Decoder decoder, final int size) {
    try {
      System.out.println("===== " + name + " (size: " + size + ") =====");

      final byte[] initial = createBytes(size);

      final byte[] encoded = encoder.encode(initial);

      final BigDecimal overhead = new BigDecimal(encoded.length).divide(new BigDecimal(size), 20, RoundingMode.HALF_UP).subtract(BigDecimal.ONE).multiply(BigDecimal.TEN);
      final String overheadString = "[" + overhead.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros().toPlainString() + "% overhead]";

      System.out.println("Encode OK! Compressed size: " + encoded.length + " " + overheadString);

      final byte[] decoded = decoder.decode(encoded);
      System.out.println("Decode OK! Uncompressed size: " + decoded.length);

      if (!Arrays.equals(initial, decoded)) {
        throw new Exception("Bytes are not equal!");
      }
    } catch (Exception e) {
      System.out.println("ERROR! " + getStackTrace(e));
    }
  }

  private static byte[] encodeNetty(final byte[] bytes) throws IOException {
    final io.netty.handler.codec.compression.Snappy snappy = new io.netty.handler.codec.compression.Snappy();

    final ByteBuf src = Unpooled.wrappedBuffer(bytes);
    final ByteBuf dest = ByteBufAllocator.DEFAULT.buffer(src.readableBytes() + NETTY_PADDING);
    snappy.encode(src, dest, src.readableBytes());
    src.release();

    final byte[] out = new byte[dest.readableBytes()];
    dest.getBytes(dest.readerIndex(), out);
    dest.release();

    return out;
  }

  private static byte[] decodeNetty(final byte[] bytes) throws IOException {
    final io.netty.handler.codec.compression.Snappy snappy = new io.netty.handler.codec.compression.Snappy();

    final ByteBuf src = Unpooled.wrappedBuffer(bytes);
    final ByteBuf dest = ByteBufAllocator.DEFAULT.buffer(src.readableBytes() + NETTY_PADDING);
    snappy.decode(src, dest);
    src.release();

    final byte[] out = new byte[dest.readableBytes()];
    dest.getBytes(dest.readerIndex(), out);
    dest.release();

    return out;
  }

  private static class CustomEncoder extends io.netty.handler.codec.compression.SnappyFrameEncoder {
    public void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) throws Exception {
      super.encode(ctx, in, out);
    }
  }

  private static class CustomDecoder extends io.netty.handler.codec.compression.SnappyFrameDecoder {
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      super.decode(ctx, in, out);
    }
  }

  private static byte[] encodeNettyFramed(final byte[] bytes) throws Exception {
    final CustomEncoder encoder = new CustomEncoder();

    final ChannelHandlerContext ctx = new FakeChannelHandlerContext();
    final ByteBuf src = Unpooled.wrappedBuffer(bytes);
    final ByteBuf dest = ByteBufAllocator.DEFAULT.buffer(src.readableBytes() + NETTY_PADDING);
    encoder.encode(ctx, src, dest);
    src.release();

    final byte[] out = new byte[dest.readableBytes()];
    dest.getBytes(dest.readerIndex(), out);
    dest.release();

    return out;
  }

  private static byte[] decodeNettyFramed(final byte[] bytes) throws Exception {
    final CustomDecoder decoder = new CustomDecoder();

    final ChannelHandlerContext ctx = new FakeChannelHandlerContext();
    final ByteBuf src = Unpooled.wrappedBuffer(bytes);
    final List<Object> dest = new ArrayList<>();

    while (src.readableBytes() > 0) {
      decoder.decode(ctx, src, dest);
    }
    src.release();

    final ByteArrayOutputStream merged = new ByteArrayOutputStream();
    for (Object obj : dest) {
      final ByteBuf buf = (ByteBuf) obj;
      buf.readBytes(merged, buf.readableBytes());
      buf.release();
    }

    return merged.toByteArray();
  }

  private static byte[] encodeXerial(final byte[] bytes) throws IOException {
    return org.xerial.snappy.Snappy.compress(bytes);
  }

  private static byte[] decodeXerial(final byte[] bytes) throws IOException {
    return org.xerial.snappy.Snappy.uncompress(bytes);
  }

  private static byte[] encodeXerialFramed(final byte[] bytes) throws IOException {
    final ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
    final SnappyFramedOutputStream fos = new SnappyFramedOutputStream(encodedStream);
    fos.write(bytes);
    fos.close();

    return encodedStream.toByteArray();
  }

  private static byte[] decodeXerialFramed(final byte[] bytes) throws IOException {
    final SnappyFramedInputStream fis = new SnappyFramedInputStream(new ByteArrayInputStream(bytes));
    final ByteArrayOutputStream decodedStream = new ByteArrayOutputStream();
    fis.transferTo(decodedStream);
    fis.close();

    return decodedStream.toByteArray();
  }

  @FunctionalInterface
  private interface Encoder {
    byte[] encode(byte[] bytes) throws Exception;
  }

  @FunctionalInterface
  private interface Decoder {
    byte[] decode(byte[] bytes) throws Exception;
  }

  private static byte[] createBytes(final int size) {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Random random = new Random(RANDOM_SEED);

    for (int i = 0; i < size; i++) {
      stream.write(random.nextInt());
    }

    return stream.toByteArray();
  }

  private static String getStackTrace(final Throwable t) {
    final StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw, true));
    return sw.getBuffer().toString();
  }
}
