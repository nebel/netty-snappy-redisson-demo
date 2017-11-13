package demo.snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

public class Main {
  private final static List<Integer> SIZES = Arrays.asList(1, 10, 100, 1_000, 10_000, 30_000, 44_895, 44_896, 50_000, 100_000, 1_000_000, 10_000_000);

  private final static int RANDOM_SEED = 0;

  private final static int NETTY_PADDING = 8192;

  public static void main(final String[] args) throws IOException {
    SIZES.forEach(size -> testEncoder("Netty", Main::encodeNetty, size));
    SIZES.forEach(size -> testEncoder("Xerial", Main::encodeXerial, size));
  }

  private static void testEncoder(final String name, final Encoder encoder, final int size) {
    try {
      System.out.println("===== " + name + " (size: " + size + ") =====");

      final byte[] bytes = createBytes(size);
      final byte[] result = encoder.encode(bytes);

      System.out.println("OK! Compressed size: " + result.length);
    } catch (Exception e) {
      System.out.println("ERROR! " + getStackTrace(e));
    }
  }

  private static byte[] encodeNetty(final byte[] bytes) {
    final io.netty.handler.codec.compression.Snappy snappy = new io.netty.handler.codec.compression.Snappy();

    final ByteBuf src = Unpooled.wrappedBuffer(bytes);
    final ByteBuf dest = ByteBufAllocator.DEFAULT.buffer(src.readableBytes() + NETTY_PADDING);
    snappy.encode(src, dest, src.readableBytes());

    final byte[] out = new byte[dest.readableBytes()];
    dest.getBytes(dest.readerIndex(), out);

    return out;
  }

  private static byte[] encodeXerial(final byte[] bytes) throws IOException {
    return org.xerial.snappy.Snappy.compress(bytes);
  }

  @FunctionalInterface
  private interface Encoder {
    byte[] encode(byte[] bytes) throws Exception;
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
