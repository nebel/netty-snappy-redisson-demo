package demo.r2106;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.codec.MsgPackJacksonCodec;
import org.redisson.codec.SnappyCodec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@SuppressWarnings("Duplicates")
public class Main {
  private static final int SIZE = 1_000;

  public static void main(final String[] args) throws IOException {
    final Codec codec = new SnappyCodec(new MsgPackJacksonCodec());
    final Encoder encoder = codec.getValueEncoder();
    final Decoder<Object> decoder = codec.getValueDecoder();

    final List<String> list = createList(SIZE);

    // For Redisson 2.10.6-SNAPSHOT
    final ByteBuf encoded = encoder.encode(list);
    saveBuf(encoded);

    final byte[] diskBytes = Files.readAllBytes(FileSystems.getDefault().getPath("C:\\tmp\\encoded-2105.data"));
    final ByteBuf diskBuf = Unpooled.wrappedBuffer(diskBytes);

    final List<String> result = (List<String>) decoder.decode(diskBuf, null);

    System.out.println("Result has " + result.size() + " elements.");
    System.out.println("Element 0: " + result.get(0));
    System.out.println("Element " + result.size() + ": " + result.get(result.size() - 1));

    if (!list.equals(result)) {
      throw new IOException("Decoding failed! Input and output are not equal.");
    }
  }

  private static void saveBuf(final ByteBuf buf) throws IOException {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);

    Files.write(FileSystems.getDefault().getPath("C:\\tmp\\encoded-2106.data"), bytes);
  }

  private static List<String> createList(final int size) {
//    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final Random random = new Random(0);

    System.out.println("Creating list with " + size + " elements...");

    final List<String> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add("R[" + Integer.toString(random.nextInt()) + "]");
    }

    System.out.println("Created.");

    return list;
  }
}
