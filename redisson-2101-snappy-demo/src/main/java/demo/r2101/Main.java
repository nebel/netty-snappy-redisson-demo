package demo.r2101;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.codec.MsgPackJacksonCodec;
import org.redisson.codec.SnappyCodec;

import io.netty.buffer.Unpooled;

@SuppressWarnings("Duplicates")
public class Main {
  private static final int SIZE = 500_000;

  public static void main(final String[] args) throws IOException {
    final Codec codec = new SnappyCodec(new MsgPackJacksonCodec());
    final Encoder encoder = codec.getValueEncoder();
    final Decoder<Object> decoder = codec.getValueDecoder();

    final List<String> list = createList(SIZE);

    // For Redisson 2.10.1
    final List<String> result = (List<String>) decoder.decode(Unpooled.wrappedBuffer(encoder.encode(list)), null);

    System.out.println("Result has " + result.size() + " elements.");
    System.out.println("Element 0: " + result.get(0));
    System.out.println("Element " + result.size() + ": " + result.get(result.size() - 1));

    if (!list.equals(result)) {
      throw new IOException("Decoding failed! Input and output are not equal.");
    }
  }

  private static List<String> createList(final int size) {
    final ThreadLocalRandom random = ThreadLocalRandom.current();

    System.out.println("Creating list with " + size + " elements...");

    final List<String> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add("R[" + Integer.toString(random.nextInt(0, Integer.MAX_VALUE)) + "]");
    }

    System.out.println("Created.");

    return list;
  }
}
