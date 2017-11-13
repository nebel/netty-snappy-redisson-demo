# netty-snappy-redisson-demo

Demonstrates an encoding issue with the Snappy codec from Netty and its impact on Redisson.

## Snappy codec from Netty

The Snappy codec shipped with Netty as of 4.1.16.Final is likely to throw an ArrayIndexOutOfBoundsException when given payloads over a certain length (e.g. 100,000).

## Impact on Redisson

Redisson versions 2.10.1 and 3.5.1 are the last versions to use the Snappy implementation from Xerial (https://github.com/xerial/snappy-java). From 2.10.2 and 3.5.2 onwards, the Snappy implementation from recent versions of Netty was used instead.

Due to this and the aforementioned issue, moderately-sized payloads which used to encode without problem may now fail completely when the Snappy codec is being used.

