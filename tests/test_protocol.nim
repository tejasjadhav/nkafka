import unittest

import nkafka/protocol/types/base
import nkafka/protocol/types/numbers
import nkafka/protocol/types/sequences
import nkafka/protocol/recordset

suite "protocol/types/base":
  test "should return empty byte sequence for base object":
    check Base().encode == newSeq[byte](0)

  test "should reverse a specified byte sequence":
    check @[byte(1), byte(2), byte(3), byte(4)].reverse == @[byte(4), byte(3), byte(2), byte(1)]

suite "protocol/types/numbers":
  test "should convert int8 to a byte sequence":
    check Int[Int8](value: 0).encode == @[byte(0)]
    check Int[Int8](value: high(int8)).encode == @[byte(127)]

  test "should convert negative int8 to a byte sequence":
    check Int[Int8](value: low(int8)).encode == @[byte(128)]
    check Int[Int8](value: -1).encode == @[byte(255)]

  test "should convert int16 to a byte sequence":
    check Int[Int16](value: 0).encode == @[byte(0), byte(0)]
    check Int[Int16](value: high(int16)).encode == @[byte(127), byte(255)]

  test "should convert negative int16 to a byte sequence":
    check Int[Int16](value: low(int16)).encode == @[byte(128), byte(0)]
    check Int[Int16](value: -1).encode == @[byte(255), byte(255)]

  test "should convert int32 to a byte sequence":
    check Int[Int32](value: 0).encode == @[byte(0), byte(0), byte(0), byte(0)]
    check Int[Int32](value: high(int32)).encode == @[byte(127), byte(255), byte(255), byte(255)]

  test "should convert negative int32 to a byte sequence":
    check Int[Int32](value: low(int32)).encode == @[byte(128), byte(0), byte(0), byte(0)]
    check Int[Int32](value: -1).encode == @[byte(255), byte(255), byte(255), byte(255)]

  test "should convert uint32 to a byte sequence":
    check Int[Uint32](value: 0).encode == @[byte(0), byte(0), byte(0), byte(0)]
    check Int[Uint32](value: high(uint32)).encode == @[byte(255), byte(255), byte(255), byte(255)]

  test "should convert int64 to a byte sequence":
    check Int[Int64](value: 0).encode == @[byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0)]
    check Int[Int64](value: high(int64)).encode == @[byte(127), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255)]

  test "should convert negative int64 to a byte sequence":
    check Int[Int64](value: low(int64)).encode == @[byte(128), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0)]
    check Int[Int64](value: -1).encode == @[byte(255), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255), byte(255)]

  test "should convert varint to byte sequence for int8 number":
    check Int[VarInt](value: 0).encode == @[byte(0)]
    check Int[VarInt](value: high(int8)).encode == @[byte(254), byte(1)]
    check Int[VarInt](value: -1).encode == @[byte(1)]
    check Int[VarInt](value: low(int8)).encode == @[byte(255), byte(1)]

  test "should convert varint to byte sequence for int16 number":
    check Int[VarInt](value: high(int16)).encode == @[byte(254), byte(255), byte(3)]
    check Int[VarInt](value: low(int16)).encode == @[byte(255), byte(255), byte(3)]

  test "should convert varint to byte sequence for int32 number":
    check Int[VarInt](value: high(int32)).encode == @[byte(254), byte(255), byte(255), byte(255), byte(15)]
    check Int[VarInt](value: low(int32)).encode == @[byte(255), byte(255), byte(255), byte(255), byte(15)]

suite "protocol/type/sequences":
  test "should convert bytes to a byte sequence":
    check Sequence[Bytes](value: "hello").encode == @[byte(0), byte(0), byte(0), byte(5), byte(104), byte(101), byte(108), byte(108), byte(111)]

  test "should convert zero length byte to a byte sequence":
    check Sequence[Bytes](value: "").encode == @[byte(0), byte(0), byte(0), byte(0)]

  test "should convert string to a byte sequence":
    check Sequence[String](value: "hello").encode == @[byte(0), byte(5), byte(104), byte(101), byte(108), byte(108), byte(111)]

  test "should convert zero length string to a byte sequence":
    check Sequence[String](value: "").encode == @[byte(0), byte(0)]

  test "should convert varint string to a byte sequence":
    check Sequence[VarIntString](value: "hello").encode == @[byte(10), byte(104), byte(101), byte(108), byte(108), byte(111)]

  test "should convert zero length varint string to a byte sequence":
    check Sequence[VarIntString](value: "").encode == @[byte(0)]

  test "should convert array of numbers to a byte sequence":
    check Array[Int[Int8]](values: @[Int[Int8](value: 1), Int[Int8](value: 2)]).encode == @[byte(0), byte(0), byte(0), byte(2), byte(1), byte(2)]

  test "should convert zero length array of numbers to a byte sequence":
    check Array[Int[Int8]](values: @[]).encode == @[byte(0), byte(0), byte(0), byte(0)]

suite "protocol/recordset":
  test "should convert record to a byte sequence":
    let record = Record(
      attributes: Int[Int8](value: 1),
      timestampDelta: Int[VarInt](value: 1000),
      offsetDelta: Int[VarInt](value: 1500),
      key: Sequence[VarIntString](value: "some-key"),
      value: Sequence[VarIntString](value: "some-value"),
    )

    check record.encode == @[
      byte(52), # Record length
      byte(1), # Attributes
      byte(208), byte(15), # Timestamp delta
      byte(184), byte(23), # Offset delta
      byte(16), # Key length
      byte(115), byte(111), byte(109), byte(101), byte(45), byte(107), byte(101), byte(121), # Key
      byte(20), # Value length
      byte(115), byte(111), byte(109), byte(101), byte(45), byte(118), byte(97), byte(108), byte(117), byte(101), # Value
      byte(0), # Record headers
    ]
