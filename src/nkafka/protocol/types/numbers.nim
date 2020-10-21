import base

type
  Int8* = int8
  Int16* = int16
  Int32* = int32
  Int64* = int64
  Uint32* = uint32
  VarInt* = int
  FixedIntTypes = Int8 | Int16 | Int32 | Int64 | Uint32
  IntTypes = FixedIntTypes | VarInt

  ## Generic Kafka integer type. Supports int8, int16, int32, uint32, int64 and
  ## varint integer types.
  Int*[I: IntTypes] = ref object of Base
    value*: I

proc encode*[I: FixedIntTypes](self: Int[I]): seq[byte] =
  ## Encode fixed size integer (int8, int16, int32, uint32, int64) to byte sequence.
  var intByteSeq: seq[byte] =
    if I is Int8: @cast[array[1, byte]](I(self.value))
    elif I is Int16: @cast[array[2, byte]](I(self.value))
    elif I is Int32: @cast[array[4, byte]](I(self.value))
    elif I is Uint32: @cast[array[4, byte]](I(self.value))
    else: @cast[array[8, byte]](I(self.value))
  return intByteSeq.reverse

proc encode*[I: VarInt](self: Int[I]): seq[byte] =
  ## Encode varint to byte sequence.
  var encodedValues: seq[byte]
  var value = (self.value shl 1) xor (self.value shr 63)

  var byteToBeAdded = value and 127
  value = value shr 7

  while value > 0:
    encodedValues.add(byte(byteToBeAdded or 128))
    byteToBeAdded = value and 127
    value = value shr 7

  encodedValues.add(byte(byteToBeAdded))
  return encodedValues
