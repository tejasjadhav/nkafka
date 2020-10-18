type ByteArray* = seq[byte]
proc `$`* (arr: ByteArray): string =
  var concatStr = ""
  for ch in arr:
    concatStr.add(chr(ch))
  return concatStr

type Base* = ref object of RootObj
method encode*(this: Base): ByteArray {.base.} =
  return @[]

type Int8* = ref object of Base
  value*: int

method encode*(this: Int8): ByteArray =
  return @cast[array[1, byte]](int8(this.value))

type Int16* = ref object of Base
  value*: int

method encode*(this: Int16): ByteArray =
  # FIXME: Make this generic by accepting type from parameter and decide array size based on type
  var encodedValues: ByteArray
  var castedValue = cast[array[2, byte]](int16(this.value))

  # FIXME: Find better way to reverse seq
  for i in countdown(castedValue.len - 1, 0):
    encodedValues.add(castedValue[i])

  return encodedValues

type Int32* = ref object of Base
  value*: int

method encode*(this: Int32): ByteArray =
  var encodedValues: ByteArray
  var castedValue = cast[array[4, byte]](int32(this.value))

  for i in countdown(castedValue.len - 1, 0):
    encodedValues.add(castedValue[i])

  return encodedValues

type Uint32* = ref object of Base
  value*: uint

method encode*(this: Uint32): ByteArray =
  var encodedValues: ByteArray
  var castedValue = cast[array[4, byte]](uint32(this.value))

  for i in countdown(castedValue.len - 1, 0):
    encodedValues.add(castedValue[i])

  return encodedValues

type Int64* = ref object of Base
  value*: int64

method encode*(this: Int64): ByteArray =
  var encodedValues: ByteArray
  var castedValue = cast[array[8, byte]](this.value)

  for i in countdown(castedValue.len - 1, 0):
    encodedValues.add(castedValue[i])

  return encodedValues

type VarInt* = ref object of Base
  value*: int

method encode*(this: VarInt): ByteArray =
  var encodedValues: ByteArray
  var value = (this.value shl 1) xor (this.value shr 63)

  var byteToBeAdded = value and 127
  value = value shr 7

  while value > 0:
    encodedValues.add(byte(byteToBeAdded or 128))
    byteToBeAdded = value and 127
    value = value shr 7

  encodedValues.add(byte(byteToBeAdded))
  return encodedValues

type Byte* = ref object of Base
  value*: string

method encode*(this: Byte): ByteArray =
  var encodedValues: ByteArray
  for c in this.value:
    encodedValues.add(byte(c))

  return Int32(value: encodedValues.len).encode & encodedValues

type String* = ref object of Base
  value*: string

method encode*(this: String): ByteArray =
  var encodedValues: ByteArray
  for c in this.value:
    encodedValues.add(byte(c))

  return Int16(value: encodedValues.len).encode & encodedValues

type Array*[T] = ref object of Base
  values*: seq[T]

method encode*(this: Array): ByteArray =
  var encodedValues: ByteArray
  for value in this.values:
    encodedValues = encodedValues & value.encode

  return Int32(value: this.values.len).encode & encodedValues

type VarIntString* = ref object of Base
  value*: string

method encode*(this: VarIntString): ByteArray =
  var encodedValues: ByteArray
  for c in this.value:
    encodedValues.add(byte(c))

  return VarInt(value: encodedValues.len).encode & encodedValues
