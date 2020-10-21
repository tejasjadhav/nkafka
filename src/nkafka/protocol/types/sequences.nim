import base
import numbers

type
  Bytes* = distinct string
  String* = distinct string
  VarIntString* = distinct string
  SequenceTypes = Bytes | String | VarIntString

  ## Generic Kafka type for string-like structures.
  Sequence*[S] = ref object of Base
    value*: string

proc encode*[S: SequenceTypes](self: Sequence[S]): seq[byte] =
  ## Encodes string into byte sequence prefixed with the length of
  ## the string which is also encoded in byte sequence.
  var encodedValues: seq[byte]
  for c in self.value:
    encodedValues.add(byte(c))

  var seqSize: seq[byte] = if S is String: Int[Int16](value: Int16(encodedValues.len)).encode
  elif S is VarIntString: Int[VarInt](value: int32(encodedValues.len)).encode
  else: Int[Int32](value: Int32(encodedValues.len)).encode
  return seqSize & encodedValues

type
  ## Kafka array.
  Array*[T] = ref object of Base
    values*: seq[T]

proc encode*(self: Array): seq[byte] =
  ## Encodes an array into byte sequence prefixed with the length of
  ## the array which is also encoded in byte sequence.
  var encodedValues: seq[byte]
  for value in self.values:
    encodedValues = encodedValues & value.encode

  return Int[Int32](value: Int32(self.values.len)).encode & encodedValues
