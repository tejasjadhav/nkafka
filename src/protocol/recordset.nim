import times

import types/base
import types/numbers
import types/sequences
import ../utils/crc32c

type
  ## Basic unit containing the user message in a RecordSet.
  Record* = ref object of Base
    attributes*: Int[Int8]
    timestampDelta*: Int[VarInt]
    offsetDelta*: Int[VarInt]
    key*: Sequence[VarIntString]
    value*: Sequence[VarIntString]

proc encode*(self: Record): seq[byte] =
  ## Encodes the record into a byte sequence.
  var encodedValues = (
    self.attributes.encode &
    self.timestampDelta.encode &
    self.offsetDelta.encode &
    self.key.encode &
    self.value.encode &
    Int[VarInt](value: 0).encode # Record headers, intentionally left blank
  )

  return Int[VarInt](value: encodedValues.len).encode & encodedValues

type
  ## Container for Kafka messages along with common headers.
  RecordSet* = ref object of Base
    baseOffset*: Int[Int64]
    partitionLeaderEpoch: Int[Int32]
    magic*: Int[Int8]
    attributes*: Int[Int16]
    lastOffsetDelta: Int[Int32]
    firstTimestamp: Int[Int64]
    maxTimestamp: Int[Int64]
    producerId: Int[Int64]
    producerEpoc: Int[Int16]
    baseSequence: Int[Int32]
    records: Array[Record]

proc newRecordSet*(baseOffset: Int[Int64], magic: Int[Int8], attributes: Int[Int16]): RecordSet =
  return RecordSet(
    baseOffset: baseOffset,
    partitionLeaderEpoch: Int[Int32](value: 0),
    magic: magic,
    attributes: attributes,
    lastOffsetDelta: Int[Int32](value: 0),
    firstTimestamp: Int[Int64](value: 0),
    maxTimestamp: Int[Int64](value: 0),
    producerId: Int[Int64](value: -1),
    producerEpoc: Int[Int16](value: -1),
    baseSequence: Int[Int32](value: -1),
    records: Array[Record](values: @[]),
  )

proc addRecord*(self: RecordSet, key: string, value: string) =
  ## Adds a record to the message set for the specified key and value. Appropriately
  ## sets the last offset delta, first timestamp and last timestamp.
  var currentTimestamp = Int[Int64](value: int64(epochTime() * 1000))
  if self.firstTimestamp.isNil or self.firstTimestamp.value == 0:
    self.firstTimestamp = currentTimestamp

  var timestampDelta = currentTimestamp.value - self.firstTimestamp.value
  self.maxTimestamp = currentTimestamp

  var recordLength = self.records.values.len
  var offsetDelta = recordLength
  if recordLength == 0:
    offsetDelta = 0

  var record = Record(
    attributes: Int[Int8](value: 0),
    timestampDelta: Int[VarInt](value: int(timestampDelta)),
    offsetDelta: Int[VarInt](value: offsetDelta),
    key: Sequence[VarIntString](value: key),
    value: Sequence[VarIntString](value: value),
  )

  self.records.values.add(record)

  if self.lastOffsetDelta.isNil:
    self.lastOffsetDelta = Int[Int32](value: Int32(offsetDelta))
  else:
    self.lastOffsetDelta.value = Int32(offsetDelta)

proc encode*(self: RecordSet): seq[byte] =
  ## Encodes the message set into a byte sequence prefixed with the size of the message set in bytes
  ## which is also encoded as a byte sequence. Calculates the CRC value for the total payload.
  var crcData = (
    self.attributes.encode &
    self.lastOffsetDelta.encode &
    self.firstTimestamp.encode &
    self.maxTimestamp.encode &
    self.producerId.encode &
    self.producerEpoc.encode &
    self.baseSequence.encode &
    self.records.encode
  )

  var crc = crc32c(crcData)

  var recordSetData = (
    self.partitionLeaderEpoch.encode &
    self.magic.encode &
    Int[Uint32](value: Uint32(crc)).encode &
    crcData
  )

  var recordSet = (
    self.baseOffset.encode &
    Int[Int32](value: Int32(recordSetData.len)).encode &
    recordSetData
  )

  return Int[Int32](value: Int32(recordSet.len)).encode & recordSet
