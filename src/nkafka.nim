import net
import types
import crc32
import sugar
import times

var conn = newSocket()
conn.connect("127.0.0.1", Port(9092))

type RequestHeader = object
  apiKey: Int16
  apiVersion: Int16
  correlationId: Int32
  clientId: String

method encode(this: RequestHeader): seq[byte] =
  return (
    this.apiKey.encode &
    this.apiVersion.encode &
    this.correlationId.encode &
    this.clientId.encode
  )

type Record = object
  attributes: Int8
  timestampDelta: VarInt
  offsetDelta: VarInt
  key: VarIntString
  value: VarIntString

method encode(this: Record): ByteArray =
  var encodedValues = (
    this.attributes.encode &
    this.timestampDelta.encode &
    this.offsetDelta.encode &
    this.key.encode &
    this.value.encode &
    VarInt(value: 0).encode
  )

  return VarInt(value: encodedValues.len).encode & encodedValues

type RecordSet = object
  baseOffset: Int64
  partitionLeaderEpoch: Int32
  magic: Int8
  crc: Uint32
  attributes: Int16
  lastOffsetDelta: Int32
  firstTimestamp: Int64
  maxTimestamp: Int64
  producerId: Int64
  producerEpoc: Int16
  baseSequence: Int32
  records: Array[Record]

method addRecord(this: ref RecordSet, key: string, value: string) =
  var currentTimestamp = Int64(value: int64(epochTime() * 1000))
  if this.firstTimestamp.isNil or this.firstTimestamp.value == 0:
    this.firstTimestamp = currentTimestamp

  var timestampDelta = currentTimestamp.value - this.firstTimestamp.value
  this.maxTimestamp = currentTimestamp

  var recordLength = this.records.values.len
  var offsetDelta = recordLength
  if recordLength == 0:
    offsetDelta = 0

  var record = Record(
    attributes: Int8(value: 0),
    timestampDelta: VarInt(value: int(timestampDelta)),
    offsetDelta: VarInt(value: offsetDelta),
    key: VarIntString(value: key),
    value: VarIntString(value: value),
  )

  this.records.values.add(record)

  if this.lastOffsetDelta.isNil:
    this.lastOffsetDelta = Int32(value: offsetDelta)
  else:
    this.lastOffsetDelta.value = offsetDelta

method encode(this: ref RecordSet): seq[byte] =
  var crcData = (
    this.attributes.encode &
    this.lastOffsetDelta.encode &
    this.firstTimestamp.encode &
    this.maxTimestamp.encode &
    this.producerId.encode &
    this.producerEpoc.encode &
    this.baseSequence.encode &
    this.records.encode
  )

  var crc = crc32(crcData)

  var recordSetData = (
    this.partitionLeaderEpoch.encode &
    this.magic.encode &
    Uint32(value: uint32(crc)).encode &
    crcData
  )

  var recordSet = (
    this.baseOffset.encode &
    Int32(value: recordSetData.len).encode &
    recordSetData
  )

  return Int32(value: recordSet.len).encode & recordSet

type PartitionData = object
  partition: Int32
  records: ref RecordSet

method encode(this: PartitionData): seq[byte] =
  return (
    this.partition.encode &
    this.records.encode
  )

type TopicData = object
  topic: String
  data: Array[PartitionData]

method encode(this: TopicData): seq[byte] =
  return (
    this.topic.encode &
    this.data.encode
  )

type ProduceRequest = object
  transactionId: String
  acks: Int16
  timeout: Int32
  topicData: Array[TopicData]

method encode(this: ProduceRequest): seq[byte] =
  return (
    this.transactionId.encode &
    this.acks.encode &
    this.timeout.encode &
    this.topicData.encode
  )

type ProduceMessage = object
  header: RequestHeader
  request: ProduceRequest

method encode(this: ProduceMessage): seq[byte] =
  var headerBytes = this.header.encode
  var requestBytes = this.request.encode

  return (
    Int32(value: (headerBytes.len + requestBytes.len)).encode &
    headerBytes &
    requestBytes
  )

var recordSet = new(RecordSet)
recordSet.baseOffset = Int64(value: 0)
recordSet.partitionLeaderEpoch = Int32(value: 0)
recordSet.magic = Int8(value: 2)
recordSet.attributes = Int16(value: 0)
recordSet.producerId = Int64(value: -1)
recordSet.producerEpoc = Int16(value: -1)
recordSet.baseSequence = Int32(value: -1)
recordSet.records = Array[Record](values: @[])

recordSet.addRecord("hello", "40e151dc-b685-456c-af3f-5a9e81d95976a48c696b-d928-4444-bae7-9809ef983989f38d8b25-9846-4e73-ae7c-6de450caf2ab1bb9c52a-23db-47b2-bb52-54eade9c8dfded53ffaf-ea0e-438c-aeae-5349b064eef524a02c36-56cf-41ed-beb5-b045959d876ca5f6f747-dc5c-44e5-8a29-93d1e9de6adb")
recordSet.addRecord("hola", "4e0af478-3382-48a8-82e2-c51e10610d7365f224d0-dee7-4fb7-a30a-307ff278e22250244c8c-eb5e-476d-bda7-62ebd6ce9f57dffb65ff-61bc-43f0-9ce7-d3b353faa0a6")

var message = ProduceMessage(
  header: RequestHeader(
    apiKey: Int16(value: 0),
    apiVersion: Int16(value: 8),
    correlationId: Int32(value: 1),
    clientId: String(value: "")
  ),
  request: ProduceRequest(
    transactionId: String(value: "txn-id"),
    acks: Int16(value: 1),
    timeout: Int32(value: 10),
    topicData: Array[TopicData](
      values: @[
        TopicData(
          topic: String(value: "hello"),
          data: Array[PartitionData](
            values: @[
              PartitionData(
                partition: Int32(value: 0),
                records: recordSet,
              ),
            ],
          ),
        ),
      ],
    ),
  ),
)

dump message

var finalMessage: string

for c in message.encode:
  stdout.write c
  stdout.write " "
  finalMessage.add(char(c))

stdout.writeLine ""
stdout.writeLine finalMessage
conn.send finalMessage
