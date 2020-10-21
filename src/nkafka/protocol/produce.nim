import types/base
import types/numbers
import types/sequences
import recordset

type
  PartitionData* = ref object of Base
    partition*: Int[Int32]
    records*: RecordSet

proc encode*(self: PartitionData): seq[byte] =
  return (
    self.partition.encode &
    self.records.encode
  )

type
  TopicData* = ref object of Base
    topic*: Sequence[String]
    data*: Array[PartitionData]

proc encode*(self: TopicData): seq[byte] =
  return (
    self.topic.encode &
    self.data.encode
  )

type
  ## Request object for sending a message to a Kafka broker.
  ProduceRequest* = ref object of Base
    transactionId*: Sequence[String]
    acks*: Int[Int16]
    timeout*: Int[Int32]
    topicData*: Array[TopicData]

proc encode*(self: ProduceRequest): seq[byte] =
  return (
    self.transactionId.encode &
    self.acks.encode &
    self.timeout.encode &
    self.topicData.encode
  )
