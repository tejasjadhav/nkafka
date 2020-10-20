import net
import protocol/types/numbers
import protocol/types/sequences
import protocol/request
import protocol/recordset
import protocol/produce

var conn = newSocket()
conn.connect("127.0.0.1", Port(9092))

var recordSet1 = newRecordSet(Int[Int64](value: 0), Int[Int8](value: 2), Int[Int16](value: 0))
recordSet1.addRecord("k", "v1")

var produceRequest = ProduceRequest(
  transactionId: Sequence[String](value: "txn-id"),
  acks: Int[Int16](value: 1),
  timeout: Int[Int32](value: 10),
  topicData: Array[TopicData](
    values: @[
      TopicData(
        topic: Sequence[String](value: "hello"),
        data: Array[PartitionData](
          values: @[
            PartitionData(
              partition: Int[Int32](value: 0),
              records: recordSet1,
            ),
          ],
        ),
      ),
    ],
  ),
)

var message = KafkaRequest(
  header: RequestHeader(
    apiKey: Int[Int16](value: 0),
    apiVersion: Int[Int16](value: 8),
    correlationId: Int[Int32](value: 1),
    clientId: Sequence[String](value: "")
  ),
  request: produceRequest,
)

stdout.writeLine produceRequest.encode
var finalMessage: string

for c in message.encode:
  stdout.write c
  stdout.write " "
  finalMessage.add(char(c))

stdout.writeLine ""
stdout.writeLine finalMessage
conn.send finalMessage
