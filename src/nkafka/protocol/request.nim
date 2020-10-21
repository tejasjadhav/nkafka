import types/base
import types/numbers
import types/sequences
import produce

type
  ## Common request header that gets passed in all requests to
  ## the Kafka broker.
  RequestHeader* = ref object of Base
    apiKey*: Int[Int16]
    apiVersion*: Int[Int16]
    correlationId*: Int[Int32]
    clientId*: Sequence[String]

proc encode*(self: RequestHeader): seq[byte] =
  return (
    self.apiKey.encode &
    self.apiVersion.encode &
    self.correlationId.encode &
    self.clientId.encode
  )

type
  KafkaCommandRequest* = ref object of Base

type
  ## Generic Kafka request for executing a Kafka command.
  KafkaRequest* = ref object of Base
    header*: RequestHeader
    request*: ProduceRequest

proc encode*(self: KafkaRequest): seq[byte] =
  var headerBytes = self.header.encode
  var requestBytes = self.request.encode

  return (
    Int[Int32](value: Int32(headerBytes.len + requestBytes.len)).encode &
    headerBytes &
    requestBytes
  )
