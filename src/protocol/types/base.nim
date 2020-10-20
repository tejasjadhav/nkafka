type
  ## Base object for Kafka objects.
  Base* = ref object of RootObj

proc encode*(self: Base): seq[byte] =
  ## Encodes the object to a byte array.
  return @[]

proc reverse*(byteSeq: seq[byte]): seq[byte] =
  ## Reverses a byte sequence.
  var byteSeqSize = byteSeq.len
  result = newSeq[byte](byteSeqSize)
  for i in 0..<byteSeqSize:
    result[byteSeqSize - i - 1] = byteSeq[i]
