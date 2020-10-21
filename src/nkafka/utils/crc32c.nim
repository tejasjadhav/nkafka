type TCrc32* = uint32
const InitCrc32* = TCrc32(0xFFFFFFFF)

proc createCrcTable(): array[0..255, TCrc32] =
  for i in 0..255:
    var rem = TCrc32(i)
    for j in 0..7:
      if (rem and 1) > 0: rem = (rem shr 1) xor TCrc32(0x82F63B78)
      else: rem = rem shr 1
    result[i] = rem

# Table created at compile time
const crc32table = createCrcTable()

proc crc32c*(s: seq[byte]): TCrc32 =
  ## Calculates the CRC32C for the byte sequence.
  result = InitCrc32
  for c in s:
    var tableIndex = (result xor c) and 0xff
    result = (crc32table[tableIndex] xor (result shr 8)) and InitCrc32
  return result xor InitCrc32
