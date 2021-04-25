import builtins


def protobuf_encode(a: str) -> str:
    # TODO: PROTOBUF
    return a


def append_documentation(data: list, documentation: str):
    if not documentation:
        return
    documentation.split()
    lines: list = documentation.split("\n")

    if len(lines) > 1 and lines[-1]:
        lines = lines.pop()

    for line in lines:
        data.append("# ")
        data.append(line)
        data.append("\n")


def append_options(data: list, options: list):
    count = len(options)
    if count == 1:
        data.append('[')
        data.append(options[0].to_schema())
        data.append(']')
        return

    data.append("[\n")
    for i in range(0, count):
        if i < count - 1:
            endl = ","
        else:
            endl = ""
        append_indented(data, options[i].to_schema() + endl)
    data.append(']')


def append_indented(data: list, value: str):
    lines = value.split("\n")
    if len(lines) > 1 and not lines[-1]:
        lines = lines.pop()

    for line in lines:
        data.append("  ")
        data.append(line)
        data.append("\n")


MIN_TAG_VALUE = 1
MAX_TAG_VALUE = ((1 << 29) & 0xffffffffffffffff) - 1  # 536,870,911

RESERVED_TAG_VALUE_START = 19000
RESERVED_TAG_VALUE_END = 19999

""" True if the supplied value is in the valid tag range and not reserved.  """


class MyInt(int):
    def is_valid_tag(self) -> bool:
        return (MIN_TAG_VALUE <= self <= RESERVED_TAG_VALUE_START) or (
                RESERVED_TAG_VALUE_END + 1 <= self <= MAX_TAG_VALUE + 1)


builtins.int = MyInt

# TODO: remove following text if not implemented

""" internal expect fun Char.isDigit(): bool

internal expect fun str.toEnglishLowerCase(): str

expect interface MutableQueue<T : Any> : MutableCollection<T> {
  fun poll(): T?
}

internal expect fun <T : Any> mutableQueueOf(): MutableQueue<T>

# TODO internal and friend for wire-compiler: https:#youtrack.jetbrains.com/issue/KT-34102

 * Replace types in this schema which are present in [typesToStub] with empty shells that have no
 * outward references. This has to be done in this module so that we can access the internal
 * constructor to avoid re-linking.

fun Schema.withStubs(typesToStub: Set<ProtoType>): Schema {
  if (typesToStub.isEmpty():
    return this
  }
  return Schema(protoFiles.map { protoFile ->
    protoFile.copy(
        types = protoFile.types.map { type ->
          if (type.type in typesToStub) type.asStub() else type
        },
        services = protoFile.services.map { service ->
          if (service.type in typesToStub) service.asStub() else service
        }
    )
  })
}

 Return a copy of this type with all possible type references removed. 
private fun Type.asStub(): Type = when {
  # Don't stub the built-in protobuf types which model concepts like options.
  type.tostr().startsWith("google.protobuf.") -> this

  this is MessageType -> copy(
      declaredFields = emptyList(),
      extensionFields = mutableListOf(),
      nestedTypes = nestedTypes.map { it.asStub() },
      options = Options(Options.MESSAGE_OPTIONS, emptyList())
  )

  this is EnumType -> copy(
      constants = emptyList(),
      options = Options(Options.ENUM_OPTIONS, emptyList())
  )

  this is EnclosingType -> copy(
      nestedTypes = nestedTypes.map { it.asStub() }
  )

  else -> throw AssertionError("Unknown type $type")
}

 Return a copy of this service with all possible type references removed. 
private fun Service.asStub() = copy(
    rpcs = emptyList(),
    options = Options(Options.SERVICE_OPTIONS, emptyList())
)
        
"""
