import msgspec


class Message(msgspec.Struct):
    text: str
