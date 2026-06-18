class ArbitraryEvent:
    def __init__(self, name, body, required_acl=None):
        self.name = name
        self._body = dict(body)
        if required_acl:
            self.required_acl = required_acl

    def marshal(self):
        return self._body

    def __eq__(self, other):
        if not isinstance(other, ArbitraryEvent):
            return NotImplemented
        return (
            self.name == other.name
            and self._body == other._body
            and getattr(self, "required_acl", None)
            == getattr(other, "required_acl", None)
        )
