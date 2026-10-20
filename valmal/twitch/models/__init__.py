"""The shapes Twitch sends and returns, described as msgspec Structs.

`parse` is how every caller turns a body into one, so all of them parse alike.

Nothing is re-exported here: import from the module that owns the name, so a
reader lands on the file that defines it rather than on a list of them.
`valmal/db/models/` is the other model package, and holds the SQLModel tables.
"""
