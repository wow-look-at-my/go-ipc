# Turns ipcgen --lang json output into PEER_MESSAGES(X), which calls X(<C type>, <type ID macro>) once per message.
.package as $p
| "/* Generated from the schema by c/peer/messages.jq. */\n#define PEER_MESSAGES(X) \\\n"
+ (.messages | map("\tX(\($p)_\(.snake), \($p | ascii_upcase)_\(.snake | ascii_upcase)_TYPE) \\\n") | join(""))
+ "\n"
