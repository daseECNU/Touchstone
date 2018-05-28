
Running command: "java -jar Touchstone.jar XXX.conf" (can run in Windows)

The OS of servers in configuration must be Linux.
The password of the root user is used to clear the cache of the operating system (avoiding jvm gc).

Do not rename and move the input & lib dir.

Only supprt Java 8+.

...
Filter node: [0, exp1@op1#exp2@op2 ... #and|or, probability]
FKJoin node: [2, fk1#fk2 ..., probability, pk1#pk2 ..., num1, num2]
PKJoin node: [1, pk1#pk2 ..., num1, num2, ...]
// num1 is the identifier that can join, num2 is the identifier that can not join