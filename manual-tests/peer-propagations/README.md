# Peer propagation test

This test is to check that the sync connections get set up properly, even through indirect information about peers.

1. Start servers 1, 2 and 3
2. Add member 2 to server 1
3. Add member 3 to server 1
4. Stop server 1
5. Write to server 2 and check it syncs to server 3
6. Write to server 3 and check it syncs to server 2
