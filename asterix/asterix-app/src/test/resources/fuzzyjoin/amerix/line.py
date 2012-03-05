
m = 0
for line in open('/data/fuzzyjoin/pub/csx-id.txt'):
    l = len(line)
    if (l > m):
        m = l
print m
