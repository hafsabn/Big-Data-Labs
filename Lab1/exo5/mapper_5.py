import sys

for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        sorted_word = ''.join(sorted(word))
        print('{}\t{}'.format(sorted_word, word))