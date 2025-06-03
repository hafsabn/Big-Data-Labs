import sys

current_word = None

anagrams = []
for line in sys.stdin :
    words = line.strip().split()
    if current_word == words[0]:
        anagrams.append(words[1])
    else: 
        if current_word:
            print('{}\t{}'.format(current_word, anagrams))
        current_word = words[0]
        anagrams = []
        anagrams.append(words[1]) 
    current_word = words[0]

print('{}\t{}'.format(current_word, anagrams))
    
    
