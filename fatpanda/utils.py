import random, string
import hashlib


def salt(n):
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(n)])

def source_hasher(source):
    return "T_"+hashlib.sha256(source.encode()).hexdigest()


def splitup(l, n):
	for i in range(0,len(l),n):
		yield l[i:i+n]