import secrets


def gen_random_hasher(max_val=9999999):
    seed = secrets.randbits(64)
    return lambda val: (hash(val) ^ seed) % max_val


s1 = gen_random_hasher()
run_id = s1('aaa')

print(run_id)
