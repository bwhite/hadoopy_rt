import hadoopy


def mapper(k, v):
    yield '1' * k, '0' * v


if __name__ == '__main__':
    hadoopy.run(mapper)
