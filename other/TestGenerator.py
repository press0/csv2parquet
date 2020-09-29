"""https://wiki.python.org/moin/Generators"""

def firstn_list(n):
    """Build and return a list"""
    num, nums = 0, []
    while num < n:
        nums.append(num)
        num += 1
    return nums


def firstn_generator(n):
    """generator that yields items instead of returning a list"""
    num = 0
    while num < n:
        yield num
        num += 1


if __name__ == '__main__':
    sum_of_first_n_list = sum(firstn_list(11))
    print(sum_of_first_n_list)

    sum_of_first_n_generator = sum(firstn_generator(11))
    print(sum_of_first_n_generator)
