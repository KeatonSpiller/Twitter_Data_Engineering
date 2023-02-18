import numpy as np, random, string
def randomize_key(randomize_from = -10, randomize_to = 10, size = 255, specified=string.printable):
    """_summary_

    Args:
        randomize_from (int, optional): _description_. Defaults to -10.
        randomize_to (int, optional): _description_. Defaults to 10.
        size (int, optional): _description_. Defaults to 255.
        specified (_type_, optional): _description_. Defaults to string.printable.

    Returns:
        _type_: _description_
    """
    choice = ''
    a = np.arange(randomize_from, randomize_to)
    for i in specified:
        num = random.choice(a)
        if( num < 0 and num > randomize_from//2):
            addition = i*abs(num)
        elif( num == randomize_from//2 or num == randomize_to//2):
            addition = ''
        elif( num > 0 and num < randomize_to//2):
            addition = i
        else:
            addition = i
        choice = choice + addition
    possibilites = random.sample(choice, len(choice))
    complex_password = "".join(random.choice(possibilites) for i in range(size))
    return complex_password