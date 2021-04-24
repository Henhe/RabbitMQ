import random
import json
from pathlib import Path

str_ = 'asjfdklhjqkwlerhc xzmcnvv mnar lejwkl qrm nczx ljklw qjreqwerenm zcxv klasj dflkajwe rljqlw rjkqlw r'
lst = ['1.oleg.2', '1.test1.2']
dict = {}
for i in range(10):
    beg = random.randint(0, len(str_) - 1)
    end = random.randint(beg, len(str_) - 1)
    message = str_[beg: end]

    beg = random.randint(0, len(str_) - 1)
    end = random.randint(beg, min(beg + 10, len(str_) - 1))
    filename = f'{str_[beg: end]}.json'

    flag = random.randint(0, 1)
    routkey = lst[flag]
    # print(message, "*******************", filename, "***************", routkey)
    if len(filename) > 5:
        path = Path('OUT', filename)
        dict['routingkey'] = routkey
        dict['message'] = message
        with open(path, "w") as write_file:
            json.dump(dict, write_file)