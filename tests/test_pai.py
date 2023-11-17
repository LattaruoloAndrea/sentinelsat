from test_pai_sec import TestPaiSec

class TestPai:

    def __init__(self):
        self.pp = TestPaiSec(self)
        self.ll = "Primo"

if __name__ == "__main__":
    a = TestPai()
    i=0
    while True:
        a = a.pp
        i+=1
        print(i,a.ll)
    print("end")