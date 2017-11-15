from threading import Thread
from time import sleep

class TestThreads(object):
    def __init__(self):
        self.thread1 = None
        self.thread2 = None

    def function01(self, arg, name):
        for i in range(arg):
            print(name,'i---->',i,'\n')
            print (name,"arg---->",arg,'\n')
            sleep(1)

    def test01(self):
        self.thread1 = Thread(target = self.function01, args = (20, 'thread1', ))
        self.thread1.start()
        self.thread2 = Thread(target = self.function01, args = (30, 'thread2', ))
        self.thread2.start()

    def wrapUp(self):
        for i in range(15):
            print ("{} al cuadrado es {}".format(i, i*i))

        self.thread1.join()
        self.thread2.join()
        print ("threads finished...")
        print ("Now it's time to exit...")


def main():
    tst = TestThreads()
    tst.test01()
    tst.wrapUp()


if __name__ == '__main__':
    main()
