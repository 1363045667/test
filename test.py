class student:
    def __init__(self,salary):
        self.salary = salary

    def __call__(self,salary):
        print(salary)


s = student(2000)
s(3000)