for i in range(1000):
    n1= eval(input("Enter 1st No: "))
    n2=eval(input("Enter 2nd No: "))
    key=eval(input("""Enter your Choice:- Press:- 1 to Add, 2 to Substract, 3 to Multiply, 4 to Divide :"""))
    def add(x,y):
        return x+y
    def sub(x,y):
        return x-y
    def prod(x,y):
        return(x*y)
    def div(x,y):
        return(x/y)
    print("Your Answer: ")
    if key==1:
        print(add(n1,n2))
    elif key==2:
        print(sub(n1,n2))
    elif key==3:
        print(prod(n1,n2))
    elif key==4:
        print(div(n1,n2))
    Op=input("Do you want to quit? Y/N: ")
    if Op=='N':
        continue
    elif Op=='Y':
        quit()