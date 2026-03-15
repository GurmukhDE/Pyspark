list1 = [1,2,3,4]
list2 = list1


list1

list2

list1, list2

list1[1] = 1000

list2

list1

a = [1,2,3,4]


a

#a.append([4,5])

a

a.extend([4,5])

a

lis = [1,2,34,45,5]

sqr_list = list(map(lambda x: x*2, lis))

print(sqr_list)

lis = [1,2,34,45,5]

sqr_list = list(filter(lambda x: x*2, lis))

print(sqr_list)

from functools import reduce

num = [1,2,3,5,5,6]
result = reduce(lambda x,y: x/y,num)
print(result)

a = [1,2,3,4,5,5,6,7,8,9,9,0,0,6,5,44]

even = []
odd= []
for i in a:
    if i%2==0:
        even.append(i)
    else:
        odd.append(i)
        
print(even, odd)

def even_odd(a):
    even = []
    odd = []
    for i in a:
        if i %2==0:
            even.append(i)
        else:
            odd.append(i)
            
    return even, odd

a = [1,3,4,5,7,8,9,9,5,8,9,0,2030030,24]
my_even = even_odd(a)
print(my_even)
            

even = [i for i in range(0,11) if i%2==0]
odd = [i for i in range(0,11) if i%2!=0]
print(even)
print(odd)

even = list(filter(lambda i:i%2==0, range(0,11)))
print(even)
odd = list(filter(lambda i:i%2!=0, range(0,11)))
print(odd)

st = "gurmukh"

print(st[::-1])

rev_str = ""


for i in st:
    if i in rev_str:
        i+=1
        print(rev_str)
print(rev_str)       
        

