import datetime

dObj=dict()
dValues=dict()
dCompaction=dict()
def compaction(dictObj,now,hObj):
    lstKeys=list(dictObj.keys())
    qsort(lstKeys)
    
    print(lstKeys)
    for x in lstKeys:
        print(x)
        if now <= x:
            #print("yes {str} : {str2} ".format(str=dictObj[x],str2=x))
            #print(dictObj[x])
            break
        else:
            #print("No {str} : {str2} ".format(str=dictObj[x],str2=x))
            #print(dictObj[x])
            hObj.remove(dictObj[x])
            dictObj.pop(x)

def qsort(arr):

    qsort_help(arr,0,len(arr)-1)

def qsort_help(arr,first,last):
    
    if first<last:
        

        splitpoint = partition(arr,first,last)

        qsort_help(arr,first,splitpoint-1)
        qsort_help(arr,splitpoint+1,last)


def partition(arr,first,last):
    
    pivotvalue = arr[first]

    leftmark = first+1
    rightmark = last

    done = False
    while not done:

        while leftmark <= rightmark and arr[leftmark] <= pivotvalue:
            leftmark = leftmark + 1

        while arr[rightmark] >= pivotvalue and rightmark >= leftmark:
            rightmark = rightmark -1

        if rightmark < leftmark:
            done = True
        else:
            temp = arr[leftmark]
            arr[leftmark] = arr[rightmark]
            arr[rightmark] = temp

    temp = arr[first]
    arr[first] = arr[rightmark]
    arr[rightmark] = temp


    return rightmark

def expired(key,dObj):
        now=now = datetime.datetime.now()
        if dObj[key]<now:
            return True
        else:
            return False

class HashTable(object):
    
    
    def __init__(self,size):
        
        # Set up size and slots and data
        self.size = size
        self.slots = [None] * self.size
        self.data = [None] * self.size
        
    def put(self,key,data,ttl):
        #Note, we'll only use integer keys for ease of use with the Hash Function
        
        # Get the hash value
        hashvalue = self.hashfunction(key,len(self.slots))

        # If Slot is Empty
        if self.slots[hashvalue] == None:
            self.slots[hashvalue] = key
            self.data[hashvalue] = data
        
        else:
            
            # If key already exists, replace old value
            if self.slots[hashvalue] == key:
                self.data[hashvalue] = data  
            
            # Otherwise, find the next available slot
            else:
                
                nextslot = self.rehash(hashvalue,len(self.slots))
                
                # Get to the next slot
                while self.slots[nextslot] != None and self.slots[nextslot] != key:
                    nextslot = self.rehash(nextslot,len(self.slots))
                
                # Set new key, if NONE
                if self.slots[nextslot] == None:
                    self.slots[nextslot]=key
                    self.data[nextslot]=data
                    
                # Otherwise replace old value
                else:
                    self.data[nextslot] = data 
        now = datetime.datetime.now()
        ttl=now+datetime.timedelta(0,ttl) 
        dObj[key] = ttl
        dCompaction[ttl] = key
        if data not in dValues.keys(): 
            lKeys=list()
        else:
            lKeys=dValues[data]
        if key not in lKeys:
            lKeys.append(key)
            dValues[data]=lKeys

    def hashfunction(self,key,size):
        # Remainder Method
        return key%size

    def rehash(self,oldhash,size):
        # For finding next possible positions
        return (oldhash+1)%size
    
    
    def get(self,key):
        
        # Getting items given a key
        
        # Set up variables for our search
        startslot = self.hashfunction(key,len(self.slots))
        data = None
        stop = False
        found = False
        position = startslot
        
        # Until we discern that its not empty or found (and haven't stopped yet)
        while self.slots[position] != None and not found and not stop:
            
            if self.slots[position] == key and expired(key,dObj)==False: # This line can be commented and next one uncommented to see if compaction works
            #if self.slots[position] == key:# and expired(key,dObj)==False:
                found = True
                data = self.data[position]
                
            else:
                position=self.rehash(position,len(self.slots))
                if position == startslot:
                    
                    stop = True
        return data

    
    # Special Methods for use with Python indexing
    def remove(self,key):
        startslot = self.hashfunction(key,len(self.slots))
        data = None
        stop = False
        found = False
        position = startslot
        
        # Until we discern that its not empty or found (and haven't stopped yet)
        while self.slots[position] != None and not found and not stop:
            
            if self.slots[position] == key :
                found = True
                hashvalue = self.hashfunction(key,len(self.slots))
                data = self.data[position]
                self.slots[hashvalue] = None
                self.data[hashvalue] = None
                #del self.data[position]
                #update the dVal dictionary
                lKeys=dValues[data]
                if  key in lKeys:
                    lKeys.remove(key)
                    dValues[data]=lKeys
            else:
                position=self.rehash(position,len(self.slots))
                if position == startslot:
                    
                    stop = True
        

    def __getitem__(self,key):
        return self.get(key)

    def __setitem__(self,key,data,ttl):
        self.put(key,data,ttl)

if __name__ == '__main__':
    h=HashTable(7)

    #h.put(1,['A'])
    h.put(1,'AbCD',600)
    h.put(2,'A',300)
    h.put(2,'PQRS',-300)
    h.put(3,'PQRS',300)
    h.put(4,'PQRS',300)
    h.put(5,'PQRS',300)
    h.put(6,'AbCD',300)
    h.put(7,'AbCD',300)
    h.remove(1)
    #h.put(1,"XYZ",1000)
    #print(h[2])
    #print(h[1])
    h.put(8,"ccd",300)

    #print(h.get(1))

    #print(dValues)
    #print(dObj)
    compaction(dCompaction,datetime.datetime.now(),h)
    print(h[2])
    #print(dObj)
