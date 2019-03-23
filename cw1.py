#!/usr/bin/env python
import sys, math ,random
import numpy as np
import matplotlib.pyplot as plt
class GAP:
    def GoogleAppengine(self):
        s3=S3()
        mode=int(input("1.given number 2.given decimal place of accuracy "))
        server=int(input("which calculater do you choose 1.AWS LAMBDA 2.AWS EMR"))
        if(mode==1):
            inputNum=self.setinput()
            R=self.setR()
            Q=self.setQ()
            if(inputNum<R*Q):
                print("the parameter of you input is not suitable to run")
            else:
                data=self.call_calculater(mode,server,inputNum,R,Q)
                s3.setResult(data)
                print(data)
                pi=np.full((R*Q),math.pi)
                print (pi)
                plt.plot(pi)
                plt.plot(data)
                plt.show()
        if(mode==2):
            inputAccurary=int(input("please input the Accurary"))
            inputNum=int(input("please input the number of each try"))
            R=self.setR()
            Q=self.setQ()
            if(inputNum<R*Q):
                print("the parameter of you input is not suitable to run")
            else:
                data=self.call_calculater(mode,server,inputNum,R,Q,inputAccurary)
                s3.setResult(data)
                print(data)
                pi=np.full((R*Q),math.pi)
                print (pi)
                plt.plot(pi)
                plt.plot(data)
                
                plt.show()
    def call_calculater(self,mode,server,inputNum,R,Q,inputAccurary=None):
            if(mode==1):
                run=AWSLAMBDA()
                if(server==1):
                    shotsForEachBlock=inputNum//(Q*R)
                    incircleArray=np.zeros(Q*R)
                    finalResult=np.zeros(Q*R)
                    for thread in range(0,R):
                        incircleArray[thread*Q:(thread*Q)+Q]=run.Lambda_calculater(inputNum//R,Q)
                    finalResult[0]=incircleArray[0]
                    for i in range(1,len(finalResult)):
                        finalResult[i]=incircleArray[i]+finalResult[i-1]
                        finalResult[i-1]/=shotsForEachBlock*(i)
                    finalResult[len(finalResult)-1]/=shotsForEachBlock*len(finalResult)
                    return finalResult
                if(server==2):
                    taskSplit=R*Q
                    shotsForEachBlock=inputNum//taskSplit
                    taskData=S3(np.full(taskSplit,shotsForEachBlock))
                    if(inputNum%taskSplit!=0):# If inputNum cannot be divisible by R*Q
                        np.append( taskData.inputData,inputNum%taskSplit)
                    Cluster=AWSEMR(R,taskData)
                    taskData=Cluster.runCluster()
                    for i in range(0,len(taskData.output)-1):
                        taskData.output[i]/=shotsForEachBlock*(i+1)
                    if(inputNum%taskSplit!=0):# If inputNum cannot be divisible by R*Q
                        taskData.output[len(taskData.output)]/=shotsForEachBlock*len(taskData.output)+inputNum%taskSplit
                    else:
                        taskData.output[len(taskData.output)-1]/=shotsForEachBlock*(len(taskData.output))
                    return taskData.output
                

            if(mode==2):
                if(server==1):
                    found=0
                    run=AWSLAMBDA()
                    finalResult=np.zeros(Q*R)
                    incircleArray=np.zeros(Q*R)
                    shotsForEachBlock=inputNum//(Q*R)
                    runTimes=0
                    while(found==0):
                        runTimes+=1
                        for thread in range(0,R):
                            incircleArray[thread*Q:(thread*Q)+Q]+=run.Lambda_calculater(inputNum//R,Q)
                        if(round(np.sum(incircleArray)/(shotsForEachBlock*len(incircleArray)*runTimes),inputAccurary)==round(math.pi,inputAccurary)):
                            found=1
                        else:
                            print(np.sum(incircleArray)/(shotsForEachBlock*len(incircleArray)*runTimes))
                    finalResult[0]=incircleArray[0]
                    for i in range(1,len(finalResult)):
                        finalResult[i]=incircleArray[i]+finalResult[i-1]
                        finalResult[i-1]/=shotsForEachBlock*i*runTimes
                    finalResult[len(finalResult)-1]/=shotsForEachBlock*len(finalResult)*runTimes
                    return finalResult
                        
                            

                if(server==2):
                    found=0
                    taskSplit=R*Q
                    shotsForEachBlock=inputNum//taskSplit
                    taskData=S3(np.full(taskSplit,shotsForEachBlock))
                    run=AWSEMR(R,taskData)
                    runTimes=0
                    finalResult=np.zeros(Q*R)
                    while(found==0):
                        runTimes+=1
                        run.runCluster()
                        finalResult+=taskData.output
                        if(round(finalResult[len(finalResult)-1]/(shotsForEachBlock*len(finalResult)*runTimes),inputAccurary)==round(math.pi,inputAccurary)):
                            found=1
                        else:
                            print(finalResult[len(finalResult)-1]/(shotsForEachBlock*len(finalResult)*runTimes))
                    for i in range(0,len(finalResult)):
                        finalResult[i]/=shotsForEachBlock*(i+1)*runTimes
                    return finalResult
    def setinput(self):
        inputNum=input("please input the number(default is 10000)")
        if (inputNum!=""):
            return int(inputNum)
        else:
            return 10000
    def setR(self):
        R=input("set the resourse count you want use(default is 2)")
        if (R!=""):
            return int(R)
        else:
            return 2
    def setQ(self):
        Q=input("set the Report rate you want use(default is 10)")
        if (Q!=""):
            return int(Q)
        else:
            return 10
class AWSLAMBDA:
    
    def Lambda_calculater(self,inputNum,Q):
        result=np.zeros(Q)
        for j in range(0,Q):
            incircle=0
            for s in range(0,inputNum//Q):
                random1 = random.uniform(-1.0, 1.0)
                random2 = random.uniform(-1.0, 1.0)
                if( ( random1*random1 + random2*random2 ) < 1 ):
                    incircle += 1
            result[j]= 4.0 * incircle
        return result




        
class AWSEMR():
    R=0
    def __init__(self,R,S3):
        self.R=R
        self.S3=S3
        self.step=0
    def AWSEMRmap(self):
        count=0
        for line in self.S3.inputData:
            incircle = 0
            shots=line
            for i in range(0, shots):
                
                random1 = random.uniform(-1.0, 1.0)
                random2 = random.uniform(-1.0, 1.0)
                if( ( random1*random1 + random2*random2 ) < 1 ):
                    incircle += 1

            self.S3.output[count]=4.0 * incircle #self.S3.output[count]= 4.0 * incircle/shots
            count+=1
    def reduceFunction(self):
        intial=0
        for i in range(0,len(self.S3.output)):
            if (i!=0):
                self.S3.output[i]+=self.S3.output[i-1]
            else:
                self.S3.output[0]+=intial
    def runCluster(self,step=0):
        self.step=step
        self.AWSEMRmap()
        self.reduceFunction()
        return self.S3
class S3():
    dataNum=0
    def setinput(self, inputData):
        self.inputData=inputData
        self.dataNum=len(inputData)
        self.output=np.zeros(self.dataNum)
    def setResult(self,result):
        self.result=result
if __name__ =="__main__":
    run=GAP()
    run.GoogleAppengine()

