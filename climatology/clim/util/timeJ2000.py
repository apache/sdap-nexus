# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/bin/env python

"""
timeJ2000.py -- Date & Time class based on native Python datetime, time, and calendar
                libraries. Represents a Date/Time as seconds past J2000 epoch
                and provides various format conversions and date delta arithmetic.
                Also includes some new smart functions that perform desired
                transformations on a Do The Right Thing basis.
"""

import sys, datetime, calendar, time, types

##CONSTANTS
J2000_1970_EPOCH = 946684800 + 12*60*60 #2000/01/01,12:00:00 in seconds past 1970
LATEST_TIME      =  9999999999          #Highest (latest)  time in J2000 to care about... useful for initializations
EARLIEST_TIME    = -9999999999          #Lowest  (earlist) time in J2000 to care about... useful for initializations

def echo   (str          ): sys.stdout.write(str + "\n")
def err    (str          ): sys.stderr.write(str + "\n")
def warn   (str          ): err("---WARNING, IONOTIME: "+str)
def die    (str, status=1): err("***ERROR: "+str); sys.exit(status)

##BASE TRANSFORMATIONS
def ensureYYYY(y):
    if y>99: return y
    if y>50: return 1900+y
    return 2000+y

def ensureYY(y):
    return y%100

#transforms an hms string to a float hours
def hms_to_hours(str):
    return float(str[0:2])+float(str[2:4])/60.0+float(str[4:6])/360.0

def J2000_to_list(sec=0.0):
    #check for fractional seconds
    frac=0.0
    if sec > int(sec):
        frac=sec-int(sec)
        sec =int(sec)
    callist=list(time.gmtime(sec+J2000_1970_EPOCH))
    #add back in fractional seconds if present
    if frac > 0.0:
        callist[5]=callist[5]+frac
    return callist[0:6]
def list_to_J2000(inlist):
    #check for fractional seconds and remove
    clist=[0,0,0,0,0,0.0] #default to zeros everywhere
    clist[:len(inlist)]=inlist
    ss=clist[5]
    frac=0.0
    if ss > int(ss):
        frac=ss-int(ss)
        clist[5]=int(ss)
    #transform, adding fractional seconds afterwards
    return calendar.timegm(clist)-J2000_1970_EPOCH+frac

##INTELLIGENT FUNCTIONS
def valid_formats():
    return ('J2000',               #int or float bare number
            'HHMMSS',              #string
            'YYMMDD',              #string
            'YYYYMMDD',            #string
            'YYMMDDHHMMSS',        #string .
            'YYYYMMDDHHMMSS',      #string .
            'YYYYMMDD_HHMMSS',     #string .
            'YYMMDD_HHMMSS',       #string .
            'DOY',                 #string
            'HOD',"HOURSINDAY",    #string hours of day
            'MOD',"MINUTESINDAY",  #string minutes of day
            'SOD',"SECONDSINDAY",  #string seconds of day
            'YYDOY',               #string
            'LIST',                #list(y,m,d,h,m,s)
            'HMS',                 #string
            'YMD',                 #string
            'YMDHMS',              #string
            'GAIMSTRING',          #string yyyy/mm/dd,hh:mm:ss.frac
            'TENETHOURLY',         #string siteDOYlmm.yy.tenet
            'LOCALHMS',            #string HHMMSS.F adjusted for local time (requires longitude in deg)
            'HOURLETTER'#,          #string a where a(a,x) for each hour of day
#            'RINEX'                #string 
            )

def to_J2000(input,format=None):
    sec=0 #internal representation
    if format: format=format.upper()
    
    #assume J2000 seconds for any bare number
    if   isinstance(input,types.IntType) or isinstance(input,types.FloatType) or isinstance(input,types.LongType) or format=='J2000': return float(input)
    #if it's a list, simple... will be interpretted as y,m,d,hh,mm,ss with 0's in any unspecified slot
    elif isinstance(input,types.ListType) or isinstance(input,types.TupleType): return list_to_J2000(input)
    #if it's a string, could be many things
    elif isinstance(input,types.StringType):
        #strip off any fractional second information first
        p=input.find('.')
        frac=0.0
        if p>=0:
            if input.find('tenet') < 0:
                frac=float(input[p:])
                input  =input[:p]
        #Autoguess format based on length or user-specified request
        if len(input)==len('siteDOYlmm.yy.tenet') and format=="TENETHOURLY":
            (doy,hl,mm,y)=(int(input[4:7]),input[7:8],int(input[8:10]),int(input[11:13]))
            (yyyy,m,d)=J2000_to_list(list_to_J2000((ensureYYYY(int(y)),1,doy)))[0:3]
            return list_to_J2000((yyyy,m,d,ord(hl)-ord('a'),mm,0))
        
        if format=="DOY": 
            return list_to_J2000((2000,1,int(input)))

        if format in ("HOD","HOURSINDAY"): 
            return list_to_J2000((2000,1,1,int(input),0,0))

        if format in ("MOD","MINUTESINDAY"): 
            return list_to_J2000((2000,1,1,0,int(input),0))

        if format in ("SOD","SECONDSINDAY"): 
            return list_to_J2000((2000,1,1,0,0,int(input)))

        if format=="YYDOY":
            return list_to_J2000((ensureYYYY(int(input[0:2])),1,int(input[2:])))
        
        if len(input)==len('a') or format=='HOURLETTER':
            return list_to_J2000((2000,1,1,ord(input)-ord('a'),0,0))
        if len(input)==len('YYYY/MM/DD,HH:MM:SS') or format=='GAIMSTRING' or format=='ISO':
            return list_to_J2000((int(input[0:4]),
                                  int(input[5:7]),
                                  int(input[8:10]),
                                  int(input[11:13]),
                                  int(input[14:16]),
                                  int(input[17:19])+frac))
        if len(input)==len('YYYYMMDD_HHMMSS') or format=='YYYYMMDD_HHMMSS':
            return list_to_J2000((int(input[0:4]),
                                  int(input[4:6]),
                                  int(input[6:8]),
                                  int(input[9:11]),
                                  int(input[11:13]),
                                  int(input[13:15])+frac))
        
        if len(input)==len('YYMMDD_HHMMSS') or format=='YYMMDD_HHMMSS':
            return list_to_J2000((ensureYYYY(int(input[0:2])),
                                  int(input[2:4]),
                                  int(input[4:6]),
                                  int(input[7:9]),
                                  int(input[9:11]),
                                  int(input[11:13])+frac))
        
        if len(input)==len('YYYYMMDDHHMMSS') or format=='YYYYMMDDHHMMSS':
            return list_to_J2000((int(input[0:4]),
                                  int(input[4:6]),
                                  int(input[6:8]),
                                  int(input[8:10]),
                                  int(input[10:12]),
                                  int(input[12:14])+frac))
        
        if len(input)==len('YYMMDDHHMMSS') or format=='YYMMDDHHMMSS' or format=="YMDHMS":
            return list_to_J2000((ensureYYYY(int(input[0:2])),
                                  int(input[2:4]),
                                  int(input[4:6]),
                                  int(input[6:8]),
                                  int(input[8:10]),
                                  int(input[10:12])+frac))
        
        if len(input)==len('YYYYMMDD') or format=='YYYYMMDD':
            return list_to_J2000((int(input[0:4]),
                                  int(input[4:6]),
                                  int(input[6:8])))

        if len(input)==len('HHMMSS') and format in ('HHMMSS','HMS'): 
            return list_to_J2000((2000,1,1,
                                  int(input[0:2]),
                                  int(input[2:4]),
                                  int(input[4:6])+frac))
        
        if len(input)==len('YYMMDD') or format in ('YYMMDD','YMD'): 
            return list_to_J2000((ensureYYYY(int(input[0:2])),
                                  int(input[2:4]),
                                  int(input[4:6])))

        die("Unknown string format",input)
    die("Unknown input type to to_J2000:",input)

def from_J2000(sec=0,format="YYYYMMDD_HHMMSS",aux=None):
    #aux contains spare information, thusfar only used for site id's for filenames or longitude for localtime
    format=format.upper()
    if format == "J2000"                 : return sec
    (y,m,d,hh,mm,ss)=J2000_to_list(sec)
    f=""
    if ss > int(ss): f=("%f"%(ss-int(ss))).strip('0') #remove leading and trailing 0
    if format == "LIST"                  : return [y,m,d,hh,mm,ss]
    if format == "HOURLETTER"            : return chr(hh+ord('a'))
    if format in("HOURSINDAY","HOD")     : return hh+mm/60.0+ss/60.0/60.0
    if format in("MINUTESINDAY","MOD")   : return hh*60+mm+ss/60.0
    if format in("SECONDSINDAY","SOD")   : return (hh*60+mm)*60+ss
    if format in("HHMMSS","HMS")         : return "%02d%02d%02d"%(hh,mm,ss)+f
    if format in("YYMMDD","YMD")         : return "%02d%02d%02d"%(ensureYY(y),m,d)
    if format == "YYYYMMDD"              : return "%04d%02d%02d"%(y,m,d)
    if format in("YYMMDDHHMMSS","YMDHMS"): return "%02d%02d%02d%02d%02d%02d"%(ensureYY(y),m,d,hh,mm,ss)+f
    if format == "YYYYMMDDHHMMSS"        : return "%04d%02d%02d%02d%02d%02d"%(y,m,d,hh,mm,ss)+f
    if format == "YYMMDD_HHMMSS"         : return "%02d%02d%02d_%02d%02d%02d"%(ensureYY(y),m,d,hh,mm,ss)+f
    if format == "YYYYMMDD_HHMMSS"       : return "%04d%02d%02d_%02d%02d%02d"%(y,m,d,hh,mm,ss)+f
    if format == "GAIMSTRING"            : return "%04d/%02d/%02d,%02d:%02d:%02d"%(y,m,d,hh,mm,ss)+f
    if format == "ISO"                   : return "%04d-%02d-%02dT%02d:%02d:%02dZ"%(y,m,d,hh,mm,ss)+f
    doy = time.gmtime(sec+J2000_1970_EPOCH)[7] #fetch doy
    if format == "DOY"                   : return "%03d"%(doy)
    if format == "YYDOY"                 : return "%02d%03d"%(ensureYY(y),doy)
    if format == "TENETHOURLY"           :
        if not aux: aux="site"
        return "%4s%03d%1s%02d.%02d.tenet"%(aux,doy,chr(ord('a')+hh),mm,ensureYY(y))
    if format == "LOCALHMS"              : 
        if not aux: aux=0
        localtime = hh + aux/360.0*24.0        #in this case, aux is longitude in deg
        while (localtime <   0): localtime+=+24
        while (localtime >= 24): localtime-= 24
        return "%02d%02d%02d"%(localtime,mm,ss)+f
    die("Unrecognized format string in from_J2000 "+format)

class IonoTime:
    "Handles conversions between times and dates for all variety of ionospheric time interests"
    #internal representation is seconds past J2000
    def __init__(self,input=None):
            self.sec = 0
            self.set(input)
    def set(self,input=None,format=None):
        if not input: return self
        if isinstance(input,IonoTime):
            self.sec=input.sec
        else:
            self.sec = to_J2000(input,format)
        return self
    def to(self,format=None,aux=None):
        if not format: return self.sec
        return from_J2000(self.sec,format,aux)
    def now(self):
        self.sec = to_J2000(time.localtime()[0:6])
        return self
    def nowUTC(self):
        self.sec = to_J2000(time.gmtime()[0:6])
        return self
    def addSeconds(self,s):
        self.sec+=s
        return self
    def addMinutes(self,m):
        self.sec+=m*60.0
        return self
    def addHours  (self,h):
        self.sec+=h*60.0*60.0
        return self
    def addDays   (self,d):
        self.sec+=d*60.0*60.0*24.0
        return self
    def addMonths (self,mi):
        (y,m,d,hh,mm,ss)=from_J2000(self.sec,"LIST")
        m+=mi
        while m > 12:
            y=y+1
            m-=12
        while m < 1:
            y=y-1
            m+=12
        self.sec=to_J2000((y,m,d,hh,mm,ss))
        return self
    def addYears (self,yi):
        (y,m,d,hh,mm,ss)=from_J2000(self.sec,"LIST")
        self.sec=to_J2000((y+yi,m,d,hh,mm,ss))
        return self
    def copy      (self):
        n=IonoTime(self.sec)
        return n
    def makemidnight(self):
        (y,m,d,hh,mm,ss)=from_J2000(self.sec,"LIST")
        self.sec=to_J2000((y,m,d))
        return self
    def floor(self,interval): #round current object to a specified accuracy
        (y,m,d,hh,mm,ss)=from_J2000(self.sec,"LIST")
        interval=interval.lower()
        if   interval.find('year'  )>=0: self.sec=to_J2000((y, 1, 0,  0,  0,      0))
        elif interval.find('month' )>=0: self.sec=to_J2000((y, m, 1,  0,  0,      0))
        elif interval.find('day'   )>=0: self.sec=to_J2000((y, m, d,  0,  0,      0))
        elif interval.find('hour'  )>=0: self.sec=to_J2000((y, m, d, hh,  0,      0))
        elif interval.find('minute')>=0: self.sec=to_J2000((y, m, d, hh, mm,      0))
        elif interval.find('second')>=0: self.sec=to_J2000((y, m, d, hh, mm,int(ss)))
        else                           : die("IonoTime: Floor: Malformed interval: "+interval)
        return self
    def __sub__(self,other):
        return IonoTime(self.sec-other)
    def __add__(self,other):
        return IonoTime(self.sec+other)
#    def __iadd__(self,other):
#        return IonoTime(self.sec+other)
#    def __isub__(self,other):
#        return IonoTime(self.sec-other)
    def __cmp__(self,other):
        return cmp(self.sec,other.sec) 
    def __coerce__(self,other):
        if isinstance(other,types.FloatType) or isinstance(other,types.IntType) or isinstance(other,types.LongType):
            return (self.sec,other)
        if isinstance(other,types.StringType):
            return (from_J2000(self.sec,"YYYYMMDD_HHMMSS"),other)
        if isinstance(other,types.ListType) or isinstance(other,types.TupleType):
            return (from_J2000(self.sec,"LIST"),other)
    def __repr__(self):
        return from_J2000(self.sec,"YYYYMMDD_HHMMSS")
        
def test():
   print "Testing timeJ2000 routines:"
   print "Checking to_J2000"
   if not to_J2000("20040606"         )==139752000      : die("FAILED YYYYMMDD test")
   if not to_J2000("040606"           )==139752000      : die("FAILED YYMMDD test")
   if not to_J2000("20040606010101"   )==139755661      : die("FAILED YYYYMMDDHHMMSS test")
   if not to_J2000("c"                )==-36000.0       : die("FAILED HOURLETTER test")
   if not to_J2000("20040606010101.1" )==139755661.1    : die("FAILED YYYYMMDDHHMMSS.F test")
   if not to_J2000("20040606_010101"  )==139755661      : die("FAILED YYYYMMDD_HHMMSS test")
   if not to_J2000("20040606_010101.1")==139755661.1    : die("FAILED YYYYMMDD_HHMMSS.F test")
   if not to_J2000("040606_010101"    )==139755661      : die("FAILED YYMMDD_HHMMSS test")
   if not to_J2000("040606_010101.1"  )==139755661.1    : die("FAILED YYMMDD_HHMMSS.F test")
   if not to_J2000("040606010101"     )==139755661      : die("FAILED YYMMDDHHMMSS test")
   if not to_J2000("040606010101.1"   )==139755661.1    : die("FAILED YYMMDDHHMMSS.F test")
   if not to_J2000("121212.1",'HHMMSS')==732.1          : die("FAILED HHMMSS test")
   if not to_J2000(10244201.1         )==10244201.1     : die("FAILED J2000 test")
   if not to_J2000((2004,6,6,1,1,1.1) )==139755661.1    : die("FAILED list test")
   if not to_J2000("103",'DOY'        )==8769600        : die("FAILED DOY test")
   if not to_J2000("00103",'YYDOY'    )==8769600        : die("FAILED YYDOY test")
   if not to_J2000("2004/06/06,01:01:01.1")==139755661.1: die("FAILED GAIMSTRING test")
   if not to_J2000("help158b01.04.tenet",'TENETHOURLY')==139755660.0  : die("FAILED TENETHOURLY test")
   print "Passed to_J2000"

   print "Checking from_J2000"
   if not from_J2000(139752000  ,"YYYYMMDD"       )=="20040606"             : die("FAILED YYYYMMDD test")
   if not from_J2000(139752000.1,"YYYYMMDD"       )=="20040606"             : die("FAILED YYYYMMDD test")
   if not from_J2000(139752000  ,"YYMMDD"         )=="040606"               : die("FAILED YYMMDD test")
   if not from_J2000(139752000.1,"YYMMDD"         )=="040606"               : die("FAILED YYMMDD test")
   if not from_J2000(139755661  ,"HOURLETTER"     )=="b"                    : die("FAILED HOURLETTER test")
   if not from_J2000(139755661  ,"YYYYMMDDHHMMSS" )=="20040606010101"       : die("FAILED YYYYMMDDHHMMSS test")
   if not from_J2000(139755661.1,"YYYYMMDDHHMMSS" )=="20040606010101.1"     : die("FAILED YYYYMMDDHHMMSS.F test")
   if not from_J2000(139755661  ,"YYYYMMDD_HHMMSS")=="20040606_010101"      : die("FAILED YYYYMMDD_HHMMSS test")
   if not from_J2000(139755661.1,"YYYYMMDD_HHMMSS")=="20040606_010101.1"    : die("FAILED YYYYMMDD_HHMMSS.F test")
   if not from_J2000(139755661  ,"YYMMDD_HHMMSS"  )=="040606_010101"        : die("FAILED YYMMDD_HHMMSS test")
   if not from_J2000(139755661.1,"YYMMDD_HHMMSS"  )=="040606_010101.1"      : die("FAILED YYMMDD_HHMMSS.F test")
   if not from_J2000(139755661  ,"YYMMDDHHMMSS"   )=="040606010101"         : die("FAILED YYMMDDHHMMSS test")
   if not from_J2000(139755661.1,"YYMMDDHHMMSS"   )=="040606010101.1"       : die("FAILED YYMMDDHHMMSS.F test")
   if not from_J2000(732.1      ,"HHMMSS"         )=="121212.1"             : die("FAILED HHMMSS.F test")
   if not from_J2000(139752000.1,"J2000"          )==139752000.1            : die("FAILED J2000 test")
# (1,1.1) == (1,1.1000000001) ?!
#   if not from_J2000(139755661.1,"LIST"           )==(2004,6,6,1,1,1.1)     : die("FAILED LIST test")
   if not from_J2000(8769600    ,"DOY"            )=="103"                  : die("FAILED DOY test")
   if not from_J2000(8769600    ,"YYDOY"          )=="00103"                : die("FAILED YYDOY test")
   if not from_J2000(139755661.1,"GAIMSTRING"     )=="2004/06/06,01:01:01.1": die("FAILED GAIMSTRING test")
   if not from_J2000(139755661.1,"TENETHOURLY",'help')=="help158b01.04.tenet": die("FAILED TENETHOURLY test")
   print "Passed from_J2000"

   print "Testing IonoTime"
   if not IonoTime(0)+"a"  =="20000101_120000a"     : die("FAILED string coersion test")
   if not IonoTime(0)+1.0  ==1                      : die("FAILED integer coersion test")
   if not IonoTime(0)+[1,2]==[2000,1,1,12,0,0,1,2]  : die("FAILED list coersion test")
   if not IonoTime(0).addDays(2).addHours(2).addMinutes(2).addSeconds(2) == ((2*24+2)*60+2)*60+2: die("FAILED deltatime test")
   if not IonoTime(10)     == IonoTime(10)     : die("FAILED equivalence test")
   if not IonoTime(12) - IonoTime(10) == 2     : die("FAILED subtraction test")
   if not IonoTime(12) + IonoTime(10) == 22    : die("FAILED addition test")
   if not IonoTime(12).makemidnight().to('LOCALHMS',140) == "090000" : die("FAILED Midnight or LOCALHMS test")
   if not IonoTime(6576).floor('day').to('YYYYMMDDHHMMSS') == "20000101000000": die("FAILED floor test")
   print "Passed IonoTime"


def main(args):
    test()

if __name__ == "__main__":
  main(sys.argv[1:])
