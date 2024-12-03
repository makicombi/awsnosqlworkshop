from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key, Attr
import argparse
import time
from decimal import *
import random
from datetime import datetime, timedelta
import sys,os

def main():
    # args = sys.argv[1:]
    iURL = os.environ.get('INVOKE_URL') #args[0]
    recordcount = 0
    data = []
    finallist = {}
    nowtime = datetime.now().replace(microsecond=0)
    run_trips_curls = ''
    
    for x in range(1,17):
        tripidnum = "TCnr" + str(x + 501)

        startime = nowtime.replace(microsecond=0)
        sorty = "TRIP#" + startime.isoformat() + "Z#" + tripidnum

        printstrpost = "curl -fsSL -w '\\n' -X POST -d " + "\'{\"AssetID\": \"ASSET#S77818\", \"TripSK\": \"" + sorty +"\", \"UserID\": \"USER#1046\", \"StartTime\": \"" + startime.isoformat() +"Z" +"\", \"TripID\": \"" + tripidnum +"\"}\' " + iURL +"\n"
        
        stoptime = startime + timedelta(0,2)
        printstrput = "curl -fsSL -w '\\n'  -X PUT -d " + "\'{\"AssetID\": \"ASSET#S77818\", \"TripSK\": \"" + sorty +"\", \"UserID\": \"USER#1046\", \"StopTime\": \"" + stoptime.isoformat() +"Z" +  "\", \"Fare\": " + str(random.randint(1,30)) +", \"Miles\": " + str(round(random.uniform(1.5, 65.5), 2)) +"}\' " + iURL +"\n"
        
        if x == 1:
            with open('create-trip.sh', 'w') as fh_create:
                fh_create.write('#!/bin/bash \n## Start the first Trip - POST method\n')
                fh_create.write(printstrpost)
                fh_create.write("\n")
                fh_create.close()
                
            with open('update-trip.sh', 'w') as fh_update:
                fh_update.write('#!/bin/bash \n## End the first Trip - PUT method\n')                
                fh_update.write(printstrput)
                fh_update.write("\n")
                fh_update.close()
            
        else:
            run_trips_curls = run_trips_curls + printstrpost + printstrput

    nowtime = stoptime + timedelta(0,2)
    
    with open('run-trips.sh', 'w') as fout:
        fout.write('#!/bin/bash \n## Invoke API for remainder of the trips\n')
        fout.write(run_trips_curls)
        fout.close()

if __name__ == '__main__':
    main()