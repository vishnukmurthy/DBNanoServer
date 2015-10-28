# -*- coding: utf-8 -*-
"""
Demo for DBNanoServer


"""


import requests
import json
import numpy as np
import datetime
import time

def main():

    # Generate a sine and cosine wave
    Fs = 800
    f = 60
    sample = 50
    x = np.arange(sample)
    y_sin = np.sin(2 * np.pi * f * x / Fs)
    y_cos = np.cos(2 * np.pi * f * x / Fs)
    
    
    # 
    # Send the data to the server
    #
    # Set url address.
    base = 'http://127.0.0.1:5000/'
    # Set query (i.e. http://url.com/?key=value).
    query = {}
    # Set header.
    header = {'Content-Type':'application/json'}
    
    for i in range(sample):
        print "Send:",
        # Generature UNIX timestamps for each data point
        at = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
        
        # First, send the sine wave
        endpoint = 'network/Demo/object/Waves/stream/Sine'
        payload = [ {'value':y_sin[i],'at':at } ]
        print y_sin[i],",",
        # Set body (also referred to as data or payload). Body is a JSON string.
        body = json.dumps(payload)
        # Form and send request. Set timeout to 2 minutes. Receive response.
        r = requests.request('post', base + endpoint, data=body, params=query, headers=header, timeout=120 )

        # Second, send the cosine wave
        endpoint = 'network/Demo/object/Waves/stream/Cosine'
        payload = [ {'value':y_cos[i],'at':at } ]
        print y_cos[i]
        body = json.dumps(payload)
        
        # Form and send request. Set timeout to 2 minutes. Receive response.
        r = requests.request('post', base + endpoint, data=body, params=query, headers=header, timeout=120 )

    
        time.sleep(2)

    

main()