# -*- coding: utf-8 -*-
"""
Demo for DBNanoServer


"""


import requests
import json
import numpy as np
import datetime
import matplotlib.pyplot as plt

def main():

    # Generate a sine and cosine wave
    Fs = 800
    f = 60
    sample = 50
    x = np.arange(sample)
    y_sin = np.sin(2 * np.pi * f * x / Fs)
    y_cos = np.cos(2 * np.pi * f * x / Fs)
    
    # Generature UNIX timestamps for each data point
    unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
    at = unixtime + x - sample
    print at
    
    # Plot the sine wave
    plt.plot(y_sin)
    plt.show()


    # 
    # Send the data to the server
    #

    # Set url address.
    base = 'http://127.0.0.1:5000/'
    # Set query (i.e. http://url.com/?key=value).
    query = {}
    # Set header.
    header = {'Content-Type':'application/json'}
    
    
    # First, send the sine wave
    endpoint = 'network/Demo/object/Waves/stream/Sine'
    payload = []
    for i in range(sample):
        payload.append( {'value':y_sin[i],'at':at[i]} )
    # Set body (also referred to as data or payload). Body is a JSON string.
    body = json.dumps(payload)
    # Form and send request. Set timeout to 2 minutes. Receive response.
    r = requests.request('post', base + endpoint, data=body, params=query, headers=header, timeout=120 )

    print r.url
    # Text is JSON string. Convert to Python dictionary/list
    print r.text
    #print json.loads( r.text )


    # Second, send the cosine wave
    endpoint = 'network/Demo/object/Waves/stream/Cosine'
    payload = []
    for i in range(sample):
        payload.append( {'value':y_cos[i],'at':at[i]} )
    body = json.dumps(payload)
    
    # Form and send request. Set timeout to 2 minutes. Receive response.
    r = requests.request('post', base + endpoint, data=body, params=query, headers=header, timeout=120 )

    print r.url
    # Text is JSON string. Convert to Python dictionary/list
    print r.text
    #print json.loads( r.text )

    
    # Third, read the data from the Cosine stream
    endpoint = 'network/Demo/object/Waves/stream/Cosine'
    address = base + endpoint
    query = {'limit':100}
    
    # Form and send request. Set timeout to 2 minutes. Receive response.
    r = requests.request('get', address, params=query, headers=header, timeout=120 )

    print r.url
    # Text is JSON string. Convert to Python dictionary/list
    print r.text
    #print json.loads( r.text )
    

main()