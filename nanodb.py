# -*- coding: utf-8 -*-
"""
Created on Thu May 28 11:59:27 2015

@author: eric
"""

import sqlite3
import json
import sys
import datetime

class ConnectDB:
    
    db_details = {
        'name':'nanodb',
        'prefix':'nano_',
        'type':'SQLite'
    }
    debug = True
    
    '''
    Print Messages
    '''
    def debug(self,text):
        if self.debug:
            print text
            
    '''
    Connect to the database
    '''
    def connect(self):
        try:
            conn = None
            if 'SQLite' == self.db_details['type']:
                conn = sqlite3.connect(self.db_details['name']+'.sqlite')
                return [True,conn]
        except:    
            pass
  
        return [False,None]
    
    def getType(self,indi_type):
        # Interpret indi_type=0 as signed char
        # Ref: https://docs.python.org/2/library/struct.html
        c_type_options = ['b','?','c','b','B','h','H','i','I','q','Q','f','d']
        c_type_details = {
            'b' : {
                'c_type' : 'signed char',
                'python_type' : 'integer',
                'standard_size' : 1
            },
            '?' : {
                'c_type' : '_Bool',
                'python_type' : 'bool',
                'standard_size' : 1
            },
            'c' : {
                'c_type' : 'char',
                'python_type' : 'string',
                'standard_size' : 1
            },
            's' : {
                'c_type' : 'char[]',
                'python_type' : 'string',
                'standard_size' : 1
            },
            'B' : {
                'c_type' : 'unsigned char',
                'python_type' : 'integer',
                'standard_size' : 1
            },
            'h' : {
                'c_type' : 'short',
                'python_type' : 'integer',
                'standard_size' : 2
            },
            'H' : {
                'c_type' : 'unsigned short',
                'python_type' : 'integer',
                'standard_size' : 2
            },
            'i' : {
                'c_type' : 'int',
                'python_type' : 'integer',
                'standard_size' : 4
            },
            'I' : {
                'c_type' : 'unsigned int',
                'python_type' : 'integer',
                'standard_size' : 4
            },
            'f' : {
                'c_type' : 'float',
                'python_type' : 'float',
                'standard_size' : 4
            },
            'q' : {
                'c_type' : 'long long',
                'python_type' : 'integer',
                'standard_size' : 8
            },
            'Q' : {
                'c_type' : 'unsigned long long',
                'python_type' : 'integer',
                'standard_size' : 8
            },
            'd' : {
                'c_type' : 'double',
                'python_type' : 'float',
                'standard_size' : 8
            }
        }

        return c_type_details[c_type_options[indi_type]]['python_type']


    '''
    Create Acount, Object, and/or Stream
    '''
    def create(self,network_id,create_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        response = {}
        try:            
            # Check network_id
            if not isinstance(network_id,basestring):
                self.debug( "network_id Should Be String" )
                response["msg"] = "network_id Should Be String"
                return response
            if "network_id" not in create_details:
                create_details["network_id"] = network_id
                
            # Check if network exists
            network_exists, network_details = self.networkExists(network_id)
            if not network_exists:
                self.debug( "Create Network: "+network_id )
                # Check that "network_id" and "network_details" are in create_details
                if "network_id" not in create_details or "network_details" not in create_details:
                    self.debug( "Incomplete Create Details. Missing network_id and/or network_details" )
                    response["msg"] = "Incomplete Create Details. Missing network_id and/or network_details"
                    return response
                
                # Add empty dictionary, if necessary
                if 'objects' not in create_details or not isinstance(create_details['objects'],dict):
                    create_details['objects'] = {} # Empty object dict
                    
                # Create network
                created = self.createNetwork(network_id,create_details,unixtime)
                
                # Something went wrong
                if not created:
                    self.debug( "Network "+network_id+" Not Created" )
                    response["msg"] = "Network "+network_id+" Not Created"
                    return response
            
                # Set new info as network info
                network_details = create_details

            elif 'objects' not in create_details or len(create_details['objects']) == 0:
                # Unnecessary network create request
                self.debug( "Network "+network_id+" Already Exists" )
                response["msg"] = "Network "+network_id+" Already Exists"
                return response

            # Check for objects
            if 'objects' in create_details and len(create_details['objects']) > 0:
                response['objects'] = {}
                for object_id in create_details['objects']:
                    response['objects'][object_id] = {}
                    # Check if object exists
                    object_exists, object_details = self.objectExists(network_id,object_id,network_details)
                    if not object_exists:               
                        self.debug( "Create Object: "+object_id )
                        # Check that "object_id" and "object_details" are in create_details
                        create_details['objects'][object_id]['object_id'] = object_id
                        if "object_details" not in create_details['objects'][object_id]:
                            self.debug( "Incomplete Create Details for Object "+object_id )
                            response['objects'][object_id]["msg"] = "Incomplete Create Details for Object "+object_id
                            return response

                        # Add empty dictionary, if necessary
                        if 'streams' not in create_details['objects'][object_id] or not isinstance(create_details['objects'][object_id]['streams'],dict):
                            create_details['objects'][object_id]['streams'] = {} # Empty object dict

                        created = self.createObject(network_id,object_id,create_details['objects'][object_id],unixtime)

                        # Something went wrong
                        if not created:
                            self.debug( "Object "+object_id+" Not Created" )
                            response['objects'][object_id]["msg"] = "Object "+object_id+" Not Created"
                            return response
                        else:
                            self.debug( "Object "+object_id+" Created" )
                            response['objects'][object_id]["msg"] = "Object "+object_id+" Created"

                    elif 'streams' not in create_details['objects'][object_id] or len(create_details['objects'][object_id]['streams']) == 0:
                        # Unnecessary object create request
                        self.debug( "Object "+object_id+" Already Exists" )
                        response['objects'][object_id]["msg"] = "Object "+object_id+" Already Exists"
                        
                    # Check for streams
                    if 'streams' in create_details['objects'][object_id] and len(create_details['objects'][object_id]['streams']) > 0:
                        response['objects'][object_id]['streams'] = {}
                        for stream_id in create_details['objects'][object_id]['streams']:
                            response['objects'][object_id]['streams'][stream_id] = {}
                            # Check if stream exists
                            stream_exists, stream_details = self.streamExists(network_id,object_id,stream_id,network_details)
                            if not stream_exists:               
                                self.debug( "Create Stream: "+stream_id )
                                # Check that "stream_id" and "stream_details" are in create_details
                                create_details['objects'][object_id]['streams'][stream_id]['stream_id'] = stream_id
                                if "stream_details" not in create_details['objects'][object_id]['streams'][stream_id]:
                                    self.debug( "Incomplete Create Details for Stream "+stream_id )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Incomplete Create Details for Stream "+stream_id
                                    return response
                                if "indi_details" not in create_details['objects'][object_id]['streams'][stream_id]['stream_details']:
                                    self.debug( "Incomplete INDI Details for Stream "+stream_id )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Incomplete INDI Details for Stream "+stream_id
                                    return response
                                indi = create_details['objects'][object_id]['streams'][stream_id]['stream_details']['indi_details']
                                if not all(k in indi for k in ("stream_type","data_type","data_length","data_unit")):
                                    self.debug( "Incomplete INDI Details for Stream "+stream_id )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Incomplete INDI Details for Stream "+stream_id
                                    return response
                                    
                                created = self.createStream(network_id,object_id,stream_id,create_details['objects'][object_id]['streams'][stream_id],unixtime)
                            
                                # Something went wrong
                                if not created:
                                    self.debug( "Stream "+stream_id+" Not Created" )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Not Created"
                                    return response
                                else:
                                    self.debug( "Stream "+stream_id+" Created" )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Created"
                                    
                            else:
                                self.debug( "Stream "+stream_id+" Exists" )
                                response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Already Exists"
                                return response
        except:
            response["msg"] = "Error Occured"
            response["error"] = True
            self.debug( "Create Error" )
            
        return response

    '''
    Read Stream(s). 
    '''
    def read(self,network_id,read_details):
            
        response = {}
        try:            
            # Check network_id
            if not isinstance(network_id,basestring):
                self.debug( "network_id Should Be String" )
                response["msg"] = "network_id Should Be String"
                return response
            response["network_id"] = network_id
                
            # Check if network exists
            network_exists, network_details = self.networkExists(network_id)
            if not network_exists:                
                self.debug( "Read Failed. Network "+network_id+" Not Found" )
                response["msg"] = "Read Failed. Network "+network_id+" Not Found."
                return response

            # Read network_details
            if "network_details" in read_details:
                response['network_details'] = network_details['network_details']
                self.debug( "Network "+network_id+" Details Read" )
                response["msg"] = "Network "+network_id+" Details Read"
              
            # Done reading
            if 'objects' not in read_details or len(read_details['objects']) == 0:
                pass

            # Check for objects
            if 'objects' in read_details and len(read_details['objects']) > 0:
                response['objects'] = {}
                for object_id in read_details['objects']:
                    response['objects'][object_id] = {}
                    
                    # Check if object exists
                    object_exists, object_details = self.objectExists(network_id,object_id,network_details)
                    if not object_exists:
                        self.debug( "Object "+object_id+" Not Found" )
                        response['objects'][object_id]["msg"] = "Object "+object_id+" Not Found."
                    else:
                        self.debug( "Read Object: "+object_id )
                        
                        # Check if "object_id" and "object_details" are in read_details
                        response['objects'][object_id]['object_id'] = object_id
                        if "object_details" in read_details['objects'][object_id]:
                            # Read object_details
                            response['objects'][object_id]["object_details"] = network_details['objects'][object_id]["object_details"]
                            self.debug( "Object "+object_id+" Details Read" )
                            response['objects'][object_id]["msg"] = "Object "+object_id+" Details Read"

                        # Done reading
                        if 'streams' not in read_details['objects'][object_id] or len(read_details['objects'][object_id]['streams']) == 0:
                            pass                   

                        # Check for streams
                        if 'streams' in read_details['objects'][object_id] and len(read_details['objects'][object_id]['streams']) > 0:
                            response['objects'][object_id]['streams'] = {}
                            for stream_id in read_details['objects'][object_id]['streams']:
                                response['objects'][object_id]['streams'][stream_id] = {}
                                
                                # Check if stream exists
                                stream_exists, stream_details = self.streamExists(network_id,object_id,stream_id,network_details)
                                if not stream_exists:               
                                    self.debug( "Stream "+stream_id+" Not Found" )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Not Found."
                                else:
                                    # Check that "stream_id" and "stream_details" are in read_details
                                    response['objects'][object_id]['streams'][stream_id]['stream_id'] = stream_id
                                    if "stream_details" in read_details['objects'][object_id]['streams'][stream_id]:
                                        # Read stream_details
                                        response['objects'][object_id]['streams'][stream_id]['stream_details'] = network_details['objects'][object_id]['streams'][stream_id]['stream_details']
                                        self.debug( "Stream "+stream_id+" Details Read" )
                                        response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Details Read"

                                    # Done reading
                                    if 'points' not in read_details['objects'][object_id]['streams'][stream_id]:
                                        pass
                                    
                                    else:
                                        points = []
                                        read_stream_details = read_details['objects'][object_id]['streams'][stream_id]

                                        if any (k in read_stream_details for k in ("start","end","limit")):
                                            # Get records from stream db
                                            read, points = self.readStream(network_id,object_id,stream_id,read_stream_details)
                                        elif 'points' in network_details['objects'][object_id]['streams'][stream_id]:
                                            # Get recent records from network info
                                            points = network_details['objects'][object_id]['streams'][stream_id]['points']                            
                                    
                                        response['objects'][object_id]['streams'][stream_id]['points'] = points
                                        
                                        if len(points) > 1 and isinstance(points[0]['value'],(int,long,float)):
                                            min_val = points[0]['value']
                                            max_val = points[0]['value']
                                            for i in range(1,len(points)):
                                                if points[i]['value'] > max_val:
                                                    max_val = points[i]['value']
                                                elif points[i]['value'] < min_val:
                                                    min_val = points[i]['value']
                                            response['objects'][object_id]['streams'][stream_id]['min_value'] = min_val
                                            response['objects'][object_id]['streams'][stream_id]['max_value'] = max_val

        except:
            response["msg"] = "Error Occured"
            response["error"] = True
            self.debug( "Read Error" )
            
        return response
        

        
    '''
    Update Stream(s)
    '''
    def update(self,network_id,update_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        response = {}
        try:            
            # Check network_id
            if not isinstance(network_id,basestring):
                self.debug( "network_id Should Be String" )
                response["msg"] = "network_id Should Be String"
                return response
            if "network_id" not in update_details:
                update_details["network_id"] = network_id
                
            # Check if network exists
            network_exists, network_details = self.networkExists(network_id)
            if not network_exists:                
                self.debug( "Update Failed. Network "+network_id+" Not Found" )
                response["msg"] = "Update Failed. Network "+network_id+" Not Found."
                return response

            # Update network_details
            if "network_details" in update_details:
                network_details['network_details'] = update_details['network_details']
                self.debug( "Network "+network_id+" Details Updated" )
                response["msg"] = "Network "+network_id+" Details Updated"
              
            # Done updating
            if 'objects' not in update_details or len(update_details['objects']) == 0:
                if "network_details" in update_details:
                    updated = self.updateNetwork(network_id,network_details,unixtime)
                    if not updated:
                        response["msg"] = "Error Updating"
                        response["error"] = True
                else:
                    self.debug( "Network "+network_id+" Not Updated" )
                    response["msg"] = "Network "+network_id+" Not Updated"

            # Check for objects
            if 'objects' in update_details and len(update_details['objects']) > 0:
                response['objects'] = {}
                for object_id in update_details['objects']:
                    response['objects'][object_id] = {}
                    
                    # Check if object exists
                    object_exists, object_details = self.objectExists(network_id,object_id,network_details)
                    if not object_exists:
                        self.debug( "Object "+object_id+" Not Found" )
                        response['objects'][object_id]["msg"] = "Object "+object_id+" Not Found."
                    else:
                        self.debug( "Update Object: "+object_id )
                        
                        # Check that "object_id" and "object_details" are in update_details
                        update_details['objects'][object_id]['object_id'] = object_id
                        if "object_details" in update_details['objects'][object_id]:
                            # Update object_details
                            network_details['objects'][object_id]["object_details"] = update_details['objects'][object_id]["object_details"]
                            self.debug( "Object "+object_id+" Details Updated" )
                            response['objects'][object_id]["msg"] = "Object "+object_id+" Details Updated"

                        # Done updating
                        if 'streams' not in update_details['objects'][object_id] or len(update_details['objects'][object_id]['streams']) == 0:
                            if "object_details" in update_details['objects'][object_id]:
                                updated = self.updateNetwork(network_id,network_details,unixtime)
                                if not updated:
                                    response['objects'][object_id]["msg"] = "Error Updating"
                                    response['objects'][object_id]["error"] = True
                            else:
                                self.debug( "Object "+object_id+" Not Updated" )
                                response['objects'][object_id]["msg"] = "Object "+object_id+" Not Updated"                      

                        # Check for streams
                        if 'streams' in update_details['objects'][object_id] and len(update_details['objects'][object_id]['streams']) > 0:
                            response['objects'][object_id]['streams'] = {}
                            for stream_id in update_details['objects'][object_id]['streams']:
                                response['objects'][object_id]['streams'][stream_id] = {}
                                
                                # Check if stream exists
                                stream_exists, stream_details = self.streamExists(network_id,object_id,stream_id,network_details)
                                if not stream_exists:               
                                    self.debug( "Stream "+stream_id+" Not Found" )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Not Found."
                                else:
                                    # Check that "stream_id" and "stream_details" are in update_details
                                    update_details['objects'][object_id]['streams'][stream_id]['stream_id'] = stream_id
                                    new_stream_details = None
                                    if "stream_details" in update_details['objects'][object_id]['streams'][stream_id]:
                                        # Update stream_details
                                        new_stream_details = update_details['objects'][object_id]['streams'][stream_id]['stream_details']
                                        if "indi_details" in new_stream_details:
                                            new_stream_details = None
                                            self.debug( "Stream "+stream_id+" INDI Details Cannot Be Updated. No Changes Made To Stream Details." )
                                            response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" INDI Details Cannot Be Updated. No Changes Made To Stream Details."
                                        else:
                                            network_details['objects'][object_id]['streams'][stream_id]["stream_details"] = new_stream_details
                                            self.debug( "Stream "+stream_id+" Details Updated" )
                                            response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Details Updated"

                                    # Done updating
                                    if 'points' not in update_details['objects'][object_id]['streams'][stream_id] or len(update_details['objects'][object_id]['streams'][stream_id]['points']) == 0:
                                        if new_stream_details is not None:
                                            updated = self.updateNetwork(network_id,network_details,unixtime)
                                            if not updated:
                                                response['objects'][object_id]['streams'][stream_id]["msg"] = "Error Updating"
                                                response['objects'][object_id]['streams'][stream_id]["error"] = True
                                        else:
                                            self.debug( "Stream "+stream_id+" Not Updated" )
                                            response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Not Updated"

                                    else:
                                        # Update Points
                                        points = update_details['objects'][object_id]['streams'][stream_id]['points']
                                        indi_details = stream_details['indi_details']
    
                                        if len(points) > 0:
                                            new_points = []
                                            for point in points:
                                                if 'value' in point:
                                                    # Assume the value is the correct type.
                                                    # TODO: Add check
                                                    at = unixtime
                                                    if 'at' in point and isinstance(point['at'],(int,long)):
                                                        at = point['at']
                                                    new_points.append( {'at':at,'value':point['value']} )
    
                                            updated = self.updateStream(network_id,object_id,stream_id,new_points,indi_details)
    
                                            if updated:
                                                response['objects'][object_id]['streams'][stream_id]['msg'] = "Stream updated"
    
                                                if 'points' in network_details['objects'][object_id]['streams'][stream_id]:
                                                    all_points = network_details['objects'][object_id]['streams'][stream_id]['points'] + new_points
                                                    all_points = sorted(all_points, key=lambda k: k['at'],reverse=True) 
                                                    if len(all_points) > 5:
                                                        all_points = all_points[:5]
                                                    network_details['objects'][object_id]['streams'][stream_id]['points'] = all_points
                                                else:
                                                    new_points = sorted(new_points, key=lambda k: k['at'],reverse=True)
                                                    if len(new_points) > 5:
                                                        new_points = new_points[:5]
                                                    network_details['objects'][object_id]['streams'][stream_id]['points'] = new_points
                                                    
                                                # Update Network record with most recent points.
                                                updated = self.updateNetwork(network_id,network_details,unixtime)
                                            

        except:
            response["msg"] = "Error Occured"
            response["error"] = True
            self.debug( "Update Error" )
            
        return response


    '''
    Delete Accout, Object, and/or Streams. 
    '''
    def delete(self,network_id,delete_details):
            
        response = {}
        try:                
            # Check if network exists
            network_exists, network_details = self.networkExists(network_id)
            if not network_exists:                
                self.debug( "Delete Failed. Network "+network_id+" Not Found" )
                response["msg"] = "Delete Failed. Network "+network_id+" Not Found."
                return response
             
            response['network_id'] = network_id
            # Done deleting
            if 'objects' not in delete_details or len(delete_details['objects']) == 0:
                # Delete Network
                if 'objects' in network_details['objects']:
                    for object_id in network_details['objects']:
                        if 'streams' in network_details['objects'][object_id]['streams']:
                            for stream_id in network_details['objects'][object_id]['streams']:
                                deleted = self.deleteStream(network_id,object_id,stream_id)
                deleted = self.deleteNetwork(network_id)
                if deleted:
                    response["msg"] = "Network Deleted"

            # Check for objects
            if 'objects' in delete_details and len(delete_details['objects']) > 0:
                response['objects'] = {}
                for object_id in delete_details['objects']:
                    response['objects'][object_id] = {}
                    
                    # Check if object exists
                    object_exists, object_details = self.objectExists(network_id,object_id,network_details)
                    if not object_exists:
                        self.debug( "Object "+object_id+" Not Found" )
                        response['objects'][object_id]["msg"] = "Object "+object_id+" Not Found."
                    else:                        
                        response['objects'][object_id]['object_id'] = object_id

                        # Done deleting
                        if 'streams' not in delete_details['objects'][object_id] or len(delete_details['objects'][object_id]['streams']) == 0:
                            if 'streams' in network_details['objects'][object_id]['streams']:
                                for stream_id in network_details['objects'][object_id]['streams']:
                                    self.deleteStream(network_id,object_id,stream_id)
                            del(network_details['objects'][object_id])
                            updated = self.updateNetwork(network_id,network_details)                   
                            if updated:
                                response['objects'][object_id]["msg"] = "Object Deleted"
                    
                        # Check for streams
                        if 'streams' in delete_details['objects'][object_id] and len(delete_details['objects'][object_id]['streams']) > 0:
                            response['objects'][object_id]['streams'] = {}
                            for stream_id in delete_details['objects'][object_id]['streams']:
                                response['objects'][object_id]['streams'][stream_id] = {}
                                
                                # Check if stream exists
                                stream_exists, stream_details = self.streamExists(network_id,object_id,stream_id,network_details)
                                if not stream_exists:               
                                    self.debug( "Stream "+stream_id+" Not Found" )
                                    response['objects'][object_id]['streams'][stream_id]["msg"] = "Stream "+stream_id+" Not Found."
                                else:
                                    response['objects'][object_id]['streams'][stream_id]['stream_id'] = stream_id

                                    deleted = self.deleteStream(network_id,object_id,stream_id)
                                    if deleted:
                                        del(network_details['objects'][object_id]['streams'][stream_id])
                                        updated = self.updateNetwork(network_id,network_details)                   
                                        if updated:
                                            response['objects'][object_id]['streams'][stream_id]["msg"] = "Object Deleted"

                                            

        except:
            response["msg"] = "Error Occured"
            response["error"] = True
            self.debug( "Read Error" )
            
        return response
        
    '''
    Create network. Network must not already exist. Assumes network info well formatted.
    '''
    def createNetwork(self,network_id,create_network_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        created = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                # Create database table
                self.debug( "Create Network" )
                query = 'CREATE TABLE '+self.db_details['prefix']+network_id
                query += '(unixtime integer,registry text)'                  
                c.execute(query)
                
                query = 'INSERT INTO '+self.db_details['prefix']+network_id+' '
                query += '(unixtime,registry) VALUES ('+str(unixtime)+',\''+json.dumps( create_network_details )+'\')'
                c.execute(query)
                conn.commit()
                created = True
                
            except sqlite3.OperationalError:
                self.debug( "Network Not Created" )
                conn.rollback()
                
            except:
                self.debug( "Network Not Created" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return created
        
    '''
    Create object. Object must not already exist. Network must exist. Assume object info well formatted.
    '''
    def createObject(self,network_id,object_id,create_object_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        created = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                # Create database table
                query = 'SELECT * FROM '+self.db_details['prefix']+network_id
                c.execute(query)
                self.debug( "Network Found" )

                # Get most recent network information
                contents = c.fetchall()
                network_details = json.loads( contents[-1][1] )          
                
                if object_id in network_details['objects']:
                    self.debug( "Object "+object_id+" Found" )
                else:
                    network_details['objects'][object_id] = create_object_details
                
                    query = 'UPDATE '+self.db_details['prefix']+network_details['network_id']+' SET '
                    query += 'unixtime='+str(unixtime)+','
                    query += 'registry=\''+json.dumps( network_details )+'\''
                    c.execute(query)
                    conn.commit()
                    created = True
                    
            except sqlite3.OperationalError:
                self.debug( "Object Not Created" )
                conn.rollback()
                
            except:
                self.debug( "Object Not Created" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return created
        
    '''
    Create stream and stream db. Stream must not already exist. Network and Object must exist. Assume stream info well formated.
    '''
    def createStream(self,network_id,object_id,stream_id,create_stream_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        created = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                # Create database table
                query = 'SELECT * FROM '+self.db_details['prefix']+network_id
                c.execute(query)
                self.debug( "Network Found" )

                # Get most recent network information
                contents = c.fetchall()
                network_details = json.loads( contents[-1][1] )          
                
                if object_id not in network_details['objects']:
                    self.debug( "Object "+object_id+" Not Found" )
                else:
                    self.debug( "Object "+object_id+" Found" )
                                            
                    try:
                        #Create Table
                        query = 'CREATE TABLE '+self.db_details['prefix']+network_id+'_'+object_id+'_'+stream_id+' '
                        query += '(unixtime integer'
                        
                        indi_details = create_stream_details['stream_details']['indi_details']
                        python_type = self.getType( indi_details['data_type']  )
                        # Check the data type
                        if 0 == indi_details['data_length']:
                            if python_type == 'string':
                                query += ', value text'
                            elif python_type == 'integer':
                                query += ', value integer'
                            elif python_type == 'float':
                                query += ', value real'
                            elif python_type == 'bool':
                                query += ', value integer'
                        else:
                            if python_type == 'string':
                                query += ', value text'
                            elif python_type == 'integer':
                                for i in range(indi_details['data_length']):
                                    query += ', value'+str(i)+' integer'
                            elif python_type == 'float':
                                for i in range(indi_details['data_length']):
                                    query += ', value'+str(i)+' real'
                            elif python_type == 'bool':
                                for i in range(indi_details['data_length']):
                                    query += ', value'+str(i)+' integer'
                        query = query + ')'
  
                        c.execute(query)
                        #conn.commit()
                        
                        network_details['objects'][object_id]['streams'][stream_id] = create_stream_details
                        
                        query = 'UPDATE '+self.db_details['prefix']+network_details['network_id']+' SET '
                        query += 'unixtime='+str(unixtime)+','
                        query += 'registry=\''+json.dumps( network_details )+'\''
                        c.execute(query)
                        conn.commit()
                        created = True
                
                    except sqlite3.OperationalError:
                        self.debug( "Stream Not Created" )
                        conn.rollback()
                
                    except:
                        # There was an error.
                        self.debug( "Unexpected error (0):"+str(sys.exc_details()[0]) )
                
            except:
                self.debug( "Stream Not Created" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return created
                
    '''
    Read stream. Assumes indi info and points well formatted.
    '''
    def readStream(self,network_id,object_id,stream_id,read_stream_details):
        
        read = False
        points = []
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:

                # Get historic data
                query = 'SELECT unixtime,value FROM '+self.db_details['prefix']+network_id+'_'+object_id+'_'+stream_id+' '
                        
                if 'start' in read_stream_details and 'end' in read_stream_details:
                    query += 'WHERE unixtime >= '+str(read_stream_details['start'])+' AND unixtime <= '+str(read_stream_details['end'])
                elif 'start' in read_stream_details:
                    query += 'WHERE unixtime >= '+str(read_stream_details['start'])
                elif 'end' in read_stream_details:
                    query += 'WHERE unixtime <= '+str(read_stream_details['end'])
                
                limit = 100
                if 'limit' in read_stream_details:
                    if read_stream_details['limit'] < 1000:
                        limit = read_stream_details['limit'] 
                    else:
                        limit = 1000
                        
                query += ' ORDER BY unixtime DESC LIMIT '+str(limit)

                c.execute(query)
                contents = c.fetchall()
                
                for point in contents:
                    points.append({'at':point[0],'value':point[1]})                          
                
                read = True
                    
            except sqlite3.OperationalError:
                self.debug( "SQL Error" )
                conn.rollback()
                    
            except:
                self.debug( "Stream Not Read" )
                    
            conn.close()
                
        return read, points
        
    '''
    Update network. Assumes network info well formatted.
    '''
    def updateNetwork(self,network_id,update_network_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        updated = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                # Update database table                
                query = 'UPDATE '+self.db_details['prefix']+network_id+' SET '
                query += 'unixtime='+str(unixtime)+','
                query += 'registry=\''+json.dumps( update_network_details )+'\''
                c.execute(query)
                conn.commit()
                updated = True
                
            except sqlite3.OperationalError:
                self.debug( "Network Not Updated" )
                conn.rollback()
                
            except:
                self.debug( "Network Not Updated" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return updated
        
    '''
    Update stream. Assumes indi info and points well formatted.
    '''
    def updateStream(self,network_id,object_id,stream_id,points,indi_details,unixtime=None):
        if unixtime is None:
            unixtime = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
            
        updated = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                python_type = self.getType( indi_details['data_type']  )
                for point in points:                 
                    # Update database table                
                    at = unixtime
                    if 'at' in point and isinstance(point['at'],(int,long)):
                        at = point['at']
                    payload = point['value']
                    
                    query = 'INSERT INTO '+self.db_details['prefix']+network_id+'_'+object_id+'_'+stream_id+' '
                    
                    # Check the data type
                    if 0 == indi_details['data_length']:
                        if python_type == 'string':
                            query += "(unixtime,value) VALUES ("+str(at)+",'"+payload+"')"
                        elif python_type == 'integer':
                            query += "(unixtime,value) VALUES ("+str(at)+","+str(payload)+")"
                        elif python_type == 'float':
                            query += "(unixtime,value) VALUES ("+str(at)+","+str(payload)+")"
                        elif python_type == 'bool':
                            query += "(unixtime,value) VALUES ("+str(at)+","+str(int(payload))+")"
                    else:
                        if python_type == 'string':
                            query += "(unixtime,value) VALUES ("+str(at)+",'"+payload+"')"
                        elif python_type == 'integer':
                            ids = '(unixtime'
                            vals = '('+str(at)
                            for i in range(indi_details['data_length']):
                                ids += ',value'+str(i)
                                vals += ','+str(payload[i])
                            query += ids+') VALUES '+vals+')'
                        elif python_type == 'float':
                            ids = '(unixtime'
                            vals = '('+str(at)
                            for i in range(indi_details['data_length']):
                                ids += ',value'+str(i)
                                vals += ','+str(payload[i])
                            query += ids+') VALUES '+vals+')' 
                        elif python_type == 'bool':
                            ids = '(unixtime'
                            vals = '('+str(at)
                            for i in range(indi_details['data_length']):
                                ids += ',value'+str(i)
                                vals += ','+str(int(payload[i]))
                            query += ids+') VALUES '+vals+')'
                                                        
                    c.execute(query)
                            
                conn.commit()
                updated = True
                
            except sqlite3.OperationalError:
                self.debug( "Stream Not Updated" )
                conn.rollback()
                
            except:
                self.debug( "Stream Not Updated" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return updated

    '''
    Delete network. 
    '''
    def deleteNetwork(self,network_id):
            
        deleted = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                # Update database table                
                query = 'DROP TABLE '+self.db_details['prefix']+network_id
                c.execute(query)
                conn.commit()
                deleted = True
                
            except sqlite3.OperationalError:
                self.debug( "Network Not Deleted" )
                conn.rollback()
                
            except:
                self.debug( "Network Not Deleted" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return deleted
        
    '''
    Delete stream. 
    '''
    def deleteStream(self,network_id,object_id,stream_id):
            
        deleted = False
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:
                # Update database table                
                query = 'DROP TABLE '+self.db_details['prefix']+network_id+'_'+object_id+'_'+stream_id+' '
                c.execute(query)
                conn.commit()
                deleted = True
                
            except sqlite3.OperationalError:
                self.debug( "Stream DB Not Deleted" )
                conn.rollback()
                
            except:
                self.debug( "Stream DB Not Deleted" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
            
        return deleted
        
    '''
    Check if network exists. Return network info if True.    
    '''
    def networkExists(self,network_id):
        exists = False
        network_details = {}
        # Connect to database
        connected,conn = self.connect()
        # If connected...
        if connected:
            # Get cursor
            c = conn.cursor()
            try:

                # Try to query database
                query = 'SELECT * FROM '+self.db_details['prefix']+network_id
                c.execute(query)
                self.debug( "Network Found" )

                # Get most recent network information
                contents = c.fetchall()
                network_details = json.loads( contents[-1][1] )
                
                exists = True
                    
            except sqlite3.OperationalError:
                self.debug( "Network Not Found" )
                conn.rollback()
                                
            except:
                self.debug( "Network Not Found" )
                
            # We can also close the connection if we are done with it.
            # Just be sure any changes have been committed or they will be lost.
            conn.close()
              
        return exists, network_details
        
    '''
    Check if object exists. Return object info if True.    
    '''
    def objectExists(self,network_id,object_id,network_details=None):
        exists = False
        object_details = {}
        
        if network_details is None:
            # Connect to database
            connected,conn = self.connect()
            # If connected...
            if connected:
                # Get cursor
                c = conn.cursor()
                try:
    
                    # Try to query database
                    query = 'SELECT * FROM '+self.db_details['prefix']+network_id
                    c.execute(query)
                    self.debug( "Network Found" )
    
                    # Get most recent network information
                    contents = c.fetchall()
                    network_details = json.loads( contents[-1][1] )
                    
                except sqlite3.OperationalError:
                    self.debug( "Network Not Found" )
                    conn.rollback()
                                    
                except:
                    self.debug( "Network Not Found" )
                    
                # We can also close the connection if we are done with it.
                # Just be sure any changes have been committed or they will be lost.
                conn.close()
        
        try:
            object_details = network_details['objects'][object_id]
            exists = True
            self.debug( "Object Found" )
        except:
            self.debug( "Object Not Found" )
        
        return exists, object_details
        
    '''
    Check if stream exists and includes indi info. Return stream info if True.    
    '''
    def streamExists(self,network_id,object_id,stream_id,network_details=None):
        exists = False
        stream_details = {}
        
        if network_details is None:
            # Connect to database
            connected,conn = self.connect()
            # If connected...
            if connected:
                # Get cursor
                c = conn.cursor()
                try:
    
                    # Try to query database
                    query = 'SELECT * FROM '+self.db_details['prefix']+network_id
                    c.execute(query)
                    self.debug( "Network Found" )
    
                    # Get most recent network information
                    contents = c.fetchall()
                    network_details = json.loads( contents[-1][1] )
                    
                except sqlite3.OperationalError:
                    self.debug( "Network Not Found" )
                    conn.rollback()
                                    
                except:
                    self.debug( "Network Not Found" )
                    
                # We can also close the connection if we are done with it.
                # Just be sure any changes have been committed or they will be lost.
                conn.close()

        if network_details is None:
            return exists, stream_details
            
        try:
            db_exists = False
            # Connect to database
            connected,conn = self.connect()
            # If connected...
            if connected:
                # Get cursor
                c = conn.cursor()
                try:
                    # Try to query database
                    query = 'SELECT * FROM '+self.db_details['prefix']+network_id+'_'+object_id+'_'+stream_id
                    c.execute(query)
                    self.debug( "Stream DB Found" )
                    db_exists = True
                except sqlite3.OperationalError:
                    self.debug( "Stream DB Not Found" )
                    conn.rollback()
                    
            # Close database connection
            conn.close()
            
            if db_exists:
                # Check for indi info (Just try and catch failures)
                stream_details = network_details['objects'][object_id]['streams'][stream_id]['stream_details']
                indi = stream_details['indi_details']
                assert all(k in indi for k in ("stream_type","data_type","data_length","data_unit"))
                exists = True
                self.debug( "Stream Found" )
            else:
                self.debug( "Stream Not Found" )
            
        except:
            self.debug( "Stream Not Found" )
        
        return exists, stream_details
        
        
'''              

c = ConnectDB()
print c.create("ce186",{
  "network_id": "ce186", 
  'objects': {
    "fridge": {
      "object_id": "fridge", 
      'streams': {
        "temperature": {
          "stream_details":{
              "indi_details": {
                "data_length": 0, 
                "data_type": 5, 
                "data_unit": "none", 
                "stream_type": 1
              }
          },
          'points': [
            {
              "at": 4, 
              "value": 1.2
            }, 
            {
              "at": 5, 
              "value": 1.5
            }
          ],
        },
        "t2": {
          "stream_details":{
              "indi_details": {
                "data_length": 0, 
                "data_type": 5, 
                "data_unit": "none", 
                "stream_type": 1
              }
          },
          'points': [
            {
              "at": 4, 
              "value": 1.2
            }, 
            {
              "at": 5, 
              "value": 1.5
            }
          ], 
        }
      }
    },
    "heater2" : {
        "object_details":{
        
        }    
    }
  }
})
'''



'''
print
print c.create({'network_id':'test'})
print
print
print c.create({'network_id':'test','objects':{'WF':{'object_id':'WF'},'W2F':{'object_id':'W2F'}}})
print 
print
print c.create(
{
    'network_id':'test',
    'objects':
    {
        'WF': 
        {
            'object_id':'WF',
            'streams': 
            {
                '12': 
                    {
                        'stream_id':'12',
                        'indi_details':
                            {
                                'stream_type':1,
                                'data_type':11,
                                'data_length':0,
                                'data_unit':'degF'
                            }
                    },
            }
        },
        'WF3': 
        {
            'object_id':'WF3',
        }
    }
})
print
print
print c.read({'network_id':'test','objects':{'WF':{'object_id':'WF'},'W2F':{'object_id':'W2F'}}})
print
print
print c.update(
{
    'network_id':'test',
    'objects':
    {
        'WF': 
        {
            'object_id':'WF',
            'streams': 
            {
                '12': 
                    {
                        'stream_id':'12',
                        'points':
                            [{
                                'value':1.2,
                                'at':4
                            },
                            {
                                'value':1.5,
                                'at':5
                            }]
                    },
            }
        }
    }
})
print
print
print c.read({'network_id':'test','objects':{'WF':{'object_id':'WF'},'W2F':{'object_id':'W2F'}}})
print
print c.read({'network_id':'test','objects':{'WF':{'object_id':'WF','streams':{'12':{'stream_id':'12','start':0}}}}})

'''
'''
print c.createNetwork({'network_id':'test','name':'W'})
print c.readNetwork('b')
print c.readNetwork('test')
print c.updateNetwork({'network_id':'test','name':'WF'})
print c.createobject({'network_id':'test','objects':{'WF':{'object_id':'WF'},'W2F':{'object_id':'W2F'}}})
print c.readobject('test','W2F')
print c.deleteNetwork('test')
print c.deleteNetwork('b')

'''