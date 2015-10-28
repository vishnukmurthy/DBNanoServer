from flask import Flask, request, jsonify, render_template, send_from_directory
from nanodb import ConnectDB

c = ConnectDB()

app = Flask(__name__)

# Routes
# Route index/dashboard html file
@app.route('/')
def root():
  return render_template('nanodashboard.html')
# Route static files
@app.route('/<path:filename>')
def send_file(filename):
    print filename
    return send_from_directory('static', filename)

# Route Network Requests
@app.route('/network/<network_id>', methods=['GET'])
def network(network_id):
    
    if request.method == 'GET':
        # Read Network Details
        network_exists, network_details = c.networkExists(network_id)
        if not network_exists:
            return jsonify(**{'message':'Network not found','code':1})
        
        # Read everything (read() should remove any private info from network_details)
        response = c.read(network_id,network_details)

        return jsonify(**response)

# Route Stream Requests
@app.route('/network/<network_id>/object/<objectid>/stream/<streamid>', methods=['GET','POST'])
def stream(network_id,objectid,streamid):    
    print 'Reached this point'    
    stream_request = {
        'network_id':network_id,
        'objects':
        {
            objectid: 
            {
                'streams': 
                {
                    streamid: 
                        {
                            'points': []
                        },
                }
            }
        }
    }
      
    if request.method == 'GET':
        # Read Stream Details
        response = {}
        # Max number of data points (Optional)
        limit = request.args.get('limit',None,type=int)
        # Start date/time (Optional)
        start = request.args.get('start',None,type=int)
        # End date/time (Optional)
        end = request.args.get('end',None,type=int)
        
        # Stream Details Input
        '''
        {
            'limit': 100,
            'start': <UNIXTIME>,
            'end': <UNIXTIME>
        }
        '''
        stream_exists, stream_details = c.streamExists(network_id,objectid,streamid)
        if not stream_exists:
            return jsonify(**{'message':'Stream not found','code':1})
        else:
            if limit is not None and isinstance(limit,int):
                stream_request['objects'][objectid]['streams'][streamid]['limit'] = limit
            if start is not None and isinstance(start,(long,int)):
                stream_request['objects'][objectid]['streams'][streamid]['start'] = start
            if end is not None and isinstance(end,(long,int)):
                stream_request['objects'][objectid]['streams'][streamid]['end'] = end
                
            response = c.read(network_id,stream_request)
        
        return jsonify(**response)
    
    elif request.method == 'POST':
        # Update Stream Details
        # Endpoint will automatically create network, object, and stream.
        # Endpoint will autopatically detect data type. 
        # Strings supported, but arrays not allowed.
        points = request.get_json()
        
        # Check format of points
        '''
        'points':
            [{
                'value':1.2,
                'at':4
            },
            {
                'value':1.5,
                'at':5
            }]
        '''
        if not isinstance(points, list) and not isinstance(points, dict):
            return jsonify(**{'message':'Payload must be a datapoint or a list of datapoints','code':1})
        if isinstance(points, dict):
            points = [points]
        for p in points:
            if 'value' not in p:
                return jsonify(**{'message':'Each datapoint in payload must have a \'value\' term','code':2})
            
        point_values = [d['value'] for d in points if 'value' in d]
        stream_exists, stream_details = c.streamExists(network_id,objectid,streamid)
        indi = None
        if not stream_exists:
            indi = {
                'stream_type':1,
                'data_type':11,
                'data_length':0,
                'data_unit':'none'
            }
            if not all(isinstance(n, (int,float,basestring)) for n in point_values):
                return jsonify(**{'message':'Data must be Integer, Float, or String','code':3})
            elif all(isinstance(n, int) for n in point_values):
                indi['data_type'] = 5
            elif all(isinstance(n, float) for n in point_values):
                indi['data_type'] = 11
            elif all(isinstance(n, basestring) for n in point_values):
                indi['data_type'] = 2
                indi['data_length'] = 140
            else:
                return jsonify(**{'message':'All points must have the same type','code':4})
            stream_request['network_details'] = {}
            stream_request['objects'][objectid]['object_details'] = {}
            stream_request['objects'][objectid]['streams'][streamid]['stream_details'] = {}
            stream_request['objects'][objectid]['streams'][streamid]['stream_details']['indi_details'] = indi
            create_response = c.create(network_id,stream_request)
            # Remove Indi before sending to update()
            del(stream_request['objects'][objectid]['streams'][streamid]['stream_details'])
            
            if "error" in create_response:
                return jsonify(**{'message':'Error creating stream','code':5})
                
        elif 'indi_details' in stream_details:
            indi = stream_details['indi_details']
            
        # Check if INDI info available
        if indi is None:
            return jsonify(**{'message':'Missing INDI Details','code':6})
            
        if indi['data_type'] == 5:
            for p in point_values:
                if not isinstance(p,int):
                    if isinstance(p,float):
                        return jsonify(**{'message':'Expected Integer, got Float','code':7})
                    elif isinstance(p,basestring):
                        return jsonify(**{'message':'Expected Integer, got String','code':7})
        elif indi['data_type'] == 11:
            for p in point_values:
                if not isinstance(p,float):
                    if isinstance(p,int):
                        return jsonify(**{'message':'Expected Float, got Integer','code':7})
                    elif isinstance(p,basestring):
                        return jsonify(**{'message':'Expected Float, got String','code':7})
        elif indi['data_type'] == 2:
            for p in point_values:
                if not isinstance(p,basestring):
                    if isinstance(p,float):
                        return jsonify(**{'message':'Expected String, got Float','code':7})
                    elif isinstance(p,int):
                        return jsonify(**{'message':'Expected String, got Integer','code':7})
        
        stream_request['objects'][objectid]['streams'][streamid]['points'] = points
        response = c.update(network_id,stream_request)
        
        return jsonify(**response)


@app.errorhandler(500)
def internal_error(error):

    return jsonify(**{'message':'An error occured. Sorry.','code':0})

@app.errorhandler(404)
def not_found(error):
    return jsonify(**{'message':'Not a valid endpoint','code':0})
    
    
if __name__ == '__main__':
    app.run()