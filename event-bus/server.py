#!/usr/bin/env python3
"""TARS Event Bus v1.0.0 — Real-time WebSocket event streaming.
Connects to HA WebSocket, watches all state changes, routes significant
events to intelligence layer. Publishes to MQTT. Provides SSE stream.

Endpoints:
  GET /health, /events, /events/significant, /events/stream (SSE)
  GET /stats, /entities, /entity/<id>
"""
import os,json,time,logging,threading,re
from datetime import datetime
from collections import deque,Counter
from flask import Flask,jsonify,Response,request
import requests as http
import websocket

HA_URL=os.environ.get('HA_URL','http://localhost:8123')
HA_TOKEN=os.environ.get('HA_TOKEN','')
API_PORT=int(os.environ.get('API_PORT','8092'))
INTEL=os.environ.get('INTELLIGENCE_URL','http://localhost:8093')
MQTT_HOST=os.environ.get('MQTT_HOST','')
MQTT_PORT=int(os.environ.get('MQTT_PORT','1883'))
WATCHED=os.environ.get('WATCHED_DOMAINS','').split(',')
WS_URL=HA_URL.replace('http','ws')+'/api/websocket'

app=Flask(__name__)
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
logger=logging.getLogger('event-bus')

events=deque(maxlen=500)
counts=Counter()
states={}
ws_ok=False
ws_id=1
mqtt=None

def setup_mqtt():
    global mqtt
    if not MQTT_HOST: return
    try:
        import paho.mqtt.client as m
        mqtt=m.Client('tars_event_bus')
        mqtt.connect(MQTT_HOST,MQTT_PORT,60)
        mqtt.loop_start()
        logger.info(f'MQTT: {MQTT_HOST}:{MQTT_PORT}')
    except Exception as e: logger.error(f'MQTT: {e}')

def pub(topic,payload):
    if mqtt:
        try: mqtt.publish(f'tars/{topic}',json.dumps(payload))
        except: pass

def route(ev):
    eid=ev.get('entity_id','')
    new=ev.get('new_state','')
    old=ev.get('old_state','')
    dom=eid.split('.')[0]
    pub(f'events/{dom}/{eid}',{'state':new,'old':old})
    sig=False
    reason=''
    if eid=='binary_sensor.iphone_presence' and old!=new:
        sig=True; reason=f'Presence: {old}->{new}'
        if new=='on':
            try: http.post(f'{INTEL}/arrive',timeout=5)
            except: pass
    elif eid=='lock.front_door_lock' and old!=new: sig=True; reason=f'Lock: {old}->{new}'
    elif 'motion' in eid and new=='on' and old!='on': sig=True; reason=f'Motion: {eid}'
    elif eid=='media_player.living_room' and old!=new: sig=True; reason=f'Music: {old}->{new}'
    elif eid=='weather.forecast_home' and old!=new: sig=True; reason=f'Weather: {old}->{new}'
    elif 'vacuum' in eid and old!=new: sig=True; reason=f'Vacuum: {old}->{new}'
    elif 'temperature' in eid:
        try:
            if abs(float(new)-float(old))>3: sig=True; reason=f'Temp: {eid} {old}->{new}'
        except: pass
    if sig:
        ev['significant']=True; ev['reason']=reason
        counts['significant']+=1
        logger.info(f'SIG: {reason}')

def ws_thread():
    global ws_ok,ws_id
    def on_msg(ws,msg):
        global ws_id
        d=json.loads(msg)
        t=d.get('type','')
        if t=='auth_required': ws.send(json.dumps({'type':'auth','access_token':HA_TOKEN}))
        elif t=='auth_ok':
            logger.info('WS authenticated')
            ws.send(json.dumps({'id':ws_id,'type':'subscribe_events','event_type':'state_changed'}))
            ws_id+=1
        elif t=='event':
            ed=d.get('event',{}).get('data',{})
            eid=ed.get('entity_id','')
            ns=ed.get('new_state',{})
            os_=ed.get('old_state',{})
            dom=eid.split('.')[0] if eid else ''
            if dom not in WATCHED: return
            nv=ns.get('state','') if ns else ''
            ov=os_.get('state','') if os_ else ''
            if nv==ov: return
            ev={'entity_id':eid,'domain':dom,'old_state':ov,'new_state':nv,'time':datetime.now().isoformat(),'significant':False}
            events.append(ev); counts[dom]+=1; counts['total']+=1; states[eid]=nv
            route(ev)
    def on_err(ws,e): global ws_ok; ws_ok=False; logger.error(f'WS: {e}')
    def on_close(ws,c,m): global ws_ok; ws_ok=False; logger.info(f'WS closed')
    def on_open(ws): global ws_ok; ws_ok=True; logger.info('WS connected')
    while True:
        try:
            ws=websocket.WebSocketApp(WS_URL,on_message=on_msg,on_error=on_err,on_close=on_close,on_open=on_open)
            ws.run_forever(ping_interval=30,ping_timeout=10)
        except: pass
        logger.info('WS reconnecting in 10s...')
        time.sleep(10)

@app.route('/')
def index(): return jsonify({'name':'TARS Event Bus','version':'1.0.0','connected':ws_ok,'total':counts.get('total',0),'significant':counts.get('significant',0)})
@app.route('/health')
def health(): return jsonify({'status':'ok' if ws_ok else 'disconnected','websocket':ws_ok,'total':counts.get('total',0),'mqtt':mqtt is not None})
@app.route('/events')
def get_events():
    lim=request.args.get('limit',50,type=int)
    return jsonify(list(events)[-lim:])
@app.route('/events/significant')
def sig_events(): return jsonify([e for e in events if e.get('significant')][-20:])
@app.route('/events/stream')
def stream():
    def gen():
        idx=len(events)
        while True:
            cur=len(events)
            if cur>idx:
                for e in list(events)[idx:]: yield f'data: {json.dumps(e)}\n\n'
                idx=cur
            time.sleep(0.5)
    return Response(gen(),mimetype='text/event-stream')
@app.route('/stats')
def stats_(): return jsonify({'total':counts.get('total',0),'significant':counts.get('significant',0),'by_domain':dict(counts),'entities':len(states),'connected':ws_ok})
@app.route('/entities')
def ents(): return jsonify(states)
@app.route('/entity/<eid>')
def ent(eid):
    if eid in states: return jsonify({'entity':eid,'state':states[eid],'recent':[e for e in events if e.get('entity_id')==eid][-10:]})
    return jsonify({'error':'Not tracked'}),404

if __name__=='__main__':
    logger.info(f'TARS Event Bus v1.0.0 on :{API_PORT}')
    logger.info(f'WS: {WS_URL}')
    logger.info(f'Domains: {WATCHED}')
    setup_mqtt()
    threading.Thread(target=ws_thread,daemon=True).start()
    logger.info('WebSocket listener started')
    app.run(host='0.0.0.0',port=API_PORT,debug=False)
