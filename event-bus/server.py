#!/usr/bin/env python3
"""TARS Event Bus v2.0.0 — Real-time WebSocket event streaming.
Connects to HA WebSocket, watches all state changes, routes significant
events to intelligence layer. Publishes to MQTT. Provides SSE stream.

v2.0 additions:
  - Pattern detection: identify recurring event sequences
  - Anomaly flagging: events at unusual times = potential issues
  - Rate limiting: >10 events/min from same entity = malfunction alert
  - Event classification: routine/unusual/critical
  - Persistent pattern storage in /data/patterns.json

Endpoints:
  GET /health, /events, /events/significant, /events/stream (SSE)
  GET /stats, /entities, /entity/<id>
  GET /patterns — Learned event patterns
  GET /anomalies — Recent anomaly flags
  GET /rate-limits — Current rate limit status
"""
import os,json,time,logging,threading,re
from datetime import datetime,timedelta
from collections import deque,Counter,defaultdict
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

# v2.0: Pattern detection
PATTERN_FILE='/data/patterns.json'
patterns={}  # {"entity_a->entity_b": {count, last_seen, avg_gap_seconds}}
recent_sequence=deque(maxlen=20)  # Recent entity changes for sequence detection

# v2.0: Anomaly detection
entity_hour_histogram=defaultdict(lambda: Counter())  # entity_id -> {hour: count}
anomalies=deque(maxlen=100)

# v2.0: Rate limiting
rate_window=defaultdict(list)  # entity_id -> [timestamps]
rate_alerts=deque(maxlen=50)
RATE_LIMIT=10  # events per minute threshold

def load_patterns():
    global patterns, entity_hour_histogram
    try:
        if os.path.exists(PATTERN_FILE):
            d=json.load(open(PATTERN_FILE))
            patterns=d.get('patterns',{})
            entity_hour_histogram=defaultdict(lambda: Counter(), {k:Counter(v) for k,v in d.get('histograms',{}).items()})
            logger.info(f'Loaded {len(patterns)} patterns, {len(entity_hour_histogram)} histograms')
    except Exception as e:
        logger.error(f'Load patterns: {e}')

def save_patterns():
    try:
        d={'patterns':patterns,'histograms':{k:dict(v) for k,v in entity_hour_histogram.items()}}
        json.dump(d,open(PATTERN_FILE,'w'),indent=2)
    except: pass

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

def classify_event(ev):
    """v2.0: Classify event as routine/unusual/critical."""
    eid=ev.get('entity_id','')
    hour=datetime.now().hour
    
    # Critical: presence, lock, security
    if 'presence' in eid or 'lock' in eid:
        return 'critical'
    
    # Check if this entity normally fires at this hour
    hist=entity_hour_histogram[eid]
    total=sum(hist.values())
    if total>50:  # Only classify after enough data
        hour_pct=hist.get(str(hour),0)/total if total>0 else 0
        if hour_pct<0.02:  # <2% of events happen at this hour
            return 'unusual'
    
    return 'routine'

def detect_anomaly(ev):
    """v2.0: Flag events at unusual times."""
    eid=ev.get('entity_id','')
    hour=datetime.now().hour
    hist=entity_hour_histogram[eid]
    total=sum(hist.values())
    
    if total>100:
        hour_pct=hist.get(str(hour),0)/total if total>0 else 0
        if hour_pct<0.01:  # Very unusual time
            anomaly={'entity_id':eid,'hour':hour,'expected_pct':round(hour_pct*100,2),'time':datetime.now().isoformat(),'event':ev}
            anomalies.append(anomaly)
            logger.warning(f'ANOMALY: {eid} at hour {hour} (only {hour_pct*100:.1f}% historical)')
            pub(f'anomaly/{eid}',anomaly)
            return anomaly
    return None

def check_rate_limit(ev):
    """v2.0: Detect entities firing too rapidly."""
    eid=ev.get('entity_id','')
    now=time.time()
    
    # Clean old entries (>60s)
    rate_window[eid]=[t for t in rate_window[eid] if now-t<60]
    rate_window[eid].append(now)
    
    if len(rate_window[eid])>RATE_LIMIT:
        alert={'entity_id':eid,'events_per_min':len(rate_window[eid]),'time':datetime.now().isoformat(),'message':f'{eid} firing {len(rate_window[eid])} events/min — possible malfunction'}
        # Only alert once per 5 minutes per entity
        recent_alerts=[a for a in rate_alerts if a['entity_id']==eid and (now-datetime.fromisoformat(a['time']).timestamp())<300]
        if not recent_alerts:
            rate_alerts.append(alert)
            logger.warning(f'RATE LIMIT: {alert["message"]}')
            pub(f'rate_limit/{eid}',alert)
        return alert
    return None

def detect_pattern(ev):
    """v2.0: Track event sequences and identify recurring patterns."""
    eid=ev.get('entity_id','')
    now=time.time()
    
    # Add to sequence
    recent_sequence.append({'entity_id':eid,'time':now})
    
    # Check for patterns: entity A followed by entity B within 60s
    if len(recent_sequence)>=2:
        prev=recent_sequence[-2]
        gap=now-prev['time']
        if gap<60 and prev['entity_id']!=eid:  # Different entities within 60s
            key=f"{prev['entity_id']}->{eid}"
            if key not in patterns:
                patterns[key]={'count':0,'last_seen':'','avg_gap':0,'learned':False}
            p=patterns[key]
            p['count']+=1
            p['last_seen']=datetime.now().isoformat()
            # Running average of gap
            p['avg_gap']=round((p['avg_gap']*(p['count']-1)+gap)/p['count'],1)
            
            # Flag as learned routine after 7+ occurrences
            if p['count']>=7 and not p['learned']:
                p['learned']=True
                logger.info(f'PATTERN LEARNED: {key} ({p["count"]} times, avg {p["avg_gap"]}s gap)')
                pub('pattern/learned',{'sequence':key,'count':p['count'],'avg_gap':p['avg_gap']})
    
    # Periodic save
    if counts.get('total',0)%100==0:
        save_patterns()

def route(ev):
    eid=ev.get('entity_id','')
    new=ev.get('new_state','')
    old=ev.get('old_state','')
    dom=eid.split('.')[0]
    
    # v2.0: Classification
    ev['classification']=classify_event(ev)
    
    # v2.0: Update histogram
    entity_hour_histogram[eid][str(datetime.now().hour)]+=1
    
    # v2.0: Anomaly check
    anomaly=detect_anomaly(ev)
    if anomaly:
        ev['anomaly']=True
    
    # v2.0: Rate limit check
    rate_alert=check_rate_limit(ev)
    if rate_alert:
        ev['rate_limited']=True
    
    # v2.0: Pattern detection
    detect_pattern(ev)
    
    pub(f'state/{dom}/{eid}',{'state':new,'old':old,'classification':ev['classification']})
    
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
    elif 'media_player.75_the_frame' in eid and old!=new: sig=True; reason=f'TV: {old}->{new}'
    elif eid=='weather.forecast_home' and old!=new: sig=True; reason=f'Weather: {old}->{new}'
    elif 'vacuum' in eid and old!=new: sig=True; reason=f'Vacuum: {old}->{new}'
    elif eid=='sun.sun' and old!=new: sig=True; reason=f'Sun: {old}->{new}'
    elif 'temperature' in eid:
        try:
            if abs(float(new)-float(old))>3: sig=True; reason=f'Temp: {eid} {old}->{new}'
        except: pass
    if sig:
        ev['significant']=True; ev['reason']=reason
        counts['significant']+=1
        logger.info(f'SIG [{ev["classification"]}]: {reason}')
        pub(f'significant/{ev["classification"]}',{'entity_id':eid,'reason':reason,'new':new,'old':old})

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
            ev={'entity_id':eid,'domain':dom,'old_state':ov,'new_state':nv,'time':datetime.now().isoformat(),'significant':False,'classification':'routine'}
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

def pattern_save_loop():
    """Periodic pattern persistence."""
    while True:
        time.sleep(300)
        save_patterns()

@app.route('/')
def index(): return jsonify({'name':'TARS Event Bus','version':'2.0.0','connected':ws_ok,'total':counts.get('total',0),'significant':counts.get('significant',0),'patterns_learned':len([p for p in patterns.values() if p.get('learned')])})
@app.route('/health')
def health(): return jsonify({'status':'ok' if ws_ok else 'disconnected','websocket':ws_ok,'total':counts.get('total',0),'mqtt':mqtt is not None,'patterns':len(patterns),'anomalies':len(anomalies)})
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
def stats_(): return jsonify({'total':counts.get('total',0),'significant':counts.get('significant',0),'by_domain':dict(counts),'entities':len(states),'connected':ws_ok,'patterns_total':len(patterns),'patterns_learned':len([p for p in patterns.values() if p.get('learned')]),'anomalies_recent':len(anomalies),'rate_alerts':len(rate_alerts)})
@app.route('/entities')
def ents(): return jsonify(states)
@app.route('/entity/<eid>')
def ent(eid):
    if eid in states: return jsonify({'entity':eid,'state':states[eid],'recent':[e for e in events if e.get('entity_id')==eid][-10:],'histogram':dict(entity_hour_histogram.get(eid,{}))})
    return jsonify({'error':'Not tracked'}),404

# v2.0 endpoints
@app.route('/patterns')
def get_patterns():
    learned=[{'sequence':k,**v} for k,v in patterns.items() if v.get('learned')]
    all_p=[{'sequence':k,**v} for k,v in patterns.items()]
    all_p.sort(key=lambda x:x['count'],reverse=True)
    return jsonify({'learned':learned,'top_50':all_p[:50],'total':len(patterns)})

@app.route('/anomalies')
def get_anomalies():
    return jsonify(list(anomalies)[-20:])

@app.route('/rate-limits')
def get_rate_limits():
    now=time.time()
    active={eid:len([t for t in times if now-t<60]) for eid,times in rate_window.items() if len([t for t in times if now-t<60])>3}
    return jsonify({'active_high_rate':active,'recent_alerts':list(rate_alerts)[-10:]})

if __name__=='__main__':
    logger.info(f'TARS Event Bus v2.0.0 on :{API_PORT}')
    logger.info(f'WS: {WS_URL}')
    logger.info(f'Domains: {WATCHED}')
    load_patterns()
    setup_mqtt()
    threading.Thread(target=ws_thread,daemon=True).start()
    threading.Thread(target=pattern_save_loop,daemon=True).start()
    logger.info('WebSocket listener + pattern engine started')
    app.run(host='0.0.0.0',port=API_PORT,debug=False)
