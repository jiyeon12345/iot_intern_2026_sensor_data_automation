# 파이썬 실행 방법: 
# 1. 파이썬 가상환경 생성 : python -m venv venv
# 2. 파이썬 가상환경 활성화 : venv\Scripts\activate
# 3. 파이썬 패키지 설치 : pip install paho-mqtt
# 4. 파이썬 실행 : python sensor_jy.py

import json
import time
import random
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import CallbackAPIVersion

BROKER = 'localhost'
PORT = 1883
# TOPIC = '/oneM2M/req/PillarMonitor/cHdzH189a/json'
TOPIC = f'/oneM2M/req/${AE}/${CseId}/json'
DURATION = 300
INTERVAL = 10

def generate_onem2m_message():
    """oneM2M 형식으로 데이터 생성"""
    sensor_data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), # 타임스탬프
        "humidity": round(random.uniform(0, 70), 2), # 습도
        "temperature": round(random.uniform(5, 35), 1) # 온도
    }
    
    # oneM2M ContentInstance 생성 메시지
    onem2m_message = {
        "m2m:rqp": {
            "fr": f"${aei}", """AE의 aei"""
            "to": f"/${csebaseId}/${aeId}/${cntId}",  
            "op": 1,
            "rqi": f"req{int(time.time())}",
            "pc": {
                "m2m:cin": {
                    "con": json.dumps(sensor_data)
                }
            },
            "ty": 4
        }
    }
        
    return sensor_data, json.dumps(onem2m_message)

def main():
    client = mqtt_client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f'pillar-sensor-{random.randint(0, 1000)}'
    )
    
    try:
        client.connect(BROKER, PORT, keepalive=60)
    except Exception as e:
        return
    
    end_time = time.time() + DURATION
    count = 0
    try:
        while time.time() < end_time:
            sensor_data, onem2m_msg = generate_onem2m_message()
            
            result = client.publish(TOPIC, onem2m_msg, qos=1)
            
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                count += 1
            else:
                print(f"[{count}] 전송 실패 (code: {result.rc})")
            
            client.loop()
            time.sleep(INTERVAL)
            
    except KeyboardInterrupt:
        print("\n사용자 중단")
    finally:
        client.disconnect()
        print(f"\n총 {count}개 데이터 전송")

if __name__ == '__main__':

    main()

