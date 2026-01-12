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
TOPIC = '/oneM2M/req/PillarMonitor/Mobius/json'
# TOPIC = f'/oneM2M/req/${AE}/${CseId}/json'
DURATION = 300
INTERVAL = 10

def generate_onem2m_message():
    """oneM2M 형식으로 데이터 생성"""
    sensor_data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), # 타임스탬프
        "humidity": round(random.uniform(0, 7), 2), # 습도
        "temperature": round(random.uniform(5, 35), 1) # 온도
    }
    
    # oneM2M ContentInstance 생성 메시지
    # onem2m_message = {
    #     "m2m:rqp": {
    #         "fr": f"${aei}", """AE의 aei"""
    #         "to": f"/${csebaseId}/${aeId}/${cntId}",  
    #         "op": 1,
    #         "rqi": f"req{int(time.time())}",
    #         "pc": {
    #             "m2m:cin": {
    #                 "con": json.dumps(sensor_data)
    #             }
    #         },
    #         "ty": 4
    #     }
    # }
    onem2m_message = {
        "m2m:rqp": {
            "fr": "S9t2YYKIXbPjm",
            "to": "/Mobius/PillarMonitor/SensorData",
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
        print(f"MQTT 브로커 연결 시도 중... ({BROKER}:{PORT})")
        client.connect(BROKER, PORT, keepalive=60)
        print("✓ MQTT 브로커 연결 성공!")
    except Exception as e:
        print(f"✗ MQTT 브로커 연결 실패: {e}")
        print(f"  브로커가 {BROKER}:{PORT}에서 실행 중인지 확인하세요.")
        return
    
    end_time = time.time() + DURATION
    count = 0
    print(f"\n데이터 전송 시작 (총 {DURATION}초, {INTERVAL}초 간격)")
    print(f"토픽: {TOPIC}\n")
    try:
        while time.time() < end_time:
            sensor_data, onem2m_msg = generate_onem2m_message()
            
            result = client.publish(TOPIC, onem2m_msg, qos=1)
            
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                count += 1
                print(f"[{count}] 전송 성공 - {sensor_data['timestamp']} | 온도: {sensor_data['temperature']}°C, 습도: {sensor_data['humidity']}%")
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

