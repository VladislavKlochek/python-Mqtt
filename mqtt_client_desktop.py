import time
import paho.mqtt.client as mqtt
import screen_brightness_control as sbc
import cv2
from threading import Thread
import ctypes
import pyaudio
import wave
import datetime
from comtypes import CLSCTX_ALL
from pycaw.pycaw import AudioUtilities, IAudioEndpointVolume

camera_thread = None
camera_running = False
microphone_thread = None
microphone_running = False
pyAudioUnit = None
frames = []

def log_to_file(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}\n"

    with open("D:\\Minor\\mqtt\\mqtt_client_desktop_log.txt", "a") as log_file:
        log_file.write(log_message)

class MOUSEINPUT(ctypes.Structure):
    _fields_ = [
        ("dx", ctypes.c_long),
        ("dy", ctypes.c_long),
        ("mouseData", ctypes.c_ulong),
        ("dwFlags", ctypes.c_ulong),
        ("time", ctypes.c_ulong),
        ("dwExtraInfo", ctypes.POINTER(ctypes.c_ulong))
    ]


class INPUT(ctypes.Structure):
    _fields_ = [
        ("type", ctypes.c_ulong),
        ("mi", MOUSEINPUT)
    ]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Успешное подключение к брокеру")
    else:
        print("Не удалось подключиться. Код ошибки: " + str(rc))


def on_message(client, userdata, message):
    msg = str(message.payload.decode())
    if message.topic == "Laptop/brightness":
        try:
            brightness = sbc.get_brightness()[0]
            if msg[0] == '+':
                sbc.set_brightness(brightness + int(msg[1:]))
            elif msg[0] == '-':
                sbc.set_brightness(brightness - int(msg[1:]))
        except ValueError:
            print("Значение " + msg[1:] + " не является int значением")
    elif message.topic == "Laptop/screen":
        if msg.lower() == "on" or msg.lower() == "1":
            screen_on()
        elif msg.lower() == "off" or msg.lower() == "0":
            screen_off()
    elif message.topic == "Laptop/camera":
        if msg.lower() == "on" or msg.lower() == "1":
            start_camera()
        elif msg.lower() == "off" or msg.lower() == "0":
            stop_camera()
    elif message.topic == "Laptop/microphone":
        if msg.lower() == "on" or msg.lower() == "1":
            start_microphone()
        if msg.lower() == "off" or msg.lower() == "0":
            stop_microphone()
    elif message.topic == "Laptop/camera/start_each":
        if msg.lower().isdigit():
            start_camera_for(int(msg.lower()))
        if msg.lower() == "off" or msg.lower() == "0":
            stop_camera_for()
    elif message.topic == "Laptop/volume":
        devices = AudioUtilities.GetSpeakers()
        interface = devices.Activate(
            IAudioEndpointVolume._iid_, CLSCTX_ALL, None)
        volume = ctypes.cast(interface, ctypes.POINTER(IAudioEndpointVolume))
        current_volume = volume.GetMasterVolumeLevelScalar()
        try:
            if msg[0] == '+':
                new_volume = max(0.0, min(1.0, current_volume + int(msg[1:]) / 100.0))
            elif msg[0] == '-':
                new_volume = max(0.0, min(1.0, current_volume - int(msg[1:]) / 100.0))
            volume.SetMasterVolumeLevelScalar(new_volume, None)
        except ValueError:
            print("Значение " + msg[1:] + " не является int значением")

    print("Получено сообщение на тему '" + message.topic + "': " + str(message.payload.decode()))





def screen_on():
    input_structure = INPUT()
    input_structure.type = 0
    input_structure.mi.dx = 0
    input_structure.mi.dy = 0
    input_structure.mi.mouseData = 0
    input_structure.mi.dwFlags = 0x0001
    input_structure.mi.time = 0
    input_structure.mi.dwExtraInfo = ctypes.pointer(ctypes.c_ulong(0))
    ctypes.windll.user32.SendInput(1, ctypes.byref(input_structure), ctypes.sizeof(input_structure))


def screen_off():
    ctypes.windll.user32.SendMessageW(65535, 274, 61808, 2)


def start_camera():
    global camera_thread, camera_running
    if camera_running:
        return

    camera_thread = Thread(target=camera_thread_function)
    camera_thread.start()


def stop_camera():
    global camera_thread, camera_running
    camera_running = False
    if camera_thread:
        camera_thread.join()
        camera_thread = None


def camera_thread_function():
    global camera_running
    camera_running = True

    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Не удалось открыть камеру.")
        camera_running = False
        return

    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    output_video = cv2.VideoWriter(
        f'D:\Minor\mqtt\Camera_records\{datetime.datetime.now().strftime("%d.%m.%Y_%H-%M-%S")}.avi', fourcc, 20.0,
        (640, 480))

    while camera_running:
        ret, frame = cap.read()

        if not ret:
            print("Ошибка при чтении кадра.")
            break

        cv2.waitKey(5)
        output_video.write(frame)
    cap.release()
    output_video.release()
    cv2.destroyAllWindows()
    camera_running = False


def camera_thread_for_each_function(minutes: int):
    global camera_running
    camera_running = True

    start_recording()
    while camera_running:
        if not camera_running:
            return
        time.sleep(minutes * 60)
        if not camera_running:
            return
        start_recording()
    return


def start_recording():
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Не удалось открыть камеру.")
        return

    current_time = datetime.datetime.now()
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    output_video = cv2.VideoWriter(
        f'D:\Minor\mqtt\Records_by_minutes\{current_time.strftime("%d.%m.%Y_%H-%M-%S")}.avi', fourcc, 30, (640, 480))

    frame_count = 0
    record_stop_time = time.time() + 60
    while time.time() < record_stop_time:
        ret, frame = cap.read()

        if not ret:
            print("Ошибка при чтении кадра.")
            break

        output_video.write(frame)
        frame_count += 1

    client.publish(
        "Laptop/camera/each_received", f'Запись сделана {current_time.strftime("%d.%m.%Y_%H-%M-%S")},'
                                       f' запись длилась {(frame_count / 30):.0f} секунд')
    cap.release()
    output_video.release()
    cv2.destroyAllWindows()


def microphone_thread_function():
    chunk = 1024
    FORMAT = pyaudio.paInt32
    CHANNELS = 2
    RATE = 44100
    WAVE_OUTPUT_FILENAME = f'D:\\Minor\\mqtt\\Microphone_records\\{datetime.datetime.now().strftime("%d.%m.%Y_%H-%M-%S")}.wav'

    global microphone_running
    microphone_running = True
    global pyAudioUnit
    pyAudioUnit = pyaudio.PyAudio()

    stream = pyAudioUnit.open(format=FORMAT,
                              channels=CHANNELS,
                              rate=RATE,
                              input=True,
                              frames_per_buffer=chunk)
    print("* Запись аудио...")
    while microphone_running:
        data = stream.read(chunk)
        frames.append(data)

    stream.stop_stream()
    stream.close()
    pyAudioUnit.terminate()
    with open(WAVE_OUTPUT_FILENAME, 'wb') as wf:
        wf = wave.open(WAVE_OUTPUT_FILENAME, 'wb')
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(pyAudioUnit.get_sample_size(FORMAT))
        wf.setframerate(RATE)
        wf.writeframes(b''.join(frames))
    frames.clear()


def start_microphone():
    global microphone_thread, microphone_running
    if microphone_running:
        return

    microphone_thread = Thread(target=microphone_thread_function)
    microphone_thread.start()


def stop_microphone():
    global microphone_thread, microphone_running
    microphone_running = False
    if microphone_thread:
        microphone_thread.join()
        microphone_thread = None


def start_camera_for(minutes: int):
    global camera_thread, camera_running
    if camera_running:
        return

    camera_thread = Thread(target=camera_thread_for_each_function, args=(minutes,))
    camera_thread.start()


def stop_camera_for():
    global camera_thread, camera_running
    camera_running = False
    if camera_thread:
        camera_thread = None


if __name__ == '__main__':
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 11)

    client.subscribe("Laptop/screen")
    client.subscribe("Laptop/brightness")
    client.subscribe("Laptop/camera")
    client.subscribe("Laptop/microphone")
    client.subscribe("Laptop/camera/start_each")
    client.subscribe("Laptop/volume")
    client.loop_forever()
