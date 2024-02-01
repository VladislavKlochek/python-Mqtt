import time
import paho.mqtt.client as mqtt
import cv2
from threading import Thread
import ctypes
import pyaudio
import wave
import datetime
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket

camera_thread = None
camera_running = False
microphone_thread = None
microphone_running = False
pyAudioUnit = None
frames = []


def log_to_file(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}\n"

    with open("D:\\Minor\\mqtt\\Mqtt_as_service_log.txt", "a") as log_file:
        log_file.write(log_message)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log_to_file("Успешное подключение к брокеру")
    else:
        log_to_file("Не удалось подключиться. Код ошибки: " + str(rc))


def on_message(client, userdata, message):
    msg = str(message.payload.decode())

    if message.topic == "Laptop/camera":
        if msg.lower() == "on" or msg.lower() == "1":
            start_camera()
            log_to_file("camera_started")
        elif msg.lower() == "off" or msg.lower() == "0":
            stop_camera()
            log_to_file("camera_stopped")
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
        log_to_file("Не удалось открыть камеру.")
        camera_running = False
        return

    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    output_video = cv2.VideoWriter(
        f'D:\Minor\mqtt\Camera_records\{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.avi', fourcc, 20.0,
        (640, 480))

    while camera_running:
        ret, frame = cap.read()

        if not ret:
            log_to_file("Ошибка при чтении кадра.")
            break

        cv2.imshow("Camera", frame)
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
        log_to_file("Не удалось открыть камеру.")
        return

    current_time = datetime.datetime.now()
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    output_video = cv2.VideoWriter(
        f'D:\Minor\mqtt\Records_by_minutes\{current_time.strftime("%Y-%m-%d_%H-%M-%S")}.avi', fourcc, 30, (640, 480))

    frame_count = 0
    record_stop_time = time.time() + 60
    while time.time() < record_stop_time:
        ret, frame = cap.read()

        if not ret:
            log_to_file("Ошибка при чтении кадра.")
            break

        output_video.write(frame)
        frame_count += 1

    client.publish(
        "Laptop/camera/each_received", f'Сделана запись в {current_time.strftime("%Y-%m-%d_%H-%M-%S")},'
                                       f' запись длилась {(frame_count / 30):.0f} секунд')
    cap.release()
    output_video.release()
    cv2.destroyAllWindows()


def microphone_thread_function():
    chunk = 1024  # Количество фреймов в буфере
    FORMAT = pyaudio.paInt32  # Формат звука (32 бит, стерео)
    CHANNELS = 2
    RATE = 44100  # Количество фреймов в секунду
    WAVE_OUTPUT_FILENAME = f'D:\Minor\mqtt\Microphone_records\{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.wav'

    global microphone_running
    microphone_running = True
    global pyAudioUnit
    pyAudioUnit = pyaudio.PyAudio()

    stream = pyAudioUnit.open(format=FORMAT,
                              channels=CHANNELS,
                              rate=RATE,
                              input=True,
                              frames_per_buffer=chunk)

    log_to_file("microphone_running")
    while microphone_running:
        data = stream.read(chunk)
        frames.append(data)

    stream.stop_stream()
    stream.close()
    pyAudioUnit.terminate()

    wf = wave.open(WAVE_OUTPUT_FILENAME, 'wb')
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(pyAudioUnit.get_sample_size(FORMAT))
    wf.setframerate(RATE)
    wf.writeframes(b''.join(frames))
    wf.close()

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
        log_to_file("microphone_running")


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


client = mqtt.Client()


class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PythonMqttService'
    _svc_display_name_ = 'PythonMqttService'

    @classmethod
    def parse_command_line(cls):
        win32serviceutil.HandleCommandLine(cls)

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_alive = True
        self.start()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_alive = False
        self.stop()

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))

        self.main()

    def main(self):
        while self.is_alive:
            result = win32event.WaitForSingleObject(self.hWaitStop, 1000)  # Check every second
            if result == win32event.WAIT_OBJECT_0:
                break
            time.sleep(1)

    def start(self):
        global client
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect("localhost", 1883, 11)
        client.subscribe("Laptop/camera")
        client.subscribe("Laptop/microphone")
        client.subscribe("Laptop/camera/start_each")
        client.loop_start()

    def stop(self):
        global client
        client.loop_stop()
        client.unsubscribe("Laptop/camera")
        client.unsubscribe("Laptop/microphone")
        client.unsubscribe("Laptop/camera/start_each")
        client.disconnect()


if __name__ == '__main__':
    MyService.parse_command_line()
