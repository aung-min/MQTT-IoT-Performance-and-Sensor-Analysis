#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IMU6500 Vibration Monitor — Tkinter + Matplotlib (TkAgg)
MQTT Edition + Debug Tab (JSON + QoS + Latency)
- MQTT connect/disconnect (broker/port/topic fields)
- Live plots: HP Magnitude (g) & RMS (g)
- Threshold lines toggle (STRUCT/FOOT/PLAY/JUMP)
- Simulation: load CSV or synthetic generator
- CSV logging (+ open)
- Calibration: Start/Stop/Clear, Import/Export/Append, Compute Thresholds
- Debug/JSON tab with table (topic, QoS, device ms, local time, latency),
  raw JSON viewer, and live latency/rate stats
"""

import os, sys, csv, time, math, threading, queue, statistics, json, random
from collections import deque
from dataclasses import dataclass
from typing import List, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# Matplotlib for TkAgg
import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt

# MQTT
import paho.mqtt.client as mqtt

# ---------------- Constants ----------------
MAX_HP  = 0.6
MAX_RMS = 0.6
CAPACITY = 800    # ~8s at 100 Hz

MIN_SAMPLES_PER_CLASS = 30
DEBUG_ROWS = 400          # table capacity

# Colors (RGB normalized for matplotlib)
CLR_HP   = (0/255, 191/255, 255/255)   # DeepSkyBlue
CLR_RMS  = (0/255, 255/255, 127/255)   # MediumSpringGreen
CLR_CALM   = (46/255, 204/255, 113/255)
CLR_STRUCT = (108/255, 192/255, 255/255)
CLR_WARN   = (255/255, 200/255, 87/255)
CLR_PLAY   = (255/255, 179/255, 71/255)
CLR_BAD    = (255/255,  90/255, 95/255)

# -------------- Data structures --------------
@dataclass
class Sample:
    ms: int
    ax: float
    ay: float
    az: float
    mag: float
    hp_abs: float
    rms: float
    label: str

@dataclass
class MqttFrame:
    topic: str
    qos: int
    recv_ts: float     # local time (seconds since epoch)
    raw_json: str
    sample: Sample     # parsed Sample (with device ms)

# -------------- Utils --------------
def clamp(v, lo, hi):
    return lo if v < lo else (hi if v > hi else v)

def compute_label_from_rms_custom(rms, th_struct, th_foot, th_kid, th_jump):
    if rms >= th_jump: return "JUMP"
    if rms >= th_kid:  return "PLAY"
    if rms >= th_foot: return "FOOT"
    if rms >= th_struct: return "STRUCT"
    return "CALM"

def clean_rms(lst: List[float]) -> List[float]:
    out = []
    for v in lst:
        if v is None or math.isnan(v) or math.isinf(v):
            continue
        out.append(clamp(v, 0.0, 1.0))
    return out

def try_boundary(lower: List[float], higher: List[float]) -> Tuple[bool, float]:
    """Minimize overlap error between adjacent classes to pick threshold."""
    if not lower or not higher:
        return False, 0.0
    a = sorted(lower)
    b = sorted(higher)
    comb = sorted(a + b)
    uniq = []
    last = None
    for v in comb:
        if last is None or abs(v - last) > 1e-12:
            uniq.append(v)
            last = v
    nA, nB = len(a), len(b)
    cumA, cumB = [0]*len(uniq), [0]*len(uniq)
    iA = iB = 0
    for i, v in enumerate(uniq):
        while iA < nA and a[iA] <= v: iA += 1
        while iB < nB and b[iB] <= v: iB += 1
        cumA[i] = iA
        cumB[i] = iB

    best_err = float('inf'); best_t = uniq[0]
    for i in range(-1, len(uniq)):
        if i == -1:
            t = uniq[0] - 1e-6
            a_le = 0; b_le = 0
        elif i == len(uniq)-1:
            t = uniq[i] + 1e-6
            a_le = cumA[i]; b_le = cumB[i]
        else:
            t = 0.5*(uniq[i] + uniq[i+1])
            a_le = cumA[i]; b_le = cumB[i]
        a_gt = nA - a_le
        b_leq = b_le
        err = a_gt + b_leq
        if err < best_err:
            best_err = err
            best_t = t
    return True, best_t

def compute_thresholds(calib):
    """Return ok, th_struct, th_foot, th_kid, th_jump, msg"""
    calm   = clean_rms(calib["CALM"])
    struct = clean_rms(calib["STRUCT"])
    foot   = clean_rms(calib["FOOT"])
    play   = clean_rms(calib["PLAY"])
    jump   = clean_rms(calib["JUMP"])
    if not all(len(lst) >= MIN_SAMPLES_PER_CLASS for lst in [calm, struct, foot, play, jump]):
        return (False, 0,0,0,0, f"Need ≥{MIN_SAMPLES_PER_CLASS} samples per class.")
    ok1, thStruct = try_boundary(calm, struct)
    ok2, thFoot   = try_boundary(struct, foot)
    ok3, thKid    = try_boundary(foot, play)
    ok4, thJump   = try_boundary(play, jump)
    if not (ok1 and ok2 and ok3 and ok4):
        return (False, 0,0,0,0, "Adjacent classes overlap too much; collect more/cleaner data.")
    eps = 0.005
    thStruct = clamp(thStruct, 0.001, 0.6)
    thFoot   = clamp(max(thFoot, thStruct+eps), 0.001, 0.6)
    thKid    = clamp(max(thKid,  thFoot  +eps), 0.001, 0.6)
    thJump   = clamp(max(thJump, thKid   +eps), 0.001, 1.0)
    return (True, thStruct, thFoot, thKid, thJump, "OK")

# -------------- MQTT Reader Thread --------------
class MqttReader(threading.Thread):
    def __init__(self, host, port, topic, out_queue, on_status, on_closed):
        super().__init__(daemon=True)
        self.host = host
        self.port = int(port)
        self.topic = topic
        self.out_queue = out_queue
        self.on_status = on_status
        self.on_closed = on_closed
        self._stop = False

        # paho-mqtt compatibility: try v2 API, fall back to v1
        self.client = None
        try:
            # paho >= 2.0
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"rpi-gui-{int(time.time())}")
            self.v2 = True
        except Exception:
            # paho 1.x
            self.client = mqtt.Client(client_id=f"rpi-gui-{int(time.time())}", protocol=mqtt.MQTTv311)
            self.v2 = False

    def run(self):
        try:
            # Bind callbacks (support both signatures)
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_disconnect = self._on_disconnect

            self.client.connect(self.host, self.port, keepalive=30)
            self.client.loop_start()
            while not self._stop:
                time.sleep(0.05)
        except Exception as e:
            self.on_status(f"MQTT error: {e}")
        finally:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except:
                pass
            self.on_closed()

    def stop(self):
        self._stop = True

    # Compatible on_connect: paho2: (client, userdata, flags, rc, properties)
    #                        paho1: (client, userdata, flags, rc)
    def _on_connect(self, client, userdata, *args, **kwargs):
        # args may be (flags, rc) or (flags, rc, properties)
        rc = None
        if len(args) >= 2:
            rc = args[1]
        elif len(args) == 1:
            rc = args[0]
        else:
            rc = 0
        if rc == 0:
            self.on_status(f"MQTT connected → {self.host}:{self.port}, sub '{self.topic}'")
            client.subscribe(self.topic, qos=0)
        else:
            self.on_status(f"MQTT connect failed rc={rc}")

    def _on_disconnect(self, client, userdata, *args, **kwargs):
        self.on_status("MQTT disconnected")

    def _on_message(self, client, userdata, msg):
        try:
            recv_ts = time.time()
            payload = msg.payload.decode("utf-8", errors="ignore")
            d = json.loads(payload)
            smp = Sample(
                ms   = int(float(d.get("ms", 0))),
                ax   = float(d.get("ax", 0.0)),
                ay   = float(d.get("ay", 0.0)),
                az   = float(d.get("az", 0.0)),
                mag  = float(d.get("mag", 0.0)),
                hp_abs = float(d.get("hp_abs", 0.0)),
                rms  = float(d.get("rms", 0.0)),
                label= str(d.get("label", "CALM")).upper()
            )
            frame = MqttFrame(
                topic = msg.topic,
                qos   = int(getattr(msg, "qos", 0)),
                recv_ts = recv_ts,
                raw_json = payload,
                sample = smp
            )
            self.out_queue.put(frame)
        except Exception:
            pass  # ignore malformed frames

# -------------- Simulation Thread --------------
class SimReader(threading.Thread):
    def __init__(self, lines, rate_hz, loop, out_queue, on_finished):
        super().__init__(daemon=True)
        self.lines = lines
        self.rate_hz = max(1.0, float(rate_hz or 100.0))
        self.loop = loop
        self.out_queue = out_queue
        self.on_finished = on_finished
        self._stop = False

    def run(self):
        delay = 1.0 / self.rate_hz
        i = 0
        n = len(self.lines)
        while not self._stop:
            if n <= 0: break
            if i >= n:
                if not self.loop: break
                i = 0
            line = self.lines[i].strip(); i += 1
            if line and not line.startswith("#"):
                p = [x.strip() for x in line.split(",")]
                if len(p) >= 7:
                    try:
                        smp = Sample(
                            ms=int(float(p[0])),
                            ax=float(p[1]), ay=float(p[2]), az=float(p[3]),
                            mag=float(p[4]), hp_abs=float(p[5]),
                            rms=float(p[6]),
                            label=p[7].upper() if len(p) >= 8 and p[7] else "CALM"
                        )
                        frame = MqttFrame(topic="simulation", qos=0, recv_ts=time.time(),
                                          raw_json=json.dumps({
                                              "ms":smp.ms,"ax":smp.ax,"ay":smp.ay,"az":smp.az,
                                              "mag":smp.mag,"hp_abs":smp.hp_abs,"rms":smp.rms,"label":smp.label
                                          }), sample=smp)
                        self.out_queue.put(frame)
                    except: pass
            time.sleep(delay)
        self.on_finished()

    def stop(self):
        self._stop = True

# -------------- Main App --------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("IMU6500 Vibration Monitor (MQTT + Debug)")
        self.geometry("1280x900")

        # state
        self.frame_queue = queue.Queue()
        self.mqtt_thread = None
        self.sim_thread = None
        self.simulating = False
        self.sim_lines = []
        self.logging = False
        self.log_fp = None
        self.log_path = ""

        self.hp_series  = deque(maxlen=CAPACITY)
        self.rms_series = deque(maxlen=CAPACITY)

        self.rate_hz_est = 100.0
        self._last_ms = -1
        self._ema_rate = 100.0

        self.th_struct = 0.03
        self.th_foot   = 0.10
        self.th_kid    = 0.20
        self.th_jump   = 0.35

        self.calibrating = False
        self.calib_active_label = "CALM"
        self.calib = { "CALM":[], "STRUCT":[], "FOOT":[], "PLAY":[], "JUMP":[] }

        # Debug buffers
        self.debug_rows = deque(maxlen=DEBUG_ROWS)   # tuples for table
        self.lat_samples = deque(maxlen=1000)        # latency ms
        self.arrival_ts  = deque(maxlen=1000)        # recv_ts for rate
        self._last_raw_json = None

        self._build_ui()
        self.after(16, self._ui_pump)

    # ---------- UI ----------
    def _build_ui(self):
        # Top bar: MQTT + Logging
        f0 = ttk.Frame(self); f0.pack(fill="x", padx=10, pady=6)
        ttk.Label(f0, text="Broker:").pack(side="left")
        self.broker_var = tk.StringVar(value="192.168.88.4")
        tk.Entry(f0, textvariable=self.broker_var, width=16).pack(side="left", padx=4)

        ttk.Label(f0, text="Port:").pack(side="left")
        self.port_var = tk.StringVar(value="1883")
        tk.Entry(f0, textvariable=self.port_var, width=6).pack(side="left", padx=4)

        ttk.Label(f0, text="Topic:").pack(side="left")
        self.topic_var = tk.StringVar(value="smarthome/imu")
        tk.Entry(f0, textvariable=self.topic_var, width=32).pack(side="left", padx=4)

        self.btn_mqtt_connect = ttk.Button(f0, text="Connect", command=self.connect_mqtt); self.btn_mqtt_connect.pack(side="left", padx=2)
        self.btn_mqtt_disconnect = ttk.Button(f0, text="Disconnect", command=self.disconnect_mqtt, state="disabled"); self.btn_mqtt_disconnect.pack(side="left", padx=2)

        ttk.Label(f0, text="  ").pack(side="left", padx=6)
        self.btn_log = ttk.Button(f0, text="Start Logging", command=self.toggle_logging); self.btn_log.pack(side="left")
        self.btn_openlog = ttk.Button(f0, text="Open Log", command=self.open_log, state="disabled"); self.btn_openlog.pack(side="left", padx=4)
        self.log_path_lbl = ttk.Label(f0, text="", width=60, anchor="w"); self.log_path_lbl.pack(side="left", padx=6, fill="x", expand=True)

        # Tabs
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=6)

        # --- Tab 1: Plots & Calibration ---
        tab1 = ttk.Frame(self.notebook)
        self.notebook.add(tab1, text="Plots & Calibration")

        # Status
        f3 = ttk.LabelFrame(tab1, text="Status"); f3.pack(fill="x", padx=4, pady=6)
        self.status_lbl = ttk.Label(f3, text="—", font=("TkDefaultFont", 14, "bold")); self.status_lbl.pack(side="left", padx=6)
        self.status_chip = tk.Canvas(f3, width=20, height=20, highlightthickness=0); self.status_chip.pack(side="left")
        self._set_chip((60,60,60))
        self.lbl_rms = ttk.Label(f3, text="RMS: —"); self.lbl_rms.pack(side="left", padx=8)
        self.lbl_hp  = ttk.Label(f3, text="HP: —");  self.lbl_hp.pack(side="left", padx=8)
        self.lbl_mag = ttk.Label(f3, text="Mag: —"); self.lbl_mag.pack(side="left", padx=8)

        # Plot options
        f4 = ttk.Frame(tab1); f4.pack(fill="x", padx=4, pady=4)
        self.show_thresh_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(f4, text="Show Thresholds", variable=self.show_thresh_var, command=self.redraw_all).pack(side="right")

        # Plots
        fplots = ttk.Frame(tab1); fplots.pack(fill="both", expand=True, padx=4, pady=6)
        self.fig, (self.ax_hp, self.ax_rms) = plt.subplots(1, 2, figsize=(10,4), dpi=100)
        self.fig.tight_layout(pad=3)
        self.ax_hp.set_title("HP Magnitude (g)")
        self.ax_hp.set_xlabel("time (s, scroll →)"); self.ax_hp.set_ylabel("g"); self.ax_hp.set_ylim(0, MAX_HP)
        self.hp_line, = self.ax_hp.plot([], [], color=CLR_HP, lw=1.8, antialiased=True)
        self.ax_rms.set_title("RMS (g)")
        self.ax_rms.set_xlabel("time (s, scroll →)"); self.ax_rms.set_ylabel("g"); self.ax_rms.set_ylim(0, MAX_RMS)
        self.rms_line, = self.ax_rms.plot([], [], color=CLR_RMS, lw=1.8, antialiased=True)
        self.th_struct_line = self.ax_rms.axhline(self.th_struct, color=CLR_STRUCT, lw=1.0, ls="--")
        self.th_foot_line   = self.ax_rms.axhline(self.th_foot,   color=CLR_WARN,   lw=1.0, ls="--")
        self.th_kid_line    = self.ax_rms.axhline(self.th_kid,    color=CLR_PLAY,   lw=1.0, ls="--")
        self.th_jump_line   = self.ax_rms.axhline(self.th_jump,   color=CLR_BAD,    lw=1.0, ls="--")
        self.canvas = FigureCanvasTkAgg(self.fig, master=fplots)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        # Calibration block
        f2 = ttk.LabelFrame(tab1, text="Calibration"); f2.pack(fill="x", padx=4, pady=6)
        ttk.Label(f2, text="Label:").pack(side="left")
        self.calib_combo = ttk.Combobox(f2, values=["CALM","STRUCT","FOOT","PLAY","JUMP"], width=10, state="readonly")
        self.calib_combo.set("CALM"); self.calib_combo.bind("<<ComboboxSelected>>", self._calib_label_changed)
        self.calib_combo.pack(side="left", padx=4)
        self.btn_calib_start = ttk.Button(f2, text="Start", command=self.calib_start); self.btn_calib_start.pack(side="left", padx=2)
        self.btn_calib_stop  = ttk.Button(f2, text="Stop", command=self.calib_stop, state="disabled"); self.btn_calib_stop.pack(side="left", padx=2)
        self.btn_calib_clear = ttk.Button(f2, text="Clear", command=self.calib_clear); self.btn_calib_clear.pack(side="left", padx=2)
        self.btn_compute = ttk.Button(f2, text="Compute Thresholds", command=self.compute_thresholds_clicked, state="disabled"); self.btn_compute.pack(side="left", padx=8)
        self.btn_save_th = ttk.Button(f2, text="Save Thresholds", command=self.save_thresholds, state="disabled"); self.btn_save_th.pack(side="left")
        self.btn_load_th = ttk.Button(f2, text="Load Thresholds", command=self.load_thresholds); self.btn_load_th.pack(side="left", padx=2)
        self.btn_export_cal = ttk.Button(f2, text="Export Calib CSV…", command=self.export_calib_csv); self.btn_export_cal.pack(side="left", padx=8)
        self.btn_import_cal = ttk.Button(f2, text="Import Calib CSV…", command=self.import_calib_csv); self.btn_import_cal.pack(side="left", padx=2)
        self.btn_append_cal = ttk.Button(f2, text="Append From Data CSV…", command=self.append_from_data_csv); self.btn_append_cal.pack(side="left", padx=2)
        self.calib_counts_lbl = ttk.Label(f2, text="Samples — CALM:0  STRUCT:0  FOOT:0  PLAY:0  JUMP:0", width=60)
        self.calib_counts_lbl.pack(side="left", padx=8)

        # Simulation controls (minimal)
        simf = ttk.LabelFrame(tab1, text="Simulation"); simf.pack(fill="x", padx=4, pady=6)
        ttk.Button(simf, text="Load CSV…", command=self.load_sim_csv).pack(side="left", padx=2)
        self.btn_start_sim = ttk.Button(simf, text="Start Sim", command=self.start_sim); self.btn_start_sim.pack(side="left", padx=2)
        self.btn_stop_sim  = ttk.Button(simf, text="Stop Sim", command=self.stop_sim, state="disabled"); self.btn_stop_sim.pack(side="left", padx=2)
        ttk.Label(simf, text="Rate (Hz):").pack(side="left", padx=6)
        self.sim_rate_var = tk.StringVar(value="100")
        tk.Entry(simf, textvariable=self.sim_rate_var, width=6).pack(side="left")
        self.sim_loop_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(simf, text="Loop", variable=self.sim_loop_var).pack(side="left", padx=6)
        self.sim_file_lbl = ttk.Label(simf, text="", width=60, anchor="w"); self.sim_file_lbl.pack(side="left", padx=8)

        # --- Tab 2: Debug / JSON ---
        tab2 = ttk.Frame(self.notebook)
        self.notebook.add(tab2, text="Debug / JSON")

        # Stats panel
        s1 = ttk.LabelFrame(tab2, text="Latency / Rate"); s1.pack(fill="x", padx=6, pady=6)
        self.lbl_cur_lat = ttk.Label(s1, text="Current latency: — ms"); self.lbl_cur_lat.pack(side="left", padx=6)
        self.lbl_avg_lat = ttk.Label(s1, text="Avg: — ms"); self.lbl_avg_lat.pack(side="left", padx=6)
        self.lbl_min_lat = ttk.Label(s1, text="Min: — ms"); self.lbl_min_lat.pack(side="left", padx=6)
        self.lbl_max_lat = ttk.Label(s1, text="Max: — ms"); self.lbl_max_lat.pack(side="left", padx=6)
        self.lbl_rate    = ttk.Label(s1, text="Rate: — msgs/s"); self.lbl_rate.pack(side="left", padx=6)

        # Split: table (top) and raw JSON (bottom)
        split = ttk.Panedwindow(tab2, orient="vertical"); split.pack(fill="both", expand=True, padx=6, pady=6)

        # Table
        table_frame = ttk.Frame(split)
        cols = ("local_time","device_ms","qos","lat_ms","label","rms","hp_abs","topic")
        self.tbl = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        for c, w in zip(cols, (140,100,60,80,80,80,80,240)):
            self.tbl.heading(c, text=c)
            self.tbl.column(c, width=w, anchor="center")
        yscroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tbl.yview)
        self.tbl.configure(yscrollcommand=yscroll.set)
        self.tbl.pack(side="left", fill="both", expand=True)
        yscroll.pack(side="left", fill="y")
        split.add(table_frame, weight=3)

        # Raw JSON viewer
        json_frame = ttk.Frame(split)
        self.json_text = tk.Text(json_frame, height=10, wrap="none")
        self.json_text.configure(font=("Courier New", 10))
        yscroll2 = ttk.Scrollbar(json_frame, orient="vertical", command=self.json_text.yview)
        self.json_text.configure(yscrollcommand=yscroll2.set)
        self.json_text.pack(side="left", fill="both", expand=True)
        yscroll2.pack(side="left", fill="y")
        split.add(json_frame, weight=2)

        # Footer hint
        f5 = ttk.Frame(self); f5.pack(fill="x", padx=10, pady=6)
        ttk.Label(f5, text='Expecting MQTT JSON on "smarthome/imu": {"ms","ax","ay","az","mag","hp_abs","rms","label"}').pack(side="right")

    # ---------- MQTT connect/disconnect ----------
    def connect_mqtt(self):
        if getattr(self, "mqtt_thread", None):
            messagebox.showwarning("MQTT", "Already connected.")
            return
        host = self.broker_var.get().strip()
        port = self.port_var.get().strip() or "1883"
        topic = self.topic_var.get().strip()
        if not host or not topic:
            messagebox.showwarning("MQTT", "Enter broker and topic first.")
            return
        self.mqtt_thread = MqttReader(host, port, topic, self.frame_queue, self._toast, self._mqtt_closed)
        self.mqtt_thread.start()
        self.btn_mqtt_connect.config(state="disabled")
        self.btn_mqtt_disconnect.config(state="normal")
        self.status_lbl.config(text="MQTT Connecting…"); self._set_chip((108,192,255))
        self._last_ms = -1; self._ema_rate = self.rate_hz_est

    def _mqtt_closed(self):
        self.mqtt_thread = None
        self.btn_mqtt_connect.config(state="normal")
        self.btn_mqtt_disconnect.config(state="disabled")
        self.status_lbl.config(text="MQTT Stopped"); self._set_chip((60,60,60))

    def disconnect_mqtt(self):
        if self.mqtt_thread:
            self.mqtt_thread.stop()
            self.mqtt_thread = None

    # ---------- Simulation ----------
    def load_sim_csv(self):
        path = filedialog.askopenfilename(title="Load CSV", filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if not path: return
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            self.sim_lines = [ln for ln in lines if not ln.startswith("#")]
            self.sim_file_lbl.config(text=os.path.basename(path)+f" ({len(self.sim_lines)} lines)")
            self.btn_start_sim.config(state="normal")
        except Exception as e:
            self._toast(f"Failed to load: {e}")

    def start_sim(self):
        if self.simulating: return
        # stop MQTT first
        self.disconnect_mqtt()
        try:
            rate = float(self.sim_rate_var.get().strip() or "100")
        except:
            rate = 100.0
        loop = bool(self.sim_loop_var.get())
        if not self.sim_lines:
            # synthetic 30s
            self.sim_lines = self._generate_synthetic_csv(30, rate)
            self.sim_file_lbl.config(text=f"Synthetic 30s @ {rate:.0f} Hz ({len(self.sim_lines)} lines)")
        self.hp_series.clear(); self.rms_series.clear(); self.redraw_all()
        self.sim_thread = SimReader(self.sim_lines, rate, loop, self.frame_queue, self._sim_finished)
        self.sim_thread.start()
        self.simulating = True
        self.btn_start_sim.config(state="disabled")
        self.btn_stop_sim.config(state="normal")
        self.status_lbl.config(text="Simulating"); self._set_chip((108,192,255))

    def stop_sim(self):
        if self.sim_thread:
            self.sim_thread.stop()
            self.sim_thread = None
        self._sim_finished()

    def _sim_finished(self):
        self.simulating = False
        self.btn_start_sim.config(state="normal" if self.sim_lines else "disabled")
        self.btn_stop_sim.config(state="disabled")
        self.status_lbl.config(text="Simulation Stopped"); self._set_chip((60,60,60))

    # ---------- Logging ----------
    def toggle_logging(self):
        if not self.logging:
            try:
                logdir = os.path.join(os.path.expanduser("~"), "IMU6500Logs")
                os.makedirs(logdir, exist_ok=True)
                path = os.path.join(logdir, time.strftime("imu6500_%Y%m%d_%H%M%S.csv"))
                self.log_fp = open(path, "w", encoding="utf-8", newline="")
                self.log_fp.write("#HDR ms,ax,ay,az,mag,hp_abs,rms,label\n")
                self.logging = True; self.log_path = path
                self.btn_log.config(text="Stop Logging"); self.btn_openlog.config(state="normal")
                self.log_path_lbl.config(text=path)
            except Exception as e:
                self._toast(f"Cannot start logging: {e}")
        else:
            try:
                self.logging = False
                if self.log_fp:
                    self.log_fp.flush(); self.log_fp.close(); self.log_fp = None
                self.btn_log.config(text="Start Logging")
            except:
                pass

    def open_log(self):
        if not self.log_path or not os.path.exists(self.log_path):
            self._toast("No log file to open.")
            return
        os.system(f'xdg-open "{self.log_path}" >/dev/null 2>&1 &')

    # ---------- UI pump ----------
    def _ui_pump(self):
        drained = 0
        last_latency_ms = None
        last_raw_json = None

        while drained < 500 and not self.frame_queue.empty():
            frame = self.frame_queue.get()
            if not isinstance(frame, MqttFrame):
                continue

            smp = frame.sample

            # Logging (CSV)
            if self.logging and self.log_fp:
                self.log_fp.write(f"{smp.ms},{smp.ax:.4f},{smp.ay:.4f},{smp.az:.4f},{smp.mag:.4f},{smp.hp_abs:.4f},{smp.rms:.4f},{smp.label}\n")

            # Rate estimate from device ms (plots) and arrival_ts (stats)
            if smp.ms > 0:
                if self._last_ms >= 0:
                    d = smp.ms - self._last_ms
                    if d > 0:
                        inst = 1000.0 / d
                        self._ema_rate = 0.9*self._ema_rate + 0.1*inst
                        self.rate_hz_est = self._ema_rate
                self._last_ms = smp.ms

            # Update time-series for plots
            self.hp_series.append(clamp(smp.hp_abs, 0.0, MAX_HP))
            self.rms_series.append(clamp(smp.rms, 0.0, MAX_RMS))

            # Calibration capture
            if self.calibrating:
                self.calib[self.calib_active_label].append(smp.rms)
                self.btn_compute.config(state="normal")
                if sum(len(v) for v in self.calib.values()) % 50 == 0:
                    self._update_calib_counts()

            # Status text + chip
            self.lbl_hp.config(text=f"HP: {smp.hp_abs:.3f}")
            self.lbl_rms.config(text=f"RMS: {smp.rms:.3f}")
            self.lbl_mag.config(text=f"Mag: {smp.mag:.3f}")
            self._update_status(smp.label)

            # Debug tab: latency & table
            self.arrival_ts.append(frame.recv_ts)
            now_ms = time.time() * 1000.0
            latency_ms = max(0.0, now_ms - float(smp.ms)) if smp.ms > 0 else 0.0
            self.lat_samples.append(latency_ms)
            last_latency_ms = latency_ms

            lt = time.strftime("%H:%M:%S", time.localtime(frame.recv_ts)) + f".{int((frame.recv_ts%1)*1000):03d}"
            row = (lt, smp.ms, frame.qos, f"{latency_ms:.1f}", smp.label, f"{smp.rms:.3f}", f"{smp.hp_abs:.3f}", frame.topic)
            self.debug_rows.append(row)

            last_raw_json = frame.raw_json
            self._last_raw_json = frame.raw_json

            drained += 1

        if drained:
            self.redraw_all()
            self._refresh_debug_table()
            self._refresh_json_viewer(last_raw_json or self._last_raw_json)
            self._refresh_stats(last_latency_ms)

        self.after(16, self._ui_pump)

    # ---------- Drawing ----------
    def redraw_all(self):
        n = max(len(self.hp_series), len(self.rms_series))
        if n <= 1:
            self.hp_line.set_data([], [])
            self.rms_line.set_data([], [])
            self.canvas.draw_idle()
            return
        rate = self.rate_hz_est if self.rate_hz_est > 1 else 100.0
        window = (n-1) / rate
        x = [i*(window/(n-1)) for i in range(n)]  # 0..window seconds

        self.ax_hp.set_xlim(0, window)
        self.ax_rms.set_xlim(0, window)

        self.hp_line.set_data(x, list(self.hp_series))
        self.rms_line.set_data(x, list(self.rms_series))

        # Threshold lines
        vis = self.show_thresh_var.get()
        for line in (self.th_struct_line, self.th_foot_line, self.th_kid_line, self.th_jump_line):
            line.set_visible(vis)
        self.th_struct_line.set_ydata([self.th_struct, self.th_struct])
        self.th_foot_line.set_ydata([self.th_foot, self.th_foot])
        self.th_kid_line.set_ydata([self.th_kid, self.th_kid])
        self.th_jump_line.set_ydata([self.th_jump, self.th_jump])

        self.canvas.draw_idle()

    # ---------- Debug helpers ----------
    def _refresh_debug_table(self):
        self.tbl.delete(*self.tbl.get_children())
        for row in list(self.debug_rows)[-DEBUG_ROWS:]:
            self.tbl.insert("", "end", values=row)
        if self.tbl.get_children():
            self.tbl.see(self.tbl.get_children()[-1])

    def _refresh_json_viewer(self, raw_json):
        if not raw_json:
            return
        try:
            obj = json.loads(raw_json)
            pretty = json.dumps(obj, indent=2)
        except Exception:
            pretty = raw_json
        self.json_text.delete("1.0", "end")
        self.json_text.insert("1.0", pretty)

    def _refresh_stats(self, last_latency_ms):
        if last_latency_ms is not None:
            self.lbl_cur_lat.config(text=f"Current latency: {last_latency_ms:.1f} ms")
        if len(self.lat_samples) >= 2:
            lat_list = list(self.lat_samples)
            self.lbl_avg_lat.config(text=f"Avg: {statistics.fmean(lat_list):.1f} ms")
            self.lbl_min_lat.config(text=f"Min: {min(lat_list):.1f} ms")
            self.lbl_max_lat.config(text=f"Max: {max(lat_list):.1f} ms")
        else:
            self.lbl_avg_lat.config(text="Avg: — ms")
            self.lbl_min_lat.config(text="Min: — ms")
            self.lbl_max_lat.config(text="Max: — ms")

        if len(self.arrival_ts) >= 3:
            diffs = [self.arrival_ts[i]-self.arrival_ts[i-1] for i in range(1, len(self.arrival_ts))]
            mean_dt = statistics.fmean(diffs)
            rate = 1.0 / mean_dt if mean_dt > 0 else 0.0
            self.lbl_rate.config(text=f"Rate: {rate:.1f} msgs/s")
        else:
            self.lbl_rate.config(text="Rate: — msgs/s")

    # ---------- Calibration ----------
    def _calib_label_changed(self, *_):
        self.calib_active_label = self.calib_combo.get().strip().upper() or "CALM"

    def calib_start(self):
        self.calibrating = True
        self.btn_calib_start.config(state="disabled")
        self.btn_calib_stop.config(state="normal")
        self.btn_compute.config(state="normal")

    def calib_stop(self):
        self.calibrating = False
        self.btn_calib_start.config(state="normal")
        self.btn_calib_stop.config(state="disabled")

    def calib_clear(self):
        for k in self.calib.keys():
            self.calib[k].clear()
        self._update_calib_counts()
        self._toast("Calibration buffers cleared.")

    def _update_calib_counts(self):
        txt = "Samples — " + "  ".join([f"{k}:{len(v)}" for k,v in self.calib.items()])
        self.calib_counts_lbl.config(text=txt)

    def compute_thresholds_clicked(self):
        ok, a,b,c,d, msg = compute_thresholds(self.calib)
        if not ok:
            messagebox.showwarning("Calibration", msg); return
        self.th_struct, self.th_foot, self.th_kid, self.th_jump = a,b,c,d
        self.btn_save_th.config(state="normal")
        self.redraw_all()
        messagebox.showinfo("Calibration",
            f"New thresholds (g):\nSTRUCT ≥ {a:.3f}\nFOOT   ≥ {b:.3f}\nPLAY   ≥ {c:.3f}\nJUMP   ≥ {d:.3f}")

    def save_thresholds(self):
        path = filedialog.asksaveasfilename(title="Save Thresholds", defaultextension=".txt",
                                            filetypes=[("Text files","*.txt"),("All files","*.*")])
        if not path: return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("# thresholds (g)\n")
                f.write(f"TH_STRUCT={self.th_struct:.6f}\n")
                f.write(f"TH_FOOT={self.th_foot:.6f}\n")
                f.write(f"TH_KID={self.th_kid:.6f}\n")
                f.write(f"TH_JUMP={self.th_jump:.6f}\n")
            self._toast(f"Saved: {path}")
        except Exception as e:
            self._toast(f"Save failed: {e}")

    def load_thresholds(self):
        path = filedialog.askopenfilename(title="Load Thresholds", filetypes=[("Text files","*.txt"),("All files","*.*")])
        if not path: return
        try:
            with open(path, "r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln or ln.startswith("#"): continue
                    if "=" not in ln: continue
                    k,v = ln.split("=",1)
                    try: d = float(v.strip())
                    except: continue
                    k = k.strip().upper()
                    if   k == "TH_STRUCT": self.th_struct = d
                    elif k == "TH_FOOT":   self.th_foot   = d
                    elif k == "TH_KID":    self.th_kid    = d
                    elif k == "TH_JUMP":   self.th_jump   = d
            self.redraw_all()
            messagebox.showinfo("Calibration",
                f"Loaded (g):\nSTRUCT ≥ {self.th_struct:.3f}\nFOOT   ≥ {self.th_foot:.3f}\nPLAY   ≥ {self.th_kid:.3f}\nJUMP   ≥ {self.th_jump:.3f}")
        except Exception as e:
            self._toast(f"Load failed: {e}")

    def export_calib_csv(self):
        path = filedialog.asksaveasfilename(title="Export Calibration CSV", defaultextension=".csv",
                                            filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if not path: return
        try:
            with open(path, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                f.write("#CALIB rms,label\n")
                for lbl, lst in self.calib.items():
                    for v in lst:
                        w.writerow([f"{v:.6f}", lbl])
            self._toast(f"Exported: {path}")
        except Exception as e:
            self._toast(f"Export failed: {e}")

    def import_calib_csv(self):
        path = filedialog.askopenfilename(title="Import Calibration CSV", filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if not path: return
        try:
            added = 0
            with open(path, "r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln or ln.startswith("#"): continue
                    parts = [x.strip() for x in ln.split(',')]
                    if len(parts) >= 2:
                        try:
                            rms = float(parts[0]); lbl = parts[1].upper()
                            if lbl in self.calib:
                                self.calib[lbl].append(rms); added += 1
                        except: pass
                    elif len(parts) == 1:
                        try:
                            rms = float(parts[0]); self.calib[self.calib_active_label].append(rms); added += 1
                        except: pass
            self._update_calib_counts()
            self.btn_compute.config(state="normal")
            self._toast(f"Imported {added} samples")
        except Exception as e:
            self._toast(f"Import failed: {e}")

    def append_from_data_csv(self):
        """Append RMS from data CSV (column 7) into current calibration class."""
        path = filedialog.askopenfilename(title="Append From Data CSV", filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if not path: return
        try:
            added = 0
            with open(path, "r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln or ln.startswith("#"): continue
                    parts = [x.strip() for x in ln.split(',')]
                    if len(parts) >= 7:
                        try:
                            rms = float(parts[6])
                            self.calib[self.calib_active_label].append(rms)
                            added += 1
                        except:
                            pass
            self._update_calib_counts()
            self.btn_compute.config(state="normal")
            self._toast(f"Appended {added} RMS samples → {self.calib_active_label}")
        except Exception as e:
            self._toast(f"Append failed: {e}")

    # ---------- Status / helpers ----------
    def _set_chip(self, rgb_255_tuple):
        r,g,b = rgb_255_tuple
        self.status_chip.delete("all")
        self.status_chip.create_rectangle(0,0,20,20, fill=f"#{r:02x}{g:02x}{b:02x}", width=0)

    def _set_chip_rgb(self, rgb01):
        r,g,b = [int(255*x) for x in rgb01]
        self._set_chip((r,g,b))

    def _update_status(self, label):
        up = (label or "").upper()
        self.status_lbl.config(text=label)
        if up == "JUMP": self._set_chip_rgb(CLR_BAD)
        elif up == "PLAY": self._set_chip_rgb(CLR_PLAY)
        elif up == "FOOT": self._set_chip_rgb(CLR_WARN)
        elif up == "STRUCT": self._set_chip_rgb(CLR_STRUCT)
        else: self._set_chip_rgb(CLR_CALM)

    def _toast(self, msg):
        try:
            self.status_lbl.config(text=msg)
        except:
            pass

    # ---------- Synthetic CSV ----------
    def _generate_synthetic_csv(self, seconds: int, rate_hz: float) -> List[str]:
        total = max(1, int(seconds * rate_hz))
        lines = []
        rnd = random.Random(1)
        win = 25
        q = deque(maxlen=win)
        sumsq = 0.0
        for i in range(1, total+1):
            t = i / rate_hz
            burst = math.sin(2*math.pi*1.2*t) * (0.05 + 0.05*rnd.random())
            transient = 0.15 if (i % 777 == 0) else 0.0
            noise = (rnd.random() - 0.5) * 0.01
            hp = abs(burst + transient) + abs(noise)
            hp = clamp(hp, 0.0, 0.6)
            if len(q) == win:
                old = q[0]; sumsq -= old*old
            q.append(hp); sumsq += hp*hp
            rms = math.sqrt(sumsq / len(q))
            label = compute_label_from_rms_custom(rms, self.th_struct, self.th_foot, self.th_kid, self.th_jump)
            ax = noise; ay=noise; az=1.0+noise; mag = math.sqrt(ax*ax+ay*ay+az*az)
            ms = int(round(i * (1000.0 / rate_hz)))
            lines.append(f"{ms},{ax:.4f},{ay:.4f},{az:.4f},{mag:.4f},{hp:.4f},{rms:.4f},{label}")
        return lines

# -------------- main --------------
def main():
    try:
        from tkinter import font
        default_font = font.nametofont("TkDefaultFont"); default_font.configure(size=10)
    except: pass
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
